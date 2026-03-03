"""
pawn_queue.lease
~~~~~~~~~~~~~~~~
Lease acquisition, refresh, release, and janitor logic.

Two concurrency strategies are supported:

conditional_write
    Uses ``If-None-Match: *`` on PutObject.  Supported by AWS S3 (≥ Aug 2024)
    and Cloudflare R2.  Not supported by Hetzner (Ceph RGW) as of March 2026.

csprng_verify  (default / fallback)
    Cryptographic compare-and-swap via S3 ETags.

    1. Generate a 256-bit CSPRNG nonce — collision probability ≈ 2⁻²⁵⁶.
    2. Serialize the lease JSON canonically (sort_keys=True, no whitespace) so
       the MD5 is pre-computable.
    3. PUT the lease object.  S3 last-write-wins; we don't know yet who won.
    4. Sleep a random jitter window (100–400 ms by default) so any concurrent
       PUT requests in flight have time to arrive and be applied at S3.
    5. HEAD the lease key → read the surviving ETag.
    6. If ``ETag == MD5(our_bytes)`` → first-round win; continue.
    7. Optionally re-verify after another short sleep (verify_retries rounds).
    8. If ETag ≠ ours → another consumer's bytes survived; back off.
    9. Post-verify confirmation: sleep jitter_min_ms once more, then do a
       final HEAD.  This guards against late-arriving competitor writes that
       could overwrite our lease after step 7 but before we claim the message.

    Safety guarantee: two consumers write different bytes (different nonces).
    S3 strong read-after-write consistency ensures each HEAD always returns the
    current winning write's ETag.  The post-verify confirmation window (step 9)
    ensures we detect any write that arrived at S3 after our last retry HEAD.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from .config import PawnQueueConfig
from .exceptions import LeaseConflictError, LeaseExpiredError
from .utils import (
    canonical_json,
    generate_nonce,
    lease_key,
    md5_etag,
    parse_iso,
    utcnow,
    utcnow_iso,
)

if TYPE_CHECKING:
    from .client import S3Client

logger = logging.getLogger(__name__)


class LeaseContent:
    """Holds the canonical JSON bytes and expected ETag for a lease."""

    __slots__ = ("consumer_id", "message_id", "claimed_at", "expires_at", "nonce", "_bytes", "_etag")

    def __init__(
        self,
        consumer_id: str,
        message_id: str,
        claimed_at: str,
        expires_at: str,
        nonce: str,
    ) -> None:
        self.consumer_id = consumer_id
        self.message_id = message_id
        self.claimed_at = claimed_at
        self.expires_at = expires_at
        self.nonce = nonce
        self._bytes: bytes = canonical_json(self.to_dict())
        self._etag: str = md5_etag(self._bytes)

    def to_dict(self) -> dict:
        return {
            "consumer_id": self.consumer_id,
            "message_id": self.message_id,
            "claimed_at": self.claimed_at,
            "expires_at": self.expires_at,
            "nonce": self.nonce,
        }

    @property
    def body(self) -> bytes:
        return self._bytes

    @property
    def expected_etag(self) -> str:
        return self._etag

    def is_expired(self) -> bool:
        try:
            return parse_iso(self.expires_at) < utcnow()
        except Exception:
            return True

    @classmethod
    def from_dict(cls, data: dict) -> "LeaseContent":
        return cls(
            consumer_id=data["consumer_id"],
            message_id=data["message_id"],
            claimed_at=data["claimed_at"],
            expires_at=data["expires_at"],
            nonce=data.get("nonce", ""),
        )


class LeaseManager:
    """Acquires, refreshes, releases, and garbage-collects leases."""

    def __init__(self, client: "S3Client", config: PawnQueueConfig) -> None:
        self._client = client
        self._config = config

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def acquire(self, topic: str, message_id: str, consumer_id: str) -> LeaseContent:
        """
        Try to acquire a lease for *message_id* on behalf of *consumer_id*.

        Returns the LeaseContent on success.
        Raises LeaseConflictError if another consumer already holds the lease.
        """
        strategy = self._config.effective_strategy()
        if strategy == "conditional_write":
            return await self._acquire_conditional(topic, message_id, consumer_id)
        else:
            return await self._acquire_csprng(topic, message_id, consumer_id)

    async def refresh(self, topic: str, lease: LeaseContent) -> LeaseContent:
        """
        Extend the expiry of an existing lease.

        Returns a new LeaseContent with an updated expires_at.
        """
        visibility = self._config.polling.visibility_timeout_seconds
        expires_at = (utcnow() + timedelta(seconds=visibility)).isoformat()
        refreshed = LeaseContent(
            consumer_id=lease.consumer_id,
            message_id=lease.message_id,
            claimed_at=lease.claimed_at,
            expires_at=expires_at,
            nonce=lease.nonce,  # keep same nonce so ETag is stable for csprng_verify
        )
        key = lease_key(topic, lease.message_id)
        await self._client.put_object(key, refreshed.body)
        logger.debug("Lease refreshed for message %s, expires %s", lease.message_id, expires_at)
        return refreshed

    async def release(self, topic: str, message_id: str) -> None:
        """Delete the lease object (called by ack/nack via Message)."""
        key = lease_key(topic, message_id)
        await self._client.delete_object(key)

    async def janitor_pass(self, topic: str) -> int:
        """
        Scan all leases for *topic* and delete any that have expired.

        Returns the number of leases reclaimed.  Stale leases are created when
        consumers crash without releasing their lease; once deleted, the
        corresponding message becomes visible to other consumers again.
        """
        prefix = f"{topic}/leases/"
        keys = await self._client.list_objects(prefix)
        reclaimed = 0
        for key in keys:
            try:
                data = await self._client.get_object_json(key)
                lc = LeaseContent.from_dict(data)
                if lc.is_expired():
                    await self._client.delete_object(key)
                    reclaimed += 1
                    logger.info(
                        "Janitor: reclaimed expired lease for message %s (consumer %s)",
                        lc.message_id,
                        lc.consumer_id,
                    )
            except Exception as exc:
                logger.debug("Janitor: could not process lease %s: %s", key, exc)
        return reclaimed

    async def is_leased(self, topic: str, message_id: str) -> bool:
        """Return True if a valid (non-expired) lease exists for *message_id*."""
        key = lease_key(topic, message_id)
        try:
            data = await self._client.get_object_json(key)
            lc = LeaseContent.from_dict(data)
            return not lc.is_expired()
        except (KeyError, Exception):
            return False

    # ------------------------------------------------------------------
    # Strategy implementations
    # ------------------------------------------------------------------

    async def _acquire_conditional(
        self, topic: str, message_id: str, consumer_id: str
    ) -> LeaseContent:
        """Lease acquisition via If-None-Match: * (atomic, requires backend support)."""
        visibility = self._config.polling.visibility_timeout_seconds
        now = utcnow_iso()
        expires_at = (utcnow() + timedelta(seconds=visibility)).isoformat()
        nonce = generate_nonce()
        lease = LeaseContent(
            consumer_id=consumer_id,
            message_id=message_id,
            claimed_at=now,
            expires_at=expires_at,
            nonce=nonce,
        )
        key = lease_key(topic, message_id)
        # Will raise LeaseConflictError if another consumer already holds the key
        await self._client.put_object(key, lease.body, if_none_match=True)
        logger.debug("Lease acquired (conditional_write) for message %s", message_id)
        return lease

    async def _acquire_csprng(
        self, topic: str, message_id: str, consumer_id: str
    ) -> LeaseContent:
        """
        Lease acquisition via Cryptographic CAS (csprng_verify strategy).

        Works on any S3-compatible storage with read-after-write consistency,
        including Hetzner Object Storage (Ceph RGW).
        """
        cv = self._config.concurrency.csprng_verify
        visibility = self._config.polling.visibility_timeout_seconds
        now = utcnow_iso()
        expires_at = (utcnow() + timedelta(seconds=visibility)).isoformat()
        nonce = generate_nonce()
        lease = LeaseContent(
            consumer_id=consumer_id,
            message_id=message_id,
            claimed_at=now,
            expires_at=expires_at,
            nonce=nonce,
        )
        key = lease_key(topic, message_id)

        # Step 1 — write (last writer wins; don't know yet if we won)
        await self._client.put_object(key, lease.body)

        # Step 2 — random jitter: let all concurrent writers finish
        jitter_ms = random.randint(cv.jitter_min_ms, cv.jitter_max_ms)
        await asyncio.sleep(jitter_ms / 1000.0)

        # Step 3 — verify: did our bytes survive?
        if not await self._verify_lease(key, lease.expected_etag, cv.verify_retries, cv.verify_retry_delay_ms):
            raise LeaseConflictError(message_id)

        # Step 4 — post-verify confirmation: pause for jitter_min_ms to let any
        # late-arriving competitor writes settle, then do a final HEAD.  This
        # closes the window where a slow-network PUT arrives at S3 *after* we
        # completed all retry verifications but before we claim the message.
        await asyncio.sleep(cv.jitter_min_ms / 1000.0)
        try:
            meta = await self._client.head_object(key)
            final_etag = meta.get("ETag", "").strip('"').lower()
            if final_etag != lease.expected_etag.strip('"').lower():
                raise LeaseConflictError(message_id)
        except KeyError:
            raise LeaseConflictError(message_id)

        logger.debug("Lease acquired (csprng_verify) for message %s", message_id)
        return lease

    async def _verify_lease(
        self,
        key: str,
        expected_etag: str,
        retries: int,
        retry_delay_ms: int,
    ) -> bool:
        """
        HEAD *key* and compare the stored ETag to *expected_etag*.

        Returns True only if the stored ETag matches on all *retries*.
        """
        for attempt in range(retries + 1):
            if attempt > 0:
                await asyncio.sleep(retry_delay_ms / 1000.0)
            try:
                meta = await self._client.head_object(key)
            except KeyError:
                # Object disappeared — another consumer may have won and cleaned up
                return False
            stored_etag = meta.get("ETag", "")
            # Normalize: some backends wrap in quotes, some don't
            stored_norm = stored_etag.strip('"').lower()
            expected_norm = expected_etag.strip('"').lower()
            if stored_norm != expected_norm:
                return False
        return True

    def _make_lease_content(
        self,
        consumer_id: str,
        message_id: str,
        visibility: float,
        nonce: str | None = None,
    ) -> LeaseContent:
        now = utcnow_iso()
        expires_at = (utcnow() + timedelta(seconds=visibility)).isoformat()
        return LeaseContent(
            consumer_id=consumer_id,
            message_id=message_id,
            claimed_at=now,
            expires_at=expires_at,
            nonce=nonce or generate_nonce(),
        )

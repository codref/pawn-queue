"""
pawn_queue.utils
~~~~~~~~~~~~~~~~
Shared utility helpers.
"""

from __future__ import annotations

import hashlib
import json
import secrets
import uuid
from datetime import datetime, timezone


def new_uuid() -> str:
    """Generate a random UUIDv4 string."""
    return str(uuid.uuid4())


def utcnow() -> datetime:
    """Return current UTC datetime (timezone-aware)."""
    return datetime.now(timezone.utc)


def utcnow_iso() -> str:
    """Return current UTC time as an ISO-8601 string."""
    return utcnow().isoformat()


def sortable_key_prefix() -> str:
    """
    Return a timestamp prefix suitable for S3 keys that sorts in FIFO order.
    Format: YYYYMMDDTHHMMSS_ffffff
    """
    now = utcnow()
    return now.strftime("%Y%m%dT%H%M%S_") + f"{now.microsecond:06d}"


def message_key(topic: str, key_prefix: str, message_id: str) -> str:
    """S3 key for a pending message object."""
    return f"{topic}/messages/{key_prefix}-{message_id}.json"


def lease_key(topic: str, message_id: str) -> str:
    """S3 key for a lease object."""
    return f"{topic}/leases/{message_id}.lease"


def dead_letter_key(topic: str, message_id: str) -> str:
    """S3 key for a dead-letter message object."""
    return f"{topic}/dead-letter/{message_id}.json"


def topic_marker_key(topic: str) -> str:
    """S3 key that marks a topic as registered."""
    return f"{topic}/.topic"


def registry_key(role: str, entity_id: str) -> str:
    """S3 key for a registry entry."""
    return f"_registry/{role}/{entity_id}.json"


def canonical_json(data: dict) -> bytes:
    """
    Serialize *data* to canonical UTF-8 JSON.

    sort_keys=True + no whitespace ensures the same bytes regardless of
    insertion order — a prerequisite for pre-computing MD5 ETags.
    """
    return json.dumps(data, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def md5_etag(data: bytes) -> str:
    """
    Compute the MD5 hex digest of *data* and wrap it in double-quotes as
    S3 returns it in the ETag header.
    """
    digest = hashlib.md5(data).hexdigest()
    return f'"{digest}"'


def generate_nonce() -> str:
    """
    Generate a 256-bit CSPRNG nonce as a hex string.
    Collision probability ≈ 2⁻²⁵⁶ — effectively impossible.
    """
    return secrets.token_hex(32)


def parse_iso(ts: str) -> datetime:
    """Parse an ISO-8601 timestamp (with timezone) produced by utcnow_iso()."""
    return datetime.fromisoformat(ts)

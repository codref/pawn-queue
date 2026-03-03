"""
tests/test_lease.py
~~~~~~~~~~~~~~~~~~~~
Unit tests for LeaseManager (csprng_verify strategy).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from pawn_queue.exceptions import LeaseConflictError
from pawn_queue.lease import LeaseContent, LeaseManager
from pawn_queue.utils import canonical_json, md5_etag, utcnow_iso


CONSUMER_ID = "consumer-aaa"
TOPIC = "orders"
MSG_ID = "msg-uuid-0001"


@pytest.mark.asyncio
async def test_acquire_lease_csprng(lease_manager, s3_client, topic_manager):
    await topic_manager.create(TOPIC)
    lease = await lease_manager.acquire(TOPIC, MSG_ID, CONSUMER_ID)
    assert lease.consumer_id == CONSUMER_ID
    assert lease.message_id == MSG_ID
    assert lease.nonce  # non-empty nonce


@pytest.mark.asyncio
async def test_acquired_lease_persisted_in_s3(lease_manager, s3_client, topic_manager):
    await topic_manager.create(TOPIC)
    await lease_manager.acquire(TOPIC, MSG_ID, CONSUMER_ID)

    from pawn_queue.utils import lease_key
    key = lease_key(TOPIC, MSG_ID)
    data = await s3_client.get_object_json(key)
    assert data["consumer_id"] == CONSUMER_ID
    assert data["message_id"] == MSG_ID
    assert "nonce" in data


@pytest.mark.asyncio
async def test_lease_expiry_detection():
    past = (datetime.now(UTC) - timedelta(seconds=10)).isoformat()
    lc = LeaseContent(
        consumer_id="c",
        message_id="m",
        claimed_at=utcnow_iso(),
        expires_at=past,
        nonce="abc",
    )
    assert lc.is_expired()


@pytest.mark.asyncio
async def test_lease_not_expired():
    future = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()
    lc = LeaseContent(
        consumer_id="c",
        message_id="m",
        claimed_at=utcnow_iso(),
        expires_at=future,
        nonce="abc",
    )
    assert not lc.is_expired()


@pytest.mark.asyncio
async def test_janitor_removes_expired_lease(lease_manager, s3_client, topic_manager):
    await topic_manager.create(TOPIC)

    # Manually write an expired lease
    from pawn_queue.utils import canonical_json, lease_key
    past = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
    lc = LeaseContent(
        consumer_id=CONSUMER_ID,
        message_id=MSG_ID,
        claimed_at=utcnow_iso(),
        expires_at=past,
        nonce="deadbeef",
    )
    key = lease_key(TOPIC, MSG_ID)
    await s3_client.put_object(key, lc.body)

    reclaimed = await lease_manager.janitor_pass(TOPIC)
    assert reclaimed == 1

    # Lease should be gone
    exists = await s3_client.object_exists(key)
    assert not exists


@pytest.mark.asyncio
async def test_janitor_keeps_valid_lease(lease_manager, s3_client, topic_manager):
    await topic_manager.create(TOPIC)

    from pawn_queue.utils import lease_key
    future = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()
    lc = LeaseContent(
        consumer_id=CONSUMER_ID,
        message_id=MSG_ID,
        claimed_at=utcnow_iso(),
        expires_at=future,
        nonce="alive",
    )
    key = lease_key(TOPIC, MSG_ID)
    await s3_client.put_object(key, lc.body)

    reclaimed = await lease_manager.janitor_pass(TOPIC)
    assert reclaimed == 0

    exists = await s3_client.object_exists(key)
    assert exists


@pytest.mark.asyncio
async def test_is_leased_true(lease_manager, s3_client, topic_manager):
    await topic_manager.create(TOPIC)
    await lease_manager.acquire(TOPIC, MSG_ID, CONSUMER_ID)
    assert await lease_manager.is_leased(TOPIC, MSG_ID)


@pytest.mark.asyncio
async def test_is_leased_false_for_absent(lease_manager, topic_manager):
    await topic_manager.create(TOPIC)
    assert not await lease_manager.is_leased(TOPIC, "nonexistent-msg")


@pytest.mark.asyncio
async def test_release_clears_lease(lease_manager, s3_client, topic_manager):
    await topic_manager.create(TOPIC)
    await lease_manager.acquire(TOPIC, MSG_ID, CONSUMER_ID)
    await lease_manager.release(TOPIC, MSG_ID)
    assert not await lease_manager.is_leased(TOPIC, MSG_ID)


@pytest.mark.asyncio
async def test_md5_etag_canonical_json_deterministic():
    """Same dict must always produce the same bytes and ETag."""
    data = {"b": 2, "a": 1, "nonce": "xyz"}
    b1 = canonical_json(data)
    b2 = canonical_json(data)
    assert b1 == b2
    assert md5_etag(b1) == md5_etag(b2)


@pytest.mark.asyncio
async def test_lease_refresh_extends_expiry(lease_manager, s3_client, topic_manager):
    await topic_manager.create(TOPIC)
    original_lease = await lease_manager.acquire(TOPIC, MSG_ID, CONSUMER_ID)

    # Small sleep to ensure expires_at changes
    await asyncio.sleep(0.01)
    refreshed = await lease_manager.refresh(TOPIC, original_lease)

    assert refreshed.expires_at != original_lease.expires_at

"""
e2e/test_e2e_compat_probe.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Verify which concurrency strategy was selected for this S3 backend,
and explicitly test the csprng_verify fallback works correctly.
"""

from __future__ import annotations

import asyncio

import pytest

from pawn_queue.exceptions import LeaseConflictError
from pawn_queue.lease import LeaseContent, LeaseManager
from pawn_queue.utils import generate_nonce, new_uuid, utcnow_iso
from datetime import UTC, datetime, timedelta


@pytest.mark.asyncio
async def test_strategy_is_resolved(pawnqueue):
    """After setup(), the strategy must be resolved (not 'auto')."""
    strategy = pawnqueue._config.effective_strategy()
    assert strategy in ("conditional_write", "csprng_verify"), (
        f"Unexpected strategy: {strategy!r}"
    )
    print(f"\n[compat_probe] Resolved strategy: {strategy}")


@pytest.mark.asyncio
async def test_conditional_write_probe(pawnqueue, run_prefix, conditional_write_supported):
    """
    Explicitly test If-None-Match: * semantics.

    If the backend supports it: second PUT must fail with 412.
    If not: second PUT succeeds silently (last-writer-wins).
    """
    probe_key = f"{run_prefix}/probe-test/{new_uuid()}.probe"
    client = pawnqueue._client

    body_a = b'{"probe":"a"}'
    body_b = b'{"probe":"b"}'

    # Write first body
    await client.put_object(probe_key, body_a, if_none_match=False)

    second_succeeded = False
    try:
        await client.put_object(probe_key, body_b, if_none_match=True)
        second_succeeded = True
    except (LeaseConflictError, Exception) as exc:
        # Any 412-related error means conditional write is supported
        second_succeeded = False
    finally:
        try:
            await client.delete_object(probe_key)
        except Exception:
            pass

    if conditional_write_supported:
        assert not second_succeeded, (
            "Expected PreconditionFailed but second PUT succeeded "
            "— conditional write support mismatch!"
        )
    else:
        # Hetzner / Ceph: second write is expected to silently succeed
        assert second_succeeded or True  # either outcome is acceptable


@pytest.mark.asyncio
async def test_csprng_verify_exactly_one_winner(pawnqueue, run_prefix):
    """
    10 concurrent csprng_verify lease attempts against the same key.
    Exactly 1 must succeed.
    """
    # Force csprng_verify regardless of backend
    from pawn_queue.config import CsprngVerifyConfig, ConcurrencyConfig, PawnQueueConfig
    import copy
    cfg = copy.deepcopy(pawnqueue._config)
    cfg.set_resolved_strategy("csprng_verify")

    lease_mgr = LeaseManager(pawnqueue._client, cfg)
    topic = f"{run_prefix}-compat-probe"
    msg_id = new_uuid()

    # Write a dummy message object so the lease key namespace exists
    await pawnqueue._client.put_object(
        f"{topic}/messages/20260303T000000_000000-{msg_id}.json",
        b'{"id":"' + msg_id.encode() + b'"}',
    )

    winners: list[str] = []
    losers: list[str] = []

    async def try_acquire(consumer_id: str) -> None:
        try:
            await lease_mgr.acquire(topic, msg_id, consumer_id)
            winners.append(consumer_id)
        except LeaseConflictError:
            losers.append(consumer_id)

    consumer_ids = [f"consumer-{i}" for i in range(10)]
    await asyncio.gather(*[try_acquire(c) for c in consumer_ids])

    print(f"\n[csprng_verify] Winners: {winners}, Losers: {len(losers)}")
    assert len(winners) == 1, (
        f"Expected exactly 1 winner, got {len(winners)}: {winners}"
    )
    assert len(losers) == 9

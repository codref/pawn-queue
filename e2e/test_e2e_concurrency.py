"""
e2e/test_e2e_concurrency.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
E2E tests that verify each message is consumed exactly once under
concurrent consumer load.

Parametrized across both concurrency strategies:
- conditional_write: skipped automatically if the backend does not support it
- csprng_verify:     always runs (the expected path on Hetzner)
"""

from __future__ import annotations

import asyncio

import pytest

from pawn_queue import PawnQueue
from pawn_queue.consumer import Consumer
from pawn_queue.lease import LeaseManager


async def _run_consumers_until_empty(
    consumers: list[Consumer],
    expected_count: int,
    max_rounds: int = 60,
    round_sleep: float = 0.3,
) -> tuple[list[str], list[str]]:
    """
    Repeatedly poll all consumers until *expected_count* messages are received
    or *max_rounds* is reached.

    Returns (all_received_ids, duplicate_ids).
    """
    all_ids: list[str] = []
    seen: set[str] = set()
    duplicates: list[str] = []

    for _round in range(max_rounds):
        tasks = [c.poll() for c in consumers]
        results = await asyncio.gather(*tasks)
        any_received = False
        for msgs in results:
            for msg in msgs:
                if msg.id in seen:
                    duplicates.append(msg.id)
                else:
                    seen.add(msg.id)
                    all_ids.append(msg.id)
                await msg.ack()
                any_received = True

        if len(all_ids) >= expected_count:
            break
        if not any_received:
            await asyncio.sleep(round_sleep)

    return all_ids, duplicates


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["csprng_verify", "conditional_write"])
async def test_no_duplicate_delivery_3_consumers(
    pawnqueue: PawnQueue,
    topic,
    conditional_write_supported: bool,
    strategy: str,
):
    if strategy == "conditional_write" and not conditional_write_supported:
        pytest.skip("Backend does not support conditional writes — skipping conditional_write variant.")

    import copy
    cfg = copy.deepcopy(pawnqueue._config)
    cfg.set_resolved_strategy(strategy)
    # Reduce jitter for speed in tests, but keep jitter_min high enough
    # that in-flight PUT requests on a remote S3 backend (RTT ~50 ms) have
    # time to arrive at S3 before we start verifying ETags.
    cfg.concurrency.csprng_verify.jitter_min_ms = 150
    cfg.concurrency.csprng_verify.jitter_max_ms = 400

    producer = await pawnqueue.register_producer(f"prod-conc-{strategy[:4]}-{topic[-8:]}")
    n_messages = 20
    for i in range(n_messages):
        await producer.publish(topic, {"i": i, "strategy": strategy})

    consumers = []
    for idx in range(3):
        entry_name = f"conc-{strategy[:4]}-{topic[-8:]}-{idx}"
        from pawn_queue.registry import RegistryEntry
        from pawn_queue.utils import new_uuid, utcnow_iso
        entry = RegistryEntry(
            role="consumers",
            name=entry_name,
            entity_id=new_uuid(),
            registered_at=utcnow_iso(),
            last_seen=utcnow_iso(),
        )
        lm = LeaseManager(pawnqueue._client, cfg)
        c = Consumer(
            entry=entry,
            topics=[topic],
            client=pawnqueue._client,
            topic_manager=pawnqueue._topics,
            lease_manager=lm,
            config=cfg,
        )
        consumers.append(c)

    received_ids, duplicates = await _run_consumers_until_empty(consumers, n_messages)

    print(
        f"\n[{strategy}] received={len(received_ids)} "
        f"expected={n_messages} duplicates={len(duplicates)}"
    )

    assert len(received_ids) == n_messages, (
        f"Expected {n_messages} unique messages, got {len(received_ids)}. "
        f"Possible stale leases or missed messages."
    )
    assert duplicates == [], (
        f"Duplicate deliveries detected: {duplicates}"
    )

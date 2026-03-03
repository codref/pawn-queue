"""
e2e/test_e2e_registry.py
~~~~~~~~~~~~~~~~~~~~~~~~~
E2E tests for producer/consumer registration in real S3.
"""

from __future__ import annotations

import pytest

from pawn_queue.utils import new_uuid


@pytest.mark.asyncio
async def test_register_3_producers_3_consumers(pawnqueue, run_prefix):
    prefix = f"{run_prefix}-reg3-{new_uuid()[:8]}"

    prod_names = [f"{prefix}-prod-{i}" for i in range(3)]
    cons_names = [f"{prefix}-cons-{i}" for i in range(3)]

    prods = [await pawnqueue.register_producer(n) for n in prod_names]
    consums = [await pawnqueue.register_consumer(n, topics=[]) for n in cons_names]

    # Verify IDs are unique across all 6
    all_ids = [p.id for p in prods] + [c.id for c in consums]
    assert len(all_ids) == len(set(all_ids)), "Duplicate IDs across producers and consumers!"


@pytest.mark.asyncio
async def test_registered_entities_appear_in_list(pawnqueue, run_prefix):
    prefix = f"{run_prefix}-reglist-{new_uuid()[:8]}"
    pname = f"{prefix}-prod"
    cname = f"{prefix}-cons"

    await pawnqueue.register_producer(pname)
    await pawnqueue.register_consumer(cname, topics=[])

    all_entries = await pawnqueue.registry.list()
    names = {e.name for e in all_entries}
    assert pname in names
    assert cname in names


@pytest.mark.asyncio
async def test_idempotent_reregistration(pawnqueue, run_prefix):
    """
    Registering the same name twice must return the same ID.
    Clears the in-process cache between registrations to force an S3 scan.
    """
    name = f"{run_prefix}-idem-{new_uuid()[:8]}"
    prod1 = await pawnqueue.register_producer(name)

    # Clear cache to force S3 lookup on second call
    pawnqueue._registry._cache.clear()
    prod2 = await pawnqueue.register_producer(name)

    assert prod1.id == prod2.id


@pytest.mark.asyncio
async def test_ids_are_valid_uuids(pawnqueue, run_prefix):
    import uuid
    prefix = f"{run_prefix}-uuid-{new_uuid()[:8]}"
    prod = await pawnqueue.register_producer(f"{prefix}-prod")
    cons = await pawnqueue.register_consumer(f"{prefix}-cons", topics=[])

    # Should not raise
    uuid.UUID(prod.id)
    uuid.UUID(cons.id)

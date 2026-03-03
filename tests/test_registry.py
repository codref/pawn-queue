"""
tests/test_registry.py
~~~~~~~~~~~~~~~~~~~~~~~
Unit tests for producer/consumer registration.
"""

from __future__ import annotations

import pytest

from pawn_queue.exceptions import RegistrationError
from pawn_queue.registry import Registry


@pytest.mark.asyncio
async def test_register_producer(registry):
    entry = await registry.register("producers", "order-service")
    assert entry.entity_id
    assert entry.name == "order-service"
    assert entry.role == "producers"
    assert entry.registered_at
    assert entry.last_seen


@pytest.mark.asyncio
async def test_register_consumer(registry):
    entry = await registry.register("consumers", "billing-svc")
    assert entry.entity_id
    assert entry.name == "billing-svc"
    assert entry.role == "consumers"


@pytest.mark.asyncio
async def test_idempotent_registration(registry):
    """Re-registering with the same name returns the same ID."""
    entry1 = await registry.register("producers", "my-service")
    # Clear in-memory cache to force S3 scan
    registry._cache.clear()
    entry2 = await registry.register("producers", "my-service")
    assert entry1.entity_id == entry2.entity_id


@pytest.mark.asyncio
async def test_different_names_get_different_ids(registry):
    a = await registry.register("producers", "service-a")
    b = await registry.register("producers", "service-b")
    assert a.entity_id != b.entity_id


@pytest.mark.asyncio
async def test_producer_and_consumer_same_name_different_ids(registry):
    prod = await registry.register("producers", "worker")
    cons = await registry.register("consumers", "worker")
    assert prod.entity_id != cons.entity_id


@pytest.mark.asyncio
async def test_list_all(registry):
    await registry.register("producers", "p1")
    await registry.register("producers", "p2")
    await registry.register("consumers", "c1")

    entries = await registry.list()
    names = {e.name for e in entries}
    assert {"p1", "p2", "c1"} <= names


@pytest.mark.asyncio
async def test_list_filter_by_role(registry):
    await registry.register("producers", "px")
    await registry.register("consumers", "cx")

    producers = await registry.list("producers")
    assert all(e.role == "producers" for e in producers)

    consumers = await registry.list("consumers")
    assert all(e.role == "consumers" for e in consumers)


@pytest.mark.asyncio
async def test_ids_are_unique_across_many_registrations(registry):
    entries = [await registry.register("producers", f"svc-{i}") for i in range(20)]
    ids = [e.entity_id for e in entries]
    assert len(ids) == len(set(ids)), "Duplicate IDs detected!"

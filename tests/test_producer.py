"""
tests/test_producer.py
~~~~~~~~~~~~~~~~~~~~~~~
Unit tests for the Producer class.
"""

from __future__ import annotations

import pytest

from pawn_queue.exceptions import TopicNotFoundError
from pawn_queue.producer import Producer
from pawn_queue.registry import RegistryEntry
from pawn_queue.utils import utcnow_iso


def make_entry(name: str = "order-svc", role: str = "producers") -> RegistryEntry:
    return RegistryEntry(
        role=role,
        name=name,
        entity_id="prod-uuid-1111",
        registered_at=utcnow_iso(),
        last_seen=utcnow_iso(),
    )


@pytest.mark.asyncio
async def test_publish_creates_s3_object(s3_client, topic_manager):
    await topic_manager.create("orders")
    producer = Producer(make_entry(), s3_client, topic_manager)
    msg_id = await producer.publish("orders", {"order_id": 42})

    # Verify the message exists in S3
    keys = await s3_client.list_objects("orders/messages/")
    assert len(keys) == 1
    assert msg_id in keys[0]


@pytest.mark.asyncio
async def test_publish_message_content(s3_client, topic_manager):
    await topic_manager.create("orders")
    producer = Producer(make_entry(), s3_client, topic_manager)
    await producer.publish("orders", {"order_id": 99, "item": "widget"})

    keys = await s3_client.list_objects("orders/messages/")
    data = await s3_client.get_object_json(keys[0])

    assert data["payload"] == {"order_id": 99, "item": "widget"}
    assert data["producer_id"] == "prod-uuid-1111"
    assert data["topic"] == "orders"
    assert "id" in data
    assert "created_at" in data


@pytest.mark.asyncio
async def test_publish_to_nonexistent_topic_raises(s3_client, topic_manager):
    producer = Producer(make_entry(), s3_client, topic_manager)
    with pytest.raises(TopicNotFoundError):
        await producer.publish("ghost-topic", {"x": 1})


@pytest.mark.asyncio
async def test_publish_returns_unique_ids(s3_client, topic_manager):
    await topic_manager.create("orders")
    producer = Producer(make_entry(), s3_client, topic_manager)
    ids = [await producer.publish("orders", {"i": i}) for i in range(10)]
    assert len(ids) == len(set(ids)), "Duplicate message IDs!"


@pytest.mark.asyncio
async def test_publish_multiple_messages_fifo_order(s3_client, topic_manager):
    """Keys should be lexicographically sorted (timestamp prefix ensures FIFO)."""
    await topic_manager.create("events")
    producer = Producer(make_entry(), s3_client, topic_manager)
    ids = []
    for i in range(5):
        mid = await producer.publish("events", {"seq": i})
        ids.append(mid)

    keys = await s3_client.list_objects("events/messages/")
    assert len(keys) == 5
    # Sorted keys should match publish order (timestamp prefix guarantees FIFO)
    assert keys == sorted(keys)

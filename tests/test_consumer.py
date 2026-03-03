"""
tests/test_consumer.py
~~~~~~~~~~~~~~~~~~~~~~~
Unit tests for the Consumer class.
"""

from __future__ import annotations

import asyncio

import pytest

from pawn_queue.consumer import Consumer
from pawn_queue.producer import Producer
from pawn_queue.registry import RegistryEntry
from pawn_queue.utils import utcnow_iso


def make_entry(name: str, role: str = "consumers") -> RegistryEntry:
    from pawn_queue.utils import new_uuid
    return RegistryEntry(
        role=role,
        name=name,
        entity_id=new_uuid(),
        registered_at=utcnow_iso(),
        last_seen=utcnow_iso(),
    )


def make_producer_entry(name: str = "prod") -> RegistryEntry:
    from pawn_queue.utils import new_uuid
    return RegistryEntry(
        role="producers",
        name=name,
        entity_id=new_uuid(),
        registered_at=utcnow_iso(),
        last_seen=utcnow_iso(),
    )


@pytest.mark.asyncio
async def test_poll_returns_empty_on_empty_topic(
    s3_client, topic_manager, lease_manager, config
):
    await topic_manager.create("orders")
    consumer = Consumer(
        entry=make_entry("billing"),
        topics=["orders"],
        client=s3_client,
        topic_manager=topic_manager,
        lease_manager=lease_manager,
        config=config,
    )
    messages = await consumer.poll()
    assert messages == []


@pytest.mark.asyncio
async def test_poll_returns_message(
    s3_client, topic_manager, lease_manager, config
):
    await topic_manager.create("orders")
    producer = Producer(make_producer_entry(), s3_client, topic_manager)
    msg_id = await producer.publish("orders", {"x": 1})

    consumer = Consumer(
        entry=make_entry("billing"),
        topics=["orders"],
        client=s3_client,
        topic_manager=topic_manager,
        lease_manager=lease_manager,
        config=config,
    )
    messages = await consumer.poll()
    assert len(messages) == 1
    assert messages[0].id == msg_id
    assert messages[0].payload == {"x": 1}


@pytest.mark.asyncio
async def test_ack_removes_message(
    s3_client, topic_manager, lease_manager, config
):
    await topic_manager.create("orders")
    producer = Producer(make_producer_entry(), s3_client, topic_manager)
    await producer.publish("orders", {"order": 1})

    consumer = Consumer(
        entry=make_entry("billing"),
        topics=["orders"],
        client=s3_client,
        topic_manager=topic_manager,
        lease_manager=lease_manager,
        config=config,
    )
    messages = await consumer.poll()
    assert len(messages) == 1
    await messages[0].ack()

    # Message and lease should be gone
    msg_keys = await s3_client.list_objects("orders/messages/")
    lease_keys = await s3_client.list_objects("orders/leases/")
    assert msg_keys == []
    assert lease_keys == []


@pytest.mark.asyncio
async def test_nack_moves_to_dead_letter(
    s3_client, topic_manager, lease_manager, config
):
    await topic_manager.create("orders")
    producer = Producer(make_producer_entry(), s3_client, topic_manager)
    await producer.publish("orders", {"order": 2})

    consumer = Consumer(
        entry=make_entry("billing"),
        topics=["orders"],
        client=s3_client,
        topic_manager=topic_manager,
        lease_manager=lease_manager,
        config=config,
    )
    messages = await consumer.poll()
    assert len(messages) == 1
    await messages[0].nack()

    msg_keys = await s3_client.list_objects("orders/messages/")
    dl_keys = await s3_client.list_objects("orders/dead-letter/")
    lease_keys = await s3_client.list_objects("orders/leases/")
    assert msg_keys == []
    assert len(dl_keys) == 1
    assert lease_keys == []


@pytest.mark.asyncio
async def test_same_message_not_polled_twice_by_same_consumer(
    s3_client, topic_manager, lease_manager, config
):
    """Once a message is claimed, a second poll should not return it again."""
    await topic_manager.create("orders")
    producer = Producer(make_producer_entry(), s3_client, topic_manager)
    await producer.publish("orders", {"order": 3})

    consumer = Consumer(
        entry=make_entry("billing"),
        topics=["orders"],
        client=s3_client,
        topic_manager=topic_manager,
        lease_manager=lease_manager,
        config=config,
    )
    first_poll = await consumer.poll()
    assert len(first_poll) == 1

    second_poll = await consumer.poll()
    assert len(second_poll) == 0


@pytest.mark.asyncio
async def test_poll_respects_max_messages_per_poll(
    s3_client, topic_manager, lease_manager, config
):
    await topic_manager.create("orders")
    producer = Producer(make_producer_entry(), s3_client, topic_manager)
    for i in range(15):
        await producer.publish("orders", {"i": i})

    consumer = Consumer(
        entry=make_entry("billing"),
        topics=["orders"],
        client=s3_client,
        topic_manager=topic_manager,
        lease_manager=lease_manager,
        config=config,
    )
    messages = await consumer.poll()
    # max_messages_per_poll is 10 in BASE_CONFIG
    assert len(messages) <= 10


@pytest.mark.asyncio
async def test_listen_delivers_messages_via_callback(
    s3_client, topic_manager, lease_manager, config
):
    await topic_manager.create("events")
    producer = Producer(make_producer_entry(), s3_client, topic_manager)
    for i in range(3):
        await producer.publish("events", {"seq": i})

    consumer = Consumer(
        entry=make_entry("worker"),
        topics=["events"],
        client=s3_client,
        topic_manager=topic_manager,
        lease_manager=lease_manager,
        config=config,
    )

    received = []

    async def handler(msg):
        received.append(msg.payload)
        await msg.ack()

    # Run listen with a 1-second timeout then cancel
    listen_task = asyncio.create_task(consumer.listen(handler))
    await asyncio.sleep(0.5)
    listen_task.cancel()
    try:
        await listen_task
    except asyncio.CancelledError:
        pass

    assert len(received) == 3
    seqs = sorted(r["seq"] for r in received)
    assert seqs == [0, 1, 2]


@pytest.mark.asyncio
async def test_two_consumers_each_get_unique_messages(
    s3_client, topic_manager, lease_manager, config
):
    """
    Two concurrent consumers must not both receive the same message.
    Publish 6 messages; run two consumers concurrently; assert total == 6
    with no duplicates.
    """
    await topic_manager.create("events")
    producer = Producer(make_producer_entry(), s3_client, topic_manager)
    for i in range(6):
        await producer.publish("events", {"i": i})

    from pawn_queue.lease import LeaseManager

    # Each consumer gets its own lease manager instance (shared S3)
    lm1 = LeaseManager(s3_client, config)
    lm2 = LeaseManager(s3_client, config)

    c1 = Consumer(
        entry=make_entry("worker-1"),
        topics=["events"],
        client=s3_client,
        topic_manager=topic_manager,
        lease_manager=lm1,
        config=config,
    )
    c2 = Consumer(
        entry=make_entry("worker-2"),
        topics=["events"],
        client=s3_client,
        topic_manager=topic_manager,
        lease_manager=lm2,
        config=config,
    )

    # Alternate polls until all messages are consumed (with a safety limit)
    all_ids = []
    for _ in range(10):
        msgs1 = await c1.poll()
        msgs2 = await c2.poll()
        for m in msgs1 + msgs2:
            all_ids.append(m.id)
            await m.ack()
        if len(all_ids) >= 6:
            break

    assert len(all_ids) == 6
    assert len(set(all_ids)) == 6, "Duplicate message delivery detected!"

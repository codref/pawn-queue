"""
e2e/test_e2e_produce_consume.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
E2E tests for the publish → poll/listen → ack/nack lifecycle.
"""

from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_producer_registered_in_s3(pawnqueue, run_prefix):
    name = f"{run_prefix}-reg-prod"
    producer = await pawnqueue.register_producer(name)
    assert producer.id

    entries = await pawnqueue.registry.list("producers")
    names = {e.name for e in entries}
    assert name in names


@pytest.mark.asyncio
async def test_publish_10_messages(pawnqueue, topic):
    producer = await pawnqueue.register_producer(f"prod-{topic[-8:]}")
    ids = []
    for i in range(10):
        mid = await producer.publish(topic, {"index": i})
        ids.append(mid)

    assert len(ids) == 10
    assert len(set(ids)) == 10  # all unique


@pytest.mark.asyncio
async def test_consumer_receives_all_published_messages(pawnqueue, topic):
    producer = await pawnqueue.register_producer(f"prod-pub-{topic[-8:]}")
    published = {}
    for i in range(10):
        mid = await producer.publish(topic, {"seq": i})
        published[mid] = i

    consumer = await pawnqueue.register_consumer(
        f"cons-pub-{topic[-8:]}", topics=[topic]
    )

    received = {}
    safety = 0
    while len(received) < 10 and safety < 20:
        msgs = await consumer.poll()
        for msg in msgs:
            received[msg.id] = msg.payload
            await msg.ack()
        if not msgs:
            await asyncio.sleep(0.5)
        safety += 1

    assert len(received) == 10
    for mid, seq_val in published.items():
        assert mid in received
        assert received[mid]["seq"] == seq_val


@pytest.mark.asyncio
async def test_ack_removes_all_objects(pawnqueue, topic):
    producer = await pawnqueue.register_producer(f"prod-ack-{topic[-8:]}")
    for i in range(3):
        await producer.publish(topic, {"i": i})

    consumer = await pawnqueue.register_consumer(
        f"cons-ack-{topic[-8:]}", topics=[topic]
    )
    msgs = await consumer.poll()
    assert len(msgs) == 3
    for msg in msgs:
        await msg.ack()

    msg_keys = await pawnqueue._client.list_objects(f"{topic}/messages/")
    lease_keys = await pawnqueue._client.list_objects(f"{topic}/leases/")
    assert msg_keys == []
    assert lease_keys == []


@pytest.mark.asyncio
async def test_nack_moves_to_dead_letter(pawnqueue, topic):
    producer = await pawnqueue.register_producer(f"prod-nack-{topic[-8:]}")
    await producer.publish(topic, {"should": "fail"})

    consumer = await pawnqueue.register_consumer(
        f"cons-nack-{topic[-8:]}", topics=[topic]
    )
    msgs = await consumer.poll()
    assert len(msgs) == 1
    await msgs[0].nack()

    msg_keys = await pawnqueue._client.list_objects(f"{topic}/messages/")
    dl_keys = await pawnqueue._client.list_objects(f"{topic}/dead-letter/")
    assert msg_keys == []
    assert len(dl_keys) == 1


@pytest.mark.asyncio
async def test_listen_mode_delivers_and_stops(pawnqueue, topic):
    producer = await pawnqueue.register_producer(f"prod-listen-{topic[-8:]}")
    for i in range(5):
        await producer.publish(topic, {"seq": i})

    consumer = await pawnqueue.register_consumer(
        f"cons-listen-{topic[-8:]}", topics=[topic]
    )

    received = []

    async def handler(msg):
        received.append(msg.payload["seq"])
        await msg.ack()

    listen_task = asyncio.create_task(consumer.listen(handler))
    # Wait up to 15 seconds for all 5 messages
    for _ in range(30):
        if len(received) >= 5:
            break
        await asyncio.sleep(0.5)

    listen_task.cancel()
    try:
        await listen_task
    except asyncio.CancelledError:
        pass

    assert sorted(received) == [0, 1, 2, 3, 4]

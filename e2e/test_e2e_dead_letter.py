"""
e2e/test_e2e_dead_letter.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
E2E tests for the dead-letter queue behaviour.
"""

from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_nacked_messages_appear_in_dead_letter(pawnqueue, topic):
    producer = await pawnqueue.register_producer(f"prod-dl-{topic[-8:]}")
    payloads = [{"fail": i} for i in range(3)]
    for p in payloads:
        await producer.publish(topic, p)

    consumer = await pawnqueue.register_consumer(
        f"cons-dl-{topic[-8:]}", topics=[topic]
    )
    msgs = await consumer.poll()
    assert len(msgs) == 3

    for msg in msgs:
        await msg.nack()

    msg_keys = await pawnqueue._client.list_objects(f"{topic}/messages/")
    dl_keys = await pawnqueue._client.list_objects(f"{topic}/dead-letter/")
    lease_keys = await pawnqueue._client.list_objects(f"{topic}/leases/")

    assert msg_keys == [], "Original messages should be deleted after nack"
    assert len(dl_keys) == 3, f"Expected 3 dead-letter objects, got {len(dl_keys)}"
    assert lease_keys == [], "Leases should be deleted after nack"


@pytest.mark.asyncio
async def test_dead_letter_payload_matches_original(pawnqueue, topic):
    producer = await pawnqueue.register_producer(f"prod-dlp-{topic[-8:]}")
    original_payload = {"error_test": True, "value": 42}
    await producer.publish(topic, original_payload)

    consumer = await pawnqueue.register_consumer(
        f"cons-dlp-{topic[-8:]}", topics=[topic]
    )
    msgs = await consumer.poll()
    assert len(msgs) == 1
    await msgs[0].nack()

    dl_keys = await pawnqueue._client.list_objects(f"{topic}/dead-letter/")
    assert len(dl_keys) == 1

    dl_data = await pawnqueue._client.get_object_json(dl_keys[0])
    assert dl_data["payload"] == original_payload


@pytest.mark.asyncio
async def test_acked_messages_do_not_appear_in_dead_letter(pawnqueue, topic):
    producer = await pawnqueue.register_producer(f"prod-dlack-{topic[-8:]}")
    await producer.publish(topic, {"should": "ack"})

    consumer = await pawnqueue.register_consumer(
        f"cons-dlack-{topic[-8:]}", topics=[topic]
    )
    msgs = await consumer.poll()
    assert len(msgs) == 1
    await msgs[0].ack()

    dl_keys = await pawnqueue._client.list_objects(f"{topic}/dead-letter/")
    assert dl_keys == [], "Acked messages must not appear in dead-letter"


@pytest.mark.asyncio
async def test_mix_ack_and_nack(pawnqueue, topic):
    """3 messages: 1 acked, 2 nacked → exactly 2 in dead-letter."""
    producer = await pawnqueue.register_producer(f"prod-mix-{topic[-8:]}")
    for i in range(3):
        await producer.publish(topic, {"i": i})

    consumer = await pawnqueue.register_consumer(
        f"cons-mix-{topic[-8:]}", topics=[topic]
    )
    msgs = await consumer.poll()
    assert len(msgs) == 3

    await msgs[0].ack()
    await msgs[1].nack()
    await msgs[2].nack()

    dl_keys = await pawnqueue._client.list_objects(f"{topic}/dead-letter/")
    msg_keys = await pawnqueue._client.list_objects(f"{topic}/messages/")

    assert len(dl_keys) == 2
    assert msg_keys == []

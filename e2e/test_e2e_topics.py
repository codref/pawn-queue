"""
e2e/test_e2e_topics.py
~~~~~~~~~~~~~~~~~~~~~~~
E2E tests for topic creation and listing against real S3.
"""

from __future__ import annotations

import pytest

from pawn_queue.exceptions import TopicNotFoundError


@pytest.mark.asyncio
async def test_create_topic(pawnqueue, topic):
    """topic fixture already creates the topic; verify it appears in list."""
    topics = await pawnqueue.list_topics()
    assert topic in topics


@pytest.mark.asyncio
async def test_create_topic_idempotent(pawnqueue, topic):
    """Creating the same topic twice must not raise."""
    await pawnqueue.create_topic(topic, exist_ok=True)
    await pawnqueue.create_topic(topic, exist_ok=True)
    topics = await pawnqueue.list_topics()
    assert topics.count(topic) == 1


@pytest.mark.asyncio
async def test_list_topics_contains_created(pawnqueue, run_prefix):
    """Create two distinct topics and verify both appear in list_topics()."""
    from pawn_queue.utils import new_uuid
    t1 = f"{run_prefix}-lt-{new_uuid()[:8]}"
    t2 = f"{run_prefix}-lt-{new_uuid()[:8]}"
    await pawnqueue.create_topic(t1)
    await pawnqueue.create_topic(t2)

    topics = await pawnqueue.list_topics()
    assert t1 in topics
    assert t2 in topics


@pytest.mark.asyncio
async def test_publish_to_nonexistent_topic_raises(pawnqueue, run_prefix):
    """Publishing to an unregistered topic must raise TopicNotFoundError."""
    producer = await pawnqueue.register_producer(f"{run_prefix}-ghost-prod")
    ghost_topic = f"{run_prefix}-ghost-{__import__('uuid').uuid4()}"

    with pytest.raises(TopicNotFoundError):
        await producer.publish(ghost_topic, {"data": "should_fail"})

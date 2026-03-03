"""
pawn_queue.topic
~~~~~~~~~~~~~~~~
Topic (folder) management.

A topic is "registered" by writing a zero-byte marker object at:
  {topic}/.topic

Listing topics = listing all .topic markers.
"""

from __future__ import annotations

import logging

from .client import S3Client
from .exceptions import TopicNotFoundError
from .utils import canonical_json, topic_marker_key, utcnow_iso

logger = logging.getLogger(__name__)


class TopicManager:
    """Manages topic creation and listing."""

    def __init__(self, client: S3Client) -> None:
        self._client = client
        self._known: set[str] = set()  # in-process cache

    async def create(self, topic: str, *, exist_ok: bool = True) -> None:
        """
        Create a topic by writing a marker object.

        If *exist_ok* is True (default), silently returns if the topic already
        exists.  If False, raises TopicAlreadyExistsError.
        """
        marker = topic_marker_key(topic)
        already = await self._client.object_exists(marker)
        if already:
            if not exist_ok:
                from .exceptions import TopicAlreadyExistsError
                raise TopicAlreadyExistsError(topic)
            self._known.add(topic)
            return

        meta = {"topic": topic, "created_at": utcnow_iso()}
        await self._client.put_object(marker, canonical_json(meta))
        self._known.add(topic)
        logger.info("Created topic %r", topic)

    async def list(self) -> list[str]:
        """Return a sorted list of all registered topic names."""
        keys = await self._client.list_objects("_")
        # Also grab top-level prefixes that have .topic markers
        # We list every key that ends with /.topic
        all_keys = await self._client.list_objects("")
        topics: list[str] = []
        for key in all_keys:
            if key.endswith("/.topic"):
                topic = key[: -len("/.topic")]
                topics.append(topic)
                self._known.add(topic)
        return sorted(topics)

    async def exists(self, topic: str) -> bool:
        """Return True if the topic has been registered."""
        if topic in self._known:
            return True
        exists = await self._client.object_exists(topic_marker_key(topic))
        if exists:
            self._known.add(topic)
        return exists

    async def assert_exists(self, topic: str) -> None:
        """Raise TopicNotFoundError if *topic* has not been registered."""
        if not await self.exists(topic):
            raise TopicNotFoundError(topic)

"""
pawn_queue.producer
~~~~~~~~~~~~~~~~~~~~
Producer: publishes JSON messages to a topic.
"""

from __future__ import annotations

import logging
from typing import Any

from .client import S3Client
from .exceptions import PublishError
from .registry import RegistryEntry
from .topic import TopicManager
from .utils import canonical_json, message_key, new_uuid, sortable_key_prefix, utcnow_iso

logger = logging.getLogger(__name__)


class Producer:
    """
    Publishes messages to a topic queue.

    Obtain an instance via ``PawnQueue.register_producer()``.
    """

    def __init__(
        self,
        entry: RegistryEntry,
        client: S3Client,
        topic_manager: TopicManager,
    ) -> None:
        self._entry = entry
        self._client = client
        self._topic_manager = topic_manager

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def id(self) -> str:
        """Stable UUID assigned to this producer at registration time."""
        return self._entry.entity_id

    @property
    def name(self) -> str:
        return self._entry.name

    async def publish(self, topic: str, payload: dict[str, Any]) -> str:
        """
        Publish *payload* as a JSON message to *topic*.

        Returns the message ID (UUID).

        Raises:
            TopicNotFoundError: if *topic* has not been registered.
            PublishError: if the S3 write fails.
        """
        await self._topic_manager.assert_exists(topic)

        message_id = new_uuid()
        key_prefix = sortable_key_prefix()
        s3_key = message_key(topic, key_prefix, message_id)

        body: dict[str, Any] = {
            "id": message_id,
            "topic": topic,
            "producer_id": self.id,
            "created_at": utcnow_iso(),
            "payload": payload,
        }

        try:
            await self._client.put_object(s3_key, canonical_json(body))
        except Exception as exc:
            raise PublishError(f"Failed to publish message to topic {topic!r}: {exc}") from exc

        logger.debug("Published message %s to topic %r (key=%s)", message_id, topic, s3_key)
        return message_id

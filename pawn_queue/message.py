"""
pawn_queue.message
~~~~~~~~~~~~~~~~~~
Message dataclass and ack/nack coroutines.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .utils import canonical_json, dead_letter_key, lease_key, message_key

if TYPE_CHECKING:
    from .client import S3Client

logger = logging.getLogger(__name__)


@dataclass
class Message:
    """A single message retrieved from a topic queue."""

    # Message identity
    id: str
    topic: str
    producer_id: str
    created_at: str
    payload: dict[str, Any]

    # Internal routing — not part of the public payload
    _s3_key: str = field(repr=False)
    _client: "S3Client" = field(repr=False)
    _acked: bool = field(default=False, repr=False)

    # ------------------------------------------------------------------
    # Public coroutines
    # ------------------------------------------------------------------

    async def ack(self) -> None:
        """
        Acknowledge successful processing.

        Deletes both the message object and its lease from S3 so the
        message is permanently removed from the queue.
        """
        if self._acked:
            logger.debug("Message %s already acked — ignoring duplicate ack.", self.id)
            return
        l_key = lease_key(self.topic, self.id)
        try:
            await self._client.delete_object(self._s3_key)
        except Exception as exc:
            logger.warning("Failed to delete message object %s: %s", self._s3_key, exc)
        try:
            await self._client.delete_object(l_key)
        except Exception as exc:
            logger.warning("Failed to delete lease %s: %s", l_key, exc)
        self._acked = True
        logger.debug("Message %s acked.", self.id)

    async def nack(self) -> None:
        """
        Negatively acknowledge — move the message to the dead-letter prefix.

        Copies the message to ``{topic}/dead-letter/{id}.json``, then removes
        the original message and its lease.
        """
        if self._acked:
            logger.debug("Message %s already acked/nacked — ignoring.", self.id)
            return
        dl_key = dead_letter_key(self.topic, self.id)
        l_key = lease_key(self.topic, self.id)
        try:
            await self._client.copy_object(self._s3_key, dl_key)
        except Exception as exc:
            logger.warning("Failed to copy message to dead-letter %s: %s", dl_key, exc)
        try:
            await self._client.delete_object(self._s3_key)
        except Exception as exc:
            logger.warning("Failed to delete original message %s: %s", self._s3_key, exc)
        try:
            await self._client.delete_object(l_key)
        except Exception as exc:
            logger.warning("Failed to delete lease %s: %s", l_key, exc)
        self._acked = True
        logger.debug("Message %s nacked → dead-letter.", self.id)

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "topic": self.topic,
            "producer_id": self.producer_id,
            "created_at": self.created_at,
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, data: dict, *, s3_key: str, client: "S3Client") -> "Message":
        return cls(
            id=data["id"],
            topic=data["topic"],
            producer_id=data["producer_id"],
            created_at=data["created_at"],
            payload=data["payload"],
            _s3_key=s3_key,
            _client=client,
        )

"""
pawn_queue.consumer
~~~~~~~~~~~~~~~~~~~~
Consumer: polls topics, claims leases, and delivers messages.

Two usage modes:

poll()
    Returns a list of Message objects.  The caller drives the event loop.

listen(callback)
    Runs an internal loop that continuously polls and calls *callback* for
    each message.  Also runs background tasks for lease refresh and janitor
    cleanup.  Blocks until cancelled (asyncio.CancelledError).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Coroutine

from .client import S3Client
from .config import PawnQueueConfig
from .exceptions import LeaseConflictError
from .lease import LeaseContent, LeaseManager
from .message import Message
from .registry import RegistryEntry
from .topic import TopicManager
from .utils import utcnow_iso

logger = logging.getLogger(__name__)

MessageHandler = Callable[[Message], Coroutine[Any, Any, None]]


class Consumer:
    """
    Consumes messages from one or more topic queues.

    Obtain an instance via ``PawnQueue.register_consumer()``.
    """

    def __init__(
        self,
        entry: RegistryEntry,
        topics: list[str],
        client: S3Client,
        topic_manager: TopicManager,
        lease_manager: LeaseManager,
        config: PawnQueueConfig,
    ) -> None:
        self._entry = entry
        self._topics = topics
        self._client = client
        self._topic_manager = topic_manager
        self._lease_manager = lease_manager
        self._config = config
        # message_id → lease (for the refresh background task)
        self._active_leases: dict[str, tuple[str, LeaseContent]] = {}  # msg_id → (topic, lease)

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    def id(self) -> str:
        """Stable UUID assigned at registration time."""
        return self._entry.entity_id

    @property
    def name(self) -> str:
        return self._entry.name

    # ------------------------------------------------------------------
    # Pull mode
    # ------------------------------------------------------------------

    async def poll(self) -> list[Message]:
        """
        Poll all subscribed topics once and return up to
        ``config.polling.max_messages_per_poll`` claimed messages.

        Never raises on individual lease conflicts — those are silently skipped.
        """
        max_msgs = self._config.polling.max_messages_per_poll
        messages: list[Message] = []

        for topic in self._topics:
            if len(messages) >= max_msgs:
                break
            # Janitor pass first to free stale leases
            try:
                reclaimed = await self._lease_manager.janitor_pass(topic)
                if reclaimed:
                    logger.debug("Janitor reclaimed %d lease(s) in topic %r", reclaimed, topic)
            except Exception as exc:
                logger.warning("Janitor pass failed for topic %r: %s", topic, exc)

            # List pending messages (sorted FIFO by key prefix)
            prefix = f"{topic}/messages/"
            try:
                keys = await self._client.list_objects(prefix)
            except Exception as exc:
                logger.warning("Failed to list messages for topic %r: %s", topic, exc)
                continue

            for key in keys:
                if len(messages) >= max_msgs:
                    break
                msg = await self._try_claim(topic, key)
                if msg is not None:
                    messages.append(msg)

        return messages

    # ------------------------------------------------------------------
    # Push mode
    # ------------------------------------------------------------------

    async def listen(self, handler: MessageHandler) -> None:
        """
        Continuously poll all subscribed topics and call *handler* for each
        message.  Runs until the task is cancelled.

        Internal background tasks:
        - _poll_loop: calls poll() on the configured interval
        - _lease_refresher: keeps active leases alive
        - _janitor_loop: periodic janitor pass (handled inside poll)

        Usage::

            async def on_message(msg: Message) -> None:
                print(msg.payload)
                await msg.ack()

            await consumer.listen(on_message)
        """
        await asyncio.gather(
            self._poll_loop(handler),
            self._lease_refresher(),
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _try_claim(self, topic: str, s3_key: str) -> Message | None:
        """
        Attempt to claim a single message.  Returns None on any conflict or error.
        """
        # Extract message_id from key: {topic}/messages/{prefix}-{uuid}.json
        filename = s3_key.split("/")[-1]  # e.g. "20260303T100000_000000-uuid.json"
        if not filename.endswith(".json"):
            return None
        # The message_id is the UUID after the last '-'
        name_no_ext = filename[:-5]  # strip ".json"
        parts = name_no_ext.split("-")
        # UUIDs have 5 groups; the prefix uses one '-'; grab last 5 parts
        if len(parts) < 6:
            return None
        message_id = "-".join(parts[-5:])  # reconstruct UUID

        # Skip if already leased by someone (fast path via direct lease check)
        if await self._lease_manager.is_leased(topic, message_id):
            return None

        # Read message body
        try:
            data = await self._client.get_object_json(s3_key)
        except (KeyError, Exception) as exc:
            logger.debug("Could not read message %s: %s", s3_key, exc)
            return None

        # Try to acquire the lease
        try:
            lease = await self._lease_manager.acquire(topic, message_id, self.id)
        except LeaseConflictError:
            logger.debug("Message %s already claimed by another consumer.", message_id)
            return None
        except Exception as exc:
            logger.warning("Lease acquisition error for message %s: %s", message_id, exc)
            return None

        # Track the lease for the refresher
        self._active_leases[message_id] = (topic, lease)

        msg = Message.from_dict(data, s3_key=s3_key, client=self._client)
        # Wrap ack/nack to clean up _active_leases
        original_ack = msg.ack
        original_nack = msg.nack

        async def _ack_wrapper() -> None:
            await original_ack()
            self._active_leases.pop(message_id, None)

        async def _nack_wrapper() -> None:
            await original_nack()
            self._active_leases.pop(message_id, None)

        # Replace methods (bind to the instance)
        import types
        msg.ack = _ack_wrapper  # type: ignore[method-assign]
        msg.nack = _nack_wrapper  # type: ignore[method-assign]

        return msg

    async def _poll_loop(self, handler: MessageHandler) -> None:
        """Continuously poll and dispatch messages to *handler*."""
        interval = self._config.polling.interval_seconds
        while True:
            try:
                messages = await self.poll()
                for msg in messages:
                    try:
                        await handler(msg)
                    except Exception as exc:
                        logger.error("Handler raised exception for message %s: %s", msg.id, exc)
                        if not msg._acked:
                            try:
                                await msg.nack()
                            except Exception:
                                pass
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Poll loop error: %s", exc)
            await asyncio.sleep(interval)

    async def _lease_refresher(self) -> None:
        """Background task: refresh all active leases before they expire."""
        refresh_interval = self._config.polling.lease_refresh_interval_seconds
        while True:
            try:
                await asyncio.sleep(refresh_interval)
                for message_id, (topic, lease) in list(self._active_leases.items()):
                    try:
                        refreshed = await self._lease_manager.refresh(topic, lease)
                        self._active_leases[message_id] = (topic, refreshed)
                    except Exception as exc:
                        logger.warning("Failed to refresh lease for message %s: %s", message_id, exc)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Lease refresher error: %s", exc)

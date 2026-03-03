"""
pawn_queue.core
~~~~~~~~~~~~~~~
PawnQueue — the main entry point for the library.

Wires together S3Client, Registry, TopicManager, LeaseManager,
Producer, and Consumer.  Handles startup (compat probe) and teardown.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

from .client import S3Client
from .config import PawnQueueConfig
from .consumer import Consumer
from .lease import LeaseManager
from .producer import Producer
from .registry import Registry
from .topic import TopicManager
from .utils import new_uuid

logger = logging.getLogger(__name__)


class PawnQueue:
    """
    Main library façade.

    Typical usage::

        pawnqueue = await PawnQueue.from_config("config.yaml")
        await pawnqueue.setup()

        await pawnqueue.create_topic("orders")

        producer = await pawnqueue.register_producer("order-service")
        await producer.publish("orders", {"order_id": 42})

        consumer = await pawnqueue.register_consumer("billing-svc", topics=["orders"])
        messages = await consumer.poll()
        for msg in messages:
            await msg.ack()

        await pawnqueue.teardown()
    """

    def __init__(self, config: PawnQueueConfig) -> None:
        self._config = config
        self._client = S3Client(config)
        self._registry = Registry(self._client)
        self._topics = TopicManager(self._client)
        self._lease_manager = LeaseManager(self._client, config)
        self._started = False

    # ------------------------------------------------------------------
    # Factories
    # ------------------------------------------------------------------

    @classmethod
    async def from_config(cls, config: str | Path | PawnQueueConfig | dict) -> "PawnQueue":
        """
        Create a PawnQueue instance from a config source.

        Accepts:
        - file path (str or Path) to a YAML config file
        - a PawnQueueConfig object
        - a plain dict
        """
        if isinstance(config, (str, Path)):
            cfg = PawnQueueConfig.from_yaml(config)
        elif isinstance(config, dict):
            cfg = PawnQueueConfig.from_dict(config)
        elif isinstance(config, PawnQueueConfig):
            cfg = config
        else:
            raise TypeError(f"Unsupported config type: {type(config)}")
        return cls(cfg)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        """
        Open the S3 client connection, verify bucket access, and run the
        compatibility probe if the strategy is ``auto``.

        This method is idempotent.
        """
        if self._started:
            return
        await self._client.start()
        await self._client.ensure_bucket()

        if self._config.concurrency.strategy == "auto":
            await self._run_compat_probe()

        self._started = True
        logger.info(
            "PawnQueue ready — bucket=%r strategy=%s",
            self._config.s3.bucket_name,
            self._config.effective_strategy(),
        )

    async def teardown(self) -> None:
        """Close the S3 client gracefully."""
        if not self._started:
            return
        await self._client.stop()
        self._started = False

    # Context-manager support
    async def __aenter__(self) -> "PawnQueue":
        await self.setup()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.teardown()

    # ------------------------------------------------------------------
    # Topics
    # ------------------------------------------------------------------

    async def create_topic(self, topic: str, *, exist_ok: bool = True) -> None:
        """Register a new topic (S3 folder marker)."""
        self._assert_started()
        await self._topics.create(topic, exist_ok=exist_ok)

    async def list_topics(self) -> list[str]:
        """Return all registered topics."""
        self._assert_started()
        return await self._topics.list()

    # ------------------------------------------------------------------
    # Producers
    # ------------------------------------------------------------------

    async def register_producer(self, name: str) -> Producer:
        """
        Register a producer with the given *name*.

        Idempotent: same name always returns the same stable ID.
        """
        self._assert_started()
        entry = await self._registry.register("producers", name)
        return Producer(entry, self._client, self._topics)

    # ------------------------------------------------------------------
    # Consumers
    # ------------------------------------------------------------------

    async def register_consumer(self, name: str, *, topics: list[str]) -> Consumer:
        """
        Register a consumer with the given *name*, subscribed to *topics*.

        Idempotent: same name always returns the same stable ID.
        """
        self._assert_started()
        entry = await self._registry.register("consumers", name)
        return Consumer(
            entry=entry,
            topics=topics,
            client=self._client,
            topic_manager=self._topics,
            lease_manager=self._lease_manager,
            config=self._config,
        )

    # ------------------------------------------------------------------
    # Registry access
    # ------------------------------------------------------------------

    @property
    def registry(self) -> Registry:
        """Direct access to the Registry (for listing / heartbeat)."""
        return self._registry

    # ------------------------------------------------------------------
    # Compatibility probe
    # ------------------------------------------------------------------

    async def _run_compat_probe(self) -> None:
        probe_key = f"_probe/{new_uuid()}.probe"
        logger.debug("Running conditional-write compatibility probe (key=%s)", probe_key)
        supported = await self._client.probe_conditional_write(probe_key)
        strategy: Literal["conditional_write", "csprng_verify"] = (
            "conditional_write" if supported else "csprng_verify"
        )
        self._config.set_resolved_strategy(strategy)
        if supported:
            logger.info("Conditional writes supported — using conditional_write strategy.")
        else:
            logger.info(
                "Conditional writes NOT supported by this S3 backend "
                "(expected for Hetzner/Ceph) — using csprng_verify strategy."
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _assert_started(self) -> None:
        if not self._started:
            raise RuntimeError("PawnQueue has not been started. Call await pawnqueue.setup() first.")

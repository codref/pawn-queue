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
from .config import (
    ConcurrencyConfig,
    CsprngVerifyConfig,
    PawnQueueConfig,
    PollingConfig,
    RegistryConfig,
    S3Config,
)
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


# =============================================================================
# PawnQueueBuilder — Fluent API for programmatic configuration
# =============================================================================


class PawnQueueBuilder:
    """
    Fluent builder for constructing a PawnQueue instance with programmatic
    configuration, environment variable overlays, or a mix of both.

    Example usage::

        # Programmatic + environment fallback
        pawnqueue = await (PawnQueueBuilder()
            .s3(endpoint_url="http://localhost:9000", bucket_name="pawnqueue")
            .from_env()
            .build())

        # Pure builder
        pawnqueue = await (PawnQueueBuilder()
            .s3(
                endpoint_url="http://localhost:9000",
                bucket_name="pawnqueue",
                access_key="test",
                secret_key="test"
            )
            .polling(interval_seconds=2)
            .concurrency(strategy="conditional_write")
            .build())

        # Environment-only
        pawnqueue = await PawnQueueBuilder().from_env().build()

    All builder methods return ``self`` to enable chaining. Call ``.build()``
    to construct the PawnQueue instance.
    """

    def __init__(self) -> None:
        self._s3: dict = {}
        self._polling: dict = {}
        self._concurrency: dict = {}
        self._csprng_verify: dict = {}
        self._registry: dict = {}

    def s3(
        self,
        *,
        endpoint_url: str | None = None,
        bucket_name: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        region_name: str | None = None,
        use_ssl: bool | None = None,
    ) -> "PawnQueueBuilder":
        """
        Configure S3 connection parameters.

        Args:
            endpoint_url: S3 endpoint URL (e.g., "https://fsn1.your-objectstorage.com")
            bucket_name: S3 bucket name
            access_key: AWS access key ID (maps to aws_access_key_id)
            secret_key: AWS secret access key (maps to aws_secret_access_key)
            region_name: AWS region (default: "us-east-1")
            use_ssl: Whether to use SSL (default: True)

        Returns:
            self for method chaining
        """
        if endpoint_url is not None:
            self._s3["endpoint_url"] = endpoint_url
        if bucket_name is not None:
            self._s3["bucket_name"] = bucket_name
        if access_key is not None:
            self._s3["aws_access_key_id"] = access_key
        if secret_key is not None:
            self._s3["aws_secret_access_key"] = secret_key
        if region_name is not None:
            self._s3["region_name"] = region_name
        if use_ssl is not None:
            self._s3["use_ssl"] = use_ssl
        return self

    def polling(
        self,
        *,
        interval_seconds: float | None = None,
        max_messages_per_poll: int | None = None,
        visibility_timeout_seconds: float | None = None,
        lease_refresh_interval_seconds: float | None = None,
        jitter_max_ms: int | None = None,
    ) -> "PawnQueueBuilder":
        """
        Configure polling behavior.

        Args:
            interval_seconds: How often to poll for messages (default: 5.0)
            max_messages_per_poll: Max messages to return per poll (default: 10)
            visibility_timeout_seconds: How long a leased message stays hidden (default: 30.0)
            lease_refresh_interval_seconds: How often to auto-renew leases (default: 10.0)
            jitter_max_ms: Max jitter for polling interval (default: 200)

        Returns:
            self for method chaining
        """
        if interval_seconds is not None:
            self._polling["interval_seconds"] = interval_seconds
        if max_messages_per_poll is not None:
            self._polling["max_messages_per_poll"] = max_messages_per_poll
        if visibility_timeout_seconds is not None:
            self._polling["visibility_timeout_seconds"] = visibility_timeout_seconds
        if lease_refresh_interval_seconds is not None:
            self._polling["lease_refresh_interval_seconds"] = lease_refresh_interval_seconds
        if jitter_max_ms is not None:
            self._polling["jitter_max_ms"] = jitter_max_ms
        return self

    def concurrency(
        self,
        *,
        strategy: Literal["auto", "conditional_write", "csprng_verify"] | None = None,
        jitter_min_ms: int | None = None,
        jitter_max_ms: int | None = None,
        verify_retries: int | None = None,
        verify_retry_delay_ms: int | None = None,
    ) -> "PawnQueueBuilder":
        """
        Configure concurrency strategy and csprng_verify settings.

        Args:
            strategy: Lease acquisition strategy:
                     - "auto" (default): Auto-detect based on S3 backend
                     - "conditional_write": Atomic writes (AWS S3, Cloudflare R2)
                     - "csprng_verify": Crypto compare-and-swap (Hetzner, MinIO, etc.)
            jitter_min_ms: Min jitter for csprng_verify retries (default: 100 ms)
                          ⚠️  Must exceed 2× expected network RTT to S3
            jitter_max_ms: Max jitter for csprng_verify (default: 400 ms)
            verify_retries: Number of ETag verification retries (default: 2)
            verify_retry_delay_ms: Delay between verify retries (default: 150 ms)

        Returns:
            self for method chaining
        """
        if strategy is not None:
            self._concurrency["strategy"] = strategy
        if jitter_min_ms is not None:
            self._csprng_verify["jitter_min_ms"] = jitter_min_ms
        if jitter_max_ms is not None:
            self._csprng_verify["jitter_max_ms"] = jitter_max_ms
        if verify_retries is not None:
            self._csprng_verify["verify_retries"] = verify_retries
        if verify_retry_delay_ms is not None:
            self._csprng_verify["verify_retry_delay_ms"] = verify_retry_delay_ms
        return self

    def registry(
        self,
        *,
        heartbeat_interval_seconds: float | None = None,
    ) -> "PawnQueueBuilder":
        """
        Configure registry (producer/consumer registration) behavior.

        Args:
            heartbeat_interval_seconds: How often to send heartbeats (default: 60.0)

        Returns:
            self for method chaining
        """
        if heartbeat_interval_seconds is not None:
            self._registry["heartbeat_interval_seconds"] = heartbeat_interval_seconds
        return self

    def from_env(self) -> "PawnQueueBuilder":
        """
        Fill any unset configuration values from environment variables.

        Respects the full PAWNQUEUE_* env var mapping documented in config.py.
        Env vars override builder-set values.

        Returns:
            self for method chaining
        """
        import os

        # Rebuild config dict with current state
        config_dict: dict = {
            "s3": self._s3.copy() if self._s3 else {},
            "polling": self._polling.copy() if self._polling else {},
            "concurrency": {
                "strategy": self._concurrency.get("strategy"),
                "csprng_verify": self._csprng_verify.copy() if self._csprng_verify else {},
            } if self._concurrency or self._csprng_verify else {},
            "registry": self._registry.copy() if self._registry else {},
        }

        # Remove None values and empty dicts for cleaner Pydantic input
        config_dict = {
            k: v for k, v in config_dict.items()
            if v is not None and (not isinstance(v, dict) or v)
        }

        # PawnQueueConfig._apply_env_overrides is called automatically by
        # Pydantic's model_validator, so just creating the config object
        # will pick up all environment variables.
        cfg = PawnQueueConfig.model_validate(config_dict)

        # Re-extract the validated values back into builder state
        # (This ensures consistency between what was set and what was validated)
        if cfg.s3:
            self._s3 = {
                "endpoint_url": cfg.s3.endpoint_url,
                "bucket_name": cfg.s3.bucket_name,
                "aws_access_key_id": cfg.s3.aws_access_key_id,
                "aws_secret_access_key": cfg.s3.aws_secret_access_key,
                "region_name": cfg.s3.region_name,
                "use_ssl": cfg.s3.use_ssl,
            }
        if cfg.polling:
            self._polling = {
                "interval_seconds": cfg.polling.interval_seconds,
                "max_messages_per_poll": cfg.polling.max_messages_per_poll,
                "visibility_timeout_seconds": cfg.polling.visibility_timeout_seconds,
                "lease_refresh_interval_seconds": cfg.polling.lease_refresh_interval_seconds,
                "jitter_max_ms": cfg.polling.jitter_max_ms,
            }
        if cfg.concurrency:
            self._concurrency = {"strategy": cfg.concurrency.strategy}
            self._csprng_verify = {
                "jitter_min_ms": cfg.concurrency.csprng_verify.jitter_min_ms,
                "jitter_max_ms": cfg.concurrency.csprng_verify.jitter_max_ms,
                "verify_retries": cfg.concurrency.csprng_verify.verify_retries,
                "verify_retry_delay_ms": cfg.concurrency.csprng_verify.verify_retry_delay_ms,
            }
        if cfg.registry:
            self._registry = {
                "heartbeat_interval_seconds": cfg.registry.heartbeat_interval_seconds,
            }

        return self

    async def build(self) -> PawnQueue:
        """
        Construct and return a PawnQueue instance ready for use.

        The instance is NOT automatically started; call `await pawnqueue.setup()`
        or use it as an async context manager.

        Raises:
            ValueError: If required S3 parameters (endpoint_url, bucket_name,
                       access_key, secret_key) are missing.

        Returns:
            A PawnQueue instance
        """
        # Build the complete config dict
        config_dict: dict = {}

        # S3 is required
        if not self._s3 or not all(
            k in self._s3 for k in ["endpoint_url", "bucket_name", "aws_access_key_id", "aws_secret_access_key"]
        ):
            raise ValueError(
                "Missing required S3 parameters. Call .s3(endpoint_url=..., bucket_name=..., "
                "access_key=..., secret_key=...) before .build()"
            )
        config_dict["s3"] = self._s3

        # Optional sections
        if self._polling:
            config_dict["polling"] = self._polling
        if self._concurrency or self._csprng_verify:
            config_dict["concurrency"] = {
                "strategy": self._concurrency.get("strategy", "auto"),
                "csprng_verify": self._csprng_verify,
            }
        if self._registry:
            config_dict["registry"] = self._registry

        # Create config and PawnQueue
        config = PawnQueueConfig.from_dict(config_dict)
        return PawnQueue(config)

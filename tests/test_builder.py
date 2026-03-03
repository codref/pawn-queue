"""
tests/test_builder.py
~~~~~~~~~~~~~~~~~~~~~
Unit tests for PawnQueueBuilder (fluent configuration API).

Tests cover:
- Builder method chaining
- Partial configuration with from_env() overlay
- Environment variable precedence
- Required parameter validation
- Config merging
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from pawn_queue.config import PawnQueueConfig
from pawn_queue.core import PawnQueueBuilder


class TestBuilderBasics:
    """Test basic builder chaining and method returns."""

    def test_builder_returns_self(self):
        """All builder methods return self for chaining."""
        builder = PawnQueueBuilder()
        assert builder.s3(endpoint_url="http://localhost:9000", bucket_name="test") is builder
        assert builder.polling(interval_seconds=2) is builder
        assert builder.concurrency(strategy="conditional_write") is builder
        assert builder.registry(heartbeat_interval_seconds=30) is builder

    def test_builder_chaining(self):
        """Builder methods can be chained together."""
        builder = (
            PawnQueueBuilder()
            .s3(
                endpoint_url="http://localhost:9000",
                bucket_name="test-bucket",
                access_key="test",
                secret_key="test",
            )
            .polling(interval_seconds=2)
            .concurrency(strategy="conditional_write")
        )
        assert builder._s3["endpoint_url"] == "http://localhost:9000"
        assert builder._polling["interval_seconds"] == 2
        assert builder._concurrency["strategy"] == "conditional_write"

    def test_s3_full_config(self):
        """S3 builder captures all parameters."""
        builder = PawnQueueBuilder()
        builder.s3(
            endpoint_url="http://localhost:9000",
            bucket_name="my-bucket",
            access_key="key123",
            secret_key="secret123",
            region_name="eu-central-1",
            use_ssl=False,
        )
        assert builder._s3["endpoint_url"] == "http://localhost:9000"
        assert builder._s3["bucket_name"] == "my-bucket"
        assert builder._s3["aws_access_key_id"] == "key123"
        assert builder._s3["aws_secret_access_key"] == "secret123"
        assert builder._s3["region_name"] == "eu-central-1"
        assert builder._s3["use_ssl"] is False

    def test_polling_full_config(self):
        """Polling builder captures all parameters."""
        builder = PawnQueueBuilder()
        builder.polling(
            interval_seconds=1.5,
            max_messages_per_poll=20,
            visibility_timeout_seconds=60,
            lease_refresh_interval_seconds=15,
            jitter_max_ms=100,
        )
        assert builder._polling["interval_seconds"] == 1.5
        assert builder._polling["max_messages_per_poll"] == 20
        assert builder._polling["visibility_timeout_seconds"] == 60
        assert builder._polling["lease_refresh_interval_seconds"] == 15
        assert builder._polling["jitter_max_ms"] == 100

    def test_concurrency_full_config(self):
        """Concurrency builder captures all parameters."""
        builder = PawnQueueBuilder()
        builder.concurrency(
            strategy="csprng_verify",
            jitter_min_ms=150,
            jitter_max_ms=500,
            verify_retries=3,
            verify_retry_delay_ms=200,
        )
        assert builder._concurrency["strategy"] == "csprng_verify"
        assert builder._csprng_verify["jitter_min_ms"] == 150
        assert builder._csprng_verify["jitter_max_ms"] == 500
        assert builder._csprng_verify["verify_retries"] == 3
        assert builder._csprng_verify["verify_retry_delay_ms"] == 200

    def test_registry_config(self):
        """Registry builder captures parameters."""
        builder = PawnQueueBuilder()
        builder.registry(heartbeat_interval_seconds=45.0)
        assert builder._registry["heartbeat_interval_seconds"] == 45.0


class TestBuilderPartialConfig:
    """Test partial configuration (some params set, some from env)."""

    def test_builder_partial_s3_with_from_env(self):
        """from_env() fills in missing S3 credentials from env vars."""
        with patch.dict(
            os.environ,
            {
                "PAWNQUEUE_S3_ENDPOINT_URL": "http://localhost:9000",
                "PAWNQUEUE_S3_BUCKET_NAME": "env-bucket",
                "PAWNQUEUE_S3_ACCESS_KEY": "env-key",
                "PAWNQUEUE_S3_SECRET_KEY": "env-secret",
                "PAWNQUEUE_S3_REGION": "us-west-2",
            },
            clear=False,
        ):
            builder = PawnQueueBuilder()
            builder.from_env()
            assert builder._s3["endpoint_url"] == "http://localhost:9000"
            assert builder._s3["bucket_name"] == "env-bucket"
            assert builder._s3["aws_access_key_id"] == "env-key"
            assert builder._s3["aws_secret_access_key"] == "env-secret"
            assert builder._s3["region_name"] == "us-west-2"

    def test_env_overrides_builder(self):
        """Environment variables override builder-set values."""
        with patch.dict(
            os.environ,
            {
                "PAWNQUEUE_S3_ENDPOINT_URL": "http://env-override:9000",
                "PAWNQUEUE_POLLING_INTERVAL_SECONDS": "3.5",
            },
            clear=False,
        ):
            builder = (
                PawnQueueBuilder()
                .s3(
                    endpoint_url="http://builder:9000",
                    bucket_name="bucket",
                    access_key="key",
                    secret_key="secret",
                )
                .polling(interval_seconds=1.0)
                .from_env()
            )
            # Env var should win
            assert builder._s3["endpoint_url"] == "http://env-override:9000"
            assert builder._polling["interval_seconds"] == 3.5

    def test_from_env_preserves_unset_values(self):
        """from_env() preserves explicitly set builder values."""
        with patch.dict(
            os.environ,
            {
                "PAWNQUEUE_S3_ENDPOINT_URL": "http://localhost:9000",
                "PAWNQUEUE_S3_BUCKET_NAME": "bucket",
                "PAWNQUEUE_S3_ACCESS_KEY": "key",
                "PAWNQUEUE_S3_SECRET_KEY": "secret",
            },
            clear=False,
        ):
            builder = (
                PawnQueueBuilder()
                .polling(interval_seconds=5.0)
                .from_env()
            )
            # Builder-set polling interval should be preserved
            assert builder._polling["interval_seconds"] == 5.0


class TestBuilderEnvVars:
    """Test full environment variable support for all settings."""

    def test_polling_env_vars(self):
        """Polling settings can be set via env vars."""
        with patch.dict(
            os.environ,
            {
                "PAWNQUEUE_S3_ENDPOINT_URL": "http://localhost:9000",
                "PAWNQUEUE_S3_BUCKET_NAME": "bucket",
                "PAWNQUEUE_S3_ACCESS_KEY": "key",
                "PAWNQUEUE_S3_SECRET_KEY": "secret",
                "PAWNQUEUE_POLLING_INTERVAL_SECONDS": "2.5",
                "PAWNQUEUE_POLLING_MAX_MESSAGES_PER_POLL": "25",
                "PAWNQUEUE_POLLING_VISIBILITY_TIMEOUT": "45.0",
                "PAWNQUEUE_POLLING_LEASE_REFRESH_INTERVAL": "8.0",
                "PAWNQUEUE_POLLING_JITTER_MAX_MS": "150",
            },
            clear=False,
        ):
            builder = PawnQueueBuilder().from_env()
            assert builder._polling["interval_seconds"] == 2.5
            assert builder._polling["max_messages_per_poll"] == 25
            assert builder._polling["visibility_timeout_seconds"] == 45.0
            assert builder._polling["lease_refresh_interval_seconds"] == 8.0
            assert builder._polling["jitter_max_ms"] == 150

    def test_concurrency_env_vars(self):
        """Concurrency settings can be set via env vars."""
        with patch.dict(
            os.environ,
            {
                "PAWNQUEUE_S3_ENDPOINT_URL": "http://localhost:9000",
                "PAWNQUEUE_S3_BUCKET_NAME": "bucket",
                "PAWNQUEUE_S3_ACCESS_KEY": "key",
                "PAWNQUEUE_S3_SECRET_KEY": "secret",
                "PAWNQUEUE_CONCURRENCY_STRATEGY": "conditional_write",
                "PAWNQUEUE_CSPRNG_JITTER_MIN_MS": "200",
                "PAWNQUEUE_CSPRNG_JITTER_MAX_MS": "600",
                "PAWNQUEUE_CSPRNG_VERIFY_RETRIES": "4",
                "PAWNQUEUE_CSPRNG_RETRY_DELAY_MS": "250",
            },
            clear=False,
        ):
            builder = PawnQueueBuilder().from_env()
            assert builder._concurrency["strategy"] == "conditional_write"
            assert builder._csprng_verify["jitter_min_ms"] == 200
            assert builder._csprng_verify["jitter_max_ms"] == 600
            assert builder._csprng_verify["verify_retries"] == 4
            assert builder._csprng_verify["verify_retry_delay_ms"] == 250

    def test_registry_env_vars(self):
        """Registry settings can be set via env vars."""
        with patch.dict(
            os.environ,
            {
                "PAWNQUEUE_S3_ENDPOINT_URL": "http://localhost:9000",
                "PAWNQUEUE_S3_BUCKET_NAME": "bucket",
                "PAWNQUEUE_S3_ACCESS_KEY": "key",
                "PAWNQUEUE_S3_SECRET_KEY": "secret",
                "PAWNQUEUE_REGISTRY_HEARTBEAT_INTERVAL": "90.0",
            },
            clear=False,
        ):
            builder = PawnQueueBuilder().from_env()
            assert builder._registry["heartbeat_interval_seconds"] == 90.0

    def test_s3_use_ssl_env_var(self):
        """S3 use_ssl flag can be set via env var (boolean parsing)."""
        with patch.dict(
            os.environ,
            {
                "PAWNQUEUE_S3_ENDPOINT_URL": "http://localhost:9000",
                "PAWNQUEUE_S3_BUCKET_NAME": "bucket",
                "PAWNQUEUE_S3_ACCESS_KEY": "key",
                "PAWNQUEUE_S3_SECRET_KEY": "secret",
                "PAWNQUEUE_S3_USE_SSL": "false",
            },
            clear=False,
        ):
            builder = PawnQueueBuilder().from_env()
            assert builder._s3["use_ssl"] is False

        with patch.dict(
            os.environ,
            {
                "PAWNQUEUE_S3_ENDPOINT_URL": "http://localhost:9000",
                "PAWNQUEUE_S3_BUCKET_NAME": "bucket",
                "PAWNQUEUE_S3_ACCESS_KEY": "key",
                "PAWNQUEUE_S3_SECRET_KEY": "secret",
                "PAWNQUEUE_S3_USE_SSL": "true",
            },
            clear=False,
        ):
            builder = PawnQueueBuilder().from_env()
            assert builder._s3["use_ssl"] is True


class TestBuilderBuild:
    """Test .build() method and config creation."""

    async def test_build_requires_s3(self):
        """build() raises ValueError if S3 config is incomplete."""
        builder = PawnQueueBuilder()
        with pytest.raises(ValueError, match="Missing required S3 parameters"):
            await builder.build()

    async def test_build_requires_s3_endpoint(self):
        """build() requires at least endpoint_url."""
        builder = (
            PawnQueueBuilder()
            .s3(
                bucket_name="bucket",
                access_key="key",
                secret_key="secret",
                # missing endpoint_url
            )
        )
        with pytest.raises(ValueError, match="Missing required S3 parameters"):
            await builder.build()

    async def test_build_full_config(self):
        """build() returns a PawnQueue with all settings applied."""
        from pawn_queue.core import PawnQueue

        builder = (
            PawnQueueBuilder()
            .s3(
                endpoint_url="http://localhost:9000",
                bucket_name="test-bucket",
                access_key="test-key",
                secret_key="test-secret",
                region_name="eu-west-1",
                use_ssl=False,
            )
            .polling(interval_seconds=2.0, max_messages_per_poll=15)
            .concurrency(strategy="conditional_write")
            .registry(heartbeat_interval_seconds=45.0)
        )
        pq = await builder.build()
        assert isinstance(pq, PawnQueue)
        assert pq._config.s3.endpoint_url == "http://localhost:9000"
        assert pq._config.s3.bucket_name == "test-bucket"
        assert pq._config.s3.region_name == "eu-west-1"
        assert pq._config.s3.use_ssl is False
        assert pq._config.polling.interval_seconds == 2.0
        assert pq._config.polling.max_messages_per_poll == 15
        assert pq._config.concurrency.strategy == "conditional_write"
        assert pq._config.registry.heartbeat_interval_seconds == 45.0

    async def test_build_with_defaults(self):
        """build() applies config defaults for unset optional values."""
        from pawn_queue.core import PawnQueue

        builder = PawnQueueBuilder().s3(
            endpoint_url="http://localhost:9000",
            bucket_name="bucket",
            access_key="key",
            secret_key="secret",
        )
        pq = await builder.build()
        assert isinstance(pq, PawnQueue)
        # Check defaults are applied
        assert pq._config.s3.region_name == "us-east-1"
        assert pq._config.s3.use_ssl is True  # default
        assert pq._config.polling.interval_seconds == 5.0  # default
        assert pq._config.polling.max_messages_per_poll == 10  # default
        assert pq._config.concurrency.strategy == "auto"  # default


class TestBuilderRealWorldScenarios:
    """Test realistic usage patterns."""

    async def test_docker_12factor_env_only(self):
        """Full config from environment variables (Docker 12-factor)."""
        from pawn_queue.core import PawnQueue

        with patch.dict(
            os.environ,
            {
                "PAWNQUEUE_S3_ENDPOINT_URL": "https://minio.internal:9000",
                "PAWNQUEUE_S3_BUCKET_NAME": "queues",
                "PAWNQUEUE_S3_ACCESS_KEY": "minioadmin",
                "PAWNQUEUE_S3_SECRET_KEY": "minioadmin",
                "PAWNQUEUE_S3_REGION": "minio",
                "PAWNQUEUE_POLLING_INTERVAL_SECONDS": "1.0",
                "PAWNQUEUE_CONCURRENCY_STRATEGY": "csprng_verify",
            },
            clear=False,
        ):
            pq = await PawnQueueBuilder().from_env().build()
            assert isinstance(pq, PawnQueue)
            assert pq._config.s3.endpoint_url == "https://minio.internal:9000"
            assert pq._config.s3.bucket_name == "queues"
            assert pq._config.polling.interval_seconds == 1.0
            assert pq._config.concurrency.strategy == "csprng_verify"

    async def test_local_dev_with_env_override(self):
        """Local dev with defaults + specific env overrides."""
        from pawn_queue.core import PawnQueue

        with patch.dict(
            os.environ,
            {"PAWNQUEUE_POLLING_INTERVAL_SECONDS": "0.1"},  # Fast local polling
            clear=False,
        ):
            pq = await (
                PawnQueueBuilder()
                .s3(
                    endpoint_url="http://localhost:9000",
                    bucket_name="local-dev",
                    access_key="test",
                    secret_key="test",
                )
                .from_env()
                .build()
            )
            assert isinstance(pq, PawnQueue)
            assert pq._config.polling.interval_seconds == 0.1

    async def test_programmatic_full_control(self):
        """Full programmatic control without env vars."""
        from pawn_queue.core import PawnQueue

        pq = await (
            PawnQueueBuilder()
            .s3(
                endpoint_url="https://fsn1.your-objectstorage.com",
                bucket_name="production-queue",
                access_key="prod-key",
                secret_key="prod-secret",
                region_name="eu-central-1",
                use_ssl=True,
            )
            .polling(
                interval_seconds=5.0,
                max_messages_per_poll=20,
                visibility_timeout_seconds=60.0,
                lease_refresh_interval_seconds=15.0,
            )
            .concurrency(
                strategy="csprng_verify",
                jitter_min_ms=150,
                jitter_max_ms=500,
                verify_retries=3,
            )
            .registry(heartbeat_interval_seconds=120.0)
            .build()
        )
        assert isinstance(pq, PawnQueue)
        assert pq._config.s3.bucket_name == "production-queue"
        assert pq._config.polling.max_messages_per_poll == 20


class TestBuilderEquivalence:
    """Verify builder produces same config as from_config()."""

    async def test_builder_matches_from_config_dict(self):
        """Builder-created config matches from_config() with dict."""
        config_dict = {
            "s3": {
                "endpoint_url": "http://localhost:9000",
                "bucket_name": "test",
                "aws_access_key_id": "key",
                "aws_secret_access_key": "secret",
                "region_name": "us-west-2",
                "use_ssl": False,
            },
            "polling": {
                "interval_seconds": 2.0,
                "max_messages_per_poll": 15,
            },
            "concurrency": {
                "strategy": "conditional_write",
            },
        }

        # Create via from_config
        cfg1 = PawnQueueConfig.from_dict(config_dict)

        # Create via builder
        from pawn_queue.core import PawnQueue
        pq = await (
            PawnQueueBuilder()
            .s3(
                endpoint_url="http://localhost:9000",
                bucket_name="test",
                access_key="key",
                secret_key="secret",
                region_name="us-west-2",
                use_ssl=False,
            )
            .polling(interval_seconds=2.0, max_messages_per_poll=15)
            .concurrency(strategy="conditional_write")
            .build()
        )
        cfg2 = pq._config

        # Compare
        assert cfg1.s3.endpoint_url == cfg2.s3.endpoint_url
        assert cfg1.s3.bucket_name == cfg2.s3.bucket_name
        assert cfg1.polling.interval_seconds == cfg2.polling.interval_seconds
        assert cfg1.concurrency.strategy == cfg2.concurrency.strategy

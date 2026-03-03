"""
tests/conftest.py
~~~~~~~~~~~~~~~~~
Shared pytest fixtures for unit tests.

Uses moto's ThreadedMotoServer (a real HTTP server) so that aioboto3's
async HTTP stack works correctly — unlike the mock_aws() decorator which
intercepts at the botocore layer with synchronous stubs incompatible with
aiobotocore's async response handling.
"""

from __future__ import annotations

import socket

import boto3
import pytest
import pytest_asyncio
from moto.server import ThreadedMotoServer

from pawn_queue.client import S3Client
from pawn_queue.config import PawnQueueConfig
from pawn_queue.lease import LeaseManager
from pawn_queue.registry import Registry
from pawn_queue.topic import TopicManager

BUCKET = "test-pawnqueue-sqs"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _base_config(endpoint: str) -> dict:
    return {
        "s3": {
            "endpoint_url": endpoint,
            "bucket_name": BUCKET,
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
            "region_name": "us-east-1",
            "use_ssl": False,
        },
        "concurrency": {
            "strategy": "csprng_verify",
            "csprng_verify": {
                "jitter_min_ms": 0,
                "jitter_max_ms": 1,
                "verify_retries": 1,
                "verify_retry_delay_ms": 0,
            },
        },
        "polling": {
            "interval_seconds": 0.1,
            "max_messages_per_poll": 10,
            "visibility_timeout_seconds": 30,
            "lease_refresh_interval_seconds": 5,
            "jitter_max_ms": 0,
        },
    }


# ---------------------------------------------------------------------------
# Session-scoped moto HTTP server — one server shared across all tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def moto_server():
    """Start a ThreadedMotoServer and yield its base URL.  Stops on teardown."""
    port = _free_port()
    server = ThreadedMotoServer(port=port)
    server.start()
    endpoint = f"http://127.0.0.1:{port}"
    yield endpoint
    server.stop()


# ---------------------------------------------------------------------------
# Per-test bucket — fresh bucket for every test function
# ---------------------------------------------------------------------------

@pytest.fixture
def moto_bucket(moto_server):
    """Create a clean S3 bucket for the current test, then delete it."""
    s3 = boto3.client(
        "s3",
        endpoint_url=moto_server,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    s3.create_bucket(Bucket=BUCKET)
    yield BUCKET
    # Cleanup: delete all objects then the bucket
    try:
        objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        if objects:
            s3.delete_objects(
                Bucket=BUCKET,
                Delete={"Objects": [{"Key": o["Key"]} for o in objects]},
            )
        s3.delete_bucket(Bucket=BUCKET)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Async fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def s3_client(moto_server, moto_bucket):
    """A real S3Client pointed at the moto HTTP server."""
    config = PawnQueueConfig.from_dict(_base_config(moto_server))
    client = S3Client(config)
    await client.start()
    yield client
    await client.stop()


@pytest.fixture
def config(moto_server):
    return PawnQueueConfig.from_dict(_base_config(moto_server))


@pytest_asyncio.fixture
async def registry(s3_client):
    return Registry(s3_client)


@pytest_asyncio.fixture
async def topic_manager(s3_client):
    return TopicManager(s3_client)


@pytest_asyncio.fixture
async def lease_manager(s3_client, config):
    return LeaseManager(s3_client, config)

"""
e2e/conftest.py
~~~~~~~~~~~~~~~
Session-scoped fixtures for end-to-end tests against a real S3 bucket.

Credential resolution order:
1. Environment variables (PAWNQUEUE_S3_ENDPOINT_URL, PAWNQUEUE_S3_BUCKET_NAME, …)
2. e2e/config.yaml  (NOT committed — copy and fill from config.template.yaml)
3. e2e/config.template.yaml (fallback; contains dummy values → tests will fail
   with a clear error if credentials were not provided)

Isolation:
  Each test session writes under a unique prefix  pawnqueue-e2e-{uuid}/
  A session-scoped autouse fixture deletes all objects under that prefix
  after the suite finishes, regardless of pass/fail.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import pytest_asyncio
import yaml

from pawn_queue import PawnQueue
from pawn_queue.config import PawnQueueConfig
from pawn_queue.utils import new_uuid

E2E_DIR = Path(__file__).parent


def _load_e2e_config() -> dict:
    """Load e2e config with env-var overlays."""
    # Prefer e2e/config.yaml, fall back to template
    config_path = E2E_DIR / "config.yaml"
    if not config_path.exists():
        config_path = E2E_DIR / "config.template.yaml"

    with open(config_path) as f:
        raw: dict = yaml.safe_load(f)

    # Environment variable overlays
    s3 = raw.setdefault("s3", {})
    env_map = {
        "PAWNQUEUE_S3_ENDPOINT_URL": "endpoint_url",
        "PAWNQUEUE_S3_BUCKET_NAME": "bucket_name",
        "PAWNQUEUE_S3_ACCESS_KEY": "aws_access_key_id",
        "PAWNQUEUE_S3_SECRET_KEY": "aws_secret_access_key",
        "PAWNQUEUE_S3_REGION": "region_name",
    }
    for env_key, field in env_map.items():
        val = os.environ.get(env_key)
        if val is not None:
            s3[field] = val

    return raw


# ---------------------------------------------------------------------------
# Session-level run prefix — all e2e objects live under this key prefix
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def run_prefix() -> str:
    prefix = f"pawnqueue-e2e-{new_uuid()}"
    return prefix


@pytest.fixture(scope="session")
def e2e_config_dict(run_prefix) -> dict:
    raw = _load_e2e_config()
    return raw


@pytest.fixture(scope="session")
def e2e_config(e2e_config_dict) -> PawnQueueConfig:
    return PawnQueueConfig.from_dict(e2e_config_dict)


# ---------------------------------------------------------------------------
# Main pawnqueue fixture — session-scoped, shared across all e2e tests
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def pawnqueue(e2e_config, run_prefix) -> PawnQueue:
    """
    Returns a fully set-up PawnQueue instance.
    Cleans up all objects under run_prefix on teardown.
    """
    instance = await PawnQueue.from_config(e2e_config)
    await instance.setup()
    yield instance

    # ── Teardown: delete all test objects ─────────────────────────────
    try:
        all_keys = await instance._client.list_objects(run_prefix)
        if all_keys:
            await instance._client.delete_objects(all_keys)
    except Exception as exc:
        print(f"\n[E2E teardown] WARNING: cleanup failed: {exc}")

    await instance.teardown()


# ---------------------------------------------------------------------------
# Conditional-write capability — probed once, shared across all tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def conditional_write_supported(pawnqueue) -> bool:
    """
    Returns True if the S3 backend supports If-None-Match: *.

    This fixture is synchronous because PawnQueue.setup() already ran the probe.
    """
    return pawnqueue._config.effective_strategy() == "conditional_write"


# ---------------------------------------------------------------------------
# Per-test topic factory — creates a prefixed topic and registers it
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def topic(pawnqueue, run_prefix, request) -> str:
    """
    Create a unique topic for the current test and return its name.
    Topic name = run_prefix/test_name (slashes replaced with underscores).
    """
    safe_name = request.node.name.replace("[", "_").replace("]", "").replace(" ", "_")
    topic_name = f"{run_prefix}-{safe_name}"[:63]  # S3 prefix length safety
    await pawnqueue.create_topic(topic_name)
    return topic_name

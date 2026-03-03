"""
pawn_queue.client
~~~~~~~~~~~~~~~~~
Thin async wrapper around aioboto3 for S3 operations.

All methods accept and return raw bytes / dicts to keep higher-level modules
free of boto3 specifics.  The context-manager lifecycle is managed by the
caller (PawnQueue.setup() / teardown()).
"""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import aioboto3
from botocore.exceptions import ClientError

from .config import PawnQueueConfig
from .exceptions import CompatibilityError

logger = logging.getLogger(__name__)


class S3Client:
    """Async S3 client wrapping aioboto3."""

    def __init__(self, config: PawnQueueConfig) -> None:
        self._config = config
        self._session = aioboto3.Session(
            aws_access_key_id=config.s3.aws_access_key_id,
            aws_secret_access_key=config.s3.aws_secret_access_key,
            region_name=config.s3.region_name,
        )
        self._client: Any = None  # opened in start()
        self._bucket = config.s3.bucket_name

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Open the underlying aioboto3 client.  Must be called before any operation."""
        self._ctx = self._session.client(
            "s3",
            endpoint_url=self._config.s3.endpoint_url,
            use_ssl=self._config.s3.use_ssl,
        )
        self._client = await self._ctx.__aenter__()

    async def stop(self) -> None:
        """Close the underlying aioboto3 client."""
        if self._ctx is not None:
            await self._ctx.__aexit__(None, None, None)
            self._client = None

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    async def put_object(
        self,
        key: str,
        body: bytes,
        *,
        if_none_match: bool = False,
        content_type: str = "application/json",
    ) -> dict:
        """
        Upload *body* to *key*.

        If *if_none_match* is True, add the ``If-None-Match: *`` header which
        causes the request to fail with PreconditionFailed if the key already
        exists (conditional write / compare-and-swap).
        """
        kwargs: dict[str, Any] = {
            "Bucket": self._bucket,
            "Key": key,
            "Body": body,
            "ContentType": content_type,
        }
        if if_none_match:
            kwargs["IfNoneMatch"] = "*"
        try:
            response = await self._client.put_object(**kwargs)
            return response
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            if if_none_match and error_code in ("PreconditionFailed", "412"):
                from .exceptions import LeaseConflictError
                raise LeaseConflictError(key) from exc
            raise

    async def get_object(self, key: str) -> bytes:
        """Download and return the body of *key*."""
        response = await self._client.get_object(Bucket=self._bucket, Key=key)
        async with response["Body"] as stream:
            return await stream.read()

    async def get_object_json(self, key: str) -> dict:
        """Download *key* and deserialize as JSON."""
        body = await self.get_object(key)
        return json.loads(body)

    async def head_object(self, key: str) -> dict:
        """
        Return object metadata (including ETag) without downloading the body.
        Raises KeyError if the object does not exist.
        """
        try:
            return await self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                raise KeyError(key) from exc
            raise

    async def delete_object(self, key: str) -> None:
        """Delete a single object.  Idempotent (no error if key absent)."""
        await self._client.delete_object(Bucket=self._bucket, Key=key)

    async def delete_objects(self, keys: list[str]) -> None:
        """Bulk-delete up to 1000 objects in a single API call."""
        if not keys:
            return
        objects = [{"Key": k} for k in keys]
        await self._client.delete_objects(
            Bucket=self._bucket,
            Delete={"Objects": objects, "Quiet": True},
        )

    async def copy_object(self, source_key: str, dest_key: str) -> None:
        """Copy *source_key* to *dest_key* within the same bucket."""
        await self._client.copy_object(
            Bucket=self._bucket,
            CopySource={"Bucket": self._bucket, "Key": source_key},
            Key=dest_key,
        )

    async def list_objects(self, prefix: str) -> list[str]:
        """
        List all keys under *prefix* (handles pagination automatically).
        Returns a list of key strings sorted lexicographically (FIFO for time-prefixed keys).
        """
        keys: list[str] = []
        paginator = self._client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return sorted(keys)

    async def object_exists(self, key: str) -> bool:
        """Return True if *key* exists in the bucket."""
        try:
            await self.head_object(key)
            return True
        except KeyError:
            return False

    async def ensure_bucket(self) -> None:
        """Verify the configured bucket is accessible; raise if not."""
        try:
            await self._client.head_bucket(Bucket=self._bucket)
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            raise CompatibilityError(
                f"Bucket {self._bucket!r} not accessible (HTTP {code}). "
                "Ensure the bucket exists and credentials are correct."
            ) from exc

    async def probe_conditional_write(self, probe_key: str) -> bool:
        """
        Probe whether the S3 backend supports If-None-Match: * on PutObject.

        Writes *probe_key* twice: the second write must either raise
        PreconditionFailed (supported) or succeed silently (not supported).
        Returns True if conditional writes work.
        """
        import asyncio
        body_a = b'{"probe":"a"}'
        body_b = b'{"probe":"b"}'

        # First write — establishes the object
        await self.put_object(probe_key, body_a, if_none_match=False)

        # Second write with If-None-Match — should fail if supported
        try:
            await self.put_object(probe_key, body_b, if_none_match=True)
            # If we reach here the header was silently ignored
            return False
        except Exception as exc:
            # Check for PreconditionFailed (412)
            if hasattr(exc, "response"):
                code = exc.response.get("Error", {}).get("Code", "")
                if code in ("PreconditionFailed", "412"):
                    return True
            # LeaseConflictError (our wrapper) means 412 — supported
            from .exceptions import LeaseConflictError
            if isinstance(exc, LeaseConflictError):
                return True
            logger.warning("Conditional write probe raised unexpected error: %s", exc)
            return False
        finally:
            try:
                await self.delete_object(probe_key)
            except Exception:
                pass

"""
pawn_queue.registry
~~~~~~~~~~~~~~~~~~~~
Producer / consumer registration backed by S3.

Each registered entity gets a stable UUIDv4 that is persisted as:
  _registry/{role}/{entity_id}.json

Re-registering with the same (role, name) pair is idempotent — the same ID
is returned.  IDs are stored in an in-memory index keyed by name so that
repeated calls within the same process session skip the list_objects scan.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Literal

from .client import S3Client
from .exceptions import RegistrationError
from .utils import canonical_json, new_uuid, registry_key, utcnow_iso

logger = logging.getLogger(__name__)

Role = Literal["producers", "consumers"]


class RegistryEntry:
    __slots__ = ("role", "name", "entity_id", "registered_at", "last_seen")

    def __init__(self, role: str, name: str, entity_id: str, registered_at: str, last_seen: str) -> None:
        self.role = role
        self.name = name
        self.entity_id = entity_id
        self.registered_at = registered_at
        self.last_seen = last_seen

    def to_dict(self) -> dict:
        return {
            "role": self.role,
            "name": self.name,
            "entity_id": self.entity_id,
            "registered_at": self.registered_at,
            "last_seen": self.last_seen,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RegistryEntry":
        return cls(
            role=data["role"],
            name=data["name"],
            entity_id=data["entity_id"],
            registered_at=data["registered_at"],
            last_seen=data["last_seen"],
        )


class Registry:
    """Manages producer and consumer registrations in S3."""

    def __init__(self, client: S3Client) -> None:
        self._client = client
        # name → RegistryEntry; populated lazily
        self._cache: dict[tuple[str, str], RegistryEntry] = {}

    async def register(self, role: Role, name: str) -> RegistryEntry:
        """
        Register an entity with the given *role* and *name*.

        Idempotent: if an entry with the same (role, name) already exists in
        S3. the existing ID is returned unchanged.  Raises RegistrationError
        on failure.
        """
        cache_key = (role, name)
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Scan S3 registry to find an existing entry for this name
        existing = await self._find_by_name(role, name)
        if existing is not None:
            # Update last_seen
            existing.last_seen = utcnow_iso()
            await self._write_entry(existing)
            self._cache[cache_key] = existing
            return existing

        # Create a new entry
        entity_id = new_uuid()
        now = utcnow_iso()
        entry = RegistryEntry(
            role=role,
            name=name,
            entity_id=entity_id,
            registered_at=now,
            last_seen=now,
        )
        try:
            await self._write_entry(entry)
        except Exception as exc:
            raise RegistrationError(f"Failed to register {role}/{name}: {exc}") from exc

        self._cache[cache_key] = entry
        logger.info("Registered %s %r as %s", role.rstrip("s"), name, entity_id)
        return entry

    async def heartbeat(self, entry: RegistryEntry) -> None:
        """Update last_seen for an existing registered entity."""
        entry.last_seen = utcnow_iso()
        await self._write_entry(entry)

    async def list(self, role: Role | None = None) -> list[RegistryEntry]:
        """
        List all registered entities.  Optionally filter by *role*
        (``'producers'`` or ``'consumers'``).
        """
        roles: list[str] = [role] if role else ["producers", "consumers"]
        entries: list[RegistryEntry] = []
        for r in roles:
            prefix = f"_registry/{r}/"
            keys = await self._client.list_objects(prefix)
            for key in keys:
                try:
                    data = await self._client.get_object_json(key)
                    entries.append(RegistryEntry.from_dict(data))
                except Exception as exc:
                    logger.warning("Could not read registry entry %s: %s", key, exc)
        return entries

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _find_by_name(self, role: str, name: str) -> RegistryEntry | None:
        prefix = f"_registry/{role}/"
        keys = await self._client.list_objects(prefix)
        for key in keys:
            try:
                data = await self._client.get_object_json(key)
                if data.get("name") == name and data.get("role") == role:
                    return RegistryEntry.from_dict(data)
            except Exception as exc:
                logger.debug("Skipping registry entry %s: %s", key, exc)
        return None

    async def _write_entry(self, entry: RegistryEntry) -> None:
        key = registry_key(entry.role, entry.entity_id)
        body = canonical_json(entry.to_dict())
        await self._client.put_object(key, body)

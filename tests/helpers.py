"""Shared helpers for Secure Meters integration tests."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Mapping

from homeassistant.config_entries import ConfigEntry, ConfigEntryState

from custom_components.securemtr import DOMAIN


def create_config_entry(
    *,
    entry_id: str = "entry",
    data: Mapping[str, Any] | None = None,
    options: Mapping[str, Any] | None = None,
    title: str = "SecureMTR",
    unique_id: str | None = None,
    source: str = "test",
    version: int = 1,
    minor_version: int = 1,
) -> ConfigEntry:
    """Return a ConfigEntry instance configured for tests."""

    return ConfigEntry(
        domain=DOMAIN,
        data=dict(data or {}),
        options=dict(options or {}),
        version=version,
        minor_version=minor_version,
        title=title,
        source=source,
        unique_id=unique_id,
        entry_id=entry_id,
        discovery_keys=MappingProxyType({}),
        created_at=None,
        modified_at=None,
        disabled_by=None,
        pref_disable_new_entities=None,
        pref_disable_polling=None,
        state=ConfigEntryState.NOT_LOADED,
        subentries_data=None,
    )


__all__ = ["create_config_entry"]

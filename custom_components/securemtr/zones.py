"""Shared zone configuration metadata for Secure Meters entities."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType


@dataclass(frozen=True, slots=True)
class ZoneMetadata:
    """Describe static metadata for a controller zone."""

    key: str
    label: str
    energy_field: str
    runtime_field: str
    scheduled_field: str
    energy_suffix: str
    runtime_suffix: str
    schedule_suffix: str
    translation_keys: Mapping[str, str]


_ZONE_TRANSLATIONS_PRIMARY = MappingProxyType(
    {
        "energy": "primary_energy_total",
        "runtime": "primary_runtime_daily",
        "scheduled": "primary_scheduled_daily",
    }
)

_ZONE_TRANSLATIONS_BOOST = MappingProxyType(
    {
        "energy": "boost_energy_total",
        "runtime": "boost_runtime_daily",
        "scheduled": "boost_scheduled_daily",
    }
)

ZONE_METADATA: Mapping[str, ZoneMetadata] = MappingProxyType(
    {
        "primary": ZoneMetadata(
            key="primary",
            label="Primary",
            energy_field="primary_energy_kwh",
            runtime_field="primary_active_minutes",
            scheduled_field="primary_scheduled_minutes",
            energy_suffix="primary_energy_kwh",
            runtime_suffix="primary_runtime_h",
            schedule_suffix="primary_sched_h",
            translation_keys=_ZONE_TRANSLATIONS_PRIMARY,
        ),
        "boost": ZoneMetadata(
            key="boost",
            label="Boost",
            energy_field="boost_energy_kwh",
            runtime_field="boost_active_minutes",
            scheduled_field="boost_scheduled_minutes",
            energy_suffix="boost_energy_kwh",
            runtime_suffix="boost_runtime_h",
            schedule_suffix="boost_sched_h",
            translation_keys=_ZONE_TRANSLATIONS_BOOST,
        ),
    }
)


__all__ = ["ZONE_METADATA", "ZoneMetadata"]

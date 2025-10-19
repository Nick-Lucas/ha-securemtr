"""Energy accumulation helpers for SecureMTR energy sensors."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import hashlib
import logging
from typing import Any, TypedDict

from homeassistant.helpers.storage import Store

_LOGGER = logging.getLogger(__name__)

_ZONE_KEYS = ("primary", "boost")
_TOLERANCE = 1e-6
_DIGEST_PRECISION = "{:.6f}"  # Match recorder rounding to improve idempotency.


class LedgerEntry(TypedDict, total=False):
    """Represent a persisted ledger entry for a processed day."""

    energy: float
    digest: str


def _default_zone_state() -> dict[str, Any]:
    """Return the default persistent structure for a zone."""

    return {
        "ledger": {},
        "cumulative_kwh": 0.0,
        "last_processed_day": None,
        "series_start": None,
    }


def _sanitize_day_map(day_map: dict[str, Any]) -> dict[str, LedgerEntry]:
    """Return a sanitized copy of the stored per-day energy ledger."""

    sanitized: dict[str, LedgerEntry] = {}
    for key, value in day_map.items():
        if not isinstance(key, str):
            continue
        try:
            date.fromisoformat(key)
        except ValueError:
            continue

        entry: LedgerEntry = {}
        if isinstance(value, dict):
            energy_value = value.get("energy")
            digest_value = value.get("digest")
        else:
            energy_value = value
            digest_value = None

        try:
            numeric = float(energy_value)
        except (TypeError, ValueError):
            continue

        if numeric < 0:
            continue

        entry["energy"] = numeric
        if isinstance(digest_value, str) and digest_value:
            entry["digest"] = digest_value
        sanitized[key] = entry

    return sanitized


def _entry_digest(energy_kwh: float) -> str:
    """Return a deterministic digest for the provided energy sample."""

    canonical = _DIGEST_PRECISION.format(energy_kwh)
    digest = hashlib.sha1(canonical.encode("utf-8"), usedforsecurity=False)
    return digest.hexdigest()


def _normalise_zone_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Convert stored payloads into the canonical ledger representation."""

    ledger_raw = payload.get("ledger")
    ledger: dict[str, LedgerEntry] = {}
    if isinstance(ledger_raw, dict):
        ledger = _sanitize_day_map(ledger_raw)

    days_raw = payload.get("days")
    if not ledger and isinstance(days_raw, dict):
        ledger = _sanitize_day_map(days_raw)

    cumulative_raw = payload.get("cumulative_kwh")
    try:
        cumulative = float(cumulative_raw)
    except (TypeError, ValueError):
        cumulative = sum(entry.get("energy", 0.0) for entry in ledger.values())
    if cumulative < 0:
        cumulative = 0.0

    last_processed = payload.get("last_processed_day") or payload.get("last_day")
    if not isinstance(last_processed, str) or last_processed not in ledger:
        last_processed = max(ledger.keys(), default=None)

    series_start = payload.get("series_start")
    if not isinstance(series_start, str) or series_start not in ledger:
        series_start = min(ledger.keys(), default=None)

    return {
        "ledger": ledger,
        "cumulative_kwh": cumulative,
        "last_processed_day": last_processed,
        "series_start": series_start,
    }


@dataclass(slots=True)
class EnergyAccumulator:
    """Persist monotonic cumulative energy totals for SecureMTR zones."""

    store: Store[dict[str, Any]]
    _state: dict[str, dict[str, Any]] = field(init=False)
    _loaded: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        """Initialise the runtime state containers."""

        self._state = {zone: _default_zone_state() for zone in _ZONE_KEYS}

    async def async_load(self) -> None:
        """Load persisted accumulator state if available."""

        if self._loaded:
            return

        stored = await self.store.async_load()
        if isinstance(stored, dict):
            for zone in _ZONE_KEYS:
                zone_payload = stored.get(zone)
                if not isinstance(zone_payload, dict):
                    continue
                self._state[zone] = _normalise_zone_payload(zone_payload)

        self._loaded = True

    async def async_add_day(self, zone: str, report_day: date, energy_kwh: float) -> bool:
        """Incorporate a daily energy total for the requested zone."""

        if zone not in _ZONE_KEYS:
            raise ValueError(f"Unsupported energy zone: {zone}")

        await self.async_load()

        normalized = max(0.0, float(energy_kwh))
        day_key = report_day.isoformat()
        zone_state = self._state[zone]
        ledger: dict[str, LedgerEntry] = zone_state.setdefault("ledger", {})
        existing = ledger.get(day_key)
        if existing is not None and abs(existing.get("energy", 0.0) - normalized) <= _TOLERANCE:
            return False

        ledger[day_key] = {
            "energy": normalized,
            "digest": _entry_digest(normalized),
        }
        ordered_days = sorted(ledger)
        cumulative = 0.0
        for key in ordered_days:
            cumulative += ledger[key].get("energy", 0.0)

        previous_total = float(zone_state.get("cumulative_kwh", 0.0))
        if cumulative + _TOLERANCE < previous_total:
            _LOGGER.warning(
                "SecureMTR %s energy decreased from %.3f to %.3f; resetting series",
                zone,
                previous_total,
                cumulative,
            )
            ledger.clear()
            ledger[day_key] = {
                "energy": normalized,
                "digest": _entry_digest(normalized),
            }
            ordered_days = [day_key]
            cumulative = normalized

        zone_state["cumulative_kwh"] = cumulative
        zone_state["last_processed_day"] = ordered_days[-1]
        zone_state["series_start"] = ordered_days[0]

        await self.store.async_save(self._state)
        return True

    def as_sensor_state(self) -> dict[str, dict[str, Any]]:
        """Return the sensor-friendly view of the accumulator state."""

        if not self._loaded:
            return {
                zone: {"energy_sum": 0.0, "last_day": None, "series_start": None}
                for zone in _ZONE_KEYS
            }

        state: dict[str, dict[str, Any]] = {}
        for zone, payload in self._state.items():
            state[zone] = {
                "energy_sum": float(payload.get("cumulative_kwh", 0.0)),
                "last_day": payload.get("last_processed_day"),
                "series_start": payload.get("series_start"),
            }
        return state

    async def async_reset_zone(self, zone: str) -> None:
        """Clear the stored ledger for the requested zone."""

        if zone not in _ZONE_KEYS:
            raise ValueError(f"Unsupported energy zone: {zone}")

        await self.async_load()

        self._state[zone] = _default_zone_state()
        await self.store.async_save(self._state)

    def zone_total(self, zone: str) -> float:
        """Return the cumulative kWh total for the provided zone."""

        if zone not in _ZONE_KEYS:
            raise ValueError(f"Unsupported energy zone: {zone}")

        if not self._loaded:
            return 0.0

        zone_state = self._state.get(zone)
        if not isinstance(zone_state, dict):
            return 0.0

        try:
            return float(zone_state.get("cumulative_kwh", 0.0))
        except (TypeError, ValueError):
            return 0.0


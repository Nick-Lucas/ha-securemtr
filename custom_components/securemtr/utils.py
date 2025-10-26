"""Utility helpers for Secure Meters statistics processing."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from math import log
from statistics import median
from typing import Literal, NamedTuple
from zoneinfo import ZoneInfo

from homeassistant.util import dt as dt_util

LN10 = log(10)
MINUTES_PER_DAY = 24 * 60


class EnergyCalibration(NamedTuple):
    """Describe energy calibration parameters."""

    use_scale: bool
    scale: float
    source: Literal["device_scaled", "duration_power"]


def to_local(value: datetime | float, tz: tzinfo) -> datetime:
    """Convert a timestamp into an aware datetime in the provided timezone."""

    if isinstance(value, datetime):
        dt_value = value
    else:
        dt_value = datetime.fromtimestamp(float(value), UTC)

    if dt_value.tzinfo is None:
        dt_value = dt_value.replace(tzinfo=UTC)

    return dt_value.astimezone(tz)


def assign_report_day(value: datetime, tz: ZoneInfo) -> date:
    """Return the SecureMTR reporting day for the provided timestamp."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("assign_report_day requires an aware datetime")

    local_dt = value.astimezone(tz)
    return (local_dt - timedelta(days=1)).date()


def safe_anchor_datetime(day: date, anchor: time | None, tz: tzinfo) -> datetime:
    """Return an aware datetime within the requested day while honouring DST gaps and folds."""

    anchor_time = anchor or time(0)
    midnight_local = datetime(day.year, day.month, day.day, tzinfo=tz)
    midnight_utc = dt_util.as_utc(midnight_local)

    minute_map: dict[int, datetime] = {}
    # Iterate through the possible minutes in the day plus a buffer for DST folds.
    for minute_offset in range(MINUTES_PER_DAY + 120):
        candidate_utc = midnight_utc + timedelta(minutes=minute_offset)
        candidate_local = candidate_utc.astimezone(tz)

        if candidate_local.date() < day:
            continue
        if candidate_local.date() > day:
            if minute_map:
                break
            continue

        local_minute = candidate_local.hour * 60 + candidate_local.minute
        minute_map[local_minute] = candidate_local

    if not minute_map:
        return datetime(day.year, day.month, day.day, 23, 59, 59, 999999, tzinfo=tz)

    target_minute = anchor_time.hour * 60 + anchor_time.minute
    sorted_minutes = sorted(minute_map)
    selected_minute = next((m for m in sorted_minutes if m >= target_minute), None)
    if selected_minute is None:
        selected_minute = sorted_minutes[-1]
        if selected_minute < target_minute:
            return midnight_local

    candidate = minute_map[selected_minute]
    if anchor_time.second or anchor_time.microsecond:
        candidate_utc = dt_util.as_utc(candidate)
        candidate = (
            candidate_utc
            + timedelta(
                seconds=anchor_time.second,
                microseconds=anchor_time.microsecond,
            )
        ).astimezone(tz)

    day_end = datetime(day.year, day.month, day.day, 23, 59, 59, 999999, tzinfo=tz)
    return min(candidate, day_end)


def split_runtime_segments(
    anchor: datetime, runtime_hours: float, total_energy_kwh: float
) -> list[tuple[datetime, float, float]]:
    """Return hour-aligned runtime segments capped at the day boundary."""

    if anchor.tzinfo is None or anchor.utcoffset() is None:
        raise ValueError("split_runtime_segments requires an aware datetime anchor")

    if runtime_hours <= 0 or total_energy_kwh <= 0:
        return []

    day_start = anchor.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)

    segments: list[tuple[datetime, float]] = []
    current_start = anchor
    remaining_hours = runtime_hours

    while remaining_hours > 0 and current_start < day_end:
        hour_start = current_start.replace(minute=0, second=0, microsecond=0)
        next_hour = hour_start + timedelta(hours=1)
        slot_end = min(next_hour, day_end)
        slot_seconds = (slot_end - current_start).total_seconds()
        slot_hours = min(remaining_hours, max(slot_seconds / 3600.0, 0.0))
        if slot_hours <= 0:  # pragma: no cover - defensive guard for zero-length slots
            break

        segments.append((current_start, slot_hours))
        remaining_hours -= slot_hours
        current_start = current_start + timedelta(seconds=slot_hours * 3600.0)

    if not segments:  # pragma: no cover - runtime guards prevent empty collections
        return []

    total_duration = sum(duration for _, duration in segments)
    if total_duration <= 0:  # pragma: no cover - defensive guard against precision loss
        return []

    remaining_energy = total_energy_kwh
    scaled_segments: list[tuple[datetime, float, float]] = []
    for index, (slot_start, slot_hours) in enumerate(segments):
        if index == len(segments) - 1:
            slot_energy = remaining_energy
        else:
            slot_energy = (slot_hours / total_duration) * total_energy_kwh
            remaining_energy -= slot_energy

        scaled_segments.append((slot_start, slot_hours, max(slot_energy, 0.0)))

    return scaled_segments


def _collect_ratios(
    rows: Iterable[Mapping[str, object]],
    energy_field: str,
    duration_field: str,
    fallback_power_kw: float,
) -> list[float]:
    """Collect runtime-to-energy ratios from the provided rows."""

    ratios: list[float] = []
    for row in rows:
        energy_raw = row.get(energy_field)
        duration_raw = row.get(duration_field)

        if not isinstance(energy_raw, (int, float)):
            continue
        if not isinstance(duration_raw, (int, float)):
            continue

        duration_minutes = float(duration_raw)
        if duration_minutes <= 0:
            continue

        fallback_energy = (duration_minutes / 60.0) * fallback_power_kw
        if fallback_energy <= 0:
            continue

        reported_energy = float(energy_raw)
        if reported_energy <= 0:
            continue

        ratios.append(fallback_energy / reported_energy)

    return ratios


def calibrate_energy_scale(
    rows: Sequence[Mapping[str, object]] | Iterable[Mapping[str, object]],
    energy_field: str,
    duration_field: str,
    fallback_power_kw: float,
    *,
    tolerance: float = 0.2,
) -> EnergyCalibration:
    """Return calibration data describing how to interpret device energy."""

    ratios = _collect_ratios(rows, energy_field, duration_field, fallback_power_kw)
    if not ratios:
        return EnergyCalibration(False, fallback_power_kw, "duration_power")

    median_ratio = median(ratios)
    if abs(median_ratio - LN10) / LN10 <= tolerance:
        return EnergyCalibration(True, LN10, "device_scaled")

    return EnergyCalibration(False, fallback_power_kw, "duration_power")


def energy_from_row(
    row: Mapping[str, object],
    energy_field: str,
    duration_field: str,
    calibration: EnergyCalibration,
    fallback_power_kw: float,
) -> float:
    """Compute energy for a row using calibration or fallback duration power."""

    energy_raw = row.get(energy_field)
    if calibration.use_scale and isinstance(energy_raw, (int, float)):
        energy = float(energy_raw) * calibration.scale
        if energy >= 0:
            return energy

    duration_raw = row.get(duration_field)
    duration_minutes = (
        float(duration_raw) if isinstance(duration_raw, (int, float)) else 0.0
    )
    if duration_minutes <= 0:
        return 0.0

    power_kw = calibration.scale if not calibration.use_scale else fallback_power_kw
    return (duration_minutes / 60.0) * power_kw


def cumulative_update(current: float | None, delta: float) -> float:
    """Return the updated cumulative value from the provided delta."""

    base = current or 0.0
    return base + delta

"""Statistics helpers for the SecureMTR integration."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
import logging
from typing import Any
from zoneinfo import ZoneInfo

from aiohttp import ClientWebSocketResponse
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.util import dt as dt_util

from . import runtime_helpers
from .beanbag import BeanbagBackend, BeanbagSession, WeeklyProgram
from .energy import EnergyAccumulator
from .schedule import canonicalize_weekly, day_intervals
from .utils import (
    EnergyCalibration,
    calibrate_energy_scale,
    energy_from_row,
    safe_anchor_datetime,
    split_runtime_segments,
)
from .zones import ZONE_METADATA

_LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class StatisticsOptions:
    """Represent statistics configuration derived from entry options."""

    timezone: ZoneInfo
    timezone_name: str
    primary_anchor: time
    boost_anchor: time
    fallback_power_kw: float
    prefer_device_energy: bool


@dataclass(slots=True)
class ZoneContext:
    """Describe mapping and schedule context for a controller zone."""

    label: str
    energy_field: str
    runtime_field: str
    scheduled_field: str
    energy_suffix: str
    runtime_suffix: str
    schedule_suffix: str
    fallback_anchor: time
    program: WeeklyProgram | None
    canonical: list[tuple[int, int]] | None


@dataclass(slots=True)
class PreparedSamples:
    """Represent normalised consumption samples ready for processing."""

    rows: list[dict[str, Any]]
    options: StatisticsOptions


@dataclass(slots=True)
class ZoneProcessingResult:
    """Collect the outcomes from zone-level consumption processing."""

    statistics_samples: dict[str, list[dict[str, Any]]]
    sensor_state: dict[str, Any]
    recent_measurements: dict[str, Any]
    energy_state_changed: bool
    dispatch_needed: bool


async def _read_zone_programs(
    backend: BeanbagBackend,
    session: BeanbagSession,
    websocket: ClientWebSocketResponse,
    *,
    gateway_id: str,
    entry_identifier: str,
) -> tuple[dict[str, WeeklyProgram | None], dict[str, list[tuple[int, int]] | None]]:
    """Fetch and canonicalise weekly programs for each zone."""

    programs: dict[str, WeeklyProgram | None] = {}
    canonicals: dict[str, list[tuple[int, int]] | None] = {}

    for zone_key in ZONE_METADATA:
        program = await runtime_helpers.async_read_zone_program(
            backend,
            session,
            websocket,
            gateway_id=gateway_id,
            zone=zone_key,
            entry_identifier=entry_identifier,
        )
        programs[zone_key] = program
        canonicals[zone_key] = canonicalize_weekly(program) if program else None

    return programs, canonicals


def _build_zone_contexts(
    options: StatisticsOptions,
    programs: Mapping[str, WeeklyProgram | None],
    canonicals: Mapping[str, list[tuple[int, int]] | None],
) -> dict[str, ZoneContext]:
    """Construct zone contexts describing metadata and schedules."""

    anchors = {zone_key: getattr(options, f"{zone_key}_anchor") for zone_key in programs}

    return {
        zone_key: ZoneContext(
            label=metadata.label,
            energy_field=metadata.energy_field,
            runtime_field=metadata.runtime_field,
            scheduled_field=metadata.scheduled_field,
            energy_suffix=metadata.energy_suffix,
            runtime_suffix=metadata.runtime_suffix,
            schedule_suffix=metadata.schedule_suffix,
            fallback_anchor=anchors[zone_key],
            program=programs.get(zone_key),
            canonical=canonicals.get(zone_key),
        )
        for zone_key, metadata in ZONE_METADATA.items()
    }


def _build_zone_calibrations(
    processed_rows: list[dict[str, Any]],
    contexts: Mapping[str, ZoneContext],
    options: StatisticsOptions,
    entry_identifier: str,
) -> dict[str, EnergyCalibration]:
    """Generate per-zone energy calibrations."""

    calibrations: dict[str, EnergyCalibration] = {}

    for zone_key, context in contexts.items():
        calibration = calibrate_energy_scale(
            processed_rows,
            context.energy_field,
            context.runtime_field,
            options.fallback_power_kw,
        )
        calibrations[zone_key] = calibration
        _LOGGER.info(
            "%s calibration for %s: use_scale=%s scale=%.6f source=%s",
            context.label,
            entry_identifier,
            calibration.use_scale,
            calibration.scale,
            calibration.source,
        )

    if not options.prefer_device_energy:
        calibrations["primary"] = EnergyCalibration(
            False, options.fallback_power_kw, "duration_power"
        )
        calibrations["boost"] = EnergyCalibration(
            False, options.fallback_power_kw, "duration_power"
        )

    return calibrations


async def _process_zone_records(
    accumulator: EnergyAccumulator,
    processed_rows: list[dict[str, Any]],
    contexts: Mapping[str, ZoneContext],
    calibrations: Mapping[str, EnergyCalibration],
    options: StatisticsOptions,
    entry_identifier: str,
) -> tuple[
    dict[str, list[dict[str, Any]]],
    dict[str, tuple[date, float | None, float | None]],
    bool,
]:
    """Update accumulators and build statistic samples for each zone."""

    statistics_samples: dict[str, list[dict[str, Any]]] = {}
    zone_summaries: dict[str, tuple[date, float | None, float | None]] = {}
    energy_changed = False

    for zone_key, context in contexts.items():
        calibration = calibrations[zone_key]
        latest_runtime_hours: float | None = None
        latest_scheduled_hours: float | None = None
        latest_day: date | None = None
        running_sum = accumulator.zone_total(zone_key)
        zone_records: list[dict[str, Any]] = []

        for row in processed_rows:
            report_day: date = row["report_day"]

            energy_value = energy_from_row(
                row,
                context.energy_field,
                context.runtime_field,
                calibration,
                options.fallback_power_kw,
            )
            energy_value = max(0.0, energy_value)
            before_total = running_sum
            updated = await accumulator.async_add_day(zone_key, report_day, energy_value)
            zone_total = accumulator.zone_total(zone_key)
            if updated:
                energy_changed = True
                _LOGGER.info(
                    "SecureMTR %s energy on %s for %s: day=%.3f kWh cumulative=%.3f kWh",
                    context.label,
                    report_day.isoformat(),
                    entry_identifier,
                    energy_value,
                    zone_total,
                )

            runtime_minutes = float(row.get(context.runtime_field, 0.0))
            runtime_hours = max(runtime_minutes, 0.0) / 60.0

            intervals: list[tuple[datetime, datetime]] = []
            if context.program is not None and context.canonical is not None:
                intervals = day_intervals(
                    context.program,
                    day=report_day,
                    tz=options.timezone,
                    canonical=context.canonical,
                )

            anchor, anchor_source, anchor_interval = _resolve_anchor(
                report_day,
                context,
                options,
                intervals,
                runtime_hours,
            )

            scheduled_minutes = float(row.get(context.scheduled_field, 0.0))
            scheduled_hours = max(scheduled_minutes, 0.0) / 60.0

            _LOGGER.debug(
                "%s energy sample for %s on %s (updated=%s): anchor_source=%s "
                "anchor=%s energy=%.3f runtime_h=%.2f scheduled_h=%.2f intervals=%d",
                context.label,
                entry_identifier,
                report_day.isoformat(),
                updated,
                anchor_source,
                anchor.isoformat(),
                energy_value,
                runtime_hours,
                scheduled_hours,
                len(intervals),
            )

            segment_energy = zone_total - before_total
            if (
                updated
                and runtime_hours > 0
                and energy_value > 0
                and segment_energy > 0
            ):
                records = _build_zone_statistics_samples(
                    anchor,
                    runtime_hours,
                    segment_energy,
                    before_total,
                    interval=anchor_interval,
                )
                if records:
                    zone_records.extend(records)

            running_sum = zone_total
            latest_runtime_hours = runtime_hours
            latest_scheduled_hours = scheduled_hours
            latest_day = report_day

        if zone_records:
            first_record = zone_records[0]
            last_record = zone_records[-1]
            _LOGGER.debug(
                "%s prepared %d statistic samples for %s: first_start=%s last_start=%s "
                "first_sum=%.3f last_sum=%.3f",
                context.label,
                len(zone_records),
                zone_key,
                first_record["start"].isoformat()
                if isinstance(first_record.get("start"), datetime)
                else first_record.get("start"),
                last_record["start"].isoformat()
                if isinstance(last_record.get("start"), datetime)
                else last_record.get("start"),
                float(first_record.get("sum", 0.0)),
                float(last_record.get("sum", 0.0)),
            )
            statistics_samples[zone_key] = zone_records

        if latest_day is not None:
            zone_summaries[zone_key] = (
                latest_day,
                latest_runtime_hours,
                latest_scheduled_hours,
            )

    return statistics_samples, zone_summaries, energy_changed


def _submit_statistics_samples(
    hass: HomeAssistant,
    statistics_samples: Mapping[str, list[dict[str, Any]]],
    energy_entity_ids: Mapping[str, str],
    *,
    statistic_writer: Callable[
        [HomeAssistant, dict[str, Any], Iterable[dict[str, Any]]], None
    ],
    mean_type: Any,
) -> None:
    """Forward prepared statistic samples to the recorder."""

    for zone_key, samples in statistics_samples.items():
        if not samples:
            continue
        entity_id = energy_entity_ids.get(zone_key)
        if entity_id is None:
            continue
        if "." not in entity_id:
            _LOGGER.error(
                "Skipping statistics for %s because entity_id %s is invalid",
                zone_key,
                entity_id,
            )
            continue
        statistic_domain, object_id = entity_id.split(".", 1)
        statistic_id = f"{statistic_domain}:{object_id}"
        metadata = {
            "has_sum": True,
            "mean_type": mean_type,
            "name": None,
            "source": statistic_domain,
            "statistic_id": statistic_id,
            "unit_of_measurement": UnitOfEnergy.KILO_WATT_HOUR,
            "unit_class": "energy",
        }
        try:
            first_sample = samples[0]
            last_sample = samples[-1]
            _LOGGER.debug(
                "Recording %d statistic samples for %s (statistic_id=%s): "
                "first_start=%s last_start=%s first_sum=%.3f last_sum=%.3f",
                len(samples),
                zone_key,
                statistic_id,
                first_sample["start"].isoformat()
                if isinstance(first_sample.get("start"), datetime)
                else first_sample.get("start"),
                last_sample["start"].isoformat()
                if isinstance(last_sample.get("start"), datetime)
                else last_sample.get("start"),
                float(first_sample.get("sum", 0.0)),
                float(last_sample.get("sum", 0.0)),
            )
            statistic_writer(hass, metadata, samples)
        except HomeAssistantError:
            _LOGGER.exception(
                "Failed to add statistics for %s (statistic_id=%s)",
                zone_key,
                statistic_id,
            )


def _build_zone_statistics_samples(
    anchor: datetime,
    runtime_hours: float,
    segment_energy: float,
    before_total: float,
    *,
    interval: tuple[datetime, datetime] | None = None,
) -> list[dict[str, Any]]:
    """Return statistic samples for a runtime segment anchored to a day."""

    if runtime_hours <= 0 or segment_energy <= 0:
        return []

    segments = split_runtime_segments(anchor, runtime_hours, segment_energy)
    cumulative = before_total
    records: list[dict[str, Any]] = []
    interval_start: datetime | None = None
    interval_end: datetime | None = None
    if interval is not None:
        interval_start, interval_end = interval

    for slot_start, _slot_hours, slot_energy in segments:
        if slot_energy <= 0:
            continue
        record_start = slot_start
        if interval_start is not None and interval_end is not None:
            record_start = max(record_start, interval_start)
            if record_start >= interval_end:
                continue
        cumulative += slot_energy
        records.append(
            {
                "start": dt_util.as_utc(record_start),
                "sum": cumulative,
                "state": cumulative,
            }
        )

    return records


def _resolve_anchor(
    report_day: date,
    context: ZoneContext,
    options: StatisticsOptions,
    intervals: Iterable[tuple[datetime, datetime]],
    runtime_hours: float,
) -> tuple[datetime, str, tuple[datetime, datetime] | None]:
    """Select a schedule-aware anchor and interval for the provided day."""

    day_start = datetime(
        report_day.year,
        report_day.month,
        report_day.day,
        tzinfo=options.timezone,
    )
    day_end = day_start + timedelta(days=1)

    fallback_anchor = safe_anchor_datetime(
        report_day, context.fallback_anchor, options.timezone
    )

    selected_interval: tuple[datetime, datetime] | None = None
    anchor_source = "configured"
    anchor = fallback_anchor

    runtime_seconds = max(runtime_hours, 0.0) * 3600.0
    tolerance_seconds = 60.0
    best_slack: float | None = None
    best_offset: float | None = None
    best_anchor: datetime | None = None

    if runtime_seconds > 0.0:
        for start, end in intervals:
            if end <= start:
                continue
            clamped_start = max(start, day_start)
            clamped_end = min(end, day_end)
            if clamped_end <= clamped_start:
                continue
            span_seconds = (clamped_end - clamped_start).total_seconds()
            slack = abs(span_seconds - runtime_seconds)
            offset = abs((clamped_start - fallback_anchor).total_seconds())
            allow_interval = runtime_seconds <= span_seconds + tolerance_seconds
            candidate_interval: tuple[datetime, datetime] | None = None
            if allow_interval:
                candidate_interval = (clamped_start, clamped_end)

            better = False
            if best_slack is None or slack < best_slack - 1e-6:
                better = True
            elif best_slack is not None and abs(slack - best_slack) <= 1e-6:
                if best_offset is None or offset < best_offset - 1e-6:
                    better = True
                elif best_offset is not None and abs(offset - best_offset) <= 1e-6:
                    if selected_interval is None and candidate_interval is not None:
                        better = True

            if better:
                best_slack = slack
                best_offset = offset
                best_anchor = clamped_start
                selected_interval = candidate_interval

    if best_anchor is not None:
        anchor_source = "schedule"
        anchor = best_anchor

    if anchor < day_start:
        anchor = day_start
    elif anchor >= day_end:
        anchor = day_end - timedelta(microseconds=1)

    _LOGGER.debug(
        "Anchor for %s (%s) on %s: runtime=%.2f hours anchor=%s source=%s"
        " interval=%s fallback=%s",
        context.label,
        context.runtime_suffix,
        report_day.isoformat(),
        runtime_hours,
        anchor.isoformat(),
        anchor_source,
        "present" if selected_interval else "absent",
        fallback_anchor.isoformat(),
    )

    return anchor, anchor_source, selected_interval

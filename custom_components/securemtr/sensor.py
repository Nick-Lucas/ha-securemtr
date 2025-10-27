"""Sensors for Secure Meters runtime and statistics metadata."""

from __future__ import annotations

import asyncio
from datetime import datetime
import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfEnergy, UnitOfTime
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.components.sensor import SensorEntity
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import DOMAIN, SecuremtrController, SecuremtrRuntimeData, runtime_update_signal
from .entity import build_device_info, slugify_identifier

_LOGGER = logging.getLogger(__name__)

DEVICE_CLASS_ENERGY = "energy"
DEVICE_CLASS_DURATION = "duration"
DEVICE_CLASS_TIMESTAMP = "timestamp"
STATE_CLASS_MEASUREMENT = "measurement"
STATE_CLASS_TOTAL_INCREASING = "total_increasing"

_CONTROLLER_WAIT_TIMEOUT = 15.0


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the Secure Meters sensors for boost and statistics."""

    runtime: SecuremtrRuntimeData = hass.data[DOMAIN][entry.entry_id]
    entry_label = getattr(entry, "title", None) or getattr(entry, "entry_id", DOMAIN)
    _LOGGER.info("Starting SecureMTR sensor setup for %s", entry_label)

    try:
        await asyncio.wait_for(
            runtime.controller_ready.wait(), _CONTROLLER_WAIT_TIMEOUT
        )
    except TimeoutError as error:
        raise HomeAssistantError(
            "Timed out waiting for Secure Meters controller metadata"
        ) from error

    controller = runtime.controller
    if controller is None:
        raise HomeAssistantError("Secure Meters controller metadata was not available")

    _LOGGER.info(
        "Preparing SecureMTR sensor entities for %s using controller %s",
        entry_label,
        controller.identifier,
    )

    zone_labels = {"primary": "Primary", "boost": "Boost"}
    sensors: list[SecuremtrSensorEntity] = [
        SecuremtrBoostEndsSensor(runtime, controller, entry.entry_id)
    ]
    _LOGGER.debug(
        "Prepared boost end-time sensor %s",
        sensors[0].unique_id,
    )

    for zone_key, label in zone_labels.items():
        sensors.append(
            SecuremtrEnergyTotalSensor(
                runtime, controller, entry.entry_id, zone_key, label
            )
        )
        _LOGGER.info(
            "Prepared SecureMTR %s energy sensor (entity_id=%s, unique_id=%s)",
            label,
            sensors[-1].entity_id,
            sensors[-1].unique_id,
        )
        sensors.append(
            SecuremtrDailyDurationSensor(
                runtime,
                controller,
                entry.entry_id,
                zone_key,
                label,
                "runtime",
                "Runtime (Last Day)",
                "runtime_daily",
            )
        )
        sensors.append(
            SecuremtrDailyDurationSensor(
                runtime,
                controller,
                entry.entry_id,
                zone_key,
                label,
                "scheduled",
                "Scheduled (Last Day)",
                "scheduled_daily",
            )
        )
        _LOGGER.debug(
            "Prepared SecureMTR %s runtime/schedule sensors (unique_ids=%s, %s)",
            label,
            sensors[-2].unique_id,
            sensors[-1].unique_id,
        )

    async_add_entities(sensors)
    _LOGGER.info(
        "Registered %d SecureMTR sensor entities for %s",
        len(sensors),
        entry_label,
    )


class SecuremtrSensorEntity(SensorEntity):
    """Provide shared behaviour for Secure Meters sensors."""

    _attr_should_poll = False
    _attr_device_class: str | None = None
    _attr_state_class: str | None = None
    _attr_native_unit_of_measurement: str | None = None

    def __init__(
        self,
        runtime: SecuremtrRuntimeData,
        controller: SecuremtrController,
        entry_id: str,
    ) -> None:
        """Initialise the sensor with runtime context and controller metadata."""

        self._runtime = runtime
        self._controller = controller
        self._entry_id = entry_id

    @property
    def available(self) -> bool:
        """Return whether the backend is currently connected."""

        if self._runtime.controller is None:
            return False

        if self._runtime.websocket is not None:
            return True

        energy_state = self._runtime.energy_state
        if isinstance(energy_state, dict) and energy_state:
            return True

        return False

    async def async_added_to_hass(self) -> None:
        """Register dispatcher callbacks when added to Home Assistant."""

        await super().async_added_to_hass()
        hass = self.hass
        if hass is None:
            return

        remove = async_dispatcher_connect(
            hass, runtime_update_signal(self._entry_id), self.async_write_ha_state
        )
        self.async_on_remove(remove)

    @property
    def device_class(self) -> str | None:
        """Return the assigned sensor device class."""

        return self._attr_device_class

    @property
    def native_unit_of_measurement(self) -> str | None:
        """Return the unit of measurement for the sensor value."""

        return self._attr_native_unit_of_measurement

    @property
    def state_class(self) -> str | None:
        """Return the statistics state class for the sensor value."""

        return self._attr_state_class

    @property
    def device_info(self) -> dict[str, object]:
        """Return device registry information for the controller."""

        return build_device_info(self._controller)

    def _identifier_slug(self) -> str:
        """Return the slugified identifier for the controller."""

        controller = self._controller
        serial_identifier = controller.serial_number or controller.identifier
        return slugify_identifier(serial_identifier)


class SecuremtrBoostEndsSensor(SecuremtrSensorEntity):
    """Report the expected end time of the active boost run."""

    _attr_device_class = DEVICE_CLASS_TIMESTAMP

    def __init__(
        self,
        runtime: SecuremtrRuntimeData,
        controller: SecuremtrController,
        entry_id: str,
    ) -> None:
        """Initialise the boost end-time sensor."""

        super().__init__(runtime, controller, entry_id)
        self._attr_unique_id = f"{self._identifier_slug()}_boost_ends"
        self._attr_name = "Boost Ends"

    @property
    def native_value(self) -> datetime | None:
        """Return the boost end timestamp when active."""

        if self._runtime.timed_boost_active is not True:
            return None
        return self._runtime.timed_boost_end_time


class SecuremtrEnergyTotalSensor(SecuremtrSensorEntity):
    """Expose the cumulative energy total for a controller zone."""

    _attr_device_class = DEVICE_CLASS_ENERGY
    _attr_state_class = STATE_CLASS_TOTAL_INCREASING
    _attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR

    def __init__(
        self,
        runtime: SecuremtrRuntimeData,
        controller: SecuremtrController,
        entry_id: str,
        zone: str,
        label: str,
    ) -> None:
        """Initialise the energy total sensor for the requested zone."""

        super().__init__(runtime, controller, entry_id)
        self._zone = zone
        self._attr_name = f"SecureMTR {label} Energy kWh"
        self._attr_unique_id = f"{self._identifier_slug()}_{zone}_energy_kwh"
        self.entity_id = f"sensor.securemtr_{zone}_energy_kwh"

    def _zone_state(self) -> dict[str, object] | None:
        """Return the persisted energy state for the zone."""

        state = self._runtime.energy_state
        if not isinstance(state, dict):
            return None
        zone_state = state.get(self._zone)
        return zone_state if isinstance(zone_state, dict) else None

    @property
    def native_value(self) -> float | None:
        """Return the cumulative energy total in kilowatt-hours."""

        zone_state = self._zone_state()
        if not zone_state:
            return None
        energy_raw = zone_state.get("energy_sum")
        if isinstance(energy_raw, (int, float)):
            return float(energy_raw)
        return None

    @property
    def extra_state_attributes(self) -> dict[str, object] | None:
        """Return metadata about the most recent statistic day."""

        zone_state = self._zone_state()
        if not zone_state:
            return None
        last_day = zone_state.get("last_day")
        attributes: dict[str, object] = {}
        if isinstance(last_day, str):
            attributes["last_report_day"] = last_day
        series_start = zone_state.get("series_start")
        if isinstance(series_start, str):
            attributes["series_start_day"] = series_start
        offset = zone_state.get("offset_kwh")
        if isinstance(offset, (int, float)):
            attributes["offset_kwh"] = float(offset)
        return attributes or None


class SecuremtrDailyDurationSensor(SecuremtrSensorEntity):
    """Expose the previous day's runtime or scheduled duration."""

    _attr_device_class = DEVICE_CLASS_DURATION
    _attr_state_class = STATE_CLASS_MEASUREMENT
    _attr_native_unit_of_measurement = UnitOfTime.HOURS

    def __init__(
        self,
        runtime: SecuremtrRuntimeData,
        controller: SecuremtrController,
        entry_id: str,
        zone: str,
        label: str,
        metric: str,
        name_suffix: str,
        unique_suffix: str,
    ) -> None:
        """Initialise the daily duration sensor for the requested zone."""

        super().__init__(runtime, controller, entry_id)
        self._zone = zone
        self._metric = metric
        self._attr_name = f"{label} {name_suffix}"
        self._attr_unique_id = (
            f"{self._identifier_slug()}_{zone}_{unique_suffix}"
        )

    def _recent_state(self) -> dict[str, object] | None:
        """Return the in-memory statistics summary for the zone."""

        recent = self._runtime.statistics_recent
        if not isinstance(recent, dict):
            return None
        zone_state = recent.get(self._zone)
        return zone_state if isinstance(zone_state, dict) else None

    @property
    def native_value(self) -> float | None:
        """Return the previous day's duration in hours."""

        zone_state = self._recent_state()
        if not zone_state:
            return None

        key = f"{self._metric}_hours"
        value = zone_state.get(key)
        if isinstance(value, (int, float)):
            return float(value)
        return None

    @property
    def extra_state_attributes(self) -> dict[str, object] | None:
        """Return the report day and cumulative energy context."""

        zone_state = self._recent_state()
        if not zone_state:
            return None

        attributes: dict[str, object] = {}
        report_day = zone_state.get("report_day")
        if isinstance(report_day, str):
            attributes["report_day"] = report_day
        energy_sum = zone_state.get("energy_sum")
        if isinstance(energy_sum, (int, float)):
            attributes["energy_total_kwh"] = float(energy_sum)
        return attributes or None


__all__ = [
    "SecuremtrBoostEndsSensor",
    "SecuremtrDailyDurationSensor",
    "SecuremtrEnergyTotalSensor",
]

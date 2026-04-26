"""Binary sensors for Secure Meters boost and alarm state."""

from __future__ import annotations

from datetime import UTC, datetime
import logging
from typing import Any

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady, HomeAssistantError
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import (
    CONNECTION_MODE_LOCAL_BLE,
    DOMAIN,
    SecuremtrController,
    SecuremtrRuntimeData,
)
from .entity import SecuremtrRuntimeEntityMixin, async_get_ready_controller

_LOGGER = logging.getLogger(__name__)

_ALARM_DEFINITIONS: tuple[tuple[str, str, str], ...] = (
    (
        "low_battery_ext_temp_sensor",
        "Low battery (external sensor)",
        "battery_alert",
    ),
    ("han_comms_state", "HAN comms failure", "network_alert"),
    ("service_clock_expired", "Service clock", "clock_alert"),
    ("over_current", "Over current", "current_ac"),
    ("switch_weld", "Switch weld", "flash_alert"),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the Secure Meters boost binary sensor."""

    entry_label = getattr(entry, "title", None) or getattr(entry, "entry_id", DOMAIN)

    try:
        runtime, controller = await async_get_ready_controller(hass, entry)
    except HomeAssistantError as error:
        _LOGGER.warning(
            "Skipping SecureMTR binary sensor setup for %s: %s",
            entry_label,
            error,
        )
        if isinstance(error.__cause__, TimeoutError):
            raise ConfigEntryNotReady(
                f"SecureMTR controller for {entry_label} is not ready"
            ) from error
        return

    entities: list[BinarySensorEntity] = [
        SecuremtrBoostActiveBinarySensor(runtime, controller, entry)
    ]

    if runtime.connection_mode == CONNECTION_MODE_LOCAL_BLE:
        entities.extend(
            SecuremtrAlarmBinarySensor(
                runtime,
                controller,
                entry,
                alarm_key=alarm_key,
                alarm_name=alarm_name,
                icon_name=icon_name,
            )
            for alarm_key, alarm_name, icon_name in _ALARM_DEFINITIONS
        )

    async_add_entities(entities)


class SecuremtrBoostActiveBinarySensor(SecuremtrRuntimeEntityMixin, BinarySensorEntity):
    """Indicate whether a timed boost run is currently active."""

    _attr_device_class = BinarySensorDeviceClass.RUNNING

    def __init__(
        self,
        runtime: SecuremtrRuntimeData,
        controller: SecuremtrController,
        entry: ConfigEntry,
    ) -> None:
        """Initialise the boost active sensor."""

        super().__init__(runtime, controller, entry)
        self._set_slug_identifiers("boost_active")
        self._attr_translation_key = "boost_active"

    @property
    def is_on(self) -> bool:
        """Return whether the timed boost is active."""

        return self._runtime.timed_boost_active is True


def _coerce_alarm_timestamp(raw_value: float | None) -> datetime | None:
    """Coerce an alarm timestamp value into an aware UTC datetime."""

    if raw_value is None:
        return None

    timestamp = float(raw_value)
    if timestamp <= 0:
        return None

    if timestamp >= 1_000_000_000_000:
        timestamp = timestamp / 1000.0

    if timestamp < 946_684_800:
        return None

    try:
        return datetime.fromtimestamp(timestamp, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


class SecuremtrAlarmBinarySensor(SecuremtrRuntimeEntityMixin, BinarySensorEntity):
    """Expose one local BLE alarm state as a binary sensor."""

    _attr_device_class = BinarySensorDeviceClass.PROBLEM

    def __init__(
        self,
        runtime: SecuremtrRuntimeData,
        controller: SecuremtrController,
        entry: ConfigEntry,
        *,
        alarm_key: str,
        alarm_name: str,
        icon_name: str,
    ) -> None:
        """Initialise one alarm binary sensor."""

        super().__init__(runtime, controller, entry)
        self._alarm_key = alarm_key
        self._attr_name = alarm_name
        self._attr_icon = f"mdi:{icon_name}"
        self._set_slug_identifiers(f"alarm_{alarm_key}")

    @property
    def available(self) -> bool:
        """Return whether local BLE alarm payloads are available."""

        if self._runtime.connection_mode != CONNECTION_MODE_LOCAL_BLE:
            return False
        return isinstance(self._runtime.alarms_state, dict)

    def _alarm_state(self) -> dict[str, Any] | None:
        """Return the parsed alarm state payload for this alarm key."""

        alarms_state = self._runtime.alarms_state
        if not isinstance(alarms_state, dict):
            return None
        alarm_state = alarms_state.get(self._alarm_key)
        if not isinstance(alarm_state, dict):
            return None
        return alarm_state

    @property
    def is_on(self) -> bool:
        """Return whether this alarm is currently active."""

        alarm_state = self._alarm_state()
        if alarm_state is None:
            return False
        return alarm_state.get("active") is True

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Return additional alarm metadata from the last BLE snapshot."""

        alarm_state = self._alarm_state()
        if alarm_state is None:
            return None

        attributes: dict[str, Any] = {}

        alarm_id = alarm_state.get("alarm_id")
        if isinstance(alarm_id, int):
            attributes["alarm_id"] = alarm_id

        active_count = alarm_state.get("active_count")
        if isinstance(active_count, int):
            attributes["active_count"] = active_count

        channels = alarm_state.get("channels")
        if isinstance(channels, list):
            attributes["channels"] = channels

        latest_raw_value = alarm_state.get("latest_raw_value")
        if isinstance(latest_raw_value, (int, float)):
            attributes["latest_raw_value"] = float(latest_raw_value)
            latest_timestamp = _coerce_alarm_timestamp(latest_raw_value)
            if latest_timestamp is not None:
                attributes["last_event_utc"] = latest_timestamp.isoformat()
            elif self._alarm_key == "service_clock_expired":
                attributes["days_remaining"] = int(latest_raw_value)

        return attributes or None


__all__ = ["SecuremtrAlarmBinarySensor", "SecuremtrBoostActiveBinarySensor"]

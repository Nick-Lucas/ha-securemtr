import asyncio
import logging
from types import SimpleNamespace
from typing import Any

import pytest

from custom_components.securemtr import (
    DOMAIN,
    SecuremtrController,
    SecuremtrRuntimeData,
)
from custom_components.securemtr.binary_sensor import (
    SecuremtrAlarmBinarySensor,
    SecuremtrBoostActiveBinarySensor,
    async_setup_entry,
)
from homeassistant.components.binary_sensor import BinarySensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.exceptions import ConfigEntryNotReady


from tests.helpers import create_config_entry


class DummyBackend:
    """Provide backend stubs to satisfy the runtime interface."""

    async def read_device_metadata(
        self, *args: Any, **kwargs: Any
    ) -> None:  # pragma: no cover
        """Unused helper for interface completeness."""


def _create_runtime() -> SecuremtrRuntimeData:
    """Return runtime data with a connected controller."""

    runtime = SecuremtrRuntimeData(backend=DummyBackend())
    runtime.session = SimpleNamespace()
    runtime.websocket = SimpleNamespace()
    runtime.controller = SecuremtrController(
        identifier="controller-1",
        name="E7+ Smart Water Heater Controller",
        gateway_id="gateway-1",
        serial_number="serial-1",
        firmware_version="1.0.0",
        model="E7+",
    )
    runtime.timed_boost_active = False
    runtime.controller_ready.set()
    return runtime


@pytest.mark.asyncio
async def test_binary_sensor_setup_and_state() -> None:
    """Ensure the binary sensor exposes the boost active flag."""

    runtime = _create_runtime()
    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = create_config_entry(entry_id="entry")
    entities: list[BinarySensorEntity] = []

    await async_setup_entry(hass, entry, entities.extend)

    assert len(entities) == 1
    sensor = entities[0]
    assert isinstance(sensor, SecuremtrBoostActiveBinarySensor)
    assert sensor.unique_id == "serial_1_boost_active"
    assert sensor.is_on is False
    assert sensor.available is True
    info = sensor.device_info
    assert info["name"] == "E7+ Smart Water Heater Controller"
    assert sensor.has_entity_name is True

    runtime.timed_boost_active = True
    assert sensor.is_on is True

    runtime.websocket = None
    assert sensor.available is False

    sensor.hass = SimpleNamespace()

    async def _fake_added(self: SecuremtrBoostActiveBinarySensor) -> None:
        return None

    connections: list[tuple[object, str, Any]] = []

    def _connect(hass_obj: object, signal: str, callback: Any) -> Any:
        connections.append((hass_obj, signal, callback))
        return lambda: None

    removals: list[Any] = []

    def _record_remove(remover: Any) -> None:
        removals.append(remover)

    sensor.async_on_remove = _record_remove  # type: ignore[assignment]

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "custom_components.securemtr.binary_sensor.BinarySensorEntity.async_added_to_hass",
            _fake_added,
        )
        mp.setattr(
            "custom_components.securemtr.entity.async_dispatcher_connect",
            _connect,
        )
        await sensor.async_added_to_hass()

    assert connections[0][0] is sensor.hass
    assert removals


@pytest.mark.asyncio
async def test_binary_sensor_requires_controller() -> None:
    """Ensure setup skips binary sensor creation when controller metadata is missing."""

    runtime = _create_runtime()
    runtime.controller = None
    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = create_config_entry(entry_id="entry")

    entities: list[SecuremtrBoostActiveBinarySensor] = []

    await async_setup_entry(hass, entry, entities.extend)

    assert entities == []


@pytest.mark.asyncio
async def test_binary_sensor_setup_times_out(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Ensure setup skips binary sensor creation when controller metadata is delayed."""

    runtime = _create_runtime()
    runtime.controller_ready = asyncio.Event()
    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = create_config_entry(entry_id="entry")

    monkeypatch.setattr(
        "custom_components.securemtr.entity._CONTROLLER_READY_TIMEOUT", 0.01
    )

    entities: list[SecuremtrBoostActiveBinarySensor] = []

    with caplog.at_level(logging.WARNING):
        with pytest.raises(ConfigEntryNotReady, match="not ready"):
            await async_setup_entry(hass, entry, entities.extend)

    assert entities == []
    assert "Skipping SecureMTR binary sensor setup" in caplog.text


@pytest.mark.asyncio
async def test_binary_sensor_async_added_to_hass_without_hass() -> None:
    """Ensure the dispatcher registration exits when hass is missing."""

    runtime = _create_runtime()
    entry = create_config_entry(entry_id="entry")
    sensor = SecuremtrBoostActiveBinarySensor(runtime, runtime.controller, entry)
    sensor.hass = None
    await sensor.async_added_to_hass()


@pytest.mark.asyncio
async def test_binary_sensor_setup_local_mode_includes_alarms() -> None:
    """Ensure local BLE mode exposes per-alarm binary sensors."""

    runtime = _create_runtime()
    runtime.connection_mode = "local_ble"
    runtime.alarms_state = {
        "han_comms_state": {
            "alarm_id": 9,
            "active": True,
            "active_count": 1,
            "channels": [1],
            "latest_raw_value": 1_712_000_100.0,
        },
        "service_clock_expired": {
            "alarm_id": 10,
            "active": True,
            "active_count": 1,
            "channels": [1],
            "latest_raw_value": 4.0,
        },
    }
    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = create_config_entry(entry_id="entry")
    entities: list[BinarySensorEntity] = []

    await async_setup_entry(hass, entry, entities.extend)

    assert len(entities) == 6
    alarms = [entity for entity in entities if isinstance(entity, SecuremtrAlarmBinarySensor)]
    assert len(alarms) == 5

    by_unique_id = {entity.unique_id: entity for entity in alarms}

    han_alarm = by_unique_id["serial_1_alarm_han_comms_state"]
    assert han_alarm.is_on is True
    han_attrs = han_alarm.extra_state_attributes
    assert han_attrs is not None
    assert han_attrs["alarm_id"] == 9
    assert han_attrs["active_count"] == 1
    assert han_attrs["channels"] == [1]
    assert han_attrs["last_event_utc"].startswith("2024")

    service_clock_alarm = by_unique_id["serial_1_alarm_service_clock_expired"]
    service_clock_attrs = service_clock_alarm.extra_state_attributes
    assert service_clock_attrs is not None
    assert service_clock_attrs["days_remaining"] == 4

    unknown_alarm = by_unique_id["serial_1_alarm_over_current"]
    assert unknown_alarm.is_on is False

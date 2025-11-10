import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from custom_components.securemtr import DOMAIN, SecuremtrController, SecuremtrRuntimeData
from custom_components.securemtr.sensor import (
    DEVICE_CLASS_DURATION,
    DEVICE_CLASS_ENERGY,
    DEVICE_CLASS_TIMESTAMP,
    SecuremtrBoostEndsSensor,
    SecuremtrDailyDurationSensor,
    SecuremtrEnergyTotalSensor,
    SecuremtrSensorEntity,
    STATE_CLASS_MEASUREMENT,
    STATE_CLASS_TOTAL_INCREASING,
    async_setup_entry,
)
from custom_components.securemtr.zones import ZONE_METADATA
from homeassistant.const import UnitOfEnergy, UnitOfTime
from homeassistant.exceptions import HomeAssistantError


SERIAL_SLUG = "serial_1"
PRIMARY_ENERGY_ENTITY_ID = f"sensor.securemtr_{SERIAL_SLUG}_primary_energy_kwh"
BOOST_ENERGY_ENTITY_ID = f"sensor.securemtr_{SERIAL_SLUG}_boost_energy_kwh"


@dataclass(slots=True)
class DummyEntry:
    """Provide the minimal config entry attributes."""

    entry_id: str


class DummyBackend:
    """Provide backend stubs to satisfy the runtime interface."""

    async def read_device_metadata(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
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
    runtime.controller_ready.set()
    runtime.timed_boost_active = False
    runtime.timed_boost_end_time = None
    return runtime


def test_zone_payload_validates_per_zone_dicts() -> None:
    """Ensure the helper returns per-zone dict payloads when valid."""

    runtime = _create_runtime()
    runtime.energy_state = {
        "primary": {"energy_sum": 1.0},
        "boost": "invalid",
    }
    runtime.statistics_recent = {
        "primary": {"runtime_hours": 2.0},
    }
    entry = DummyEntry(entry_id="entry")
    sensor = SecuremtrEnergyTotalSensor(
        runtime,
        runtime.controller,
        entry,
        "primary",
        "energy",
    )

    assert sensor._zone_payload("energy_state", "primary") == {"energy_sum": 1.0}
    assert sensor._zone_payload("energy_state", "boost") is None
    assert sensor._zone_payload("energy_state", "missing") is None

    runtime.energy_state = None
    assert sensor._zone_payload("energy_state", "primary") is None

    assert sensor._zone_payload("statistics_recent", "primary") == {
        "runtime_hours": 2.0
    }

    runtime.statistics_recent = []  # type: ignore[assignment]
    assert sensor._zone_payload("statistics_recent", "primary") is None


@pytest.mark.asyncio
async def test_sensor_reports_end_time() -> None:
    """Ensure the sensor reports the boost end timestamp when active."""

    runtime = _create_runtime()
    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = DummyEntry(entry_id="entry")
    entities: list[SecuremtrSensorEntity] = []

    await async_setup_entry(hass, entry, entities.extend)

    sensor = next(
        entity
        for entity in entities
        if isinstance(entity, SecuremtrBoostEndsSensor)
    )
    assert sensor.unique_id == "serial_1_boost_ends"
    assert sensor.native_value is None
    assert (
        sensor.device_info["name"] == "E7+ Smart Water Heater Controller"
    )
    assert sensor.available is True
    assert sensor.device_class == DEVICE_CLASS_TIMESTAMP
    assert sensor.has_entity_name is True

    sensor.hass = SimpleNamespace()

    async def _fake_added(self: SecuremtrBoostEndsSensor) -> None:
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
            "homeassistant.helpers.entity.Entity.async_added_to_hass",
            _fake_added,
        )
        mp.setattr(
            "custom_components.securemtr.entity.async_dispatcher_connect",
            _connect,
        )
        await sensor.async_added_to_hass()

    assert connections[0][0] is sensor.hass
    assert removals

    runtime.timed_boost_active = True
    expected = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    runtime.timed_boost_end_time = expected
    assert sensor.native_value == expected

    sensor.hass = None
    await sensor.async_added_to_hass()

    runtime.websocket = None
    assert sensor.available is False
    runtime.controller = None
    assert sensor.available is False


@pytest.mark.asyncio
async def test_sensor_requires_controller() -> None:
    """Ensure setup raises when controller metadata is missing."""

    runtime = _create_runtime()
    runtime.controller = None
    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = DummyEntry(entry_id="entry")

    with pytest.raises(HomeAssistantError):
        await async_setup_entry(hass, entry, lambda entities: None)


@pytest.mark.asyncio
async def test_energy_sensors_report_totals() -> None:
    """Ensure the energy sensors expose cumulative and daily values."""

    runtime = _create_runtime()
    runtime.energy_state = {
        "primary": {
            "energy_sum": 12.5,
            "last_day": "2024-03-01",
            "series_start": "2024-02-20",
            "offset_kwh": 0.0,
        },
        "boost": {
            "energy_sum": 4.75,
            "last_day": "2024-03-01",
            "series_start": "2024-02-22",
            "offset_kwh": 1.25,
        },
    }
    runtime.statistics_recent = {
        "primary": {
            "report_day": "2024-03-01",
            "runtime_hours": 3.25,
            "scheduled_hours": 4.0,
            "energy_sum": 12.5,
        },
        "boost": {
            "report_day": "2024-03-01",
            "runtime_hours": 0.5,
            "scheduled_hours": 1.0,
            "energy_sum": 4.75,
        },
    }

    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = DummyEntry(entry_id="entry")
    entities: list[SecuremtrSensorEntity] = []

    await async_setup_entry(hass, entry, entities.extend)

    assert len(entities) == 1 + len(ZONE_METADATA) * 3
    sensors_by_id = {entity.unique_id: entity for entity in entities}

    primary_metadata = ZONE_METADATA["primary"]
    boost_metadata = ZONE_METADATA["boost"]

    primary_energy = sensors_by_id["serial_1_primary_energy_kwh"]
    assert isinstance(primary_energy, SecuremtrEnergyTotalSensor)
    assert primary_energy.translation_key == primary_metadata.translation_keys["energy"]
    assert primary_energy.has_entity_name is True
    assert primary_energy.entity_id == PRIMARY_ENERGY_ENTITY_ID
    assert primary_energy.native_value == pytest.approx(12.5)
    assert (
        primary_energy.native_unit_of_measurement
        == UnitOfEnergy.KILO_WATT_HOUR
    )
    assert primary_energy.device_class == DEVICE_CLASS_ENERGY
    assert primary_energy.state_class == STATE_CLASS_TOTAL_INCREASING
    assert primary_energy.extra_state_attributes == {
        "last_report_day": "2024-03-01",
        "series_start_day": "2024-02-20",
        "offset_kwh": 0.0,
    }
    device_info = primary_energy.device_info
    assert device_info["identifiers"] == {(DOMAIN, "serial-1")}
    assert device_info["manufacturer"] == "Secure Meters"

    runtime.energy_entity_ids = {}
    runtime.config_entry = DummyEntry(entry_id="entry")
    loop = asyncio.get_running_loop()
    primary_energy.hass = SimpleNamespace(
        async_create_task=lambda coro: loop.create_task(coro),
        data={},
        loop=loop,
    )

    async def _noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "custom_components.securemtr._async_ensure_utility_meters",
            _noop,
        )
        await primary_energy.async_added_to_hass()
    assert runtime.energy_entity_ids["primary"] == PRIMARY_ENERGY_ENTITY_ID

    boost_energy = sensors_by_id["serial_1_boost_energy_kwh"]
    assert isinstance(boost_energy, SecuremtrEnergyTotalSensor)
    assert boost_energy.translation_key == boost_metadata.translation_keys["energy"]
    assert boost_energy.has_entity_name is True
    assert boost_energy.entity_id == BOOST_ENERGY_ENTITY_ID
    assert boost_energy.native_value == pytest.approx(4.75)
    assert boost_energy.extra_state_attributes == {
        "last_report_day": "2024-03-01",
        "series_start_day": "2024-02-22",
        "offset_kwh": 1.25,
    }

    runtime.websocket = None
    assert primary_energy.available is True
    assert primary_energy.native_value == pytest.approx(12.5)
    assert boost_energy.available is True

    primary_runtime = sensors_by_id["serial_1_primary_runtime_daily"]
    assert isinstance(primary_runtime, SecuremtrDailyDurationSensor)
    assert primary_runtime.native_value == pytest.approx(3.25)
    assert primary_runtime.translation_key == primary_metadata.translation_keys["runtime"]
    assert primary_runtime.native_unit_of_measurement == UnitOfTime.HOURS
    assert primary_runtime.device_class == DEVICE_CLASS_DURATION
    assert primary_runtime.state_class == STATE_CLASS_MEASUREMENT
    primary_runtime_attrs = primary_runtime.extra_state_attributes
    assert primary_runtime_attrs is not None
    assert primary_runtime_attrs["report_day"] == "2024-03-01"
    assert primary_runtime_attrs["energy_total_kwh"] == pytest.approx(12.5)

    boost_runtime = sensors_by_id["serial_1_boost_runtime_daily"]
    assert boost_runtime.native_value == pytest.approx(0.5)
    assert boost_runtime.translation_key == boost_metadata.translation_keys["runtime"]

    primary_scheduled = sensors_by_id["serial_1_primary_scheduled_daily"]
    assert primary_scheduled.native_value == pytest.approx(4.0)
    assert (
        primary_scheduled.translation_key
        == primary_metadata.translation_keys["scheduled"]
    )
    primary_scheduled_attrs = primary_scheduled.extra_state_attributes
    assert primary_scheduled_attrs is not None
    assert primary_scheduled_attrs["report_day"] == "2024-03-01"
    assert primary_scheduled_attrs["energy_total_kwh"] == pytest.approx(12.5)

    runtime.energy_state = {"boost": {"energy_sum": "invalid", "last_day": 123}}
    assert boost_energy.native_value is None
    assert boost_energy.extra_state_attributes is None

    runtime.energy_state = None
    assert boost_energy.native_value is None
    assert boost_energy.extra_state_attributes is None

    runtime.statistics_recent = {"boost": {"runtime_hours": "invalid"}}
    assert boost_runtime.native_value is None

    runtime.statistics_recent = None
    assert boost_runtime.native_value is None
    assert boost_runtime.extra_state_attributes is None


@pytest.mark.asyncio
async def test_sensor_setup_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure setup raises when controller metadata is delayed."""

    runtime = _create_runtime()
    runtime.controller_ready = asyncio.Event()
    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = DummyEntry(entry_id="entry")

    monkeypatch.setattr(
        "custom_components.securemtr.entity._CONTROLLER_READY_TIMEOUT", 0.01
    )

    with pytest.raises(HomeAssistantError):
        await async_setup_entry(hass, entry, lambda entities: None)


@pytest.mark.asyncio
async def test_energy_sensor_available_with_cached_state() -> None:
    """Ensure energy totals remain available without an active websocket."""

    runtime = _create_runtime()
    runtime.websocket = None
    runtime.energy_state = {
        "primary": {
            "energy_sum": 9.5,
            "last_day": "2024-02-01",
            "series_start": "2024-01-01",
            "offset_kwh": 0.0,
        }
    }

    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = DummyEntry(entry_id="entry")
    entities: list[SecuremtrSensorEntity] = []

    await async_setup_entry(hass, entry, entities.extend)

    energy_sensors = [
        entity for entity in entities if isinstance(entity, SecuremtrEnergyTotalSensor)
    ]
    assert energy_sensors

    primary_energy = next(
        sensor for sensor in energy_sensors if sensor.unique_id.endswith("primary_energy_kwh")
    )
    assert primary_energy.available is True
    assert primary_energy.native_value == pytest.approx(9.5)

"""Tests for the securemtr switch platform."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, cast

import pytest

import custom_components.securemtr.entity as securemtr_entity
import custom_components.securemtr.runtime_helpers as runtime_helpers
from custom_components.securemtr import (
    DOMAIN,
    SecuremtrController,
    SecuremtrRuntimeData,
)
from custom_components.securemtr.beanbag import BeanbagError
from custom_components.securemtr.entity import slugify_identifier
from custom_components.securemtr.switch import (
    SecuremtrPowerSwitch,
    SecuremtrTimedBoostSwitch,
    async_setup_entry,
)
from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.exceptions import HomeAssistantError


from tests.helpers import create_config_entry


class DummyBackend:
    """Capture power commands issued by the switch entity."""

    def __init__(self) -> None:
        self.on_calls: list[tuple[Any, Any, str]] = []
        self.off_calls: list[tuple[Any, Any, str]] = []
        self.timed_boost_calls: list[tuple[Any, Any, str, bool]] = []

    async def turn_controller_on(
        self, session: Any, websocket: Any, gateway_id: str
    ) -> None:
        """Record an on command."""

        self.on_calls.append((session, websocket, gateway_id))

    async def turn_controller_off(
        self, session: Any, websocket: Any, gateway_id: str
    ) -> None:
        """Record an off command."""

        self.off_calls.append((session, websocket, gateway_id))

    async def set_timed_boost_enabled(
        self,
        session: Any,
        websocket: Any,
        gateway_id: str,
        *,
        enabled: bool,
    ) -> None:
        """Record a timed boost toggle command."""

        self.timed_boost_calls.append((session, websocket, gateway_id, enabled))

    async def read_device_metadata(
        self, *args: Any, **kwargs: Any
    ) -> None:  # pragma: no cover - unused stub
        """Placeholder to satisfy the runtime interface."""


def _create_runtime() -> tuple[SecuremtrRuntimeData, DummyBackend]:
    """Construct a runtime data object with a ready controller."""

    backend = DummyBackend()
    runtime = SecuremtrRuntimeData(backend=backend)
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
    runtime.primary_power_on = False
    runtime.timed_boost_enabled = False
    runtime.controller_ready.set()
    return runtime, backend


@pytest.mark.asyncio
async def test_switch_setup_creates_entity(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure the switch platform exposes the controller power switch."""

    runtime, backend = _create_runtime()
    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = create_config_entry(entry_id="entry")
    entities: list[SwitchEntity] = []

    def _add_entities(new_entities: list[SwitchEntity]) -> None:
        entities.extend(new_entities)

    await async_setup_entry(hass, entry, _add_entities)

    assert {entity.unique_id for entity in entities} == {
        "serial_1_primary_power",
        "serial_1_timed_boost",
    }

    power_switch = next(
        entity for entity in entities if entity.unique_id.endswith("primary_power")
    )
    timed_switch = next(
        entity for entity in entities if entity.unique_id.endswith("timed_boost")
    )

    assert isinstance(power_switch, SecuremtrPowerSwitch)
    assert isinstance(timed_switch, SecuremtrTimedBoostSwitch)
    assert power_switch.available
    assert timed_switch.available
    assert power_switch.device_info["identifiers"] == {(DOMAIN, "serial-1")}
    assert timed_switch.device_info["name"] == "E7+ Smart Water Heater Controller"
    assert timed_switch.device_info["model"] == "E7+"
    assert power_switch.translation_key == "controller_power"
    assert timed_switch.translation_key == "timed_boost"
    assert power_switch.has_entity_name is True
    assert timed_switch.has_entity_name is True
    assert power_switch.is_on is False
    assert timed_switch.is_on is False

    async def _fake_run_with_reconnect(
        entry_obj: ConfigEntry,
        runtime_obj: SecuremtrRuntimeData,
        operation: Callable[[Any, Any, Any], Awaitable[Any]],
    ) -> Any:
        if runtime_obj.session is None or runtime_obj.websocket is None:
            raise BeanbagError("no connection")
        return await operation(
            runtime_obj.backend, runtime_obj.session, runtime_obj.websocket
        )

    monkeypatch.setattr(
        "custom_components.securemtr.async_run_with_reconnect",
        _fake_run_with_reconnect,
    )

    dispatcher_calls: list[tuple[object, str]] = []
    monkeypatch.setattr(
        "custom_components.securemtr.runtime_helpers.async_dispatch_runtime_update",
        lambda hass_obj, entry_id: dispatcher_calls.append((hass_obj, entry_id)),
    )

    helper_calls: list[dict[str, Any]] = []
    original_helper = securemtr_entity.SecuremtrCommandMixin._async_controller_command

    async def _wrapped_helper(
        self: Any,
        method_name: str,
        *,
        runtime_update: Any,
        **kwargs: Any,
    ) -> Any:
        helper_calls.append(
            {
                "self": self,
                "method_name": method_name,
                "runtime_update": runtime_update,
                "kwargs": dict(kwargs),
            }
        )
        return await original_helper(
            self,
            method_name,
            runtime_update=runtime_update,
            **kwargs,
        )

    monkeypatch.setattr(
        "custom_components.securemtr.entity.SecuremtrCommandMixin._async_controller_command",
        _wrapped_helper,
    )

    power_switch.hass = SimpleNamespace()
    power_hass = power_switch.hass
    power_switch.entity_id = "switch.securemtr_controller"
    state_writes: list[str] = []

    def _record_state_write() -> None:
        state_writes.append("write")

    power_switch.async_write_ha_state = _record_state_write  # type: ignore[assignment]

    await power_switch.async_turn_on()
    assert backend.on_calls == [(runtime.session, runtime.websocket, "gateway-1")]
    assert runtime.primary_power_on is True
    assert power_switch.is_on is True
    assert state_writes == ["write"]
    assert helper_calls[0]["method_name"] == "turn_controller_on"
    assert (
        helper_calls[0]["kwargs"]["log_context"]
        == "Failed to toggle Secure Meters controller"
    )
    assert helper_calls[0]["kwargs"].get("write_state") is True

    power_switch.hass = None
    state_writes.clear()

    await power_switch.async_turn_off()
    assert backend.off_calls == [(runtime.session, runtime.websocket, "gateway-1")]
    assert runtime.primary_power_on is False
    assert power_switch.is_on is False
    assert state_writes == []

    timed_switch.hass = SimpleNamespace()
    timed_hass = timed_switch.hass
    timed_switch.entity_id = "switch.securemtr_timed_boost"
    timed_state_writes: list[str] = []

    def _record_timed_state_write() -> None:
        timed_state_writes.append("write")

    timed_switch.async_write_ha_state = _record_timed_state_write  # type: ignore[assignment]

    await timed_switch.async_turn_on()
    assert backend.timed_boost_calls == [
        (runtime.session, runtime.websocket, "gateway-1", True)
    ]
    assert runtime.timed_boost_enabled is True
    assert timed_switch.is_on is True
    assert timed_state_writes == ["write"]
    assert helper_calls[2]["method_name"] == "set_timed_boost_enabled"
    assert (
        helper_calls[2]["kwargs"]["log_context"]
        == "Failed to toggle Secure Meters timed boost feature"
    )
    assert helper_calls[2]["kwargs"].get("write_state") is True

    timed_switch.hass = None
    timed_state_writes.clear()

    await timed_switch.async_turn_off()
    assert backend.timed_boost_calls[-1] == (
        runtime.session,
        runtime.websocket,
        "gateway-1",
        False,
    )
    assert runtime.timed_boost_enabled is False
    assert timed_switch.is_on is False
    assert timed_state_writes == []
    assert dispatcher_calls == [(power_hass, "entry"), (timed_hass, "entry")]
    assert len(helper_calls) == 4


@pytest.mark.asyncio
async def test_power_switch_helper_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Surface helper failures as Home Assistant errors for power toggles."""

    runtime, _backend = _create_runtime()
    switch = SecuremtrPowerSwitch(
        runtime, runtime.controller, create_config_entry(entry_id="entry")
    )
    switch.hass = SimpleNamespace()

    recorded_method: list[str] = []
    recorded_kwargs: dict[str, Any] = {}

    async def _failing_helper(
        self: Any,
        method_name: str,
        *,
        runtime_update: Any,
        **kwargs: Any,
    ) -> Any:
        recorded_method.append(method_name)
        recorded_kwargs.update(kwargs)
        raise HomeAssistantError("boom")

    monkeypatch.setattr(
        "custom_components.securemtr.entity.SecuremtrCommandMixin._async_controller_command",
        _failing_helper,
    )

    with pytest.raises(HomeAssistantError, match="boom"):
        await switch.async_turn_on()

    assert recorded_method == ["turn_controller_on"]
    assert (
        recorded_kwargs["log_context"] == "Failed to toggle Secure Meters controller"
    )
    assert recorded_kwargs.get("write_state") is True


def test_power_switch_requires_config_entry() -> None:
    """Ensure the constructor rejects non-ConfigEntry objects."""

    runtime, _backend = _create_runtime()

    with pytest.raises(TypeError):
        SecuremtrPowerSwitch(runtime, runtime.controller, cast(ConfigEntry, object()))


@pytest.mark.asyncio
async def test_async_mutate_wraps_single_exception_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Convert individual exception types into a tuple for the helper."""

    runtime, _backend = _create_runtime()
    switch = SecuremtrPowerSwitch(
        runtime, runtime.controller, create_config_entry(entry_id="entry")
    )

    recorded_kwargs: dict[str, Any] = {}

    async def _noop_helper(*args: Any, **kwargs: Any) -> None:
        recorded_kwargs.update(kwargs)

    monkeypatch.setattr(
        "custom_components.securemtr.entity.async_mutate_runtime",
        _noop_helper,
    )

    async def _operation(*args: Any, **kwargs: Any) -> None:
        return None

    await switch._async_mutate(
        operation=_operation,
        mutation=lambda data: None,
        log_context="ctx",
        exception_types=HomeAssistantError,
    )

    assert recorded_kwargs["exception_types"] == (HomeAssistantError,)


def test_switch_device_info_without_serial() -> None:
    """Ensure device registry names fall back to the identifier when no serial exists."""

    runtime, _backend = _create_runtime()
    controller = SecuremtrController(
        identifier="controller-1",
        name="E7+ Smart Water Heater Controller",
        gateway_id="gateway-1",
        serial_number=None,
        firmware_version=None,
        model=None,
    )

    switch = SecuremtrPowerSwitch(
        runtime, controller, create_config_entry(entry_id="entry")
    )
    device_info = switch.device_info
    assert device_info["name"] == "E7+ Smart Water Heater Controller"
    assert device_info["serial_number"] is None


@pytest.mark.asyncio
async def test_switch_setup_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify the platform raises when metadata is not ready in time."""

    runtime, _backend = _create_runtime()
    runtime.controller_ready = asyncio.Event()
    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = create_config_entry(entry_id="entry")

    monkeypatch.setattr(
        "custom_components.securemtr.entity._CONTROLLER_READY_TIMEOUT", 0.01
    )

    with pytest.raises(HomeAssistantError):
        await async_setup_entry(hass, entry, lambda entities: None)


@pytest.mark.asyncio
async def test_switch_setup_requires_controller() -> None:
    """Ensure a missing controller raises an explicit error."""

    runtime, _backend = _create_runtime()
    runtime.controller = None
    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = create_config_entry(entry_id="entry")

    with pytest.raises(HomeAssistantError):
        await async_setup_entry(hass, entry, lambda entities: None)


@pytest.mark.asyncio
async def test_switch_turn_on_requires_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure the switch raises when the runtime lacks a live connection."""

    runtime, backend = _create_runtime()
    runtime.session = None
    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = create_config_entry(entry_id="entry")
    entities: list[SwitchEntity] = []

    await async_setup_entry(hass, entry, entities.extend)

    async def _fake_run_with_reconnect(
        entry_obj: ConfigEntry,
        runtime_obj: SecuremtrRuntimeData,
        operation: Callable[[Any, Any, Any], Awaitable[Any]],
    ) -> Any:
        raise BeanbagError("no connection")

    monkeypatch.setattr(
        "custom_components.securemtr.async_run_with_reconnect",
        _fake_run_with_reconnect,
    )

    power_switch = next(
        entity for entity in entities if entity.unique_id.endswith("primary_power")
    )
    with pytest.raises(HomeAssistantError):
        await power_switch.async_turn_on()

    assert backend.on_calls == []


@pytest.mark.asyncio
async def test_switch_turn_on_requires_controller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure the power switch validates controller availability."""

    runtime, backend = _create_runtime()
    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = create_config_entry(entry_id="entry")
    entities: list[SwitchEntity] = []

    await async_setup_entry(hass, entry, entities.extend)

    async def _fake_run_with_reconnect(
        entry_obj: ConfigEntry,
        runtime_obj: SecuremtrRuntimeData,
        operation: Callable[[Any, Any, Any], Awaitable[Any]],
    ) -> Any:
        if runtime_obj.session is None or runtime_obj.websocket is None:
            raise BeanbagError("no connection")
        return await operation(
            runtime_obj.backend, runtime_obj.session, runtime_obj.websocket
        )

    monkeypatch.setattr(
        "custom_components.securemtr.async_run_with_reconnect",
        _fake_run_with_reconnect,
    )

    power_switch = next(
        entity for entity in entities if entity.unique_id.endswith("primary_power")
    )

    runtime.controller = None

    with pytest.raises(HomeAssistantError):
        await power_switch.async_turn_on()

    assert backend.on_calls == []


@pytest.mark.asyncio
async def test_timed_boost_requires_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure the timed boost switch raises when the runtime lacks a connection."""

    runtime, backend = _create_runtime()
    runtime.session = None
    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = create_config_entry(entry_id="entry")
    entities: list[SwitchEntity] = []

    await async_setup_entry(hass, entry, entities.extend)

    async def _fake_run_with_reconnect(
        entry_obj: ConfigEntry,
        runtime_obj: SecuremtrRuntimeData,
        operation: Callable[[Any, Any, Any], Awaitable[Any]],
    ) -> Any:
        raise BeanbagError("no connection")

    monkeypatch.setattr(
        "custom_components.securemtr.async_run_with_reconnect",
        _fake_run_with_reconnect,
    )

    timed_switch = next(
        entity for entity in entities if entity.unique_id.endswith("timed_boost")
    )

    with pytest.raises(HomeAssistantError):
        await timed_switch.async_turn_on()

    assert backend.timed_boost_calls == []


@pytest.mark.asyncio
async def test_timed_boost_requires_controller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure the timed boost switch validates controller availability."""

    runtime, backend = _create_runtime()
    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = create_config_entry(entry_id="entry")
    entities: list[SwitchEntity] = []

    await async_setup_entry(hass, entry, entities.extend)

    async def _fake_run_with_reconnect(
        entry_obj: ConfigEntry,
        runtime_obj: SecuremtrRuntimeData,
        operation: Callable[[Any, Any, Any], Awaitable[Any]],
    ) -> Any:
        if runtime_obj.session is None or runtime_obj.websocket is None:
            raise BeanbagError("no connection")
        return await operation(
            runtime_obj.backend, runtime_obj.session, runtime_obj.websocket
        )

    monkeypatch.setattr(
        "custom_components.securemtr.async_run_with_reconnect",
        _fake_run_with_reconnect,
    )

    timed_switch = next(
        entity for entity in entities if entity.unique_id.endswith("timed_boost")
    )

    runtime.controller = None

    with pytest.raises(HomeAssistantError):
        await timed_switch.async_turn_on()

    assert backend.timed_boost_calls == []


@pytest.mark.asyncio
async def test_timed_boost_backend_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Convert backend failures into Home Assistant errors for timed boost."""

    runtime, backend = _create_runtime()
    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = create_config_entry(entry_id="entry")
    entities: list[SwitchEntity] = []

    await async_setup_entry(hass, entry, entities.extend)

    timed_switch = next(
        entity for entity in entities if entity.unique_id.endswith("timed_boost")
    )

    async def _fake_run_with_reconnect(
        entry_obj: ConfigEntry,
        runtime_obj: SecuremtrRuntimeData,
        operation: Callable[[Any, Any, Any], Awaitable[Any]],
    ) -> Any:
        if runtime_obj.session is None or runtime_obj.websocket is None:
            raise BeanbagError("no connection")
        return await operation(
            runtime_obj.backend, runtime_obj.session, runtime_obj.websocket
        )

    monkeypatch.setattr(
        "custom_components.securemtr.async_run_with_reconnect",
        _fake_run_with_reconnect,
    )

    async def _raise(*args: Any, **kwargs: Any) -> None:
        raise BeanbagError("fail")

    monkeypatch.setattr(backend, "set_timed_boost_enabled", _raise)

    with pytest.raises(HomeAssistantError):
        await timed_switch.async_turn_on()

    assert runtime.timed_boost_enabled is False


@pytest.mark.asyncio
async def test_switch_turn_on_handles_backend_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Beanbag errors propagate as Home Assistant errors."""

    runtime, _backend = _create_runtime()

    class ErrorBackend(DummyBackend):
        async def turn_controller_on(
            self, session: Any, websocket: Any, gateway_id: str
        ) -> None:
            raise BeanbagError("boom")

    runtime.backend = ErrorBackend()  # type: ignore[assignment]
    hass = SimpleNamespace(data={DOMAIN: {"entry": runtime}})
    entry = create_config_entry(entry_id="entry")
    entities: list[SecuremtrPowerSwitch] = []

    await async_setup_entry(hass, entry, entities.extend)

    async def _fake_run_with_reconnect(
        entry_obj: ConfigEntry,
        runtime_obj: SecuremtrRuntimeData,
        operation: Callable[[Any, Any, Any], Awaitable[Any]],
    ) -> Any:
        if runtime_obj.session is None or runtime_obj.websocket is None:
            raise BeanbagError("no connection")
        return await operation(
            runtime_obj.backend, runtime_obj.session, runtime_obj.websocket
        )

    monkeypatch.setattr(
        "custom_components.securemtr.async_run_with_reconnect",
        _fake_run_with_reconnect,
    )

    switch = entities[0]
    with pytest.raises(HomeAssistantError):
        await switch.async_turn_on()

    assert runtime.primary_power_on is False


def test_slugify_identifier_generates_stable_slug() -> None:
    """Ensure the helper normalises identifiers as expected."""

    assert slugify_identifier(" Controller #1 ") == "controller__1"


@pytest.mark.asyncio
async def test_switch_async_added_to_hass(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure dispatcher callbacks are registered during entity setup."""

    runtime, _backend = _create_runtime()
    switch = SecuremtrPowerSwitch(
        runtime, runtime.controller, create_config_entry(entry_id="entry")
    )
    switch.hass = SimpleNamespace()

    added_calls: list[SecuremtrPowerSwitch] = []

    async def _fake_added(self: SecuremtrPowerSwitch) -> None:
        added_calls.append(self)

    monkeypatch.setattr(
        "custom_components.securemtr.switch.SwitchEntity.async_added_to_hass",
        _fake_added,
    )

    connections: list[tuple[object, str, Any]] = []

    def _connect(hass_obj: object, signal: str, callback: Any) -> Any:
        connections.append((hass_obj, signal, callback))
        return lambda: None

    monkeypatch.setattr(
        "custom_components.securemtr.entity.async_dispatcher_connect",
        _connect,
    )

    removals: list[Any] = []

    def _record_remove(remover: Any) -> None:
        removals.append(remover)

    switch.async_on_remove = _record_remove  # type: ignore[assignment]

    await switch.async_added_to_hass()

    assert added_calls
    assert connections[0][0] is switch.hass
    assert removals

    switch.hass = None
    await switch.async_added_to_hass()

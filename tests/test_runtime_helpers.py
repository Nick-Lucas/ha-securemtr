"""Tests for runtime helper utilities."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from unittest.mock import AsyncMock

import custom_components.securemtr.runtime_helpers as runtime_helpers
from custom_components.securemtr import SecuremtrController, SecuremtrRuntimeData


@pytest.mark.asyncio
async def test_async_mutate_runtime_handles_async_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure the helper awaits asynchronous mutations and forwards kwargs."""

    backend = SimpleNamespace()
    session = SimpleNamespace()
    websocket = SimpleNamespace()
    controller = SecuremtrController(
        identifier="controller-1",
        name="Controller",
        gateway_id="gateway-1",
        serial_number="serial-1",
        firmware_version="1.0.0",
        model="E7+",
    )
    runtime = SecuremtrRuntimeData(backend=backend)
    runtime.session = session
    runtime.websocket = websocket
    runtime.controller = controller

    entry = SimpleNamespace(entry_id="entry")

    dispatcher_calls: list[tuple[Any, str]] = []
    monkeypatch.setattr(
        "custom_components.securemtr.runtime_helpers.async_dispatch_runtime_update",
        lambda hass_obj, entry_id: dispatcher_calls.append((hass_obj, entry_id)),
    )

    execute_calls: list[tuple[SecuremtrRuntimeData, object, dict[str, Any]]] = []

    async def _fake_execute(
        runtime_arg: SecuremtrRuntimeData,
        entry_arg: object,
        operation: Any,
        **kwargs: Any,
    ) -> object:
        execute_calls.append((runtime_arg, entry_arg, kwargs))
        return await operation(backend, session, websocket, controller)

    monkeypatch.setattr(
        "custom_components.securemtr.runtime_helpers.async_execute_controller_command",
        _fake_execute,
    )

    result_marker = object()

    async def _operation(
        backend_obj: object,
        session_obj: object,
        websocket_obj: object,
        controller_obj: SecuremtrController,
    ) -> object:
        assert backend_obj is backend
        assert session_obj is session
        assert websocket_obj is websocket
        assert controller_obj is controller
        return result_marker

    async def _mutation(data: SecuremtrRuntimeData) -> None:
        data.primary_power_on = True

    write_calls: list[str] = []

    def _write_state() -> None:
        write_calls.append("write")

    hass = SimpleNamespace()
    result = await runtime_helpers.async_mutate_runtime(
        runtime,
        entry,
        entry_id="entry",
        hass=hass,
        operation=_operation,
        mutation=_mutation,
        log_context="log",
        error_message="error",
        exception_types=(ValueError,),
        write_ha_state=_write_state,
    )

    assert result is result_marker
    assert runtime.primary_power_on is True
    assert write_calls == ["write"]
    assert dispatcher_calls == [(hass, "entry")]
    assert execute_calls and execute_calls[0][2]["error_message"] == "error"
    assert execute_calls[0][2]["exception_types"] == (ValueError,)


@pytest.mark.asyncio
async def test_async_execute_controller_command_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure the command proxy delegates to the integration helper."""

    sentinel = object()
    fake = AsyncMock(return_value=sentinel)
    monkeypatch.setattr(
        "custom_components.securemtr.async_execute_controller_command",
        fake,
    )

    runtime = SimpleNamespace()
    entry = SimpleNamespace()
    operation = AsyncMock()

    result = await runtime_helpers.async_execute_controller_command(
        runtime,
        entry,
        operation,
        extra="value",
    )

    assert result is sentinel
    fake.assert_awaited_once_with(runtime, entry, operation, extra="value")


def test_async_dispatch_runtime_update_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure the dispatch proxy forwards to the integration helper."""

    calls: list[tuple[object, str]] = []

    def _fake_dispatch(hass: object, entry_id: str) -> None:
        calls.append((hass, entry_id))

    monkeypatch.setattr(
        "custom_components.securemtr.async_dispatch_runtime_update",
        _fake_dispatch,
    )

    hass = object()
    runtime_helpers.async_dispatch_runtime_update(hass, "entry")

    assert calls == [(hass, "entry")]

"""Runtime mutation helpers for Secure Meters entities."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from aiohttp import ClientWebSocketResponse
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from . import (
    SecuremtrController,
    SecuremtrRuntimeData,
    async_dispatch_runtime_update,
    async_execute_controller_command,
)
from .beanbag import BeanbagBackend, BeanbagSession

_ResultT = TypeVar("_ResultT")

OperationCallable = Callable[
    [
        BeanbagBackend,
        BeanbagSession,
        ClientWebSocketResponse,
        SecuremtrController,
    ],
    Awaitable[_ResultT],
]

MutationCallable = Callable[[SecuremtrRuntimeData], Awaitable[None] | None]


async def async_mutate_runtime(
    runtime: SecuremtrRuntimeData,
    entry: ConfigEntry,
    *,
    entry_id: str,
    hass: HomeAssistant | None,
    operation: OperationCallable,
    mutation: MutationCallable,
    log_context: str,
    error_message: str | None = None,
    exception_types: tuple[type[Exception], ...] | None = None,
    write_ha_state: Callable[[], None] | None = None,
) -> _ResultT:
    """Execute an operation, mutate the runtime, and notify listeners."""

    execute_kwargs: dict[str, object] = {
        "log_context": log_context,
    }
    if error_message is not None:
        execute_kwargs["error_message"] = error_message
    if exception_types is not None:
        execute_kwargs["exception_types"] = exception_types

    result = await async_execute_controller_command(
        runtime,
        entry,
        operation,
        **execute_kwargs,
    )

    mutation_result = mutation(runtime)
    if asyncio.iscoroutine(mutation_result):
        await mutation_result

    if hass is not None:
        if write_ha_state is not None:
            write_ha_state()
        async_dispatch_runtime_update(hass, entry_id)

    return result


__all__ = ["async_mutate_runtime"]

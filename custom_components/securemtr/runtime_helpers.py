"""Runtime helpers for Secure Meters entities."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, TypeVar

from aiohttp import ClientWebSocketResponse
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .beanbag import BeanbagBackend, BeanbagError, BeanbagSession, WeeklyProgram

if TYPE_CHECKING:
    from . import SecuremtrController, SecuremtrRuntimeData

_ResultT = TypeVar("_ResultT")

OperationCallable = Callable[
    [
        BeanbagBackend,
        BeanbagSession,
        ClientWebSocketResponse,
        "SecuremtrController",
    ],
    Awaitable[_ResultT],
]

MutationCallable = Callable[["SecuremtrRuntimeData"], Awaitable[None] | None]


_LOGGER = logging.getLogger(__name__)


async def async_execute_controller_command(
    runtime: "SecuremtrRuntimeData",
    entry: ConfigEntry,
    operation: OperationCallable,
    **kwargs: object,
) -> _ResultT:
    """Delegate controller commands to the integration runtime helper."""

    from . import async_execute_controller_command as package_executor

    return await package_executor(runtime, entry, operation, **kwargs)


def async_dispatch_runtime_update(hass: HomeAssistant, entry_id: str) -> None:
    """Dispatch runtime updates via the integration signal helper."""

    from . import async_dispatch_runtime_update as package_dispatch

    package_dispatch(hass, entry_id)


async def async_mutate_runtime(
    runtime: "SecuremtrRuntimeData",
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


async def async_read_zone_program(
    backend: BeanbagBackend,
    session: BeanbagSession,
    websocket: ClientWebSocketResponse,
    *,
    gateway_id: str,
    zone: str,
    entry_identifier: str,
) -> WeeklyProgram | None:
    """Fetch the weekly program for a zone, returning None on failure."""

    try:
        return await backend.read_weekly_program(
            session,
            websocket,
            gateway_id,
            zone=zone,
        )
    except BeanbagError as error:
        _LOGGER.error(
            "Failed to fetch %s weekly program for %s: %s",
            zone,
            entry_identifier,
            error,
        )
    except Exception:
        _LOGGER.exception(
            "Unexpected error while fetching %s weekly program for %s",
            zone,
            entry_identifier,
        )
    return None


__all__ = ["async_mutate_runtime", "async_read_zone_program"]

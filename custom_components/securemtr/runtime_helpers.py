"""Runtime helpers for Secure Meters entities."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any, TYPE_CHECKING, TypeVar, cast

from aiohttp import ClientWebSocketResponse
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .beanbag import BeanbagBackend, BeanbagError, BeanbagSession, WeeklyProgram
from .schedule import canonicalize_weekly
from .zones import ZONE_METADATA

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


async def async_read_zone_programs(
    backend: BeanbagBackend,
    session: BeanbagSession,
    websocket: ClientWebSocketResponse,
    *,
    gateway_id: str,
    entry_identifier: str,
) -> tuple[dict[str, WeeklyProgram | None], dict[str, list[tuple[int, int]] | None]]:
    """Fetch weekly programs for each zone and their canonical forms."""

    programs: dict[str, WeeklyProgram | None] = {}
    canonicals: dict[str, list[tuple[int, int]] | None] = {}

    for zone_key in ZONE_METADATA:
        program = await async_read_zone_program(
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


def controller_gateway_operation(
    method_name: str, /, **operation_kwargs: Any
) -> OperationCallable[_ResultT]:
    """Return an operation invoking a backend method with the controller gateway."""

    async def _operation(
        backend: BeanbagBackend,
        session: BeanbagSession,
        websocket: ClientWebSocketResponse,
        controller: "SecuremtrController",
    ) -> _ResultT:
        backend_method = cast(
            Callable[..., Awaitable[_ResultT]], getattr(backend, method_name)
        )
        return await backend_method(
            session,
            websocket,
            controller.gateway_id,
            **operation_kwargs,
        )

    return _operation


__all__ = [
    "async_mutate_runtime",
    "async_read_zone_program",
    "async_read_zone_programs",
    "controller_gateway_operation",
]

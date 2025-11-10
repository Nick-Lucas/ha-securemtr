"""Shared entity helpers for the Secure Meters integration."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import sys
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.dispatcher import async_dispatcher_connect

from . import (
    DEFAULT_DEVICE_LABEL,
    DOMAIN,
    SecuremtrController,
    SecuremtrRuntimeData,
    runtime_update_signal,
)

_CONTROLLER_READY_TIMEOUT = 15.0


async def async_get_ready_controller(
    hass: HomeAssistant, entry: ConfigEntry
) -> tuple[SecuremtrRuntimeData, SecuremtrController]:
    """Return runtime context and controller metadata once ready."""

    runtime: SecuremtrRuntimeData = hass.data[DOMAIN][entry.entry_id]

    try:
        await asyncio.wait_for(
            runtime.controller_ready.wait(), _CONTROLLER_READY_TIMEOUT
        )
    except TimeoutError as error:
        raise HomeAssistantError(
            "Timed out waiting for Secure Meters controller metadata"
        ) from error

    controller = runtime.controller
    if controller is None:
        raise HomeAssistantError("Secure Meters controller metadata was not available")

    return runtime, controller


def slugify_identifier(identifier: str) -> str:
    """Convert a controller identifier into a slug for unique IDs."""

    return (
        "".join(ch.lower() if ch.isalnum() else "_" for ch in identifier).strip("_")
        or DOMAIN
    )


def build_device_info(controller: SecuremtrController) -> DeviceInfo:
    """Construct device registry metadata for the provided controller."""

    serial_identifier = controller.serial_number or controller.identifier
    device_name = DEFAULT_DEVICE_LABEL
    return DeviceInfo(
        identifiers={(DOMAIN, serial_identifier)},
        manufacturer="Secure Meters",
        model=controller.model or "E7+",
        name=device_name,
        sw_version=controller.firmware_version,
        serial_number=controller.serial_number,
    )


class SecuremtrRuntimeEntityMixin:
    """Provide shared runtime helpers for Secure Meters entities."""

    _attr_should_poll = False
    _attr_has_entity_name = True

    def __init__(
        self,
        runtime: SecuremtrRuntimeData,
        controller: SecuremtrController,
        *,
        entry: ConfigEntry | None = None,
        entry_id: str | None = None,
    ) -> None:
        """Initialise the entity with runtime context and dispatcher hooks."""

        super().__init__()
        self._runtime = runtime
        self._controller = controller
        self._entry: ConfigEntry | None = entry
        if entry is not None:
            self._entry_id = entry.entry_id
        elif entry_id is not None:
            self._entry_id = entry_id
        else:
            raise ValueError(  # pragma: no cover - defensive validation
                "Either entry or entry_id must be provided for SecureMTR entities"
            )

    @property
    def available(self) -> bool:
        """Return whether the controller metadata is currently available."""

        return self._runtime_connected()

    def _runtime_connected(self) -> bool:
        """Return whether the backend runtime currently exposes controller state."""

        return (
            self._runtime.websocket is not None and self._runtime.controller is not None
        )

    async def async_added_to_hass(self) -> None:
        """Register dispatcher callbacks when the entity is added to Home Assistant."""

        await super().async_added_to_hass()
        hass = self.hass
        if hass is None:
            return

        connector = self._resolve_dispatcher_connect()
        remove = connector(
            hass, runtime_update_signal(self._entry_id), self.async_write_ha_state
        )
        self.async_on_remove(remove)

    def _resolve_dispatcher_connect(
        self,
    ) -> Callable[[Any, str, Callable[..., Any]], Callable[[], None]]:
        """Return the dispatcher connect helper, honouring per-module overrides."""

        module = sys.modules.get(self.__module__)
        if module is not None:
            connector = getattr(module, "async_dispatcher_connect", None)
            if callable(connector):
                return connector

        return async_dispatcher_connect  # pragma: no cover - fallback path

    @property
    def device_info(self) -> DeviceInfo:
        """Return device registry information for the associated controller."""

        return build_device_info(self._controller)

    def _identifier_slug(self) -> str:
        """Return the slugified identifier for the controller."""

        controller = self._controller
        identifier = controller.serial_number or controller.identifier
        return slugify_identifier(identifier)

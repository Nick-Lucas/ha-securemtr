"""Binary sensors for Secure Meters timed boost state."""

from __future__ import annotations

import asyncio
import logging
from typing import cast

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import DOMAIN, SecuremtrController, SecuremtrRuntimeData
from .entity import (
    SecuremtrRuntimeEntityMixin,
    async_dispatcher_connect as _async_dispatcher_connect,
)

async_dispatcher_connect = _async_dispatcher_connect

_LOGGER = logging.getLogger(__name__)

_CONTROLLER_WAIT_TIMEOUT = 15.0


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the Secure Meters boost binary sensor."""

    runtime: SecuremtrRuntimeData = hass.data[DOMAIN][entry.entry_id]

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

    async_add_entities([SecuremtrBoostActiveBinarySensor(runtime, controller, entry)])


class SecuremtrBoostActiveBinarySensor(
    SecuremtrRuntimeEntityMixin, BinarySensorEntity
):
    """Indicate whether a timed boost run is currently active."""

    _attr_device_class = BinarySensorDeviceClass.RUNNING

    def __init__(
        self,
        runtime: SecuremtrRuntimeData,
        controller: SecuremtrController,
        entry: ConfigEntry | str,
    ) -> None:
        """Initialise the boost active sensor."""

        if hasattr(entry, "entry_id"):
            super().__init__(runtime, controller, entry=cast(ConfigEntry, entry))
        else:
            super().__init__(runtime, controller, entry_id=str(entry))
        self._attr_unique_id = f"{self._identifier_slug()}_boost_active"
        self._attr_translation_key = "boost_active"

    @property
    def is_on(self) -> bool:
        """Return whether the timed boost is active."""

        return self._runtime.timed_boost_active is True


__all__ = ["SecuremtrBoostActiveBinarySensor"]

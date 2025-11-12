"""Binary sensors for Secure Meters timed boost state."""

from __future__ import annotations

import logging

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import DOMAIN, SecuremtrController, SecuremtrRuntimeData
from .entity import SecuremtrRuntimeEntityMixin, async_get_ready_controller

_LOGGER = logging.getLogger(__name__)


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
        return

    async_add_entities([SecuremtrBoostActiveBinarySensor(runtime, controller, entry)])


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


__all__ = ["SecuremtrBoostActiveBinarySensor"]

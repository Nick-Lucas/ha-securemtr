"""Switch entities for the Secure Meters water heater controller."""

from __future__ import annotations

import logging

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import (
    SecuremtrController,
    SecuremtrRuntimeData,
    async_dispatch_runtime_update,
    async_run_with_reconnect,
)
from .beanbag import BeanbagError
from .entity import (
    SecuremtrRuntimeEntityMixin,
    async_dispatcher_connect as _async_dispatcher_connect,
    async_get_ready_controller,
)

async_dispatcher_connect = _async_dispatcher_connect

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Secure Meters switch entities for a config entry."""

    runtime, controller = await async_get_ready_controller(hass, entry)

    async_add_entities(
        [
            SecuremtrPowerSwitch(runtime, controller, entry),
            SecuremtrTimedBoostSwitch(runtime, controller, entry),
        ]
    )


class SecuremtrPowerSwitch(SecuremtrRuntimeEntityMixin, SwitchEntity):
    """Represent a maintained power toggle for the Secure Meters controller."""

    def __init__(
        self,
        runtime: SecuremtrRuntimeData,
        controller: SecuremtrController,
        entry: ConfigEntry,
    ) -> None:
        """Initialise the switch entity with runtime context."""

        super().__init__(runtime, controller, entry=entry)
        self._attr_unique_id = f"{self._identifier_slug()}_primary_power"
        self._attr_translation_key = "controller_power"

    @property
    def is_on(self) -> bool:
        """Return whether the controller reports the primary power as on."""

        return self._runtime.primary_power_on is True

    async def async_turn_on(self, **kwargs: object) -> None:
        """Send an on command to the Secure Meters controller."""

        await self._async_set_power_state(True)

    async def async_turn_off(self, **kwargs: object) -> None:
        """Send an off command to the Secure Meters controller."""

        await self._async_set_power_state(False)

    async def _async_set_power_state(self, turn_on: bool) -> None:
        """Drive the backend to the requested primary power state."""

        runtime = self._runtime
        controller = runtime.controller

        if controller is None:
            raise HomeAssistantError("Secure Meters controller is not connected")

        entry = self._entry
        async with runtime.command_lock:
            try:
                await async_run_with_reconnect(
                    entry,
                    runtime,
                    (
                        lambda backend, session, websocket: backend.turn_controller_on(
                            session,
                            websocket,
                            controller.gateway_id,
                        )
                        if turn_on
                        else backend.turn_controller_off(
                            session,
                            websocket,
                            controller.gateway_id,
                        )
                    ),
                )
            except BeanbagError as error:
                _LOGGER.error("Failed to toggle Secure Meters controller: %s", error)
                raise HomeAssistantError(
                    "Failed to toggle Secure Meters controller"
                ) from error

            runtime.primary_power_on = turn_on

        hass = self.hass
        if hass is None:
            return

        self.async_write_ha_state()
        async_dispatch_runtime_update(hass, self._entry_id)


class SecuremtrTimedBoostSwitch(SecuremtrRuntimeEntityMixin, SwitchEntity):
    """Expose the timed boost feature toggle reported by Beanbag."""

    def __init__(
        self,
        runtime: SecuremtrRuntimeData,
        controller: SecuremtrController,
        entry: ConfigEntry,
    ) -> None:
        """Initialise the timed boost switch for the controller."""

        super().__init__(runtime, controller, entry=entry)
        self._attr_unique_id = f"{self._identifier_slug()}_timed_boost"
        self._attr_translation_key = "timed_boost"

    @property
    def is_on(self) -> bool:
        """Return whether timed boost is currently enabled."""

        return self._runtime.timed_boost_enabled is True

    async def async_turn_on(self, **kwargs: object) -> None:
        """Enable the timed boost feature in the backend."""

        await self._async_set_timed_boost(True)

    async def async_turn_off(self, **kwargs: object) -> None:
        """Disable the timed boost feature in the backend."""

        await self._async_set_timed_boost(False)

    async def _async_set_timed_boost(self, enabled: bool) -> None:
        """Drive the backend to the requested timed boost state."""

        runtime = self._runtime
        controller = runtime.controller

        if controller is None:
            raise HomeAssistantError("Secure Meters controller is not connected")

        entry = self._entry
        async with runtime.command_lock:
            try:
                await async_run_with_reconnect(
                    entry,
                    runtime,
                    lambda backend, session, websocket: backend.set_timed_boost_enabled(
                        session,
                        websocket,
                        controller.gateway_id,
                        enabled=enabled,
                    ),
                )
            except BeanbagError as error:
                _LOGGER.error(
                    "Failed to toggle Secure Meters timed boost feature: %s", error
                )
                raise HomeAssistantError(
                    "Failed to toggle Secure Meters timed boost feature"
                ) from error

            runtime.timed_boost_enabled = enabled

        hass = self.hass
        if hass is None:
            return

        self.async_write_ha_state()
        async_dispatch_runtime_update(hass, self._entry_id)

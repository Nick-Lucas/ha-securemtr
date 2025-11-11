"""Switch entities for the Secure Meters water heater controller."""

from __future__ import annotations

import logging

from aiohttp import ClientWebSocketResponse
from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import SecuremtrController, SecuremtrRuntimeData
from .beanbag import BeanbagBackend, BeanbagSession
from .entity import SecuremtrRuntimeEntityMixin, async_get_ready_controller

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

        super().__init__(runtime, controller, entry)
        self._set_slug_identifiers("primary_power")
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

        async def _call_operation(
            backend: BeanbagBackend,
            session: BeanbagSession,
            websocket: ClientWebSocketResponse,
            controller: SecuremtrController,
        ) -> None:
            if turn_on:
                await backend.turn_controller_on(
                    session, websocket, controller.gateway_id
                )
                return

            await backend.turn_controller_off(session, websocket, controller.gateway_id)

        await self._async_mutate(
            operation=_call_operation,
            mutation=lambda data: self._apply_power_state(data, turn_on),
            log_context="Failed to toggle Secure Meters controller",
            write_state=True,
        )

    @staticmethod
    def _apply_power_state(
        runtime: SecuremtrRuntimeData,
        turn_on: bool,
    ) -> None:
        """Update runtime state after toggling the controller power."""

        runtime.primary_power_on = turn_on


class SecuremtrTimedBoostSwitch(SecuremtrRuntimeEntityMixin, SwitchEntity):
    """Expose the timed boost feature toggle reported by Beanbag."""

    def __init__(
        self,
        runtime: SecuremtrRuntimeData,
        controller: SecuremtrController,
        entry: ConfigEntry,
    ) -> None:
        """Initialise the timed boost switch for the controller."""

        super().__init__(runtime, controller, entry)
        self._set_slug_identifiers("timed_boost")
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

        await self._async_mutate(
            operation=lambda backend,
            session,
            websocket,
            controller: backend.set_timed_boost_enabled(
                session,
                websocket,
                controller.gateway_id,
                enabled=enabled,
            ),
            mutation=lambda data: self._apply_timed_boost_state(data, enabled),
            log_context="Failed to toggle Secure Meters timed boost feature",
            write_state=True,
        )

    @staticmethod
    def _apply_timed_boost_state(
        runtime: SecuremtrRuntimeData,
        enabled: bool,
    ) -> None:
        """Update runtime state after toggling timed boost."""

        runtime.timed_boost_enabled = enabled

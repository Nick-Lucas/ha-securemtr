"""Switch entities for the Secure Meters water heater controller."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable
import logging
from typing import Any

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import DOMAIN, SecuremtrController, SecuremtrRuntimeData
from .entity import SecuremtrCommandMixin, async_get_ready_controller

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Secure Meters switch entities for a config entry."""

    entry_label = getattr(entry, "title", None) or getattr(entry, "entry_id", DOMAIN)

    try:
        runtime, controller = await async_get_ready_controller(hass, entry)
    except HomeAssistantError as error:
        _LOGGER.warning(
            "Skipping SecureMTR switch setup for %s: %s",
            entry_label,
            error,
        )
        return

    async_add_entities(
        [
            SecuremtrPowerSwitch(runtime, controller, entry),
            SecuremtrTimedBoostSwitch(runtime, controller, entry),
        ]
    )


class SecuremtrToggleSwitch(SecuremtrCommandMixin, SwitchEntity, ABC):
    """Provide a reusable toggle helper for Secure Meters switches."""

    def __init__(
        self,
        runtime: SecuremtrRuntimeData,
        controller: SecuremtrController,
        entry: ConfigEntry,
        *,
        slug_suffix: str,
        translation_key: str,
        log_context: str,
        write_state: bool = True,
    ) -> None:
        """Initialise the toggle switch with shared runtime context."""

        super().__init__(runtime, controller, entry)
        self._set_slug_identifiers(slug_suffix)
        self._attr_translation_key = translation_key
        self._toggle_log_context = log_context
        self._toggle_write_state = write_state

    @property
    def is_on(self) -> bool:
        """Return the current on/off state reported by the runtime."""

        return self._current_state()

    async def async_turn_on(self, **kwargs: object) -> None:
        """Request that the backend enable the associated feature."""

        await self._async_toggle(True)

    async def async_turn_off(self, **kwargs: object) -> None:
        """Request that the backend disable the associated feature."""

        await self._async_toggle(False)

    async def _async_toggle(self, turn_on: bool) -> None:
        """Execute the configured backend command for the desired state."""

        method_name, kwargs = self._backend_command(turn_on)
        await self._async_controller_command(
            method_name,
            runtime_update=lambda data: self._apply_runtime_state(data, turn_on),
            log_context=self._toggle_log_context,
            write_state=self._toggle_write_state,
            **kwargs,
        )

    @abstractmethod
    def _current_state(self) -> bool:
        """Return the current toggle state from the runtime."""

    @abstractmethod
    def _backend_command(self, turn_on: bool) -> tuple[str, dict[str, Any]]:
        """Return the backend method and kwargs for the desired state."""

    @abstractmethod
    def _apply_runtime_state(
        self, runtime: SecuremtrRuntimeData, turn_on: bool
    ) -> Awaitable[None] | None:
        """Mutate the runtime after a successful backend toggle."""


class SecuremtrPowerSwitch(SecuremtrToggleSwitch):
    """Represent a maintained power toggle for the Secure Meters controller."""

    def __init__(
        self,
        runtime: SecuremtrRuntimeData,
        controller: SecuremtrController,
        entry: ConfigEntry,
    ) -> None:
        """Initialise the switch entity with runtime context."""

        super().__init__(
            runtime,
            controller,
            entry,
            slug_suffix="primary_power",
            translation_key="controller_power",
            log_context="Failed to toggle Secure Meters controller",
        )

    def _current_state(self) -> bool:
        """Return whether the controller reports the primary power as on."""

        return self._runtime.primary_power_on is True

    def _backend_command(self, turn_on: bool) -> tuple[str, dict[str, Any]]:
        """Return the backend command to drive the requested power state."""

        method_name = "turn_controller_on" if turn_on else "turn_controller_off"
        return method_name, {}

    def _apply_runtime_state(
        self, runtime: SecuremtrRuntimeData, turn_on: bool
    ) -> None:
        """Update runtime state after toggling the controller power."""

        runtime.primary_power_on = turn_on


class SecuremtrTimedBoostSwitch(SecuremtrToggleSwitch):
    """Expose the timed boost feature toggle reported by Beanbag."""

    def __init__(
        self,
        runtime: SecuremtrRuntimeData,
        controller: SecuremtrController,
        entry: ConfigEntry,
    ) -> None:
        """Initialise the timed boost switch for the controller."""

        super().__init__(
            runtime,
            controller,
            entry,
            slug_suffix="timed_boost",
            translation_key="timed_boost",
            log_context="Failed to toggle Secure Meters timed boost feature",
        )

    def _current_state(self) -> bool:
        """Return whether timed boost is currently enabled."""

        return self._runtime.timed_boost_enabled is True

    def _backend_command(self, turn_on: bool) -> tuple[str, dict[str, Any]]:
        """Return the backend command to drive the requested timed boost state."""

        return "set_timed_boost_enabled", {"enabled": turn_on}

    def _apply_runtime_state(
        self, runtime: SecuremtrRuntimeData, turn_on: bool
    ) -> None:
        """Update runtime state after toggling timed boost."""

        runtime.timed_boost_enabled = turn_on

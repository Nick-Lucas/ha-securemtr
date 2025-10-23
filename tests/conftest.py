"""Global pytest configuration for securemtr integration tests."""

from __future__ import annotations

import importlib
import sys

import types
import warnings
from typing import Any


def _install_http_stub() -> None:
    """Provide a minimal stub for Home Assistant's HTTP component."""

    if "homeassistant.components.http" in sys.modules:
        return

    components = importlib.import_module("homeassistant.components")

    http_module = types.ModuleType("homeassistant.components.http")
    http_module.KEY_HASS = "hass"
    http_module.KEY_AUTHENTICATED = "authenticated"
    http_module.KEY_REAL_IP = "real_ip"

    class HomeAssistantView:  # pragma: no cover - trivial stub container
        """Serve as a stand-in for the Home Assistant HTTP view base class."""

        url: str | None = None
        name: str | None = None
        requires_auth: bool = True

        async def get(self, request: Any) -> Any:
            """Handle GET requests for the stub view."""

            raise NotImplementedError

    http_module.HomeAssistantView = HomeAssistantView
    http_module.__all__ = [
        "KEY_HASS",
        "KEY_AUTHENTICATED",
        "KEY_REAL_IP",
        "HomeAssistantView",
    ]

    ban_module = types.ModuleType("homeassistant.components.http.ban")

    def process_success_login(*_args: Any, **_kwargs: Any) -> None:
        """Record a successful login attempt."""

    def process_wrong_login(*_args: Any, **_kwargs: Any) -> None:
        """Record an unsuccessful login attempt."""

    ban_module.process_success_login = process_success_login
    ban_module.process_wrong_login = process_wrong_login

    setattr(http_module, "ban", ban_module)
    setattr(components, "http", http_module)
    sys.modules["homeassistant.components.http"] = http_module
    sys.modules["homeassistant.components.http.ban"] = ban_module


_install_http_stub()

warnings.filterwarnings(
    "error",
    category=DeprecationWarning,
    module=r"^custom_components\\.securemtr",
)
warnings.filterwarnings(
    "once",
    "Inheritance class HomeAssistantApplication from web.Application is discouraged",
    category=DeprecationWarning,
    module=r"^aiohttp\\.web_app",
)

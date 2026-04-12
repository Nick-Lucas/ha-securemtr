"""Tests for local BLE commissioning helpers."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any
import sys
from unittest.mock import AsyncMock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from custom_components.securemtr.local_ble_commissioning import (
    FINAL_PACKET_INDEX,
    LocalBleCommissioningError,
    _async_commission_over_rpc,
    _format_mac_address,
    _packetize_payload,
    _reassemble_payload,
    async_commission_local_ble,
)


def test_packetize_payload_includes_final_packet_for_exact_multiple() -> None:
    """Ensure framing includes a final packet even for exact 19-byte multiples."""

    payload = bytes(range(38))
    packets = _packetize_payload(payload)

    assert len(packets) == 3
    assert packets[0][0] == 1
    assert packets[1][0] == 2
    assert packets[2][0] == FINAL_PACKET_INDEX
    assert packets[2][1:] == b""


def test_reassemble_payload_uses_prefixed_length() -> None:
    """Ensure payload reconstruction respects the 2-byte length prefix."""

    body = b'{"R":"ok"}'
    framed = len(body).to_bytes(2, byteorder="big") + body
    packets = {
        1: framed[:5],
        FINAL_PACKET_INDEX: framed[5:],
    }

    assert _reassemble_payload(packets) == body


def test_reassemble_payload_rejects_missing_final_packet() -> None:
    """Ensure reconstruction rejects responses missing packet 255."""

    with pytest.raises(LocalBleCommissioningError):
        _reassemble_payload({1: b"abcd"})


def test_format_mac_address_colonizes_compact_mac() -> None:
    """Ensure compact MAC values are transformed to canonical BLE address form."""

    assert _format_mac_address("AABBCCDDEEFF") == "AA:BB:CC:DD:EE:FF"


@pytest.mark.asyncio
async def test_async_commission_over_rpc_requests_owner_then_ble_key() -> None:
    """Ensure commissioning sends owner request before BLE key fetch."""

    calls: list[dict[str, Any]] = []

    class FakeRpcClient:
        async def async_rpc_request(self, **kwargs: Any) -> Any:
            calls.append(kwargs)
            if len(calls) == 1:
                return "owner-token"
            return "base64-ble-key"

    credentials = await _async_commission_over_rpc(
        FakeRpcClient(),
        gateway_mac_id="12345",
        serial_number="A1B2C3D4",
        owner_email="homeassistant@local",
    )

    assert len(calls) == 2
    assert calls[0]["handler_id"] == 7
    assert calls[0]["service_id"] == 151
    assert calls[1]["handler_id"] == 50
    assert calls[1]["service_id"] == 11
    assert credentials["local_owner_token"] == "owner-token"
    assert credentials["local_ble_key"] == "base64-ble-key"


@pytest.mark.asyncio
async def test_async_commission_over_rpc_rejects_missing_ble_key() -> None:
    """Ensure commissioning fails if gateway does not return BLE key."""

    class FakeRpcClient:
        async def async_rpc_request(self, **kwargs: Any) -> Any:
            if kwargs["handler_id"] == 7:
                return "owner-token"
            return ""

    with pytest.raises(LocalBleCommissioningError):
        await _async_commission_over_rpc(
            FakeRpcClient(),
            gateway_mac_id="12345",
            serial_number="A1B2C3D4",
            owner_email="homeassistant@local",
        )


@pytest.mark.asyncio
async def test_async_commission_local_ble_orchestrates_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure local BLE commissioning executes resolve, connect, and RPC stages."""

    fake_hass = SimpleNamespace()
    fake_device = SimpleNamespace(address="AA:BB:CC:DD:EE:FF")
    fake_client = SimpleNamespace()

    class FakeRpcClient:
        def __init__(self, client: Any) -> None:
            self.client = client

        async def async_initialize(self) -> None:
            return None

        async def async_close(self) -> None:
            return None

    resolve = AsyncMock(return_value=fake_device)
    connect = AsyncMock(return_value=fake_client)
    commission = AsyncMock(
        return_value={
            "local_ble_key": "base64-ble-key",
            "local_owner_token": "owner-token",
        }
    )
    disconnect = AsyncMock()

    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._async_resolve_connectable_device",
        resolve,
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._async_connect_client",
        connect,
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._async_commission_over_rpc",
        commission,
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._async_disconnect_client",
        disconnect,
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._BleUartRpcClient",
        FakeRpcClient,
    )

    credentials = await async_commission_local_ble(
        fake_hass,
        serial_number="A1B2C3D4",
        mac_address="AABBCCDDEEFF",
        device_type="e7plus",
    )

    assert credentials["local_ble_key"] == "base64-ble-key"
    resolve.assert_awaited_once_with(fake_hass, "AA:BB:CC:DD:EE:FF")
    connect.assert_awaited_once()
    assert connect.await_args.args == (fake_device,)
    assert "ble_device_callback" in connect.await_args.kwargs
    commission.assert_awaited_once()
    disconnect.assert_awaited_once_with(fake_client)


@pytest.mark.asyncio
async def test_async_commission_local_ble_rejects_unknown_device_type() -> None:
    """Ensure commissioning only accepts E7+ device type for now."""

    with pytest.raises(LocalBleCommissioningError):
        await async_commission_local_ble(
            SimpleNamespace(),
            serial_number="A1B2C3D4",
            mac_address="AABBCCDDEEFF",
            device_type="receiver4",
        )

"""Local BLE commissioning transport and RPC helpers."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import json
import logging
import random
import time
from typing import Any
from uuid import UUID

from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError

try:  # pragma: no cover - depends on runtime environment
    from bleak import BleakClient
    from bleak.exc import BleakError
except ImportError:  # pragma: no cover - depends on runtime environment
    BleakClient = None

    class BleakError(Exception):
        """Fallback Bleak error type when bleak is unavailable."""


try:  # pragma: no cover - depends on runtime environment
    from bleak_retry_connector import establish_connection
except ImportError:  # pragma: no cover - depends on runtime environment
    establish_connection = None


_LOGGER = logging.getLogger(__name__)

UART_SERVICE_UUID = UUID("b973f2e0-b19e-11e2-9e96-0800200c9a66")
UART_RX_UUID = UUID("da73f3e1-b19e-11e2-9e96-0800200c9a66")
UART_TX_UUID = UUID("e973f2e2-b19e-11e2-9e96-0800200c9a66")

PACKET_SIZE = 19
FINAL_PACKET_INDEX = 255
BLE_DISCOVERY_TIMEOUT_SECONDS = 20.0
BLE_CONNECT_TIMEOUT_SECONDS = 15.0
BLE_REQUEST_TIMEOUT_SECONDS = 15.0

SERVICE_ID_HAN_MANAGEMENT = 11
SERVICE_ID_COMMS_MANAGER = 151
HANDLER_SET_OWNER_IN_RECEIVER = 7
HANDLER_GET_BLE_KEY = 50

DEFAULT_OWNER_EMAIL = "homeassistant@local"


class LocalBleCommissioningError(HomeAssistantError):
    """Raised when local BLE commissioning cannot complete."""


class _BleUartRpcClient:
    """Manage packetized UART-over-BLE RPC exchanges."""

    def __init__(self, client: Any) -> None:
        """Store the BLE client and initialize response state."""

        self._client = client
        self._loop = asyncio.get_running_loop()
        self._response_packets: dict[int, bytes] = {}
        self._response_event = asyncio.Event()

    async def async_initialize(self) -> None:
        """Validate service characteristics and subscribe to TX notifications."""

        services = getattr(self._client, "services", None)
        if services is not None:
            service = services.get_service(UART_SERVICE_UUID)
            if service is None:
                raise LocalBleCommissioningError("SecureMTR UART service was not found")
            if service.get_characteristic(UART_RX_UUID) is None:
                raise LocalBleCommissioningError(
                    "SecureMTR UART RX characteristic missing"
                )
            if service.get_characteristic(UART_TX_UUID) is None:
                raise LocalBleCommissioningError(
                    "SecureMTR UART TX characteristic missing"
                )

        await self._client.start_notify(UART_TX_UUID, self._notification_callback)

    async def async_close(self) -> None:
        """Stop notifications when possible."""

        try:
            await self._client.stop_notify(UART_TX_UUID)
        except (BleakError, RuntimeError, OSError):
            _LOGGER.debug("Unable to stop TX notifications", exc_info=True)

    async def async_rpc_request(
        self,
        *,
        gateway_mac_id: str,
        handler_id: int,
        service_id: int,
        args: list[Any] | None,
        timeout: float = BLE_REQUEST_TIMEOUT_SECONDS,
    ) -> Any:
        """Send one RPC request and return the response result payload."""

        request_id = f"{int(time.time())}-{random.randint(1, 2_147_483_647)}"
        request_payload: dict[str, Any] = {
            "V": "1.0",
            "DTS": int(time.time()),
            "I": request_id,
            "M": "Request",
            "P": [
                {
                    "GMI": gateway_mac_id,
                    "HI": handler_id,
                    "SI": service_id,
                }
            ],
        }
        if args is not None:
            request_payload["P"].append(args)

        response = await self._async_exchange(request_payload, timeout=timeout)
        if response.get("I") != request_id:
            raise LocalBleCommissioningError("RPC response ID mismatch")

        error_data = response.get("E")
        if isinstance(error_data, dict):
            error_code = error_data.get("C")
            error_message = error_data.get("M")
            raise LocalBleCommissioningError(
                f"Gateway returned RPC error {error_code}: {error_message}"
            )

        if "R" not in response:
            raise LocalBleCommissioningError("RPC response did not include a result")

        return response["R"]

    async def _async_exchange(
        self,
        request_payload: dict[str, Any],
        *,
        timeout: float,
    ) -> dict[str, Any]:
        """Write packetized JSON and wait for the packetized response."""

        self._response_packets.clear()
        self._response_event.clear()

        request_json = json.dumps(request_payload, separators=(",", ":"))
        request_bytes = request_json.encode("utf-8")
        framed_request = len(request_bytes).to_bytes(2, byteorder="big") + request_bytes

        for packet in _packetize_payload(framed_request):
            await self._client.write_gatt_char(UART_RX_UUID, packet, response=True)

        try:
            await asyncio.wait_for(self._response_event.wait(), timeout=timeout)
        except TimeoutError as error:
            raise LocalBleCommissioningError(
                "Timed out waiting for BLE response"
            ) from error

        response_body = _reassemble_payload(self._response_packets)
        try:
            response = json.loads(response_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise LocalBleCommissioningError(
                "Failed to parse BLE JSON response"
            ) from error

        if not isinstance(response, dict):
            raise LocalBleCommissioningError("BLE JSON response is not an object")
        return response

    def _notification_callback(self, _characteristic: Any, data: bytearray) -> None:
        """Capture incoming packetized data from TX notifications."""

        packet = bytes(data)
        self._loop.call_soon_threadsafe(self._handle_packet, packet)

    def _handle_packet(self, packet: bytes) -> None:
        """Store one response packet and signal completion when final arrives."""

        if not packet:
            return
        packet_index = packet[0]
        if packet_index not in self._response_packets:
            self._response_packets[packet_index] = packet[1:]
        if packet_index == FINAL_PACKET_INDEX:
            self._response_event.set()


def _packetize_payload(payload: bytes) -> list[bytes]:
    """Split payload into UART packets with packet index prefixes."""

    packets: list[bytes] = []
    total_packets = (len(payload) // PACKET_SIZE) + 1
    offset = 0

    for packet_number in range(1, total_packets + 1):
        if packet_number < total_packets:
            chunk = payload[offset : offset + PACKET_SIZE]
            packets.append(bytes([packet_number]) + chunk)
            offset += PACKET_SIZE
        else:
            packets.append(bytes([FINAL_PACKET_INDEX]) + payload[offset:])

    return packets


def _reassemble_payload(response_packets: dict[int, bytes]) -> bytes:
    """Reassemble packetized payload and strip the 2-byte length prefix."""

    if FINAL_PACKET_INDEX not in response_packets:
        raise LocalBleCommissioningError("Response did not include the final packet")

    assembled = b"".join(response_packets[index] for index in sorted(response_packets))
    if len(assembled) < 2:
        raise LocalBleCommissioningError("Response was shorter than the length prefix")

    payload_length = int.from_bytes(assembled[:2], byteorder="big")
    payload = assembled[2 : 2 + payload_length]
    if len(payload) != payload_length:
        raise LocalBleCommissioningError("Response payload length mismatch")

    return payload


def _format_mac_address(mac_address: str) -> str:
    """Convert compact uppercase MAC into colon-separated canonical form."""

    return ":".join(
        mac_address[index : index + 2] for index in range(0, len(mac_address), 2)
    )


async def _async_resolve_connectable_device(hass: HomeAssistant, address: str) -> Any:
    """Resolve a connectable BLEDevice via Home Assistant's shared scanner."""

    from homeassistant.components import bluetooth  # noqa: PLC0415

    scanner_count = bluetooth.async_scanner_count(hass, connectable=True)
    if scanner_count <= 0:
        raise LocalBleCommissioningError(
            "No connectable Bluetooth adapters are available in Home Assistant"
        )

    ble_device = bluetooth.async_ble_device_from_address(
        hass, address, connectable=True
    )
    if ble_device is not None:
        return ble_device

    _LOGGER.info("Waiting for BLE advertisement from %s", address)

    def _process_advertisement(service_info: Any) -> bool:
        return service_info.address.upper() == address.upper()

    try:
        await bluetooth.async_process_advertisements(
            hass,
            _process_advertisement,
            {"address": address, "connectable": True},
            bluetooth.BluetoothScanningMode.ACTIVE,
            BLE_DISCOVERY_TIMEOUT_SECONDS,
        )
    except TimeoutError as error:
        raise LocalBleCommissioningError(
            f"Device {address} was not discovered within {BLE_DISCOVERY_TIMEOUT_SECONDS:.0f}s"
        ) from error

    ble_device = bluetooth.async_ble_device_from_address(
        hass, address, connectable=True
    )
    if ble_device is None:
        raise LocalBleCommissioningError(
            f"Device {address} is not reachable from a connectable adapter"
        )

    return ble_device


async def _async_connect_client(
    ble_device: Any,
    *,
    ble_device_callback: Callable[[], Any] | None = None,
) -> Any:
    """Connect to the BLE device and return a connected BleakClient."""

    if BleakClient is None:
        raise LocalBleCommissioningError("Bleak is not available in this environment")

    if establish_connection is not None:
        try:
            return await establish_connection(
                BleakClient,
                ble_device,
                ble_device.address,
                max_attempts=4,
                ble_device_callback=ble_device_callback,
                timeout=BLE_CONNECT_TIMEOUT_SECONDS,
            )
        except Exception as error:
            raise LocalBleCommissioningError(
                f"Could not establish BLE connection to {ble_device.address}"
            ) from error

    client = BleakClient(ble_device, timeout=BLE_CONNECT_TIMEOUT_SECONDS)
    try:
        connected = await client.connect()
    except Exception as error:
        raise LocalBleCommissioningError(
            f"Failed to connect to BLE device {ble_device.address}"
        ) from error

    if not connected:
        raise LocalBleCommissioningError(
            f"Could not establish BLE connection to {ble_device.address}"
        )

    return client


async def _async_disconnect_client(client: Any) -> None:
    """Disconnect BLE client safely."""

    try:
        await client.disconnect()
    except (BleakError, RuntimeError, OSError):
        _LOGGER.debug("Failed to disconnect BLE client cleanly", exc_info=True)


async def _async_commission_over_rpc(
    rpc_client: _BleUartRpcClient,
    *,
    gateway_mac_id: str,
    serial_number: str,
    owner_email: str,
) -> dict[str, str | None]:
    """Run the owner/key commissioning RPC sequence over BLE."""

    owner_result = await rpc_client.async_rpc_request(
        gateway_mac_id=gateway_mac_id,
        handler_id=HANDLER_SET_OWNER_IN_RECEIVER,
        service_id=SERVICE_ID_COMMS_MANAGER,
        args=[
            {
                "UEA": owner_email,
                "SN": f"SecureMTR {serial_number}",
                "MN": None,
            }
        ],
    )

    ble_key_result = await rpc_client.async_rpc_request(
        gateway_mac_id=gateway_mac_id,
        handler_id=HANDLER_GET_BLE_KEY,
        service_id=SERVICE_ID_HAN_MANAGEMENT,
        args=None,
    )

    ble_key = ble_key_result if isinstance(ble_key_result, str) else None
    if not ble_key:
        raise LocalBleCommissioningError("Gateway did not return a BLE key")

    owner_token = owner_result if isinstance(owner_result, str) else None
    return {
        "local_ble_key": ble_key,
        "local_owner_token": owner_token,
    }


async def async_commission_local_ble(
    hass: HomeAssistant,
    *,
    serial_number: str,
    mac_address: str,
    device_type: str,
) -> dict[str, str | None]:
    """Commission an E7+ device and return persisted BLE credential values."""

    if device_type.strip().lower() != "e7plus":
        raise LocalBleCommissioningError(
            f"Unsupported local BLE device type for commissioning: {device_type}"
        )

    ble_address = _format_mac_address(mac_address)
    gateway_mac_id = str(int(mac_address, 16))

    _LOGGER.info(
        "Starting local BLE commissioning for serial %s at %s",
        serial_number,
        ble_address,
    )

    ble_device = await _async_resolve_connectable_device(hass, ble_address)
    _LOGGER.info("Resolved connectable BLE device %s", ble_device.address)

    def _refresh_ble_device() -> Any:
        from homeassistant.components import bluetooth  # noqa: PLC0415

        refreshed = bluetooth.async_ble_device_from_address(
            hass,
            ble_address,
            connectable=True,
        )
        return refreshed if refreshed is not None else ble_device

    client = await _async_connect_client(
        ble_device,
        ble_device_callback=_refresh_ble_device,
    )
    _LOGGER.info("Connected to BLE device %s", ble_device.address)
    rpc_client = _BleUartRpcClient(client)
    try:
        await rpc_client.async_initialize()
        _LOGGER.info("Initialized UART notification channel for %s", ble_device.address)
        credentials = await _async_commission_over_rpc(
            rpc_client,
            gateway_mac_id=gateway_mac_id,
            serial_number=serial_number,
            owner_email=DEFAULT_OWNER_EMAIL,
        )
    finally:
        await rpc_client.async_close()
        await _async_disconnect_client(client)

    _LOGGER.info(
        "Local BLE commissioning succeeded for serial %s",
        serial_number,
    )
    return credentials

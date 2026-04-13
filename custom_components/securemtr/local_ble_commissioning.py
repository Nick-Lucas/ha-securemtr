"""Local BLE commissioning transport and RPC helpers."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Callable
import json
import logging
import random
import secrets
import time
from typing import Any
from uuid import UUID

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
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
SERVICE_ID_BBSERVER = 1
SERVICE_ID_COMMS_MANAGER = 151
SERVICE_ID_TIME_MANAGEMENT = 103
SERVICE_ID_WLAN = 102
SERVICE_ID_MODE = 15
SERVICE_ID_HOT_WATER = 16
HANDLER_WRITE_DATA = 2
HANDLER_SET_TIME = 2
HANDLER_GET_ALL_SERVICE_VALUES = 3
HANDLER_SET_TIMEZONE_ID = 7
HANDLER_SET_OWNER_IN_RECEIVER = 7
HANDLER_UPDATE_PHYSICAL_DEVICE_DETAILS = 6
HANDLER_GET_BLE_KEY = 50
HANDLER_SET_WIFI_CREDENTIAL = 5
HANDLER_ADD_HAN_DEVICES = 48

E7_PLUS_HARDWARE_TYPE = 65
E7_PLUS_CHANNEL_COUNT = 2
DEFAULT_TIMEZONE_ID = 2
SET_OWNER_TIMEOUT_ERROR = 20001
SET_OWNER_RETRY_LIMIT = 5
BLE_COMMAND_RETRY_LIMIT = 2
BLE_COMMAND_RETRY_DELAY_SECONDS = 2.0

BLE_ACK_SUCCESS = 0
BLE_ACK_INVALID_MESSAGE = 1
BLE_ACK_KEY_MISMATCH = 2

DEFAULT_OWNER_EMAIL = "homeassistant@local"


class LocalBleCommissioningError(HomeAssistantError):
    """Raised when local BLE commissioning cannot complete."""

    def __init__(self, message: str, *, error_code: int | None = None) -> None:
        """Initialize the commissioning error with an optional RPC code."""

        super().__init__(message)
        self.error_code = error_code


class _BleUartRpcClient:
    """Manage packetized UART-over-BLE RPC exchanges."""

    def __init__(self, client: Any) -> None:
        """Store the BLE client and initialize response state."""

        self._client = client
        self._loop = asyncio.get_running_loop()
        self._response_packets: dict[int, bytes] = {}
        self._response_event = asyncio.Event()
        self._auth_key: bytes | None = None

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

        await self._async_send_payload(
            json.dumps(request_payload, separators=(",", ":")).encode("utf-8"),
            use_java_utf8_length=True,
        )

        deadline = self._loop.time() + timeout
        while True:
            remaining = deadline - self._loop.time()
            if remaining <= 0:
                raise LocalBleCommissioningError(
                    f"RPC response ID mismatch (expected {request_id})"
                )

            response_body = await self._async_wait_for_response_payload(
                timeout=remaining,
            )
            try:
                response = json.loads(response_body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise LocalBleCommissioningError(
                    "Failed to parse BLE JSON response"
                ) from error

            if not isinstance(response, dict):
                raise LocalBleCommissioningError("BLE JSON response is not an object")

            response_id = response.get("I")
            if response_id != request_id:
                _LOGGER.debug(
                    "Ignoring BLE response with mismatched id (expected=%s, got=%s)",
                    request_id,
                    response_id,
                )
                self._reset_response_state()
                continue
            break

        error_data = response.get("E")
        if isinstance(error_data, dict):
            error_code = error_data.get("C")
            error_message = error_data.get("M")
            parsed_error_code: int | None = None
            if isinstance(error_code, int):
                parsed_error_code = error_code
            elif isinstance(error_code, str) and error_code.isdigit():
                parsed_error_code = int(error_code)
            raise LocalBleCommissioningError(
                f"Gateway returned RPC error {error_code}: {error_message}",
                error_code=parsed_error_code,
            )

        if "R" not in response:
            raise LocalBleCommissioningError("RPC response did not include a result")

        return response["R"]

    async def async_authorize(self, auth_key: bytes) -> None:
        """Execute 4-pass BLE authorization using the provided key bytes."""

        if len(auth_key) != 16:
            raise LocalBleCommissioningError("BLE key must decode to 16 bytes")

        app_random = secrets.token_bytes(16)
        pass1_request = bytes([1, 0, 16, 0]) + app_random
        pass2_response = await self._async_exchange_bytes(
            pass1_request,
            use_java_utf8_length=True,
        )
        meter_random = _parse_four_pass_response(pass2_response, expected_type=2)

        encrypted_meter_random = _aes_ecb_encrypt(auth_key, meter_random)
        pass3_request = bytes([3, 0, 16, 0]) + encrypted_meter_random
        pass4_response = await self._async_exchange_bytes(
            pass3_request,
            use_java_utf8_length=True,
        )
        encrypted_app_random = _parse_four_pass_response(
            pass4_response, expected_type=4
        )

        decrypted_app_random = _aes_ecb_decrypt(auth_key, encrypted_app_random)
        if decrypted_app_random != app_random:
            raise LocalBleCommissioningError(
                "BLE authorization failed due to random key mismatch"
            )

        self._auth_key = auth_key

    async def _async_exchange_json(
        self,
        request_payload: dict[str, Any],
        *,
        timeout: float,
    ) -> dict[str, Any]:
        """Write packetized JSON and wait for the packetized response."""

        request_json = json.dumps(request_payload, separators=(",", ":"))
        response_body = await self._async_exchange_bytes(
            request_json.encode("utf-8"),
            timeout=timeout,
            use_java_utf8_length=True,
        )
        try:
            response = json.loads(response_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise LocalBleCommissioningError(
                "Failed to parse BLE JSON response"
            ) from error

        if not isinstance(response, dict):
            raise LocalBleCommissioningError("BLE JSON response is not an object")
        return response

    async def _async_exchange_bytes(
        self,
        request_payload: bytes,
        *,
        timeout: float = BLE_REQUEST_TIMEOUT_SECONDS,
        use_java_utf8_length: bool = False,
    ) -> bytes:
        """Write packetized bytes and wait for a packetized byte response."""

        await self._async_send_payload(
            request_payload,
            use_java_utf8_length=use_java_utf8_length,
        )
        return await self._async_wait_for_response_payload(timeout=timeout)

    async def _async_send_payload(
        self,
        request_payload: bytes,
        *,
        use_java_utf8_length: bool,
    ) -> None:
        """Frame and transmit one BLE payload."""

        self._reset_response_state()

        payload_length = (
            _java_utf8_character_length(request_payload)
            if use_java_utf8_length
            else len(request_payload)
        )
        framed_request = payload_length.to_bytes(2, byteorder="big") + request_payload
        if self._auth_key is not None:
            framed_request = _encrypt_payload(self._auth_key, framed_request)

        for packet in _packetize_payload(framed_request):
            await self._client.write_gatt_char(UART_RX_UUID, packet, response=True)

    def _reset_response_state(self) -> None:
        """Clear staged response packets and readiness state."""

        self._response_packets.clear()
        self._response_event.clear()

    async def _async_wait_for_response_payload(self, *, timeout: float) -> bytes:
        """Wait for one BLE response payload and decode transport framing."""

        try:
            await asyncio.wait_for(self._response_event.wait(), timeout=timeout)
        except TimeoutError as error:
            raise LocalBleCommissioningError(
                "Timed out waiting for BLE response"
            ) from error

        response_body = _reassemble_packet_bytes(self._response_packets)
        if self._auth_key is not None:
            response_body = _decrypt_payload(self._auth_key, response_body)
        return _extract_length_prefixed_payload(response_body)

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


def _reassemble_packet_bytes(response_packets: dict[int, bytes]) -> bytes:
    """Reassemble packetized bytes without decoding the framed payload."""

    if FINAL_PACKET_INDEX not in response_packets:
        raise LocalBleCommissioningError("Response did not include the final packet")

    return b"".join(response_packets[index] for index in sorted(response_packets))


def _extract_length_prefixed_payload(payload: bytes) -> bytes:
    """Return payload bytes referenced by the leading 2-byte length field."""

    if len(payload) < 2:
        raise LocalBleCommissioningError("Response was shorter than the length prefix")

    payload_length = int.from_bytes(payload[:2], byteorder="big")
    payload_slice = payload[2 : 2 + payload_length]
    if len(payload_slice) != payload_length:
        raise LocalBleCommissioningError("Response payload length mismatch")

    return payload_slice


def _java_utf8_character_length(payload: bytes) -> int:
    """Return the UTF-8 decoded character length matching Java String behavior."""

    return len(payload.decode("utf-8", errors="replace"))


def _pad_zero(data: bytes) -> bytes:
    """Pad a payload with zeroes to the AES 16-byte block size."""

    remainder = len(data) % 16
    if remainder == 0:
        return data
    return data + (b"\x00" * (16 - remainder))


def _aes_ecb_encrypt(key: bytes, payload: bytes) -> bytes:
    """Encrypt bytes using AES-ECB without adding padding."""

    encryptor = Cipher(algorithms.AES(key), modes.ECB()).encryptor()
    return encryptor.update(payload) + encryptor.finalize()


def _aes_ecb_decrypt(key: bytes, payload: bytes) -> bytes:
    """Decrypt bytes using AES-ECB without stripping padding."""

    decryptor = Cipher(algorithms.AES(key), modes.ECB()).decryptor()
    return decryptor.update(payload) + decryptor.finalize()


def _encrypt_payload(key: bytes, payload: bytes) -> bytes:
    """Encrypt a length-prefixed payload with zero-padding for BLE transport."""

    return _aes_ecb_encrypt(key, _pad_zero(payload))


def _decrypt_payload(key: bytes, payload: bytes) -> bytes:
    """Decrypt an AES-encrypted BLE transport payload."""

    if len(payload) % 16 != 0:
        raise LocalBleCommissioningError(
            "Encrypted BLE payload length is not a 16-byte multiple"
        )
    return _aes_ecb_decrypt(key, payload)


def _parse_four_pass_response(payload: bytes, *, expected_type: int) -> bytes:
    """Parse and validate one BLE 4-pass payload response."""

    if len(payload) < 4:
        raise LocalBleCommissioningError("4-pass response was shorter than 4 bytes")

    response_type = payload[0]
    ack = payload[1]
    value_length = payload[2]

    if response_type == 5:
        if ack == BLE_ACK_INVALID_MESSAGE:
            raise LocalBleCommissioningError("4-pass rejected invalid message format")
        if ack == BLE_ACK_KEY_MISMATCH:
            raise LocalBleCommissioningError("4-pass key mismatch")
        raise LocalBleCommissioningError(f"4-pass failed with ack code {ack}")

    if response_type != expected_type:
        raise LocalBleCommissioningError(
            f"Unexpected 4-pass response type {response_type}; expected {expected_type}"
        )

    if ack != BLE_ACK_SUCCESS:
        raise LocalBleCommissioningError(f"4-pass failed with ack code {ack}")

    if len(payload) < 4 + value_length:
        raise LocalBleCommissioningError("4-pass response payload length mismatch")

    return payload[4 : 4 + value_length]


def _format_mac_address(mac_address: str) -> str:
    """Convert compact uppercase MAC into colon-separated canonical form."""

    return ":".join(
        mac_address[index : index + 2] for index in range(0, len(mac_address), 2)
    )


def _service_uuid_matches(service_info: Any) -> bool | None:
    """Return True/False if UUID match is known, or None when unavailable."""

    service_uuids = getattr(service_info, "service_uuids", None)
    if not isinstance(service_uuids, list):
        return None
    if not service_uuids:
        return None
    expected = str(UART_SERVICE_UUID)
    return any(str(uuid).lower() == expected for uuid in service_uuids)


def _advertised_serial(service_info: Any) -> str | None:
    """Extract the advertised serial/name from Home Assistant service info."""

    name = getattr(service_info, "name", None)
    if isinstance(name, str) and name.strip():
        return name.strip()

    device = getattr(service_info, "device", None)
    device_name = getattr(device, "name", None)
    if isinstance(device_name, str) and device_name.strip():
        return device_name.strip()
    return None


def _extract_raw_scan_record(service_info: Any) -> bytes | None:
    """Best-effort extraction of raw scan-record bytes from service metadata."""

    advertisement = getattr(service_info, "advertisement", None)
    if advertisement is not None:
        for attribute in ("bytes", "raw_data", "data"):
            value = getattr(advertisement, attribute, None)
            if isinstance(value, (bytes, bytearray)):
                return bytes(value)
        platform_data = getattr(advertisement, "platform_data", None)
        candidate = _extract_first_bytes(platform_data)
        if candidate is not None:
            return candidate

    device = getattr(service_info, "device", None)
    details = getattr(device, "details", None)
    candidate = _extract_first_bytes(details)
    if candidate is not None:
        return candidate

    manufacturer_data = getattr(service_info, "manufacturer_data", None)
    if isinstance(manufacturer_data, dict):
        candidate = _extract_first_bytes(tuple(manufacturer_data.values()))
        if candidate is not None:
            return candidate

    return None


def _extract_first_bytes(value: Any) -> bytes | None:
    """Recursively return the first bytes-like payload found in nested objects."""

    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, dict):
        for nested in value.values():
            candidate = _extract_first_bytes(nested)
            if candidate is not None:
                return candidate
        return None
    if isinstance(value, (list, tuple, set)):
        for nested in value:
            candidate = _extract_first_bytes(nested)
            if candidate is not None:
                return candidate
        return None
    return None


def _validate_advertisement_identity(
    service_info: Any,
    *,
    expected_address: str,
    expected_serial: str | None,
) -> None:
    """Validate discovered advertisement identity before BLE connection."""

    address = getattr(service_info, "address", None)
    if not isinstance(address, str) or address.upper() != expected_address.upper():
        raise LocalBleCommissioningError(
            f"Advertisement address mismatch: expected {expected_address}, got {address}"
        )

    uuid_match = _service_uuid_matches(service_info)
    if uuid_match is False:
        raise LocalBleCommissioningError(
            "Advertisement did not include SecureMTR UART service UUID"
        )
    if uuid_match is None:
        _LOGGER.debug(
            "Advertisement service UUID list unavailable for %s", expected_address
        )

    if expected_serial is not None:
        serial = _advertised_serial(service_info)
        if serial is None or serial.upper() != expected_serial.upper():
            raise LocalBleCommissioningError(
                f"Advertisement serial mismatch: expected {expected_serial}, got {serial}"
            )

    raw_scan_record = _extract_raw_scan_record(service_info)
    if raw_scan_record is None:
        _LOGGER.debug("Raw advertisement bytes unavailable for %s", expected_address)
        return

    if len(raw_scan_record) <= 30:
        _LOGGER.debug(
            "Raw advertisement bytes shorter than expected for protocol validation"
        )
        return

    hardware_type = raw_scan_record[21]
    if hardware_type != E7_PLUS_HARDWARE_TYPE:
        raise LocalBleCommissioningError(
            f"Advertisement hardware type mismatch: expected {E7_PLUS_HARDWARE_TYPE}, got {hardware_type}"
        )


async def _async_resolve_connectable_device(
    hass: HomeAssistant,
    address: str,
    *,
    expected_serial: str | None = None,
) -> Any:
    """Resolve a connectable BLEDevice via Home Assistant's shared scanner."""

    from homeassistant.components import bluetooth  # noqa: PLC0415

    scanner_count = bluetooth.async_scanner_count(hass, connectable=True)
    if scanner_count <= 0:
        raise LocalBleCommissioningError(
            "No connectable Bluetooth adapters are available in Home Assistant"
        )

    last_service_info = bluetooth.async_last_service_info(
        hass,
        address,
        connectable=True,
    )
    if last_service_info is not None:
        _validate_advertisement_identity(
            last_service_info,
            expected_address=address,
            expected_serial=expected_serial,
        )

    ble_device = bluetooth.async_ble_device_from_address(
        hass, address, connectable=True
    )
    if ble_device is not None:
        return ble_device

    _LOGGER.info("Waiting for BLE advertisement from %s", address)

    def _process_advertisement(service_info: Any) -> bool:
        if service_info.address.upper() != address.upper():
            return False
        try:
            _validate_advertisement_identity(
                service_info,
                expected_address=address,
                expected_serial=expected_serial,
            )
        except LocalBleCommissioningError:
            return False
        return True

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
    timezone_id: int,
    owner_email: str,
    receiver_name: str,
    include_wifi_step: bool = False,
) -> dict[str, str | None]:
    """Commission over BLE, trying the already-commissioned path first.

    Matches the app behavior: SystemAlreadyCommissionedActivity tries
    SetOwner -> GetBLEKey without touching timezone/time/device details.
    Only falls back to the full first-time sequence if the simple path
    fails with a transport-level error.
    """

    try:
        return await _async_commission_already_paired(
            rpc_client,
            gateway_mac_id=gateway_mac_id,
            owner_email=owner_email,
            receiver_name=receiver_name,
        )
    except LocalBleCommissioningError as error:
        _LOGGER.info(
            "Already-commissioned path failed (%s), trying full commissioning",
            error,
        )

    await rpc_client.async_rpc_request(
        gateway_mac_id=gateway_mac_id,
        handler_id=HANDLER_SET_TIMEZONE_ID,
        service_id=SERVICE_ID_TIME_MANAGEMENT,
        args=[timezone_id],
    )
    await rpc_client.async_rpc_request(
        gateway_mac_id=gateway_mac_id,
        handler_id=HANDLER_SET_TIME,
        service_id=SERVICE_ID_TIME_MANAGEMENT,
        args=[int(time.time())],
    )
    await rpc_client.async_rpc_request(
        gateway_mac_id=gateway_mac_id,
        handler_id=HANDLER_UPDATE_PHYSICAL_DEVICE_DETAILS,
        service_id=SERVICE_ID_HAN_MANAGEMENT,
        args=[
            {
                "P": 1,
                "N": receiver_name,
                "L": "",
                "LO": "",
                "FT": 0,
            }
        ],
    )

    owner_result = await _async_set_owner_with_retry(
        rpc_client,
        gateway_mac_id=gateway_mac_id,
        owner_email=owner_email,
        receiver_name=receiver_name,
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

    if include_wifi_step:
        raise LocalBleCommissioningError(
            "SetWifiCredential commissioning step is not implemented yet"
        )

    await rpc_client.async_rpc_request(
        gateway_mac_id=gateway_mac_id,
        handler_id=HANDLER_ADD_HAN_DEVICES,
        service_id=SERVICE_ID_HAN_MANAGEMENT,
        args=_default_han_device_details(),
    )

    owner_token = owner_result if isinstance(owner_result, str) else None
    return {
        "local_ble_key": ble_key,
        "local_owner_token": owner_token,
    }


async def _async_commission_already_paired(
    rpc_client: _BleUartRpcClient,
    *,
    gateway_mac_id: str,
    owner_email: str,
    receiver_name: str,
) -> dict[str, str | None]:
    """Retrieve credentials from an already-commissioned device.

    Tries GetBLEKey first — if the device already has a key it is returned
    without touching ownership.  Only calls SetOwner when the device does
    not yet have a key assigned.
    """

    ble_key_result = await rpc_client.async_rpc_request(
        gateway_mac_id=gateway_mac_id,
        handler_id=HANDLER_GET_BLE_KEY,
        service_id=SERVICE_ID_HAN_MANAGEMENT,
        args=None,
    )
    ble_key = ble_key_result if isinstance(ble_key_result, str) else None
    if ble_key:
        return {
            "local_ble_key": ble_key,
            "local_owner_token": None,
        }

    owner_result = await _async_set_owner_with_retry(
        rpc_client,
        gateway_mac_id=gateway_mac_id,
        owner_email=owner_email,
        receiver_name=receiver_name,
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


async def _async_set_owner_with_retry(
    rpc_client: _BleUartRpcClient,
    *,
    gateway_mac_id: str,
    owner_email: str,
    receiver_name: str,
) -> Any:
    """Set receiver owner with app-equivalent retries for timeout error 20001."""

    for attempt in range(SET_OWNER_RETRY_LIMIT + 1):
        try:
            return await rpc_client.async_rpc_request(
                gateway_mac_id=gateway_mac_id,
                handler_id=HANDLER_SET_OWNER_IN_RECEIVER,
                service_id=SERVICE_ID_COMMS_MANAGER,
                args=[{"UEA": owner_email, "SN": receiver_name, "MN": None}],
                timeout=BLE_REQUEST_TIMEOUT_SECONDS,
            )
        except LocalBleCommissioningError as error:
            if error.error_code != SET_OWNER_TIMEOUT_ERROR:
                raise
            if attempt >= SET_OWNER_RETRY_LIMIT:
                raise
            _LOGGER.warning(
                "SetOwnerInReceiver timed out with %s (attempt %s/%s), retrying",
                SET_OWNER_TIMEOUT_ERROR,
                attempt + 1,
                SET_OWNER_RETRY_LIMIT + 1,
            )

    raise LocalBleCommissioningError("SetOwnerInReceiver retries exhausted")


def _default_han_device_details() -> list[dict[str, Any]]:
    """Build a default two-channel HAN device list for E7+ commissioning."""

    return [
        {
            "ZT": 1,
            "CN": 1,
            "DT": 0,
            "ZN": 0,
            "MC": 0,
            "SN": "",
            "ZNM": "Hot Water",
            "RHT": E7_PLUS_HARDWARE_TYPE,
            "CT": 0,
        },
        {
            "ZT": 2,
            "CN": 2,
            "DT": 0,
            "ZN": 0,
            "MC": 0,
            "SN": "",
            "ZNM": "Timer",
            "RHT": E7_PLUS_HARDWARE_TYPE,
            "CT": 0,
        },
    ]


def _resolve_timezone_id(hass: HomeAssistant) -> int:
    """Resolve the gateway timezone ID with conservative fallback to app default."""

    timezone_name = getattr(getattr(hass, "config", None), "time_zone", "")
    if isinstance(timezone_name, str) and timezone_name.strip() == "UTC":
        return 1
    return DEFAULT_TIMEZONE_ID


def _command_to_rpc_payload(
    method_name: str,
    operation_kwargs: dict[str, Any],
    *,
    mode_boi: int,
    hot_water_boi: int,
) -> tuple[int, int, list[Any] | None]:
    """Translate a local command method name to RPC header and arguments."""

    if method_name == "start_timed_boost":
        duration = operation_kwargs.get("duration_minutes")
        if not isinstance(duration, int) or duration <= 0:
            raise ValueError("Boost duration must be a positive number of minutes")
        return (
            HANDLER_WRITE_DATA,
            SERVICE_ID_HOT_WATER,
            [hot_water_boi, {"D": duration, "I": 4, "OT": 2, "V": 0}],
        )

    if method_name == "stop_timed_boost":
        return (
            HANDLER_WRITE_DATA,
            SERVICE_ID_HOT_WATER,
            [hot_water_boi, {"D": 0, "I": 4, "OT": 2, "V": 0}],
        )

    if method_name == "set_timed_boost_enabled":
        enabled = operation_kwargs.get("enabled")
        if not isinstance(enabled, bool):
            raise ValueError("Timed boost enabled flag must be boolean")
        return (
            HANDLER_WRITE_DATA,
            SERVICE_ID_HOT_WATER,
            [hot_water_boi, {"I": 27, "V": 1 if enabled else 0}],
        )

    if method_name == "turn_controller_on":
        return (
            HANDLER_WRITE_DATA,
            SERVICE_ID_MODE,
            [mode_boi, {"I": 6, "V": 2}],
        )

    if method_name == "turn_controller_off":
        return (
            HANDLER_WRITE_DATA,
            SERVICE_ID_MODE,
            [mode_boi, {"I": 6, "V": 0}],
        )

    raise LocalBleCommissioningError(
        f"Unsupported local BLE command method: {method_name}"
    )


async def _async_resolve_service_bois(
    rpc_client: _BleUartRpcClient,
    *,
    gateway_mac_id: str,
) -> tuple[int, int]:
    """Resolve mode and hot-water BOIs from GetAllServiceValues response."""

    response = await rpc_client.async_rpc_request(
        gateway_mac_id=gateway_mac_id,
        handler_id=HANDLER_GET_ALL_SERVICE_VALUES,
        service_id=SERVICE_ID_BBSERVER,
        args=None,
    )

    mode_boi = 1
    hot_water_boi = 2

    service_values: list[Any] | None = None
    if isinstance(response, dict):
        response_values = response.get("V")
        if isinstance(response_values, list):
            service_values = response_values
    elif isinstance(response, list):
        service_values = response

    if isinstance(service_values, list):
        for entry in service_values:
            if not isinstance(entry, dict):
                continue

            service_id = entry.get("SI")
            if not isinstance(service_id, int):
                service_id = entry.get("value")

            boi = entry.get("I")
            if not isinstance(service_id, int) or not isinstance(boi, int):
                continue

            if service_id == SERVICE_ID_MODE:
                mode_boi = boi
            elif service_id == SERVICE_ID_HOT_WATER:
                hot_water_boi = boi

    return mode_boi, hot_water_boi


async def async_execute_local_command(
    hass: HomeAssistant,
    *,
    mac_address: str,
    serial_number: str | None,
    ble_key: str,
    method_name: str,
    operation_kwargs: dict[str, Any],
) -> Any:
    """Execute one local BLE command over an authenticated BLE session."""

    ble_address = _format_mac_address(mac_address)
    gateway_mac_id = str(int(mac_address, 16))

    try:
        decoded_key = base64.b64decode(ble_key)
    except Exception as error:
        raise LocalBleCommissioningError(
            "Stored BLE key is not valid base64"
        ) from error

    if len(decoded_key) != 16:
        raise LocalBleCommissioningError("Stored BLE key must decode to 16 bytes")

    last_error: LocalBleCommissioningError | None = None
    for attempt in range(BLE_COMMAND_RETRY_LIMIT + 1):
        try:
            result = await _async_execute_ble_rpc_command(
                hass,
                ble_address=ble_address,
                expected_serial=serial_number,
                gateway_mac_id=gateway_mac_id,
                method_name=method_name,
                operation_kwargs=operation_kwargs,
                auth_key=decoded_key,
            )
            break
        except LocalBleCommissioningError as error:
            last_error = error
            if attempt < BLE_COMMAND_RETRY_LIMIT:
                _LOGGER.warning(
                    "BLE command %s failed (attempt %s/%s): %s, retrying",
                    method_name,
                    attempt + 1,
                    BLE_COMMAND_RETRY_LIMIT + 1,
                    error,
                )
                await asyncio.sleep(BLE_COMMAND_RETRY_DELAY_SECONDS)
            else:
                raise
    else:
        raise last_error  # type: ignore[misc]

    if result not in (0, "0", None):
        raise LocalBleCommissioningError(
            f"Unexpected local BLE acknowledgement for {method_name}: {result}"
        )

    return result


async def _async_execute_ble_rpc_command(
    hass: HomeAssistant,
    *,
    ble_address: str,
    expected_serial: str | None,
    gateway_mac_id: str,
    method_name: str,
    operation_kwargs: dict[str, Any],
    auth_key: bytes | None,
) -> Any:
    """Connect over BLE, optionally authorize, and execute one RPC request."""

    ble_device = await _async_resolve_connectable_device(
        hass,
        ble_address,
        expected_serial=expected_serial,
    )

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
    rpc_client = _BleUartRpcClient(client)
    try:
        await rpc_client.async_initialize()
        if auth_key is not None:
            await rpc_client.async_authorize(auth_key)
        mode_boi, hot_water_boi = await _async_resolve_service_bois(
            rpc_client,
            gateway_mac_id=gateway_mac_id,
        )
        handler_id, service_id, args = _command_to_rpc_payload(
            method_name,
            operation_kwargs,
            mode_boi=mode_boi,
            hot_water_boi=hot_water_boi,
        )
        return await rpc_client.async_rpc_request(
            gateway_mac_id=gateway_mac_id,
            handler_id=handler_id,
            service_id=service_id,
            args=args,
        )
    finally:
        await rpc_client.async_close()
        await _async_disconnect_client(client)


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
    receiver_name = f"SecureMTR {serial_number}"
    timezone_id = _resolve_timezone_id(hass)

    _LOGGER.info(
        "Starting local BLE commissioning for serial %s at %s",
        serial_number,
        ble_address,
    )

    ble_device = await _async_resolve_connectable_device(
        hass,
        ble_address,
        expected_serial=serial_number,
    )
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
            timezone_id=timezone_id,
            owner_email=DEFAULT_OWNER_EMAIL,
            receiver_name=receiver_name,
        )
    finally:
        await rpc_client.async_close()
        await _async_disconnect_client(client)

    _LOGGER.info(
        "Local BLE commissioning succeeded for serial %s",
        serial_number,
    )
    return credentials

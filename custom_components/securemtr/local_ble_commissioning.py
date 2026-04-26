"""Local BLE commissioning transport and RPC helpers."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
import json
import logging
import math
import random
import secrets
import time
from typing import Any, TYPE_CHECKING
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
SERVICE_ID_MODE_LEGACY = 13
SERVICE_ID_HOT_WATER = 16
SERVICE_ID_PRIMARY_STATE = 33
SERVICE_ID_DYNAMIC_TARIFF = 36
HANDLER_WRITE_DATA = 2
HANDLER_SET_TIME = 2
HANDLER_GET_ALL_SERVICE_VALUES = 3
HANDLER_GET_ALL_BB_ALARM = 5
HANDLER_GET_CONSUMPTION_STATE = 9
HANDLER_SET_TIMEZONE_ID = 7
HANDLER_SET_OWNER_IN_RECEIVER = 7
HANDLER_UPDATE_PHYSICAL_DEVICE_DETAILS = 6
HANDLER_GET_BLE_KEY = 50
HANDLER_SET_WIFI_CREDENTIAL = 5
HANDLER_ADD_HAN_DEVICES = 48

E7_PLUS_HARDWARE_TYPE = 65
E7_PLUS_CHANNEL_COUNT = 2
MODE_HOME_VALUE = 2
DEVICE_CHARACTERISTIC_HOT_WATER_STATE = 4
DEVICE_CHARACTERISTIC_MODE = 6
DEVICE_CHARACTERISTIC_ACTIVE_ENERGY = 19
DEVICE_CHARACTERISTIC_SCHEDULE_ENABLE_DISABLE = 27
OVERRIDE_TYPE_ADVANCE = 2
DEFAULT_TIMEZONE_ID = 2
SET_OWNER_TIMEOUT_ERROR = 20001
SET_OWNER_RETRY_LIMIT = 5
BLE_COMMAND_RETRY_LIMIT = 2
BLE_COMMAND_RETRY_DELAY_SECONDS = 2.0
BLE_NOTIFY_RECOVERY_DELAY_SECONDS = 0.25
BLE_DEBUG_PAYLOAD_MAX_CHARS = 4000

BLE_ACK_SUCCESS = 0
BLE_ACK_INVALID_MESSAGE = 1
BLE_ACK_KEY_MISMATCH = 2

ALARM_ID_LOW_BATTERY_EXT_TEMP_SENSOR = 8
ALARM_ID_HAN_COMMS_STATE = 9
ALARM_ID_SERVICE_CLOCK_EXP = 10
ALARM_ID_OVER_CURRENT = 11
ALARM_ID_SWITCH_WELD = 12

ALARM_KEY_BY_ID: dict[int, str] = {
    ALARM_ID_LOW_BATTERY_EXT_TEMP_SENSOR: "low_battery_ext_temp_sensor",
    ALARM_ID_HAN_COMMS_STATE: "han_comms_state",
    ALARM_ID_SERVICE_CLOCK_EXP: "service_clock_expired",
    ALARM_ID_OVER_CURRENT: "over_current",
    ALARM_ID_SWITCH_WELD: "switch_weld",
}

DEFAULT_OWNER_EMAIL = "homeassistant@local"

_APP_TIMEZONE_ID_LOOKUP: dict[str, int] = {
    "asia/dubai": 1,
    "asia/kolkata": 2,
    "asia/calcutta": 2,
    "ist": 2,
    "europe/london": 3,
    "utc": 3,
    "gmt": 3,
    "europe/paris": 4,
    "europe/stockholm": 5,
    "australia/sydney": 6,
    "austraila/nsw": 6,
    "australia/melbourne": 7,
    "austraila/victoria": 7,
    "antarctica/macquarie": 8,
    "australia/currie": 9,
    "australia/hobart": 10,
    "austraila/tasmania": 10,
    "australia/lord_howe": 11,
    "australia/canberra": 12,
    "australia/adelaide": 13,
    "austraila/south": 13,
    "australia/broken_hill": 14,
    "australia/brisbane": 15,
    "austraila/queensland": 15,
    "australia/lindeman": 16,
    "australia/darwin": 17,
    "austraila/north": 17,
    "australia/eucla": 18,
    "australia/perth": 19,
    "austraila/west": 19,
    "asia/singapore": 20,
    "asia/brunei": 20,
    "sst": 20,
    "asia/riyadh": 21,
    "asia/kuwait": 22,
    "asia/muscat": 23,
    "asia/qatar": 24,
    "asia/bahrain": 25,
    "asia/kuala_lumpur": 26,
    "asia/kuching": 26,
    "mst": 26,
    "asia/bangkok": 27,
    "tha": 27,
    "asia/aden": 28,
    "ast": 21,
}

SERVICE_ID_SCHEDULE = 17
HANDLER_READ_WEEKLY_PROGRAM = 22
HANDLER_WRITE_WEEKLY_PROGRAM = 21

PROGRAM_ZONE_INDEX: dict[str, int] = {
    "primary": 1,
    "boost": 2,
}

if TYPE_CHECKING:
    from .beanbag import WeeklyProgram


class LocalBleCommissioningError(HomeAssistantError):
    """Raised when local BLE commissioning cannot complete."""

    def __init__(self, message: str, *, error_code: int | None = None) -> None:
        """Initialize the commissioning error with an optional RPC code."""

        super().__init__(message)
        self.error_code = error_code


def _is_notify_already_acquired_error(error: Exception) -> bool:
    """Return True when BlueZ reports notifications already acquired."""

    message = str(error).lower()
    return "notify acquired" in message or (
        "notpermitted" in message and "notify" in message
    )


def _format_debug_payload(
    payload: Any, *, max_chars: int = BLE_DEBUG_PAYLOAD_MAX_CHARS
) -> str:
    """Return a bounded JSON-like string for BLE debug logging."""

    try:
        rendered = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    except TypeError:
        rendered = repr(payload)

    if len(rendered) <= max_chars:
        return rendered

    return f"{rendered[:max_chars]}...(truncated {len(rendered) - max_chars} chars)"


def _summarize_service_values(response: Any) -> list[dict[str, Any]]:
    """Return compact per-service diagnostics from service values payloads."""

    summaries: list[dict[str, Any]] = []
    for service_entry in _extract_service_values(response):
        service_id = service_entry.get("SI")
        if not isinstance(service_id, int):
            service_id = service_entry.get("value")
        if not isinstance(service_id, int):
            continue

        boi = _extract_service_boi(service_entry)
        characteristic_ids: list[int] = []
        for char_id in _extract_characteristic_map(service_entry):
            characteristic_ids.append(char_id)
        characteristic_ids.sort()

        summaries.append(
            {
                "si": service_id,
                "boi": boi,
                "chars": characteristic_ids,
            }
        )

    return summaries


@dataclass(slots=True)
class LocalBleSnapshot:
    """Represent parsed local BLE values for runtime sensors."""

    primary_power_on: bool | None = None
    timed_boost_enabled: bool | None = None
    timed_boost_active: bool | None = None
    timed_boost_duration_minutes: int | None = None
    primary_energy_kwh: float | None = None
    boost_energy_kwh: float | None = None
    consumption_days: list[dict[str, Any]] | None = None
    statistics_recent: dict[str, dict[str, Any]] | None = None
    schedule_zone_bois: dict[str, int] | None = None
    alarms_state: dict[str, dict[str, Any]] | None = None


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

        try:
            await self._client.start_notify(UART_TX_UUID, self._notification_callback)
        except (BleakError, RuntimeError, OSError) as error:
            if _is_notify_already_acquired_error(error):
                _LOGGER.debug(
                    "TX notify channel already acquired; attempting one recovery cycle"
                )
                with suppress(BleakError, RuntimeError, OSError):
                    await self._client.stop_notify(UART_TX_UUID)
                await asyncio.sleep(BLE_NOTIFY_RECOVERY_DELAY_SECONDS)
                try:
                    await self._client.start_notify(
                        UART_TX_UUID,
                        self._notification_callback,
                    )
                    return
                except (BleakError, RuntimeError, OSError) as retry_error:
                    raise LocalBleCommissioningError(
                        "Failed to subscribe to SecureMTR TX notifications"
                    ) from retry_error

            raise LocalBleCommissioningError(
                "Failed to subscribe to SecureMTR TX notifications"
            ) from error

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
            try:
                await self._client.write_gatt_char(UART_RX_UUID, packet, response=True)
            except (BleakError, RuntimeError, OSError) as error:
                raise LocalBleCommissioningError(
                    "Failed to write BLE UART packet"
                ) from error

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
    """Resolve the gateway timezone ID using app-parity timezone mappings."""

    timezone_name = getattr(getattr(hass, "config", None), "time_zone", "")
    if isinstance(timezone_name, str):
        lookup_key = timezone_name.strip().lower()
        if lookup_key:
            matched_timezone = _APP_TIMEZONE_ID_LOOKUP.get(lookup_key)
            if matched_timezone is not None:
                return matched_timezone
    return DEFAULT_TIMEZONE_ID


def _extract_service_values(response: Any) -> list[dict[str, Any]]:
    """Extract ServiceValuesDTO entries from a GetAllServiceValues response."""

    service_values: list[Any] | None = None
    if isinstance(response, dict):
        response_values = response.get("V")
        if isinstance(response_values, list):
            service_values = response_values
    elif isinstance(response, list):
        service_values = response

    if not isinstance(service_values, list):
        return []

    return [entry for entry in service_values if isinstance(entry, dict)]


def _extract_characteristic_map(
    service_entry: dict[str, Any],
) -> dict[int, dict[str, Any]]:
    """Build a characteristic-id map from a service values entry."""

    raw_characteristics = service_entry.get("V")
    if not isinstance(raw_characteristics, list):
        return {}

    characteristic_map: dict[int, dict[str, Any]] = {}
    for characteristic in raw_characteristics:
        if not isinstance(characteristic, dict):
            continue
        char_id = characteristic.get("I")
        if not isinstance(char_id, int):
            continue
        if char_id in characteristic_map:
            continue
        characteristic_map[char_id] = characteristic

    return characteristic_map


def _extract_numeric_value(payload: dict[str, Any], key: str) -> float | None:
    """Return a float value for a dictionary key when numeric."""

    value = payload.get(key)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        candidate = value.strip()
        if candidate == "":
            return None
        try:
            parsed = float(candidate)
        except ValueError:
            return None
        if math.isfinite(parsed):
            return parsed
    return None


def _extract_service_boi(service_entry: dict[str, Any]) -> int | None:
    """Return a valid business-object identifier from a service values entry."""

    boi = service_entry.get("I")
    if isinstance(boi, int):
        return boi
    return None


def _extract_service_identifier(service_entry: dict[str, Any]) -> int | None:
    """Return a service identifier from a service-values entry."""

    service_id = service_entry.get("SI")
    if not isinstance(service_id, int):
        service_id = service_entry.get("value")
    if isinstance(service_id, int):
        return service_id
    return None


def _find_service_boi(response: Any, *, service_id: int) -> int | None:
    """Find the first BOI for the requested service identifier."""

    for service_entry in _extract_service_values(response):
        if _extract_service_identifier(service_entry) != service_id:
            continue
        boi = _extract_service_boi(service_entry)
        if boi is not None:
            return boi
    return None


def _find_service_bois(response: Any, *, service_id: int) -> list[int]:
    """Find all distinct BOIs for the requested service identifier."""

    bois: list[int] = []
    for service_entry in _extract_service_values(response):
        if _extract_service_identifier(service_entry) != service_id:
            continue
        boi = _extract_service_boi(service_entry)
        if boi is None or boi in bois:
            continue
        bois.append(boi)
    return bois


def _coerce_schedule_zone_bois(
    fallback_zone_bois: Mapping[str, Any] | None,
) -> dict[str, int]:
    """Normalize optional zone-BOI hints into a strict mapping."""

    zone_bois = dict(PROGRAM_ZONE_INDEX)
    if not isinstance(fallback_zone_bois, Mapping):
        return zone_bois

    for zone in PROGRAM_ZONE_INDEX:
        candidate = fallback_zone_bois.get(zone)
        if isinstance(candidate, int) and candidate > 0:
            zone_bois[zone] = candidate

    return zone_bois


def _resolve_mode_and_hot_water_service_bois(
    response: Any,
) -> tuple[int | None, int | None]:
    """Resolve mode-service and hot-water-service BOIs from service values."""

    mode_boi = _find_service_boi(response, service_id=SERVICE_ID_MODE)
    if mode_boi is None:
        mode_boi = _find_service_boi(response, service_id=SERVICE_ID_MODE_LEGACY)

    hot_water_boi = _find_service_boi(response, service_id=SERVICE_ID_HOT_WATER)
    return mode_boi, hot_water_boi


def _resolve_schedule_zone_bois_from_service_values(
    response: Any,
    *,
    fallback_zone_bois: Mapping[str, Any] | None = None,
) -> dict[str, int]:
    """Resolve primary/boost schedule BOIs from GetAllServiceValues payloads."""

    zone_bois = _coerce_schedule_zone_bois(fallback_zone_bois)

    primary_zone_boi_hint, boost_zone_boi_hint = (
        _resolve_mode_and_hot_water_service_bois(response)
    )
    if primary_zone_boi_hint is not None:
        zone_bois["primary"] = primary_zone_boi_hint
    if boost_zone_boi_hint is not None:
        zone_bois["boost"] = boost_zone_boi_hint

    schedule_bois = _find_service_bois(response, service_id=SERVICE_ID_SCHEDULE)
    if not schedule_bois:
        return zone_bois

    if zone_bois["primary"] not in schedule_bois:
        zone_bois["primary"] = schedule_bois[0]

    if (
        zone_bois["boost"] == zone_bois["primary"]
        or zone_bois["boost"] not in schedule_bois
    ):
        alternate = next(
            (
                boi
                for boi in schedule_bois
                if boi != zone_bois["primary"]
            ),
            None,
        )
        if alternate is not None:
            zone_bois["boost"] = alternate

    return zone_bois


async def _async_get_all_service_values(
    rpc_client: _BleUartRpcClient,
    *,
    gateway_mac_id: str,
) -> Any:
    """Fetch the local BLE GetAllServiceValues payload."""

    return await rpc_client.async_rpc_request(
        gateway_mac_id=gateway_mac_id,
        handler_id=HANDLER_GET_ALL_SERVICE_VALUES,
        service_id=SERVICE_ID_BBSERVER,
        args=None,
    )


async def _async_resolve_schedule_zone_bois(
    rpc_client: _BleUartRpcClient,
    *,
    gateway_mac_id: str,
    fallback_zone_bois: Mapping[str, Any] | None = None,
) -> dict[str, int]:
    """Resolve primary/boost schedule BOIs from live service values."""

    response = await _async_get_all_service_values(
        rpc_client,
        gateway_mac_id=gateway_mac_id,
    )
    return _resolve_schedule_zone_bois_from_service_values(
        response,
        fallback_zone_bois=fallback_zone_bois,
    )


def _active_energy_to_kwh(raw_value: float | None) -> float | None:
    """Convert ActiveEnergy characteristic values to kilowatt-hours."""

    if raw_value is None or raw_value < 0:
        return None
    return raw_value / 1000.0


def _minutes_to_hours(value: float | None) -> float | None:
    """Convert runtime minutes into fractional hours."""

    if value is None or value < 0:
        return None
    return value / 60.0


def _extract_duration_minutes(row: dict[str, Any], key: str) -> float:
    """Extract duration minutes from a consumption row with zero default."""

    value = _extract_numeric_value(row, key)
    if value is None or value < 0:
        return 0.0
    return value


def _extract_consumption_energy_kwh(row: dict[str, Any], key: str) -> float | None:
    """Extract a daily energy value from DynamicTariff rows as kWh."""

    value = _extract_numeric_value(row, key)
    if value is None or value < 0:
        return None
    return value / 1000.0


def _parse_consumption_report_day(timestamp_value: float | None) -> str | None:
    """Parse the dynamic tariff timestamp into an ISO report day."""

    if timestamp_value is None or timestamp_value <= 0:
        return None
    timestamp_seconds = timestamp_value
    if timestamp_seconds >= 1_000_000_000_000:
        timestamp_seconds = timestamp_seconds / 1000.0

    try:
        return time.strftime("%Y-%m-%d", time.gmtime(timestamp_seconds))
    except (OverflowError, ValueError):
        return None


def _extract_consumption_rows(response: Any) -> list[dict[str, Any]]:
    """Extract DynamicDaySchedule-like rows from a consumption payload."""

    payload = response
    if isinstance(payload, dict):
        payload_values = payload.get("V")
        if isinstance(payload_values, list):
            payload = payload_values
        elif isinstance(payload.get("D"), list):
            payload = [payload]
        elif isinstance(payload.get("D"), dict):
            payload = [{"D": [payload.get("D")]}]
        elif any(key in payload for key in ("OA", "OS", "BA", "BS", "T")):
            payload = [payload]

    if not isinstance(payload, list):
        _LOGGER.debug(
            "DynamicTariff consumption payload is not a list: %s",
            _format_debug_payload(response),
        )
        return []

    rows: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        if any(key in item for key in ("OA", "OS", "BA", "BS", "T")):
            rows.append(item)
            continue
        day_entries = item.get("D")
        if not isinstance(day_entries, list):
            continue
        rows.extend(entry for entry in day_entries if isinstance(entry, dict))

    return rows


def _parse_consumption_day_rows(response: Any) -> list[dict[str, Any]]:
    """Parse DynamicTariff payload rows into normalized per-day values."""

    rows = _extract_consumption_rows(response)
    if not rows:
        _LOGGER.debug(
            "DynamicTariff consumption payload had no day rows: %s",
            _format_debug_payload(response),
        )
        return []

    parsed_rows: list[dict[str, Any]] = []
    for row in rows:
        timestamp = _extract_numeric_value(row, "T")
        report_day = _parse_consumption_report_day(timestamp)

        parsed_rows.append(
            {
                "timestamp": timestamp,
                "report_day": report_day,
                "primary_runtime_hours": _minutes_to_hours(
                    _extract_duration_minutes(row, "OA")
                ),
                "primary_scheduled_hours": _minutes_to_hours(
                    _extract_duration_minutes(row, "OS")
                ),
                "boost_runtime_hours": _minutes_to_hours(
                    _extract_duration_minutes(row, "BA")
                ),
                "boost_scheduled_hours": _minutes_to_hours(
                    _extract_duration_minutes(row, "BS")
                ),
                "primary_energy_kwh": _extract_consumption_energy_kwh(row, "OP"),
                "boost_energy_kwh": _extract_consumption_energy_kwh(row, "BP"),
                "raw": row,
            }
        )

    return parsed_rows


def _select_latest_consumption_row(
    parsed_rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Select the latest parsed consumption row using timestamp ordering."""

    if not parsed_rows:
        return None

    def _sort_key(item: dict[str, Any]) -> float:
        timestamp = item.get("timestamp")
        if not isinstance(timestamp, (int, float)):
            return float("-inf")
        return float(timestamp)

    return max(parsed_rows, key=_sort_key)


def _consumption_rows_have_energy(parsed_rows: list[dict[str, Any]]) -> bool:
    """Return whether parsed consumption rows contain OP/BP energy values."""

    for row in parsed_rows:
        if not isinstance(row, dict):
            continue
        for field_name in ("primary_energy_kwh", "boost_energy_kwh"):
            value = row.get(field_name)
            if isinstance(value, (int, float)) and value >= 0:
                return True
    return False


def _parse_consumption_state(response: Any) -> dict[str, dict[str, Any]] | None:
    """Parse DynamicTariff consumption payload into recent duration summaries."""

    parsed_rows = _parse_consumption_day_rows(response)
    if not parsed_rows:
        return None

    latest = _select_latest_consumption_row(parsed_rows)
    if latest is None:
        return None

    report_day = latest.get("report_day")
    if not isinstance(report_day, str):
        report_day = None

    parsed = {
        "primary": {
            "report_day": report_day,
            "runtime_hours": latest.get("primary_runtime_hours"),
            "scheduled_hours": latest.get("primary_scheduled_hours"),
            "energy_kwh": latest.get("primary_energy_kwh"),
        },
        "boost": {
            "report_day": report_day,
            "runtime_hours": latest.get("boost_runtime_hours"),
            "scheduled_hours": latest.get("boost_scheduled_hours"),
            "energy_kwh": latest.get("boost_energy_kwh"),
        },
    }

    raw_row = latest.get("raw")
    if not isinstance(raw_row, dict):
        raw_row = {}

    _LOGGER.debug(
        "Parsed DynamicTariff consumption row: report_day=%s OA=%s OS=%s BA=%s BS=%s OP=%s BP=%s",
        report_day,
        raw_row.get("OA"),
        raw_row.get("OS"),
        raw_row.get("BA"),
        raw_row.get("BS"),
        raw_row.get("OP"),
        raw_row.get("BP"),
    )
    return parsed


def _alarm_is_active(alarm_id: int, alarm_status: int | None) -> bool:
    """Return whether an alarm entry should be treated as active."""

    if alarm_status is None:
        return False
    if alarm_id == ALARM_ID_HAN_COMMS_STATE:
        return alarm_status == 0
    return alarm_status == 1


def _parse_local_ble_alarms(response: Any) -> dict[str, dict[str, Any]] | None:
    """Parse GetAllBBAlarm payload into per-alarm active state."""

    payload = response
    if isinstance(payload, dict):
        values = payload.get("V")
        if isinstance(values, list):
            payload = values

    if not isinstance(payload, list):
        _LOGGER.debug(
            "GetAllBBAlarm payload is not a list: %s",
            _format_debug_payload(response),
        )
        return None

    parsed_state: dict[str, dict[str, Any]] = {
        alarm_key: {
            "alarm_id": alarm_id,
            "active": False,
            "active_count": 0,
            "channels": [],
            "latest_raw_value": None,
        }
        for alarm_id, alarm_key in ALARM_KEY_BY_ID.items()
    }

    for service_entry in payload:
        if not isinstance(service_entry, dict):
            continue

        channel_value = _extract_numeric_value(service_entry, "I")
        channel_number = int(channel_value) if channel_value is not None else None

        alarm_rows = service_entry.get("V")
        if not isinstance(alarm_rows, list):
            continue

        for alarm_row in alarm_rows:
            if not isinstance(alarm_row, dict):
                continue

            alarm_id_value = _extract_numeric_value(alarm_row, "ALI")
            if alarm_id_value is None:
                continue

            alarm_id = int(alarm_id_value)
            alarm_key = ALARM_KEY_BY_ID.get(alarm_id)
            if alarm_key is None:
                continue

            alarm_status_value = _extract_numeric_value(alarm_row, "OR")
            alarm_status = (
                int(alarm_status_value) if alarm_status_value is not None else None
            )
            if not _alarm_is_active(alarm_id, alarm_status):
                continue

            alarm_state = parsed_state[alarm_key]
            alarm_state["active"] = True
            alarm_state["active_count"] = int(alarm_state["active_count"]) + 1

            if channel_number is not None:
                channels = alarm_state["channels"]
                if isinstance(channels, list) and channel_number not in channels:
                    channels.append(channel_number)

            raw_value = _extract_numeric_value(alarm_row, "TS")
            if raw_value is not None:
                latest_raw = alarm_state.get("latest_raw_value")
                if not isinstance(latest_raw, (int, float)) or raw_value > latest_raw:
                    alarm_state["latest_raw_value"] = float(raw_value)

    for alarm_state in parsed_state.values():
        channels = alarm_state.get("channels")
        if isinstance(channels, list):
            channels.sort()

    _LOGGER.debug(
        "Parsed local BLE alarms: %s",
        _format_debug_payload(parsed_state),
    )
    return parsed_state


def _parse_local_ble_snapshot(response: Any) -> LocalBleSnapshot:
    """Parse GetAllServiceValues payload into local runtime fields."""

    snapshot = LocalBleSnapshot()
    snapshot.schedule_zone_bois = _resolve_schedule_zone_bois_from_service_values(
        response
    )

    for service_entry in _extract_service_values(response):
        service_id = service_entry.get("SI")
        if not isinstance(service_id, int):
            service_id = service_entry.get("value")
        if not isinstance(service_id, int):
            continue

        characteristic_map = _extract_characteristic_map(service_entry)
        if service_id in (
            SERVICE_ID_MODE,
            SERVICE_ID_MODE_LEGACY,
            SERVICE_ID_PRIMARY_STATE,
        ):
            mode_characteristic = characteristic_map.get(DEVICE_CHARACTERISTIC_MODE)
            if mode_characteristic is not None:
                mode_value = _extract_numeric_value(mode_characteristic, "V")
                if mode_value is not None:
                    snapshot.primary_power_on = int(mode_value) == MODE_HOME_VALUE

            primary_energy_characteristic = characteristic_map.get(
                DEVICE_CHARACTERISTIC_ACTIVE_ENERGY
            )
            if primary_energy_characteristic is not None:
                energy_value = _extract_numeric_value(
                    primary_energy_characteristic, "V"
                )
                snapshot.primary_energy_kwh = _active_energy_to_kwh(energy_value)

        elif service_id == SERVICE_ID_HOT_WATER:
            boost_state_characteristic = characteristic_map.get(
                DEVICE_CHARACTERISTIC_HOT_WATER_STATE
            )
            if boost_state_characteristic is not None:
                boost_state_value = _extract_numeric_value(
                    boost_state_characteristic, "V"
                )
                override_type = _extract_numeric_value(boost_state_characteristic, "OT")
                duration_value = _extract_numeric_value(boost_state_characteristic, "D")

                if boost_state_value is not None:
                    snapshot.timed_boost_active = (
                        override_type is not None
                        and int(override_type) == OVERRIDE_TYPE_ADVANCE
                    )

                if duration_value is not None and duration_value > 0:
                    snapshot.timed_boost_duration_minutes = int(duration_value)

            timed_boost_enabled_characteristic = characteristic_map.get(
                DEVICE_CHARACTERISTIC_SCHEDULE_ENABLE_DISABLE
            )
            if timed_boost_enabled_characteristic is not None:
                enabled_value = _extract_numeric_value(
                    timed_boost_enabled_characteristic,
                    "V",
                )
                if enabled_value is not None:
                    snapshot.timed_boost_enabled = int(enabled_value) != 0

            boost_energy_characteristic = characteristic_map.get(
                DEVICE_CHARACTERISTIC_ACTIVE_ENERGY
            )
            if boost_energy_characteristic is not None:
                boost_energy_value = _extract_numeric_value(
                    boost_energy_characteristic, "V"
                )
                snapshot.boost_energy_kwh = _active_energy_to_kwh(boost_energy_value)

    _LOGGER.debug(
        "Parsed local BLE snapshot: primary_power_on=%s timed_boost_enabled=%s timed_boost_active=%s boost_duration=%s primary_energy_kwh=%s boost_energy_kwh=%s schedule_zone_bois=%s",
        snapshot.primary_power_on,
        snapshot.timed_boost_enabled,
        snapshot.timed_boost_active,
        snapshot.timed_boost_duration_minutes,
        snapshot.primary_energy_kwh,
        snapshot.boost_energy_kwh,
        snapshot.schedule_zone_bois,
    )

    return snapshot


def _command_to_rpc_payload(
    method_name: str,
    operation_kwargs: dict[str, Any],
    *,
    mode_service_boi: int,
    hot_water_service_boi: int,
) -> tuple[int, int, list[Any] | None]:
    """Translate a local command method name to RPC header and arguments."""

    if method_name == "start_timed_boost":
        duration = operation_kwargs.get("duration_minutes")
        if not isinstance(duration, int) or duration <= 0:
            raise ValueError("Boost duration must be a positive number of minutes")
        return (
            HANDLER_WRITE_DATA,
            SERVICE_ID_HOT_WATER,
            [hot_water_service_boi, {"D": duration, "I": 4, "OT": 2, "V": 0}],
        )

    if method_name == "stop_timed_boost":
        return (
            HANDLER_WRITE_DATA,
            SERVICE_ID_HOT_WATER,
            [hot_water_service_boi, {"D": 0, "I": 4, "OT": 2, "V": 0}],
        )

    if method_name == "set_timed_boost_enabled":
        enabled = operation_kwargs.get("enabled")
        if not isinstance(enabled, bool):
            raise ValueError("Timed boost enabled flag must be boolean")
        return (
            HANDLER_WRITE_DATA,
            SERVICE_ID_HOT_WATER,
            [hot_water_service_boi, {"I": 27, "V": 1 if enabled else 0}],
        )

    if method_name == "turn_controller_on":
        return (
            HANDLER_WRITE_DATA,
            SERVICE_ID_MODE,
            [mode_service_boi, {"I": 6, "V": 2}],
        )

    if method_name == "turn_controller_off":
        return (
            HANDLER_WRITE_DATA,
            SERVICE_ID_MODE,
            [mode_service_boi, {"I": 6, "V": 0}],
        )

    raise LocalBleCommissioningError(
        f"Unsupported local BLE command method: {method_name}"
    )


async def _async_resolve_mode_and_hot_water_service_bois(
    rpc_client: _BleUartRpcClient,
    *,
    gateway_mac_id: str,
) -> tuple[int, int]:
    """Resolve mode-service and hot-water-service BOIs with defaults."""

    response = await _async_get_all_service_values(
        rpc_client,
        gateway_mac_id=gateway_mac_id,
    )

    resolved_mode_service_boi, resolved_hot_water_service_boi = (
        _resolve_mode_and_hot_water_service_bois(response)
    )

    mode_service_boi = (
        resolved_mode_service_boi if resolved_mode_service_boi is not None else 1
    )
    hot_water_service_boi = (
        resolved_hot_water_service_boi if resolved_hot_water_service_boi is not None else 2
    )

    return mode_service_boi, hot_water_service_boi


async def async_read_local_snapshot(
    hass: HomeAssistant,
    *,
    mac_address: str,
    serial_number: str | None,
    ble_key: str,
) -> LocalBleSnapshot:
    """Read and parse local BLE service values for runtime sensors."""

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

    for attempt in range(BLE_COMMAND_RETRY_LIMIT + 1):
        try:
            return await _async_read_ble_snapshot_once(
                hass,
                ble_address=ble_address,
                expected_serial=serial_number,
                gateway_mac_id=gateway_mac_id,
                auth_key=decoded_key,
            )
        except LocalBleCommissioningError as error:
            if attempt >= BLE_COMMAND_RETRY_LIMIT:
                raise
            _LOGGER.warning(
                "BLE snapshot read failed (attempt %s/%s): %s, retrying",
                attempt + 1,
                BLE_COMMAND_RETRY_LIMIT + 1,
                error,
            )
            await asyncio.sleep(BLE_COMMAND_RETRY_DELAY_SECONDS)

    raise LocalBleCommissioningError("BLE snapshot retries exhausted")


async def _async_read_ble_snapshot_once(
    hass: HomeAssistant,
    *,
    ble_address: str,
    expected_serial: str | None,
    gateway_mac_id: str,
    auth_key: bytes,
) -> LocalBleSnapshot:
    """Connect over BLE once and read a snapshot payload."""

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
        await rpc_client.async_authorize(auth_key)
        response = await rpc_client.async_rpc_request(
            gateway_mac_id=gateway_mac_id,
            handler_id=HANDLER_GET_ALL_SERVICE_VALUES,
            service_id=SERVICE_ID_BBSERVER,
            args=None,
        )
        _LOGGER.debug(
            "GetAllServiceValues summary for %s: %s",
            ble_address,
            _summarize_service_values(response),
        )
        _LOGGER.debug(
            "GetAllServiceValues raw payload for %s: %s",
            ble_address,
            _format_debug_payload(response),
        )
        snapshot = _parse_local_ble_snapshot(response)

        try:
            alarms_response = await rpc_client.async_rpc_request(
                gateway_mac_id=gateway_mac_id,
                handler_id=HANDLER_GET_ALL_BB_ALARM,
                service_id=SERVICE_ID_BBSERVER,
                args=None,
            )
            _LOGGER.debug(
                "GetAllBBAlarm raw payload for %s: %s",
                ble_address,
                _format_debug_payload(alarms_response),
            )
            snapshot.alarms_state = _parse_local_ble_alarms(alarms_response)
        except LocalBleCommissioningError as error:
            _LOGGER.debug(
                "Failed to read local BLE alarms for %s: %s",
                ble_address,
                error,
            )

        dynamic_tariff_boi = _find_service_boi(
            response,
            service_id=SERVICE_ID_DYNAMIC_TARIFF,
        )
        consumption_args_candidates: list[list[int]] = [[1], [0]]
        if dynamic_tariff_boi is not None:
            consumption_args_candidates.append([dynamic_tariff_boi])

        deduped_consumption_args: list[list[int]] = []
        seen_consumption_args: set[tuple[int, ...]] = set()
        for consumption_args in consumption_args_candidates:
            key = tuple(consumption_args)
            if key in seen_consumption_args:
                continue
            seen_consumption_args.add(key)
            deduped_consumption_args.append(consumption_args)

        for consumption_args in deduped_consumption_args:
            try:
                consumption_response = await rpc_client.async_rpc_request(
                    gateway_mac_id=gateway_mac_id,
                    handler_id=HANDLER_GET_CONSUMPTION_STATE,
                    service_id=SERVICE_ID_DYNAMIC_TARIFF,
                    args=consumption_args,
                )
                _LOGGER.debug(
                    "GetConsumptionState raw payload for %s args=%s: %s",
                    ble_address,
                    consumption_args,
                    _format_debug_payload(consumption_response),
                )
                snapshot.consumption_days = _parse_consumption_day_rows(
                    consumption_response
                )
                has_consumption_energy = _consumption_rows_have_energy(
                    snapshot.consumption_days
                )
                snapshot.statistics_recent = _parse_consumption_state(
                    consumption_response
                )
                if snapshot.statistics_recent is not None:
                    primary_stats = snapshot.statistics_recent.get("primary")
                    if (
                        snapshot.primary_energy_kwh is None
                        and isinstance(primary_stats, dict)
                        and isinstance(primary_stats.get("energy_kwh"), (int, float))
                    ):
                        snapshot.primary_energy_kwh = float(primary_stats["energy_kwh"])

                    boost_stats = snapshot.statistics_recent.get("boost")
                    if (
                        snapshot.boost_energy_kwh is None
                        and isinstance(boost_stats, dict)
                        and isinstance(boost_stats.get("energy_kwh"), (int, float))
                    ):
                        snapshot.boost_energy_kwh = float(boost_stats["energy_kwh"])

                    _LOGGER.debug(
                        "Parsed statistics_recent from GetConsumptionState args=%s: %s",
                        consumption_args,
                        _format_debug_payload(snapshot.statistics_recent),
                    )
                    if (
                        has_consumption_energy
                        or snapshot.primary_energy_kwh is not None
                        or snapshot.boost_energy_kwh is not None
                    ):
                        break
            except LocalBleCommissioningError as error:
                _LOGGER.debug(
                    "Failed to read local BLE consumption state for args %s: %s",
                    consumption_args,
                    error,
                )

        if snapshot.statistics_recent is None:
            _LOGGER.debug(
                "No statistics_recent parsed from local BLE consumption responses for %s",
                ble_address,
            )

        return snapshot
    finally:
        await rpc_client.async_close()
        await _async_disconnect_client(client)


def _parse_local_weekly_program(payload: Any) -> "WeeklyProgram":
    """Parse one weekly program payload from local BLE RPC format."""

    from .beanbag import BeanbagBackend, BeanbagError  # noqa: PLC0415

    parse_payload = payload
    if isinstance(parse_payload, dict):
        values = parse_payload.get("V")
        if isinstance(values, list):
            parse_payload = values

    try:
        return BeanbagBackend._parse_weekly_program(parse_payload)
    except BeanbagError as error:
        raise LocalBleCommissioningError(
            f"Failed to parse local BLE weekly program payload: {error}"
        ) from error


async def async_read_local_weekly_programs(
    hass: HomeAssistant,
    *,
    mac_address: str,
    serial_number: str | None,
    ble_key: str,
    zone_bois: Mapping[str, int] | None = None,
) -> tuple[
    dict[str, "WeeklyProgram | None"],
    dict[str, list[tuple[int, int]] | None],
]:
    """Read primary and boost weekly schedules from local BLE RPC."""

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

    for attempt in range(BLE_COMMAND_RETRY_LIMIT + 1):
        try:
            return await _async_read_ble_weekly_programs_once(
                hass,
                ble_address=ble_address,
                expected_serial=serial_number,
                gateway_mac_id=gateway_mac_id,
                auth_key=decoded_key,
                zone_bois=zone_bois,
            )
        except LocalBleCommissioningError as error:
            if attempt >= BLE_COMMAND_RETRY_LIMIT:
                raise
            _LOGGER.warning(
                "BLE weekly schedule read failed (attempt %s/%s): %s, retrying",
                attempt + 1,
                BLE_COMMAND_RETRY_LIMIT + 1,
                error,
            )
            await asyncio.sleep(BLE_COMMAND_RETRY_DELAY_SECONDS)

    raise LocalBleCommissioningError("BLE weekly schedule retries exhausted")


async def _async_read_ble_weekly_programs_once(
    hass: HomeAssistant,
    *,
    ble_address: str,
    expected_serial: str | None,
    gateway_mac_id: str,
    auth_key: bytes,
    zone_bois: Mapping[str, int] | None = None,
) -> tuple[
    dict[str, "WeeklyProgram | None"],
    dict[str, list[tuple[int, int]] | None],
]:
    """Connect once and read both weekly programs from local BLE RPC."""

    from .schedule import canonicalize_weekly  # noqa: PLC0415

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
        await rpc_client.async_authorize(auth_key)
        resolved_zone_bois = await _async_resolve_schedule_zone_bois(
            rpc_client,
            gateway_mac_id=gateway_mac_id,
            fallback_zone_bois=zone_bois,
        )

        programs: dict[str, Any] = {}
        canonicals: dict[str, list[tuple[int, int]] | None] = {}

        for zone_key in PROGRAM_ZONE_INDEX:
            zone_index = resolved_zone_bois[zone_key]
            response = await rpc_client.async_rpc_request(
                gateway_mac_id=gateway_mac_id,
                handler_id=HANDLER_READ_WEEKLY_PROGRAM,
                service_id=SERVICE_ID_SCHEDULE,
                args=[zone_index],
            )
            _LOGGER.debug(
                "ReadWeeklyProgram raw payload for %s zone=%s idx=%s: %s",
                ble_address,
                zone_key,
                zone_index,
                _format_debug_payload(response),
            )

            program = _parse_local_weekly_program(response)
            programs[zone_key] = program
            canonicals[zone_key] = canonicalize_weekly(program)

        return programs, canonicals
    finally:
        await rpc_client.async_close()
        await _async_disconnect_client(client)


async def async_write_local_weekly_program(
    hass: HomeAssistant,
    *,
    mac_address: str,
    serial_number: str | None,
    ble_key: str,
    zone: str,
    program: "WeeklyProgram",
    zone_bois: Mapping[str, int] | None = None,
) -> None:
    """Write a weekly schedule for one zone over local BLE RPC."""

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

    if zone not in PROGRAM_ZONE_INDEX:
        raise ValueError(f"Unsupported weekly schedule zone: {zone}")

    for attempt in range(BLE_COMMAND_RETRY_LIMIT + 1):
        try:
            await _async_write_local_weekly_program_once(
                hass,
                ble_address=ble_address,
                expected_serial=serial_number,
                gateway_mac_id=gateway_mac_id,
                auth_key=decoded_key,
                zone=zone,
                program=program,
                zone_bois=zone_bois,
            )
            return
        except LocalBleCommissioningError as error:
            if attempt >= BLE_COMMAND_RETRY_LIMIT:
                raise
            _LOGGER.warning(
                "BLE weekly schedule write failed (attempt %s/%s): %s, retrying",
                attempt + 1,
                BLE_COMMAND_RETRY_LIMIT + 1,
                error,
            )
            await asyncio.sleep(BLE_COMMAND_RETRY_DELAY_SECONDS)

    raise LocalBleCommissioningError("BLE weekly schedule write retries exhausted")


async def _async_write_local_weekly_program_once(
    hass: HomeAssistant,
    *,
    ble_address: str,
    expected_serial: str | None,
    gateway_mac_id: str,
    auth_key: bytes,
    zone: str,
    program: "WeeklyProgram",
    zone_bois: Mapping[str, int] | None = None,
) -> None:
    """Connect once and write one weekly program payload over BLE."""

    from .beanbag import BeanbagBackend  # noqa: PLC0415

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
        await rpc_client.async_authorize(auth_key)
        resolved_zone_bois = await _async_resolve_schedule_zone_bois(
            rpc_client,
            gateway_mac_id=gateway_mac_id,
            fallback_zone_bois=zone_bois,
        )
        zone_index = resolved_zone_bois[zone]
        payload = BeanbagBackend._build_weekly_program_payload(program, zone_index)
        acknowledgement = await rpc_client.async_rpc_request(
            gateway_mac_id=gateway_mac_id,
            handler_id=HANDLER_WRITE_WEEKLY_PROGRAM,
            service_id=SERVICE_ID_SCHEDULE,
            args=payload,
        )
        if acknowledgement not in (0, "0", None):
            raise LocalBleCommissioningError(
                f"Unexpected local BLE weekly schedule acknowledgement: {acknowledgement}"
            )
    finally:
        await rpc_client.async_close()
        await _async_disconnect_client(client)


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
        mode_service_boi, hot_water_service_boi = (
            await _async_resolve_mode_and_hot_water_service_bois(
                rpc_client,
                gateway_mac_id=gateway_mac_id,
            )
        )
        handler_id, service_id, args = _command_to_rpc_payload(
            method_name,
            operation_kwargs,
            mode_service_boi=mode_service_boi,
            hot_water_service_boi=hot_water_service_boi,
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

"""Tests for local BLE commissioning helpers."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any
import sys
from unittest.mock import AsyncMock, Mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from custom_components.securemtr.local_ble_commissioning import (
    FINAL_PACKET_INDEX,
    LocalBleSnapshot,
    _BleUartRpcClient,
    LocalBleCommissioningError,
    _async_commission_over_rpc,
    _async_read_ble_snapshot_once,
    _async_read_ble_weekly_programs_once,
    _async_resolve_service_bois,
    _async_set_owner_with_retry,
    _command_to_rpc_payload,
    _parse_consumption_day_rows,
    _parse_consumption_state,
    _parse_local_weekly_program,
    _extract_length_prefixed_payload,
    _parse_local_ble_snapshot,
    _format_mac_address,
    _java_utf8_character_length,
    _packetize_payload,
    _reassemble_packet_bytes,
    _validate_advertisement_identity,
    async_read_local_snapshot,
    async_execute_local_command,
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

    assert _extract_length_prefixed_payload(_reassemble_packet_bytes(packets)) == body


def test_reassemble_payload_rejects_missing_final_packet() -> None:
    """Ensure reconstruction rejects responses missing packet 255."""

    with pytest.raises(LocalBleCommissioningError):
        _reassemble_packet_bytes({1: b"abcd"})


def test_command_to_rpc_payload_start_timed_boost() -> None:
    """Ensure start_timed_boost maps to documented write-data payload."""

    handler_id, service_id, args = _command_to_rpc_payload(
        "start_timed_boost",
        {"duration_minutes": 30},
        mode_boi=11,
        hot_water_boi=22,
    )

    assert handler_id == 2
    assert service_id == 16
    assert args == [22, {"D": 30, "I": 4, "OT": 2, "V": 0}]


def test_command_to_rpc_payload_rejects_unknown_command() -> None:
    """Ensure unsupported local command names fail fast."""

    with pytest.raises(LocalBleCommissioningError):
        _command_to_rpc_payload(
            "read_zone_topology",
            {},
            mode_boi=1,
            hot_water_boi=2,
        )


def test_java_utf8_character_length_mimics_java_decode() -> None:
    """Ensure UTF-8 character length follows Java replacement behavior."""

    assert _java_utf8_character_length(b"abc") == 3
    assert _java_utf8_character_length(b"\xff") == 1


@pytest.mark.asyncio
async def test_async_resolve_service_bois_parses_value_field() -> None:
    """Ensure BOI resolution accepts the decompiled ServiceValuesDTO `value` key."""

    class FakeRpcClient:
        async def async_rpc_request(self, **kwargs: Any) -> Any:
            return {"V": [{"value": 15, "I": 11}, {"value": 16, "I": 22}]}

    mode_boi, hot_water_boi = await _async_resolve_service_bois(
        FakeRpcClient(),
        gateway_mac_id="12345",
    )

    assert mode_boi == 11
    assert hot_water_boi == 22


def test_parse_local_ble_snapshot_maps_runtime_fields() -> None:
    """Ensure local snapshot parsing maps power, boost, and energy fields."""

    response = {
        "V": [
            {
                "SI": 15,
                "I": 11,
                "V": [
                    {"I": 6, "V": 2},
                    {"I": 19, "V": 12345},
                ],
            },
            {
                "SI": 16,
                "I": 22,
                "V": [
                    {"I": 27, "V": 1},
                    {"I": 4, "V": 1, "OT": 2, "D": 30},
                    {"I": 19, "V": 4567},
                ],
            },
        ]
    }

    snapshot = _parse_local_ble_snapshot(response)

    assert snapshot.primary_power_on is True
    assert snapshot.timed_boost_enabled is True
    assert snapshot.timed_boost_active is True
    assert snapshot.timed_boost_duration_minutes == 30
    assert snapshot.primary_energy_kwh == pytest.approx(12.345)
    assert snapshot.boost_energy_kwh == pytest.approx(4.567)


def test_parse_local_ble_snapshot_accepts_primary_state_service() -> None:
    """Ensure primary-state service values are accepted for local runtime fields."""

    response = {
        "V": [
            {
                "SI": 33,
                "I": 11,
                "V": [
                    {"I": 6, "V": 2},
                    {"I": 19, "V": 9876},
                ],
            }
        ]
    }

    snapshot = _parse_local_ble_snapshot(response)

    assert snapshot.primary_power_on is True
    assert snapshot.primary_energy_kwh == pytest.approx(9.876)


def test_parse_local_ble_snapshot_marks_scheduled_hot_water_not_boost() -> None:
    """Ensure scheduled hot-water on state clears timed boost activity."""

    response = {
        "V": [
            {
                "SI": 16,
                "I": 22,
                "V": [
                    {"I": 4, "V": 1, "OT": 0, "D": 30},
                ],
            }
        ]
    }

    snapshot = _parse_local_ble_snapshot(response)

    assert snapshot.timed_boost_active is False


def test_parse_consumption_state_maps_duration_hours() -> None:
    """Ensure dynamic tariff durations map to runtime and scheduled hours."""

    response = [
        {
            "I": 5,
            "D": [
                {
                    "T": 1_712_000_000,
                    "OA": 120,
                    "BA": 30,
                    "OS": 180,
                    "BS": 45,
                }
            ],
        }
    ]

    recent = _parse_consumption_state(response)

    assert recent is not None
    assert recent["primary"]["runtime_hours"] == pytest.approx(2.0)
    assert recent["primary"]["scheduled_hours"] == pytest.approx(3.0)
    assert recent["primary"]["energy_kwh"] is None
    assert recent["boost"]["runtime_hours"] == pytest.approx(0.5)
    assert recent["boost"]["scheduled_hours"] == pytest.approx(0.75)
    assert recent["boost"]["energy_kwh"] is None
    assert recent["primary"]["report_day"] == "2024-04-01"


def test_parse_consumption_state_accepts_single_payload_dict() -> None:
    """Ensure consumption parsing accepts direct {I,D} payload dictionaries."""

    response = {
        "I": 0,
        "D": [
            {
                "T": 1_712_000_000,
                "OA": 60,
                "BA": 30,
                "OS": 90,
                "BS": 45,
            }
        ],
    }

    recent = _parse_consumption_state(response)

    assert recent is not None
    assert recent["primary"]["runtime_hours"] == pytest.approx(1.0)
    assert recent["boost"]["runtime_hours"] == pytest.approx(0.5)


def test_parse_consumption_state_accepts_flat_day_rows() -> None:
    """Ensure consumption parsing accepts direct DynamicDaySchedule rows."""

    response = [
        {
            "T": 1_712_100_000,
            "OA": 30,
            "BA": 15,
            "OS": 60,
            "BS": 45,
        }
    ]

    recent = _parse_consumption_state(response)

    assert recent is not None
    assert recent["primary"]["runtime_hours"] == pytest.approx(0.5)
    assert recent["boost"]["runtime_hours"] == pytest.approx(0.25)


def test_parse_consumption_state_accepts_single_day_dict() -> None:
    """Ensure consumption parsing accepts payloads where D is a single object."""

    response = {
        "D": {
            "T": 1_712_200_000,
            "OA": 90,
            "BA": 30,
            "OS": 120,
            "BS": 60,
        }
    }

    recent = _parse_consumption_state(response)

    assert recent is not None
    assert recent["primary"]["runtime_hours"] == pytest.approx(1.5)
    assert recent["boost"]["scheduled_hours"] == pytest.approx(1.0)


def test_parse_consumption_state_defaults_missing_scheduled_to_zero() -> None:
    """Ensure missing scheduled duration fields default to zero hours."""

    response = [
        {
            "T": 1_712_300_000,
            "OA": 75,
            "BA": 20,
        }
    ]

    recent = _parse_consumption_state(response)

    assert recent is not None
    assert recent["primary"]["scheduled_hours"] == pytest.approx(0.0)
    assert recent["boost"]["scheduled_hours"] == pytest.approx(0.0)


def test_parse_consumption_state_accepts_string_duration_values() -> None:
    """Ensure string-encoded duration values are parsed like app Gson floats."""

    response = {
        "D": {
            "T": "1712400000",
            "OA": "120",
            "BA": "30",
            "OS": "180",
            "BS": "45",
        }
    }

    recent = _parse_consumption_state(response)

    assert recent is not None
    assert recent["primary"]["runtime_hours"] == pytest.approx(2.0)
    assert recent["boost"]["scheduled_hours"] == pytest.approx(0.75)


def test_parse_consumption_state_maps_energy_fields() -> None:
    """Ensure consumption parser maps OP/BP fields into kWh values."""

    response = {
        "D": {
            "T": 1_712_400_000,
            "OA": 120,
            "BA": 30,
            "OS": 180,
            "BS": 45,
            "OP": 1234,
            "BP": 567,
        }
    }

    recent = _parse_consumption_state(response)

    assert recent is not None
    assert recent["primary"]["energy_kwh"] == pytest.approx(1.234)
    assert recent["boost"]["energy_kwh"] == pytest.approx(0.567)


def test_parse_consumption_day_rows_keeps_energy_and_duration_data() -> None:
    """Ensure parsed day rows preserve OP/BP and OA/OS/BA/BS values."""

    response = [
        {
            "T": 1_712_500_000,
            "OA": "60",
            "BA": "30",
            "OS": "90",
            "BS": "45",
            "OP": "2000",
            "BP": "500",
        }
    ]

    rows = _parse_consumption_day_rows(response)

    assert len(rows) == 1
    assert rows[0]["primary_runtime_hours"] == pytest.approx(1.0)
    assert rows[0]["boost_scheduled_hours"] == pytest.approx(0.75)
    assert rows[0]["primary_energy_kwh"] == pytest.approx(2.0)
    assert rows[0]["boost_energy_kwh"] == pytest.approx(0.5)


def test_parse_local_weekly_program_accepts_dict_payload() -> None:
    """Ensure local weekly program parsing accepts wrapped dictionary payloads."""

    transitions = [{"O": 65535, "T": 255} for _ in range(42)]
    transitions[0] = {"O": 60, "T": 1}
    transitions[1] = {"O": 120, "T": 0}

    program = _parse_local_weekly_program({"V": [{"I": 1, "D": transitions}]})

    assert program[0].on_minutes[0] == 60
    assert program[0].off_minutes[0] == 120
    assert len(program) == 7


@pytest.mark.asyncio
async def test_async_read_local_snapshot_retries_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure local snapshot reads retry transient BLE errors."""

    snapshot = LocalBleSnapshot(primary_power_on=True)
    read_once = AsyncMock(side_effect=[LocalBleCommissioningError("boom"), snapshot])
    sleep = AsyncMock()

    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._async_read_ble_snapshot_once",
        read_once,
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning.asyncio.sleep",
        sleep,
    )

    result = await async_read_local_snapshot(
        SimpleNamespace(),
        mac_address="AABBCCDDEEFF",
        serial_number="A1B2C3D4",
        ble_key="ABEiM0RVZneImaq7zN3u/w==",
    )

    assert result is snapshot
    assert read_once.await_count == 2
    sleep.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_read_ble_snapshot_once_reads_consumption_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure BLE snapshot read requests dynamic tariff consumption state."""

    fake_hass = SimpleNamespace()
    fake_device = SimpleNamespace(address="AA:BB:CC:DD:EE:FF")
    fake_client = SimpleNamespace()

    class FakeRpcClient:
        def __init__(self, client: Any) -> None:
            self.client = client
            self.requests: list[dict[str, Any]] = []

        async def async_initialize(self) -> None:
            return None

        async def async_authorize(self, key: bytes) -> None:
            return None

        async def async_rpc_request(self, **kwargs: Any) -> Any:
            self.requests.append(kwargs)
            if kwargs["handler_id"] == 3 and kwargs["service_id"] == 1:
                return {
                    "V": [
                        {"SI": 15, "I": 1, "V": [{"I": 6, "V": 2}]},
                        {"SI": 16, "I": 2, "V": [{"I": 4, "V": 0, "OT": 2}]},
                        {"SI": 36, "I": 5, "V": []},
                    ]
                }
            return [
                {
                    "I": 5,
                    "D": [
                        {
                            "T": 1_712_000_000,
                            "OA": 60,
                            "BA": 30,
                            "OS": 120,
                            "BS": 45,
                            "OP": 1234,
                            "BP": 567,
                        }
                    ],
                }
            ]

        async def async_close(self) -> None:
            return None

    resolve = AsyncMock(return_value=fake_device)
    connect = AsyncMock(return_value=fake_client)
    disconnect = AsyncMock()
    rpc_instances: list[FakeRpcClient] = []

    def _rpc_factory(client: Any) -> FakeRpcClient:
        instance = FakeRpcClient(client)
        rpc_instances.append(instance)
        return instance

    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._async_resolve_connectable_device",
        resolve,
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._async_connect_client",
        connect,
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._async_disconnect_client",
        disconnect,
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._BleUartRpcClient",
        _rpc_factory,
    )

    snapshot = await _async_read_ble_snapshot_once(
        fake_hass,
        ble_address="AA:BB:CC:DD:EE:FF",
        expected_serial="E0044275",
        gateway_mac_id="12345",
        auth_key=bytes.fromhex("00112233445566778899aabbccddeeff"),
    )

    assert len(rpc_instances) == 1
    requests = rpc_instances[0].requests
    assert requests[0]["handler_id"] == 3
    assert requests[0]["service_id"] == 1
    assert requests[1]["handler_id"] == 9
    assert requests[1]["service_id"] == 36
    assert requests[1]["args"] == [1]
    assert snapshot.statistics_recent is not None
    assert snapshot.statistics_recent["primary"]["runtime_hours"] == pytest.approx(1.0)
    assert snapshot.primary_energy_kwh == pytest.approx(1.234)
    assert snapshot.boost_energy_kwh == pytest.approx(0.567)
    assert snapshot.consumption_days is not None
    assert snapshot.consumption_days[0]["primary_energy_kwh"] == pytest.approx(1.234)
    disconnect.assert_awaited_once_with(fake_client)


@pytest.mark.asyncio
async def test_async_read_ble_weekly_programs_once_reads_both_zones(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure local BLE weekly schedule read requests both primary and boost zones."""

    fake_hass = SimpleNamespace()
    fake_device = SimpleNamespace(address="AA:BB:CC:DD:EE:FF")
    fake_client = SimpleNamespace()

    transitions = [{"O": 65535, "T": 255} for _ in range(42)]
    transitions[0] = {"O": 60, "T": 1}
    transitions[1] = {"O": 120, "T": 0}

    class FakeRpcClient:
        def __init__(self, client: Any) -> None:
            self.client = client
            self.requests: list[dict[str, Any]] = []

        async def async_initialize(self) -> None:
            return None

        async def async_authorize(self, key: bytes) -> None:
            return None

        async def async_rpc_request(self, **kwargs: Any) -> Any:
            self.requests.append(kwargs)
            return [{"I": kwargs["args"][0], "D": transitions}]

        async def async_close(self) -> None:
            return None

    resolve = AsyncMock(return_value=fake_device)
    connect = AsyncMock(return_value=fake_client)
    disconnect = AsyncMock()
    rpc_instances: list[FakeRpcClient] = []

    def _rpc_factory(client: Any) -> FakeRpcClient:
        instance = FakeRpcClient(client)
        rpc_instances.append(instance)
        return instance

    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._async_resolve_connectable_device",
        resolve,
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._async_connect_client",
        connect,
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._async_disconnect_client",
        disconnect,
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._BleUartRpcClient",
        _rpc_factory,
    )

    programs, canonicals = await _async_read_ble_weekly_programs_once(
        fake_hass,
        ble_address="AA:BB:CC:DD:EE:FF",
        expected_serial="E0044275",
        gateway_mac_id="12345",
        auth_key=bytes.fromhex("00112233445566778899aabbccddeeff"),
    )

    assert len(rpc_instances) == 1
    requests = rpc_instances[0].requests
    assert requests[0]["handler_id"] == 22
    assert requests[0]["service_id"] == 17
    assert requests[0]["args"] == [1]
    assert requests[1]["args"] == [2]
    assert programs["primary"] is not None
    assert programs["boost"] is not None
    assert canonicals["primary"] == [(60, 120)]
    assert canonicals["boost"] == [(60, 120)]
    disconnect.assert_awaited_once_with(fake_client)


@pytest.mark.asyncio
async def test_async_rpc_request_ignores_mismatched_response_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure RPC request waits for matching response ID when stray replies arrive."""

    rpc_client = _BleUartRpcClient(SimpleNamespace())
    monkeypatch.setattr(rpc_client, "_async_send_payload", AsyncMock())
    reset_state = Mock()
    monkeypatch.setattr(rpc_client, "_reset_response_state", reset_state)
    monkeypatch.setattr(
        rpc_client,
        "_async_wait_for_response_payload",
        AsyncMock(
            side_effect=[
                b'{"I":"wrong","R":0}',
                b'{"I":"123-456","R":0}',
            ]
        ),
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning.random.randint",
        lambda _a, _b: 456,
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning.time.time",
        lambda: 123,
    )

    result = await rpc_client.async_rpc_request(
        gateway_mac_id="12345",
        handler_id=1,
        service_id=2,
        args=None,
    )

    assert result == 0
    assert rpc_client._async_wait_for_response_payload.await_count == 2
    reset_state.assert_called_once()


@pytest.mark.asyncio
async def test_async_initialize_recovers_notify_already_acquired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure TX notification setup retries once when BlueZ reports acquisition."""

    fake_client = SimpleNamespace(
        services=None,
        start_notify=AsyncMock(side_effect=[RuntimeError("Notify acquired"), None]),
        stop_notify=AsyncMock(),
    )
    rpc_client = _BleUartRpcClient(fake_client)
    sleep = AsyncMock()
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning.asyncio.sleep",
        sleep,
    )

    await rpc_client.async_initialize()

    assert fake_client.start_notify.await_count == 2
    fake_client.stop_notify.assert_awaited_once()
    sleep.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_initialize_wraps_notify_failure() -> None:
    """Ensure TX notification setup raises integration error on hard failure."""

    fake_client = SimpleNamespace(
        services=None,
        start_notify=AsyncMock(side_effect=RuntimeError("boom")),
        stop_notify=AsyncMock(),
    )
    rpc_client = _BleUartRpcClient(fake_client)

    with pytest.raises(LocalBleCommissioningError, match="TX notifications"):
        await rpc_client.async_initialize()


@pytest.mark.asyncio
async def test_async_send_payload_wraps_write_failures() -> None:
    """Ensure UART write errors are normalized as commissioning errors."""

    fake_client = SimpleNamespace(
        write_gatt_char=AsyncMock(side_effect=RuntimeError("bad"))
    )
    rpc_client = _BleUartRpcClient(fake_client)

    with pytest.raises(LocalBleCommissioningError, match="BLE UART packet"):
        await rpc_client._async_send_payload(b"abc", use_java_utf8_length=False)


@pytest.mark.asyncio
async def test_async_rpc_request_raises_timeout_after_id_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure RPC request surfaces timeout if no matching response ID arrives."""

    rpc_client = _BleUartRpcClient(SimpleNamespace())
    monkeypatch.setattr(rpc_client, "_async_send_payload", AsyncMock())
    reset_state = Mock()
    monkeypatch.setattr(rpc_client, "_reset_response_state", reset_state)
    monkeypatch.setattr(
        rpc_client,
        "_async_wait_for_response_payload",
        AsyncMock(
            side_effect=[
                b'{"I":"wrong","R":0}',
                LocalBleCommissioningError("Timed out waiting for BLE response"),
            ]
        ),
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning.random.randint",
        lambda _a, _b: 456,
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning.time.time",
        lambda: 123,
    )

    with pytest.raises(LocalBleCommissioningError, match="Timed out"):
        await rpc_client.async_rpc_request(
            gateway_mac_id="12345",
            handler_id=1,
            service_id=2,
            args=None,
            timeout=0.1,
        )

    reset_state.assert_called_once()


def test_format_mac_address_colonizes_compact_mac() -> None:
    """Ensure compact MAC values are transformed to canonical BLE address form."""

    assert _format_mac_address("AABBCCDDEEFF") == "AA:BB:CC:DD:EE:FF"


def test_validate_advertisement_identity_allows_missing_service_uuid_metadata() -> None:
    """Ensure UUID validation degrades gracefully when metadata is unavailable."""

    service_info = SimpleNamespace(
        address="AA:BB:CC:DD:EE:FF",
        service_uuids=None,
        name="E0044275",
        advertisement=None,
        device=SimpleNamespace(name="E0044275", details=None),
        manufacturer_data=None,
    )

    _validate_advertisement_identity(
        service_info,
        expected_address="AA:BB:CC:DD:EE:FF",
        expected_serial="E0044275",
    )


def test_validate_advertisement_identity_rejects_explicit_wrong_uuid() -> None:
    """Ensure UUID validation rejects advertisements with known wrong UUID lists."""

    service_info = SimpleNamespace(
        address="AA:BB:CC:DD:EE:FF",
        service_uuids=["0000180f-0000-1000-8000-00805f9b34fb"],
        name="E0044275",
        advertisement=None,
        device=SimpleNamespace(name="E0044275", details=None),
        manufacturer_data=None,
    )

    with pytest.raises(LocalBleCommissioningError, match="UART service UUID"):
        _validate_advertisement_identity(
            service_info,
            expected_address="AA:BB:CC:DD:EE:FF",
            expected_serial="E0044275",
        )


@pytest.mark.asyncio
async def test_async_commission_over_rpc_uses_already_paired_path() -> None:
    """Ensure commissioning tries GetBLEKey first, skipping SetOwner."""

    calls: list[dict[str, Any]] = []

    class FakeRpcClient:
        async def async_rpc_request(self, **kwargs: Any) -> Any:
            calls.append(kwargs)
            if kwargs["handler_id"] == 50:
                return "base64-ble-key"
            return "owner-token"

    credentials = await _async_commission_over_rpc(
        FakeRpcClient(),
        gateway_mac_id="12345",
        timezone_id=2,
        owner_email="homeassistant@local",
        receiver_name="SecureMTR A1B2C3D4",
    )

    assert [call["handler_id"] for call in calls] == [50]
    assert [call["service_id"] for call in calls] == [11]
    assert credentials["local_owner_token"] is None
    assert credentials["local_ble_key"] == "base64-ble-key"


@pytest.mark.asyncio
async def test_async_commission_over_rpc_falls_back_to_full_sequence() -> None:
    """Ensure full commissioning runs when already-paired path fails."""

    calls: list[dict[str, Any]] = []
    get_ble_key_attempt = 0

    class FakeRpcClient:
        async def async_rpc_request(self, **kwargs: Any) -> Any:
            nonlocal get_ble_key_attempt
            calls.append(kwargs)
            if kwargs["handler_id"] == 50:
                get_ble_key_attempt += 1
                if get_ble_key_attempt <= 2:
                    return ""
                return "base64-ble-key"
            if kwargs["handler_id"] == 7 and kwargs["service_id"] == 151:
                return "owner-token"
            return "ok"

    credentials = await _async_commission_over_rpc(
        FakeRpcClient(),
        gateway_mac_id="12345",
        timezone_id=2,
        owner_email="homeassistant@local",
        receiver_name="SecureMTR A1B2C3D4",
    )

    # Already-paired: GetBLEKey (empty) -> SetOwner -> GetBLEKey (empty) -> fails
    # Full sequence: SetTimeZone -> SetTime -> UpdateDevice -> SetOwner -> GetBLEKey (ok) -> AddHAN
    assert [call["handler_id"] for call in calls] == [50, 7, 50, 7, 2, 6, 7, 50, 48]
    assert [call["service_id"] for call in calls] == [
        11,
        151,
        11,
        103,
        103,
        11,
        151,
        11,
        11,
    ]
    assert credentials["local_ble_key"] == "base64-ble-key"


@pytest.mark.asyncio
async def test_async_commission_over_rpc_rejects_missing_ble_key() -> None:
    """Ensure commissioning fails if gateway never returns BLE key."""

    class FakeRpcClient:
        async def async_rpc_request(self, **kwargs: Any) -> Any:
            if kwargs["handler_id"] == 50:
                return ""
            if kwargs["handler_id"] == 7 and kwargs["service_id"] == 151:
                return "owner-token"
            return "ok"

    with pytest.raises(LocalBleCommissioningError):
        await _async_commission_over_rpc(
            FakeRpcClient(),
            gateway_mac_id="12345",
            timezone_id=2,
            owner_email="homeassistant@local",
            receiver_name="SecureMTR A1B2C3D4",
        )


@pytest.mark.asyncio
async def test_async_commission_over_rpc_rejects_wifi_step_placeholder() -> None:
    """Ensure optional wifi commissioning step fails fast until implemented."""

    get_ble_key_attempt = 0

    class FakeRpcClient:
        async def async_rpc_request(self, **kwargs: Any) -> Any:
            nonlocal get_ble_key_attempt
            if kwargs["handler_id"] == 50:
                get_ble_key_attempt += 1
                if get_ble_key_attempt <= 2:
                    return ""
                return "base64-ble-key"
            if kwargs["handler_id"] == 7 and kwargs["service_id"] == 151:
                return "owner-token"
            return "ok"

    with pytest.raises(LocalBleCommissioningError, match="not implemented"):
        await _async_commission_over_rpc(
            FakeRpcClient(),
            gateway_mac_id="12345",
            timezone_id=2,
            owner_email="homeassistant@local",
            receiver_name="SecureMTR A1B2C3D4",
            include_wifi_step=True,
        )


@pytest.mark.asyncio
async def test_set_owner_retries_timeout_error_20001() -> None:
    """Ensure SetOwner retries five times for gateway timeout error 20001."""

    attempts = 0

    class FakeRpcClient:
        async def async_rpc_request(self, **kwargs: Any) -> Any:
            nonlocal attempts
            attempts += 1
            if attempts <= 3:
                raise LocalBleCommissioningError("timeout", error_code=20001)
            return "owner-token"

    result = await _async_set_owner_with_retry(
        FakeRpcClient(),
        gateway_mac_id="12345",
        owner_email="homeassistant@local",
        receiver_name="SecureMTR A1B2C3D4",
    )

    assert attempts == 4
    assert result == "owner-token"


@pytest.mark.asyncio
async def test_set_owner_stops_after_retry_limit() -> None:
    """Ensure SetOwner raises after exhausting timeout retries."""

    class FakeRpcClient:
        async def async_rpc_request(self, **kwargs: Any) -> Any:
            raise LocalBleCommissioningError("timeout", error_code=20001)

    with pytest.raises(LocalBleCommissioningError):
        await _async_set_owner_with_retry(
            FakeRpcClient(),
            gateway_mac_id="12345",
            owner_email="homeassistant@local",
            receiver_name="SecureMTR A1B2C3D4",
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
    resolve.assert_awaited_once_with(
        fake_hass,
        "AA:BB:CC:DD:EE:FF",
        expected_serial="A1B2C3D4",
    )
    connect.assert_awaited_once()
    assert connect.await_args.args == (fake_device,)
    assert "ble_device_callback" in connect.await_args.kwargs
    commission.assert_awaited_once()
    assert commission.await_args.kwargs["receiver_name"] == "SecureMTR A1B2C3D4"
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


@pytest.mark.asyncio
async def test_async_execute_local_command_orchestrates_auth_and_rpc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure local command execution resolves, authenticates, and dispatches RPC."""

    fake_hass = SimpleNamespace()
    fake_device = SimpleNamespace(address="AA:BB:CC:DD:EE:FF")
    fake_client = SimpleNamespace()

    class FakeRpcClient:
        def __init__(self, client: Any) -> None:
            self.client = client
            self.authorized: bytes | None = None
            self.requests: list[dict[str, Any]] = []

        async def async_initialize(self) -> None:
            return None

        async def async_authorize(self, key: bytes) -> None:
            self.authorized = key

        async def async_rpc_request(self, **kwargs: Any) -> Any:
            self.requests.append(kwargs)
            if kwargs["handler_id"] == 3 and kwargs["service_id"] == 1:
                return {"V": [{"SI": 15, "I": 11}, {"SI": 16, "I": 22}]}
            return 0

        async def async_close(self) -> None:
            return None

    resolve = AsyncMock(return_value=fake_device)
    connect = AsyncMock(return_value=fake_client)
    disconnect = AsyncMock()
    rpc_instances: list[FakeRpcClient] = []

    def _rpc_factory(client: Any) -> FakeRpcClient:
        instance = FakeRpcClient(client)
        rpc_instances.append(instance)
        return instance

    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._async_resolve_connectable_device",
        resolve,
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._async_connect_client",
        connect,
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._async_disconnect_client",
        disconnect,
    )
    monkeypatch.setattr(
        "custom_components.securemtr.local_ble_commissioning._BleUartRpcClient",
        _rpc_factory,
    )

    await async_execute_local_command(
        fake_hass,
        mac_address="AABBCCDDEEFF",
        serial_number="A1B2C3D4",
        ble_key="ABEiM0RVZneImaq7zN3u/w==",
        method_name="start_timed_boost",
        operation_kwargs={"duration_minutes": 30},
    )

    resolve.assert_awaited_once_with(
        fake_hass,
        "AA:BB:CC:DD:EE:FF",
        expected_serial="A1B2C3D4",
    )
    connect.assert_awaited_once()
    assert len(rpc_instances) == 1
    instance = rpc_instances[0]
    assert instance.authorized == bytes.fromhex("00112233445566778899aabbccddeeff")
    assert instance.requests[0]["handler_id"] == 3
    assert instance.requests[0]["service_id"] == 1
    assert instance.requests[1]["handler_id"] == 2
    assert instance.requests[1]["service_id"] == 16
    assert instance.requests[1]["args"] == [22, {"D": 30, "I": 4, "OT": 2, "V": 0}]
    disconnect.assert_awaited_once_with(fake_client)

from __future__ import annotations

from datetime import date
from typing import Any, cast

import pytest

from custom_components.securemtr.energy import EnergyAccumulator


class FakeStore:
    """Provide an in-memory persistence helper for accumulator tests."""

    def __init__(self, initial: dict[str, object] | None = None) -> None:
        self.data = initial
        self.saved: list[dict[str, object]] = []

    async def async_load(self) -> dict[str, object] | None:
        """Return previously persisted data."""

        return self.data

    async def async_save(self, data: dict[str, object]) -> None:
        """Record the saved payload for inspection."""

        self.data = data
        self.saved.append(data)


@pytest.mark.asyncio
async def test_accumulator_progresses_monotonically() -> None:
    """Ensure sequential days increase the cumulative total and persist."""

    store = FakeStore()
    accumulator = EnergyAccumulator(store=cast(Any, store))
    await accumulator.async_load()

    day_1 = date(2024, 3, 1)
    day_2 = date(2024, 3, 2)

    await accumulator.async_add_day("primary", day_1, 2.5)
    await accumulator.async_add_day("primary", day_2, 3.0)

    assert store.saved
    state = accumulator.as_sensor_state()
    primary_state = state["primary"]
    assert primary_state["energy_sum"] == pytest.approx(5.5)
    assert primary_state["last_day"] == day_2.isoformat()
    assert primary_state["series_start"] == day_1.isoformat()


@pytest.mark.asyncio
async def test_accumulator_ignores_duplicate_day() -> None:
    """Ensure duplicate samples do not trigger persistence."""

    store = FakeStore()
    accumulator = EnergyAccumulator(store=cast(Any, store))
    await accumulator.async_load()

    day = date(2024, 4, 5)
    await accumulator.async_add_day("boost", day, 1.2)
    first_save_count = len(store.saved)

    await accumulator.async_add_day("boost", day, 1.2)

    assert len(store.saved) == first_save_count
    boost_state = accumulator.as_sensor_state()["boost"]
    assert boost_state["energy_sum"] == pytest.approx(1.2)


@pytest.mark.asyncio
async def test_accumulator_handles_out_of_order_and_reset(caplog: pytest.LogCaptureFixture) -> None:
    """Ensure out-of-order samples are incorporated and resets restart the series."""

    store = FakeStore()
    accumulator = EnergyAccumulator(store=cast(Any, store))
    await accumulator.async_load()

    day_2 = date(2024, 5, 2)
    day_1 = date(2024, 5, 1)

    await accumulator.async_add_day("primary", day_2, 4.0)
    await accumulator.async_add_day("primary", day_1, 3.0)

    primary_state = accumulator.as_sensor_state()["primary"]
    assert primary_state["energy_sum"] == pytest.approx(7.0)
    assert primary_state["series_start"] == day_1.isoformat()

    caplog.clear()
    await accumulator.async_add_day("primary", day_2, 1.0)

    primary_state = accumulator.as_sensor_state()["primary"]
    assert primary_state["energy_sum"] == pytest.approx(1.0)
    assert primary_state["series_start"] == day_2.isoformat()
    assert any("energy decreased" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_accumulator_defaults_before_load() -> None:
    """Ensure as_sensor_state returns defaults before loading."""

    store = FakeStore()
    accumulator = EnergyAccumulator(store=cast(Any, store))
    state = accumulator.as_sensor_state()
    assert state["primary"]["energy_sum"] == 0.0
    assert state["primary"]["last_day"] is None


@pytest.mark.asyncio
async def test_accumulator_load_sanitises_state() -> None:
    """Ensure stored data is normalised when loaded."""

    store = FakeStore(
        {
            "primary": {
                "days": {
                    "2024-04-01": 2.0,
                    5: 3.0,
                    "2024-04-02": "4.0",
                    "bad": 1.0,
                    "2024-04-03": -1.0,
                    "2024-04-04": object(),
                },
                "cumulative_kwh": "not-a-number",
                "last_day": "2024-03-01",
                "series_start": "2024-03-01",
            },
            "boost": {
                "days": None,
                "cumulative_kwh": -5,
                "last_day": None,
                "series_start": None,
            },
        }
    )
    accumulator = EnergyAccumulator(store=cast(Any, store))
    await accumulator.async_load()

    state = accumulator.as_sensor_state()
    assert state["primary"]["energy_sum"] == pytest.approx(6.0)
    assert state["primary"]["last_day"] == "2024-04-02"
    assert state["primary"]["series_start"] == "2024-04-01"
    assert state["boost"]["energy_sum"] == 0.0


@pytest.mark.asyncio
async def test_accumulator_rejects_unknown_zone() -> None:
    """Ensure unsupported zone names raise a ValueError."""

    store = FakeStore()
    accumulator = EnergyAccumulator(store=cast(Any, store))
    with pytest.raises(ValueError):
        await accumulator.async_add_day("invalid", date(2024, 5, 1), 1.0)


@pytest.mark.asyncio
async def test_accumulator_ignores_non_dict_zone() -> None:
    """Ensure stored payloads with unexpected structures are skipped."""

    store = FakeStore({"primary": "invalid"})
    accumulator = EnergyAccumulator(store=cast(Any, store))
    await accumulator.async_load()

    state = accumulator.as_sensor_state()
    assert state["primary"]["energy_sum"] == 0.0


@pytest.mark.asyncio
async def test_accumulator_persistence_roundtrip() -> None:
    """Ensure persisted state reloads and continues accumulating."""

    store = FakeStore()
    accumulator = EnergyAccumulator(store=cast(Any, store))
    await accumulator.async_load()

    day_1 = date(2024, 6, 1)
    await accumulator.async_add_day("primary", day_1, 2.0)

    saved_payload = store.data
    assert saved_payload is not None

    reload_store = FakeStore(saved_payload)
    reloaded = EnergyAccumulator(store=cast(Any, reload_store))
    await reloaded.async_load()

    day_2 = date(2024, 6, 2)
    await reloaded.async_add_day("primary", day_2, 1.5)

    state = reloaded.as_sensor_state()["primary"]
    assert state["energy_sum"] == pytest.approx(3.5)
    assert state["last_day"] == day_2.isoformat()
    assert reload_store.saved, "Reloaded accumulator should persist updates"


@pytest.mark.asyncio
async def test_accumulator_reset_zone() -> None:
    """Ensure resetting a zone clears the ledger and totals."""

    store = FakeStore()
    accumulator = EnergyAccumulator(store=cast(Any, store))
    await accumulator.async_load()

    day = date(2024, 7, 1)
    await accumulator.async_add_day("boost", day, 4.5)
    assert accumulator.as_sensor_state()["boost"]["energy_sum"] == pytest.approx(4.5)

    await accumulator.async_reset_zone("boost")

    state = accumulator.as_sensor_state()["boost"]
    assert state["energy_sum"] == 0.0
    assert state["last_day"] is None
    assert store.saved[-1]["boost"]["ledger"] == {}


@pytest.mark.asyncio
async def test_accumulator_reset_rejects_unknown_zone() -> None:
    """Ensure reset requests validate the zone identifier."""

    store = FakeStore()
    accumulator = EnergyAccumulator(store=cast(Any, store))

    with pytest.raises(ValueError):
        await accumulator.async_reset_zone("invalid")


@pytest.mark.asyncio
async def test_zone_total_covers_all_paths() -> None:
    """Ensure zone_total handles unloaded state, bad data, and invalid zones."""

    store = FakeStore()
    accumulator = EnergyAccumulator(store=cast(Any, store))

    assert accumulator.zone_total("primary") == 0.0

    await accumulator.async_load()
    assert accumulator.zone_total("primary") == 0.0

    accumulator._state["primary"] = None  # type: ignore[assignment]
    assert accumulator.zone_total("primary") == 0.0

    accumulator._state["primary"] = {"cumulative_kwh": object()}  # type: ignore[assignment]
    assert accumulator.zone_total("primary") == 0.0

    accumulator._state["primary"] = {
        "ledger": {},
        "cumulative_kwh": 0.0,
        "last_processed_day": None,
        "series_start": None,
    }

    day = date(2024, 7, 1)
    await accumulator.async_add_day("primary", day, 1.5)
    assert accumulator.zone_total("primary") == pytest.approx(1.5)

    with pytest.raises(ValueError):
        accumulator.zone_total("invalid")

# Developer Notes

## Nightly energy refresh

The integration refreshes cumulative energy state shortly after **01:00** in the controller's configured timezone. The nightly job executes the following steps for each of the seven history samples returned by the Beanbag API:

1. **Report-day assignment.** Convert the sample timestamp from UTC to the configured timezone, subtract one day (because the controller reports the day that just finished), and store the resulting calendar date. The helper `assign_report_day()` handles daylight-saving gaps and folds.
2. **Schedule analysis.** Fetch the primary and boost weekly programs, normalise them with `canonicalize_weekly()`, and build day-specific intervals via `day_intervals()`. Regardless of the intervals detected, anchor each day with `safe_anchor_datetime()` using the user-configured primary and boost anchor times.
3. **Energy calibration.** Compare the device-reported energy with the runtime-derived estimate. When the ratio matches `ln(10)` within tolerance, keep the reported value (`EnergyCalibration.use_scale=True`). Otherwise derive energy from runtime minutes and the configured fallback power.
4. **Energy accumulation.** Feed the per-day totals into the `EnergyAccumulator`, which maintains the monotonic sums for the primary and boost sensors and persists the rolling ledger of processed days (`ledger`, `cumulative_kwh`, `last_processed_day`). Persistence occurs on every increment so that subsequent imports can skip duplicates, detect resets, and survive restarts without losing totals. After updating the accumulator, `_submit_statistics()` converts the **device-local hour-aligned** samples into entity-bound statistics via `recorder.get_instance(hass).async_import_statistics()`. Hour starts are calculated in the controller timezone (`Europe/London`, DST-safe) before being translated to UTC for recorder storage.
5. **Reset support.** When an operator calls the `securemtr.reset_energy_accumulator` service, the accumulator clears the ledger for the requested zone and dispatches a runtime update so the energy sensors immediately report `0 kWh` until fresh samples arrive.
5. **Sensor update.** Store the most recent daily summary inside `SecuremtrRuntimeData.energy_state` and fire dispatcher events so the sensors reflect the latest import. The per-day runtime and schedule metrics remain available in `SecuremtrRuntimeData.statistics_recent` for the duration sensors.

The SecureMTR energy sensors expose `state_class: total_increasing` so they appear in the Energy Dashboard selector. Consumption remains driven entirely by the anchored hourly statistics imported through `_submit_statistics()` with ZoneInfo-based timezone handling—no fixed offsets.

Key helpers live in `utils.py` and `schedule.py`. Update `docs/function_map.txt` whenever a new helper or public function is added.

## Logging

The nightly pipeline emits INFO-level records for each processed day that note the report date, input kWh, and the updated cumulative total. Detailed sample metadata—including the configured anchor timestamp and runtime scheduling context—remains available at DEBUG level. When totals change, another INFO log summarises the new cumulative energy values so you can confirm the sensors match expectations.

## QA checklist

Run the following checklist after major changes to the energy stack to confirm the end-to-end behaviour remains intact:

1. **Fresh install.** Set up the integration on a clean Home Assistant instance and confirm the SecureMTR device and both energy sensors register under the device page.
2. **Sample import.** Replay seven days of WS/REST history and verify the accumulator logs the per-day INFO entries while the sensors increase monotonically.
3. **Energy Dashboard.** Open **Settings → Dashboards → Energy**, add the SecureMTR primary and boost sensors as device consumption sources, and confirm they appear in the selector.
4. **Utility meters.** Check that the daily and weekly utility_meter helpers exist, have the SecureMTR sensors as their source, and begin counting from the next boundary.
5. **Persistence.** Restart Home Assistant and ensure the sensors resume their previous totals before any new samples arrive.
6. **Reset service.** Call `securemtr.reset_energy_accumulator` for each zone and confirm the ledger clears, the sensors drop to `0 kWh`, and subsequent imports rebuild the totals without duplication.

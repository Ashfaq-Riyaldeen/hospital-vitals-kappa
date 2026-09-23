"""The configuration validators, and the simulated clock's arithmetic.

These tests exist because both couplings they guard are invisible in ordinary reading
and expensive to discover at runtime. `plan/03 section 3.3` says to mention the
watermark test in the viva -- it shows the coupling was understood rather than
stumbled into.

Each validator is asserted in BOTH directions: that a sane configuration is accepted,
and that an unsafe one is rejected. A validator that has never been seen to reject
anything is not a validator.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from ward.settings import ProcessingSettings, SimulationSettings, StorageSettings
from ward.simclock import SimClock


def _clock(day_seconds: int = 300) -> SimClock:
    epoch = datetime(2026, 4, 1, tzinfo=UTC)
    return SimClock(epoch_wall=epoch, epoch_sim=epoch, day_seconds=day_seconds)


# ------------------------------------------------------------------- the clock


def test_speedup_is_288_at_the_configured_compression() -> None:
    """One simulated day in 300 real seconds. Every duration in the system is a
    conversion away from this single number."""
    assert _clock(300).speedup == 288.0


def test_sixty_simulated_minutes_is_twelve_and_a_half_real_seconds() -> None:
    """★ The arithmetic the watermark depends on, asserted rather than assumed."""
    assert _clock(300).sim_to_real(60 * 60) == pytest.approx(12.5)


def test_one_real_second_is_four_point_eight_simulated_minutes() -> None:
    """The conversion in the other direction, which is the one that surprises people."""
    assert _clock(300).real_to_sim(1) / 60 == pytest.approx(4.8)


def test_simulated_time_advances_from_the_injected_epoch() -> None:
    """The epoch is injectable precisely so this is testable without Docker."""
    clock = _clock(300)
    one_real_minute_later = clock.epoch_wall + timedelta(minutes=1)
    advanced = clock.sim_now(one_real_minute_later) - clock.epoch_sim
    # 60 real seconds x 288 = 17280 simulated seconds = 4.8 simulated hours.
    assert advanced.total_seconds() == pytest.approx(17_280)


# -------------------------------------------------------- the watermark coupling


def test_default_watermark_is_accepted() -> None:
    assert ProcessingSettings().watermark_sim_minutes == 60


def test_a_watermark_too_short_for_real_jitter_is_rejected(monkeypatch) -> None:
    """★ The trap from plan/03 section 3.3.

    A 5-simulated-minute watermark sounds cautious. At 288x it tolerates about one
    real second of lateness -- less than a GC pause. Readings would be dropped from
    windowed aggregates, windows would come back empty, and NOTHING would error.
    """
    monkeypatch.setenv("PROC_WATERMARK_SIM_MINUTES", "5")
    with pytest.raises(ValueError, match="real seconds of lateness tolerance"):
        ProcessingSettings()


def test_the_error_names_the_fix_not_just_the_fault(monkeypatch) -> None:
    """A validator that says 'invalid' teaches nobody anything at 3am."""
    monkeypatch.setenv("PROC_WATERMARK_SIM_MINUTES", "5")
    with pytest.raises(ValueError) as excinfo:
        ProcessingSettings()
    message = str(excinfo.value)
    assert "Raise the watermark or slow the clock" in message
    assert "SIM_DAY_SECONDS" in message


def test_slowing_the_clock_also_satisfies_the_watermark_rule(monkeypatch) -> None:
    """The coupling is genuinely two-sided: the same watermark becomes safe if the
    simulation runs slower, which is what makes it a coupling rather than a limit."""
    monkeypatch.setenv("PROC_WATERMARK_SIM_MINUTES", "5")
    monkeypatch.setenv("SIM_DAY_SECONDS", "3600")  # 24x instead of 288x
    assert ProcessingSettings().watermark_sim_minutes == 5


# ------------------------------------------------------- the trend-window coupling


def test_trend_window_must_hold_enough_readings(monkeypatch) -> None:
    """A 1-hour window at one reading per 15 simulated minutes holds 4 samples; asking
    for 8 means every score is low-confidence forever and no trend alert ever fires --
    silently, because 'no alerts' looks exactly like 'no deterioration'."""
    monkeypatch.setenv("PROC_TREND_WINDOW_SIM_HOURS", "1")
    monkeypatch.setenv("PROC_MIN_READINGS_FOR_TREND", "8")
    with pytest.raises(ValueError, match="no trend alert could ever fire"):
        ProcessingSettings()


# ----------------------------------------------------------------- the TTL rule


def test_default_snapshot_ttl_is_accepted() -> None:
    assert StorageSettings().snapshot_ttl_seconds == 120


def test_a_ttl_shorter_than_a_few_micro_batches_is_rejected(monkeypatch) -> None:
    """The TTL is a safety mechanism: if the stream stops, the ward screen empties and
    that is unmistakable. But a TTL below a few trigger intervals blanks the screen
    while the stream is perfectly healthy, turning the safety mechanism into a false
    alarm -- and a monitor that cries wolf gets ignored."""
    monkeypatch.setenv("STORE_SNAPSHOT_TTL_SECONDS", "10")
    monkeypatch.setenv("PROC_TRIGGER_SECONDS", "5")
    with pytest.raises(ValueError, match="ward screen would blank"):
        StorageSettings()


def test_retention_is_the_historical_store() -> None:
    """Under Kappa there is no warehouse behind the log. 30 simulated days of
    retention IS the system's memory, and is what makes replay from offset 0
    possible -- so the number is asserted, not left to drift."""
    from ward.settings import KafkaSettings

    assert KafkaSettings().vitals_retention_sim_days == 30


def test_simulation_epoch_is_fixed_so_screenshots_are_stable() -> None:
    assert SimulationSettings().epoch_sim == datetime(2026, 4, 1, tzinfo=UTC)

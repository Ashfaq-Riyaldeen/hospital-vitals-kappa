"""The stream's per-reading decisions, driven with no Kafka or Cassandra.

Each test here pins a bug that existed in the first version of the pipeline:
- the future-timestamp check compared a reading with itself, so it never fired;
- duplicate readings were scored twice;
- the same alert was raised again on every reading;
- the daily summary was overwritten by each reading;
- lab results were keyed "LACTATE" but looked up as "lactate", so labs never counted;
- P007's single spike raised a trend alert.
"""

from __future__ import annotations

import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from random import Random

import pytest
from ward.contracts.models import Admission, LabResult, VitalsReading
from ward.producers.physiology import narratives
from ward.producers.physiology.walk import step
from ward.producers.ward_model import build_ward
from ward.stream.alerts import ALERT_NEWS2_HIGH
from ward.stream.processor import (
    KIND_DUPLICATE,
    KIND_LATE,
    KIND_REJECTED,
    KIND_SCORED,
    StreamProcessor,
)
from ward.stream.windows import calculate_slope

EPOCH = datetime(2026, 4, 1, tzinfo=UTC)


def _reading(
    at: datetime,
    patient: str = "P001",
    reading_id: str | None = None,
    **vitals: object,
) -> VitalsReading:
    values: dict[str, object] = {
        "heart_rate": 72,
        "spo2": 97,
        "respiratory_rate": 16,
        "systolic_bp": 125,
        "diastolic_bp": 80,
        "temperature": 36.8,
        "consciousness": "A",
        "on_supplemental_oxygen": False,
    }
    values.update(vitals)
    return VitalsReading(
        reading_id=reading_id or str(uuid.uuid4()),
        patient_id=patient,
        bed_id="BED-01",
        device_id="DEV-01",
        measured_at=at,
        ingest_time=at,
        producer_id="test",
        **values,  # type: ignore[arg-type]
    )


def test_duplicate_reading_is_not_scored_twice() -> None:
    proc = StreamProcessor("v1")
    first = _reading(EPOCH, reading_id="same-id")
    assert proc.process(first, EPOCH).kind == KIND_SCORED
    again = _reading(EPOCH + timedelta(minutes=15), reading_id="same-id")
    assert proc.process(again, EPOCH).kind == KIND_DUPLICATE


def test_future_timestamp_is_caught_against_the_clock() -> None:
    proc = StreamProcessor("v1")
    future = _reading(EPOCH + timedelta(hours=2))
    out = proc.process(future, clock_now=EPOCH)
    assert out.kind == KIND_REJECTED
    assert out.reason == "FUTURE_TIMESTAMP"


def test_old_reading_during_replay_is_not_future() -> None:
    proc = StreamProcessor("v2")
    assert proc.process(_reading(EPOCH), clock_now=EPOCH + timedelta(days=5)).kind == KIND_SCORED


def test_reading_behind_the_watermark_is_routed_late_not_dropped() -> None:
    proc = StreamProcessor("v1", watermark_minutes=60)
    proc.process(_reading(EPOCH + timedelta(hours=3)), EPOCH + timedelta(hours=3))
    out = proc.process(_reading(EPOCH + timedelta(hours=1)), EPOCH + timedelta(hours=3))
    assert out.kind == KIND_LATE


def test_same_alert_is_raised_once_per_episode() -> None:
    proc = StreamProcessor("v1")
    sick = {"heart_rate": 135, "spo2": 90, "respiratory_rate": 26, "systolic_bp": 95}
    raised: list[str] = []
    ids: set[str] = set()
    for i in range(12):
        at = EPOCH + timedelta(minutes=15 * i)
        out = proc.process(_reading(at, **sick), at)
        raised += [a.alert_type for a in out.alerts]
        ids |= {a.alert_id for a in out.alerts if a.alert_type == ALERT_NEWS2_HIGH}
    assert raised.count(ALERT_NEWS2_HIGH) == 1
    assert len(ids) == 1

    # Quiet for longer than the window, then sick again: a NEW episode.
    t = EPOCH + timedelta(hours=3)
    for _ in range(20):
        t += timedelta(minutes=15)
        proc.process(_reading(t), t)
    t += timedelta(minutes=15)
    out = proc.process(_reading(t, **sick), t)
    new = [a for a in out.alerts if a.alert_type == ALERT_NEWS2_HIGH]
    assert len(new) == 1
    assert new[0].alert_id not in ids


def test_daily_summary_accumulates_instead_of_overwriting() -> None:
    proc = StreamProcessor("v1")
    scores = []
    out = None
    for i, rr in enumerate([16, 22, 16]):
        at = EPOCH + timedelta(minutes=15 * i)
        out = proc.process(_reading(at, respiratory_rate=rr), at)
        assert out.news2 is not None
        scores.append(out.news2.total)
    bad = proc.process(_reading(EPOCH + timedelta(minutes=45), spo2=0), EPOCH)
    assert bad.kind == KIND_REJECTED
    assert bad.summary is not None and out is not None and out.summary is not None
    assert out.summary.readings_count == 3
    assert out.summary.max_news2 == max(scores)
    assert float(out.summary.mean_news2) == pytest.approx(sum(scores) / 3, abs=0.01)
    assert bad.summary.readings_rejected == 1
    assert bad.summary.readings_count == 3


def _lab(test: str, value: float, reported_at: datetime) -> LabResult:
    return LabResult(
        patient_id="P001",
        test_type=test,
        result_value=value,
        unit="u",
        reference_low=0.5,
        reference_high=2.2,
        collected_at=reported_at - timedelta(hours=1),
        reported_at=reported_at,
    )


def test_labs_count_whatever_the_case_of_test_type() -> None:
    labs = {"P001": {"LACTATE": _lab("LACTATE", 3.8, EPOCH)}}
    proc = StreamProcessor("v1", labs=labs)
    out = proc.process(_reading(EPOCH + timedelta(hours=1)), EPOCH + timedelta(hours=1))
    assert out.composite is not None
    assert out.composite.lab_contribution == 1


def test_labs_reported_after_the_reading_are_not_used() -> None:
    labs = {"P001": {"lactate": _lab("lactate", 3.8, EPOCH + timedelta(days=1))}}
    proc = StreamProcessor("v2", labs=labs)
    out = proc.process(_reading(EPOCH + timedelta(hours=1)), EPOCH + timedelta(days=5))
    assert out.composite is not None
    assert out.composite.lab_contribution == 0


def test_one_outlier_does_not_make_a_trend() -> None:
    flat_then_spike = [80.0] * 15 + [145.0]
    slope = calculate_slope(flat_then_spike)
    assert slope is not None and abs(slope) < 1
    climbing = [78.0 + 3 * i for i in range(16)]
    assert calculate_slope(climbing) == 3


def _run_ward(days: int, version: str = "v1") -> dict[str, Counter[str]]:
    """The scripted ward, through the real processor. Returns alert types per patient."""
    rng = Random(42)
    ward = build_ward(40, EPOCH, 42)
    admissions = {
        p.patient_id: Admission(
            patient_id=p.patient_id,
            bed_id=p.bed_id,
            admitted_at=EPOCH,
            age=p.age,
            sex=p.sex,
            primary_condition=p.condition,
            copd_scale2=p.copd_scale2,
        )
        for p in ward
    }
    proc = StreamProcessor(version, admissions=admissions)
    alerts: dict[str, Counter[str]] = {p.patient_id: Counter() for p in ward}
    t = EPOCH
    while t < EPOCH + timedelta(days=days):
        for p in ward:
            p.state = step(p.state, p.baseline, t, rng)
            ctx = narratives.NarrativeContext(p.patient_id, t, EPOCH)
            p.state = narratives.apply(p.state, ctx)
            s = narratives.emitted(p.state, ctx)
            r = VitalsReading(
                reading_id=str(uuid.uuid4()),
                patient_id=p.patient_id,
                bed_id=p.bed_id,
                device_id=p.device_id,
                measured_at=t,
                ingest_time=t,
                heart_rate=round(s.heart_rate),
                spo2=round(s.spo2),
                respiratory_rate=round(s.respiratory_rate),
                systolic_bp=round(s.systolic_bp),
                diastolic_bp=round(s.diastolic_bp),
                temperature=round(s.temperature, 1),
                consciousness="A",
                on_supplemental_oxygen=p.on_supplemental_oxygen,
                producer_id="test",
            )
            out = proc.process(r, t)
            spike_at = EPOCH + timedelta(days=narratives.SPIKE_DAY - 1, hours=narratives.SPIKE_HOUR)
            near_spike = abs((t - spike_at).total_seconds()) <= 2 * 3600
            for a in out.alerts:
                alerts[p.patient_id][a.alert_type] += 1
                if p.patient_id == narratives.SPIKE_PATIENT and near_spike:
                    alerts["P007_SPIKE"][a.alert_type] += 1
        t += timedelta(minutes=15)
        alerts.setdefault("P007_SPIKE", Counter())
    return alerts


def test_scripted_patients_through_the_real_stream_logic() -> None:
    alerts = _run_ward(days=2)
    # P014 deteriorates on day 2 and must raise NEWS2_HIGH exactly once, not every
    # 15 minutes for the rest of the day.
    assert alerts["P014"][ALERT_NEWS2_HIGH] == 1
    # P007's lone heart-rate spike (day 1, 14:00) raises nothing within 2 hours.
    assert sum(alerts["P007_SPIKE"].values()) == 0


def test_future_stamp_is_caught_even_in_a_backlog() -> None:
    """Reproduces the first live outage test: after a restart the stream processed a
    backlog while the clock was hours ahead, so a reading stamped 2 h in the future
    passed, and the patient's next four real readings were routed to the late topic."""
    from ward.simclock import SimClock
    from ward.stream.processor import sent_at_sim

    real0 = datetime(2026, 9, 27, 6, 0, tzinfo=UTC)
    clock = SimClock(epoch_wall=real0, epoch_sim=EPOCH, day_seconds=300)
    proc = StreamProcessor("v1")
    sent = real0 + timedelta(seconds=60)  # 4.8 simulated hours after the epoch
    sim_sent = clock.sim_now(sent)
    ok = _reading(sim_sent).model_copy(update={"ingest_time": sent})
    future = _reading(sim_sent + timedelta(hours=2)).model_copy(update={"ingest_time": sent})
    nxt = _reading(sim_sent + timedelta(minutes=15)).model_copy(
        update={"ingest_time": sent + timedelta(seconds=3)}
    )
    for r in (ok, future, nxt):
        # Processed much later than sent, as in a backlog.
        out = proc.process(r, clock_now=sent_at_sim(clock, r))
        if r is future:
            assert out.kind == KIND_REJECTED and out.reason == "FUTURE_TIMESTAMP"
        else:
            assert out.kind == KIND_SCORED

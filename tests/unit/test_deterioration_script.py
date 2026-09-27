"""The scripted narratives, asserted against the NEWS2 scores they must produce.

★ Everything downstream depends on these being right. ★

The demo, the alert escalation, the replay and the report's results section all assume
P014 deteriorates, P031 sits steady at a saturation only Scale 2 handles correctly,
and P007's spike does not alert. If any of those is wrong, it is wrong everywhere --
and it would be discovered on camera.

So the narratives are asserted here, in pure Python, before anything is wired into
Spark. Same reason the scorer came first: these run in milliseconds with no Kafka and
no Cassandra, and a failure points at one file.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from random import Random

import pytest
from ward.clinical.news2 import score_news2
from ward.producers.physiology.baselines import draw_baseline
from ward.producers.physiology.narratives import (
    COPD_PATIENT,
    SEPSIS_ONSET_DAY,
    SEPSIS_ONSET_HOUR,
    SEPSIS_PATIENT,
    SPIKE_DAY,
    SPIKE_HOUR,
    SPIKE_PATIENT,
    NarrativeContext,
    apply,
    emitted,
)
from ward.producers.physiology.walk import VitalsState, step

EPOCH = datetime(2026, 4, 1, tzinfo=UTC)
SEED = 42


def _trajectory(
    patient_id: str,
    condition: str,
    *,
    start: datetime,
    hours: float,
    interval_minutes: int = 15,
    copd_scale2: bool = False,
    version: str = "v2",
) -> list[tuple[datetime, int]]:
    """Walk a patient forward and score every reading. Returns (time, NEWS2 total)."""
    rng = Random(SEED)
    baseline = draw_baseline(age=68, condition=condition, rng=rng)
    state = VitalsState.at_baseline(baseline)

    out: list[tuple[datetime, int]] = []
    steps = int(hours * 60 / interval_minutes)
    for i in range(steps):
        now = start + timedelta(minutes=i * interval_minutes)
        state = step(state, baseline, now, rng)
        state = apply(state, NarrativeContext(patient_id, now, EPOCH))
        result = score_news2(
            respiratory_rate=round(state.respiratory_rate),
            spo2=round(state.spo2),
            on_supplemental_oxygen=False,
            systolic_bp=round(state.systolic_bp),
            heart_rate=round(state.heart_rate),
            consciousness="A",
            temperature=state.temperature,
            copd_scale2=copd_scale2,
            version=version,  # type: ignore[arg-type]
        )
        out.append((now, result.total))
    return out


# ------------------------------------------------------------- P014: sepsis


def _sepsis_scores() -> list[int]:
    onset = EPOCH + timedelta(days=SEPSIS_ONSET_DAY - 1, hours=SEPSIS_ONSET_HOUR)
    return [total for _, total in _trajectory(SEPSIS_PATIENT, "sepsis_risk", start=onset, hours=5)]


def test_p014_reaches_high_risk() -> None:
    """★ The demo centrepiece. NEWS2 must reach 7 -- emergency response.

    If this fails, the alert escalation, the composite risk demonstration and the
    report's headline result all fail with it.
    """
    assert max(_sepsis_scores()) >= 7, (
        f"P014 peaked at {max(_sepsis_scores())}; the sepsis narrative must reach 7"
    )


def test_p014_climbs_through_the_tiers_rather_than_jumping() -> None:
    """A score arriving already at 7 demonstrates an alarm. A score that climbs
    through 5 demonstrates a TREND, which is what the business question asks about."""
    scores = _sepsis_scores()
    assert scores[0] < 5, f"P014 started at {scores[0]}; it must begin in the low band"
    assert any(5 <= s < 7 for s in scores), "P014 never passed through the MEDIUM tier"
    assert max(scores) >= 7


def test_p014_stays_sick_once_developed() -> None:
    """Sepsis does not resolve on its own. A narrative that switched off would show
    the score dropping back unaided, which would undercut the whole demonstration."""
    onset = EPOCH + timedelta(days=SEPSIS_ONSET_DAY - 1, hours=SEPSIS_ONSET_HOUR)
    later = [
        t
        for _, t in _trajectory(
            SEPSIS_PATIENT, "sepsis_risk", start=onset + timedelta(hours=6), hours=3
        )
    ]
    assert min(later) >= 5, f"P014's score fell back to {min(later)} after developing"


def test_p014_is_normal_before_onset() -> None:
    """The narrative must not leak backwards, or there is no 'before' in the demo."""
    before = [
        t
        for _, t in _trajectory(
            SEPSIS_PATIENT, "sepsis_risk", start=EPOCH + timedelta(hours=2), hours=4
        )
    ]
    assert max(before) < 5, f"P014 scored {max(before)} on day 1, before sepsis onset"


# --------------------------------------------------------------- P031: COPD


def test_p031_sits_in_the_scale_2_band() -> None:
    """Steady 89-91% -- normal for this patient, abnormal on the standard scale."""
    rng = Random(SEED)
    baseline = draw_baseline(age=71, condition="copd", rng=rng)
    state = VitalsState.at_baseline(baseline)
    saturations = []
    for i in range(32):
        now = EPOCH + timedelta(minutes=i * 15)
        state = step(state, baseline, now, rng)
        state = apply(state, NarrativeContext(COPD_PATIENT, now, EPOCH))
        saturations.append(state.spo2)
    assert all(88.5 <= s <= 91.5 for s in saturations), (
        f"P031 saturation left the Scale 2 band: {min(saturations):.1f}-{max(saturations):.1f}"
    )


def test_p031_is_over_scored_under_v1_and_correct_under_v2() -> None:
    """★ The replay's entire justification, on the actual generated data.

    The unit test in test_news2 proves the scorer handles Scale 2. This proves the
    SIMULATOR produces a patient for whom it matters -- without which the replay demo
    would correct nothing.
    """
    start = EPOCH + timedelta(hours=1)
    v1 = [
        t
        for _, t in _trajectory(
            COPD_PATIENT, "copd", start=start, hours=4, copd_scale2=True, version="v1"
        )
    ]
    v2 = [
        t
        for _, t in _trajectory(
            COPD_PATIENT, "copd", start=start, hours=4, copd_scale2=True, version="v2"
        )
    ]
    assert sum(v1) > sum(v2), "P031 must score HIGHER under v1 than v2"
    assert all(b <= a for a, b in zip(v1, v2, strict=True)), (
        "the replay must only ever move COPD scores downward"
    )


# ------------------------------------------------- P007: the negative result


def test_p007_spike_is_a_single_reading() -> None:
    """★ The negative result, and it is worth as much as the positive ones.

    A windowed trend detector that alerts on one outlier would be useless on a real
    ward -- every patient would trigger eventually. The spike must be exactly one
    reading, so a 4-hour window containing 16 readings cannot be dragged by it.
    """
    start = EPOCH + timedelta(days=SPIKE_DAY - 1, hours=SPIKE_HOUR - 1)
    rng = Random(SEED)
    baseline = draw_baseline(age=54, condition="post_op", rng=rng)
    state = VitalsState.at_baseline(baseline)

    elevated = 0
    for i in range(16):
        now = start + timedelta(minutes=i * 15)
        ctx = NarrativeContext(SPIKE_PATIENT, now, EPOCH)
        state = step(state, baseline, now, rng)
        state = apply(state, ctx)
        # 131 bpm is where the heart-rate parameter starts scoring 3. The first
        # version of this test used 140, which hid the walk carrying the spike on
        # into the next reading at 132.
        if emitted(state, ctx).heart_rate >= 131:
            elevated += 1
        assert state.heart_rate < 131, "the spike leaked into the patient's state"
    assert elevated == 1, f"P007's spike covered {elevated} readings; it must cover exactly 1"


# ------------------------------------------------------ the unscripted ward


def test_an_ordinary_patient_stays_in_the_low_band() -> None:
    """If routine patients scored MEDIUM, the ward dashboard would be all red and the
    scripted deterioration would not stand out -- the demo would prove nothing."""
    scores = [
        t for _, t in _trajectory("P002", "stable", start=EPOCH + timedelta(hours=6), hours=6)
    ]
    assert max(scores) < 5, f"a stable patient reached NEWS2 {max(scores)}"


def test_narratives_are_reproducible_under_a_fixed_seed() -> None:
    """The recorded demo and a live run must agree. Without this, they need not."""
    onset = EPOCH + timedelta(days=SEPSIS_ONSET_DAY - 1, hours=SEPSIS_ONSET_HOUR)
    first = _trajectory(SEPSIS_PATIENT, "sepsis_risk", start=onset, hours=4)
    second = _trajectory(SEPSIS_PATIENT, "sepsis_risk", start=onset, hours=4)
    assert first == second


@pytest.mark.parametrize("patient", ["P002", "P019", "P040"])
def test_unscripted_patients_are_untouched_by_the_narratives(patient: str) -> None:
    """Only three beds are deterministic. If `apply` altered anyone else, the ward
    would stop being a control for the scripted patients."""
    rng = Random(SEED)
    baseline = draw_baseline(age=60, condition="stable", rng=rng)
    state = VitalsState.at_baseline(baseline)
    now = EPOCH + timedelta(days=1, hours=SPIKE_HOUR)
    stepped = step(state, baseline, now, rng)
    assert apply(stepped, NarrativeContext(patient, now, EPOCH)) == stepped


def test_sepsis_never_produces_an_impossible_blood_pressure() -> None:
    """The validator rejects systolic <= diastolic. In the final clean run the sepsis
    script produced exactly that for P014 (systolic 96, diastolic 97), so the sickest
    patient's real readings were dead-lettered. Driven through the real monitor,
    because it depends on P014's baseline and the ward's random sequence."""
    from ward.producers import bedside_monitor
    from ward.simclock import SimClock

    clock = SimClock(epoch_wall=datetime(2026, 9, 27, tzinfo=UTC), epoch_sim=EPOCH, day_seconds=300)
    monitors = bedside_monitor.BedsideMonitors(clock, dry_run=True)
    inverted = 0
    t = EPOCH
    while t < EPOCH + timedelta(days=3):
        for patient in monitors.patients:
            payload, defect = monitors._reading(patient, t)
            sbp, dbp = payload["systolic_bp"], payload["diastolic_bp"]
            if defect is None and sbp is not None and sbp <= dbp:
                inverted += 1
        t += timedelta(minutes=15)
    assert inverted == 0, f"{inverted} real readings had systolic <= diastolic"

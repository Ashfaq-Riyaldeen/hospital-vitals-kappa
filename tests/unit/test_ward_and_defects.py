"""The simulated ward, and the defect injector that is the DLQ's control.

The injected defect rate is not decoration. It is the known value the dead-letter path
is measured against: "2 % of readings were rejected" means nothing on its own, while
"1.75 % were injected and 1.75 % were rejected" says the validator catches what it
claims to. These tests protect that control.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from random import Random

import pytest
from ward.producers.physiology import defects
from ward.producers.physiology.narratives import COPD_PATIENT, SEPSIS_PATIENT, SPIKE_PATIENT
from ward.producers.physiology.walk import VitalsState
from ward.producers.ward_model import build_ward

ADMITTED = datetime(2026, 4, 1, tzinfo=UTC)
SEED = 42


def ward(beds: int = 40):
    return build_ward(beds=beds, admitted_at=ADMITTED, seed=SEED)


# ------------------------------------------------------------------ the ward


def test_the_ward_has_exactly_the_configured_number_of_beds() -> None:
    """A check against an independently known value: WARD_BEDS is 40, so anything
    other than 40 distinct patients on the topic is a bug, not a judgement call."""
    patients = ward(40)
    assert len(patients) == 40
    assert len({p.patient_id for p in patients}) == 40


def test_the_ward_is_reproducible_under_a_fixed_seed() -> None:
    """A recorded demo and a live run must produce the same ward, or the video and
    the system disagree about who is in which bed."""
    first, second = ward(), ward()
    assert [(p.patient_id, p.condition, p.age) for p in first] == [
        (p.patient_id, p.condition, p.age) for p in second
    ]


@pytest.mark.parametrize(
    ("patient_id", "condition"),
    [(SEPSIS_PATIENT, "sepsis_risk"), (COPD_PATIENT, "copd"), (SPIKE_PATIENT, "post_op")],
)
def test_scripted_patients_get_the_condition_their_narrative_needs(
    patient_id: str, condition: str
) -> None:
    """P031's COPD is what makes Scale 2 apply; P014's sepsis_risk gives the lower
    systolic baseline the trajectory starts from. A scripted patient with the wrong
    condition would make the narrative incoherent."""
    patient = next(p for p in ward() if p.patient_id == patient_id)
    assert patient.condition == condition


def test_copd_scale2_follows_the_condition_rather_than_a_separate_draw() -> None:
    """★ A patient flagged for Scale 2 without COPD physiology, or COPD physiology
    without the flag, would make the replay's expected diff incoherent -- the replay
    asserts that exactly the Scale 2 patients change and nobody else does."""
    for patient in ward():
        assert patient.copd_scale2 == (patient.condition == "copd")


def test_copd_patients_actually_sit_lower_on_saturation() -> None:
    """The flag must correspond to physiology, or Scale 2 would be correcting nothing."""
    patients = ward()
    copd = [p.baseline.spo2 for p in patients if p.copd_scale2]
    others = [p.baseline.spo2 for p in patients if not p.copd_scale2]
    assert copd, "no COPD patients in the ward -- the replay demo would have no target"
    assert sum(copd) / len(copd) < sum(others) / len(others)


# --------------------------------------------------------------- the defects


def test_the_configured_rate_is_what_the_plan_specifies() -> None:
    assert pytest.approx(0.0175, abs=1e-6) == defects.TOTAL_DEFECT_RATE


def test_measured_injection_rate_matches_the_configured_rate() -> None:
    """★ The control. If these drift apart, every DLQ measurement downstream is
    meaningless, because the expected answer would no longer be known."""
    rng = Random(SEED)
    state = VitalsState(72, 97, 120, 78, 16, 36.8)
    injected = sum(1 for _ in range(40_000) if defects.maybe_inject(state, rng)[1] is not None)
    rate = injected / 40_000
    assert rate == pytest.approx(defects.TOTAL_DEFECT_RATE, abs=0.003), (
        f"measured {rate:.4%} against a configured {defects.TOTAL_DEFECT_RATE:.4%}"
    )


def test_every_defect_type_is_produced() -> None:
    """A type that never fires is a validator branch that is never exercised."""
    rng = Random(SEED)
    state = VitalsState(72, 97, 120, 78, 16, 36.8)
    seen = Counter(
        d.reason for _, d in (defects.maybe_inject(state, rng) for _ in range(60_000)) if d
    )
    expected = {
        "SPO2_PROBE_DETACHED",
        "HR_OUT_OF_RANGE",
        "TEMP_OUT_OF_RANGE",
        "BP_INVERTED",
        "EMPTY_READING",
        "FUTURE_TIMESTAMP",
        "DUPLICATE_READING",
    }
    assert set(seen) == expected, f"missing: {expected - set(seen)}"


def test_only_one_defect_lands_per_reading() -> None:
    """One roll, not seven. A reading that is both empty and has an inverted blood
    pressure is not something a real monitor produces, and two faults on one reading
    would make the injected total ambiguous -- destroying the control."""
    rng = Random(SEED)
    state = VitalsState(72, 97, 120, 78, 16, 36.8)
    for _ in range(5_000):
        corrupted, defect = defects.maybe_inject(state, rng)
        if defect is None:
            continue
        changed = sum(
            1
            for f in ("heart_rate", "spo2", "temperature", "diastolic_bp")
            if getattr(corrupted, f) != getattr(state, f)
        )
        assert changed <= 1, f"{defect.reason} altered {changed} parameters"


def test_injected_values_are_physiologically_impossible_not_merely_abnormal() -> None:
    """★ The clinical-safety distinction this whole module exists to encode.

    An SpO2 of 88% is a SICK PATIENT and must reach the scorer. An SpO2 of 0 is a
    DETACHED PROBE and must not. Validation downstream therefore uses impossibility
    bounds, never statistical outlier detection -- a 3-sigma filter would have
    suppressed P014's sepsis deterioration, because the readings that matter most are
    by construction the ones furthest from the mean.
    """
    rng = Random(SEED)
    state = VitalsState(72, 97, 120, 78, 16, 36.8)
    for _ in range(20_000):
        corrupted, defect = defects.maybe_inject(state, rng)
        if defect is None or defect.duplicate or defect.future_timestamp:
            continue
        if defect.reason == "SPO2_PROBE_DETACHED":
            assert corrupted.spo2 == 0
        elif defect.reason == "HR_OUT_OF_RANGE":
            assert corrupted.heart_rate == 0 or corrupted.heart_rate > 250
        elif defect.reason == "TEMP_OUT_OF_RANGE":
            assert corrupted.temperature < 25 or corrupted.temperature > 45
        elif defect.reason == "BP_INVERTED":
            assert corrupted.diastolic_bp >= corrupted.systolic_bp


def test_future_timestamp_is_two_simulated_hours_ahead() -> None:
    now = datetime(2026, 4, 2, 12, 0, tzinfo=UTC)
    assert (defects.future_timestamp(now) - now).total_seconds() == 2 * 3600

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


# ------------------------------------------------- the ward must start calm

class TestTheWardIsCalmBeforeAnythingHappens:
    """★ Added after an end-to-end run showed the opposite.

    Two modelling errors put the whole ward at MEDIUM/HIGH risk on day 1, with
    nothing wrong with anybody:

    1. `draw_baseline` had no plausibility guard, so a 3.5-sigma draw gave P031 a
       baseline systolic of 81 mmHg. That scores 3 on every reading AND triggers the
       hypotension coupling, dragging heart and respiratory rate up too. Mean NEWS2
       10.0 for the patient whose whole purpose is to be stable.
    2. `on_supplemental_oxygen` was tied 1:1 to COPD, which is not true clinically
       and put a permanent +2 on eleven patients.

    Neither raised anything. Both were only visible by scoring the generated data and
    looking at the distribution -- which is what the end-to-end script is for.

    The demo depends on this: if the ward sits red before anything happens, P014's
    actual deterioration does not stand out and the demonstration proves nothing.
    """

    def _day_one_scores(self) -> dict[str, list[int]]:
        from datetime import timedelta

        from ward.clinical.news2 import score_news2
        from ward.producers.physiology import narratives
        from ward.producers.physiology.walk import step

        out: dict[str, list[int]] = {}
        for patient in ward():
            rng = Random(SEED)
            totals = []
            for i in range(24):  # six simulated hours at 15-minute observations
                now = ADMITTED + timedelta(minutes=15 * i)
                patient.state = step(patient.state, patient.baseline, now, rng)
                patient.state = narratives.apply(
                    patient.state,
                    narratives.NarrativeContext(patient.patient_id, now, ADMITTED),
                )
                totals.append(
                    score_news2(
                        respiratory_rate=round(patient.state.respiratory_rate),
                        spo2=round(patient.state.spo2),
                        on_supplemental_oxygen=patient.on_supplemental_oxygen,
                        systolic_bp=round(patient.state.systolic_bp),
                        heart_rate=round(patient.state.heart_rate),
                        consciousness="A",
                        temperature=patient.state.temperature,
                        copd_scale2=patient.copd_scale2,
                    ).total
                )
            out[patient.patient_id] = totals
        return out

    def test_nobody_is_at_high_risk_before_anything_happens(self) -> None:
        """HIGH is an emergency response. Nobody should be there on day 1.

        With the two bugs above, P031 averaged 10.0 -- comfortably HIGH -- so this
        assertion is what would have caught them.
        """
        scores = self._day_one_scores()
        offenders = {
            pid: round(sum(t) / len(t), 1)
            for pid, t in scores.items()
            if sum(t) / len(t) >= 7
        }
        assert not offenders, f"patients averaging HIGH risk on day one: {offenders}"

    def test_only_a_couple_of_patients_sit_at_medium(self) -> None:
        """Not zero -- a real 40-bed ward has a patient or two needing closer
        observation, and a simulation where everybody is perfectly well would be its
        own kind of unrealistic. What matters is that the dashboard is not RED, so
        P014's climb to HIGH stands out against it.

        Before the fix this was eleven patients; the bound is set where a genuine
        regression is still caught.
        """
        scores = self._day_one_scores()
        at_medium = [pid for pid, t in scores.items() if sum(t) / len(t) >= 5]
        assert len(at_medium) <= 2, (
            f"{len(at_medium)} of {len(scores)} patients average MEDIUM before anything "
            f"has happened: {at_medium}"
        )

    def test_the_ward_average_is_low(self) -> None:
        scores = self._day_one_scores()
        means = [sum(t) / len(t) for t in scores.values()]
        ward_mean = sum(means) / len(means)
        assert ward_mean < 3.0, f"ward mean NEWS2 is {ward_mean:.1f}; the dashboard would be red"

    def test_baselines_are_physiologically_plausible(self) -> None:
        """A baseline is a SET POINT, not an observation: it is where a patient sits
        when nothing is wrong, so it must be a state a ward patient can be in."""
        from ward.producers.physiology.baselines import PLAUSIBLE

        for patient in ward():
            for name, (low, high) in PLAUSIBLE.items():
                value = getattr(patient.baseline, name)
                assert low <= value <= high, (
                    f"{patient.patient_id} baseline {name}={value:.1f} outside {low}-{high}"
                )

    def test_supplemental_oxygen_is_not_tied_one_to_one_to_copd(self) -> None:
        """Oxygen scores +2 on NEWS2, so a blanket rule is worth two points on every
        COPD patient forever. Most COPD patients are on air at rest."""
        patients = ward()
        copd = [p for p in patients if p.copd_scale2]
        on_oxygen = [p for p in copd if p.on_supplemental_oxygen]
        assert copd, "no COPD patients"
        assert len(on_oxygen) < len(copd), "every COPD patient is on oxygen"

    def test_diastolic_is_always_below_systolic(self) -> None:
        """Systolic <= diastolic is a cuff error, not a patient. It must only ever
        appear when the defect injector puts it there deliberately."""
        for patient in ward():
            assert patient.baseline.diastolic_bp < patient.baseline.systolic_bp

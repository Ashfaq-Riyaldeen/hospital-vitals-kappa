"""The simulated ward: 40 beds, their patients, and their admission records.

Built once at start-up from a fixed seed so that the whole ward — baselines,
conditions, ages — is identical on every run. The demo depends on it: a recorded
video and a live run must agree, and the three scripted patients must land in the
same beds with the same underlying physiology.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from random import Random

from ward.producers.physiology.baselines import CONDITIONS, Baseline, draw_baseline
from ward.producers.physiology.narratives import (
    COPD_PATIENT,
    SEPSIS_PATIENT,
    SPIKE_PATIENT,
)
from ward.producers.physiology.walk import VitalsState

# The scripted patients need a specific condition for their narrative to make sense —
# P031's COPD is what makes Scale 2 apply, and P014's sepsis_risk gives the slightly
# lower systolic baseline that the trajectory starts from.
SCRIPTED_CONDITIONS = {
    SEPSIS_PATIENT: "sepsis_risk",
    COPD_PATIENT: "copd",
    SPIKE_PATIENT: "post_op",
}


@dataclass(slots=True)
class Patient:
    """One bed. `state` is the only mutable field: everything else is set at
    admission and stays put, which is what makes the ward reproducible."""

    patient_id: str
    bed_id: str
    device_id: str
    age: int
    sex: str
    condition: str
    copd_scale2: bool
    baseline: Baseline
    state: VitalsState
    admitted_at: datetime


def build_ward(beds: int, admitted_at: datetime, seed: int) -> list[Patient]:
    """Create the ward. Deterministic for a given seed.

    Patient ids are P001..P0NN so that the scripted ids (P007, P014, P031) land on
    real beds rather than being special-cased outside the ward.
    """
    rng = Random(seed)
    patients: list[Patient] = []

    for i in range(1, beds + 1):
        patient_id = f"P{i:03d}"
        condition = SCRIPTED_CONDITIONS.get(patient_id) or rng.choice(CONDITIONS)
        age = rng.randint(38, 89)
        baseline = draw_baseline(age=age, condition=condition, rng=rng)
        patients.append(
            Patient(
                patient_id=patient_id,
                bed_id=f"BED-{i:02d}",
                device_id=f"MON-{i:02d}",
                age=age,
                sex=rng.choice(("F", "M")),
                condition=condition,
                # copd_scale2 follows the CONDITION, not a separate coin flip. A
                # patient flagged for Scale 2 without COPD physiology, or the
                # reverse, would make the replay's expected diff incoherent.
                copd_scale2=condition == "copd",
                baseline=baseline,
                state=VitalsState.at_baseline(baseline),
                admitted_at=admitted_at,
            )
        )
    return patients

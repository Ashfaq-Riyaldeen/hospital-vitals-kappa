"""Deliberate equipment-fault injection — the control for the dead-letter path.

★ THE DISTINCTION THIS MODULE ENCODES IS THE MOST IMPORTANT CLINICAL-SAFETY POINT IN
the data-quality design, and the report states it explicitly. ★

There are two kinds of abnormal reading and they must be handled in opposite ways:

**(a) Physiologically impossible — an EQUIPMENT fault.** SpO2 of 0 means the probe
fell off, not that the patient has no oxygen. A heart rate of 300 is a lead artefact.
These go to the DLQ.

**(b) Abnormal but possible — a SICK PATIENT.** SpO2 of 88 % is not a data error. It
is precisely the reading the system exists to notice.

So validation downstream uses PHYSIOLOGICAL IMPOSSIBILITY bounds, never statistical
outlier detection. A 3-sigma filter would have suppressed P014's sepsis deterioration:
the readings that matter most are, by construction, the ones furthest from the mean.
Cleaning must never suppress a genuine clinical signal.

WHY INJECT AT A KNOWN RATE. The injected rate is the control that makes the DLQ
measurable. "2 % of readings were rejected" tells you nothing on its own; "1.7 % were
injected and 1.7 % were rejected" tells you the validator catches what it claims to.
The expected answer is known before the measurement is taken, which is what makes it
evidence.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from random import Random
from typing import Final

from ward.producers.physiology.walk import VitalsState

# Rates from plan/03 section 4.4. They sum to ~1.65%, which the producer's metrics
# and the DLQ's message count must agree on.
SPO2_PROBE_DETACHED: Final[float] = 0.005
HR_OUT_OF_RANGE: Final[float] = 0.004
TEMP_OUT_OF_RANGE: Final[float] = 0.002
BP_INVERTED: Final[float] = 0.002
EMPTY_READING: Final[float] = 0.003
FUTURE_TIMESTAMP: Final[float] = 0.001
DUPLICATE_READING: Final[float] = 0.0005

TOTAL_DEFECT_RATE: Final[float] = (
    SPO2_PROBE_DETACHED
    + HR_OUT_OF_RANGE
    + TEMP_OUT_OF_RANGE
    + BP_INVERTED
    + EMPTY_READING
    + FUTURE_TIMESTAMP
    + DUPLICATE_READING
)

FUTURE_OFFSET_SIM_HOURS: Final[int] = 2


@dataclass(frozen=True, slots=True)
class Defect:
    """What was injected, so the producer can count it by type.

    `duplicate` and `future_timestamp` do not corrupt the VALUES, so they are carried
    as flags for the producer to act on rather than applied here.
    """

    reason: str
    duplicate: bool = False
    future_timestamp: bool = False


def maybe_inject(state: VitalsState, rng: Random) -> tuple[VitalsState, Defect | None]:
    """Roll once against the defect table. Returns the (possibly corrupted) state.

    One roll, not seven: rolling per defect type would let two faults land on the same
    reading, and a reading that is both empty and has an inverted blood pressure is
    not a thing a real monitor produces. It would also make the injected total
    ambiguous, which would ruin the control.
    """
    roll = rng.random()
    cumulative = 0.0

    cumulative += SPO2_PROBE_DETACHED
    if roll < cumulative:
        # The probe fell off. Zero is not a low saturation; it is an absent one.
        return replace(state, spo2=0.0), Defect("SPO2_PROBE_DETACHED")

    cumulative += HR_OUT_OF_RANGE
    if roll < cumulative:
        # Lead artefact: either a runaway count or a flat trace.
        value = 0.0 if rng.random() < 0.5 else rng.uniform(260, 320)
        return replace(state, heart_rate=value), Defect("HR_OUT_OF_RANGE")

    cumulative += TEMP_OUT_OF_RANGE
    if roll < cumulative:
        value = rng.uniform(15, 24) if rng.random() < 0.5 else rng.uniform(46, 55)
        return replace(state, temperature=value), Defect("TEMP_OUT_OF_RANGE")

    cumulative += BP_INVERTED
    if roll < cumulative:
        # A cuff error. Systolic at or below diastolic is not a patient state.
        return replace(state, diastolic_bp=state.systolic_bp + rng.uniform(5, 20)), Defect(
            "BP_INVERTED"
        )

    cumulative += EMPTY_READING
    if roll < cumulative:
        # Monitor disconnected. Every vital absent - which is DIFFERENT from every
        # vital being zero, and the validator must tell them apart.
        return state, Defect("EMPTY_READING")

    cumulative += FUTURE_TIMESTAMP
    if roll < cumulative:
        return state, Defect("FUTURE_TIMESTAMP", future_timestamp=True)

    cumulative += DUPLICATE_READING
    if roll < cumulative:
        return state, Defect("DUPLICATE_READING", duplicate=True)

    return state, None


def future_timestamp(measured_at: datetime) -> datetime:
    """A clock-skewed monitor, two SIMULATED hours ahead."""
    return measured_at + timedelta(hours=FUTURE_OFFSET_SIM_HOURS)

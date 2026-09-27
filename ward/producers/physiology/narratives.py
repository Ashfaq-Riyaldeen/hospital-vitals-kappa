"""Scripted clinical narratives — the demo backbone.

Three patients follow deterministic trajectories on top of the random walk, so that
what a reviewer sees is reproducible rather than whatever the seed happened to
produce. Everything else on the ward is ordinary AR(1) noise.

    P014  sepsis        NEWS2 climbs 1 -> 5 -> 7. The centrepiece.
    P031  COPD          SpO2 steady 89-91%. The replay's target.
    P007  post-op       One transient spike. The NEGATIVE result.

P007 earns its place by what it must NOT cause. A single heart rate of 145 followed by
normal readings is noise, and a windowed trend detector that alerts on it would be
useless on a real ward -- every patient would trigger eventually. Demonstrating an
alert that correctly does not fire is worth as much as demonstrating one that does,
and it is the kind of negative result reports usually omit.

These are applied as OFFSETS to the walk's output rather than replacing it, so a
scripted patient still carries realistic noise and coupling. A hard-coded ramp would
look synthetic next to the other 37 beds.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Final

from ward.producers.physiology.walk import VitalsState

SEPSIS_PATIENT: Final[str] = "P014"
COPD_PATIENT: Final[str] = "P031"
SPIKE_PATIENT: Final[str] = "P007"

SCRIPTED_PATIENTS: Final[tuple[str, ...]] = (SEPSIS_PATIENT, COPD_PATIENT, SPIKE_PATIENT)

# --- P014: sepsis -------------------------------------------------------
#
# Begins simulated day 2 at 08:00 and develops over 4 simulated hours. The endpoints
# are from plan/03 section 4.3 and are chosen so NEWS2 crosses BOTH escalation
# thresholds -- 5 (MEDIUM) and 7 (HIGH) -- rather than jumping straight to HIGH. A
# score that arrives already at 7 demonstrates an alarm; a score that climbs through
# the tiers demonstrates a trend, which is what the business question asks about.

SEPSIS_ONSET_DAY: Final[int] = 2
SEPSIS_ONSET_HOUR: Final[int] = 8
SEPSIS_DURATION_HOURS: Final[float] = 4.0

# (start, end) at onset and full development.
SEPSIS_TRAJECTORY: Final[dict[str, tuple[float, float]]] = {
    "temperature": (36.9, 38.9),
    "heart_rate": (78.0, 122.0),
    "respiratory_rate": (16.0, 26.0),
    "systolic_bp": (128.0, 96.0),
    "spo2": (97.0, 93.0),
}

# --- P031: COPD ---------------------------------------------------------
#
# Not a deterioration. A steady saturation that is NORMAL FOR THIS PATIENT and
# abnormal on the standard scale. Under scorer v1 they are persistently over-scored;
# under v2's Scale 2 the score corrects. That difference is the entire replay demo.

COPD_SPO2_LOW: Final[float] = 89.0
COPD_SPO2_HIGH: Final[float] = 91.0

# --- P007: the transient spike ------------------------------------------

SPIKE_DAY: Final[int] = 1
SPIKE_HOUR: Final[int] = 14
SPIKE_HEART_RATE: Final[float] = 145.0
# One observation only. At a 15-simulated-minute cadence a 4-hour trend window holds
# 16 readings, so one outlier must not move the slope enough to alert.
SPIKE_DURATION_MINUTES: Final[int] = 15


@dataclass(frozen=True, slots=True)
class NarrativeContext:
    """Everything a narrative needs to decide what to do at this instant."""

    patient_id: str
    sim_time: datetime
    sim_epoch: datetime

    @property
    def sim_day(self) -> int:
        """1-based simulated day. Day 1 is the first day of the run, matching how the
        schedule and the demo script talk about it."""
        return (self.sim_time.date() - self.sim_epoch.date()).days + 1


def _lerp(start: float, end: float, fraction: float) -> float:
    return start + (end - start) * max(0.0, min(1.0, fraction))


def _sepsis_progress(ctx: NarrativeContext) -> float | None:
    """How far into the sepsis trajectory we are, or None if it has not started.

    Returns 1.0 after full development rather than None, because the patient stays
    sick -- a narrative that switched off would show the score dropping back on its
    own, which is not what sepsis does and would undercut the demo.
    """
    if ctx.sim_day < SEPSIS_ONSET_DAY:
        return None
    onset = ctx.sim_time.replace(hour=SEPSIS_ONSET_HOUR, minute=0, second=0, microsecond=0)
    if ctx.sim_day == SEPSIS_ONSET_DAY and ctx.sim_time < onset:
        return None
    if ctx.sim_day > SEPSIS_ONSET_DAY:
        return 1.0
    elapsed_hours = (ctx.sim_time - onset).total_seconds() / 3600.0
    return elapsed_hours / SEPSIS_DURATION_HOURS


def _in_spike_window(ctx: NarrativeContext) -> bool:
    if ctx.sim_day != SPIKE_DAY:
        return False
    start = ctx.sim_time.replace(hour=SPIKE_HOUR, minute=0, second=0, microsecond=0)
    return start <= ctx.sim_time < start + timedelta(minutes=SPIKE_DURATION_MINUTES)


def apply(state: VitalsState, ctx: NarrativeContext) -> VitalsState:
    """Apply any scripted narrative for this patient at this instant.

    Patients with no narrative are returned unchanged, so the ordinary 37 beds cost
    nothing and the scripted three are the only deterministic thing on the ward.
    """
    if ctx.patient_id == SEPSIS_PATIENT:
        progress = _sepsis_progress(ctx)
        if progress is None:
            return state
        return replace(
            state,
            temperature=_lerp(*SEPSIS_TRAJECTORY["temperature"], progress),
            heart_rate=_lerp(*SEPSIS_TRAJECTORY["heart_rate"], progress),
            respiratory_rate=_lerp(*SEPSIS_TRAJECTORY["respiratory_rate"], progress),
            systolic_bp=_lerp(*SEPSIS_TRAJECTORY["systolic_bp"], progress),
            spo2=_lerp(*SEPSIS_TRAJECTORY["spo2"], progress),
        )

    if ctx.patient_id == COPD_PATIENT:
        # Clamp rather than ramp: this patient is STABLE at a saturation that the
        # standard scale calls abnormal. The point is steadiness, not change.
        return replace(state, spo2=min(max(state.spo2, COPD_SPO2_LOW), COPD_SPO2_HIGH))

    return state


def emitted(state: VitalsState, ctx: NarrativeContext) -> VitalsState:
    """What the monitor REPORTS, as opposed to what the patient IS.

    P007's spike is a measurement artefact (a patient moving, a loose lead), so it is
    applied to the emitted reading only and never written back into the patient's
    state. It used to be written into the state, and the random walk then carried it
    forward - 145, then 132, then 121 - so the "single" spike was really three
    elevated readings and the next one still scored 3 for heart rate.
    """
    if ctx.patient_id == SPIKE_PATIENT and _in_spike_window(ctx):
        return replace(state, heart_rate=SPIKE_HEART_RATE)
    return state

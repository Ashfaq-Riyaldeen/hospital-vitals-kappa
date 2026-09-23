"""The AR(1) walk, physiological coupling and circadian rhythm.

Three layers, each adding realism the one below cannot:

1. **Mean-reverting walk.** Vitals drift and return toward a baseline; they do not
   resample independently each observation. An independent draw produces a patient
   whose heart rate is 70 then 95 then 68, which no clinician would believe.

2. **Coupling.** Parameters move together, because the body compensates. This is the
   detail that makes the data credible to someone who knows the domain -- and it is
   also what makes a deterioration look like a deterioration rather than one number
   moving on its own.

3. **Circadian rhythm.** Heart rate and temperature dip overnight. It shows up as a
   visible band on the ward dashboard and is direct evidence that the simulated clock
   is driving the physiology rather than sitting beside it.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from random import Random
from typing import Final

from ward.producers.physiology.baselines import Baseline

# Ornstein-Uhlenbeck reversion strength. At 0.25 a displaced vital returns to
# baseline over roughly four observations -- about an hour at a 15-simulated-minute
# cadence, which is the right order for ward observations.
THETA: Final[float] = 0.25

# Per-parameter volatility, in the parameter's own units.
SIGMA: Final[dict[str, float]] = {
    "heart_rate": 3.0,
    "spo2": 0.8,
    "systolic_bp": 5.0,
    "diastolic_bp": 4.0,
    "respiratory_rate": 1.0,
    "temperature": 0.12,
}

# --- coupling constants, all from plan/03 section 4.2 ---------------------

# Compensatory tachycardia: a falling saturation drives heart rate up.
HR_PER_SPO2_POINT: Final[float] = 2.0
# Roughly 8 bpm per degree Celsius above baseline.
HR_PER_DEGREE: Final[float] = 8.0
# Below this systolic, the body compensates with rate.
HYPOTENSION_THRESHOLD: Final[float] = 95.0
HR_PER_HYPOTENSIVE_MMHG: Final[float] = 0.4
RR_PER_HYPOTENSIVE_MMHG: Final[float] = 0.15
# Respiratory rate and saturation are inversely coupled.
RR_PER_SPO2_POINT: Final[float] = 0.5

# Overnight dip, simulated 02:00-05:00.
CIRCADIAN_START_HOUR: Final[int] = 2
CIRCADIAN_END_HOUR: Final[int] = 5
CIRCADIAN_HR_DIP: Final[float] = 6.0
CIRCADIAN_TEMP_DIP: Final[float] = 0.3


@dataclass(frozen=True, slots=True)
class VitalsState:
    """A patient's current vitals. Frozen; `step` returns a new state.

    Immutability is not ceremony here: the walk is a pure function of (previous state,
    baseline, time, rng), which is what lets a scripted narrative be replayed to the
    identical trajectory in a test with no producer running.
    """

    heart_rate: float
    spo2: float
    systolic_bp: float
    diastolic_bp: float
    respiratory_rate: float
    temperature: float

    @classmethod
    def at_baseline(cls, baseline: Baseline) -> VitalsState:
        return cls(
            heart_rate=baseline.heart_rate,
            spo2=baseline.spo2,
            systolic_bp=baseline.systolic_bp,
            diastolic_bp=baseline.diastolic_bp,
            respiratory_rate=baseline.respiratory_rate,
            temperature=baseline.temperature,
        )


def _ou(current: float, target: float, sigma: float, rng: Random) -> float:
    """One Ornstein-Uhlenbeck step: x + theta*(target - x) + sigma*epsilon."""
    return current + THETA * (target - current) + sigma * rng.gauss(0, 1)


def _circadian(sim_time: datetime) -> tuple[float, float]:
    """Overnight heart-rate and temperature dip, as (hr_delta, temp_delta).

    Returns deltas rather than applying them so the effect is visible at the call
    site. A hidden time-dependent adjustment inside the walk would be very hard to
    reason about when a trend looks wrong.
    """
    if CIRCADIAN_START_HOUR <= sim_time.hour < CIRCADIAN_END_HOUR:
        return -CIRCADIAN_HR_DIP, -CIRCADIAN_TEMP_DIP
    return 0.0, 0.0


def step(state: VitalsState, baseline: Baseline, sim_time: datetime, rng: Random) -> VitalsState:
    """Advance one observation.

    Order matters: the walk runs first against the baseline, then coupling adjusts
    heart rate and respiratory rate in response to where the *other* parameters
    landed. Applying coupling first would have it chase last observation's values.
    """
    hr_dip, temp_dip = _circadian(sim_time)

    spo2 = _ou(state.spo2, baseline.spo2, SIGMA["spo2"], rng)
    temperature = _ou(state.temperature, baseline.temperature + temp_dip, SIGMA["temperature"], rng)
    systolic = _ou(state.systolic_bp, baseline.systolic_bp, SIGMA["systolic_bp"], rng)
    diastolic = _ou(state.diastolic_bp, baseline.diastolic_bp, SIGMA["diastolic_bp"], rng)
    respiratory = _ou(
        state.respiratory_rate, baseline.respiratory_rate, SIGMA["respiratory_rate"], rng
    )
    heart_rate = _ou(state.heart_rate, baseline.heart_rate + hr_dip, SIGMA["heart_rate"], rng)

    # --- coupling: the body compensating -------------------------------

    # Falling saturation -> compensatory tachycardia, and faster breathing.
    spo2_deficit = max(0.0, baseline.spo2 - spo2)
    heart_rate += spo2_deficit * HR_PER_SPO2_POINT
    respiratory += spo2_deficit * RR_PER_SPO2_POINT

    # Fever drives rate.
    heart_rate += (temperature - baseline.temperature) * HR_PER_DEGREE

    # Hypotension drives both rate and respiratory effort.
    if systolic < HYPOTENSION_THRESHOLD:
        shortfall = HYPOTENSION_THRESHOLD - systolic
        heart_rate += shortfall * HR_PER_HYPOTENSIVE_MMHG
        respiratory += shortfall * RR_PER_HYPOTENSIVE_MMHG

    # A cuff cannot read systolic at or below diastolic. Clamping here keeps a
    # physiologically impossible pair out of the stream unless the artefact injector
    # puts one there DELIBERATELY -- otherwise the DLQ would fill with our own noise
    # and the injected-vs-rejected control would be worthless.
    diastolic = min(diastolic, systolic - 15)

    return replace(
        state,
        heart_rate=heart_rate,
        spo2=spo2,
        systolic_bp=systolic,
        diastolic_bp=diastolic,
        respiratory_rate=respiratory,
        temperature=temperature,
    )

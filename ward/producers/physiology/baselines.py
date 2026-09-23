"""Per-patient physiological baselines, drawn once at admission.

Uniform random vitals would make the risk score meaningless: NEWS2 would fire
constantly on noise, and the ward dashboard would be unconvincing to anyone who has
seen a real one. Each patient instead gets a baseline drawn from age- and
condition-adjusted distributions, and their readings mean-revert toward it.

The distributions are from plan/03 section 4.2. They are approximations chosen for
plausibility, not clinical reference values, and the report says so.
"""

from __future__ import annotations

from dataclasses import dataclass
from random import Random
from typing import Final

# Condition-specific offsets applied to the population baseline.
#
# `copd: spo2 -4` is the one that matters architecturally: it produces patients who
# sit at 88-92% saturation, which is NORMAL FOR THEM and is exactly the population
# NEWS2 Scale 2 exists for. Without it the replay demo would have nothing to correct.
CONDITION_OFFSETS: Final[dict[str, dict[str, float]]] = {
    "copd": {"heart_rate": 8, "spo2": -4, "respiratory_rate": 3},
    "cardiac": {"heart_rate": 12},
    "post_op": {"heart_rate": 10},
    "sepsis_risk": {"systolic_bp": -6},
    "stable": {},
}

CONDITIONS: Final[tuple[str, ...]] = tuple(CONDITION_OFFSETS)


@dataclass(frozen=True, slots=True)
class Baseline:
    """One patient's physiological set point. Frozen: a baseline is drawn at
    admission and does not move. What moves is the reading around it."""

    heart_rate: float
    spo2: float
    systolic_bp: float
    diastolic_bp: float
    respiratory_rate: float
    temperature: float


def _age_adjustment(age: int) -> float:
    """Older patients run slightly higher heart rate and blood pressure and slightly
    lower saturation. A crude linear term is enough for plausibility; pretending to
    more precision than that would be false confidence."""
    return (age - 60) / 20.0


def draw_baseline(age: int, condition: str, rng: Random) -> Baseline:
    """Draw one patient's baseline.

    `rng` is injected rather than module-global so a fixed seed reproduces the entire
    ward exactly. The demo depends on that: the scripted narratives must land the same
    way every run, or a recorded video and a live run would disagree.
    """
    adj = _age_adjustment(age)
    offsets = CONDITION_OFFSETS.get(condition, {})

    def offset(name: str) -> float:
        return offsets.get(name, 0.0)

    systolic = rng.gauss(124 + adj, 12) + offset("systolic_bp")
    return Baseline(
        heart_rate=rng.gauss(72 + adj, 8) + offset("heart_rate"),
        spo2=rng.gauss(97 - adj, 1.5) + offset("spo2"),
        systolic_bp=systolic,
        # Diastolic is derived rather than drawn independently: a pulse pressure of
        # 30-50 mmHg is physiological, and two independent draws would routinely
        # produce systolic <= diastolic, which is a cuff error, not a patient.
        diastolic_bp=systolic - rng.gauss(42, 6),
        respiratory_rate=rng.gauss(16, 2) + offset("respiratory_rate"),
        temperature=rng.gauss(36.8, 0.25),
    )

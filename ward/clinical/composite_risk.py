"""Composite clinical risk calculation.

Combines the NEWS2 early warning score with laboratory pathology results
to produce an auditable composite risk tier.

Answers the business question:
"Which patients show concerning vital-sign trends right now, and how do
yesterday's lab results change the risk picture for those patients going forward?"
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Final

from ward.contracts.models import LabResult

# Risk Tier constants
TIER_LOW: Final[str] = "LOW"
TIER_MEDIUM: Final[str] = "MEDIUM"
TIER_HIGH: Final[str] = "HIGH"
TIER_CRITICAL: Final[str] = "CRITICAL"


@dataclass(frozen=True, slots=True)
class CompositeRiskResult:
    """The unified risk calculation for one patient at one instant."""

    patient_id: str
    news2_total: int
    lab_contribution: int
    composite_risk: int
    risk_tier: str
    labs_stale: bool
    explanation: str
    lab_findings: tuple[str, ...] = field(default_factory=tuple)


def evaluate_composite_risk(
    patient_id: str,
    news2_total: int,
    labs: Sequence[LabResult] | dict[str, LabResult],
    as_of: datetime | None = None,
    max_lab_age_days: int = 2,
) -> CompositeRiskResult:
    """Evaluate composite risk from NEWS2 total and recent lab results.

    Stale-labs rule: if labs are absent or older than max_lab_age_days simulated days,
    the lab contribution is 0 and labs_stale is flagged as True.
    """
    lab_map: dict[str, LabResult] = (
        labs if isinstance(labs, dict) else {lab.test_type.lower(): lab for lab in labs}
    )

    if not lab_map:
        tier = _tier_for_score(news2_total)
        return CompositeRiskResult(
            patient_id=patient_id,
            news2_total=news2_total,
            lab_contribution=0,
            composite_risk=news2_total,
            risk_tier=tier,
            labs_stale=True,
            explanation=(
                f"NEWS2 {news2_total} ({tier}) based on vitals alone (no lab results available)."
            ),
            lab_findings=(),
        )

    # Check freshness
    labs_stale = False
    if as_of is not None:
        newest_reported = max(lab.reported_at for lab in lab_map.values())
        age_seconds = (as_of - newest_reported).total_seconds()
        # In simulated time, 1 simulated day = 86400 simulated seconds
        if age_seconds > max_lab_age_days * 86_400:
            labs_stale = True

    if labs_stale:
        tier = _tier_for_score(news2_total)
        return CompositeRiskResult(
            patient_id=patient_id,
            news2_total=news2_total,
            lab_contribution=0,
            composite_risk=news2_total,
            risk_tier=tier,
            labs_stale=True,
            explanation=(
                f"NEWS2 {news2_total} ({tier}); lab results are stale "
                f"(> {max_lab_age_days} sim days old)."
            ),
            lab_findings=(),
        )

    # Evaluate clinical lab modifiers
    findings: list[str] = []
    contribution = 0

    # 1. Lactate
    if lactate := lab_map.get("lactate"):
        val = lactate.result_value
        if val > 4.0:
            contribution += 2
            findings.append(f"lactate {val} {lactate.unit} (> 4.0, strong sepsis indicator)")
        elif val > 2.0:
            contribution += 1
            findings.append(f"lactate {val} {lactate.unit} (> 2.0, tissue hypoperfusion)")

    # 2. White Blood Cell Count (WBC)
    if (wbc := lab_map.get("wbc")) and wbc.is_abnormal:
        contribution += 1
        findings.append(f"WBC {wbc.result_value} {wbc.unit} (leucocytosis/infection)")

    # 3. C-reactive Protein (CRP)
    if crp := lab_map.get("crp"):
        if crp.result_value > 100.0:
            contribution += 1
            findings.append(f"CRP {crp.result_value} {crp.unit} (> 100, marked inflammation)")
        elif crp.is_abnormal:
            contribution += 1
            findings.append(f"CRP {crp.result_value} {crp.unit} (elevated inflammatory marker)")

    # 4. Creatinine (Kidney function)
    if (creat := lab_map.get("creatinine")) and (creat.result_value > 120.0 or creat.is_abnormal):
        contribution += 1
        findings.append(
            f"creatinine {creat.result_value} {creat.unit} (acute kidney injury indicator)"
        )

    # 5. Potassium (Arrhythmia risk)
    if (pot := lab_map.get("potassium")) and pot.is_abnormal:
        contribution += 1
        findings.append(f"potassium {pot.result_value} {pot.unit} (abnormal, arrhythmia risk)")

    # 6. Haemoglobin (Anaemia)
    if (hb := lab_map.get("haemoglobin")) and hb.result_value < 80.0:
        contribution += 1
        findings.append(f"haemoglobin {hb.result_value} {hb.unit} (< 80, significant anaemia)")

    composite = news2_total + contribution
    tier = _tier_for_score(composite)

    if findings:
        reasons = "; ".join(findings)
        explanation = (
            f"NEWS2 {news2_total} raised to composite {composite} ({tier}) "
            f"by abnormal labs: {reasons}."
        )
    else:
        explanation = f"NEWS2 {news2_total} ({tier}) confirmed with normal laboratory results."

    return CompositeRiskResult(
        patient_id=patient_id,
        news2_total=news2_total,
        lab_contribution=contribution,
        composite_risk=composite,
        risk_tier=tier,
        labs_stale=False,
        explanation=explanation,
        lab_findings=tuple(findings),
    )


def _tier_for_score(score: int) -> str:
    """Map aggregate clinical score to risk tier."""
    if score >= 10:
        return TIER_CRITICAL
    if score >= 7:
        return TIER_HIGH
    if score >= 5:
        return TIER_MEDIUM
    return TIER_LOW

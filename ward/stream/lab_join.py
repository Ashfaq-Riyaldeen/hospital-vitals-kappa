"""Lab join stage: joins vital trends with latest lab pathology results.

Invokes the pure clinical composite risk engine to compute composite risk and tier.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from ward.clinical.composite_risk import CompositeRiskResult, evaluate_composite_risk
from ward.contracts.models import LabResult


def join_and_evaluate_risk(
    patient_id: str,
    news2_total: int,
    labs_for_patient: Sequence[LabResult] | dict[str, LabResult],
    as_of: datetime | None = None,
) -> CompositeRiskResult:
    """Join latest labs for patient and evaluate composite risk."""
    return evaluate_composite_risk(
        patient_id=patient_id,
        news2_total=news2_total,
        labs=labs_for_patient,
        as_of=as_of,
    )

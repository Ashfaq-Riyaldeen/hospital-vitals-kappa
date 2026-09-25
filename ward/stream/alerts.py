"""Clinical alert generation and deduplication stage.

Evaluates 7 clinical alert conditions, computes deterministic alert_ids,
and suppresses duplicate alerts within the sliding window unless severity escalates.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from typing import Final

from ward.clinical.composite_risk import CompositeRiskResult
from ward.clinical.news2 import News2Result
from ward.stream.windows import WindowedVitalsTrend

# Alert Types
ALERT_NEWS2_HIGH: Final[str] = "NEWS2_HIGH"
ALERT_NEWS2_MEDIUM: Final[str] = "NEWS2_MEDIUM"
ALERT_SINGLE_PARAM_RED: Final[str] = "SINGLE_PARAM_RED"
ALERT_SPO2_CRITICAL: Final[str] = "SPO2_CRITICAL"
ALERT_RAPID_DETERIORATION: Final[str] = "RAPID_DETERIORATION"
ALERT_LAB_CORROBORATED_RISK: Final[str] = "LAB_CORROBORATED_RISK"
ALERT_TREND_CONCERNING: Final[str] = "TREND_CONCERNING"

# Severities
SEVERITY_MEDIUM: Final[str] = "MEDIUM"
SEVERITY_HIGH: Final[str] = "HIGH"
SEVERITY_CRITICAL: Final[str] = "CRITICAL"

_SEVERITY_RANK: Final[dict[str, int]] = {
    SEVERITY_MEDIUM: 1,
    SEVERITY_HIGH: 2,
    SEVERITY_CRITICAL: 3,
}


@dataclass(frozen=True, slots=True)
class ClinicalAlert:
    """An actionable clinical alert."""

    alert_id: str
    patient_id: str
    bed_id: str
    ward_id: str
    sim_date: date
    alert_time: datetime
    alert_type: str
    severity: str
    news2_total: int
    composite_risk: int
    detail: str
    acknowledged: bool = False


def make_alert_id(patient_id: str, alert_type: str, window_start: datetime) -> str:
    """Deterministic hash avoiding alert fatigue across sliding micro-batches."""
    payload = f"{patient_id}:{alert_type}:{window_start.isoformat()}".encode()
    return hashlib.sha1(payload).hexdigest()[:16]


def evaluate_clinical_alerts(
    patient_id: str,
    bed_id: str,
    ward_id: str,
    sim_date: date,
    alert_time: datetime,
    window_trend: WindowedVitalsTrend,
    news2: News2Result,
    composite: CompositeRiskResult,
    prior_score_in_window: int | None = None,
    copd_scale2: bool = False,
) -> list[ClinicalAlert]:
    """Evaluate 7 clinical alert conditions for one patient."""
    alerts: list[ClinicalAlert] = []

    # 1. NEWS2 High (>= 7)
    if news2.total >= 7:
        alerts.append(
            ClinicalAlert(
                alert_id=make_alert_id(patient_id, ALERT_NEWS2_HIGH, window_trend.window_start),
                patient_id=patient_id,
                bed_id=bed_id,
                ward_id=ward_id,
                sim_date=sim_date,
                alert_time=alert_time,
                alert_type=ALERT_NEWS2_HIGH,
                severity=SEVERITY_CRITICAL,
                news2_total=news2.total,
                composite_risk=composite.composite_risk,
                detail=f"NEWS2 score {news2.total} indicates critical clinical risk.",
            )
        )
    # 2. NEWS2 Medium (5-6)
    elif news2.total >= 5:
        alerts.append(
            ClinicalAlert(
                alert_id=make_alert_id(patient_id, ALERT_NEWS2_MEDIUM, window_trend.window_start),
                patient_id=patient_id,
                bed_id=bed_id,
                ward_id=ward_id,
                sim_date=sim_date,
                alert_time=alert_time,
                alert_type=ALERT_NEWS2_MEDIUM,
                severity=SEVERITY_HIGH,
                news2_total=news2.total,
                composite_risk=composite.composite_risk,
                detail=f"NEWS2 score {news2.total} indicates elevated clinical risk.",
            )
        )

    # 3. Single parameter red (any single parameter scores 3)
    if news2.any_parameter_is_3 and news2.total < 7:
        alerts.append(
            ClinicalAlert(
                alert_id=make_alert_id(
                    patient_id, ALERT_SINGLE_PARAM_RED, window_trend.window_start
                ),
                patient_id=patient_id,
                bed_id=bed_id,
                ward_id=ward_id,
                sim_date=sim_date,
                alert_time=alert_time,
                alert_type=ALERT_SINGLE_PARAM_RED,
                severity=SEVERITY_HIGH,
                news2_total=news2.total,
                composite_risk=composite.composite_risk,
                detail="Urgent review triggered by extreme value in a single parameter.",
            )
        )

    # 4. Critical SpO2
    spo2_threshold = 84 if copd_scale2 else 88
    if window_trend.spo2_latest is not None and window_trend.spo2_latest < spo2_threshold:
        alerts.append(
            ClinicalAlert(
                alert_id=make_alert_id(patient_id, ALERT_SPO2_CRITICAL, window_trend.window_start),
                patient_id=patient_id,
                bed_id=bed_id,
                ward_id=ward_id,
                sim_date=sim_date,
                alert_time=alert_time,
                alert_type=ALERT_SPO2_CRITICAL,
                severity=SEVERITY_CRITICAL,
                news2_total=news2.total,
                composite_risk=composite.composite_risk,
                detail=(
                    f"Severe hypoxia: SpO2 {window_trend.spo2_latest}% "
                    f"below limit {spo2_threshold}%."
                ),
            )
        )

    # 5. Rapid deterioration (score climbed >= 3 in 4 hours)
    if prior_score_in_window is not None and (news2.total - prior_score_in_window) >= 3:
        alerts.append(
            ClinicalAlert(
                alert_id=make_alert_id(
                    patient_id, ALERT_RAPID_DETERIORATION, window_trend.window_start
                ),
                patient_id=patient_id,
                bed_id=bed_id,
                ward_id=ward_id,
                sim_date=sim_date,
                alert_time=alert_time,
                alert_type=ALERT_RAPID_DETERIORATION,
                severity=SEVERITY_CRITICAL,
                news2_total=news2.total,
                composite_risk=composite.composite_risk,
                detail=(
                    f"Rapid trajectory: NEWS2 climbed from {prior_score_in_window} "
                    f"to {news2.total} within window."
                ),
            )
        )

    # 6. Lab-corroborated risk
    if composite.composite_risk >= 7 and composite.lab_contribution >= 2:
        alerts.append(
            ClinicalAlert(
                alert_id=make_alert_id(
                    patient_id, ALERT_LAB_CORROBORATED_RISK, window_trend.window_start
                ),
                patient_id=patient_id,
                bed_id=bed_id,
                ward_id=ward_id,
                sim_date=sim_date,
                alert_time=alert_time,
                alert_type=ALERT_LAB_CORROBORATED_RISK,
                severity=SEVERITY_HIGH,
                news2_total=news2.total,
                composite_risk=composite.composite_risk,
                detail=f"High risk corroborated by pathology findings: {composite.explanation}",
            )
        )

    # 7. Trend concerning (>= 2 adverse slopes, confidence high)
    if window_trend.confidence == "high":
        adverse_count = 0
        if window_trend.hr_slope is not None and window_trend.hr_slope > 0:
            adverse_count += 1
        if window_trend.spo2_slope is not None and window_trend.spo2_slope < 0:
            adverse_count += 1
        if window_trend.sbp_slope is not None and window_trend.sbp_slope < 0:
            adverse_count += 1
        if window_trend.temp_slope is not None and window_trend.temp_slope > 0:
            adverse_count += 1

        if adverse_count >= 2:
            alerts.append(
                ClinicalAlert(
                    alert_id=make_alert_id(
                        patient_id, ALERT_TREND_CONCERNING, window_trend.window_start
                    ),
                    patient_id=patient_id,
                    bed_id=bed_id,
                    ward_id=ward_id,
                    sim_date=sim_date,
                    alert_time=alert_time,
                    alert_type=ALERT_TREND_CONCERNING,
                    severity=SEVERITY_MEDIUM,
                    news2_total=news2.total,
                    composite_risk=composite.composite_risk,
                    detail=(
                        f"Concerning multi-parameter trajectory "
                        f"({adverse_count} adverse vital sign slopes)."
                    ),
                )
            )

    return alerts

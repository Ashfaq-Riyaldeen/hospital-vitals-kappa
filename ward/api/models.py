"""Pydantic data models for the hospital vitals serving layer.

Defines API responses, request bodies, and audit structures.
Every risk evaluation maintains complete explainability:
subscores breakdown, red parameter escalation, lab contribution,
and clinical narrative strings.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class BaseAPIModel(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)


class News2Subscores(BaseAPIModel):
    """Component scores for each physiological vital sign."""

    respiratory_rate: int = 0
    spo2: int = 0
    systolic_bp: int = 0
    heart_rate: int = 0
    temperature: int = 0
    consciousness: int = 0
    supplemental_oxygen: int = 0


class WardPatientSnapshot(BaseAPIModel):
    """Current risk status of a patient on the live ward monitor."""

    ward_id: str
    patient_id: str
    bed_id: str
    risk_score: int
    risk_tier: str
    news2_total: int
    lab_contribution: int
    labs_stale: bool
    scored_at: datetime
    scorer_version: str
    hr_latest: int | None = None
    spo2_latest: int | None = None
    sbp_latest: int | None = None
    temp_latest: float | None = None


class WardMonitorResponse(BaseAPIModel):
    """Live ward monitor payload, ordered worst-first."""

    ward_id: str
    scorer_version: str
    timestamp: datetime
    total_patients: int
    patients: list[WardPatientSnapshot]


class WardSummaryResponse(BaseAPIModel):
    """Aggregate risk and alert metrics across the ward."""

    ward_id: str
    timestamp: datetime
    total_patients: int
    critical_count: int
    high_count: int
    medium_count: int
    low_count: int
    deteriorating_count: int
    stale_labs_count: int
    mean_composite_risk: float


class VitalReadingItem(BaseAPIModel):
    """Single vital sign observation for trend sparklines."""

    patient_id: str
    measured_at: datetime
    reading_id: str
    bed_id: str
    heart_rate: int | None = None
    spo2: int | None = None
    systolic_bp: int | None = None
    diastolic_bp: int | None = None
    temperature: float | None = None
    respiratory_rate: int | None = None
    consciousness: str | None = None
    on_supplemental_oxygen: bool = False
    parameters_missing: int = 0
    ingest_time: datetime


class LabResultItem(BaseAPIModel):
    """Pathology result with laboratory reference range and abnormal flag."""

    patient_id: str
    test_type: str
    collected_at: datetime
    result_value: float
    unit: str
    reference_low: float | None = None
    reference_high: float | None = None
    out_of_range: bool = False
    sim_date: date | None = None


class RiskTrajectoryItem(BaseAPIModel):
    """Auditable historical risk evaluation with subscore breakdown."""

    patient_id: str
    scorer_version: str
    scored_at: datetime
    window_start: datetime
    window_end: datetime
    news2_total: int
    news2_subscores: dict[str, int]
    any_parameter_is_3: bool
    clinical_risk: str
    lab_contribution: int
    composite_risk: int
    risk_tier: str
    labs_stale: bool
    confidence: str
    readings_in_window: int
    hr_slope: float | None = None
    spo2_slope: float | None = None
    sbp_slope: float | None = None
    temp_slope: float | None = None
    explanation: str = Field(
        default="",
        description="Clinical explanation string describing risk drivers and lab pathology",
    )


class PatientDetailResponse(BaseAPIModel):
    """Comprehensive patient record combining vitals, risk evaluation, and lab panel."""

    patient_id: str
    bed_id: str
    admitting_condition: str | None = None
    current_risk: RiskTrajectoryItem | None = None
    latest_vitals: list[VitalReadingItem] = Field(default_factory=list)
    latest_labs: list[LabResultItem] = Field(default_factory=list)


class AlertItem(BaseAPIModel):
    """Clinical deterioration alert item."""

    alert_id: str
    ward_id: str
    sim_date: date
    alert_time: datetime
    patient_id: str
    bed_id: str
    alert_type: str
    severity: str
    news2_total: int
    composite_risk: int
    detail: str
    acknowledged: bool


class AlertAcknowledgeRequest(BaseModel):
    """Request payload to acknowledge a clinical alert."""

    acknowledged_by: str = Field(..., description="Clinician ID or name")
    note: str | None = Field(default=None, description="Optional clinical triage notes")


class AlertAcknowledgeResponse(BaseAPIModel):
    """Response returned upon acknowledging an alert."""

    alert_id: str
    status: str
    acknowledged_at: datetime
    acknowledged_by: str


class DailyReportMetaResponse(BaseAPIModel):
    """Metadata regarding a daily clinical risk summary report."""

    ward_id: str
    sim_date: str
    pdf_path: str
    html_path: str
    generated: bool
    download_url: str


class ReplayDiffResponse(BaseAPIModel):
    """Version divergence comparison evaluating v1 vs v2 replay outcomes."""

    v1_version: str
    v2_version: str
    total_patients_compared: int
    copd_reclassifications: int
    mean_risk_delta: float
    clinical_summary: str
    patient_diffs: list[dict[str, Any]] = Field(default_factory=list)


class PipelineStatusResponse(BaseAPIModel):
    """Pipeline health, clock synchronization, and streaming metrics."""

    status: str
    sim_now: datetime
    sim_date: str
    active_scorer_version: str
    speedup_factor: float
    connected_services: dict[str, str]


class CutoverRequest(BaseAPIModel):
    """Request payload to switch or roll back active scorer version."""

    target_version: str = Field(..., description="Target version (e.g. v2 or v1)")


class CutoverResponse(BaseAPIModel):
    """Result of an atomic scorer version cutover."""

    status: str
    previous_version: str
    active_version: str
    timestamp: datetime

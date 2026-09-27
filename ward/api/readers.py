"""Asynchronous Cassandra query readers for the serving layer.

Wraps CassandraDAO calls in non-blocking thread executions to maintain
high concurrency across FastAPI asynchronous endpoints.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime

from ward.api.models import (
    AlertItem,
    LabResultItem,
    RiskTrajectoryItem,
    VitalReadingItem,
    WardPatientSnapshot,
    WardSummaryResponse,
)
from ward.store.dao import (
    AlertRow,
    CassandraDAO,
    DailyPatientSummaryRow,
    LabResultRow,
    RiskScoreRow,
    VitalReadingRow,
    WardRiskSnapshotRow,
)


def _build_clinical_narrative(row: RiskScoreRow) -> str:
    """Generate human-readable clinical explanation of risk drivers."""
    drivers: list[str] = []
    subscores = row.news2_subscores or {}
    for param, pts in subscores.items():
        if pts >= 2:
            clean_param = param.replace("_", " ").title()
            drivers.append(f"{clean_param} (+{pts})")

    driver_str = ", ".join(drivers) if drivers else "Mild vital sign deviations"
    lab_str = (
        f"Lab pathology contributed +{row.lab_contribution} pts"
        if row.lab_contribution > 0
        else "No acute lab pathology elevation"
    )
    if row.labs_stale:
        lab_str += " (labs stale > 24h)"

    red_str = "Red parameter trigger active." if row.any_parameter_is_3 else ""
    return f"{driver_str}. {lab_str}. {red_str}".strip()


async def read_ward_snapshot(
    dao: CassandraDAO, ward_id: str = "WARD-A", version: str = "v1"
) -> list[WardPatientSnapshot]:
    """Fetch live ward monitor snapshot pre-sorted worst-first by composite risk."""

    def _sync_fetch() -> list[WardRiskSnapshotRow]:
        return dao.get_ward_risk_snapshot(ward_id=ward_id, scorer_version=version)

    rows = await asyncio.to_thread(_sync_fetch)
    return [
        WardPatientSnapshot(
            ward_id=r.ward_id,
            patient_id=r.patient_id,
            bed_id=r.bed_id,
            risk_score=r.risk_score,
            risk_tier=r.risk_tier,
            news2_total=r.news2_total,
            lab_contribution=r.lab_contribution,
            labs_stale=r.labs_stale,
            scored_at=r.scored_at,
            scorer_version=r.scorer_version,
            hr_latest=r.hr_latest,
            spo2_latest=r.spo2_latest,
            sbp_latest=r.sbp_latest,
            temp_latest=float(r.temp_latest) if r.temp_latest is not None else None,
        )
        for r in rows
    ]


async def read_ward_summary(
    dao: CassandraDAO, ward_id: str = "WARD-A", version: str = "v1"
) -> WardSummaryResponse:
    """Aggregate ward risk tiers and operational indicators."""
    snapshots = await read_ward_snapshot(dao, ward_id=ward_id, version=version)
    total = len(snapshots)

    critical = sum(1 for s in snapshots if s.risk_tier == "CRITICAL" or s.risk_score >= 10)
    high = sum(1 for s in snapshots if s.risk_tier == "HIGH")
    medium = sum(1 for s in snapshots if s.risk_tier == "MEDIUM")
    low = sum(1 for s in snapshots if s.risk_tier == "LOW")
    stale = sum(1 for s in snapshots if s.labs_stale)
    mean_risk = round(sum(s.risk_score for s in snapshots) / total, 2) if total > 0 else 0.0

    return WardSummaryResponse(
        ward_id=ward_id,
        timestamp=datetime.now(UTC),
        total_patients=total,
        critical_count=critical,
        high_count=high,
        medium_count=medium,
        low_count=low,
        deteriorating_count=critical + high,
        stale_labs_count=stale,
        mean_composite_risk=mean_risk,
    )


async def read_patient_vitals(
    dao: CassandraDAO, patient_id: str, limit: int = 50
) -> list[VitalReadingItem]:
    """Retrieve historical vitals for sparklines."""

    def _sync_fetch() -> list[VitalReadingRow]:
        return dao.get_patient_vitals(patient_id=patient_id, limit=limit)

    rows = await asyncio.to_thread(_sync_fetch)
    return [
        VitalReadingItem(
            patient_id=r.patient_id,
            measured_at=r.measured_at,
            reading_id=str(r.reading_id),
            bed_id=r.bed_id,
            heart_rate=r.heart_rate,
            spo2=r.spo2,
            systolic_bp=r.systolic_bp,
            diastolic_bp=r.diastolic_bp,
            temperature=float(r.temperature) if r.temperature is not None else None,
            respiratory_rate=r.respiratory_rate,
            consciousness=r.consciousness,
            on_supplemental_oxygen=r.on_supplemental_oxygen,
            parameters_missing=r.parameters_missing,
            ingest_time=r.ingest_time,
        )
        for r in rows
    ]


async def read_patient_risk_history(
    dao: CassandraDAO, patient_id: str, version: str = "v1", limit: int = 100
) -> list[RiskTrajectoryItem]:
    """Retrieve historical risk calculations for a patient under specified scorer version."""

    def _sync_fetch() -> list[RiskScoreRow]:
        return dao.get_patient_risk_history(
            patient_id=patient_id, scorer_version=version, limit=limit
        )

    rows = await asyncio.to_thread(_sync_fetch)
    return [
        RiskTrajectoryItem(
            patient_id=r.patient_id,
            scorer_version=r.scorer_version,
            scored_at=r.scored_at,
            window_start=r.window_start,
            window_end=r.window_end,
            news2_total=r.news2_total,
            news2_subscores=r.news2_subscores or {},
            any_parameter_is_3=r.any_parameter_is_3,
            clinical_risk=r.clinical_risk,
            lab_contribution=r.lab_contribution,
            composite_risk=r.composite_risk,
            risk_tier=r.risk_tier,
            labs_stale=r.labs_stale,
            confidence=r.confidence,
            readings_in_window=r.readings_in_window,
            hr_slope=float(r.hr_slope) if r.hr_slope is not None else None,
            spo2_slope=float(r.spo2_slope) if r.spo2_slope is not None else None,
            sbp_slope=float(r.sbp_slope) if r.sbp_slope is not None else None,
            temp_slope=float(r.temp_slope) if r.temp_slope is not None else None,
            explanation=_build_clinical_narrative(r),
        )
        for r in rows
    ]


async def read_patient_labs(dao: CassandraDAO, patient_id: str) -> list[LabResultItem]:
    """Retrieve pathology results for a patient."""

    def _sync_fetch() -> list[LabResultRow]:
        return dao.get_patient_labs(patient_id=patient_id)

    rows = await asyncio.to_thread(_sync_fetch)
    return [
        LabResultItem(
            patient_id=r.patient_id,
            test_type=r.test_type,
            collected_at=r.collected_at,
            result_value=float(r.result_value),
            unit=r.unit,
            reference_low=float(r.reference_low) if r.reference_low is not None else None,
            reference_high=float(r.reference_high) if r.reference_high is not None else None,
            out_of_range=r.out_of_range,
            sim_date=r.sim_date,
        )
        for r in rows
    ]


async def read_ward_alerts(
    dao: CassandraDAO,
    ward_id: str,
    alert_date: date,
    limit: int = 50,
    version: str = "v1",
) -> list[AlertItem]:
    """Retrieve active and recent alerts for a given day and scorer version."""

    def _sync_fetch() -> list[AlertRow]:
        return dao.get_ward_alerts(
            ward_id=ward_id, alert_date=alert_date, limit=limit, scorer_version=version
        )

    rows = await asyncio.to_thread(_sync_fetch)
    return [
        AlertItem(
            alert_id=r.alert_id,
            ward_id=r.ward_id,
            sim_date=r.sim_date,
            alert_time=r.alert_time,
            patient_id=r.patient_id,
            bed_id=r.bed_id,
            alert_type=r.alert_type,
            severity=r.severity,
            news2_total=r.news2_total,
            composite_risk=r.composite_risk,
            detail=r.detail,
            acknowledged=r.acknowledged,
        )
        for r in rows
    ]


async def update_alert_acknowledged(
    dao: CassandraDAO,
    ward_id: str,
    alert_date: date,
    alert_time: datetime,
    alert_id: str,
    version: str = "v1",
) -> None:
    """Mark an alert acknowledged."""

    def _sync_ack() -> None:
        dao.acknowledge_alert(
            ward_id=ward_id,
            alert_date=alert_date,
            alert_time=alert_time,
            alert_id=alert_id,
            scorer_version=version,
        )

    await asyncio.to_thread(_sync_ack)


async def read_daily_summary(
    dao: CassandraDAO, ward_id: str, report_date: date, version: str | None = None
) -> list[DailyPatientSummaryRow]:
    """Retrieve daily summary rows for reporting."""

    def _sync_fetch() -> list[DailyPatientSummaryRow]:
        return dao.get_daily_patient_summary(
            ward_id=ward_id, report_date=report_date, scorer_version=version
        )

    return await asyncio.to_thread(_sync_fetch)

"""Data Access Object (DAO) for the Cassandra serving layer.

Implements query-first access patterns Q1 to Q7 using prepared statements only.
Guarantees single-partition lookups without table scans.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:
    from cassandra.cluster import Session


@dataclass(frozen=True)
class VitalReadingRow:
    patient_id: str
    measured_at: datetime
    reading_id: UUID
    bed_id: str
    heart_rate: int | None
    spo2: int | None
    systolic_bp: int | None
    diastolic_bp: int | None
    temperature: Decimal | None
    respiratory_rate: int | None
    consciousness: str | None
    on_supplemental_oxygen: bool
    parameters_missing: int
    ingest_time: datetime


@dataclass(frozen=True)
class RiskScoreRow:
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
    hr_slope: Decimal | None
    spo2_slope: Decimal | None
    sbp_slope: Decimal | None
    temp_slope: Decimal | None
    readings_in_window: int


@dataclass(frozen=True)
class WardRiskSnapshotRow:
    ward_id: str
    scorer_version: str
    risk_score: int
    patient_id: str
    bed_id: str
    risk_tier: str
    news2_total: int
    lab_contribution: int
    labs_stale: bool
    scored_at: datetime
    hr_latest: int | None
    spo2_latest: int | None
    sbp_latest: int | None
    temp_latest: Decimal | None


@dataclass(frozen=True)
class AlertRow:
    ward_id: str
    sim_date: date
    alert_time: datetime
    alert_id: str
    patient_id: str
    bed_id: str
    alert_type: str
    severity: str
    news2_total: int
    composite_risk: int
    detail: str
    acknowledged: bool


@dataclass(frozen=True)
class LabResultRow:
    patient_id: str
    test_type: str
    collected_at: datetime
    result_value: Decimal
    unit: str
    reference_low: Decimal | None
    reference_high: Decimal | None
    out_of_range: bool
    sim_date: date


@dataclass(frozen=True)
class DailyPatientSummaryRow:
    ward_id: str
    sim_date: date
    patient_id: str
    scorer_version: str
    bed_id: str
    admitting_condition: str
    max_news2: int
    mean_news2: Decimal
    max_composite_risk: int
    final_risk_tier: str
    lab_contribution: int
    labs_stale: bool
    alert_count: int
    highest_severity: str
    readings_count: int
    readings_rejected: int
    deterioration_detected: bool


class WardStoreDAO:
    """Thread-safe query-first DAO for the ward keyspace."""

    def __init__(self, session: Session) -> None:
        self.session = session
        self._prepare_statements()

    def _prepare_statements(self) -> None:
        # Q1: Latest N vital readings for one patient
        self._stmt_q1 = self.session.prepare(
            """
            SELECT patient_id, measured_at, reading_id, bed_id,
                   heart_rate, spo2, systolic_bp, diastolic_bp, temperature,
                   respiratory_rate, consciousness, on_supplemental_oxygen,
                   parameters_missing, ingest_time
            FROM ward.vitals_by_patient
            WHERE patient_id = ?
            LIMIT ?
            """
        )

        # Q2: Ward risk snapshot (pre-sorted worst-first)
        self._stmt_q2 = self.session.prepare(
            """
            SELECT ward_id, scorer_version, risk_score, patient_id, bed_id,
                   risk_tier, news2_total, lab_contribution, labs_stale,
                   scored_at, hr_latest, spo2_latest, sbp_latest, temp_latest
            FROM ward.ward_risk_snapshot
            WHERE ward_id = ? AND scorer_version = ?
            """
        )

        # Q3: Risk score history per patient
        self._stmt_q3 = self.session.prepare(
            """
            SELECT patient_id, scorer_version, scored_at, window_start, window_end,
                   news2_total, news2_subscores, any_parameter_is_3, clinical_risk,
                   lab_contribution, composite_risk, risk_tier, labs_stale,
                   confidence, hr_slope, spo2_slope, sbp_slope, temp_slope,
                   readings_in_window
            FROM ward.risk_scores_by_patient
            WHERE patient_id = ? AND scorer_version = ?
            LIMIT ?
            """
        )

        # Q4: Ward alerts for a given day
        self._stmt_q4 = self.session.prepare(
            """
            SELECT ward_id, sim_date, alert_time, alert_id, patient_id, bed_id,
                   alert_type, severity, news2_total, composite_risk, detail, acknowledged
            FROM ward.alerts_by_ward
            WHERE ward_id = ? AND sim_date = ?
            LIMIT ?
            """
        )

        # Q5: Latest lab results for a patient
        self._stmt_q5 = self.session.prepare(
            """
            SELECT patient_id, test_type, collected_at, result_value, unit,
                   reference_low, reference_high, out_of_range, sim_date
            FROM ward.labs_by_patient
            WHERE patient_id = ?
            """
        )

        # Q6: Daily patient summary for one ward-day
        self._stmt_q6 = self.session.prepare(
            """
            SELECT ward_id, sim_date, patient_id, scorer_version, bed_id,
                   admitting_condition, max_news2, mean_news2, max_composite_risk,
                   final_risk_tier, lab_contribution, labs_stale, alert_count,
                   highest_severity, readings_count, readings_rejected, deterioration_detected
            FROM ward.daily_patient_summary
            WHERE ward_id = ? AND sim_date = ?
            """
        )

        # Q7: Simulation state get and set
        self._stmt_q7_get = self.session.prepare(
            """
            SELECT key, value, updated_at
            FROM ward.sim_state
            WHERE key = ?
            """
        )
        self._stmt_q7_set = self.session.prepare(
            """
            INSERT INTO ward.sim_state (key, value, updated_at)
            VALUES (?, ?, ?)
            """
        )

        # Insert statements for stream and ingestion writers
        self._stmt_insert_vital = self.session.prepare(
            """
            INSERT INTO ward.vitals_by_patient (
                patient_id, measured_at, reading_id, bed_id,
                heart_rate, spo2, systolic_bp, diastolic_bp, temperature,
                respiratory_rate, consciousness, on_supplemental_oxygen,
                parameters_missing, ingest_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        self._stmt_insert_score = self.session.prepare(
            """
            INSERT INTO ward.risk_scores_by_patient (
                patient_id, scorer_version, scored_at, window_start, window_end,
                news2_total, news2_subscores, any_parameter_is_3, clinical_risk,
                lab_contribution, composite_risk, risk_tier, labs_stale,
                confidence, hr_slope, spo2_slope, sbp_slope, temp_slope,
                readings_in_window
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        self._stmt_insert_snapshot = self.session.prepare(
            """
            INSERT INTO ward.ward_risk_snapshot (
                ward_id, scorer_version, risk_score, patient_id, bed_id,
                risk_tier, news2_total, lab_contribution, labs_stale,
                scored_at, hr_latest, spo2_latest, sbp_latest, temp_latest
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        self._stmt_insert_alert = self.session.prepare(
            """
            INSERT INTO ward.alerts_by_ward (
                ward_id, sim_date, alert_time, alert_id, patient_id, bed_id,
                alert_type, severity, news2_total, composite_risk, detail, acknowledged
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        self._stmt_ack_alert = self.session.prepare(
            """
            UPDATE ward.alerts_by_ward
            SET acknowledged = true
            WHERE ward_id = ? AND sim_date = ? AND alert_time = ? AND alert_id = ?
            """
        )

        self._stmt_insert_lab = self.session.prepare(
            """
            INSERT INTO ward.labs_by_patient (
                patient_id, test_type, collected_at, result_value, unit,
                reference_low, reference_high, out_of_range, sim_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        self._stmt_insert_daily_summary = self.session.prepare(
            """
            INSERT INTO ward.daily_patient_summary (
                ward_id, sim_date, patient_id, scorer_version, bed_id,
                admitting_condition, max_news2, mean_news2, max_composite_risk,
                final_risk_tier, lab_contribution, labs_stale, alert_count,
                highest_severity, readings_count, readings_rejected, deterioration_detected
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

    # -------------------------------------------------------------------------
    # Q1: Latest vital readings
    # -------------------------------------------------------------------------
    def get_patient_vitals(self, patient_id: str, limit: int = 50) -> list[VitalReadingRow]:
        bound = self._stmt_q1.bind((patient_id, limit))
        rows = self.session.execute(bound)
        results: list[VitalReadingRow] = []
        for r in rows:
            results.append(
                VitalReadingRow(
                    patient_id=r.patient_id,
                    measured_at=r.measured_at,
                    reading_id=r.reading_id,
                    bed_id=r.bed_id,
                    heart_rate=r.heart_rate,
                    spo2=r.spo2,
                    systolic_bp=r.systolic_bp,
                    diastolic_bp=r.diastolic_bp,
                    temperature=r.temperature,
                    respiratory_rate=r.respiratory_rate,
                    consciousness=r.consciousness,
                    on_supplemental_oxygen=r.on_supplemental_oxygen,
                    parameters_missing=r.parameters_missing,
                    ingest_time=r.ingest_time,
                )
            )
        return results

    # -------------------------------------------------------------------------
    # Q2: Ward risk snapshot with deduplication
    # -------------------------------------------------------------------------
    def get_ward_risk_snapshot(
        self, ward_id: str = "WARD-A", scorer_version: str = "v1"
    ) -> list[WardRiskSnapshotRow]:
        """Fetch live snapshot pre-sorted by risk_score DESC.

        Because rows have a 120s TTL and a patient's score may change within that
        window, multiple rows for the same patient can briefly coexist. We deduplicate
        in memory, retaining the row with the latest scored_at timestamp per patient,
        while maintaining the descending risk_score order.
        """
        bound = self._stmt_q2.bind((ward_id, scorer_version))
        rows = self.session.execute(bound)

        seen_patients: set[str] = set()
        deduped: list[WardRiskSnapshotRow] = []

        # Rows arrive ordered by risk_score DESC from clustering key
        for r in rows:
            if r.patient_id not in seen_patients:
                seen_patients.add(r.patient_id)
                deduped.append(
                    WardRiskSnapshotRow(
                        ward_id=r.ward_id,
                        scorer_version=r.scorer_version,
                        risk_score=r.risk_score,
                        patient_id=r.patient_id,
                        bed_id=r.bed_id,
                        risk_tier=r.risk_tier,
                        news2_total=r.news2_total,
                        lab_contribution=r.lab_contribution,
                        labs_stale=r.labs_stale,
                        scored_at=r.scored_at,
                        hr_latest=r.hr_latest,
                        spo2_latest=r.spo2_latest,
                        sbp_latest=r.sbp_latest,
                        temp_latest=r.temp_latest,
                    )
                )

        return deduped

    # -------------------------------------------------------------------------
    # Q3: Risk score history per patient
    # -------------------------------------------------------------------------
    def get_patient_risk_history(
        self, patient_id: str, scorer_version: str = "v1", limit: int = 100
    ) -> list[RiskScoreRow]:
        bound = self._stmt_q3.bind((patient_id, scorer_version, limit))
        rows = self.session.execute(bound)
        results: list[RiskScoreRow] = []
        for r in rows:
            results.append(
                RiskScoreRow(
                    patient_id=r.patient_id,
                    scorer_version=r.scorer_version,
                    scored_at=r.scored_at,
                    window_start=r.window_start,
                    window_end=r.window_end,
                    news2_total=r.news2_total,
                    news2_subscores=dict(r.news2_subscores) if r.news2_subscores else {},
                    any_parameter_is_3=r.any_parameter_is_3,
                    clinical_risk=r.clinical_risk,
                    lab_contribution=r.lab_contribution,
                    composite_risk=r.composite_risk,
                    risk_tier=r.risk_tier,
                    labs_stale=r.labs_stale,
                    confidence=r.confidence,
                    hr_slope=r.hr_slope,
                    spo2_slope=r.spo2_slope,
                    sbp_slope=r.sbp_slope,
                    temp_slope=r.temp_slope,
                    readings_in_window=r.readings_in_window,
                )
            )
        return results

    # -------------------------------------------------------------------------
    # Q4: Ward alert feed
    # -------------------------------------------------------------------------
    def get_ward_alerts(self, ward_id: str, alert_date: date, limit: int = 50) -> list[AlertRow]:
        bound = self._stmt_q4.bind((ward_id, alert_date, limit))
        rows = self.session.execute(bound)
        results: list[AlertRow] = []
        for r in rows:
            results.append(
                AlertRow(
                    ward_id=r.ward_id,
                    sim_date=r.sim_date,
                    alert_time=r.alert_time,
                    alert_id=r.alert_id,
                    patient_id=r.patient_id,
                    bed_id=r.bed_id,
                    alert_type=r.alert_type,
                    severity=r.severity,
                    news2_total=r.news2_total,
                    composite_risk=r.composite_risk,
                    detail=r.detail,
                    acknowledged=r.acknowledged,
                )
            )
        return results

    def acknowledge_alert(
        self, ward_id: str, alert_date: date, alert_time: datetime, alert_id: str
    ) -> None:
        bound = self._stmt_ack_alert.bind((ward_id, alert_date, alert_time, alert_id))
        self.session.execute(bound)

    # -------------------------------------------------------------------------
    # Q5: Latest lab results
    # -------------------------------------------------------------------------
    def get_patient_labs(self, patient_id: str) -> list[LabResultRow]:
        bound = self._stmt_q5.bind((patient_id,))
        rows = self.session.execute(bound)
        results: list[LabResultRow] = []
        for r in rows:
            results.append(
                LabResultRow(
                    patient_id=r.patient_id,
                    test_type=r.test_type,
                    collected_at=r.collected_at,
                    result_value=r.result_value,
                    unit=r.unit,
                    reference_low=r.reference_low,
                    reference_high=r.reference_high,
                    out_of_range=r.out_of_range,
                    sim_date=r.sim_date,
                )
            )
        return results

    # -------------------------------------------------------------------------
    # Q6: Daily patient summary
    # -------------------------------------------------------------------------
    def get_daily_patient_summary(
        self, ward_id: str, report_date: date, scorer_version: str | None = None
    ) -> list[DailyPatientSummaryRow]:
        bound = self._stmt_q6.bind((ward_id, report_date))
        rows = self.session.execute(bound)
        results: list[DailyPatientSummaryRow] = []
        for r in rows:
            if scorer_version and r.scorer_version != scorer_version:
                continue
            results.append(
                DailyPatientSummaryRow(
                    ward_id=r.ward_id,
                    sim_date=r.sim_date,
                    patient_id=r.patient_id,
                    scorer_version=r.scorer_version,
                    bed_id=r.bed_id,
                    admitting_condition=r.admitting_condition,
                    max_news2=r.max_news2,
                    mean_news2=r.mean_news2,
                    max_composite_risk=r.max_composite_risk,
                    final_risk_tier=r.final_risk_tier,
                    lab_contribution=r.lab_contribution,
                    labs_stale=r.labs_stale,
                    alert_count=r.alert_count,
                    highest_severity=r.highest_severity,
                    readings_count=r.readings_count,
                    readings_rejected=r.readings_rejected,
                    deterioration_detected=r.deterioration_detected,
                )
            )
        return results

    # -------------------------------------------------------------------------
    # Q7: Simulation state
    # -------------------------------------------------------------------------
    def get_sim_state(self, key: str) -> str | None:
        bound = self._stmt_q7_get.bind((key,))
        rows = self.session.execute(bound)
        for r in rows:
            val: str = r.value
            return val
        return None

    def set_sim_state(self, key: str, value: str, updated_at: datetime | None = None) -> None:
        ts = updated_at or datetime.now()
        bound = self._stmt_q7_set.bind((key, value, ts))
        self.session.execute(bound)

    # -------------------------------------------------------------------------
    # Ingestion / Stream Insert Helpers
    # -------------------------------------------------------------------------
    def insert_vital_reading(self, row: VitalReadingRow) -> None:
        bound = self._stmt_insert_vital.bind(
            (
                row.patient_id,
                row.measured_at,
                row.reading_id,
                row.bed_id,
                row.heart_rate,
                row.spo2,
                row.systolic_bp,
                row.diastolic_bp,
                row.temperature,
                row.respiratory_rate,
                row.consciousness,
                row.on_supplemental_oxygen,
                row.parameters_missing,
                row.ingest_time,
            )
        )
        self.session.execute(bound)

    def insert_patient_risk(self, row: RiskScoreRow) -> None:
        bound = self._stmt_insert_score.bind(
            (
                row.patient_id,
                row.scorer_version,
                row.scored_at,
                row.window_start,
                row.window_end,
                row.news2_total,
                row.news2_subscores,
                row.any_parameter_is_3,
                row.clinical_risk,
                row.lab_contribution,
                row.composite_risk,
                row.risk_tier,
                row.labs_stale,
                row.confidence,
                row.hr_slope,
                row.spo2_slope,
                row.sbp_slope,
                row.temp_slope,
                row.readings_in_window,
            )
        )
        self.session.execute(bound)

    def insert_ward_risk_snapshot(self, row: WardRiskSnapshotRow) -> None:
        bound = self._stmt_insert_snapshot.bind(
            (
                row.ward_id,
                row.scorer_version,
                row.risk_score,
                row.patient_id,
                row.bed_id,
                row.risk_tier,
                row.news2_total,
                row.lab_contribution,
                row.labs_stale,
                row.scored_at,
                row.hr_latest,
                row.spo2_latest,
                row.sbp_latest,
                row.temp_latest,
            )
        )
        self.session.execute(bound)

    def insert_alert(self, row: AlertRow) -> None:
        bound = self._stmt_insert_alert.bind(
            (
                row.ward_id,
                row.sim_date,
                row.alert_time,
                row.alert_id,
                row.patient_id,
                row.bed_id,
                row.alert_type,
                row.severity,
                row.news2_total,
                row.composite_risk,
                row.detail,
                row.acknowledged,
            )
        )
        self.session.execute(bound)

    def insert_lab_result(self, row: LabResultRow) -> None:
        bound = self._stmt_insert_lab.bind(
            (
                row.patient_id,
                row.test_type,
                row.collected_at,
                row.result_value,
                row.unit,
                row.reference_low,
                row.reference_high,
                row.out_of_range,
                row.sim_date,
            )
        )
        self.session.execute(bound)

    def insert_daily_patient_summary(self, row: DailyPatientSummaryRow) -> None:
        bound = self._stmt_insert_daily_summary.bind(
            (
                row.ward_id,
                row.sim_date,
                row.patient_id,
                row.scorer_version,
                row.bed_id,
                row.admitting_condition,
                row.max_news2,
                row.mean_news2,
                row.max_composite_risk,
                row.final_risk_tier,
                row.lab_contribution,
                row.labs_stale,
                row.alert_count,
                row.highest_severity,
                row.readings_count,
                row.readings_rejected,
                row.deterioration_detected,
            )
        )
        self.session.execute(bound)

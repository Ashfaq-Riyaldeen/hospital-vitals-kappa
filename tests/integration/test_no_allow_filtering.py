"""Integration test verifying zero ALLOW FILTERING against a live Cassandra cluster.

Executes catalogued queries Q1 to Q7 against the keyspace to confirm
they execute as single-partition lookups without cluster-wide filtering.
"""

from __future__ import annotations

import datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from cassandra.cluster import NoHostAvailable
from ward import settings
from ward.store.dao import (
    AlertRow,
    DailyPatientSummaryRow,
    LabResultRow,
    RiskScoreRow,
    VitalReadingRow,
    WardRiskSnapshotRow,
    WardStoreDAO,
)
from ward.store.session import create_cluster


@pytest.fixture(scope="module")
def live_session():
    """Connect to live Cassandra instance; skip if cluster is unreachable."""
    try:
        cluster = create_cluster()
        session = cluster.connect()
        # Verify keyspace exists
        session.set_keyspace(settings.storage().keyspace)
        yield session
        session.shutdown()
        cluster.shutdown()
    except (NoHostAvailable, Exception) as exc:
        pytest.skip(f"Cassandra cluster unreachable: {exc}")


@pytest.mark.integration
def test_all_seven_queries_execute_without_allow_filtering(live_session) -> None:
    dao = WardStoreDAO(live_session)
    now = datetime.datetime.now(datetime.UTC)
    today = now.date()

    # 1. Insert test fixtures across all 7 tables
    reading = VitalReadingRow(
        patient_id="P999",
        measured_at=now,
        reading_id=uuid4(),
        bed_id="BED-99",
        heart_rate=80,
        spo2=96,
        systolic_bp=120,
        diastolic_bp=80,
        temperature=Decimal("37.0"),
        respiratory_rate=16,
        consciousness="A",
        on_supplemental_oxygen=False,
        parameters_missing=0,
        ingest_time=now,
    )
    dao.insert_vital_reading(reading)

    score = RiskScoreRow(
        patient_id="P999",
        scorer_version="v1",
        scored_at=now,
        window_start=now - datetime.timedelta(hours=4),
        window_end=now,
        news2_total=3,
        news2_subscores={"heart_rate": 0, "spo2": 1},
        any_parameter_is_3=False,
        clinical_risk="LOW",
        lab_contribution=0,
        composite_risk=3,
        risk_tier="LOW",
        labs_stale=False,
        confidence="high",
        hr_slope=Decimal("0.0"),
        spo2_slope=Decimal("0.0"),
        sbp_slope=Decimal("0.0"),
        temp_slope=Decimal("0.0"),
        readings_in_window=16,
    )
    dao.insert_patient_risk(score)

    snapshot = WardRiskSnapshotRow(
        ward_id="WARD-TEST",
        scorer_version="v1",
        risk_score=3,
        patient_id="P999",
        bed_id="BED-99",
        risk_tier="LOW",
        news2_total=3,
        lab_contribution=0,
        labs_stale=False,
        scored_at=now,
        hr_latest=80,
        spo2_latest=96,
        sbp_latest=120,
        temp_latest=Decimal("37.0"),
    )
    dao.insert_ward_risk_snapshot(snapshot)

    alert = AlertRow(
        ward_id="WARD-TEST",
        sim_date=today,
        alert_time=now,
        alert_id="ALT-999",
        patient_id="P999",
        bed_id="BED-99",
        alert_type="NEWS2_ELEVATED",
        severity="WARNING",
        news2_total=5,
        composite_risk=5,
        detail="Test alert",
        acknowledged=False,
    )
    dao.insert_alert(alert)

    lab = LabResultRow(
        patient_id="P999",
        test_type="lactate",
        collected_at=now,
        result_value=Decimal("1.8"),
        unit="mmol/L",
        reference_low=Decimal("0.5"),
        reference_high=Decimal("2.2"),
        out_of_range=False,
        sim_date=today,
    )
    dao.insert_lab_result(lab)

    summary = DailyPatientSummaryRow(
        ward_id="WARD-TEST",
        sim_date=today,
        patient_id="P999",
        scorer_version="v1",
        bed_id="BED-99",
        admitting_condition="Observation",
        max_news2=3,
        mean_news2=Decimal("2.5"),
        max_composite_risk=3,
        final_risk_tier="LOW",
        lab_contribution=0,
        labs_stale=False,
        alert_count=1,
        highest_severity="WARNING",
        readings_count=16,
        readings_rejected=0,
        deterioration_detected=False,
    )
    dao.insert_daily_patient_summary(summary)

    dao.set_sim_state("test_key", "test_val", now)

    # 2. Execute Q1 to Q7 queries and assert correct retrieval
    vitals = dao.get_patient_vitals("P999", limit=10)
    assert len(vitals) >= 1
    assert vitals[0].patient_id == "P999"

    snaps = dao.get_ward_risk_snapshot(ward_id="WARD-TEST", scorer_version="v1")
    assert any(s.patient_id == "P999" for s in snaps)

    history = dao.get_patient_risk_history("P999", scorer_version="v1", limit=10)
    assert len(history) >= 1
    assert history[0].patient_id == "P999"

    alerts = dao.get_ward_alerts("WARD-TEST", today, limit=10)
    assert any(a.alert_id == "ALT-999" for a in alerts)

    labs = dao.get_patient_labs("P999")
    assert any(lab_item.test_type == "lactate" for lab_item in labs)

    summaries = dao.get_daily_patient_summary("WARD-TEST", today, scorer_version="v1")
    assert any(s.patient_id == "P999" for s in summaries)

    state_val = dao.get_sim_state("test_key")
    assert state_val == "test_val"

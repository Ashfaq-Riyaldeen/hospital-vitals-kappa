"""API Contract and Schema Verification Tests for FastAPI Serving Layer.

Verifies:
1. Endpoint contracts, OpenAPI response shapes, and HTTP status codes.
2. Worst-first patient ordering from the Q2 Cassandra snapshot query.
3. Clinical explainability invariants: subscores sum to total, lab adjustments.
4. Alerts querying and acknowledgment.
5. Replay comparison diff calculation.
6. Multi-level health probes.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from ward.api.app import app
from ward.store.dao import (
    AlertRow,
    LabResultRow,
    RiskScoreRow,
    VitalReadingRow,
    WardRiskSnapshotRow,
)


@pytest.fixture
def mock_dao():
    dao = MagicMock()
    sim_d = date(2026, 4, 1)
    now = datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC)

    # Mock Q1: Vitals
    dao.get_patient_vitals.return_value = [
        VitalReadingRow(
            patient_id="P014",
            measured_at=now,
            reading_id=uuid4(),
            bed_id="BED-14",
            heart_rate=125,
            spo2=91,
            systolic_bp=88,
            diastolic_bp=55,
            temperature=Decimal("38.9"),
            respiratory_rate=26,
            consciousness="A",
            on_supplemental_oxygen=True,
            parameters_missing=0,
            ingest_time=now,
        )
    ]

    # Mock Q2: Snapshot (ordered worst-first)
    dao.get_ward_risk_snapshot.side_effect = lambda ward_id, scorer_version: (
        [
            WardRiskSnapshotRow(
                ward_id=ward_id,
                scorer_version=scorer_version,
                risk_score=10,
                patient_id="P014",
                bed_id="BED-14",
                risk_tier="CRITICAL",
                news2_total=7,
                lab_contribution=3,
                labs_stale=False,
                scored_at=now,
                hr_latest=125,
                spo2_latest=91,
                sbp_latest=88,
                temp_latest=Decimal("38.9"),
            ),
            WardRiskSnapshotRow(
                ward_id=ward_id,
                scorer_version=scorer_version,
                risk_score=6 if scorer_version == "v1" else 3,
                patient_id="P031",
                bed_id="BED-31",
                risk_tier="MEDIUM" if scorer_version == "v1" else "LOW",
                news2_total=6 if scorer_version == "v1" else 3,
                lab_contribution=0,
                labs_stale=False,
                scored_at=now,
                hr_latest=85,
                spo2_latest=90,
                sbp_latest=120,
                temp_latest=Decimal("36.8"),
            ),
            WardRiskSnapshotRow(
                ward_id=ward_id,
                scorer_version=scorer_version,
                risk_score=1,
                patient_id="P001",
                bed_id="BED-01",
                risk_tier="LOW",
                news2_total=1,
                lab_contribution=0,
                labs_stale=False,
                scored_at=now,
                hr_latest=72,
                spo2_latest=98,
                sbp_latest=118,
                temp_latest=Decimal("36.6"),
            ),
        ]
    )

    # Mock Q3: Risk History
    dao.get_patient_risk_history.return_value = [
        RiskScoreRow(
            patient_id="P014",
            scorer_version="v1",
            scored_at=now,
            window_start=now,
            window_end=now,
            news2_total=7,
            news2_subscores={
                "respiratory_rate": 3,
                "spo2": 2,
                "systolic_bp": 2,
                "heart_rate": 0,
                "temperature": 0,
                "consciousness": 0,
                "supplemental_oxygen": 0,
            },
            any_parameter_is_3=True,
            clinical_risk="HIGH",
            lab_contribution=3,
            composite_risk=10,
            risk_tier="CRITICAL",
            labs_stale=False,
            confidence="high",
            hr_slope=Decimal("2.5"),
            spo2_slope=Decimal("-1.2"),
            sbp_slope=Decimal("-4.0"),
            temp_slope=Decimal("0.3"),
            readings_in_window=16,
        )
    ]

    # Mock Q4: Alerts
    dao.get_ward_alerts.return_value = [
        AlertRow(
            ward_id="WARD-A",
            sim_date=sim_d,
            alert_time=now,
            alert_id="ALT-P014-001",
            patient_id="P014",
            bed_id="BED-14",
            alert_type="CRITICAL_RISK",
            severity="CRITICAL",
            news2_total=7,
            composite_risk=10,
            detail="NEWS2 7, lab corroboration +3 (lactate 3.8)",
            acknowledged=False,
        ),
        AlertRow(
            ward_id="WARD-A",
            sim_date=sim_d,
            alert_time=now,
            alert_id="ALT-P031-001",
            patient_id="P031",
            bed_id="BED-31",
            alert_type="TREND_CONCERNING",
            severity="MEDIUM",
            news2_total=4,
            composite_risk=4,
            detail="SpO2 trending downward",
            acknowledged=True,
        ),
    ]

    # Mock Q5: Labs
    dao.get_patient_labs.return_value = [
        LabResultRow(
            patient_id="P014",
            test_type="LACTATE",
            collected_at=now,
            result_value=Decimal("3.8"),
            unit="mmol/L",
            reference_low=Decimal("0.5"),
            reference_high=Decimal("2.2"),
            out_of_range=True,
            sim_date=sim_d,
        ),
        LabResultRow(
            patient_id="P014",
            test_type="WBC",
            collected_at=now,
            result_value=Decimal("18.4"),
            unit="10^9/L",
            reference_low=Decimal("4.0"),
            reference_high=Decimal("11.0"),
            out_of_range=True,
            sim_date=sim_d,
        ),
    ]

    return dao


@pytest.fixture
def client(mock_dao):
    app.state.dao = mock_dao
    session = MagicMock()
    session.execute.return_value.one.return_value = [datetime.now(UTC)]
    app.state.session = session
    return TestClient(app)


def test_health_endpoints(client):
    r_live = client.get("/health/live")
    assert r_live.status_code == 200
    assert r_live.json() == {"status": "alive"}

    r_ready = client.get("/health/ready")
    assert r_ready.status_code == 200
    assert r_ready.json() == {"status": "ready"}

    r_deep = client.get("/health/deep")
    assert r_deep.status_code == 200
    assert r_deep.json()["status"] == "healthy"
    assert "cassandra_latency_ms" in r_deep.json()


def test_ward_monitor_returns_worst_first_order(client):
    res = client.get("/api/v1/ward/WARD-A/monitor")
    assert res.status_code == 200
    data = res.json()

    assert data["ward_id"] == "WARD-A"
    assert data["total_patients"] == 3
    patients = data["patients"]
    assert len(patients) == 3

    # Assert worst-first order: 10 >= 6 >= 1
    scores = [p["risk_score"] for p in patients]
    assert scores == [10, 6, 1]
    assert patients[0]["patient_id"] == "P014"
    assert patients[0]["risk_tier"] == "CRITICAL"


def test_ward_summary_calculates_risk_bands(client):
    res = client.get("/api/v1/ward/WARD-A/summary")
    assert res.status_code == 200
    data = res.json()

    assert data["ward_id"] == "WARD-A"
    assert data["total_patients"] == 3
    assert data["critical_count"] == 1
    assert data["medium_count"] == 1
    assert data["low_count"] == 1
    assert data["deteriorating_count"] == 1
    assert data["mean_composite_risk"] == 5.67


def test_patient_detail_includes_risk_decomposition_and_narrative(client):
    res = client.get("/api/v1/patients/P014")
    assert res.status_code == 200
    data = res.json()

    assert data["patient_id"] == "P014"
    assert data["bed_id"] == "BED-14"
    risk = data["current_risk"]
    assert risk is not None

    # Auditability invariant: composite_risk = news2_total + lab_contribution
    assert risk["composite_risk"] == risk["news2_total"] + risk["lab_contribution"]
    assert risk["composite_risk"] == 10
    assert risk["lab_contribution"] == 3
    assert risk["any_parameter_is_3"] is True

    # Subscores breakdown exists
    subscores = risk["news2_subscores"]
    assert subscores["respiratory_rate"] == 3
    assert subscores["spo2"] == 2

    # Narrative explanation present
    assert "Respiratory Rate" in risk["explanation"]
    assert "+3" in risk["explanation"]

    # Vitals and labs joined
    assert len(data["latest_vitals"]) >= 1
    assert len(data["latest_labs"]) >= 2
    assert data["latest_labs"][0]["out_of_range"] is True


def test_patient_not_found(client, mock_dao):
    mock_dao.get_patient_vitals.return_value = []
    mock_dao.get_patient_risk_history.return_value = []
    mock_dao.get_patient_labs.return_value = []

    res = client.get("/api/v1/patients/P999")
    assert res.status_code == 404
    assert "not found" in res.json()["detail"].lower()


def test_alerts_feed_and_filtering(client):
    res = client.get("/api/v1/alerts?ward_id=WARD-A&sim_date=2026-04-01")
    assert res.status_code == 200
    alerts = res.json()
    assert len(alerts) == 2

    res_crit = client.get("/api/v1/alerts?ward_id=WARD-A&severity=CRITICAL")
    assert res_crit.status_code == 200
    crit_alerts = res_crit.json()
    assert len(crit_alerts) == 1
    assert crit_alerts[0]["severity"] == "CRITICAL"


def test_alert_acknowledgement(client, mock_dao):
    payload = {
        "acknowledged_by": "Dr. Sarah Connor",
        "note": "Reviewed bedside, initiated IV fluids",
    }
    res = client.post(
        "/api/v1/alerts/ALT-P014-001/acknowledge?sim_date=2026-04-01",
        json=payload,
    )
    assert res.status_code == 200
    data = res.json()
    assert data["alert_id"] == "ALT-P014-001"
    assert data["status"] == "ACKNOWLEDGED"
    assert data["acknowledged_by"] == "Dr. Sarah Connor"
    assert mock_dao.acknowledge_alert.called


def test_replay_comparison_diff(client):
    res = client.get("/api/v1/replay/compare?v1=v1&v2=v2")
    assert res.status_code == 200
    diff = res.json()

    assert diff["v1_version"] == "v1"
    assert diff["v2_version"] == "v2"
    assert diff["total_patients_compared"] == 3
    assert diff["copd_reclassifications"] == 1
    assert len(diff["patient_diffs"]) == 1

    p031 = diff["patient_diffs"][0]
    assert p031["patient_id"] == "P031"
    assert p031["v1_risk"] == 6
    assert p031["v2_risk"] == 3
    assert p031["delta"] == -3


def test_pipeline_status(client):
    res = client.get("/api/v1/pipeline/status")
    assert res.status_code == 200
    status_data = res.json()
    assert status_data["status"] == "RUNNING"
    assert status_data["connected_services"]["architecture"] == "Kappa"

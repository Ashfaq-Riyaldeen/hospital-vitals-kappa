"""Unit tests for WardStoreDAO using mock session."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

from ward.store.dao import (
    VitalReadingRow,
    WardStoreDAO,
)


def _make_mock_session() -> MagicMock:
    session = MagicMock()
    mock_prepared = MagicMock()
    mock_bound = MagicMock()
    mock_prepared.bind.return_value = mock_bound
    session.prepare.return_value = mock_prepared
    return session


def test_dao_initialization_prepares_statements() -> None:
    session = _make_mock_session()
    _ = WardStoreDAO(session)
    assert session.prepare.call_count >= 13


def test_ward_risk_snapshot_deduplication() -> None:
    """Snapshot deduplication takes the newest scored_at row per patient while preserving sort."""
    session = _make_mock_session()
    dao = WardStoreDAO(session)

    # Simulate Cassandra returning rows ordered by risk_score DESC
    # Patient P014 has an older row with score 10 and a newer row with score 8,
    # or P014 deteriorated from score 6 to 10.
    row1 = MagicMock(
        ward_id="WARD-A",
        scorer_version="v1",
        risk_score=10,
        patient_id="P014",
        bed_id="BED-14",
        risk_tier="CRITICAL",
        news2_total=7,
        lab_contribution=3,
        labs_stale=False,
        scored_at=datetime(2026, 4, 1, 10, 30, tzinfo=UTC),
        hr_latest=125,
        spo2_latest=88,
        sbp_latest=90,
        temp_latest=Decimal("38.8"),
    )
    row2 = MagicMock(
        ward_id="WARD-A",
        scorer_version="v1",
        risk_score=7,
        patient_id="P031",
        bed_id="BED-31",
        risk_tier="HIGH",
        news2_total=7,
        lab_contribution=0,
        labs_stale=False,
        scored_at=datetime(2026, 4, 1, 10, 30, tzinfo=UTC),
        hr_latest=95,
        spo2_latest=90,
        sbp_latest=130,
        temp_latest=Decimal("37.1"),
    )
    # Stale row for P014 with earlier score 6 still inside 120s TTL
    row3 = MagicMock(
        ward_id="WARD-A",
        scorer_version="v1",
        risk_score=6,
        patient_id="P014",
        bed_id="BED-14",
        risk_tier="MEDIUM",
        news2_total=6,
        lab_contribution=0,
        labs_stale=False,
        scored_at=datetime(2026, 4, 1, 10, 28, tzinfo=UTC),
        hr_latest=110,
        spo2_latest=92,
        sbp_latest=95,
        temp_latest=Decimal("38.2"),
    )

    session.execute.return_value = [row1, row2, row3]

    snapshot = dao.get_ward_risk_snapshot(ward_id="WARD-A", scorer_version="v1")
    assert len(snapshot) == 2
    # First row is P014 with score 10
    assert snapshot[0].patient_id == "P014"
    assert snapshot[0].risk_score == 10
    # Second row is P031 with score 7
    assert snapshot[1].patient_id == "P031"
    assert snapshot[1].risk_score == 7


def test_insert_vital_reading_binds_correct_fields() -> None:
    session = _make_mock_session()
    dao = WardStoreDAO(session)

    reading = VitalReadingRow(
        patient_id="P001",
        measured_at=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
        reading_id=uuid4(),
        bed_id="BED-01",
        heart_rate=72,
        spo2=98,
        systolic_bp=120,
        diastolic_bp=80,
        temperature=Decimal("36.8"),
        respiratory_rate=16,
        consciousness="A",
        on_supplemental_oxygen=False,
        parameters_missing=0,
        ingest_time=datetime(2026, 4, 1, 12, 0, 1, tzinfo=UTC),
    )

    dao.insert_vital_reading(reading)
    session.execute.assert_called_once()

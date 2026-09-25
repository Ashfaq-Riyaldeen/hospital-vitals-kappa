"""Unit tests for physiological impossibility cleaning."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from ward.contracts.models import VitalsReading
from ward.stream.clean import (
    REASON_BP_INVERTED,
    REASON_EMPTY_READING,
    REASON_FUTURE_TIMESTAMP,
    REASON_HR_OUT_OF_RANGE,
    REASON_SPO2_DETACHED,
    REASON_TEMP_OUT_OF_RANGE,
    validate_reading,
)


def _base_reading(**kwargs) -> VitalsReading:
    now = datetime(2026, 4, 1, 10, 0, tzinfo=UTC)
    fields = {
        "reading_id": "r-001",
        "patient_id": "P001",
        "bed_id": "BED-01",
        "device_id": "DEV-01",
        "measured_at": now,
        "ingest_time": now,
        "heart_rate": 75,
        "spo2": 98,
        "respiratory_rate": 16,
        "systolic_bp": 120,
        "diastolic_bp": 80,
        "temperature": 37.0,
        "consciousness": "A",
        "on_supplemental_oxygen": False,
        "producer_id": "bedside-01",
    }
    fields.update(kwargs)
    return VitalsReading(**fields)


def test_normal_reading_passes() -> None:
    valid, reason, missing = validate_reading(_base_reading())
    assert valid
    assert reason is None
    assert missing == 0


def test_severe_clinical_deterioration_is_preserved() -> None:
    """★ Invariant: Abnormal clinical values must NOT be filtered as outliers."""
    sick_reading = _base_reading(
        heart_rate=145,  # Tachycardia
        spo2=72,  # Critical hypoxia, but physiologically possible
        systolic_bp=75,  # Severe hypotension
        diastolic_bp=45,
        temperature=39.6,
        respiratory_rate=32,
    )
    valid, reason, _ = validate_reading(sick_reading)
    assert valid, f"Sick patient was incorrectly rejected: {reason}"


def test_detached_spo2_sensor_rejected_to_dlq() -> None:
    zero_spo2 = _base_reading(spo2=0)
    valid, reason, _ = validate_reading(zero_spo2)
    assert not valid
    assert reason == REASON_SPO2_DETACHED


def test_inverted_blood_pressure_rejected() -> None:
    inverted = _base_reading(systolic_bp=80, diastolic_bp=120)
    valid, reason, _ = validate_reading(inverted)
    assert not valid
    assert reason == REASON_BP_INVERTED


def test_heart_rate_lead_artefact_rejected() -> None:
    impossible_hr = _base_reading(heart_rate=320)
    valid, reason, _ = validate_reading(impossible_hr)
    assert not valid
    assert reason == REASON_HR_OUT_OF_RANGE


def test_extreme_temperature_rejected() -> None:
    impossible_temp = _base_reading(temperature=18.0)
    valid, reason, _ = validate_reading(impossible_temp)
    assert not valid
    assert reason == REASON_TEMP_OUT_OF_RANGE


def test_empty_reading_rejected() -> None:
    empty = _base_reading(
        heart_rate=None,
        spo2=None,
        respiratory_rate=None,
        systolic_bp=None,
        diastolic_bp=None,
        temperature=None,
        consciousness=None,
    )
    valid, reason, missing = validate_reading(empty)
    assert not valid
    assert reason == REASON_EMPTY_READING
    assert missing == 6


def test_future_timestamp_rejected() -> None:
    now = datetime(2026, 4, 1, 10, 0, tzinfo=UTC)
    future = _base_reading(measured_at=now + timedelta(hours=2))
    valid, reason, _ = validate_reading(future, sim_now=now)
    assert not valid
    assert reason == REASON_FUTURE_TIMESTAMP


def test_partial_reading_survives_with_missing_count() -> None:
    """Partial readings (e.g. detached SpO2 probe with valid HR) must be scored."""
    partial = _base_reading(spo2=None)
    valid, reason, missing = validate_reading(partial)
    assert valid
    assert reason is None
    assert missing == 1

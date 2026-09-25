"""Physiological impossibility validation and DLQ routing.

Enforces bounds drawn from human physiological survivability.
Rejects equipment failures (e.g. detached sensor, lead disconnect) to the DLQ,
while preserving severe clinical abnormalities (e.g. critical hypoxia).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Final

from ward.contracts.models import VitalsReading

# Reason codes matching ward/producers/physiology/defects.py
REASON_SPO2_DETACHED: Final[str] = "SPO2_PROBE_DETACHED"
REASON_HR_OUT_OF_RANGE: Final[str] = "HR_OUT_OF_RANGE"
REASON_TEMP_OUT_OF_RANGE: Final[str] = "TEMP_OUT_OF_RANGE"
REASON_BP_INVERTED: Final[str] = "BP_INVERTED"
REASON_BP_OUT_OF_RANGE: Final[str] = "BP_OUT_OF_RANGE"
REASON_RR_OUT_OF_RANGE: Final[str] = "RR_OUT_OF_RANGE"
REASON_EMPTY_READING: Final[str] = "EMPTY_READING"
REASON_FUTURE_TIMESTAMP: Final[str] = "FUTURE_TIMESTAMP"


def validate_reading(
    reading: VitalsReading,
    sim_now: datetime | None = None,
    future_tolerance_minutes: int = 5,
) -> tuple[bool, str | None, int]:
    """Validate a single vital reading against physiological impossibility bounds.

    Returns:
        (is_valid, rejection_reason, parameters_missing)
    """
    # 1. Check for empty reading (all primary vitals null)
    vitals_present = [
        reading.heart_rate is not None,
        reading.spo2 is not None,
        reading.respiratory_rate is not None,
        reading.systolic_bp is not None,
        reading.temperature is not None,
        reading.consciousness is not None,
    ]
    if not any(vitals_present):
        return False, REASON_EMPTY_READING, 6

    parameters_missing = vitals_present.count(False)

    # 2. Check for future timestamp
    if sim_now is not None:
        cutoff = sim_now + timedelta(minutes=future_tolerance_minutes)
        # Ensure tz-aware comparison
        r_time = reading.measured_at
        if r_time.tzinfo is None and sim_now.tzinfo is not None:
            r_time = r_time.replace(tzinfo=sim_now.tzinfo)
        elif r_time.tzinfo is not None and sim_now.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=r_time.tzinfo)
        if r_time > cutoff:
            return False, REASON_FUTURE_TIMESTAMP, parameters_missing

    # 3. SpO2 plausible range: 50% to 100%
    if reading.spo2 is not None and (reading.spo2 <= 0 or reading.spo2 < 50 or reading.spo2 > 100):
        return False, REASON_SPO2_DETACHED, parameters_missing

    # 4. Heart rate: 20 to 250 bpm
    if reading.heart_rate is not None and (reading.heart_rate < 20 or reading.heart_rate > 250):
        return False, REASON_HR_OUT_OF_RANGE, parameters_missing

    # 5. Temperature: 25.0 to 45.0 C
    if reading.temperature is not None and (
        reading.temperature < 25.0 or reading.temperature > 45.0
    ):
        return False, REASON_TEMP_OUT_OF_RANGE, parameters_missing

    # 6. Blood pressure: systolic > diastolic, systolic between 50 and 260
    if reading.systolic_bp is not None and reading.diastolic_bp is not None:
        if reading.systolic_bp <= reading.diastolic_bp:
            return False, REASON_BP_INVERTED, parameters_missing
        if reading.systolic_bp < 50 or reading.systolic_bp > 260:
            return False, REASON_BP_OUT_OF_RANGE, parameters_missing
    elif reading.systolic_bp is not None and (
        reading.systolic_bp < 50 or reading.systolic_bp > 260
    ):
        return False, REASON_BP_OUT_OF_RANGE, parameters_missing

    # 7. Respiratory rate: 4 to 60 breaths/min
    if reading.respiratory_rate is not None and (
        reading.respiratory_rate < 4 or reading.respiratory_rate > 60
    ):
        return False, REASON_RR_OUT_OF_RANGE, parameters_missing

    return True, None, parameters_missing

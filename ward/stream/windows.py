"""Windowing and trend regression calculation.

Calculates 4-simulated-hour sliding window aggregates and vital sign slopes.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from ward.contracts.models import VitalsReading


@dataclass(frozen=True, slots=True)
class WindowedVitalsTrend:
    """Sliding window summary and regression slopes for one patient."""

    patient_id: str
    window_start: datetime
    window_end: datetime
    readings_in_window: int
    confidence: str  # "high" | "low"

    # Latest parameters in window
    hr_latest: int | None
    spo2_latest: int | None
    sbp_latest: int | None
    dbp_latest: int | None
    temp_latest: Decimal | None
    rr_latest: int | None
    consciousness_latest: str | None
    on_o2_latest: bool

    # Regression slopes (rate of change per observation or per hour)
    hr_slope: Decimal | None
    spo2_slope: Decimal | None
    sbp_slope: Decimal | None
    temp_slope: Decimal | None


def calculate_slope(values: Sequence[float | None]) -> Decimal | None:
    """Compute simple linear regression slope over sequential observations.

    Returns None if fewer than 2 valid points exist.
    """
    valid_points: list[tuple[int, float]] = [
        (idx, val) for idx, val in enumerate(values) if val is not None
    ]
    n = len(valid_points)
    if n < 2:
        return None

    sum_x = sum(x for x, _ in valid_points)
    sum_y = sum(y for _, y in valid_points)
    sum_xx = sum(x * x for x, _ in valid_points)
    sum_xy = sum(x * y for x, y in valid_points)

    denominator = (n * sum_xx) - (sum_x * sum_x)
    if denominator == 0:
        return Decimal("0.00")

    numerator = (n * sum_xy) - (sum_x * sum_y)
    slope = numerator / denominator
    return Decimal(f"{slope:.3f}")


def aggregate_window_trends(
    patient_id: str,
    readings: Sequence[VitalsReading],
    window_start: datetime,
    window_end: datetime,
    min_readings_for_trend: int = 4,
) -> WindowedVitalsTrend:
    """Compute windowed vitals aggregates and slopes from readings in window."""
    sorted_readings = sorted(readings, key=lambda r: r.measured_at)
    count = len(sorted_readings)
    confidence = "high" if count >= min_readings_for_trend else "low"

    if not sorted_readings:
        return WindowedVitalsTrend(
            patient_id=patient_id,
            window_start=window_start,
            window_end=window_end,
            readings_in_window=0,
            confidence="low",
            hr_latest=None,
            spo2_latest=None,
            sbp_latest=None,
            dbp_latest=None,
            temp_latest=None,
            rr_latest=None,
            consciousness_latest=None,
            on_o2_latest=False,
            hr_slope=None,
            spo2_slope=None,
            sbp_slope=None,
            temp_slope=None,
        )

    last_r = sorted_readings[-1]

    # Calculate slopes
    hr_vals = [float(r.heart_rate) if r.heart_rate is not None else None for r in sorted_readings]
    spo2_vals = [float(r.spo2) if r.spo2 is not None else None for r in sorted_readings]
    sbp_vals = [
        float(r.systolic_bp) if r.systolic_bp is not None else None for r in sorted_readings
    ]
    temp_vals = [
        float(r.temperature) if r.temperature is not None else None for r in sorted_readings
    ]

    hr_slope = calculate_slope(hr_vals)
    spo2_slope = calculate_slope(spo2_vals)
    sbp_slope = calculate_slope(sbp_vals)
    temp_slope = calculate_slope(temp_vals)

    temp_latest_dec = Decimal(str(last_r.temperature)) if last_r.temperature is not None else None

    return WindowedVitalsTrend(
        patient_id=patient_id,
        window_start=window_start,
        window_end=window_end,
        readings_in_window=count,
        confidence=confidence,
        hr_latest=last_r.heart_rate,
        spo2_latest=last_r.spo2,
        sbp_latest=last_r.systolic_bp,
        dbp_latest=last_r.diastolic_bp,
        temp_latest=temp_latest_dec,
        rr_latest=last_r.respiratory_rate,
        consciousness_latest=last_r.consciousness,
        on_o2_latest=last_r.on_supplemental_oxygen,
        hr_slope=hr_slope,
        spo2_slope=spo2_slope,
        sbp_slope=sbp_slope,
        temp_slope=temp_slope,
    )

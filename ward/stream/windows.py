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
    """Theil-Sen slope per observation: the median of all pairwise slopes.

    Least squares was used first and P007 showed why it is wrong here. One heart rate
    of 145 at the end of a flat 4-hour window drags a least-squares line up steeply
    enough to count as a "rising trend" - the single-reading artefact that the trend
    detector exists to ignore. The median of pairwise slopes barely moves for one
    outlier, while a real climb (P014) moves every pair and so moves the median.

    Returns None if fewer than 2 valid points exist.
    """
    valid_points: list[tuple[int, float]] = [
        (idx, val) for idx, val in enumerate(values) if val is not None
    ]
    if len(valid_points) < 2:
        return None

    slopes = sorted(
        (y2 - y1) / (x2 - x1)
        for i, (x1, y1) in enumerate(valid_points)
        for x2, y2 in valid_points[i + 1 :]
    )
    mid = len(slopes) // 2
    slope = slopes[mid] if len(slopes) % 2 else (slopes[mid - 1] + slopes[mid]) / 2
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

"""End-to-end transform pipeline unit test on scripted patient P014 (septic shock).

Validates the complete transform chain in pure Python:
1. Physiological validation (clean.py)
2. Admissions enrichment (enrich.py)
3. 4-hour sliding window trend detection (windows.py)
4. NEWS2 scoring (ward/clinical/news2.py)
5. Compacted lab pathology join (lab_join.py & composite_risk.py)
6. Multi-condition clinical alert evaluation (alerts.py)
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from random import Random

from ward.clinical.composite_risk import evaluate_composite_risk
from ward.clinical.news2 import score_news2
from ward.contracts.models import Admission, LabResult, VitalsReading
from ward.producers.physiology.baselines import draw_baseline
from ward.producers.physiology.narratives import (
    SEPSIS_ONSET_DAY,
    SEPSIS_ONSET_HOUR,
    SEPSIS_PATIENT,
    NarrativeContext,
    apply,
)
from ward.producers.physiology.walk import VitalsState, step
from ward.stream.alerts import (
    ALERT_LAB_CORROBORATED_RISK,
    ALERT_NEWS2_HIGH,
    ALERT_RAPID_DETERIORATION,
    evaluate_clinical_alerts,
)
from ward.stream.clean import validate_reading
from ward.stream.enrich import enrich_reading
from ward.stream.windows import aggregate_window_trends

EPOCH = datetime(2026, 4, 1, tzinfo=UTC)
SEED = 42


def test_p014_end_to_end_deterioration_pipeline() -> None:
    """Run patient P014 through the entire Kappa stream transformation pipeline."""
    rng = Random(SEED)
    patient_id = SEPSIS_PATIENT
    onset = EPOCH + timedelta(days=SEPSIS_ONSET_DAY - 1, hours=SEPSIS_ONSET_HOUR)

    admissions_map = {
        patient_id: Admission(
            patient_id=patient_id,
            bed_id="BED-14",
            admitted_at=EPOCH,
            age=68,
            sex="M",
            primary_condition="sepsis_risk",
            copd_scale2=False,
        )
    }

    # Sepsis labs from pathology: elevated lactate (+2) and leukocytosis (+1)
    lab_results = [
        LabResult(
            patient_id=patient_id,
            test_type="lactate",
            result_value=3.8,  # > 2.0 and close to 4.0; +1 or +2
            unit="mmol/L",
            reference_low=0.5,
            reference_high=2.2,
            collected_at=onset - timedelta(hours=6),
            reported_at=onset - timedelta(hours=4),
        ),
        LabResult(
            patient_id=patient_id,
            test_type="wbc",
            result_value=18.4,  # elevated white blood cells
            unit="10^9/L",
            reference_low=4.0,
            reference_high=11.0,
            collected_at=onset - timedelta(hours=6),
            reported_at=onset - timedelta(hours=4),
        ),
    ]

    baseline = draw_baseline(age=68, condition="sepsis_risk", rng=rng)
    state = VitalsState.at_baseline(baseline)

    history: list[VitalsReading] = []
    history_scores: list[int] = []
    all_alerts: list[str] = []
    max_news2 = 0
    max_composite = 0
    final_tier = "LOW"

    # Simulate 5 hours of 15-minute readings across sepsis deterioration
    steps_count = int(5 * 60 / 15)  # 20 readings
    for i in range(steps_count):
        current_time = onset + timedelta(minutes=i * 15)
        state = step(state, baseline, current_time, rng)
        state = apply(state, NarrativeContext(patient_id, current_time, EPOCH))

        reading = VitalsReading(
            reading_id=str(uuid.uuid4()),
            patient_id=patient_id,
            bed_id="BED-14",
            device_id="DEV-BED-14",
            measured_at=current_time,
            ingest_time=current_time + timedelta(seconds=1),
            heart_rate=round(state.heart_rate),
            spo2=round(state.spo2),
            respiratory_rate=round(state.respiratory_rate),
            systolic_bp=round(state.systolic_bp),
            diastolic_bp=round(state.diastolic_bp),
            temperature=round(state.temperature, 1),
            consciousness="A",
            on_supplemental_oxygen=False,
            producer_id="bedside-monitor",
        )

        # 1. Clean & validate against physiological bounds
        is_valid, reject_reason, _ = validate_reading(reading, sim_now=current_time)
        assert is_valid, f"Reading unexpectedly rejected: {reject_reason}"

        # 2. Enrich with admissions reference data
        enriched, has_admission = enrich_reading(reading, admissions_map)
        assert has_admission
        assert not enriched.copd_scale2

        # 3. Maintain 4-hour window
        history.append(reading)
        window_duration_seconds = 4 * 3600
        cutoff = reading.measured_at.timestamp() - window_duration_seconds
        history = [r for r in history if r.measured_at.timestamp() >= cutoff]

        trend = aggregate_window_trends(
            patient_id=patient_id,
            readings=history,
            window_start=datetime.fromtimestamp(cutoff, tz=UTC),
            window_end=reading.measured_at,
            min_readings_for_trend=4,
        )

        # 4. Score NEWS2
        news2_res = score_news2(
            respiratory_rate=reading.respiratory_rate,
            spo2=reading.spo2,
            on_supplemental_oxygen=reading.on_supplemental_oxygen,
            systolic_bp=reading.systolic_bp,
            heart_rate=reading.heart_rate,
            consciousness=reading.consciousness,
            temperature=reading.temperature,
            copd_scale2=enriched.copd_scale2,
            version="v2",
        )
        if news2_res.total > max_news2:
            max_news2 = news2_res.total

        # 5. Join labs and evaluate composite risk
        composite_res = evaluate_composite_risk(
            patient_id=patient_id,
            news2_total=news2_res.total,
            labs=lab_results,
            as_of=reading.measured_at,
        )
        if composite_res.composite_risk > max_composite:
            max_composite = composite_res.composite_risk
            final_tier = composite_res.risk_tier

        # 6. Evaluate clinical alerts
        # prior_score_in_window represents score at the start of the 4-hour window
        window_start_score = history_scores[0] if history_scores else news2_res.total
        alerts = evaluate_clinical_alerts(
            patient_id=patient_id,
            bed_id=reading.bed_id,
            ward_id=enriched.ward_id,
            sim_date=reading.measured_at.date(),
            alert_time=reading.measured_at,
            window_trend=trend,
            news2=news2_res,
            composite=composite_res,
            prior_score_in_window=window_start_score,
            copd_scale2=enriched.copd_scale2,
        )
        for a in alerts:
            all_alerts.append(a.alert_type)

        history_scores.append(news2_res.total)
        if len(history_scores) > len(history):
            history_scores = history_scores[-len(history) :]

    # Verify clinical trajectory milestones
    assert max_news2 >= 7, f"NEWS2 peak was {max_news2}, expected >= 7 for severe sepsis"
    assert max_composite >= 9, f"Composite risk peak was {max_composite}, expected >= 9"
    assert final_tier in ("HIGH", "CRITICAL")
    assert ALERT_NEWS2_HIGH in all_alerts
    assert ALERT_LAB_CORROBORATED_RISK in all_alerts
    assert ALERT_RAPID_DETERIORATION in all_alerts

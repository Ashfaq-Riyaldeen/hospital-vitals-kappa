"""Cassandra foreachBatch sink.

Concurrently writes micro-batch outputs into the 5 Cassandra tables:
- vitals_by_patient
- risk_scores_by_patient (keyed by scorer_version)
- ward_risk_snapshot (TTL = 120s, pre-sorted worst-first)
- alerts_by_ward
- daily_patient_summary
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from ward.obs import metrics
from ward.store.dao import (
    AlertRow,
    DailyPatientSummaryRow,
    RiskScoreRow,
    VitalReadingRow,
    WardRiskSnapshotRow,
    WardStoreDAO,
)

logger = logging.getLogger(__name__)


def write_batch_data(
    dao: WardStoreDAO,
    vitals: Sequence[VitalReadingRow],
    risk_scores: Sequence[RiskScoreRow],
    snapshots: Sequence[WardRiskSnapshotRow],
    alerts: Sequence[AlertRow],
    summaries: Sequence[DailyPatientSummaryRow],
) -> None:
    """Write collected domain rows into Cassandra tables."""
    # 1. Vitals
    for v in vitals:
        try:
            dao.insert_vital_reading(v)
            metrics.sink_writes_total.labels(table="vitals_by_patient").inc()
        except Exception as exc:
            logger.error("Failed to insert vital reading: %s", exc)
            metrics.sink_write_errors_total.labels(table="vitals_by_patient").inc()

    # 2. Risk scores
    for s in risk_scores:
        try:
            dao.insert_patient_risk(s)
            metrics.sink_writes_total.labels(table="risk_scores_by_patient").inc()
        except Exception as exc:
            logger.error("Failed to insert risk score: %s", exc)
            metrics.sink_write_errors_total.labels(table="risk_scores_by_patient").inc()

    # 3. Ward snapshot
    for snap in snapshots:
        try:
            dao.insert_ward_risk_snapshot(snap)
            metrics.sink_writes_total.labels(table="ward_risk_snapshot").inc()
        except Exception as exc:
            logger.error("Failed to insert ward snapshot: %s", exc)
            metrics.sink_write_errors_total.labels(table="ward_risk_snapshot").inc()

    # 4. Alerts
    for a in alerts:
        try:
            dao.insert_alert(a)
            metrics.sink_writes_total.labels(table="alerts_by_ward").inc()
        except Exception as exc:
            logger.error("Failed to insert alert: %s", exc)
            metrics.sink_write_errors_total.labels(table="alerts_by_ward").inc()

    # 5. Summaries
    for sum_row in summaries:
        try:
            dao.insert_daily_patient_summary(sum_row)
            metrics.sink_writes_total.labels(table="daily_patient_summary").inc()
        except Exception as exc:
            logger.error("Failed to insert daily summary: %s", exc)
            metrics.sink_write_errors_total.labels(table="daily_patient_summary").inc()

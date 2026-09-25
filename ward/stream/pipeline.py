"""The Single Stream Processing Pipeline.

Runs a long-lived PySpark Structured Streaming query or driver loop.
Consumes bedside vitals, cleans physiological impossibilities,
enriches with admissions reference data, computes 4-hour sliding trends,
evaluates NEWS2 clinical scores, joins latest lab results for composite risk,
generates deduplicated clinical alerts, and sinks to Cassandra.

Under Kappa, this is the ONLY processing pipeline in the platform.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from datetime import UTC, datetime
from decimal import Decimal
from types import FrameType
from typing import Any
from uuid import UUID

from confluent_kafka import Consumer, KafkaError

from ward import settings
from ward.clinical.composite_risk import evaluate_composite_risk
from ward.clinical.news2 import score_news2
from ward.contracts.models import Admission, LabResult, VitalsReading
from ward.contracts.serialization import deserialize, serialize_key, serialize_value
from ward.obs import metrics
from ward.obs.kafka_client import build_producer
from ward.obs.log import configure, get_logger
from ward.store.dao import (
    AlertRow,
    DailyPatientSummaryRow,
    RiskScoreRow,
    VitalReadingRow,
    WardRiskSnapshotRow,
    WardStoreDAO,
)
from ward.store.session import create_cluster, get_session
from ward.stream.alerts import evaluate_clinical_alerts
from ward.stream.clean import validate_reading
from ward.stream.enrich import enrich_reading
from ward.stream.sinks import write_batch_data
from ward.stream.windows import aggregate_window_trends

log = get_logger()


class StreamPipelineRunner:
    """Coordinates stream consumption, transformation, and Cassandra persistence."""

    def __init__(
        self,
        scorer_version: str = "v1",
        consumer_group: str = "ward-stream-v1",
    ) -> None:
        self.scorer_version = scorer_version
        self.consumer_group = consumer_group
        self.running = True

        self.k_cfg = settings.kafka()
        self.store_cfg = settings.storage()
        self.sim_cfg = settings.sim()
        self.proc_cfg = settings.processing()

        self._cluster = create_cluster()
        self._session = get_session(keyspace=self.store_cfg.keyspace, cluster=self._cluster)
        self.dao = WardStoreDAO(self._session)

        self.producer = build_producer(f"stream-{scorer_version}")

        self.admissions_cache: dict[str, Admission] = {}
        self.labs_cache: dict[str, dict[str, LabResult]] = {}
        self.window_history: dict[str, list[VitalsReading]] = {}
        self.prior_scores: dict[str, int] = {}

    def stop(self) -> None:
        self.running = False
        log.info("stream_pipeline_stopping", scorer_version=self.scorer_version)
        try:
            self.producer.flush(timeout=5)
            self._session.shutdown()
            self._cluster.shutdown()
        except Exception as exc:
            log.warning("error_during_shutdown", error=str(exc))

    def load_reference_data(self) -> None:
        """Load compacted admissions and lab reference topics into in-memory caches."""
        conf = {
            "bootstrap.servers": self.k_cfg.bootstrap,
            "group.id": f"ref-loader-{self.scorer_version}-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        consumer = Consumer(conf)

        try:
            # 1. Admissions
            consumer.subscribe([self.k_cfg.admissions_topic])
            start_t = time.monotonic()
            while time.monotonic() - start_t < 2.0:
                msg = consumer.poll(0.2)
                if msg is None:
                    continue
                if msg.error():
                    break
                val_bytes = msg.value()
                if val_bytes:
                    try:
                        payload = deserialize(
                            val_bytes,
                            self.k_cfg.admissions_topic,
                            url=self.k_cfg.schema_registry_url,
                            schema_file="admission.avsc",
                        )
                        adm = Admission(**payload)
                        self.admissions_cache[adm.patient_id] = adm
                    except Exception as exc:
                        log.debug("failed_to_decode_admission", error=str(exc))

            # 2. Labs
            consumer.subscribe([self.k_cfg.labs_topic])
            start_t = time.monotonic()
            while time.monotonic() - start_t < 2.0:
                msg = consumer.poll(0.2)
                if msg is None:
                    continue
                if msg.error():
                    break
                val_bytes = msg.value()
                if val_bytes:
                    try:
                        payload = deserialize(
                            val_bytes,
                            self.k_cfg.labs_topic,
                            url=self.k_cfg.schema_registry_url,
                            schema_file="lab_result.avsc",
                        )
                        lab = LabResult(**payload)
                        p_labs = self.labs_cache.setdefault(lab.patient_id, {})
                        p_labs[lab.test_type] = lab
                    except Exception as exc:
                        log.debug("failed_to_decode_lab", error=str(exc))
        except Exception as exc:
            log.warning("reference_cache_load_warning", error=str(exc))
        finally:
            consumer.close()

        log.info(
            "reference_data_cached",
            admissions_count=len(self.admissions_cache),
            patients_with_labs=len(self.labs_cache),
        )

    def process_reading(self, reading: VitalsReading, sim_now: datetime) -> None:
        """Core streaming transformation on a single vital reading."""
        # 1. Clean & validate against physiological bounds
        is_valid, reject_reason, missing_count = validate_reading(reading, sim_now=sim_now)
        if not is_valid:
            metrics.readings_validated_total.labels(result="rejected").inc()
            metrics.readings_dlq_total.labels(
                reason=reject_reason or "UNKNOWN", device_id=reading.device_id
            ).inc()
            # Forward invalid reading to DLQ topic
            dlq_val = {
                "original_payload": b"",
                "rejection_reason": reject_reason or "UNKNOWN",
                "rejection_detail": f"Physiological validation failed: {reject_reason}",
                "validator": "ward.stream.clean",
                "patient_id": reading.patient_id,
                "device_id": reading.device_id,
                "rejected_at_sim": int(reading.measured_at.timestamp() * 1000),
                "rejected_at_real": int(datetime.now(UTC).timestamp() * 1000),
                "source_topic": self.k_cfg.vitals_topic,
                "source_partition": 0,
                "source_offset": 0,
                "trace_id": None,
            }
            try:
                self.producer.produce(
                    topic=self.k_cfg.dlq_topic,
                    key=serialize_key(reading.patient_id, self.k_cfg.dlq_topic),
                    value=serialize_value(
                        dlq_val,
                        self.k_cfg.dlq_topic,
                        self.k_cfg.schema_registry_url,
                        "dlq_envelope.avsc",
                    ),
                )
                self.producer.poll(0)
            except Exception as e:
                log.warning("failed_to_produce_dlq", error=str(e))
            return

        metrics.readings_validated_total.labels(result="valid").inc()

        # 2. Enrich with admissions reference context
        enriched, has_admission = enrich_reading(reading, self.admissions_cache)
        if not has_admission:
            metrics.readings_without_admission_total.inc()

        # 3. Maintain sliding window history (4 simulated hours)
        history = self.window_history.setdefault(reading.patient_id, [])
        history.append(reading)

        # Prune readings older than 4 simulated hours
        window_duration_seconds = self.proc_cfg.trend_window_sim_hours * 3600
        cutoff = reading.measured_at.timestamp() - window_duration_seconds
        self.window_history[reading.patient_id] = [
            r for r in history if r.measured_at.timestamp() >= cutoff
        ]
        history = self.window_history[reading.patient_id]

        window_start = datetime.fromtimestamp(cutoff, tz=UTC)
        window_end = reading.measured_at

        # 4. Compute window aggregates and slopes
        trend = aggregate_window_trends(
            patient_id=reading.patient_id,
            readings=history,
            window_start=window_start,
            window_end=window_end,
            min_readings_for_trend=self.proc_cfg.min_readings_for_trend,
        )

        # 5. Score with NEWS2 (the ONE clinical rule)
        news2_res = score_news2(
            respiratory_rate=reading.respiratory_rate,
            spo2=reading.spo2,
            on_supplemental_oxygen=reading.on_supplemental_oxygen,
            systolic_bp=reading.systolic_bp,
            heart_rate=reading.heart_rate,
            consciousness=reading.consciousness,
            temperature=reading.temperature,
            copd_scale2=enriched.copd_scale2,
            version=self.scorer_version,
        )

        # 6. Join latest labs and evaluate composite risk
        patient_labs = self.labs_cache.get(reading.patient_id, {})
        composite_res = evaluate_composite_risk(
            patient_id=reading.patient_id,
            news2_total=news2_res.total,
            labs=patient_labs,
            as_of=reading.measured_at,
        )

        # 7. Evaluate clinical alerts
        prior_score = self.prior_scores.get(reading.patient_id)
        self.prior_scores[reading.patient_id] = news2_res.total

        alerts = evaluate_clinical_alerts(
            patient_id=reading.patient_id,
            bed_id=reading.bed_id,
            ward_id=enriched.ward_id,
            sim_date=reading.measured_at.date(),
            alert_time=reading.measured_at,
            window_trend=trend,
            news2=news2_res,
            composite=composite_res,
            prior_score_in_window=prior_score,
            copd_scale2=enriched.copd_scale2,
        )

        for alert in alerts:
            metrics.clinical_alerts_emitted_total.labels(
                type=alert.alert_type, severity=alert.severity
            ).inc()
            alert_dict = {
                "alert_id": alert.alert_id,
                "patient_id": alert.patient_id,
                "bed_id": alert.bed_id,
                "ward_id": alert.ward_id,
                "sim_date": alert.sim_date.isoformat(),
                "alert_time": alert.alert_time.isoformat(),
                "alert_type": alert.alert_type,
                "severity": alert.severity,
                "news2_total": alert.news2_total,
                "composite_risk": alert.composite_risk,
                "detail": alert.detail,
                "acknowledged": alert.acknowledged,
            }
            try:
                self.producer.produce(
                    topic=self.k_cfg.alerts_topic,
                    key=alert.patient_id.encode("utf-8"),
                    value=json.dumps(alert_dict).encode("utf-8"),
                )
                self.producer.poll(0)
            except Exception as e:
                log.warning("failed_to_produce_alert", error=str(e))

        # 8. Sink to Cassandra
        vital_row = VitalReadingRow(
            patient_id=reading.patient_id,
            measured_at=reading.measured_at,
            reading_id=UUID(reading.reading_id),
            bed_id=reading.bed_id,
            heart_rate=reading.heart_rate,
            spo2=reading.spo2,
            systolic_bp=reading.systolic_bp,
            diastolic_bp=reading.diastolic_bp,
            temperature=(
                Decimal(str(reading.temperature)) if reading.temperature is not None else None
            ),
            respiratory_rate=reading.respiratory_rate,
            consciousness=reading.consciousness,
            on_supplemental_oxygen=reading.on_supplemental_oxygen,
            parameters_missing=missing_count,
            ingest_time=reading.ingest_time,
        )

        score_row = RiskScoreRow(
            patient_id=reading.patient_id,
            scorer_version=self.scorer_version,
            scored_at=reading.measured_at,
            window_start=trend.window_start,
            window_end=trend.window_end,
            news2_total=news2_res.total,
            news2_subscores=news2_res.subscores,
            any_parameter_is_3=news2_res.any_parameter_is_3,
            clinical_risk=news2_res.clinical_risk,
            lab_contribution=composite_res.lab_contribution,
            composite_risk=composite_res.composite_risk,
            risk_tier=composite_res.risk_tier,
            labs_stale=composite_res.labs_stale,
            confidence=trend.confidence,
            hr_slope=trend.hr_slope,
            spo2_slope=trend.spo2_slope,
            sbp_slope=trend.sbp_slope,
            temp_slope=trend.temp_slope,
            readings_in_window=trend.readings_in_window,
        )

        snap_row = WardRiskSnapshotRow(
            ward_id=enriched.ward_id,
            scorer_version=self.scorer_version,
            risk_score=composite_res.composite_risk,
            patient_id=reading.patient_id,
            bed_id=reading.bed_id,
            risk_tier=composite_res.risk_tier,
            news2_total=news2_res.total,
            lab_contribution=composite_res.lab_contribution,
            labs_stale=composite_res.labs_stale,
            scored_at=reading.measured_at,
            hr_latest=trend.hr_latest,
            spo2_latest=trend.spo2_latest,
            sbp_latest=trend.sbp_latest,
            temp_latest=trend.temp_latest,
        )

        alert_rows = [
            AlertRow(
                ward_id=a.ward_id,
                sim_date=a.sim_date,
                alert_time=a.alert_time,
                alert_id=a.alert_id,
                patient_id=a.patient_id,
                bed_id=a.bed_id,
                alert_type=a.alert_type,
                severity=a.severity,
                news2_total=a.news2_total,
                composite_risk=a.composite_risk,
                detail=a.detail,
                acknowledged=a.acknowledged,
            )
            for a in alerts
        ]

        summary_row = DailyPatientSummaryRow(
            ward_id=enriched.ward_id,
            sim_date=reading.measured_at.date(),
            patient_id=reading.patient_id,
            scorer_version=self.scorer_version,
            bed_id=reading.bed_id,
            admitting_condition=enriched.admitting_condition,
            max_news2=news2_res.total,
            mean_news2=Decimal(str(news2_res.total)),
            max_composite_risk=composite_res.composite_risk,
            final_risk_tier=composite_res.risk_tier,
            lab_contribution=composite_res.lab_contribution,
            labs_stale=composite_res.labs_stale,
            alert_count=len(alerts),
            highest_severity=alerts[0].severity if alerts else "NONE",
            readings_count=1,
            readings_rejected=0,
            deterioration_detected=(news2_res.total >= 7 or len(alerts) > 0),
        )

        write_batch_data(
            dao=self.dao,
            vitals=[vital_row],
            risk_scores=[score_row],
            snapshots=[snap_row],
            alerts=alert_rows,
            summaries=[summary_row],
        )

    def run_consumer(self) -> None:
        """Direct Kafka consumption loop (used when running standalone or locally)."""
        self.load_reference_data()

        conf = {
            "bootstrap.servers": self.k_cfg.bootstrap,
            "group.id": self.consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
        consumer = Consumer(conf)
        consumer.subscribe([self.k_cfg.vitals_topic])

        log.info("consumer_loop_started", topic=self.k_cfg.vitals_topic)

        try:
            while self.running:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log.error("consumer_error", error=str(msg.error()))
                    continue

                val_bytes = msg.value()
                if not val_bytes:
                    continue

                try:
                    payload = deserialize(
                        val_bytes,
                        self.k_cfg.vitals_topic,
                        url=self.k_cfg.schema_registry_url,
                        schema_file="vitals_reading.avsc",
                    )
                    reading = VitalsReading(**payload)
                    self.process_reading(reading, sim_now=reading.measured_at)
                except Exception as exc:
                    log.error("reading_processing_error", error=str(exc))
        finally:
            consumer.close()

    def run_spark(self) -> None:
        """PySpark Structured Streaming query runner (used in the Spark app container)."""
        from pyspark.sql import SparkSession

        self.load_reference_data()

        spark = (
            SparkSession.builder.appName(f"ward-stream-{self.scorer_version}")
            .master("local[4]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.port", "4040")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.k_cfg.bootstrap)
            .option("subscribe", self.k_cfg.vitals_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        def write_micro_batch(batch_df: Any, batch_id: int) -> None:
            rows = batch_df.collect()
            if not rows:
                return
            for r in rows:
                val = r.value
                if not val:
                    continue
                try:
                    payload = deserialize(
                        val,
                        self.k_cfg.vitals_topic,
                        url=self.k_cfg.schema_registry_url,
                        schema_file="vitals_reading.avsc",
                    )
                    reading = VitalsReading(**payload)
                    self.process_reading(reading, sim_now=reading.measured_at)
                except Exception as e:
                    log.error("spark_batch_record_failed", error=str(e), batch_id=batch_id)

        query = (
            df.writeStream.foreachBatch(write_micro_batch)
            .trigger(processingTime="5 seconds")
            .option("checkpointLocation", f"/checkpoints/ward-stream-{self.scorer_version}")
            .start()
        )

        log.info("spark_structured_streaming_query_started", scorer_version=self.scorer_version)
        query.awaitTermination()


def main() -> int:
    parser = argparse.ArgumentParser(description="Ward Stream Processing Pipeline")
    parser.add_argument(
        "--scorer-version",
        default=os.environ.get("SCORER_VERSION", "v1"),
        help="Clinical scorer version (v1 or v2)",
    )
    parser.add_argument(
        "--consumer-group",
        default=os.environ.get("CONSUMER_GROUP", "ward-stream-v1"),
        help="Kafka consumer group ID",
    )
    parser.add_argument(
        "--engine",
        default=os.environ.get("STREAM_ENGINE", "auto"),
        choices=["auto", "spark", "consumer"],
        help="Processing engine (spark, consumer, or auto)",
    )
    args = parser.parse_args()

    obs = settings.observability()
    configure(
        service=f"ward-stream-{args.scorer_version}",
        stage="stream",
        level=obs.log_level,
        json_output=obs.log_json,
    )

    metrics.serve(port=obs.metrics_port)
    log.info(
        "stream_pipeline_started",
        scorer_version=args.scorer_version,
        consumer_group=args.consumer_group,
        engine=args.engine,
    )

    runner = StreamPipelineRunner(
        scorer_version=args.scorer_version,
        consumer_group=args.consumer_group,
    )

    def _sig_handler(sig: int, frame: FrameType | None) -> None:
        runner.stop()

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    has_spark = False
    if args.engine in ("auto", "spark"):
        try:
            import pyspark  # noqa: F401

            has_spark = True
        except ImportError:
            has_spark = False

    if args.engine == "spark" or (args.engine == "auto" and has_spark):
        runner.run_spark()
    else:
        runner.run_consumer()

    return 0


if __name__ == "__main__":
    sys.exit(main())

"""The single stream processing pipeline.

Reads bedside vitals from Kafka with PySpark Structured Streaming and, for every
reading, decides (in `processor.py`) whether it is a duplicate, impossible, late or
real; scores it with NEWS2; joins the latest lab results; raises new alerts; and
writes everything to Cassandra.

Under Kappa this is the ONLY processing pipeline. The replay runs this same file with
SCORER_VERSION=v2 and STARTING_OFFSETS=earliest - nothing else changes.

I/O lives here; decisions live in `processor.py`, so they can be tested without Docker.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import FrameType
from typing import Any
from uuid import UUID

from confluent_kafka import Consumer, KafkaError, TopicPartition

from ward import settings
from ward.contracts.models import Admission, LabResult, VitalsReading
from ward.contracts.serialization import deserialize, serialize_key, serialize_value
from ward.obs import metrics
from ward.obs.kafka_client import build_producer
from ward.obs.log import configure, get_logger
from ward.simclock import SimClock, read_anchor
from ward.store.dao import (
    AlertRow,
    DailyPatientSummaryRow,
    LabResultRow,
    RiskScoreRow,
    VitalReadingRow,
    WardRiskSnapshotRow,
    WardStoreDAO,
)
from ward.store.session import create_cluster, get_session
from ward.stream.processor import (
    KIND_DUPLICATE,
    KIND_LATE,
    KIND_REJECTED,
    Outcome,
    StreamProcessor,
    sent_at_sim,
)
from ward.stream.sinks import write_batch_data

log = get_logger()


class ReferenceData:
    """Keeps the admissions and lab caches current from their compacted topics.

    The first version read both topics once, for two seconds, at start-up. Lab results
    arrive once per simulated day, AFTER the stream has started, so the lab join never
    saw a single result and the composite risk was always NEWS2 alone. Here one
    consumer reads both topics to the end before scoring starts, then keeps reading on
    a background thread for the life of the stream.
    """

    def __init__(
        self,
        k_cfg: settings.KafkaSettings,
        scorer_version: str,
        on_lab: Callable[[LabResult], None] | None = None,
    ) -> None:
        self.k_cfg = k_cfg
        self._on_lab = on_lab
        self.admissions: dict[str, Admission] = {}
        # Every result per patient and test, in the order reported, so a reading is
        # joined with the labs that existed at ITS time, even during a backlog.
        self.labs: dict[str, dict[str, LabResult | list[LabResult]]] = {}
        self._consumer = Consumer(
            {
                "bootstrap.servers": k_cfg.bootstrap,
                "group.id": f"ward-reference-{scorer_version}-{int(time.time())}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe([k_cfg.admissions_topic, k_cfg.labs_topic])
        self._running = True

    def _apply(self, topic: str | None, value: bytes | None) -> None:
        if not value or topic is None:
            return
        try:
            if topic == self.k_cfg.admissions_topic:
                adm = Admission(**deserialize(value, topic, schema_file="admission.avsc"))
                self.admissions[adm.patient_id] = adm
                metrics.admissions_refresh_total.inc()
            else:
                lab = LabResult(**deserialize(value, topic, schema_file="lab_result.avsc"))
                history = self.labs.setdefault(lab.patient_id, {}).setdefault(
                    lab.test_type.lower(), []
                )
                if isinstance(history, list) and lab not in history:
                    history.append(lab)
                metrics.lab_results_cached_total.inc()
                if self._on_lab is not None:
                    self._on_lab(lab)
        except Exception as exc:
            log.warning("reference_record_undecodable", topic=topic, error=str(exc))

    def load_until_caught_up(self, timeout_seconds: float = 60.0) -> None:
        deadline = time.monotonic() + timeout_seconds
        ends: dict[tuple[str, int], int] = {}
        while time.monotonic() < deadline:
            msg = self._consumer.poll(0.5)
            if msg is not None and not msg.error():
                self._apply(msg.topic(), msg.value())
            assignment = self._consumer.assignment()
            if not assignment:
                continue
            if not ends:
                for tp in assignment:
                    _, high = self._consumer.get_watermark_offsets(tp, timeout=5)
                    ends[(tp.topic, tp.partition)] = high
            positions = self._consumer.position([TopicPartition(t, p) for (t, p) in ends])
            if all(
                tp.offset >= ends[(tp.topic, tp.partition)] or ends[(tp.topic, tp.partition)] == 0
                for tp in positions
            ):
                break
        log.info(
            "reference_data_loaded",
            admissions=len(self.admissions),
            patients_with_labs=len(self.labs),
        )

    def follow(self) -> None:
        def _loop() -> None:
            while self._running:
                msg = self._consumer.poll(1.0)
                if msg is not None and not msg.error():
                    self._apply(msg.topic(), msg.value())
            self._consumer.close()

        threading.Thread(target=_loop, daemon=True, name="reference-data").start()

    def stop(self) -> None:
        self._running = False

    def close(self) -> None:
        """For one-off readers that never call follow()."""
        self._consumer.close()


def _clock() -> SimClock | None:
    path = Path(os.environ.get("SIM_STATE_PATH", "/state/sim_epoch.json"))
    try:
        return read_anchor(path)
    except FileNotFoundError:
        log.warning("no_sim_clock_anchor", path=str(path), detail="future-timestamp check off")
        return None


class StreamPipelineRunner:
    """Kafka in, Cassandra and Kafka out, around one StreamProcessor."""

    def __init__(self, scorer_version: str = "v1", consumer_group: str = "ward-stream-v1") -> None:
        self.scorer_version = scorer_version
        self.consumer_group = consumer_group
        self.running = True

        self.k_cfg = settings.kafka()
        self.store_cfg = settings.storage()
        self.proc_cfg = settings.processing()
        self.clock = _clock()

        self._cluster = create_cluster()
        self._session = get_session(keyspace=self.store_cfg.keyspace, cluster=self._cluster)
        self.dao = WardStoreDAO(self._session)
        self.producer = build_producer(f"stream-{scorer_version}")
        # Dead letters and late readings are facts about the INPUT, not about the
        # scoring rule. A replay re-reads the same log, so it must not write them a
        # second time (the first replay doubled the dead-letter topic).
        self.write_side_outputs = os.environ.get("WRITE_SIDE_OUTPUTS", "true") == "true"

        self.reference = ReferenceData(self.k_cfg, scorer_version, on_lab=self._store_lab)
        self.processor = StreamProcessor(
            scorer_version=scorer_version,
            admissions=self.reference.admissions,
            labs=self.reference.labs,
            window_hours=self.proc_cfg.trend_window_sim_hours,
            min_readings_for_trend=self.proc_cfg.min_readings_for_trend,
            watermark_minutes=self.proc_cfg.watermark_sim_minutes,
            summary_loader=self._stored_summary,
        )

    def stop(self) -> None:
        self.running = False
        self.reference.stop()
        log.info("stream_pipeline_stopping", scorer_version=self.scorer_version)
        try:
            self.producer.flush(timeout=5)
            self._session.shutdown()
            self._cluster.shutdown()
        except Exception as exc:
            log.warning("error_during_shutdown", error=str(exc))

    def start_reference_data(self) -> None:
        self.reference.load_until_caught_up()
        self.reference.follow()

    # ------------------------------------------------------------ per record

    def handle(self, raw: bytes, partition: int, offset: int) -> None:
        """One Kafka record, start to finish."""
        try:
            payload = deserialize(raw, self.k_cfg.vitals_topic, schema_file="vitals_reading.avsc")
            reading = VitalsReading(**payload)
        except Exception as exc:
            metrics.readings_validated_total.labels(result="rejected").inc()
            metrics.readings_dlq_total.labels(reason="UNDECODABLE", device_id="unknown").inc()
            if self.write_side_outputs:
                self._dead_letter(raw, "UNDECODABLE", str(exc), None, None, None, partition, offset)
            return

        out = self.processor.process(reading, clock_now=sent_at_sim(self.clock, reading))

        if out.kind == KIND_DUPLICATE:
            metrics.readings_deduplicated_total.inc()
            return
        if out.kind == KIND_REJECTED:
            reason = out.reason or "UNKNOWN"
            metrics.readings_validated_total.labels(result="rejected").inc()
            metrics.readings_dlq_total.labels(reason=reason, device_id=reading.device_id).inc()
            if self.write_side_outputs:
                self._dead_letter(
                    raw,
                    reason,
                    f"physiologically impossible reading: {reason}",
                    reading.patient_id,
                    reading.device_id,
                    reading.measured_at,
                    partition,
                    offset,
                )
            if out.summary is not None:
                write_batch_data(self.dao, [], [], [], [], [out.summary])
            return
        if out.kind == KIND_LATE:
            metrics.readings_late_total.inc()
            if self.write_side_outputs:
                self._produce(self.k_cfg.late_topic, reading.patient_id, raw)
            write_batch_data(self.dao, [self._vital_row(reading)], [], [], [], [])
            return

        metrics.readings_validated_total.labels(result="valid").inc()
        if not out.has_admission:
            metrics.readings_without_admission_total.inc()
        self._persist_outcome(out)

    def _persist_outcome(self, out: Outcome) -> None:
        reading, news2, composite, trend, enriched = (
            out.reading,
            out.news2,
            out.composite,
            out.trend,
            out.enriched,
        )
        assert news2 is not None and composite is not None
        assert trend is not None and enriched is not None

        for alert in out.alerts:
            metrics.clinical_alerts_emitted_total.labels(
                type=alert.alert_type, severity=alert.severity
            ).inc()
            body = {
                "alert_id": alert.alert_id,
                "patient_id": alert.patient_id,
                "bed_id": alert.bed_id,
                "ward_id": alert.ward_id,
                "scorer_version": self.scorer_version,
                "sim_date": alert.sim_date.isoformat(),
                "alert_time": alert.alert_time.isoformat(),
                "alert_type": alert.alert_type,
                "severity": alert.severity,
                "news2_total": alert.news2_total,
                "composite_risk": alert.composite_risk,
                "detail": alert.detail,
            }
            self._produce(
                self.k_cfg.alerts_topic, alert.patient_id, json.dumps(body).encode("utf-8")
            )

        score_row = RiskScoreRow(
            patient_id=reading.patient_id,
            scorer_version=self.scorer_version,
            scored_at=reading.measured_at,
            window_start=trend.window_start,
            window_end=trend.window_end,
            news2_total=news2.total,
            news2_subscores=news2.subscores,
            any_parameter_is_3=news2.any_parameter_is_3,
            clinical_risk=news2.clinical_risk,
            lab_contribution=composite.lab_contribution,
            composite_risk=composite.composite_risk,
            risk_tier=composite.risk_tier,
            labs_stale=composite.labs_stale,
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
            risk_score=composite.composite_risk,
            patient_id=reading.patient_id,
            bed_id=reading.bed_id,
            risk_tier=composite.risk_tier,
            news2_total=news2.total,
            lab_contribution=composite.lab_contribution,
            labs_stale=composite.labs_stale,
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
                scorer_version=self.scorer_version,
            )
            for a in out.alerts
        ]
        write_batch_data(
            dao=self.dao,
            vitals=[self._vital_row(reading)],
            risk_scores=[score_row],
            snapshots=[snap_row],
            alerts=alert_rows,
            summaries=[out.summary] if out.summary else [],
        )

        v = self.scorer_version
        metrics.risk_scores_written_total.labels(scorer_version=v, tier=composite.risk_tier).inc()
        metrics.patient_news2.labels(patient_id=reading.patient_id, scorer_version=v).set(
            news2.total
        )
        metrics.patient_composite_risk.labels(patient_id=reading.patient_id, scorer_version=v).set(
            composite.composite_risk
        )
        metrics.stream_end_to_end_seconds.labels(scorer_version=v).observe(
            max(0.0, time.time() - reading.ingest_time.timestamp())
        )

    def _stored_summary(self, patient_id: str, day: date) -> DailyPatientSummaryRow | None:
        rows = self.dao.get_daily_patient_summary(
            ward_id="WARD-A", report_date=day, scorer_version=self.scorer_version
        )
        return next((r for r in rows if r.patient_id == patient_id), None)

    # ---------------------------------------------------------------- output

    def _store_lab(self, lab: LabResult) -> None:
        """Labs reach Cassandra through the stream, like everything else. Before this,
        nothing wrote labs_by_patient and the API's labs endpoint was always empty."""
        low, high = lab.reference_low, lab.reference_high
        try:
            self.dao.insert_lab_result(
                LabResultRow(
                    patient_id=lab.patient_id,
                    test_type=lab.test_type.lower(),
                    collected_at=lab.collected_at,
                    result_value=Decimal(str(lab.result_value)),
                    unit=lab.unit,
                    reference_low=Decimal(str(low)) if low is not None else None,
                    reference_high=Decimal(str(high)) if high is not None else None,
                    out_of_range=lab.is_abnormal,
                    sim_date=lab.collected_at.date(),
                )
            )
            metrics.sink_writes_total.labels(table="labs_by_patient").inc()
        except Exception as exc:
            metrics.sink_write_errors_total.labels(table="labs_by_patient").inc()
            log.warning("lab_write_failed", error=str(exc))

    def _vital_row(self, reading: VitalsReading) -> VitalReadingRow:
        present = [
            reading.heart_rate,
            reading.spo2,
            reading.respiratory_rate,
            reading.systolic_bp,
            reading.temperature,
            reading.consciousness,
        ]
        return VitalReadingRow(
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
            parameters_missing=sum(1 for x in present if x is None),
            ingest_time=reading.ingest_time,
        )

    def _produce(self, topic: str, key: str, value: bytes) -> None:
        try:
            self.producer.produce(topic=topic, key=serialize_key(key, topic), value=value)
            self.producer.poll(0)
        except Exception as exc:
            log.warning("produce_failed", topic=topic, error=str(exc))

    def _dead_letter(
        self,
        raw: bytes,
        reason: str,
        detail: str,
        patient_id: str | None,
        device_id: str | None,
        measured_at: datetime | None,
        partition: int,
        offset: int,
    ) -> None:
        """Dead-letter with the original bytes and exact log position, so the record
        can be traced back and replayed. The first version sent an empty payload and
        partition 0 / offset 0 for everything."""
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        envelope = {
            "original_payload": raw,
            "rejection_reason": reason,
            "rejection_detail": detail,
            "validator": "ward.stream.clean",
            "patient_id": patient_id,
            "device_id": device_id,
            "rejected_at_sim": int(measured_at.timestamp() * 1000) if measured_at else now_ms,
            "rejected_at_real": now_ms,
            "source_topic": self.k_cfg.vitals_topic,
            "source_partition": partition,
            "source_offset": offset,
            "trace_id": None,
        }
        try:
            value = serialize_value(
                envelope,
                self.k_cfg.dlq_topic,
                self.k_cfg.schema_registry_url,
                "dlq_envelope.avsc",
            )
            self._produce(self.k_cfg.dlq_topic, patient_id or "unknown", value)
        except Exception as exc:
            log.warning("dlq_serialize_failed", error=str(exc))

    # --------------------------------------------------------------- engines

    def run_consumer(self) -> None:
        """Plain Kafka consumer loop, for running outside Spark (local debugging)."""
        self.start_reference_data()
        consumer = Consumer(
            {
                "bootstrap.servers": self.k_cfg.bootstrap,
                "group.id": self.consumer_group,
                "auto.offset.reset": os.environ.get("STARTING_OFFSETS", "earliest"),
                "enable.auto.commit": True,
            }
        )
        consumer.subscribe([self.k_cfg.vitals_topic])
        log.info("consumer_loop_started", topic=self.k_cfg.vitals_topic)
        try:
            while self.running:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                err = msg.error()
                if err is not None:
                    if err.code() != KafkaError._PARTITION_EOF:
                        log.error("consumer_error", error=str(err))
                    continue
                value, partition, offset = msg.value(), msg.partition(), msg.offset()
                if value and partition is not None and offset is not None:
                    self.handle(value, partition, offset)
                    metrics.stream_partition_offset.labels(
                        scorer_version=self.scorer_version, partition=str(partition)
                    ).set(offset + 1)
        finally:
            consumer.close()

    def run_spark(self) -> None:
        """PySpark Structured Streaming: Kafka source, foreachBatch sink."""
        from pyspark.sql import SparkSession
        from pyspark.sql.streaming import StreamingQueryListener

        self.start_reference_data()
        version = self.scorer_version

        spark = (
            SparkSession.builder.appName(f"ward-stream-{version}")
            .master("local[4]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.port", "4040")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        class _Progress(StreamingQueryListener):
            """Spark has no scrape endpoint of its own, so the listener copies each
            micro-batch's progress into this process's /metrics."""

            def onQueryStarted(self, event: Any) -> None:  # noqa: N802
                log.info("streaming_query_started", id=str(event.id), scorer_version=version)

            def onQueryProgress(self, event: Any) -> None:  # noqa: N802
                p = event.progress
                metrics.stream_batch_duration_seconds.labels(scorer_version=version).set(
                    p.batchDuration / 1000.0
                )
                metrics.stream_batch_input_rows.labels(scorer_version=version).set(p.numInputRows)
                metrics.stream_last_progress_timestamp_seconds.labels(scorer_version=version).set(
                    time.time()
                )

            def onQueryIdle(self, event: Any) -> None:  # noqa: N802
                metrics.stream_last_progress_timestamp_seconds.labels(scorer_version=version).set(
                    time.time()
                )

            def onQueryTerminated(self, event: Any) -> None:  # noqa: N802
                log.error("streaming_query_terminated", exception=str(event.exception))

        spark.streams.addListener(_Progress())

        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.k_cfg.bootstrap)
            .option("subscribe", self.k_cfg.vitals_topic)
            .option("startingOffsets", os.environ.get("STARTING_OFFSETS", "earliest"))
            .option("maxOffsetsPerTrigger", os.environ.get("MAX_OFFSETS_PER_TRIGGER", "5000"))
            .option("failOnDataLoss", "false")
            .load()
            .select("value", "partition", "offset")
        )

        def write_micro_batch(batch_df: Any, batch_id: int) -> None:
            # Ordered per partition, so each patient's readings are handled in log
            # order - the processor's windows and alert episodes depend on it.
            rows = batch_df.orderBy("partition", "offset").collect()
            last: dict[int, int] = {}
            for r in rows:
                if r.value:
                    self.handle(bytes(r.value), r.partition, r.offset)
                last[r.partition] = r.offset
            for partition, offset in last.items():
                metrics.stream_partition_offset.labels(
                    scorer_version=version, partition=str(partition)
                ).set(offset + 1)
            self.producer.flush(timeout=10)

        query = (
            df.writeStream.foreachBatch(write_micro_batch)
            .trigger(processingTime=f"{self.proc_cfg.trigger_seconds} seconds")
            .option("checkpointLocation", f"/checkpoints/ward-stream-{version}")
            .start()
        )
        log.info("spark_structured_streaming_query_started", scorer_version=version)
        query.awaitTermination()


def main() -> int:
    parser = argparse.ArgumentParser(description="Ward stream processing pipeline")
    parser.add_argument("--scorer-version", default=os.environ.get("SCORER_VERSION", "v1"))
    parser.add_argument(
        "--consumer-group", default=os.environ.get("CONSUMER_GROUP", "ward-stream-v1")
    )
    parser.add_argument(
        "--engine",
        default=os.environ.get("STREAM_ENGINE", "auto"),
        choices=["auto", "spark", "consumer"],
    )
    args = parser.parse_args()

    obs = settings.observability()
    configure(
        service=f"ward-stream-{args.scorer_version}",
        stage="process",
        level=obs.log_level,
        json_output=obs.log_json,
    )
    metrics.serve(port=obs.metrics_port)
    log.info(
        "stream_pipeline_started",
        scorer_version=args.scorer_version,
        engine=args.engine,
        starting_offsets=os.environ.get("STARTING_OFFSETS", "earliest"),
    )

    runner = StreamPipelineRunner(args.scorer_version, args.consumer_group)

    def _sig_handler(sig: int, frame: FrameType | None) -> None:
        runner.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    use_spark = args.engine == "spark"
    if args.engine == "auto":
        try:
            import pyspark  # noqa: F401

            use_spark = True
        except ImportError:
            use_spark = False

    if use_spark:
        runner.run_spark()
    else:
        runner.run_consumer()
    return 0


if __name__ == "__main__":
    sys.exit(main())

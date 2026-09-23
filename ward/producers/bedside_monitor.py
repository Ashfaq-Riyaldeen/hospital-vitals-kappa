"""The streaming source: 40 bedside monitors publishing vital signs to Kafka.

One reading per patient every 15 SIMULATED minutes. At a 288x speed-up that is one
reading per patient every 3.1 real seconds, so the ward produces about 13 readings a
second in total.

KEYED BY patient_id, AND THAT IS A CORRECTNESS REQUIREMENT. Kafka guarantees ordering
within a partition, not across partitions. Trend detection asks "is this patient
deteriorating", which is a slope over their ordered readings -- so all of one
patient's readings must land on one partition. Keying by bed_id or device_id would
work until a patient moved bed or a monitor was swapped, at which point their history
would split mid-admission and the slope would become meaningless. Key on the entity
the state is ABOUT.

Run:
    python -m ward.producers.bedside_monitor
    python -m ward.producers.bedside_monitor --dry-run   # no Kafka needed
"""

from __future__ import annotations

import argparse
import signal
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from random import Random
from types import FrameType

from ward import settings
from ward.obs import metrics
from ward.obs.kafka_client import build_producer
from ward.obs.log import configure, get_logger
from ward.producers.physiology import defects, narratives
from ward.producers.physiology.walk import step
from ward.producers.ward_model import Patient, build_ward
from ward.simclock import SimClock, read_anchor, utcnow

log = get_logger()


class BedsideMonitors:
    def __init__(self, clock: SimClock, dry_run: bool = False) -> None:
        self.clock = clock
        self.dry_run = dry_run
        self.cfg = settings.ward()
        self.kafka_cfg = settings.kafka()
        self.rng = Random(self.cfg.random_seed)
        self.patients = build_ward(
            beds=self.cfg.beds, admitted_at=clock.epoch_sim, seed=self.cfg.random_seed
        )
        self.producer = None if dry_run else build_producer("bedside-monitor")
        self._running = True
        self._emitted = 0
        # Retained so a DUPLICATE_READING defect can republish a real previous id
        # rather than a fabricated one -- a duplicate of an id that never existed
        # would not exercise the deduplication path the way a real replay does.
        self._last_reading_id: dict[str, str] = {}

    # ------------------------------------------------------------------ build

    def _reading(self, patient: Patient, sim_now: datetime) -> tuple[dict, str | None]:
        """Advance one patient and build their reading. Returns (payload, defect)."""
        patient.state = step(patient.state, patient.baseline, sim_now, self.rng)
        patient.state = narratives.apply(
            patient.state,
            narratives.NarrativeContext(patient.patient_id, sim_now, self.clock.epoch_sim),
        )

        state, defect = defects.maybe_inject(patient.state, self.rng)
        measured_at = sim_now
        reading_id = str(uuid.uuid4())

        if defect and defect.future_timestamp:
            measured_at = defects.future_timestamp(sim_now)
        if defect and defect.duplicate:
            reading_id = self._last_reading_id.get(patient.patient_id, reading_id)

        empty = defect is not None and defect.reason == "EMPTY_READING"

        payload = {
            "reading_id": reading_id,
            "patient_id": patient.patient_id,
            "bed_id": patient.bed_id,
            "device_id": patient.device_id,
            "measured_at": measured_at,
            "ingest_time": utcnow(),
            # A disconnected monitor reports NOTHING, which is different from
            # reporting zeros. The validator must be able to tell those apart, so the
            # empty case sends nulls rather than a payload of 0s.
            "heart_rate": None if empty else round(state.heart_rate),
            "spo2": None if empty else round(state.spo2),
            "respiratory_rate": None if empty else round(state.respiratory_rate),
            "systolic_bp": None if empty else round(state.systolic_bp),
            "diastolic_bp": None if empty else round(state.diastolic_bp),
            "temperature": None if empty else round(state.temperature, 1),
            # Consciousness is not modelled by the walk; ward observations are
            # overwhelmingly "Alert", and inventing variation would put noise into a
            # NEWS2 parameter that scores 3 whenever it is anything else.
            "consciousness": None if empty else "A",
            "on_supplemental_oxygen": patient.on_supplemental_oxygen,
            "producer_id": "bedside-monitor",
            "schema_version": 1,
        }
        self._last_reading_id[patient.patient_id] = reading_id
        return payload, (defect.reason if defect else None)

    # ---------------------------------------------------------------- publish

    def _delivery_report(self, err: object, _msg: object) -> None:
        if err is not None:
            metrics.readings_produced_total.labels(status="failed").inc()
            metrics.producer_errors_total.labels(error_type=type(err).__name__).inc()
            log.error("delivery_failed", error=str(err), stage="ingest")
        else:
            metrics.readings_produced_total.labels(status="ok").inc()

    def _publish(self, payload: dict) -> None:
        if self.dry_run or self.producer is None:
            metrics.readings_produced_total.labels(status="ok").inc()
            return

        from ward.contracts.serialization import serialize_key, serialize_value

        topic = self.kafka_cfg.vitals_topic
        while True:
            try:
                self.producer.produce(
                    topic=topic,
                    key=serialize_key(payload["patient_id"], topic),
                    value=serialize_value(
                        payload,
                        topic,
                        self.kafka_cfg.schema_registry_url,
                        "vitals_reading.avsc",
                    ),
                    on_delivery=self._delivery_report,
                )
                break
            except BufferError:
                # ★ BLOCK, never drop. The local queue is full, so we wait for it to
                # drain rather than discarding the reading. Dropping a vital sign to
                # keep a buffer healthy is the wrong trade in this domain: the
                # backpressure is counted and visible here, a discarded reading would
                # not be. A real monitor that keeps measuring during a network outage
                # and delivers late is behaving correctly.
                metrics.producer_backpressure_total.inc()
                self.producer.poll(0.5)

        self.producer.poll(0)

    # -------------------------------------------------------------------- run

    def run(self) -> None:
        interval_real = self.clock.sim_to_real(self.cfg.reading_interval_sim_minutes * 60)
        log.info(
            "monitors_starting",
            beds=len(self.patients),
            interval_sim_minutes=self.cfg.reading_interval_sim_minutes,
            interval_real_seconds=round(interval_real, 2),
            expected_readings_per_second=round(len(self.patients) / interval_real, 1),
            dry_run=self.dry_run,
            stage="ingest",
        )

        next_sweep = time.monotonic()
        last_log = time.monotonic()

        while self._running:
            sim_now = self.clock.sim_now()

            # One sweep of the whole ward per interval. Real monitors are not
            # synchronised, but a sweep keeps the readings-per-window count exact,
            # which matters because the trend window's confidence threshold is
            # expressed in readings.
            for patient in self.patients:
                payload, defect = self._reading(patient, sim_now)
                if defect:
                    metrics.defects_injected_total.labels(defect_type=defect).inc()
                self._publish(payload)
                self._emitted += 1

            metrics.sim_clock_day_index.set(self.clock.sim_day_index())

            if time.monotonic() - last_log >= 30:
                self._log_status(sim_now)
                last_log = time.monotonic()

            next_sweep += interval_real
            sleep_for = next_sweep - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                # The host could not keep up with the configured cadence. Resync
                # rather than accumulating debt: a producer silently falling further
                # behind real time would make every latency measurement meaningless.
                log.warning(
                    "sweep_overran",
                    behind_seconds=round(-sleep_for, 2),
                    detail="host cannot sustain the configured reading cadence",
                    stage="ingest",
                )
                next_sweep = time.monotonic()

    def _log_status(self, sim_now: datetime) -> None:
        log.info(
            "monitors_status",
            emitted=self._emitted,
            sim_time=sim_now.isoformat(),
            sim_day=self.clock.sim_day_index(),
            stage="ingest",
        )

    def request_stop(self, signum: int, _frame: FrameType | None) -> None:
        log.info("shutdown_requested", signal=signal.Signals(signum).name, stage="ingest")
        self._running = False

    def shutdown(self) -> None:
        if self.producer is not None:
            # Flush before exit or the last sweep is lost. A monitor that drops its
            # final readings on shutdown is the kind of thing nobody notices until a
            # patient's last hour is missing from the record.
            remaining = self.producer.flush(timeout=10)
            log.info("flushed", undelivered=remaining, emitted=self._emitted, stage="ingest")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Simulated bedside monitors")
    parser.add_argument("--dry-run", action="store_true", help="generate but do not publish")
    args = parser.parse_args(argv)

    obs = settings.observability()
    configure(
        service="bedside-monitor", stage="ingest", level=obs.log_level, json_output=obs.log_json
    )

    clock = read_anchor(Path(settings.sim().state_path))
    monitors = BedsideMonitors(clock, dry_run=args.dry_run)

    signal.signal(signal.SIGTERM, monitors.request_stop)
    signal.signal(signal.SIGINT, monitors.request_stop)

    if not args.dry_run:
        metrics.serve(obs.metrics_port)

    try:
        monitors.run()
    finally:
        monitors.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())

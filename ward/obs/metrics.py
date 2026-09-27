"""Prometheus metrics.

Named and typed per plan/09 section 3. Conventions, enforced by
`tests/unit/test_metrics_registry.py`: counters end in `_total`, durations end in
`_seconds`, and every metric carries HELP text.

The catalogue below is the ingest and store half. The streaming query metrics cannot
be scraped - the Structured Streaming queries run inside the Spark driver with no HTTP
endpoint - so they are PUSHED from a StreamingQueryListener; see `push_gauges`.

WHY A MISSING METRIC MATTERS MORE HERE. plan/09's framing is that silence is not
safety: in a ward monitor, "no alerts" and "the pipeline died" produce the same empty
screen. Several of these metrics exist specifically so the two can be told apart.
"""

from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

# --- ingestion -----------------------------------------------------------

readings_produced_total = Counter(
    "readings_produced_total",
    "Vital-sign readings handed to Kafka, by outcome of the delivery callback.",
    ["status"],
)
producer_send_duration_seconds = Histogram(
    "producer_send_duration_seconds",
    "Wall-clock time from produce() to the delivery callback.",
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0),
)
producer_errors_total = Counter(
    "producer_errors_total", "Producer errors by class.", ["error_type"]
)
producer_buffered_readings = Gauge(
    "producer_buffered_readings",
    "Readings held in a bedside monitor's disconnect buffer. A real monitor keeps "
    "measuring while the network is down; a rising depth is the ward's network, not "
    "the patient.",
)
producer_backpressure_total = Counter(
    "producer_backpressure_total",
    "Times the local produce queue was full and the producer blocked. Blocking is "
    "deliberate: silently dropping a vital-sign reading is a clinical-safety bug.",
)
defects_injected_total = Counter(
    "defects_injected_total",
    "Deliberately corrupted readings, by defect type. This is the CONTROL for the "
    "dead-letter path - what the simulator injects must come out the other end.",
    ["defect_type"],
)
lab_files_written_total = Counter(
    "lab_files_written_total", "Daily lab result files dropped.", ["scenario"]
)
lab_file_delay_seconds = Histogram(
    "lab_file_delay_seconds",
    "How late a lab file arrived relative to its simulated day boundary.",
)

# --- processing (app-level; streaming-query metrics are PUSHED, see push_gauges) ---

readings_validated_total = Counter(
    "readings_validated_total", "Readings after validation.", ["result"]
)
readings_dlq_total = Counter(
    "readings_dlq_total",
    "Readings dead-lettered, by reason and originating device. The device_id label "
    "turns a data-quality metric into a ward-operations one: 'BED-22's probe keeps "
    "detaching' is actionable in a way that a global reject rate is not.",
    ["reason", "device_id"],
)
readings_late_total = Counter(
    "readings_late_total", "Readings arriving after the watermark, side-processed."
)
readings_deduplicated_total = Counter(
    "readings_deduplicated_total", "Duplicate readings suppressed."
)
readings_without_admission_total = Counter(
    "readings_without_admission_total",
    "Readings for a patient with no admission record - an unknown patient on a "
    "monitored bed, which is a ward-process problem rather than a pipeline one.",
)
clinical_alerts_emitted_total = Counter(
    "clinical_alerts_emitted_total",
    "Alerts about PATIENTS, as distinct from pipeline-health alerts about the "
    "system. The two are kept apart deliberately: mixing them is how staff learn to "
    "ignore both.",
    ["type", "severity"],
)
lab_join_hit_rate = Gauge(
    "lab_join_hit_rate", "Fraction of scores enriched with a fresh lab result."
)
admissions_refresh_total = Counter("admissions_refresh_total", "Admissions broadcast reloads.")
lab_results_cached_total = Counter(
    "lab_results_cached_total",
    "Lab results read from the compacted labs topic into the stream's join cache.",
)

risk_scores_written_total = Counter(
    "risk_scores_written_total",
    "Risk scores produced by the stream, by scorer version and risk tier. Its RATE is "
    "the heartbeat behind WardMonitoringSilent: when it drops to zero the ward screen "
    "is showing nothing new, whatever else still looks healthy.",
    ["scorer_version", "tier"],
)
patient_news2 = Gauge(
    "patient_news2",
    "Latest NEWS2 total per patient and scorer version. 40 patients x 2 versions is a "
    "small, fixed label set, so a per-patient gauge is safe here.",
    ["patient_id", "scorer_version"],
)
patient_composite_risk = Gauge(
    "patient_composite_risk",
    "Latest composite risk (NEWS2 plus lab contribution) per patient and scorer version.",
    ["patient_id", "scorer_version"],
)
stream_end_to_end_seconds = Histogram(
    "stream_end_to_end_seconds",
    "REAL seconds from the monitor stamping ingest_time to the score being written to "
    "Cassandra. This is what a nurse waits for.",
    ["scorer_version"],
    buckets=(0.5, 1, 2, 3, 5, 7.5, 10, 15, 20, 30, 60, 120, 300),
)
stream_batch_duration_seconds = Gauge(
    "stream_batch_duration_seconds",
    "Duration of the last Structured Streaming micro-batch, from the query listener.",
    ["scorer_version"],
)
stream_batch_input_rows = Gauge(
    "stream_batch_input_rows",
    "Rows read by the last micro-batch.",
    ["scorer_version"],
)
stream_last_progress_timestamp_seconds = Gauge(
    "stream_last_progress_timestamp_seconds",
    "Unix time of the last micro-batch progress event. Its age separates a dead query "
    "from an idle one.",
    ["scorer_version"],
)
stream_partition_offset = Gauge(
    "stream_partition_offset",
    "Next Kafka offset the stream will read, per partition. Structured Streaming keeps "
    "offsets in its checkpoint, not in a consumer group, so this is how a replay's "
    "progress is measured.",
    ["scorer_version", "partition"],
)
serving_stale_labs_patients = Gauge(
    "serving_stale_labs_patients",
    "Patients whose latest score was made without fresh lab results.",
)
serving_mean_composite_risk = Gauge(
    "serving_mean_composite_risk",
    "Mean composite risk across the ward's current snapshot.",
)

# --- storage -------------------------------------------------------------

sink_writes_total = Counter("sink_writes_total", "Rows written to Cassandra.", ["table"])
sink_write_errors_total = Counter("sink_write_errors_total", "Failed Cassandra writes.", ["table"])

# --- the simulated clock -------------------------------------------------

sim_clock_day_index = Gauge("sim_clock_day_index", "Simulated days elapsed since the run started.")
sim_clock_drift_seconds = Gauge(
    "sim_clock_drift_seconds",
    "Difference between this process's simulated time and the shared anchor. Should "
    "be ~0; anything else means the one-clock design is broken and every window, "
    "trend and daily report downstream is suspect.",
)

# --- replay (the Kappa capability) ---------------------------------------

replay_progress_pct = Gauge("replay_progress_pct", "Percent of the log re-derived.")
replay_events_processed_total = Counter(
    "replay_events_processed_total", "Events re-processed during a replay."
)
replay_scores_changed_total = Counter(
    "replay_scores_changed_total",
    "Scores that differ between the old and new scorer version. The expected shape "
    "is the whole point: only COPD patients should move, and only downward.",
    ["direction"],
)
replay_duration_seconds = Gauge(
    "replay_duration_seconds",
    "Wall-clock seconds for a full replay - the headline number in the demo.",
)


class _Handler(BaseHTTPRequestHandler):
    """Serves /metrics and /health from one port.

    Compose healthchecks need a liveness endpoint and Prometheus needs a scrape
    endpoint; running two servers per container to get both is not worth it.
    """

    def do_GET(self) -> None:
        if self.path.startswith("/metrics"):
            body = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
        elif self.path.startswith("/health"):
            body = b'{"status":"ok"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
        else:
            body = b"not found"
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args: object) -> None:
        """Silence the default stderr access log - it is unstructured and would
        pollute the JSON log stream."""


def serve(port: int) -> ThreadingHTTPServer:
    """Expose /metrics and /health on a daemon thread. Called once at start-up."""
    server = ThreadingHTTPServer(("0.0.0.0", port), _Handler)
    Thread(target=server.serve_forever, daemon=True, name="metrics").start()
    return server


# --- serving layer (plan/09 section 3) ------------------------------------

serving_batch_watermark_age_sim_days = Gauge(
    "serving_batch_watermark_age_sim_days",
    "Simulated days between the batch high-water mark and the current simulated date. "
    "The single most important number about the batch layer: a watermark that stops "
    "advancing is the batch layer having quietly died, and from every other angle "
    "that looks identical to a healthy pipeline.",
)

serving_degraded_responses_total = Counter(
    "serving_degraded_responses_total",
    "Responses served with one store unreachable, by which store was missing. "
    "Degrading is correct behaviour, so this is not an error counter - but a rate "
    "that stays above zero means the demo is running on half the architecture.",
    ["missing_store"],
)

serving_merge_boundary_crossings_total = Counter(
    "serving_merge_boundary_crossings_total",
    "Requests whose date range spanned the batch/speed boundary, so both views "
    "contributed rows. This is the reconciliation actually happening; a flat zero "
    "means every request landed wholly inside one view and the merge was never "
    "exercised.",
)

serving_uncovered_dates_total = Counter(
    "serving_uncovered_dates_total",
    "Requested simulated dates that neither view could answer for, because the batch "
    "layer has fallen more than one simulated day behind the clock.",
)


# --- pushgateway ----------------------------------------------------------
#
# Spark's Structured Streaming queries cannot be scraped. They run inside the driver
# with no HTTP endpoint of their own, and - the part that actually matters - consumer
# lag does NOT measure them: the queries checkpoint their own offsets and never
# commit to a consumer group. Setting `kafka.group.id` to make lag work killed the
# DLQ query after ~4,500 micro-batches (plan/09 §3, the correction block).
#
# So the driver PUSHES. Pushgateway is a known anti-pattern for exactly one reason -
# a pushed metric outlives the process that pushed it, so a dead job looks alive
# forever. That is mitigated by pushing `..._last_progress_timestamp` and alerting on
# its AGE rather than on the values themselves: a query that has stopped stops
# updating the timestamp, and the timestamp going stale is the signal.


def push_gauges(job: str, values: dict[str, float], grouping: dict[str, str] | None = None) -> bool:
    """Push a set of gauge values to the Pushgateway. Returns whether it worked.

    Never raises. This is called from inside a `StreamingQueryListener` callback on
    the Spark driver; an exception escaping there would take down the listener and,
    with it, all visibility into the speed layer - trading the entire observability
    story for a transient HTTP error. A failed push is logged and counted, and the
    next micro-batch tries again.
    """
    from prometheus_client import CollectorRegistry, push_to_gateway
    from prometheus_client import Gauge as _Gauge

    from ward import settings as config
    from ward.obs.log import get_logger

    registry = CollectorRegistry()
    for name, value in values.items():
        _Gauge(name, f"{name} (pushed from the Spark driver)", registry=registry).set(value)

    try:
        push_to_gateway(
            config.observability().pushgateway_url,
            job=job,
            registry=registry,
            grouping_key=grouping or {},
            timeout=5,
        )
        return True
    except Exception as exc:
        get_logger().warning("pushgateway_failed", job=job, error=str(exc))
        return False

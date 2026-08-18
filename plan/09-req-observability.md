# 09 — Requirement 4: Observability

> **PDF minimum:** *"Structured logging across ingestion, processing, and storage stages. At least one basic alert or health-check rule (e.g. no data received in N minutes, error rate above threshold)."*
>
> **Rubric weight: 10 marks** — *"**Logging, metrics and tracing** across pipeline stages to detect and diagnose pipeline failures."*
>
> **Honesty requirement:** Prometheus, Grafana, Alertmanager, OpenTelemetry and Loki were **not taught in this module**. The report introduces them in a clearly-labelled paragraph as deliberate extensions. The module's own observability content is the Airflow Web UI and Kafka's *"Operational Metrics"* / *"Log Aggregation"* use cases, both of which we also use.

---

## 1. The design principle that makes this project's observability different

In most pipelines, "no data" is a warning. **Here it is the most dangerous state the system can be in.**

A ward monitor showing 40 patients all at LOW risk looks identical whether (a) all 40 patients are genuinely stable, or (b) the pipeline died 20 minutes ago and the screen is frozen. A clinician cannot distinguish these, and the second is a patient-safety event.

> **"Absence of alerts must never be mistaken for absence of risk."**

This single sentence drives three design decisions that a non-clinical project would not make, and the report should present them as a set:

1. **`WardMonitoringSilent` is a *critical* alert**, not a warning — no risk score written for *any* patient in 3 minutes.
2. **`ward_risk_snapshot` carries a 120-second TTL** (`07 §3`), so if the stream stops the ward monitor **empties** rather than freezing. An empty screen is unmistakably broken; a stale screen is not.
3. **Stale labs contribute zero to risk *and* set a visible flag** (`05 §6.3`) — missing data is never silently treated as reassuring.

Three layers — alerting, data model, business logic — expressing one principle. That coherence is worth pointing out explicitly.

---

## 2. Structured logging

`structlog` → JSON → stdout, configured once in `ward/obs/log.py` and imported by producers, the Spark driver, Airflow tasks and the API.

**Mandatory field schema on every line:**

```json
{
  "ts": "2026-08-04T18:22:07.412Z",
  "level": "info",
  "service": "ward-stream",
  "stage": "process",
  "event": "batch_scored",
  "sim_date": "2026-04-02",
  "sim_time": "2026-04-02T12:00:00Z",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "correlation_id": "P014",
  "scorer_version": "v2",
  "duration_ms": 84.2,
  "record_count": 40
}
```

Rules, enforced by `test_logging_schema.py`:
- `stage` ∈ `{ingest, process, store, serve, orchestrate}` — this is what makes "logging **across** stages" verifiable rather than asserted.
- `correlation_id` is the `patient_id` wherever one exists, so grepping one patient's journey across every service works.
- **`scorer_version` on every processing and serving log line** — when two versions run concurrently during a replay, logs must be attributable. Without it, replay debugging is impossible.
- `trace_id`/`span_id` injected from the active OTel context, so logs and traces join.
- No f-strings in log messages — `log.info("batch_scored", record_count=n)`, never `log.info(f"scored {n}")`.

**Aggregation (stretch):** Promtail → Loki → a Grafana log panel. First on the cut list; `docker compose logs` satisfies the PDF's minimum.

---

## 3. Metrics

### 3.1 Ingestion (`:8101`, `:8102`)

| Metric | Type | Labels |
|---|---|---|
| `readings_produced_total` | counter | `status=ok\|failed` |
| `producer_send_duration_seconds` | histogram | — |
| `producer_errors_total` | counter | `error_type` |
| `producer_buffered_readings` | gauge | Monitor disconnect-buffer depth |
| `producer_backpressure_total` | counter | — |
| `defects_injected_total` | counter | `defect_type` |
| `sim_clock_day_index` | gauge | — |
| `sim_clock_drift_seconds` | gauge | Should be ~0; non-zero means the shared-clock design is broken |
| `lab_files_written_total` | counter | — |
| `lab_file_delay_seconds` | histogram | Late-arrival distribution |

### 3.2 Kafka

`kafka-exporter` on `:9308`:
- `kafka_consumergroup_lag{group,topic,partition}` — **the single most important metric.** During a replay it also *is* the replay progress indicator.
- `kafka_topic_partition_current_offset` — used to compute `replay_progress_pct`

### 3.3 Processing (Spark)

Structured Streaming does not expose Prometheus metrics from PySpark out of the box. Options evaluated:

| Option | Verdict |
|---|---|
| Spark's `PrometheusServlet` (`spark.ui.prometheus.enabled=true`) | Exposes JVM/executor metrics but not Structured Streaming progress detail (watermark, state rows, input rate) in usable form; scrape-target discovery is awkward in compose |
| **`StreamingQueryListener` → Pushgateway** ✅ | We choose exactly what to export; identical mechanism for the live job and a replay job; no scrape-target discovery |

**Chosen: the listener + Pushgateway.** We note in the report that Pushgateway is generally an anti-pattern for long-lived services — it retains the last pushed value forever, so a dead job looks alive — and we mitigate that precisely: we also export `stream_last_progress_timestamp` and **alert on its age**, never on its presence. Naming the anti-pattern and showing the mitigation is a better answer than using it naively.

```python
class WardStreamListener(StreamingQueryListener):
    def onQueryProgress(self, event):
        p = event.progress
        push_metrics({
            "stream_input_rows_per_second":     p.inputRowsPerSecond,
            "stream_processed_rows_per_second": p.processedRowsPerSecond,
            "stream_batch_duration_seconds":    p.durationMs["triggerExecution"] / 1000,
            "stream_state_rows":                sum(s.numRowsTotal for s in p.stateOperators),
            "stream_watermark_lag_seconds":     _watermark_lag(p.eventTime),
            "stream_last_progress_timestamp":   time.time(),
        }, labels={"query": p.name, "scorer_version": SCORER_VERSION})

    def onQueryTerminated(self, event):
        log.error("stream_terminated", query_id=str(event.id), reason=event.exception)
```

Note `scorer_version` as a label — so live and replay jobs are separable on every panel.

Business counters emitted from within the pipeline:

| Metric | Labels | Why |
|---|---|---|
| `readings_validated_total` | `result=valid\|invalid` | |
| `readings_dlq_total` | `reason`, **`device_id`** | `device_id` turns a data-quality metric into a ward-operations one: *"BED-22's probe keeps detaching"* |
| `readings_late_total` | — | Late arrivals side-processed (`04 §6.2`) |
| `readings_deduplicated_total` | — | |
| `readings_without_admission_total` | — | Unknown patient on a monitored bed |
| **`risk_scores_written_total`** | `tier`, `scorer_version` | **The heartbeat of the whole system** — the metric `WardMonitoringSilent` watches |
| `clinical_alerts_emitted_total` | `type`, `severity` | |
| `lab_join_hit_rate` | — | % of scores with fresh labs |
| `admissions_refresh_total` | — | Broadcast reload count |

### 3.4 Storage (Cassandra)

`jmx_exporter` sidecar (Cassandra exposes JMX, not Prometheus natively — call this out; it is a real integration detail):
- Read/write latency percentiles, pending compactions, **tombstone scanned histogram** (should stay ~0, which is our TTL-not-delete design being verified in production), dropped mutations, heap usage.

App-level: `sink_write_duration_seconds{table}`, `sink_write_errors_total{table}`, `cassandra_rows_written_total{table}`.

### 3.5 Serving

`prometheus-fastapi-instrumentator` gives request rate, latency and status per route. Plus:
- `serving_active_scorer_version` (as a labelled gauge) — makes a cutover visible on a dashboard
- `serving_stale_labs_patients` — how many patients are being assessed on vitals alone
- `serving_snapshot_age_seconds` — how old the ward snapshot is

### 3.6 Orchestration

Airflow StatsD → `statsd-exporter` → Prometheus: DAG duration, task failures, scheduler heartbeat.

### 3.7 Replay

| Metric | Purpose |
|---|---|
| `replay_progress_pct` | Drives the demo's progress panel |
| `replay_events_processed_total` | |
| `replay_scores_changed_total{direction}` | The diff, live |
| `replay_duration_seconds` | The headline number: *"115,200 events re-derived in N seconds"* |

---

## 4. Alerting — two kinds, deliberately separated

**This separation is a report point in its own right.** A clinician paged about Kafka consumer lag is noise; an SRE paged about a patient's NEWS2 is negligence.

### 4.1 Pipeline-health alerts (Prometheus → Alertmanager)

| Alert | Expression (simplified) | For | Severity |
|---|---|---|---|
| **`WardMonitoringSilent`** | `rate(risk_scores_written_total[3m]) == 0` | 3m | **critical** |
| **`NoVitalsIngested`** | `rate(readings_produced_total{status="ok"}[2m]) == 0` | 3m | critical |
| **`HighDLQRate`** | `rate(readings_dlq_total[5m]) / rate(readings_produced_total[5m]) > 0.05` | 5m | warning |
| `StreamingQueryStalled` | `time() - stream_last_progress_timestamp > 120` | 1m | critical |
| `ConsumerLagGrowing` | `kafka_consumergroup_lag{group="ward-stream-v1"} > 5000` | 5m | warning |
| `WatermarkLagHigh` | `stream_watermark_lag_seconds > 7200` | 5m | warning |
| `StateStoreGrowing` | `deriv(stream_state_rows[15m]) > 50` | 15m | warning |
| `SnapshotIncomplete` | `serving_snapshot_patients < ward_size * 0.9` | 3m | warning |
| `LabDataStale` | `time() - lab_last_ingested_timestamp > 2 sim-days` | 5m | warning |
| `CassandraWriteErrors` | `rate(sink_write_errors_total[5m]) > 0` | 2m | critical |
| `CassandraTombstoneScan` | `cassandra_tombstone_scanned > 100` | 10m | warning |
| `DeviceFaultRate` | `rate(readings_dlq_total[15m]) by (device_id) > 0.2` | 15m | warning |
| `AirflowDagFailed` | `airflow_dag_run_failed > 0` | 1m | warning |
| `SimClockDrift` | `abs(sim_clock_drift_seconds) > 5` | 2m | critical |
| `ApiHighLatency` | `histogram_quantile(0.95, ...) > 0.5` | 5m | warning |

`NoVitalsIngested` and `HighDLQRate` are the PDF's explicitly requested rules. `WardMonitoringSilent` is the one the domain demands, and it is listed first deliberately.

Note **`DeviceFaultRate`**: it is technically a pipeline metric but clinically actionable — a specific bedside monitor is faulty. Worth mentioning as a case where the two categories genuinely overlap, which is more honest than pretending the taxonomy is perfectly clean.

Alertmanager routes by severity to a webhook receiver that logs alerts as structured JSON and exposes them at `/api/v1/alerts/pipeline`, so firing alerts are visible in the demo without email or Slack.

### 4.2 Clinical alerts (emitted by the stream)

Produced into `vitals.alerts.v1` and written to Cassandra; surfaced via the API and the Ward Monitor. Full table in `05 §7`: `NEWS2_HIGH`, `NEWS2_MEDIUM`, `SINGLE_PARAM_RED`, `SPO2_CRITICAL`, `RAPID_DETERIORATION`, `LAB_CORROBORATED_RISK`, `TREND_CONCERNING`.

Deduplicated by `alert_id = sha1(patient_id, alert_type, window_start)` and re-emitted only on escalation — **alert fatigue is a real clinical failure mode**, and designing against it belongs in the report.

### 4.3 Proving the alerts work

An alert rule that has never fired is untested code. `make chaos` provides:

| Command | Injects | Expect |
|---|---|---|
| `make chaos-kill-monitors` | Stops the bedside producer | `NoVitalsIngested` ≤ 3 min, then `WardMonitoringSilent`; **the Ward Monitor visibly empties as TTLs expire** |
| `make chaos-kill-stream` | Stops the Spark job | `WardMonitoringSilent` + `StreamingQueryStalled` ≤ 3 min while vitals keep flowing — **isolating the failure to the processing stage** |
| `make chaos-bad-data` | `DEFECT_RATE=0.30` | `HighDLQRate` ≤ 5 min |
| `make chaos-device-fault` | One device emits only invalid readings | `DeviceFaultRate` for that `device_id` |
| `make chaos-skip-labs` | Suppress the next lab file | `LabDataStale`; API reports `labs_stale`; **ward keeps being monitored** |

**`chaos-kill-monitors` is run on camera** (`13 §1`). Watching the ward monitor empty and the critical alert fire is the most persuasive 30 seconds of the observability section, because it demonstrates the design principle from §1 rather than describing it.

---

## 5. Distributed tracing — and an honest limitation

### 5.1 What we do

- **Producers:** one span per flush; W3C `traceparent` injected into Kafka message headers.
- **FastAPI:** auto-instrumented, with child spans per Cassandra query — a slow endpoint shows *which* partition read was slow.
- **Airflow:** native OTel traces where the version supports it, else manual spans per `@task` — one trace per DAG run.
- **Spark:** one span per **micro-batch** on the driver inside `foreachBatch`, carrying `batch_id`, row count, watermark, `scorer_version`, and child spans per Cassandra table write. From a **sample** of the batch's Kafka headers we extract incoming `traceparent` values and attach them as **span links**.
- **Collector:** OTel Collector → Jaeger all-in-one (`:16786`).

Result: for a sampled reading we can show `monitor.send` → (link) → `stream.micro_batch` → `stream.sink.cassandra` → `api.request` → `api.cassandra.read`.

### 5.2 The limitation, stated plainly

**True per-event distributed tracing through a micro-batch engine is not achievable at reasonable cost, and we do not claim it.**

1. A micro-batch collapses many readings into one unit of work — there is no per-event span to nest.
2. PySpark executors are separate processes; a tracer initialised per partition yields disconnected fragments, and the JVM-side shuffle is invisible to a Python tracer.
3. Emitting a span per reading would generate more telemetry than data.

**So we trace at micro-batch granularity and link sampled events**, which is what production streaming systems actually do. The report states this with that framing, in both §7 and §9. A marker who knows the space will recognise it as the correct answer; claiming full per-event tracing would be recognised as false.

---

## 6. Health checks

| Endpoint / mechanism | Checks |
|---|---|
| `GET /health/live` | Process responsive |
| `GET /health/ready` | Cassandra session established |
| `GET /health/deep` | Actually queries Kafka metadata, Cassandra, and the snapshot table; returns per-dependency status and latency |
| `GET /api/v1/pipeline/status` | Sim clock, stream liveness, consumer lag, **active scorer version**, lab freshness, snapshot completeness |
| Docker `healthcheck` | Every service; `depends_on: service_healthy` orders start-up |
| `ward_healthcheck` DAG | Independent probe every 2 real minutes (`08 §6`) |

---

## 7. Grafana dashboards

Provisioned from `observability/grafana/provisioning/` so a fresh clone gets working dashboards.

**1. Ward Monitor** (business) — a 40-bed grid coloured by risk tier; per-patient NEWS2 sparklines; live alert feed; tier distribution; ward mean score; **stale-labs count**; patients with low-confidence scores.

**2. Pipeline Health** — laid out left-to-right by stage so a break is visually located:
```
INGEST                PROCESS                   STORE                SERVE
readings/s            batch duration            write latency        req rate
producer errors       input vs processed rate   write errors         p95 latency
DLQ rate by reason    watermark lag             tombstone scans      snapshot age
late arrivals         state rows                rows written/table   stale-labs count
sim clock drift       ★ risk_scores_written/s   heap / compactions   active scorer version
```
Plus: firing alerts table, consumer lag per group, Airflow DAG status.

**3. Replay Comparison** — v1 vs v2 score series per patient; the diff summary; `replay_progress_pct`; consumer-group offsets side by side.

**4. Traces & Logs** — Jaeger trace search, latency exemplars, Loki panel by `stage`/`level` (if included).

---

## 8. Testing observability

| Test | Asserts |
|---|---|
| `test_logging_schema.py` | Mandatory fields present; `stage` from the allowed set; `scorer_version` on process/serve lines |
| `test_metrics_registry.py` | No duplicate names; HELP text on every metric; Prometheus naming conventions (`_total`, `_seconds`) |
| `test_alert_rules.py` | `promtool check rules` passes in CI; every rule has `severity`, `summary`, `description` |
| `test_alert_firing.py` (integration) | Synthetic series into a test Prometheus; each rule fires and resolves |
| **`test_silence_detection.py`** | With `risk_scores_written_total` flat, `WardMonitoringSilent` fires within the configured window. **The most safety-relevant test in the observability suite.** |
| `test_trace_propagation.py` | A `traceparent` written by the producer is readable and valid on the consumer side |
| `test_health_deep.py` | Degrades gracefully with Cassandra down |

---

## 9. What the report must show (§7)

- **§1 first** — the "silence is not safety" principle and the three design decisions it drives across alerting, the data model and the business logic. This is the most original observability content in either project; lead with it.
- The four-stage measurement table with the **why** column filled in — the rubric asks for *"what is measured, how, and why"*.
- The log schema with a real line, including `scorer_version`.
- The full alert table **split into pipeline-health and clinical**, with the honest note that `DeviceFaultRate` straddles both.
- The Pushgateway choice with the anti-pattern named and mitigated.
- The tracing approach **with the micro-batch limitation**.
- The `make chaos` table as evidence the alerts were actually verified, with measured detection times.
- Screenshots: Ward Monitor healthy; Ward Monitor **empty** after `chaos-kill-monitors` with `WardMonitoringSilent` firing in Alertmanager; Pipeline Health showing the cliff; a Jaeger waterfall; Kafka UI consumer lag; the Replay Comparison dashboard.

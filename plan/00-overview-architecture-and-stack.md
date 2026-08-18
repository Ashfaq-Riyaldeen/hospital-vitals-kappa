# 00 — Overview: Architecture & Technology Stack

> The single document describing **what we are building and what we are building it with**. Every other document expands one box on this page.

---

## 1. The use case in one table

| | |
|---|---|
| **Scenario** | A hospital ward wants continuous, near-real-time monitoring of patient vitals from bedside sensors, correlated daily with lab test results that the pathology lab uploads once a day. |
| **Streaming source** | Bedside monitors emitting `patient_id, heart_rate, spo2, systolic_bp, diastolic_bp, temperature, timestamp` every few seconds, including occasional simulated abnormal spikes. |
| **Daily-batch source** | One daily file of lab results per patient: `patient_id, test_type, result_value, reference_range, collected_at`. |
| **Business question** | *Which patients show concerning vital-sign trends **right now**, and how do **yesterday's lab results** change the risk picture for those patients **going forward**?* |
| **Required outputs** | (1) API for real-time ward monitoring figures; (2) threshold alerts per patient; (3) a daily consolidated patient risk report joining vitals trends with the latest lab results. |

---

## 2. Architecture: **Kappa**

### The decision in one sentence

Every output this system produces — the ward screen, the per-patient alert, the daily risk report — is a view of **the same quantity: a patient's current risk**; splitting that computation across a speed layer and a batch layer would mean two implementations of one clinical rule, and two implementations of a clinical rule is not a maintenance inconvenience, it is a **patient-safety hazard**.

Full reasoning, the rejected Lambda alternative, and the viva defence: **`01-architecture-decision.md`**.

### The architecture, in the module's own vocabulary

The lecturer defines Kappa as *"an alternative to Lambda that uses a **stream-processing platform as the backbone for all data handling — ingestion, storage, and serving** — enabling both real-time and batch processing on the same data by reading live event streams and **replaying large chunks** for batch processing."*

Our implementation maps onto that definition literally:

| Module's phrase | Our implementation |
|---|---|
| "stream-processing platform as the backbone" | Kafka is the system of record. Everything downstream is derived and rebuildable from it. |
| "…for **ingestion**" | Vitals, labs and admissions all enter as Kafka topics. |
| "…**storage**" | `vitals.readings` retains **30 simulated days**. That retention *is* our historical store — we do not maintain a separate warehouse. |
| "…**serving**" | Cassandra holds only **materialised views** of the stream, shaped per query. It is a cache of stream output, not an independent source of truth. |
| "replaying large chunks" | `06-replay-reprocessing.md` — a new consumer group from offset 0 re-derives all history under a new clinical rule version. |
| "single pipeline for both real-time and historical data" | One Structured Streaming job. One `ward/clinical/news2.py`. |

---

## 3. System dataflow

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                            SIMULATED SOURCES (Python)                          │
│                                                                                │
│  bedside_monitor.py                lab_uploader.py         admissions.py       │
│  40 beds · reading every           1 JSON file per          ward reference      │
│  15 simulated minutes              simulated day            data                │
└──────────┬──────────────────────────────┬──────────────────────┬───────────────┘
           │                              │ writes file          │
           │                              ▼                      │
           │                   ┌────────────────────┐            │
           │                   │  labs/inbox/       │            │
           │                   │  labs_<date>.json  │            │
           │                   └─────────┬──────────┘            │
           │                             │ FileSensor            │
           │                             ▼                       │
           │                   ┌────────────────────┐            │
           │                   │  APACHE AIRFLOW    │            │
           │                   │  lab_ingest DAG    │            │
           │                   │  ── produces only, │            │
           │                   │     no logic ──    │            │
           │                   └─────────┬──────────┘            │
           │                             │                       │
           ▼                             ▼                       ▼
  ╔══════════════════════════════════════════════════════════════════════════╗
  ║                          APACHE KAFKA  (KRaft)                           ║
  ║                    ── THE BACKBONE / SYSTEM OF RECORD ──                 ║
  ║                                                                          ║
  ║  vitals.readings.v1     6 partitions · key=patient_id · retain 30 sim-d  ║
  ║  labs.results.v1        3 partitions · key=patient_id|test · COMPACTED   ║
  ║  ward.admissions.v1     3 partitions · key=patient_id     · COMPACTED    ║
  ║  vitals.alerts.v1       3 partitions · key=patient_id                    ║
  ║  vitals.readings.dlq · vitals.late                                       ║
  ╚═══════════════════════════════════╤══════════════════════════════════════╝
                                      │
                                      ▼
  ╔══════════════════════════════════════════════════════════════════════════╗
  ║          ★ THE SINGLE STREAM PROCESSING PATH ★                           ║
  ║          PySpark Structured Streaming — ward/stream/pipeline.py          ║
  ║                                                                          ║
  ║   clean ──▶ validate ──▶ enrich ──▶ window ──▶ SCORE ──▶ lab join ──▶ ▶  ║
  ║   (physiologically   (admissions   (4 sim-h   (NEWS2)   (compacted       ║
  ║    impossible →       broadcast)    sliding)   ▲         topic,          ║
  ║    DLQ)                                        │         broadcast)      ║
  ║                                                │                         ║
  ║                          ward/clinical/news2.py — THE ONE CLINICAL RULE  ║
  ║                          (nothing else in the repo computes a score)     ║
  ╚═══════════════════════════════════╤══════════════════════════════════════╝
                                      │
              ┌───────────────────────┼───────────────────────┐
              ▼                       ▼                       ▼
   ┌────────────────────┐   ┌──────────────────┐   ┌────────────────────┐
   │    CASSANDRA       │   │ vitals.alerts.v1 │   │  vitals.late       │
   │  (materialised     │   │ (business alerts)│   │  (beyond watermark │
   │   views, query-    │   └──────────────────┘   │   — side-processed,│
   │   first modelling) │                          │   never dropped)   │
   │                    │                          └────────────────────┘
   │ vitals_by_patient  │
   │ risk_scores_by_    │        ┌──────────────────────────────────┐
   │   patient (versioned)◀──────│  REPLAY: new consumer group      │
   │ ward_risk_snapshot │        │  from offset 0 → scorer v2       │
   │ alerts_by_ward     │        │  → *_v2 rows → compare → cutover │
   │ labs_by_patient    │        └──────────────────────────────────┘
   │ daily_patient_     │
   │   summary          │
   └─────────┬──────────┘
             │
   ┌─────────┴──────────────────────────────┐
   ▼                                        ▼
┌────────────────────┐          ┌──────────────────────────┐
│  FastAPI           │          │  AIRFLOW                 │
│  ward monitoring   │          │  daily_risk_report DAG   │
│  API               │          │  (reads Cassandra,       │
└─────────┬──────────┘          │   renders PDF — no       │
          │                     │   recomputation)         │
          ▼                     └────────────┬─────────────┘
   ┌──────────────┐                          ▼
   │   GRAFANA    │              ┌───────────────────────────┐
   │ ward monitor │              │ Daily patient risk report │
   └──────────────┘              │  PDF / HTML               │
                                 └───────────────────────────┘

 ── OBSERVABILITY SPINE (cross-cutting) ───────────────────────────────────
    structured JSON logs · Prometheus · OpenTelemetry → Jaeger · Alertmanager
    Airflow UI · Kafka UI · Spark UI · Grafana
```

**The thing to notice:** there is exactly **one** arrow into the scoring box, and exactly one box that computes a score. That is Kappa.

---

## 4. Technology stack — the summary table

Full justification per layer: **`02-technology-stack.md`**.

| Layer | Technology | Version / image | One-line reason it fits *this* scenario |
|---|---|---|---|
| **Backbone / ingestion** | Apache Kafka (KRaft) | `confluentinc/cp-kafka:7.6.1` | The log *is* the system of record. 30-day retention is our history; `patient_id` keying preserves per-patient ordering, which trend detection requires. |
| Schema management | Schema Registry + Avro | `cp-schema-registry:7.6.1` | Adding `respiratory_rate` and `consciousness` for NEWS2 is a live schema-evolution case |
| Broker UI | Kafka UI | `provectuslabs/kafka-ui` | Makes topics, offsets, lag and **replay position** visible — central to the replay demo |
| **Processing** | PySpark Structured Streaming — **one job** | `bitnami/spark:3.5.1` | Single path = single clinical rule; `withWatermark`/`window` implement the taught stream theory directly |
| **Serving store** | **Apache Cassandra** | `cassandra:4.1` | Wide-column: partition by `patient_id`, cluster by time — "all of one patient's data on one node, newest first" is a single sequential read. Write-heavy, no updates, native TTL. |
| **Orchestration** | Apache Airflow | `apache/airflow:2.9.3` | Orchestrates *around* the stream: lab-file ingestion, replay runs, report rendering, retention. Never a second processing path. |
| **Serving API** | FastAPI | build | Ward monitoring endpoints, per-patient detail, replay comparison |
| **Dashboard** | Grafana | `grafana/grafana:11.1.0` | Ward monitor + pipeline health, provisioned as code |
| **Metrics** | Prometheus + Pushgateway + exporters | `prom/prometheus:v2.53.0` | PromQL expresses the health rules; JMX exporter for Cassandra |
| **Alerting** | Alertmanager | `prom/alertmanager:v0.27.0` | Including `WardMonitoringSilent` — the clinically critical one |
| **Tracing** | OTel Collector + Jaeger | `otel/opentelemetry-collector-contrib` | W3C context through Kafka headers |
| **Logging** | `structlog` → JSON | — | Structured logs across ingestion, processing, storage |

> **Honesty note carried into the report:** Prometheus, Grafana, Alertmanager and OpenTelemetry were **not** taught in this module and are introduced as deliberate extensions. Kafka, Spark, Airflow and Cassandra (the module's canonical wide-column store) *were* taught, and the report uses the module's vocabulary for those.

---

## 5. Simulated clock — stated up front

| Parameter | Value |
|---|---|
| **1 simulated day** | **5 real minutes** |
| **Speed-up factor** | **288×** |
| Ward size | 40 beds |
| Vitals sampling interval | every **15 simulated minutes** per patient (routine ward observation cadence) |
| Aggregate event rate | ≈ **13 events/second** |
| Events per simulated day | 40 × 96 = **3,840** |
| Lab file cadence | 1 file per simulated day |
| Kafka retention on vitals | **30 simulated days** ≈ 115,000 events ≈ 30 MB |

**Note the volume figure — it is load-bearing.** 3,840 events per simulated day is roughly **19× smaller** than the sibling ride-hailing project's 72,000. The module's own criterion says *"the Kappa Architecture is ideal for processing smaller data volumes that can be handled in real-time"*, and at this volume a **full replay of 30 simulated days takes seconds**, which is exactly why Kappa's reprocessing story is viable here and would not be at fleet scale.

All components derive simulated time from one shared module (`ward/simclock.py`) anchored to a single epoch at stack start-up. Watermarks are expressed in simulated time; the arithmetic and its trap are in `03 §3.3`.

---

## 6. How the pipeline stays observable

Full design in `09-req-observability.md`.

| URL | What it shows |
|---|---|
| `localhost:3100` | **Grafana** — Ward Monitor, Pipeline Health, Replay Comparison, Traces & Logs |
| `localhost:8180` | **Kafka UI** — topics, partitions, consumer groups, **replay offset progress** |
| `localhost:8182` | **Airflow UI** — lab ingestion, report generation, replay orchestration |
| `localhost:8190` | **Spark UI** — Structured Streaming: batch duration, input rate, watermark, state rows |
| `localhost:9190` | **Prometheus** |
| `localhost:9193` | **Alertmanager** |
| `localhost:16786` | **Jaeger** |
| `localhost:8100/docs` | **FastAPI** OpenAPI docs |

**Two kinds of alert, deliberately separated:**
- **Pipeline-health** (Alertmanager): no vitals ingested for 3 min, DLQ rate > 5%, consumer lag, streaming query stalled, and — the clinically important one — **`WardMonitoringSilent`**: no risk score written for *any* patient in 3 minutes. In this domain a silent monitoring system looks identical to a ward full of healthy patients, and that is the most dangerous failure mode in the system.
- **Business/clinical** (emitted into `vitals.alerts.v1`): NEWS2 ≥ 7 (emergency), 5–6 (urgent review), any single parameter scoring 3, SpO2 critical, rapid deterioration, lab-corroborated risk.

---

## 7. What "done" looks like

Within ~15 real minutes (3 simulated days) of `make demo`, a reviewer can see:

1. Vitals flowing in Kafka UI across 6 partitions keyed by `patient_id`.
2. The Grafana **Ward Monitor** showing 40 patients coloured by NEWS2, updating live.
3. Physiologically impossible readings (SpO2 = 0 from a probe fall-off) being routed to the DLQ rather than scored.
4. **Patient `P014` deteriorating on script** — temperature and heart rate rising, blood pressure falling — NEWS2 climbing 2 → 5 → 7, with alerts escalating.
5. A lab file landing, Airflow ingesting it to the compacted topic, and `P014`'s **composite risk tier rising further** because lactate and WBC are elevated — the business question answered end to end.
6. The **daily consolidated patient risk report** PDF, joining vitals trends with the latest labs.
7. **★ The replay ★** — a scorer v2 with a revised SpO2 threshold started from offset 0, re-deriving 3 simulated days of history in seconds, with v1 and v2 charted side by side and the patients whose tier changed listed.
8. `make chaos-kill-monitors` → `WardMonitoringSilent` firing in Alertmanager within 3 minutes.
9. A Jaeger trace from a bedside reading through the micro-batch into Cassandra and out via the API.

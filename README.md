# Hospital Patient Vital Signs Monitoring

**EC8203 Applied Big Data Engineering - Mini Project (Use Case 2)**  
**Architecture: Kappa**: a single stream-processing path; the retained Kafka log is the historical store.

## Safety Disclaimer

This system processes **entirely simulated data** and is a teaching exercise. NEWS2 is implemented from its published Royal College of Physicians specification for realism, but this software is **not a medical device**, has not been clinically validated, and must not be used for any clinical purpose. No real patient data is involved and no patient identifiers exist.

## The Clinical Business Question

> *Which patients show concerning vital-sign trends **right now**, and how do **yesterday's lab results** change the risk picture for those patients **going forward**?*

Both halves of this question converge on **one single number per patient: their current composite risk**. Laboratory results do not produce a separate answer; they *modify* the same answer.

This is the foundational argument for selecting a **pure Kappa Architecture** over Lambda:
- Splitting the calculation across a speed layer (real-time stream) and a batch layer (nightly batch job) would necessitate two independent implementations of the identical clinical risk calculation.
- Having two implementations of a clinical deterioration rule is a patient-safety hazard: subtle differences between stream windowing and batch aggregation could cause the ward monitor and the daily clinical report to disagree about whether a patient is septic.
- Under Kappa, the **single streaming pipeline (`ward/stream/pipeline.py`)** is the only place risk is calculated, while the retained Kafka log (30 simulated days) serves as the complete historical repository.

Read the architectural analysis in [`plan/01-architecture-decision.md`](plan/01-architecture-decision.md).

## Non-Negotiables & Architectural Invariants

1. **One Clinical Rule, One Implementation**: `ward/clinical/` is the sole package where risk scores and NEWS2 subscores are computed. The AST test `tests/unit/test_news2.py::test_only_one_scorer_exists` scans the entire repository outside `ward/clinical/` and immediately fails if any function or method name contains `score` or `news2`.
2. **Pure Orchestration with Airflow**: Airflow DAGs (`airflow/dags/`) perform zero data transformations, aggregations, or clinical calculations. Airflow strictly coordinates ingestion sensing, replay execution, and reporting. Enforced by `tests/unit/test_dag_purity.py`.
3. **Kafka Retention IS the Historical Store**: The `vitals.readings.v1` topic retains 30 simulated days of readings. Reprocessing runs the same stream (`ward-stream-v2`, started by `make replay`) from offset 0 with its own checkpoint; progress is measured from its per-partition offsets, because Structured Streaming does not commit to a consumer group.
4. **Silence Is Not Safety**: In clinical monitoring, absence of alerts must never be mistaken for absence of risk. If a stream processor dies, the ward monitor must not freeze green; it empties via Cassandra TTL (120s) and triggers the critical alert `WardMonitoringSilent`.
5. **Replay Invariant Verification**: Re-deriving history under revised clinical rules (NEWS2 SpO2 Scale 2 for COPD patients) may only change COPD patients (down for their usual 88-92 % saturation, up only when on oxygen at 93 %+) and must leave every other patient unchanged. Checked on the live data by `make diff` and by the `ward_replay` DAG before its approval step.

## Quick Start

See [**`SETUP.md`**](SETUP.md) for full step-by-step instructions.

```bash
# 1. Copy pre-configured environment
cp .env.example .env

# 2. Launch full platform stack
make up

# 3. Wait for all services to report healthy
make wait

# 4. Run full test suite on host
make test
```

## Platform Architecture & Ports

| Service | Host Port / URL | Credentials / Notes |
|---|---|---|
| **Grafana Dashboards** | [http://localhost:3100](http://localhost:3100) | Anonymous Admin (No login needed) |
| **FastAPI Swagger Docs** | [http://localhost:8100/docs](http://localhost:8100/docs) | Interactive clinical serving endpoints |
| **Airflow Web UI** | [http://localhost:8182](http://localhost:8182) | `admin` / `admin` |
| **Kafka UI** | [http://localhost:8180](http://localhost:8180) | Topic inspection, message browser, lag |
| **Prometheus** | [http://localhost:9190](http://localhost:9190) | Metrics scraper & alerting rules |
| **Alertmanager** | [http://localhost:9193](http://localhost:9193) | Clinical safety vs SRE alert routing |
| **Schema Registry** | [http://localhost:8181/subjects](http://localhost:8181/subjects) | Avro schemas |
| **Cassandra** | `localhost:9142` | CQL database |
| **Kafka Broker** | `localhost:9192` | Plaintext listener |

*Port Isolation Note*: Host ports 91xx, 81xx, and 31xx are used deliberately to avoid colliding with default ports and the sibling ride-hailing project.

## Production Grafana Dashboards

Provisioned automatically as code from `observability/grafana/`:

1. **Ward · Clinical Monitor (live)** (`ward-clinical-monitor`):
   - Real-time 40-bed status grid color-coded by clinical response tier (LOW, MEDIUM, HIGH, CRITICAL).
   - Deteriorating patient counters, stale lab alerts, and live clinical alert feeds.
2. **Ward · Pipeline Health** (`ward-pipeline-health`):
   - End-to-end telemetry across INGEST -> STREAM -> STORE -> SERVE.
   - Watermark delay, micro-batch latency, DLQ error rates, and the `WardMonitoringSilent` safety indicator.
3. **Ward · Replay & Reprocessing (v1 vs v2)** (`ward-replay-comparison`):
   - Demonstrates Kappa log replay from offset 0.
   - Compares COPD patient trajectories (P031) under v1 vs v2 alongside the bit-identical non-COPD control group.

## Kappa Replay & Cutover Commands

```bash
# Launch stream replay from offset 0 with target scorer version
make replay VERSION=v2

# Track consumer group lag catching up
make replay-status

# Generate clinical audit difference report
make diff

# Instantaneously switch active serving version across the API
make cutover VERSION=v2

# Instantaneous rollback if needed
make rollback VERSION=v1
```

## Simulated Clock & Acceleration

The platform operates at **288× acceleration**:
- **1 simulated day = 300 real seconds (5 minutes)**.
- Every container derives simulated time from a single synchronized anchor file (`state/sim_epoch.json`).
- A 60-simulated-minute watermark represents 12.5 real seconds.

## Repository Layout

| Path | Purpose / Kappa Stage |
|---|---|
| `ward/clinical/` | The single clinical risk engine (NEWS2, SpO2 Scale 1 & 2, SOFA/lactate modifiers) |
| `ward/producers/` | Ingestion layer: bedside vital monitors, daily lab uploader, admissions reference feed |
| `ward/stream/` | Stream processing: PySpark Structured Streaming pipeline, cleaning, 4-hour windowing |
| `ward/store/` | Storage layer: Cassandra schema (Q1-Q7 query-first tables) and WardStoreDAO |
| `ward/reporting/` | Orchestrated daily PDF report generator (Jinja2 + WeasyPrint) |
| `ward/api/` | Serving layer: FastAPI service on port 8100 with worst-first monitor and clinical explainability |
| `ward/replay/` | Replay runner, version comparison engine, and cutover mechanics |
| `airflow/dags/` | 5 production DAGs: lab ingest, daily report, replay coordination, healthcheck, retention |
| `observability/` | Prometheus scrape configs, alert rules, Alertmanager routing, and Grafana dashboards as code |
| `tests/` | 533 passing unit and contract tests, plus one integration test run against the live stack |
| `plan/` | 16 exhaustive architecture planning documents |

## Team Contributions

| Contributor | Area of Focus |
|---|---|
| **Munsif** (`munsif-dev`) | Core clinical engine, contracts, stream processing pipeline, observability integration |
| **Ashfaq** (`Ashfaq-Riyaldeen`) | Infrastructure, Docker stack, Cassandra storage layer, FastAPI serving layer, replay runner |
| **Lareef** (`Lareefmohamed`) | Tooling, Airflow orchestration, reporting renderer, contract testing, dashboard validation |

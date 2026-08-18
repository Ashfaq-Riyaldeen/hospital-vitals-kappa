# 07 — Requirement 3: Cassandra & the Serving Layer

> **PDF requirement:** *"Processed results must land in a queryable store suited to your serving needs. The final deliverable of the running system is a consolidated (daily/hourly) report or dashboard... that answers the business question."*
>
> **Rubric weight: 10 marks** — *"correctness of implementation of the serving layer."*

---

## 1. Query-first data modelling — the method

Cassandra is not modelled from entities; it is modelled from **queries**. You enumerate every question the system must answer, then design one table per question so that each is a **single-partition read**. The module's framing — *"fast by row key but limited complex queries"* — is the constraint that forces this discipline.

**Under Kappa this is exactly right**, and the two decisions reinforce each other: all aggregation and joining already happened in the stream, so the store never needs to compute anything. It holds **materialised views of stream output**. Cassandra's inability to aggregate is not a limitation we work around — it is a property that *enforces* that computation stays in the one place we want it.

**The cost, stated honestly:** a question nobody anticipated needs a new table, and a new table needs a **replay** to backfill it. That is only affordable because replay here is cheap (`06 §6`). At a scale where replay were expensive, query-first modelling would be a serious liability. Say both halves.

### The query catalogue

| # | Question | Asked by | Table |
|---|---|---|---|
| Q1 | Latest N vital readings for patient X | Patient detail view, sparklines | `vitals_by_patient` |
| Q2 | Current risk for every patient in the ward, worst first | The ward monitor — **the main screen** | `ward_risk_snapshot` |
| Q3 | Risk score history for patient X over 24 simulated hours | Trend chart; **v1 vs v2 comparison** | `risk_scores_by_patient` |
| Q4 | Recent alerts for the ward, newest first | Alert feed | `alerts_by_ward` |
| Q5 | Latest lab results for patient X | Patient detail; report | `labs_by_patient` |
| Q6 | Everything needed for one simulated day's report | Daily report DAG | `daily_patient_summary` |
| Q7 | Pipeline/simulation state | Health endpoint | `sim_state` |

Seven questions, seven tables. **No query in this system requires `ALLOW FILTERING`**, and a test asserts that string appears nowhere in the repository.

---

## 2. Schema — `ward/store/schema.cql`

```sql
CREATE KEYSPACE ward WITH replication =
  {'class': 'SimpleStrategy', 'replication_factor': 1};
  -- Demo only. Production: NetworkTopologyStrategy, RF=3, LOCAL_QUORUM reads/writes.

-- Q1: latest N readings for one patient, newest first
CREATE TABLE ward.vitals_by_patient (
    patient_id      text,
    measured_at     timestamp,
    reading_id      uuid,
    bed_id          text,
    heart_rate      int,
    spo2            int,
    systolic_bp     int,
    diastolic_bp    int,
    temperature     decimal,
    respiratory_rate int,
    consciousness   text,
    on_supplemental_oxygen boolean,
    parameters_missing int,
    ingest_time     timestamp,
    PRIMARY KEY ((patient_id), measured_at, reading_id)
) WITH CLUSTERING ORDER BY (measured_at DESC, reading_id ASC)
  AND default_time_to_live = 10800;   -- 30 simulated days = 3 real hours

-- Q3: score history per patient, PER SCORER VERSION (this is what makes replay safe)
CREATE TABLE ward.risk_scores_by_patient (
    patient_id       text,
    scorer_version   text,
    scored_at        timestamp,
    window_start     timestamp,
    window_end       timestamp,
    news2_total      int,
    news2_subscores  map<text, int>,
    any_parameter_is_3 boolean,
    clinical_risk    text,
    lab_contribution int,
    composite_risk   int,
    risk_tier        text,
    labs_stale       boolean,
    confidence       text,
    hr_slope         decimal,
    spo2_slope       decimal,
    sbp_slope        decimal,
    temp_slope       decimal,
    readings_in_window int,
    PRIMARY KEY ((patient_id), scorer_version, scored_at)
) WITH CLUSTERING ORDER BY (scorer_version ASC, scored_at DESC)
  AND default_time_to_live = 10800;

-- Q2: the ward monitor. Single-partition read, PRE-SORTED BY RISK.
CREATE TABLE ward.ward_risk_snapshot (
    ward_id        text,
    scorer_version text,
    risk_score     int,
    patient_id     text,
    bed_id         text,
    risk_tier      text,
    news2_total    int,
    lab_contribution int,
    labs_stale     boolean,
    scored_at      timestamp,
    hr_latest      int,
    spo2_latest    int,
    sbp_latest     int,
    temp_latest    decimal,
    PRIMARY KEY ((ward_id, scorer_version), risk_score, patient_id)
) WITH CLUSTERING ORDER BY (risk_score DESC, patient_id ASC)
  AND default_time_to_live = 120;     -- ★ see §3 — TTL instead of tombstones

-- Q4: ward alert feed, partitioned by day so partitions stay bounded
CREATE TABLE ward.alerts_by_ward (
    ward_id      text,
    sim_date     date,
    alert_time   timestamp,
    alert_id     text,
    patient_id   text,
    bed_id       text,
    alert_type   text,
    severity     text,
    news2_total  int,
    composite_risk int,
    detail       text,
    acknowledged boolean,
    PRIMARY KEY ((ward_id, sim_date), alert_time, alert_id)
) WITH CLUSTERING ORDER BY (alert_time DESC, alert_id ASC)
  AND default_time_to_live = 10800;

-- Q5: latest labs per patient
CREATE TABLE ward.labs_by_patient (
    patient_id      text,
    test_type       text,
    collected_at    timestamp,
    result_value    decimal,
    unit            text,
    reference_low   decimal,
    reference_high  decimal,
    out_of_range    boolean,
    sim_date        date,
    PRIMARY KEY ((patient_id), test_type, collected_at)
) WITH CLUSTERING ORDER BY (test_type ASC, collected_at DESC);

-- Q6: one partition = everything the daily report needs for one ward-day
CREATE TABLE ward.daily_patient_summary (
    ward_id             text,
    sim_date            date,
    patient_id          text,
    scorer_version      text,
    bed_id              text,
    admitting_condition text,
    max_news2           int,
    mean_news2          decimal,
    max_composite_risk  int,
    final_risk_tier     text,
    lab_contribution    int,
    labs_stale          boolean,
    alert_count         int,
    highest_severity    text,
    readings_count      int,
    readings_rejected   int,
    deterioration_detected boolean,
    PRIMARY KEY ((ward_id, sim_date), patient_id, scorer_version)
);

CREATE TABLE ward.sim_state (
    key text PRIMARY KEY, value text, updated_at timestamp
);
```

### Design decisions worth defending

| Decision | Reason |
|---|---|
| Partition by `patient_id` on Q1/Q3/Q5 | All of one patient's data on one node, contiguous. The dominant query is a single sequential read with no sort. |
| Cluster `DESC` on time | "Latest N" is a prefix scan — no ORDER BY, no reversal cost. |
| **`risk_score` in the clustering key of Q2** | Cassandra cannot `ORDER BY` across partitions. Putting the score *in* the clustering key means **the ward monitor's "worst patients first" ordering is free** — a single partition read returns rows already sorted. This is the single cleverest piece of modelling in the project and it is worth explaining in the report. |
| **`sim_date` in the partition key of Q4** | Bounds partition growth. Without it, `alerts_by_ward` would grow forever into one unbounded partition — the classic Cassandra anti-pattern. **Time-bucketing partitions is the standard fix and naming it shows real familiarity.** |
| **`scorer_version` in the key of Q2/Q3** | v1 and v2 coexist; replay is non-destructive and reversible (`06 §3`). |
| `default_time_to_live` on Q1/Q3/Q4 | Retention enforced by the database, not by a cron job. Maps to the module's *"data retention policies... balancing value against storage costs and compliance mandates"*. |
| `map<text,int>` for NEWS2 subscores | Keeps the score **explainable** — the API can say *which* parameter contributed what, which matters for a clinical number. |

---

## 3. TTL instead of tombstones — the elegant bit

`ward_risk_snapshot` is a **mutable current-state view**: when a patient's risk changes from 5 to 7, the old row must stop being returned.

The naive approach is `DELETE` then `INSERT`. **That is a trap.** Cassandra deletes write **tombstones**, which are markers that must be read and skipped on every subsequent query until compaction removes them. A table updated every micro-batch would accumulate tombstones continuously, read latency would degrade, and eventually queries would fail on the tombstone threshold. This is one of the best-known ways to misuse Cassandra.

**Our approach: never delete. Use a short TTL.**

```
default_time_to_live = 120 seconds
```

The stream writes a fresh snapshot row for every patient on every micro-batch (5-second trigger). Each row lives 120 seconds. Stale rows **expire themselves**; no tombstone is ever written by the application.

Consequences, all of which we accept knowingly:
- Multiple risk_score rows for one patient may briefly coexist (the previous score has not yet expired). **The API takes the row with the newest `scored_at` per patient.** ~15 lines in the DAO, unit-tested.
- If the stream stops, the snapshot table **empties within 120 seconds**. That is a *feature*: an empty ward monitor is unmistakably a system failure, whereas a screen frozen on stale values looks like a ward full of stable patients. **This is the "silence is not safety" principle expressed in the data model**, and it connects directly to the `WardMonitoringSilent` alert (`09 §4.1`).

That last point is the strongest thing to say about this table in a viva: **the TTL is a safety mechanism, not just a cleanup mechanism.**

---

## 4. Consistency and driver configuration

| Setting | Demo | Production | Why |
|---|---|---|---|
| Replication | `SimpleStrategy`, RF=1 | `NetworkTopologyStrategy`, RF=3 | Single node in demo — declared as a limitation |
| Write consistency | `LOCAL_ONE` | `LOCAL_QUORUM` | RF=1 makes anything else meaningless |
| Read consistency | `LOCAL_ONE` | `LOCAL_QUORUM` | |
| Load balancing | `TokenAwarePolicy(DCAwareRoundRobinPolicy)` | same | Correct even at one node; would be a latent bug otherwise |
| Prepared statements | Always | always | Never string-interpolate CQL — injection safety and performance |
| Batches | **Only single-partition `BEGIN BATCH`** | same | Multi-partition batches are a Cassandra anti-pattern (they create coordinator hotspots). We use `execute_concurrent` for cross-partition writes instead. **Knowing this distinction is a good viva answer.** |

---

## 5. The serving layer — FastAPI

`ward/api/`. Async throughout, using the async Cassandra driver.

| Method | Path | Table | Notes |
|---|---|---|---|
| GET | `/api/v1/ward/{ward_id}/monitor` | Q2 | **The main screen.** Every patient, worst first. One partition read. |
| GET | `/api/v1/ward/{ward_id}/summary` | Q2 | Counts by tier, mean score, patients with stale labs |
| GET | `/api/v1/patients/{id}` | Q1+Q3+Q5 | Detail: latest vitals, current score with subscores, latest labs. `asyncio.gather` over three partition reads. |
| GET | `/api/v1/patients/{id}/vitals?limit=100` | Q1 | Sparkline data |
| GET | `/api/v1/patients/{id}/risk-history?hours=24` | Q3 | Trend chart |
| GET | `/api/v1/patients/{id}/risk-history?scorer_version=v2` | Q3 | **Replay validation** |
| GET | `/api/v1/patients/{id}/labs` | Q5 | With `out_of_range` flags |
| GET | `/api/v1/alerts?ward_id=&severity=&limit=` | Q4 | Alert feed |
| POST | `/api/v1/alerts/{alert_id}/acknowledge` | Q4 | The one write endpoint — models clinical acknowledgement |
| GET | `/api/v1/reports/daily/{sim_date}` | Q6 | Report metadata + link to the PDF |
| GET | `/api/v1/replay/compare?v1=&v2=` | Q3 | **The diff endpoint** (`06 §5`) |
| GET | `/api/v1/pipeline/status` | all | Sim clock, stream liveness, consumer lag, active scorer version, lab freshness |
| GET | `/metrics` · `/health/live` · `/health/ready` · `/health/deep` | — | Observability |

### Response design

Every risk response carries the fields that make the number **auditable**:

```json
{
  "patient_id": "P014",
  "bed_id": "BED-14",
  "scored_at": "2026-04-02T12:00:00Z",
  "scorer_version": "v2",
  "news2_total": 7,
  "news2_subscores": {"respiratory_rate": 2, "spo2": 1, "systolic_bp": 2,
                      "heart_rate": 2, "temperature": 0, "consciousness": 0,
                      "supplemental_oxygen": 0},
  "any_parameter_is_3": false,
  "clinical_risk": "HIGH",
  "lab_contribution": 3,
  "labs_stale": false,
  "composite_risk": 10,
  "risk_tier": "CRITICAL",
  "confidence": "high",
  "readings_in_window": 16,
  "explanation": "NEWS2 7 (HIGH) raised to composite 10 (CRITICAL) by lactate 3.8 mmol/L (ref 0.5-2.2) and WBC 18.4 x10^9/L (ref 4.0-11.0)."
}
```

Three deliberate properties:
1. **`scorer_version` is in the payload** — a client always knows which rule produced the number. Without it, a cutover would silently change the meaning of the response.
2. **The decomposition is explicit** (`news2_total`, `lab_contribution`, `composite_risk`) — this *is* the business question, answered in a single object: here is the vitals picture, here is how the labs changed it.
3. **`explanation` is human-readable** — a clinical score that cannot be explained is not usable. Generated from the same data, not hand-written.

Screenshot `/docs` showing this model for the report.

---

## 6. The consolidated daily report

The PDF requires *"a daily consolidated patient risk report joining vitals trends with the latest lab results"*.

Generated by the `ward_daily_report` DAG (`08 §3`): Jinja2 → HTML → **WeasyPrint** → PDF, written to `reports/ward_risk_report_<sim_date>.pdf`.

**Critically: the DAG reads `daily_patient_summary` (Q6) and renders it. It recomputes nothing.** Every number in the report was produced by the stream. That is what keeps the report and the ward screen in agreement, and it is the practical payoff of the architecture — worth one explicit sentence in the report itself.

Contents:
1. **Header** — ward, simulated date, generation time, **active scorer version**, data-completeness statement
2. **Ward summary** — patients monitored, tier distribution, alerts raised, readings ingested/rejected
3. **The answer, part 1** — patients by peak risk, descending, with 24-simulated-hour NEWS2 sparklines
4. **The answer, part 2** — **the lab-corroboration table**: for each patient, NEWS2 vs composite, the lab contribution, and which specific results drove it. *This table is the literal answer to "how do yesterday's lab results change the risk picture".*
5. **Deterioration events** — patients whose score rose ≥ 3 in 4 simulated hours, with timelines
6. **Patients with stale or missing labs** — risk assessed on vitals alone, flagged explicitly
7. **Data quality** — readings rejected by reason, **by device** (so "BED-22's probe keeps detaching" surfaces), late arrivals
8. **Footer** — job run ID, scorer version, source table, row counts. *Provenance, so any figure is traceable.*

---

## 7. Grafana dashboards

Provisioned as code in `observability/grafana/`.

| Dashboard | Key panels |
|---|---|
| **Ward Monitor** (business) | 40-bed grid coloured by risk tier; per-patient NEWS2 sparklines; live alert feed; tier distribution; ward mean score; **stale-labs indicator** |
| **Pipeline Health** | `09 §7` |
| **Replay Comparison** | v1 vs v2 score series per patient; diff summary; replay progress; consumer-group offsets |
| **Traces & Logs** | Jaeger links, latency exemplars, log panel by `stage`/`level` |

**Datasource recommendation: the Infinity (JSON) datasource pointed at our own FastAPI endpoints**, rather than the third-party Cassandra plugin. Two reasons: one less plugin to install and pin, and — better — the dashboard then exercises the serving layer instead of bypassing it, so the dashboard and the API cannot disagree. Note the Cassandra plugin as the alternative considered.

---

## 8. Testing

| Test | Asserts |
|---|---|
| `test_schema.py` | Every table creates cleanly; every query in the catalogue runs **without `ALLOW FILTERING`** |
| **`test_no_allow_filtering.py`** | The string appears nowhere in the repo |
| `test_snapshot_latest_wins.py` | With several un-expired snapshot rows for one patient, the DAO returns the newest `scored_at` |
| `test_ttl_applied.py` | Inserted rows carry the expected TTL |
| `test_partition_bounds.py` | `alerts_by_ward` partitions are day-bucketed; a simulated-year of alerts never lands in one partition |
| `test_version_isolation.py` | v1 and v2 rows coexist; querying one version never returns the other |
| `test_api_contract.py` | Every risk response carries `scorer_version`, the decomposition, and `labs_stale`; OpenAPI snapshot |
| `test_explanation.py` | The generated explanation names the actual out-of-range results |
| `test_health_deep.py` | Degrades rather than crashing when Cassandra is unavailable |
| `test_prepared_statements.py` | No f-string or `%`-formatted CQL anywhere |

---

## 9. What the report must show (§6.4 / §6.5)

- The **query catalogue** (§1) and the query-first method, with the honest cost (new question ⇒ new table ⇒ replay).
- The schema, with the two clever bits called out: **`risk_score` in the clustering key** (free ordering) and **`sim_date` in the partition key** (bounded partitions).
- **TTL instead of tombstones**, and the safety argument that an empty ward monitor is better than a frozen one.
- The endpoint table and the self-describing response with `scorer_version` and the risk decomposition.
- The point that the daily report **recomputes nothing** — and why that is the architecture paying off.
- The honest weakness list from `02 §3`: no joins, no ad-hoc aggregation, `ALLOW FILTERING` banned, denormalisation accepted, single node in demo.
- Screenshots: the Ward Monitor dashboard, a page of the daily report PDF (especially the lab-corroboration table), and `/docs`.

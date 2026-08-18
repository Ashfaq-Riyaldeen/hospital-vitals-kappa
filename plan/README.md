# Project B — Hospital Patient Vital Signs Monitoring · Plan Index

**Module:** EC8203 Applied Big Data Engineering — Mini Project (25% of module grade)
**Use Case:** 2 — Hospital Patient Vital Signs Monitoring
**Architecture:** **Kappa** (a single stream-processing path; the log is the backbone)
**Status:** PLANNING — no code written yet. Review these documents, then approve to start building.

---

## The business question we must answer

> *"Which patients show concerning vital-sign trends **right now**, and how do **yesterday's lab results** change the risk picture for those patients **going forward**?"*

Both halves are about **one number per patient: their current risk.** The lab results do not produce a separate answer — they *modify* the same answer. That observation is what drives the entire architecture decision. See `01-architecture-decision.md`.

---

## How to read this plan

| # | Document | What it covers | Rubric marks |
|---|---|---|---|
| **00** | [Overview: Architecture & Technology](00-overview-architecture-and-stack.md) | **START HERE.** The whole system on one page | Context |
| **01** | [Architecture Decision — Kappa vs Lambda](01-architecture-decision.md) | The 8-criteria argument, the rejected alternative, the patient-safety case, viva defence | **20** |
| **02** | [Technology Stack Justification](02-technology-stack.md) | Every layer, tied to a ward's constraints | **10** |
| **03** | [Requirement 1 — Simulated Data Sources](03-req-data-sources.md) | Bedside monitors, lab uploads, the simulated clock, the scripted deterioration | **15** |
| **04** | [Requirement 1 — Ingestion & Kafka Design](04-req-ingestion-kafka.md) | Topics, compaction, retention-as-storage, DLQ, late arrivals | **15** |
| **05** | [Requirement 2 — The Single Stream Pipeline](05-req-processing-stream.md) | Clean → enrich → window → NEWS2 → lab join → sink. One path, one clinical rule. | **15** |
| **06** | [Reprocessing & Replay](06-replay-reprocessing.md) | Re-deriving history when the clinical rule changes — Kappa's defining capability | **15/20** |
| **07** | [Requirement 3 — Cassandra & Serving Layer](07-req-storage-serving.md) | Query-first data modelling, TTL-over-tombstones, the ward API | **10** |
| **08** | [Orchestration with Airflow](08-orchestration-airflow.md) | Orchestrating *around* the stream, never a second processing path | **15** |
| **09** | [Requirement 4 — Observability](09-req-observability.md) | Metrics, logs, tracing, alerts — and why silence is a clinical emergency | **10** |
| **10** | [Infrastructure & Docker Compose](10-infrastructure-docker.md) | Service topology, Cassandra tuning, WSL2 gotchas | **5** |
| **11** | [Code Quality, Testing & Repo Structure](11-code-quality-testing.md) | Layout, the NEWS2 reference tests, config | **5** |
| **12** | [Report Writing Plan](12-report-plan.md) | Section-by-section outline, diagrams, screenshots | **15** |
| **13** | [Demo Video & Submission](13-demo-and-submission.md) | Demo script, submission checklist | Required |
| **14** | [Two-Week Schedule](14-schedule.md) | Day-by-day build order | — |
| **15** | [Risks & What to Cut](15-risks-and-cuts.md) | Risk register and cut order | — |

---

## The one-paragraph summary

Bedside monitors for a 40-bed ward publish vital-sign readings to **Kafka**, keyed by `patient_id`. The pathology lab uploads one results file per simulated day; **Airflow** senses it and publishes each result to a **log-compacted** `labs.results` topic — a thin ingestion step containing no business logic. A **single PySpark Structured Streaming job** does everything else: it cleans physiologically impossible readings, enriches with admission context, computes windowed vital-sign trends, calculates a **NEWS2 early-warning score**, joins the latest lab results to produce a composite risk tier, and writes query-shaped rows into **Cassandra**. There is no batch layer and no second implementation of the clinical rule — the ward screen, the alerts and the daily risk report are all derived from the same computation, so they cannot disagree about a patient. When the clinical rule changes, we **replay the retained Kafka log from offset 0** into a new scorer version writing to versioned rows, compare v1 against v2 side by side, and cut over. A **FastAPI** serving layer and **Grafana** dashboards read Cassandra; **Airflow** also renders the daily consolidated patient-risk report. The whole pipeline carries structured JSON logs, Prometheus metrics, OpenTelemetry traces and Alertmanager rules.

---

## Non-negotiables

1. **One clinical rule, one implementation.** `ward/clinical/news2.py` must be the only place a risk score is computed. If a second implementation ever appears, the architecture argument collapses. A test enforces this.
2. **The replay demo must work.** It is Kappa's whole justification and the centrepiece of the video (`06`).
3. **Airflow must contain no transformation logic.** Sensing, producing, triggering, rendering — nothing else. A test enforces this.
4. **The "silence is not safety" alert** (`WardMonitoringSilent`) must fire when the stream stops. In this domain, absence of alerts must never be mistaken for absence of risk.
5. **Kafka retention is the historical store** — 30 simulated days, and the report must say so explicitly, because that is what makes Kappa's replay possible.
6. **`make clean && make up && make demo` from a fresh clone.**

---

## Safety disclaimer (must appear in the README and the report)

> This system processes **entirely simulated data** and is a teaching exercise. NEWS2 is implemented from its published specification for realism, but this software is **not a medical device**, has not been clinically validated, and must not be used for any clinical purpose. No real patient data is involved and no patient identifiers exist.

---

## Relationship to the sibling project

`../ride-hailing-lambda` is a different group's submission using a **Lambda** architecture over ride-hailing fleet data with Redis, MinIO/Parquet and PostgreSQL. The two projects share no code and reach opposite architectural conclusions from the same module material — which is the point. See `01 §2.2` for how the module's own "data volume" criterion discriminates between the two.

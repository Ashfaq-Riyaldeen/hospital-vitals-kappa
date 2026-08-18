# 12 — Report Writing Plan

> **PDF:** *"Report (recommended 8–15 pages)"* covering: use case and business requirements; architecture decision Lambda vs Kappa **with explicit justification and rejected alternative**; architecture diagrams covering ingestion, processing, storage and serving; technology stack with justification per component; observability design — *"what is measured, how, and why"*; results with screenshots; limitations, trade-offs, and what you would do differently at production scale.
>
> **Rubric weight: 15 marks** — *"clarity of architecture diagrams, explanation of design decisions, tech stack rationale, and presentation of results; **honesty about limitations**."*
>
> The report carries 15 marks directly but *describes* the sections worth another 40 (architecture 20 + stack 10 + observability 10). **A weak report loses marks twice.**

---

## 1. Target: 13 pages of body

| § | Section | Pages | Sources |
|---|---|---|---|
| 1 | Introduction & Use Case | 0.75 | `00 §1` |
| 2 | Business Requirements Interpretation | 0.75 | `01 §1` |
| 3 | **Architecture Decision: Kappa vs Lambda** | **3.0** | `01` |
| 4 | Architecture Design & Diagrams | 2.0 | `00 §3`, `06 §2` |
| 5 | Technology Stack Justification | 1.5 | `02` |
| 6 | Implementation | 2.5 | `03`–`08` |
| 7 | Observability Design | 1.5 | `09` |
| 8 | Results | 1.5 | `13` |
| 9 | Limitations, Trade-offs & Production Scale | 1.0 | §5 below |
| 10 | Conclusion | 0.4 | |
| — | References + Appendices | (excluded) | |

---

## 2. Section-by-section notes

### §1 Introduction & Use Case (0.75 p)
Scenario in two paragraphs. Restate the business question verbatim, then **immediately make the observation that both halves ask for one quantity — a patient's current risk** (`01 §1`). That sets up §3 and signals from page 1 that the architecture was requirement-driven.

**Put the safety disclaimer here, prominently**, not buried in an appendix: simulated data, NEWS2 implemented for realism, not a medical device, not clinically validated.

Name the stakeholders (ward nursing staff; the clinical governance lead) and include the group members. Individual contributions statement in an appendix, but it **must** exist.

### §2 Business Requirements Interpretation (0.75 p)
A requirements table with measurable non-functional SLOs:

| ID | Requirement | Type | Target | Satisfied by |
|---|---|---|---|---|
| FR-1 | Continuous risk score per patient | Functional | — | Stream → `ward_risk_snapshot` |
| FR-2 | Threshold alerts per patient | Functional | NEWS2 ≥ 5 / ≥ 7 | `vitals.alerts.v1` |
| FR-3 | Daily risk report joining vitals trends with latest labs | Functional | 1/simulated day | Airflow → PDF |
| FR-4 | Re-derive history under a revised clinical rule | Functional | ≤ 30 sim-days | Replay from offset 0 |
| NFR-1 | Deterioration detection latency | Latency | < 30 s real | 5 s micro-batch |
| NFR-2 | One consistent risk value across all surfaces | **Consistency** | Exactly one | Single processing path |
| NFR-3 | Score explainability | Auditability | Subscores + lab decomposition | Response model |
| NFR-4 | Pipeline-silence detection | Observability | < 3 min | `WardMonitoringSilent` |
| NFR-5 | Reproducibility | Ops | 1 command | Docker Compose |

**NFR-2 is the one to highlight** — "exactly one value" is an unusual non-functional requirement, and it is what selects the architecture.

### §3 Architecture Decision (3.0 p) — the big one
Structure exactly as `01 §6`. Non-negotiables:
- Sub-headings named after **the module's five criteria**.
- The 8-criterion summary table.
- **Both concessions stated as concessions** (historical analysis, fault tolerance) with concrete mitigations.
- A full, fair statement of the Lambda case *before* rejecting it.
- **§3.6: reconciling the module's own contradiction** on Kappa's cost/complexity (`01 §2.5`). Short, and almost no other report will do it.
- The falsification conditions (`01 §4.3`).

### §4 Architecture Design & Diagrams (2.0 p)

Four diagrams, **drawn** (draw.io / Mermaid), exported as SVG or PDF — never raster.

| Diagram | Tool | Shows |
|---|---|---|
| **D1 — Context** | draw.io | Actors (bedside monitors, pathology lab, ward staff, governance lead) and the system boundary |
| **D2 — The single path** ★ | draw.io | The full dataflow from `00 §3`, with the log as backbone and **one** arrow into the scoring box. Half a page. |
| **D3 — Event sequence** | Mermaid | One reading: monitor → Kafka → micro-batch → score → Cassandra → API → ward screen, with `trace_id` carried through |
| **D4 — Replay** ★ | draw.io | The two-consumer-group diagram from `06 §2`, emphasising that **both branches are the same file** |

Optional D5: the simulated-clock timeline. Use one colour convention across all four with a legend.

### §5 Technology Stack (1.5 p)
The summary table from `02 §8`, keeping the **"Trade-off accepted"** column. Then short paragraphs expanding the four choices that most need defending: **Spark Structured Streaming over Storm**, **Cassandra over TimescaleDB** (the strongest and most distinctive argument — its weaknesses are Kappa's upstream responsibilities), **Kafka retention as the historical store**, and **Avro with a real evolution case**.

Add the honesty paragraph: Kafka, Spark, Airflow and Cassandra were covered in the module; Prometheus, Grafana, Alertmanager and OpenTelemetry were not, and are introduced as deliberate extensions.

### §6 Implementation (2.5 p)
- **6.1 Simulated sources and the simulated clock** — the declaration block from `03 §7` verbatim, in a call-out box. Include the scope-adaptation justification (`respiratory_rate` and `consciousness` added because NEWS2 requires them).
- **6.2 Ingestion** — the topic table from `04 §1`; the `patient_id` partition-key argument; **the two genuine compaction cases**; retention as an architectural decision with the storage arithmetic.
- **6.3 Processing** — the single-path diagram; impossibility-not-outlier cleaning; window choice and why tumbling was rejected; **NEWS2 as the one clinical rule**; the lab-join mechanism decision; the risk decomposition as the literal answer to the business question.
- **6.4 Storage** — the query catalogue; the two clever modelling choices (`risk_score` in the clustering key, `sim_date` in the partition key); **TTL instead of tombstones and the safety argument**.
- **6.5 Serving & reprocessing** — endpoint table; the self-describing response with `scorer_version`; and **§6.5.1: the replay** (`06`), which deserves its own half-page.

### §7 Observability (1.5 p)
**Lead with "silence is not safety"** (`09 §1`) and the three design decisions it drives across alerting, the data model and the business logic. Then: the log schema; the metric catalogue by stage; the **pipeline-health vs clinical alerts** split; the tracing approach with its stated limitation; the dashboard list; the `make chaos` verification table with measured detection times.

### §8 Results (1.5 p) — see §3 below.

### §9 Limitations & Production Scale (1.0 p) — see §5 below. **Do not shorten this.**

### §10 Conclusion (0.4 p)
What was built, whether it answers the business question, and the single most important thing learned.

---

## 3. Results — the screenshot plan

Captured during the day-11 dry run at 1920×1080, cropped, with consistent simulated dates.

| # | Screenshot | Proves |
|---|---|---|
| R1 | Grafana **Ward Monitor** — 40-bed grid, `P014` red | The live half of the business question |
| R2 | `P014`'s NEWS2 trend rising 1 → 5 → 7 over 4 simulated hours | Windowed trend detection works |
| R3 | Alert feed showing the escalation `LOW → MEDIUM → HIGH` for `P014` | Threshold alerts, deduplicated and escalating |
| R4 | `/api/v1/patients/P014` showing the **risk decomposition** and `explanation` | **The lab half of the business question, answered and explainable** |
| R5 | Page of the **daily risk report PDF**, especially the lab-corroboration table | The consolidated report deliverable |
| R6 | **Airflow grid** — green simulated-day columns | Orchestration on schedule |
| R7 | **Airflow graph** — the missing-lab-file branch taken, ward still monitored | Branching + honest degradation |
| R8 | **Kafka UI** — topics with cleanup policies (compact vs delete) visible | Compaction understood, not accidental |
| R9 | **★ Kafka UI during replay** — two consumer groups at very different offsets | *"Replaying large chunks"*, made visible |
| R10 | **Replay Comparison dashboard** — v1 vs v2 diverging for COPD patients only | Reprocessing verified, not assumed |
| R11 | The replay **diff summary** with the zero-change control group | The replay is correct |
| R12 | API before/after `make cutover` — `P031` `MEDIUM → LOW`, `scorer_version` changes | Atomic, reversible cutover |
| R13 | **Ward Monitor empty** after `chaos-kill-monitors` + `WardMonitoringSilent` firing | **"Silence is not safety", demonstrated** |
| R14 | Grafana **Pipeline Health** during the same incident — the cliff, localised to a stage | Observability locates failures |
| R15 | **Jaeger** waterfall: monitor → micro-batch → Cassandra → API | Tracing across stages |
| R16 | **Spark UI** Structured Streaming tab | Stream health |
| R17 | `make test` output with coverage, and the `archrules` job passing | Testing + architectural enforcement |

**Measured numbers table** — reports that quantify score better:

| Metric | Observed |
|---|---|
| Sustained ingestion rate | ~13 readings/s |
| End-to-end latency (monitor → ward screen), p50 / p95 | _measure_ |
| Micro-batch duration, mean / p95 | _measure_ |
| **Replay duration for 30 simulated days (~115k events)** | _measure — the headline Kappa number_ |
| Scores changed by v1→v2 | _measure (expect ~3.3%)_ |
| False alerts removed by v2 | _measure (expect ~180)_ |
| Alert detection time (kill monitors → alert firing) | _measure_ |
| Time for the ward monitor to empty after stream death | _measure (expect ~120 s = the TTL)_ |
| Cold start to first scored patient | _measure_ |

---

## 4. Diagram production

- **draw.io** for D1, D2, D4; `.drawio` sources committed to `docs/diagrams/`.
- **Mermaid** for D3, rendered to SVG with `mmdc`.
- Export to **PDF or SVG**, never PNG.
- One palette + legend across all diagrams.

---

## 5. Limitations — write these honestly

The rubric rewards honesty here, and every item is something a viva could otherwise catch you on.

**Demo-scale simplifications**
- **Single Kafka broker, RF=1 — and under Kappa this is more serious than under Lambda**, because the log is the only copy of the record. Say this explicitly.
- **Single-node Cassandra defeats Cassandra's own purpose** — no replication, no tunable consistency, no failover.
- Kafka retention set to 3 real hours (= 30 simulated days) for laptop constraints.
- One 40-bed ward; a single Spark master with one or two local workers.
- No authentication, TLS or ACLs anywhere; default credentials in `.env.example`.

**Correctness and semantics**
- **End-to-end exactly-once is not achieved.** At-least-once with idempotent sinks (`04 §5`).
- Readings later than the 60-simulated-minute watermark are excluded from the window they belong to — though they are **side-processed to `vitals.late`, not dropped**, and reprocessed on the next replay.
- **The bitemporal enrichment problem** (`06 §7.4`): replay enriches historical readings with *current* admission and lab data. Correct for a corrected flag, wrong for a bed transfer. Properly solved with a bitemporal model, which we did not build. **This is the most sophisticated honest limitation in the project — do not omit it.**
- Changing the streaming aggregation invalidates checkpoints and requires a reset.
- Vitals are sampled at 15 simulated minutes, not continuous waveform capture; real deterioration detection often uses higher-frequency data.

**Clinical**
- **NEWS2 is implemented from its published specification but is not clinically validated in this implementation. This is not a medical device.** Repeat the disclaimer here.
- The lab-modifier rules are a plausible construction, **not** a published composite score. Unlike NEWS2, they have no external authority — say so plainly rather than letting them borrow NEWS2's credibility.
- Simulated physiology is a mean-reverting AR(1) model with hand-tuned coupling; real vitals are far messier, and a model trained or validated on real data would behave differently.

**Observability**
- **Per-event distributed tracing is not achieved** — micro-batch granularity with sampled links (`09 §5.2`).
- Pushgateway retains last values; mitigated by alerting on progress-timestamp age.
- Loki is optional and may not be in the final build.

**What we would do differently at production scale**
- Kafka: RF=3, `min.insync.replicas=2`, multi-AZ, tiered storage for a longer replay horizon; mTLS + SASL + ACLs; MQTT bridge at the device edge.
- Cassandra: 3+ nodes, RF=3, `LOCAL_QUORUM`, proper compaction strategy tuning (TWCS for the time-series tables).
- Processing: Spark on Kubernetes; RocksDB state backend; **a separate cluster for replay jobs** so reprocessing cannot starve live monitoring.
- Reprocessing: **bitemporal admissions and labs** so replay reconstructs the enrichment context as it was, not as it is.
- Serving: horizontally-scaled API; read replicas; caching for the ward view.
- Observability: OTel everywhere with Tempo; **SLOs with error budgets** rather than static thresholds; genuine on-call routing for `WardMonitoringSilent`.
- Governance (the module's own pillars): **this is health data — HIPAA/GDPR apply.** Encryption at rest and in transit, access control, audit trails on every read of a patient record, a data catalogue with lineage, and defined retention/deletion honouring patient rights. Our simulation has no PII, but a real deployment's governance burden would be substantial and would likely dominate the engineering effort.
- Clinical governance: formal validation of the scoring implementation against a reference dataset, change control on scorer versions, and sign-off workflow — of which our manual replay gate is a small prototype.

---

## 6. Production tooling

Write the source in **Markdown** in `docs/report/`, then produce the PDF with the **`professional-latex-pdf-engine`** skill (`pdflatex` and `lualatex` are installed on the host). Keep diagrams as external SVG/PDF assets.

**Timeline:** diagrams day 12 (slower than expected), prose days 12–13, final PDF day 13. Do not leave it to the last day.

---

## 7. Final checklist

- [ ] 8–15 pages of body (target 13)
- [ ] Every PDF-mandated section present
- [ ] **Safety disclaimer prominent in §1**
- [ ] Simulated clock stated clearly, in a call-out
- [ ] Scope adaptation (added NEWS2 fields) justified
- [ ] Rejected alternative (Lambda) argued fairly before rejection
- [ ] The module's cost/complexity contradiction reconciled (§3.6)
- [ ] All four diagrams vector, legible, consistently styled, legend included
- [ ] Technology table has a per-component justification tied to *this* ward
- [ ] Observability section leads with "silence is not safety" and answers what/how/why
- [ ] **The replay has its own subsection with real measured numbers**
- [ ] ≥ 15 screenshots, legible at print size
- [ ] Measured-numbers table filled with real values
- [ ] Limitations genuinely honest — including the bitemporal problem and the unvalidated lab rules
- [ ] Individual contributions statement included
- [ ] References: module lecture decks, the NEWS2 specification, Kafka/Spark/Airflow/Cassandra docs
- [ ] Proofread; no placeholders

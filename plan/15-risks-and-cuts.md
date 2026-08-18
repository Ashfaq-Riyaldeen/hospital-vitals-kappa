# 15 — Risks & What to Cut

---

## 1. Risk register

Ordered by expected cost (probability × impact).

| # | Risk | Prob | Impact | Mitigation | Trigger to act |
|---|---|---|---|---|---|
| R1 | **Cassandra instability on WSL2** — slow start, heap grab, OOM | High | High | `MAX_HEAP_SIZE=1G`, `HEAP_NEWSIZE=256M`, `start_period: 90s`, `mem_limit: 2g` — all configured on **day 3** | Still unstable after 3 h → take the **TimescaleDB fallback** (§3) and honestly rewrite the storage justification |
| R2 | **Watermark / simulated-time bugs** — empty windows, non-obvious cause | High | High | Watermark in simulated time with the arithmetic commented; a startup validator rejects unsafe combinations; a unit test fails CI | >1 h debugging empty windows → temporarily drop the speed-up to 96× to widen real-time tolerance, confirm the logic, then restore |
| R3 | **Memory exhaustion** — 13.8 G nominal against a 12 G WSL cap | High | High | Default `make up` runs **one** Spark worker; Loki off; explicit `mem_limit` everywhere; **only one project's stack up at a time** | Swap thrashing → `make down` the other project; drop to `make up-lite` |
| R4 | **The replay doesn't work cleanly** — the single most valuable feature | Medium | **Very High** | Built on **day 8**, not day 12; `test_replay_expected_diff.py` asserts the COPD-only result; versioned primary keys make it non-destructive and re-runnable | Not working by end of day 8 → **stop and fix it.** Cut observability II instead. This is the architecture argument. |
| R5 | **Two projects, one person, two weeks** | Medium | High | Shared foundations written once on day 1 | Behind by >1 day at day 7 → freeze project A's observability at "lean" and finish B's replay first (it carries more argumentative weight) |
| R6 | **Spark → Cassandra write path** — connector version hell | Medium | Medium | **Avoid the `spark-cassandra-connector` entirely**; write via the pure-Python driver inside `foreachBatch`. Simpler, defensible line by line, no version matrix. | If the Python driver is slow at scale → batch with `execute_concurrent` (already planned) |
| R7 | **OpenTelemetry through Spark** | Medium | Medium | Scope already limited to batch-granularity spans with sampled links, and the limitation is a *stated report finding* | Not working by end of day 9 → ship tracing on producers + API + Airflow only; expand the limitation paragraph |
| R8 | **Airflow start-up** — DB init, connection URIs, DAG import errors | Medium | Medium | `airflow db migrate` in an init container; `test_dag_integrity.py` catches import errors before the UI does | >2 h lost → `airflow standalone` in one container; declare the simplification |
| R9 | **NEWS2 implementation subtly wrong** | Low | High | Implemented from the published specification, with **the published scoring table as test fixtures** and boundary tests on every threshold | Any test failure blocks — the clinical rule is the one thing that must be right |
| R10 | **Avro / Schema Registry friction** | Low | Medium | Registered by the init container; Kafka UI wired to the registry | >3 h lost → JSON + Pydantic contracts; document what was given up (note: this costs the *real* schema-evolution demo, which is a genuine loss here) |
| R11 | **Demo narrative doesn't fire on cue** | Low | High | Everything seeded; `test_deterioration_e2e.py` asserts it in CI; rehearsed day 11 | Timing drift at rehearsal → adjust scripted trigger times in config, not code |
| R12 | **Report left too late** | Low | **Very High** | Diagrams day 12, prose 12–13, §3 first | Behind at day 12 → write §3, §5, §7, §9 (the graded-content sections) and compress §6 |
| R13 | Laptop sleep breaks the simulated clock | Medium | Low | `SimClockDrift` alert; `make restart-sim` | — |

---

## 2. Cut order

Cut **in this order**. Everything above the line can go without materially affecting the grade.

| Order | Cut | Marks at risk | Why it's safe |
|---|---|---|---|
| 1 | **Loki / log aggregation** | ~0 | Structured JSON logging is the requirement; `docker compose logs` satisfies it |
| 2 | **Grafana dashboards 4** (Traces & Logs) | ~0.5 | Ward Monitor + Pipeline Health + Replay Comparison already evidence observability |
| 3 | **OTel reduced** to producers + API + Airflow (drop the Spark micro-batch span) | ~1–2 | The limitation is already a stated finding; reduce scope and expand the honesty |
| 4 | **`ward_retention` DAG** | ~0.5 | Cassandra TTL already enforces retention; the DAG only verifies it |
| 5 | **Avro + Schema Registry** → JSON + Pydantic | ~1.5 | Costs the real schema-evolution demonstration, which is a genuine loss here — cut later than in the sibling project |
| 6 | **`P007` (transient-spike control) narrative** | ~0.5 | A nice negative result; not required |
| 7 | **Alert acknowledgement endpoint** | ~0.5 | The one write endpoint; read-only serving is sufficient |
| 8 | **Ward size 40 → 20** | ~0 | Pure resource lever; update the stated numbers everywhere |
| ══ | **════ DO NOT CUT BELOW THIS LINE ════** | | |
| — | The Kappa-vs-Lambda argument with the rejected alternative | **20** | Largest single criterion |
| — | **`news2.py` as the single clinical rule + `test_only_one_scorer_exists`** | ~6 | The architecture argument, in code |
| — | **The replay** (`06`) | ~6 | Kappa's entire justification. Without it this is a streaming job, not a Kappa system. |
| — | Both simulated sources with the stated clock | **15** | Explicit requirement |
| — | The single streaming job with real windowing and watermarks | **15** | Explicit requirement |
| — | **The lab join** and the risk decomposition | **15** | The "join between the two sources" the PDF names, and the literal business answer |
| — | The daily consolidated risk report file | ~5 | Explicit deliverable |
| — | **`WardMonitoringSilent`** + ≥1 alert that visibly fires | **10** | Explicit requirement, and the domain's defining observability point |
| — | Structured logging across all stages | **10** | Explicit requirement |
| — | Docker Compose working from a fresh clone | **5** | Explicit rubric line |
| — | The report | **15** | And it describes 40 more marks |
| — | The demo video | Required | Non-submission risk |

---

## 3. Fallback positions, decided in advance

| If this fails | Fall back to | Report says |
|---|---|---|
| **Cassandra** | **TimescaleDB** (PostgreSQL + hypertables) | Rewrite the storage justification around hypertables, native time partitioning and retention policies — still strong and honest. **But then explicitly forbid continuous aggregates**, because they would create a second place where derived clinical values are computed, undermining the single-rule argument. Say that in the report; it turns a fallback into a demonstrated understanding. |
| Spark → Cassandra connector | Pure-Python driver in `foreachBatch` | Nothing — this is already the plan (R6) |
| `flatMapGroupsWithState` (if used for rapid-deterioration detection) | Compute the delta from the previous window's stored score in `foreachBatch` | "Deterioration velocity is computed by comparing against the previously persisted score rather than in Spark state; simpler and adequate at this scale." |
| Avro | JSON + Pydantic | "Schema Registry was scoped out; contracts are enforced by Pydantic models with a versioned schema field." Note the lost evolution demo. |
| Airflow full deployment | `airflow standalone` | "Single-process Airflow for the demo; LocalExecutor with a separate metadata database is the intended configuration." |
| Grafana Cassandra plugin | **Infinity/JSON datasource → our FastAPI** | Actually **better** — the dashboard exercises the serving layer rather than bypassing it. This is already the recommended default (`07 §7`). |
| Replay as a fully-automated DAG | **Manual `make replay` + `make cutover`** | Keep the capability, drop the orchestration. **Never cut the replay itself** — cut its automation. |

---

## 4. Checkpoints

**End of day 5 — go/no-go on scope.** If the streaming job is not writing scored rows to the serving store by end of day 5, execute cuts 1–3 immediately and reassess at day 7.

**End of day 8 — the replay must work.** This is the hard gate specific to this project. A Kappa submission without a demonstrated replay has not demonstrated Kappa. If it is not working, stop everything else.

**End of day 9 — feature freeze.** Days 10–14 are testing, screenshots, report and video only. Adding a feature on day 11 is how projects arrive with great code and no report.

**End of day 11 — screenshot freeze.** All results captured, measured-numbers tables filled.

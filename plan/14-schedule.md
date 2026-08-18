# 14 — Two-Week Schedule

> Assumes **both projects are built in parallel** (worst case: one person builds both). Where a day says "A" and "B", the work genuinely differs; the day-1 foundations are written once and adapted into each repo.
>
> This is the same master schedule as `../ride-hailing-lambda/plan/14-schedule.md`, annotated from this project's side.

---

## Day 0 — Prerequisites (½ day)

- [ ] **Enable Docker Desktop WSL integration** (`10 §0`); verify `docker run hello-world`
- [ ] `.wslconfig` with `memory=11GB`, `swap=8GB`; `wsl --shutdown` (host is 15.6 GB — see `10 §0.2`)
- [ ] `docker pull` every image in both compose files (~8 GB) — do it once, not mid-build
- [ ] `git init` both repos, `.gitignore`, initial commit
- [ ] Scaffold both directory trees
- [ ] Confirm `uv`, Python 3.11/3.12, `pdflatex`

**Milestone:** images cached, Docker working in WSL.

---

## Day 1 — Shared foundations

Written once, adapted into both repos (separate submissions, so code is copied and adjusted rather than shared as a package).

- `simclock.py`, `settings.py` (with **both validators** — watermark safety and snapshot TTL), `log.py` (structlog JSON), `metrics.py`, `tracing.py`, `kafka_client.py`, Avro serialization
- Avro schemas for both domains
- Compose skeleton: Kafka (KRaft) + Schema Registry + Kafka UI healthy in **both** stacks on their distinct port ranges
- `init` container: topics **with cleanup policies**, schema registration, clock epoch

**Milestone:** `make up` brings up Kafka in each repo; Kafka UI shows the topics with correct cleanup policies.
**Risk:** the KRaft `CLUSTER_ID` dance — budget an hour.

---

## Day 2 — Producers

- **B:** `bedside_monitor.py` — per-patient baselines, AR(1) walk with physiological coupling, circadian drift, sensor artefacts, and the scripted narratives (`P014` sepsis, `P031` COPD, `P007` transient spike); `lab_uploader.py` with atomic drop and late/missing/malformed scenarios; `admissions.py`
- **A:** telemetry producer + expense dropper
- Both: `/metrics`, `/health`, structured logs, graceful shutdown, retry/backoff, idempotent producer config

**Milestone:** readings visible in Kafka UI; producer metrics in Prometheus.
**B-specific:** get `test_deterioration_script.py` passing today — it asserts `P014` reaches NEWS2 ≥ 7, and everything downstream depends on that narrative being right.

---

## Day 3 — Storage

- **B:** Cassandra container **tuned** (`MAX_HEAP_SIZE`, `start_period: 90s`); keyspace + all seven tables; DAO with prepared statements; verify **every catalogued query runs without `ALLOW FILTERING`**
- **A:** MinIO buckets, Postgres star schema, Redis
- Both: `make db-init` idempotent from scratch

**Milestone:** `make clean && make up` recreates all schemas with no manual steps.
**B-specific risk:** Cassandra start-up and heap sizing. **Get this right today, not on demo day.** If it is still unstable after 3 hours, take the TimescaleDB fallback (`15 §3`) — it is a defensible choice and the storage justification can be honestly rewritten.

---

## Days 4–5 — ★ CRITICAL PATH: Spark Structured Streaming ★

Everything downstream depends on this.

- **B:** the single pipeline — clean → validate → dedupe → enrich (admissions broadcast) → watermark → 4-simulated-hour sliding window → **NEWS2 scoring** → lab join (compacted broadcast) → alerts → `foreachBatch` into five Cassandra tables
- **A:** three queries (raw→Parquet, aggregates→Redis, stateful idle detection)
- Both: write clinical/transform logic as **pure functions from the start**. Do not inline it in the streaming job and promise to extract it later — the single-scorer argument depends on it existing separately.

**Milestone:** a reading produced at one end appears as a scored row in Cassandra at the other.
**Expect to lose half a day** to watermark-vs-simulated-time bugs. When windows come back empty, check the watermark arithmetic first (`03 §3.3`).
**B-specific:** write `news2.py` and its published-table test **before** wiring it into Spark. It is pure Python and can be finished and verified in an hour with no infrastructure.

---

## Day 6 — Orchestration

- **B:** `ward_lab_ingest` (sensor → validate → branch → publish → verify), `ward_daily_report` (read Q6 → render PDF), `ward_healthcheck`, `ward_retention`; the report template
- **A:** the Spark batch profitability job, `fleet_daily_reconciliation`, `fleet_restatement_watcher`, the report renderer

**Milestone:** a daily report PDF is produced automatically in both projects.
**B-specific:** `test_dag_purity.py` today — it is cheap and it locks in the architecture.

---

## Day 7 — Serving layer

- **B:** all ward endpoints, patient detail with the **risk decomposition and `explanation`**, risk history with `scorer_version`, alert feed, `/api/v1/pipeline/status`
- **A:** FastAPI with `merge.py`

**Milestone:** `/docs` works; every endpoint returns real data.

---

## Day 8 — ★ B-specific: the replay ★ + observability I

**This is the day project B pulls ahead in argument value.** The replay is Kappa's justification.

- **B:** `replay_runner.py`, `compare_versions.py`, the `ward_replay` DAG with its approval gate, `make replay` / `make replay-status` / `make cutover`. Verify `test_replay_expected_diff.py` — **only COPD patients change, always downward, zero change elsewhere.**
- Both: Prometheus scrape configs, exporters, `StreamingQueryListener` → Pushgateway, alert rules, Alertmanager routing
- Both: **verify alerts actually fire** — run every `make chaos-*` and record detection times

**Milestone (B):** `make replay VERSION=v2` completes and produces the expected diff.
**Milestone (both):** an alert visibly fires and resolves.

---

## Day 9 — Observability II

- 4 Grafana dashboards per project, **provisioned as code** (B: Ward Monitor, Pipeline Health, **Replay Comparison**, Traces & Logs)
- OTel Collector + Jaeger; producer/API/Airflow instrumentation; the micro-batch span with sampled links
- Loki + Promtail **only if ahead**

**Milestone:** dashboards appear automatically on a fresh `make up`; one end-to-end trace in Jaeger.

---

## Day 10 — Testing, hardening, READMEs

- Unit tests to the coverage gates (B: **90% on `ward/clinical/`**, 85% on `simclock`)
- The `archrules` CI job: `test_only_one_scorer_exists`, `test_dag_purity`, `test_no_allow_filtering`, `test_clinical_is_pure`
- Integration smoke tests
- **`make clean && make up && make demo` from a fresh clone, twice**
- Both READMEs (B: **safety disclaimer prominent**)

**Milestone:** fresh clone works end to end with no manual steps.

---

## Day 11 — Dry run and screenshot capture

Run in **three passes**, dropping a profile level between each so memory never binds.

**Pass 1 — `make up-obs` (the main session, ~2 h)**

- Run each stack for a full multi-simulated-day session
- Verify every scripted narrative fires on cue — **`P014` deterioration, lab corroboration, `P031` under v1 vs v2, `P007` not alerting, the day-5 missing lab file**
- **Run the full replay and capture R9–R12** — the highest-value screenshots in this project
- Capture R1–R10, R12–R14, R16–R17 (`12 §3`)
- Fill in the measured-numbers table, especially **the replay duration** — the headline Kappa figure

**Pass 2 — `make chaos-*` (still on `up-obs`)**

- Run every chaos scenario; capture **R13: the ward monitor emptying** as TTLs expire with `WardMonitoringSilent` firing. This is the project's signature observability screenshot.
- Record actual detection times, and the observed time for the monitor to empty (expect ≈120 s = the TTL)

**Pass 3 — ★ `make up-full`, one stack only, nothing else running (~30 min) ★**

This pass exists because Jaeger/OTel and the Spark master/worker pair only run in the `full`
profile (`10 §1.3`). **It is not optional** — skipping it forfeits observability marks and the
one screenshot that shows distributed execution.

- [ ] `make down` the *other* project first, and close anything else consuming RAM
- [ ] **R15 — the Jaeger trace waterfall** (monitor → micro-batch → Cassandra → API). The only
      evidence for the rubric's "tracing" requirement; without it that part of the 10-mark
      observability criterion is unevidenced.
- [ ] **Spark UI with the real cluster** — the Executors tab showing tasks across two workers.
      Pair it with the local-mode screenshot so the report can show both and explain the choice.
- [ ] **Re-run the replay once on the cluster** and record the duration alongside the local-mode
      figure. Two numbers for the same replay is a genuinely good result to publish — it shows
      the reprocessing claim holds under both deployments.
- [ ] Note the observed `up-full` memory figure for `10 §1.3` — the profile numbers are estimates
      from image defaults and configured limits, and this is where they get validated
- [ ] `make down` immediately afterwards; drop back to `up-obs`

Then: rehearse both demo scripts with a timer.

**Milestone:** every screenshot captured **including R15 and the cluster view**; the replay timed under both deployments; both demos rehearsed and timed.

---

## Days 12–13 — Reports

- **Day 12: diagrams first** (they take longer than expected). D1–D4 per project.
- **Day 12–13: prose.** §3 (architecture, 20 marks) first while fresh, then §5, §7, §9, then the rest.
- **Day 13: produce the PDFs**; proofread both; check page counts.

---

## Days 13–14 — Video, polish, submit

- Record both demo videos (~9.5 min each)
- Tag `v1.0-submission`; verify `.env` is not committed
- Individual contribution statements
- Assemble and submit both bundles

---

## Critical path

```
Day 0  prereqs
Day 1  foundations ──┐
Day 2  producers ────┤
Day 3  storage ──────┤   ← B: Cassandra tuning. Do not defer.
Day 4  ★ STREAMING ★ │
Day 5  ★ STREAMING ★ ┘
Day 6  orchestration
Day 7  serving
Day 8  ★ REPLAY (B) ★ + observability I
Day 9  observability II   ← first real slack
Day 10 testing            ← can compress to ½ day
Day 11 dry run            ← CANNOT compress; screenshots are report inputs
Day 12 diagrams + report
Day 13 report + video     ← CANNOT compress
Day 14 submit
```

**If days 4–5 overrun, cut from day 9 (observability II), never from day 8 (the replay), day 11 or day 13.** The replay is the architecture argument; screenshots and the report are graded artefacts; a fourth Grafana dashboard is not.

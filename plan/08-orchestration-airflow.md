# 08 — Orchestration with Apache Airflow

> **PDF preferred stack:** *"Orchestration: Apache Airflow (for managing batch jobs or reporting pipelines)."*
>
> **The governing principle:** *"**Airflow only orchestrates.** NOT a data processing engine, NOT a database, NOT a distributed computing framework."*
>
> **The Kappa-specific principle:** Airflow orchestrates **around** the stream, never as a second processing path. If a DAG ever computes a clinical value, the architecture argument in `01` collapses.

---

## 1. Why an orchestrator exists in a Kappa system at all

The obvious objection — *"Kappa eliminates the batch layer, so what is Airflow doing here?"* — has a clean answer, and the report should make it before the examiner asks (`01 §5`).

**Orchestration is not processing.** Three things in this system are episodic, need scheduling, retries and failure alerting, and are not part of the stream:

1. **A file arrives once per simulated day.** Sensing it, validating it, publishing it to Kafka and alerting when it does not arrive is orchestration work. Note that the *publishing* step copies rows verbatim — it contains no logic.
2. **A replay is a multi-step operation with a human gate.** Launch, monitor, diff, wait for sign-off, cut over. That is a workflow.
3. **A report must be rendered on a schedule.** It reads pre-computed rows and renders them.

None of these computes a risk score. The module's own words cover it exactly: *"Airflow only orchestrates."*

---

## 2. DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `ward_lab_ingest` | `*/5 * * * *` (real) = **once per simulated day** | Sense → validate → **produce to Kafka**. No transformation. |
| `ward_daily_report` | `*/5 * * * *`, offset by 90 s | Read `daily_patient_summary` → render PDF. **No recomputation.** |
| `ward_replay` | Manual trigger only | Coordinate a scorer-version replay (`06 §4`) |
| `ward_healthcheck` | `*/2 * * * *` | Independent liveness probe (§6) |
| `ward_retention` | `0 * * * *` (real, hourly) | Verify TTL expiry is working; report storage growth |

### 2.1 The real-cron-to-simulated-day mapping

Airflow schedules in **real** time; our clinical day is **simulated**. One simulated day = 5 real minutes, so a real 5-minute cron *is* a daily schedule in the simulation.

Each DAG resolves its target `sim_date` from `ward.simclock` — the same module the producers and the Spark job use — and **not** from Airflow's `logical_date`. Using `logical_date` would bind the DAG to real time and it would target the wrong simulated day on every run.

`catchup=False`, `max_active_runs=1`. This mapping is stated in the README and asserted by `test_sim_date_resolution.py`.

---

## 3. `ward_lab_ingest` — task graph

```
        resolve_sim_date
               │
               ▼
      wait_for_lab_file            (FileSensor, mode=reschedule, timeout 90s)
               │
        ┌──────┴───────┐
     found          timed out
        │                │
        ▼                ▼
  verify_checksum   handle_missing_lab_file
        │            (alert · mark day incomplete ·
        ▼             set labs_stale flag · ward
  validate_lab_file   CONTINUES on vitals alone)
   ┌────┴────┐
 valid    invalid
   │          │
   │          ▼
   │    quarantine_file (+ alert)
   │          │
   ▼          │
publish_to_kafka        ← produces rows VERBATIM to labs.results.v1
   │                       (compacted, key = patient_id|test_type)
   ▼
verify_published        ← consume back, confirm count matches
   │
   ▼
run_dq_checks
   │
   ▼
notify_success
```

Plus `notify_failure` with `trigger_rule=ONE_FAILED` → Alertmanager webhook.

### Task detail

| Task | Operator | Notes |
|---|---|---|
| `resolve_sim_date` | `@task` (TaskFlow) | From `simclock`; honours a manual `sim_date` param; pushes to XCom |
| `wait_for_lab_file` | `FileSensor` | `mode="reschedule"` (frees the worker slot between pokes — important on a small stack), `poke_interval=10`, `timeout=90`. The module names *"file arrival"* as the canonical sensor use case. |
| `verify_checksum` | `@task` | Validates the `.sha256` sidecar |
| `validate_lab_file` | `@task.branch` | JSON schema, patient IDs exist, `reference_range` parses, values numeric and plausible, no duplicate `(patient_id, test_type)` |
| `quarantine_file` | `@task` | Move to `labs/quarantine/`, write a reason file, alert |
| `handle_missing_lab_file` | `@task` | Alert; set `labs_stale`; **the ward keeps being monitored on vitals alone.** Degrading honestly rather than failing is the correct clinical behaviour. |
| `publish_to_kafka` | `@task` | **The only "data" task, and it contains no logic** — parse, map to Avro, produce with key `patient_id|test_type`. Idempotent by construction: compaction converges on re-publish. |
| `verify_published` | `@task` | Consume the topic back to `latest` and confirm the expected number of distinct keys. **Trust-but-verify on the one place data enters the log from outside the stream.** |
| `run_dq_checks` | `@task` | Coverage (what % of admitted patients got labs), out-of-range rate, value-range sanity → Cassandra + Pushgateway |

### Airflow features used — all explicitly taught

| Taught concept | Where |
|---|---|
| DAG / DAG Run | Five DAGs, one run per simulated day |
| Operators | `FileSensor`, `TriggerDagRunOperator`, `EmptyOperator`, `BashOperator` |
| **Sensors** | `wait_for_lab_file` — the module's own "file arrival" example |
| **TaskFlow `@task`** | Every Python task; the module names the decorator explicitly |
| Dependencies declared second | Tasks defined first, wired at the bottom — the module's stated style |
| **Retries** | 2 with exponential backoff on Kafka and Cassandra tasks |
| Scheduling | Real cron mapped to simulated days |
| **XCom** | `sim_date`, `job_run_id`, row counts |
| **Branching** | `@task.branch` on validation; the missing-file path |
| **Trigger Rules** | `ONE_FAILED` for the notifier; `NONE_FAILED_MIN_ONE_SUCCESS` after branches |
| **Web UI** | The primary orchestration observability surface |

**Deliberately not used:** *Depends On Past* (a missing lab day must not block subsequent days — the ward continues), and *Latest Only* (replays legitimately target past data).

---

## 4. `ward_replay` — orchestrating a reprocessing run

Manual trigger with `conf={"target_version": "v2", "from_offset": "earliest"}`. Implements `06 §4`.

```
preflight_check          ← retention covers the horizon? record current end offsets
        │
        ▼
launch_replay_job        ← spark-submit with SCORER_VERSION=v2,
        │                   CONSUMER_GROUP=ward-stream-v2-replay,
        │                   startingOffsets=earliest
        ▼
monitor_replay_progress  ← poll consumer-group lag; emit replay_progress_pct;
        │                   fail on stall
        ▼
verify_replay_complete   ← replay group reached the recorded end offsets
        │
        ▼
compute_version_diff     ← ward/replay/compare_versions.py → Cassandra + Prometheus
        │
        ▼
publish_diff_report      ← the comparison artefact
        │
        ▼
    ⏸ AWAIT_HUMAN_APPROVAL  ← a DAG that STOPS here, deliberately
        │
        ▼
cutover_active_version   ← (separate manual trigger) set ACTIVE_SCORER_VERSION,
                            restart API + live job
```

**The DAG deliberately does not cut over automatically**, and the report explains why: changing the clinical scoring rule a ward is monitored by requires human sign-off regardless of how good the diff looks. Encoding a governance gate as a pipeline step — and being able to say it is a clinical requirement rather than a technical limitation — is a genuinely strong design point.

Note that even here, **no scoring happens in Airflow.** It launches a Spark job, watches offsets, and runs a comparison query. `compare_versions.py` reads two sets of already-computed rows and subtracts them.

---

## 5. `ward_daily_report`

```
resolve_sim_date → wait_for_day_complete → read_daily_summary
    → render_html → render_pdf → publish_report → notify
```

`wait_for_day_complete` waits until `sim_now()` has passed the target simulated day plus a watermark grace period, so the stream has finished writing that day's summary rows.

**`read_daily_summary` is a single-partition Cassandra read of Q6.** It performs no aggregation — the stream already did. Emphasise this in the report: *"the daily report and the live ward screen are literally reading the same computed values, which is why they can never disagree about a patient."*

Rendering: Jinja2 → HTML → WeasyPrint → PDF (`07 §6`).

---

## 6. `ward_healthcheck` — orchestrated observability

Every 2 real minutes, deliberately independent of the metrics pipeline, because *"the monitoring failed silently"* is the worst outcome in this domain.

| Check | Alert |
|---|---|
| Readings produced in the last 2 minutes > 0 | `NoVitalsIngested` |
| **Risk scores written for any patient in the last 3 minutes > 0** | **`WardMonitoringSilent` (critical)** |
| Streaming query last-progress age < 120 s | `StreamingQueryStalled` |
| Consumer lag on `ward-stream-v1` < 5,000 | `ConsumerLagGrowing` |
| `ward_risk_snapshot` row count ≈ ward size | `SnapshotIncomplete` |
| Latest lab file age < 2 simulated days | `LabDataStale` |
| API `/health/deep` returns 200 | `ServingLayerUnhealthy` |

Results are pushed as Prometheus gauges; Alertmanager owns the alerting. This DAG only measures. Its own failure is alertable via the DAG-failure callback, so there is no unwatched watcher.

**`WardMonitoringSilent` is the clinically important one** and belongs in the report's observability section with its rationale spelled out: a monitoring system that has stopped looks exactly like a ward where nothing is wrong.

---

## 7. Enforcing "Airflow only orchestrates"

```python
# tests/unit/test_dag_purity.py
FORBIDDEN_IMPORTS = {"pandas", "numpy", "pyspark.sql.functions"}
FORBIDDEN_CALLS   = {"groupby", "merge", "agg", "pivot_table", "score_news2"}

def test_dags_contain_no_processing_logic():
    for dag_file in Path("orchestration/dags").glob("*.py"):
        tree = ast.parse(dag_file.read_text())
        assert not (imports_of(tree) & FORBIDDEN_IMPORTS)
        assert not (calls_in(tree) & FORBIDDEN_CALLS)
```

`score_news2` is in the forbidden-calls list specifically — **a DAG must never compute a clinical score.** This test is the mechanical enforcement of the `01` argument and should be run on screen in the viva.

---

## 8. Configuration

| Item | Approach |
|---|---|
| Executor | `LocalExecutor` — two processes, appropriate for one machine |
| Metadata DB | Its own `postgres:16-alpine`, **separate from anything else**. Mixing Airflow metadata with application data is a real anti-pattern. |
| Connections | `AIRFLOW_CONN_*` URIs from `.env` — version-controlled shape, secret-free values. **No connections created by hand in the UI**, which would break reproducibility. |
| Variables | `sim_day_seconds`, `ward_id`, `active_scorer_version` — set by the init container |
| Shared code | `ward/` mounted and on `PYTHONPATH`, so DAGs import the same `simclock` the producers and Spark use. **One definition of simulated time system-wide.** |
| Logging | `[logging] json_format = True` — Airflow's own logs match the pipeline's structured format |
| Metrics | `[metrics] statsd_on = True` → `statsd-exporter` → Prometheus |

---

## 9. Testing

| Test | Asserts |
|---|---|
| `test_dag_integrity.py` | Every DAG imports, no cycles, defaults set, `catchup=False` |
| **`test_dag_purity.py`** | §7 — no processing logic, and specifically no scoring |
| `test_sim_date_resolution.py` | Resolves from `simclock`, not `logical_date`; honours the manual param |
| `test_lab_branching.py` | Valid → publish; invalid → quarantine; missing → the ward continues with `labs_stale` |
| `test_publish_idempotent.py` | Publishing the same file twice converges to the same compacted state |
| `test_replay_dag_gates.py` | The DAG halts before cutover and does not proceed without the separate trigger |

---

## 10. What the reviewer sees

The Airflow UI at `:8182` is a graded artefact:

- **Grid view** — a column per simulated day, green squares marching across in real time.
- **Graph view** — the branching structure with the taken path highlighted.
- **A deliberately failed run** — the missing-lab-file scenario, showing the red task, the branch to the recovery path, the alert, and **the ward continuing to be monitored**. Showing a failure handled correctly is worth more than showing only green.
- **The replay DAG paused at the approval gate** — a striking screenshot, because a DAG that stops on purpose is unusual and invites the right question.

---

## 11. What the report must show (§6.2 / §7)

- The §1 justification: why an orchestrator belongs in a Kappa system, and where the boundary is.
- The `ward_lab_ingest` task graph, emphasising that `publish_to_kafka` carries no logic.
- The `ward_replay` graph **with the human-approval gate** and its governance rationale.
- The taught-features table (§3) as direct evidence of applying the module's material.
- The real-cron-to-simulated-day mapping and why `logical_date` was not used.
- `test_dag_purity.py` as mechanical enforcement of the architecture.
- Screenshots: grid view; the failed-run branch; the replay DAG at its gate.

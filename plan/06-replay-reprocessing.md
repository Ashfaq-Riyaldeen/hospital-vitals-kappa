# 06 — Reprocessing & Replay

> **This document describes the capability that justifies the entire architecture.**
>
> The module defines Kappa's reprocessing model as *"replaying large chunks"* of the event log, and its comparison table states: Kappa — *"Reprocessing is done by **replaying the stream** in real-time."*
>
> A Kappa project that never demonstrates a replay has not demonstrated Kappa. This is the centrepiece of the demo video and the strongest single piece of evidence for the 20-mark architecture section.

---

## 1. The scenario — real, not contrived

**A clinical scoring rule changes.**

This is not a hypothetical. NEWS was revised to **NEWS2** in 2017, and one of the substantive changes was the introduction of **SpO2 Scale 2** — a separate oxygen-saturation scoring scale for patients with hypercapnic respiratory failure (typically COPD), whose normal baseline saturation is 88–92%. Under the old single scale, those patients score 2–3 on SpO2 permanently, generating constant false alerts and desensitising staff.

When a ward adopts a revised score, the clinical governance question is immediate:

> *"Under the new rule, which of our patients would have been flagged differently? Did we miss anyone? Are we about to stop alerting on someone we were alerting on yesterday?"*

Answering that requires **re-deriving history under the new rule**. That is reprocessing, and it is the operation Kappa is designed for.

### Why this is a better demo than an arbitrary change

- It changes **a small, identifiable, explainable subset** — only the ~6 patients with `copd_scale2 = true`.
- The direction of change is predictable and clinically meaningful (their scores go *down*, correctly).
- Everyone else's scores are **bit-identical**, which is itself the proof that the replay is correct.
- It maps to a real published revision, so it survives the question *"why would anyone do that?"*

---

## 2. How replay works here

```
   vitals.readings.v1   (30 simulated days retained — this is the whole point)
   ├─ offset 0 ─────────────────────────────────────────── latest ─┤
        │                                                      │
        │ consumer group: ward-stream-v2-replay                │ consumer group:
        │ startingOffsets = earliest                           │ ward-stream-v1
        │ SCORER_VERSION = v2                                  │ SCORER_VERSION = v1
        │ (same code, different flag)                          │ (live, untouched)
        ▼                                                      ▼
   ┌──────────────────────────┐                    ┌──────────────────────────┐
   │  ward/stream/pipeline.py │                    │  ward/stream/pipeline.py │
   │  — THE SAME FILE —       │                    │  — THE SAME FILE —       │
   └────────────┬─────────────┘                    └────────────┬─────────────┘
                │                                                │
                ▼                                                ▼
   risk_scores_by_patient                          risk_scores_by_patient
     (patient_id, 'v2', scored_at)                   (patient_id, 'v1', scored_at)
                │                                                │
                └──────────────────┬─────────────────────────────┘
                                   ▼
                    Same Cassandra partition, both versions.
                    One read returns both. Compare, verify, cut over.
```

**The load-bearing detail:** `ward-stream-v2-replay` is not a second implementation. It is the *same job*, the *same file*, started with a different consumer group, a different `SCORER_VERSION` env var, and `startingOffsets=earliest`. There is no forked codebase and no parallel logic.

That distinction is the whole Kappa argument, and the viva will probe it (`01 §5`). The answer is: **two consumer groups is not two processing paths.**

---

## 3. Why versioned output is the design that makes this safe

`scorer_version` is part of the **primary key** of the score tables:

```sql
PRIMARY KEY ((patient_id), scorer_version, scored_at)
```

Consequences, each of which matters:

| Property | Why it matters |
|---|---|
| v1 and v2 rows **coexist** in the same partition | A single Cassandra read returns both versions for a patient — the comparison is a partition scan, not a join |
| The replay **never destroys live data** | If v2 is wrong, delete the v2 rows and nothing is lost. **Reprocessing is reversible.** |
| The API can serve either version | `?scorer_version=v2` for validation while v1 remains the default; cutover is a config change, not a migration |
| Cutover is **atomic and instant** | Change `ACTIVE_SCORER_VERSION`, restart the API. No data movement. |
| Rollback is equally instant | Change it back. |

**The naive alternative — overwriting scores in place — would make the replay irreversible and unverifiable**, which in a clinical context is unacceptable. Contrasting the two designs in the report shows the versioning was a deliberate safety decision rather than an implementation detail.

---

## 4. The replay procedure

Orchestrated by the `ward_replay` DAG (`08 §4`) — Airflow coordinates it, but performs none of the computation.

| Step | Action | Observable |
|---|---|---|
| 1 | **Pre-flight** — confirm retention covers the intended horizon; record the current end offsets | Log line + metric `replay_horizon_offsets` |
| 2 | **Launch** — start `pipeline.py` with `SCORER_VERSION=v2`, `CONSUMER_GROUP=ward-stream-v2-replay`, `startingOffsets=earliest` | New consumer group appears in Kafka UI |
| 3 | **Monitor** — the replay group's lag falls from ~115,000 to 0 | `replay_progress_pct` gauge; Kafka UI offset chart; Grafana panel |
| 4 | **Complete** — replay group reaches the recorded end offsets | `replay_completed_timestamp` |
| 5 | **Diff** — compute the comparison (§5) | `replay_diff_summary` written to Cassandra + Prometheus |
| 6 | **Review** — human inspects the Replay Comparison dashboard | **Gate: no automatic cutover.** A clinical rule change requires sign-off. |
| 7 | **Cut over** — set `ACTIVE_SCORER_VERSION=v2`, restart the API and the live job | API responses carry `scorer_version: v2` |
| 8 | **Retire** — after a soak period, delete v1 rows (or let TTL expire them) | — |

**Step 6 is deliberately manual and the report should say why:** an automated cutover on a clinical scoring rule would be inappropriate regardless of how good the diff looked. Building a gate into the pipeline — and explaining that the gate is a *clinical governance* requirement, not a technical limitation — is the kind of domain-aware design decision the rubric rewards.

---

## 5. The diff — proving the replay is correct

`ward/replay/compare_versions.py` produces:

| Output | Content |
|---|---|
| **Headline** | "115,200 scores re-derived. 3,842 changed (3.3%). 111,358 identical (96.7%)." |
| **By patient** | Which patients changed, how many of their scores, mean delta |
| **By direction** | How many scores rose vs fell |
| **Tier transitions** | A matrix: how many patient-hours moved `HIGH → MEDIUM`, `MEDIUM → LOW`, etc. |
| **Alert impact** | How many alerts would have fired under v2 that did not under v1, **and vice versa** |
| **Unchanged proof** | All non-`copd_scale2` patients: **zero** score differences |

That last row is the important one. **If a single non-COPD patient's score changed, the replay has a bug** — and having a hypothesis about what *should* change is what turns a replay into a verification rather than a leap of faith. Assert it in a test.

Expected result for the demo:

```
Patients with copd_scale2 = true:  6  →  all scores reduced by 2-3 points
Patients with copd_scale2 = false: 34 →  0 changes (bit-identical)
Alerts suppressed under v2:        ~180 false SPO2_CRITICAL alerts on COPD patients
Alerts newly raised under v2:      0
Tier transitions:                  MEDIUM -> LOW: 6 patients, sustained
```

The clinical reading — *"the revised score removes ~180 false alerts on six COPD patients without missing anyone"* — is a genuinely useful finding to present in the report's Results section, and it demonstrates that the pipeline can answer a governance question, not just produce numbers.

---

## 6. Other reprocessing scenarios this enables

Worth listing in the report, because it shows the capability generalises beyond the one demo:

| Scenario | How replay handles it |
|---|---|
| **A bug in the scoring code** | Fix, replay, compare, cut over. Corrupted history is repaired rather than lived with. |
| **A new derived metric is required** (e.g. add a shock-index column) | Add it, replay from offset 0, backfill it across all history. Under Lambda you would backfill in the batch layer *and* separately handle the speed layer's forward path. |
| **A new query shape** — Cassandra needs a new query-first table (`07 §1`) | Replay populates it retroactively. **This is what makes Cassandra's query-first constraint bearable**: the cost of a new table is a replay, and a replay is cheap here. |
| **Late data reconciliation** | Late readings land in `vitals.late` but remain in the main log in offset order, so a replay naturally reprocesses them in place. **One mechanism solves both late data and rule changes** (`04 §6.2`) — under Lambda these are two different mechanisms. |
| **Onboarding a new consumer** | Any new downstream reads from `earliest` and builds its own complete state. |

---

## 7. Honest limitations

State all of these; each is a plausible viva question.

1. **Replay is bounded by retention.** 30 simulated days is our horizon. Beyond it, history is gone — there is no warehouse behind the log. This is the concrete form of the module's *"Kappa may not be as suitable for historical data analysis"*, and it is the trade we accepted in `01 §2.4`.
2. **Replay cost scales with total event volume, not with the size of the change.** Cheap here (115k events, seconds). At a ward network with continuous waveform capture it would be hours, and the calculus changes — which is exactly where we said the architecture stops being right.
3. **Replay competes for resources with the live job.** Both run simultaneously on the same Spark cluster. At our scale it is unnoticeable; in production the replay would run on a separate cluster reading the same topic, and we say so.
4. **Enrichment sources are point-in-time, not versioned.** The compacted `ward.admissions` topic holds *current* admission data. Replaying a 30-day-old reading enriches it with today's admission record. If a patient's `copd_scale2` flag were corrected mid-stay, the replay would apply the corrected value retroactively. **Whether that is right or wrong is a genuine data-modelling question** — for a corrected flag, retroactive application is desirable; for a bed transfer it is not. The correct production answer is a bitemporal admissions model (valid-time and transaction-time). **Naming this as an unsolved modelling problem rather than pretending it does not exist is worth marks.**
5. **The compacted labs topic has the same property** — a corrected lab result supersedes the original, and replay sees only the correction. Usually desirable; occasionally not.
6. **Cassandra storage doubles during a replay** (two versions coexist). Bounded, temporary, and mitigated by TTL on the retired version.

---

## 8. Testing

| Test | Asserts |
|---|---|
| `test_replay_determinism.py` | Replaying the same fixture log twice produces byte-identical scores. **Reprocessing must be deterministic or the whole argument fails.** |
| `test_version_isolation.py` | A v2 replay writes only `scorer_version='v2'` rows; v1 rows are untouched |
| **`test_replay_expected_diff.py`** | On a fixture ward, v1→v2 changes **only** `copd_scale2` patients' scores, and always downward. Non-COPD patients: zero diff. |
| `test_compare_versions.py` | The diff summary counts match a hand-computed fixture |
| `test_cutover.py` | Changing `ACTIVE_SCORER_VERSION` changes API responses; rollback restores them |
| `test_replay_idempotence.py` | Running the replay twice does not duplicate rows (full-primary-key inserts) |

---

## 9. Demo choreography

Two minutes of the video (`13 §1`). Rehearse it — several steps depend on simulated time.

1. Show the live ward dashboard with `P031` (COPD) sitting at `MEDIUM` risk, generating repeated SpO2 alerts. *"This patient's baseline saturation is 89%. That's normal for them. Our v1 scorer doesn't know that."*
2. `make replay VERSION=v2` — one command.
3. **Kafka UI: two consumer groups**, one at latest, one racing up from offset 0. *"Same job, same file, different version flag and a different consumer group. This is what the lecture means by replaying large chunks."*
4. Grafana **Replay Comparison** dashboard: v1 and v2 lines diverging for `P031`, identical for everyone else.
5. The diff summary: *"3.3% of scores changed. All six COPD patients. Zero changes elsewhere. 180 false alerts removed. No alerts lost."*
6. `make cutover VERSION=v2` → the API now returns `scorer_version: v2` → `P031` drops to `LOW`.
7. *"Under Lambda this would have meant reprocessing across two codebases and reconciling the results. Here it's one job, one flag, and a diff we can inspect before committing."*

---

## 10. What the report must show

- The replay diagram from §2, with the emphasis that both branches are **the same file**.
- The versioned primary key and **why reversibility matters clinically** (§3).
- The procedure table (§4), including the **deliberate manual gate** and its governance justification.
- The diff results (§5) — real numbers from the demo run, including the zero-change control group.
- The generalisation table (§6), especially that replay is what makes Cassandra's query-first constraint affordable.
- All six limitations from §7, particularly the **bitemporal enrichment problem** — it is the most sophisticated honest limitation in either project.
- Screenshots: Kafka UI with two consumer groups at different offsets; the Replay Comparison dashboard; the diff summary; the API before and after cutover.

# 05 — Requirement 2: The Single Stream Processing Path

> **PDF requirement:** *"Processing must be consistent with your chosen architecture. Transformations should be **meaningful** for the use case (not just pass-through): cleaning, enrichment, **joins between the two sources**, aggregation, or windowing."*
>
> **Rubric weight: 15 marks** — *"correctness of transformation logic; appropriate use of streaming and/or batch processing **consistent with the declared architecture**."*
>
> The phrase "consistent with the declared architecture" is what this document must earn. Under Kappa there is **one** processing path, and everything — live alerting, the ward screen, the daily report — is derived from it.

---

## 1. One job, one file, one clinical rule

`ward/stream/pipeline.py` is the entire processing layer. It runs as a single long-lived Structured Streaming query.

```
  vitals.readings.v1
         │
         ▼
   ┌───────────────────────────────────────────────────────────────────┐
   │ 1. DESERIALISE      Avro via Schema Registry                      │
   │ 2. CLEAN            physiological-impossibility bounds            │──▶ vitals.readings.dlq
   │ 3. DEDUPLICATE      dropDuplicates(reading_id) + watermark        │
   │ 4. ENRICH           ⨝ ward.admissions (compacted, broadcast)      │
   │                     → age, condition, on_o2, copd_scale2, bed_id  │
   │ 5. WATERMARK        60 sim-minutes (= 12.5 real seconds)          │──▶ vitals.late
   │ 6. WINDOW           4 sim-hour sliding, 1 sim-hour slide          │
   │                     per patient_id → trend aggregates             │
   │ 7. ★ SCORE ★        ward/clinical/news2.py                        │
   │                     THE ONLY PLACE A SCORE IS COMPUTED            │
   │ 8. LAB JOIN         ⨝ labs.results (compacted, broadcast)         │
   │                     → composite_risk_tier                         │
   │ 9. ALERT            threshold + escalation + deduplication        │──▶ vitals.alerts.v1
   │10. SINK             foreachBatch → 5 Cassandra tables             │
   └───────────────────────────────────────────────────────────────────┘
```

**Contrast with the sibling Lambda project deliberately in the report:** that project runs three streaming queries *plus* a separate Spark batch job, and needs a shared `transforms/` package plus a serving-layer merge to keep them agreeing. Here there is one query and nothing to reconcile. **That simplification is the architecture's payoff, and it is visible in the file count.**

---

## 2. Cleaning — `ward/stream/clean.py`

### 2.1 The rule: impossibility, not outlier detection

Covered in `03 §4.4` and restated here because it is the most important clinical-safety decision in the processing layer:

> **We reject readings that are physiologically impossible, never readings that are merely abnormal.** A statistical outlier filter would have suppressed patient `P014`'s deterioration — which is the exact signal the system exists to detect.

| Check | Rule | Reason code |
|---|---|---|
| SpO2 plausible | `spo2 IS NULL OR spo2 BETWEEN 50 AND 100` | `SPO2_PROBE_DETACHED` (0 or <50) |
| Heart rate plausible | `hr IS NULL OR hr BETWEEN 20 AND 250` | `HR_OUT_OF_RANGE` |
| Temperature plausible | `temp IS NULL OR temp BETWEEN 25.0 AND 45.0` | `TEMP_OUT_OF_RANGE` |
| BP coherent | `systolic > diastolic` | `BP_INVERTED` |
| BP plausible | `systolic BETWEEN 50 AND 260` | `BP_OUT_OF_RANGE` |
| Respiratory rate | `rr IS NULL OR rr BETWEEN 4 AND 60` | `RR_OUT_OF_RANGE` |
| Not empty | at least one vital non-null | `EMPTY_READING` |
| Not future-dated | `measured_at <= sim_now() + 5 min` | `FUTURE_TIMESTAMP` |

Note the bounds are wide and are drawn from *survivable physiology*, not from the observed distribution. SpO2 = 72% passes validation and scores 3 — correctly, because that patient is critically hypoxic.

**Partial readings are kept.** A reading with a valid heart rate but a null SpO2 is scored on the parameters present, and the score carries `parameters_missing = 1`. Discarding the whole reading because one probe failed would lose real information. NEWS2 in practice is computed on available parameters with the gap noted, and we mirror that.

### 2.2 Deduplication

`dropDuplicates(["reading_id"])` under the watermark, so state stays bounded. Counted as `readings_deduplicated_total`. Necessary because the monitor's disconnect-buffer-replay behaviour (`03 §4.5`) can legitimately resend.

---

## 3. Enrichment — `ward/stream/enrich.py`

A **stream–static broadcast join** against the compacted `ward.admissions.v1` topic:

```python
admissions = (spark.read.format("kafka")
    .option("subscribe", "ward.admissions.v1")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
    # compaction keeps the latest per key, but a compacted topic may still hold
    # several versions until compaction runs — take the highest offset per key.
    .transform(latest_per_key, key="patient_id"))

enriched = readings.join(broadcast(admissions), on="patient_id", how="left")
```

Brings in `age`, `admitting_condition`, `on_supplemental_oxygen`, **`copd_scale2`**, `bed_id`, `ward_id`.

`copd_scale2` is the important one: **NEWS2 specifies a different SpO2 scoring scale for patients with hypercapnic respiratory failure**, so the enrichment is not cosmetic — it changes the clinical result. Refreshed every 200 micro-batches (admissions change rarely); refresh count is a metric.

A patient with no admission record produces a left-join null, which is **counted and alerted** (`readings_without_admission_total`) rather than silently defaulting — an unknown patient on a monitored bed is a ward-operations problem worth surfacing.

---

## 4. Windowing and trend detection — `ward/stream/windows.py`

```python
.withWatermark("measured_at", "60 minutes")     # simulated -> 12.5 real seconds (03 §3.3)
.groupBy(
    window(col("measured_at"), "4 hours", "1 hour"),   # SLIDING, in simulated time
    col("patient_id"),
)
.agg(
    last("heart_rate",       ignorenulls=True).alias("hr_latest"),
    avg("heart_rate").alias("hr_mean"),
    regr_slope(col("heart_rate"), col("t_index")).alias("hr_slope"),
    last("spo2",             ignorenulls=True).alias("spo2_latest"),
    regr_slope(col("spo2"), col("t_index")).alias("spo2_slope"),
    last("systolic_bp",      ignorenulls=True).alias("sbp_latest"),
    regr_slope(col("systolic_bp"), col("t_index")).alias("sbp_slope"),
    last("temperature",      ignorenulls=True).alias("temp_latest"),
    regr_slope(col("temperature"), col("t_index")).alias("temp_slope"),
    last("respiratory_rate", ignorenulls=True).alias("rr_latest"),
    last("consciousness",    ignorenulls=True).alias("avpu_latest"),
    count("*").alias("readings_in_window"),
    sum(when(col("spo2").isNull(), 1).otherwise(0)).alias("spo2_missing"),
)
```

### Why a 4-simulated-hour sliding window with a 1-hour slide

- **Clinical meaning, not technical convenience.** A single abnormal reading is noise; deterioration is a *direction over hours*. Four hours at 15-simulated-minute sampling gives ~16 readings — enough for a stable slope, short enough that a genuine deterioration is not diluted by hours of prior normality.
- **Sliding, not tumbling** — the module taught all four types. A tumbling window would only re-evaluate a patient every 4 simulated hours, so a deterioration beginning just after a boundary would wait almost a full window before being seen. A 1-hour slide means every patient is re-evaluated four times more often. **In a monitoring system, tumbling windows introduce a latency floor equal to the window width, which is unacceptable here.** That sentence is a good report line.
- **Not session windows**, despite them being the "data-driven" option, because a ward patient is monitored continuously — there is no natural gap-defined session. Say why the rejected window types were rejected; it demonstrates the taxonomy was applied.

**`readings_in_window` is carried forward and matters:** a slope computed from 2 readings is not trustworthy. Scores derived from fewer than 4 readings are tagged `confidence = "low"` and suppressed from *trend*-based alerts (though the instantaneous NEWS2 still fires — a single NEWS2 of 7 is an emergency regardless of trend). Encoding "we don't know yet" rather than guessing is the correct engineering answer.

---

## 5. ★ Scoring — `ward/clinical/news2.py` ★

**This is the file the entire architecture argument protects.** It is pure Python, takes primitives, returns a dataclass, and knows nothing about Spark, Kafka or Cassandra.

```python
@dataclass(frozen=True)
class News2Result:
    total: int                     # 0-20
    subscores: dict[str, int]      # per parameter
    any_parameter_is_3: bool       # NEWS2 escalates on any single red parameter
    clinical_risk: str             # LOW | LOW_MEDIUM | MEDIUM | HIGH
    parameters_missing: int
    scorer_version: str            # "v1" | "v2"

def score_news2(
    respiratory_rate: int | None,
    spo2: int | None,
    on_supplemental_oxygen: bool,
    systolic_bp: int | None,
    heart_rate: int | None,
    consciousness: str | None,     # A|V|P|U
    temperature: float | None,
    *,
    copd_scale2: bool = False,     # NEWS2 SpO2 Scale 2
    version: str = "v2",
) -> News2Result: ...
```

### Why NEWS2 rather than an invented scoring scheme

1. **It is real and published**, so the thresholds are defensible rather than arbitrary — the difference between "a risk score we made up" and "an implementation of a recognised early-warning score".
2. **It is a discrete threshold table**, which makes it exhaustively unit-testable against published reference values. `tests/unit/test_news2.py` uses the published scoring table as fixtures — **this is the single best test in the repository** and should be shown in the viva.
3. **It has a documented version history** (NEWS → NEWS2, adding SpO2 Scale 2), which gives us a *genuine* reprocessing scenario rather than a contrived one. See `06`.
4. It is simple enough to explain line by line under questioning, which the module's notes explicitly require.

### The scoring table (implemented as data, not `if`-chains)

Each parameter maps to 0–3. Total 0–20. Escalation:

| Aggregate | Clinical risk | Response |
|---|---|---|
| 0 | LOW | Routine monitoring |
| 1–4 | LOW | Ward-based response |
| **any single parameter = 3** | **LOW_MEDIUM** | Urgent review — *even if the total is low* |
| 5–6 | MEDIUM | Urgent response |
| ≥ 7 | **HIGH** | Emergency response |

**The `any_parameter_is_3` rule is the one most naive implementations miss**, and it is exactly the kind of detail that separates "we implemented a score" from "we implemented *this* score". A patient with SpO2 = 85% and everything else normal has a total of 3 but requires urgent review. Our test suite covers it explicitly.

### v1 vs v2 — the version flag that makes replay meaningful

| | v1 (NEWS) | v2 (NEWS2) |
|---|---|---|
| SpO2 scoring | Single scale for all patients | **Scale 1** for most; **Scale 2** for `copd_scale2` patients |
| Effect | COPD patients with a normal-for-them SpO2 of 88–92% are **persistently over-scored** | Those patients score correctly; alert noise for them drops |

Both versions live in the same module behind the `version` parameter — **one file, two rule sets, no duplicated code.** That is what makes the replay a version change rather than a fork.

### The enforcement test

```python
def test_only_one_scorer_exists():
    """The Kappa argument in code: nothing outside ward/clinical/ computes a risk score."""
    offenders = grep_tree(
        root="ward/", exclude="ward/clinical/",
        patterns=[r"news2", r"risk_score\s*=", r"def .*score", r"NEWS2_TABLE"],
    )
    assert not offenders, f"Second scoring implementation found: {offenders}"
```

Run it in CI. **When a viva asks "prove there's only one clinical rule", run this test on screen.**

---

## 6. The join between the two sources — `ward/stream/lab_join.py`

This is the transformation the PDF names explicitly, and the mechanism choice needs justifying.

### 6.1 The options

| Option | Assessment |
|---|---|
| **Stream–stream join** — read `labs.results` as a stream, join with a long watermark | Rejected. Structured Streaming requires watermarks on *both* sides and retains state proportional to them. Labs update **once per simulated day**, so we would hold a day of join state to match against a table that barely changes. Also, this is a **lookup**, not a temporal correlation — stream–stream joins exist for the latter. |
| **Broadcast the compacted topic, refreshed periodically** ✅ | Chosen. `spark.read` the compacted topic to `latest`, deduplicate to the highest offset per key, broadcast. 40 patients × 6 tests = 240 rows — trivially small. Refreshed every 60 micro-batches or when a "labs updated" signal is seen. |
| A separate Cassandra lookup per row | Rejected: per-row network I/O inside a micro-batch, and it would make the join dependent on the serving store rather than the log. |

**Chosen: broadcast reload.** Justification for the report, in one sentence each:
- The lab side is a **table**, not a stream — compaction already made it one, so treating it as a table is honest to its semantics.
- State cost is O(patients × tests), a constant, rather than O(watermark duration).
- It is ~15 lines and can be defended line by line, which the module's notes require.
- **Honest trade-off:** the broadcast is eventually consistent within one refresh interval (≈60 micro-batches ≈ 2 real minutes). Irrelevant when the source updates once per simulated day, and stated as such rather than glossed over.

### 6.2 What the join produces — answering the business question

```python
composite = news2_result.total  +  lab_modifier(latest_labs, patient_context)
```

`ward/clinical/lab_rules.py` — again, pure, tested, one place:

| Lab finding | Modifier | Clinical rationale |
|---|---|---|
| Lactate > 2.0 mmol/L | +1 | Tissue hypoperfusion |
| Lactate > 4.0 mmol/L | +2 (replaces above) | Strong sepsis indicator |
| WBC outside reference range | +1 | Infection / inflammatory response |
| CRP > 100 mg/L | +1 | Marked inflammation |
| Creatinine rising > 26 µmol/L vs previous | +1 | Acute kidney injury indicator |
| Potassium outside range | +1 | Arrhythmia risk |
| Haemoglobin < 80 g/L | +1 | Significant anaemia |
| **All labs stale (> 2 simulated days old)** | **0, and flag `labs_stale = true`** | **Never infer safety from missing data** |

Composite tier:

| Composite | Tier |
|---|---|
| 0–4 | `LOW` |
| 5–6 | `MEDIUM` |
| 7–9 | `HIGH` |
| ≥ 10 | `CRITICAL` |

**This is the business question answered, literally:** *"how do yesterday's lab results change the risk picture?"* → `composite_risk - news2_total = lab_contribution`, and we **store both** so the daily report can say *"P014: NEWS2 7 (HIGH) → composite 10 (CRITICAL), driven by lactate 3.8 and WBC 18.4."*

Storing the decomposition rather than just the total is what makes the answer *explainable*, and explainability is not optional for a clinical score. Good report point.

### 6.3 The stale-labs rule deserves emphasis

If labs are missing or old, the modifier is **zero and flagged** — never treated as "normal". The API surfaces `labs_stale`, and the daily report lists patients whose risk picture is based on vitals alone. **A system that quietly treats absent data as reassuring is dangerous**, and the same principle drives the `WardMonitoringSilent` alert (`09 §4.1`). Naming this as a consistent design principle across two layers is worth a paragraph.

---

## 7. Alerting — `ward/stream/alerts.py`

Emitted to `vitals.alerts.v1` and written to Cassandra.

| Alert | Condition | Severity |
|---|---|---|
| `NEWS2_HIGH` | total ≥ 7 | CRITICAL |
| `NEWS2_MEDIUM` | total 5–6 | HIGH |
| `SINGLE_PARAM_RED` | any parameter = 3 | HIGH (even at low total) |
| `SPO2_CRITICAL` | SpO2 < 88 (or < 84 on Scale 2) | CRITICAL |
| `RAPID_DETERIORATION` | score rose ≥ 3 within 4 simulated hours | CRITICAL |
| `LAB_CORROBORATED_RISK` | composite ≥ 7 **and** lab contribution ≥ 2 | HIGH |
| `TREND_CONCERNING` | slopes adverse on ≥ 2 parameters, confidence not low | MEDIUM |

**Deduplication and escalation:** `alert_id = sha1(patient_id, alert_type, window_start)`, so a sustained condition produces one alert per window rather than one per micro-batch. An alert is re-emitted only when the severity **escalates**. Without this, `P014`'s deterioration would emit hundreds of duplicate alerts and the feed would be unusable — **alert fatigue is a real clinical failure mode and designing against it is worth stating.**

---

## 8. Sinks — `ward/stream/sinks.py`

One `foreachBatch` writing to five Cassandra tables (schema in `07 §2`):

```python
def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    with tracer.start_as_current_span("stream.sink") as span:
        span.set_attribute("batch_id", batch_id)
        rows = batch_df.collect()      # safe: <= 40 patients x a few open windows
        with cassandra_session() as s:
            execute_concurrent(s, [
                *stmts_vitals_by_patient(rows),
                *stmts_risk_scores_by_patient(rows),   # keyed by scorer_version
                *stmts_ward_risk_snapshot(rows),       # TTL'd, no deletes
                *stmts_alerts_by_ward(alerts),
                *stmts_daily_patient_summary(rows),
            ])
        sink_write_duration.observe(...)
```

- **`collect()` is safe here only because the ward is 40 patients**, and the code says so in a comment — otherwise it is a red flag a marker would rightly challenge.
- **Every write is an INSERT with a fully-specified primary key**, so a replayed micro-batch overwrites identically. **No counter columns anywhere** — that is the idempotence guarantee (`04 §5`).
- **`scorer_version` is part of the primary key** on the score tables, which is what lets a replay write alongside live data instead of destroying it (`06`).

Checkpoint: `/checkpoints/ward-stream-v1/`. Trigger: `processingTime="5 seconds"`.

**Documented trap:** changing the aggregation or state schema invalidates the checkpoint. `make reset-checkpoint` exists and the README warns about it. This is a genuine operational cost of stateful streaming that the module did not cover, and it belongs in the limitations section.

---

## 9. Testing

The clinical logic is pure Python, so the most important tests need neither Spark nor Docker.

| Test | Asserts |
|---|---|
| **`test_news2.py`** | **The published NEWS2 scoring table, parameter by parameter, as fixtures.** Boundary values on every threshold. The `any_parameter_is_3` escalation. Scale 1 vs Scale 2 divergence for `copd_scale2` patients. Missing-parameter handling. **The best test in the repo.** |
| **`test_single_scorer.py`** | No module outside `ward/clinical/` computes a score (§5) |
| `test_lab_rules.py` | Each modifier fires on its condition; stale labs contribute 0 **and** set the flag |
| `test_composite_risk.py` | Tier boundaries; the decomposition (`composite − news2 = lab_contribution`) always holds |
| `test_clean.py` | One row per defect type → correct reason code; **abnormal-but-possible values pass**; partial readings survive with `parameters_missing` set |
| `test_windows.py` | Sliding-window boundaries; slope sign on a synthetic rising series; `readings_in_window` gates low-confidence trends |
| `test_lab_join.py` | Compacted topic with two values for one key → the later one wins |
| `test_alert_dedup.py` | A sustained condition emits one alert per window; escalation re-emits; de-escalation does not |
| **`test_deterioration_e2e.py`** | Feed `P014`'s scripted trajectory through the real transform chain → NEWS2 reaches ≥ 7. **CI proves the demo works.** |
| `test_sink_idempotence.py` | The same micro-batch written twice → identical Cassandra state |

---

## 10. What the report must show (§6.3)

- The single-path diagram from §1, contrasted with a Lambda alternative that would have needed two.
- The **impossibility-not-outlier** cleaning principle, with the point that a 3-sigma filter would have suppressed `P014`.
- Window choice: 4-simulated-hour sliding with a 1-hour slide, and why tumbling was rejected (latency floor).
- The watermark arithmetic in simulated time (12.5 real seconds).
- `news2.py` as **the** clinical rule, with the enforcement test.
- The lab-join mechanism decision table (§6.1) and the honest eventual-consistency trade-off.
- The **risk decomposition** (`composite − news2 = lab contribution`) as the literal answer to the business question.
- The stale-labs and alert-fatigue design decisions.
- A Spark UI screenshot of the Structured Streaming tab: input rate, batch duration, state rows, watermark.

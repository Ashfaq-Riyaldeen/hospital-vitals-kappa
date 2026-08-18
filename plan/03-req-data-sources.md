# 03 — Requirement 1: Simulated Data Sources

> **PDF requirement:** *"Simulated sources, built as Python scripts: a **streaming source** emitting every few seconds... a **batch source** that uploads/drops a new file once per simulated day... **State your simulated clock clearly in the report.**"*
>
> **Rubric weight: 15 marks** — *"correctness and **robustness** of simulated sources."*

---

## 1. What we are building

| File | Role | Runs as |
|---|---|---|
| `ward/producers/bedside_monitor.py` | **Streaming source.** 40 bedside monitors emitting vital signs to Kafka | Long-lived container |
| `ward/producers/lab_uploader.py` | **Daily-batch source.** Writes one lab-results JSON file per simulated day | Long-lived container, sleeps between simulated days |
| `ward/producers/admissions.py` | Seeds the compacted ward-reference topic; emits occasional admissions/discharges | One-shot + periodic |
| `ward/simclock.py` | **The shared simulated clock** — imported by producers, the Spark job, Airflow DAGs and the API | Library |

---

## 2. The ward model

Defined once in `data/ward_seed.json`, consumed by all three producers so they agree on patient identity.

```
40 beds        BED-01 … BED-40
40 patients    P001 … P040
1 ward         WARD-A (a general medical ward)
```

**Patient record** (published to the compacted `ward.admissions.v1` topic, used as stream enrichment):

| Field | Values | Why it exists |
|---|---|---|
| `patient_id` | `P001`–`P040` | Key |
| `bed_id` | `BED-01`–`BED-40` | Ward layout for the dashboard grid |
| `ward_id` | `WARD-A` | Partition key for the ward-level snapshot table |
| `age` | 24–91 | Shifts baseline vitals; older patients have wider normal ranges |
| `sex` | M/F | Minor baseline effect |
| `admitting_condition` | `post_op` \| `copd` \| `sepsis_risk` \| `cardiac` \| `stable` | **Load-bearing** — drives the physiological model *and* the clinical rule |
| `on_supplemental_oxygen` | bool | **A NEWS2 scoring parameter in its own right** (+2) |
| `copd_scale2` | bool | **Load-bearing.** NEWS2 uses a *different SpO2 scoring scale* for patients with hypercapnic respiratory failure. This flag is what makes the v1→v2 replay demo clinically meaningful rather than arbitrary. |
| `admitted_at_sim` | timestamp | |

Roughly 6 of 40 patients carry `copd_scale2 = true`. When the scorer is upgraded from v1 (single SpO2 scale) to v2 (dual scale), **exactly those patients' historical scores change** — a small, explainable, verifiable diff. That is far more convincing in a demo than a change that moves every row.

---

## 3. The simulated clock

Four independent processes (monitors, lab uploader, Spark, Airflow) must agree on the simulated date, or the lab join silently matches nothing.

### 3.1 Design

```python
# ward/simclock.py
SIM_EPOCH_WALL:  datetime   # real UTC instant the simulation started
SIM_EPOCH_SIM:   datetime   # simulated datetime it corresponds to (2026-04-01T00:00:00Z)
SIM_DAY_SECONDS: int = 300  # 1 simulated day = 5 real minutes
SPEEDUP:         float = 86400 / SIM_DAY_SECONDS   # = 288.0

def sim_now() -> datetime:
    elapsed = (utcnow() - SIM_EPOCH_WALL).total_seconds()
    return SIM_EPOCH_SIM + timedelta(seconds=elapsed * SPEEDUP)
```

**Anchoring rule:** `SIM_EPOCH_WALL` is set **once** by the init container at stack start-up and written to a bind-mounted `state/sim_epoch.json` *and* into a Cassandra `sim_state` row. Every other process **reads** it; nobody computes it locally. The Spark job receives it as a broadcast value at submission so executors resolve simulated time without a per-row lookup.

`SIM_EPOCH_SIM` is fixed at `2026-04-01T00:00:00Z` so screenshots stay consistent across runs.

### 3.2 Dual timestamps on every reading

| Field | Value | Used for |
|---|---|---|
| `measured_at` | **simulated** time the reading was taken | Spark windowing, watermarks, trend slopes, Cassandra clustering |
| `ingest_time` | **real** wall-clock time it was published | Prometheus latency histograms, end-to-end lag, trace timing |

Confusing these is the classic failure: a "time to detect deterioration" panel must use real time; a "4-hour trend" must use simulated time.

### 3.3 Watermarks must be expressed in SIMULATED time — the trap

At 288×, **one real second = 4.8 simulated minutes.**

A watermark of `"30 minutes"` on `measured_at` therefore tolerates only **6.25 real seconds** of lateness. Real network jitter, a Kafka rebalance or a GC pause can exceed that, silently dropping readings and producing empty windows — which costs hours to diagnose because nothing errors.

**Rule: express the watermark generously in simulated terms.** We use **`withWatermark("measured_at", "60 minutes")`** = **12.5 real seconds** of tolerance, and document the arithmetic inline:

```python
# 60 simulated minutes ÷ 288x speedup = 12.5 real seconds of lateness tolerance.
# Vitals are sampled every 15 sim-minutes, so this admits up to 4 missed samples.
# See plan/03 §3.3.
.withWatermark("measured_at", "60 minutes")
```

A startup validator (`settings.py`) and a unit test both fail if the speed-up is changed without revisiting the watermark. **Mention this test in the viva** — it shows the coupling was understood rather than stumbled into.

### 3.4 Alignment table

| Simulated | Real | Notes |
|---|---|---|
| 1 simulated day | 5 minutes | The assignment's own suggested compression |
| 1 simulated hour | 12.5 seconds | |
| 15 simulated minutes (sampling interval) | 3.1 s | Per patient — routine ward observation cadence |
| 60 simulated minutes (watermark) | 12.5 s | Lateness tolerance |
| 4 simulated hours (trend window) | 50 s | ~16 readings per patient per window |
| 1 simulated hour (window slide) | 12.5 s | |
| 3 simulated days (full demo) | 15 minutes | Enough for deterioration + labs + replay |
| 30 simulated days (retention) | 2.5 hours | The replay horizon |

---

## 4. Streaming source — `bedside_monitor.py`

### 4.1 Reading schema (Avro, `contracts/vitals.v2.avsc`)

| Field | Type | Notes |
|---|---|---|
| `reading_id` | string (UUID) | Idempotency / dedup key |
| `patient_id` | string | **Kafka message key** |
| `bed_id` | string | |
| `heart_rate` | int, nullable | bpm |
| `spo2` | int, nullable | % |
| `systolic_bp` | int, nullable | mmHg |
| `diastolic_bp` | int, nullable | mmHg |
| `temperature` | double, nullable | °C |
| **`respiratory_rate`** | int, nullable | **Added — NEWS2 parameter** |
| **`consciousness`** | enum `A\|V\|P\|U`, nullable | **Added — NEWS2 AVPU parameter** |
| `on_supplemental_oxygen` | bool | NEWS2 parameter (+2) |
| `measured_at` | timestamp-millis | **Simulated** |
| `ingest_time` | timestamp-millis | **Real** |
| `device_id` | string | Which monitor — enables per-device fault attribution |
| `schema_version` | int | |

> **Declared deviation from the PDF's field list:** we add `reading_id`, `respiratory_rate`, `consciousness`, `on_supplemental_oxygen`, `ingest_time`, `device_id` and `schema_version`. The PDF permits adaptation (*"You may adapt scope (e.g. add/change fields as required)"*). **`respiratory_rate` and `consciousness` are not decoration — NEWS2 cannot be computed without them**, and using a real published scoring system is what makes the risk number defensible. The report states this in §6.1.

### 4.2 Physiological realism

Uniform random vitals would make the risk score meaningless and the dashboard unconvincing. The model:

**Per-patient baseline**, drawn at admission from age- and condition-adjusted distributions:
```
heart_rate        N(72 + age_adj, 8)      copd: +8   cardiac: +12   post_op: +10
spo2              N(97 - age_adj, 1.5)    copd: -4
systolic_bp       N(124 + age_adj, 12)    sepsis_risk: -6
respiratory_rate  N(16, 2)                copd: +3
temperature       N(36.8, 0.25)
```

**AR(1) / Ornstein–Uhlenbeck random walk** around each baseline — vitals drift and mean-revert, they do not jump independently each sample:
```
x[t] = x[t-1] + θ·(baseline − x[t-1]) + σ·ε      θ ≈ 0.25
```

**Physiological coupling** — the detail that makes it look real to anyone who knows the domain:
- SpO2 falling → heart rate rises (compensatory tachycardia)
- Temperature rising → heart rate rises (~8 bpm per °C)
- Systolic falling below ~95 → heart rate rises, respiratory rate rises
- Respiratory rate and SpO2 are inversely coupled

**Circadian rhythm:** heart rate and temperature dip overnight (simulated 02:00–05:00), which shows up as a visible band on the ward dashboard and proves the simulated clock is driving the physiology.

### 4.3 Scripted clinical narratives — the demo backbone

All seeded (`--seed 42`, default in compose), so every demo run is identical.

| Patient | Script | Demonstrates |
|---|---|---|
| **`P014`** (`sepsis_risk`) | From simulated **day 2, 08:00**, a 4-simulated-hour sepsis-like trajectory: temperature 36.9 → 38.9 °C, heart rate 78 → 122, respiratory rate 16 → 26, systolic 128 → 96, SpO2 97 → 93 | **NEWS2 climbs 1 → 5 → 7.** Alerts escalate `LOW` → `MEDIUM` → `HIGH`. The centrepiece of the live demo. |
| **`P014`** labs, simulated day 2 | Lactate 3.8 mmol/L (ref 0.5–2.2), WBC 18.4 ×10⁹/L (ref 4.0–11.0), CRP elevated | **The business question, answered:** the composite risk tier rises *above* what vitals alone give, because the labs corroborate the deterioration |
| **`P031`** (`copd`, `copd_scale2=true`) | Steady SpO2 around 89–91% — normal for this patient, abnormal on the standard scale | **The replay demo's target.** Under scorer v1 (single SpO2 scale) `P031` is persistently over-scored; under v2 (NEWS2 Scale 2) the score corrects. The replay shows exactly which patients change and why. |
| **`P007`** (`post_op`) | A single transient spike (heart rate 145 for one reading) then normal | Demonstrates that windowed trend detection **does not** alert on isolated noise — an important negative result to show |

### 4.4 Sensor artefacts — the cleaning requirement

The PDF wants *"cleaning"* as a meaningful transformation, and the use case explicitly mentions *"occasional simulated abnormal spikes"*. We distinguish two categories, and **the distinction itself is a report point**:

**(a) Physiologically impossible → DLQ. These are equipment faults, not patient states.**

| Defect | Rate | Reason code |
|---|---|---|
| `spo2 = 0` (probe fell off) | 0.5% | `SPO2_PROBE_DETACHED` |
| `heart_rate > 250` or `= 0` (lead artefact) | 0.4% | `HR_OUT_OF_RANGE` |
| `temperature < 25` or `> 45` | 0.2% | `TEMP_OUT_OF_RANGE` |
| `systolic <= diastolic` (cuff error) | 0.2% | `BP_INVERTED` |
| All vitals null (monitor disconnected) | 0.3% | `EMPTY_READING` |
| `measured_at` 2 simulated hours in the future | 0.1% | `FUTURE_TIMESTAMP` |
| Duplicate `reading_id` | 0.05% | deduplicated and counted |

**(b) Abnormal but physiologically possible → scored normally.** A patient with SpO2 = 88% is not a data error; they are a sick patient. **Cleaning must never suppress a genuine clinical signal.**

> **This is the most important clinical-safety point in the data-quality design, and the report should state it explicitly:** an over-aggressive outlier filter in a monitoring system would discard exactly the readings that matter most. Our validation therefore uses *physiological impossibility* bounds, not statistical outlier detection. A 3-sigma filter would have suppressed `P014`'s deterioration.

`DEFECT_RATE` is configurable at runtime so `make chaos-bad-data` can drive it up and fire the `HighDLQRate` alert on demand.

### 4.5 Robustness — where the 15 marks live

| Concern | Implementation |
|---|---|
| **Delivery guarantee** | `confluent-kafka` with `enable.idempotence=true`, `acks=all`, `retries=10`, `max.in.flight=5` — the module taught idempotent producers and EOS; we use them and say so |
| **Delivery confirmation** | Async `delivery_report` callback increments `readings_produced_total{status}`; failures logged with full error. Never fire-and-forget. |
| **Broker unavailable at start** | Retry with exponential backoff (1 s → 30 s cap), one clear log line per attempt; the container must not crash-loop while Kafka starts |
| **Disconnection buffering** | On send failure, readings are buffered locally (bounded deque, 5,000 entries) **retaining their original `measured_at`**, and replayed on reconnect. Models a real bedside monitor and exercises the late-data path. |
| **Backpressure** | Bounded producer queue; on `BufferError` block rather than drop, increment `producer_backpressure_total`, log a warning. **Dropping a clinical observation silently is not acceptable.** |
| **Graceful shutdown** | `SIGTERM`/`SIGINT` → stop generating → `flush(10)` → close → log final counts |
| **Rate control** | Token bucket against real time; `TARGET_EPS` ≈ 13 |
| **Health & metrics** | `/health` and `/metrics` on 8101; compose healthcheck |
| **Logging** | JSON, `stage="ingest"`, `correlation_id = patient_id`, `trace_id` |
| **Tracing** | One span per flush; `traceparent` injected into Kafka headers |
| **Config** | `pydantic-settings`; `--dry-run` prints to stdout without Kafka |

---

## 5. Daily-batch source — `lab_uploader.py`

### 5.1 What it produces

One JSON file per simulated day at `labs/inbox/labs_2026-04-02.json` (a bind-mounted directory that Airflow's `FileSensor` watches).

```json
{
  "ward_id": "WARD-A",
  "sim_date": "2026-04-02",
  "collected_at_sim": "2026-04-02T06:00:00Z",
  "submitted_at_real": "2026-08-04T18:22:07Z",
  "lab_id": "PATH-LAB-1",
  "results": [
    {"patient_id": "P014", "test_type": "lactate",    "result_value": 3.8,
     "unit": "mmol/L", "reference_range": "0.5-2.2",  "collected_at": "2026-04-02T05:40:00Z"},
    {"patient_id": "P014", "test_type": "wbc",        "result_value": 18.4,
     "unit": "10^9/L",  "reference_range": "4.0-11.0","collected_at": "2026-04-02T05:40:00Z"}
  ]
}
```

**Test panel** — 6 test types per patient per simulated day: `wbc`, `crp`, `lactate`, `creatinine`, `haemoglobin`, `potassium`. Values are drawn correlated with the patient's condition and current trajectory, so labs *corroborate* vitals rather than contradicting them randomly — which is what makes the "labs change the risk picture" answer coherent.

**`reference_range` is a string** (`"4.0-11.0"`), as the PDF specifies. Parsing it into bounds — and handling the one-sided forms (`"<5.0"`, `">40"`) that a real lab feed contains — is a small, genuinely testable cleaning transformation. `tests/unit/test_reference_range.py` covers it.

### 5.2 Robustness

| Concern | Implementation |
|---|---|
| **Atomic drop** | Write `labs/inbox/.tmp/labs_<date>.json`, then `os.rename` to the final path. `rename` is atomic on the same filesystem, so the `FileSensor` can never fire on a half-written file. **This is a real and very common bug.** |
| **Late arrival** | With probability 0.15 the file is delayed 20–80 real seconds past the simulated day boundary, so the sensor's `poke`/timeout path is genuinely exercised |
| **Missing day** | On simulated day 5 (`SIMULATE_MISSING_LAB_DAY=5`) no file is dropped → sensor times out → DAG branches to `handle_missing_lab_file` → alert → **the ward continues to be monitored on vitals alone**, with the API reporting `lab_data_stale: true`. Degrading honestly rather than failing is the correct clinical behaviour and a good report point. |
| **Malformed file** | On simulated day 4 (optional) a `reference_range` is corrupted → validation fails → file quarantined → alert |
| **Checksum** | `.sha256` sidecar, verified before parsing |
| **Idempotency** | Re-processing the same file republishes the same keys to the compacted topic; compaction means the result is identical. Naturally idempotent. |
| **Metrics/logs** | `/metrics` on 8102: `lab_files_written_total`, `lab_results_written_total`, `lab_file_delay_seconds` |

---

## 6. Testing the sources

| Test | Asserts |
|---|---|
| `test_simclock.py` | Round-trip conversions; `sim_date` advances exactly once per 300 real seconds |
| **`test_watermark_safety.py`** | `sim_to_real(WATERMARK_SIM_SECONDS) >= 10.0` — **fails CI if the speed-up changes without revisiting the watermark** |
| `test_physiology.py` | Vitals stay within physiological bounds over 100k samples; coupling holds (SpO2 ↓ ⇒ HR ↑) |
| `test_determinism.py` | Same seed → byte-identical first 1,000 readings. **Protects the scripted demo narratives.** |
| `test_deterioration_script.py` | `P014`'s trajectory produces NEWS2 ≥ 7 within 4 simulated hours of onset — **the demo's correctness, asserted in CI** |
| `test_defects.py` | Each defect type lands within ±20% of its configured rate over 100k readings |
| `test_reference_range.py` | `"4.0-11.0"`, `"<5.0"`, `">40"`, `"0.5-2.2"` all parse correctly; malformed input raises |
| `test_lab_correlation.py` | `P014`'s day-2 lactate and WBC are out of range; a stable patient's are in range |
| `test_atomic_drop.py` | No partial file is ever visible at the final path |
| `test_schema_evolution.py` | A v1 reading (no `respiratory_rate`) deserialises under the v2 reader schema |

---

## 7. What the report must state (§6.1)

> **Simulated clock.** One simulated day is compressed to **5 real minutes** (a speed-up of **288×**), matching the compression suggested in the assignment brief. The simulation begins at simulated `2026-04-01T00:00:00Z`, anchored to a single wall-clock epoch established at stack start-up and shared by all components. Each reading carries both a simulated `measured_at` (used for all windowing, watermarking and clustering) and a real `ingest_time` (used for latency measurement). Stream watermarks are expressed in simulated time: a 60-simulated-minute watermark corresponds to 12.5 real seconds of lateness tolerance.
>
> **Scope adaptation.** The assignment permits adapting fields. We add `respiratory_rate` and `consciousness` (AVPU) to the vital-signs schema because the **NEWS2** early-warning score — which we implement from its published specification — requires them. We also add `on_supplemental_oxygen` and a `copd_scale2` admission flag, the latter because NEWS2 specifies a separate SpO2 scoring scale for patients with hypercapnic respiratory failure.
>
> **Assumptions and simplifications.** A single 40-bed general medical ward; vitals sampled every 15 simulated minutes (routine ward observation cadence rather than continuous waveform capture); physiology modelled as a mean-reverting AR(1) process around age- and condition-adjusted baselines with inter-parameter coupling and a circadian component; approximately 1.7% of readings carry deliberate equipment-fault artefacts to exercise the cleaning and dead-letter paths; four patients follow scripted clinical narratives so demonstration outcomes are reproducible under a fixed random seed.
>
> **Safety.** All data is simulated. NEWS2 is implemented from its published specification for realism only. This software is not a medical device, has not been clinically validated, and must not be used for any clinical purpose.

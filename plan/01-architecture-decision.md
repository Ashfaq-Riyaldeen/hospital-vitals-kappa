# 01 — Architecture Decision: Kappa vs Lambda

> **Rubric weight: 20 marks — the single largest criterion.** Assessed on *"correctness and depth of the argument for the chosen architecture given the use case's latency, replay, cost and consistency requirements; honest discussion of trade-offs and rejected alternatives."*
>
> This document is the source material for §3 of the report.

---

## 1. Start from the business question

> *"Which patients show concerning vital-sign trends **right now**, and how do **yesterday's lab results** change the risk picture for those patients **going forward**?"*

Compare this carefully with the ride-hailing question, because the difference is the whole decision:

| | Ride-hailing (Lambda) | **Hospital vitals (Kappa)** |
|---|---|---|
| Structure | *Two* questions with different SLAs bolted together | **One** question, asked continuously |
| What the daily source does | Provides a **separate answer** (profitability) that the stream cannot compute | **Modifies the same answer** (risk) that the stream already computes |
| Output cardinality | Two numbers: live utilization *and* daily P&L | **One number per patient: current risk** |
| Accuracy split | Live tolerates error; financial demands exactness | **The same number serves both the bedside alert and the report** |
| Consequence of disagreement between paths | An operational annoyance | **A patient-safety incident** |

The lab results are not a second question. They are an **input to the same risk computation**. "How do yesterday's labs change the risk picture" is asking for a *revised value of the same quantity*, not a separate report that lives alongside it.

**If one quantity is produced, one pipeline should produce it.** That is Kappa.

### 1.1 The patient-safety argument, stated properly

Under Lambda we would implement the clinical risk rule **twice**: once in the speed layer for bedside alerting, once in the batch layer for the nightly consolidated report. The module names this as Lambda's own weakness — *"managing multiple codebases"* and *"reconciling data between systems"*.

In most domains that is a maintenance cost. Here it is a hazard:

- A patient could be **amber on the ward screen and red in the morning report**, or worse, the reverse.
- A clinician who notices the discrepancy loses trust in both.
- A clinician who does *not* notice acts on whichever they saw last.
- When the rule is updated (thresholds do change — NEWS was revised to NEWS2), **two** implementations must be updated in lockstep, and any lag between them is a window in which the system gives two different answers about the same patient.

**One clinical rule, one implementation, one number.** This is the strongest argument in the project and it is entirely domain-specific — which is exactly what the rubric means by *"justification tied to use-case constraints rather than generic popularity."*

---

## 2. The decision, applied against the module's own criteria

The module gives five explicit criteria and three more in its comparison table. We walk all eight, and we are honest where they cut against us. **Use these as sub-headings in the report.**

### 2.1 Latency requirements

> *Module: "If your application requires real-time processing and low latency, the **Kappa** Architecture may be a better choice."*

**Verdict: Kappa. Decisive.**

A desaturating patient must be detected in seconds. There is no part of this business question whose answer may be a day old — even the "yesterday's labs" half is about *how the risk picture changes going forward*, i.e. it must be reflected in the live risk score immediately once the labs arrive, not held until a nightly job.

Under Lambda, the batch layer would add T+1 latency to a computation whose entire purpose is immediacy. There is nothing for that latency to buy.

### 2.2 Data volume

> *Module: "The Lambda Architecture is better suited for processing **large volumes**... The **Kappa** Architecture is ideal for processing **smaller data volumes** that can be handled in real-time."*

**Verdict: Kappa.** This criterion is idiosyncratic relative to industry consensus, but it is the module's stated rule and here it genuinely favours us:

| | |
|---|---|
| Ward size | 40 beds |
| Sampling cadence | every 15 simulated minutes (routine ward observation practice) |
| Events per simulated day | **3,840** |
| Sustained rate | **≈13 events/second** |
| 30 simulated days retained | ~115,000 events ≈ **30 MB** |
| Full replay of all history | **seconds** |

This is a small dataset by any measure, and it is the reason Kappa's reprocessing story is *viable* rather than theoretical. **The usual objection to Kappa — "replaying your whole history is expensive" — simply does not bind at ward scale.**

**Concede the boundary honestly, in the report:** at a national multi-hospital registry (500 hospitals × 300 beds, continuous 1 Hz waveform capture rather than 15-minute observations) this argument inverts completely — replay becomes hours, a warehouse becomes necessary, and Lambda becomes attractive. Stating where our own argument stops being true is worth marks, and a viva will ask.

> Worth noting in the report: the sibling ride-hailing project generates ~72,000 events per simulated day — **19× more** — and correctly chose Lambda partly on this criterion. The same rule discriminating in opposite directions across two use cases is evidence it was applied rather than cited.

### 2.3 Complexity

> *Module: "The **Kappa** Architecture is **simpler and easier to manage since it eliminates the batch layer**. The Lambda Architecture is more complex and requires managing multiple layers and technologies."*

**Verdict: Kappa.** And here the simplicity is not merely operational convenience — see §1.1. One path means:

- One implementation of NEWS2 (`ward/clinical/news2.py`), unit-tested against the published scoring table.
- One place to change when a threshold changes.
- No reconciliation problem to solve, because there is nothing to reconcile.
- No high-water-mark bookkeeping, no dual-view merge logic, no divergence monitoring.

We enforce it mechanically: a test walks the source tree and fails if any module outside `ward/clinical/` computes a risk score. **An architectural principle enforced by a test is a strong viva artefact.**

### 2.4 Historical data analysis

> *Module: "If your application requires historical data analysis, the **Lambda** Architecture may be a better choice... The **Kappa** Architecture focuses on real-time processing and **may not be as suitable for historical data analysis**."*

**Verdict: this criterion favours Lambda. We concede it, and then bound it.**

Do not dodge this — it is the most obvious attack on our choice. The honest position:

**Our historical requirement is genuinely narrow.** It consists of exactly two things:
1. A **trailing trend window per patient** (4 simulated hours of vitals) to answer "is this patient deteriorating", plus a 24-simulated-hour view for the daily report. Both are held in stream state or read back from Cassandra.
2. The ability to **re-derive historical scores when the clinical rule changes** — which is `06-replay-reprocessing.md`, and which Kappa handles *natively* by replaying the retained log.

We are **not** doing multi-year epidemiological research, cohort analysis, or ML training over years of admissions. If the ward later wanted that, **the correct evolution is not to bolt on a Lambda batch layer** — it is to add a Kafka Connect sink that archives the same log to a warehouse for OLAP, leaving the single processing path untouched. That is "Kappa plus an archive", and it preserves the one-clinical-rule property that motivated the choice.

Saying explicitly *"here is where our architecture stops serving us, and here is what we would do instead"* is the difference between a defensible decision and a lucky one.

### 2.5 Cost

> *Module: "The Lambda Architecture may require more resources... The **Kappa** Architecture **can be more cost-effective** since it only focuses on real-time processing."*

**Verdict: Kappa — but the module contradicts itself here, and reconciling that is a differentiator.**

The module's Kappa definition says it *"hasn't seen widespread adoption due to **streaming complexity and cost** compared to traditional batch processing"* — while its comparison slide says Kappa is *"simpler"* and *"more cost-effective"*. **Both are true, of different things**, and the report should say so directly:

| | Kappa is… | Because |
|---|---|---|
| **Architecturally** | cheaper and simpler | One processing path, one codebase, one store, no batch compute, no reconciliation machinery |
| **Operationally** | more demanding | A 24/7 job whose failure means *no monitoring at all*. That requires checkpointing, HA, and — critically — **alerting on the pipeline's own liveness**, because in this domain a stopped pipeline is indistinguishable from a ward of healthy patients. |

We accept the operational demand and we build for it: `WardMonitoringSilent` (`09 §4.1`) is a first-class critical alert, and the report frames it as the price of the architecture rather than as a bonus feature.

Naming a contradiction in the source material and resolving it is exactly the "depth of argument" the rubric is looking for.

### 2.6 Fault tolerance

> *Module: Lambda — "Fault-tolerant, as batch processing ensures data accuracy." Kappa — "Fault-tolerant with real-time processing but **depends on stream integrity**."*

**Verdict: this criterion favours Lambda. We concede it and mitigate concretely.**

Under Kappa the stream *is* the record. If it is corrupted or lost, there is no second copy to reconcile against. Mitigations, each of which appears in the build:

| Threat | Mitigation |
|---|---|
| Broker data loss | `acks=all`, idempotent producers, `replication.factor=3` in the production design (RF=1 at demo scale — declared) |
| Streaming job crash losing state | Checkpointing to durable storage; state restored on restart |
| Bedside monitor loses connectivity | Producer buffers locally and replays on reconnect, preserving `event_time` so late readings are still correctly placed in windows |
| Readings arriving beyond the watermark | Routed to `vitals.late` — **never silently dropped.** The module says late data is *"either dropped or handled by a special side-process"*; we do the latter, deliberately, because in a clinical setting a discarded reading is a discarded observation. |
| **The pipeline stops entirely** | `WardMonitoringSilent` critical alert within 3 minutes |

**The line for the report:** *"Absence of alerts must never be mistaken for absence of risk."* That sentence justifies an entire class of monitoring that a non-clinical project would not need, and it is the single best illustration that our observability design was driven by the domain.

### 2.7 Data reprocessing

> *Module: Lambda — "Batch layer allows accurate reprocessing." Kappa — "Reprocessing is done by **replaying the stream** in real-time."*

**Verdict: Kappa. This is the decisive criterion and the demo centrepiece.**

The concrete, entirely realistic scenario: **the clinical scoring rule changes.** NEWS was revised to NEWS2 in 2017, adding a separate SpO2 scale for patients with hypercapnic respiratory failure (typically COPD). A ward adopting that revision must be able to answer: *"under the new rule, which of our current patients would have been flagged earlier?"*

- **Under Kappa:** deploy `news2 v2`, start it as a **new consumer group from offset 0**, write to version-tagged rows in Cassandra, chart v1 against v2 side by side, verify, then cut the API over. History is re-derived from the immutable log. At 115,000 events this takes **seconds**.
- **Under Lambda:** you must re-run the batch layer *and* rebuild the speed layer's state *and* verify the two agree — reprocessing across two codebases, which is the reconciliation problem again, now under time pressure.

This is not a hypothetical convenience. It is the operational capability that a monitoring system needs most, and Kappa provides it as a property of the architecture rather than as a feature someone had to build.

**We demonstrate it live** (`06`, `13 §1`). The architecture argument is executed on camera, not merely asserted.

### 2.8 Accuracy

> *Module: Lambda — "Batch layer provides **high accuracy**, speed layer offers **immediate but less accurate results**." Kappa — "Provides **consistent results**, but may not match the accuracy of dedicated batch processing."*

**Verdict: Kappa, on the grounds that consistency is the safety-relevant property here.**

The module frames this as a straight trade: Lambda buys accuracy, Kappa buys consistency. In this domain **consistency is worth more**, and the reason is clinical rather than technical: a single number that the ward screen, the alert and the report all agree on is safer than two slightly-more-accurate numbers that disagree. A clinician can calibrate to a consistent scale; they cannot calibrate to two.

And our accuracy loss is small and bounded, not open-ended:
- Late data is **side-processed rather than dropped** (`vitals.late`), so the loss is quantified and visible on a dashboard rather than invisible.
- NEWS2 is a **discrete, integer, threshold-based score**. It is not a continuous statistic where micro-differences in aggregation accumulate — a reading either crosses a threshold or it does not. Approximation in the aggregation layer barely moves it.
- No sampling or sketch data structures are used anywhere in the scoring path. (Contrast the sibling project, which deliberately uses HyperLogLog in its speed layer — appropriate there, unacceptable here.)

---

## 3. Summary table for the report

| # | Criterion (module's wording) | Favours | Weight for us | Note |
|---|---|---|---|---|
| 1 | Latency requirements | **Kappa** | **Decisive** | Detection in seconds; nothing here tolerates T+1 |
| 2 | Data volume | **Kappa** | High | 3,840 events/simulated day; full replay in seconds |
| 3 | Complexity | **Kappa** | **Decisive** | One clinical rule — a patient-safety property, not just tidiness |
| 4 | Historical data analysis | *Lambda* | — | **Conceded.** Our historical need is bounded; evolution path is "Kappa + archive", not Lambda |
| 5 | Cost | **Kappa** | Medium | Architecturally cheaper; operationally more demanding — the module contradicts itself and we reconcile it |
| 6 | Fault tolerance | *Lambda* | — | **Conceded.** "Depends on stream integrity" — mitigated by checkpointing, replication, late-data side-processing, and liveness alerting |
| 7 | Data reprocessing | **Kappa** | **Decisive** | Clinical rules change; replay from offset 0 re-derives all history |
| 8 | Accuracy | **Kappa** | Medium | Consistency beats peak accuracy when the number is clinical; NEWS2 is discrete and threshold-based |

**6 of 8 favour Kappa, including all three decisive ones. The 2 that favour Lambda are conceded openly and mitigated concretely.**

---

## 4. The rejected alternative: Lambda

### 4.1 The honest case FOR Lambda here

Make this properly before rejecting it:

- **More accurate nightly recomputation.** A batch layer could recompute each patient's daily summary exactly from a complete day of data, including every late reading, with no watermark cut-off at all.
- **Better long-horizon history.** A warehouse of admissions would support cohort analysis, outcome studies, and ML training that our 30-day log cannot.
- **The module's own guidance on historical analysis and fault tolerance points this way** (criteria 4 and 6), and we should not pretend otherwise.
- **The lab file genuinely is a daily batch source.** A batch layer would have somewhere natural to put it.
- **The module is sceptical of Kappa**: *"hasn't seen widespread adoption due to streaming complexity and cost."* Choosing it means going against the material's own stated caution, which requires justification rather than enthusiasm.

### 4.2 Why it lost

1. **Two implementations of one clinical rule is a patient-safety hazard**, not a maintenance cost (§1.1). This alone is close to decisive, and nothing in Lambda's favour outweighs it.
2. **The batch layer would have nothing to do that the stream cannot do.** At 3,840 events per simulated day there is no volume that requires processing at rest. A batch layer would exist purely to satisfy an architectural pattern — the definition of over-engineering.
3. **It adds T+1 latency to a use case defined by immediacy** (§2.1), including to the lab-corroboration half of the question.
4. **Reprocessing becomes harder, not easier.** The rule-change scenario (§2.7) is the most likely reprocessing trigger in this domain, and under Lambda it must be executed across two codebases simultaneously.
5. **The historical requirement that motivates Lambda is not actually present.** We need 4 hours of trend and a replayable log, not a warehouse — and if that changes, the right answer is an archive sink, not a second processing path (§2.4).

### 4.3 What would change our minds

- Continuous high-frequency waveform capture (1 Hz or faster) across many wards, pushing daily volume into the hundreds of millions — replay stops being cheap and the module's volume criterion flips.
- A requirement for multi-year retrospective research over the same data.
- A regulatory requirement for an independently-recomputable audit record of every score, derived by a separate implementation as a cross-check. (Note: this would argue for *dual computation as a control*, which is the one context where duplicated logic is a feature rather than a hazard.)

---

## 5. Viva defence — expected attacks and answers

| Attack | Answer |
|---|---|
| **"Your lab results arrive once a day as a file. That IS a batch source. You've built Lambda and called it Kappa."** | The distinction is the number of **processing paths that produce the answer**, not the number of *ingest modalities*. The module defines Kappa as the stream platform being the backbone for *"ingestion, storage, and serving"* — the lab file is **ingested** by a thin Airflow-orchestrated producer that publishes rows to a compacted topic and then stops. **Zero business logic lives there.** Every derived number — trends, scores, tiers, the daily report — comes from the one streaming job. Grep `orchestration/` for a scoring function: there isn't one, and `test_single_scorer.py` enforces that. |
| **"Why is Airflow in a Kappa system at all?"** | Because ingesting a once-daily file still needs scheduling, sensing, retries, validation branching and failure alerting — and because replay runs and report rendering need orchestrating. The module is explicit: *"Airflow only orchestrates."* It orchestrates **around** the stream, never as a parallel path to it. |
| **"Your own module says Kappa is weaker at historical analysis. You need historical trends. Contradiction?"** | Not a contradiction — a bounded requirement. We need a 4-hour trend window and a 24-hour report view, both served from stream state and Cassandra, plus replay for rule changes. We do **not** need a warehouse. And we state the evolution path: if long-horizon analytics is ever required, add an archive sink, keep one processing path. §2.4. |
| **"Kappa depends on stream integrity. What happens when the stream breaks?"** | The mitigation table in §2.6, and then the point that matters: we treat *pipeline silence* as a critical clinical alert, because in this domain a stopped monitor looks exactly like a healthy ward. `WardMonitoringSilent`, 3-minute detection, demonstrated live with `make chaos-kill-monitors`. |
| **"Show me that there's really only one implementation of the score."** | Open `ward/clinical/news2.py`. Open `tests/unit/test_news2.py` — it validates against the published NEWS2 scoring table as fixtures. Then run `test_single_scorer.py`, which walks the tree and fails if any other module computes a score. Then show the daily-report DAG reading pre-computed rows from Cassandra rather than recomputing. |
| **"How do you know the replay produced correct results?"** | v1 and v2 rows coexist in the same Cassandra partition, keyed by `scorer_version`. The comparison endpoint and Grafana panel show them side by side, and the replay job emits a diff summary: how many patient-hours changed tier, in which direction. We verify before cutting over — that verification step *is* the safety mechanism, and it's the reason versioned output matters. `06 §4`. |
| **"Your module taught Storm for streaming and Spark for batch. Why Spark Structured Streaming?"** | `02 §3`. Short version: the module taught the stream *theory* (event time, watermarks, tumbling/sliding windows) in the Storm deck and Spark's engine in the Spark deck; Structured Streaming is where they meet, and `withWatermark`/`window` implement the taught concepts directly. Storm was rejected because the module showed no Python path for it and our whole stack is Python. |
| **"13 events per second. Why not a single Python script?"** | Fair at demo scale, and we say so in the limitations. But a Python script gives no windowing, no watermark semantics, no checkpointed state recovery, no replay parallelism, and no path to more wards. The architecture is chosen for the ward-network scale the scenario implies, not the 40 beds we simulate. |

---

## 6. What goes in the report (§3), and at what length

Target **≈ 3 pages** — the largest section, proportional to its 20 marks.

- **3.1 Candidate architectures** (~0.4 p) — both described fairly in the module's vocabulary, with both diagrams.
- **3.2 Decision criteria applied** (~1.4 p) — the module's five criteria as sub-headings (§2.1–2.5), then a table covering fault tolerance, reprocessing and accuracy (§2.6–2.8).
- **3.3 Decision** (~0.4 p) — the summary table, then the single strongest paragraph: **one clinical rule, one implementation, one number** — plus replay as the operational capability that matters most in a monitoring system.
- **3.4 Rejected alternative: Lambda** (~0.5 p) — §4.1 and §4.2 in full.
- **3.5 Conceded weaknesses and mitigations** (~0.3 p) — historical analysis (bounded + evolution path) and stream integrity (the mitigation table + the silence alert).
- **3.6 Reconciling the module's internal contradiction on Kappa's cost/complexity** (~0.2 p) — §2.5. Short, and few reports will do it.

Do not pad with generic Lambda/Kappa background. Every paragraph should reference either the module's material or this specific ward.

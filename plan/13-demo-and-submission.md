# 13 — Demo Video & Submission

> **PDF:** *"Include a short (5–10 minute) demo video OR be prepared to demo live, showing the pipeline running end-to-end and the **observability results**."*

---

## 1. Demo script — 9.5 minutes

Run `make clean && make up` about 6 minutes before recording (Cassandra is slow to start) so simulated day 1 is underway. Record at 1920×1080.

| Time | Segment | What to show | What to say |
|---|---|---|---|
| **0:00–0:45** | **The question** | The business question on a slide | "Both halves of this sentence ask for the same thing: one patient's current risk. The labs don't produce a second answer — they modify the same one. If one quantity is produced, one pipeline should produce it. That's Kappa." |
| **0:45–1:30** | **Architecture** | Diagram D2 | Walk the single path using the module's names — the stream platform as backbone for ingestion, storage and serving. **Point at the single arrow into the scoring box.** State the clock: 1 simulated day = 5 real minutes, 288×. |
| **1:30–2:15** | **Ingestion** | Kafka UI: topics with **cleanup policies visible** — `vitals.readings` on `delete` with 30-sim-day retention, `labs.results` and `ward.admissions` **compacted** | "Retention isn't a tuning knob here — it *is* our historical store. And compaction turns the labs topic into a table: latest result per patient per test." Show the `patient_id` keying and per-partition counts. |
| **2:15–3:00** | **The single path** | Spark UI Structured Streaming tab; then `ward/clinical/news2.py` on screen; then run `pytest tests/unit/test_only_one_scorer_exists.py` | "One job. One scoring file, tested against the published NEWS2 table. And a test that fails the build if a second implementation ever appears — that's the architecture argument enforced in CI." |
| **3:00–4:00** | **★ Deterioration ★** | Ward Monitor: `P014` climbing 1 → 5 → 7; the alert feed escalating `LOW → MEDIUM → HIGH` | "Scripted and seeded — same every run. Note this is a *trend* over a four-hour sliding window, not one bad reading. Patient `P007` has a single spike and correctly does **not** alert." |
| **4:00–4:45** | **★ The lab join ★** | Lab file landing → Airflow `ward_lab_ingest` green → `/api/v1/patients/P014` showing `news2_total: 7`, `lab_contribution: 3`, `composite_risk: 10`, and the `explanation` string | **"This is the business question, answered in one object.** NEWS2 7 raised to composite 10 by lactate 3.8 and WBC 18.4. The vitals said deteriorating; the labs said why." |
| **4:45–5:15** | **The daily report** | The generated PDF — ward summary, patients by peak risk, **the lab-corroboration table**, stale-labs section | "The report reads pre-computed rows. It recomputes nothing — which is why it can never disagree with the ward screen." |
| **5:15–7:15** | **★★ THE REPLAY ★★** | `make replay VERSION=v2` → **Kafka UI with two consumer groups**, one at latest, one racing from offset 0 → Replay Comparison dashboard → the diff summary → `make cutover VERSION=v2` → `P031` drops `MEDIUM → LOW` | **The two most important minutes.** "NEWS2 added a separate SpO2 scale for COPD patients. Same job, same file — different version flag, different consumer group, starting at offset 0. 115,000 events re-derived in N seconds. 3.3% of scores changed: all six COPD patients, none of the other 34. 180 false alerts removed, zero alerts lost. **Verified before cutover — and the DAG deliberately stops for human sign-off, because you don't auto-deploy a clinical scoring rule.**" |
| **7:15–8:30** | **Observability** | Pipeline Health dashboard → `make chaos-kill-monitors` live → **the ward monitor visibly empties** as TTLs expire → `WardMonitoringSilent` critical in Alertmanager → restart → recovery. Then a Jaeger waterfall. | **"This is the design principle of the whole observability layer: a frozen screen showing 40 stable patients looks exactly like a healthy ward. So the snapshot table has a 120-second TTL — when the stream dies, the screen empties. Absence of alerts must never be mistaken for absence of risk."** Then state the tracing limitation honestly. |
| **8:30–9:30** | **Reproducibility & close** | README quick start; `make demo`; `make test` with the `archrules` job | "Fresh clone, five commands, seeded so it's identical every run." One honest sentence on the biggest limitation — the bitemporal enrichment problem, or single-node Cassandra. |

### Recording notes
- **Rehearse on day 11 with a timer.** The deterioration and the lab file both fire on simulated time — know exactly when.
- The replay segment is the highest-value 2 minutes in the video. If anything is cut for time, cut elsewhere.
- Increase terminal and browser font size.
- Narrate *decisions*, not clicks.
- If a segment fails live, cut and re-record that segment. A polished 9.5 minutes beats an authentic 14.

---

## 2. Submission checklist

**Codebase**
- [ ] All source: producers, the stream pipeline, clinical rules, store, API, replay, reporting
- [ ] Observability config: `prometheus.yml`, `alerts.yml`, `alertmanager.yml`, Grafana provisioning, OTel config
- [ ] `compose.yaml` + `.env.example`, all image tags pinned
- [ ] `schema.cql` and the topic-creation script
- [ ] README with the **safety disclaimer prominent**
- [ ] Tests green, including the `archrules` set
- [ ] `plan/` and `docs/adr/` included — they evidence that decisions preceded code
- [ ] `.env` not committed; no credentials in history
- [ ] Tagged `v1.0-submission`

**Report**
- [ ] PDF, 8–15 pages, all mandated sections
- [ ] Diagrams vector and legible
- [ ] Screenshots from the real run, including the replay
- [ ] Limitations honest — bitemporal problem, unvalidated lab rules, RF=1 under Kappa
- [ ] Individual contributions statement

**Demo**
- [ ] 5–10 minute video (target 9.5), audio clear, text legible
- [ ] Shows end-to-end operation, **the replay**, and observability results
- [ ] Link works from an incognito window

**Assumptions declaration**
- [ ] Simulated clock in README and report
- [ ] Scope adaptation (NEWS2 fields added) declared
- [ ] Seeded scenarios (`P014`, `P031`, `P007`, the day-5 missing lab file) documented as scripted
- [ ] Retention reduction declared

---

## 3. Viva preparation

Be able to open and explain without hesitation:

1. `ward/clinical/news2.py` — the scoring table, the `any_parameter_is_3` rule, and the v1/v2 difference
2. `tests/unit/test_news2.py` — validated against the **published** specification, not against our assumptions
3. `tests/unit/test_only_one_scorer_exists.py` — **run it on screen** when asked to prove the single-rule claim
4. `ward/stream/pipeline.py` — the full chain, and where the watermark is set and why in simulated time
5. `ward/stream/lab_join.py` — why broadcast reload beats a stream-stream join here
6. `ward/store/schema.cql` — `risk_score` in the clustering key; `sim_date` in the partition key; **TTL instead of tombstones**
7. `ward/replay/replay_runner.py` — and why `scorer_version` is in the primary key
8. `observability/prometheus/alerts.yml` — `WardMonitoringSilent` first, and why it is critical

Rehearse the attack/answer table in `01 §5` aloud, especially:
- *"Your lab file is a batch source — you've built Lambda and called it Kappa."*
- *"Your module says Kappa is weak at historical analysis and you need trends."*
- *"Prove there's only one implementation of the score."*

Also be ready for *"what would you do with one more week?"* — a good answer names the bitemporal enrichment model and a proper clinical-validation harness, and explains why neither made this cut.

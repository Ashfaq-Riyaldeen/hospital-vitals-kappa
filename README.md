# Hospital Patient Vital Signs Monitoring

**EC8203 Applied Big Data Engineering — Mini Project (Use Case 2)**
**Architecture: Kappa** — a single stream-processing path; the log is the backbone.

---

> ## Safety disclaimer
>
> This system processes **entirely simulated data** and is a teaching exercise. NEWS2
> is implemented from its published specification for realism, but this software is
> **not a medical device**, has not been clinically validated, and must not be used for
> any clinical purpose. No real patient data is involved and no patient identifiers
> exist.

---

## The business question

> *Which patients show concerning vital-sign trends **right now**, and how do
> **yesterday's lab results** change the risk picture for those patients **going
> forward**?*

Both halves are about **one number per patient: their current risk**. The lab results
do not produce a separate answer — they *modify* the same answer. That is why this is
Kappa and not Lambda: splitting that computation across a speed layer and a batch layer
would mean two implementations of one clinical rule, and two implementations of a
clinical rule is not a maintenance inconvenience but a patient-safety hazard.

Full argument: [`plan/01-architecture-decision.md`](plan/01-architecture-decision.md).

## Non-negotiables

1. **One clinical rule, one implementation.** `ward/clinical/news2.py` is the only place
   a risk score is computed. `test_only_one_scorer_exists` enforces it.
2. **Airflow contains no transformation logic** — sensing, producing, triggering,
   rendering, nothing else. A test enforces it.
3. **Kafka retention is the historical store** — 30 simulated days. That is what makes
   replay possible, and the report says so explicitly.
4. **Silence is not safety.** `WardMonitoringSilent` must fire when the stream stops. In
   this domain, absence of alerts must never be mistaken for absence of risk.

## Running it

```bash
make up          # Kafka (KRaft), Schema Registry, Kafka UI, producers, Cassandra
make topics      # topic list with cleanup policies
make test        # unit + contract tests (no Docker needed for the clinical tests)
make clean       # stop and delete all data
```

### One stack at a time

This host has 15.6 GB of RAM and Docker realistically gets ~11 GB. The sibling project
`../ride-hailing-lambda` peaks around 10 GB. **Do not run both stacks together** — bring
one down before starting the other. The two use distinct port ranges (this project on
91xx / 81xx / 31xx) so they cannot collide, but they cannot coexist in memory.

## Simulated clock

One simulated day is compressed into **300 real seconds** — a speed-up of **288×**.
Every container derives simulated time from one shared anchor file written at start-up,
so no two services can disagree about what day it is. Throughout the code and report,
durations are marked `sim_` or `real_`; a 60-simulated-minute watermark is 12.5 real
seconds.

## Layout

| Path | Stage |
|---|---|
| `ward/producers/` | ingest — bedside monitors, lab uploader, admissions |
| `ward/stream/` | process — **the single streaming path** |
| `ward/clinical/` | ★ the one clinical rule ★ |
| `ward/store/` | store — Cassandra, query-first tables |
| `ward/api/` | serve — FastAPI ward endpoints |
| `ward/replay/` | the Kappa capability — re-derive history under a new rule version |
| `plan/` | 16 planning documents; start at `plan/README.md` |

## Team

| Member | GitHub | Area |
|---|---|---|
| Munsif | `munsif-dev` | Clinical rule, core foundations, contracts, stream pipeline |
| Ashfaq | `Ashfaq-Riyaldeen` | Infrastructure, producers, storage, orchestration |
| Lareef | `Lareefmohamed` | Scaffold, tooling, documentation, reporting support |

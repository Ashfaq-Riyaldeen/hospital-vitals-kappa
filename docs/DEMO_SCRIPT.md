# Demo script (about 10 minutes)

Commands, what to show, and what to say. Every number here was measured in the final
clean run (`docs/evidence/rerun-measurements.md`); a new run will give similar, not
identical, numbers.

## Before recording

```bash
make clean && make up        # everything, from nothing (about 2 minutes)
make ports                   # where each UI lives
```

| UI | Address |
|---|---|
| Kafka UI | http://localhost:8180 |
| FastAPI docs | http://localhost:8100/docs |
| Airflow (admin / admin) | http://localhost:8182 |
| Grafana | http://localhost:3100 |
| Prometheus | http://localhost:9190 |
| Alertmanager | http://localhost:9193 |
| Spark UI (live / replay) | http://localhost:4140 / 4141 |

Timing: one simulated day is 5 real minutes. P014's sepsis starts about 6 min 40 s
after `make up` (day 2, 08:00) and reaches NEWS2 7 about 40 seconds later. Start
recording after that.

## 0:00 - 1:15 The question and the decision

Show `docs/diagrams/D2-layered-architecture.pdf`.

> "Use case 2: a 40-bed ward. Which patients are getting worse right now, and how do
> yesterday's blood tests change that? We chose Kappa. The score must be the same on
> the ward screen and in the daily report, so the NEWS2 rule exists once, in
> `ward/clinical/news2.py`, and a test fails if any other file contains a scoring table.
> Kafka keeps 30 simulated days, so re-scoring the past is a replay of the log through
> the same code."

## 1:15 - 2:45 The ward, sickest first

Grafana `Ward - Clinical Monitor`, then:

```bash
curl -s localhost:8100/api/v1/ward/WARD-A/monitor | jq '.patients[0:3]'
```

> "P014 is at the top: NEWS2 10, and the day-2 lab results add 4, so composite risk 14.
> Cassandra keeps the ward snapshot ordered by risk inside one partition, so this is one
> read. The snapshot rows expire after two minutes: if the stream stops, this list
> empties instead of freezing."

## 2:45 - 4:00 The daily lab file

Airflow `ward_lab_ingest` grid, then the daily report PDF in `reports/`.

> "The lab drops one file per simulated day. Airflow checks the checksum, validates each
> row, and publishes it to a compacted Kafka topic, or moves a bad file to quarantine.
> The red runs are the scripted bad days: day 4 had a corrupted reference range, day 5
> never arrived. Airflow never calculates a score; the stream joins the labs to each
> reading. The daily report is rendered from the stream's daily summaries."

## 4:00 - 6:30 Changing the rule: the replay

```bash
make replay VERSION=v2       # starts ward-stream-v2 from offset 0 and waits
make diff
```

Show Grafana `Ward - Replay v1 vs v2` and the Spark UI on port 4141.

> "Version 2 uses oxygen Scale 2 for COPD patients. `ward-stream-v2` is the same image
> and code, from offset 0, with its own checkpoint. It re-scored 15,800 readings in
> about two minutes. Result: 4,001 readings changed, all of them COPD patients; zero
> changes for the other 30 patients. 413 changes went up, and every one is a COPD patient
> on oxygen at 93 % or more, which Scale 2 scores on purpose. P031 lost both of its
> emergency alerts."

Then the `ward_replay` DAG in Airflow: it runs the same check and stops at the approval
step.

## 6:30 - 7:30 The switch

```bash
make cutover VERSION=v2
curl -s localhost:8100/api/v1/ward/WARD-A/monitor | jq '.scorer_version'
make rollback
```

> "The switch is one API call. P031 goes from 8 to 5, P014 stays at 10. Rolling back is
> the same call with v1, because the v1 rows were never touched."

## 7:30 - 9:00 A silent failure

```bash
make chaos-stop-stream
```

Prometheus Alerts, then Alertmanager, then Grafana `Ward - Pipeline Health`.

> "A dead monitoring pipeline looks exactly like a calm ward. We stop the stream.
> Readings keep arriving, scores stop. WardMonitoringSilent goes pending after about a
> minute and fires after about two, and Alertmanager sends it both to the ward and to the
> engineer."

```bash
make chaos-heal
```

> "It resolves about 40 seconds after the restart. The stream resumes from its
> checkpoint; no reading is lost."

## 9:00 - 10:00 Close

> "Every result was checked against an answer we knew in advance: 40 patient keys, every
> injected fault in the dead-letter queue under its own reason, P014 caught, P007's single
> spike ignored, only COPD patients changed by the new rule. Doing that found a series of
> silent bugs, which the report lists with their fixes. 533 automated tests pass."

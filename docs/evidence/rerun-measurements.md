# Measurements from the final clean run

Run: 27 September 2026. `make clean`, then `docker compose up`, then the stack started at
07:03:23 UTC (simulated 2026-04-01 00:00; one simulated day = 5 real minutes). Every
number below comes from this run. The report (`docs/report/measurements.tex`) quotes
these values.

## Ingestion

| Measure | Value | How |
|---|---|---|
| Readings delivered to Kafka | 13.3 per second (expected about 12.8) | `sum(rate(readings_produced_total{status="ok"}[5m]))` |
| Scores written by the v1 stream | 13.0 per second | `sum(rate(risk_scores_written_total{scorer_version="v1"}[5m]))` |
| Distinct patient keys on `vitals.readings.v1` | 40 | `make keys` |
| Topic cleanup policies | labs and admissions `compact`, the rest `delete` (30 sim days) | `make topics` |

## Fault injection against the dead-letter queue

The monitors' injected-fault counter was read first. 20 seconds later the dead-letter
topic was read in full and counted by distinct Kafka position (partition, offset).

| Reason | Injected (snapshot) | In the DLQ 20 s later |
|---|---|---|
| HR_OUT_OF_RANGE | 86 | 87 |
| FUTURE_TIMESTAMP | 20 | 20 |
| SPO2_PROBE_DETACHED | 124 | 128 |
| TEMP_OUT_OF_RANGE | 50 | 51 |
| EMPTY_READING | 75 | 78 |
| BP_INVERTED | 51 | 51 |
| total impossible | 406 | 415 |

Every injected fault reached the DLQ under its own reason; the DLQ is a little ahead
because injection continued during the 20 seconds. No reason appears in the DLQ that was
not injected, so no real reading was rejected. 9 duplicate readings were also injected;
they are skipped by id, not dead-lettered. The DLQ holds 400 messages for 400 distinct
readings (the replay no longer writes a second copy). Fault rate: 406 of 22,160
readings, 1.8 %.

In the previous run BP_INVERTED showed 63 dead letters against 47 injected: 16 were real
P014 readings made impossible by the sepsis script. That was fixed before this run.

## Latency and throughput

| Measure | Value | How |
|---|---|---|
| End-to-end, monitor to Cassandra, p50 | 4.6 s | `histogram_quantile(0.5, ... stream_end_to_end_seconds_bucket ...[10m])` |
| End-to-end p95 | 7.3 s | same, 0.95 |
| Micro-batch duration (average) | 2.5 s | `avg_over_time(stream_batch_duration_seconds[10m])` |
| API ward list, 50 requests | p50 12.7 ms, p95 28.9 ms | timed `GET /api/v1/ward/WARD-A/monitor` |
| Memory, whole stack incl. both streams | 5.9 GB | `docker stats` |

## Scripted patients

| Patient | Result |
|---|---|
| P014 (sepsis from day 2, 08:00) | TREND_CONCERNING 09:55, NEWS2_MEDIUM 10:25, RAPID_DETERIORATION 10:40, NEWS2_HIGH 11:03 (3 h 3 min after onset, about 38 real seconds). One NEWS2_HIGH in that episode. |
| P007 (one heart rate of 145 on day 1, 14:00) | One reading of 145 (14:00), then 83. No alert within 2 simulated hours. Its only day-1 alert is a NEWS2_MEDIUM at 06:20, unrelated. |
| P031 (COPD, SpO2 89-91 %) | NEWS2 about 3 lower under v2 on average; emergency (NEWS2 7+) alerts 2 under v1, 0 under v2. |

## Outage test (`make chaos-stop-stream`)

Stream stopped at simulated day 3, mid-morning.

| Measure | Value |
|---|---|
| WardMonitoringSilent pending | 56.5 s after the stop |
| WardMonitoringSilent firing | 113.9 s after the stop |
| Alertmanager receivers | `ward-safety` and `pipeline-oncall` |
| Resolved | 36.6 s after the restart (about half is Spark starting) |
| Readings routed to `vitals.late` by the backlog | 0 |
| Day-3 daily summary after the restart | 99 readings per patient under both v1 and v2 (the restart carried on from the stored summary) |

`StaleLabDataElevated` fired on its own later in the run: the day-4 lab file was
quarantined (corrupted reference range) and the day-5 file was withheld, as scripted.

## Replay (`make replay`, then `make diff`, then the `ward_replay` DAG)

| Measure | Value |
|---|---|
| Readings in the log at the start | 15,800 |
| Time for ward-stream-v2 to catch up | 126 s |
| Readings scored by both versions (Airflow run, 07:25 UTC) | 17,822 |
| Changed | 4,001, all COPD patients |
| Changed, non-COPD patients | 0 |
| Upward changes | 413, all COPD patients on oxygen at SpO2 93 %+ |
| Upward changes without that explanation | 0 |
| Readings that fall below NEWS2 5 under v2 | 1,035 |
| Readings that rise to NEWS2 5+ under v2 | 153 |
| `ward_replay` DAG | success; stops at the approval step |
| Cutover and rollback through the API | P031 served at 8 under v1 and 5 under v2; P014 at 10 under both |

The earlier `make diff` run (a few minutes before the Airflow run, so a little less
data): 17,471 compared, 3,924 changed, all COPD, 0 control changes, 404 upward, all
explained. The full diff is in `replay-diff-final-run.md`.

Alert episodes per version over the same days: P031 27 under v1 and 34 under v2 (no
margin before an episode closes, so a score near a threshold reopens it); P014 7 under v1
and 5 under v2 with identical scores (the v1 stream restarted in the outage test and
re-raised episodes that were already open).

## Tests

533 unit and contract tests passing; ruff clean; mypy clean on `ward/clinical`,
`ward/simclock.py`, `ward/store/dao.py` and `ward/stream`.

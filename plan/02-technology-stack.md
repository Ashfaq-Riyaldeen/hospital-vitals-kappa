# 02 — Technology Stack Selection & Justification

> **Rubric weight: 10 marks.** Assessed on *"appropriateness of chosen tools for each layer; justification tied to use-case constraints rather than **generic popularity**."*
>
> The test for every paragraph below: **would this justification read identically for the ride-hailing use case?** If yes, it is a description, not a justification. Rewrite it.

---

## 1. Ingestion & backbone — Apache Kafka (KRaft mode)

**Image:** `confluentinc/cp-kafka:7.6.1` · single broker, KRaft (no ZooKeeper)

### Why Kafka is not merely the ingestion layer here

In the sibling Lambda project, Kafka is a buffer with 7-day retention and the historical store is elsewhere. **Here Kafka *is* the historical store.** The module's Kappa definition — *"stream-processing platform as the backbone for **all** data handling — ingestion, storage, and serving"* — makes retention an architectural decision rather than an operational setting.

| Constraint in this use case | How Kafka satisfies it |
|---|---|
| **The log must be replayable to re-derive history** when a clinical rule changes | The module: a stream is *"an append-only, ordered log of event records that are **persisted over a longer duration**"* and an event-streaming platform provides an *"ordered, **replayable** log of records"*. Retention of 30 simulated days makes replay from offset 0 a supported operation, not a recovery hack. |
| **Trend detection is a per-patient stateful computation** — "is this patient deteriorating" needs readings in order | Ordering is guaranteed *within a partition*. Keying by `patient_id` co-locates all of one patient's readings. Out-of-order vitals would produce a nonsense trend slope. |
| **Lab results are a slowly-changing table, not a stream of events** | **Log compaction** — *"keeps only the latest record per key"* — materialises the topic as "latest result per patient per test type". Kafka becomes, in the module's words, *"a durable state store"*. |
| **A bedside monitor may lose connectivity and reconnect** | The producer buffers locally and replays; the broker's durability means nothing is lost in the gap. |
| Decoupling | The module's *"decoupled pipeline paradigm"*: monitors know nothing about scoring, storage or the API. |

### Alternatives considered

| Option | Why rejected |
|---|---|
| **RabbitMQ / classic queue** | Fatal for us: messages are *"removed from a queue once delivered and consumed"*, so **replay is impossible** — and replay is the capability the entire architecture is built around. The module makes exactly this point: *"reprocessing old messages is hard → no easy replay."* |
| **MQTT broker** (the "obvious" IoT/medical-device choice) | Genuinely the right protocol for the *device edge*, and in a real deployment MQTT would sit in front of Kafka via a bridge. Rejected as the backbone because MQTT brokers are transport, not storage — no retention, no offsets, no replay. Worth mentioning in the report as the production topology we would actually build. |
| **Redis Streams** | Lighter, has consumer groups and retention. Rejected: weaker durability guarantees for a system of record, no compaction semantics for the labs table, no Schema Registry integration, and the module taught Kafka. |
| Writing straight to Cassandra from the monitors | No replay, no decoupling, no buffer, and the scoring logic would have nowhere to live. |

### Trade-off accepted
Single broker, `replication.factor=1` — **and under Kappa this is a more serious simplification than it would be under Lambda**, because there is no second copy of the data anywhere. We say so plainly in the limitations, and give the production answer (RF=3, `min.insync.replicas=2`, multi-AZ).

### KRaft vs ZooKeeper
The module states Kafka used ZooKeeper *"before version 2.8"*. We run 3.x in KRaft mode, so no ZooKeeper ensemble appears in our compose file despite the lecture diagram showing one. Declared in the report and README as an informed divergence.

---

## 2. Stream processing — PySpark Structured Streaming (not Storm)

**Image:** `bitnami/spark:3.5.1`

The assignment allows *"Apache Spark (Structured Streaming) OR Apache Storm"*, and the module taught Storm as its streaming engine — so this needs a real defence.

### Why Spark Structured Streaming

1. **The stream theory the module taught maps 1:1 onto this API.** Event time vs processing time, watermarks (*"I am reasonably confident that no more events with a timestamp earlier than X will arrive"*), and tumbling/sliding/session/global windows were all taught in the Storm deck — but as *concepts*, with no code. Structured Streaming's `withWatermark()` and `window()` are the most direct executable expression of those concepts available. The report makes this bridge explicitly: we are implementing the taught theory using the engine whose API states it most plainly.

2. **We need windowed aggregation and stateful trend detection, not per-tuple routing.** Storm's model — *"topologies, spouts, bolts"*, event-by-event — is optimised for per-event low latency. Our unit of clinical meaning is **a trend over a window**, not a single reading: one high heart rate is noise, a rising heart rate with falling blood pressure over four hours is sepsis. A windowing engine is the right shape for this problem; a tuple-routing engine is not.

3. **Latency budget makes the throughput/latency trade irrelevant here.** The module's framing: Storm gives *"instant results but creates more overhead"*; Spark batches for throughput at the cost of the first record waiting. Our vitals arrive every 15 *simulated* minutes and our detection budget is seconds. A 2-second micro-batch is far inside budget. **Storm's advantage costs us nothing to give up.**

4. **The module gave a PySpark path and no Storm-in-Python path.** The only external API link in the entire deck set is the PySpark documentation. Storm was taught without code, without grouping types, without Trident, and without any Python integration. Our stack is Python end to end; adopting Storm would mean inventing a multi-language integration the module never demonstrated, on a two-week deadline, for no benefit.

5. **Checkpointed stateful recovery.** If the job restarts, per-patient trend state is restored. Under Kappa that matters more than usual — the stream job's state is part of the record.

### Alternatives considered

| Option | Why rejected |
|---|---|
| **Apache Storm** | See above. Wrong shape for window-based clinical trends; no Python path in the module. We concede in the report that if the requirement were *"alert within 50 ms of an arrhythmia detected in a 250 Hz ECG waveform"*, Storm's per-event model would win outright — that is a real and different problem. |
| **Kafka Streams** | The module calls it a *"**Java** library"*. Breaks a Python stack. Would otherwise be a very natural fit for a Kappa design. |
| **ksqlDB** | Could express the windowed aggregations in SQL, elegantly. Cannot express NEWS2's multi-parameter threshold table cleanly, cannot be unit-tested against published reference data, and cannot be replayed under a version flag as easily. **Testability of the clinical rule is a hard requirement here**, and that decides it. |
| **Apache Flink** | Technically the strongest alternative — true event-at-a-time with the best watermark semantics available, and a good Python API. Rejected on defensibility: not taught in this module, and the viva is on this module's material. Named in the report as what we would evaluate first at production scale. |
| **Plain Python consumer** | Would work at 13 events/second and be trivially defensible line-by-line. Rejected because it demonstrates none of the module's processing outcomes — no windowing, no watermarking, no checkpointed state, no distributed replay — and offers no path to a ward network. |

### Trade-off accepted
Micro-batch latency floor of ~1–2 seconds, and checkpoint invalidation when the aggregation logic changes (a real operational cost, documented in `05 §6`).

---

## 3. Serving store — Apache Cassandra

**Image:** `cassandra:4.1` · single node, heap pinned

This is the choice that most distinguishes this project, and the justification must be about *this* workload.

### Why Cassandra

The module's own description of the wide-column family: *"Petabyte-scale, low-latency reads/writes using **column families**; fast by row key but limited complex queries."* Every clause of that is load-bearing here.

| Constraint in this use case | How Cassandra satisfies it |
|---|---|
| **Write-heavy, append-only, never updated.** Every vitals reading and every score is a new immutable fact. Nothing is ever edited. | Cassandra's LSM-tree storage engine is optimised for exactly this: sequential writes, no read-modify-write. This is its single best-fit workload. |
| **The dominant query is "the last N readings/scores for one patient, newest first"** | Partition by `patient_id`, cluster by `reading_time DESC`. All of one patient's history is contiguous on one node, pre-sorted. The query is a **single sequential read** with no sort and no scan. This is the textbook wide-column access pattern. |
| **Ward-level retention policy** — a ward keeps observations for a defined window, then discards | **Native per-row TTL.** We never write a deletion job, and rows expire themselves. This maps directly onto the module's *"data retention policies... balancing value against storage costs and compliance mandates"*. |
| **All aggregation already happened in the stream** | Cassandra's inability to aggregate or join is **not a limitation for us** — it is a non-requirement. Under Kappa, the store holds *materialised views of stream output*, shaped per query. The architecture choice and the store choice reinforce each other. |
| Scale-out path for a ward network | Add nodes; `patient_id` is a naturally high-cardinality, evenly-distributed partition key with no hotspotting risk. |

**The reinforcement point is the strongest thing to say in the viva:** *"Cassandra's weaknesses are exactly the operations Kappa has already performed upstream. Under Lambda, where the batch layer's job is joining and aggregating, this store would be the wrong choice — and indeed the sibling project correctly chose PostgreSQL for precisely that reason. Same taxonomy, opposite answer, driven by the architecture."*

### Alternatives considered

| Option | Why rejected |
|---|---|
| **PostgreSQL / TimescaleDB** | TimescaleDB is genuinely excellent for vitals time-series — hypertables, continuous aggregates, retention policies. Rejected because **continuous aggregates would recompute aggregations in the database**, i.e. a second place where derived clinical values are produced. That is a second processing path in all but name, and it would undermine the one-rule property that motivated the whole architecture. Cassandra cannot do that, which here is a *feature*: it enforces that all computation stays in the stream. |
| **Plain PostgreSQL** | Fine at 40 beds. Rejected on write-pattern fit and on the retention story (a cron `DELETE` job vs native TTL), and because it offers no scale-out path for a ward network. Also: the sibling project uses it, and choosing it here would mean the two projects' storage decisions were not actually reasoned. |
| **Redis** | Excellent for the *current* ward snapshot, poor for 30 simulated days of per-patient history — that would be memory-resident data with no durability story for a system of record. Considered as a *supplement* (Cassandra + Redis) and rejected on stack weight; the current-snapshot query is a single-partition Cassandra read and is already fast. |
| **Elasticsearch** | Good for alert search and free-text. Overkill; our queries are all key-based. |
| **InfluxDB** | Purpose-built for time-series and a strong fit. Rejected because it was not taught in the module (Cassandra was, as the canonical wide-column store) and because it adds no capability we need over Cassandra here. |

### Weaknesses we state honestly in the report

Naming these is worth marks; being caught not knowing them is expensive:

1. **No joins.** The vitals ⨝ labs join happens in Spark, before the write. Correct under Kappa, but it means the store cannot answer a question nobody anticipated.
2. **No ad-hoc aggregation.** Every query must be designed in advance and given its own table — **query-first modelling**. New question ⇒ new table ⇒ replay to backfill it. That is a real cost, and replay is what makes it bearable.
3. **`ALLOW FILTERING` is a trap** — it turns a query into a full cluster scan. **It is banned in this codebase**, and a test asserts no CQL string in the repo contains it.
4. **Tombstones.** Deletes create tombstones that degrade reads. We avoid deletes entirely by using **TTL instead** (`07 §3`) — which is the elegant answer and a good detail to have in the report.
5. **Denormalisation.** The same fact is written to several tables. Accepted: writes are cheap, and this is the intended Cassandra design idiom rather than a compromise.
6. **A single node defeats Cassandra's own purpose** (no replication, no tunable consistency, no failover). Declared as a demo simplification with the production answer: 3+ nodes, RF=3, `LOCAL_QUORUM`.

---

## 4. Orchestration — Apache Airflow

**Image:** `apache/airflow:2.9.3-python3.11`

### Why Airflow in a Kappa system

This needs its own defence, because a naive reading says orchestration belongs to batch architectures.

The module: *"**Airflow only orchestrates.** NOT a data processing engine, NOT a database, NOT a distributed computing framework."* — and *"Kafka handles real-time ingestion, writing data to storage. Airflow can then periodically pick up that data and process it in batch."*

We use the first sentence literally and deliberately depart from the second: **Airflow orchestrates *around* our stream, never as a second processing path.**

| Job | Why it needs orchestration | Contains business logic? |
|---|---|---|
| Sense the daily lab file, validate it, produce it to the compacted topic | File arrival is asynchronous and may be late or malformed; needs sensing, retries, validation branching, alerting | **No** — it produces rows verbatim |
| Trigger and monitor a **replay** run under a new scorer version | A multi-step operation: launch, monitor progress, diff versions, gate cutover | **No** — the scoring happens in the Spark job |
| Render the daily consolidated risk report | Scheduled, needs retries, produces an artefact | **No** — it *reads pre-computed rows from Cassandra*; it never recomputes a score |
| Retention and data-quality checks | Scheduled housekeeping | No |

Enforced mechanically: `tests/unit/test_dag_purity.py` parses every DAG's AST and fails on pandas imports or `groupby`/`merge`/`agg` calls. **An architectural principle enforced by a test is exactly the kind of evidence a viva rewards.**

### Alternatives considered

| Option | Why rejected |
|---|---|
| **cron** | The module's critique applies in full: *"Difficult dependency tracking / No visualization / No retries / Poor monitoring."* We need all four, and the UI is a graded observability artefact. |
| **No orchestrator; a `while True` producer for labs** | Would remove a container, and is a defensible minimalist position. Rejected because the lab file needs sensing/validation/branching/alerting, replay needs multi-step coordination, and the assignment lists Airflow in the preferred stack. |
| **Prefect / Dagster** | Better ergonomics and native asset lineage. Rejected purely on defensibility — the module taught Airflow. |

---

## 5. Serialization — Avro + Confluent Schema Registry

The module taught Schema Registry explicitly: *"Supports **schema evolution** (e.g., adding optional fields without breaking consumers)."*

**We have a genuine, non-hypothetical evolution case**, which is what makes this justification specific rather than generic: the PDF's vitals fields do not include `respiratory_rate` or `consciousness`, but **NEWS2 requires both**. The PDF permits field changes (*"You may adapt scope"*), so v2 of the schema adds them as nullable fields, and a contract test proves a v1-serialised reading still deserialises under the v2 reader schema. That is schema evolution demonstrated on a real requirement, not a contrived one.

Also: compaction on `labs.results` and `ward.admissions` means those topics are long-lived tables whose schema will outlive several consumer versions — precisely the situation `BACKWARD` compatibility exists for.

**Trade-off:** an extra container, and binary payloads are harder to inspect. Mitigated by wiring Kafka UI to the registry so messages are readable in the browser, and a `make peek` helper on the CLI.

---

## 6. Serving API — FastAPI

- The PDF requires *"API endpoint to return real-time ward monitoring figures"*, so an API is mandated.
- **Async matters specifically here:** the ward view fans out to several Cassandra partition reads (one per patient for detail views); `asyncio.gather` with the async Cassandra driver keeps a 40-patient ward view at the cost of the slowest single read rather than the sum.
- Pydantic response models make the **`scorer_version`** field explicit in the API contract — which is what makes the replay cutover visible to a client rather than a silent change of meaning. That is a genuine design point, not decoration.
- Free Prometheus and OpenTelemetry instrumentation.

Rejected: **Flask** (sync, no validation, no OpenAPI); **Grafana querying Cassandra directly** (would satisfy "dashboard" but not the mandated API, and there would be nowhere for the version-aware serving logic to live).

---

## 7. Observability — Prometheus + Grafana + Alertmanager + OpenTelemetry/Jaeger

Full design in `09`. The stack-level justification specific to this domain:

**The alerting requirement is inverted relative to a normal system.** In most pipelines, "no data" is a warning. Here, **a silent pipeline is indistinguishable from a ward of healthy patients**, which makes silence the most dangerous state the system can be in. That demands a real alerting system with a defined detection window and routing — not a log line and hope. `WardMonitoringSilent` at 3 minutes is the concrete expression of that, and it is why Alertmanager is in the stack rather than a `print()`.

Everything else follows the standard reasoning: Prometheus for pull-based scraping across stages and PromQL for the required rules; Grafana provisioned as code for reproducibility; OTel because the rubric names tracing.

**Honesty paragraph required in the report:** none of Prometheus, Grafana, Alertmanager, OpenTelemetry or Loki was taught in this module. They are deliberate extensions and are introduced as such. The module's own observability content is the Airflow Web UI and Kafka's *"Operational Metrics"* / *"Log Aggregation"* use cases — both of which we also use.

---

## 8. Summary table for the report (§5)

| Layer | Chosen | Alternatives considered | Decisive reason *for this scenario* | Trade-off accepted |
|---|---|---|---|---|
| Backbone / ingestion | Kafka (KRaft) | RabbitMQ, MQTT, Redis Streams | **Retention IS the historical store** — replay from offset 0 is what makes rule changes safe | RF=1: no second copy of the record anywhere |
| Serialization | Avro + Schema Registry | JSON+Pydantic, Protobuf | A real evolution case: NEWS2 needs `respiratory_rate` + `consciousness` added to the PDF's field list | Binary payloads harder to inspect |
| Processing | PySpark Structured Streaming, **one job** | Storm, Flink, Kafka Streams, ksqlDB | Clinical meaning lives in **windows**, not single events; one path ⇒ one clinical rule; the rule must be unit-testable | ~1–2 s micro-batch floor; checkpoint invalidation on logic change |
| Serving store | **Cassandra** | TimescaleDB, PostgreSQL, Redis, InfluxDB | Write-heavy append-only; "latest N for one patient" is a single sequential partition read; **native TTL**; its weaknesses are Kappa's upstream responsibilities | No joins/aggregation; query-first modelling; single node in demo |
| Orchestration | Airflow | cron, none, Prefect | Lab-file sensing/validation/branching, replay coordination, report rendering — **all orchestration, no logic** | Heaviest container |
| Serving API | FastAPI | Flask, Grafana-direct | Async fan-out over patient partitions; `scorer_version` in the contract | Custom code to defend |
| Dashboard | Grafana | Streamlit, custom | Provisioned as code → reproducible | Less bespoke |
| Metrics | Prometheus (+Pushgateway, JMX exporter) | StatsD, OTel metrics | PromQL expresses the health rules directly | Pushgateway caveat — see `09 §3.3` |
| Alerting | Alertmanager | log-and-hope | **Silence is the most dangerous state**; needs real detection and routing | Another container |
| Tracing | OTel + Jaeger | none, Zipkin | Rubric names tracing | Micro-batch granularity only — stated honestly |
| Logging | `structlog` → JSON | stdlib logging | Required structured logging with a fixed field schema | Loki is a stretch goal |

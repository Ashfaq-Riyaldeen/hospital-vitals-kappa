# 04 — Requirement 1: Ingestion & Kafka Design

> **PDF preferred stack:** *"Ingestion: Apache Kafka (**Producers, Topics, Partitions**)."*
>
> Under Kappa, Kafka is not just the ingestion layer — it is the **system of record**. Retention is an architectural decision, not a tuning knob.

---

## 1. Topic design

| Topic | Partitions | RF | Key | Cleanup policy | Retention | Purpose |
|---|---|---|---|---|---|---|
| `vitals.readings.v1` | **6** | 1 | `patient_id` | `delete` | **30 simulated days** | The system of record. Replayed from offset 0 for reprocessing. |
| `labs.results.v1` | 3 | 1 | `patient_id\|test_type` | **`compact`** | infinite | Materialises as "latest result per patient per test" |
| `ward.admissions.v1` | 3 | 1 | `patient_id` | **`compact`** | infinite | Ward reference data — bed, age, condition, `copd_scale2` |
| `vitals.alerts.v1` | 3 | 1 | `patient_id` | `delete` | 30 sim-days | Clinical alerts emitted by the stream |
| `vitals.readings.dlq` | 1 | 1 | `patient_id` | `delete` | 30 sim-days | Physiologically impossible readings |
| `vitals.late` | 1 | 1 | `patient_id` | `delete` | 30 sim-days | Readings arriving beyond the watermark — **side-processed, never dropped** |

Created declaratively by `scripts/create_topics.py` (idempotent, run by the init container). `auto.create.topics.enable=false`, so a typo fails loudly rather than silently creating a topic with default settings — which, for a compacted topic, would silently be the wrong policy.

### 1.1 Why `patient_id` is the partition key

> The module: *"**Ordering Guarantee: Messages are ordered within a partition, not across partitions.** Partition Key: Ensures related messages (e.g., same `user_id`) always go to the same partition."*

Trend detection is a **per-patient stateful computation**. "Is this patient deteriorating" is a slope over their ordered readings:

```
P014:  36.9°C → 37.4 → 38.1 → 38.6 → 38.9      ⇒ rising, NEWS2 climbing
```

If those readings were spread across partitions they could be consumed out of order, the slope would be meaningless, and the deterioration alert would fire late or not at all. **Keying by `patient_id` is a correctness requirement of the clinical logic, not a load-balancing choice.**

Distribution: 40 patient keys over 6 partitions ≈ 7 keys each. Slightly uneven, but at 13 events/second irrelevant. **We state that honestly rather than claiming perfect balance** — and note that at ward-network scale (thousands of patients) the distribution becomes excellent, which is when it would matter.

Rejected alternatives:

| Key | Why rejected |
|---|---|
| `ward_id` | 1 distinct value → **every reading on one partition**. Total hotspot, zero parallelism. The module warns explicitly about *"careful partition key selection needed to avoid hotspotting"*. |
| `bed_id` | Works today, but a patient moves beds during an admission and their history would split across partitions mid-stream. Key on the entity the state is *about* — the patient. |
| `device_id` | Same problem: monitors are swapped. |
| `null` (round-robin) | Best balance, no ordering. Would break trend detection. |

### 1.2 Why 6 partitions

- Consumer parallelism ceiling: the Spark job runs 2 workers × 2 cores = 4 concurrent tasks; 6 leaves headroom to scale without re-partitioning a live topic.
- **Replay parallelism** — this is the Kappa-specific reason. A replay from offset 0 is bounded by partition count, so partitions determine how fast history can be re-derived. At 40 patients this is instant either way, but the reasoning is what matters and it generalises.
- Not more, because each partition is per-broker file overhead for no benefit at this volume.

### 1.3 Log compaction — two genuine cases

> The module: *"**Log Compaction: Keeps only the latest record per key.** Example: User profile changes → only latest name/email is retained. Benefit: **Kafka can act as a durable state store.**"*

**`labs.results.v1`** — key `patient_id|test_type`. A lab result is a *fact about the current state of a patient's bloodwork*, not an event we need a history of for scoring. We want "P014's latest lactate", and compaction gives exactly that. The topic **is** the labs table, and reading it from `earliest` materialises it. This is the textbook use of the feature.

Subtlety worth stating: compaction means a re-uploaded/corrected lab file naturally supersedes the original — **restatement handled for free by the cleanup policy**, with no application logic at all. Compare the sibling Lambda project, which needs an explicit upsert-and-re-run machinery for the same capability.

**`ward.admissions.v1`** — key `patient_id`. Bed, age, condition, `copd_scale2`. Changes rarely; we always want the current value; must be available to the stream for enrichment.

Deliberately **not** compacted:
- `vitals.readings.v1` — **compacting it would destroy the system of record.** We need every reading, and replay depends on the full history. This is the single most important cleanup-policy decision in the project, and it is worth one sentence in the report to show the choice was made.
- `vitals.alerts.v1` — an alert history is a log; keeping only the latest alert per patient would erase the incident record.

### 1.4 Retention — the architectural decision

`vitals.readings.v1`: **30 simulated days**.

Under Lambda, retention is a buffer setting. **Under Kappa, retention defines how far back you can re-derive.** If a clinical rule changes and the log only goes back 7 days, you can only re-score 7 days. Our choice of 30 simulated days is a statement about the reprocessing horizon we are committing to support, and the report frames it that way.

Storage arithmetic (do this in the report — it shows the choice was costed):
```
3,840 readings/simulated day × 30 days = 115,200 readings
× ~250 bytes Avro (snappy)             ≈ 29 MB
```
**Trivial.** 30 simulated days = 2.5 real hours of wall-clock time, so we set `retention.ms = 3h` and `retention.bytes = 1GB` in the demo config, comfortably covering it with headroom.

This is the concrete counterpoint to the module's cost criterion: at ward volume, Kafka-as-history costs ~29 MB. At fleet volume (the sibling project) it would cost gigabytes per day, which is why that project keeps history in object storage instead. **Same criterion, opposite conclusion, driven by volume.**

---

## 2. Serialization — Avro + Schema Registry

Justified in `02 §5`. Implementation notes:

- Schemas in `ward/contracts/*.avsc`, registered by the init container.
- Subject naming: `TopicNameStrategy` → `vitals.readings.v1-value`.
- **Compatibility: `BACKWARD`** — new consumers read old data. Correct for us because the streaming job is redeployed more often than the monitors, and it is what permits adding `respiratory_rate` and `consciousness` as nullable fields.
- Message **keys** are plain UTF-8 strings, so they are readable in Kafka UI and in `kafka-console-consumer`. For the labs topic the key is a composite string `P014|lactate` — simple, debuggable, and correct for compaction.
- `tests/contract/test_schema_evolution.py` registers v1, registers v2, asserts the registry accepts it under `BACKWARD`, and asserts a v1 record deserialises under the v2 reader.

---

## 3. Producer configuration

```python
{
    "bootstrap.servers": settings.kafka_bootstrap,
    "enable.idempotence": True,     # module: "Idempotent Producers: Kafka assigns
                                    #   sequence numbers -> prevents duplicates"
    "acks": "all",                  # correct production setting; a no-op at RF=1 but
                                    #   leaving acks=1 would be a latent bug
    "retries": 10,
    "retry.backoff.ms": 200,
    "max.in.flight.requests.per.connection": 5,   # safe *because* idempotence is on
    "compression.type": "snappy",
    "linger.ms": 50,                # at 13 ev/s this batches ~1 record; kept small
                                    #   because latency matters more than throughput here
    "queue.buffering.max.messages": 50000,
    "client.id": f"bedside-monitor-{device_id}",
}
```

Note `linger.ms=50` versus the sibling project's 20 with a far higher rate — **the tuning differs because the workload differs**, and being able to say why is the point. At 13 events/second there is nothing to batch, so we optimise for latency and accept the throughput we do not need.

---

## 4. Consumer groups

Under Kappa there is normally **one** processing consumer group — that is the architecture. Ours:

| Group | Reads | Purpose |
|---|---|---|
| `ward-stream-v1` | `vitals.readings.v1` | **The single processing path.** Live scoring. |
| `ward-stream-v2-replay` | `vitals.readings.v1` from **offset 0** | A replay run under a new scorer version (`06`). Temporary, deleted after cutover. |

**The replay group is the same job with a different version flag and a different output suffix — not a second implementation.** That distinction is the entire Kappa argument and the report must be explicit about it: two *consumer groups* is not two *processing paths*, because the code is identical and the logic lives in one place.

Kafka UI showing the two groups at different offsets during a replay is one of the best screenshots in the report — it makes "replaying large chunks" visible.

---

## 5. Delivery semantics — stated honestly

| Path | Guarantee | Why |
|---|---|---|
| Monitor → Kafka | **Exactly once** (per producer session) | `enable.idempotence=true` deduplicates broker-side |
| Airflow → `labs.results` | **Effectively exactly once** | Compaction by key: republishing the same key/value converges to the same state |
| Kafka → Cassandra | **At least once, made idempotent** | `foreachBatch` may replay a micro-batch. Every write is an **INSERT with a fully-specified primary key** `(patient_id, scorer_version, scored_at)` — re-inserting the identical row is a no-op in Cassandra. **This is why we never use counter columns.** |
| Kafka → `vitals.alerts` | **At least once** | Deduplicated by `alert_id = sha1(patient_id, alert_type, window_start)` — the same clinical episode always yields the same ID |
| Kafka → DLQ / late | **At least once** | Duplicates are harmless in a diagnostic store |

**End-to-end exactly-once is not achieved**, and the report says so. Kafka transactions can span a read and a Kafka sink, but not an arbitrary `foreachBatch` write to Cassandra. **Idempotent sinks are the standard production answer**, and Cassandra's insert-by-full-primary-key semantics make it a naturally good fit — which is worth mentioning as another way the store and the architecture reinforce each other.

---

## 6. The two side-channels

Under Kappa, data that leaves the main path must go *somewhere* observable, because there is no batch layer to pick it up later.

### 6.1 Dead-letter queue — `vitals.readings.dlq`

```json
{
  "original_payload": "<base64 Avro>",
  "rejection_reason": "SPO2_PROBE_DETACHED",
  "rejection_detail": "spo2=0 is physiologically impossible",
  "validator": "physiological_bounds",
  "patient_id": "P022",
  "device_id": "MON-22",
  "rejected_at_sim": "2026-04-02T14:31:00Z",
  "rejected_at_real": "2026-08-04T18:22:07.412Z",
  "source_topic": "vitals.readings.v1",
  "source_partition": 3,
  "source_offset": 18472,
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"
}
```

Carrying **partition and offset** means any dead-lettered reading is traceable to its exact position in the log. Carrying **`device_id`** means the DLQ dashboard can attribute faults to a specific monitor — *"BED-22's SpO2 probe keeps detaching"* is an actionable ward finding, which turns a data-quality metric into a clinical-operations one. That is a good report detail.

### 6.2 Late arrivals — `vitals.late`

The module on watermarks: *"If data for 9:59 PM arrives after the 10:00 PM watermark, it is considered late data and is **either dropped or handled by a special side-process**."*

**We do the latter, deliberately.** A reading discarded because it arrived late is a discarded clinical observation. Late readings are written to `vitals.late` with their original `measured_at`, counted (`readings_late_total`), charted, and — critically — **included in the next replay**, because replay reprocesses from the log where they still exist in order.

**That is a genuinely elegant Kappa property and the report should point at it:** under Lambda, late data is what the batch layer exists to reconcile. Under Kappa, late data is reconciled by the same mechanism that handles rule changes — replay. One mechanism, two problems solved.

---

## 7. Making ingestion visible

| Where | What a reviewer sees |
|---|---|
| **Kafka UI** (`:8180`) | Topics with cleanup policies visible (compact vs delete); per-partition counts; consumer groups; **during a replay, two groups at very different offsets**; Avro payloads deserialised via Schema Registry |
| **Grafana → Pipeline Health** | Production rate, DLQ rate by reason, late-arrival rate, consumer lag |
| **Prometheus** | Raw series, alert state |
| **Logs** | `docker compose logs -f bedside-monitor` — JSON with `stage="ingest"` |

---

## 8. Implementation checklist

- [ ] `scripts/create_topics.py` — declarative, idempotent, **explicit cleanup policy per topic**; broker `auto.create.topics.enable=false`
- [ ] `scripts/register_schemas.py` — all `.avsc`, `BACKWARD` compatibility
- [ ] `ward/obs/kafka_client.py` — producer/consumer factories; the single place Kafka config is constructed
- [ ] `ward/contracts/serialization.py` — Avro wrappers
- [ ] `ward/stream/dlq.py` — `reject(reading, reason, detail, source_meta)` → envelope + metric + log
- [ ] `ward/stream/late.py` — the late-arrival side-process
- [ ] OTel `traceparent` injected into headers on every send
- [ ] `make peek TOPIC=...`, `make lag`, `make offsets` (shows replay progress)
- [ ] Kafka UI wired to Schema Registry
- [ ] Tests: schema round-trip, evolution under `BACKWARD`, compaction behaviour (produce two values for one key → read from earliest → one record), key-distribution sanity

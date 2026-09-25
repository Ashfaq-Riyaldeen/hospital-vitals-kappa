# Architecture Decision Records (ADRs)

Short records of the six architectural choices that define this platform.
Each record follows the standard format: Context, Decision, Consequences, and Rejected Alternatives.

| ADR | Decision | Core Rationale | Full Argument |
|---|---|---|---|
| [001](001-kappa-over-lambda.md) | Kappa architecture, not Lambda | One clinical question = one processing path; dual paths create clinical safety hazards | `plan/01` |
| [002](002-patient-id-partition-key.md) | Partition by `patient_id` | Patient vital trends require strict per-patient ordering within a single partition | `plan/04` |
| [003](003-spark-over-storm.md) | PySpark Structured Streaming over Storm | Catalyst optimization, native windowing/watermarking, and alignment with course material | `plan/02` |
| [004](004-cassandra-query-first.md) | Cassandra with query-first data modeling | Single-partition reads for all 7 queries, free worst-first sorting via clustering key | `plan/07` |
| [005](005-ttl-over-tombstones.md) | TTL expiration over deletes | Eliminates tombstone accumulation; self-clearing tables implement "silence is not safety" | `plan/07` |
| [006](006-retained-log-replay.md) | Retained log replay from offset 0 | 30-day retention enables non-destructive scoring rule revisions (NEWS2 v1 to v2) | `plan/06` |

# ADR-004: Serving Store - Cassandra with Query-First Data Modeling

**Status:** Accepted · **Date:** 2026-09-25 · **Full argument:** [`plan/07`](../../plan/07-req-storage-serving.md)

## Context
Serving ward staff and REST API endpoints requires low-latency queries across live monitor snapshots, patient historical vital sparklines, score progression, and daily reports.

## Decision
Apache Cassandra 4.1 with query-first data modeling across 7 dedicated tables. Every query maps 1:1 to a table schema that resolves via single-partition reads without `ALLOW FILTERING`.

Key modeling decisions:
* `ward_risk_snapshot`: Embeds `risk_score` in the clustering key (`PRIMARY KEY ((ward_id, scorer_version), risk_score, patient_id)`). This guarantees that the main ward screen receives pre-sorted worst-first patients with zero sorting overhead.
* `alerts_by_ward`: Uses `sim_date` in the partition key (`PRIMARY KEY ((ward_id, sim_date), alert_time, alert_id)`) to bound partition growth and prevent wide partition degradation.
* `risk_scores_by_patient`: Includes `scorer_version` in the clustering key, enabling side-by-side v1 versus v2 comparisons for replay validation.

## Consequences
* Accepted trade-off: Schema rigidity. Unanticipated queries require creating a new table and replaying Kafka to backfill it. Under Kappa, this replay is fast and native.
* Benefits: Predictable millisecond read latencies and linear horizontal write scalability.

## Rejected Alternatives
* PostgreSQL: Good for complex relational queries, but lacks native TTL and horizontal partitioning for time-series streams.
* TimescaleDB: Viable time-series alternative (kept as documented fallback in `plan/15`), but Cassandra was explicitly taught in the module's distributed NoSQL curriculum.

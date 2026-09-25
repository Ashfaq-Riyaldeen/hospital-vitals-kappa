# ADR-002: Partition Key Strategy - patient_id

**Status:** Accepted · **Date:** 2026-09-25 · **Full argument:** [`plan/04`](../../plan/04-req-ingestion-kafka.md)

## Context
Bedside monitors emit vital sign readings every 15 simulated minutes for 40 beds across a ward. Computing physiological deterioration requires calculating 4-hour sliding window vital-sign slopes (heart rate, SpO2, blood pressure, temperature).

## Decision
All vitals telemetry topics and Cassandra tables partition by `patient_id` as the primary partition key.

* Kafka: `patient_id` ensures that all readings for a patient land on the same Kafka partition, preserving chronological ordering and allowing stateful windowing without cross-partition shuffles.
* Cassandra: `patient_id` as partition key ensures all historical vital readings and risk scores for a patient are colocated on the same storage node, making patient history lookups single-partition sequential reads.

## Consequences
* Accepted trade-off: Key distribution across 6 Kafka partitions is slightly non-uniform with 40 beds (some partitions hold 6 patients, others 8). This is harmless at 13 events/second aggregate throughput.
* Benefits: Preserves FIFO arrival per patient; eliminates distributed state synchronization across workers.

## Rejected Alternatives
* `bed_id`: Rejected because patients transfer beds, and historical continuity belongs to the individual patient, not the physical furniture.
* `ward_id`: Rejected for Kafka because all 40 patients would land on a single partition, bottlenecking consumer parallelism.

# ADR-001: Kappa Architecture, Not Lambda

**Status:** Accepted · **Date:** 2026-09-25 · **Full argument:** [`plan/01`](../../plan/01-architecture-decision.md)

## Context
The business question asks: *"Which patients show concerning vital-sign trends right now, and how do yesterday's lab results change the risk picture for those patients going forward?"*
Unlike systems where daily sources answer a distinct business query (such as financial P&L versus live telemetry), the hospital lab results modify the exact same clinical risk score that the stream already computes. There is only one number per patient: their current clinical risk tier.

## Decision
Kappa Architecture. A single PySpark Structured Streaming pipeline processes incoming bedside vitals, joins compacted lab results from Kafka, scores with NEWS2, and writes to Cassandra. Apache Kafka acts as the immutable system of record with 30 simulated days retention.

The decision is driven by patient safety:
* Implementing clinical scoring twice (speed path for alerts, batch path for daily summaries) creates a hazard where the bedside monitor and the morning report disagree.
* When scoring thresholds update (e.g. NEWS to NEWS2), maintaining two separate implementations introduces synchronization lag and diagnostic discrepancies.
* At ward scale (40 beds, ~3,840 events per simulated day, ~30 MB for 30 days), full historical replay takes seconds. The typical cost objection to Kappa does not apply.

## Consequences
* Accepted trade-off: Deep historical ad-hoc analytical queries require re-streaming the log or maintaining materialized views in Cassandra.
* Benefits: One clinical rule implementation (`ward/clinical/news2.py`), zero data reconciliation logic between layers, and no high-water-mark merge complexity.

## Rejected Alternatives
Lambda Architecture. Rejected because duplicating the clinical calculation across speed and batch layers introduces dangerous discrepancy windows and unneeded codebase complexity for a small ward dataset.

# ADR-005: Snapshot Management - TTL Expiration Over Deletes

**Status:** Accepted · **Date:** 2026-09-25 · **Full argument:** [`plan/07`](../../plan/07-req-storage-serving.md)

## Context
The live ward monitor table (`ward_risk_snapshot`) displays current patient risk scores. As patients deteriorate or stabilize, their scores update every micro-batch. Stale rows must be removed to avoid showing outdated patient states.

## Decision
Configure `default_time_to_live = 120` on `ward_risk_snapshot` and never execute explicit `DELETE` statements.

Technical and clinical rationale:
* Prevents Tombstone Saturation: In Cassandra, explicit deletes write tombstone markers that degrade read performance and can crash coordinators once thresholds are breached. Short TTL rows expire automatically during compaction without creating query bottlenecks.
* Clinical Safety - Silence is Not Safety: If the streaming pipeline stalls or dies, the snapshot table naturally empties within 120 seconds. An empty ward screen immediately alerts nursing staff to a technology failure, whereas a frozen screen showing stale normal vitals gives false reassurance while patients deteriorate.

## Consequences
* Accepted trade-off: Brief coexistence of multiple scores for a patient within the 120-second TTL window during rapid updates. Handled cleanly in `WardStoreDAO` by deduplicating on `patient_id` and selecting the row with the newest `scored_at` timestamp.
* Benefits: Zero tombstone overhead and active fail-visible safety signaling.

## Rejected Alternatives
* Explicit `DELETE` statements before insert: Rejected due to severe tombstone accumulation in high-frequency streaming sinks.
* In-place row updates: Not possible with `risk_score` in the clustering key (required for worst-first ordering).

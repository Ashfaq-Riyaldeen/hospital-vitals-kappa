# ADR-006: Reprocessing - Retained Log Replay from Offset 0

**Status:** Accepted · **Date:** 2026-09-25 · **Full argument:** [`plan/06`](../../plan/06-replay-reprocessing.md)

## Context
When clinical guidelines evolve (such as the 2017 revision from NEWS to NEWS2 introducing SpO2 Scale 2 for COPD patients), the hospital needs to re-derive patient risk history to assess historical trends and evaluate whether past patients were over- or under-flagged.

## Decision
Retain Kafka raw vitals log for 30 simulated days (`vitals.readings.v1` retention: 9,000,000 ms). Reprocess history by starting a new consumer group (`ward-stream-v2-replay`) from `startingOffsets = earliest` pointing to offset 0.

Replay characteristics:
* Same Codebase: Runs the exact same pipeline file (`ward/stream/pipeline.py`) with updated configuration (`SCORER_VERSION=v2`).
* Non-Destructive: Output rows carry `scorer_version = 'v2'`. Because `scorer_version` is embedded in Cassandra primary keys, v1 and v2 records coexist in the same partition.
* Atomic Verification: Clinicians and reviewers can query both versions simultaneously (`/api/v1/replay/compare`) to verify that only COPD patients change scores while all other patients maintain bit-identical results.

## Consequences
* Accepted trade-off: Storing 30 days of telemetry in Kafka requires disk space (~30 MB for ward scale).
* Benefits: Instantaneous rollback by toggling `ACTIVE_SCORER_VERSION`, zero risk to production live monitoring, and verifiable correctness.

## Rejected Alternatives
* In-place database migration: Destructive, irreversible, and risks showing corrupted half-migrated data during the update.
* Separate batch reprocessing pipeline: Violates Kappa principles by creating a second codebase for the same clinical calculation.

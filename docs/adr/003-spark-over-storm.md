# ADR-003: Stream Engine - Spark Structured Streaming Over Apache Storm

**Status:** Accepted · **Date:** 2026-09-25 · **Full argument:** [`plan/02`](../../plan/02-technology-stack.md)

## Context
The platform requires a real-time stream processing engine capable of windowed slope regressions, stream-to-compacted-stream broadcast joins, late data handling via watermarks, and micro-batch sinks.

## Decision
PySpark Structured Streaming (`pyspark==3.5.1`) in local micro-batch mode (5-second trigger).

Reasons:
* Structured Streaming provides first-class `withWatermark()` and sliding `groupBy(window())` semantics matching clinical observation windows.
* Catalyst optimizer performs broadcast joins against compacted reference topics (admissions, lab results) natively.
* The 5-second micro-batch trigger easily satisfies the NFR-1 target (<30s detection latency), while eliminating low-level tuple orchestration code.

## Consequences
* Accepted trade-off: Micro-batch latency floor (sub-second event-by-event processing is not possible). In routine ward monitoring where vitals are checked every 15 simulated minutes, a 5-second processing latency is negligible.
* Benefits: High throughput, robust checkpoint recovery, and declarative DataFrame operations.

## Rejected Alternatives
* Apache Storm: Lower latency (native tuple-at-a-time), but lacks high-level windowing abstractions, schema integration, and structured DataFrame APIs taught in the course.
* Apache Flink: Excellent stream processing model, but introduces extra operational complexity and was not covered in the module curriculum.

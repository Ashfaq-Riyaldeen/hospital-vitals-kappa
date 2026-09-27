"""Measure a Kappa replay: how far has the v2 stream got through the retained log?

The replay itself is `ward-stream-v2` - the SAME pipeline file as the live stream,
started with SCORER_VERSION=v2 and STARTING_OFFSETS=earliest (`make replay`). This
module only watches it.

Progress cannot be read from a consumer group, because Structured Streaming keeps its
offsets in its own checkpoint and never commits them to Kafka. (The first version of
this module polled a consumer group that nothing ever consumed into, so it could only
ever report 0 %.) Instead the stream publishes `stream_partition_offset` on its
/metrics endpoint after every micro-batch, and the replay is complete when every
partition has passed the end offset recorded at the start.
"""

from __future__ import annotations

import argparse
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, datetime

from confluent_kafka import Consumer, TopicPartition
from prometheus_client.parser import text_string_to_metric_families

from ward import settings
from ward.obs import metrics
from ward.obs.log import configure, get_logger

log = get_logger(component="replay_runner")

DEFAULT_METRICS_URL = "http://ward-stream-v2:8105/metrics"


@dataclass
class ReplayStatus:
    target_version: str
    started_at: datetime
    end_offsets: dict[int, int] = field(default_factory=dict)
    start_offsets: dict[int, int] = field(default_factory=dict)
    current_offsets: dict[int, int] = field(default_factory=dict)
    completed_at: datetime | None = None

    @property
    def total_events(self) -> int:
        return sum(
            max(0, end - self.start_offsets.get(p, 0)) for p, end in self.end_offsets.items()
        )

    @property
    def processed_events(self) -> int:
        return sum(
            max(0, min(self.current_offsets.get(p, 0), end) - self.start_offsets.get(p, 0))
            for p, end in self.end_offsets.items()
        )

    @property
    def remaining_events(self) -> int:
        return self.total_events - self.processed_events

    @property
    def progress_pct(self) -> float:
        total = self.total_events
        return 100.0 if total == 0 else round(self.processed_events / total * 100.0, 2)

    @property
    def is_complete(self) -> bool:
        return bool(self.end_offsets) and all(
            self.current_offsets.get(p, 0) >= end for p, end in self.end_offsets.items()
        )

    @property
    def duration_seconds(self) -> float | None:
        if self.completed_at is None:
            return None
        return (self.completed_at - self.started_at).total_seconds()


def record_end_offsets(bootstrap: str, topic: str) -> tuple[dict[int, int], dict[int, int]]:
    """The replay horizon: the log's start and end offsets per partition, right now."""
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "ward-replay-preflight"})
    try:
        meta = consumer.list_topics(topic, timeout=10)
        if topic not in meta.topics:
            raise RuntimeError(f"topic {topic} does not exist")
        starts: dict[int, int] = {}
        ends: dict[int, int] = {}
        for p in meta.topics[topic].partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            starts[p], ends[p] = low, high
        return starts, ends
    finally:
        consumer.close()


def parse_partition_offsets(metrics_text: str, version: str) -> dict[int, int]:
    """Pull `stream_partition_offset{scorer_version=..., partition=...}` from /metrics."""
    offsets: dict[int, int] = {}
    for family in text_string_to_metric_families(metrics_text):
        if family.name != "stream_partition_offset":
            continue
        for sample in family.samples:
            if sample.labels.get("scorer_version") == version:
                offsets[int(sample.labels["partition"])] = int(sample.value)
    return offsets


def read_stream_offsets(metrics_url: str, version: str) -> dict[int, int]:
    with urllib.request.urlopen(metrics_url, timeout=5) as resp:
        return parse_partition_offsets(resp.read().decode(), version)


def wait_for_replay(
    status: ReplayStatus,
    metrics_url: str,
    poll_seconds: float = 2.0,
    timeout_seconds: float = 1800.0,
) -> ReplayStatus:
    """Poll the v2 stream until it has passed every recorded end offset."""
    deadline = time.monotonic() + timeout_seconds
    watched_catch_up = False  # only then is the elapsed time a replay duration
    while time.monotonic() < deadline:
        try:
            status.current_offsets = read_stream_offsets(metrics_url, status.target_version)
        except OSError as exc:
            log.warning("replay_stream_unreachable", url=metrics_url, error=str(exc))
        metrics.push_gauges(
            "ward-replay",
            {
                "replay_progress_pct": status.progress_pct,
                "replay_events_remaining": float(status.remaining_events),
            },
        )
        if status.current_offsets and not status.is_complete:
            watched_catch_up = True
        log.info(
            "replay_progress",
            progress_pct=status.progress_pct,
            processed=status.processed_events,
            total=status.total_events,
        )
        if status.is_complete:
            status.completed_at = datetime.now(UTC)
            final = {"replay_progress_pct": 100.0, "replay_events_remaining": 0.0}
            if watched_catch_up:
                # A check that starts after the replay has finished must not publish
                # its own few milliseconds as "the replay took 25 ms" (seen when the
                # Airflow run followed a replay started by `make replay`).
                final["replay_duration_seconds"] = status.duration_seconds or 0.0
            metrics.push_gauges("ward-replay", final)
            log.info(
                "replay_complete",
                events=status.total_events,
                duration_seconds=status.duration_seconds,
            )
            return status
        time.sleep(poll_seconds)
    raise TimeoutError(
        f"replay did not finish in {timeout_seconds:.0f}s: "
        f"{status.remaining_events} of {status.total_events} events remaining"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Watch a Kappa replay to completion")
    parser.add_argument("--target-version", default="v2")
    parser.add_argument("--metrics-url", default=DEFAULT_METRICS_URL)
    parser.add_argument("--timeout", type=float, default=1800.0)
    args = parser.parse_args()

    configure(service=f"ward-replay-{args.target_version}", stage="process")
    k_cfg = settings.kafka()
    starts, ends = record_end_offsets(k_cfg.bootstrap, k_cfg.vitals_topic)
    status = ReplayStatus(args.target_version, datetime.now(UTC), ends, starts)
    print(f"Replay horizon: {status.total_events} events across {len(ends)} partitions")
    try:
        wait_for_replay(status, args.metrics_url, timeout_seconds=args.timeout)
    except TimeoutError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
    print(
        f"Replay complete: {status.total_events} events re-scored under "
        f"{status.target_version} in {status.duration_seconds:.1f}s"
    )


if __name__ == "__main__":
    main()

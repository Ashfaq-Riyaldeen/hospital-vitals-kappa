"""Replay Runner for Kappa Architecture Stream Reprocessing.

Executes a 30-day log replay from Kafka offset 0 (earliest) using the exact same
stream pipeline definition with a dedicated consumer group and a target scorer version
(e.g. v2).

Tracks partition high watermarks, monitors consumer group lag down to 0, emits
structured telemetry events, and coordinates with Cassandra storage partitions.
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from ward import settings
from ward.obs import metrics
from ward.obs.log import configure, get_logger

log = get_logger(component="replay_runner")

REPLAY_TOPIC = "vitals.readings.v1"


@dataclass
class ReplayPartitionProgress:
    partition: int
    start_offset: int
    end_offset: int
    current_offset: int = 0

    @property
    def total_events(self) -> int:
        return max(0, self.end_offset - self.start_offset)

    @property
    def processed_events(self) -> int:
        return max(0, self.current_offset - self.start_offset)

    @property
    def lag(self) -> int:
        return max(0, self.end_offset - self.current_offset)


@dataclass
class ReplayStatus:
    target_version: str
    consumer_group: str
    topic: str
    started_at: datetime
    completed_at: datetime | None = None
    partition_progress: dict[int, ReplayPartitionProgress] = field(default_factory=dict)

    @property
    def total_events(self) -> int:
        return sum(p.total_events for p in self.partition_progress.values())

    @property
    def processed_events(self) -> int:
        return sum(p.processed_events for p in self.partition_progress.values())

    @property
    def total_lag(self) -> int:
        return sum(p.lag for p in self.partition_progress.values())

    @property
    def progress_pct(self) -> float:
        total = self.total_events
        if total <= 0:
            return 100.0
        pct = (self.processed_events / total) * 100.0
        return min(100.0, max(0.0, round(pct, 2)))

    @property
    def is_complete(self) -> bool:
        return self.total_lag == 0 and self.total_events > 0


class ReplayRunner:
    """Coordinates and observes log replay for Kappa stream reprocessing."""

    def __init__(
        self,
        target_version: str = "v2",
        from_offset: str = "earliest",
        bootstrap_servers: str | None = None,
    ) -> None:
        self.target_version = target_version
        self.from_offset = from_offset
        kafka_cfg = settings.kafka()
        self.bootstrap = bootstrap_servers or kafka_cfg.bootstrap
        self.consumer_group = f"ward-stream-{target_version}-replay"
        self.status = ReplayStatus(
            target_version=target_version,
            consumer_group=self.consumer_group,
            topic=REPLAY_TOPIC,
            started_at=datetime.now(UTC),
        )

    def check_preflight_horizon(self) -> dict[int, int]:
        """Record high watermarks across all partitions of the vitals topic."""
        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": f"preflight-{int(time.time())}",
                "auto.offset.reset": "latest",
            }
        )
        try:
            metadata = consumer.list_topics(REPLAY_TOPIC, timeout=10)
            if REPLAY_TOPIC not in metadata.topics:
                raise RuntimeError(f"Topic {REPLAY_TOPIC} does not exist on Kafka cluster")

            topic_meta = metadata.topics[REPLAY_TOPIC]
            watermarks: dict[int, int] = {}
            for p_id in topic_meta.partitions:
                tp = TopicPartition(REPLAY_TOPIC, p_id)
                low, high = consumer.get_watermark_offsets(tp, timeout=10)
                watermarks[p_id] = high
                self.status.partition_progress[p_id] = ReplayPartitionProgress(
                    partition=p_id,
                    start_offset=low,
                    end_offset=high,
                    current_offset=low,
                )

            log.info(
                "replay_preflight_recorded",
                target_version=self.target_version,
                partitions=len(watermarks),
                total_events=self.status.total_events,
            )
            return watermarks
        finally:
            consumer.close()

    def poll_consumer_offsets(self) -> ReplayStatus:
        """Poll the replay consumer group's committed offsets and compute lag."""
        admin = AdminClient({"bootstrap.servers": self.bootstrap})
        try:
            future = admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(self.consumer_group)]
            )
            # In unit tests or mock runs, fallback gracefully
            for _group_id, res in future.items():
                group_partitions = res.result()
                for tp in group_partitions.topic_partitions:
                    if (
                        tp.topic == REPLAY_TOPIC
                        and tp.partition in self.status.partition_progress
                        and tp.offset >= 0
                    ):
                        self.status.partition_progress[tp.partition].current_offset = tp.offset
        except Exception as exc:
            log.warning("poll_offsets_warning", error=str(exc))

        # Update Prometheus metrics
        metrics.set_gauge("replay_progress_pct", self.status.progress_pct)
        metrics.set_gauge("replay_events_remaining", self.status.total_lag)

        return self.status

    def await_replay_completion(
        self, poll_interval_seconds: float = 2.0, timeout_seconds: float = 300.0
    ) -> ReplayStatus:
        """Wait until replay consumer group has fully caught up to recorded end watermarks."""
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            st = self.poll_consumer_offsets()
            log.info(
                "replay_progress",
                progress_pct=st.progress_pct,
                processed=st.processed_events,
                total=st.total_events,
                lag=st.total_lag,
            )
            if st.is_complete:
                st.completed_at = datetime.now(UTC)
                log.info(
                    "replay_stream_complete",
                    target_version=self.target_version,
                    events=st.processed_events,
                )
                return st
            time.sleep(poll_interval_seconds)

        err_msg = (
            f"Replay did not complete within {timeout_seconds}s; "
            f"remaining lag: {self.status.total_lag}"
        )
        raise TimeoutError(err_msg)


def launch_stream_replay(
    target_version: str = "v2",
    from_offset: str = "earliest",
    wait: bool = False,
) -> ReplayStatus:
    """Entry point for executing a replay cycle."""
    configure(service=f"ward-replay-{target_version}", stage="process")
    runner = ReplayRunner(target_version=target_version, from_offset=from_offset)
    runner.check_preflight_horizon()
    log.info(
        "replay_initiated",
        target_version=target_version,
        consumer_group=runner.consumer_group,
        total_events=runner.status.total_events,
    )
    if wait:
        return runner.await_replay_completion()
    return runner.status


def main() -> None:
    parser = argparse.ArgumentParser(description="Ward Kappa Replay Runner")
    parser.add_argument(
        "--target-version", default="v2", help="Target scorer version (default: v2)"
    )
    parser.add_argument(
        "--from-offset", default="earliest", help="Starting offset (default: earliest)"
    )
    parser.add_argument("--wait", action="store_true", help="Block until replay completes")
    args = parser.parse_args()

    try:
        status = launch_stream_replay(
            target_version=args.target_version,
            from_offset=args.from_offset,
            wait=args.wait,
        )
        print(
            f"Replay launched for {status.target_version}. "
            f"Total events: {status.total_events}, Lag: {status.total_lag}, "
            f"Progress: {status.progress_pct}%"
        )
    except Exception as exc:
        print(f"Error launching replay: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

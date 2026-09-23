"""The Kafka producer, and the settings that make it safe.

`enable.idempotence=true` is not a tuning knob here. Without it, a broker-side retry
after a network blip writes the reading twice, and a duplicated vital-sign reading
skews a trend slope - which is the input to a clinical score. With it, the broker
deduplicates by producer sequence number and a retry is a no-op.

BLOCKING ON A FULL QUEUE IS DELIBERATE. `queue.buffering.max.messages` is finite, and
when it fills the producer blocks rather than dropping. Dropping a vital-sign reading
to keep a buffer healthy is the wrong trade in this domain: the backpressure is
counted (`producer_backpressure_total`) and visible, whereas a silently discarded
reading is not. A bedside monitor that keeps measuring while the network is down and
then delivers late is behaving correctly; one that forgets is not.
"""


from __future__ import annotations

from typing import Any

from confluent_kafka import Producer

from ward.common import config


def producer_config(client_id: str) -> dict[str, Any]:
    k = config.kafka()
    return {
        "bootstrap.servers": k.bootstrap,
        # The module taught idempotent producers explicitly. Broker-side dedup by
        # producer id + sequence number gives exactly-once *within a session*.
        "enable.idempotence": True,
        # A no-op at replication.factor=1, but it is the correct production setting
        # and leaving it at 1 would be a latent bug the moment replicas are added.
        "acks": "all",
        "retries": 10,
        "retry.backoff.ms": 200,
        # Safe above 1 *because* idempotence is on: the broker reorders by sequence
        # number, so pipelining cannot break per-partition ordering. Without
        # idempotence this would have to be 1.
        "max.in.flight.requests.per.connection": 5,
        "compression.type": "snappy",
        # At ~240 events/s this batches roughly 5 events - throughput without
        # meaningful latency.
        "linger.ms": 20,
        "batch.size": 65536,
        "queue.buffering.max.messages": 100_000,
        "client.id": client_id,
    }


def build_producer(client_id: str) -> Producer:
    return Producer(producer_config(client_id))

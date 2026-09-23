"""Create the six topics, with their cleanup policies.

★ THE CLEANUP POLICIES ARE THE POINT OF THIS FILE. ★

Two of these topics are COMPACTED, and compaction is load-bearing rather than an
optimisation:

* `labs.results.v1` keyed by `patient_id|test_type` means the log converges to "the
  latest result per patient per test". Republishing a corrected result is then
  idempotent, and the streaming job can rebuild the whole lab picture by reading the
  topic from the beginning.
* `ward.admissions.v1` keyed by `patient_id` is the ward's reference data, broadcast
  into the stream. It must be replayable in full at any time, which a delete-retained
  topic cannot promise.

If either were silently created with the default `delete` policy, nothing would break
today. Results would start disappearing after the retention window, weeks later, with
no error anywhere - which is why `KAFKA_AUTO_CREATE_TOPICS_ENABLE` is off and why
`make topics` prints the policy of every topic rather than just the names.

RETENTION IS THE HISTORICAL STORE. `vitals.readings.v1` holds 30 simulated days, and
under Kappa there is no warehouse behind it. Shortening this does not save disk; it
deletes the system's memory and makes the replay in plan/06 impossible.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic
from ward import settings
from ward.obs.log import configure, get_logger

log = get_logger()


@dataclass(frozen=True, slots=True)
class TopicSpec:
    name: str
    partitions: int
    cleanup_policy: str
    retention_ms: int | None
    purpose: str


def topic_specs() -> list[TopicSpec]:
    k = settings.kafka()
    sim = settings.sim()

    # 30 SIMULATED days expressed in the REAL milliseconds Kafka understands. At a
    # 288x speed-up that is 2.5 real hours. Writing `2_592_000_000` here - 30 real
    # days - would look right and retain 288x more than intended.
    retention_ms = int(k.vitals_retention_sim_days * sim.day_seconds * 1000)

    return [
        TopicSpec(
            k.vitals_topic,
            k.vitals_partitions,
            "delete",
            retention_ms,
            "the system of record; replayed from offset 0 for reprocessing",
        ),
        TopicSpec(
            k.labs_topic,
            k.compacted_partitions,
            "compact",
            None,
            "latest lab result per patient per test",
        ),
        TopicSpec(
            k.admissions_topic,
            k.compacted_partitions,
            "compact",
            None,
            "ward reference data, broadcast into the stream",
        ),
        TopicSpec(
            k.alerts_topic,
            k.compacted_partitions,
            "delete",
            retention_ms,
            "clinical alerts emitted by the stream",
        ),
        TopicSpec(k.dlq_topic, 1, "delete", retention_ms, "physiologically impossible readings"),
        TopicSpec(
            k.late_topic,
            1,
            "delete",
            retention_ms,
            "arrived beyond the watermark - side-processed, never dropped",
        ),
    ]


def create(admin: AdminClient, specs: list[TopicSpec], replication: int) -> int:
    existing = set(admin.list_topics(timeout=20).topics)
    wanted = [s for s in specs if s.name not in existing]

    if not wanted:
        log.info("topics_already_exist", count=len(specs), stage="ingest")
    else:
        new_topics = []
        for spec in wanted:
            config = {"cleanup.policy": spec.cleanup_policy}
            if spec.retention_ms is not None:
                config["retention.ms"] = str(spec.retention_ms)
            new_topics.append(NewTopic(spec.name, spec.partitions, replication, config=config))
        for name, future in admin.create_topics(new_topics).items():
            future.result()
            log.info("topic_created", topic=name, stage="ingest")

    # ★ VERIFY, do not assume. A topic that already existed from an earlier run with
    # the wrong policy would otherwise be left alone and silently break the lab join.
    # Reading the policy back is the only way to know what was actually created.
    return verify(admin, specs)


def verify(admin: AdminClient, specs: list[TopicSpec]) -> int:
    resources = [ConfigResource(ConfigResource.Type.TOPIC, s.name) for s in specs]
    results = admin.describe_configs(resources)
    by_name = {s.name: s for s in specs}

    mismatches = 0
    for resource, future in results.items():
        config = future.result()
        spec = by_name[resource.name]
        actual = config["cleanup.policy"].value
        if actual != spec.cleanup_policy:
            log.error(
                "cleanup_policy_mismatch",
                topic=spec.name,
                expected=spec.cleanup_policy,
                actual=actual,
                stage="ingest",
            )
            mismatches += 1
        else:
            log.info(
                "topic_verified",
                topic=spec.name,
                policy=actual,
                partitions=spec.partitions,
                purpose=spec.purpose,
                stage="ingest",
            )
    return mismatches


def main() -> int:
    obs = settings.observability()
    configure(service="init", stage="ingest", level=obs.log_level, json_output=obs.log_json)

    k = settings.kafka()
    admin = AdminClient({"bootstrap.servers": k.bootstrap})
    specs = topic_specs()
    mismatches = create(admin, specs, k.replication_factor)

    if mismatches:
        log.error(
            "topic_setup_failed",
            mismatches=mismatches,
            detail="a compacted topic created with the wrong policy loses lab results "
            "weeks later with no error; refusing to continue",
            stage="ingest",
        )
        return 1

    log.info("topics_ready", count=len(specs), stage="ingest")
    return 0


if __name__ == "__main__":
    sys.exit(main())

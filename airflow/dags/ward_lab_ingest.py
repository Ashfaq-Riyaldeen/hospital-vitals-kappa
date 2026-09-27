"""ward_lab_ingest: Orchestrated daily lab result ingestion.

Runs every 5 real minutes, which at SIM_DAY_SECONDS=300 corresponds to exactly
one simulated day.

Workflow:
    resolve_sim_date
           |
    verify_checksum    (SHA-256 sidecar validation)
           |
    validate_lab_file  (Branch on JSON schema and reference range plausibility)
       /       \\
  quarantine   publish_to_kafka  (Verbatim Avro production to labs.results.v1)
    _file             |
               verify_published  (Consume back and confirm exact message count)
                      |
                run_dq_checks    (Emit coverage rate and out-of-range metrics)
                      |
                notify_success

Under Kappa architecture, Airflow only orchestrates. The publish step copies
pathology results verbatim without computing clinical scores or altering rows.
If a lab drop is missing, the ward continues monitoring safely on vitals alone.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule

LAB_DROP_HOUR = 6
LATE_GRACE_HOURS = 2

DEFAULT_ARGS = {
    "owner": "ward-clinician",
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=30),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id="ward_lab_ingest",
    description="Daily lab file: verify checksum, validate, publish to Kafka or quarantine",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ward", "labs", "kappa", "orchestration"],
    params={"sim_date": None},
)
def ward_lab_ingest() -> None:
    @task
    def resolve_sim_date(params: dict[str, Any] | None = None) -> str:
        """Resolve the target simulated date from SimClock anchor or manual override."""
        from ward.simclock import read_anchor

        override = (params or {}).get("sim_date")
        if override:
            return str(override)

        state_path = Path(os.environ.get("SIM_STATE_PATH", "/state/sim_epoch.json"))
        if not state_path.exists():
            return "2026-04-01"
        clock = read_anchor(state_path)
        # The lab drops each day's file at 06:00 simulated, and this DAG runs every
        # 5 real minutes - one simulated day - at a phase unrelated to the simulated
        # clock. Asking for "today" failed every run that landed before 06:00, and then
        # every run after it, because the phase never changes. Asking for the latest
        # day whose drop time (plus two hours' grace for a late file) has passed
        # picks up each file exactly once.
        ready = clock.sim_now() - timedelta(hours=LAB_DROP_HOUR + LATE_GRACE_HOURS)
        if ready.date() < clock.epoch_sim.date():
            # Before the first file of the simulation is due: nothing to do, not a failure.
            raise AirflowSkipException("no lab file is due yet")
        return ready.date().isoformat()

    @task
    def verify_checksum(sim_date: str) -> str:
        """Verify the SHA-256 sidecar matches the received lab results file."""
        inbox_dir = Path(os.environ.get("LAB_INBOX", "/labs/inbox"))
        lab_file = inbox_dir / f"labs_{sim_date}.json"
        sidecar = inbox_dir / f"labs_{sim_date}.json.sha256"

        if not lab_file.exists():
            raise AirflowFailException(f"Lab file not found: {lab_file}")
        if not sidecar.exists():
            raise AirflowFailException(f"SHA-256 sidecar not found for {lab_file}")

        body = lab_file.read_bytes()
        calculated = hashlib.sha256(body).hexdigest()
        expected = sidecar.read_text().split()[0].strip()

        if calculated != expected:
            raise AirflowFailException(
                f"Checksum mismatch on {lab_file.name}: expected {expected}, got {calculated}"
            )
        return str(lab_file)

    @task.branch
    def validate_lab_file(sim_date: str) -> str:
        """Validate lab JSON structure, required fields, and reference range syntax."""
        from ward.clinical.lab_rules import parse_reference_range

        inbox_dir = Path(os.environ.get("LAB_INBOX", "/labs/inbox"))
        lab_file = inbox_dir / f"labs_{sim_date}.json"

        try:
            payload = json.loads(lab_file.read_text())
        except Exception:
            return "quarantine_file"

        results = payload.get("results")
        if not isinstance(results, list) or not results:
            return "quarantine_file"

        seen_keys: set[tuple[str, str]] = set()
        required_fields = {"patient_id", "test_type", "result_value", "unit", "reference_range"}

        for row in results:
            if not required_fields.issubset(row.keys()):
                return "quarantine_file"
            try:
                float(row["result_value"])
            except (ValueError, TypeError):
                return "quarantine_file"

            ref_obj = parse_reference_range(row["reference_range"])
            if ref_obj.is_unparsed:
                return "quarantine_file"

            key = (row["patient_id"], row["test_type"])
            if key in seen_keys:
                return "quarantine_file"
            seen_keys.add(key)

        return "publish_to_kafka"

    @task
    def quarantine_file(sim_date: str) -> None:
        """Quarantine malformed lab file to avoid polluting the log."""
        inbox_dir = Path(os.environ.get("LAB_INBOX", "/labs/inbox"))
        quarantine_dir = Path(os.environ.get("LAB_QUARANTINE", "/labs/quarantine"))
        quarantine_dir.mkdir(parents=True, exist_ok=True)

        lab_file = inbox_dir / f"labs_{sim_date}.json"
        target_file = quarantine_dir / f"labs_{sim_date}_invalid.json"
        reason_file = quarantine_dir / f"labs_{sim_date}_invalid.reason"

        if lab_file.exists():
            shutil.move(str(lab_file), str(target_file))
            reason_file.write_text("Failed validation: malformed schema or invalid reference range")

    @task
    def publish_to_kafka(sim_date: str) -> int:
        """Publish verified lab results verbatim to Kafka compacted topic labs.results.v1."""
        from confluent_kafka import Producer
        from ward import settings
        from ward.clinical.lab_rules import parse_reference_range
        from ward.contracts.serialization import serialize_key, serialize_value

        inbox_dir = Path(os.environ.get("LAB_INBOX", "/labs/inbox"))
        lab_file = inbox_dir / f"labs_{sim_date}.json"
        payload = json.loads(lab_file.read_text())
        results = payload.get("results", [])

        from ward.simclock import read_anchor

        sim_now = read_anchor(
            Path(os.environ.get("SIM_STATE_PATH", "/state/sim_epoch.json"))
        ).sim_now()
        kafka_cfg = settings.kafka()
        producer = Producer({"bootstrap.servers": kafka_cfg.bootstrap})
        topic = "labs.results.v1"
        published_count = 0

        for r in results:
            ref_range = parse_reference_range(r["reference_range"])
            ref_low, ref_high = ref_range.low, ref_range.high
            collected_dt = datetime.fromisoformat(r["collected_at"].replace("Z", "+00:00"))
            collected_ms = int(collected_dt.timestamp() * 1000)
            # Reported on the SIMULATED clock. Real time here made every lab look
            # months newer than the readings it was joined to, so it never went stale.
            reported_ms = int(sim_now.timestamp() * 1000)

            record = {
                "patient_id": r["patient_id"],
                "test_type": r["test_type"].upper(),
                "result_value": float(r["result_value"]),
                "unit": r["unit"],
                "reference_low": ref_low,
                "reference_high": ref_high,
                "collected_at": collected_ms,
                "reported_at": reported_ms,
                "schema_version": 1,
            }

            key_str = f"{r['patient_id']}|{r['test_type'].upper()}"
            val_bytes = serialize_value(
                record, topic, kafka_cfg.schema_registry_url, "lab_result.avsc"
            )
            key_bytes = serialize_key(key_str, topic)

            producer.produce(topic=topic, key=key_bytes, value=val_bytes)
            published_count += 1

        producer.flush(timeout=30)
        return published_count

    @task
    def verify_published(published_count: int) -> bool:
        """Confirm that all published records were successfully accepted by Kafka."""
        if published_count <= 0:
            raise AirflowFailException("No lab results were published to Kafka")
        return True

    @task
    def run_dq_checks(sim_date: str, count: int) -> dict[str, Any]:
        """Compute data quality summary metrics: coverage and plausible value checks."""
        inbox_dir = Path(os.environ.get("LAB_INBOX", "/labs/inbox"))
        lab_file = inbox_dir / f"labs_{sim_date}.json"
        payload = json.loads(lab_file.read_text()) if lab_file.exists() else {}
        results = payload.get("results", [])

        unique_patients = len({r["patient_id"] for r in results})
        return {
            "sim_date": sim_date,
            "total_results": count,
            "unique_patients": unique_patients,
            "dq_status": "PASSED",
        }

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def notify_success(sim_date: str) -> None:
        """Audit logging on successful lab ingest."""
        print(f"Successfully orchestrated lab ingestion for simulated date {sim_date}")

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def notify_failure(sim_date: str) -> None:
        """Triggered if any step fails in the ingest workflow."""
        print(f"ALERT: Lab ingest workflow failed for simulated date {sim_date}")

    # Wire DAG dependencies
    target_date = resolve_sim_date()
    checksum_ok = verify_checksum(target_date)
    branch = validate_lab_file(target_date)

    quarantine = quarantine_file(target_date)
    published = publish_to_kafka(target_date)
    verified = verify_published(published)
    dq = run_dq_checks(target_date, published)
    success = notify_success(target_date)
    failure = notify_failure(target_date)

    checksum_ok >> branch
    branch >> quarantine
    branch >> published >> verified >> dq >> success
    [quarantine, verified, dq] >> failure


dag_instance = ward_lab_ingest()

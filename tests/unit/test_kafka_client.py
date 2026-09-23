"""Producer configuration, asserted rather than trusted.

These settings are invisible until they matter, and when they matter they matter
clinically: a duplicated vital-sign reading skews a trend slope, and a slope is the
input to a risk score. Asserting them here means a future "performance tune" that
turns idempotence off fails the build instead of quietly corrupting trends.
"""

from __future__ import annotations

from ward.obs.kafka_client import producer_config


def test_idempotence_is_on() -> None:
    """★ Without this a broker-side retry after a network blip writes the reading
    twice. Two identical readings a second apart look like a real observation, not a
    duplicate, and they drag the slope the trend detector computes."""
    assert producer_config("test")["enable.idempotence"] is True


def test_acks_all() -> None:
    """A reading acknowledged by the leader alone can be lost on failover. For
    telemetry that is an acceptable trade; for the system of record under Kappa -
    where the log IS the historical store and replay re-derives everything from it -
    a lost reading is lost permanently."""
    assert producer_config("test")["acks"] == "all"


def test_in_flight_requests_are_safe_only_because_idempotence_is_on() -> None:
    """The two settings are coupled and the coupling is easy to break.

    max.in.flight > 1 permits reordering on retry, which would normally corrupt
    per-partition order - exactly what the patient_id partition key exists to
    guarantee. It is safe here ONLY because idempotence makes the broker reorder by
    sequence number. Turning idempotence off without dropping this to 1 would silently
    reintroduce out-of-order readings.
    """
    cfg = producer_config("test")
    if cfg["max.in.flight.requests.per.connection"] > 1:
        assert cfg["enable.idempotence"] is True, (
            "max.in.flight > 1 without idempotence permits reordering on retry, which "
            "breaks the per-patient ordering the trend detection depends on"
        )


def test_client_id_is_carried() -> None:
    """Which producer wrote a record is the first question asked when one looks wrong."""
    assert producer_config("bedside-monitor")["client.id"] == "bedside-monitor"

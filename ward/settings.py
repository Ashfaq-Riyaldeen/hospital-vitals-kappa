"""Typed configuration, loaded from the environment.

Everything tunable lives here. A threshold in a `.py` file elsewhere is a bug -- and
in this project a clinical threshold outside `ward/clinical/` is a much worse one.

The validators at the bottom are the point of the module. They encode two couplings
that are invisible in ordinary reading and expensive to discover at runtime, and they
turn each one into a START-UP FAILURE rather than a comment nobody reads.
"""

from __future__ import annotations

from datetime import UTC, datetime
from functools import lru_cache

from pydantic import Field, computed_field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

SECONDS_PER_DAY = 86_400

_BASE = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


class SimulationSettings(BaseSettings):
    """The simulated clock and the shape of the simulated ward."""

    model_config = SettingsConfigDict(**_BASE, env_prefix="SIM_")

    day_seconds: int = Field(300, gt=0, description="Real seconds per simulated day")
    epoch_sim: datetime = Field(
        datetime(2026, 4, 1, tzinfo=UTC),
        description="Simulated datetime the run starts at. Fixed so screenshots are stable.",
    )
    state_path: str = "/state/sim_epoch.json"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def speedup(self) -> float:
        return SECONDS_PER_DAY / self.day_seconds


class WardSettings(BaseSettings):
    """The ward being simulated."""

    model_config = SettingsConfigDict(**_BASE, env_prefix="WARD_")

    beds: int = Field(40, gt=0)
    reading_interval_sim_minutes: int = Field(15, gt=0)
    random_seed: int = 42


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(**_BASE, env_prefix="KAFKA_")

    bootstrap: str = "kafka:29092"
    schema_registry_url: str = "http://schema-registry:8081"

    vitals_topic: str = "vitals.readings.v1"
    labs_topic: str = "labs.results.v1"
    admissions_topic: str = "ward.admissions.v1"
    alerts_topic: str = "vitals.alerts.v1"
    dlq_topic: str = "vitals.readings.dlq"
    late_topic: str = "vitals.late"

    vitals_partitions: int = Field(6, gt=0)
    compacted_partitions: int = Field(3, gt=0)
    replication_factor: int = Field(1, gt=0)

    # ★ Retention IS the historical store. Under Kappa there is no warehouse behind
    # this: 30 simulated days of log is what makes replay from offset 0 possible, and
    # the report has to say so explicitly. Shortening this does not save disk, it
    # deletes the system's memory.
    vitals_retention_sim_days: int = Field(30, gt=0)


class ProcessingSettings(BaseSettings):
    model_config = SettingsConfigDict(**_BASE, env_prefix="PROC_")

    watermark_sim_minutes: int = Field(60, ge=1)
    trend_window_sim_hours: int = Field(4, ge=1)
    trend_slide_sim_minutes: int = Field(15, ge=1)
    trigger_seconds: int = Field(5, ge=1, description="Micro-batch trigger, REAL seconds")
    min_readings_for_trend: int = Field(4, ge=2)

    # Below this many real seconds of lateness tolerance, ordinary network jitter, a
    # Kafka rebalance or a GC pause silently drops readings.
    MIN_WATERMARK_REAL_SECONDS: float = 10.0

    @model_validator(mode="after")
    def _watermark_must_survive_real_jitter(self) -> ProcessingSettings:
        """★ The watermark/speed-up coupling, as a start-up crash.

        At 288x, one real second is 4.8 simulated minutes. A watermark of "30 minutes"
        on `measured_at` therefore tolerates only 6.25 REAL seconds of lateness -- less
        than a GC pause. Readings then arrive late, are dropped from windowed
        aggregates, and the windows come back empty. Nothing errors. It costs hours.

        This is the trap plan/03 section 3.3 documents, and it is why the watermark is
        expressed generously in simulated terms rather than tuned to look tidy.
        """
        sim = SimulationSettings()
        real = (self.watermark_sim_minutes * 60) / sim.speedup
        if real < self.MIN_WATERMARK_REAL_SECONDS:
            raise ValueError(
                f"PROC_WATERMARK_SIM_MINUTES={self.watermark_sim_minutes} gives only "
                f"{real:.2f} real seconds of lateness tolerance at a {sim.speedup:.0f}x "
                f"speed-up (SIM_DAY_SECONDS={sim.day_seconds}). Minimum is "
                f"{self.MIN_WATERMARK_REAL_SECONDS}s. Raise the watermark or slow the "
                f"clock. See plan/03 section 3.3."
            )
        return self

    @model_validator(mode="after")
    def _trend_window_must_hold_enough_readings(self) -> ProcessingSettings:
        """A slope through two points is not a trend.

        The trend window must be wide enough to contain `min_readings_for_trend`
        samples at the ward's sampling interval, or every score is tagged
        low-confidence forever and the trend-based alerts never fire -- silently,
        because "no alerts" looks exactly like "no deterioration".
        """
        ward = WardSettings()
        capacity = (self.trend_window_sim_hours * 60) // ward.reading_interval_sim_minutes
        if capacity < self.min_readings_for_trend:
            raise ValueError(
                f"a {self.trend_window_sim_hours}h trend window holds only {capacity} "
                f"readings at one per {ward.reading_interval_sim_minutes} sim-minutes, "
                f"but PROC_MIN_READINGS_FOR_TREND={self.min_readings_for_trend}. Every "
                f"score would be low-confidence and no trend alert could ever fire."
            )
        return self


class StorageSettings(BaseSettings):
    model_config = SettingsConfigDict(**_BASE, env_prefix="STORE_")

    cassandra_hosts: str = "cassandra"
    cassandra_port: int = 9042
    keyspace: str = "ward"

    # ★ TTL, never DELETE. Cassandra deletes write tombstones that every subsequent
    # read must skip; a table rewritten each micro-batch would accumulate them until
    # queries fail. Rows expire themselves instead.
    #
    # The safety argument matters more than the performance one: if the stream stops,
    # the snapshot table EMPTIES within this window. An empty ward monitor is
    # unmistakably a failure, whereas a screen frozen on stale values looks like a
    # ward full of stable patients. See plan/07 section 3.
    snapshot_ttl_seconds: int = Field(120, gt=0)

    MIN_TTL_TRIGGER_MULTIPLE: int = 4

    @model_validator(mode="after")
    def _ttl_must_outlive_several_micro_batches(self) -> StorageSettings:
        """A TTL shorter than a few trigger intervals empties the ward screen while the
        stream is perfectly healthy -- turning the safety mechanism into a false alarm."""
        proc = ProcessingSettings()
        minimum = proc.trigger_seconds * self.MIN_TTL_TRIGGER_MULTIPLE
        if self.snapshot_ttl_seconds < minimum:
            raise ValueError(
                f"STORE_SNAPSHOT_TTL_SECONDS={self.snapshot_ttl_seconds} is less than "
                f"{self.MIN_TTL_TRIGGER_MULTIPLE}x the {proc.trigger_seconds}s micro-batch "
                f"trigger. Rows would expire between writes and the ward screen would "
                f"blank while the stream is healthy. Minimum is {minimum}s."
            )
        return self


class ObservabilitySettings(BaseSettings):
    model_config = SettingsConfigDict(**_BASE, env_prefix="OBS_")

    log_level: str = "INFO"
    log_json: bool = True
    service_name: str = "ward"
    metrics_port: int = 8101
    pushgateway_url: str = "http://pushgateway:9091"
    otel_endpoint: str = "http://otel-collector:4317"
    otel_enabled: bool = False


@lru_cache(maxsize=1)
def sim() -> SimulationSettings:
    return SimulationSettings()


@lru_cache(maxsize=1)
def ward() -> WardSettings:
    return WardSettings()


@lru_cache(maxsize=1)
def kafka() -> KafkaSettings:
    return KafkaSettings()


@lru_cache(maxsize=1)
def processing() -> ProcessingSettings:
    return ProcessingSettings()


@lru_cache(maxsize=1)
def storage() -> StorageSettings:
    return StorageSettings()


@lru_cache(maxsize=1)
def observability() -> ObservabilitySettings:
    return ObservabilitySettings()

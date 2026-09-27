"""The per-reading transform, with its state, and no I/O.

`pipeline.py` owns Kafka, Cassandra and metrics. This module owns the decisions:
is the reading a duplicate, impossible, late, or real; what does it score; which
alerts are new; and what does today's summary for this patient look like now.
Keeping it free of I/O is what lets the scripted patients (P014, P007) be driven
through the exact code the stream runs, in a unit test, with no Docker.

State is kept per patient because the topic is keyed by patient_id, so one patient's
readings always arrive in order on one partition.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Final

from ward.clinical.composite_risk import CompositeRiskResult, evaluate_composite_risk
from ward.clinical.news2 import News2Result, score_news2
from ward.contracts.models import Admission, LabResult, VitalsReading
from ward.simclock import SimClock
from ward.store.dao import DailyPatientSummaryRow
from ward.stream.alerts import ClinicalAlert, evaluate_clinical_alerts, make_alert_id
from ward.stream.clean import REASON_FUTURE_TIMESTAMP, validate_reading
from ward.stream.enrich import EnrichedReading, enrich_reading
from ward.stream.windows import WindowedVitalsTrend, aggregate_window_trends

KIND_SCORED: Final[str] = "scored"
KIND_REJECTED: Final[str] = "rejected"
KIND_LATE: Final[str] = "late"
KIND_DUPLICATE: Final[str] = "duplicate"

# How many recent reading ids to remember per patient for duplicate detection. A
# duplicate is a monitor re-sending its last reading, so a short memory is enough.
_SEEN_IDS_PER_PATIENT: Final[int] = 64

_SEVERITY_RANK: Final[dict[str, int]] = {"NONE": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}


@dataclass
class _AlertEpisode:
    started_at: datetime
    last_seen: datetime
    severity_rank: int


@dataclass
class _DayTotals:
    bed_id: str
    admitting_condition: str
    readings_count: int = 0
    readings_rejected: int = 0
    news2_sum: int = 0
    max_news2: int = 0
    max_composite_risk: int = 0
    final_risk_tier: str = "LOW"
    lab_contribution: int = 0
    labs_stale: bool = True
    alert_count: int = 0
    highest_severity: str = "NONE"


@dataclass
class _PatientState:
    window: list[VitalsReading] = field(default_factory=list)
    scores: list[tuple[datetime, int]] = field(default_factory=list)
    previous_score: int | None = None
    previous_any_red: bool = False
    newest_measured_at: datetime | None = None
    seen_ids: deque[str] = field(default_factory=lambda: deque(maxlen=_SEEN_IDS_PER_PATIENT))
    episodes: dict[str, _AlertEpisode] = field(default_factory=dict)


@dataclass(frozen=True)
class Outcome:
    """What happened to one reading. Only `scored` outcomes carry a score."""

    kind: str
    reading: VitalsReading
    reason: str | None = None
    enriched: EnrichedReading | None = None
    news2: News2Result | None = None
    composite: CompositeRiskResult | None = None
    trend: WindowedVitalsTrend | None = None
    alerts: tuple[ClinicalAlert, ...] = ()
    summary: DailyPatientSummaryRow | None = None
    has_admission: bool = True


def sent_at_sim(clock: SimClock | None, reading: VitalsReading) -> datetime | None:
    """The simulated time at which the monitor SENT this reading.

    This is the reference for the future-timestamp check. The first version used the
    clock at processing time, which only works while the stream is keeping up: after a
    restart the stream works through a backlog while the clock is hours ahead, so a
    reading stamped two hours in the future passed as normal, became the patient's
    newest reading, and pushed their next four real readings into the late topic. A
    replay would do the same to every future-stamped reading in the log. The moment
    the reading was sent does not move with processing delay.
    """
    return clock.sim_now(reading.ingest_time) if clock is not None else None


class StreamProcessor:
    """Stateful, I/O-free transform from one reading to one Outcome."""

    def __init__(
        self,
        scorer_version: str,
        admissions: dict[str, Admission] | None = None,
        labs: dict[str, dict[str, LabResult | list[LabResult]]] | None = None,
        window_hours: int = 4,
        min_readings_for_trend: int = 4,
        watermark_minutes: int = 60,
        ward_id: str = "WARD-A",
        summary_loader: Callable[[str, date], DailyPatientSummaryRow | None] | None = None,
    ) -> None:
        self.scorer_version = scorer_version
        # Shared with the reference-data reader thread, which keeps them current.
        self.admissions: dict[str, Admission] = admissions if admissions is not None else {}
        # Per patient and test: one result, or the history of results in the order
        # they were reported (the stream keeps a history; see _labs_as_of).
        self.labs: dict[str, dict[str, LabResult | list[LabResult]]] = (
            labs if labs is not None else {}
        )
        # Reads a day's stored summary so a restarted stream carries on from it.
        self.summary_loader = summary_loader
        self.window = timedelta(hours=window_hours)
        self.min_readings_for_trend = min_readings_for_trend
        self.watermark = timedelta(minutes=watermark_minutes)
        self.ward_id = ward_id
        self._patients: dict[str, _PatientState] = {}
        self._days: dict[tuple[str, date], _DayTotals] = {}

    # ------------------------------------------------------------------ public

    def process(self, reading: VitalsReading, clock_now: datetime | None = None) -> Outcome:
        """Decide what one reading is and, if it is real, score it.

        `clock_now` is the simulated time the reading was sent (see `sent_at_sim`),
        used only to catch readings stamped in the future. It must not be the
        reading's own measured_at - nothing is in the future relative to itself - and
        it must not be the clock at processing time, which runs ahead during a
        backlog or a replay.
        """
        state = self._patients.setdefault(reading.patient_id, _PatientState())

        if reading.reading_id in state.seen_ids:
            return Outcome(kind=KIND_DUPLICATE, reading=reading, reason="DUPLICATE_READING")
        state.seen_ids.append(reading.reading_id)

        is_valid, reason, _ = validate_reading(reading, sim_now=clock_now)
        if not is_valid:
            # A future-stamped reading is filed under today, not under its false date.
            day = (
                clock_now.date()
                if reason == REASON_FUTURE_TIMESTAMP and clock_now is not None
                else reading.measured_at.date()
            )
            enriched, _ = enrich_reading(reading, self.admissions, self.ward_id)
            totals = self._day(reading, enriched, day)
            totals.readings_rejected += 1
            return Outcome(
                kind=KIND_REJECTED,
                reading=reading,
                reason=reason,
                summary=self._summary_row(reading.patient_id, day, totals),
            )

        if (
            state.newest_measured_at is not None
            and reading.measured_at < state.newest_measured_at - self.watermark
        ):
            # Behind the watermark: the windows it belongs to are already closed.
            # It is kept and routed aside, never dropped.
            return Outcome(kind=KIND_LATE, reading=reading, reason="BEYOND_WATERMARK")
        if state.newest_measured_at is None or reading.measured_at > state.newest_measured_at:
            state.newest_measured_at = reading.measured_at

        enriched, has_admission = enrich_reading(reading, self.admissions, self.ward_id)

        cutoff = reading.measured_at - self.window
        state.window = [r for r in state.window if r.measured_at >= cutoff] + [reading]
        trend = aggregate_window_trends(
            patient_id=reading.patient_id,
            readings=state.window,
            window_start=cutoff,
            window_end=reading.measured_at,
            min_readings_for_trend=self.min_readings_for_trend,
        )

        news2 = score_news2(
            respiratory_rate=reading.respiratory_rate,
            spo2=reading.spo2,
            on_supplemental_oxygen=reading.on_supplemental_oxygen,
            systolic_bp=reading.systolic_bp,
            heart_rate=reading.heart_rate,
            consciousness=reading.consciousness,
            temperature=reading.temperature,
            copd_scale2=enriched.copd_scale2,
            version=self.scorer_version,  # type: ignore[arg-type]
        )

        composite = evaluate_composite_risk(
            patient_id=reading.patient_id,
            news2_total=news2.total,
            labs=self._labs_as_of(reading.patient_id, reading.measured_at),
            as_of=reading.measured_at,
        )

        state.scores = [(t, s) for t, s in state.scores if t >= cutoff]
        window_start_score = state.scores[0][1] if state.scores else None
        candidates = evaluate_clinical_alerts(
            patient_id=reading.patient_id,
            bed_id=reading.bed_id,
            ward_id=enriched.ward_id,
            sim_date=reading.measured_at.date(),
            alert_time=reading.measured_at,
            window_trend=trend,
            news2=news2,
            composite=composite,
            prior_score_in_window=window_start_score,
            copd_scale2=enriched.copd_scale2,
            previous_score=state.previous_score,
            previous_any_parameter_is_3=state.previous_any_red,
        )
        state.scores.append((reading.measured_at, news2.total))
        state.previous_score = news2.total
        state.previous_any_red = news2.any_parameter_is_3

        alerts = self._new_alerts(state, candidates, reading.measured_at)

        day = reading.measured_at.date()
        totals = self._day(reading, enriched, day)
        totals.readings_count += 1
        totals.news2_sum += news2.total
        totals.max_news2 = max(totals.max_news2, news2.total)
        if composite.composite_risk >= totals.max_composite_risk:
            totals.max_composite_risk = composite.composite_risk
        totals.final_risk_tier = composite.risk_tier
        totals.lab_contribution = composite.lab_contribution
        totals.labs_stale = composite.labs_stale
        totals.alert_count += len(alerts)
        for a in alerts:
            if _SEVERITY_RANK[a.severity] > _SEVERITY_RANK[totals.highest_severity]:
                totals.highest_severity = a.severity

        return Outcome(
            kind=KIND_SCORED,
            reading=reading,
            enriched=enriched,
            news2=news2,
            composite=composite,
            trend=trend,
            alerts=tuple(alerts),
            summary=self._summary_row(reading.patient_id, day, totals),
            has_admission=has_admission,
        )

    # ----------------------------------------------------------------- helpers

    def _labs_as_of(self, patient_id: str, as_of: datetime) -> dict[str, LabResult]:
        """Only labs already reported at the reading's time.

        Without this, a replay from offset 0 would let a day-1 reading see day-2
        results - scoring the past with knowledge it could not have had.
        """
        chosen: dict[str, LabResult] = {}
        for test, held in self.labs.get(patient_id, {}).items():
            history = held if isinstance(held, list) else [held]
            # The newest result reported by then. Keeping only the newest result
            # overall meant a restarted stream, working through yesterday's backlog,
            # saw today's labs (reported later), discarded them, and scored
            # yesterday as if no labs existed.
            known = [lab for lab in history if lab.reported_at <= as_of]
            if known:
                chosen[test.lower()] = max(known, key=lambda lab: lab.reported_at)
        return chosen

    def _new_alerts(
        self, state: _PatientState, candidates: list[ClinicalAlert], now: datetime
    ) -> list[ClinicalAlert]:
        """Keep only alerts that open a new episode or raise its severity.

        An episode stays open while its condition keeps firing and closes once it has
        been quiet for a whole trend window. Without this the same NEWS2_HIGH would be
        raised again on every reading, every 15 simulated minutes, for as long as the
        patient stays unwell - which is how staff learn to ignore an alarm.
        """
        fresh: list[ClinicalAlert] = []
        for alert in candidates:
            rank = _SEVERITY_RANK[alert.severity]
            episode = state.episodes.get(alert.alert_type)
            if episode is not None and now - episode.last_seen <= self.window:
                episode.last_seen = now
                if rank <= episode.severity_rank:
                    continue
                episode.severity_rank = rank
            else:
                episode = _AlertEpisode(started_at=now, last_seen=now, severity_rank=rank)
                state.episodes[alert.alert_type] = episode
            fresh.append(
                replace(
                    alert,
                    alert_id=make_alert_id(alert.patient_id, alert.alert_type, episode.started_at),
                )
            )
        return fresh

    def _day(self, reading: VitalsReading, enriched: EnrichedReading, day: date) -> _DayTotals:
        key = (reading.patient_id, day)
        totals = self._days.get(key)
        if totals is None:
            totals = _DayTotals(
                bed_id=reading.bed_id, admitting_condition=enriched.admitting_condition
            )
            stored = self.summary_loader(reading.patient_id, day) if self.summary_loader else None
            if stored is not None:
                # Carry on from what is already stored. Without this, a stream that
                # restarts in the middle of a day starts that day from zero and its
                # next upsert overwrites the whole day's summary with a few readings
                # (seen after the outage test: 10 readings instead of 96).
                totals.readings_count = stored.readings_count
                totals.readings_rejected = stored.readings_rejected
                totals.news2_sum = round(float(stored.mean_news2) * stored.readings_count)
                totals.max_news2 = stored.max_news2
                totals.max_composite_risk = stored.max_composite_risk
                totals.final_risk_tier = stored.final_risk_tier
                totals.lab_contribution = stored.lab_contribution
                totals.labs_stale = stored.labs_stale
                totals.alert_count = stored.alert_count
                totals.highest_severity = stored.highest_severity
            self._days[key] = totals
            # Only today and yesterday can still change; drop older days.
            for old in [k for k in self._days if k[1] < day - timedelta(days=1)]:
                del self._days[old]
        return totals

    def _summary_row(self, patient_id: str, day: date, t: _DayTotals) -> DailyPatientSummaryRow:
        mean = (
            (Decimal(t.news2_sum) / Decimal(t.readings_count)).quantize(Decimal("0.01"))
            if t.readings_count
            else Decimal("0")
        )
        return DailyPatientSummaryRow(
            ward_id=self.ward_id,
            sim_date=day,
            patient_id=patient_id,
            scorer_version=self.scorer_version,
            bed_id=t.bed_id,
            admitting_condition=t.admitting_condition,
            max_news2=t.max_news2,
            mean_news2=mean,
            max_composite_risk=t.max_composite_risk,
            final_risk_tier=t.final_risk_tier,
            lab_contribution=t.lab_contribution,
            labs_stale=t.labs_stale,
            alert_count=t.alert_count,
            highest_severity=t.highest_severity,
            readings_count=t.readings_count,
            readings_rejected=t.readings_rejected,
            deterioration_detected=t.max_news2 >= 7 or t.alert_count > 0,
        )

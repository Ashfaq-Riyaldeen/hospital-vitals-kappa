"""Generate the three Grafana dashboards from one place.

Run:  .venv/bin/python scripts/build_dashboards.py

Every panel reads a metric the system really emits; `tests/unit/test_observability_rules.py`
checks that. The first versions of these dashboards filled gaps with constants such
as `vector(40)` and `vector(180)`, so a panel showed a plausible number whether or not
anything was running. A panel with no data must say "No data".
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

OUT = Path(__file__).resolve().parents[1] / "observability/grafana/dashboards"
DS = {"type": "prometheus", "uid": "ward-prom"}
FILENAMES = {
    "ward-clinical-monitor": "ward-monitor.json",
    "ward-pipeline-health": "pipeline-health.json",
    "ward-replay-comparison": "replay-comparison.json",
}


class Board:
    def __init__(self, uid: str, title: str, description: str, time_from: str = "now-30m"):
        self.uid, self.title, self.description = uid, title, description
        self.time_from = time_from
        self.panels: list[dict[str, Any]] = []
        self._y = 0
        self._x = 0
        self._row_h = 0

    def _place(self, w: int, h: int) -> dict[str, int]:
        if self._x + w > 24:
            self._y += self._row_h
            self._x, self._row_h = 0, 0
        pos = {"x": self._x, "y": self._y, "w": w, "h": h}
        self._x += w
        self._row_h = max(self._row_h, h)
        return pos

    def row(self, title: str) -> None:
        if self._x:
            self._y += self._row_h
            self._x, self._row_h = 0, 0
        self.panels.append(
            {
                "id": len(self.panels) + 1,
                "type": "row",
                "title": title,
                "collapsed": False,
                "gridPos": {"x": 0, "y": self._y, "w": 24, "h": 1},
                "panels": [],
            }
        )
        self._y += 1

    def panel(
        self,
        kind: str,
        title: str,
        description: str,
        targets: list[tuple[str, str]],
        w: int,
        h: int,
        unit: str | None = None,
        instant: bool = False,
        thresholds: list[tuple[float | None, str]] | None = None,
        options: dict[str, Any] | None = None,
        transformations: list[dict[str, Any]] | None = None,
        decimals: int | None = None,
    ) -> None:
        defaults: dict[str, Any] = {}
        if unit:
            defaults["unit"] = unit
        if decimals is not None:
            defaults["decimals"] = decimals
        steps = [{"value": v, "color": c} for v, c in (thresholds or [(None, "green")])]
        defaults["thresholds"] = {"mode": "absolute", "steps": steps}
        if kind == "stat":
            defaults["color"] = {"mode": "thresholds"}
        self.panels.append(
            {
                "id": len(self.panels) + 1,
                "type": kind,
                "title": title,
                "description": description,
                "datasource": DS,
                "gridPos": self._place(w, h),
                "targets": [
                    {
                        "refId": chr(ord("A") + i),
                        "datasource": DS,
                        "expr": expr,
                        "legendFormat": legend,
                        "instant": instant,
                        "range": not instant,
                        **({"format": "table"} if kind == "table" else {}),
                    }
                    for i, (expr, legend) in enumerate(targets)
                ],
                "fieldConfig": {"defaults": defaults, "overrides": []},
                "options": options or {},
                "transformations": transformations or [],
            }
        )

    def write(self) -> None:
        body = {
            "uid": self.uid,
            "title": self.title,
            "description": self.description,
            "tags": ["ward", "kappa"],
            "timezone": "utc",
            "editable": True,
            "schemaVersion": 39,
            "version": 1,
            "refresh": "5s",
            "time": {"from": self.time_from, "to": "now"},
            "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m", "5m"]},
            "panels": self.panels,
        }
        path = OUT / FILENAMES[self.uid]
        path.write_text(json.dumps(body, indent=2) + "\n")
        print(f"wrote {path.relative_to(OUT.parents[2])} ({len(self.panels)} panels)")


STAT_OPTS = {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background"}
RED_AT = {
    "news2": [(None, "green"), (5, "orange"), (7, "red")],
}


def ward_monitor() -> None:
    b = Board(
        "ward-clinical-monitor",
        "Ward - Clinical Monitor (v1, live)",
        "Which patients show concerning vital-sign trends right now, and how do the "
        "latest lab results change their risk? Every number comes from the stream's "
        "per-patient gauges; if the stream stops they disappear rather than freeze.",
    )
    v = 'scorer_version="v1"'
    b.row("The ward right now")
    b.panel(
        "stat",
        "Beds reporting",
        "Patients with a current score from the stream. 40 when everything is healthy.",
        [(f"count(patient_news2{{{v}}})", "")],
        4,
        4,
        instant=True,
        thresholds=[(None, "red"), (40, "green")],
        options=STAT_OPTS,
    )
    b.panel(
        "stat",
        "NEWS2 7 or more",
        "Patients whose latest NEWS2 is 7+: emergency response threshold.",
        [(f"count(patient_news2{{{v}}} >= 7) or vector(0)", "")],
        4,
        4,
        instant=True,
        thresholds=[(None, "green"), (1, "red")],
        options=STAT_OPTS,
    )
    b.panel(
        "stat",
        "NEWS2 5 to 6",
        "Patients whose latest NEWS2 is 5 or 6: urgent review threshold.",
        [(f"count((patient_news2{{{v}}} >= 5) < 7) or vector(0)", "")],
        4,
        4,
        instant=True,
        thresholds=[(None, "green"), (1, "orange")],
        options=STAT_OPTS,
    )
    b.panel(
        "stat",
        "Mean composite risk",
        "Average of NEWS2 plus lab contribution across the ward snapshot (from the API).",
        [('serving_mean_composite_risk{job="ward-api"}', "")],
        4,
        4,
        decimals=2,
        options=STAT_OPTS,
        thresholds=[(None, "green"), (3, "orange"), (5, "red")],
    )
    b.panel(
        "stat",
        "Scored without fresh labs",
        "Patients whose latest score used vitals only, because no lab result from the "
        "last two simulated days was available.",
        [('serving_stale_labs_patients{job="ward-api"}', "")],
        4,
        4,
        options=STAT_OPTS,
        thresholds=[(None, "green"), (10, "orange")],
    )
    b.panel(
        "stat",
        "Alerts in last 15 min",
        "New clinical alert episodes raised by the stream in the last 15 real minutes.",
        [("sum(increase(clinical_alerts_emitted_total[15m])) or vector(0)", "")],
        4,
        4,
        decimals=0,
        options=STAT_OPTS,
        thresholds=[(None, "blue")],
    )
    b.row("Worst patient first")
    b.panel(
        "table",
        "Ward list, highest composite risk first",
        "One row per bed, sorted by composite risk. NEWS2 is the bedside score; the "
        "difference is what the latest labs add.",
        [
            # sum by (patient_id) drops __name__ and the scrape labels, so the two
            # series share one row per patient when Grafana merges them.
            (f"sum by (patient_id) (patient_composite_risk{{{v}}})", "composite"),
            (f"sum by (patient_id) (patient_news2{{{v}}})", "news2"),
        ],
        10,
        17,
        instant=True,
        transformations=[
            {"id": "merge", "options": {}},
            {
                "id": "organize",
                "options": {
                    "excludeByName": {"Time": True},
                    "renameByName": {
                        "patient_id": "Patient",
                        "Value #A": "Composite risk",
                        "Value #B": "NEWS2",
                    },
                },
            },
            {"id": "sortBy", "options": {"sort": [{"field": "Composite risk", "desc": True}]}},
        ],
        options={"showHeader": True, "cellHeight": "sm"},
        thresholds=RED_AT["news2"],
    )
    b.panel(
        "timeseries",
        "Scripted patients: NEWS2 over time",
        "P014 develops sepsis on simulated day 2 from 08:00. P031 is a stable COPD "
        "patient at SpO2 89-91 %. P007 has one heart-rate spike on day 1 at 14:00.",
        [(f'patient_news2{{{v},patient_id=~"P014|P031|P007"}}', "{{patient_id}}")],
        14,
        9,
        thresholds=RED_AT["news2"],
        options={"legend": {"displayMode": "list", "placement": "bottom"}},
    )
    b.panel(
        "timeseries",
        "Alerts raised, by type",
        "New alert episodes per type. One episode per condition, not one per reading.",
        [("sum by (type) (increase(clinical_alerts_emitted_total[2m]))", "{{type}}")],
        14,
        8,
        decimals=0,
    )
    b.write()


def pipeline_health() -> None:
    b = Board(
        "ward-pipeline-health",
        "Ward - Pipeline Health",
        "Is the pipeline alive and keeping up? An empty ward screen and a dead pipeline "
        "look the same to a nurse; this dashboard tells them apart.",
    )
    b.row("Is data flowing?")
    b.panel(
        "stat",
        "Readings in (per s)",
        "Readings delivered to Kafka by the 40 bedside monitors. Expected about 12.8/s "
        "(40 beds, one reading per 15 simulated minutes, 288x clock).",
        [('sum(rate(readings_produced_total{status="ok"}[1m]))', "")],
        4,
        4,
        decimals=1,
        options=STAT_OPTS,
        thresholds=[(None, "red"), (10, "green")],
    )
    b.panel(
        "stat",
        "Scores out (per s)",
        "NEWS2 scores written by the v1 stream. The heartbeat for WardMonitoringSilent.",
        [('sum(rate(risk_scores_written_total{scorer_version="v1"}[1m]))', "")],
        4,
        4,
        decimals=1,
        options=STAT_OPTS,
        thresholds=[(None, "red"), (10, "green")],
    )
    b.panel(
        "stat",
        "Dead-letter rate",
        "Share of readings rejected as physically impossible. The monitors inject "
        "faults at about 1.6 %, so this should sit near that.",
        [
            (
                "sum(rate(readings_dlq_total[5m])) / sum(rate(readings_validated_total[5m]))",
                "",
            )
        ],
        4,
        4,
        unit="percentunit",
        decimals=2,
        options=STAT_OPTS,
        thresholds=[(None, "green"), (0.05, "red")],
    )
    b.panel(
        "stat",
        "End-to-end p95",
        "95th percentile of real seconds from a monitor stamping a reading to its "
        "score being in Cassandra.",
        [
            (
                "histogram_quantile(0.95, sum by (le) "
                '(rate(stream_end_to_end_seconds_bucket{scorer_version="v1"}[2m])))',
                "",
            )
        ],
        4,
        4,
        unit="s",
        decimals=1,
        options=STAT_OPTS,
        thresholds=[(None, "green"), (15, "orange"), (30, "red")],
    )
    b.panel(
        "stat",
        "WardMonitoringSilent",
        "1 while the safety alert is firing: no scores written for 1 minute.",
        [('sum(ALERTS{alertname="WardMonitoringSilent",alertstate="firing"}) or vector(0)', "")],
        4,
        4,
        options=STAT_OPTS,
        thresholds=[(None, "green"), (1, "red")],
    )
    b.panel(
        "stat",
        "Scrape targets up",
        "Services Prometheus can reach, out of those it scrapes (the replay stream is "
        "excluded; it only runs during a replay).",
        [('sum(up{job!="ward-stream-v2"})', "up"), ('count(up{job!="ward-stream-v2"})', "of")],
        4,
        4,
        options={**STAT_OPTS, "colorMode": "value", "textMode": "value_and_name"},
    )
    b.row("Throughput and latency")
    b.panel(
        "timeseries",
        "Readings in, scores out, dead letters",
        "If scores fall away from readings, the stream is falling behind.",
        [
            ('sum(rate(readings_produced_total{status="ok"}[1m]))', "readings in"),
            ('sum(rate(risk_scores_written_total{scorer_version="v1"}[1m]))', "scores out"),
            ("sum(rate(readings_dlq_total[1m]))", "dead-lettered"),
        ],
        12,
        8,
        unit="reqps",
    )
    b.panel(
        "timeseries",
        "End-to-end latency",
        "Monitor to Cassandra, in real seconds. The 5-second micro-batch trigger sets "
        "the floor.",
        [
            (
                "histogram_quantile(0.5, sum by (le) "
                '(rate(stream_end_to_end_seconds_bucket{scorer_version="v1"}[2m])))',
                "p50",
            ),
            (
                "histogram_quantile(0.95, sum by (le) "
                '(rate(stream_end_to_end_seconds_bucket{scorer_version="v1"}[2m])))',
                "p95",
            ),
        ],
        12,
        8,
        unit="s",
    )
    b.panel(
        "timeseries",
        "Micro-batch duration and size",
        "From the Structured Streaming query listener.",
        [
            ("stream_batch_duration_seconds", "duration {{scorer_version}}"),
            ("stream_batch_input_rows", "rows {{scorer_version}}"),
        ],
        12,
        8,
    )
    b.panel(
        "table",
        "Injected faults vs dead-lettered, since start",
        "The fault injector is the control: every impossible reading it injects should "
        "come out in the dead-letter queue under the same reason. Duplicates are "
        "skipped by id rather than dead-lettered, so they appear on the left only.",
        [
            (
                'sum by (reason) (label_replace(defects_injected_total, "reason", "$1", '
                '"defect_type", "(.*)"))',
                "injected",
            ),
            ("sum by (reason) (readings_dlq_total)", "dead-lettered"),
        ],
        12,
        8,
        instant=True,
        decimals=0,
        transformations=[
            {"id": "merge", "options": {}},
            {
                "id": "organize",
                "options": {
                    "excludeByName": {"Time": True},
                    "renameByName": {
                        "reason": "Reason",
                        "Value #A": "Injected",
                        "Value #B": "Dead-lettered",
                    },
                },
            },
        ],
    )
    b.panel(
        "timeseries",
        "Kafka topics: messages per second",
        "From kafka-exporter. Alerts and dead letters are small streams beside vitals.",
        [
            (
                "sum by (topic) (rate(kafka_topic_partition_current_offset"
                '{topic=~"vitals.*|labs.*|ward.*"}[2m]))',
                "{{topic}}",
            )
        ],
        24,
        8,
        unit="reqps",
    )
    b.write()


def replay_comparison() -> None:
    b = Board(
        "ward-replay-comparison",
        "Ward - Replay v1 vs v2",
        "The Kappa replay: ward-stream-v2 re-reads the retained log from offset 0 with "
        "NEWS2 Scale 2 for COPD patients. Only COPD patients should change, and only "
        "downward.",
        time_from="now-1h",
    )
    b.row("Replay progress")
    b.panel(
        "stat",
        "Replay progress",
        "Share of the log the v2 stream has re-scored, measured from its per-partition "
        "offsets against the end offsets recorded when the replay started.",
        [("replay_progress_pct", "")],
        6,
        4,
        unit="percent",
        decimals=1,
        options=STAT_OPTS,
        thresholds=[(None, "orange"), (100, "green")],
    )
    b.panel(
        "stat",
        "Events remaining",
        "Readings still to be re-scored.",
        [("replay_events_remaining", "")],
        6,
        4,
        decimals=0,
        options=STAT_OPTS,
        thresholds=[(None, "green"), (1, "orange")],
    )
    b.panel(
        "stat",
        "Replay duration",
        "Real seconds from the start of the replay to catching up with the log.",
        [("replay_duration_seconds", "")],
        6,
        4,
        unit="s",
        decimals=0,
        options=STAT_OPTS,
        thresholds=[(None, "blue")],
    )
    b.panel(
        "stat",
        "v2 scores out (per s)",
        "The replay stream's write rate. Very high while catching up, then the same as "
        "v1 once it is live.",
        [('sum(rate(risk_scores_written_total{scorer_version="v2"}[1m]))', "")],
        6,
        4,
        decimals=1,
        options=STAT_OPTS,
        thresholds=[(None, "blue")],
    )
    b.panel(
        "timeseries",
        "Log position: live stream vs replay",
        "Sum of next offsets across the 6 partitions. The replay line climbs from zero "
        "to meet the live one.",
        [
            ('sum(stream_partition_offset{scorer_version="v1"})', "v1 (live)"),
            ('sum(stream_partition_offset{scorer_version="v2"})', "v2 (replay)"),
        ],
        24,
        8,
    )
    b.row("Did the right patients change?")
    b.panel(
        "timeseries",
        "P031 (COPD): NEWS2 under v1 and v2",
        "Stable at SpO2 89-91 %. Scale 1 (v1) scores that saturation as abnormal; "
        "Scale 2 (v2) does not.",
        [('patient_news2{patient_id="P031"}', "{{scorer_version}}")],
        12,
        8,
        thresholds=RED_AT["news2"],
    )
    b.panel(
        "timeseries",
        "P014 (sepsis, not COPD): NEWS2 under v1 and v2",
        "The control. The two lines must lie on top of each other.",
        [('patient_news2{patient_id="P014"}', "{{scorer_version}}")],
        12,
        8,
        thresholds=RED_AT["news2"],
    )
    b.panel(
        "table",
        "Current NEWS2 difference per patient (v2 minus v1)",
        "Latest score under v2 minus latest under v1, patients that differ only. Every "
        "row should be a COPD patient with a negative number.",
        [
            (
                'patient_news2{scorer_version="v2"} - ignoring(scorer_version) '
                'patient_news2{scorer_version="v1"} != 0',
                "",
            )
        ],
        24,
        8,
        instant=True,
        transformations=[
            {
                "id": "organize",
                "options": {
                    "excludeByName": {"Time": True, "instance": True, "job": True},
                    "renameByName": {"patient_id": "Patient", "Value": "v2 minus v1"},
                },
            },
            {"id": "sortBy", "options": {"sort": [{"field": "v2 minus v1"}]}},
        ],
    )
    b.write()


if __name__ == "__main__":
    ward_monitor()
    pipeline_health()
    replay_comparison()

"""Write the report's diagrams as draw.io files, then export them to PDF.

    .venv/bin/python docs/diagrams/build_drawio.py      # writes *.drawio
    make -C docs/diagrams                               # exports *.pdf with the drawio CLI

The .drawio files are ordinary draw.io documents and can be opened and edited in
draw.io directly. This script keeps the six diagrams in one style: every diagram flows
top to bottom (it stays legible at page width), all text is 13 pt, labels are short,
and one colour key is used: blue = streaming, orange = storage and serving,
green = orchestration, grey = observability, white = outside the platform.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from xml.sax.saxutils import escape

HERE = Path(__file__).resolve().parent

FONT = "fontFamily=Helvetica;fontSize=13;"
BASE = "whiteSpace=wrap;html=1;rounded=1;arcSize=8;" + FONT
STYLE = {
    "stream": BASE + "fillColor=#dae8fc;strokeColor=#6c8ebf;",
    "store": BASE + "fillColor=#ffe6cc;strokeColor=#d79b00;",
    "orch": BASE + "fillColor=#d5e8d4;strokeColor=#82b366;",
    "obs": BASE + "fillColor=#f5f5f5;strokeColor=#666666;",
    "ext": BASE + "fillColor=#ffffff;strokeColor=#333333;",
    "group": "whiteSpace=wrap;html=1;rounded=1;arcSize=4;verticalAlign=top;align=left;"
    "spacingLeft=8;" + FONT + "fontStyle=1;fillColor=#eef4fc;strokeColor=#6c8ebf;",
    "topic": "whiteSpace=wrap;html=1;rounded=0;" + FONT + "fillColor=#ffffff;strokeColor=#6c8ebf;",
    "label": "text;html=1;align=left;verticalAlign=middle;" + FONT + "fontStyle=1;",
    "note": "text;html=1;align=center;verticalAlign=middle;whiteSpace=wrap;" + FONT,
    "decision": "rhombus;whiteSpace=wrap;html=1;" + FONT + "fillColor=#fff2cc;strokeColor=#d6b656;",
}
EDGE = (
    "edgeStyle=orthogonalEdgeStyle;rounded=1;html=1;endArrow=block;endFill=1;"
    + FONT
    + "labelBackgroundColor=#ffffff;strokeColor=#333333;jumpStyle=arc;jumpSize=8;"
)


@dataclass
class Diagram:
    name: str
    cells: list[str] = field(default_factory=list)
    _n: int = 0

    def _id(self) -> str:
        self._n += 1
        return f"c{self._n}"

    def box(
        self,
        label: str,
        x: float,
        y: float,
        w: float,
        h: float,
        kind: str = "ext",
        extra: str = "",
    ) -> str:
        cid = self._id()
        self.cells.append(
            f'<mxCell id="{cid}" value="{escape(label, {chr(34): "&quot;"})}" '
            f'style="{STYLE[kind]}{extra}" vertex="1" parent="1">'
            f'<mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/></mxCell>'
        )
        return cid

    def edge(
        self,
        src: str,
        dst: str,
        label: str = "",
        extra: str = "",
        exit: tuple[float, float] | None = None,
        entry: tuple[float, float] | None = None,
    ) -> str:
        cid = self._id()
        anchors = ""
        if exit:
            anchors += f"exitX={exit[0]};exitY={exit[1]};exitDx=0;exitDy=0;"
        if entry:
            anchors += f"entryX={entry[0]};entryY={entry[1]};entryDx=0;entryDy=0;"
        self.cells.append(
            f'<mxCell id="{cid}" value="{escape(label, {chr(34): "&quot;"})}" '
            f'style="{EDGE}{anchors}{extra}" edge="1" parent="1" source="{src}" '
            f'target="{dst}"><mxGeometry relative="1" as="geometry"/></mxCell>'
        )
        return cid

    def down(self, src: str, dst: str, label: str = "", extra: str = "") -> str:
        """An arrow from the bottom of src to the top of dst."""
        return self.edge(src, dst, label, extra, exit=(0.5, 1), entry=(0.5, 0))

    def write(self) -> Path:
        xml = (
            f'<mxfile host="build_drawio.py"><diagram id="{self.name}" name="{self.name}">'
            '<mxGraphModel grid="0" page="0" math="0" shadow="0"><root>'
            '<mxCell id="0"/><mxCell id="1" parent="0"/>'
            f"{''.join(self.cells)}</root></mxGraphModel></diagram></mxfile>\n"
        )
        path = HERE / f"{self.name}.drawio"
        path.write_text(xml)
        return path


def d1_context() -> Diagram:
    d = Diagram("D1-context")
    core = d.box(
        "<b>Hospital vitals platform</b><br>one stream, one scoring rule",
        170,
        150,
        480,
        70,
        "stream",
    )
    sources = [
        ("<b>Bedside monitors</b><br>40 beds", "vital signs"),
        ("<b>Pathology lab</b><br>one file per day", "lab results"),
        ("<b>Admissions</b><br>bed, COPD flag", "admissions"),
    ]
    for i, (text, label) in enumerate(sources):
        b = d.box(text, i * 290, 0, 240, 50)
        d.edge(b, core, label, exit=(0.5, 1), entry=(0.15 + i * 0.35, 0))
    users = [
        ("<b>Ward nurses</b>", "ward list,<br>alerts"),
        ("<b>Doctors</b>", "history,<br>daily report"),
        ("<b>Clinical governance</b>", "approve a<br>rule change"),
        ("<b>Operations</b>", "pipeline<br>alerts"),
    ]
    for i, (text, label) in enumerate(users):
        b = d.box(text, i * 210, 330, 190, 45)
        d.edge(core, b, label, exit=(0.1 + i * 0.267, 1), entry=(0.5, 0))
    return d


def d2_architecture() -> Diagram:
    d = Diagram("D2-layered-architecture")
    for y, text in [
        (15, "Sources"),
        (215, "Log"),
        (385, "Processing"),
        (515, "Storage"),
        (635, "Serving"),
    ]:
        d.box(text, 0, y, 110, 40, "label")

    adm = d.box("<b>Admissions</b><br>bed, COPD flag", 130, 10, 220, 50)
    mon = d.box("<b>Bedside monitors</b><br>40 beds", 380, 10, 220, 50)
    lab = d.box("<b>Lab uploader</b><br>one file per sim day", 630, 10, 220, 50)
    ingest = d.box("<b>Airflow</b><br>ward_lab_ingest", 630, 95, 220, 45, "orch")

    d.box("Apache Kafka :9192", 120, 170, 740, 120, "group")
    t_adm = d.box("<b>ward.admissions.v1</b><br>compacted", 130, 215, 220, 55, "topic")
    t_vit = d.box("<b>vitals.readings.v1</b><br>kept 30 sim days", 380, 215, 220, 55, "topic")
    t_lab = d.box("<b>labs.results.v1</b><br>compacted", 630, 215, 220, 55, "topic")

    v2 = d.box(
        "<b>ward-stream-v2</b> (NEWS2 v2)<br>same code, replays<br>from offset 0",
        130,
        365,
        340,
        75,
        "stream",
        "dashed=1;",
    )
    v1 = d.box(
        "<b>ward-stream</b> (NEWS2 v1, live)<br>Spark, every 5 s: check,<br>NEWS2, labs, alerts",
        510,
        365,
        340,
        75,
        "stream",
    )
    cass = d.box(
        "<b>Apache Cassandra</b> :9142<br>"
        "vitals, scores, ward snapshot, alerts, labs, daily summary",
        130,
        505,
        720,
        60,
        "store",
    )
    api = d.box("<b>FastAPI</b> :8100<br>ward list, patient, cutover", 130, 625, 220, 60, "store")
    pdf = d.box("<b>Daily PDF report</b><br>made by Airflow", 380, 625, 220, 60, "store")
    graf = d.box("<b>Grafana</b> :3100<br>three dashboards", 630, 625, 220, 60, "obs")

    reg = d.box("<b>Schema Registry</b><br>:8181, Avro", 890, 215, 200, 55, "stream")
    out = d.box(
        "<b>Kafka outputs</b><br>vitals.alerts.v1<br>vitals.readings.dlq<br>vitals.late",
        890,
        360,
        200,
        85,
        "stream",
    )
    d.box("<b>Airflow</b> :8182<br>5 workflows,<br>no scoring", 890, 500, 200, 70, "orch")
    prom = d.box("<b>Prometheus</b> :9190<br>Alertmanager :9193", 890, 625, 200, 60, "obs")

    d.down(adm, t_adm)
    d.down(mon, t_vit)
    d.down(lab, ingest)
    d.down(ingest, t_lab)
    d.edge(t_vit, v2, "replay", "dashed=1;", exit=(0.25, 1), entry=(0.9, 0))
    d.edge(t_vit, v1, "live", exit=(0.75, 1), entry=(0.1, 0))
    d.edge(v1, out, exit=(1, 0.5), entry=(0, 0.5))
    d.edge(reg, t_lab, "", "dashed=1;", exit=(0, 0.5), entry=(1, 0.5))
    d.edge(v2, cass, "v2 rows", "dashed=1;", exit=(0.5, 1), entry=(0.25, 0))
    d.edge(v1, cass, "v1 rows", exit=(0.5, 1), entry=(0.75, 0))
    d.edge(cass, api, exit=(110 / 720, 1), entry=(0.5, 0))
    d.edge(cass, pdf, exit=(0.5, 1), entry=(0.5, 0))
    d.edge(prom, graf, exit=(0, 0.5), entry=(1, 0.5))

    for i, (kind, text) in enumerate(
        [
            ("stream", "Streaming"),
            ("store", "Storage, serving"),
            ("orch", "Orchestration"),
            ("obs", "Observability"),
            ("ext", "Outside"),
        ]
    ):
        d.box(text, 130 + i * 190, 720, 170, 30, kind)
    return d


def d3_sequence() -> Diagram:
    d = Diagram("D3-event-sequence")
    lab = d.box("<b>Day 2, 06:00</b><br>lab file: P014 lactate 3.8", 0, 0, 300, 50)
    pub = d.box("<b>Airflow</b> publishes<br>to labs.results.v1", 0, 100, 300, 50, "orch")
    mon = d.box("<b>Monitor</b> sends a P014<br>reading every 15 sim min", 400, 0, 300, 50)
    kaf = d.box("<b>Kafka</b><br>vitals.readings.v1", 400, 100, 300, 50, "stream")
    spark = d.box(
        "<b>Spark stream</b>, every 5 s<br>check, window, NEWS2,<br>add labs, alert episode",
        175,
        210,
        350,
        75,
        "stream",
    )
    cass = d.box("<b>Cassandra</b><br>score, snapshot, summary, alert", 175, 345, 350, 50, "store")
    ward = d.box(
        "<b>Ward list</b><br>P014 first, one alert per episode", 175, 455, 350, 50, "store"
    )
    d.down(lab, pub)
    d.down(mon, kaf)
    d.edge(pub, spark, "labs", exit=(0.5, 1), entry=(0.2, 0))
    d.edge(kaf, spark, "readings", exit=(0.5, 1), entry=(0.8, 0))
    d.down(spark, cass)
    d.down(cass, ward)
    return d


def d4_replay() -> Diagram:
    d = Diagram("D4-replay-branching")
    log = d.box("<b>vitals.readings.v1</b><br>all readings, 30 sim days", 150, 0, 400, 50, "stream")
    v2 = d.box(
        "<b>ward-stream-v2</b> (v2)<br>starts at offset 0", 0, 110, 300, 50, "stream", "dashed=1;"
    )
    v1 = d.box("<b>ward-stream</b> (v1)<br>live", 400, 110, 300, 50, "stream")
    d.edge(log, v2, "replay", "dashed=1;", exit=(0.2, 1), entry=(0.5, 0))
    d.edge(log, v1, "live", exit=(0.8, 1), entry=(0.5, 0))
    cass = d.box("<b>Cassandra</b><br>v1 and v2 rows side by side", 150, 220, 400, 50, "store")
    d.edge(v2, cass, "", "dashed=1;", exit=(0.5, 1), entry=(0.2, 0))
    d.edge(v1, cass, exit=(0.5, 1), entry=(0.8, 0))
    diff = d.box("<b>Compare</b><br>NEWS2 v1 vs v2, per reading", 150, 330, 400, 50, "orch")
    d.down(cass, diff)
    check = d.box("Only COPD patients<br>changed, as expected?", 225, 440, 250, 110, "decision")
    d.down(diff, check)
    stop = d.box("Stop and<br>investigate", 540, 470, 160, 50)
    d.edge(check, stop, "no", exit=(1, 0.5), entry=(0, 0.5))
    approve = d.box("<b>Clinician approves</b>", 150, 610, 400, 40, "orch")
    d.edge(check, approve, "yes", exit=(0.5, 1), entry=(0.5, 0))
    cut = d.box(
        "<b>Cutover</b>: API serves v2<br>rollback: the same call with v1",
        150,
        710,
        400,
        50,
        "store",
    )
    d.down(approve, cut)
    return d


def d5_tables() -> Diagram:
    d = Diagram("D5-cassandra-tables")
    tables = [
        ("vitals_by_patient", "patient_id", "measured_at, newest first", "", "recent readings"),
        (
            "ward_risk_snapshot",
            "ward_id, scorer_version",
            "risk_score, highest first",
            "TTL 120 s",
            "ward list, sickest first",
        ),
        (
            "risk_scores_by_patient",
            "patient_id, scorer_version",
            "scored_at, newest first",
            "",
            "score history per version",
        ),
        (
            "alerts_by_ward",
            "ward_id, sim_date, scorer_version",
            "alert_time, newest first",
            "TTL 3 h",
            "today's alerts",
        ),
        ("labs_by_patient", "patient_id", "test_type, collected_at", "", "a patient's labs"),
        (
            "daily_patient_summary",
            "ward_id, sim_date",
            "patient_id, scorer_version",
            "",
            "the daily report",
        ),
    ]
    for i, (name, pk, ck, ttl, serves) in enumerate(tables):
        ttl_line = f"<br>{ttl}" if ttl else ""
        d.box(
            f"<b>{name}</b><br>partition: {pk}<br>order: {ck}{ttl_line}<br>answers: {serves}",
            (i % 2) * 380,
            (i // 2) * 120,
            350,
            100,
            "store",
            "align=left;spacingLeft=10;",
        )
    d.box("One table per question: every read touches one partition.", 0, 360, 730, 30, "note")
    return d


def d6_one_reading() -> Diagram:
    d = Diagram("D6-one-reading")
    start = d.box("<b>Reading arrives</b>", 30, 0, 180, 45, "stream")
    steps = [
        ("Can it be<br>decoded?", "no", "Dead-letter topic"),
        ("Seen this<br>id before?", "yes", "Skip (duplicate)"),
        ("Physically<br>impossible?", "yes", "Dead-letter topic,<br>with original bytes"),
        ("Over 60 sim min<br>behind?", "yes", "Late topic,<br>stored, not scored"),
    ]
    flip = {"": "", "yes": "no", "no": "yes"}
    prev, prev_answer = start, ""
    y = 90
    for question, answer, outcome in steps:
        q = d.box(question, 20, y, 200, 100, "decision")
        d.edge(prev, q, flip[prev_answer], exit=(0.5, 1), entry=(0.5, 0))
        out = d.box(outcome, 300, y + 25, 200, 50)
        d.edge(q, out, answer, exit=(1, 0.5), entry=(0, 0.5))
        prev, prev_answer = q, answer
        y += 140
    score = d.box("<b>Score it</b><br>window, NEWS2, labs, alerts", 0, y, 240, 60, "stream")
    d.edge(prev, score, flip[prev_answer], exit=(0.5, 1), entry=(0.5, 0))
    store = d.box("<b>Cassandra</b>", 0, y + 110, 240, 45, "store")
    d.down(score, store)
    return d


if __name__ == "__main__":
    for build in (d1_context, d2_architecture, d3_sequence, d4_replay, d5_tables, d6_one_reading):
        print("wrote", build().write().name)

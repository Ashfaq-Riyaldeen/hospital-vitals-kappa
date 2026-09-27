"""Write the report's diagrams as draw.io files, then export them to PDF.

    .venv/bin/python docs/diagrams/build_drawio.py      # writes *.drawio
    make -C docs/diagrams                               # exports *.pdf with the drawio CLI

The .drawio files are ordinary draw.io documents and can be opened and edited in
draw.io directly; this script only keeps the six diagrams in one style and one colour
key: blue = streaming, orange = storage and serving, green = orchestration,
grey = observability, white = outside the platform.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from xml.sax.saxutils import escape

HERE = Path(__file__).resolve().parent

FONT = "fontFamily=Helvetica;fontSize=12;"
BASE = "whiteSpace=wrap;html=1;rounded=1;arcSize=8;" + FONT
STYLE = {
    "stream": BASE + "fillColor=#dae8fc;strokeColor=#6c8ebf;",
    "store": BASE + "fillColor=#ffe6cc;strokeColor=#d79b00;",
    "orch": BASE + "fillColor=#d5e8d4;strokeColor=#82b366;",
    "obs": BASE + "fillColor=#f5f5f5;strokeColor=#666666;",
    "ext": BASE + "fillColor=#ffffff;strokeColor=#333333;",
    "person": "shape=umlActor;verticalLabelPosition=bottom;verticalAlign=top;html=1;"
    + FONT
    + "fillColor=#ffffff;strokeColor=#333333;",
    "topic": "whiteSpace=wrap;html=1;rounded=0;align=left;spacingLeft=6;"
    + FONT
    + "fontSize=11;fillColor=#ffffff;strokeColor=#6c8ebf;",
    "group_stream": "whiteSpace=wrap;html=1;rounded=1;arcSize=4;verticalAlign=top;"
    + FONT
    + "fontStyle=1;fillColor=#eef4fc;strokeColor=#6c8ebf;",
    "title": "text;html=1;align=left;verticalAlign=middle;" + FONT + "fontSize=13;fontStyle=1;",
    "note": "text;html=1;align=left;verticalAlign=top;whiteSpace=wrap;" + FONT + "fontSize=11;",
    "decision": "rhombus;whiteSpace=wrap;html=1;" + FONT + "fillColor=#fff2cc;strokeColor=#d6b656;",
    "lifeline": "endArrow=none;dashed=1;html=1;strokeColor=#999999;",
}
EDGE = "edgeStyle=orthogonalEdgeStyle;rounded=1;html=1;endArrow=block;endFill=1;" + FONT
EDGE += "fontSize=11;labelBackgroundColor=#ffffff;strokeColor=#333333;jumpStyle=arc;jumpSize=8;"
STRAIGHT = "html=1;endArrow=block;endFill=1;" + FONT + "fontSize=11;labelBackgroundColor=#ffffff;"


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
        parent: str = "1",
    ) -> str:
        cid = self._id()
        self.cells.append(
            f'<mxCell id="{cid}" value="{escape(label, {chr(34): "&quot;"})}" '
            f'style="{STYLE[kind]}{extra}" vertex="1" parent="{parent}">'
            f'<mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/></mxCell>'
        )
        return cid

    def edge(
        self,
        src: str,
        dst: str,
        label: str = "",
        extra: str = "",
        style: str = EDGE,
        exit: tuple[float, float] | None = None,
        entry: tuple[float, float] | None = None,
        points: list[tuple[float, float]] | None = None,
    ) -> str:
        cid = self._id()
        anchors = ""
        if exit:
            anchors += f"exitX={exit[0]};exitY={exit[1]};exitDx=0;exitDy=0;"
        if entry:
            anchors += f"entryX={entry[0]};entryY={entry[1]};entryDx=0;entryDy=0;"
        pts = ""
        if points:
            pts = (
                '<Array as="points">'
                + "".join(f'<mxPoint x="{x}" y="{y}"/>' for x, y in points)
                + "</Array>"
            )
        self.cells.append(
            f'<mxCell id="{cid}" value="{escape(label, {chr(34): "&quot;"})}" '
            f'style="{style}{anchors}{extra}" edge="1" parent="1" source="{src}" target="{dst}">'
            f'<mxGeometry relative="1" as="geometry">{pts}</mxGeometry></mxCell>'
        )
        return cid

    def line(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
        label: str = "",
        style: str = STRAIGHT,
        extra: str = "",
    ) -> str:
        cid = self._id()
        self.cells.append(
            f'<mxCell id="{cid}" value="{escape(label, {chr(34): "&quot;"})}" '
            f'style="{style}{extra}" edge="1" parent="1"><mxGeometry relative="1" as="geometry">'
            f'<mxPoint x="{x1}" y="{y1}" as="sourcePoint"/>'
            f'<mxPoint x="{x2}" y="{y2}" as="targetPoint"/></mxGeometry></mxCell>'
        )
        return cid

    def legend(self, x: float, y: float) -> None:
        self.box("Colour key", x, y, 140, 20, "title")
        for i, (kind, text) in enumerate(
            [
                ("stream", "Streaming"),
                ("store", "Storage and serving"),
                ("orch", "Orchestration"),
                ("obs", "Observability"),
                ("ext", "Outside the platform"),
            ]
        ):
            self.box(text, x, y + 24 + i * 26, 150, 22, kind, "fontSize=11;")

    def write(self) -> Path:
        body = "".join(self.cells)
        xml = (
            f'<mxfile host="build_drawio.py"><diagram id="{self.name}" name="{self.name}">'
            '<mxGraphModel grid="0" page="0" math="0" shadow="0"><root>'
            '<mxCell id="0"/><mxCell id="1" parent="0"/>'
            f"{body}</root></mxGraphModel></diagram></mxfile>\n"
        )
        path = HERE / f"{self.name}.drawio"
        path.write_text(xml)
        return path


def d1_context() -> Diagram:
    d = Diagram("D1-context")
    core = d.box(
        "<b>Hospital vitals platform</b><br>(Kappa architecture)<br><br>"
        "one stream, one scoring rule,<br>Kafka keeps the history",
        430,
        120,
        300,
        200,
        "stream",
        "fontSize=13;",
    )
    left = [
        ("<b>Bedside monitors</b><br>40 beds on one ward", "a reading every<br>15 simulated min"),
        ("<b>Pathology lab</b><br>one results file per day", "daily file, 06:00"),
        ("<b>Admissions system</b><br>bed, condition, COPD flag", "admissions"),
    ]
    for i, (text, label) in enumerate(left):
        b = d.box(text, 20, 120 + i * 70, 220, 55)
        d.edge(b, core, label, exit=(1, 0.5), entry=(0, 0.2 + i * 0.3))
    right = [
        ("<b>Ward nurses</b><br>ward list, alerts", "worst patient first,<br>alerts"),
        ("<b>Doctors</b><br>patient detail", "history, daily<br>PDF report"),
        ("<b>Clinical governance</b><br>rule changes", "old vs new rule,<br>approve switch"),
    ]
    for i, (text, label) in enumerate(right):
        b = d.box(text, 920, 120 + i * 70, 220, 55)
        d.edge(core, b, label, exit=(1, 0.2 + i * 0.3), entry=(0, 0.5))
    ops = d.box("<b>Operations engineer</b><br>keeps it running", 470, 400, 220, 55)
    d.edge(core, ops, "pipeline alerts, dashboards", exit=(0.5, 1), entry=(0.5, 0))
    return d


def d2_architecture() -> Diagram:
    d = Diagram("D2-layered-architecture")
    for x, text in [
        (20, "1. Sources"),
        (260, "2. Log (the system of record)"),
        (760, "3. Processing (one code path)"),
        (1090, "4. Storage"),
        (1390, "5. Serving"),
    ]:
        d.box(text, x, 15, 260, 24, "title")

    mon = d.box(
        "<b>Bedside monitors</b><br>40 beds, a reading per bed<br>every 15 simulated minutes<br>"
        "Python, metrics :8101",
        20,
        60,
        200,
        85,
    )
    adm = d.box("<b>Admissions producer</b><br>bed, age, condition,<br>COPD flag", 20, 170, 200, 70)
    lab = d.box(
        "<b>Lab uploader</b><br>one JSON file per simulated<br>day at 06:00 (metrics :8102)",
        20,
        265,
        200,
        70,
    )
    ingest = d.box(
        "<b>Airflow: ward_lab_ingest</b><br>checksum, validate,<br>quarantine or publish",
        20,
        370,
        200,
        75,
        "orch",
    )

    grp = d.box("Apache Kafka (KRaft)  :9192", 260, 55, 290, 345, "group_stream")
    topics = {}
    for i, (name, info) in enumerate(
        [
            ("vitals.readings.v1", "6 partitions, delete after 30 sim days"),
            ("ward.admissions.v1", "3 partitions, compact"),
            ("labs.results.v1", "3 partitions, compact"),
            ("vitals.alerts.v1", "3 partitions, delete"),
            ("vitals.readings.dlq", "1 partition, delete"),
            ("vitals.late", "1 partition, delete"),
        ]
    ):
        topics[name] = d.box(
            f"<b>{name}</b><br>{info}", 12, 32 + i * 51, 266, 44, "topic", parent=grp
        )
    d.box("<b>Schema Registry</b> (Avro contracts)  :8181", 260, 415, 290, 32, "stream")

    v1 = d.box(
        "<b>ward-stream</b>  (NEWS2 v1, live)<br>Spark Structured Streaming<br>"
        "micro-batch every 5 s<br>check → 4 h window → NEWS2 → labs → alerts<br>"
        "keeps admissions + labs in a cache<br>Spark UI :4140, metrics :8104",
        760,
        60,
        270,
        125,
        "stream",
    )
    v2 = d.box(
        "<b>ward-stream-v2</b>  (NEWS2 v2, replay)<br>the SAME code, started by<br>"
        "<i>make replay</i> from offset 0<br>Spark UI :4141, metrics :8105",
        760,
        215,
        270,
        95,
        "stream",
        "dashed=1;",
    )
    cass = d.box(
        "<b>Apache Cassandra 4.1</b>  :9142<br><br>vitals_by_patient<br>risk_scores_by_patient<br>"
        "ward_risk_snapshot (TTL 120 s)<br>alerts_by_ward (TTL 3 h)<br>labs_by_patient<br>"
        "daily_patient_summary<br>sim_state<br><br>"
        "<i>every score row carries its scorer_version</i>",
        1090,
        60,
        260,
        250,
        "store",
        "align=left;spacingLeft=12;verticalAlign=top;spacingTop=8;",
    )
    api = d.box(
        "<b>FastAPI</b>  :8100<br>ward list (worst first),<br>patient, alerts, labs,<br>"
        "replay diff, cutover",
        1390,
        60,
        220,
        95,
        "store",
    )
    pdf = d.box(
        "<b>Daily PDF report</b><br>Airflow ward_daily_report<br>reads daily_patient_summary",
        1390,
        185,
        220,
        75,
        "store",
    )
    graf = d.box(
        "<b>Grafana</b>  :3100<br>ward monitor, pipeline health,<br>replay v1 vs v2",
        1390,
        290,
        220,
        70,
        "obs",
    )
    d.box(
        "<b>Apache Airflow</b>  :8182  (orchestration only, no scoring)<br>"
        "ward_lab_ingest · ward_daily_report · ward_replay · ward_healthcheck · ward_retention",
        760,
        345,
        580,
        55,
        "orch",
    )
    obs = d.box(
        "<b>Observability</b>:  Prometheus :9190 scrapes every service's /metrics  ·  "
        "Alertmanager :9193 routes safety alerts to the ward  ·  Pushgateway :9191  ·  "
        "kafka-exporter :9309",
        760,
        420,
        860,
        40,
        "obs",
    )

    # Kafka topic boxes are children of the group; absolute y of topic k is
    # 55 + 32 + 51k, centre +22. Waypoints below are in absolute coordinates.
    d.edge(mon, topics["vitals.readings.v1"], "", exit=(1, 0.5), entry=(0, 0.5))
    d.edge(
        adm,
        topics["ward.admissions.v1"],
        "",
        exit=(1, 0.5),
        entry=(0, 0.5),
        points=[(236, 205), (236, 160)],
    )
    d.edge(lab, ingest, "file", exit=(0.5, 1), entry=(0.5, 0))
    d.edge(
        ingest,
        topics["labs.results.v1"],
        "publish",
        exit=(1, 0.3),
        entry=(0, 0.5),
        points=[(248, 392), (248, 211)],
    )
    d.edge(topics["vitals.readings.v1"], v1, "live", exit=(1, 0.3), entry=(0, 0.3))
    d.edge(
        topics["vitals.readings.v1"],
        v2,
        "",
        "dashed=1;",
        exit=(1, 0.75),
        entry=(0, 0.5),
        points=[(600, 120), (600, 277)],
    )
    d.edge(
        v1,
        topics["vitals.alerts.v1"],
        "",
        exit=(0, 0.92),
        entry=(1, 0.5),
        points=[(645, 175), (645, 257)],
    )
    d.box("replay from offset 0", 606, 280, 150, 18, "note", "fontSize=11;")
    d.box("alerts, dead letters,<br>late readings", 650, 140, 110, 32, "note", "fontSize=11;")
    d.edge(v1, cass, "5 tables", exit=(1, 0.5), entry=(0, 0.25))
    d.edge(v2, cass, "v2 rows", "dashed=1;", exit=(1, 0.5), entry=(0, 0.8))
    d.edge(cass, api, "", exit=(1, 0.2), entry=(0, 0.5))
    d.edge(cass, pdf, "", exit=(1, 0.55), entry=(0, 0.5))
    d.edge(obs, graf, "", "dashed=1;", exit=(0.9, 0), entry=(0.5, 1))
    d.legend(20, 480)
    return d


def d3_sequence() -> Diagram:
    d = Diagram("D3-event-sequence")
    names = [
        "Bedside monitor",
        "Pathology lab<br>+ Airflow",
        "Kafka",
        "ward-stream<br>(Spark)",
        "Cassandra",
        "FastAPI /<br>Grafana",
        "Ward nurse",
    ]
    kinds = ["ext", "orch", "stream", "stream", "store", "store", "ext"]
    xs = [80 + i * 175 for i in range(len(names))]
    top, bottom = 20, 640
    for x, name, kind in zip(xs, names, kinds, strict=True):
        d.box(f"<b>{name}</b>", x - 70, top, 140, 48, kind)
        d.line(x, top + 48, x, bottom, style=STYLE["lifeline"])

    def msg(a: int, b: int, y: float, text: str, dashed: bool = False) -> None:
        d.line(xs[a], y, xs[b], y, text, extra="dashed=1;" if dashed else "")

    msg(1, 2, 105, "day 2, 06:00: P014's labs<br>lactate 3.8, WBC 18.4")
    msg(2, 3, 150, "labs kept in the join cache")
    msg(0, 2, 205, "reading for P014 (Avro, key P014)<br>every 15 simulated minutes")
    msg(2, 3, 250, "micro-batch every 5 real seconds")
    d.box(
        "1. reject impossible values (to DLQ)<br>2. update the 4-hour window<br>"
        "3. NEWS2 (ward/clinical/news2.py)<br>4. add labs reported before now<br>"
        "5. open or escalate an alert episode",
        xs[3] - 10,
        270,
        235,
        95,
        "stream",
        "align=left;spacingLeft=6;fontSize=11;",
    )
    msg(3, 4, 390, "vitals, score, ward snapshot,<br>daily summary")
    msg(3, 2, 435, "new episode only:<br>vitals.alerts.v1")
    msg(3, 4, 470, "alerts_by_ward")
    msg(6, 5, 520, "open the ward list")
    msg(5, 4, 555, "one partition read (Q2)")
    msg(4, 5, 590, "rows, newest per patient", dashed=True)
    msg(5, 6, 625, "P014 at the top, NEWS2 7+", dashed=True)
    d.box(
        "<b>What the nurse would otherwise miss</b><br>P014's sepsis starts on simulated day 2 at "
        "08:00. NEWS2 climbs through the urgent (5) and emergency (7) thresholds within a few "
        "simulated hours; each threshold raises ONE alert, not one per reading.",
        xs[5] + 60,
        150,
        230,
        130,
        "note",
    )
    return d


def d4_replay() -> Diagram:
    d = Diagram("D4-replay-branching")
    log = d.box(
        "<b>vitals.readings.v1</b>: every reading, kept for 30 simulated days   "
        "(offset 0  ───────────────────────────────►  newest)",
        40,
        40,
        900,
        45,
        "stream",
    )
    v1 = d.box(
        "<b>ward-stream</b> (NEWS2 v1)<br>reads the newest readings, live",
        640,
        130,
        300,
        60,
        "stream",
    )
    v2 = d.box(
        "<b>ward-stream-v2</b> (NEWS2 v2: Scale 2 for COPD)<br>same code, starts at offset 0, "
        "catches up,<br>then runs live beside v1",
        40,
        130,
        360,
        70,
        "stream",
        "dashed=1;",
    )
    d.edge(log, v1, "live", "exitX=0.85;exitY=1;entryX=0.5;entryY=0;")
    d.edge(log, v2, "replay", "exitX=0.1;exitY=1;entryX=0.3;entryY=0;dashed=1;")
    cass = d.box(
        "<b>Cassandra</b>: v1 rows and v2 rows side by side<br>"
        "(scorer_version is part of every score key, so nothing is overwritten)",
        240,
        240,
        500,
        55,
        "store",
    )
    d.edge(v1, cass)
    d.edge(v2, cass, "", "dashed=1;")
    diff = d.box(
        "<b>compare_versions</b><br>every reading scored by both,<br>NEWS2 v1 vs v2",
        40,
        340,
        240,
        70,
        "orch",
    )
    d.edge(cass, diff)
    check = d.box(
        "only COPD patients<br>changed, in the<br>expected direction?", 340, 325, 170, 100, "decision"
    )
    d.edge(diff, check)
    stop = d.box("stop: the new rule<br>does something<br>unexpected", 360, 470, 130, 70, "ext")
    d.edge(check, stop, "no")
    approve = d.box(
        "<b>clinician approves</b><br>(the Airflow run stops<br>here and waits)",
        570,
        340,
        170,
        70,
        "orch",
    )
    d.edge(check, approve, "yes")
    cut = d.box(
        "<b>cutover</b>: the API serves v2<br>(make cutover VERSION=v2)", 800, 340, 200, 70, "store"
    )
    d.edge(approve, cut)
    d.box(
        "<b>Rollback</b> is the same switch back to v1: the v1 rows were never touched.",
        800,
        430,
        200,
        60,
        "note",
    )
    d.legend(1060, 40)
    return d


def d5_tables() -> Diagram:
    d = Diagram("D5-cassandra-tables")
    tables = [
        (
            "vitals_by_patient",
            "patient_id",
            "measured_at DESC",
            "",
            "Q1: a patient's recent readings",
        ),
        (
            "ward_risk_snapshot",
            "ward_id, scorer_version",
            "risk_score DESC, patient_id",
            "TTL 120 s",
            "Q2: the ward list, worst first",
        ),
        (
            "risk_scores_by_patient",
            "patient_id, scorer_version",
            "scored_at DESC",
            "",
            "Q3: a patient's score history,<br>per rule version",
        ),
        (
            "alerts_by_ward",
            "ward_id, sim_date, scorer_version",
            "alert_time DESC, alert_id",
            "TTL 3 h",
            "Q4: today's alerts on the ward",
        ),
        (
            "labs_by_patient",
            "patient_id",
            "test_type, collected_at DESC",
            "",
            "Q5: a patient's lab results",
        ),
        (
            "daily_patient_summary",
            "ward_id, sim_date",
            "patient_id, scorer_version",
            "",
            "Q6: the daily PDF report",
        ),
    ]
    for i, (name, pk, ck, ttl, serves) in enumerate(tables):
        x = 30 + (i % 3) * 330
        y = 30 + (i // 3) * 145
        extra = f"<br>{ttl}" if ttl else ""
        d.box(
            f"<b>{name}</b><hr>partition key: <i>{pk}</i><br>clustering: <i>{ck}</i>{extra}"
            f"<hr>{serves}",
            x,
            y,
            300,
            125,
            "store",
            "align=left;spacingLeft=8;verticalAlign=top;",
        )
    d.box(
        "Each table is shaped for exactly one question, so every read touches one partition "
        "and none needs ALLOW FILTERING. Rows that go stale expire by TTL instead of DELETE, "
        "which would leave tombstones behind.",
        30,
        330,
        960,
        45,
        "note",
    )
    return d


def d6_one_reading() -> Diagram:
    d = Diagram("D6-one-reading")
    start = d.box("<b>a reading arrives</b><br>(Kafka record)", 40, 40, 160, 55, "stream")
    steps = [
        ("can it be decoded?", "no", "vitals.readings.dlq<br>(UNDECODABLE)"),
        ("seen this reading id<br>before?", "yes", "counted and<br>skipped (duplicate)"),
        (
            "physically impossible?<br>(e.g. SpO2 0, HR 300)",
            "yes",
            "vitals.readings.dlq<br>with raw bytes + offset",
        ),
        (
            "more than 60 sim min<br>behind this patient?",
            "yes",
            "vitals.late<br>+ stored, not scored",
        ),
    ]
    prev, prev_answer = start, ""
    y = 130
    for question, answer, outcome in steps:
        q = d.box(question, 30, y, 180, 90, "decision", "fontSize=11;")
        # The way DOWN from the previous question is the opposite of its side exit.
        d.edge(prev, q, {"": "", "yes": "no", "no": "yes"}[prev_answer])
        out = d.box(outcome, 290, y + 18, 190, 55, "ext", "fontSize=11;")
        d.edge(q, out, answer)
        prev, prev_answer = q, answer
        y += 125
    score = d.box(
        "<b>score it</b><br>4-hour window and trend (Theil-Sen)<br>NEWS2 v1 or v2<br>"
        "+ labs reported before this reading<br>alert only if a new episode",
        20,
        y,
        200,
        100,
        "stream",
    )
    d.edge(prev, score, {"yes": "no", "no": "yes"}[prev_answer])
    store = d.box("<b>Cassandra</b><br>5 tables", 290, y + 20, 190, 60, "store")
    d.edge(score, store)
    d.box(
        "Impossible is not the same as abnormal. SpO2 88 % is a sick patient and must be "
        "scored; SpO2 0 % is a probe that fell off. The checks use physical limits, never "
        "statistical outliers, because the readings that matter most are the ones furthest "
        "from normal.",
        520,
        150,
        260,
        150,
        "note",
    )
    return d


if __name__ == "__main__":
    for build in (d1_context, d2_architecture, d3_sequence, d4_replay, d5_tables, d6_one_reading):
        print("wrote", build().write().name)

"""Alert rules and dashboards may only name metrics that really exist.

The first version of this suite asserted the broken configuration: it required a
`statsd-exporter` scrape job for a service that was never defined, and it checked that
WardMonitoringSilent mentioned `risk_scores_written_total` without checking that
anything emitted it. Nothing did, so the platform's most important alert could never
fire, and every test passed.

These tests read each PromQL expression, pull out the metric names, and check them
against the metrics the code registers.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import ward.obs.metrics as metrics
import yaml
from prometheus_client import Counter, Gauge, Histogram

ROOT = Path(__file__).resolve().parents[2]
OBS = ROOT / "observability"

# Metrics from outside our code: Prometheus itself, kafka-exporter, the FastAPI
# instrumentator, and the replay gauges pushed through the Pushgateway.
EXTERNAL = {
    "up",
    "ALERTS",
    "kafka_topic_partition_current_offset",
    "kafka_consumergroup_lag",
    "http_requests_total",
    "http_request_duration_seconds",
    "replay_events_remaining",
}

PROMQL_WORDS = {
    "sum", "rate", "increase", "count", "max", "min", "avg", "by", "without", "on",
    "ignoring", "or", "and", "unless", "vector", "histogram_quantile", "time", "absent",
    "sort_desc", "sort", "topk", "bottomk", "offset", "bool", "le", "group_left",
    "group_right", "label_replace", "irate", "delta", "deriv", "clamp_min", "clamp_max",
}  # fmt: skip


def _ours() -> set[str]:
    names = set()
    for value in vars(metrics).values():
        if isinstance(value, Counter | Gauge | Histogram):
            name = value._name  # type: ignore[attr-defined]
            names.add(name + "_total" if isinstance(value, Counter) else name)
    return names


def metric_names(expr: str) -> set[str]:
    stripped = re.sub(r'"[^"]*"', " ", expr)  # string arguments, e.g. label_replace
    stripped = re.sub(r"\{[^}]*\}", " ", stripped)  # label matchers
    stripped = re.sub(r"\b(by|without|on|ignoring)\s*\([^)]*\)", " ", stripped)
    stripped = re.sub(r"\[[^\]]*\]", " ", stripped)  # range selectors
    tokens = set(re.findall(r"[A-Za-z_:][A-Za-z0-9_:]*", stripped))
    names = {t for t in tokens if t not in PROMQL_WORDS}
    return {re.sub(r"_(bucket|sum|count)$", "", n) for n in names}


def _alert_rules() -> list[dict]:
    content = yaml.safe_load((OBS / "prometheus/alerts.yml").read_text())
    return [r for g in content["groups"] for r in g["rules"] if "alert" in r]


def _dashboard_exprs() -> list[tuple[str, str]]:
    out = []
    for path in sorted((OBS / "grafana/dashboards").glob("*.json")):
        board = json.loads(path.read_text())
        for panel in board["panels"]:
            for t in panel.get("targets", []):
                out.append((f"{path.name}: {panel['title']}", t["expr"]))
    return out


def test_the_extractor_finds_what_it_should() -> None:
    expr = 'histogram_quantile(0.95, sum by (le) (rate(x_seconds_bucket{a="b"}[2m])))'
    assert metric_names(expr) == {"x_seconds"}
    assert metric_names("sum(rate(a_total[1m])) / sum(rate(b_total[1m]))") == {
        "a_total",
        "b_total",
    }


@pytest.mark.parametrize("rule", _alert_rules(), ids=lambda r: r["alert"])
def test_every_alert_rule_names_real_metrics(rule: dict) -> None:
    unknown = metric_names(rule["expr"]) - _ours() - EXTERNAL
    assert not unknown, f"{rule['alert']} uses metrics nothing emits: {unknown}"


@pytest.mark.parametrize("where,expr", _dashboard_exprs(), ids=lambda x: str(x)[:60])
def test_every_dashboard_query_names_real_metrics(where: str, expr: str) -> None:
    unknown = metric_names(expr) - _ours() - EXTERNAL
    assert not unknown, f"{where} queries metrics nothing emits: {unknown}"


@pytest.mark.parametrize("where,expr", _dashboard_exprs(), ids=lambda x: str(x)[:60])
def test_no_dashboard_shows_a_made_up_number(where: str, expr: str) -> None:
    """`or vector(0)` is honest for a count that can be zero. `vector(40)` is not: it
    shows forty beds whether or not a single one is reporting."""
    for value in re.findall(r"vector\(\s*([0-9.]+)\s*\)", expr):
        assert float(value) == 0, f"{where} falls back to a constant {value}"


def test_the_safety_alert_fires_when_the_stream_is_gone() -> None:
    """When the stream container dies its series disappear. The rule must still
    evaluate to something, or it can never fire in exactly the case it exists for."""
    rule = next(r for r in _alert_rules() if r["alert"] == "WardMonitoringSilent")
    assert "or vector(0)" in rule["expr"]
    assert rule["labels"]["category"] == "clinical_safety"


def test_scrape_targets_use_ports_inside_the_network() -> None:
    """Host-side ports (91xx, 9309) do not exist inside the compose network."""
    content = yaml.safe_load((OBS / "prometheus/prometheus.yml").read_text())
    services = yaml.safe_load((ROOT / "compose.yaml").read_text())["services"]
    targets = [
        t
        for group in content["scrape_configs"] + content["alerting"]["alertmanagers"]
        for sc in group["static_configs"]
        for t in sc["targets"]
    ]
    for target in targets:
        host, port = target.rsplit(":", 1)
        if host == "localhost":
            continue
        assert host in services, f"{target}: no compose service called {host}"
        host_ports = {str(p).split(":")[0] for p in services[host].get("ports", [])}
        inside = {str(p).split(":")[1] for p in services[host].get("ports", [])}
        assert port not in host_ports - inside, f"{target}: {port} is the HOST-side port"


def test_alertmanager_routes_clinical_safety_to_the_ward() -> None:
    content = yaml.safe_load((OBS / "alertmanager/alertmanager.yml").read_text())
    receivers = {r["name"] for r in content["receivers"]}
    assert {"ward-safety", "pipeline-oncall"} <= receivers
    route = content["route"]["routes"][0]
    assert route["receiver"] == "ward-safety"
    assert 'category="clinical_safety"' in route["matchers"]

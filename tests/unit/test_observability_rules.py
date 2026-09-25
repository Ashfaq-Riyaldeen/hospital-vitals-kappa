"""Unit tests for Observability configurations and Prometheus alert rules."""

from __future__ import annotations

from pathlib import Path

import yaml


def test_prometheus_configuration_syntax_and_jobs() -> None:
    prom_path = Path("observability/prometheus/prometheus.yml")
    assert prom_path.exists(), "prometheus.yml must exist"

    content = yaml.safe_load(prom_path.read_text())
    assert "scrape_configs" in content

    job_names = {job["job_name"] for job in content["scrape_configs"]}
    expected_jobs = {
        "ward-api",
        "spark-pushgateway",
        "kafka-exporter",
        "statsd-exporter",
        "bedside-monitor",
        "lab-uploader",
    }
    assert expected_jobs.issubset(job_names), f"Missing scrape jobs: {expected_jobs - job_names}"


def test_prometheus_alerts_syntax_and_clinical_rules() -> None:
    alerts_path = Path("observability/prometheus/alerts.yml")
    assert alerts_path.exists(), "alerts.yml must exist"

    content = yaml.safe_load(alerts_path.read_text())
    assert "groups" in content

    rules_by_name = {}
    for group in content["groups"]:
        for rule in group.get("rules", []):
            if "alert" in rule:
                rules_by_name[rule["alert"]] = rule

    # 1. WardMonitoringSilent is CRITICAL
    assert "WardMonitoringSilent" in rules_by_name
    silent_alert = rules_by_name["WardMonitoringSilent"]
    assert silent_alert["labels"]["severity"] == "critical"
    assert "risk_scores_written_total" in silent_alert["expr"]

    # 2. Ingestion & Quality rules
    assert "NoVitalsIngested" in rules_by_name
    assert "HighDLQRate" in rules_by_name
    assert "StaleLabDataElevated" in rules_by_name
    assert "ReplayLagHigh" in rules_by_name


def test_alertmanager_configuration_syntax_and_receivers() -> None:
    am_path = Path("observability/alertmanager/alertmanager.yml")
    assert am_path.exists(), "alertmanager.yml must exist"

    content = yaml.safe_load(am_path.read_text())
    assert "route" in content
    assert "receivers" in content

    receiver_names = {r["name"] for r in content["receivers"]}
    assert "devops-sre" in receiver_names
    assert "ward-safety-pager" in receiver_names

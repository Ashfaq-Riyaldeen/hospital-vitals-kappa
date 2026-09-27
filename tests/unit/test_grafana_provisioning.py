"""Unit tests for Grafana dashboard provisioning and JSON panel configurations."""

from __future__ import annotations

import json
from pathlib import Path

import yaml


def test_grafana_datasource_provisioning_syntax() -> None:
    ds_path = Path("observability/grafana/provisioning/datasources/datasources.yml")
    assert ds_path.exists(), "datasources.yml must exist"

    cfg = yaml.safe_load(ds_path.read_text())
    assert cfg.get("apiVersion") == 1
    assert "datasources" in cfg
    datasources = cfg["datasources"]
    assert len(datasources) >= 1

    prom_ds = next((d for d in datasources if d["type"] == "prometheus"), None)
    assert prom_ds is not None, "Prometheus datasource must be defined"
    assert prom_ds["uid"] == "ward-prom"
    assert prom_ds["url"] == "http://prometheus:9090"
    assert prom_ds.get("isDefault") is True


def test_grafana_dashboard_provider_syntax() -> None:
    db_path = Path("observability/grafana/provisioning/dashboards/dashboards.yml")
    assert db_path.exists(), "dashboards.yml must exist"

    cfg = yaml.safe_load(db_path.read_text())
    assert cfg.get("apiVersion") == 1
    assert "providers" in cfg
    providers = cfg["providers"]
    assert len(providers) >= 1

    ward_provider = providers[0]
    assert ward_provider["type"] == "file"
    assert ward_provider["options"]["path"] == "/var/lib/grafana/dashboards"


def test_grafana_dashboards_structure_and_panels() -> None:
    dashboards_dir = Path("observability/grafana/dashboards")
    assert dashboards_dir.exists(), "dashboards directory must exist"

    expected_dashboards = {
        "ward-monitor.json": "ward-clinical-monitor",
        "pipeline-health.json": "ward-pipeline-health",
        "replay-comparison.json": "ward-replay-comparison",
    }

    for filename, expected_uid in expected_dashboards.items():
        file_path = dashboards_dir / filename
        assert file_path.exists(), f"Dashboard {filename} must exist"

        data = json.loads(file_path.read_text())
        assert data.get("uid") == expected_uid, f"UID mismatch for {filename}"
        assert "title" in data and len(data["title"]) > 0
        assert data.get("schemaVersion", 0) >= 30
        assert "panels" in data
        assert len(data["panels"]) >= 5, f"Dashboard {filename} should have >= 5 panels"

        # Check all panels have valid type and titles
        for panel in data["panels"]:
            assert "type" in panel
            assert "title" in panel


def test_dashboard_metric_queries_match_prometheus() -> None:
    dashboards_dir = Path("observability/grafana/dashboards")

    # 1. Ward Monitor must query risk scores and clinical alerts
    wm = json.loads((dashboards_dir / "ward-monitor.json").read_text())
    wm_exprs = [t.get("expr", "") for p in wm["panels"] for t in p.get("targets", [])]
    assert any("patient_composite_risk" in e for e in wm_exprs)
    assert any("patient_news2" in e for e in wm_exprs)

    # 2. Pipeline Health must query silence alert and stage metrics
    ph = json.loads((dashboards_dir / "pipeline-health.json").read_text())
    ph_exprs = [t.get("expr", "") for p in ph["panels"] for t in p.get("targets", [])]
    assert any("WardMonitoringSilent" in e for e in ph_exprs)
    assert any("readings_produced_total" in e for e in ph_exprs)

    # 3. Replay Comparison must track replay progress and version comparison
    rc = json.loads((dashboards_dir / "replay-comparison.json").read_text())
    rc_exprs = [t.get("expr", "") for p in rc["panels"] for t in p.get("targets", [])]
    assert any("replay_progress_pct" in e for e in rc_exprs)
    # Structured Streaming commits no consumer-group offsets, so replay progress is
    # read from the stream's own per-partition offsets, never from consumer lag.
    assert any("stream_partition_offset" in e for e in rc_exprs)
    assert not any("kafka_consumergroup_lag" in e for e in rc_exprs)

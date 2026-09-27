"""FastAPI Serving Layer for Hospital Patient Vital Signs Monitoring.

Under the pure Kappa architecture, this serving layer performs zero clinical
calculation and zero batch-speed merging. It directly queries pre-computed,
query-first views from Cassandra (Q1 through Q6).
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from prometheus_fastapi_instrumentator import Instrumentator

from ward import settings
from ward.api.models import (
    AlertAcknowledgeRequest,
    AlertAcknowledgeResponse,
    AlertItem,
    CutoverRequest,
    CutoverResponse,
    DailyReportMetaResponse,
    LabResultItem,
    PatientDetailResponse,
    PipelineStatusResponse,
    ReplayDiffResponse,
    RiskTrajectoryItem,
    VitalReadingItem,
    WardMonitorResponse,
    WardSummaryResponse,
)
from ward.api.readers import (
    read_patient_labs,
    read_patient_risk_history,
    read_patient_vitals,
    read_ward_alerts,
    read_ward_snapshot,
    read_ward_summary,
    update_alert_acknowledged,
)
from ward.obs import metrics
from ward.obs.log import configure, get_logger
from ward.simclock import read_anchor, utcnow
from ward.store.dao import CassandraDAO
from ward.store.session import create_cluster, get_session

log = get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager configuring Cassandra DAO connection pool and structured logging."""
    obs = settings.observability()
    configure(service="ward-api", stage="serve", level=obs.log_level, json_output=obs.log_json)

    store_cfg = settings.store()
    cluster = None
    session = None
    gauges_task = None

    try:
        # The first version called get_cluster(contact_points=..., keyspace=...),
        # arguments that function does not take. The TypeError was swallowed by the
        # except below, so the API started "healthy" with no database at all.
        cluster = create_cluster(hosts=store_cfg.cassandra_hosts, port=store_cfg.cassandra_port)
        session = get_session(keyspace=store_cfg.keyspace, cluster=cluster)
        app.state.dao = CassandraDAO(session)
        # A cutover must survive an API restart, so the active version is read back.
        persisted = app.state.dao.get_sim_state("ACTIVE_SCORER_VERSION")
        if persisted:
            app.state.active_scorer_version = persisted
        gauges_task = asyncio.create_task(_refresh_serving_gauges())
        app.state.cluster = cluster
        app.state.session = session
        log.info(
            "serving_layer_started",
            cassandra_hosts=store_cfg.cassandra_hosts,
            port=store_cfg.cassandra_port,
        )
    except Exception as exc:
        log.error("cassandra_connection_failed", error=str(exc))
        app.state.dao = None
        app.state.cluster = None
        app.state.session = None

    yield

    if gauges_task:
        gauges_task.cancel()
    if cluster:
        cluster.shutdown()
        log.info("serving_layer_stopped")


def _active_version() -> str:
    return str(getattr(app.state, "active_scorer_version", os.environ.get("SCORER_VERSION", "v1")))


def _current_sim_date() -> date:
    state_path = Path(os.environ.get("SIM_STATE_PATH", "/state/sim_epoch.json"))
    if state_path.exists():
        return read_anchor(state_path).sim_date()
    return date(2026, 4, 1)


async def _refresh_serving_gauges() -> None:
    """Keep the ward-level gauges current for Prometheus, whether or not anyone is
    looking at the ward screen."""
    while True:
        try:
            summary = await read_ward_summary(app.state.dao, "WARD-A", _active_version())
            metrics.serving_stale_labs_patients.set(summary.stale_labs_count)
            metrics.serving_mean_composite_risk.set(summary.mean_composite_risk)
        except Exception as exc:
            log.warning("serving_gauges_failed", error=str(exc))
        await asyncio.sleep(10)


app = FastAPI(
    title="Hospital Patient Clinical Surveillance API",
    description=(
        "Kappa architecture clinical risk serving layer. Delivers live worst-first "
        "patient prioritization, auditable NEWS2 decompositions, and pathology trend views."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Expose Prometheus metrics endpoint at /metrics
Instrumentator().instrument(app).expose(app, include_in_schema=False, endpoint="/metrics")


def _get_dao() -> CassandraDAO:
    """Dependency helper to retrieve the prepared DAO."""
    dao = getattr(app.state, "dao", None)
    if dao is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Cassandra database connection is unavailable",
        )
    return dao


# -----------------------------------------------------------------------------
# Ward Monitor & Summary Endpoints
# -----------------------------------------------------------------------------


@app.get(
    "/api/v1/ward/{ward_id}/monitor",
    response_model=WardMonitorResponse,
    summary="Live Ward Risk Monitor",
    tags=["Ward Monitor"],
)
async def get_monitor_snapshot(
    ward_id: str = "WARD-A",
    version: str | None = Query(None, description="Scorer version; defaults to the active one"),
) -> WardMonitorResponse:
    """Live ward snapshot, worst patient first, one row per patient."""
    dao = _get_dao()
    version = version or _active_version()
    patients = await read_ward_snapshot(dao, ward_id=ward_id, version=version)
    return WardMonitorResponse(
        ward_id=ward_id,
        scorer_version=version,
        timestamp=datetime.now(UTC),
        total_patients=len(patients),
        patients=patients,
    )


@app.get(
    "/api/v1/ward/{ward_id}/summary",
    response_model=WardSummaryResponse,
    summary="Ward Aggregate Risk Metrics",
    tags=["Ward Monitor"],
)
async def get_ward_metrics(
    ward_id: str = "WARD-A",
    version: str | None = Query(None, description="Scorer version; defaults to the active one"),
) -> WardSummaryResponse:
    """Retrieve summary counts by risk tier, deteriorating cases, and mean composite risk."""
    dao = _get_dao()
    return await read_ward_summary(dao, ward_id=ward_id, version=version or _active_version())


# -----------------------------------------------------------------------------
# Patient Detail & History Endpoints
# -----------------------------------------------------------------------------


@app.get(
    "/api/v1/patients/{patient_id}",
    response_model=PatientDetailResponse,
    summary="Comprehensive Patient Clinical Profile",
    tags=["Patient Detail"],
)
async def get_patient_profile(
    patient_id: str,
    version: str = Query("v1", description="Scorer version"),
) -> PatientDetailResponse:
    """Fetch complete patient clinical record: current risk, vitals, and pathology results."""
    dao = _get_dao()
    vitals_future = read_patient_vitals(dao, patient_id=patient_id, limit=20)
    risk_future = read_patient_risk_history(dao, patient_id=patient_id, version=version, limit=1)
    labs_future = read_patient_labs(dao, patient_id=patient_id)

    vitals, risks, labs = await asyncio.gather(vitals_future, risk_future, labs_future)

    if not vitals and not risks:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Patient {patient_id} not found on active ward",
        )

    bed_id = vitals[0].bed_id if vitals else "BED-XX"
    current_risk = risks[0] if risks else None

    return PatientDetailResponse(
        patient_id=patient_id,
        bed_id=bed_id,
        admitting_condition=None,
        current_risk=current_risk,
        latest_vitals=vitals,
        latest_labs=labs,
    )


@app.get(
    "/api/v1/patients/{patient_id}/vitals",
    response_model=list[VitalReadingItem],
    summary="Patient Historical Vital Readings",
    tags=["Patient Detail"],
)
async def get_patient_vitals_feed(
    patient_id: str,
    limit: int = Query(50, ge=1, le=500, description="Maximum observations to return"),
) -> list[VitalReadingItem]:
    """Retrieve historical vital readings for sparkline rendering (Q1)."""
    dao = _get_dao()
    return await read_patient_vitals(dao, patient_id=patient_id, limit=limit)


@app.get(
    "/api/v1/patients/{patient_id}/risk-history",
    response_model=list[RiskTrajectoryItem],
    summary="Auditable Risk Evaluation Trajectory",
    tags=["Patient Detail"],
)
async def get_patient_evaluations(
    patient_id: str,
    version: str = Query("v1", description="Scorer version (v1 or v2)"),
    limit: int = Query(100, ge=1, le=500),
) -> list[RiskTrajectoryItem]:
    """Retrieve historical risk calculations with subscores and explanations (Q3)."""
    dao = _get_dao()
    return await read_patient_risk_history(dao, patient_id=patient_id, version=version, limit=limit)


@app.get(
    "/api/v1/patients/{patient_id}/labs",
    response_model=list[LabResultItem],
    summary="Patient Pathology Laboratory Panel",
    tags=["Patient Detail"],
)
async def get_patient_pathology(patient_id: str) -> list[LabResultItem]:
    """Retrieve pathology results with reference ranges and abnormal flags (Q5)."""
    dao = _get_dao()
    return await read_patient_labs(dao, patient_id=patient_id)


# -----------------------------------------------------------------------------
# Alerts & Acknowledgments
# -----------------------------------------------------------------------------


@app.get(
    "/api/v1/alerts",
    response_model=list[AlertItem],
    summary="Ward Deterioration Alerts Feed",
    tags=["Alerts"],
)
async def get_alerts_feed(
    ward_id: str = "WARD-A",
    sim_date: str | None = Query(None, description="Simulated calendar date (YYYY-MM-DD)"),
    severity: str | None = Query(None, description="Optional severity filter"),
    limit: int = Query(50, ge=1, le=200),
) -> list[AlertItem]:
    """Retrieve time-bounded ward alerts (Q4)."""
    dao = _get_dao()
    # Default to TODAY on the simulated clock. It used to default to the first day
    # of the run, so the alert feed went quiet from day 2 onwards.
    target_date = date.fromisoformat(sim_date) if sim_date else _current_sim_date()
    alerts = await read_ward_alerts(dao, ward_id=ward_id, alert_date=target_date, limit=limit)
    if severity:
        sev_upper = severity.upper()
        alerts = [a for a in alerts if a.severity.upper() == sev_upper]
    return alerts


@app.post(
    "/api/v1/alerts/{alert_id}/acknowledge",
    response_model=AlertAcknowledgeResponse,
    summary="Acknowledge Clinical Alert",
    tags=["Alerts"],
)
async def post_alert_acknowledgement(
    alert_id: str,
    body: AlertAcknowledgeRequest,
    ward_id: str = "WARD-A",
    sim_date: str = Query("2026-04-01", description="Alert date"),
    alert_time: datetime = Query(default_factory=lambda: datetime.now(UTC)),
) -> AlertAcknowledgeResponse:
    """Record clinician acknowledgment for an active alert in Cassandra."""
    dao = _get_dao()
    target_date = date.fromisoformat(sim_date)
    await update_alert_acknowledged(
        dao,
        ward_id=ward_id,
        alert_date=target_date,
        alert_time=alert_time,
        alert_id=alert_id,
    )
    return AlertAcknowledgeResponse(
        alert_id=alert_id,
        status="ACKNOWLEDGED",
        acknowledged_at=datetime.now(UTC),
        acknowledged_by=body.acknowledged_by,
    )


# -----------------------------------------------------------------------------
# Reports & Replay Validation
# -----------------------------------------------------------------------------


@app.get(
    "/api/v1/reports/daily/{sim_date}",
    response_model=DailyReportMetaResponse,
    summary="Clinical Daily Risk Report Metadata",
    tags=["Reports"],
)
async def get_report_metadata(sim_date: str, ward_id: str = "WARD-A") -> DailyReportMetaResponse:
    """Retrieve file links and metadata for Increment 3's daily clinical PDF report."""
    reports_dir = Path(os.environ.get("REPORTS_DIR", "./reports"))
    pdf_path = reports_dir / f"ward_daily_report_{sim_date}.pdf"
    html_path = reports_dir / f"ward_daily_report_{sim_date}.html"

    return DailyReportMetaResponse(
        ward_id=ward_id,
        sim_date=sim_date,
        pdf_path=str(pdf_path),
        html_path=str(html_path),
        generated=pdf_path.exists(),
        download_url=f"/api/v1/reports/daily/{sim_date}/download",
    )


@app.get(
    "/api/v1/reports/daily/{sim_date}/download",
    summary="Download Clinical Daily PDF Report",
    tags=["Reports"],
)
async def download_report_pdf(sim_date: str) -> FileResponse:
    """Download the compiled PDF daily clinical surveillance report."""
    reports_dir = Path(os.environ.get("REPORTS_DIR", "./reports"))
    pdf_path = reports_dir / f"ward_daily_report_{sim_date}.pdf"
    if not pdf_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Report PDF for simulated date {sim_date} does not exist",
        )
    return FileResponse(
        path=pdf_path,
        media_type="application/pdf",
        filename=f"ward_daily_report_{sim_date}.pdf",
    )


@app.get(
    "/api/v1/replay/compare",
    response_model=ReplayDiffResponse,
    summary="Scorer Version Replay Divergence Diff",
    tags=["Replay Validation"],
)
async def get_replay_divergence(
    ward_id: str = "WARD-A",
    v1: str = Query("v1", description="Baseline scorer version"),
    v2: str = Query("v2", description="Replayed target scorer version"),
) -> ReplayDiffResponse:
    """Compute risk divergence between v1 and v2 across the ward."""
    dao = _get_dao()
    v1_rows = await read_ward_snapshot(dao, ward_id=ward_id, version=v1)
    v2_rows = await read_ward_snapshot(dao, ward_id=ward_id, version=v2)

    v1_map = {r.patient_id: r for r in v1_rows}
    v2_map = {r.patient_id: r for r in v2_rows}

    diffs: list[dict[str, Any]] = []
    copd_count = 0
    total_delta = 0

    for pid in sorted(v1_map.keys() & v2_map.keys()):
        row1 = v1_map[pid]
        row2 = v2_map[pid]
        delta = row2.risk_score - row1.risk_score
        total_delta += delta

        if delta != 0:
            copd_count += 1
            diffs.append(
                {
                    "patient_id": pid,
                    "bed_id": row1.bed_id,
                    "v1_risk": row1.risk_score,
                    "v2_risk": row2.risk_score,
                    "delta": delta,
                    "v1_tier": row1.risk_tier,
                    "v2_tier": row2.risk_tier,
                }
            )

    compared = len(v1_map.keys() & v2_map.keys())
    mean_delta = round(total_delta / compared, 2) if compared > 0 else 0.0

    return ReplayDiffResponse(
        v1_version=v1,
        v2_version=v2,
        total_patients_compared=compared,
        copd_reclassifications=copd_count,
        mean_risk_delta=mean_delta,
        clinical_summary=(
            f"Compared {compared} patients' current scores under {v1} and {v2}; "
            f"{copd_count} differ. Under NEWS2 Scale 2 only COPD patients should."
        ),
        patient_diffs=diffs,
    )


@app.get(
    "/api/v1/pipeline/status",
    response_model=PipelineStatusResponse,
    summary="System Vitals & Pipeline Status",
    tags=["Observability"],
)
async def get_pipeline_vitals() -> PipelineStatusResponse:
    """Retrieve simulated clock position, active scorer version, and service connectivity."""
    state_path = Path(os.environ.get("SIM_STATE_PATH", "/state/sim_epoch.json"))
    if state_path.exists():
        clock = read_anchor(state_path)
        s_now = clock.sim_now()
        s_date = clock.sim_date().isoformat()
        speedup = clock.speedup
    else:
        s_now = datetime(2026, 4, 1, 0, 0, 0, tzinfo=UTC)
        s_date = "2026-04-01"
        speedup = 288.0

    dao_status = "HEALTHY" if getattr(app.state, "dao", None) is not None else "OFFLINE"
    active_v = _active_version()

    return PipelineStatusResponse(
        status="RUNNING",
        sim_now=s_now,
        sim_date=s_date,
        active_scorer_version=active_v,
        speedup_factor=speedup,
        connected_services={
            "cassandra": dao_status,
            "architecture": "Kappa",
        },
    )


@app.post(
    "/api/v1/pipeline/cutover",
    response_model=CutoverResponse,
    summary="Cutover or Rollback Active Scorer Version",
    tags=["Replay Validation"],
)
async def post_pipeline_cutover(
    req: CutoverRequest,
) -> CutoverResponse:
    """Instantly switch or roll back the active scorer version served across the ward."""
    prev = getattr(app.state, "active_scorer_version", os.environ.get("SCORER_VERSION", "v1"))
    app.state.active_scorer_version = req.target_version

    session = getattr(app.state, "session", None)
    if session is not None:
        try:
            stmt = session.prepare("INSERT INTO ward.sim_state (key, value) VALUES (?, ?)")
            session.execute(stmt.bind(("ACTIVE_SCORER_VERSION", req.target_version)))
        except Exception:
            pass

    return CutoverResponse(
        status="SUCCESS",
        previous_version=prev,
        active_version=req.target_version,
        timestamp=utcnow(),
    )


# -----------------------------------------------------------------------------
# Observability & Health Probes
# -----------------------------------------------------------------------------


@app.get("/health/live", summary="Liveness Probe", tags=["Observability"])
async def health_live() -> dict[str, str]:
    """Basic liveness probe confirming the HTTP server is responsive."""
    return {"status": "alive"}


@app.get("/health/ready", summary="Readiness Probe", tags=["Observability"])
async def health_ready() -> dict[str, str]:
    """Readiness probe verifying that Cassandra DAO is initialized."""
    if getattr(app.state, "dao", None) is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Serving layer is not ready: database connection pending",
        )
    return {"status": "ready"}


@app.get("/health/deep", summary="Deep Diagnostic Probe", tags=["Observability"])
async def health_deep() -> dict[str, Any]:
    """Deep probe executing a round-trip diagnostic query against Cassandra."""
    session = getattr(app.state, "session", None)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Deep healthcheck failed: session is None",
        )

    try:
        t0 = utcnow()
        row = session.execute("SELECT now() FROM system.local").one()
        latency_ms = round((utcnow() - t0).total_seconds() * 1000, 2)
        return {
            "status": "healthy",
            "cassandra_latency_ms": latency_ms,
            "cluster_time": str(row[0]) if row else "unknown",
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Cassandra query failed: {exc}",
        ) from exc

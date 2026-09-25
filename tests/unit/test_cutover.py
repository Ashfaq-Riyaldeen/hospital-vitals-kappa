"""Unit tests for Scorer Version Cutover and Rollback."""

from __future__ import annotations

from unittest.mock import MagicMock

from fastapi.testclient import TestClient
from ward.api.app import app
from ward.replay.cutover import CutoverManager


def test_cutover_manager_mock_session() -> None:
    session = MagicMock()
    # Mock query
    mock_row = MagicMock()
    mock_row.value = "v2"
    session.execute.return_value.one.return_value = mock_row

    mgr = CutoverManager(session=session)
    assert mgr.query_active_version() == "v2"

    # Test apply
    mgr.apply_active_version("v1")
    session.prepare.assert_called()
    session.execute.assert_called()


def test_api_cutover_and_rollback() -> None:
    client = TestClient(app)

    # Cutover to v2
    res_cutover = client.post(
        "/api/v1/pipeline/cutover",
        json={"target_version": "v2"},
    )
    assert res_cutover.status_code == 200
    data = res_cutover.json()
    assert data["status"] == "SUCCESS"
    assert data["active_version"] == "v2"

    # Check status endpoint
    res_status = client.get("/api/v1/pipeline/status")
    assert res_status.status_code == 200
    assert res_status.json()["active_scorer_version"] == "v2"

    # Rollback to v1
    res_rollback = client.post(
        "/api/v1/pipeline/cutover",
        json={"target_version": "v1"},
    )
    assert res_rollback.status_code == 200
    assert res_rollback.json()["active_version"] == "v1"

    # Check status endpoint after rollback
    res_status2 = client.get("/api/v1/pipeline/status")
    assert res_status2.status_code == 200
    assert res_status2.json()["active_scorer_version"] == "v1"

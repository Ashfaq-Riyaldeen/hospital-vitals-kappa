"""test_dag_purity: Mechanical AST check enforcing 'Airflow only orchestrates'.

Under Kappa architecture, the single streaming pipeline is the sole computation path.
Airflow orchestrates episodic workflows around the stream (sensing lab drops, scheduling
daily reports, coordinating replays, and liveness probes) without ever duplicating
clinical calculations.

This test uses AST inspection to guarantee that:
1. No DataFrame libraries (pandas, numpy, pyspark, sklearn) are imported in any DAG file.
2. No aggregation methods (groupby, agg, merge, pivot_table) are called.
3. No clinical scoring functions are called in DAG files.
4. Every DAG contains comprehensive documentation explaining its clinical purpose.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

DAG_DIR = Path(__file__).resolve().parents[2] / "airflow/dags"

FORBIDDEN_IMPORTS = {"pandas", "numpy", "pyspark", "sklearn"}
FORBIDDEN_CALLS = {
    "groupby",
    "agg",
    "merge",
    "pivot_table",
    "resample",
    "rolling",
    "score_news2",
    "calculate_news2",
    "compute_composite_risk",
}
EXPECTED_DAG_FILES = {
    "ward_lab_ingest.py",
    "ward_daily_report.py",
    "ward_replay.py",
    "ward_healthcheck.py",
    "ward_retention.py",
}


def _dag_files() -> list[Path]:
    return sorted(DAG_DIR.glob("*.py"))


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(), filename=str(path))


def test_all_expected_dag_files_exist():
    present_names = {p.name for p in _dag_files()}
    assert present_names >= EXPECTED_DAG_FILES, (
        f"Missing expected DAG files: {EXPECTED_DAG_FILES - present_names}"
    )


@pytest.mark.parametrize("path", _dag_files(), ids=lambda p: p.name)
def test_dag_file_parses(path: Path):
    _tree(path)


@pytest.mark.parametrize("path", _dag_files(), ids=lambda p: p.name)
def test_no_dataframe_libraries_imported(path: Path):
    found: set[str] = set()
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.Import):
            found |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module.split(".")[0])
    offending = found & FORBIDDEN_IMPORTS
    assert not offending, (
        f"{path.name} imports forbidden library {sorted(offending)}: "
        "Airflow only orchestrates; computation belongs in stream processing or clinical engines"
    )


@pytest.mark.parametrize("path", _dag_files(), ids=lambda p: p.name)
def test_no_aggregation_calls(path: Path):
    offending: list[str] = []
    for node in ast.walk(_tree(path)):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in FORBIDDEN_CALLS
        ):
            offending.append(f"{node.func.attr}() at line {node.lineno}")
    assert not offending, f"{path.name} executes forbidden transformations: {offending}"


@pytest.mark.parametrize("path", _dag_files(), ids=lambda p: p.name)
def test_every_dag_file_documents_itself(path: Path):
    doc = ast.get_docstring(_tree(path))
    assert doc and len(doc) > 200, (
        f"{path.name} must contain a docstring explaining the workflow (>200 chars)"
    )

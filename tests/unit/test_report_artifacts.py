"""Automated verification tests for report artifacts, diagrams, and figures.

Ensures that all required coursework deliverables, architectural diagrams,
evidence figures, and PDF publication artifacts exist, are non-empty, and contain
zero unresolved placeholders.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from PIL import Image

REPO_ROOT = Path(__file__).resolve().parents[2]
FIGURES_DIR = REPO_ROOT / "docs" / "report" / "figures"
REPORT_DIR = REPO_ROOT / "docs" / "report"


def test_vector_diagram_artifacts_exist() -> None:
    """Verify all four TikZ architectural vector PDFs exist and have non-zero size."""
    expected_diagrams = [
        "D1-context.pdf",
        "D2-layered-architecture.pdf",
        "D3-event-sequence.pdf",
        "D4-replay-branching.pdf",
    ]
    for name in expected_diagrams:
        pdf_path = FIGURES_DIR / name
        assert pdf_path.exists(), f"Missing vector diagram artifact: {name}"
        assert pdf_path.stat().st_size > 5000, f"Vector diagram {name} is unusually small or empty"


def test_evidence_figures_exist_and_are_valid_images() -> None:
    """Verify all ten evidence PNG figures exist and are valid readable images."""
    expected_figures = [
        "R1-ward-monitor.png",
        "R2-pipeline-health.png",
        "R3-replay-comparison.png",
        "R4-airflow-grid.png",
        "R5-airflow-graph.png",
        "R6-kafka-topics.png",
        "R7-api-docs.png",
        "R8-daily-report-sample.png",
        "R9-prometheus-alerts.png",
        "R9b-alertmanager.png",
    ]
    for fig_name in expected_figures:
        fig_path = FIGURES_DIR / fig_name
        assert fig_path.exists(), f"Missing evidence figure: {fig_name}"
        assert fig_path.stat().st_size > 1000, f"Evidence figure {fig_name} is too small"
        # Validate that PIL can read and parse the PNG format
        with Image.open(fig_path) as img:
            assert img.format == "PNG", f"Figure {fig_name} is not a valid PNG"
            assert img.width > 200 and img.height > 200, f"Figure {fig_name} dimensions too small"


def test_compiled_report_pdf_validity() -> None:
    """Verify docs/report/main.pdf exists and satisfies coursework length requirements."""
    main_pdf = REPORT_DIR / "main.pdf"
    assert main_pdf.exists(), "docs/report/main.pdf does not exist. Run 'make report' to build it."
    assert main_pdf.stat().st_size > 100_000, "main.pdf is unusually small (< 100 KB)"

    # Check page count via pdfinfo
    res = subprocess.run(["pdfinfo", str(main_pdf)], capture_output=True, text=True, check=True)
    page_line = [ln for ln in res.stdout.splitlines() if ln.startswith("Pages:")]
    assert page_line, "Could not determine page count from pdfinfo"
    pages = int(page_line[0].split(":")[1].strip())
    assert 15 <= pages <= 22, f"Report page count ({pages}) outside required 15--22 page range"


def test_report_source_has_zero_placeholders() -> None:
    """Verify docs/report/main.tex contains zero unfinished template placeholder tokens."""
    main_tex = REPORT_DIR / "main.tex"
    assert main_tex.exists(), "docs/report/main.tex not found"
    content = main_tex.read_text(encoding="utf-8")

    forbidden_tokens = ["EG/20XX", "Group XX", "TODO", "XXXX"]
    for token in forbidden_tokens:
        assert token not in content, f"Unresolved placeholder '{token}' found in main.tex"

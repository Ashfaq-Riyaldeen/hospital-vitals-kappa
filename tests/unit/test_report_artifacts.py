"""Checks on the report and its figures.

- every figure the report includes exists, and every diagram has a draw.io source;
- screenshots are real PNGs of a reasonable size (the capture script only produces
  them from the running stack; the old mock-up generator is gone);
- the report text follows the agreed rules: no hash character, no mention of the
  other coursework project, no unfilled measurement placeholder.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest
from PIL import Image

REPO_ROOT = Path(__file__).resolve().parents[2]
REPORT_DIR = REPO_ROOT / "docs" / "report"
FIGURES_DIR = REPORT_DIR / "figures"
DIAGRAMS_DIR = REPO_ROOT / "docs" / "diagrams"
MAIN_TEX = REPORT_DIR / "main.tex"


def _tex() -> str:
    return MAIN_TEX.read_text() + (REPORT_DIR / "measurements.tex").read_text()


def _referenced_figures() -> list[str]:
    tex = MAIN_TEX.read_text()
    shots = [f"{name}.png" for name in re.findall(r"\\shot\{([^}]+)\}", tex)]
    graphics = [Path(p).name for p in re.findall(r"\\includegraphics\[[^]]*\]\{([^}]+)\}", tex)]
    # Skip the \shot macro's own definition, whose path is the parameter #1.
    return [g for g in shots + graphics if g != "Logo.jpeg" and "#" not in g]


@pytest.mark.parametrize("name", _referenced_figures())
def test_every_referenced_figure_exists(name: str) -> None:
    path = FIGURES_DIR / name
    assert path.exists(), f"main.tex includes {name}, which is not in docs/report/figures"
    if name.endswith(".png"):
        with Image.open(path) as img:
            assert img.width >= 600 and img.height >= 150, f"{name} looks too small"


def test_every_diagram_has_a_drawio_source() -> None:
    for pdf in FIGURES_DIR.glob("D*.pdf"):
        assert (DIAGRAMS_DIR / f"{pdf.stem}.drawio").exists(), f"{pdf.name} has no .drawio"


def test_report_uses_no_hash_character() -> None:
    body = "\n".join(line.split("%", 1)[0] for line in _tex().splitlines())
    body = re.sub(r"#[1-9]", "", body)  # LaTeX macro parameters, not text
    assert "#" not in body


def test_report_does_not_mention_the_other_project() -> None:
    text = _tex().lower()
    for word in ("ride-hailing", "ride hailing", "sibling", "fleet"):
        assert word not in text, f"the report mentions {word!r}"


def test_no_measurement_is_left_unfilled() -> None:
    assert "TBD" not in (REPORT_DIR / "measurements.tex").read_text()


@pytest.mark.skipif(shutil.which("pdftotext") is None, reason="needs poppler")
def test_built_pdf_has_no_hash_and_no_placeholder() -> None:
    pdf = REPORT_DIR / "main.pdf"
    if not pdf.exists():
        pytest.skip("main.pdf not built")
    text = subprocess.run(
        ["pdftotext", str(pdf), "-"], capture_output=True, text=True, check=True
    ).stdout
    assert "#" not in text
    assert "??" not in text, "an unresolved reference (??) is in the PDF"

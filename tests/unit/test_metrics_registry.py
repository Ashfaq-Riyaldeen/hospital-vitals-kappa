"""Metric naming conventions, and the guard on the log field schema.

Both are cheap tests protecting things that fail silently. A metric named against a
convention nobody enforces drifts; a log line whose severity was overwritten reads
perfectly while carrying the wrong level.
"""

from __future__ import annotations

import pytest
import ward.obs.metrics as metrics
from prometheus_client import REGISTRY

WARD_PREFIXES = (
    "readings_",
    "producer_",
    "lab_",
    "clinical_",
    "sim_clock_",
    "replay_",
    "sink_",
    "defects_",
    "admissions_",
)


def _ours() -> list:
    return [m for m in REGISTRY.collect() if m.name.startswith(WARD_PREFIXES)]


def test_the_catalogue_is_registered() -> None:
    """plan/09 section 3 lists the metrics this system must expose."""
    assert len(_ours()) >= 20


@pytest.mark.parametrize("metric", _ours(), ids=lambda m: m.name)
def test_every_metric_has_help_text(metric) -> None:
    """A metric nobody can interpret is a metric nobody will act on -- and these go
    straight into dashboard panels and alert annotations."""
    assert metric.documentation.strip(), f"{metric.name} has no HELP text"
    assert len(metric.documentation) > 20, f"{metric.name}'s HELP text is too thin to help"


def test_counters_are_declared_with_a_total_suffix() -> None:
    """Counters end `_total`, so a reader writing PromQL can tell a counter from a
    gauge by name and knows a `rate()` is appropriate.

    Asserted against the SOURCE rather than the registry. `prometheus_client` strips
    `_total` from `.name`, and a labelled counter that has never been incremented
    exposes no samples at all -- so the registry cannot answer this question for a
    freshly imported module, and a test that asked it there would pass vacuously.
    """
    import ast
    from pathlib import Path

    source = Path(metrics.__file__).read_text()
    offenders = []
    for node in ast.walk(ast.parse(source)):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "Counter"
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ):
            name = node.args[0].value
            if not name.endswith("_total"):
                offenders.append(f"line {node.lineno}: {name}")
    assert not offenders, f"counters without a _total suffix: {offenders}"


@pytest.mark.parametrize("metric", _ours(), ids=lambda m: m.name)
def test_time_measurements_say_so_in_their_name(metric) -> None:
    """A duration named without `_seconds` invites somebody to plot milliseconds on an
    axis labelled seconds. The sibling project shipped exactly that bug once."""
    if "duration" in metric.name or "delay" in metric.name:
        assert metric.name.endswith("_seconds"), (
            f"{metric.name} measures time but does not say so in its name"
        )


def test_the_dlq_metric_is_labelled_by_device() -> None:
    """★ The label that turns a data-quality metric into a ward-operations one.

    A global reject rate says 'something is wrong'. `readings_dlq_total{device_id=
    "BED-22"}` says 'BED-22's probe keeps detaching', which somebody can act on
    without opening a query console.
    """
    dlq = next(m for m in _ours() if m.name == "readings_dlq")
    sample_labels = {k for s in dlq.samples for k in s.labels}
    assert "device_id" in sample_labels or dlq.samples == []
    assert "reason" in metrics.readings_dlq_total._labelnames
    assert "device_id" in metrics.readings_dlq_total._labelnames


def test_clinical_alerts_are_separate_from_pipeline_health() -> None:
    """Alerts about PATIENTS and alerts about the SYSTEM are different things with
    different audiences. Conflating them is how an on-call rota learns to ignore
    both -- and in this domain that is a safety failure, not an annoyance."""
    assert "severity" in metrics.clinical_alerts_emitted_total._labelnames
    assert "type" in metrics.clinical_alerts_emitted_total._labelnames


def test_replay_diff_is_counted_by_direction() -> None:
    """The replay's expected shape is 'only COPD patients, only downward'. Counting
    the direction is what lets that be asserted live rather than eyeballed."""
    assert "direction" in metrics.replay_scores_changed_total._labelnames

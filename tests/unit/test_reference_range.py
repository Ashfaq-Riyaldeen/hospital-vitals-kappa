"""Parsing lab reference ranges — a small cleaning transformation, done carefully.

Real lab feeds send ranges as strings and they are messier than "4.0-11.0". Getting
the one-sided forms wrong is a clinical problem in both directions: defaulting an
unparseable range to "normal" quietly reassures, and defaulting it to "abnormal"
floods the ward with false positives until staff stop reading the alerts.
"""

from __future__ import annotations

import pytest
from ward.clinical.lab_rules import UNPARSED, is_abnormal, parse_reference_range


@pytest.mark.parametrize(
    ("raw", "low", "high"),
    [
        ("4.0-11.0", 4.0, 11.0),
        ("0.5 - 2.2", 0.5, 2.2),  # whitespace
        ("0.5–2.2", 0.5, 2.2),  # noqa: RUF001 - en dash; real feeds send these
        ("<5.0", None, 5.0),  # upper bound only
        ("<= 5.0", None, 5.0),
        (">40", 40.0, None),  # lower bound only
        (">= 40", 40.0, None),
        ("135-145", 135.0, 145.0),
    ],
)
def test_parses_the_forms_a_real_feed_sends(raw, low, high) -> None:
    parsed = parse_reference_range(raw)
    assert (parsed.low, parsed.high) == (low, high)


@pytest.mark.parametrize("raw", ["", "   ", None, "normal", "4.0 to 11.0", "abc-def"])
def test_unparseable_ranges_are_reported_not_guessed(raw) -> None:
    assert parse_reference_range(raw) == UNPARSED


def test_a_reversed_range_is_treated_as_unparseable() -> None:
    """★ "11.0-4.0" is a lab-feed error, not a very wide range.

    Taken at face value it would make every result abnormal in one direction and
    none in the other, silently — which is worse than admitting we cannot read it.
    """
    assert parse_reference_range("11.0-4.0") == UNPARSED


@pytest.mark.parametrize(
    ("value", "raw", "expected"),
    [
        (3.8, "0.5-2.2", True),  # P014's day-2 lactate
        (1.2, "0.5-2.2", False),
        (0.1, "0.5-2.2", True),  # below range is abnormal too
        (3.1, "<5.0", False),  # within an upper-only bound
        (7.0, "<5.0", True),
        (55.0, ">40", False),  # above a lower-only bound is normal
        (12.0, ">40", True),
    ],
)
def test_abnormality_against_each_range_shape(value, raw, expected) -> None:
    assert is_abnormal(value, parse_reference_range(raw)) is expected


def test_unknown_range_returns_none_not_false() -> None:
    """★ "We do not know" and "this is normal" are different clinical statements.

    Collapsing them would let an unparseable range quietly reassure somebody, which
    is the failure direction that matters here.
    """
    assert is_abnormal(99.0, UNPARSED) is None

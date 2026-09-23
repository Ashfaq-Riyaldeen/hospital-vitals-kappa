"""Lab reference ranges, and what "abnormal" means.

Lives in `ward/clinical/` because deciding whether a result is out of range is a
CLINICAL rule, and this project keeps all of those in one place. It computes no risk
score -- that stays in `news2.py` -- but it feeds one, and a threshold that feeds a
risk tier does not belong in a producer or a parser.

REFERENCE RANGES ARRIVE AS STRINGS, which is how a real lab feed sends them and what
the assignment's own field list specifies. Parsing them is a small, genuinely testable
cleaning transformation, and real feeds are messier than "4.0-11.0":

    "4.0-11.0"   two-sided
    "<5.0"       upper bound only -- the lab cannot measure lower
    ">40"        lower bound only
    "0.5 - 2.2"  whitespace
    ""           absent

Handling the one-sided forms matters clinically. A creatinine reported as "<5.0" with
a value of 3.1 is NORMAL; treating the unparsed range as "no range" and defaulting to
normal would be right by accident, while defaulting to abnormal would flood the ward
with false positives. Getting this wrong in either direction is a safety problem, so
the parser returns what it knows and says when it knows nothing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

# Anchored, and tolerant of whitespace and a leading sign. Deliberately NOT tolerant
# of anything else: a range this cannot parse must surface as unparseable rather than
# be guessed at.
_TWO_SIDED: Final = re.compile(r"^\s*(-?\d+(?:\.\d+)?)\s*[-–]\s*(-?\d+(?:\.\d+)?)\s*$")  # noqa: RUF001 - real feeds send en dashes
_UPPER_ONLY: Final = re.compile(r"^\s*<\s*=?\s*(-?\d+(?:\.\d+)?)\s*$")
_LOWER_ONLY: Final = re.compile(r"^\s*>\s*=?\s*(-?\d+(?:\.\d+)?)\s*$")


@dataclass(frozen=True, slots=True)
class ReferenceRange:
    """A parsed range. `low is None` means unbounded below, not zero."""

    low: float | None
    high: float | None

    @property
    def is_unparsed(self) -> bool:
        return self.low is None and self.high is None


UNPARSED: Final = ReferenceRange(None, None)


def parse_reference_range(raw: str | None) -> ReferenceRange:
    """Parse a lab reference range. Returns UNPARSED rather than raising.

    A single malformed range must not fail the whole file: the other five results for
    that patient are still usable, and discarding them would lose real information.
    The caller checks `is_unparsed` and records it as a data-quality event.
    """
    if not raw or not raw.strip():
        return UNPARSED

    if m := _TWO_SIDED.match(raw):
        low, high = float(m.group(1)), float(m.group(2))
        # A reversed range is a lab-feed error, not a very wide range. Returning it
        # as-is would make every result "abnormal" in one direction and none in the
        # other, silently.
        return UNPARSED if low > high else ReferenceRange(low, high)

    if m := _UPPER_ONLY.match(raw):
        return ReferenceRange(None, float(m.group(1)))

    if m := _LOWER_ONLY.match(raw):
        return ReferenceRange(float(m.group(1)), None)

    return UNPARSED


def is_abnormal(value: float, reference: ReferenceRange) -> bool | None:
    """Is this result outside its range?

    Returns None when the range could not be parsed -- NOT False. "We do not know"
    and "this is normal" are different clinical statements, and collapsing them would
    let an unparseable range quietly reassure somebody.
    """
    if reference.is_unparsed:
        return None
    if reference.low is not None and value < reference.low:
        return True
    return reference.high is not None and value > reference.high

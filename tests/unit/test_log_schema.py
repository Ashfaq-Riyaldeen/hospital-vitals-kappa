"""The structured-log field schema, and the guard that protects it.

structlog silently overwrites reserved keys. A call passing `level=` as a field
clobbers the real severity and the resulting line still looks entirely normal -- which
in a ward monitor means an alert-worthy event filed as INFO and never noticed.

This was found the hard way on the sibling project. The guard processor exists because
of it, and this test exists so the guard cannot be quietly reordered away.
"""

from __future__ import annotations

import pytest
from ward.obs import log as logmod


def test_reserved_fields_are_rejected_loudly() -> None:
    """★ The whole point. Passing a reserved key must RAISE, not silently overwrite.

    `event` is excluded from the check by design: structlog puts the positional log
    message there itself, so it is present on every record and is not a caller
    mistake. Everything else in the reserved set came from the caller.
    """
    guard = logmod._reject_reserved_fields
    for reserved in sorted(logmod.RESERVED_LOG_FIELDS - {"event"}):
        with pytest.raises(ValueError, match="reserved"):
            guard(None, "info", {"event": "x", reserved: "clobbered"})


def test_the_positional_message_is_not_treated_as_a_clash() -> None:
    """structlog always sets `event`; flagging it would make every log line raise."""
    guard = logmod._reject_reserved_fields
    assert guard(None, "info", {"event": "reading_scored"}) == {"event": "reading_scored"}


def test_ordinary_fields_pass_through() -> None:
    guard = logmod._reject_reserved_fields
    payload = {"event": "reading_scored", "patient_id": "P014", "news2": 7}
    assert guard(None, "info", dict(payload)) == payload


def test_the_guard_runs_first_in_the_chain() -> None:
    """Position matters: anywhere but first and the clobbering has already happened
    by the time the guard sees the record."""
    import inspect

    source = inspect.getsource(logmod.configure)
    guard_at = source.index("_reject_reserved_fields")
    # No other processor may be added to the chain before it.
    chain_start = source.index("processors")
    assert guard_at > chain_start
    before = source[chain_start:guard_at]
    assert "structlog.processors." not in before, (
        "a processor is registered before the reserved-field guard; the guard must be first"
    )

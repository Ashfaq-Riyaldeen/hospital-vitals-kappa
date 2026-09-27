"""P007's heart-rate artefact must be exactly one reading, whatever the clock does.

In the final clean run the host clock stepped by about half a second (2-3 simulated
minutes at 288x), two monitor sweeps landed inside the 15-minute spike window, and two
readings of 145 were sent. Two in a row is a sustained finding, so the stream rightly
raised an alert - but the script says the spike is a single artefact.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from ward.producers import bedside_monitor
from ward.producers.physiology import narratives
from ward.simclock import SimClock


def test_spike_is_sent_once_even_if_two_sweeps_fall_in_its_window() -> None:
    epoch = datetime(2026, 4, 1, tzinfo=UTC)
    clock = SimClock(epoch_wall=datetime(2026, 9, 27, tzinfo=UTC), epoch_sim=epoch, day_seconds=300)
    monitors = bedside_monitor.BedsideMonitors(clock, dry_run=True)
    p007 = next(p for p in monitors.patients if p.patient_id == narratives.SPIKE_PATIENT)
    spike = epoch + timedelta(days=narratives.SPIKE_DAY - 1, hours=narratives.SPIKE_HOUR)
    sent = []
    for t in (spike + timedelta(minutes=1), spike + timedelta(minutes=12)):
        payload, _ = monitors._reading(p007, t)
        sent.append(payload["heart_rate"])
    assert sent.count(round(narratives.SPIKE_HEART_RATE)) == 1, sent

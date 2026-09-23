"""The daily-batch source: one lab-results file per simulated day.

This is the second of the assignment's two required sources, and it is genuinely
batch-shaped -- a pathology lab releases results once, in a file, not as a stream.
Under Kappa that file is turned into log records by a thin Airflow step containing no
business logic; the file itself is the source.

THE ATOMIC DROP IS THE POINT OF THIS FILE. Airflow watches `labs/inbox/` with a
FileSensor. Writing directly to the final path means the sensor can fire on a
half-written file, the DAG parses truncated JSON, and the failure is intermittent and
timing-dependent -- the worst kind to debug. So the file is written to a `.tmp`
subdirectory and then `os.rename`d into place. Rename is atomic within a filesystem,
so the sensor sees either nothing or a complete file. This is a real and very common
bug, and avoiding it is worth more than it looks.

Four scenarios are simulated deliberately, because each exercises a different branch
of the ingest DAG:

    late       the file arrives after the simulated day boundary (sensor poke path)
    missing    no file at all on one day (sensor timeout -> branch -> alert)
    malformed  a corrupted reference range (validation -> quarantine)
    normal     everything else

The missing day matters most for the report: the ward keeps being monitored on vitals
alone, and the API reports `lab_data_stale: true`. Degrading honestly rather than
failing is the correct clinical behaviour.

Run:
    python -m ward.producers.lab_uploader
    python -m ward.producers.lab_uploader --once --sim-date 2026-04-02
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from random import Random
from types import FrameType
from typing import Final

from ward import settings
from ward.obs import metrics
from ward.obs.log import configure, get_logger
from ward.producers.ward_model import Patient, build_ward
from ward.simclock import SimClock, read_anchor, utcnow

log = get_logger()

INBOX: Final[Path] = Path(os.environ.get("LAB_INBOX", "/labs/inbox"))
LAB_ID: Final[str] = "PATH-LAB-1"
WARD_ID: Final[str] = "WARD-A"

# Which simulated day gets which fault. Fixed rather than random so the demo can
# promise "watch day 5" and be right.
MISSING_LAB_DAY: Final[int] = int(os.environ.get("SIMULATE_MISSING_LAB_DAY", "5"))
MALFORMED_LAB_DAY: Final[int] = int(os.environ.get("SIMULATE_MALFORMED_LAB_DAY", "4"))
MALFORMED_PATIENT: Final[str] = "P003"
MALFORMED_TEST: Final[str] = "lactate"
LATE_PROBABILITY: Final[float] = 0.15
LATE_DELAY_REAL_SECONDS: Final[tuple[int, int]] = (20, 80)


@dataclass(frozen=True, slots=True)
class TestSpec:
    """One assay: its units and the reference range a real feed would quote.

    The range is carried as a STRING because that is what a lab sends and what the
    assignment's field list specifies. Parsing it back is a real cleaning step --
    see ward/clinical/lab_rules.py.
    """

    name: str
    unit: str
    reference_range: str
    normal_low: float
    normal_high: float
    decimals: int = 1


PANEL: Final[tuple[TestSpec, ...]] = (
    TestSpec("lactate", "mmol/L", "0.5-2.2", 0.6, 2.0),
    TestSpec("wbc", "10^9/L", "4.0-11.0", 4.5, 10.5),
    TestSpec("crp", "mg/L", "<5.0", 0.5, 4.5),
    TestSpec("creatinine", "umol/L", "60-110", 65, 105, decimals=0),
    TestSpec("haemoglobin", "g/L", "115-165", 118, 160, decimals=0),
    TestSpec("potassium", "mmol/L", "3.5-5.3", 3.6, 5.1),
)

# P014's day-2 panel. These are what make the business question answerable: the labs
# CORROBORATE the vitals deterioration, so the composite risk rises above what vitals
# alone would give. Random abnormal values would contradict the vitals as often as
# they agreed, and the "labs change the risk picture" claim would be incoherent.
SEPSIS_PANEL: Final[dict[str, float]] = {
    "lactate": 3.8,  # ref 0.5-2.2  -- tissue hypoperfusion
    "wbc": 18.4,  # ref 4.0-11.0 -- marked leucocytosis
    "crp": 142.0,  # ref <5.0     -- major inflammatory response
    "creatinine": 148,  # ref 60-110   -- acute kidney injury
}
SEPSIS_PATIENT: Final[str] = "P014"
SEPSIS_LAB_DAY: Final[int] = 2


class LabUploader:
    def __init__(self, clock: SimClock, inbox: Path = INBOX) -> None:
        self.clock = clock
        self.inbox = inbox
        self.tmp = inbox / ".tmp"
        cfg = settings.ward()
        self.rng = Random(cfg.random_seed ^ 0x1AB)
        self.patients = build_ward(cfg.beds, clock.epoch_sim, cfg.random_seed)
        self._running = True

    # ------------------------------------------------------------- generate

    def _result(self, patient: Patient, spec: TestSpec, sim_day: int) -> dict:
        if patient.patient_id == SEPSIS_PATIENT and sim_day >= SEPSIS_LAB_DAY:
            value = SEPSIS_PANEL.get(spec.name)
            if value is None:
                value = self.rng.uniform(spec.normal_low, spec.normal_high)
        else:
            value = self.rng.uniform(spec.normal_low, spec.normal_high)

        reference = spec.reference_range
        if (
            sim_day == MALFORMED_LAB_DAY
            and patient.patient_id == MALFORMED_PATIENT
            and spec.name == MALFORMED_TEST
        ):
            # Exactly ONE corrupted range in the whole file, not a wholesale broken
            # patient. That is the harder and more realistic case: the file parses
            # perfectly and only a single row's content is wrong, so the validator
            # has to check content rather than just JSON validity. A file that fails
            # to parse would be caught by accident.
            reference = "4.0--11.0"

        collected = self.clock.sim_now().replace(hour=5, minute=40, second=0, microsecond=0)
        return {
            "patient_id": patient.patient_id,
            "test_type": spec.name,
            "result_value": round(value, spec.decimals),
            "unit": spec.unit,
            "reference_range": reference,
            "collected_at": collected.isoformat(),
        }

    def build_file(self, sim_date: date, sim_day: int) -> dict:
        collected = datetime.combine(sim_date, datetime.min.time()).replace(hour=6)
        return {
            "ward_id": WARD_ID,
            "sim_date": sim_date.isoformat(),
            "collected_at_sim": collected.isoformat() + "Z",
            "submitted_at_real": utcnow().isoformat(),
            "lab_id": LAB_ID,
            "results": [self._result(p, spec, sim_day) for p in self.patients for spec in PANEL],
        }

    # ----------------------------------------------------------------- drop

    def drop(self, payload: dict, sim_date: date) -> Path:
        """Write the file atomically, with a checksum sidecar.

        ★ The rename is what makes this safe. Writing straight to the final path lets
        Airflow's FileSensor fire on a partial file, and the resulting JSON parse
        error is intermittent and timing-dependent. `os.rename` within one filesystem
        is atomic: the sensor sees nothing, or it sees a complete file.

        The checksum goes down FIRST, for the same reason. If the process dies between
        the two renames we would rather have a sidecar with no file (obvious, the DAG
        waits) than a file with no sidecar (the DAG parses unverified data).
        """
        self.tmp.mkdir(parents=True, exist_ok=True)
        self.inbox.mkdir(parents=True, exist_ok=True)

        name = f"labs_{sim_date.isoformat()}.json"
        body = json.dumps(payload, indent=2).encode()
        digest = hashlib.sha256(body).hexdigest()

        tmp_sidecar = self.tmp / f"{name}.sha256"
        tmp_sidecar.write_text(f"{digest}  {name}\n")
        os.rename(tmp_sidecar, self.inbox / f"{name}.sha256")

        tmp_file = self.tmp / name
        tmp_file.write_bytes(body)
        final = self.inbox / name
        os.rename(tmp_file, final)
        return final

    # ------------------------------------------------------------------ run

    def upload_for_day(self, sim_date: date, sim_day: int) -> Path | None:
        if sim_day == MISSING_LAB_DAY:
            # No file at all. The sensor times out, the DAG branches, an alert
            # fires, and the ward carries on being monitored on vitals alone.
            metrics.lab_files_written_total.labels(scenario="missing").inc()
            log.warning(
                "lab_file_withheld",
                sim_date=sim_date.isoformat(),
                sim_day=sim_day,
                detail="simulated missing lab day - the DAG must degrade, not fail",
                stage="ingest",
            )
            return None

        scenario = "normal"
        if self.rng.random() < LATE_PROBABILITY:
            delay = self.rng.uniform(*LATE_DELAY_REAL_SECONDS)
            scenario = "late"
            metrics.lab_file_delay_seconds.observe(delay)
            log.info("lab_file_delayed", delay_real_seconds=round(delay, 1), stage="ingest")
            time.sleep(delay)
        if sim_day == MALFORMED_LAB_DAY:
            scenario = "malformed"

        payload = self.build_file(sim_date, sim_day)
        path = self.drop(payload, sim_date)

        metrics.lab_files_written_total.labels(scenario=scenario).inc()
        log.info(
            "lab_file_written",
            path=str(path),
            sim_date=sim_date.isoformat(),
            results=len(payload["results"]),
            scenario=scenario,
            stage="ingest",
        )
        return path

    def run(self) -> None:
        log.info(
            "lab_uploader_starting",
            inbox=str(self.inbox),
            panel=[s.name for s in PANEL],
            missing_day=MISSING_LAB_DAY,
            malformed_day=MALFORMED_LAB_DAY,
            stage="ingest",
        )
        dropped: set[date] = set()
        while self._running:
            sim_now = self.clock.sim_now()
            sim_day = self.clock.sim_day_index() + 1
            sim_date = sim_now.date()

            # Labs are released in the morning of each simulated day. Waiting until
            # 06:00 rather than midnight means the file lands while the ward is
            # already producing vitals for that day, which is what the lab join
            # expects.
            if sim_date not in dropped and sim_now.hour >= 6:
                self.upload_for_day(sim_date, sim_day)
                dropped.add(sim_date)

            time.sleep(1)

    def request_stop(self, signum: int, _frame: FrameType | None) -> None:
        log.info("shutdown_requested", signal=signal.Signals(signum).name, stage="ingest")
        self._running = False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Simulated pathology lab uploader")
    parser.add_argument("--once", action="store_true", help="drop one file and exit")
    parser.add_argument("--sim-date", default=None, help="explicit date for --once")
    parser.add_argument("--inbox", default=None, help="override the inbox directory")
    args = parser.parse_args(argv)

    obs = settings.observability()
    configure(service="lab-uploader", stage="ingest", level=obs.log_level, json_output=obs.log_json)

    clock = read_anchor(Path(settings.sim().state_path))
    inbox = Path(args.inbox) if args.inbox else INBOX
    uploader = LabUploader(clock, inbox=inbox)

    if args.once:
        target = date.fromisoformat(args.sim_date) if args.sim_date else clock.sim_date()
        day = (target - clock.epoch_sim.date()).days + 1
        uploader.upload_for_day(target, day)
        return 0

    signal.signal(signal.SIGTERM, uploader.request_stop)
    signal.signal(signal.SIGINT, uploader.request_stop)
    metrics.serve(obs.metrics_port)
    uploader.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())

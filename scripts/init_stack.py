"""One-shot stack initialisation: topics, schemas, and the shared clock anchor.

Runs once at start-up and exits. Everything after it READS the anchor; nobody computes
a simulated epoch locally, because two services disagreeing about what day it is would
corrupt every window, trend and daily report downstream, silently.

THE GUARD. This refuses to run over an existing simulation. Re-anchoring a clock
mid-run would make simulated time jump backwards, and Spark's watermarks would drop
every subsequent reading as impossibly late - windows empty, nothing errors, hours
lost. `RESUME=1` is the documented way past it, and compose passes the variable
through, because a guard advertising a flag that cannot reach the container is worse
than no guard.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from ward import settings
from ward.obs.log import configure, get_logger
from ward.simclock import anchor_exists, read_anchor, write_anchor

log = get_logger()


def main() -> int:
    obs = settings.observability()
    configure(service="init", stage="ingest", level=obs.log_level, json_output=obs.log_json)

    sim = settings.sim()
    state_path = Path(sim.state_path)
    resume = os.environ.get("RESUME", "").strip().lower() in {"1", "true", "yes"}

    if anchor_exists(state_path) and not resume:
        clock = read_anchor(state_path)
        print(
            "ERROR: existing simulation data found.\n"
            f"       anchor at {state_path}, started {clock.epoch_wall.isoformat()},\n"
            f"       currently at simulated day {clock.sim_day_index()} "
            f"({clock.sim_date().isoformat()}).\n\n"
            "Starting a new run over old data produces incoherent dates and trends,\n"
            "and nothing will report an error.\n\n"
            "  make clean && make up     start fresh (recommended)\n"
            "  RESUME=1 make up          continue the previous run, reusing its clock\n",
            file=sys.stderr,
        )
        return 2

    import create_topics
    import init_cassandra
    import register_schemas

    if (rc := create_topics.main()) != 0:
        return rc
    if (rc := register_schemas.main()) != 0:
        return rc
    if (rc := init_cassandra.main()) != 0:
        return rc

    if not anchor_exists(state_path):
        clock = write_anchor(sim.epoch_sim, sim.day_seconds, state_path)
        log.info(
            "clock_anchored",
            epoch_sim=clock.epoch_sim.isoformat(),
            day_seconds=clock.day_seconds,
            speedup=clock.speedup,
            stage="ingest",
        )
    else:
        clock = read_anchor(state_path)
        log.info("clock_resumed", sim_day=clock.sim_day_index(), stage="ingest")

    log.info("init_complete", stage="ingest")
    return 0


if __name__ == "__main__":
    sys.exit(main())

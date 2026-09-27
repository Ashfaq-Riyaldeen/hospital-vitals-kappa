"""Clinical audit and version comparison engine for Kappa stream reprocessing.

Compares patient trajectories computed under two scorer versions (e.g. v1 and v2)
coexisting within the same Cassandra partition.

Answers the clinical governance question:
"Under the new rule, which of our patients would have been flagged differently?
Did we miss anyone? Are we about to stop alerting on someone we were alerting on yesterday?"

Expected result, known before the replay runs:
- Patients with copd_scale2 == True: some NEWS2 scores change, and only DOWNWARD
  (Scale 2 stops penalising their usual 88-92 % saturation).
- Every other patient: zero changes. This is the control group.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ward.obs.log import configure, get_logger
from ward.store.dao import RiskScoreRow, WardStoreDAO
from ward.store.session import create_cluster, get_session

log = get_logger(component="comparator")

URGENT_NEWS2 = 5
HISTORY_LIMIT = 20_000


@dataclass
class PatientDiffSummary:
    patient_id: str
    is_copd: bool
    total_evaluations: int
    changed_evaluations: int
    identical_evaluations: int
    mean_delta: float
    max_delta: int
    tier_transitions: dict[str, int] = field(default_factory=dict)
    alerts_v1: int = 0
    alerts_v2: int = 0

    @property
    def changed_pct(self) -> float:
        if self.total_evaluations == 0:
            return 0.0
        return round((self.changed_evaluations / self.total_evaluations) * 100.0, 2)


@dataclass
class ReplayComparisonReport:
    base_version: str
    target_version: str
    generated_at: str
    total_evaluations: int
    total_changed: int
    total_identical: int
    percent_changed: float
    copd_patients_count: int
    non_copd_patients_count: int
    copd_evaluations_changed: int
    non_copd_evaluations_changed: int
    control_group_bit_identical: bool
    tier_transition_matrix: dict[str, int]
    estimated_false_alarms_suppressed: int
    new_alarms_raised: int
    upward_changes: int = 0
    patient_summaries: list[PatientDiffSummary] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_markdown(self) -> str:
        lines = [
            f"# Clinical Scorer Audit Diff: {self.base_version} vs {self.target_version}",
            "",
            f"**Generated At**: {self.generated_at}  ",
            f"**Total Patient Evaluations Re-derived**: {self.total_evaluations:,}  ",
            f"**Changed Evaluations**: {self.total_changed:,} ({self.percent_changed}%)  ",
            (
                f"**Identical Evaluations**: {self.total_identical:,} "
                f"({100.0 - self.percent_changed:.2f}%)  "
            ),
            "",
            "## 1. Core Kappa Invariant & Control Group Verification",
            "",
            f"- **COPD Patients (copd_scale2 = True)**: {self.copd_patients_count} patients  ",
            (
                f"  - Re-evaluations Changed: {self.copd_evaluations_changed:,} "
                f"(upward changes anywhere: {self.upward_changes})  "
            ),
            (
                f"- **Non-COPD Control Group (copd_scale2 = False)**: "
                f"{self.non_copd_patients_count} patients  "
            ),
            f"  - Re-evaluations Changed: {self.non_copd_evaluations_changed}  ",
            (
                f"  - **Bit-Identical Control Proof**: "
                f"{'PASSED (Zero difference)' if self.control_group_bit_identical else 'FAILED'}  "
            ),
            "",
            "## 2. Clinical Risk Tier Transition Matrix",
            "",
            "| Transition (v1 -> v2) | Patient-Evaluations Count | Clinical Interpretation |",
            "|---|---|---|",
        ]
        for trans, count in sorted(self.tier_transition_matrix.items()):
            before, after = trans.split(" -> ")
            interp = "unchanged" if before == after else "band changed"
            lines.append(f"| `{trans}` | {count:,} | {interp} |")

        lines.extend(
            [
                "",
                "## 3. Alarm Impact",
                "",
                (
                    f"- **Scores that drop below NEWS2 5 under v2**: "
                    f"{self.estimated_false_alarms_suppressed}  "
                ),
                f"- **Scores that rise to NEWS2 5 or more under v2**: {self.new_alarms_raised}  ",
                "",
                "## 4. Patient Detail Summary",
                "",
                "| Patient ID | COPD Scale 2 | Evals | Changed | Change % | Mean Delta "
                "| Alerts v1 | Alerts v2 |",
                "|---|---|---|---|---|---|---|---|",
            ]
        )

        for p in self.patient_summaries:
            lines.append(
                f"| `{p.patient_id}` | {p.is_copd} | {p.total_evaluations} | "
                f"{p.changed_evaluations} | {p.changed_pct}% | {p.mean_delta:+.2f} | "
                f"{p.alerts_v1} | {p.alerts_v2} |"
            )

        return "\n".join(lines)


def evaluate_version_differences(
    trajectories_v1: list[RiskScoreRow],
    trajectories_v2: list[RiskScoreRow],
    copd_patient_ids: set[str],
    alerts_v1: dict[str, int] | None = None,
    alerts_v2: dict[str, int] | None = None,
) -> ReplayComparisonReport:
    """Compare the two versions' NEWS2 scores, reading by reading.

    The comparison is on NEWS2, the scorer that changed. Composite risk also carries
    the lab contribution, and a replay can legitimately see fewer historical labs
    than the live stream did (the labs topic is compacted to the latest result), so
    comparing composites would blame the scorer for a difference in inputs.
    """
    map_v1 = {(r.patient_id, r.scored_at): r for r in trajectories_v1}
    map_v2 = {(r.patient_id, r.scored_at): r for r in trajectories_v2}

    patient_evals: dict[str, list[Any]] = defaultdict(list)
    for p_id, s_at in set(map_v1) & set(map_v2):
        patient_evals[p_id].append(s_at)

    total_evals = total_changed = total_identical = 0
    copd_changed = non_copd_changed = upward = 0
    false_alarms_suppressed = new_alarms = 0
    tier_matrix: dict[str, int] = defaultdict(int)
    patient_summaries: list[PatientDiffSummary] = []

    for p_id in sorted(patient_evals):
        is_copd = p_id in copd_patient_ids
        times = sorted(patient_evals[p_id])
        deltas: list[int] = []
        p_changed = 0
        p_tier_trans: dict[str, int] = defaultdict(int)

        for t in times:
            r1, r2 = map_v1[(p_id, t)], map_v2[(p_id, t)]
            total_evals += 1
            delta = r2.news2_total - r1.news2_total
            deltas.append(delta)
            transition = f"{r1.clinical_risk} -> {r2.clinical_risk}"
            tier_matrix[transition] += 1
            p_tier_trans[transition] += 1

            # NEWS2 5 is the national threshold for an urgent clinical review.
            if r1.news2_total >= URGENT_NEWS2 > r2.news2_total:
                false_alarms_suppressed += 1
            if r2.news2_total >= URGENT_NEWS2 > r1.news2_total:
                new_alarms += 1

            if delta != 0:
                p_changed += 1
                total_changed += 1
                upward += delta > 0
                if is_copd:
                    copd_changed += 1
                else:
                    non_copd_changed += 1
            else:
                total_identical += 1

        patient_summaries.append(
            PatientDiffSummary(
                patient_id=p_id,
                is_copd=is_copd,
                total_evaluations=len(times),
                changed_evaluations=p_changed,
                identical_evaluations=len(times) - p_changed,
                mean_delta=round(sum(deltas) / len(times), 2) if times else 0.0,
                max_delta=max(deltas, key=abs) if deltas else 0,
                tier_transitions=dict(p_tier_trans),
                alerts_v1=(alerts_v1 or {}).get(p_id, 0),
                alerts_v2=(alerts_v2 or {}).get(p_id, 0),
            )
        )

    compared = set(patient_evals)
    return ReplayComparisonReport(
        base_version=trajectories_v1[0].scorer_version if trajectories_v1 else "v1",
        target_version=trajectories_v2[0].scorer_version if trajectories_v2 else "v2",
        generated_at=datetime.now(UTC).isoformat(),
        total_evaluations=total_evals,
        total_changed=total_changed,
        total_identical=total_identical,
        percent_changed=round(total_changed / total_evals * 100, 2) if total_evals else 0.0,
        copd_patients_count=len(compared & copd_patient_ids),
        non_copd_patients_count=len(compared - copd_patient_ids),
        copd_evaluations_changed=copd_changed,
        non_copd_evaluations_changed=non_copd_changed,
        control_group_bit_identical=non_copd_changed == 0,
        tier_transition_matrix=dict(tier_matrix),
        estimated_false_alarms_suppressed=false_alarms_suppressed,
        new_alarms_raised=new_alarms,
        upward_changes=upward,
        patient_summaries=patient_summaries,
    )


def load_copd_patient_ids(timeout_seconds: float = 30.0) -> set[str]:
    """Which patients use NEWS2 Scale 2, read from the admissions topic itself.

    The first version hard-coded six ids, and five of them were wrong for the ward
    the simulator actually builds - so the "control group" contained COPD patients.
    """
    from ward import settings
    from ward.stream.pipeline import ReferenceData

    ref = ReferenceData(settings.kafka(), "diff")
    ref.load_until_caught_up(timeout_seconds)
    ref.close()
    return {pid for pid, adm in ref.admissions.items() if adm.copd_scale2}


def extract_ward_differences(
    dao: WardStoreDAO,
    base_version: str = "v1",
    target_version: str = "v2",
    copd_patient_ids: set[str] | None = None,
    patient_ids: list[str] | None = None,
    ward_id: str = "WARD-A",
) -> ReplayComparisonReport:
    """Fetch stored scores and alerts for both versions and compare them."""
    copd = copd_patient_ids if copd_patient_ids is not None else load_copd_patient_ids()
    p_ids = patient_ids or [f"P{i:03d}" for i in range(1, 41)]

    trajectories_v1: list[RiskScoreRow] = []
    trajectories_v2: list[RiskScoreRow] = []
    for pid in p_ids:
        trajectories_v1.extend(
            dao.get_patient_risk_history(pid, scorer_version=base_version, limit=HISTORY_LIMIT)
        )
        trajectories_v2.extend(
            dao.get_patient_risk_history(pid, scorer_version=target_version, limit=HISTORY_LIMIT)
        )

    days = sorted({r.scored_at.date() for r in trajectories_v1 + trajectories_v2})
    alerts: dict[str, dict[str, int]] = {base_version: {}, target_version: {}}
    for version, counts in alerts.items():
        for day in days:
            for a in dao.get_ward_alerts(ward_id, day, limit=10_000, scorer_version=version):
                counts[a.patient_id] = counts.get(a.patient_id, 0) + 1

    return evaluate_version_differences(
        trajectories_v1=trajectories_v1,
        trajectories_v2=trajectories_v2,
        copd_patient_ids=copd,
        alerts_v1=alerts[base_version],
        alerts_v2=alerts[target_version],
    )


def generate_diff_artifacts(report: ReplayComparisonReport, output_dir: str = "reports") -> Path:
    """Save report as markdown and JSON artifacts for governance review."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    md_path = out / f"replay_diff_{report.base_version}_vs_{report.target_version}.md"
    json_path = out / f"replay_diff_{report.base_version}_vs_{report.target_version}.json"

    md_path.write_text(report.to_markdown())
    json_path.write_text(json.dumps(report.to_dict(), indent=2))

    log.info("replay_diff_artifacts_saved", md_report=str(md_path), json_report=str(json_path))
    return md_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Clinical Version Comparison Diff Engine")
    parser.add_argument("--v1", default="v1", help="Base version")
    parser.add_argument("--v2", default="v2", help="Target version")
    parser.add_argument("--out-dir", default="reports", help="Output directory for reports")
    args = parser.parse_args()

    configure(service="ward-replay-diff", stage="process")
    try:
        cluster = create_cluster()
        session = get_session("ward", cluster=cluster)
        dao = WardStoreDAO(session)

        report = extract_ward_differences(dao, base_version=args.v1, target_version=args.v2)
        md_file = generate_diff_artifacts(report, output_dir=args.out_dir)

        print("\n" + "=" * 60)
        print(f"REPLAY CLINICAL AUDIT: {report.base_version} vs {report.target_version}")
        print("=" * 60)
        print(f"Evaluations Re-derived : {report.total_evaluations:,}")
        print(f"Total Changed          : {report.total_changed:,} ({report.percent_changed}%)")
        print(f"COPD Patients Changed  : {report.copd_evaluations_changed:,}")
        print(
            f"Non-COPD Changes       : {report.non_copd_evaluations_changed} "
            f"(Control Invariant: {report.control_group_bit_identical})"
        )
        print(f"Upward changes         : {report.upward_changes}")
        print(f"Below NEWS2 5 under v2 : {report.estimated_false_alarms_suppressed}")
        print(f"Report artifact written to: {md_file}")
        print("=" * 60 + "\n")
    except Exception as exc:
        print(f"Error computing version diff: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

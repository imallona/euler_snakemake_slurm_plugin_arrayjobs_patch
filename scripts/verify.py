"""Turn probe records into a summary table and a verdict.

Reports whether an array was formed, whether each task ran its own job, and how
many sbatch calls the run cost. The snakemake log carries none of the three: a
rule selected for array submission is still sent job by job until enough of its
jobs are ready at once, and a mismatched dispatch surfaces only as a
MissingOutputException naming an unrelated wildcard.

Runs as the workflow's summarise rule, and by hand on a run that failed part
way:

    python3 scripts/verify.py --probes results/array/probe
"""

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from probe import specs_in_dispatch_payload, tasks_in_specs  # noqa: E402


def collect(paths):
    """Read every probe record named by a list of files or directories."""
    files = []
    for path in paths:
        path = Path(path)
        if path.is_dir():
            files.extend(sorted(path.glob("*.json")))
        elif path.exists():
            files.append(path)
    records = []
    for file in files:
        with open(file, encoding="utf-8") as handle:
            record = json.load(handle)
        record["_file"] = str(file)
        records.append(record)
    return sorted(records, key=lambda record: record["task"])


def task_from_filename(record):
    match = re.search(r"task_(\d+)\.json$", record["_file"])
    return match.group(1) if match else ""


def wrapper_target(record):
    """The tasks the job's wrapper was told to build, and where that was read.

    The batch script is authoritative: srun launches its step under a separate
    slurmstepd, so the wrapper is not in the probe's process ancestry. The
    outermost ancestry link is the fallback for a record written before
    scontrol was consulted, or outside Slurm.
    """
    script = record.get("batch_script") or {}
    tasks = script.get("target_tasks") or []
    if tasks:
        return tasks, "batch_script"
    text = script.get("text")
    if text:
        # A patched plugin encodes the target, so the script holds no readable
        # one. Decoding here as well as in probe.py lets records written before
        # probe.py could decode be checked without another run.
        task_id = (record.get("slurm") or {}).get("SLURM_ARRAY_TASK_ID")
        decoded = tasks_in_specs(specs_in_dispatch_payload(text, task_id))
        if decoded:
            return decoded, "batch_script"
        return [], "script_no_target"

    links = record.get("snakemake_links") or []
    if links:
        outer = max(links, key=lambda link: link["depth"])
        if outer["target_tasks"]:
            return outer["target_tasks"], "ancestry"
    return [], ""


def analyse(records):
    # Several records sharing one Slurm job id means a group job. Its batch
    # script names one member and snakemake rebuilds the rest from
    # --local-groupid, so the wrapper naming another member is expected and the
    # dispatch check does not apply. An array gives each task its own job id.
    shared = Counter(
        (record.get("slurm") or {}).get("SLURM_JOB_ID")
        for record in records
        if (record.get("slurm") or {}).get("SLURM_JOB_ID")
    )

    rows = []
    for record in records:
        slurm = record.get("slurm") or {}
        wrapper_tasks, source = wrapper_target(record)
        job_id = slurm.get("SLURM_JOB_ID") or ""
        if job_id and shared[job_id] > 1 and not slurm.get("SLURM_ARRAY_TASK_ID"):
            wrapper_tasks, source = [], "group"
        rows.append(
            {
                "task": record["task"],
                "file_task": task_from_filename(record),
                "host": record.get("host", ""),
                "seconds": round(record.get("finished", 0) - record.get("started", 0), 1),
                "job_id": job_id,
                "array_job_id": slurm.get("SLURM_ARRAY_JOB_ID") or "",
                "array_task_id": slurm.get("SLURM_ARRAY_TASK_ID") or "",
                "wrapper_tasks": ",".join(wrapper_tasks) or "",
                "wrapper_source": source,
                "dispatch_ok": (not wrapper_tasks) or wrapper_tasks == [record["task"]],
                "tmpdir": slurm.get("TMPDIR") or "",
            }
        )
    return rows


def verdict_lines(rows, expected=0):
    lines = []
    total = len(rows)
    if total == 0:
        return ["No probe records found. The probe jobs never started."]

    if expected and total < expected:
        lines.append(
            f"MISSING RECORDS: {expected - total} of {expected}. A job that "
            "wrote nothing and a file not yet visible on the shared filesystem "
            "look the same here, so check the driver log for "
            "MissingOutputException before reading this as lost work."
        )
        lines.append("")

    arrayed = [row for row in rows if row["array_task_id"]]
    allocations = len({row["job_id"] for row in rows if row["job_id"]})
    submissions = len({row["array_job_id"] or row["job_id"] for row in rows if row["job_id"]})

    lines.append(f"probe records            {total}")
    lines.append(f"ran as an array task     {len(arrayed)}")
    lines.append(f"distinct sbatch calls    {submissions}")
    lines.append(f"distinct Slurm job ids   {allocations}")
    lines.append(f"distinct hosts           {len({row['host'] for row in rows})}")
    lines.append("")

    if allocations == 0:
        lines.append(
            "No task saw a Slurm job id, so this was not a cluster run. The "
            "dispatch checks below still apply, but the submission counts do not."
        )
    elif not arrayed:
        lines.append(
            "No task saw SLURM_ARRAY_TASK_ID, so no array was formed. With "
            "--slurm-array-jobs set, run with DEBUG=1 and read the plugin's "
            "'Array job collection incomplete' lines in the driver log."
        )
    elif len(arrayed) < total:
        lines.append(
            f"Only {len(arrayed)} of {total} tasks ran inside an array. The rest "
            "were submitted one by one. The plugin does that with a rule that "
            "has a single pending job left."
        )

    mismatched = [row for row in rows if not row["dispatch_ok"]]
    if mismatched:
        lines.append("")
        lines.append(
            f"DISPATCH MISMATCH on {len(mismatched)} of {total} tasks. The "
            "wrapper was told to build a different task's output than the one "
            "that ran here. When the inner command returns it checks its own "
            "target, waits out latency-wait, and fails if the task holding "
            "that target has not finished. This is the array dispatch defect "
            "in snakemake-executor-plugin-slurm 2.7.1 and 2.8.0."
        )
        for row in mismatched[:MISMATCH_LINES]:
            lines.append(
                f"  task {row['task']} (array task {row['array_task_id']}) ran under "
                f"a wrapper targeting task {row['wrapper_tasks']}"
            )
        if len(mismatched) > MISMATCH_LINES:
            targets = sorted({row["wrapper_tasks"] for row in mismatched})
            lines.append(
                f"  and {len(mismatched) - MISMATCH_LINES} more, targeting "
                f"{', '.join(targets)}"
            )
    elif arrayed:
        lines.append("")
        unchecked = [row for row in arrayed if row["wrapper_source"] != "batch_script"]
        if unchecked:
            # srun hides the wrapper from the ancestry, so every task reads as
            # correct whether it is or not.
            lines.append(
                f"{len(unchecked)} of {len(arrayed)} array tasks were not "
                "checked, so this run says nothing about their dispatch."
            )
            if any(row["wrapper_source"] == "script_no_target" for row in unchecked):
                lines.append(
                    "Their batch script names no target. A patched plugin "
                    "encodes it, so probe.py has to decode the payload."
                )
            else:
                lines.append(
                    "They read no batch script. Check that scontrol is on PATH "
                    "in the job and answering."
                )
        else:
            lines.append(
                "Every array task ran under a wrapper targeting its own job. "
                "Array dispatch is correct in this run."
            )

    wrong_file = [row for row in rows if row["file_task"] and row["file_task"] != row["task"]]
    if wrong_file:
        lines.append("")
        lines.append(f"CONTENT MISMATCH: {len(wrong_file)} records name a task other")
        lines.append("than the one in their filename. An output was written by the")
        lines.append("wrong job.")

    duplicated = [
        f"{array_job}_{task_id}"
        for (array_job, task_id), count in Counter(
            (row["array_job_id"], row["array_task_id"]) for row in arrayed
        ).items()
        if count > 1
    ]
    if duplicated:
        lines.append("")
        lines.append("DUPLICATE array task ids: " + ", ".join(sorted(duplicated)))

    grouped = [row for row in rows if row["wrapper_source"] == "group"]
    if grouped:
        lines.append("")
        lines.append(
            f"{len(grouped)} of {total} tasks shared an allocation with another "
            "task, so they ran as group jobs. A group's batch script names one "
            "member, and the dispatch check does not apply to them."
        )

    if submissions:
        per_submission = defaultdict(int)
        for row in rows:
            if row["job_id"]:
                per_submission[row["array_job_id"] or row["job_id"]] += 1
        lines.append("")
        lines.append(
            f"{total} probe jobs cost {submissions} sbatch calls "
            f"({min(per_submission.values())} to {max(per_submission.values())} "
            "jobs each)."
        )
    return lines


# Enough to see the pattern. The summary table holds every row.
MISMATCH_LINES = 5

COLUMNS = (
    "task",
    "array_task_id",
    "job_id",
    "array_job_id",
    "host",
    "seconds",
    "wrapper_tasks",
    "wrapper_source",
    "dispatch_ok",
    "tmpdir",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probes", nargs="+", required=True, help="files or directories")
    parser.add_argument("--summary", type=Path)
    parser.add_argument("--verdict", type=Path)
    parser.add_argument(
        "--expect",
        type=int,
        default=0,
        help="how many records there should be; a shortfall is reported rather "
        "than silently shrinking the verdict",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit non-zero when a mismatch is found; off by default so the "
        "workflow's own summarise rule still completes",
    )
    args = parser.parse_args()

    rows = analyse(collect(args.probes))
    lines = verdict_lines(rows, expected=args.expect)

    table = ["\t".join(COLUMNS)]
    table.extend("\t".join(str(row[column]) for column in COLUMNS) for row in rows)

    if args.summary:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text("\n".join(table) + "\n", encoding="utf-8")
    if args.verdict:
        args.verdict.parent.mkdir(parents=True, exist_ok=True)
        args.verdict.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("\n".join(table))
    print()
    print("\n".join(lines))

    if args.strict and any(not row["dispatch_ok"] for row in rows):
        sys.exit(1)


if __name__ == "__main__":
    main()

"""Turn probe records into a summary table and a verdict.

Answers three questions that the snakemake log alone does not:

  Was an array actually used?  A rule can be selected for array submission and
  still be sent job by job, because the plugin only forms an array once enough
  jobs of the rule are ready at the same time.

  Did each array task run its own job?  In snakemake-executor-plugin-slurm
  2.7.1 and 2.8.0 every task of a chunk is launched with the same wrapped
  command, the one belonging to the chunk's first job. Each probe record names
  the target of every snakemake process above it, so a mismatch between the
  outermost target and the task's own wildcard is visible directly.

  How many Slurm submissions did the run cost?  This is the number the array,
  or a group job, is supposed to reduce.

Usable both as the workflow's summarise rule and by hand on a run that failed
part way, which is the normal case here:

    python3 scripts/verify.py --probes results/array/probe
"""

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path


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


def outermost_link(record):
    links = record.get("snakemake_links") or []
    return max(links, key=lambda link: link["depth"]) if links else None


def analyse(records):
    rows = []
    for record in records:
        slurm = record.get("slurm") or {}
        outer = outermost_link(record)
        outer_tasks = outer["target_tasks"] if outer else []
        rows.append(
            {
                "task": record["task"],
                "file_task": task_from_filename(record),
                "host": record.get("host", ""),
                "seconds": round(record.get("finished", 0) - record.get("started", 0), 1),
                "job_id": slurm.get("SLURM_JOB_ID") or "",
                "array_job_id": slurm.get("SLURM_ARRAY_JOB_ID") or "",
                "array_task_id": slurm.get("SLURM_ARRAY_TASK_ID") or "",
                "snakemake_depth": len(record.get("snakemake_links") or []),
                "outer_target_tasks": ",".join(outer_tasks) or "",
                "dispatch_ok": (not outer_tasks) or outer_tasks == [record["task"]],
                "tmpdir": slurm.get("TMPDIR") or "",
            }
        )
    return rows


def verdict_lines(rows):
    lines = []
    total = len(rows)
    if total == 0:
        return ["No probe records found. The probe jobs never started."]

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
            "No task saw SLURM_ARRAY_TASK_ID. Either array submission was off, or "
            "the plugin never gathered enough ready jobs of the rule at one time "
            "to form an array. Run with DEBUG=1 and look for the plugin's "
            "'Array job collection incomplete' lines in the driver log."
        )
    elif len(arrayed) < total:
        lines.append(
            f"Only {len(arrayed)} of {total} tasks ran inside an array. The rest "
            "were submitted one by one, which is what the plugin does with a rule "
            "that has a single pending job left."
        )

    mismatched = [row for row in rows if not row["dispatch_ok"]]
    if mismatched:
        lines.append("")
        lines.append(
            f"DISPATCH MISMATCH on {len(mismatched)} of {total} tasks. The "
            "outermost snakemake process was told to build a different task's "
            "output than the one that actually ran here. That outer process "
            "checks its own target when the inner command returns, does not find "
            "it, and raises MissingOutputException naming a task this job never "
            "had. This is the array dispatch defect in "
            "snakemake-executor-plugin-slurm 2.7.1 and 2.8.0."
        )
        for row in mismatched:
            lines.append(
                f"  task {row['task']} (array task {row['array_task_id']}) ran under "
                f"a wrapper targeting task {row['outer_target_tasks']}"
            )
    elif arrayed:
        lines.append("")
        lines.append(
            "Every array task ran under a wrapper targeting its own job. Array "
            "dispatch is correct in this run."
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


COLUMNS = (
    "task",
    "array_task_id",
    "job_id",
    "array_job_id",
    "host",
    "seconds",
    "snakemake_depth",
    "outer_target_tasks",
    "dispatch_ok",
    "tmpdir",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probes", nargs="+", required=True, help="files or directories")
    parser.add_argument("--summary", type=Path)
    parser.add_argument("--verdict", type=Path)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit non-zero when a mismatch is found; off by default so the "
        "workflow's own summarise rule still completes",
    )
    args = parser.parse_args()

    rows = analyse(collect(args.probes))
    lines = verdict_lines(rows)

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

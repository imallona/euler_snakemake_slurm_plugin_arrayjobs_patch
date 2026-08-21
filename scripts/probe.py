"""Record where a probe job ran and which process chain launched it.

The process ancestry is the point. A Slurm array task submitted by
snakemake-executor-plugin-slurm runs a chain of snakemake processes, and every
link carries its own --target-jobs. If the outermost link names a task other
than this one, the array dispatch is mismatched: the inner process makes this
task's output while the outer one goes on to check a different task's output.
That is what verify.py looks for, and reading the raw record by hand shows it
just as clearly.
"""

import argparse
import json
import os
import re
import socket
import time
from pathlib import Path

# Everything Slurm says about this task's place in an array, plus the scratch
# path, because an inherited TMPDIR is the other thing that goes wrong here.
SLURM_VARS = (
    "SLURM_JOB_ID",
    "SLURM_JOB_NAME",
    "SLURM_ARRAY_JOB_ID",
    "SLURM_ARRAY_TASK_ID",
    "SLURM_ARRAY_TASK_MIN",
    "SLURM_ARRAY_TASK_MAX",
    "SLURM_ARRAY_TASK_COUNT",
    "SLURM_CPUS_ON_NODE",
    "SLURM_SUBMIT_DIR",
    "SLURM_JOB_PARTITION",
    "TMPDIR",
)

MAX_ANCESTRY_DEPTH = 16


def read_argv(pid):
    with open(f"/proc/{pid}/cmdline", "rb") as handle:
        raw = handle.read().decode("utf-8", "replace")
    return [part for part in raw.split("\0") if part]


def read_ppid(pid):
    with open(f"/proc/{pid}/stat", encoding="utf-8") as handle:
        # The command name is in parentheses and may contain spaces, so split
        # after the closing one. ppid is then the second field.
        return int(handle.read().rsplit(") ", 1)[1].split()[1])


def ancestry():
    """This process and its ancestors, innermost first."""
    chain = []
    pid = os.getpid()
    for depth in range(MAX_ANCESTRY_DEPTH):
        try:
            argv = read_argv(pid)
            ppid = read_ppid(pid)
        except (OSError, IndexError, ValueError):
            break
        chain.append({"depth": depth, "pid": pid, "argv": argv})
        if pid == 1 or ppid in (0, pid):
            break
        pid = ppid
    return chain


def target_job_specs(argv):
    """The --target-jobs values of one command line.

    Snakemake writes them as RULE:WILDCARD=VALUE, one argument per job, either
    after a bare --target-jobs or attached with an equals sign.
    """
    specs = []
    for index, arg in enumerate(argv):
        if arg == "--target-jobs":
            for candidate in argv[index + 1 :]:
                if candidate.startswith("-"):
                    break
                specs.append(candidate)
        elif arg.startswith("--target-jobs="):
            specs.append(arg.split("=", 1)[1])
    return specs


def tasks_in_specs(specs):
    """The task wildcard values named by a list of target job specs."""
    return sorted({match for spec in specs for match in re.findall(r"task=(\d+)", spec)})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", required=True, help="the task wildcard of this job")
    parser.add_argument("--sleep", type=float, default=0.0)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    started = time.time()
    chain = ancestry()

    # Only the links that are snakemake processes carry a target. Keeping the
    # depth makes "outermost" well defined for verify.py.
    snakemake_links = []
    for link in chain:
        specs = target_job_specs(link["argv"])
        if not specs:
            continue
        snakemake_links.append(
            {
                "depth": link["depth"],
                "pid": link["pid"],
                "target_job_specs": specs,
                "target_tasks": tasks_in_specs(specs),
            }
        )

    if args.sleep > 0:
        time.sleep(args.sleep)

    record = {
        "task": args.task,
        "output": str(args.out),
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "started": started,
        "finished": time.time(),
        "slurm": {name: os.environ.get(name) for name in SLURM_VARS},
        "snakemake_links": snakemake_links,
        "ancestry": [
            {"depth": link["depth"], "pid": link["pid"], "command": " ".join(link["argv"])}
            for link in chain
        ],
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(record, handle, indent=2, sort_keys=True)
        handle.write("\n")


if __name__ == "__main__":
    main()

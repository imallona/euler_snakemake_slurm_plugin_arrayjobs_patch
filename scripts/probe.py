"""Record where a probe job ran and which command the batch script carried.

A Slurm array task submitted by snakemake-executor-plugin-slurm runs a chain of
snakemake processes, each with its own --target-jobs. A batch script naming a
task other than this one means the wrapper builds this task's output and then
checks a different task's.

The wrapper is not in this process's ancestry: srun launches its step under a
separate slurmstepd, so walking /proc stops one snakemake short of it. The
batch script comes from scontrol instead, and the ancestry is kept for the part
of the chain that is visible.
"""

import argparse
import json
import os
import re
import socket
import subprocess
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

# Every task of an array asks the controller at the same moment.
SCONTROL_ATTEMPTS = 4
SCONTROL_RETRY_SECONDS = 3


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


def batch_script():
    """The batch script of this job, as the controller holds it, and any error.

    For an array task this is the chunk's script, shared by every task, so its
    --target-jobs is what the wrapper was told to build.

    The controller answers over RPC and refuses under load, so the error is
    recorded rather than swallowed: on 2026-08-21 nine of twelve probes asking
    at once came back empty, and an empty script is indistinguishable from a
    correct dispatch.
    """
    job_id = os.environ.get("SLURM_JOB_ID")
    if not job_id:
        return "", "not a Slurm job"
    for attempt in range(SCONTROL_ATTEMPTS):
        try:
            result = subprocess.run(
                ["scontrol", "write", "batch_script", job_id, "-"],
                capture_output=True,
                text=True,
                timeout=30,
            )
        except FileNotFoundError:
            return "", "scontrol not on PATH"
        except (OSError, subprocess.SubprocessError) as error:
            return "", f"{type(error).__name__}: {error}"
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout, ""
        if attempt + 1 < SCONTROL_ATTEMPTS:
            time.sleep(SCONTROL_RETRY_SECONDS * (attempt + 1))
    return "", (result.stderr or "").strip() or f"scontrol exited {result.returncode}"


def specs_in_text(text):
    """The --target-jobs values in a shell script or command string.

    shlex would choke on the script's own quoting, so the specs are matched
    directly. Snakemake writes them as RULE:WILDCARD=VALUE and quotes them when
    they contain a comma.
    """
    specs = []
    for match in re.finditer(r"--target-jobs[= ]+((?:'[^']*'|\"[^\"]*\"|[^\s'\"]+))", text):
        specs.append(match.group(1).strip("'\""))
    return specs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", required=True, help="the task wildcard of this job")
    parser.add_argument("--sleep", type=float, default=0.0)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    started = time.time()
    chain = ancestry()

    # Only the links that are snakemake processes carry a target. Keeping the
    # depth orders them from this process outwards.
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

    script, script_error = batch_script()
    script_specs = specs_in_text(script)

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
        "batch_script": {
            "target_job_specs": script_specs,
            "target_tasks": tasks_in_specs(script_specs),
            "error": script_error,
            "text": script,
        },
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

"""Offline checks. Nothing here needs a cluster.

The two that matter are the verify tests: they feed synthetic probe records
shaped like a mismatched array dispatch and like a correct one, so the
detection is exercised without submitting anything.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import verify  # noqa: E402
import patch_slurm_plugin  # noqa: E402


def write_record(directory, task, array_task_id, outer_task):
    """One probe record, as probe.py would write it.

    outer_task is the task named by the outermost snakemake process. It equals
    task when the dispatch is correct and the chunk's first task when it is not.
    """
    record = {
        "task": task,
        "output": f"probe/task_{task}.json",
        "host": "eu-a2p-001",
        "pid": 1,
        "started": 0.0,
        "finished": 1.0,
        "slurm": {
            "SLURM_JOB_ID": f"1000{array_task_id}",
            "SLURM_ARRAY_JOB_ID": "1000",
            "SLURM_ARRAY_TASK_ID": str(array_task_id),
            "SLURM_ARRAY_TASK_MIN": "1",
            "TMPDIR": "/scratch/1000",
        },
        "snakemake_links": [
            {
                "depth": 1,
                "pid": 3,
                "target_job_specs": [f"probe:task={task}"],
                "target_tasks": [task],
            },
            {
                "depth": 2,
                "pid": 2,
                "target_job_specs": [f"probe:task={outer_task}"],
                "target_tasks": [outer_task],
            },
        ],
        "ancestry": [],
    }
    path = directory / f"task_{task}.json"
    path.write_text(json.dumps(record), encoding="utf-8")
    return path


def test_mismatched_dispatch_is_reported(tmp_path):
    # Three tasks of one chunk, all wrapped in the first task's command. This is
    # what the unpatched plugin produces.
    for index, task in enumerate(["001", "002", "003"], start=1):
        write_record(tmp_path, task, index, outer_task="001")

    rows = verify.analyse(verify.collect([tmp_path]))
    text = "\n".join(verify.verdict_lines(rows))

    assert sum(not row["dispatch_ok"] for row in rows) == 2
    assert "DISPATCH MISMATCH on 2 of 3 tasks" in text
    assert "3 probe jobs cost 1 sbatch calls" in text


def test_correct_dispatch_is_accepted(tmp_path):
    for index, task in enumerate(["001", "002", "003"], start=1):
        write_record(tmp_path, task, index, outer_task=task)

    rows = verify.analyse(verify.collect([tmp_path]))
    text = "\n".join(verify.verdict_lines(rows))

    assert all(row["dispatch_ok"] for row in rows)
    assert "Array dispatch is correct" in text
    assert "MISMATCH" not in text


def test_partial_run_still_verifies(tmp_path):
    # The array mode fails part way, so verify has to work on what exists.
    write_record(tmp_path, "001", 1, outer_task="001")
    rows = verify.analyse(verify.collect([tmp_path]))
    assert len(rows) == 1
    assert "probe records            1" in "\n".join(verify.verdict_lines(rows))


def test_no_records_is_stated_plainly(tmp_path):
    assert "never started" in "\n".join(verify.verdict_lines(verify.analyse([])))


def test_probe_parses_its_own_target_jobs():
    import probe

    argv = ["snakemake", "--target-jobs", "probe:task=007", "--allowed-rules", "probe"]
    assert probe.target_job_specs(argv) == ["probe:task=007"]
    assert probe.tasks_in_specs(probe.target_job_specs(argv)) == ["007"]
    assert probe.target_job_specs(["snakemake", "--target-jobs=probe:task=009"]) == [
        "probe:task=009"
    ]


def test_patch_anchors_match_the_installed_plugin():
    """The patch refuses to guess, so its anchors must be exact."""
    try:
        path = patch_slurm_plugin.plugin_path()
    except SystemExit:
        pytest.skip("snakemake-executor-plugin-slurm is not installed here")

    import importlib.metadata

    version = importlib.metadata.version("snakemake-executor-plugin-slurm")
    if version not in patch_slurm_plugin.SUPPORTED_VERSIONS:
        pytest.skip(f"plugin {version} is outside the versions the patch targets")

    text = path.read_text(encoding="utf-8")
    if patch_slurm_plugin.SENTINEL in text:
        pytest.skip("plugin is already patched")
    assert text.count(patch_slurm_plugin.OLD_PAYLOAD_RANGE) == 1
    assert text.count(patch_slurm_plugin.OLD_SUBMISSION_BRANCH) == 1


def test_profile_declares_what_the_run_relies_on():
    profile = yaml.safe_load((ROOT / "profiles" / "euler" / "config.yaml").read_text())
    assert profile["executor"] == "slurm"
    assert profile["default-resources"]["slurm_account"] == "es_platt"
    # Per core, because Euler documents --mem-per-cpu and not --mem.
    assert "mem_mb_per_cpu" in profile["default-resources"]
    assert "mem_mb" not in profile["default-resources"]
    # A retry would resubmit the failed tasks as plain jobs and hide the defect.
    assert profile["retries"] == 0
    # Both needed to read the per job logs afterwards.
    assert profile["slurm-keep-successful-logs"] is True
    assert profile["slurm-logdir"] == "slurm/snakemake_logs"
    # An array needs more ready jobs than the default config asks for.
    config = yaml.safe_load((ROOT / "config" / "config.yaml").read_text())
    assert profile["jobs"] >= config["n_tasks"]
    assert config["n_tasks"] >= 2


def test_workflow_builds_the_expected_dag(tmp_path):
    if subprocess.run(["which", "snakemake"], capture_output=True).returncode != 0:
        pytest.skip("snakemake is not on PATH")
    result = subprocess.run(
        [
            "snakemake",
            "--snakefile",
            str(ROOT / "workflow" / "Snakefile"),
            "--directory",
            str(ROOT),
            "--dry-run",
            "--config",
            f"outdir={tmp_path}",
            "n_tasks=5",
            "sleep_seconds=1",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "probe" in result.stdout
    assert "summarise" in result.stdout

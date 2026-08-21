"""Offline checks. Nothing here needs a cluster.

The verify tests feed synthetic probe records shaped like a mismatched array
dispatch and like a correct one, so the detection runs without submitting
anything.
"""

import base64
import json
import os
import subprocess
import sys
import zlib
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import verify  # noqa: E402
import patch_slurm_plugin  # noqa: E402


def write_record(directory, task, array_task_id, outer_task):
    """One probe record, as probe.py would write it.

    outer_task is the task the batch script names. It equals task when the
    dispatch is correct and the chunk's first task when it is not.
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
        ],
        "batch_script": {
            "target_job_specs": [f"probe:task={outer_task}"],
            "target_tasks": [outer_task],
            "text": f"#!/bin/sh\nsnakemake --target-jobs 'probe:task={outer_task}'\n",
        },
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


def test_no_records(tmp_path):
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
    """The patch matches on exact text, so its anchors must still be present."""
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


def test_profile_settings():
    profile = yaml.safe_load((ROOT / "profiles" / "euler" / "config.yaml").read_text())
    assert profile["executor"] == "slurm"
    assert profile["default-resources"]["slurm_account"] == "es_platt"
    # Per core, because Euler documents --mem-per-cpu and not --mem.
    assert "mem_mb_per_cpu" in profile["default-resources"]
    assert "mem_mb" not in profile["default-resources"]
    # A retry resubmits the failed tasks, and the plugin sends the last one plain.
    assert profile["retries"] == 0
    # Both needed to read the per job logs afterwards.
    assert profile["slurm-keep-successful-logs"] is True
    assert profile["slurm-logdir"] == "slurm/snakemake_logs"
    # An array is formed only from jobs ready at the same moment.
    config = yaml.safe_load((ROOT / "config" / "config.yaml").read_text())
    assert profile["jobs"] >= config["n_tasks"]
    assert config["n_tasks"] >= 2


def test_dag_job_counts(tmp_path):
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


def test_wrapper_target_falls_back_to_ancestry():
    """A record with no batch script still reports the outermost link."""
    record = {
        "task": "004",
        "snakemake_links": [
            {"depth": 1, "pid": 3, "target_job_specs": [], "target_tasks": ["004"]},
            {"depth": 3, "pid": 1, "target_job_specs": [], "target_tasks": ["001"]},
        ],
    }
    assert verify.wrapper_target(record) == (["001"], "ancestry")
    assert verify.wrapper_target({"task": "004"}) == ([], "")


def test_probe_reads_target_jobs_out_of_a_batch_script():
    import probe

    script = (
        "#!/bin/sh\n"
        "/usr/bin/python -m snakemake --snakefile 'workflow/Snakefile' "
        "--target-jobs 'probe:task=001' --allowed-rules probe "
        "--executor slurm-jobstep --slurm-jobstep-array-execs=eyJ9\n"
    )
    assert probe.specs_in_text(script) == ["probe:task=001"]
    assert probe.tasks_in_specs(probe.specs_in_text(script)) == ["001"]
    assert probe.specs_in_text("#!/bin/sh\necho nothing\n") == []


def test_ancestry_only_array_run_is_inconclusive(tmp_path):
    """srun hides the wrapper, so an ancestry-only record proves nothing."""
    for index, task in enumerate(["001", "002"], start=1):
        path = write_record(tmp_path, task, index, outer_task=task)
        record = json.loads(path.read_text())
        del record["batch_script"]
        path.write_text(json.dumps(record))

    rows = verify.analyse(verify.collect([tmp_path]))
    text = "\n".join(verify.verdict_lines(rows))
    assert all(row["wrapper_source"] == "ancestry" for row in rows)
    assert "2 of 2 array tasks were not checked" in text
    assert "read no batch script" in text
    assert "dispatch is correct" not in text


def test_group_members_are_not_flagged(tmp_path):
    """Four tasks in one allocation is a group job, not a mismatched array.

    A group's batch script names one member; snakemake rebuilds the rest from
    --local-groupid and checks all their outputs.
    """
    for index, task in enumerate(["001", "007", "011", "012"], start=1):
        path = write_record(tmp_path, task, index, outer_task="007")
        record = json.loads(path.read_text())
        record["slurm"]["SLURM_JOB_ID"] = "11392044"
        record["slurm"]["SLURM_ARRAY_TASK_ID"] = None
        record["slurm"]["SLURM_ARRAY_JOB_ID"] = None
        path.write_text(json.dumps(record))

    rows = verify.analyse(verify.collect([tmp_path]))
    text = "\n".join(verify.verdict_lines(rows))

    assert all(row["dispatch_ok"] for row in rows)
    assert all(row["wrapper_source"] == "group" for row in rows)
    assert "MISMATCH" not in text
    assert "4 of 4 tasks shared an allocation" in text


def test_array_tasks_are_still_checked(tmp_path):
    """Array tasks each get their own job id, so the group rule must not hide them."""
    for index, task in enumerate(["001", "002", "003"], start=1):
        write_record(tmp_path, task, index, outer_task="001")

    rows = verify.analyse(verify.collect([tmp_path]))
    assert {row["wrapper_source"] for row in rows} == {"batch_script"}
    assert sum(not row["dispatch_ok"] for row in rows) == 2


def test_replace_atomically_breaks_hardlinks(tmp_path):
    """conda hardlinks site-packages into its package cache.

    An in-place write reaches the cache, and every environment later built
    from it. A rename gives the file its own inode instead.
    """
    installed = tmp_path / "__init__.py"
    installed.write_text("original\n", encoding="utf-8")
    cached = tmp_path / "cache.py"
    os.link(installed, cached)
    assert installed.stat().st_nlink == 2

    patch_slurm_plugin.replace_atomically(installed, "patched\n")

    assert installed.read_text() == "patched\n"
    assert cached.read_text() == "original\n"
    assert installed.stat().st_nlink == 1


def build_dispatch_script(commands):
    """A batch script shaped like the one patch_slurm_plugin.py submits."""
    payload = base64.b64encode(
        json.dumps(
            {task: zlib.compress(command.encode(), 9).hex() for task, command in commands.items()}
        ).encode()
    ).decode()
    return f"#!/bin/sh\nset -e\nsnakemake_call=$(python -c 'decode' {payload})\neval \"$snakemake_call\"\n"


def test_probe_decodes_a_patched_dispatch_script():
    """The patch encodes the target, so plain matching finds nothing."""
    import probe

    script = build_dispatch_script(
        {
            "1": "snakemake --target-jobs 'probe:task=001' --executor slurm-jobstep",
            "7": "snakemake --target-jobs 'probe:task=007' --executor slurm-jobstep",
        }
    )
    assert probe.specs_in_text(script) == []
    assert probe.specs_in_dispatch_payload(script, "7") == ["probe:task=007"]
    assert probe.tasks_in_specs(probe.specs_in_dispatch_payload(script, "1")) == ["001"]
    assert probe.specs_in_dispatch_payload(script, "99") == []
    assert probe.specs_in_dispatch_payload("#!/bin/sh\ntrue\n", "1") == []


def test_unreadable_target_is_not_reported_as_correct(tmp_path):
    """A script read but not understood must not pass as a correct dispatch."""
    for index, task in enumerate(["001", "002"], start=1):
        path = write_record(tmp_path, task, index, outer_task=task)
        record = json.loads(path.read_text())
        record["batch_script"] = {"target_job_specs": [], "target_tasks": [], "text": "#!/bin/sh\n"}
        path.write_text(json.dumps(record))

    rows = verify.analyse(verify.collect([tmp_path]))
    text = "\n".join(verify.verdict_lines(rows))
    assert all(row["wrapper_source"] == "script_no_target" for row in rows)
    assert "2 of 2 array tasks were not checked" in text
    assert "has to decode the payload" in text
    assert "dispatch is correct" not in text


def test_verify_decodes_a_record_probe_could_not(tmp_path):
    """Records written before probe.py could decode are still checkable."""
    for task_id, task in ((1, "001"), (7, "007")):
        path = write_record(tmp_path, task, task_id, outer_task=task)
        record = json.loads(path.read_text())
        record["slurm"]["SLURM_ARRAY_TASK_ID"] = str(task_id)
        record["batch_script"] = {
            "target_job_specs": [],
            "target_tasks": [],
            "text": build_dispatch_script(
                {
                    "1": "snakemake --target-jobs 'probe:task=001'",
                    "7": "snakemake --target-jobs 'probe:task=009'",
                }
            ),
        }
        path.write_text(json.dumps(record))

    rows = verify.analyse(verify.collect([tmp_path]))
    by_task = {row["task"]: row for row in rows}
    assert by_task["001"]["wrapper_source"] == "batch_script"
    assert by_task["001"]["dispatch_ok"] is True
    # Task 7's payload entry names 009, so the decode has to catch it.
    assert by_task["007"]["wrapper_tasks"] == "009"
    assert by_task["007"]["dispatch_ok"] is False

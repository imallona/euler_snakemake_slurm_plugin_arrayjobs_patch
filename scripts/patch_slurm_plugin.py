"""Make each Slurm array task run its own snakemake command.

snakemake-executor-plugin-slurm 2.7.1 and 2.8.0 submit an array chunk with the
wrapped command of its first job and substitute the per task command one
process deeper, leaving the wrapper checking the first job's output in every
task. See docs/adr/0001-slurm-array-jobs.md. This rewrites the submission so
the batch script decodes the command for $SLURM_ARRAY_TASK_ID itself, using the
payload the plugin already builds.

    python3 scripts/patch_slurm_plugin.py --status
    python3 scripts/patch_slurm_plugin.py --apply
    python3 scripts/patch_slurm_plugin.py --revert

This edits the installed plugin inside the active conda environment. The
original is kept beside it as __init__.py.orig and --revert restores it.
Reinstalling or updating the plugin discards the patch.

The OLD_ constants below are verbatim excerpts of snakemake-executor-plugin-slurm
2.7.1 and 2.8.0, which is MIT licensed, quoted so the edit matches on exact text.
They remain under their original terms.
"""

import argparse
import importlib.metadata
import importlib.util
import os
import shutil
from pathlib import Path

SUPPORTED_VERSIONS = ("2.7.1", "2.8.0")
SENTINEL = "# platt_arrayjobs: per task array dispatch"

# The payload must carry the chunk's first task too, since the batch script now
# looks up every task including that one.
OLD_PAYLOAD_RANGE = """                sub_array_execs = {
                    str(i): array_execs[i]
                    for i in range(start_index + 1, end_index + 1)
                }
"""
NEW_PAYLOAD_RANGE = """                sub_array_execs = {
                    str(i): array_execs[i]
                    for i in range(start_index, end_index + 1)
                }
"""

# Replaces the branch that either wraps the first job's command or writes it
# into a script. Both forms carried the wrong target for every task but the
# first.
OLD_SUBMISSION_BRANCH = '''                    if not use_script_submission:
                        # Use --wrap for the base execution command.
                        call_with_array += (
                            f' --wrap="{exec_job}'
                            f" --slurm-jobstep-array-execs="
                            f"{shlex.quote(array_execs_payload)}"
                            '"'
                        )
                        subprocess_stdin = None
                        self.logger.debug(f"call with array: {call_with_array}")
                    else:
                        # Use /dev/stdin to pass the base execution command as a script.
                        sbatch_script = "\\n".join(
                            [
                                "#!/bin/sh",
                                f"{exec_job}",
                                "--slurm-jobstep-array-execs",
                                shlex.quote(array_execs_payload),
                            ]
                        )
                        call_with_array += " /dev/stdin"
                        subprocess_stdin = sbatch_script
'''
NEW_SUBMISSION_BRANCH = f'''                    {SENTINEL}
                    # The batch script picks the command for its own task, so
                    # no snakemake process in the task carries another job's
                    # target. use_script_submission and exec_job are left in
                    # place but no longer decide anything here.
                    subprocess_stdin = _array_dispatch_script(
                        self.get_python_executable(), array_execs_payload
                    )
                    call_with_array += " /dev/stdin"
                    self.logger.debug(f"call with array: {{call_with_array}}")
'''

HELPER = f'''

{SENTINEL}
def _array_dispatch_script(python_executable: str, payload: str) -> str:
    """Batch script running the command that belongs to this array task.

    The payload is the mapping the plugin already builds: array task id to a
    zlib compressed, hex encoded snakemake command. Decoding it in the batch
    script rather than one snakemake process deeper is the only change.
    """
    decoder = (
        "import base64, json, os, sys, zlib\\n"
        "mapping = json.loads(base64.b64decode(sys.argv[1]))\\n"
        "task = os.environ['SLURM_ARRAY_TASK_ID']\\n"
        "sys.stdout.write(zlib.decompress(bytes.fromhex(mapping[task])).decode())\\n"
    )
    lookup = (
        f"{{shlex.quote(python_executable)}} -c {{shlex.quote(decoder)}} "
        f"{{shlex.quote(payload)}}"
    )
    return "\\n".join(
        [
            "#!/bin/sh",
            "set -e",
            f"snakemake_call=$({{lookup}})",
            'eval "$snakemake_call"',
        ]
    )
'''


def plugin_path():
    spec = importlib.util.find_spec("snakemake_executor_plugin_slurm")
    if spec is None or not spec.origin:
        raise SystemExit(
            "snakemake_executor_plugin_slurm is not importable. Activate the "
            "snakemake environment first."
        )
    return Path(spec.origin)


def backup_path(path):
    return path.with_name(path.name + ".orig")


def replace_atomically(path, text):
    """Put text at path without the file ever being visible half written.

    Snakemake imports this module at startup whatever executor is selected, so
    every job of every workflow in the environment reads it. A truncate and
    rewrite leaves a window in which a starting job sees a partial file and
    dies with an ImportError. Writing a sibling and renaming has no such
    window, since rename within a directory is atomic.

    The rename also gives the file a new inode. conda hardlinks site-packages
    into its package cache, so an in-place write reaches the cache and every
    other environment built from it.
    """
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    shutil.copystat(path, temporary)
    os.replace(temporary, path)


def report_status(path, version):
    text = path.read_text(encoding="utf-8")
    links = path.stat().st_nlink
    print(f"plugin   {path}")
    print(f"version  {version}")
    print(f"patched  {'yes' if SENTINEL in text else 'no'}")
    print(f"backup   {'present' if backup_path(path).exists() else 'absent'}")
    print(f"links    {links}")
    if links > 1:
        # Applying breaks the link, so this only shows up on a file patched by
        # an earlier version of this script, or on a pristine install.
        print(
            "         shared with conda's package cache. If the file is also "
            "patched, so is the cache."
        )


def apply_patch(path, version):
    text = path.read_text(encoding="utf-8")
    if SENTINEL in text:
        print("Already patched, nothing to do.")
        return
    if version not in SUPPORTED_VERSIONS:
        raise SystemExit(
            f"Plugin version {version} is not one this patch was written "
            f"against ({', '.join(SUPPORTED_VERSIONS)}). Read the array "
            "submission code before forcing it."
        )
    for old in (OLD_PAYLOAD_RANGE, OLD_SUBMISSION_BRANCH):
        if text.count(old) != 1:
            raise SystemExit(
                "The plugin source does not contain an expected block exactly "
                "once. Refusing to edit it."
            )
    backup = backup_path(path)
    if not backup.exists():
        shutil.copy2(path, backup)
    text = text.replace(OLD_PAYLOAD_RANGE, NEW_PAYLOAD_RANGE)
    text = text.replace(OLD_SUBMISSION_BRANCH, NEW_SUBMISSION_BRANCH)
    replace_atomically(path, text + HELPER)
    print(f"Patched {path}, original kept at {backup}.")


def revert_patch(path):
    backup = backup_path(path)
    if not backup.exists():
        raise SystemExit(f"No backup at {backup}, nothing to restore.")
    replace_atomically(path, backup.read_text(encoding="utf-8"))
    print(f"Restored {path} from {backup}.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--status", action="store_true")
    group.add_argument("--apply", action="store_true")
    group.add_argument("--revert", action="store_true")
    args = parser.parse_args()

    path = plugin_path()

    if args.status:
        report_status(path, importlib.metadata.version("snakemake-executor-plugin-slurm"))
        return

    if args.apply:
        apply_patch(path, importlib.metadata.version("snakemake-executor-plugin-slurm"))
    else:
        revert_patch(path)

    # Stale bytecode would keep the previous code alive.
    shutil.rmtree(path.parent / "__pycache__", ignore_errors=True)


if __name__ == "__main__":
    main()

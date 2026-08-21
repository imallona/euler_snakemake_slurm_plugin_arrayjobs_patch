# platt_arrayjobs

A small snakemake workflow whose only product is evidence about how Slurm array
jobs behave on ETH Euler, plus a patch that makes them work.

The short answer to why array jobs fail: with
snakemake-executor-plugin-slurm 2.7.1 or 2.8.0, every task of an array chunk is
submitted with the same wrapped command, the one belonging to the chunk's first
job. The per task command is substituted one process deeper, so the task does
build the right output, but the wrapping snakemake still holds the first job's
target and fails with `MissingOutputException` naming a job that task never
ran. Euler itself is fine: `MaxArraySize` is 15000 and there is no per user
submit limit on `es_platt`. The reading of the plugin source is in
[docs/adr/0001-slurm-array-jobs.md](docs/adr/0001-slurm-array-jobs.md).

## What is here

The workflow runs `n_tasks` independent probe jobs. Each is one core and a
sleep, and each writes a JSON record of where it ran: hostname, the `SLURM_*`
array variables, and the command line of every process above it read from
`/proc`. That last part is the diagnosis. Each snakemake process in the chain
carries its own `--target-jobs`, so a task launched under another task's
wrapper says so in its own record, before any exception is raised.

`scripts/verify.py` reads those records and writes a summary table and a
verdict. It reads a partial set too, which is what you need when the run failed
half way, and that is the expected outcome of the array mode on an unpatched
plugin.

Three modes, same twelve jobs:

| mode | flags | what it is |
| --- | --- | --- |
| `plain` | none | one sbatch per job, the control |
| `array` | `--slurm-array-jobs=probe` | the thing under test |
| `group` | `--groups probe=probe_batch --group-components probe_batch=4` | several jobs per Slurm job, works today |

## Running it

Activate the snakemake environment on the login node. `sbatch` exports the
submitting shell, so the drivers inherit it and nothing else needs setting up.

```
source /cluster/project/platt/$USER/miniforge3/etc/profile.d/conda.sh
conda activate /cluster/project/platt/$USER/envs/snakemake
cd /cluster/project/platt/$USER/src/platt_arrayjobs

sbatch slurm/00_probe.sh      # versions, cluster limits, the three plans
sbatch slurm/01_compare.sh    # runs all three modes and reports
```

`00_probe.sh` submits no child jobs. `01_compare.sh` does, and it does not stop
when a mode fails, because a failing mode is the measurement. Read its log:

```
tail -200 slurm/logs/arrayjobs-compare-<jobid>.out
cat results/array/verdict.txt
column -t results/array/summary.tsv
```

Arguments after the script name are passed to make, and `MODES` selects which
modes run:

```
sbatch slurm/01_compare.sh MODES="array" N=40 SLEEP=60
```

Everything works from a login node too, for a quick check without a driver job:

```
make versions
make dry MODE=array
make verify MODE=array
bash scripts/diagnose.sh
```

`DEBUG=1` is the default and adds `--verbose`. The plugin announces its array
decisions only at debug level, and those lines, `Array job collection
incomplete`, `Submitting array-selected jobs`, `Submitting array job with
sbatch call`, are usually the first thing to read when no array was formed.

## What a run should say

On an unpatched plugin, `results/array/verdict.txt` reports a dispatch mismatch
on every task but the first, and names the task each wrapper was actually
targeting. `results/plain/verdict.txt` reports no mismatch and one sbatch call
per job. `results/group/verdict.txt` reports no mismatch and four jobs per
sbatch call.

If the array mode reports "No task saw SLURM_ARRAY_TASK_ID", no array was ever
formed. The plugin builds one only from jobs of a single rule that are ready at
the same moment, so `jobs` in the profile has to be at least `n_tasks`; it is
64 against 12 by default.

## The patch

`scripts/patch_slurm_plugin.py` replaces the array wrapper with a batch script
that decodes the command for its own `$SLURM_ARRAY_TASK_ID` and runs it. The
task then contains one snakemake process holding its own target, and the
jobstep executor's ordinary path puts it under `srun`. It edits the installed
plugin inside the active conda environment, keeps the original beside it as
`__init__.py.orig`, refuses to touch a version it was not written against
(2.7.1 and 2.8.0), and reverts.

```
make patch-status
make patch
sbatch slurm/01_compare.sh MODES="array"
make unpatch      # if it does not do what it should
```

The patch has been exercised offline against both supported versions: it
applies, the result parses, the generated batch script returns the right
command for each task and propagates the inner exit status, and `--revert`
restores the file byte for byte. It has not been run on Euler. That is what
`01_compare.sh` is for, and `results/array/verdict.txt` is the answer.

Any reinstall of the plugin discards the patch without saying so, which is why
`make versions` prints the patch state and the drivers print it before doing
anything.

## Whether to use arrays at all

For a workflow of tens of long jobs, no; the per job overhead is noise. For
many short ones it is worth it. `sacct` for the twelve hours to 2026-08-21
shows 1546 one core jobs of nineteen to sixty-two seconds under a single
fastder simulation run, and two rules are 1440 of them: `eval_fuzzy_metrics`
with 760 and `run_gffcompare` with 680. Those are the two to name, not `all`.

Until the patch is confirmed on the cluster, `--groups` is the working way to
cut that count: it packs jobs of a rule into one Slurm job, runs them
sequentially inside it, and needs no patched plugin. The `group` mode here
measures exactly that.

## Layout

```
config/config.yaml          n_tasks, sleep_seconds, outdir; the Makefile overrides all three
workflow/Snakefile          probe and summarise
profiles/euler/config.yaml  the Slurm profile, array flags deliberately not in it
scripts/probe.py            one JSON record per job, including process ancestry
scripts/verify.py           records to summary table and verdict
scripts/diagnose.sh         sacct and log evidence for a driver job
scripts/patch_slurm_plugin.py
slurm/00_probe.sh           versions, limits, plans; submits nothing
slurm/01_compare.sh         runs the modes and reports
tests/                      offline, no cluster needed: make test
docs/adr/0001-slurm-array-jobs.md
```

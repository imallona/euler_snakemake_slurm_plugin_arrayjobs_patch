# ADR 0001: Why array jobs fail on Euler, and what this repository does about it

Status: accepted
Date: 2026-08-21
Applies to: snakemake 9.25.1, snakemake-executor-plugin-slurm 2.7.1 and 2.8.0,
snakemake-executor-plugin-slurm-jobstep 0.6.0 and 0.6.1

## Context

Two runs make the case for array jobs. `platt_invivo_cortical_neurons` submits
tens of long jobs, where per job scheduling overhead does not matter. The
fastder simulation does not: `sacct` for the twelve hours to 2026-08-21 shows
1546 jobs under a single snakemake run uuid, each asking for one core and
finishing in nineteen to sixty-two seconds. Each of those is an sbatch call, a
queue entry, a node allocation and a snakemake process chain, for half a minute
of work. That is what an array is for.

Array submission has been off since 2026-08-06, recorded in
`platt_prime_editing_total_rnaseq/docs/adr/0001-euler-software-deployment.md`
as "every array task is launched with the first task's command". This ADR is
the reading of the plugin source that establishes why, and it holds for 2.8.0,
which is the current release.

Euler is not the constraint. `scontrol show config` on eu-login-44 reports
`MaxArraySize = 15000` and `MaxJobCount = 200000`, Slurm is 25.05.5, and
`sacctmgr` shows no per user submit limit on `es_platt`. Arrays are permitted;
the plugin is what does not work.

## What the plugin does

A normal, non-array job goes through three processes:

```
sbatch --wrap "snakemake --target-jobs rule:w=A --executor slurm-jobstep --mode remote"
  -> srun -n1 --cpus-per-task=N snakemake --target-jobs rule:w=A --mode remote
       -> the rule's shell command
```

The outer snakemake delegates to the jobstep executor, which places the middle
one under `srun`, and the middle one has no `--executor` flag so it runs the
command itself. When the innermost returns, each level checks the output of the
job it was given. All three were given job A, so all three agree.

`run_array_jobs` in `snakemake_executor_plugin_slurm/__init__.py` builds a
chunk differently. It compresses one full snakemake command per job into a
mapping from array task id to command, then submits

```python
exec_job = self.format_job_exec(jobs[start_index - 1])   # the chunk's FIRST job
...
call_with_array += f' --wrap="{exec_job} --slurm-jobstep-array-execs=..."'
```

so every task of the array runs the same wrapper, and that wrapper carries
`--target-jobs` for the chunk's first job. The per task command is substituted
one level down, in the jobstep executor:

```python
elif self.job_array_task and self.workflow.executor_settings.array_execs:
    array_index = int(os.getenv("SLURM_ARRAY_TASK_ID"))
    if _is_first_array_task(array_index):
        call = strip_array_execs_option(self.format_job_exec(job))
    else:
        call = _decompress_array_task_call(..., array_index)
```

For array task k, the substitution is correct: the inner command builds job k
and job k's output appears. The wrapper above it was never told about job k. It
reports its own job, the first one, as finished, snakemake postprocesses that
job, does not find its output, and raises `MissingOutputException` naming a
wildcard the task never had. That is the error seen in `star_pass1` and
`fastqc` on 2026-08-06.

There is a second way for the same chunk to fail. If the first job's output
already exists when task k builds its DAG, the wrapper concludes there is
nothing to do and exits without ever running the inner command, so job k's
output is never written and the driver reports it missing instead.

Diffing 2.7.1 against 2.8.0 shows this region unchanged. 2.8.0 adds
`disable_memory_fudge`, `no_requeue`, and metadata emission before submission;
jobstep 0.6.1 adds GPU and node settings and strips inherited `SLURM_*`
variables before the nested `srun`. Neither touches the wrapper's target.

## Decision

Three things, in this repository rather than in a production workflow.

**A reproducer.** `workflow/Snakefile` runs one core and a sleep per task and
writes down its process ancestry, read from `/proc`. The wrapper's
`--target-jobs` is in that record, so a mismatch between it and the task's own
wildcard is a fact in a file rather than an inference from an exception.
`scripts/verify.py` reads the records and states the verdict. It reads a failed
run too, which is the normal case.

**A comparison.** The same jobs run three ways: `plain`, one sbatch per job;
`array`, the thing under test; and `group`, snakemake's own `--groups` and
`--group-components`, which packs several jobs of a rule into one Slurm job.
Group jobs are explicitly excluded from array submission by the plugin and go
through the ordinary path, so they work today. For the fastder case they are
the answer available now: at four jobs per group, 1546 submissions become 387.

**A patch, opt in.** `scripts/patch_slurm_plugin.py` replaces the wrapper with
a batch script that decodes the command for its own `$SLURM_ARRAY_TASK_ID` and
runs it. The task then holds a single snakemake process carrying its own
target, and the jobstep executor's ordinary non-array path puts it under
`srun`. The payload the plugin already builds is what gets decoded; the only
other change is that the chunk's first task is included in it. The patch checks
the plugin version, refuses to edit source it does not recognise, keeps the
original as `__init__.py.orig`, and reverts.

## Alternatives

**Wait for upstream.** The defect is in the current release and the design that
causes it, one wrapper per chunk with substitution one level down, is what the
feature is built on, so a fix is a rewrite of `run_array_jobs` rather than a
patch release. Worth rechecking on every plugin update; `make patch-status`
says whether the local patch is still in place, and the tests say whether its
anchors still match.

**Group jobs instead of arrays.** Cheaper and already working, but not the same
thing. A group runs its members sequentially inside one allocation, so it
serialises what an array would run in parallel, and the allocation has to be
sized for the whole group. It suits many short jobs; it does not suit jobs that
should run at the same time.

**One job per sample, as now.** What the fastder run does. Correct, and the
reason nothing is on fire. It costs Slurm 1546 scheduling decisions for
thirteen hours of work and it fills the queue with entries that are shorter
than the time they wait.

## Consequences

- The patch edits a file inside the conda environment at
  `/cluster/project/platt/$USER/envs/snakemake`. Any reinstall or update of the
  plugin discards it silently, which is why `make versions` reports the patch
  state on every run and the drivers print it before doing anything.
- `retries` is 0 in `profiles/euler/config.yaml`, unlike the production
  profiles. A retry resubmits the failed tasks, and once one is left the plugin
  sends it as a plain job, which succeeds and hides what is being measured.
- The probe rule needs `jobs` in the profile to be at least `n_tasks`.
  The plugin forms an array only from jobs of one rule that are ready at the
  same moment, so a lower `--jobs` produces a run with no array in it and no
  error to explain why.
- The array is formed per rule and per chunk. `--slurm-array-limit` bounds the
  chunk; it defaults to 1000 in the plugin and to 200 here, because the encoded
  commands travel inside the batch script and Slurm caps that at a few MB.

## Checked on the cluster

- eu-login-44, 2026-08-21: `MaxArraySize = 15000`, `MaxJobCount = 200000`,
  Slurm 25.05.5.
- `/cluster/project/platt/$USER/envs/snakemake` holds snakemake 9.25.1, plugin
  2.7.1 and jobstep 0.6.0. `python -c "import snakemake_executor_plugin_slurm"`
  resolves to the `lib/python3.12` copy.
- That environment also has a `lib/python3.1/site-packages` tree holding a
  second copy of both plugins. Nothing imports it, since `python3.1` is not on
  `sys.path` for python 3.12, but it is a trap for anyone reading versions off
  the filesystem instead of off `importlib.metadata`.
- `sacct` for the twelve hours to 2026-08-21 11:00: 1546 jobs under run uuid
  `1e5dfc74-cb86-4539-b5a4-33cba3b9af78`, the fastder simulation driver, one
  core each, nineteen to sixty-two seconds each. Their `Comment` field names
  the rule, and two rules are almost all of it: `eval_fuzzy_metrics` with 760
  jobs and `run_gffcompare` with 680. Those two are what to name in
  `--slurm-array-jobs` or `--group-components` first; the rest of that workflow
  is tens of jobs and would gain nothing.

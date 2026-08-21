# ADR 0001: Slurm array job dispatch

Status: accepted
Date: 2026-08-21
Applies to: snakemake 9.25.1, snakemake-executor-plugin-slurm 2.7.1 and 2.8.0, snakemake-executor-plugin-slurm-jobstep 0.6.0 and 0.6.1

## Context

`sacct` for the twelve hours to 2026-08-21 shows 1546 jobs under a single snakemake run uuid, each asking for one core and finishing in nineteen to sixty-two seconds. Each is an sbatch call, a queue entry, a node allocation and a snakemake process chain.

Array submission has been off since 2026-08-06, recorded in `platt_prime_editing_total_rnaseq/docs/adr/0001-euler-software-deployment.md` as "every array task is launched with the first task's command".

Euler is not the constraint. `scontrol show config` on eu-login-44 reports `MaxArraySize = 15000` and `MaxJobCount = 200000`, Slurm is 25.05.5, and `sacctmgr` shows no per user submit limit on `es_platt`.

## Dispatch

A non-array job runs three processes:

```
sbatch --wrap "snakemake --target-jobs rule:w=A --executor slurm-jobstep --mode remote"
  -> srun -n1 --cpus-per-task=N snakemake --target-jobs rule:w=A --mode remote
       -> the rule's shell command
```

The outer snakemake delegates to the jobstep executor, which places the middle one under `srun`. The middle one has no `--executor` flag, so it runs the command itself. Each level then checks the output of job A.

`run_array_jobs` in `snakemake_executor_plugin_slurm/__init__.py` compresses one snakemake command per job into a mapping from array task id to command, then submits

```python
exec_job = self.format_job_exec(jobs[start_index - 1])   # the chunk's FIRST job
...
call_with_array += f' --wrap="{exec_job} --slurm-jobstep-array-execs=..."'
```

Every task of the array runs that same wrapper, carrying `--target-jobs` for the chunk's first job. The per task command is substituted one level down, in the jobstep executor:

```python
elif self.job_array_task and self.workflow.executor_settings.array_execs:
    array_index = int(os.getenv("SLURM_ARRAY_TASK_ID"))
    if _is_first_array_task(array_index):
        call = strip_array_execs_option(self.format_job_exec(job))
    else:
        call = _decompress_array_task_call(..., array_index)
```

For array task k the inner command builds job k, and job k's output appears. The wrapper above it was never told about job k, so what happens next depends on the first job's output rather than on k's.

Whether that fails is a race, measured on Euler on 2026-08-21 and not settled by reading the source. Two outcomes:

- Every task starts at once. Each runs its own decoded command, the array's minimum task is the one that builds the first job, and by the time the others postprocess they find its output present. All tasks exit 0 and every output is written. Job 11391488 did exactly this.
- A task starts after the first job's output already exists. Its wrapper builds a DAG for a job with nothing to do, exits without running the inner command, and reports success. That task's own output is never written, and the driver raises `MissingOutputException` for it. This is the error seen in `star_pass1` and `fastqc` on 2026-08-06, where 54 libraries did not fit in the share at once.

The first outcome is not the defect being absent. The wrapper still checks the wrong job in every task, so a run passes only while the timing holds, and the same array on a busier cluster does not.

Diffing 2.7.1 against 2.8.0 shows this region unchanged. 2.8.0 adds `disable_memory_fudge`, `no_requeue`, and metadata emission before submission; jobstep 0.6.1 adds GPU and node settings and strips inherited `SLURM_*` variables before the nested `srun`.

## Decision

**A reproducer.** `workflow/Snakefile` runs one core and a sleep per task and records the `--target-jobs` its batch script carries, so a mismatch with the task's own wildcard is a fact in a file rather than an inference from an exception. `scripts/verify.py` reads the records, including those of a run that failed part way.

The batch script comes from `scontrol write batch_script`, not from the process ancestry. `srun` launches its step under a separate `slurmstepd`, so walking `/proc` from inside the rule's shell stops one snakemake short of the wrapper and every task looks correct. The first cluster run, job 11390936, reported "array dispatch is correct" for an array whose wrapper targeted `probe:task=004` in all twelve tasks.

**A comparison.** The same jobs run three ways: `plain`, one sbatch per job; `array`; and `group`, using `--groups` and `--group-components`. The plugin excludes group jobs from array submission and sends them through the ordinary path, so they work unpatched. At four jobs per group, 1546 submissions become 387.

**A patch, opt in.** `scripts/patch_slurm_plugin.py` replaces the wrapper with a batch script that decodes the command for its own `$SLURM_ARRAY_TASK_ID` and runs it. The task then holds a single snakemake process carrying its own target, and the jobstep executor's non-array path puts it under `srun`. The payload the plugin already builds is what gets decoded; the chunk's first task is added to it. The patch checks the plugin version, refuses to edit source it does not recognise, keeps the original as `__init__.py.orig`, and reverts.

## Alternatives

**Wait for upstream.** One wrapper per chunk with substitution one level down is what the feature is built on, so a fix is a rewrite of `run_array_jobs` rather than a patch release. Recheck on every plugin update: `make patch-status` reports whether the local patch is still in place, and the tests whether its anchors still match.

**Group jobs instead of arrays.** A group runs its members sequentially inside one allocation, so it serialises what an array would run in parallel, and the allocation has to be sized for the whole group.

**One job per sample.** Costs Slurm 1546 scheduling decisions for thirteen hours of work, and fills the queue with entries shorter than the time they wait.

## Consequences

- The patch edits a file inside the conda environment at `/cluster/project/platt/$USER/envs/snakemake`. Any reinstall or update of the plugin discards it silently, so `make versions` reports the patch state and the drivers print it before doing anything.
- `retries` is 0 in `profiles/euler/config.yaml`, unlike the production profiles. A retry resubmits the failed tasks, and once one is left the plugin sends it as a plain job, which succeeds.
- `jobs` in the profile has to be at least `n_tasks`. The plugin forms an array only from jobs of one rule that are ready at the same moment, so a lower `--jobs` produces a run with no array in it and no error explaining why.
- `--slurm-array-limit` bounds the chunk. It defaults to 1000 in the plugin and to 200 here, because the encoded commands travel inside the batch script and Slurm caps that at a few MB.

## On the cluster

- eu-login-44, 2026-08-21: `MaxArraySize = 15000`, `MaxJobCount = 200000`, Slurm 25.05.5.
- `/cluster/project/platt/$USER/envs/snakemake` holds snakemake 9.25.1, plugin 2.7.1 and jobstep 0.6.0. `import snakemake_executor_plugin_slurm` resolves to the `lib/python3.12` copy.
- That environment also has a `lib/python3.1/site-packages` tree holding a second copy of both plugins. Nothing imports it, since `python3.1` is not on `sys.path` for python 3.12, but reading versions off the filesystem there gives the wrong answer; use `importlib.metadata`.
- `sacct` for the twelve hours to 2026-08-21 11:00: 1546 jobs under run uuid `1e5dfc74-cb86-4539-b5a4-33cba3b9af78`, the fastder simulation driver, one core each, nineteen to sixty-two seconds each. Their `Comment` field names the rule: `eval_fuzzy_metrics` 760 jobs and `run_gffcompare` 680, out of 1546. Those two are what to name in `--slurm-array-jobs` or `--group-components`; the rest of that workflow is tens of jobs.
- Probe job 11390488, 2026-08-21: `slurm/00_probe.sh` completed in 18 seconds on eu-a2p-480, reporting plugin 2.7.1 unpatched and twelve probe jobs in all three plans.
- Job 11390936, 2026-08-21, plugin 2.7.1 unpatched. The plain mode cost 12 sbatch calls for 12 jobs. The array mode cost one, `sbatch ... --array=1-12`, whose `--wrap` carried `--target-jobs 'probe:task=004'` for every task. All twelve tasks completed with exit 0 in 30 to 41 seconds and all twelve outputs were written, because they started together and the minimum task built job 004 before the others postprocessed.

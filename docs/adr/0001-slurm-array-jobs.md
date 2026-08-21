# ADR 0001: Slurm array job dispatch

Status: accepted
Date: 2026-08-21
Applies to: snakemake 9.25.1, snakemake-executor-plugin-slurm 2.7.1 and 2.8.0, snakemake-executor-plugin-slurm-jobstep 0.6.0 and 0.6.1

## Context

`sacct` for the twelve hours to 2026-08-21 shows 1546 jobs under one snakemake run uuid, each one core, 19 to 62 seconds. Their `Comment` names the rule: `eval_fuzzy_metrics` 760 and `run_gffcompare` 680. Each job is an sbatch call, a queue entry, a node allocation and a snakemake process chain.

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

`--force` is in the spawned job's arguments, so a wrapper whose target already exists rebuilds it. The task reaches the jobstep executor either way, the task's own command is substituted, and the task's own output is written. The wrapper then checks the first job's output.

`latency-wait` is 60 seconds here, and it bounds that check. The run fails when the task holding the first job has not finished within 60 seconds of another task checking for it.

Job 11399743, 60 probes of 20 seconds: 50 tasks started at 13:06:37 and 10 at 13:07:31. All 60 outputs written, driver exit 0, 59 of 60 wrappers targeting `probe:task=006`. `star_pass1` and `fastqc` over 54 libraries of hours failed on 2026-08-06 with `MissingOutputException` naming other samples.

Diffing 2.7.1 against 2.8.0 shows this region unchanged. 2.8.0 adds `disable_memory_fudge`, `no_requeue`, and metadata emission before submission; jobstep 0.6.1 adds GPU and node settings and strips inherited `SLURM_*` variables before the nested `srun`.

## Decision

**A reproducer.** `workflow/Snakefile` runs one core and a sleep per task and records the `--target-jobs` its batch script carries, read with `scontrol write batch_script`. Not the process ancestry: `srun` launches its step under a separate `slurmstepd`, so walking `/proc` stops one snakemake short of the wrapper. Job 11390936 reported "array dispatch is correct" for an array whose wrapper targeted `probe:task=004` in all twelve tasks.

A patched script holds no readable target, so the probe and `verify.py` decode the payload instead.

**A comparison.** `slurm/` is a sequence, one submission per step: 12 jobs three ways, 60 array tasks unpatched, patch, the same 60 patched, unpatch. 02 and 04 share `array_experiment.sh` and refuse if the plugin is in the wrong state.

The three ways are `plain`, one sbatch per job; `array`; and `group`, using `--groups` and `--group-components`. The plugin sends group jobs through the ordinary path, so they work unpatched. At four jobs per group, 1546 submissions become 387.

A group's batch script names one member and snakemake rebuilds the rest from `--local-groupid`, so its wrapper targets another task legitimately. `verify.py` recognises a group by several records sharing one `SLURM_JOB_ID`, which an array never does.

**A patch, opt in.** `scripts/patch_slurm_plugin.py` replaces the wrapper with a batch script that decodes the command for its own `$SLURM_ARRAY_TASK_ID`. The task then holds one snakemake process carrying its own target. It checks the plugin version, keeps `__init__.py.orig`, and reverts. `patches/0001-per-task-array-dispatch.patch` is the same change as a diff, and `docs/upstream.md` the report.

## Alternatives

**Wait for upstream.** One wrapper per chunk with substitution one level down is what the feature is built on, so a fix is a rewrite of `run_array_jobs` rather than a patch release. Recheck on every plugin update: `make patch-status` reports whether the local patch is still in place, and the tests whether its anchors still match.

**Group jobs instead of arrays.** A group runs its members sequentially inside one allocation, so it serialises what an array would run in parallel, and the allocation has to be sized for the whole group.

**One job per sample.** Costs Slurm 1546 scheduling decisions for thirteen hours of work, and fills the queue with entries shorter than the time they wait.

## Consequences

- The patch edits a file in the conda environment. A reinstall discards it, so `make versions` reports the patch state and the drivers print it before running.
- Snakemake imports the Slurm plugin at startup under every executor, `--executor local` included, so every job in the environment reads the patched file. The patch changes only `run_array_jobs`, and the write is a rename.
- conda hardlinks site-packages into its package cache, so an in-place write reaches the cache. `--status` reports the link count.
- `retries` is 0 in `profiles/euler/config.yaml`. A retry resubmits the failed tasks, and the plugin sends the last one as a plain job, which succeeds.
- `jobs` in the profile has to be at least `n_tasks`. An array is formed only from jobs of one rule ready at the same moment; a lower `--jobs` gives a run with no array and no error saying why.
- `--slurm-array-limit` bounds the chunk, 1000 in the plugin and 200 here. The encoded commands travel inside the batch script, which Slurm caps at a few MB.

## On the cluster

eu-login-44, 2026-08-21: `MaxArraySize = 15000`, `MaxJobCount = 200000`, Slurm 25.05.5. `/cluster/project/platt/$USER/envs/snakemake` holds snakemake 9.25.1, plugin 2.7.1, jobstep 0.6.0.

| job | patched | tasks | mode | sbatch calls | wrapper target | outputs | exit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 11390936 | no | 12 | plain | 12 | own | 12 | 0 |
| 11390936 | no | 12 | array | 1 | task 004, 12 of 12 | 12 | 0 |
| 11390936 | no | 12 | group | 3 | own group's member | 12 | 0 |
| 11399743 | no | 60 | array | 1 | task 006, 59 of 60 | 60 | 0 |
| 11395649 | yes | 60 | array | 1 | own, 60 of 60 | 60 | 0 |
| 11400849 | yes | 60 | array | 1 | own, 60 of 60 | 60 | 0 |

In 11399743 the tasks did not start together: 50 at 13:06:37 and 10 at 13:07:31.

That environment also carries a `lib/python3.1/site-packages` tree with a second copy of both plugins. Nothing imports it, but reading versions off the filesystem there gives the wrong answer; use `importlib.metadata`.

Four defects in the harness, found by those runs:

- The ancestry cannot see the wrapper, so 11390936 reported an array with the wrong target as correct. The probe reads the batch script now.
- A group member's wrapper names another member legitimately, and was reported as a mismatch.
- `scontrol write batch_script` answered 3 of 12 probes asking at once. The probe retries and records the error.
- `diagnose.sh` grepped the driver log for `MissingOutputException`, which `verify.py` prints in its own verdict, so it reported its own output as an error.

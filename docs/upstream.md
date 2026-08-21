# Upstream report

For https://github.com/snakemake/snakemake-executor-plugin-slurm. The fix is `patches/0001-per-task-array-dispatch.patch`, against 2.8.0, applied with `patch -p1` from a checkout root. The same two blocks are in 2.7.1.

## Title

Array tasks run under a wrapper holding the chunk's first job's target

## Versions

snakemake 9.25.1, plugin 2.7.1 and 2.8.0, jobstep 0.6.0 and 0.6.1, Slurm 25.05.5.

## Cause

`run_array_jobs` submits a chunk with one wrapped command:

```python
exec_job = self.format_job_exec(jobs[start_index - 1])   # the chunk's first job
...
call_with_array += f' --wrap="{exec_job} --slurm-jobstep-array-execs=..."'
```

Every task runs that wrapper, so every task's outer snakemake carries `--target-jobs` for the chunk's first job. The per task command is substituted one process lower, in the jobstep executor:

```python
elif self.job_array_task and self.workflow.executor_settings.array_execs:
    array_index = int(os.getenv("SLURM_ARRAY_TASK_ID"))
    if _is_first_array_task(array_index):
        call = strip_array_execs_option(self.format_job_exec(job))
    else:
        call = _decompress_array_task_call(..., array_index)
```

Task k builds job k. The wrapper above it postprocesses the first job and checks that job's output.

## When it fails

`--force` is in the spawned job's arguments, so a wrapper whose target already exists rebuilds it and each task's own output is still written. `latency-wait` bounds the check on the first job's output. The run fails when the task holding that job has not finished within `latency-wait` of another task checking for it, and raises `MissingOutputException` naming a wildcard that task never had.

Short, evenly sized jobs stay inside that window. Long or unevenly sized ones do not.

## Reproduction

https://github.com/imallona/platt_arrayjobs submits 60 one core jobs as an array and records, per task, the `--target-jobs` its batch script carries:

| | unpatched | patched |
| --- | --- | --- |
| sbatch calls | 1 | 1 |
| wrapper target | first job, 59 of 60 | own job, 60 of 60 |
| outputs written | 60 | 60 |
| driver exit | 0 | 0 |

Both pass at 20 seconds per job with `latency-wait: 60`. The difference is the wrapper target, so the reproducer reads the batch script, not the exit status.

## Fix

The batch script picks the command for its own `$SLURM_ARRAY_TASK_ID`. The task then holds one snakemake process carrying its own target, and the jobstep executor's non-array path runs it under `srun`. The payload the plugin already builds is what gets decoded; the chunk's first task is added to it, and the `--wrap` and script branches collapse into one submission on `/dev/stdin`.

The array branch in the jobstep executor becomes unreachable and could be removed separately.

Checked: applies to 2.7.1 and 2.8.0, the result imports, the generated batch script returns the right command per task and propagates the inner exit status, and a 60 task array reports its own target in all 60 tasks.

## Out of scope

- `apply_mem_fudge` reassigns `call` inside the chunk loop, so a second chunk is submitted with the first chunk's fudge already added.
- A large array puts a large base64 payload in the batch script, against Slurm's script size limit. `--slurm-array-limit` bounds it; its default of 1000 may already be too high.

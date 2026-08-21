# platt_arrayjobs

A reproducer and a patch for a Slurm array job defect, on ETH Euler.

## Credit

[snakemake-executor-plugin-slurm](https://github.com/snakemake/snakemake-executor-plugin-slurm) and [snakemake-executor-plugin-slurm-jobstep](https://github.com/snakemake/snakemake-executor-plugin-slurm-jobstep) are written and maintained by Christian Meesters, David Laehnemann and Johannes Koester, array support included. This repository reports one defect and patches around it, quoting their code to match and replace it. It belongs upstream as an issue or a pull request.

## Defect

In plugin 2.7.1 and 2.8.0 every task of an array chunk runs the wrapped command of the chunk's first job. Each task still builds its own output, but every wrapper then checks the first job's. The run fails when the task holding that job has not finished within `latency-wait` of another task checking for it. On Euler: 60 tasks of 20 seconds passed, with 59 of 60 wrappers targeting the wrong job; 54 libraries of hours failed. `MaxArraySize` is 15000 there, with no per user submit limit. The source reading and the measurements are in [ADR 0001](docs/adr/0001-slurm-array-jobs.md).

## Use

Activate the snakemake environment on a login node.

```
sbatch slurm/00_probe.sh             # versions, cluster limits, plans; submits nothing
sbatch slurm/01_compare.sh           # 12 jobs plain, as an array, and grouped
sbatch slurm/02_unpatched_array.sh   # 60 array tasks, the failure case
sbatch slurm/03_patch.sh             # apply the patch
sbatch slurm/04_patched_array.sh     # the same 60 tasks, patched
sbatch slurm/05_unpatch.sh           # restore the plugin
```

One at a time, each waiting for the last. 02 and 04 are the same run either side of 03, and each refuses if the plugin is in the wrong state.

Each probe job records its `SLURM_*` variables and the command line of every process above it, from `/proc`. `scripts/verify.py` writes `results/<mode>/verdict.txt`, naming the task each mismatched wrapper targeted. It also reads a run that failed part way. The `group` mode uses `--groups`, which bypasses array submission and works unpatched.

## Patch

`scripts/patch_slurm_plugin.py` makes the batch script decode the command for its own `$SLURM_ARRAY_TASK_ID`. It checks the plugin version, keeps `__init__.py.orig`, and reverts. Run against 2.7.1 and 2.8.0 offline, not on a cluster. `make help` lists the other targets.

```
make patch-status
make patch
make unpatch
```

It edits the environment every snakemake job of the account uses. The write is a rename, so no starting job sees a partial file, and only `run_array_jobs` changes.

## Upstream

[docs/upstream.md](docs/upstream.md) is the report, and `patches/0001-per-task-array-dispatch.patch` the same change as a diff against 2.8.0, applied with `patch -p1` from a checkout root. A test asserts the diff and `scripts/patch_slurm_plugin.py` produce the same file.

## License

MIT, see [LICENSE](LICENSE). The plugin excerpts in `scripts/patch_slurm_plugin.py` stay under their own MIT license.

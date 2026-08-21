# platt_arrayjobs

A reproducer and a patch for a Slurm array job defect, on ETH Euler.

## Credit

[snakemake-executor-plugin-slurm](https://github.com/snakemake/snakemake-executor-plugin-slurm) and [snakemake-executor-plugin-slurm-jobstep](https://github.com/snakemake/snakemake-executor-plugin-slurm-jobstep) are written and maintained by Christian Meesters, David Laehnemann and Johannes Koester, array support included. This repository reports one defect and patches around it, quoting their code to match and replace it. It belongs upstream as an issue or a pull request.

## Defect

In plugin 2.7.1 and 2.8.0 every task of an array chunk runs the wrapped command of the chunk's first job. Each task still builds its own output, but every wrapper then checks the first job's, so the run is correct only while the timing holds: a task that starts after that output exists finds nothing to do, exits 0, and writes nothing, and the driver raises `MissingOutputException` for it. Measured on Euler, one array of twelve simultaneous tasks passed and one of 54 staggered ones did not. Euler allows arrays: `MaxArraySize` is 15000, with no per user submit limit. The source reading and the measurements are in [ADR 0001](docs/adr/0001-slurm-array-jobs.md).

## Use

Activate the snakemake environment on a login node.

```
sbatch slurm/00_probe.sh      # versions, cluster limits, plans; submits nothing
sbatch slurm/01_compare.sh    # the same jobs plain, as an array, and grouped
```

Each probe job records its `SLURM_*` variables and the command line of every process above it, from `/proc`. `scripts/verify.py` writes `results/<mode>/verdict.txt`, naming the task each mismatched wrapper targeted. It also reads a run that failed part way. The `group` mode uses `--groups`, which bypasses array submission and works unpatched.

## Patch

`scripts/patch_slurm_plugin.py` makes the batch script decode the command for its own `$SLURM_ARRAY_TASK_ID`. It checks the plugin version, keeps `__init__.py.orig`, and reverts. Run against 2.7.1 and 2.8.0 offline, not on a cluster. `make help` lists the other targets.

```
make patch-status
make patch
make unpatch
```

## License

MIT, see [LICENSE](LICENSE). The plugin excerpts in `scripts/patch_slurm_plugin.py` stay under their own MIT license.

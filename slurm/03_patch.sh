#!/bin/bash
# Apply the per task array dispatch patch, and record the before and after.
#
#   sbatch slurm/03_patch.sh
#
# Snakemake imports the Slurm executor plugin at startup under every executor,
# so this file is read by every job of every workflow in the environment. The
# write is a rename, so no job can see it half written, and the patch changes
# only run_array_jobs, which a workflow setting no --slurm-array flag never
# calls. slurm/05_unpatch.sh reverses it.
#
#SBATCH --job-name=arrayjobs-patch
#SBATCH --time=00:10:00
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=2000
#SBATCH --output=slurm/logs/%x-%j.out

source slurm/common.sh

echo "=================== before ==================="
python3 scripts/patch_slurm_plugin.py --status

echo
echo "=================== apply ==================="
python3 scripts/patch_slurm_plugin.py --apply

echo
echo "=================== after ==================="
python3 scripts/patch_slurm_plugin.py --status
python3 -c "import snakemake_executor_plugin_slurm; print('the patched module imports')"

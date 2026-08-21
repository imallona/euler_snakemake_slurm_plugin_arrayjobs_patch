#!/bin/bash
# Restore the plugin from the backup slurm/03_patch.sh kept.
#
#   sbatch slurm/05_unpatch.sh
#
#SBATCH --job-name=arrayjobs-unpatch
#SBATCH --time=00:10:00
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=2000
#SBATCH --output=slurm/logs/%x-%j.out

source slurm/common.sh

echo "=================== before ==================="
python3 scripts/patch_slurm_plugin.py --status

echo
echo "=================== revert ==================="
python3 scripts/patch_slurm_plugin.py --revert

echo
echo "=================== after ==================="
python3 scripts/patch_slurm_plugin.py --status
python3 -c "import snakemake_executor_plugin_slurm; print('the restored module imports')"

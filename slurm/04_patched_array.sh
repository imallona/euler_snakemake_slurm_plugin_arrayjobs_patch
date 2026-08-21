#!/bin/bash
# 60 array tasks on a patched plugin, the same run as 02.
#
#   sbatch slurm/03_patch.sh
#   sbatch slurm/04_patched_array.sh
#
#SBATCH --job-name=arrayjobs-patched
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=2000
#SBATCH --output=slurm/logs/%x-%j.out
#SBATCH --signal=B:TERM@300

source slurm/common.sh

if ! python3 scripts/patch_slurm_plugin.py --status | grep -q "patched  yes"; then
    echo
    echo "Not patched, so this would repeat 02. Run slurm/03_patch.sh first."
    exit 1
fi

RESULTS=results/patched
source slurm/array_experiment.sh
exit "$status"

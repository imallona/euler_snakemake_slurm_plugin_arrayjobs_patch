#!/bin/bash
# 60 array tasks on an unpatched plugin.
#
#   sbatch slurm/02_unpatched_array.sh
#
# 01_compare.sh runs twelve tasks that all start together, which the defect
# survives: every task builds its own output before any wrapper looks for the
# first job's. Sixty against 208 cores start in waves, and a task starting
# after that output exists finds nothing to do and writes nothing.
#
#SBATCH --job-name=arrayjobs-unpatched
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=2000
#SBATCH --output=slurm/logs/%x-%j.out
#SBATCH --signal=B:TERM@300

source slurm/common.sh

if python3 scripts/patch_slurm_plugin.py --status | grep -q "patched  yes"; then
    echo
    echo "Patched, so this would repeat 04. Run slurm/05_unpatch.sh first."
    exit 1
fi

RESULTS=results/unpatched
source slurm/array_experiment.sh
exit "$status"

#!/bin/bash
# Reports the installed versions, the cluster's array limits, whether the
# plugin is patched, and the sbatch line each mode would use.
#
#   sbatch slurm/00_probe.sh
#
# Run it once on a cluster this has never run on, and again after any change to
# the snakemake environment. It submits no child jobs.
#
#SBATCH --job-name=arrayjobs-probe
#SBATCH --time=00:15:00
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=2000
#SBATCH --output=slurm/logs/%x-%j.out

source slurm/common.sh

echo "=================== cluster array limits ==================="
# MaxArraySize bounds one sbatch --array. The plugin subtracts one from it and
# takes the smaller of that and --slurm-array-limit.
scontrol show config | grep -E "MaxArraySize|MaxJobCount|MaxArrayTasks" || true
echo "slurm       $(sinfo --version)"
echo
echo "account limits:"
sacctmgr -n show assoc where user="$USER" format=account%20,maxsubmit,maxjobs 2>&1 | head
echo

echo "=================== the three plans ==================="
# Captured rather than piped: the command line is the first line of the
# target's output and the job counts are near the last.
for mode in plain array group; do
    echo "------------------- $mode"
    plan="$(make dry MODE="$mode" 2>&1)"
    printf '%s\n' "$plan" | head -1
    printf '%s\n' "$plan" | grep -m 1 -A 6 "^Job stats:" || printf '%s\n' "$plan" | tail -20
    echo
done

echo "=================== what happens next ==================="
echo "Each plan should list one probe job per task and one summarise job, and"
echo "the array plan should carry --slurm-array-jobs=probe. If so:"
echo "  sbatch slurm/01_compare.sh"

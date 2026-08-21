#!/bin/bash
# Step 0. Settles what can be settled without submitting anything: the versions
# in play, the cluster's array limits, whether the plugin is patched, and the
# sbatch line each mode would use.
#
#   sbatch slurm/00_probe.sh
#
# Run it once on a cluster this has never run on, and again after any change to
# the snakemake environment. It submits no child jobs, so it costs one short
# allocation and tells you whether 01_compare.sh is worth starting.
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
# Each mode differs only in the flags, so printing them side by side is the
# whole comparison up front.
for mode in plain array group; do
    echo "------------------- $mode"
    make dry MODE="$mode" 2>&1 | tail -25
    echo
done

echo "=================== what happens next ==================="
echo "Each plan should list one probe job per task and one summarise job, and"
echo "the array plan should carry --slurm-array-jobs=probe. If so:"
echo "  sbatch slurm/01_compare.sh"

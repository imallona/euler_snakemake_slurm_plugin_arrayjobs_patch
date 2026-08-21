#!/bin/bash
# The array mode on a patched plugin, at 60 tasks.
#
#   sbatch slurm/02_patched_array.sh
#
# 01_compare.sh runs twelve tasks that all start together, which an unpatched
# plugin survives. Sixty against 208 cores do not, and a task starting after
# the first job's output exists writes nothing.
#
# Apply the patch on a login node first, with `make patch`. This script refuses
# rather than editing an environment shared with whatever else is submitting.
#
#SBATCH --job-name=arrayjobs-patched
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=2000
#SBATCH --output=slurm/logs/%x-%j.out
#SBATCH --signal=B:TERM@300

source slurm/common.sh

N="${N:-60}"
RESULTS=results/patched
export RESULTS

if ! python3 scripts/patch_slurm_plugin.py --status | grep -q "patched  yes"; then
    echo
    echo "Not patched. On a login node: make patch"
    exit 1
fi

outdir=$(make -s print-outdir MODE=array RESULTS="$RESULTS")
echo "tasks       $N"
echo "outdir      $outdir"
echo

child=""
terminate() {
    echo "wall clock approaching, stopping snakemake"
    if [ -n "$child" ]; then
        kill -TERM "$child" 2>/dev/null || true
        wait "$child" || true
    fi
}
trap terminate TERM

echo "=================== the run ==================="
date -Is
set +e
make run MODE=array RESULTS="$RESULTS" N="$N" &
child=$!
wait "$child"
status=$?
set -e
echo "finished with status $status, $(date -Is)"

echo
echo "=================== the records ==================="
make verify MODE=array RESULTS="$RESULTS" N="$N" || true

echo
echo "=================== slurm evidence ==================="
bash scripts/diagnose.sh || true

echo
echo "=================== verdict ==================="
cat "$outdir/verdict.txt" 2>/dev/null || echo "no verdict written"
exit "$status"

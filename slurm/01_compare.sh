#!/bin/bash
# Runs the same probe jobs three ways and reports what each costs and whether
# each task ran its own work.
#
#   sbatch slurm/01_compare.sh
#   sbatch slurm/01_compare.sh MODES="array" N=40
#   sbatch slurm/01_compare.sh MODES="plain array" DEBUG=1
#
# Anything on the command line that looks like a make variable is passed to
# make, so N, SLEEP, JOBS, GROUP_SIZE, ARRAY_LIMIT and EXTRA work here. MODES is
# consumed by this script.
#
# The array mode is expected to fail on an unpatched plugin, so a failing mode
# does not stop the script: it runs verify against the records that did get
# written and moves on.
#
#SBATCH --job-name=arrayjobs-compare
#SBATCH --time=02:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=2000
#SBATCH --output=slurm/logs/%x-%j.out
# SIGTERM to this shell five minutes before the wall clock, so snakemake
# finishes writing its metadata instead of dying mid write and leaving a lock.
#SBATCH --signal=B:TERM@300

source slurm/common.sh

MODES="plain array group"
make_args=()
for argument in "$@"; do
    case "$argument" in
        MODES=*) MODES="${argument#MODES=}" ;;
        *) make_args+=("$argument") ;;
    esac
done

echo "modes       $MODES"
echo "make args   ${make_args[*]:-none}"
echo

child=""
# --signal reaches this shell, not the snakemake it is waiting on, so the trap
# forwards it and lets snakemake write its metadata rather than leaving a lock.
terminate() {
    echo "wall clock approaching, stopping snakemake"
    if [ -n "$child" ]; then
        kill -TERM "$child" 2>/dev/null || true
        wait "$child" || true
    fi
}
trap terminate TERM

for mode in $MODES; do
    echo "=================== $mode ==================="
    date -Is
    # A failing mode is a result. Snakemake's own exit status is recorded and
    # the loop continues, because the records written before the failure are
    # what verify reads.
    set +e
    make run MODE="$mode" ${make_args[@]+"${make_args[@]}"} &
    child=$!
    wait "$child"
    status=$?
    set -e
    echo "$mode finished with status $status, $(date -Is)"
    echo
    echo "------------------- $mode records"
    # Runs whether or not snakemake succeeded, and never fails the driver.
    make verify MODE="$mode" ${make_args[@]+"${make_args[@]}"} || true
    echo
done

echo "=================== slurm evidence ==================="
bash scripts/diagnose.sh || true

echo
echo "=================== verdicts ==================="
for mode in $MODES; do
    verdict="results/$mode/verdict.txt"
    echo "------------------- $mode"
    [ -f "$verdict" ] && cat "$verdict" || echo "no verdict written"
    echo
done

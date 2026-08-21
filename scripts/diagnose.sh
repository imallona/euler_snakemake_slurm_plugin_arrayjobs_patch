#!/bin/bash
# Slurm's own account of a run, next to what snakemake said about it.
#
#   bash scripts/diagnose.sh                    the newest driver log
#   bash scripts/diagnose.sh slurm/logs/x.out   a particular one
#
# Three things are worth reading here. Whether the child jobs carry an
# underscore in their job id, which is the only reliable sign that an array was
# used at all. What the plugin decided, which it says only at debug level, so
# the run needs DEBUG=1. And which task each MissingOutputException names,
# which is what distinguishes a mismatched array dispatch from an ordinary
# missing file.

set -uo pipefail

driver_log="${1:-$(ls -t slurm/logs/*.out 2>/dev/null | head -1)}"
if [ -z "$driver_log" ] || [ ! -f "$driver_log" ]; then
    echo "No driver log found. Pass one as the first argument." >&2
    exit 1
fi
echo "driver log  $driver_log"

# The log is named <jobname>-<jobid>.out.
driver_job="$(basename "$driver_log" .out | sed 's/.*-//')"
echo "driver job  $driver_job"
echo

echo "=================== what snakemake decided ==================="
# Every array submission the plugin made, and every one it declined to make.
grep -E "Submitted array job|Array job collection|Array submission requested|array mode disabled|Submitting array-selected" "$driver_log" | tail -40
echo

echo "=================== the sbatch calls ==================="
grep -E "^.*sbatch --parsable" "$driver_log" | sed 's/.*sbatch/sbatch/' | cut -c1-400 | tail -10
echo

echo "=================== slurm's account ==================="
start="$(sacct -n -X -j "$driver_job" -o Start 2>/dev/null | head -1 | tr -d ' ')"
if [ -z "$start" ] || [ "$start" = "Unknown" ]; then
    start="$(date -d '6 hours ago' +%Y-%m-%dT%H:%M:%S)"
    echo "no start time for job $driver_job, falling back to $start"
fi
# -X keeps one line per job rather than one per step. Array tasks appear as
# <parent>_<task>, which is the thing to look for.
sacct -X -S "$start" -o JobID%18,JobName%38,State%12,Elapsed,ExitCode,ReqCPUS,NodeList%16 \
    | head -80
echo
echo "states:"
sacct -n -X -S "$start" -o State | sort | uniq -c | sort -rn
echo
echo "array tasks among them:"
sacct -n -X -S "$start" -o JobID | grep -c "_" || true
echo

echo "=================== missing output errors ==================="
# The name in the exception is the evidence. A task that was told to build
# another task's output names a wildcard it never had.
grep -A 4 "MissingOutputException" "$driver_log" | head -60
if [ -d slurm/snakemake_logs ]; then
    echo
    echo "per job logs under slurm/snakemake_logs:"
    grep -rl "MissingOutputException" slurm/snakemake_logs 2>/dev/null | head -20
fi

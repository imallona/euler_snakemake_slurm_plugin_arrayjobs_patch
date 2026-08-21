# The body of 02 and 04, which are the same 60 task array run either side of
# the patch. Sourced, not executed. The caller sets RESULTS and may set N.

N="${N:-60}"
export RESULTS

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
# The probe files are written by other nodes, and a NetApp lookup can lag
# behind the job that wrote one. Waiting first keeps a slow file from being
# read as a task that produced nothing, which is what this run measures.
deadline=$((SECONDS + 180))
while [ "$(find "$outdir/probe" -name '*.json' 2>/dev/null | wc -l)" -lt "$N" ] \
      && [ "$SECONDS" -lt "$deadline" ]; do
    sleep 10
done
echo "$(find "$outdir/probe" -name '*.json' 2>/dev/null | wc -l) of $N records visible"
make verify MODE=array RESULTS="$RESULTS" N="$N" || true

echo
echo "=================== slurm evidence ==================="
bash scripts/diagnose.sh || true

echo
echo "=================== verdict ==================="
cat "$outdir/verdict.txt" 2>/dev/null || echo "no verdict written"

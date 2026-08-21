# Shared setup for the drivers here. Sourced, not executed.
#
# Each driver is one batch job holding snakemake while it submits the probe jobs
# and waits for them. Only the environment activation happens on a login node,
# and sbatch exports it into the job.

set -euo pipefail

# Slurm points TMPDIR at this node's local disk and the executor plugin submits
# every child with --export=ALL, so an inherited value would send a child to a
# path belonging to another node. Nothing here writes to TMPDIR, and the probe
# records whatever value it finds.
unset TMPDIR

cd "${SLURM_SUBMIT_DIR:-$PWD}"

# sbatch exports the submitting shell, so an environment activated on the login
# node is already on PATH. CONDA_INIT and CONDA_ENV are the fallback for a job
# submitted from a shell that had nothing active.
if ! command -v snakemake > /dev/null; then
    if [ -n "${CONDA_INIT:-}" ]; then
        # profile.d/conda.sh, not bin/activate: sourcing a script with no
        # arguments hands it the caller's positional parameters, so a driver
        # invoked as `sbatch slurm/01_compare.sh MODES=array` would pass
        # MODES=array to conda as an environment name.
        source "$CONDA_INIT"
        conda activate "${CONDA_ENV:?CONDA_INIT is set, so CONDA_ENV must be too}"
    fi
fi
if ! command -v snakemake > /dev/null; then
    echo "snakemake is not on PATH. Activate the environment on the login node" >&2
    echo "before sbatch, or export CONDA_INIT and CONDA_ENV." >&2
    exit 1
fi

echo "host        $(hostname)"
echo "job id      ${SLURM_JOB_ID:-none}"
echo "workdir     $PWD"
echo "TMPDIR      ${TMPDIR:-unset}"
make versions
echo

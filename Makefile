# Every target assumes the snakemake environment is already active: activate it
# on the Euler login node before sbatch, which exports it into the batch job.
#
#   make versions                what is installed, and whether it is patched
#   make dry MODE=array          the plan and the flags, no submission
#   make plain                   one sbatch per job
#   make array                   array submission
#   make group                   several jobs of a rule per Slurm job
#   make verify MODE=array       read the records of a finished or failed run
#   make diagnose                sacct and log evidence for the last driver job
#   make patch                   per task array dispatch, in the active env
#
# MODE names both the flags and the output directory, so the three runs never
# overwrite each other and can be compared afterwards.

SHELL := /bin/bash

MODE ?= plain
N ?= 12
SLEEP ?= 20
JOBS ?= 64
GROUP_SIZE ?= 4
# Tasks per sbatch call. Slurm caps a batch script at a few MB and the encoded
# commands go inside it, so this stays well under Euler's MaxArraySize of 15000.
ARRAY_LIMIT ?= 200
PROFILE ?= profiles/euler
RESULTS ?= results
# On by default. The plugin logs its array decisions only at debug level.
DEBUG ?= 1
EXTRA ?=

OUTDIR := $(RESULTS)/$(MODE)

base_args := --profile $(PROFILE) --jobs $(JOBS) \
  --config outdir=$(OUTDIR) n_tasks=$(N) sleep_seconds=$(SLEEP)

ifeq ($(DEBUG),1)
base_args += --verbose
endif

# Selected per rule rather than with all, so summarise stays a plain job and
# the log shows the selection explicitly.
array_args := --slurm-array-jobs=probe --slurm-array-limit=$(ARRAY_LIMIT)
group_args := --groups probe=probe_batch --group-components probe_batch=$(GROUP_SIZE)

ifeq ($(MODE),array)
mode_args := $(array_args)
else ifeq ($(MODE),group)
mode_args := $(group_args)
else
mode_args :=
endif

snakemake := snakemake $(base_args) $(mode_args) $(EXTRA)

.PHONY: help versions dry run plain array group verify diagnose print-outdir \
        patch-status patch unpatch clean test

help:
	@awk '/^#/ {sub(/^# ?/, ""); print; next} {exit}' Makefile

versions:
	@echo "snakemake  $$(snakemake --version)"
	@python3 -c 'import importlib.metadata as m; \
	  print("slurm      " + m.version("snakemake-executor-plugin-slurm")); \
	  print("jobstep    " + m.version("snakemake-executor-plugin-slurm-jobstep"))'
	@echo "MaxArraySize $$(scontrol show config 2>/dev/null | sed -n 's/^MaxArraySize *= *//p')"
	@python3 scripts/patch_slurm_plugin.py --status

dry:
	@echo "$(snakemake) --dry-run"
	$(snakemake) --dry-run

run:
	$(snakemake)

plain:
	$(MAKE) run MODE=plain

array:
	$(MAKE) run MODE=array

group:
	$(MAKE) run MODE=group

# Reads whatever records exist, so it is useful precisely when the run failed.
verify:
	python3 scripts/verify.py --probes $(OUTDIR)/probe --expect $(N) \
	  --summary $(OUTDIR)/summary.tsv --verdict $(OUTDIR)/verdict.txt

diagnose:
	bash scripts/diagnose.sh

# So the drivers do not rebuild the path this file already owns.
print-outdir:
	@echo $(OUTDIR)

patch-status:
	python3 scripts/patch_slurm_plugin.py --status

patch:
	python3 scripts/patch_slurm_plugin.py --apply

unpatch:
	python3 scripts/patch_slurm_plugin.py --revert

test:
	python3 -m pytest -q tests

clean:
	rm -rf $(RESULTS) .snakemake slurm/snakemake_logs

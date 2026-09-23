#!/bin/bash
#SBATCH --job-name=boltbs --nodes=1 --ntasks=1 --cpus-per-task=96 --time=01:00:00 --partition=debug --account=PAS2271
#SBATCH --output=buildlogs/%x_%j.log --error=buildlogs/%x_%j.err
# usage: BRANCH=<git ref> TAG=<name> sbatch build_and_save.sh : checkout, benchmarks-build, save + relocate binaries.
set -o pipefail; cd /users/PAS2065/ruochenj/gpu-db-research/bolt || exit 1
git checkout -q ${BRANCH:?} || exit 1; echo "building $(git rev-parse --short HEAD) ($BRANCH) as $TAG"
SLURM_JOB_ID=$SLURM_JOB_ID bash build_bolt_bench.sh; rc=$?
if [ $rc -eq 0 ] && [ -x _build/Release/bolt/benchmarks/tpch/bolt_tpch_benchmark ]; then
  B=/fs/scratch/PAS2065/ruochenj/bolt_bins/${TAG:?}; mkdir -p $B
  cp _build/Release/bolt/benchmarks/tpch/bolt_tpch_benchmark _build/Release/bolt/benchmarks/tpcds_hybrid/bolt_tpcds_hybrid_benchmark _build/Release/bolt/libbolt_testutils.so _build/Release/bolt/libbolt_engine.so $B/ && /fs/scratch/PAS2065/ruochenj/bolt_bins/relocate.sh $B && echo "SAVED $B"
fi
exit $rc

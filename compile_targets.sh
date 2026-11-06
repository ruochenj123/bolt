#!/bin/bash
#SBATCH --job-name=boltcc2
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=48
#SBATCH --time=00:40:00
#SBATCH --partition=debug
#SBATCH --account=PAS2526
#SBATCH --output=buildlogs/%x_%j.log
#SBATCH --error=buildlogs/%x_%j.err
set -o pipefail
module purge 2>/dev/null || true
export CC=/usr/bin/gcc CXX=/usr/bin/g++
cd /users/PAS2065/ruochenj/gpu-db-research/bolt
source _build/Release/generators/conanbuild.sh 2>/dev/null || true
/fs/scratch/PAS2065/ruochenj/conan2/p/b/cmake646a38acd9e13/p/bin/cmake --build _build/Release -j 48 --target bolt_tpch_benchmark bolt_tpcds_hybrid_benchmark > buildlogs/compile_$SLURM_JOB_ID.log 2>&1
rc=$?
tail -5 buildlogs/compile_$SLURM_JOB_ID.log
ls -la _build/Release/bolt/benchmarks/tpch/bolt_tpch_benchmark
exit $rc

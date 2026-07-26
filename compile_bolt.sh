#!/bin/bash
#SBATCH --job-name=boltcc
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=48
#SBATCH --time=01:00:00
#SBATCH --partition=debug
#SBATCH --account=pas2271
#SBATCH --output=buildlogs/%x_%j.log
#SBATCH --error=buildlogs/%x_%j.err
# Direct compile of the verified configure (benchmarks ON, link conflicts fixed).
set -o pipefail
module purge 2>/dev/null || true
export CC=/usr/bin/gcc CXX=/usr/bin/g++
source _build/Release/generators/conanbuild.sh 2>/dev/null || true
cd /users/PAS2065/ruochenj/gpu-db-research/bolt
/fs/scratch/PAS2065/ruochenj/conan2/p/b/cmake646a38acd9e13/p/bin/cmake --build _build/Release -j 48 --target bolt_tpch_benchmark bolt_tpcds_benchmark > buildlogs/compile_$SLURM_JOB_ID.log 2>&1
rc=$?
tail -20 buildlogs/compile_$SLURM_JOB_ID.log
echo "=== exit=$rc; benchmark binaries: ==="
find _build/Release/bolt/benchmarks -type f -executable -name "bolt_*benchmark*" 2>/dev/null
exit $rc

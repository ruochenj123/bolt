#!/bin/bash
#SBATCH --job-name=boltbuild
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=48
#SBATCH --time=01:00:00
#SBATCH --partition=debug
#SBATCH --account=pas2271
#SBATCH --output=buildlogs/%x_%j.log
#SBATCH --error=buildlogs/%x_%j.err
# Build bolt release. CRITICAL: OSC default-loads Intel oneAPI and exports
# CC/CXX=icc/icpc, which CMake honors over the conan profile -> old icc chokes on
# duckdb's 128-bit __builtin_*_overflow ("catastrophic error"). Force gcc and
# unload Intel modules. Package cache was purged so every dep rebuilds with gcc.
set -o pipefail
export PATH=$HOME/.local/bin:$PATH
export CONAN_HOME=/fs/scratch/PAS2065/ruochenj/conan2
module purge 2>/dev/null || true
export CC=/usr/bin/gcc
export CXX=/usr/bin/g++

# Shim: arrow's substrait_ep must use conan's protoc (3.21.4), not /usr/bin/protoc (3.14)
SHIM=/fs/scratch/PAS2065/ruochenj/protoc_shim; mkdir -p $SHIM
PROTOC=$(find $CONAN_HOME/p -maxdepth 6 -path "*/bin/protoc" 2>/dev/null | head -1)
if [ -n "$PROTOC" ]; then ln -sf "$PROTOC" $SHIM/protoc; export PATH=$SHIM:$PATH; echo "protoc shim -> $($SHIM/protoc --version)"; else echo "WARN: conan protoc not found, no shim"; fi

unset CFLAGS CXXFLAGS
cd /users/PAS2065/ruochenj/gpu-db-research/bolt
echo "compiler check: $($CC --version | head -1) / $($CXX --version | head -1)"
curl -sI --max-time 10 https://center2.conan.io >/dev/null && echo "network OK" || echo "NO NETWORK"
export MAKEFLAGS=-j48
make release > buildlogs/full_build_$SLURM_JOB_ID.log 2>&1
rc=$?
tail -40 buildlogs/full_build_$SLURM_JOB_ID.log
echo "=== exit=$rc; artifacts ==="
find _build -name "bolt_tpch_benchmark" -o -name "bolt_tpcds_hybrid_benchmark" 2>/dev/null
exit $rc

#!/bin/bash
#SBATCH --job-name=boltbench
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
# Some recipes fail their first build with the exported patches missing from
# the source folder (seen with icu, cpr). Evicting the package from the cache
# and retrying fixes it, so loop on that exact error.
for attempt in 1 2 3 4 5 6; do
  make benchmarks-build > buildlogs/bench_build_$SLURM_JOB_ID.log 2>&1
  rc=$?
  [ $rc -eq 0 ] && break
  PKG=$(grep -oE "No such file or directory: '/fs/scratch/PAS2065/ruochenj/conan2/p/[a-z0-9_.-]+/s/patches/" buildlogs/bench_build_$SLURM_JOB_ID.log | tail -1 | sed -E "s#.*/p/([a-z0-9_.-]+)[0-9a-f]{13}/s/patches/#\\1#")
  REF=$(grep -oE "^ERROR: [a-z0-9_.-]+/[0-9A-Za-z._-]+: Error in build\(\)" buildlogs/bench_build_$SLURM_JOB_ID.log | tail -1 | sed -E 's/^ERROR: ([^:]+):.*/\1/')
  if [ -n "$REF" ] && grep -q "s/patches/" buildlogs/bench_build_$SLURM_JOB_ID.log; then
    echo "attempt $attempt: evicting $REF (patches missing from source folder) and retrying"
    cp buildlogs/bench_build_$SLURM_JOB_ID.log buildlogs/bench_build_${SLURM_JOB_ID}_attempt$attempt.log
    conan remove "$REF" -c > /dev/null 2>&1
    continue
  fi
  break
done
tail -40 buildlogs/bench_build_$SLURM_JOB_ID.log
echo "=== exit=$rc; artifacts ==="
find _build -name "bolt_tpch_benchmark" -o -name "bolt_tpcds_hybrid_benchmark" 2>/dev/null
exit $rc

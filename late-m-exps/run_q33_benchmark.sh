#!/bin/bash
# Q33 Benchmark Script - Runs 4 modes with cold cache
# Usage: sudo ./run_q33_benchmark.sh
# Output: raw results in q33_results_<timestamp>.txt

set -e

BENCHMARK="/home/jiang.2091/velox_join/bolt/_build/Release/bolt/benchmarks/tpch/bolt_tpch_benchmark"
DATA_PATH="/home/jiang.2091/velox_join/data/join_benchmark_v2"
S_SEL=30
T_SEL=60
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_FILE="/home/jiang.2091/velox_join/bolt/late-m-exps/q33_results_${TIMESTAMP}.txt"

echo "Q33 Benchmark - S=${S_SEL}% T=${T_SEL}% selectivity"
echo "Output: ${OUTPUT_FILE}"
echo ""

run_benchmark() {
    local MODE_NAME=$1
    local HYBRID_JOIN=$2
    local HYBRID_SORT=$3
    local LATE_M=$4
    local LATE_M_REUSE=$5
    
    echo "========================================"
    echo "Running: ${MODE_NAME}"
    echo "========================================"
    
    # Drop caches for cold run
    echo 3 > /proc/sys/vm/drop_caches
    sleep 2
    
    echo ""
    echo "=== ${MODE_NAME} ===" >> "${OUTPUT_FILE}"
    echo "Timestamp: $(date)" >> "${OUTPUT_FILE}"
    echo "" >> "${OUTPUT_FILE}"
    
    ${BENCHMARK} \
        -run_query_verbose=33 \
        -data_path=${DATA_PATH} \
        -hybrid_join_enabled=${HYBRID_JOIN} \
        -hybrid_sort_enabled=${HYBRID_SORT} \
        -late_materialization_enabled=${LATE_M} \
        -hybrid_join_pointer_reuse_enabled=${LATE_M_REUSE} \
        -s_selectivity_pct=${S_SEL} \
        -t_selectivity_pct=${T_SEL} \
        -num_splits_per_file=1 \
        2>&1 | tee -a "${OUTPUT_FILE}"
    
    echo "" >> "${OUTPUT_FILE}"
    echo "========================================" >> "${OUTPUT_FILE}"
    echo "" >> "${OUTPUT_FILE}"
    
    echo ""
    echo "Completed: ${MODE_NAME}"
    echo ""
}

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Please run with sudo to drop caches"
    exit 1
fi

# Initialize output file
echo "Q33 Benchmark Results" > "${OUTPUT_FILE}"
echo "S_selectivity: ${S_SEL}%" >> "${OUTPUT_FILE}"
echo "T_selectivity: ${T_SEL}%" >> "${OUTPUT_FILE}"
echo "Date: $(date)" >> "${OUTPUT_FILE}"
echo "" >> "${OUTPUT_FILE}"

# Run all 4 modes
# 1. Baseline (no hybrid, no late-m)
run_benchmark "BASELINE" "false" "false" "false" "false"

# 2. Hybrid-only (hybrid join, no late-m)
run_benchmark "HYBRID-ONLY" "true" "true" "false" "false"

# 3. Late-M Standard (no hybrid, late-m without reuse)
run_benchmark "LATE-M-STANDARD" "true" "true" "true" "false"

# 4. Late-M Reuse (no hybrid, late-m with reuse)
run_benchmark "LATE-M-REUSE" "true" "true" "true" "true"

echo "========================================"
echo "All benchmarks complete!"
echo "Results saved to: ${OUTPUT_FILE}"
echo "========================================"

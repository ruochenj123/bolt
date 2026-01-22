#!/bin/bash
# Benchmark Q23/Q24 with DEFAULT settings and verify correctness

BENCHMARK_BIN="/home/jiang.2091/velox_join/bolt/_build/Release/bolt/benchmarks/tpch/bolt_tpch_benchmark"
DATA_PATH="/home/jiang.2091/velox_join/data/tpch_parquet/sf10_hive"
OUTPUT_DIR="/home/jiang.2091/velox_join/bolt/benchmark_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "${OUTPUT_DIR}"

echo "=============================================="
echo "TPC-H Q23/Q24 Benchmark - Default Settings"
echo "=============================================="
echo "Data path: ${DATA_PATH}"
echo "Using DEFAULT batch size settings"
echo "Timestamp: ${TIMESTAMP}"
echo ""

# Function to run benchmark and show results
run_benchmark() {
    local CONFIG_NAME=$1
    local HYBRID_JOIN=$2
    local HYBRID_SORT=$3
    local LATE_M=$4
    
    echo ""
    echo "=========================================="
    echo "Config: ${CONFIG_NAME}"
    echo "  hybrid_join_enabled=${HYBRID_JOIN}"
    echo "  hybrid_sort_enabled=${HYBRID_SORT}"
    echo "  late_materialization_enabled=${LATE_M}"
    echo "=========================================="
    
    # Run Q23
    echo ""
    echo "--- Q23 Performance ---"
    ${BENCHMARK_BIN} \
        --data_path="${DATA_PATH}" \
        --bm_regex="q23" \
        --num_repeats=3 \
        --hybrid_join_enabled=${HYBRID_JOIN} \
        --hybrid_sort_enabled=${HYBRID_SORT} \
        --late_materialization_enabled=${LATE_M} \
        2>&1
    
    # Run Q23 with result output
    echo ""
    echo "--- Q23 Result (first 10 rows) ---"
    ${BENCHMARK_BIN} \
        --data_path="${DATA_PATH}" \
        --bm_regex="q23" \
        --num_repeats=1 \
        --hybrid_join_enabled=${HYBRID_JOIN} \
        --hybrid_sort_enabled=${HYBRID_SORT} \
        --late_materialization_enabled=${LATE_M} \
        --include_results=true \
        2>&1 | head -50
    
    # Run Q24
    echo ""
    echo "--- Q24 Performance ---"
    ${BENCHMARK_BIN} \
        --data_path="${DATA_PATH}" \
        --bm_regex="q24" \
        --num_repeats=3 \
        --hybrid_join_enabled=${HYBRID_JOIN} \
        --hybrid_sort_enabled=${HYBRID_SORT} \
        --late_materialization_enabled=${LATE_M} \
        2>&1
    
    # Run Q24 with result output
    echo ""
    echo "--- Q24 Result (first 10 rows) ---"
    ${BENCHMARK_BIN} \
        --data_path="${DATA_PATH}" \
        --bm_regex="q24" \
        --num_repeats=1 \
        --hybrid_join_enabled=${HYBRID_JOIN} \
        --hybrid_sort_enabled=${HYBRID_SORT} \
        --late_materialization_enabled=${LATE_M} \
        --include_results=true \
        2>&1 | head -50
}

# Run all three configurations
run_benchmark "Baseline" "false" "false" "false"
run_benchmark "Hybrid" "true" "true" "false"
run_benchmark "Late-M" "true" "true" "true"

echo ""
echo "=========================================="
echo "Benchmark Complete!"
echo "=========================================="

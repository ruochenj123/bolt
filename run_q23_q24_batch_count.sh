#!/bin/bash
# Benchmark Q23/Q24 with varying NUMBER OF BATCHES
# SF10 lineitem has ~60M rows
# num_batches = total_rows / batch_size
# So: batch_size = 60M / desired_batches

BENCHMARK_BIN="/home/jiang.2091/velox_join/bolt/_build/Release/bolt/benchmarks/tpch/bolt_tpch_benchmark"
DATA_PATH="/home/jiang.2091/velox_join/data/tpch_parquet/sf10_hive"
OUTPUT_DIR="/home/jiang.2091/velox_join/bolt/benchmark_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="${OUTPUT_DIR}/q23_batch_count_${TIMESTAMP}.txt"

mkdir -p "${OUTPUT_DIR}"

# Desired batch counts and corresponding batch sizes for SF10 (~60M rows)
# batch_size = 60,000,000 / num_batches
declare -A BATCH_CONFIG
BATCH_CONFIG[100]=600000      # 100 batches → 600K rows/batch
BATCH_CONFIG[1000]=60000      # 1K batches → 60K rows/batch
BATCH_CONFIG[5000]=12000      # 5K batches → 12K rows/batch
BATCH_CONFIG[50000]=1200      # 50K batches → 1200 rows/batch

BATCH_COUNTS=(100 1000 5000 50000)
NUM_REPEATS=3

echo "=============================================="
echo "TPC-H Q23 Benchmark - Batch COUNT Analysis"
echo "=============================================="
echo "Data path: ${DATA_PATH}"
echo "Output dir: ${OUTPUT_DIR}"
echo "Target batch counts: ${BATCH_COUNTS[*]}"
echo "Repeats per test: ${NUM_REPEATS}"
echo "Timestamp: ${TIMESTAMP}"
echo ""
echo "Results will be saved to: ${RESULTS_FILE}"
echo ""

# Write header to results file
{
    echo "=============================================="
    echo "TPC-H Q23 Benchmark - Batch COUNT Analysis"
    echo "=============================================="
    echo "Data path: ${DATA_PATH}"
    echo "Target batch counts: ${BATCH_COUNTS[*]}"
    echo "Repeats per test: ${NUM_REPEATS}"
    echo "Timestamp: ${TIMESTAMP}"
    echo ""
} > "${RESULTS_FILE}"

for NUM_BATCHES in "${BATCH_COUNTS[@]}"; do
    BATCH_SIZE=${BATCH_CONFIG[$NUM_BATCHES]}
    
    echo "======================================"
    echo "Testing with ~${NUM_BATCHES} batches (batch_size=${BATCH_SIZE})"
    echo "======================================"
    {
        echo ""
        echo "======================================"
        echo "Testing with ~${NUM_BATCHES} batches (batch_size=${BATCH_SIZE})"
        echo "======================================"
    } >> "${RESULTS_FILE}"
    
    # Baseline: no optimizations
    echo ""
    echo "--- Baseline (no optimizations) ---"
    echo "--- Baseline (no optimizations) ---" >> "${RESULTS_FILE}"
    ${BENCHMARK_BIN} \
        --data_path="${DATA_PATH}" \
        --bm_regex="q23" \
        --num_repeats=${NUM_REPEATS} \
        --hybrid_join_enabled=false \
        --hybrid_sort_enabled=false \
        --late_materialization_enabled=false \
        --preferred_output_batch_rows=${BATCH_SIZE} \
        --max_output_batch_rows=${BATCH_SIZE} \
        2>&1 | tee -a "${RESULTS_FILE}"
    
    # Hybrid: hybrid_join + hybrid_sort enabled
    echo ""
    echo "--- Hybrid (hybrid_join + hybrid_sort) ---"
    echo "--- Hybrid (hybrid_join + hybrid_sort) ---" >> "${RESULTS_FILE}"
    ${BENCHMARK_BIN} \
        --data_path="${DATA_PATH}" \
        --bm_regex="q23" \
        --num_repeats=${NUM_REPEATS} \
        --hybrid_join_enabled=true \
        --hybrid_sort_enabled=true \
        --late_materialization_enabled=false \
        --preferred_output_batch_rows=${BATCH_SIZE} \
        --max_output_batch_rows=${BATCH_SIZE} \
        2>&1 | tee -a "${RESULTS_FILE}"
    
    # Late-M: all optimizations enabled
    echo ""
    echo "--- Late-M (all optimizations) ---"
    echo "--- Late-M (all optimizations) ---" >> "${RESULTS_FILE}"
    ${BENCHMARK_BIN} \
        --data_path="${DATA_PATH}" \
        --bm_regex="q23" \
        --num_repeats=${NUM_REPEATS} \
        --hybrid_join_enabled=true \
        --hybrid_sort_enabled=true \
        --late_materialization_enabled=true \
        --preferred_output_batch_rows=${BATCH_SIZE} \
        --max_output_batch_rows=${BATCH_SIZE} \
        2>&1 | tee -a "${RESULTS_FILE}"
done

echo ""
echo "======================================"
echo "Benchmark Complete!"
echo "======================================"
echo "Results saved to: ${RESULTS_FILE}"

{
    echo ""
    echo "======================================"
    echo "Benchmark Complete!"
    echo "======================================"
} >> "${RESULTS_FILE}"

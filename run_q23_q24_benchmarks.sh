#!/bin/bash

# TPC-H Q23/Q24 Benchmark Script
# Tests different batch sizes (100, 1000, 5000, 50000) with three configurations:
# - Baseline: No optimizations
# - Hybrid: hybrid_join + hybrid_sort enabled  
# - Late-M: hybrid_join + hybrid_sort + late_materialization enabled

BENCHMARK="/home/jiang.2091/velox_join/bolt/_build/Release/bolt/benchmarks/tpch/bolt_tpch_benchmark"
DATA_PATH="/home/jiang.2091/velox_join/data/tpch_parquet/sf10_hive"
OUTPUT_DIR="/home/jiang.2091/velox_join/bolt/benchmark_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "$OUTPUT_DIR"

# Batch sizes to test
BATCH_SIZES=(100 1000 5000 50000)

# Number of repeats for each benchmark
NUM_REPEATS=3

echo "=============================================="
echo "TPC-H Q23/Q24 Benchmark - Batch Size Analysis"
echo "=============================================="
echo "Data path: $DATA_PATH"
echo "Output dir: $OUTPUT_DIR"
echo "Batch sizes: ${BATCH_SIZES[*]}"
echo "Repeats per test: $NUM_REPEATS"
echo "Timestamp: $TIMESTAMP"
echo ""

# Results file
RESULTS_FILE="$OUTPUT_DIR/q23_q24_results_${TIMESTAMP}.txt"
echo "Results will be saved to: $RESULTS_FILE"
echo ""

echo "TPC-H Q23/Q24 Benchmark Results - $TIMESTAMP" > "$RESULTS_FILE"
echo "=============================================" >> "$RESULTS_FILE"
echo "" >> "$RESULTS_FILE"

for BATCH_SIZE in "${BATCH_SIZES[@]}"; do
    echo "======================================" | tee -a "$RESULTS_FILE"
    echo "Testing with batch size: $BATCH_SIZE" | tee -a "$RESULTS_FILE"
    echo "======================================" | tee -a "$RESULTS_FILE"
    echo "" | tee -a "$RESULTS_FILE"
    
    # Configuration 1: Baseline (no optimizations)
    echo "--- Baseline (no optimizations) ---" | tee -a "$RESULTS_FILE"
    $BENCHMARK \
        --benchmark \
        --bm_regex="q23|q24" \
        --data_path="$DATA_PATH" \
        --preferred_output_batch_rows=$BATCH_SIZE \
        --max_output_batch_rows=$BATCH_SIZE \
        --hybrid_join_enabled=false \
        --hybrid_sort_enabled=false \
        --late_materialization_enabled=false \
        --num_repeats=$NUM_REPEATS \
        --bm_max_secs=120 \
        2>&1 | tee -a "$RESULTS_FILE"
    echo "" | tee -a "$RESULTS_FILE"
    
    # Configuration 2: Hybrid (hybrid_join + hybrid_sort)
    echo "--- Hybrid (hybrid_join + hybrid_sort) ---" | tee -a "$RESULTS_FILE"
    $BENCHMARK \
        --benchmark \
        --bm_regex="q23|q24" \
        --data_path="$DATA_PATH" \
        --preferred_output_batch_rows=$BATCH_SIZE \
        --max_output_batch_rows=$BATCH_SIZE \
        --hybrid_join_enabled=true \
        --hybrid_sort_enabled=true \
        --late_materialization_enabled=false \
        --num_repeats=$NUM_REPEATS \
        --bm_max_secs=120 \
        2>&1 | tee -a "$RESULTS_FILE"
    echo "" | tee -a "$RESULTS_FILE"
    
    # Configuration 3: Late-M (all optimizations)
    echo "--- Late-M (all optimizations) ---" | tee -a "$RESULTS_FILE"
    $BENCHMARK \
        --benchmark \
        --bm_regex="q23|q24" \
        --data_path="$DATA_PATH" \
        --preferred_output_batch_rows=$BATCH_SIZE \
        --max_output_batch_rows=$BATCH_SIZE \
        --hybrid_join_enabled=true \
        --hybrid_sort_enabled=true \
        --late_materialization_enabled=true \
        --num_repeats=$NUM_REPEATS \
        --bm_max_secs=120 \
        2>&1 | tee -a "$RESULTS_FILE"
    echo "" | tee -a "$RESULTS_FILE"
done

echo "=============================================="
echo "Benchmark complete! Results saved to: $RESULTS_FILE"
echo "=============================================="

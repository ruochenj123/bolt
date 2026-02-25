#!/bin/bash

DATA_PATH="/home/jiang.2091/velox_join/data/join_benchmark_v2"
BENCHMARK="/home/jiang.2091/velox_join/bolt/_build/Release/bolt/benchmarks/tpch/bolt_tpch_benchmark"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RAW_OUTPUT_DIR="$SCRIPT_DIR/raw_outputs"

# Create raw output directory
mkdir -p $RAW_OUTPUT_DIR

echo "========================================"
echo "Q33 Benchmark Comparison"
echo "Data: $DATA_PATH"
echo "Raw outputs: $RAW_OUTPUT_DIR"
echo "Date: $(date)"
echo "========================================"

run_benchmark() {
    local name=$1
    local hybrid=$2
    local latem=$3
    local reuse=$4
    
    echo ""
    echo ">>> $name"
    echo "    hybrid=$hybrid, late_m=$latem, reuse=$reuse"
    
    # Save raw output to file
    local raw_file="$RAW_OUTPUT_DIR/${name}.log"
    
    $BENCHMARK \
        --run_query_verbose=33 \
        --data_path=$DATA_PATH \
        --num_drivers=1 \
        --num_splits_per_file=1 \
        --hybrid_join_enabled=$hybrid \
        --late_materialization_enabled=$latem \
        --hybrid_join_pointer_reuse_enabled=$reuse 2>&1 > "$raw_file"
    
    result=$(cat "$raw_file")
    
    # E2E time
    time=$(echo "$result" | grep "Execution time:" | head -1 | awk '{print $3, $4}')
    
    # Get the final join output line (first "Output:" line)
    output_line=$(echo "$result" | grep "Output:" | head -1)
    rows=$(echo "$output_line" | grep -oP '\d+(?= rows)')
    memory=$(echo "$output_line" | grep -oP 'Peak memory: [^,]+' | cut -d: -f2)
    cpu_time=$(echo "$output_line" | grep -oP 'Cpu time: [^,]+' | cut -d: -f2)
    wall_time=$(echo "$output_line" | grep -oP 'Wall time: [^,]+' | cut -d: -f2)
    
    echo "    E2E: $time | CPU:$cpu_time | Wall:$wall_time | Rows: $rows | Memory:$memory"
    echo "    Raw: $raw_file"
}

echo ""
echo "========== 1. BASELINE (vanilla bolt) =========="
run_benchmark "baseline" false false false

echo ""
echo "========== 2. HYBRID-ONLY =========="
run_benchmark "hybrid-only" true false false

echo ""
echo "========== 3. LATE-M-STANDARD =========="
run_benchmark "late-m-standard" true true false

echo ""
echo "========== 4. LATE-M-REUSE (VirtualRow) =========="
run_benchmark "late-m-reuse" true true true

echo ""
echo "========================================"
echo "Benchmark Complete: $(date)"
echo "Raw outputs saved to: $RAW_OUTPUT_DIR"
echo "========================================"
ls -la $RAW_OUTPUT_DIR

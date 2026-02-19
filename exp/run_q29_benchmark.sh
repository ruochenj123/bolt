#!/bin/bash
# Q29 Comprehensive Benchmark Script
# 3 modes × 4 sort keys × 4 projection cols × 3 repeats = 144 runs

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_PATH=/home/jiang.2091/velox_join/data/tpch_parquet/sf10_hive
BUILD_DIR=/home/jiang.2091/velox_join/bolt/_build/Release
BENCHMARK=$BUILD_DIR/bolt/benchmarks/tpch/bolt_tpch_benchmark
RESULTS_FILE=$SCRIPT_DIR/q29_benchmark_results.csv

echo "mode,sort_keys,proj_cols,repeat,exec_time_s,sort_cpu_s,sort_I_ms,sort_O_ms,sort_F_s,peak_mem_GB" > $RESULTS_FILE

run_benchmark() {
    local mode=$1
    local sort_keys=$2
    local proj_cols=$3
    local repeat=$4
    
    local hybrid_flag=""
    local extraction_flag=""
    
    case $mode in
        baseline)
            hybrid_flag="--hybrid_sort_enabled=false"
            ;;
        hybrid-opt)
            hybrid_flag="--hybrid_sort_enabled=true"
            extraction_flag="--hybrid_sort_extraction_optimized=true"
            ;;
        hybrid-nonopt)
            hybrid_flag="--hybrid_sort_enabled=true"
            extraction_flag="--hybrid_sort_extraction_optimized=false"
            ;;
    esac
    
    output=$($BENCHMARK \
        --data_path=$DATA_PATH \
        --run_query_verbose=29 \
        --q29_sort_keys=$sort_keys \
        --q29_projection_cols=$proj_cols \
        $hybrid_flag \
        $extraction_flag \
        --include_custom_stats=true \
        2>&1)
    
    # Extract execution time
    exec_time=$(echo "$output" | grep "^Execution time:" | head -1 | awk '{print $3}' | sed 's/s//')
    
    # Extract OrderBy stats
    orderby_line=$(echo "$output" | grep "OrderBy\[1\]" -A3 | head -4)
    
    # Extract CPU time (from "Cpu time: X.XXs")
    sort_cpu=$(echo "$orderby_line" | grep -oP 'Cpu time: \K[0-9.]+' | head -1)
    
    # Extract Peak memory (from "Peak memory: X.XXGB")
    peak_mem=$(echo "$orderby_line" | grep -oP 'Peak memory: \K[0-9.]+' | head -1)
    
    # Extract CPU breakdown B/I/O/F
    breakdown=$(echo "$orderby_line" | grep -oP 'B/I/O/F \(\K[^)]+' | head -1)
    IFS='/' read -r B_raw I_raw O_raw F_raw <<< "$breakdown"
    
    # Convert to ms for I and O
    I_ms=$(echo "$I_raw" | sed 's/ms//' | sed 's/s/*1000/' | bc 2>/dev/null || echo "$I_raw" | sed 's/[^0-9.]//g')
    O_ms=$(echo "$O_raw" | sed 's/ms//' | sed 's/s/*1000/' | bc 2>/dev/null || echo "$O_raw" | sed 's/[^0-9.]//g')
    F_val=$(echo "$F_raw" | sed 's/[^0-9.]//g')
    F_unit=$(echo "$F_raw" | sed 's/[0-9.]//g')
    if [ "$F_unit" = "ms" ]; then
        F_s=$(echo "scale=3; $F_val / 1000" | bc)
    else
        F_s=$F_val
    fi
    
    echo "$mode,$sort_keys,$proj_cols,$repeat,$exec_time,$sort_cpu,$I_ms,$O_ms,$F_s,$peak_mem" >> $RESULTS_FILE
    echo "$(date '+%H:%M:%S') $mode | keys=$sort_keys | cols=$proj_cols | rep=$repeat | time=${exec_time}s | sort_F=${F_s}s"
}

echo "Starting Q29 Benchmark Suite at $(date)"
echo "====================================="
echo "Total runs: 144 (3 modes × 4 sort keys × 4 proj cols × 3 repeats)"
echo "Results: $RESULTS_FILE"
echo ""

for mode in baseline hybrid-opt hybrid-nonopt; do
    echo ""
    echo "Mode: $mode ($(date))"
    echo "-------------------------------------"
    for sort_keys in 1 2 3 4; do
        for proj_cols in 4 8 12 16; do
            for repeat in 1 2 3; do
                run_benchmark $mode $sort_keys $proj_cols $repeat
            done
        done
    done
done

echo ""
echo "====================================="
echo "Benchmark completed at $(date)"
echo "Results saved to: $RESULTS_FILE"

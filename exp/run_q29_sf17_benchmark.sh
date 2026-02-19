#!/bin/bash
# Q29 Sort Benchmark on SF17 (102M rows)
# 3 modes × 4 sort keys × 4 projection cols × 3 repeats = 144 runs

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_PATH=/home/jiang.2091/velox_join/data/tpch_parquet/sf17_hive
BUILD_DIR=/home/jiang.2091/velox_join/bolt/_build/Release
BENCHMARK=$BUILD_DIR/bolt/benchmarks/tpch/bolt_tpch_benchmark
RESULTS_FILE=$SCRIPT_DIR/q29_sf17_results.csv
RAW_OUTPUT_DIR=$SCRIPT_DIR/q29_sf17_raw

# Create raw output directory
mkdir -p $RAW_OUTPUT_DIR

echo "mode,sort_keys,proj_cols,repeat,exec_time_s,sort_cpu_s,sort_I_s,sort_O_s,sort_F_s,peak_mem_GB" > $RESULTS_FILE

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
    
    # Run benchmark and capture output
    output=$($BENCHMARK \
        --data_path=$DATA_PATH \
        --run_query_verbose=29 \
        --q29_sort_keys=$sort_keys \
        --q29_projection_cols=$proj_cols \
        $hybrid_flag \
        $extraction_flag \
        --include_custom_stats=true \
        2>&1)
    
    # Save raw output
    raw_file="$RAW_OUTPUT_DIR/${mode}_keys${sort_keys}_cols${proj_cols}_rep${repeat}.txt"
    echo "$output" > "$raw_file"
    
    # Extract execution time (handles both "8.14s" and "1m 7s" formats)
    exec_time_raw=$(echo "$output" | grep "^Execution time:" | head -1)
    if echo "$exec_time_raw" | grep -qP '\dm'; then
        # Format: "Execution time: Xm Ys"
        mins=$(echo "$exec_time_raw" | grep -oP '\d+(?=m)')
        secs=$(echo "$exec_time_raw" | grep -oP '\d+(?=s)')
        exec_time=$(echo "$mins * 60 + $secs" | bc)
    else
        # Format: "Execution time: X.XXs"
        exec_time=$(echo "$exec_time_raw" | awk '{print $3}' | sed 's/s//')
    fi
    
    # Extract OrderBy stats
    orderby_line=$(echo "$output" | grep "OrderBy\[1\]" -A3 | head -4)
    
    # Extract CPU time (from "Cpu time: X.XXs")
    sort_cpu=$(echo "$orderby_line" | grep -oP 'Cpu time: \K[0-9.]+' | head -1)
    
    # Extract Peak memory (from "Peak memory: X.XXGB")
    peak_mem=$(echo "$orderby_line" | grep -oP 'Peak memory: \K[0-9.]+' | head -1)
    
    # Extract CPU breakdown B/I/O/F
    breakdown=$(echo "$orderby_line" | grep -oP 'B/I/O/F \(\K[^)]+' | head -1)
    IFS='/' read -r B_raw I_raw O_raw F_raw <<< "$breakdown"
    
    # Convert time values to seconds
    convert_to_s() {
        local val=$1
        local num=$(echo "$val" | sed 's/[^0-9.]//g')
        local unit=$(echo "$val" | sed 's/[0-9.]//g')
        if [ "$unit" = "ms" ]; then
            echo "scale=4; $num / 1000" | bc
        elif [ "$unit" = "us" ]; then
            echo "scale=6; $num / 1000000" | bc
        else
            echo "$num"
        fi
    }
    
    I_s=$(convert_to_s "$I_raw")
    O_s=$(convert_to_s "$O_raw")
    F_s=$(convert_to_s "$F_raw")
    
    echo "$mode,$sort_keys,$proj_cols,$repeat,$exec_time,$sort_cpu,$I_s,$O_s,$F_s,$peak_mem" >> $RESULTS_FILE
    echo "$(date '+%H:%M:%S') $mode | keys=$sort_keys | cols=$proj_cols | rep=$repeat | time=${exec_time}s | sort_F=${F_s}s"
}

echo "Starting Q29 Sort Benchmark on SF17 (102M rows) at $(date)"
echo "==========================================================="
echo "Total runs: 144 (3 modes × 4 sort keys × 4 proj cols × 3 repeats)"
echo "Data: $DATA_PATH"
echo "Results: $RESULTS_FILE"
echo "Raw output: $RAW_OUTPUT_DIR"
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
echo "==========================================================="
echo "Benchmark completed at $(date)"
echo "Results saved to: $RESULTS_FILE"
echo "Raw outputs in: $RAW_OUTPUT_DIR"

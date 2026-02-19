#!/bin/bash
# Q30 Join Benchmark with R (25M build) / S (100M probe)
# 2 modes × 4 selectivities × 2 driver counts × 3 repeats = 48 runs

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_PATH=/home/jiang.2091/velox_join/data/join_benchmark
BUILD_DIR=/home/jiang.2091/velox_join/bolt/_build/Release
BENCHMARK=$BUILD_DIR/bolt/benchmarks/tpch/bolt_tpch_benchmark
RESULTS_FILE=$SCRIPT_DIR/q30_join_results.csv
RAW_OUTPUT_DIR=$SCRIPT_DIR/q30_join_raw

# Create raw output directory
mkdir -p $RAW_OUTPUT_DIR

echo "mode,selectivity,num_drivers,repeat,exec_time_s,join_cpu_s,build_cpu_s,probe_cpu_s,build_I_s,probe_I_s,probe_O_s,peak_mem_GB,output_rows" > $RESULTS_FILE

run_benchmark() {
    local mode=$1
    local selectivity=$2
    local num_drivers=$3
    local repeat=$4
    
    local hybrid_flag=""
    
    case $mode in
        baseline)
            hybrid_flag="--hybrid_join_enabled=false"
            ;;
        hybrid-opt)
            hybrid_flag="--hybrid_join_enabled=true"
            ;;
    esac
    
    # Run benchmark and capture output
    output=$($BENCHMARK \
        --data_path=$DATA_PATH \
        --run_query_verbose=30 \
        --q30_probe_selectivity=$selectivity \
        --num_drivers=$num_drivers \
        $hybrid_flag \
        --include_custom_stats=true \
        2>&1)
    
    # Save raw output
    raw_file="$RAW_OUTPUT_DIR/${mode}_sel${selectivity}_drv${num_drivers}_rep${repeat}.txt"
    echo "$output" > "$raw_file"
    
    # Extract execution time (handles both "X.XXs" and "Xm Ys" formats)
    exec_time_raw=$(echo "$output" | grep "^Execution time:" | head -1)
    if echo "$exec_time_raw" | grep -qP '\dm'; then
        mins=$(echo "$exec_time_raw" | grep -oP '\d+(?=m)')
        secs=$(echo "$exec_time_raw" | grep -oP '\d+(?=s)')
        exec_time=$(echo "$mins * 60 + $secs" | bc)
    else
        exec_time=$(echo "$exec_time_raw" | awk '{print $3}' | sed 's/s//')
    fi
    
    # Extract HashJoin stats
    join_line=$(echo "$output" | grep "HashJoin\[" -A3 | head -4)
    join_cpu=$(echo "$join_line" | grep -oP 'Cpu time: \K[0-9.]+' | head -1)
    peak_mem=$(echo "$join_line" | grep -oP 'Peak memory: \K[0-9.]+' | head -1)
    output_rows=$(echo "$join_line" | grep -oP 'Output: \K[0-9]+' | head -1)
    
    # Extract HashBuild stats
    build_line=$(echo "$output" | grep "HashBuild:" -A1 | head -2)
    build_cpu=$(echo "$build_line" | grep -oP 'Cpu time: \K[0-9.]+' | head -1)
    build_breakdown=$(echo "$build_line" | grep -oP 'B/I/O/F \(\K[^)]+' | head -1)
    IFS='/' read -r B_b I_b O_b F_b <<< "$build_breakdown"
    
    # Extract HashProbe stats
    probe_line=$(echo "$output" | grep "HashProbe:" -A1 | head -2)
    probe_cpu=$(echo "$probe_line" | grep -oP 'Cpu time: \K[0-9.]+' | head -1)
    probe_breakdown=$(echo "$probe_line" | grep -oP 'B/I/O/F \(\K[^)]+' | head -1)
    IFS='/' read -r B_p I_p O_p F_p <<< "$probe_breakdown"
    
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
    
    build_I_s=$(convert_to_s "$I_b")
    probe_I_s=$(convert_to_s "$I_p")
    probe_O_s=$(convert_to_s "$O_p")
    
    echo "$mode,$selectivity,$num_drivers,$repeat,$exec_time,$join_cpu,$build_cpu,$probe_cpu,$build_I_s,$probe_I_s,$probe_O_s,$peak_mem,$output_rows" >> $RESULTS_FILE
    echo "$(date '+%H:%M:%S') $mode | sel=$selectivity% | drv=$num_drivers | rep=$repeat | time=${exec_time}s | rows=${output_rows}"
}

echo "Starting Q30 Join Benchmark at $(date)"
echo "==========================================================="
echo "Build (R): 25M rows | Probe (S): 100M rows"
echo "Total runs: 48 (2 modes × 4 selectivities × 2 drivers × 3 repeats)"
echo "Data: $DATA_PATH"
echo "Results: $RESULTS_FILE"
echo "Raw output: $RAW_OUTPUT_DIR"
echo ""

for mode in baseline hybrid-opt; do
    echo ""
    echo "Mode: $mode ($(date))"
    echo "-------------------------------------"
    for selectivity in 10 30 60 90; do
        for num_drivers in 1 4; do
            for repeat in 1 2 3; do
                run_benchmark $mode $selectivity $num_drivers $repeat
            done
        done
    done
done

echo ""
echo "==========================================================="
echo "Benchmark completed at $(date)"
echo "Results saved to: $RESULTS_FILE"
echo "Raw outputs in: $RAW_OUTPUT_DIR"

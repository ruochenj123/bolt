#!/bin/bash
# Q30 Join Benchmark Script
# 2 modes × 4 selectivities × 2 driver counts × 3 repeats = 48 runs

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_PATH=/home/jiang.2091/velox_join/data/tpch_parquet/sf10_hive
BUILD_DIR=/home/jiang.2091/velox_join/bolt/_build/Release
BENCHMARK=$BUILD_DIR/bolt/benchmarks/tpch/bolt_tpch_benchmark
RESULTS_FILE=$SCRIPT_DIR/q30_benchmark_results.csv

echo "mode,selectivity,drivers,repeat,exec_time_s,join_cpu_s,join_B_s,join_I_s,join_O_s,join_F_s,peak_mem_GB,output_rows" > $RESULTS_FILE

run_benchmark() {
    local mode=$1
    local selectivity=$2
    local drivers=$3
    local repeat=$4
    
    local hybrid_flag=""
    
    case $mode in
        baseline)
            hybrid_flag="--hybrid_join_enabled=false"
            ;;
        hybrid)
            hybrid_flag="--hybrid_join_enabled=true"
            ;;
    esac
    
    output=$($BENCHMARK \
        --data_path=$DATA_PATH \
        --run_query_verbose=30 \
        --q30_probe_selectivity=$selectivity \
        --num_drivers=$drivers \
        $hybrid_flag \
        --include_custom_stats=true \
        2>&1)
    
    # Extract execution time
    exec_time=$(echo "$output" | grep "^Execution time:" | head -1 | awk '{print $3}' | sed 's/s//')
    
    # Extract output rows from HashJoin line (Output: NNNN rows)
    output_rows=$(echo "$output" | grep -E "HashJoin\[[0-9]+\]" -A1 | grep -oP 'Output: \K[0-9]+' | head -1)
    
    # Extract HashJoin stats (look for HashJoin[3] which is typically the join operator)
    join_line=$(echo "$output" | grep -E "HashJoin\[[0-9]+\]" -A3 | head -4)
    
    # Extract CPU time (from "Cpu time: X.XXs")
    join_cpu=$(echo "$join_line" | grep -oP 'Cpu time: \K[0-9.]+' | head -1)
    
    # Extract Peak memory (from "Peak memory: X.XXGB")
    peak_mem=$(echo "$join_line" | grep -oP 'Peak memory: \K[0-9.]+' | head -1)
    
    # Extract CPU breakdown B/I/O/F
    breakdown=$(echo "$join_line" | grep -oP 'B/I/O/F \(\K[^)]+' | head -1)
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
    
    B_s=$(convert_to_s "$B_raw")
    I_s=$(convert_to_s "$I_raw")
    O_s=$(convert_to_s "$O_raw")
    F_s=$(convert_to_s "$F_raw")
    
    echo "$mode,$selectivity,$drivers,$repeat,$exec_time,$join_cpu,$B_s,$I_s,$O_s,$F_s,$peak_mem,$output_rows" >> $RESULTS_FILE
    echo "$(date '+%H:%M:%S') $mode | sel=${selectivity}% | drv=$drivers | rep=$repeat | time=${exec_time}s | rows=$output_rows"
}

echo "Starting Q30 Join Benchmark Suite at $(date)"
echo "============================================="
echo "Total runs: 48 (2 modes × 4 selectivities × 2 drivers × 3 repeats)"
echo "Results: $RESULTS_FILE"
echo ""

for mode in baseline hybrid; do
    echo ""
    echo "Mode: $mode ($(date))"
    echo "-------------------------------------"
    for selectivity in 10 30 60 90; do
        for drivers in 1 4; do
            for repeat in 1 2 3; do
                run_benchmark $mode $selectivity $drivers $repeat
            done
        done
    done
done

echo ""
echo "============================================="
echo "Benchmark completed at $(date)"
echo "Results saved to: $RESULTS_FILE"

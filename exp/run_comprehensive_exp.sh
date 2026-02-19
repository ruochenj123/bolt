#!/bin/bash
# Comprehensive Hybrid Join Experiment
# Configurations: baseline, hybrid-opt, hybrid-non-opt
# Selectivities: 10%, 30%, 60%, 90%
# Drivers: 1, 4
# Repeats: 3 for baseline/hybrid-opt, 1 for hybrid-non-opt

set -e

BENCHMARK="/home/jiang.2091/velox_join/bolt/_build/Release/bolt/benchmarks/tpch/bolt_tpch_benchmark"
DATA_DIR="/home/jiang.2091/velox_join/data/join_benchmark_v2"
OUTPUT_DIR="/home/jiang.2091/velox_join/bolt/exp/results_$(date +%Y%m%d_%H%M%S)"

mkdir -p "$OUTPUT_DIR/raw"

SELECTIVITIES=(10 30 60 90)
DRIVERS=(1 4)

# CSV header
echo "config,selectivity,drivers,repeat,total_time_s,build_cpu_s,build_i_s,build_f_s,probe_cpu_s,probe_i_s,probe_o_s,hashjoin_cpu_s" > "$OUTPUT_DIR/results.csv"

parse_result() {
    local file=$1
    local config=$2
    local sel=$3
    local drv=$4
    local rep=$5
    
    # Extract timing values from the output format
    # Execution time: 43.43s
    local total_time=$(grep "Execution time:" "$file" | awk '{print $3}' | sed 's/s//')
    
    # HashBuild: ... Cpu time: 24.41s, ... CPU breakdown: B/I/O/F (10.26ms/18.28s/8.13ms/6.11s)
    local build_line=$(grep "HashBuild:" "$file")
    local build_cpu=$(echo "$build_line" | grep -oP 'Cpu time: \K[0-9.]+')
    local build_breakdown=$(echo "$build_line" | grep -oP 'CPU breakdown: B/I/O/F \(\K[^)]+')
    local build_i=$(echo "$build_breakdown" | cut -d'/' -f2 | sed 's/s$//' | sed 's/ms$/e-3/' | sed 's/us$/e-6/' | sed 's/ns$/e-9/')
    local build_f=$(echo "$build_breakdown" | cut -d'/' -f4 | sed 's/s$//' | sed 's/ms$/e-3/' | sed 's/us$/e-6/' | sed 's/ns$/e-9/')
    
    # HashProbe: ... Cpu time: 1.25s, ... CPU breakdown: B/I/O/F (57.82ms/354.16ms/819.74ms/15.82ms)
    local probe_line=$(grep "HashProbe:" "$file")
    local probe_cpu=$(echo "$probe_line" | grep -oP 'Cpu time: \K[0-9.]+')
    local probe_breakdown=$(echo "$probe_line" | grep -oP 'CPU breakdown: B/I/O/F \(\K[^)]+')
    local probe_i=$(echo "$probe_breakdown" | cut -d'/' -f2 | sed 's/s$//' | sed 's/ms$/e-3/' | sed 's/us$/e-6/' | sed 's/ns$/e-9/')
    local probe_o=$(echo "$probe_breakdown" | cut -d'/' -f3 | sed 's/s$//' | sed 's/ms$/e-3/' | sed 's/us$/e-6/' | sed 's/ns$/e-9/')
    
    # Convert scientific notation to decimal if needed
    build_i=$(echo "$build_i" | awk '{printf "%.4f", $1}')
    build_f=$(echo "$build_f" | awk '{printf "%.4f", $1}')
    probe_i=$(echo "$probe_i" | awk '{printf "%.4f", $1}')
    probe_o=$(echo "$probe_o" | awk '{printf "%.4f", $1}')
    
    local hashjoin_cpu=$(echo "$build_cpu + $probe_cpu" | bc 2>/dev/null || echo "0")
    
    # Handle missing values
    [ -z "$total_time" ] && total_time="NA"
    [ -z "$build_cpu" ] && build_cpu="NA"
    [ -z "$build_i" ] && build_i="NA"
    [ -z "$build_f" ] && build_f="NA"
    [ -z "$probe_cpu" ] && probe_cpu="NA"
    [ -z "$probe_i" ] && probe_i="NA"
    [ -z "$probe_o" ] && probe_o="NA"
    [ -z "$hashjoin_cpu" ] && hashjoin_cpu="NA"
    
    echo "$config,$sel,$drv,$rep,$total_time,$build_cpu,$build_i,$build_f,$probe_cpu,$probe_i,$probe_o,$hashjoin_cpu" >> "$OUTPUT_DIR/results.csv"
}

run_benchmark() {
    local config=$1
    local sel=$2
    local drv=$3
    local rep=$4
    local output_file="$OUTPUT_DIR/raw/${config}_sel${sel}_drv${drv}_rep${rep}.txt"
    
    echo "Running: $config | Selectivity: ${sel}% | Drivers: $drv | Repeat: $rep"
    
    # Set flags based on config
    local hybrid_flag="false"
    local opt_flag="true"
    
    if [ "$config" == "baseline" ]; then
        hybrid_flag="false"
        opt_flag="true"
    elif [ "$config" == "hybrid-opt" ]; then
        hybrid_flag="true"
        opt_flag="true"
    elif [ "$config" == "hybrid-non-opt" ]; then
        hybrid_flag="true"
        opt_flag="false"
    fi
    
    # Run benchmark
    "$BENCHMARK" \
        --data_path="$DATA_DIR" \
        --data_format=parquet \
        --run_query_verbose=30 \
        --q30_probe_selectivity="$sel" \
        --num_drivers="$drv" \
        --num_splits_per_file=1 \
        --hybrid_join_enabled="$hybrid_flag" \
        --hybrid_join_extraction_optimized="$opt_flag" \
        2>&1 | tee "$output_file"
    
    # Parse and append to CSV
    parse_result "$output_file" "$config" "$sel" "$drv" "$rep"
    
    echo "----------------------------------------"
}

echo "============================================"
echo "Starting Comprehensive Hybrid Join Experiment"
echo "Output directory: $OUTPUT_DIR"
echo "============================================"

# Run baseline (3 repeats)
echo ""
echo "=== BASELINE ==="
for sel in "${SELECTIVITIES[@]}"; do
    for drv in "${DRIVERS[@]}"; do
        for rep in 1 2 3; do
            run_benchmark "baseline" "$sel" "$drv" "$rep"
        done
    done
done

# Run hybrid-opt (3 repeats)
echo ""
echo "=== HYBRID-OPT ==="
for sel in "${SELECTIVITIES[@]}"; do
    for drv in "${DRIVERS[@]}"; do
        for rep in 1 2 3; do
            run_benchmark "hybrid-opt" "$sel" "$drv" "$rep"
        done
    done
done

# Run hybrid-non-opt (1 repeat only - it's very slow)
echo ""
echo "=== HYBRID-NON-OPT (1 repeat only) ==="
for sel in "${SELECTIVITIES[@]}"; do
    for drv in "${DRIVERS[@]}"; do
        run_benchmark "hybrid-non-opt" "$sel" "$drv" "1"
    done
done

echo ""
echo "============================================"
echo "Experiment Complete!"
echo "Results CSV: $OUTPUT_DIR/results.csv"
echo "Raw outputs: $OUTPUT_DIR/raw/"
echo "============================================"

# Print summary
echo ""
echo "=== RESULTS SUMMARY ==="
cat "$OUTPUT_DIR/results.csv" | column -t -s','

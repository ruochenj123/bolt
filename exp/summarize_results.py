#!/usr/bin/env python3
import csv
from collections import defaultdict

# Read raw results
data = defaultdict(lambda: defaultdict(list))

with open('/home/jiang.2091/velox_join/bolt/exp/q29_benchmark_results.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        key = (row['mode'], int(row['sort_keys']), int(row['proj_cols']))
        data[key]['addInput'].append(float(row['sort_I_ms']))
        data[key]['getOutput'].append(float(row['sort_O_ms']))
        data[key]['sortTime'].append(float(row['sort_F_s']))
        data[key]['cpuTime'].append(float(row['sort_cpu_s']))
        data[key]['execTime'].append(float(row['exec_time_s']))

# Write summary
with open('/home/jiang.2091/velox_join/bolt/exp/q29_summary.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['mode', 'sort_keys', 'proj_cols', 'addInput_ms', 'getOutput_ms', 'sortTime_s', 'cpuTime_s', 'execTime_s'])
    
    for (mode, sort_keys, proj_cols) in sorted(data.keys(), key=lambda x: (x[1], x[2], ['baseline', 'hybrid-opt', 'hybrid-nonopt'].index(x[0]))):
        d = data[(mode, sort_keys, proj_cols)]
        writer.writerow([
            mode,
            sort_keys,
            proj_cols,
            f"{sum(d['addInput'])/len(d['addInput']):.2f}",
            f"{sum(d['getOutput'])/len(d['getOutput']):.2f}",
            f"{sum(d['sortTime'])/len(d['sortTime']):.3f}",
            f"{sum(d['cpuTime'])/len(d['cpuTime']):.2f}",
            f"{sum(d['execTime'])/len(d['execTime']):.2f}"
        ])

print("Summary saved to q29_summary.csv")

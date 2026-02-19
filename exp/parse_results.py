#!/usr/bin/env python3
"""Parse and summarize hybrid join benchmark results."""

import os
import re
import csv

RAW_DIR = "/home/jiang.2091/velox_join/bolt/exp/results_20260218_164128/raw"
OUTPUT_CSV = "/home/jiang.2091/velox_join/bolt/exp/results_20260218_164128/results_clean.csv"

def parse_time(time_str):
    """Parse time string like '1m 23s', '45.5s', '500ms' to seconds."""
    if not time_str:
        return None
    
    # Handle "1m 23s" format
    m = re.match(r'(\d+)m\s*(\d+)?s?', time_str)
    if m:
        minutes = int(m.group(1))
        seconds = int(m.group(2)) if m.group(2) else 0
        return minutes * 60 + seconds
    
    # Handle "45.5s" format
    m = re.match(r'([\d.]+)s', time_str)
    if m:
        return float(m.group(1))
    
    # Handle "500ms" format
    m = re.match(r'([\d.]+)ms', time_str)
    if m:
        return float(m.group(1)) / 1000
    
    # Handle plain number (assume seconds)
    try:
        return float(time_str)
    except:
        return None

def parse_file(filepath):
    """Parse a single raw output file."""
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Check for crash
    if 'Aborted' in content or 'SIGSEGV' in content:
        return {'error': 'CRASH'}
    
    result = {}
    
    # Execution time: 43.43s or 1m 23s
    m = re.search(r'Execution time:\s*(\S+(?:\s+\d+s)?)', content)
    if m:
        result['total_time'] = parse_time(m.group(1))
    
    # HashBuild line
    build_match = re.search(r'HashBuild:.*?Cpu time:\s*([\d.]+)s.*?CPU breakdown: B/I/O/F \(([^)]+)\)', content)
    if build_match:
        result['build_cpu'] = float(build_match.group(1))
        breakdown = build_match.group(2).split('/')
        result['build_i'] = parse_time(breakdown[1]) if len(breakdown) > 1 else None
        result['build_f'] = parse_time(breakdown[3]) if len(breakdown) > 3 else None
    
    # HashProbe line - handle both seconds and minutes format for Cpu time
    probe_match = re.search(r'HashProbe:.*?Cpu time:\s*(\d+m\s*\d+s|[\d.]+s).*?CPU breakdown: B/I/O/F \(([^)]+)\)', content)
    if probe_match:
        result['probe_cpu'] = parse_time(probe_match.group(1))
        breakdown = probe_match.group(2).split('/')
        result['probe_i'] = parse_time(breakdown[1]) if len(breakdown) > 1 else None
        result['probe_o'] = parse_time(breakdown[2]) if len(breakdown) > 2 else None
    
    if 'build_cpu' in result and 'probe_cpu' in result:
        result['hashjoin_cpu'] = result['build_cpu'] + result['probe_cpu']
    
    return result

def main():
    rows = []
    
    for filename in sorted(os.listdir(RAW_DIR)):
        if not filename.endswith('.txt'):
            continue
        
        # Parse filename: config_selXX_drvX_repX.txt
        m = re.match(r'(\w+(?:-\w+)?)-?_sel(\d+)_drv(\d+)_rep(\d+)\.txt', filename)
        if not m:
            m = re.match(r'([\w-]+)_sel(\d+)_drv(\d+)_rep(\d+)\.txt', filename)
            if not m:
                continue
        
        config = m.group(1)
        sel = int(m.group(2))
        drv = int(m.group(3))
        rep = int(m.group(4))
        
        filepath = os.path.join(RAW_DIR, filename)
        result = parse_file(filepath)
        
        row = {
            'config': config,
            'selectivity': sel,
            'drivers': drv,
            'repeat': rep,
            'total_time_s': result.get('total_time', 'NA'),
            'build_cpu_s': result.get('build_cpu', 'NA'),
            'build_i_s': result.get('build_i', 'NA'),
            'build_f_s': result.get('build_f', 'NA'),
            'probe_cpu_s': result.get('probe_cpu', 'NA'),
            'probe_i_s': result.get('probe_i', 'NA'),
            'probe_o_s': result.get('probe_o', 'NA'),
            'hashjoin_cpu_s': result.get('hashjoin_cpu', 'NA'),
            'error': result.get('error', '')
        }
        rows.append(row)
    
    # Write CSV
    fieldnames = ['config', 'selectivity', 'drivers', 'repeat', 'total_time_s', 
                  'build_cpu_s', 'build_i_s', 'build_f_s', 'probe_cpu_s', 
                  'probe_i_s', 'probe_o_s', 'hashjoin_cpu_s', 'error']
    
    with open(OUTPUT_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Wrote {len(rows)} results to {OUTPUT_CSV}")
    
    # Print summary
    print("\n=== SUMMARY (averaged over repeats) ===")
    from collections import defaultdict
    summary = defaultdict(lambda: defaultdict(list))
    
    for row in rows:
        if row['hashjoin_cpu_s'] != 'NA' and row['error'] == '':
            key = (row['config'], row['selectivity'], row['drivers'])
            summary[key]['hashjoin_cpu'].append(float(row['hashjoin_cpu_s']))
            summary[key]['build_i'].append(float(row['build_i_s']))
            summary[key]['probe_o'].append(float(row['probe_o_s']))
    
    print(f"{'Config':<20} {'Sel%':<6} {'Drv':<4} {'HashJoin CPU':>12} {'Build I':>10} {'Probe O':>10}")
    print("-" * 75)
    
    for key in sorted(summary.keys()):
        config, sel, drv = key
        hj = sum(summary[key]['hashjoin_cpu']) / len(summary[key]['hashjoin_cpu'])
        bi = sum(summary[key]['build_i']) / len(summary[key]['build_i'])
        po = sum(summary[key]['probe_o']) / len(summary[key]['probe_o'])
        print(f"{config:<20} {sel:<6} {drv:<4} {hj:>12.2f} {bi:>10.2f} {po:>10.2f}")

if __name__ == '__main__':
    main()

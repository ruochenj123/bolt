#!/usr/bin/env python3
"""
Create R and S tables for join benchmark with 4 join keys.

4 Join keys: row_id, l_suppkey, l_returnflag, l_linestatus

R table: 100M rows - 4 join keys + lineitem payload
S table: 200M rows - 4 join keys + s_orderkey

Match ratio control:
- For matched S rows: copy all 4 join key values from R[s_idx % r_rows]
- For unmatched S rows: row_id = -1 (won't match any R row)

Example with 50% match on 200M S rows:
- 100M matched rows: copy join keys from R, each R row matched once on average
- 100M unmatched rows: row_id = -1, won't join
"""

import argparse
import os
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import date


def create_r_schema():
    """R table schema: 4 join keys + lineitem payload."""
    return pa.schema([
        # 4 Join keys
        ('row_id', pa.int64()),
        ('l_suppkey', pa.int64()),
        ('l_returnflag', pa.string()),
        ('l_linestatus', pa.string()),
        # Payload columns
        ('l_orderkey', pa.int64()),
        ('l_partkey', pa.int64()),
        ('l_linenumber', pa.int64()),
        ('l_quantity', pa.float64()),
        ('l_extendedprice', pa.float64()),
        ('l_discount', pa.float64()),
        ('l_tax', pa.float64()),
        ('l_shipdate', pa.date32()),
        ('l_commitdate', pa.date32()),
        ('l_receiptdate', pa.date32()),
        ('l_shipinstruct', pa.string()),
        ('l_shipmode', pa.string()),
        ('l_comment', pa.string()),
    ])


def create_s_schema():
    """S table schema: 4 join keys + probe payload."""
    return pa.schema([
        # 4 Join keys (same as R)
        ('row_id', pa.int64()),
        ('l_suppkey', pa.int64()),
        ('l_returnflag', pa.string()),
        ('l_linestatus', pa.string()),
        # Probe-side payload
        ('s_orderkey', pa.int64()),
    ])


def generate_r_table(output_dir: str, num_rows: int, batch_size: int = 1000000):
    """Generate R table and return join key arrays for S generation."""
    print(f"Generating R table with {num_rows:,} rows...")
    
    schema = create_r_schema()
    output_path = os.path.join(output_dir, 'R', 'data.parquet')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Store join keys for S generation
    all_suppkeys = np.empty(num_rows, dtype=np.int64)
    all_returnflags = []
    all_linestatuses = []
    
    writer = pq.ParquetWriter(output_path, schema, compression='snappy')
    
    # Sample values for string columns
    return_flags = ['A', 'N', 'R']
    line_statuses = ['F', 'O']
    ship_instructs = ['DELIVER IN PERSON', 'COLLECT COD', 'NONE', 'TAKE BACK RETURN']
    ship_modes = ['REG AIR', 'AIR', 'RAIL', 'SHIP', 'TRUCK', 'MAIL', 'FOB']
    
    base_date = date(1992, 1, 1).toordinal()
    date_range = 2556  # ~7 years
    
    rows_written = 0
    while rows_written < num_rows:
        batch_rows = min(batch_size, num_rows - rows_written)
        
        # Generate join key values
        row_ids = np.arange(rows_written, rows_written + batch_rows, dtype=np.int64)
        suppkeys = np.random.randint(1, 1000001, batch_rows, dtype=np.int64)
        returnflags = np.random.choice(return_flags, batch_rows)
        linestatuses = np.random.choice(line_statuses, batch_rows)
        
        # Store for S generation
        all_suppkeys[rows_written:rows_written+batch_rows] = suppkeys
        all_returnflags.extend(returnflags.tolist())
        all_linestatuses.extend(linestatuses.tolist())
        
        batch = pa.table({
            # 4 Join keys
            'row_id': row_ids,
            'l_suppkey': suppkeys,
            'l_returnflag': pa.array(returnflags),
            'l_linestatus': pa.array(linestatuses),
            # Payload
            'l_orderkey': np.random.randint(1, 150000001, batch_rows, dtype=np.int64),
            'l_partkey': np.random.randint(1, 20000001, batch_rows, dtype=np.int64),
            'l_linenumber': np.random.randint(1, 8, batch_rows, dtype=np.int64),
            'l_quantity': np.random.uniform(1, 50, batch_rows),
            'l_extendedprice': np.random.uniform(900, 105000, batch_rows),
            'l_discount': np.random.uniform(0, 0.1, batch_rows),
            'l_tax': np.random.uniform(0, 0.08, batch_rows),
            'l_shipdate': pa.array(
                (base_date + np.random.randint(0, date_range, batch_rows)).astype('datetime64[D]')
            ),
            'l_commitdate': pa.array(
                (base_date + np.random.randint(0, date_range, batch_rows)).astype('datetime64[D]')
            ),
            'l_receiptdate': pa.array(
                (base_date + np.random.randint(0, date_range, batch_rows)).astype('datetime64[D]')
            ),
            'l_shipinstruct': pa.array(np.random.choice(ship_instructs, batch_rows)),
            'l_shipmode': pa.array(np.random.choice(ship_modes, batch_rows)),
            'l_comment': pa.array([f'comment_{i}' for i in range(rows_written, rows_written + batch_rows)]),
        }, schema=schema)
        
        writer.write_table(batch)
        rows_written += batch_rows
        
        if rows_written % 10000000 == 0:
            print(f"  R: {rows_written:,} / {num_rows:,} rows written")
    
    writer.close()
    
    file_size = os.path.getsize(output_path)
    print(f"R table created: {output_path}")
    print(f"  Rows: {num_rows:,}, Size: {file_size / (1024**3):.2f} GB")
    
    return {
        'suppkeys': all_suppkeys,
        'returnflags': all_returnflags,
        'linestatuses': all_linestatuses,
    }


def generate_s_table(output_dir: str, num_rows: int, r_rows: int, 
                     match_pct: int, r_join_keys: dict, batch_size: int = 1000000):
    """
    Generate S table with 4 join keys.
    
    For matched rows: copy join keys from R[s_idx % r_rows]
    For unmatched rows: row_id = -1 (won't match)
    """
    print(f"Generating S table with {num_rows:,} rows, {match_pct}% match rate...")
    
    schema = create_s_schema()
    output_path = os.path.join(output_dir, 'S', 'data.parquet')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    writer = pq.ParquetWriter(output_path, schema, compression='snappy')
    
    matched_count = int(num_rows * match_pct / 100)
    print(f"  Matched rows: {matched_count:,}, Unmatched rows: {num_rows - matched_count:,}")
    
    r_suppkeys = r_join_keys['suppkeys']
    r_returnflags = r_join_keys['returnflags']
    r_linestatuses = r_join_keys['linestatuses']
    
    rows_written = 0
    while rows_written < num_rows:
        batch_rows = min(batch_size, num_rows - rows_written)
        
        row_ids = np.empty(batch_rows, dtype=np.int64)
        suppkeys = np.empty(batch_rows, dtype=np.int64)
        returnflags = []
        linestatuses = []
        
        for i in range(batch_rows):
            s_idx = rows_written + i
            if s_idx < matched_count:
                # Matched: copy join keys from R
                r_idx = s_idx % r_rows
                row_ids[i] = r_idx  # row_id matches R's row_id
                suppkeys[i] = r_suppkeys[r_idx]
                returnflags.append(r_returnflags[r_idx])
                linestatuses.append(r_linestatuses[r_idx])
            else:
                # Unmatched: row_id = -1 won't match any R row
                row_ids[i] = -1
                suppkeys[i] = -1
                returnflags.append('X')
                linestatuses.append('X')
        
        batch = pa.table({
            'row_id': row_ids,
            'l_suppkey': suppkeys,
            'l_returnflag': pa.array(returnflags),
            'l_linestatus': pa.array(linestatuses),
            's_orderkey': np.random.randint(1, 150000001, batch_rows, dtype=np.int64),
        }, schema=schema)
        
        writer.write_table(batch)
        rows_written += batch_rows
        
        if rows_written % 10000000 == 0:
            print(f"  S: {rows_written:,} / {num_rows:,} rows written")
    
    writer.close()
    
    file_size = os.path.getsize(output_path)
    print(f"S table created: {output_path}")
    print(f"  Rows: {num_rows:,}, Size: {file_size / (1024**3):.2f} GB")
    print(f"  Match rate: {match_pct}% ({matched_count:,} matched rows)")
    print(f"  Expected join output: {matched_count:,} rows")


def main():
    parser = argparse.ArgumentParser(description='Create R and S tables for join benchmark (4 join keys)')
    parser.add_argument('--r_rows', type=int, default=100_000_000,
                        help='Number of rows in R table (default: 100M)')
    parser.add_argument('--s_rows', type=int, default=200_000_000,
                        help='Number of rows in S table (default: 200M)')
    parser.add_argument('--match_pct', type=int, default=50,
                        help='Percentage of S rows that match R (default: 50)')
    parser.add_argument('--output_dir', type=str, 
                        default='/home/jiang.2091/velox_join/data/join_benchmark_v2',
                        help='Output directory for tables')
    parser.add_argument('--batch_size', type=int, default=1_000_000,
                        help='Batch size for writing (default: 1M)')
    
    args = parser.parse_args()
    
    print(f"=== Join Benchmark Table Generator (4 Join Keys) ===")
    print(f"R rows: {args.r_rows:,}")
    print(f"S rows: {args.s_rows:,}")
    print(f"Match %: {args.match_pct}%")
    print(f"Output: {args.output_dir}")
    print(f"Join keys: row_id, l_suppkey, l_returnflag, l_linestatus")
    print()
    
    os.makedirs(args.output_dir, exist_ok=True)
    
    r_join_keys = generate_r_table(args.output_dir, args.r_rows, args.batch_size)
    print()
    
    generate_s_table(args.output_dir, args.s_rows, args.r_rows, 
                     args.match_pct, r_join_keys, args.batch_size)
    
    print("\n=== Done ===")
    print(f"Tables created in: {args.output_dir}")
    print(f"  R: {args.r_rows:,} rows (build side, 4 keys + 13 payload cols)")
    print(f"  S: {args.s_rows:,} rows (probe side, 4 keys + 1 payload col)")
    print(f"  4 Join keys: row_id, l_suppkey, l_returnflag, l_linestatus")
    print(f"  Match ratio: {args.match_pct}%")
    print(f"  Expected join output: ~{int(args.s_rows * args.match_pct / 100):,} rows")


if __name__ == '__main__':
    main()

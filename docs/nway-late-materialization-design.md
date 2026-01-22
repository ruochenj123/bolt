# N-Way Join Late Materialization Design

## Overview

Generalize late materialization from single HashJoin → Sort to N-way joins:

```
Table1 JOIN Table2 ON k1 JOIN Table3 ON k2 ... → Sort on sort_keys
```

Where `sort_keys` may be a subset of any previous join keys.

## Current Limitation

Current late-m only supports:
- Single HashJoin → Sort
- Sort key must match join key
- Only build-side payload extraction

## Goals

1. Support arbitrary number of joins before Sort
2. Join keys may differ between joins
3. Sort keys can be any subset of accumulated keys
4. Extract payloads from all source tables at the end

## Architecture

### Key Insight

For a chain of N joins:
```
T1 JOIN T2 ON k1 → J1
J1 JOIN T3 ON k2 → J2
...
JN-1 JOIN TN ON kN → Final → Sort
```

Each join produces matches. We need to track:
1. **For each match row**: rowIds pointing back to original source tables
2. **Keys needed for subsequent joins/sort**: kept in row-wise RowContainer
3. **Payloads**: stay in original columnar format until final extraction

### Design: PayloadRegistry

A global registry that tracks all payload sources across the pipeline:

```cpp
/// PayloadRegistry: Tracks payload columns from all source operators.
/// Each source (table scan, join result) registers its payloads with a unique sourceId.
struct PayloadRegistry {
  /// A source of payload columns (table or join output)
  struct PayloadSource {
    uint8_t sourceId;                          // Unique identifier (0-255)
    std::shared_ptr<BaseHashTable> table;      // For HashJoin build side
    std::unique_ptr<ProbePayloadContainer> probePayload; // For probe side
    std::vector<TypePtr> columnTypes;          // Column types in this source
  };
  
  /// All registered payload sources, indexed by sourceId
  std::unordered_map<uint8_t, PayloadSource> sources;
  
  /// Column projections from (sourceId, columnIndex) → outputColumnIndex
  struct FinalProjection {
    uint8_t sourceId;
    column_index_t sourceColumn;
    column_index_t outputColumn;
  };
  std::vector<FinalProjection> projections;
  
  /// Register a new payload source, returns assigned sourceId
  uint8_t registerSource(std::shared_ptr<BaseHashTable> table);
  uint8_t registerSource(std::unique_ptr<ProbePayloadContainer> probePayload);
  
  /// Extract all columns for final output
  void extractAll(const std::vector<RowIdSet>& rowIds, RowVectorPtr output);
};

/// RowIdSet: For each output row, stores rowIds for each source
struct RowIdSet {
  /// sourceId → rowId in that source
  std::unordered_map<uint8_t, uint64_t> rowIds;
};
```

### Match RowContainer Structure

For N sources, the Match RowContainer stores:
```
| sort_key_1 | sort_key_2 | ... | rowId_source_0 | rowId_source_1 | ... | rowId_source_N-1 |
```

Each `rowId_source_X` is a BIGINT that encodes position in source X's payload storage.

### Join Processing

#### First Join (Table1 JOIN Table2)

```
HashBuild (Table2 as build):
  - Store keys in RowContainer (for probing)
  - Store payloads in HybridContainer (columnar)
  - Register with PayloadRegistry: sourceId=1

HashProbe (Table1 as probe):
  - Store probe payloads in ProbePayloadContainer
  - Register with PayloadRegistry: sourceId=0
  - For each match:
    - Create match row: (join_key, rowId_source_0=probeIdx, rowId_source_1=buildIdx)
  - Pass matches to next operator
```

#### Subsequent Joins (JoinResult JOIN Table3)

```
HashBuild (Table3 as build):
  - Store keys in RowContainer
  - Store payloads in HybridContainer
  - Register with PayloadRegistry: sourceId=2

HashProbe (JoinResult as probe):
  - Probe side is already match rows from previous join
  - For each match:
    - Create new match row:
      (new_join_key, rowId_source_0, rowId_source_1, rowId_source_2=buildIdx)
    - Note: rowId_source_0/1 are copied from the probe-side match row
  - Pass matches to next operator
```

#### Sort and Final Extraction

```
OrderBy:
  - Receives match rows with all rowIds accumulated
  - Sort by sort keys (stored in match rows)
  - For each output batch:
    - Extract columns using PayloadRegistry:
      - For each output column, lookup (sourceId, sourceColumn)
      - Extract from corresponding payload source using rowId
```

## RowId Encoding

Each rowId is a 64-bit value:
```
| reserved (8 bits) | batch_id (24 bits) | row_in_batch (32 bits) |
```

This allows addressing:
- Up to 16M batches per source
- Up to 4B rows per batch
- Total: ~67 trillion rows per source (more than enough)

## Implementation Plan

### Phase 1: Refactor Current Implementation

1. Create `PayloadRegistry` class
2. Modify `DriverCtx` to use PayloadRegistry instead of current fields
3. Migrate single-join late-m to use PayloadRegistry

### Phase 2: N-Way Join Support

1. Modify `HashBuild` to register with PayloadRegistry
2. Modify `HashProbe` to:
   - Handle probe-side that's already a match row (copy existing rowIds)
   - Append new rowIds for current join's build side
3. Modify `OrderBy` to use PayloadRegistry for extraction

### Phase 3: Testing

1. Create 2-way join queries with same/different keys
2. Create 3-way join queries
3. Verify correctness against baseline

## Example: 3-Way Join

Query:
```sql
SELECT o.o_orderkey, o.o_orderdate, l.l_quantity, c.c_name
FROM orders o
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
JOIN customer c ON o.o_custkey = c.c_custkey
ORDER BY o.o_orderkey
```

Execution:
```
1. TableScan(orders) → source_0
   Payloads: [o_orderdate]  (o_orderkey, o_custkey are keys)

2. HashBuild(lineitem) → source_1
   Keys: [l_orderkey]
   Payloads: [l_quantity]

3. HashProbe(orders JOIN lineitem)
   Match row: (o_orderkey, o_custkey, rowId_0, rowId_1)
   
4. HashBuild(customer) → source_2
   Keys: [c_custkey]
   Payloads: [c_name]

5. HashProbe(J1 JOIN customer)
   Match row: (o_orderkey, rowId_0, rowId_1, rowId_2)
   
6. OrderBy (sort by o_orderkey)
   Sort match rows
   Extract:
     o_orderkey → from match row (key)
     o_orderdate → source_0[rowId_0]
     l_quantity → source_1[rowId_1]  
     c_name → source_2[rowId_2]
```

## Key Invariants

1. **Immutable payloads**: Once registered, payload sources never change
2. **RowId validity**: Each rowId is valid throughout pipeline lifetime
3. **Projection ordering**: PayloadRegistry projections define final output column order
4. **Memory lifetime**: PayloadRegistry holds shared_ptrs to keep sources alive

## Challenges

1. **Memory pressure**: N sources × M rows × rowId_size can be significant
2. **Cache locality**: Final extraction does random access to N sources
3. **Key accumulation**: Match row grows with each join (more rowId columns)

## Mitigation

1. **Selective late-m**: Only apply when payload extraction cost > accumulation cost
2. **Coalescing**: Ensure payload sources are coalesced for better extraction locality
3. **Pruning**: Remove rowId columns for sources with no payload projections

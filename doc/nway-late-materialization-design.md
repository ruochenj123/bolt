# Late Materialization Design Document

## Table of Contents
1. [Overview](#overview)
2. [Problem Statement](#problem-statement)
3. [Late Materialization Concept](#late-materialization-concept)
4. [Architecture](#architecture)
5. [Single-Join Late Materialization](#single-join-late-materialization)
6. [Direct Late-M Mode (Sort Key = Join Key)](#direct-late-m-mode-sort-key--join-key)
7. [Sort Optimization](#sort-optimization)
8. [Performance Results](#performance-results)
9. [Implementation Details](#implementation-details)
10. [Configuration](#configuration)
11. [Limitations](#limitations)
12. [File Summary](#file-summary)
13. [Appendix: N-Way Late-M (Removed)](#appendix-n-way-late-m-removed)

---

## Overview

Late materialization is a query execution optimization technique that **defers the extraction of payload columns until they are actually needed**. Instead of materializing all output columns at each operator, we only pass lightweight row identifiers (pointers or indices) through the pipeline and extract the actual column data at the final output stage.

This design document covers:
- The core late materialization concept and its benefits
- Single-join and N-way (multi-join) late materialization
- **Direct late-m mode**: A specialized optimization when sort key equals join key
- **Sort optimization**: Caching sort keys to achieve O(1) comparisons

---

## Problem Statement

### Traditional Execution (Hybrid Join without Late-M)

In a typical HashJoin → OrderBy pipeline:

\`\`\`
TableScan(orders) → HashBuild
TableScan(lineitem) → HashProbe → [Full Output: 18M rows × 25 columns] → OrderBy → Output
\`\`\`

**Problems:**
1. **Memory Explosion**: OrderBy must buffer ALL 18M rows with ALL columns (e.g., 7-8GB for TPC-H Q29 at SF3)
2. **Redundant Copying**: Columns are copied from HashTable to output vectors, then copied again to OrderBy's RowContainer
3. **Cache Inefficiency**: Sorting operates on large row structures with many unused columns

### Example: TPC-H Q29 (SF3)
\`\`\`sql
SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
       o_orderpriority, o_clerk, o_shippriority, o_comment,
       l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
       l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
       l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment
FROM orders JOIN lineitem ON o_orderkey = l_orderkey
ORDER BY o_orderkey
\`\`\`

- **18M output rows** (lineitem count, since 1:N join)
- **25 columns** (9 from orders + 16 from lineitem)
- **~7GB OrderBy memory** in hybrid-only mode

---

## Late Materialization Concept

### Core Idea

Instead of passing full rows through operators, pass **lightweight references**:

\`\`\`
HashProbe outputs: [buildRowPointer, probeRowId]  -- 16 bytes per row
Instead of:        [all 25 columns]               -- ~400+ bytes per row
\`\`\`

**Defer extraction**: Only when OrderBy outputs to the client do we extract actual column values.

### Benefits

| Aspect | Hybrid-Only | Late Materialization |
|--------|-------------|----------------------|
| OrderBy Memory | ~7-8 GB | ~4 MB |
| Data Copied | Full rows at each step | Only row IDs, extract once at end |
| Sort Comparison | Compare full RowContainer rows | Compare cached sort keys |
| Cache Efficiency | Poor (large row structures) | Excellent (compact structs) |

### Key Insight

For \`HashJoin → OrderBy\` patterns where **sort key = join key**:
- The sort key is already stored in the HashTable's build side
- We don't need to copy it anywhere - just sort the build row pointers
- Extract columns only for the final sorted output

---

## Architecture

### Data Flow Comparison

**Traditional (Hybrid-Only):**
\`\`\`
TableScan → HashBuild ──────────────────┐
                                        ↓
TableScan → HashProbe → [Full 18M rows × 25 cols] → OrderBy (7GB buffer) → Output
\`\`\`

**Late Materialization:**
\`\`\`
TableScan → HashBuild (keeps rows in HashTable) ──┐
                                                  │
TableScan → HashProbe → [18M (buildRow*, probeId)] → OrderBy (4MB) → Extract → Output
                ↑                                         │              │
                └── ProbePayloadContainer ────────────────┘              │
                    (deferred probe extraction)                          ↓
                                                              PayloadRegistry
                                                              (extracts from both sides)
\`\`\`

### Key Components

1. **BuildProbeMatch** - Compact struct for join matches:
   \`\`\`cpp
   struct BuildProbeMatch {
     char* buildRow;      // Pointer to row in HashTable's RowContainer
     uint64_t probeRowId; // Index into ProbePayloadContainer
     int64_t sortKey;     // Cached sort key for O(1) comparison
   };
   \`\`\`

2. **ProbePayloadContainer** - Stores probe-side columns:
   - Accumulates probe input columns during HashProbe
   - Provides efficient column extraction by rowId

3. **PayloadRegistry** - Central extraction coordinator:
   - Registers BUILD_SIDE sources (HybridContainer from HashTable)
   - Registers PROBE_SIDE sources (ProbePayloadContainer)
   - Maps output columns to their sources
   - Extracts columns using rowIds

4. **SortBuffer** - Optimized for late-m:
   - Direct mode: Sorts BuildProbeMatch structs
   - Cached sortKey enables O(1) comparisons
   - Extracts columns only during output

---

## Single-Join Late Materialization

### Activation Conditions

Single-join late-m is enabled when:
- \`lateMaterializationEnabled = true\`
- \`hybridJoinEnabled = true\`
- Single HashJoin in pipeline
- OrderBy/Sort downstream

### Flow

\`\`\`cpp
// HashProbe::getOutput()
if (singleJoinLateMEnabled_) {
  // Don't extract columns - just record matches
  for (match in matches) {
    outputBuildRows.push_back(match.buildRow);
    outputProbeRowIds.push_back(probeRowIdBase + match.probeIndex);
  }
  // Accumulate probe payload
  probePayloadContainer_->append(probeInput);
}

// HashProbe::noMoreInput()
// Transfer to SortBuffer via setLateMaterializationData()
\`\`\`

---

## N-Way Late Materialization

### ⚠️ Status: REMOVED

The probe-side N-way late-m code has been **completely removed** from the codebase.

**Reason**: The probe-side N-way approach caused severe performance degradation (18x slower) due to:
1. Key re-extraction overhead from PayloadRegistry for each batch
2. Row expansion to track all 1:N matches
3. Repeated hash table probing

**Current Approach**: Use **single-join late-m** which works per-pipeline. For multi-join queries like TPC-H Q27, each pipeline has at most one join, so single-join late-m applies naturally.

---

### Recommended: Pattern 1 (Join Result as Build Side)

For multi-table joins, use **Pattern 1** where join results become the build side of subsequent joins:

```sql
-- Q27 with Pattern 1
SELECT c_custkey, c_name, o_totalprice, l_quantity
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON o_orderkey = l_orderkey
ORDER BY c_custkey
```

**Plan structure (Pattern 1)**:
```
Pipeline 2: orders (probe) × customer (build) → result goes to HashBuild[1]
Pipeline 1: lineitem (probe) × (orders×customer) (build) → OrderBy
```

**Each pipeline has exactly 1 join**, so **single-join late-m applies**:
- Pipeline 1: Single-join late-m defers build-side columns at HashProbe[1]
- Speedup: **1.29x** at SF30 (69s → 53.4s)

### Pattern Comparison

| Pattern | Join Structure | Single-join Late-M |
|---------|---------------|-------------------|
| **Pattern 1** (Recommended) | `A×B → (A×B)×C` | ✅ Yes, per-pipeline |
| Pattern 2 (Probe streaming) | `A × B × C` in same pipeline | ⚠️ Disabled (would need N-way) |

### Why Pattern 1 Works

1. **Standard TPC-H pattern**: Q3, Q5, Q10 all use Pattern 1
2. **Smaller build side**: Join results (filtered) are typically smaller than raw tables
3. **Single-join per pipeline**: Each HashProbe benefits from late-m independently
4. **No expansion overhead**: Build-side rows are matched multiple times without duplication

---

### Legacy: Probe-Side N-Way Design (Disabled)

The following documents the original probe-side N-way approach for reference.
It is currently disabled in `LocalPlanner.cpp` (`ctx->nWayLateMEnabled = false`).

#### Original Use Case: Q27 (Probe-Side, Disabled)

```sql
SELECT c_custkey, c_name, o_totalprice, l_quantity
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON o_orderkey = l_orderkey
ORDER BY c_custkey
```

**Join Cardinalities:**
- customer × orders: 450K × 4.5M → **4.5M rows** (1:N, each customer has ~10 orders)
- (customer×orders) × lineitem: 4.5M × 18M → **18M rows** (1:N, each order has ~4 lineitems)

#### Original Data Flow (Probe-Side)

```
TableScan(customer) → HashBuild₀ ──────────────────────────────────────┐
                                                                       ↓
TableScan(orders) → HashProbe₀ (join 0) → [LateMState: 4.5M rows] ────┤
                          ↓                                            │
                    registerBuildSource(0)                             │
                    registerProbeSource(0)                             │
                                                                       ↓
TableScan(lineitem) → HashBuild₁ ──────────────────────────────────────┤
                                                                       ↓
[LateMState] → HashProbe₁ (join 1) → [LateMState: 18M rows] ──────────┤
                          ↓                                            │
                    registerBuildSource(1)                             │
                                                                       ↓
                    OrderBy/Sort ← PayloadRegistry.extractAll() ←──────┘
```

#### LateMState Structure (Reference)

```cpp
class LateMState {
  // Number of joins in the pipeline
  int numJoins_;                              // e.g., 2 for Q27
  
  // Total rows (may grow due to 1:N expansion)
  int64_t numRows_;                           // 4.5M → 18M after join 1
  
  // Row IDs: [b0, p0, b1, p1, ...] per row
  // bi = buildRowId for join i
  // pi = probeRowId for join i (0 if not used)
  std::vector<uint64_t> rowIdStorage_;        // size = numRows * numJoins * 2
  
  // Bitmap: which rows survived all joins
  SelectivityVector aliveRows_;
  
  // Reference to extraction sources
  std::unique_ptr<PayloadRegistry> payloadRegistry_;
};
```

**Example for Q27 with 2 joins:**
```
rowIdStorage_ layout (stride = 4 uint64_t per row):
Row 0: [build0_rowId, probe0_rowId, build1_rowId, probe1_rowId]
Row 1: [build0_rowId, probe0_rowId, build1_rowId, probe1_rowId]
...
Row N: [build0_rowId, probe0_rowId, build1_rowId, probe1_rowId]
```

#### Row ID Encoding

```
buildRowId: driverId(8 bits) | globalRowIndex(56 bits)
```

- **driverId**: Top 8 bits identify which driver (for multi-driver parallelism)
- **globalRowIndex**: Bottom 56 bits are the insertion index in HybridContainer

This encoding allows extracting columns from the correct driver's HybridContainer.

---

## The 1:N Expansion Problem (Probe-Side, Reference)

### What is Expansion?

In a **1:N join**, one probe row matches multiple build rows. For example:
- `orders × lineitem`: 1 order → ~4 lineitems
- 4.5M orders → 18M lineitem matches

**Without expansion tracking**, we'd lose the association between the original order row and its multiple lineitem matches.

### How Expansion Works

When `processLateMStateInput()` probes lineitem for each order:

```
Original LateMState (after join 0): 4.5M rows
┌─────┬────────────────────┬────────────────────┐
│ Row │ (build0, probe0)   │ (build1, probe1)   │
├─────┼────────────────────┼────────────────────┤
│  0  │ (cust_rowId, ord0) │ (unset, 0)         │
│  1  │ (cust_rowId, ord1) │ (unset, 0)         │
│  2  │ (cust_rowId, ord2) │ (unset, 0)         │
│ ... │       ...          │       ...          │
└─────┴────────────────────┴────────────────────┘
```

Now when we probe lineitem for order row 0:
- Order 0 (`o_orderkey=1`) matches 4 lineitems
- We need to **expand** row 0 into 4 rows, each with a different `build1_rowId`

```
After expansion for row 0:
┌─────┬────────────────────┬────────────────────┐
│ Row │ (build0, probe0)   │ (build1, probe1)   │
├─────┼────────────────────┼────────────────────┤
│  0  │ (cust_rowId, ord0) │ (lineitem0, 0)     │  ← First match (reuse original row)
│  1  │ (cust_rowId, ord1) │ (unset, 0)         │
│  2  │ (cust_rowId, ord2) │ (unset, 0)         │
│ ... │       ...          │       ...          │
│4.5M │ (cust_rowId, ord0) │ (lineitem1, 0)     │  ← EXPANDED: copy of row 0
│4.5M+1│(cust_rowId, ord0) │ (lineitem2, 0)     │  ← EXPANDED: copy of row 0
│4.5M+2│(cust_rowId, ord0) │ (lineitem3, 0)     │  ← EXPANDED: copy of row 0
└─────┴────────────────────┴────────────────────┘
```

### Expansion Algorithm

```cpp
// processLateMStateInput() in HashProbe.cpp

for each original row in LateMState:
    // Extract probe key from PayloadRegistry (e.g., o_orderkey)
    probeKey = registry.extractColumn(sourceId, rowId, keyColumn);
    
    // Probe hash table (lineitem)
    matches = hashTable.probe(probeKey);
    
    if matches.empty():
        markDead(row);  // Inner join: no match = filtered out
    else:
        // First match: set buildRowId on original row
        setRowIdPair(row, joinIndex, matches[0].buildRowId, 0);
        
        // Additional matches: EXPAND by creating new rows
        for m = 1 to matches.size() - 1:
            newRow = lateMState.addRow();
            // Copy all previous joins' rowId pairs
            for j = 0 to joinIndex - 1:
                newRow.setRowIdPair(j, originalRow.getRowIdPair(j));
            // Set this join's buildRowId
            newRow.setRowIdPair(joinIndex, matches[m].buildRowId, 0);
```

### Expansion Count

For Q27 at SF3:
- Join 0 (customer × orders): 450K customers → 4.5M orders (10x expansion)
- Join 1 (orders × lineitem): 4.5M orders → 18M lineitems (4x expansion)

**Total expansions in join 1**: 18M - 4.5M = **13.5 million new rows created**

---

## Batch Expansion Optimization

The naive expansion (one `addRow()` per match) is slow. We use **batched expansion**:

```cpp
int64_t LateMState::batchExpand(
    const int64_t* sourceRowIndices,  // Original rows
    const int32_t* numExpansions,     // How many matches each row has
    int64_t count,
    int joinIndex,
    const uint64_t* buildRowIds,      // All buildRowIds to set
    int64_t totalBuildRowIds) {
    
  // 1. Calculate total new rows needed
  int64_t totalNewRows = sum(numExpansions[i] - 1);  // -1 because first match reuses original
  
  // 2. Pre-allocate all storage at once
  numRows_ = oldNumRows + totalNewRows;
  rowIdStorage_.resize(numRows_ * numJoins_ * 2);
  aliveRows_.resize(numRows_);
  
  // 3. Process expansions with memcpy for rowId pair copying
  for each source row:
      // First match on original row
      setRowIdPair(srcRow, joinIndex, buildRowIds[idx++], 0);
      
      // Expanded rows
      for m = 1 to expansionCount:
          // memcpy previous joins' rowId pairs (fast!)
          memcpy(&rowIdStorage_[newRow * stride], 
                 &rowIdStorage_[srcRow * stride], 
                 joinIndex * 2 * sizeof(uint64_t));
          setRowIdPair(newRow, joinIndex, buildRowIds[idx++], 0);
          newRow++;
}
```

---

## PayloadRegistry: Column Source Tracking

### Purpose

PayloadRegistry tracks where each output column comes from, enabling deferred extraction.

### Source Registration

```cpp
// During join processing
registry.registerBuildSource(
    table,              // HashTable containing HybridContainer
    columnNames,        // ["c_custkey", "c_name", ...]
    columnTypes,        // [BIGINT, VARCHAR, ...]
    numKeyColumns);     // How many are keys vs payloads

registry.registerProbeSource(
    probePayloadContainer,
    columnNames,
    columnTypes);
```

### Source IDs

Sources are registered in order:
- **Source 0**: Build side of join 0 (e.g., customer)
- **Source 1**: Probe side of join 0 (e.g., orders)  
- **Source 2**: Build side of join 1 (e.g., lineitem)
- etc.

### Column Lookup

```cpp
// Find which source contains column "o_orderkey"
auto [sourceId, columnIndex] = registry.findColumnSource("o_orderkey");
// sourceId = 1 (probe0), columnIndex = 0

// Determine which rowId to use
int joinIndex = sourceId / 2;        // 0 for sources 0,1; 1 for sources 2,3
bool useBuildRowId = (sourceId % 2 == 0);
```

### Extraction

```cpp
// Extract column for given rowIds
registry.extractColumn(
    sourceId,       // Which source
    rowIds,         // Array of encoded rowIds
    numRows,
    columnIndex,    // Column within that source
    output);        // Output vector
```
  
  // Marks which rows survived all joins
  SelectivityVector aliveBitmap_;
  
  // Reference to extraction sources
  std::shared_ptr<PayloadRegistry> payloadRegistry_;
};
\`\`\`

### Row ID Encoding

\`\`\`
buildRowId: driverId(8 bits) | globalRowIndex(56 bits)
probeRowId: direct index into coalesced ProbePayloadContainer
\`\`\`

---

## Direct Late-M Mode (Sort Key = Join Key)

### Optimization Insight

When the ORDER BY key equals the JOIN key (e.g., \`ORDER BY o_orderkey\` with \`JOIN ON o_orderkey = l_orderkey\`):

1. The sort key is **already in the HashTable's build side**
2. No need for a separate RowContainer in OrderBy
3. Sort the \`BuildProbeMatch\` structs directly
4. The sort key can be **cached in the struct** for O(1) comparisons

### Detection (LocalPlanner)

\`\`\`cpp
// Check if sort key = join key
bool sortKeyMatchesJoinKey = false;
if (orderByNode && hashJoinNode) {
  auto& sortKeys = orderByNode->sortingKeys();
  auto& joinKeys = hashJoinNode->joinKeys();
  
  // Check if first sort key matches join build key
  if (sortKeys.size() >= 1 && isSameColumn(sortKeys[0], joinKeys.buildKey)) {
    sortKeyMatchesJoinKey = true;
  }
}

if (sortKeyMatchesJoinKey) {
  LOG(INFO) << "LocalPlanner: sort key = join key, enabling direct late-m mode";
  // Enable direct mode flag
}
\`\`\`

### HashProbe Direct Mode

\`\`\`cpp
// HashProbe.cpp
if (directLateMEnabled_) {
  // Collect (buildRow, probeRowId) pairs without intermediate RowContainer
  for (match in matches) {
    lateMDirectBuildRows_.push_back(match.buildRow);
    lateMDirectProbeRowIds_.push_back(currentProbeRowId_++);
  }
  
  // Accumulate probe payload
  probePayloadContainer_->append(probeInput);
}

// On noMoreInput, transfer to SortBuffer
sortBuffer->setDirectLateMaterializationData(
    std::move(lateMDirectBuildRows_),
    std::move(lateMDirectProbeRowIds_),
    std::move(probePayloadContainer_),
    lateMaterializationContainer_,  // Build side container
    probePayloadOutputTypes_,
    buildPayloadOutputChannels_);
\`\`\`

---

## Sort Optimization

### The Problem: Pointer Chasing

Traditional sort comparison requires **dereferencing row pointers**:

\`\`\`cpp
// Slow: O(N log N) pointer dereferences
sorter_.sort(matches.begin(), matches.end(),
    [rowContainer](const BuildProbeMatch& left, const BuildProbeMatch& right) {
      // Must dereference buildRow pointers to get sort key values
      return rowContainer->compare(left.buildRow, right.buildRow, 0, flags);
    });
\`\`\`

For 18M rows with ~24 comparisons per element (log₂(18M) ≈ 24):
- **432M pointer dereferences** → cache misses, slow!

### The Solution: Cached Sort Key

Cache the sort key value directly in the \`BuildProbeMatch\` struct:

\`\`\`cpp
struct BuildProbeMatch {
  char* buildRow;      // 8 bytes
  uint64_t probeRowId; // 8 bytes
  int64_t sortKey;     // 8 bytes - CACHED SORT KEY
};
\`\`\`

### Implementation

**Step 1: Cache sort key during initialization**

\`\`\`cpp
// SortBuffer::setDirectLateMaterializationData()
void SortBuffer::setDirectLateMaterializationData(
    std::vector<char*>&& buildRows,
    std::vector<uint64_t>&& probeRowIds,
    ...) {
  
  // Get sort key offset in RowContainer
  auto* buildKeys = lateMaterializationContainer_->getKeys();
  const auto sortKeyOffset = buildKeys->columnAt(0).offset();
  
  // Populate BuildProbeMatch with cached sort key
  lateMDirectMatches_.resize(numRows);
  for (size_t i = 0; i < numRows; ++i) {
    // Extract sort key from build row (O(1) per row)
    int64_t sortKey = RowContainer::valueAt<int64_t>(buildRows[i], sortKeyOffset);
    
    lateMDirectMatches_[i] = {
      buildRows[i],   // buildRow pointer
      probeRowIds[i], // probe row id
      sortKey         // cached sort key
    };
  }
}
\`\`\`

**Step 2: O(1) comparison during sort**

\`\`\`cpp
// SortBuffer::noMoreInput()
if (lateMDirectMode_ && numSortKeys == 1) {
  bool ascending = sortCompareFlags_[0].ascending;
  
  if (ascending) {
    sorter_.sort(
        lateMDirectMatches_.begin(),
        lateMDirectMatches_.end(),
        [](const BuildProbeMatch& left, const BuildProbeMatch& right) {
          return left.sortKey < right.sortKey;  // O(1) - no pointer chasing!
        });
  } else {
    sorter_.sort(
        lateMDirectMatches_.begin(),
        lateMDirectMatches_.end(),
        [](const BuildProbeMatch& left, const BuildProbeMatch& right) {
          return left.sortKey > right.sortKey;
        });
  }
}
\`\`\`

### Why This Is Fast

| Aspect | Before (pointer chasing) | After (cached sortKey) |
|--------|--------------------------|------------------------|
| Comparison | Dereference \`buildRow\`, read from RowContainer | Direct \`int64_t\` comparison |
| Memory Access | Random access (cache miss likely) | Sequential (struct is contiguous) |
| Instructions | Multiple loads + offset calculation | Single comparison |
| Cache Behavior | Poor (scattered RowContainer data) | Excellent (24-byte structs) |

**Result: Sort time reduced from 1700ms → 162ms (10x faster!)**

---

## Performance Results

### Q27 Pattern 1 (3-Table Join, SF3)

**Query**: `customer × orders × lineitem → Sort by c_custkey`

Using **Pattern 1** (join result as build side), single-join late-m applies:

| Mode | Execution Time | Speedup | OrderBy Memory |
|------|----------------|---------|----------------|
| Baseline | 7.26s | 1x | 1.13GB |
| Single-join Late-M | **3.29s** | **2.2x** | 672KB |

**Plan structure**:
```
Pipeline 2: orders × customer → 4.5M rows → HashBuild[1]
Pipeline 1: lineitem × (orders×customer) → 18M rows → OrderBy (late-m enabled)
```

### TPC-H Q29 (SF3: 18M lineitem, 4.5M orders)

| Mode | Execution Time | OrderBy Memory | Sort Time |
|------|----------------|----------------|-----------|
| Hybrid-Only | **15.6-15.8s** | **7.69 GB** | ~2.5s |
| Late-M (optimized) | **11.4-11.5s** | **4.08 MB** | **162ms** |
| **Improvement** | **~27% faster** | **1880x less** | **10x faster** |

### Timing Breakdown (Late-M, SF3)

| Phase | Time |
|-------|------|
| Sort (cached sortKey) | 162ms |
| Copy sorted rows | 69ms |
| getRowIds | 27ms |
| probeExtract | 450ms |
| buildExtract | 450ms |
| **Total extraction** | ~1s |

### Memory Comparison

\`\`\`
Hybrid-Only OrderBy:     7.69 GB (full rows buffered)
Late-M OrderBy:          4.08 MB (only BuildProbeMatch structs)
                         ↓
                    1880x memory reduction
\`\`\`

---

## Implementation Details

### LocalPlanner Detection

\`\`\`cpp
// LocalPlanner.cpp - createDriverFactoryAt()
void LocalPlanner::detectLateMOptimizations(PlanNode* root) {
  int numHashJoins = 0;
  bool hasSortOrOrderBy = false;
  int orderByIndex = -1;
  int lastJoinIndex = -1;
  
  // Traverse plan nodes...
  for (node : planNodes) {
    if (isHashJoin(node)) {
      lastJoinIndex = numHashJoins++;
    }
    if (isOrderBy(node) || isSort(node)) {
      hasSortOrOrderBy = true;
      orderByIndex = nodeIndex;
    }
  }
  
  // Check for direct late-m opportunity
  if (numHashJoins == 1 && hasSortOrOrderBy && orderByIndex > lastJoinIndex) {
    auto sortKeys = getSortKeys(orderByNode);
    auto joinBuildKey = getJoinBuildKey(hashJoinNode);
    
    if (sortKeys[0] == joinBuildKey) {
      LOG(INFO) << "sort key = join key, enabling direct late-m mode";
      directLateMEnabled = true;
    }
  }
}
\`\`\`

### Extraction Process

\`\`\`cpp
// SortBuffer::getOutputWithoutSpill()
RowVectorPtr SortBuffer::getOutputWithoutSpill() {
  if (lateMDirectMode_) {
    // Get sorted indices
    auto startIdx = currentOutputRow_;
    auto endIdx = std::min(startIdx + outputBatchSize_, numInputRows_);
    auto batchSize = endIdx - startIdx;
    
    // Collect row IDs for this batch
    std::vector<uint64_t> buildRowIds(batchSize);
    std::vector<uint64_t> probeRowIds(batchSize);
    for (size_t i = 0; i < batchSize; ++i) {
      auto& match = lateMDirectMatches_[startIdx + i];
      buildRowIds[i] = encodeRowPtr(match.buildRow);
      probeRowIds[i] = match.probeRowId;
    }
    
    // Extract probe payload columns
    for (outputChannel : probeOutputChannels) {
      output->childAt(outputChannel) = 
          probePayloadContainer_->extractColumn(columnIndex, probeRowIds);
    }
    
    // Extract build payload columns
    for (outputChannel : buildOutputChannels) {
      output->childAt(outputChannel) = 
          buildContainer->extractColumn(columnIndex, buildRowIds);
    }
    
    return output;
  }
}
\`\`\`

### ProbePayloadContainer Optimized Extraction

\`\`\`cpp
// ProbePayloadContainer::extractColumn() - optimized for fixed-width types
VectorPtr extractColumn(column_index_t columnIndex, 
                        const std::vector<uint64_t>& rowIds) {
  auto& columnType = columnTypes_[columnIndex];
  
  // Fast path for fixed-width types
  switch (columnType->kind()) {
    case TypeKind::BIGINT:
      return extractFixedWidth<int64_t>(columnIndex, rowIds);
    case TypeKind::DOUBLE:
      return extractFixedWidth<double>(columnIndex, rowIds);
    case TypeKind::INTEGER:
      return extractFixedWidth<int32_t>(columnIndex, rowIds);
    // ... other fixed-width types
    default:
      return extractGeneric(columnIndex, rowIds);
  }
}

template<typename T>
VectorPtr extractFixedWidth(column_index_t col, const vector<uint64_t>& rowIds) {
  auto result = BaseVector::create<FlatVector<T>>(type, rowIds.size(), pool);
  auto* rawValues = result->mutableRawValues();
  
  for (size_t i = 0; i < rowIds.size(); ++i) {
    auto rowId = rowIds[i];
    auto [batchIdx, rowInBatch] = decodeBatchAndRow(rowId);
    auto* sourceVector = batches_[batchIdx]->childAt(col)->asFlatVector<T>();
    rawValues[i] = sourceVector->valueAt(rowInBatch);
  }
  return result;
}
\`\`\`

---

## Configuration

Enable via query config:
\`\`\`cpp
// Enable late materialization
queryConfig().lateMaterializationEnabled() = true;

// Hybrid join must be enabled (provides RowContainer for deferred extraction)
queryConfig().hybridJoinEnabled() = true;
\`\`\`

### Automatic Detection

Late-m modes are automatically enabled when conditions are met:

| Mode | Conditions |
|------|------------|
| Single-Join Late-M | 1 HashJoin + downstream Sort/OrderBy |
| N-Way Late-M | 2+ HashJoins + downstream Sort/OrderBy |
| Direct Late-M | Single-Join + sort key = join key |

---

## Limitations

### Current Limitations (v1)

#### General Limitations

- **Inner joins only**: Outer/semi joins not yet supported
- **No filter support**: Filters require early materialization
- **Single sort key for cached optimization**: Multi-key falls back to RowContainer comparison
- **Single pipeline**: No exchange support
- **Requires hybrid join**: Late-m depends on RowContainer from hybrid mode

#### N-Way Late Materialization Specific Limitations

##### 1. **1:N Join Performance Degradation** ⚠️ CRITICAL

N-way late-m is **significantly slower** than baseline for 1:N joins due to:

| Factor | Impact |
|--------|--------|
| Row expansion | Each 1:N match creates a new LateMState row |
| Key re-extraction | Probe keys extracted from PayloadRegistry for each batch |
| Hash table re-probing | Intermediate joins require hash table lookups |
| Memory copying | Previous join rowId pairs copied to expanded rows |

**Benchmark: Q27 (customer × orders × lineitem → Sort), SF3**

| Mode | Execution Time | HashJoin[3] (join 0) | Memory |
|------|----------------|----------------------|--------|
| Baseline | **7.46s** | 550ms | 1.13GB |
| Hybrid-only | **7.49s** | 430ms | 1.13GB |
| N-way late-m | **2m 16s** (18x slower!) | 2m 10s | 129MB |

The bottleneck is `processLateMStateInput()` which:
1. Iterates through 4.5M original rows in batches of 1024
2. For each batch:
   - Extracts probe keys from PayloadRegistry (~30ms)
   - Builds RowVector for probing
   - Probes hash table
   - Expands rows for 1:N matches (13.5M expansions total)
3. Total: ~4400 batches × ~30ms = ~2+ minutes

##### 2. **Key Extraction Overhead**

Each probe requires extracting keys from the previous join's output:

```cpp
// For each batch in processLateMStateInput()
for (key : probeKeys) {
    // Find which source has this key
    auto [sourceId, col] = registry.findColumnSource(key.name);
    
    // Collect rowIds
    for (row : batch) {
        rowIds[i] = lateMState.getRowIdPair(row, joinIndex).buildRowId;
    }
    
    // Extract key values (calls into HybridContainer)
    registry.extractColumn(sourceId, rowIds, batchSize, col, keyVector);
}
```

This indirection through PayloadRegistry → HybridContainer → RowContainer is slow compared to direct data flow.

##### 3. **Memory Layout Inefficiency**

LateMState stores rowId pairs as a flat array:
```
[b0, p0, b1, p1, b0, p0, b1, p1, ...]  // 4 uint64_t per row for 2 joins
```

For 18M rows × 2 joins × 2 rowIds × 8 bytes = **576MB** just for row IDs.

##### 4. **No Selectivity Optimization**

If an intermediate join has low selectivity (many rows filtered), we still:
- Process all rows in batches
- Extract keys for dead rows
- Only filter after probing

### When N-Way Late-M Works Well

N-way late-m is beneficial when:
- All joins are **1:1** (no expansion)
- The **final output is highly selective** (Top-N with low N)
- The **payload columns are large** (many VARCHAR, large arrays)

### When to Avoid N-Way Late-M

Avoid N-way late-m when:
- Any join is **1:N** with high fan-out (orders → lineitem)
- The final output includes **most rows** (no aggregation/limit)
- The **sort key is simple** (baseline OrderBy is fast enough)

### Future Improvements

1. **Selectivity-based mode switching**: Detect 1:N joins and disable N-way late-m
2. **Streaming expansion**: Don't buffer all expansions; stream to Sort
3. **Key caching in LateMState**: Store frequently-used keys to avoid re-extraction
4. **Vectorized extraction**: Use SIMD for bulk column extraction
5. **Skip intermediate late-m**: Only apply late-m at the final join before Sort

### Recommended Configuration

| Query Pattern | Recommended Mode |
|---------------|------------------|
| Single 1:1 join → Sort | Single-join late-m ✅ |
| Single 1:N join → Sort | Direct late-m if sort key = join key ✅ |
| Multi 1:1 joins → Sort | N-way late-m (should work) |
| Multi joins with 1:N → Sort | **Disable N-way late-m** ❌ |

---

## File Summary

| File | Purpose |
|------|---------|
| [SortBuffer.h](../bolt/exec/SortBuffer.h) | BuildProbeMatch struct, late-m fields |
| [SortBuffer.cpp](../bolt/exec/SortBuffer.cpp) | Direct mode sorting, cached sortKey comparison, extraction |
| [HashProbe.h](../bolt/exec/HashProbe.h) | Late-m member variables |
| [HashProbe.cpp](../bolt/exec/HashProbe.cpp) | Direct/single-join late-m logic, ProbePayloadContainer |
| [LocalPlanner.cpp](../bolt/exec/LocalPlanner.cpp) | Detection logic, sort key = join key check |
| [OrderBy.cpp](../bolt/exec/OrderBy.cpp) | Late-m integration with SortBuffer |
| [Driver.h](../bolt/exec/Driver.h) | Late-m fields in DriverCtx |
| [ProbePayloadContainer.h](../bolt/exec/ProbePayloadContainer.h) | Probe-side column storage |
| [ProbePayloadContainer.cpp](../bolt/exec/ProbePayloadContainer.cpp) | Optimized column extraction |

### Removed Files (N-way cleanup)

The following files were **removed** as part of the N-way late-m cleanup:
- `PayloadRegistry.h/cpp` - Was used for N-way payload source registration
- `LateMState.h/cpp` - Was used for N-way state tracking

---

## Appendix: Benchmark Commands

```bash
# Run with late-m enabled (optimized)
./bolt_tpch_benchmark --run_query_verbose=27 \
    --late_materialization_enabled=true \
    --data_path=/path/to/tpch_parquet/sf30_hive

# Run with late-m disabled (baseline)
./bolt_tpch_benchmark --run_query_verbose=29 \
    --late_materialization_enabled=false \
    --hybrid_join_enabled=true \
    --data_path=/path/to/tpch_parquet/sf3_hive
```

---

## Appendix: Pointer Reuse Implementation Plan

### Motivation

In N-way joins with the same join key (e.g., Q33: lineitem ⋈ partsupp ⋈ part on `partkey`), 
intermediate joins currently copy key columns into their local `RowContainer`. This is wasteful 
because the key data already exists in the base table's `HybridContainer`.

**Goal**: Avoid key copy by building hash table directly from external row pointers.

### Key Insight: Schema Compatibility

External rows from upstream's `keys_` RowContainer have the **same schema** as local rows would:
```
Row layout: [key1 | key2 | ... | next_ptr | hybridRowId]
```

This means:
- **Existing `hashRows()`** can hash external rows directly (same column offsets)
- **Existing `compareKeys()`** can compare external rows directly
- **No schema translation needed!**

### Key Insight: Next Pointer for Distinct Keys

For **distinct keys** (no duplicates in build side):
- Each row has a unique key
- `compareKeys()` never finds a match → `pushNext()` is never called
- **Next pointers are not needed!**

We can pass external row pointers directly to hash table build, and the existing 
hash/compare logic works unchanged.

### Assumption

**Q33 has distinct join keys** - no duplicate `partkey` values in build side results.
This simplifies the implementation. Duplicate key handling is left as future work.

---

## Phase 1: HashTable Infrastructure

### Goal
Enable HashTable to build from a list of external row pointers instead of iterating 
its internal `rows_` RowContainer.

### 1.1 Add External Row Mode

Add new members to `HashTable`:

```cpp
// bolt/exec/HashTable.h
private:
  bool externalRowMode_ = false;
  std::vector<char*> externalRowPtrs_;
  RowContainer* externalKeySource_ = nullptr;  // For hash/compare operations
```

### 1.2 New API: Build from External Pointers

```cpp
// bolt/exec/HashTable.h
/// Build hash table from external row pointers.
/// External rows must have compatible schema (same key columns at same offsets).
/// @param rowPtrs Pointers to rows in an external RowContainer
/// @param keySource The RowContainer where rowPtrs point into (for hash/compare)
void buildFromExternalPointers(
    std::vector<char*> rowPtrs,
    RowContainer* keySource);
```

### 1.3 Implementation

```cpp
// bolt/exec/HashTable.cpp
template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::buildFromExternalPointers(
    std::vector<char*> rowPtrs,
    RowContainer* keySource) {
  
  externalRowMode_ = true;
  externalRowPtrs_ = std::move(rowPtrs);
  externalKeySource_ = keySource;
  
  // Set numDistinct for capacity calculation
  numDistinct_ = externalRowPtrs_.size();
  
  // Determine hash mode and allocate table
  decideHashMode(0);
  
  // Build hash table - reuse existing insertBatch logic
  buildFromExternalPointersInternal();
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::buildFromExternalPointersInternal() {
  constexpr int32_t kBatchSize = 1024;
  raw_vector<uint64_t> hashes;
  hashes.resize(kBatchSize);
  
  for (size_t i = 0; i < externalRowPtrs_.size(); i += kBatchSize) {
    size_t batchEnd = std::min(i + kBatchSize, externalRowPtrs_.size());
    size_t batchSize = batchEnd - i;
    
    // Hash the external rows
    // Use externalKeySource_ for hash computation
    hashExternalRows(
        folly::Range<char**>(externalRowPtrs_.data() + i, batchSize),
        hashes);
    
    // Insert into hash table
    insertForJoin(externalRowPtrs_.data() + i, hashes.data(), batchSize, nullptr);
  }
}
```

### 1.4 Hash External Rows

```cpp
// Compute hashes for external rows using externalKeySource_
template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::hashExternalRows(
    folly::Range<char**> rows,
    raw_vector<uint64_t>& hashes) {
  
  // Use externalKeySource_ instead of rows_ for hash computation
  for (int32_t i = 0; i < hashers_.size(); ++i) {
    if (hashMode_ == HashMode::kHash) {
      externalKeySource_->hash(i, rows, i > 0, hashes.data());
    } else {
      auto column = externalKeySource_->columnAt(i);
      hashers_[i]->computeValueIdsForRows(
          rows.data(), rows.size(),
          column.offset(), column.nullByte(),
          ignoreNullKeys ? 0 : column.nullMask(),
          hashes);
    }
  }
}
```

### 1.5 Compare Keys for External Mode

Modify `compareKeys()` to use correct RowContainer:

```cpp
template <bool ignoreNullKeys>
bool HashTable<ignoreNullKeys>::compareKeys(
    const char* group,
    const char* inserted) {
  
  // Use externalKeySource_ if in external row mode
  auto* keyContainer = externalRowMode_ ? externalKeySource_ : rows_.get();
  
  auto numKeys = hashers_.size();
  int32_t i = 0;
  do {
    if (keyContainer->compare(group, inserted, i, CompareFlags{true, true})) {
      return false;
    }
  } while (++i < numKeys);
  return true;
}
```

### 1.6 Probe-side Compare

```cpp
template <bool ignoreNullKeys>
bool HashTable<ignoreNullKeys>::compareKeys(
    const char* group,
    HashLookup& lookup,
    vector_size_t row) {
  
  // Use externalKeySource_ if in external row mode
  auto* keyContainer = externalRowMode_ ? externalKeySource_ : rows_.get();
  
  int32_t numKeys = lookup.hashers.size();
  int32_t i = 0;
  do {
    auto& hasher = lookup.hashers[i];
    if (!keyContainer->equals<!ignoreNullKeys>(
            group, keyContainer->columnAt(i), hasher->decodedVector(), row)) {
      return false;
    }
  } while (++i < numKeys);
  return true;
}
```

### 1.7 Accessor Methods

```cpp
// bolt/exec/HashTable.h
bool isExternalRowMode() const { return externalRowMode_; }
const std::vector<char*>& getExternalRowPtrs() const { return externalRowPtrs_; }
```

---

## Phase 2: HashBuild Integration

### Goal
Make HashBuild use external row pointers when pointer reuse is enabled.

### 2.1 Configuration Flag

Already exists:
```cpp
// bolt/core/QueryConfig.h
static constexpr const char* kHybridJoinPointerReuseEnabled =
    "hybrid_join_pointer_reuse_enabled";
```

### 2.2 Modify addInputLateMaterialization

Skip local row allocation entirely - just collect external pointers:

```cpp
void HashBuild::addInputLateMaterialization(const RowVectorPtr& input) {
  auto* driverCtx = operatorCtx_->driverCtx();
  
  const bool pointerReuseEnabled = 
      driverCtx->queryConfig().hybridJoinPointerReuseEnabled() &&
      !driverCtx->buildSideLateMBuildRowPtrs.empty();
  
  if (pointerReuseEnabled) {
    // Pointer reuse path: just collect external pointers, no local storage
    // Accumulate in a member variable for later use in noMoreInputInternal
    externalBuildRowPtrs_.insert(
        externalBuildRowPtrs_.end(),
        driverCtx->buildSideLateMBuildRowPtrs.begin(),
        driverCtx->buildSideLateMBuildRowPtrs.end());
    
    // Also store probe row IDs for extraction
    table_->hybridData()->appendUpstreamRefs(
        driverCtx->buildSideLateMBuildRowPtrs,
        driverCtx->buildSideLateMProbeRowIds);
    
    driverCtx->clearBatchState();
    return;
  }
  
  // Normal path: store keys locally (existing code)
  // ...
}
```

### 2.3 Modify noMoreInputInternal

Use `buildFromExternalPointers()` when pointer reuse is enabled:

```cpp
void HashBuild::noMoreInputInternal() {
  auto* driverCtx = operatorCtx_->driverCtx();
  
  const bool pointerReuseEnabled = 
      driverCtx->queryConfig().hybridJoinPointerReuseEnabled() &&
      !externalBuildRowPtrs_.empty();
  
  if (pointerReuseEnabled) {
    // Get the key source (upstream's keys_ RowContainer)
    auto* keySource = driverCtx->primaryUpstreamBuildContainer->getKeys();
    
    // Build hash table from external pointers
    table_->buildFromExternalPointers(
        std::move(externalBuildRowPtrs_),
        keySource);
    
    // Update columnSourceMap for extraction
    updateSourceMapForNWay();
  } else {
    // Normal path (existing code)
    table_->prepareJoinTable(...);
  }
  
  // ... rest of noMoreInputInternal
}
```

### 2.4 New Member Variable

```cpp
// bolt/exec/HashBuild.h
private:
  // For pointer reuse mode: accumulated external row pointers
  std::vector<char*> externalBuildRowPtrs_;
```

---

## Phase 3: HashProbe Integration

### Goal
Ensure probe side works correctly with external row mode.

### 3.1 No Changes Needed for Probe Logic

The probe logic uses `compareKeys()` which we already modified to use 
`externalKeySource_` when in external row mode.

### 3.2 Extraction Uses columnSourceMap

Extraction already uses `columnSourceMap` which points to upstream containers.
No changes needed.

---

## Testing

### Test Cases

1. **Q33**: 3-way join with distinct keys
2. **Q34**: Q33 + Sort for deterministic result verification

### Commands

```bash
# Baseline (no late-m)
./bolt_tpch_benchmark -run_query_verbose=34 \
    --hybrid_join_enabled=true \
    --data_path=/path/to/data

# Late-m without pointer reuse
./bolt_tpch_benchmark -run_query_verbose=34 \
    --late_materialization_enabled=true \
    --data_path=/path/to/data

# Late-m with pointer reuse
./bolt_tpch_benchmark -run_query_verbose=34 \
    --late_materialization_enabled=true \
    --nway_pointer_reuse_enabled=true \
    --data_path=/path/to/data
```

### Verification

Compare sorted output between all three modes - should be identical.

---

## Implementation Status

### Completed (2026-02-22)

**Phase 1: HashTable Infrastructure** ✅
- Added `buildFromExternalPointers()` method to HashTable
- Added `externalRowMode_`, `externalRowPtrs_`, `externalKeySource_` members
- Modified `compareKeys()` and `hashRows()` to use `externalKeySource_` in external mode

**Phase 2: HashBuild Integration** ✅  
- Added `pointerReuseEnabled_`, `externalRowPtrs_`, `externalKeySource_` members to HashBuild
- Modified `addInputLateMaterialization()` to collect external pointers instead of copying keys
- Modified `finishHashBuild()` to call `buildFromExternalPointers()` when pointer reuse enabled

**Phase 3: Extraction Support** ✅
- Modified `extractColumnsFromUpstream()` to use `getEffectiveKeyContainer()` for key extraction
- This allows extraction to work correctly when row pointers come from upstream container

### Testing

**Q23 (2-way join)** ✅ Works correctly
```bash
./bolt_hybrid_late_m_benchmark --late_m=true --pointer_reuse=true --query=23
# Result: 600572 rows (matches baseline)
```

**Q27 (3-way join)** ❌ Fails - intermediate level payload extraction issue
- Intermediate levels need `hybridRowId` for payload extraction
- In pointer reuse mode, local rows don't exist, so `hybridRowId` is not available
- See "Known Limitations" below

### Known Limitations

1. **Intermediate level payloads**: Pointer reuse works for final join level but not for 
   intermediate levels that need to extract payload columns. This is because `getRowIds()` 
   expects to read the `hybridRowId` from local rows, but in pointer reuse mode, the rows 
   are external.

2. **Duplicate keys**: Current implementation assumes distinct keys. See "Future Work" section.

### Configuration

Enable pointer reuse with the `hybrid_join_pointer_reuse_enabled` config:
```cpp
// In QueryConfig.h
static constexpr const char* kHybridJoinPointerReuseEnabled =
    "hybrid_join_pointer_reuse_enabled";
```

Command line flag: `--hybrid_join_pointer_reuse_enabled=true`

---

## Future Work

### Handle Duplicate Keys

Current implementation assumes distinct keys. For duplicate keys:
- Same row pointer may appear multiple times
- `compareKeys()` returns true → `pushNext()` is called
- Setting `row->next = row` causes infinite loop!

**Solution**: Use external next chain instead of in-row next pointer:
```cpp
std::vector<int32_t> externalNextChain_;  // index → next index (-1 = end)
```

### Fix Intermediate Level Payloads

To support N-way joins (3+ joins) with pointer reuse, need to:
1. Store `hybridRowId` separately for pointer reuse mode, OR
2. Modify `getRowIds()` to traverse to the effective key container, OR  
3. Update columnSourceMap to point directly to upstream container

### Memory Tracking

Account for shared data in memory usage reporting.

### Spill Support

Handle spilling when using external row pointers.



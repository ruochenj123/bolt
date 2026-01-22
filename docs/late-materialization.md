# Late Materialization for HashJoin → Sort Pattern

## Overview

This document describes the late materialization optimization implemented for the HashJoin → Sort (OrderBy) pattern in Bolt. The optimization avoids redundant data layout conversion when both operators work on the same key columns.

## Problem Statement

In a typical HashJoin → Sort pipeline:

```
HashJoin (build on key K) → Sort (order by key K)
```

The traditional flow involves redundant work:

1. **HashBuild**: Stores data in HybridContainer (keys in RowContainer, payloads in RowVectorPtr batches)
2. **HashProbe**: Extracts matched rows from HybridContainer → materializes to column vectors
3. **SortBuffer**: Receives column vectors → copies into its own RowContainer → sorts → extracts

The redundancy: HashJoin already has the data in a RowContainer (sorted by the join key), but we extract it to column vectors, only to copy it back into another RowContainer for sorting.

## Solution: Late Materialization

Pass row pointers directly from HashProbe to SortBuffer, allowing SortBuffer to:
1. Sort using the existing RowContainer (no data copying)
2. Extract directly from HybridContainer after sorting

```
Traditional:
HashProbe → [extract to columns] → SortBuffer → [copy to RowContainer] → sort → extract

Late-M:
HashProbe → [pass row pointers] → SortBuffer → sort (using existing RowContainer) → extract
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         HashProbe                                │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              HybridContainer (from HashBuild)            │    │
│  │  ┌─────────────────┐    ┌─────────────────────────────┐ │    │
│  │  │  keys_ (RowCtr) │    │  payloads_ (RowVectorPtr)   │ │    │
│  │  │  [o_orderkey]   │    │  [o_orderdate, ...]         │ │    │
│  │  │  [rowId=BIGINT] │    │                             │ │    │
│  │  └────────┬────────┘    └─────────────────────────────┘ │    │
│  └───────────┼──────────────────────────────────────────────┘    │
│              │                                                   │
│              │ outputTableRows_ (char**)                         │
└──────────────┼───────────────────────────────────────────────────┘
               │
               │  Via DriverCtx:
               │  - lateMaterializationTable (shared_ptr<BaseHashTable>)
               │  - lateMaterializationRows (vector<char*>)
               │  - lateMaterializationProjections (column mappings)
               ▼
┌─────────────────────────────────────────────────────────────────┐
│                         SortBuffer                               │
│                                                                  │
│  1. Receive row pointers from DriverCtx (no data copy)          │
│  2. Sort using HybridContainer's RowContainer for comparison    │
│  3. Extract from HybridContainer using projections              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Implementation Details

### 1. Configuration

**File**: `bolt/core/QueryConfig.h`

```cpp
static constexpr const char* kLateMaterializationEnabled =
    "late_materialization_enabled";

bool lateMaterializationEnabled() const {
  return get<bool>(kLateMaterializationEnabled, false);
}
```

### 2. DriverCtx Extensions

**File**: `bolt/exec/Driver.h`

Added late materialization state to `DriverCtx`:

```cpp
struct DriverCtx {
  // ... existing fields ...

  /// Holds reference to the HashTable to keep HybridContainer alive
  /// until downstream operators finish processing.
  std::shared_ptr<BaseHashTable> lateMaterializationTable;

  /// Row pointers from HashProbe pointing into HybridContainer's RowContainer.
  std::vector<char*> lateMaterializationRows;

  /// Column projections: maps from HybridContainer column index to output column index.
  std::vector<std::pair<column_index_t, column_index_t>> lateMaterializationProjections;

  void clearLateMaterializationState() {
    lateMaterializationTable.reset();
    lateMaterializationRows.clear();
    lateMaterializationProjections.clear();
  }
};
```

### 3. HashProbe Modifications

**File**: `bolt/exec/HashProbe.cpp`

#### Detection (in `initialize()`)
```cpp
lateMaterializationEnabled_ =
    operatorCtx_->driverCtx()->queryConfig().lateMaterializationEnabled();
```

#### Activation (in `asyncWaitForHashTable()`)
```cpp
if (lateMaterializationEnabled_ && table_->hybridData() == nullptr) {
  LOG(INFO) << "HashProbe: late materialization disabled - no hybrid data";
  lateMaterializationEnabled_ = false;
}
```

#### Row Pointer Passing (in `fillOutput()`)
```cpp
if (lateMaterializationEnabled_ && table_->hybridData() != nullptr) {
  auto* driverCtx = operatorCtx_->driverCtx();
  
  // Store table shared_ptr to keep HybridContainer alive
  driverCtx->lateMaterializationTable = table_;

  // Append row pointers to DriverCtx
  auto& rows = driverCtx->lateMaterializationRows;
  size_t prevSize = rows.size();
  rows.resize(prevSize + size);
  std::copy(outputTableRows_.data(), outputTableRows_.data() + size,
            rows.data() + prevSize);

  // Set column projections (from table column to output)
  if (driverCtx->lateMaterializationProjections.empty()) {
    for (const auto& proj : tableOutputProjections_) {
      driverCtx->lateMaterializationProjections.emplace_back(
          proj.inputChannel, proj.outputChannel);
    }
  }

  // Output null constant placeholders (not used by downstream)
  for (auto projection : tableOutputProjections_) {
    auto& child = output_->childAt(projection.outputChannel);
    child = BaseVector::createNullConstant(
        outputType_->childAt(projection.outputChannel), size, pool());
  }
  return;
}
```

### 4. SortBuffer Modifications

**File**: `bolt/exec/SortBuffer.cpp`

#### State Variables
```cpp
bool lateMaterializationActive_{false};
std::shared_ptr<BaseHashTable> lateMaterializationTable_;
HybridContainer* lateMaterializationContainer_{nullptr};
std::vector<char*> lateMaterializationRows_;
std::vector<std::pair<column_index_t, column_index_t>> lateMaterializationProjections_;
```

#### Input Handling (`addInput()`)
```cpp
if (operatorCtx_ != nullptr) {
  auto* driverCtx = operatorCtx_->driverCtx();
  if (driverCtx->lateMaterializationTable != nullptr &&
      !driverCtx->lateMaterializationRows.empty()) {
    
    if (!lateMaterializationActive_) {
      lateMaterializationActive_ = true;
      lateMaterializationTable_ = driverCtx->lateMaterializationTable;
      lateMaterializationContainer_ = driverCtx->lateMaterializationTable->hybridData();
      lateMaterializationProjections_ = driverCtx->lateMaterializationProjections;
    }
    
    // Take ownership of row pointers (no data copy!)
    lateMaterializationRows_.insert(
        lateMaterializationRows_.end(),
        driverCtx->lateMaterializationRows.begin(),
        driverCtx->lateMaterializationRows.end());
    numInputRows_ += driverCtx->lateMaterializationRows.size();
    
    driverCtx->clearLateMaterializationState();
    return;  // Skip normal input processing
  }
}
```

#### Sorting (`noMoreInput()`)
```cpp
if (lateMaterializationActive_) {
  auto* rowContainer = lateMaterializationContainer_->getKeys();
  sortedRows_.swap(lateMaterializationRows_);

  // Use JIT-compiled comparison when available (same as normal path)
#ifdef ENABLE_BOLT_JIT
  if (cmp_ == nullptr && operatorCtx_ &&
      operatorCtx_->driverCtx()->queryConfig().enableJitRowCmpRow()) {
    if (rowContainer->JITable(rowContainer->keyTypes())) {
      auto [jitMod, rowRowCmpfn] = rowContainer->codegenCompare(
          rowContainer->keyTypes(), sortCompareFlags_,
          bytedance::bolt::jit::CmpType::SORT_LESS, true);
      jitModule_ = std::move(jitMod);
      cmp_ = (RowRowCompare)jitModule_->getFuncPtr(rowRowCmpfn);
    }
  }
  if (cmp_) {
    sorter_.sort(sortedRows_.begin(), sortedRows_.end(), cmp_);
  } else {
#endif
    sorter_.sort(sortedRows_.begin(), sortedRows_.end(),
        [rowContainer, this](const char* leftRow, const char* rightRow) {
          for (vector_size_t index = 0; index < sortCompareFlags_.size(); ++index) {
            if (auto result = rowContainer->compare(leftRow, rightRow, index,
                                                     sortCompareFlags_[index])) {
              return result < 0;
            }
          }
          return false;
        });
#ifdef ENABLE_BOLT_JIT
  }
#endif
  return;
}
```

#### Output Extraction (`getOutputWithoutSpill()`)
```cpp
if (lateMaterializationActive_) {
  std::vector<HybridRowId> outputRowIds;
  outputRowIds.resize(output_->size());
  lateMaterializationContainer_->getRowIds(
      sortedRows_.data() + numOutputRows_, output_->size(), outputRowIds);
  
  for (const auto& [inputChannel, outputChannel] : lateMaterializationProjections_) {
    lateMaterializationContainer_->extractColumn(
        sortedRows_.data() + numOutputRows_,
        output_->size(),
        inputChannel,
        output_->childAt(outputChannel),
        outputRowIds);
  }
  numOutputRows_ += output_->size();
  return;
}
```

## Lifetime Management

**Critical Issue**: The HybridContainer is owned by the HashTable. If HashProbe's `close()` is called before SortBuffer finishes, the HybridContainer would be destroyed, leaving dangling pointers.

**Solution**: Store `shared_ptr<BaseHashTable>` in DriverCtx (and SortBuffer), keeping the table alive until downstream operators complete.

```cpp
// In DriverCtx
std::shared_ptr<BaseHashTable> lateMaterializationTable;

// In HashProbe::fillOutput()
driverCtx->lateMaterializationTable = table_;  // Increment ref count

// In SortBuffer::addInput()
lateMaterializationTable_ = driverCtx->lateMaterializationTable;  // Keep alive
```

## Files Modified

| File | Changes |
|------|---------|
| `bolt/core/QueryConfig.h` | Added `kLateMaterializationEnabled` config |
| `bolt/exec/Driver.h` | Added late-m state to `DriverCtx` |
| `bolt/exec/HashProbe.h` | Added `lateMaterializationEnabled_` member |
| `bolt/exec/HashProbe.cpp` | Detection, activation, row pointer passing |
| `bolt/exec/SortBuffer.h` | Added late-m state variables |
| `bolt/exec/SortBuffer.cpp` | Input handling, sorting, extraction |
| `bolt/benchmarks/QueryBenchmarkBase.cpp` | Added CLI flags |
| `bolt/exec/tests/utils/TpchQueryBuilder.cpp` | Added q23 test query |

## Benchmark Results

**Test Query**: q23 (orders JOIN lineitem, output o_orderkey, o_orderdate, ORDER BY o_orderkey)
- Dataset: TPC-H SF10 (60M rows join result)
- Configuration: single driver, single split

### Execution Time

| Configuration | Time | vs Baseline |
|---------------|------|-------------|
| Baseline (vanilla) | 4.37s | - |
| Hybrid Only | 4.32s | -1% |
| **Late-M** | **3.62s** | **-17%** |

### Memory Usage

| Operator | Baseline | Hybrid | Late-M |
|----------|----------|--------|--------|
| HashBuild | 672MB | 792MB | 792MB |
| HashProbe Output | 1.00GB | 1.02GB | **687KB** |
| OrderBy Peak | 744MB | 744MB | **18KB** |

### Operator-Level Breakdown

#### HashProbe
| Metric | Baseline | Hybrid | Late-M |
|--------|----------|--------|--------|
| CPU Time | 906ms | 967ms | **868ms** |
| Output Size | 1.00GB | 1.02GB | **687KB** |
| CPU Output (O) | 329ms | 389ms | **285ms** |

**Late-M saves ~100ms** by skipping column extraction.

#### OrderBy
| Metric | Baseline | Hybrid | Late-M |
|--------|----------|--------|--------|
| CPU Time | 1.48s | 1.44s | **853ms** |
| Peak Memory | 744MB | 744MB | **18KB** |
| CPU Input (I) | 781ms | 741ms | **449ms** |
| CPU Sort (F) | 530ms | 534ms | **237ms** |

**Late-M saves ~600ms** total:
- **Input phase**: -300ms (no data copy to RowContainer)
- **Sort phase**: -300ms (using JIT on existing RowContainer)

## Usage

### Command Line
```bash
./bolt_tpch_benchmark \
  --data_path=/path/to/tpch \
  --run_query_verbose=23 \
  --hybrid_join_enabled=true \
  --late_materialization_enabled=true
```

### Programmatic
```cpp
auto queryCtx = core::QueryCtx::create();
queryCtx->setConfigOverrides({
  {core::QueryConfig::kHybridJoinEnabled, "true"},
  {core::QueryConfig::kLateMaterializationEnabled, "true"},
});
```

## Limitations and Future Work

### Current Limitations

1. **Build-side only**: Currently only supports outputting columns from the build side of the join.
2. **Single join**: Optimized for single HashJoin → Sort pattern. N-way joins not yet supported.
3. **Same key assumption**: Sort key must match join key for maximum benefit.
4. **No spill support**: Late-m path doesn't support spilling yet.

### N-Way Join Generalization (In Progress)

The N-way join generalization extends late materialization to support arbitrary chains of joins:

```
T1 JOIN T2 ON k1 → J1 JOIN T3 ON k2 → ... → Sort on any key
```

**Key Design Elements:**

1. **PayloadRegistry**: Central registry tracking payload columns from all sources
   - Each join registers build/probe sources with unique sourceId
   - Stores `shared_ptr<BaseHashTable>` for build-side, `ProbePayloadContainer` for probe-side
   
2. **RowId Accumulation**: Each join output row accumulates rowIds from all sources:
   - J1 output: rowId(T1), rowId(T2)
   - J2 output: rowId(T1), rowId(T2), rowId(T3)
   - Final Sort: has rowIds to extract from all original tables

3. **Projection Mapping**: Maps (sourceId, sourceColumn) → outputColumn for final extraction

**Implementation Files:**
- `bolt/exec/PayloadRegistry.h/cpp`: Registry for tracking payload sources
- `bolt/exec/RowContainer.h`: `HybridContainer::extractColumnByRowId()` for direct rowId-based extraction
- `bolt/exec/RowContainer.h`: `ProbePayloadContainer` for probe-side payload storage

**Test Queries (Q26-Q28):**
- Q26: 2-way join, same join key as sort key
- Q27: 3-way join (customer → orders → lineitem), different join keys
- Q28: 2-way join, different join key vs sort key

See [nway-late-materialization-design.md](nway-late-materialization-design.md) for full design details.

### Future Optimizations

1. **Probe-side support**: Extend to handle probe-side columns in output. ✅ (via ProbePayloadContainer)
2. **Multi-key support**: Handle cases with multiple join/sort keys.
3. **Aggregation fusion**: Apply similar optimization to HashJoin → Aggregation patterns.
4. **Automatic detection**: Planner-level detection of late-m opportunities.

## Testing

### Test Query (q23)
```sql
SELECT o_orderkey, o_orderdate
FROM orders, lineitem
WHERE l_orderkey = o_orderkey
ORDER BY o_orderkey
LIMIT 100
```

### Verification
Results are verified to be identical between all configurations (baseline, hybrid, late-m).

## Commits

1. **Initial implementation**: Added config, DriverCtx plumbing, HashProbe/SortBuffer modifications
2. **JIT fix**: Enabled JIT-compiled comparison for late-m path (critical for performance)

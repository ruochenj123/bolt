# N-Way Build-Side Late Materialization Design

**Version**: 4.0  
**Date**: 2026-02-08  
**Status**: Ready for implementation

---

## 1. Overview

### 1.1 Goal

Avoid redundant column-to-row-to-column conversions in multi-way joins by deferring payload extraction until the final output.

### 1.2 Scope

**Build-side late materialization only**: When a join result feeds into the **build side** of the next join, we defer materialization of non-key columns.

### 1.3 Query Pattern (Q27 Example)

```
Pipeline 0: customer → HashBuild₀
Pipeline 1: orders → HashProbe₀ → HashBuild₁  
Pipeline 2: lineitem → HashProbe₁ → OrderBy → Output
```

Here:
- Join₁ result (customer ⋈ orders) feeds into **build side** of Join₂
- N-way late-m defers extraction of customer/orders columns until final HashProbe₁

### 1.4 Key Insight

Instead of extracting and re-storing columns at each join level, we:
1. **Store only join keys** in each HashBuild's RowContainer
2. **Store metadata** mapping output columns → original source containers
3. **Extract all columns at once** at the final materialization point

---

## 2. Core Data Structures

### 2.1 ColumnSource: Direct Pointer to Data Source

Each output column maps to exactly ONE source location:

```cpp
/// Describes where a column's data lives.
struct ColumnSource {
  enum Type {
    HYBRID_KEY,      // From HybridContainer's keys_ (RowContainer)
    HYBRID_PAYLOAD,  // From HybridContainer's owningInputs_ (columnar batches)
    PROBE_PAYLOAD,   // From ProbePayloadContainer (columnar batches)
    CURRENT_PROBE    // From current probe input (not stored, just dictionary wrap)
  };
  
  Type type;
  int32_t columnIndex;  // Column index within the source container
  
  // Direct pointer to source - no depth traversal needed
  union {
    HybridContainer* hybridContainer;              // For HYBRID_KEY, HYBRID_PAYLOAD
    ProbePayloadContainer* probePayloadContainer;  // For PROBE_PAYLOAD
  };
  
  // For multi-driver probe side: which driver's container
  // (Not needed for HybridContainer since we use char* pointers directly)
  uint8_t driverId = 0;  // Only used for PROBE_PAYLOAD
};
```

### 2.2 ColumnSourceMap: Output Channel → Source

```cpp
/// Maps output channel index → where to extract the data
/// Passed through DriverCtx from operator to operator
using ColumnSourceMap = std::unordered_map<int32_t, ColumnSource>;
```

### 2.3 HybridContainer Structure

```cpp
class HybridContainer {
  // === Row-based key storage ===
  RowContainer* keys_;              // Join keys + hybridRowId (uint64_t)
  
  // === Columnar payload storage ===
  std::vector<RowVectorPtr> owningInputs_;  // Payload columns (coalesced into one batch)
  
  // === Upstream row pointers (for N-way chain) ===
  std::vector<char*> upstreamBuildRowPtrs_;   // Direct pointers to parent join's rows
  std::vector<uint64_t> upstreamProbeRowIds_; // Encoded (driverId << 56 | rowIndex)
  
  // === Cross-driver container access (after merge via JoinBridge) ===
  std::unordered_map<uint8_t, std::shared_ptr<HybridContainer>> upstreamBuildContainers_;
  std::unordered_map<uint8_t, std::shared_ptr<ProbePayloadContainer>> upstreamProbePayloads_;
};
```

### 2.4 ProbePayloadContainer Structure

```cpp
class ProbePayloadContainer {
  std::vector<RowVectorPtr> batches_;  // Probe-side payload columns
  std::vector<std::string> payloadNames_;  // Column names for metadata
  
  // Coalesced into single batch for efficient extraction
  RowVectorPtr coalescedBatch_;
};
```

---

## 3. Row ID Encoding

### 3.1 hybridRowId (Stored in keys_)

Each row in `keys_` has a `hybridRowId` column (uint64_t) after the join keys:

```
hybridRowId = (driverId << 56) | localRowIndex
```

- Top 8 bits: driver ID (0-255)
- Bottom 56 bits: local row index within that driver's HybridContainer

### 3.2 Upstream Pointers

For N-way joins, each HybridContainer stores:
- `upstreamBuildRowPtrs_[localIdx]` → `char*` pointer to parent join's row
- `upstreamProbeRowIds_[localIdx]` → encoded rowId for probe payload

---

## 4. Workflow: Metadata Flow Through Operators

### 4.1 Base Table (HashBuild₀)

```
Input: customer (c_custkey, c_name, c_address, c_nationkey)
Join key: c_custkey

HybridContainer₀:
  keys_: [c_custkey | hybridRowId]
  owningInputs_: [c_name, c_address, c_nationkey]  ← payload only, no key

ColumnSourceMap (for downstream):
  0 → {HYBRID_KEY, 0, HybridContainer₀*}      // c_custkey
  1 → {HYBRID_PAYLOAD, 0, HybridContainer₀*}  // c_name
  2 → {HYBRID_PAYLOAD, 1, HybridContainer₀*}  // c_address
  3 → {HYBRID_PAYLOAD, 2, HybridContainer₀*}  // c_nationkey
```

### 4.2 First Join (HashProbe₀ → HashBuild₁)

```
Probe input: orders (o_orderkey, o_custkey, o_orderdate, o_totalprice)
Build side: HybridContainer₀ (customer)
Join condition: o_custkey = c_custkey

HashProbe₀ outputs:
  - For each match, store (buildRowPtr, probeRowId) pairs
  - Pass to HashBuild₁ via DriverCtx

HashBuild₁ receives:
  Join key for next level: o_orderkey (extracted from orders)

HybridContainer₁:
  keys_: [o_orderkey | hybridRowId]  ← only this level's join key
  owningInputs_: empty (orders payload stored in ProbePayloadContainer)
  upstreamBuildRowPtrs_: [char* → customer rows]
  upstreamProbeRowIds_: [encoded rowIds → orders payload]

ProbePayloadContainer₁:
  batches_: [o_orderdate, o_totalprice]  ← orders non-key columns

ColumnSourceMap (updated for new output channels):
  0 → {HYBRID_KEY, 0, HybridContainer₁*}           // o_orderkey (new key)
  1 → {HYBRID_KEY, 0, HybridContainer₀*}           // c_custkey (from parent)
  2 → {HYBRID_PAYLOAD, 0, HybridContainer₀*}       // c_name
  3 → {HYBRID_PAYLOAD, 1, HybridContainer₀*}       // c_address
  4 → {PROBE_PAYLOAD, 0, ProbePayloadContainer₁*}  // o_orderdate
  5 → {PROBE_PAYLOAD, 1, ProbePayloadContainer₁*}  // o_totalprice
```

### 4.3 Second Join (HashProbe₁ → Output)

```
Probe input: lineitem (l_orderkey, l_quantity, l_price)
Build side: HybridContainer₁ (customer ⋈ orders result)
Join condition: l_orderkey = o_orderkey

HashProbe₁ (final probe - materialization point):
  1. Probe hash table, get matched rows
  2. For each matched row, use ColumnSourceMap to extract all columns
  3. Current probe columns (lineitem) use dictionary wrapping

Final output: [c_name, c_address, o_orderdate, l_quantity, l_price, ...]
```

---

## 5. Extraction Algorithm

### 5.1 Overview

At the materialization point (final HashProbe), extraction is simple:

```cpp
for (outputChannel : outputColumns) {
  ColumnSource& source = columnSourceMap[outputChannel];
  
  switch (source.type) {
    case HYBRID_KEY:
      extractFromRowContainer(source.hybridContainer->keys_, rows, source.columnIndex, result);
      break;
      
    case HYBRID_PAYLOAD:
      extractFromPayloadBatch(source.hybridContainer, rows, source.columnIndex, result);
      break;
      
    case PROBE_PAYLOAD:
      extractFromProbePayload(source.probePayloadContainer, probeRowIds, source.columnIndex, result);
      break;
      
    case CURRENT_PROBE:
      result = dictionaryWrap(probeInput->childAt(source.columnIndex), outputRowMapping);
      break;
  }
}
```

### 5.2 Row Pointer Resolution

For HYBRID_KEY and HYBRID_PAYLOAD, we need the `char*` row pointers. These come from two sources:

1. **Current container (depth 0)**: Use `matchedRows` directly from HashProbe
2. **Upstream containers (depth > 0)**: Follow `upstreamBuildRowPtrs_` chain

```cpp
/// Resolve row pointers for a specific HybridContainer
std::vector<char*> resolveRowPointers(
    char* const* matchedRows,
    int32_t numRows,
    HybridContainer* currentContainer,
    HybridContainer* targetContainer) {
  
  if (currentContainer == targetContainer) {
    // Depth 0: use matched rows directly
    return std::vector<char*>(matchedRows, matchedRows + numRows);
  }
  
  // Depth > 0: trace through upstreamBuildRowPtrs_
  std::vector<char*> result(numRows);
  
  for (int32_t i = 0; i < numRows; ++i) {
    char* row = matchedRows[i];
    HybridContainer* current = currentContainer;
    
    while (current != targetContainer && current != nullptr) {
      // Decode localIdx from hybridRowId
      uint64_t hybridRowId = current->keys_->valueAt<uint64_t>(row, hybridRowIdOffset);
      uint64_t localIdx = hybridRowId & ((1ULL << 56) - 1);
      
      // Follow upstream pointer
      row = current->upstreamBuildRowPtrs_[localIdx];
      current = current->upstreamBuildContainer_;
    }
    
    result[i] = row;
  }
  
  return result;
}
```

### 5.3 Extraction from HYBRID_PAYLOAD

```cpp
void extractFromPayloadBatch(
    HybridContainer* container,
    const std::vector<char*>& rows,
    int32_t columnIndex,
    VectorPtr& result,
    memory::MemoryPool* pool) {
  
  // Ensure payload is coalesced into single flat batch
  if (!container->isCoalesced()) {
    container->coalesceBatches();
  }
  
  auto& batch = container->getCoalescedBatch();
  auto srcVector = batch->childAt(columnIndex);
  
  // Build indices from row pointers
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(rows.size(), pool);
  auto* indicesPtr = indices->asMutable<vector_size_t>();
  
  for (int32_t i = 0; i < rows.size(); ++i) {
    uint64_t hybridRowId = container->keys_->valueAt<uint64_t>(rows[i], hybridRowIdOffset);
    indicesPtr[i] = static_cast<vector_size_t>(hybridRowId & ((1ULL << 56) - 1));
  }
  
  // Dictionary wrap (O(1) creation, no data copy)
  result = BaseVector::wrapInDictionary(nullptr, indices, rows.size(), srcVector);
}
```

### 5.4 Extraction from PROBE_PAYLOAD

```cpp
void extractFromProbePayload(
    const std::vector<uint64_t>& probeRowIds,
    int32_t columnIndex,
    const std::unordered_map<uint8_t, ProbePayloadContainer*>& containers,
    VectorPtr& result,
    memory::MemoryPool* pool) {
  
  // Single driver: use efficient dictionary wrap
  if (containers.size() == 1) {
    auto* container = containers.begin()->second;
    if (!container->isCoalesced()) {
      container->coalesceBatches();
    }
    
    BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(probeRowIds.size(), pool);
    auto* indicesPtr = indices->asMutable<vector_size_t>();
    for (size_t i = 0; i < probeRowIds.size(); ++i) {
      indicesPtr[i] = static_cast<vector_size_t>(probeRowIds[i] & ((1ULL << 56) - 1));
    }
    
    auto srcVector = container->getCoalescedBatch()->childAt(columnIndex);
    result = BaseVector::wrapInDictionary(nullptr, indices, probeRowIds.size(), srcVector);
    return;
  }
  
  // Multi-driver: gather from multiple containers
  // First, ensure all containers are coalesced
  for (auto& [driverId, container] : containers) {
    if (!container->isCoalesced()) {
      container->coalesceBatches();
    }
  }
  
  auto resultType = containers.begin()->second->getCoalescedBatch()->childAt(columnIndex)->type();
  result = BaseVector::create(resultType, probeRowIds.size(), pool);
  
  for (size_t i = 0; i < probeRowIds.size(); ++i) {
    uint8_t driverId = probeRowIds[i] >> 56;
    uint64_t localIdx = probeRowIds[i] & ((1ULL << 56) - 1);
    auto* container = containers.at(driverId);
    result->copy(container->getCoalescedBatch()->childAt(columnIndex).get(), i, localIdx, 1);
  }
}
```

### 5.5 Probe Row ID Resolution

Similar to row pointer resolution, but for probe side:

```cpp
/// Resolve probe row IDs for a specific ProbePayloadContainer
std::vector<uint64_t> resolveProbeRowIds(
    char* const* matchedRows,
    int32_t numRows,
    HybridContainer* currentContainer,
    ProbePayloadContainer* targetContainer) {
  
  std::vector<uint64_t> result(numRows);
  
  for (int32_t i = 0; i < numRows; ++i) {
    char* row = matchedRows[i];
    HybridContainer* current = currentContainer;
    
    // Walk to the level that owns the target ProbePayloadContainer
    while (current->probePayloadContainer_ != targetContainer && current != nullptr) {
      uint64_t hybridRowId = current->keys_->valueAt<uint64_t>(row, hybridRowIdOffset);
      uint64_t localIdx = hybridRowId & ((1ULL << 56) - 1);
      
      // Get probe rowId from this level
      result[i] = current->upstreamProbeRowIds_[localIdx];
      
      // Move to parent
      row = current->upstreamBuildRowPtrs_[localIdx];
      current = current->upstreamBuildContainer_;
    }
    
    // At target level, get the probe rowId
    uint64_t hybridRowId = current->keys_->valueAt<uint64_t>(row, hybridRowIdOffset);
    uint64_t localIdx = hybridRowId & ((1ULL << 56) - 1);
    result[i] = current->upstreamProbeRowIds_[localIdx];
  }
  
  return result;
}
```

---

## 6. Payload → Key Extraction Handling

### 6.1 The Problem

When a column that was originally **payload** becomes a **join key** in a downstream join:

```
Join 1: customer (build) ⋈ orders (probe) on c_custkey = o_custkey
  c_nationkey is payload in HybridContainer₀.owningInputs_

Join 2: result ⋈ nation (probe) on c_nationkey = n_nationkey
  c_nationkey must be extracted and stored in HybridContainer₁.keys_
```

### 6.2 The Solution

When HashBuild extracts a key column from upstream:

1. **Extract the column** from the upstream source (using ColumnSourceMap)
2. **Store it in current `keys_`** (RowContainer)
3. **Update ColumnSourceMap** to point to new location:

```cpp
// Before: c_nationkey comes from parent's payload
sources[outputChannel] = {HYBRID_PAYLOAD, 2, HybridContainer₀*};

// After extraction as key: c_nationkey now in current keys_
sources[outputChannel] = {HYBRID_KEY, keyIndex, HybridContainer₁*};
```

4. **(Optional) Free original payload column** if no other references

### 6.3 Implementation in HashBuild

```cpp
void HashBuild::addInput(RowVectorPtr input) {
  if (driverCtx->buildSideLateMEnabled) {
    // 1. Extract only join key columns from upstream
    RowVectorPtr extractedKeys = extractKeysFromUpstream(
        driverCtx->buildSideLateMBuildRowPtrs,
        driverCtx->columnSourceMap,
        keyChannels_,
        pool());
    
    // 2. Update source map: these keys now live in our keys_
    for (size_t i = 0; i < keyChannels_.size(); ++i) {
      int32_t outputChannel = keyChannels_[i];
      driverCtx->columnSourceMap[outputChannel] = {
        ColumnSource::HYBRID_KEY, 
        static_cast<int32_t>(i), 
        table_->hybridData()
      };
    }
    
    // 3. Store extracted keys in RowContainer
    table_->addInput(extractedKeys);
    
    // 4. Store upstream row pointers for payload access later
    table_->hybridData()->appendUpstreamRefs(
        driverCtx->buildSideLateMBuildRowPtrs,
        driverCtx->buildSideLateMProbeRowIds);
    
    // 5. Clear batch-level state
    driverCtx->buildSideLateMBuildRowPtrs.clear();
    driverCtx->buildSideLateMProbeRowIds.clear();
  } else {
    // Normal path: add full input
    table_->addInput(input);
  }
}
```

---

## 7. DriverCtx Fields for Late Materialization

```cpp
struct DriverCtx {
  // === Late Materialization State ===
  
  /// Whether build-side late-m is enabled for this pipeline
  bool buildSideLateMEnabled = false;
  
  /// Column source metadata: output channel → source location
  /// Passed from HashProbe to HashBuild, updated as columns are extracted
  ColumnSourceMap columnSourceMap;
  
  /// Upstream row pointers for current batch (build side)
  /// These are char* pointers directly into upstream HybridContainer rows
  std::vector<char*> buildSideLateMBuildRowPtrs;
  
  /// Upstream probe row IDs for current batch
  /// Encoded as (driverId << 56) | localRowIndex
  std::vector<uint64_t> buildSideLateMProbeRowIds;
  
  /// Shared ownership of upstream containers (keeps char* pointers valid)
  std::unordered_map<uint8_t, std::shared_ptr<HybridContainer>> 
      buildSideLateMUpstreamBuildContainers;
  std::unordered_map<uint8_t, std::shared_ptr<ProbePayloadContainer>> 
      buildSideLateMUpstreamProbePayloads;
  
  /// Plan node ID where materialization should happen (final probe)
  std::string materializationPlanNodeId;
  
  /// Clear batch-level state between batches
  void clearBatchState() {
    buildSideLateMBuildRowPtrs.clear();
    buildSideLateMProbeRowIds.clear();
  }
  
  /// Clear all late-m state (called at pipeline end)
  void clearLateMaterializationState() {
    buildSideLateMEnabled = false;
    columnSourceMap.clear();
    buildSideLateMBuildRowPtrs.clear();
    buildSideLateMProbeRowIds.clear();
    buildSideLateMUpstreamBuildContainers.clear();
    buildSideLateMUpstreamProbePayloads.clear();
    materializationPlanNodeId.clear();
  }
};
```

---

## 8. Operator-Specific Logic

### 8.1 HashProbe

```cpp
void HashProbe::fillOutput(int32_t size) {
  if (!lateMaterializationEnabled_) {
    // Standard extraction (baseline path)
    extractColumns(table_, rows, projections, resultVectors);
    return;
  }
  
  bool isMaterializationPoint = (planNodeId() == driverCtx_->materializationPlanNodeId);
  
  if (isMaterializationPoint) {
    // FINAL PROBE: Extract all columns using ColumnSourceMap
    extractAllColumnsLateMaterialization(
        table_,
        outputTableRows_.data(),
        size,
        input_,
        outputRowMapping_,
        driverCtx_->columnSourceMap,
        pool(),
        outputType_,
        output_->children());
  } else {
    // INTERMEDIATE PROBE: Pass row pointers to downstream HashBuild
    driverCtx_->buildSideLateMBuildRowPtrs.reserve(
        driverCtx_->buildSideLateMBuildRowPtrs.size() + size);
    driverCtx_->buildSideLateMProbeRowIds.reserve(
        driverCtx_->buildSideLateMProbeRowIds.size() + size);
    
    for (int32_t i = 0; i < size; ++i) {
      char* matchedRow = outputTableRows_[i];
      
      // Store build row pointer for downstream
      driverCtx_->buildSideLateMBuildRowPtrs.push_back(matchedRow);
      
      // Store probe row ID (encoded with driverId)
      uint64_t probeRowId = (static_cast<uint64_t>(driverId_) << 56) | probeRowIdCounter_++;
      driverCtx_->buildSideLateMProbeRowIds.push_back(probeRowId);
    }
    
    // Store probe payload for this level
    if (!probePayloadContainer_) {
      probePayloadContainer_ = std::make_shared<ProbePayloadContainer>(
          probePayloadChannels_, pool());
    }
    probePayloadContainer_->addBatch(input_);
    
    // Update ColumnSourceMap for probe columns
    for (auto& projection : projectedInputColumns_) {
      driverCtx_->columnSourceMap[projection.outputChannel] = ColumnSource{
        ColumnSource::PROBE_PAYLOAD,
        projection.inputChannel,
        probePayloadContainer_.get(),
        driverId_
      };
    }
    
    // Pass shared ownership to DriverCtx
    driverCtx_->buildSideLateMUpstreamProbePayloads[driverId_] = probePayloadContainer_;
  }
}
```

### 8.2 HashBuild

```cpp
void HashBuild::addInput(RowVectorPtr input) {
  if (driverCtx_->buildSideLateMEnabled && 
      !driverCtx_->buildSideLateMBuildRowPtrs.empty()) {
    // Late-m path: extract only keys from upstream
    
    // 1. Extract key columns using ColumnSourceMap
    RowVectorPtr extractedKeys = extractKeysFromSourceMap(
        driverCtx_->buildSideLateMBuildRowPtrs,
        driverCtx_->columnSourceMap,
        keyChannels_,
        pool());
    
    // 2. Add keys to hash table (RowContainer)
    table_->addInput(extractedKeys);
    
    // 3. Store upstream refs for later extraction
    table_->hybridData()->appendUpstreamRefs(
        driverCtx_->buildSideLateMBuildRowPtrs,
        driverCtx_->buildSideLateMProbeRowIds);
    
    // 4. Update ColumnSourceMap: keys now in this container
    for (size_t i = 0; i < keyChannels_.size(); ++i) {
      int32_t channel = keyChannels_[i];
      driverCtx_->columnSourceMap[channel] = ColumnSource{
        ColumnSource::HYBRID_KEY,
        static_cast<int32_t>(i),
        table_->hybridData()
      };
    }
    
    // 5. Clear batch state
    driverCtx_->clearBatchState();
  } else {
    // Standard path OR base table
    table_->addInput(input);
    
    // For base table: populate initial ColumnSourceMap
    if (driverCtx_->buildSideLateMEnabled && table_->hybridData()) {
      populateBaseTableSourceMap();
    }
  }
}

void HashBuild::populateBaseTableSourceMap() {
  // Keys
  for (size_t i = 0; i < keyChannels_.size(); ++i) {
    int32_t channel = keyChannels_[i];
    driverCtx_->columnSourceMap[channel] = ColumnSource{
      ColumnSource::HYBRID_KEY,
      static_cast<int32_t>(i),
      table_->hybridData()
    };
  }
  
  // Payload
  for (size_t i = 0; i < dependentChannels_.size(); ++i) {
    int32_t channel = dependentChannels_[i];
    driverCtx_->columnSourceMap[channel] = ColumnSource{
      ColumnSource::HYBRID_PAYLOAD,
      static_cast<int32_t>(i),
      table_->hybridData()
    };
  }
}
```

### 8.3 Sort (OrderBy)

```cpp
void OrderBy::addInput(RowVectorPtr input) {
  if (driverCtx_->buildSideLateMEnabled) {
    // Extract only sort key columns
    RowVectorPtr sortKeys = extractSortKeysFromSourceMap(
        driverCtx_->buildSideLateMBuildRowPtrs,
        driverCtx_->columnSourceMap,
        sortKeyChannels_,
        pool());
    
    // Store row pointers for final extraction (after sorting)
    storedBuildRowPtrs_.insert(
        storedBuildRowPtrs_.end(),
        driverCtx_->buildSideLateMBuildRowPtrs.begin(),
        driverCtx_->buildSideLateMBuildRowPtrs.end());
    storedProbeRowIds_.insert(
        storedProbeRowIds_.end(),
        driverCtx_->buildSideLateMProbeRowIds.begin(),
        driverCtx_->buildSideLateMProbeRowIds.end());
    
    // Add to sort container with row pointer indices
    sortContainer_->addInput(sortKeys);
    
    driverCtx_->clearBatchState();
  } else {
    sortContainer_->addInput(input);
  }
}

RowVectorPtr OrderBy::getOutput() {
  if (!sortContainer_->hasOutput()) {
    return nullptr;
  }
  
  if (driverCtx_->buildSideLateMEnabled) {
    // Get sorted indices
    auto sortedIndices = sortContainer_->getSortedIndices();
    
    // Reorder row pointers according to sort order
    std::vector<char*> sortedRowPtrs(sortedIndices.size());
    std::vector<uint64_t> sortedProbeRowIds(sortedIndices.size());
    for (size_t i = 0; i < sortedIndices.size(); ++i) {
      sortedRowPtrs[i] = storedBuildRowPtrs_[sortedIndices[i]];
      sortedProbeRowIds[i] = storedProbeRowIds_[sortedIndices[i]];
    }
    
    // Extract all columns in sorted order
    return extractAllColumnsLateMaterialization(
        sortedRowPtrs,
        sortedProbeRowIds,
        driverCtx_->columnSourceMap,
        pool(),
        outputType_);
  } else {
    return sortContainer_->getOutput();
  }
}
```

---

## 9. Memory Ownership

### 9.1 Ownership Rules

| Data | Owner | Kept Alive By |
|------|-------|---------------|
| `HybridContainer` | `HashTable::hybridData_` | `upstreamBuildContainers_` (shared_ptr) |
| `ProbePayloadContainer` | `HashProbe` initially | `upstreamProbePayloads_` (shared_ptr) |
| `char*` row pointers | `RowContainer` in HybridContainer | HybridContainer lifetime |
| `ColumnSourceMap` | DriverCtx | Driver lifetime |

### 9.2 Cleanup Order

```cpp
// In Driver::closeOperators():
1. driverCtx_->clearLateMaterializationState()  // Release shared_ptrs
2. for each operator: op->close()               // HashTable destroyed here
```

---

## 10. Implementation Checklist

### Phase 1: Data Structures

- [ ] Add `ColumnSource` struct to RowContainer.h
- [ ] Add `ColumnSourceMap` typedef to RowContainer.h
- [ ] Add late-m fields to DriverCtx (Driver.h):
  - [ ] `buildSideLateMEnabled`
  - [ ] `columnSourceMap`
  - [ ] `buildSideLateMBuildRowPtrs`
  - [ ] `buildSideLateMProbeRowIds`
  - [ ] `buildSideLateMUpstreamBuildContainers`
  - [ ] `buildSideLateMUpstreamProbePayloads`
  - [ ] `materializationPlanNodeId`
- [ ] Add `clearBatchState()` and `clearLateMaterializationState()` to DriverCtx

### Phase 2: HybridContainer

- [ ] Add `appendUpstreamRefs(buildRowPtrs, probeRowIds)` method
- [ ] Add `upstreamBuildRowPtrs_` and `upstreamProbeRowIds_` vectors
- [ ] Add `getHybridRowId(char* row)` helper method
- [ ] Add `isCoalesced()` and `coalesceBatches()` for payload extraction
- [ ] Add `getCoalescedBatch()` accessor

### Phase 3: HashBuild Changes

- [ ] Detect late-m mode: check `driverCtx->buildSideLateMEnabled`
- [ ] Base table: populate initial `ColumnSourceMap` with keys and payload
- [ ] N-way: extract only key columns using `ColumnSourceMap`
- [ ] N-way: call `appendUpstreamRefs()` to store pointers
- [ ] N-way: update `ColumnSourceMap` for extracted keys

### Phase 4: HashProbe Changes

- [ ] Check `materializationPlanNodeId` to determine if final probe
- [ ] Intermediate probe:
  - [ ] Store `(buildRowPtr, probeRowId)` pairs in DriverCtx
  - [ ] Create/update `ProbePayloadContainer`
  - [ ] Update `ColumnSourceMap` for probe columns
- [ ] Final probe:
  - [ ] Implement `extractAllColumnsLateMaterialization()`
  - [ ] Resolve row pointers for each source container
  - [ ] Extract each column based on `ColumnSourceMap`

### Phase 5: Sort Integration (if applicable)

- [ ] Extract only sort keys in late-m mode
- [ ] Store row pointers with sort indices
- [ ] Extract all columns in sorted order at output

### Phase 6: Testing

- [ ] Unit tests for `ColumnSource` / `ColumnSourceMap`
- [ ] Integration test: 2-way join
- [ ] Integration test: 3-way join (Q27)
- [ ] Multi-driver test (num_drivers=4)
- [ ] Performance benchmark vs baseline

---

## 11. Success Criteria

**Build command**: 
```bash
ninja -C _build/Release bolt_tpch_benchmark
```

**Test command (late-m enabled)**:
```bash
_build/Release/bolt/benchmarks/tpch/bolt_tpch_benchmark \
  --run_query_verbose=27 \
  --num_drivers=4 \
  --late_materialization_enabled=true \
  --hybrid_join_enabled=true \
  --hybrid_sort_enabled=true \
  --data_path=/home/jiang.2091/velox_join/data/tpch_parquet/sf3_hive
```

**Baseline command**:
```bash
_build/Release/bolt/benchmarks/tpch/bolt_tpch_benchmark \
  --run_query_verbose=27 \
  --num_drivers=4 \
  --late_materialization_enabled=false \
  --hybrid_join_enabled=false \
  --hybrid_sort_enabled=false \
  --data_path=/home/jiang.2091/velox_join/data/tpch_parquet/sf3_hive
```

| Metric | Target |
|--------|--------|
| Q27 runtime | ≤ baseline (27.60s with num_drivers=1) |
| Multi-driver | Works correctly with num_drivers=4 |
| Result correctness | Matches baseline output |
| Memory usage | ≤ baseline (no duplicate storage) |

---

## 12. Appendix: Example Data Flow for Q27

```
┌─────────────────────────────────────────────────────────────────────┐
│ Pipeline 0: customer → HashBuild₀                                   │
├─────────────────────────────────────────────────────────────────────┤
│ HybridContainer₀:                                                   │
│   keys_: [c_custkey | hybridRowId]                                  │
│   owningInputs_: [c_name, c_address, c_nationkey]                   │
│                                                                     │
│ ColumnSourceMap (initialized):                                      │
│   {0: {HYBRID_KEY, 0, HC₀},                                         │
│    1: {HYBRID_PAYLOAD, 0, HC₀},                                     │
│    2: {HYBRID_PAYLOAD, 1, HC₀},                                     │
│    3: {HYBRID_PAYLOAD, 2, HC₀}}                                     │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│ Pipeline 1: orders → HashProbe₀ → HashBuild₁                        │
├─────────────────────────────────────────────────────────────────────┤
│ HashProbe₀ (intermediate):                                          │
│   - Probes HybridContainer₀                                         │
│   - Stores (char*, uint64_t) pairs in DriverCtx                     │
│   - Creates ProbePayloadContainer for orders payload                │
│   - Updates ColumnSourceMap with orders columns                     │
│                                                                     │
│ HashBuild₁:                                                         │
│   - Extracts o_orderkey from orders (key for next join)             │
│   - Stores in HybridContainer₁.keys_                                │
│   - Stores upstream refs                                            │
│                                                                     │
│ HybridContainer₁:                                                   │
│   keys_: [o_orderkey | hybridRowId]                                 │
│   owningInputs_: []  (empty)                                        │
│   upstreamBuildRowPtrs_: [→ customer rows]                          │
│   upstreamProbeRowIds_: [→ orders payload]                          │
│                                                                     │
│ ColumnSourceMap (updated):                                          │
│   {0: {HYBRID_KEY, 0, HC₁},           // o_orderkey (in HC₁)        │
│    1: {HYBRID_KEY, 0, HC₀},           // c_custkey                  │
│    2: {HYBRID_PAYLOAD, 0, HC₀},       // c_name                     │
│    3: {HYBRID_PAYLOAD, 1, HC₀},       // c_address                  │
│    4: {PROBE_PAYLOAD, 0, PPC₀},       // o_orderdate                │
│    5: {PROBE_PAYLOAD, 1, PPC₀}}       // o_totalprice               │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│ Pipeline 2: lineitem → HashProbe₁ → OrderBy → Output                │
├─────────────────────────────────────────────────────────────────────┤
│ HashProbe₁ (materialization point):                                 │
│   - Probes HybridContainer₁                                         │
│   - For each output column:                                         │
│       1. Look up source in ColumnSourceMap                          │
│       2. Resolve row pointers (follow upstreamBuildRowPtrs_ if needed)│
│       3. Extract:                                                   │
│          - HYBRID_KEY: from keys_ RowContainer                      │
│          - HYBRID_PAYLOAD: from owningInputs_ (dictionary wrap)     │
│          - PROBE_PAYLOAD: from ProbePayloadContainer                │
│          - CURRENT_PROBE: dictionary wrap lineitem columns          │
│   - Output: fully materialized RowVectorPtr                         │
│                                                                     │
│ OrderBy: sort the materialized output                               │
│ Output: final query result                                          │
└─────────────────────────────────────────────────────────────────────┘
```

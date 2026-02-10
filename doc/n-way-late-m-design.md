# N-Way Build-Side Late Materialization Design

**Version**: 5.0  
**Date**: 2026-02-10  
**Status**: Implemented

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

Each output column maps to exactly ONE source location (defined in RowContainer.h):

```cpp
/// Describes where a column's data lives.
struct ColumnSource {
  enum class Type {
    HYBRID_KEY,      // From HybridContainer's keys_ (RowContainer)
    HYBRID_PAYLOAD,  // From HybridContainer's owningInputs_ (columnar batches)
    PROBE_PAYLOAD,   // From ProbePayloadContainer (columnar batches)
    CURRENT_PROBE    // From current probe input (not stored, just dictionary wrap)
  };
  
  Type type;
  int32_t columnIndex;  // Column index within the source container
  
  // Direct pointer to source - no depth traversal needed
  HybridContainer* hybridContainer = nullptr;         // For HYBRID_KEY, HYBRID_PAYLOAD
  ProbePayloadContainer* probePayloadContainer = nullptr;  // For PROBE_PAYLOAD
};
```

### 2.2 LevelRowRefs: Pre-computed Extraction Levels

For N-way extraction, we pre-compute row references at each level:

```cpp
/// Holds row references for one level in the N-way join chain.
/// Used by extractColumnsFromUpstream for efficient batch extraction.
struct LevelRowRefs {
  std::vector<char*> buildRowPtrs;    // Build row pointers for this level
  std::vector<uint64_t> probeRowIds;  // Encoded probe row IDs
  HybridContainer* hybridContainer;   // Container at this level
  ProbePayloadContainer* probePayloadContainer;  // For PROBE_PAYLOAD matching
};
```

### 2.3 HybridContainer Structure

```cpp
class HybridContainer {
  // === Row-based key storage ===
  RowContainer* keys_;              // Join keys + hybridRowId (uint64_t)
  
  // === Columnar payload storage ===
  std::vector<RowVectorPtr> owningInputs_;  // Payload columns (coalesced into one batch)
  RowVectorPtr coalescedInput_;             // Single batch for extraction
  
  // === Upstream row pointers (for N-way chain) ===
  std::vector<char*> upstreamBuildRowPtrs_;   // Direct pointers to parent join's rows
  std::vector<uint64_t> upstreamProbeRowIds_; // Encoded (driverId << 56 | rowIndex)
  
  // === Multi-driver support ===
  uint8_t containerId_;  // Driver/container ID (0-255)
  std::unordered_map<uint8_t, HybridContainer*> allContainers_;  // All containers after merge
  
  // === Cross-driver container access (after merge via JoinBridge) ===
  std::unordered_map<uint8_t, std::shared_ptr<HybridContainer>> upstreamBuildContainers_;
  std::unordered_map<uint8_t, std::shared_ptr<ProbePayloadContainer>> upstreamProbePayloads_;
  
public:
  // Multi-driver support methods
  void setId(uint8_t id) { containerId_ = id; }
  uint8_t getId() const { return containerId_; }
  void setAllContainers(const std::unordered_map<uint8_t, HybridContainer*>& containers);
  const std::unordered_map<uint8_t, HybridContainer*>& getAllContainers() const;
  
  // Upstream reference management
  void appendUpstreamRefs(const std::vector<char*>& buildRowPtrs,
                          const std::vector<uint64_t>& probeRowIds);
  char* getUpstreamBuildRowPtr(size_t localIdx) const;
  uint64_t getUpstreamProbeRowId(size_t localIdx) const;
  
  // Batch management
  void coalesceBatches();
  bool isCoalesced() const;
};
```

### 2.4 ProbePayloadContainer Structure

```cpp
class ProbePayloadContainer {
  memory::MemoryPool* pool_;
  std::vector<RowVectorPtr> batches_;   // Probe-side payload columns
  RowVectorPtr coalescedBatch_;         // Coalesced for extraction
  bool isCoalesced_{false};
  
  // Multi-driver support
  uint8_t containerId_{0};
  std::unordered_map<uint8_t, ProbePayloadContainer*> allContainers_;
  
public:
  void addBatch(RowVectorPtr batch);
  void coalesceBatches();
  bool isCoalesced() const { return isCoalesced_; }
  RowVectorPtr getCoalescedBatch() const { return coalescedBatch_; }
  
  void setId(uint8_t id) { containerId_ = id; }
  uint8_t getId() const { return containerId_; }
  void setAllContainers(const std::unordered_map<uint8_t, ProbePayloadContainer*>& containers);
};
```

---

## 3. Row ID Encoding

### 3.1 HybridRowId (Stored in keys_)

Each row in `keys_` has a `hybridRowId` column (uint64_t) after the join keys:

```
hybridRowId = (containerId << 56) | localRowIndex
```

- Top 8 bits: container/driver ID (0-255)
- Bottom 56 bits: local row index within that container

### 3.2 Upstream Pointers

For N-way joins, each HybridContainer stores:
- `upstreamBuildRowPtrs_[localIdx]` → `char*` pointer to parent join's row
- `upstreamProbeRowIds_[localIdx]` → encoded rowId for probe payload

---

## 4. Pattern Detection in LocalPlanner

N-way join chains are detected at planning time in `LocalPlanner.cpp`:

```cpp
/// Detects N-way join patterns where a HashJoin's output becomes another
/// HashJoin's build input. When detected, marks factories for late materialization.
void detectNWayJoinChains(
    const core::PlanNodePtr& root,
    std::vector<std::unique_ptr<DriverFactory>>& factories) {
  
  // Traverse plan tree to find HashJoin chains
  // Pattern: HashJoin₁ output → HashJoin₂ build side (source[1])
  
  std::function<void(const core::PlanNodePtr&, const core::PlanNodeId*)> traversePlan;
  traversePlan = [&](const core::PlanNodePtr& node, const core::PlanNodeId* outerJoinId) {
    if (auto hashJoin = std::dynamic_pointer_cast<const core::HashJoinNode>(node)) {
      // Check if build side contains another HashJoin
      auto buildSource = hashJoin->sources().size() > 1 ? hashJoin->sources()[1] : nullptr;
      
      if (hasBuildSideJoin(buildSource)) {
        // Found N-way pattern - mark for late-m
        markNWayChain(hashJoin, outerJoinId ? *outerJoinId : hashJoin->id());
      }
    }
  };
  
  traversePlan(root, nullptr);
  
  // Mark factories with nWayJoinLateMEnabled and nWayMaterializationPlanNodeId
}
```

### 4.1 DriverFactory Fields

```cpp
struct DriverFactory {
  // ... existing fields ...
  
  // === N-way Join Late Materialization ===
  
  /// Whether this pipeline participates in an N-way join chain
  bool nWayJoinLateMEnabled{false};

  /// Plan node ID where final materialization should happen
  core::PlanNodeId nWayMaterializationPlanNodeId;
};
```

### 4.2 Propagation to DriverCtx

In `DriverFactory::createDriver()`:

```cpp
std::shared_ptr<Driver> DriverFactory::createDriver(...) {
  auto driver = std::shared_ptr<Driver>(new Driver());
  ctx->driver = driver.get();

  // Propagate N-way join late materialization settings to DriverCtx
  if (nWayJoinLateMEnabled) {
    ctx->buildSideLateMEnabled = true;
    ctx->materializationPlanNodeId = nWayMaterializationPlanNodeId;
  }
  // ...
}
```

---

## 5. DriverCtx Late Materialization State

```cpp
struct DriverCtx {
  // === Late Materialization State ===
  
  /// Whether build-side late-m is enabled for this pipeline
  bool buildSideLateMEnabled = false;
  
  /// Column source metadata: output channel → source location
  std::unordered_map<int32_t, ColumnSource> columnSourceMap;
  
  /// Upstream row pointers for current batch (build side)
  std::vector<char*> buildSideLateMBuildRowPtrs;
  
  /// Upstream probe row IDs for current batch
  std::vector<uint64_t> buildSideLateMProbeRowIds;
  
  /// Shared ownership of upstream containers
  std::unordered_map<uint8_t, std::shared_ptr<HybridContainer>> 
      buildSideLateMUpstreamBuildContainers;
  std::unordered_map<uint8_t, std::shared_ptr<ProbePayloadContainer>> 
      buildSideLateMUpstreamProbePayloads;
  
  /// Plan node ID where materialization should happen (final probe)
  std::string materializationPlanNodeId;
  
  void clearBatchState();
  void clearLateMaterializationState();
};
```

---

## 6. HashBuild Implementation

### 6.1 Mode Detection

```cpp
HashBuild::HashBuild(...) {
  // Enable N-way late-m if:
  // 1. buildSideLateMEnabled is set
  // 2. We're not the materialization point (not final build)
  isNWayLateMEnabled_ = driverCtx->buildSideLateMEnabled &&
      !driverCtx->materializationPlanNodeId.empty();
}
```

### 6.2 Base Table Path (No Upstream)

For the first HashBuild in a chain (base table):

```cpp
void HashBuild::addInput(RowVectorPtr input) {
  if (isNWayLateMEnabled_ && driverCtx_->buildSideLateMBuildRowPtrs.empty()) {
    // Base table: populate initial ColumnSourceMap
    // Keys → HYBRID_KEY
    // Payload → HYBRID_PAYLOAD
    table_->addInput(input);
    // ColumnSourceMap populated in noMoreInput
  }
}

void HashBuild::updateSourceMapForNWay() {
  auto* hybridData = table_->hybridData();
  
  // Map key columns
  for (size_t i = 0; i < keyChannelMap_.size(); ++i) {
    int32_t outputChannel = keyChannelMap_[i].outputChannel;
    driverCtx_->columnSourceMap[outputChannel] = ColumnSource{
        ColumnSource::Type::HYBRID_KEY,
        static_cast<int32_t>(i),
        hybridData};
  }
  
  // Map payload columns
  for (size_t i = 0; i < dependentChannelMap_.size(); ++i) {
    int32_t outputChannel = dependentChannelMap_[i].outputChannel;
    driverCtx_->columnSourceMap[outputChannel] = ColumnSource{
        ColumnSource::Type::HYBRID_PAYLOAD,
        static_cast<int32_t>(i),
        hybridData};
  }
}
```

### 6.3 N-Way Path (With Upstream)

For subsequent HashBuilds receiving upstream data:

```cpp
void HashBuild::addInput(RowVectorPtr input) {
  if (isNWayLateMEnabled_ && !driverCtx_->buildSideLateMBuildRowPtrs.empty()) {
    // N-way path: extract only key columns from upstream
    RowVectorPtr extractedKeys = extractKeysFromUpstream(
        driverCtx_->buildSideLateMBuildRowPtrs,
        driverCtx_->columnSourceMap,
        keyChannels_);
    
    // Add keys to hash table
    table_->addInput(extractedKeys);
    
    // Store upstream refs for later extraction
    table_->hybridData()->appendUpstreamRefs(
        driverCtx_->buildSideLateMBuildRowPtrs,
        driverCtx_->buildSideLateMProbeRowIds);
    
    // Clear batch state
    driverCtx_->clearBatchState();
  }
}
```

---

## 7. HashProbe Implementation

### 7.1 Mode Detection

```cpp
HashProbe::HashProbe(...) {
  // Output is N-way if there's a downstream HashBuild waiting
  // (i.e., we're not the final materialization point)
  isNWayLateMOutput_ = driverCtx->buildSideLateMEnabled &&
      !driverCtx->materializationPlanNodeId.empty() &&
      driverCtx->materializationPlanNodeId != planNodeId();
}
```

### 7.2 Initialization

```cpp
void HashProbe::initialize() {
  // ... standard initialization ...
  
  if (isNWayLateMOutput_ && !projectedInputColumns_.empty()) {
    // Create ProbePayloadContainer for this level
    probePayloadContainer_ = std::make_shared<ProbePayloadContainer>(pool());
    probePayloadContainer_->setId(driverId_);
    
    // Register in DriverCtx for downstream access
    driverCtx->buildSideLateMUpstreamProbePayloads[driverId_] = probePayloadContainer_;
    
    // Update columnSourceMap for probe columns
    for (size_t i = 0; i < projectedInputColumns_.size(); ++i) {
      int32_t outputChannel = projectedInputColumns_[i].outputChannel;
      driverCtx->columnSourceMap[outputChannel] = ColumnSource{
          ColumnSource::Type::PROBE_PAYLOAD,
          static_cast<int32_t>(i),
          probePayloadContainer_.get()};
    }
  }
}
```

### 7.3 Fill Output Dispatch

```cpp
void HashProbe::fillOutput(vector_size_t size) {
  // N-way late materialization paths
  if (isNWayLateMOutput_) {
    // Intermediate probe: pass rowIds downstream
    fillOutputLateMaterialization(size);
    return;
  }

  auto* driverCtx = operatorCtx_->driverCtx();
  if (driverCtx->buildSideLateMEnabled &&
      driverCtx->materializationPlanNodeId == planNodeId() &&
      !driverCtx->columnSourceMap.empty()) {
    // Final probe: do full materialization
    fillOutputFinalMaterialization(size);
    return;
  }

  // Standard path (no late-m)
  // ... existing code ...
}
```

### 7.4 Intermediate Probe (fillOutputLateMaterialization)

```cpp
void HashProbe::fillOutputLateMaterialization(vector_size_t size) {
  auto* driverCtx = operatorCtx_->driverCtx();
  
  // 1. Store probe-side input rows in ProbePayloadContainer
  if (probePayloadContainer_ && !projectedInputColumns_.empty()) {
    // Extract payload columns from input and add to container
    std::vector<VectorPtr> payloadColumns;
    for (auto projection : projectedInputColumns_) {
      payloadColumns.push_back(input_->childAt(projection.inputChannel));
    }
    auto payloadBatch = std::make_shared<RowVector>(
        pool(), probePayloadType_, nullptr, input_->size(), payloadColumns);
    probePayloadContainer_->addBatch(payloadBatch);
  }
  
  // 2. For each match, store (buildRowPtr, probeRowId) in DriverCtx
  for (int32_t i = 0; i < size; ++i) {
    char* matchedRow = outputTableRows_[i];
    if (matchedRow) {
      driverCtx->buildSideLateMBuildRowPtrs.push_back(matchedRow);
      
      // Encode probe row ID
      uint64_t probeRowIdx = probePayloadContainer_->totalRows() - input_->size() 
                            + outputRowMapping_->as<vector_size_t>()[i];
      uint64_t encodedProbeRowId = (static_cast<uint64_t>(driverId_) << 56) | probeRowIdx;
      driverCtx->buildSideLateMProbeRowIds.push_back(encodedProbeRowId);
    }
  }
  
  // 3. Update columnSourceMap for build-side columns
  updateColumnSourceMapForOutput();
  
  // 4. Create minimal output for framework compatibility
  prepareOutput(size);
}
```

### 7.5 Final Probe (fillOutputFinalMaterialization)

```cpp
void HashProbe::fillOutputFinalMaterialization(vector_size_t size) {
  prepareOutput(size);
  auto* driverCtx = operatorCtx_->driverCtx();
  
  // Use extractColumnsFromUpstream for N-way extraction
  extractColumnsFromUpstream(
      outputTableRows_.data(),
      size,
      table_->hybridData(),
      driverCtx->columnSourceMap,
      nullptr,  // currentProbePayload (handled by wrapIndirectChildren)
      pool(),
      outputType_->children(),
      output_->children());
  
  // Handle current probe columns via dictionary wrap
  wrapIndirectChildren(
      projectedInputColumns_,
      input_->children(),
      size,
      outputRowMapping_,
      output_->children());
}
```

### 7.6 Coalesce on NoMoreInput

```cpp
void HashProbe::noMoreInputInternal() {
  checkRunning();

  // N-way late materialization: coalesce probe payload batches
  if (isNWayLateMOutput_ && probePayloadContainer_) {
    probePayloadContainer_->coalesceBatches();
  }
  
  // ... existing code ...
}
```

---

## 8. N-Way Extraction Algorithm

### 8.1 Overview

The core extraction function `extractColumnsFromUpstream` uses a 3-phase algorithm:

```cpp
void extractColumnsFromUpstream(
    char* const* matchedRows,
    int32_t numRows,
    HybridContainer* currentHybrid,
    const std::unordered_map<int32_t, ColumnSource>& columnSourceMap,
    ProbePayloadContainer* currentProbePayload,  // nullptr for final probe
    memory::MemoryPool* pool,
    const std::vector<TypePtr>& resultTypes,
    std::vector<VectorPtr>& resultVectors);
```

### 8.2 Phase 1: Pre-compute Levels

Traverse from matched rows backward through `upstreamBuildRowPtrs_` chain:

```cpp
// Phase 1: Pre-compute row refs at each level
std::vector<LevelRowRefs> levels;
levels.push_back({
    std::vector<char*>(matchedRows, matchedRows + numRows),
    {},  // probeRowIds computed per-level
    currentHybrid,
    nullptr});

while (levels.back().hybridContainer->hasUpstreamRefs()) {
  LevelRowRefs& current = levels.back();
  LevelRowRefs nextLevel;
  nextLevel.buildRowPtrs.resize(numRows);
  nextLevel.probeRowIds.resize(numRows);
  
  auto* levelHybrid = current.hybridContainer;
  const auto& allContainers = levelHybrid->getAllContainers();
  
  for (int32_t i = 0; i < numRows; ++i) {
    // Decode containerId and localIdx from hybridRowId
    auto localRowIds = levelHybrid->getHybridRowIds(current.buildRowPtrs.data(), numRows);
    uint8_t containerId = localRowIds[i].containerId_;
    size_t localIdx = localRowIds[i].rowId_;
    
    // Get owning container (may differ from levelHybrid in multi-driver)
    HybridContainer* owningContainer = allContainers.at(containerId);
    nextLevel.buildRowPtrs[i] = owningContainer->getUpstreamBuildRowPtr(localIdx);
    nextLevel.probeRowIds[i] = owningContainer->getUpstreamProbeRowId(localIdx);
  }
  
  // Set upstream container info
  nextLevel.hybridContainer = levelHybrid->getUpstreamBuildContainer();
  nextLevel.probePayloadContainer = levelHybrid->getUpstreamProbePayloadContainer();
  
  levels.push_back(std::move(nextLevel));
}
```

### 8.3 Phase 2: Map Source Containers to Levels

```cpp
// Phase 2: Map ColumnSource containers to levels
std::unordered_map<HybridContainer*, size_t> hybridContainerToLevel;
std::unordered_map<ProbePayloadContainer*, size_t> probeContainerToLevel;

for (size_t lvl = 0; lvl < levels.size(); ++lvl) {
  // Register the level's primary container
  if (levels[lvl].hybridContainer) {
    hybridContainerToLevel[levels[lvl].hybridContainer] = lvl;
    // Also register ALL containers at this level (multi-driver support)
    for (const auto& [cid, container] : levels[lvl].hybridContainer->getAllContainers()) {
      hybridContainerToLevel[container] = lvl;
    }
  }
  if (levels[lvl].probePayloadContainer) {
    probeContainerToLevel[levels[lvl].probePayloadContainer] = lvl;
    for (const auto& [cid, container] : 
         levels[lvl].probePayloadContainer->getAllContainers()) {
      probeContainerToLevel[container] = lvl;
    }
  }
}
```

### 8.4 Phase 3: Extract from Each Source

```cpp
// Phase 3: Extract columns using appropriate level's row refs
for (const auto& [outputChannel, source] : columnSourceMap) {
  switch (source.type) {
    case ColumnSource::Type::HYBRID_KEY: {
      size_t lvl = hybridContainerToLevel[source.hybridContainer];
      extractFromKeys(levels[lvl].buildRowPtrs, source, resultVectors[outputChannel]);
      break;
    }
    case ColumnSource::Type::HYBRID_PAYLOAD: {
      size_t lvl = hybridContainerToLevel[source.hybridContainer];
      extractFromPayload(levels[lvl].buildRowPtrs, source, resultVectors[outputChannel]);
      break;
    }
    case ColumnSource::Type::PROBE_PAYLOAD: {
      size_t lvl = probeContainerToLevel[source.probePayloadContainer];
      extractFromProbePayload(levels[lvl].probeRowIds, source, resultVectors[outputChannel]);
      break;
    }
    case ColumnSource::Type::CURRENT_PROBE:
      // Handled by wrapIndirectChildren in caller
      break;
  }
}
```

---

## 9. Multi-Driver Support

### 9.1 Container ID Encoding

All row IDs encode the originating container/driver:
```
rowId = (containerId << 56) | localRowIndex
```

### 9.2 Container Registration After Merge

When `JoinBridge` merges hash tables from multiple drivers:

```cpp
void HashTable::mergeContainers(std::vector<HybridContainer*> allContainers) {
  // Build allContainers_ map for cross-driver access
  std::unordered_map<uint8_t, HybridContainer*> containerMap;
  for (auto* container : allContainers) {
    containerMap[container->getId()] = container;
  }
  
  // Set on all containers so any can resolve any containerId
  for (auto* container : allContainers) {
    container->setAllContainers(containerMap);
  }
}
```

### 9.3 Cross-Driver Extraction

During extraction, when following `upstreamBuildRowPtrs_`:

```cpp
// The hybridRowId encodes which container the row came from
uint8_t containerId = hybridRowId >> 56;
HybridContainer* owningContainer = allContainers_.at(containerId);

// Use owning container (not current) to get upstream ref
char* upstreamRowPtr = owningContainer->getUpstreamBuildRowPtr(localIdx);
```

---

## 10. Memory Ownership

### 10.1 Ownership Rules

| Data | Owner | Kept Alive By |
|------|-------|---------------|
| `HybridContainer` | `HashTable::hybridData_` | `upstreamBuildContainers_` (shared_ptr) |
| `ProbePayloadContainer` | `HashProbe` initially | `buildSideLateMUpstreamProbePayloads` (shared_ptr) |
| `char*` row pointers | `RowContainer` in HybridContainer | HybridContainer lifetime |
| `ColumnSourceMap` | DriverCtx | Driver lifetime |

### 10.2 Cleanup Order

```cpp
// In Driver::closeOperators():
1. driverCtx_->clearLateMaterializationState()  // Release shared_ptrs
2. for each operator: op->close()               // HashTable destroyed here
```

---

## 11. Configuration

### 11.1 Query Config

```cpp
// In QueryConfig.h
static constexpr const char* kLateMaterializationEnabled = "late_materialization_enabled";

bool lateMaterializationEnabled() const {
  return get<bool>(kLateMaterializationEnabled, false);
}
```

### 11.2 Detection Gating

In `LocalPlanner::plan()`:

```cpp
// Detect N-way join chains for late materialization (only if enabled)
if (queryConfig.lateMaterializationEnabled()) {
  detail::detectNWayJoinChains(planFragment.planNode, *driverFactories);
}
```

---

## 12. Files Modified

### Core Data Structures
- **RowContainer.h**: `ColumnSource`, `LevelRowRefs`, `ProbePayloadContainer`, `extractColumnsFromUpstream`

### Operators
- **HashBuild.cpp/h**: `isNWayLateMEnabled_`, `updateSourceMapForNWay()`, upstream ref storage
- **HashProbe.cpp/h**: `isNWayLateMOutput_`, `fillOutputLateMaterialization()`, `fillOutputFinalMaterialization()`

### Planning
- **LocalPlanner.cpp**: `detectNWayJoinChains()`, factory field propagation
- **Driver.h**: `DriverFactory::nWayJoinLateMEnabled`, `DriverCtx` late-m fields

---

## 13. Testing

### 13.1 Build

```bash
ninja -C _build/Release bolt_tpch_benchmark
```

### 13.2 Test with Late-M Enabled

```bash
_build/Release/bolt/benchmarks/tpch/bolt_tpch_benchmark \
  --run_query_verbose=27 \
  --num_drivers=4 \
  --late_materialization_enabled=true \
  --hybrid_join_enabled=true \
  --hybrid_sort_enabled=true \
  --data_path=/home/jiang.2091/velox_join/data/tpch_parquet/sf3_hive
```

### 13.3 Baseline Comparison

```bash
_build/Release/bolt/benchmarks/tpch/bolt_tpch_benchmark \
  --run_query_verbose=27 \
  --num_drivers=4 \
  --late_materialization_enabled=false \
  --hybrid_join_enabled=false \
  --hybrid_sort_enabled=false \
  --data_path=/home/jiang.2091/velox_join/data/tpch_parquet/sf3_hive
```

---

## 14. Example Data Flow (Q27)

```
┌─────────────────────────────────────────────────────────────────────┐
│ Pipeline 0: customer → HashBuild₀                                   │
├─────────────────────────────────────────────────────────────────────┤
│ HybridContainer₀:                                                   │
│   keys_: [c_custkey | hybridRowId]                                  │
│   owningInputs_: [c_name, c_address, c_nationkey]                   │
│                                                                     │
│ ColumnSourceMap (initialized via updateSourceMapForNWay):          │
│   {c_custkey:    {HYBRID_KEY, 0, HC₀},                              │
│    c_name:       {HYBRID_PAYLOAD, 0, HC₀},                          │
│    c_address:    {HYBRID_PAYLOAD, 1, HC₀},                          │
│    c_nationkey:  {HYBRID_PAYLOAD, 2, HC₀}}                          │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│ Pipeline 1: orders → HashProbe₀ → HashBuild₁                        │
├─────────────────────────────────────────────────────────────────────┤
│ HashProbe₀ (intermediate, isNWayLateMOutput_=true):                 │
│   - Probes HybridContainer₀                                         │
│   - Stores probe payloads in ProbePayloadContainer₀                 │
│   - Passes (buildRowPtr, probeRowId) via DriverCtx                  │
│   - Updates ColumnSourceMap with orders columns → PROBE_PAYLOAD     │
│                                                                     │
│ HashBuild₁ (isNWayLateMEnabled_=true):                              │
│   - Extracts o_orderkey from upstream via columnSourceMap           │
│   - Stores in HybridContainer₁.keys_                                │
│   - Stores upstream refs via appendUpstreamRefs()                   │
│                                                                     │
│ HybridContainer₁:                                                   │
│   keys_: [o_orderkey | hybridRowId]                                 │
│   owningInputs_: []  (empty - payloads stay upstream)               │
│   upstreamBuildRowPtrs_: [→ customer rows in HC₀]                   │
│   upstreamProbeRowIds_: [→ orders rows in PPC₀]                     │
│                                                                     │
│ ColumnSourceMap (updated):                                          │
│   {o_orderkey:   {HYBRID_KEY, 0, HC₁},                              │
│    c_custkey:    {HYBRID_KEY, 0, HC₀},                              │
│    c_name:       {HYBRID_PAYLOAD, 0, HC₀},                          │
│    c_address:    {HYBRID_PAYLOAD, 1, HC₀},                          │
│    o_orderdate:  {PROBE_PAYLOAD, 0, PPC₀},                          │
│    o_totalprice: {PROBE_PAYLOAD, 1, PPC₀}}                          │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│ Pipeline 2: lineitem → HashProbe₁ → OrderBy → Output                │
├─────────────────────────────────────────────────────────────────────┤
│ HashProbe₁ (final, materializationPlanNodeId == planNodeId()):      │
│   - Probes HybridContainer₁                                         │
│   - Calls extractColumnsFromUpstream():                             │
│       Phase 1: Pre-compute levels (HC₁ → HC₀)                       │
│       Phase 2: Map source containers to levels                      │
│       Phase 3: Extract each column from its source                  │
│   - Current probe (lineitem) via wrapIndirectChildren               │
│   - Output: fully materialized RowVectorPtr                         │
│                                                                     │
│ OrderBy: sort the materialized output                               │
│ Output: final query result                                          │
└─────────────────────────────────────────────────────────────────────┘
```

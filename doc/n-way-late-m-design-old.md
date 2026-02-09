# N-Way Build-Side Late Materialization Design

## 1. Overview

### 1.1 Goal
Avoid redundant col-to-row-to-col conversions between subsequent joins by deferring payload extraction until final output.

### 1.2 Scope
**Build-side late materialization only**: When a join result feeds into the **build side** of the next join, we defer materialization.

### 1.3 Query Pattern (Q27 Example)
```
Pipeline 0: customer → HashBuild₀
Pipeline 1: orders → HashProbe₀ → HashBuild₁  
Pipeline 2: lineitem → HashProbe₁ → OrderBy → Output
```

Here:
- Join₁ result (customer × orders) feeds into **build side** of Join₂
- N-way late-m defers extraction of customer/orders columns until final HashProbe₁

---

## 2. Design Principles

### 2.1 Upstream Reference Storage

For N-way late-m, we store references to upstream data differently for build vs probe sides:

| Side | Storage | Reason |
|------|---------|--------|
| **Build** | `char*` (row pointer) | Build side is row-based (RowContainer). Direct pointer access is efficient and thread-safe. |
| **Probe** | `uint64_t` (encoded rowId) | Probe side is columnar (RowVectorPtr). Need index for `childAt(col)->valueAt(index)`. |

### 2.2 Why Row Pointers for Build Side

Using `char*` instead of encoded `uint64_t` for build-side references:

1. **Efficient key extraction**: Keys are in RowContainer, accessible via `RowContainer::valueAt<T>(row, offset)`
2. **No encoding/decoding**: Direct pointer dereference, no `>> 56` or `& mask` needed
3. **Thread-safe**: Pointer directly addresses memory, no cross-driver map lookup
4. **Memory valid**: Upstream HybridContainers kept alive via `shared_ptr`

### 2.3 No Redundant Key Storage

Keys are stored ONLY in `keys_` (RowContainer), NOT in `payloadBatches_`:
- At extraction time, we have row pointers from HashProbe match results
- Keys extracted directly from RowContainer using row pointer
- Payload extracted from columnar `payloadBatches_` using hybridRowId index

---

## 3. Data Structures

### 3.1 HybridContainer (Updated)

```cpp
class HybridContainer {
  // === Per-driver storage (local) ===
  RowContainer* keys_;                        // Join keys + hybridRowId
  std::vector<RowVectorPtr> owningInputs_;    // Payload columns ONLY (no keys)
  
  // === N-way upstream references ===
  std::vector<char*> upstreamBuildRowPtrs_;   // Direct pointers to upstream build rows
  std::vector<uint64_t> upstreamProbeRowIds_; // Encoded (driverId << 56 | index) for probe
  
  // === Cross-driver access (after merge) ===
  std::unordered_map<uint8_t, HybridContainer*> allContainers_;  // Raw ptrs
  std::unordered_map<uint8_t, std::shared_ptr<ProbePayloadContainer>> upstreamProbePayloads_;
  std::unordered_map<uint8_t, std::shared_ptr<HybridContainer>> upstreamBuildContainers_;
};
```

### 3.2 RowId Encoding (Probe Side Only)

Probe rowIds use: `(driverId << 56) | localIndex`
- Top 8 bits: driver ID (0-255)
- Bottom 56 bits: local row index in ProbePayloadContainer

Build side uses direct `char*` pointers - no encoding needed.

### 3.3 Pipeline Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│ Pipeline 0: customer → HashBuild₀                                   │
├─────────────────────────────────────────────────────────────────────┤
│ HybridContainer₀:                                                   │
│   keys_: [c_custkey | hybridRowId]  (RowContainer)                  │
│   owningInputs_: [c_name, c_address, ...]  (payload only, columnar) │
│   (no upstream refs - this is a base table)                         │
└─────────────────────────────────────────────────────────────────────┘
                              ↓ (merged via JoinBridge)
┌─────────────────────────────────────────────────────────────────────┐
│ Pipeline 1: orders → HashProbe₀ → HashBuild₁                        │
├─────────────────────────────────────────────────────────────────────┤
│ HashProbe₀:                                                         │
│   - Matches customer rows, outputs join keys for HashBuild₁         │
│   - Stores (char* buildRow, uint64_t probeRowId) in DriverCtx       │
│                                                                     │
│ HashBuild₁:                                                         │
│   - Receives join keys via normal addInput()                        │
│   - Stores payload via addPayload() (NO keys in payload)            │
│   - Picks up upstream refs from DriverCtx                           │
│   HybridContainer₁:                                                 │
│     keys_: [o_orderkey | hybridRowId]                               │
│     owningInputs_: [o_totalprice, ...]  (orders payload only)       │
│     upstreamBuildRowPtrs_: [char* → customer rows]                  │
│     upstreamProbeRowIds_: [uint64_t → orders ProbePayload]          │
│     upstreamBuildContainers_: [shared_ptr → HybridContainer₀]       │
│     upstreamProbePayloads_: [shared_ptr → ProbePayloadContainer]    │
└─────────────────────────────────────────────────────────────────────┘
                              ↓ (merged via JoinBridge)
┌─────────────────────────────────────────────────────────────────────┐
│ Pipeline 2: lineitem → HashProbe₁ → OrderBy → Output                │
├─────────────────────────────────────────────────────────────────────┤
│ HashProbe₁ (isNWayMode=true):                                       │
│   - Probes HybridContainer₁                                         │
│   - For each match at hybridRowId:                                  │
│       1. Decode hybridRowId → localIdx₁                             │
│       2. char* upstreamRow = upstreamBuildRowPtrs_[localIdx₁]       │
│       3. uint64_t probeRowId = upstreamProbeRowIds_[localIdx₁]      │
│       4. Extract customer keys from upstreamRow (RowContainer)      │
│       5. Extract customer payload via hybridRowId in upstreamRow    │
│       6. Extract orders columns via probeRowId → ProbePayload       │
│       7. Extract lineitem columns from current probe input          │
│   - Outputs fully materialized RowVectorPtr                         │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 4. Key Methods

### 4.1 HybridContainer Methods

```cpp
/// Append upstream references for a batch of rows.
/// Called by HashBuild when N-way late-m is enabled.
/// @param buildRowPtrs Direct pointers to upstream build rows
/// @param probeRowIds Encoded (driverId << 56 | index) for probe side
void appendUpstreamRefs(
    const std::vector<char*>& buildRowPtrs,
    const std::vector<uint64_t>& probeRowIds) {
  // Simple append - no index mapping needed
  upstreamBuildRowPtrs_.insert(
      upstreamBuildRowPtrs_.end(), buildRowPtrs.begin(), buildRowPtrs.end());
  upstreamProbeRowIds_.insert(
      upstreamProbeRowIds_.end(), probeRowIds.begin(), probeRowIds.end());
}

/// Check if this container has upstream refs (i.e., is an intermediate join result)
bool hasUpstreamRefs() const {
  return !upstreamBuildRowPtrs_.empty();
}

/// Get upstream build row pointer for extraction
char* getUpstreamBuildRowPtr(size_t localIndex) const {
  BOLT_CHECK_LT(localIndex, upstreamBuildRowPtrs_.size());
  return upstreamBuildRowPtrs_[localIndex];
}

/// Get upstream probe rowId for extraction  
uint64_t getUpstreamProbeRowId(size_t localIndex) const {
  BOLT_CHECK_LT(localIndex, upstreamProbeRowIds_.size());
  return upstreamProbeRowIds_[localIndex];
}
```

### 4.2 Extraction at Final Probe

```cpp
// In HashProbe::extractNWayColumns():
for (size_t i = 0; i < numRows; ++i) {
  // 1. Get localIdx from current hybridRowId
  uint64_t hybridRowId = RowContainer::valueAt<uint64_t>(rows[i], hybridRowIdOffset);
  size_t localIdx = hybridRowId & ((1ULL << 56) - 1);
  
  // 2. Get upstream build row pointer (direct access, no decode needed)
  char* upstreamBuildRow = hybridData->getUpstreamBuildRowPtr(localIdx);
  
  // 3. Extract build-side key columns directly from RowContainer
  for (auto keyCol : buildKeyColumns) {
    auto value = RowContainer::valueAt<T>(upstreamBuildRow, keyCol.offset);
    // ... store in result
  }
  
  // 4. Get upstream hybridRowId from the build row for payload extraction
  uint64_t upstreamHybridRowId = RowContainer::valueAt<uint64_t>(
      upstreamBuildRow, upstreamHybridRowIdOffset);
  // ... use upstreamHybridRowId to extract payload from HybridContainer₀
  
  // 5. Get probe rowId and extract probe columns
  uint64_t probeRowId = hybridData->getUpstreamProbeRowId(localIdx);
  uint8_t probeDriverId = probeRowId >> 56;
  size_t probeLocalIdx = probeRowId & mask;
  auto* probePayload = hybridData->getUpstreamProbePayload(probeDriverId);
  // ... extract from probePayload
}
```

---

## 5. Implementation Files

| File | Purpose |
|------|---------|
| `bolt/exec/RowContainer.h` | HybridContainer with `upstreamBuildRowPtrs_`, `upstreamProbeRowIds_` |
| `bolt/exec/RowContainer.cpp` | `addPayload()`, `coalesceBatches()` (original versions) |
| `bolt/exec/HashBuild.cpp` | Store upstream refs via `appendUpstreamRefs()` |
| `bolt/exec/HashProbe.cpp` | `extractNWayColumns()` using direct pointers |
| `bolt/exec/Driver.h` | DriverCtx fields for N-way state transfer |

---

## 6. Memory Ownership

### 6.1 Ownership Rules

| Data | Owner | Lifetime |
|------|-------|----------|
| Build row pointers (`char*`) | RowContainer in upstream HashTable | Until upstream HashTable destroyed |
| HybridContainer | HashTable | Until HashTable destroyed |
| upstreamBuildContainers_ | `shared_ptr` in HybridContainer | Keeps upstream alive |
| upstreamProbePayloads_ | `shared_ptr` in HybridContainer | Keeps probe data alive |

### 6.2 Why Row Pointers are Safe

The `char*` pointers in `upstreamBuildRowPtrs_` remain valid because:
1. Upstream HybridContainer is kept alive via `upstreamBuildContainers_` (shared_ptr)
2. The RowContainer memory is managed by the HybridContainer
3. Cleanup order in Driver ensures shared_ptrs released before destruction

### 6.3 Cleanup Order

```cpp
// In Driver::closeOperators():
1. driverCtx_->clearLateMaterializationState()  // Release DriverCtx shared_ptrs
2. for each operator: op->close()               // HashTable destroyed here
```

---

## 7. Changes from Previous Design

### 7.1 Removed: addFullInput

**Before**: Stored full input (keys + payload) redundantly
```cpp
table_->hybridData()->addFullInput(input);  // Keys stored twice!
```

**After**: Store only payload, extract keys from RowContainer
```cpp
table_->hybridData()->addPayload(payloadInput);  // Payload only
// Keys extracted via row pointer at extraction time
```

### 7.2 Removed: fullInputMode_ in coalesceBatches

**Before**: Complex logic to handle both fullInputMode_ and payloadTypes_
```cpp
const bool usePayloadTypes = !fullInputMode_ && ...;
// Many branches for different modes
```

**After**: Original simple coalesceBatches
```cpp
// Always use payloadTypes_, no mode switching
```

### 7.3 Changed: upstreamBuildRowIds_ → upstreamBuildRowPtrs_

**Before**: Encoded uint64_t requiring decode
```cpp
std::vector<uint64_t> upstreamBuildRowIds_;  // (driverId << 56) | localIdx
// Extraction requires: decode → find container → find row
```

**After**: Direct char* pointers
```cpp
std::vector<char*> upstreamBuildRowPtrs_;  // Direct pointer
// Extraction: just dereference
```

### 7.4 Simplified: appendUpstreamRefs

**Before**: Complex index mapping with resize and max calculation
```cpp
void addUpstreamRowIdPairs(localIndices, buildRowIds, probeRowIds) {
  // Find max index, resize, assign by index
}
```

**After**: Simple append
```cpp
void appendUpstreamRefs(buildRowPtrs, probeRowIds) {
  // Just append to vectors
}
```

---

## 8. Revision History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-02-05 | Initial rewrite focusing on current implementation |
| 2.0 | 2026-02-05 | Redesign: char* for build side, remove addFullInput, simplify coalesceBatches |

## Appendix: Old Design Doc

The previous design document has been archived as `n-way-late-m-design-old.md` for reference.

---

## v3.0 Checkpoint: Design Fixes and Refactoring Plan

**Date**: 2026-02-06

### Overview

After code review of HashProbe.cpp, we identified 10 design issues that need to be addressed. These issues cause code duplication, incorrect behavior, and the current 18-minute performance regression (vs 23s baseline) on Q27.

---

### Issue #1: Unify Single-Join and N-Way Late-M

**Problem**: Two separate code paths exist for single-join late-m (`lateMaterializationEnabled_`) and N-way late-m (`nWayLateMCandidate_`). This causes duplication in:
- Container initialization (lines 870-896 vs 905-922)
- Probe payload storage (`probePayloadContainer_` vs `nWayProbePayloadContainer_`)
- RowId counter updates (lines 1954-1962)

**Fix**: Single-join is just N=1 special case. Unify both into a single code path:
- Use one container: `probePayloadContainer_` for all cases
- Use one flag: `lateMaterializationEnabled_` (Point #5)
- Remove `nWayLateMCandidate_` and `nWayProbePayloadContainer_`

---

### Issue #2: Remove Legacy matchRowContainer_ Code

**Problem**: Lines 1477-1538 in `fillOutput()` use the old single-join late-m design with `matchRowContainer_`:
- Creates RowContainer with schema `[sort_keys..., buildRowId, probeRowId]`
- Copies sort keys (unnecessary overhead)
- Uses integer rowIds instead of `char*` pointers

**Fix**: Remove entire `matchRowContainer_` path. After unification:
- Intermediate probes: pass `(char*, uint64_t)` pairs via DriverCtx
- Final probes: extract directly or pass to Sort

---

### Issue #3: extractNWayColumns Called for All Joins

**Problem**: Lines 1571-1646 call `extractNWayColumns()` whenever `hasUpstreamRefs()` is true. But extraction should ONLY happen at the **materialization point** (final probe before sink or Sort), not at intermediate probes.

**Fix**: Check `materializationPlanNodeId` instead of `hasUpstreamRefs()`:
```cpp
if (planNodeId() == driverCtx->materializationPlanNodeId) {
  extractNWayColumns(...);  // Final probe: extract
} else {
  // Intermediate probe: pass rowIds to next HashBuild
}
```

---

### Issue #4: Wrong isNWayMode Check Uses hasUpstreamRefs()

**Problem**: Line 1577 checks `hasUpstreamRefs()` which tells us about **upstream** structure. But the decision to extract depends on **downstream** operators (whether we're the final probe before sink/Sort).

**Fix**: Materialization point must be determined during **planning phase**:
1. Plan optimizer traverses the plan tree
2. Identifies the last HashProbe before sink/Sort
3. Stores `materializationPlanNodeId` in DriverCtx
4. At runtime, each operator checks: `planNodeId() == materializationPlanNodeId`

---

### Issue #5: Two Separate Config Flags

**Problem**: Separate flags `lateMaterializationEnabled_` (single-join) and `nWayLateMCandidate_` (N-way) with different initialization logic.

**Fix**: Single unified config `late_materialization_enabled`:
- Covers both patterns: N-way join (N≥2) and N-way join + Sort (N≥1)
- Both patterns save col→row→col conversions
- Initialization in `initialize()` should set one flag based on plan structure

---

### Issue #6: HashProbe Extraction Logic

**Problem**: Current code always tries to extract or pass rowIds without distinguishing operator role.

**Fix**: Clear two-path logic based on downstream:
```cpp
if (isMaterializationPoint) {
  // SINK PATH: Extract directly like baseline
  extractColumns(...);  // Standard extraction, no rowIds
} else if (feedsSort) {
  // SORT PATH: Pass rowIds to Sort, Sort does extraction
  passRowIdsToSort(...);
} else {
  // INTERMEDIATE PATH: Pass rowIds to downstream HashBuild
  driverCtx->buildSideLateMBuildRowPtrs.push_back(...);
  driverCtx->buildSideLateMProbeRowIds.push_back(...);
}
```

---

### Issue #7: Consider Moving extractNWayColumns to HybridContainer

**Problem**: `extractNWayColumns()` is a free function in HashProbe.cpp that accesses HybridContainer internals. For uniformity, both base-table extraction and N-way extraction should have consistent interfaces.

**Fix** (optional refactoring): Move extraction logic into HybridContainer:
```cpp
class HybridContainer {
  void extractColumns(rows, projections, outputVectors);  // Base table
  void extractNWayColumns(rows, projections, outputVectors);  // N-way chain
};
```

---

### Issue #8: Duplicate probeRowIdCounter_ Updates

**Problem**: Lines 1954-1962 have two separate conditions updating `probeRowIdCounter_`:
```cpp
if (lateMaterializationEnabled_ && !probeOutputChannels_.empty()) {
  probeRowIdCounter_ += input_->size();  // Single-join path
}
if (driverCtx->buildSideLateMEnabled) {
  probeRowIdCounter_ += input_->size();  // N-way path (double increment!)
}
```

**Fix**: After unification (Issue #1), single condition:
```cpp
if (lateMaterializationEnabled_) {
  probeRowIdCounter_ += input_->size();
}
```

---

### Issue #9: Always Generating RowIds

**Problem**: Current code always generates and passes rowIds regardless of operator role. For **sink/materialization point operators**, this is unnecessary overhead.

**Fix**: Two-path approach:
| Operator Role | Generate RowIds? | Action |
|--------------|-----------------|--------|
| Intermediate Probe | Yes | Pass `(char*, uint64_t)` to HashBuild |
| Final Probe (Sink) | No | Extract directly like baseline |
| Final Probe (Sort) | Yes | Pass `(char*, uint64_t)` to Sort |

---

### Issue #10: Legacy Code in getOutput()

**Problem**: Lines 1830-1865 in `getOutput()` transfer data via `matchRowContainer_`:
```cpp
if (lateMaterializationEnabled_) {
  if (matchRowContainer_ != nullptr) {
    driverCtx->lateMaterializationTable = table_;
    driverCtx->lateMaterializationMatchContainer = std::move(matchRowContainer_);
    ...
  }
}
```
This is the old single-join design that should be removed after unification.

**Fix**: Remove entire block. After unification:
- Intermediate probes: already passed rowIds in `fillOutput()`
- Final probes (sink): already extracted in `fillOutput()`
- Final probes (sort): rowIds passed via different mechanism

Also remove the `output_ == nullptr` check at lines 1989-1993:
```cpp
if (lateMaterializationEnabled_ && output_ == nullptr) {
  continue;
}
```
After proper separation, `fillOutput()` should always produce output for sink case.

---

### Issue #1a: buildSideLateMUpstreamHashTables is Redundant

**Problem**: Lines 2522-2524 pass `buildSideLateMUpstreamHashTables` to DriverCtx:
```cpp
driverCtx.buildSideLateMUpstreamHashTables[driverId] = table_;
```
Comment says we need HashTable to keep `char*` pointers valid. But HybridContainer already owns:
- `keys_` (RowContainer) - where `char*` pointers reference
- `owningInputs_` (payload columns)

**Fix**: Remove `buildSideLateMUpstreamHashTables`. Only `buildSideLateMUpstreamBuildContainers` (HybridContainers) is needed since HybridContainer owns the RowContainer that `char*` pointers reference.

---

### Issue #11: On-Demand Key Extraction in Downstream Operators

**Problem**: Current design extracts key columns in `HashProbe::fillOutput()` and passes them as a `RowVectorPtr` to downstream operators. This is inconsistent with true late materialization philosophy - we're still extracting in HashProbe.

**Current approach (in fillOutput)**:
```
HashProbe → [extracted key columns as RowVectorPtr] → HashBuild/Sort
           + rowId pairs in DriverCtx
```

**Better approach**: Each downstream operator extracts only what IT needs in `addInput()`:
```
HashProbe → [empty/minimal input] + rowId pairs in DriverCtx → HashBuild/Sort
                                                               ↓
                                                    extract only needed columns
```

**Fix**: Move key extraction to downstream operators:

| Operator | What it needs | When to extract |
|----------|--------------|-----------------|
| HashProbe (intermediate) | Nothing | Never - just pass rowIds |
| HashBuild | Join key columns only | In `addInput()` from rowId pairs |
| Sort | Sort key columns only | In `addInput()` from rowId pairs |
| Final output (sink) | All remaining columns | After Sort completes or at final probe |

**Implementation in HashBuild::addInput()**:
```cpp
if (driverCtx->isLateMInput) {
  // Extract only join keys from upstream via rowId pairs
  auto keys = extractKeysFromUpstream(
      driverCtx->buildSideLateMBuildRowPtrs,
      joinKeyProjections_,
      pool());
  // Continue with normal hash table building using extracted keys
  ...
} else {
  // Normal path - input already contains columns
}
```

**Benefits**:
1. True late materialization - defer ALL extraction
2. Each operator extracts only what IT needs (no wasted work)
3. HashProbe becomes simpler - just pass rowId pairs
4. More modular - extraction logic lives where it's needed
5. Avoids extracting columns that won't be used

**Considerations**:
- Downstream operators need access to upstream HybridContainers (already available via DriverCtx)
- Interface is less standard (input is "virtual" - rowId pairs, not actual data)
- Requires changes to HashBuild::addInput() and Sort::addInput()

---

### Implementation Priority

1. **Phase 1**: Fix materialization point detection (Issues #3, #4)
   - Add `materializationPlanNodeId` to DriverCtx
   - Set during planning phase
   - Check at runtime in HashProbe

2. **Phase 2**: Unify single-join and N-way (Issues #1, #5, #8)
   - Remove duplicate containers and flags
   - Single code path for all late-m cases

3. **Phase 3**: Clean up legacy code (Issues #2, #10)
   - Remove `matchRowContainer_` entirely
   - Remove legacy transfer logic in `getOutput()`

4. **Phase 4**: Two-path extraction (Issues #6, #9)
   - Sink path: extract directly
   - Intermediate path: pass rowIds

5. **Phase 5**: Remove redundancy (Issue #1a)
   - Remove `buildSideLateMUpstreamHashTables`

6. **Phase 6**: On-demand extraction in downstream (Issue #11)
   - Move key extraction from HashProbe to HashBuild::addInput() and Sort::addInput()
   - HashProbe only passes rowId pairs for intermediate probes

7. **Optional**: Refactor extraction (Issue #7)
   - Move to HybridContainer for cleaner API

---

### Expected Outcome

After implementing these fixes:
1. Q27 performance should return to ~23s baseline (or better)
2. Code is cleaner with no duplication
3. Clear separation between intermediate and final probe behavior
4. Single unified late-m mechanism for all patterns

---

## v3.1 N-Way Extraction: Metadata-Driven Tree Traversal Design

**Date**: 2026-02-06

### Problem Statement

The current `extractNWayColumns()` implementation has fundamental flaws:

1. **Hardcoded column name matching**: Uses string prefixes (`c_`, `o_`) to identify column sources - fragile and non-generalizable
2. **Inline schema inference**: Creates result vectors inside the function instead of filling pre-allocated vectors
3. **Only 2-way support**: Assumes `upstreamBuildContainer(0)` is the leaf table, breaks for N > 2 joins
4. **Code duplication**: Re-implements extraction logic instead of reusing existing `HybridContainer::extractColumn()` APIs

### Proposed Solution: Tree-Based Column Source Metadata

Instead of runtime schema inference, each container stores **metadata mapping output columns to their source locations**. Extraction becomes a simple tree traversal followed by calls to existing extraction APIs.

---

### 1. Data Source Tree Structure

For N-way joins, data sources form a tree rooted at the final HybridContainer:

```
Q27 Example: customer ⋈ orders ⋈ lineitem (3-way join)

                    ┌──────────────────────────────────────┐
                    │  HybridContainer₁ (orders⋈lineitem)  │ ← Final probe target
                    │  keys_: [o_custkey] (materialized)   │
                    └──────────────────────────────────────┘
                               /                \
                              /                  \
     ┌────────────────────────┐          ┌─────────────────────────┐
     │  HybridContainer₀      │          │  ProbePayloadContainer₀ │
     │  (customer)            │          │  (orders payload)       │
     │  keys_: [c_custkey]    │          │  [o_orderdate, ...]     │
     │  payload: [c_name,...] │          └─────────────────────────┘
     └────────────────────────┘

                    + Current probe input (lineitem) - not stored in tree
```

**Key insight**: The tree structure mirrors the join order. Each node stores:
- **Keys** (always materialized for next join's probe)
- **Payload** (deferred until final extraction)
- **Pointers to parent nodes** (via `upstreamBuildRowPtrs_`, `upstreamProbeRowIds_`)

---

### 2. Column Source Categories

Each output column can come from exactly ONE of these source types:

| Source Type | Container | Location | Example (Q27) |
|-------------|-----------|----------|---------------|
| **Build Key (non-leaf)** | HybridContainer at depth > 0 | `keys_` RowContainer | `o_custkey` in HybridContainer₁ |
| **Build Key (leaf)** | HybridContainer at depth = max | `keys_` RowContainer | `c_custkey` in HybridContainer₀ |
| **Build Payload (leaf)** | HybridContainer at depth = max | `owningInputs_` columnar | `c_name`, `c_address` in HybridContainer₀ |
| **Probe Payload** | ProbePayloadContainer at any level | `batches_` columnar | `o_orderdate` in ProbePayloadContainer₀ |
| **Current Probe** | Current probe input (not in tree) | `input_` columnar | `l_quantity`, `l_price` in lineitem |

**Note**: Intermediate build nodes (depth > 0) only have keys, not payload. Keys are materialized at each join level for the next HashBuild.

---

### 3. Column Source Metadata Structure

```cpp
/// Describes where a column's data can be extracted from.
struct ColumnSource {
  enum Type {
    BUILD_KEY,        // From keys_ RowContainer (any depth)
    BUILD_PAYLOAD,    // From owningInputs_ (leaf only)
    PROBE_PAYLOAD,    // From ProbePayloadContainer (any depth)
    CURRENT_PROBE     // From current probe input (not in tree)
  };
  
  Type type;
  int32_t depth;          // 0 = this container, 1 = parent, 2 = grandparent, ...
  int32_t columnIndex;    // Column index within that source
  std::string columnName; // Original column name for debugging
};

/// In HybridContainer: maps output column name → source location
class HybridContainer {
  // ... existing members ...
  
  /// Column source metadata for N-way extraction.
  /// Key: output column name (e.g., "c_custkey", "o_orderdate")
  /// Value: where to find this column's data
  std::unordered_map<std::string, ColumnSource> columnSourceMap_;
  
  /// Maximum depth of the tree (0 = base table, 1 = first join result, ...)
  int32_t maxUpstreamDepth_ = 0;
};
```

---

### 4. When to Populate Metadata

**Option A: Planning Phase** (Preferred)
- Plan optimizer knows the full join graph
- Can compute column sources statically
- Pass metadata via plan node properties or DriverCtx

**Option B: Build Phase** (Fallback)
- When `HashBuild::addInput()` receives upstream refs, it knows:
  - Which HybridContainer the `char*` pointers reference
  - Which ProbePayloadContainer the `uint64_t` rowIds reference
- Can build `columnSourceMap_` by merging upstream metadata with current level

**Example metadata for Q27's HybridContainer₁**:
```cpp
columnSourceMap_ = {
  // Leaf build columns (customer) - depth 1
  {"c_custkey",   {BUILD_KEY,     1, 0, "c_custkey"}},   // HybridContainer₀.keys_[0]
  {"c_name",      {BUILD_PAYLOAD, 1, 0, "c_name"}},      // HybridContainer₀.owningInputs_[0]
  {"c_address",   {BUILD_PAYLOAD, 1, 1, "c_address"}},   // HybridContainer₀.owningInputs_[1]
  {"c_nationkey", {BUILD_PAYLOAD, 1, 2, "c_nationkey"}}, // HybridContainer₀.owningInputs_[2]
  
  // Non-leaf build columns (orders key) - depth 0
  {"o_custkey",   {BUILD_KEY,     0, 0, "o_custkey"}},   // HybridContainer₁.keys_[0] (current)
  
  // Probe columns (orders payload) - depth 0
  {"o_orderkey",  {PROBE_PAYLOAD, 0, 0, "o_orderkey"}},  // ProbePayloadContainer₀[0]
  {"o_orderdate", {PROBE_PAYLOAD, 0, 1, "o_orderdate"}}, // ProbePayloadContainer₀[1]
  {"o_totalprice",{PROBE_PAYLOAD, 0, 2, "o_totalprice"}},// ProbePayloadContainer₀[2]
  
  // Current probe columns (lineitem) - not in map, handled separately
};
```

---

### 5. Extraction Algorithm: Two-Phase Approach

#### Phase 1: Traverse Metadata → Resolve Source Pointers

For each output row, follow the tree to collect source `char*` pointers and `uint64_t` rowIds at each depth:

```cpp
/// Resolve source pointers for a batch of output rows.
/// Returns arrays of source pointers/rowIds indexed by depth.
struct ResolvedSources {
  // Build side: char* pointers at each depth (0 = current, 1 = parent, ...)
  std::vector<std::vector<char*>> buildRowPtrs;  // [depth][rowIdx]
  
  // Probe side: uint64_t rowIds at each depth
  std::vector<std::vector<uint64_t>> probeRowIds; // [depth][rowIdx]
};

ResolvedSources resolveSourcePointers(
    const HybridContainer* container,
    folly::Range<char**> matchedRows,  // Output of HashProbe
    int32_t maxDepth) {
  
  ResolvedSources result;
  result.buildRowPtrs.resize(maxDepth + 1);
  result.probeRowIds.resize(maxDepth + 1);
  
  const size_t numRows = matchedRows.size();
  
  // Depth 0: matchedRows are already the pointers for current container
  result.buildRowPtrs[0].assign(matchedRows.begin(), matchedRows.end());
  
  // Traverse upward for each depth
  const HybridContainer* current = container;
  for (int32_t depth = 0; depth < maxDepth; ++depth) {
    auto* keys = current->getKeys();
    const auto hybridRowIdOffset = keys->columnAt(keys->keyTypes().size()).offset();
    
    // For each row, decode hybridRowId → localIdx → upstream pointers
    result.buildRowPtrs[depth + 1].resize(numRows);
    result.probeRowIds[depth].resize(numRows);
    
    for (size_t i = 0; i < numRows; ++i) {
      char* row = result.buildRowPtrs[depth][i];
      uint64_t hybridRowId = RowContainer::valueAt<uint64_t>(row, hybridRowIdOffset);
      uint64_t localIdx = hybridRowId & ((1ULL << 56) - 1);
      
      // Get upstream pointers for next iteration
      result.buildRowPtrs[depth + 1][i] = current->getUpstreamBuildRowPtr(localIdx);
      result.probeRowIds[depth][i] = current->getUpstreamProbeRowId(localIdx);
    }
    
    // Move to parent container for next iteration
    current = current->getUpstreamBuildContainer(0);
  }
  
  return result;
}
```

#### Phase 2: Extract Each Column from Its Source

```cpp
void extractNWayColumns(
    HybridContainer* container,
    folly::Range<char**> matchedRows,
    const RowVectorPtr& probeInput,           // Current probe input
    const BufferPtr& outputRowMapping,        // Maps output → probe input row
    folly::Range<const IdentityProjection*> tableProjections,  // Build-side cols
    folly::Range<const IdentityProjection*> probeProjections,  // Current probe cols
    memory::MemoryPool* pool,
    const RowTypePtr& outputType,
    std::vector<VectorPtr>& resultVectors) {  // Pre-allocated output vectors
  
  const auto numRows = matchedRows.size();
  
  // Phase 1: Resolve all source pointers
  auto sources = resolveSourcePointers(container, matchedRows, container->maxUpstreamDepth());
  
  // Phase 2: Extract each column based on metadata
  for (auto projection : tableProjections) {
    const auto& colName = outputType->nameOf(projection.outputChannel);
    const auto& source = container->getColumnSource(colName);
    auto& resultVector = resultVectors[projection.outputChannel];
    
    switch (source.type) {
      case ColumnSource::BUILD_KEY: {
        // Extract from keys_ RowContainer at specified depth
        auto* srcContainer = getContainerAtDepth(container, source.depth);
        auto* srcKeys = srcContainer->getKeys();
        srcKeys->extractColumn(
            sources.buildRowPtrs[source.depth].data(),
            numRows,
            source.columnIndex,
            resultVector);
        break;
      }
      
      case ColumnSource::BUILD_PAYLOAD: {
        // Extract from owningInputs_ at leaf container
        auto* srcContainer = getContainerAtDepth(container, source.depth);
        srcContainer->extractPayloadColumn(
            sources.buildRowPtrs[source.depth].data(),
            numRows,
            source.columnIndex,
            resultVector,
            pool);
        break;
      }
      
      case ColumnSource::PROBE_PAYLOAD: {
        // Extract from ProbePayloadContainer
        extractProbePayloadColumn(
            container,
            sources.probeRowIds[source.depth],
            source.columnIndex,
            resultVector,
            pool);
        break;
      }
    }
  }
  
  // Current probe columns: use dictionary wrapping (existing fast path)
  for (auto projection : probeProjections) {
    resultVectors[projection.outputChannel] = BaseVector::wrapInDictionary(
        nullptr, outputRowMapping, numRows, probeInput->childAt(projection.inputChannel));
  }
}
```

---

### 6. Concrete Example: 4-Way Join

Consider a 4-way join: `A ⋈ B ⋈ C ⋈ D` (D is probe input)

```
                    ┌──────────────────────────────────────┐
                    │  HybridContainer₂ (A⋈B⋈C result)     │ ← depth 0
                    │  keys_: [c_key] (C's join key)       │
                    └──────────────────────────────────────┘
                               /                \
                              /                  \
     ┌────────────────────────┐          ┌─────────────────────────┐
     │  HybridContainer₁      │          │  ProbePayloadContainer₁ │
     │  (A⋈B result)          │ ← depth 1│  (C payload)            │ ← depth 0
     │  keys_: [b_key]        │          └─────────────────────────┘
     └────────────────────────┘
               /                \
              /                  \
┌────────────────────┐   ┌─────────────────────────┐
│  HybridContainer₀  │   │  ProbePayloadContainer₀ │
│  (A - base table)  │   │  (B payload)            │ ← depth 1
│  keys_: [a_key]    │   └─────────────────────────┘
│  payload: [a_*]    │ ← depth 2 (leaf)
└────────────────────┘
```

**Column source metadata for HybridContainer₂**:
```cpp
{
  // A columns (leaf build) - depth 2
  {"a_key",     {BUILD_KEY,     2, 0}},
  {"a_payload", {BUILD_PAYLOAD, 2, 0}},
  
  // B columns (intermediate build key + probe payload)
  {"b_key",     {BUILD_KEY,     1, 0}},  // keys_ of HybridContainer₁
  {"b_payload", {PROBE_PAYLOAD, 1, 0}},  // ProbePayloadContainer₀
  
  // C columns (current build key + probe payload)
  {"c_key",     {BUILD_KEY,     0, 0}},  // keys_ of HybridContainer₂
  {"c_payload", {PROBE_PAYLOAD, 0, 0}},  // ProbePayloadContainer₁
}
```

**Traversal for extraction**:
1. Start at depth 0 (HybridContainer₂)
2. For each row, decode hybridRowId₂ → localIdx₂
3. Get `buildRowPtrs[1][i] = upstreamBuildRowPtrs₂[localIdx₂]` (points to HybridContainer₁)
4. Get `probeRowIds[0][i] = upstreamProbeRowIds₂[localIdx₂]` (C payload)
5. Recurse: decode hybridRowId₁ from `buildRowPtrs[1][i]` → localIdx₁
6. Get `buildRowPtrs[2][i] = upstreamBuildRowPtrs₁[localIdx₁]` (points to HybridContainer₀)
7. Get `probeRowIds[1][i] = upstreamProbeRowIds₁[localIdx₁]` (B payload)
8. Now we have all source pointers, extract each column from appropriate source

---

### 7. Benefits of This Design

1. **Generalizes to N-way**: Works for any number of joins, no hardcoding
2. **Reuses existing APIs**: `extractColumn()` methods already optimized
3. **Clean separation**: Metadata resolution (Phase 1) separate from extraction (Phase 2)
4. **No schema inference at runtime**: Column sources known from metadata
5. **Simple extraction loop**: Just look up source, call appropriate extract method
6. **Enables caching**: Source pointers resolved once, reused for all columns at that depth

---

### 8. Important Design Notes

#### 8.1 Key Column Deallocation After Extraction

When HashProbe extracts key columns for the next HashBuild, those columns are **materialized into the next level's HybridContainer**. The source data for those key columns can potentially be deallocated at that point since:
- The extracted key columns now live in the new HybridContainer's `keys_`
- The parent join owns its own copy of the key data
- Source containers only need to keep payload columns alive for final extraction

**However**, be careful: payload columns from the same source must remain alive until final extraction. Only key columns can be "moved" to the next level.

#### 8.2 No Schema Inference in Extraction

The extraction phase should have **zero schema inference work**. Phase 1 is purely about:
- Mapping each output column index → `(source container, column index in source)`
- This mapping is pre-computed and stored in metadata
- At extraction time, we just look up the mapping and call the appropriate extract API

There's no string matching, no column name parsing, no type inference. Just:
```cpp
for (outputColIdx : outputColumns) {
  auto [sourceContainer, sourceColIdx] = columnSourceMap_[outputColIdx];
  sourceContainer->extractColumn(rows, sourceColIdx, resultVectors[outputColIdx]);
}
```

---

### 9. Resolved Design Decisions

#### 9.1 Where to Store `columnSourceMap_`

**Decision**: Store in each HybridContainer level, with HybridContainer linking to its ProbePayloadContainer at the same level.

```cpp
class HybridContainer {
  // ... existing members ...
  
  /// Column source metadata for this level
  std::unordered_map<std::string, ColumnSource> columnSourceMap_;
  
  /// Link to ProbePayloadContainer at same level (same join's probe side)
  std::shared_ptr<ProbePayloadContainer> probePayload_;
  
  /// Parent HybridContainer (upstream build)
  HybridContainer* upstreamBuildContainer_;  // or shared_ptr
};
```

**Tree traversal** is done starting from the root HybridContainer:
```
HybridContainer₁ (root)
├── probePayload_: ProbePayloadContainer₁ (orders payload)
└── upstreamBuildContainer_: HybridContainer₀
    ├── probePayload_: nullptr (base table has no probe side)
    └── upstreamBuildContainer_: nullptr (leaf)
```

This way, all traversal and extraction can be done through the root HybridContainer without needing separate container lookups.

#### 9.2 Multi-Driver Handling

**Not an issue** - this is already handled by the existing design:

1. **Thread-safety**: Guaranteed by encoding `driverId` in probe rowIds. Each driver writes to its own ProbePayloadContainer.

2. **Single traversal path**: After merge via JoinBridge, each HybridContainer contains pointers to **all drivers' data**:
   - `allContainers_[driverId]` → per-driver HybridContainers
   - `upstreamProbePayloads_[driverId]` → per-driver ProbePayloadContainers

3. **Extraction**: We traverse ONE HybridContainer's metadata. When extracting:
   - Build side: `char*` pointers directly address the right row
   - Probe side: decode `driverId` from rowId, look up correct container, extract

```cpp
// Probe extraction already handles multi-driver
uint64_t probeRowId = sources.probeRowIds[depth][i];
uint8_t driverId = probeRowId >> 56;
uint64_t localIdx = probeRowId & ((1ULL << 56) - 1);
auto* probeContainer = hybridContainer->getUpstreamProbePayload(driverId);
probeContainer->extractColumn(localIdx, colIdx, result);
```

#### 9.3 Batched Extraction by Source (Optimization)

**Decision**: Group columns by source for better cache locality and fewer TLB misses.

Instead of extracting column-by-column:
```cpp
// SLOW: random access pattern
for (col : allColumns) {
  extractFromSource(col.source, col.colIdx, result[col.outputIdx]);
}
```

Group by source first:
```cpp
// FAST: batch extraction per source
std::map<SourceKey, std::vector<ColumnProjection>> columnsBySource;
for (col : allColumns) {
  columnsBySource[col.source].push_back(col);
}

for (auto& [source, cols] : columnsBySource) {
  // Extract all columns from this source in one pass
  source.container->extractColumns(rows, cols, resultVectors);
}
```

This reduces:
- TLB misses (better spatial locality within each source)
- Function call overhead (one extractColumns call per source)
- Cache pollution (finish with one source before moving to next)

---

### 10. Implementation Plan: v3.1 Todo List

This section consolidates all design decisions from v3.0 and v3.1 into an actionable implementation plan with dependencies.

#### Phase 1: Data Structure Foundation

| ID | Task | Dependencies | Files |
|----|------|--------------|-------|
| **1.1** | Add `ColumnSource` struct with `{Type, depth, columnIndex}` | None | `RowContainer.h` |
| **1.2** | Add `columnSourceMap_` to HybridContainer | 1.1 | `RowContainer.h` |
| **1.3** | Add `probePayload_` link in HybridContainer (same-level ProbePayloadContainer) | None | `RowContainer.h` |
| **1.4** | Add `upstreamBuildContainer_` link in HybridContainer (parent) | None | `RowContainer.h` |
| **1.5** | Add `maxUpstreamDepth_` field to HybridContainer | None | `RowContainer.h` |
| **1.6** | Add accessor methods: `getColumnSource()`, `getContainerAtDepth()` | 1.2, 1.4 | `RowContainer.h/cpp` |

#### Phase 2: Metadata Population

| ID | Task | Dependencies | Files |
|----|------|--------------|-------|
| **2.1** | Add `materializationPlanNodeId` to DriverCtx | None | `Driver.h` |
| **2.2** | Set `materializationPlanNodeId` during planning (identify final probe before sink/Sort) | 2.1 | `LocalPlanner.cpp` or equivalent |
| **2.3** | Populate `columnSourceMap_` in HashBuild when receiving upstream refs | 1.1, 1.2 | `HashBuild.cpp` |
| **2.4** | Merge upstream `columnSourceMap_` with current level's columns | 2.3 | `HashBuild.cpp` |
| **2.5** | Set `probePayload_` and `upstreamBuildContainer_` links in HashBuild | 1.3, 1.4 | `HashBuild.cpp` |

#### Phase 3: Tree Traversal Infrastructure

| ID | Task | Dependencies | Files |
|----|------|--------------|-------|
| **3.1** | Implement `resolveSourcePointers()` function | 1.4, 1.5 | `HybridContainer` or `HashProbe.cpp` |
| **3.2** | Add `ResolvedSources` struct (buildRowPtrs[depth], probeRowIds[depth]) | None | `RowContainer.h` |
| **3.3** | Implement tree traversal: decode hybridRowId → get upstream pointers | 3.1 | `HybridContainer` |

#### Phase 4: New extractNWayColumns Implementation

| ID | Task | Dependencies | Files |
|----|------|--------------|-------|
| **4.1** | Rewrite `extractNWayColumns()` using two-phase approach | 3.1, 3.2, 3.3 | `HashProbe.cpp` or `HybridContainer` |
| **4.2** | Phase 1: Call `resolveSourcePointers()` once per batch | 4.1 | Same |
| **4.3** | Phase 2: Iterate columns, look up `columnSourceMap_`, call extract API | 4.1, 1.6 | Same |
| **4.4** | Group columns by source before extraction (optimization 9.3) | 4.3 | Same |
| **4.5** | Reuse existing `extractColumn()` APIs (no reimplementation) | 4.3 | Same |

#### Phase 5: Cleanup and Unification (from v3.0)

| ID | Task | Dependencies | Files |
|----|------|--------------|-------|
| **5.1** | Remove hardcoded column name matching (`c_`, `o_` prefixes) | 4.1 | `HashProbe.cpp` |
| **5.2** | Remove inline schema inference/vector creation | 4.1 | `HashProbe.cpp` |
| **5.3** | Unify `lateMaterializationEnabled_` and `nWayLateMCandidate_` into single flag | None | `HashProbe.h/cpp` |
| **5.4** | Remove `nWayProbePayloadContainer_`, use unified `probePayloadContainer_` | 5.3 | `HashProbe.h/cpp` |
| **5.5** | Remove `matchRowContainer_` and related legacy code | 5.3 | `HashProbe.cpp` |
| **5.6** | Remove duplicate `probeRowIdCounter_` updates | 5.3 | `HashProbe.cpp` |
| **5.7** | Remove `buildSideLateMUpstreamHashTables` (redundant) | None | `Driver.h`, `HashProbe.cpp` |

#### Phase 6: Extraction Logic Separation

| ID | Task | Dependencies | Files |
|----|------|--------------|-------|
| **6.1** | Check `materializationPlanNodeId` instead of `hasUpstreamRefs()` | 2.1, 2.2 | `HashProbe.cpp` |
| **6.2** | Final probe (sink): extract directly, no rowId generation | 6.1 | `HashProbe.cpp` |
| **6.3** | Intermediate probe: pass rowId pairs to HashBuild, no extraction | 6.1 | `HashProbe.cpp` |
| **6.4** | Final probe (Sort): pass rowId pairs to Sort | 6.1 | `HashProbe.cpp`, `OrderBy.cpp` |

#### Phase 7: Optional Refactoring

| ID | Task | Dependencies | Files |
|----|------|--------------|-------|
| **7.1** | Move `extractNWayColumns()` into HybridContainer class | 4.1 | `RowContainer.h/cpp` |
| **7.2** | Move key extraction to HashBuild::addInput() (Issue #11) | 4.1 | `HashBuild.cpp` |
| **7.3** | Move sort key extraction to Sort::addInput() | 4.1 | `OrderBy.cpp` |

---

### 11. Dependency Graph

```
Phase 1 (Data Structures)
    1.1 ColumnSource struct
     │
     ▼
    1.2 columnSourceMap_  ─────┐
     │                         │
     ▼                         ▼
    1.6 accessors         Phase 2 (Metadata)
                               │
    1.3 probePayload_ link     │
    1.4 upstreamBuildContainer_│
    1.5 maxUpstreamDepth_      │
     │                         │
     └──────────┬──────────────┘
                ▼
          Phase 3 (Tree Traversal)
           3.1 resolveSourcePointers()
           3.2 ResolvedSources
           3.3 traverse & decode
                │
                ▼
          Phase 4 (New extractNWayColumns)
           4.1-4.5 two-phase extraction
                │
                ├─────────────────────┐
                ▼                     ▼
          Phase 5 (Cleanup)     Phase 6 (Logic Separation)
           5.1-5.7 remove        6.1-6.4 materialization
           legacy code           point routing
                │                     │
                └──────────┬──────────┘
                           ▼
                    Phase 7 (Optional)
                     7.1-7.3 refactoring
```

---

### 12. Implementation Order (Recommended)

**Sprint 1: Foundation (can be done first, no runtime changes)**
1. 1.1-1.6: Add data structures to HybridContainer
2. 3.2: Add ResolvedSources struct
3. 2.1: Add `materializationPlanNodeId` to DriverCtx

**Sprint 2: Metadata & Traversal**
1. 2.3-2.5: Populate metadata in HashBuild
2. 3.1, 3.3: Implement tree traversal

**Sprint 3: Core Extraction Rewrite**
1. 4.1-4.5: New extractNWayColumns implementation
2. Test with Q27 - should work for N=2

**Sprint 4: Cleanup**
1. 5.1-5.7: Remove legacy code
2. 6.1-6.4: Proper materialization point routing
3. Remove old extractNWayColumns

**Sprint 5: Optimization & Polish**
1. 4.4: Batched extraction by source
2. 7.1-7.3: Optional refactoring

---

### 13. Success Criteria

build command: ninja -C _build/Release bolt_tpch_benchmark
run Q27: 
late-m: _build/Release/bolt/benchmarks/tpch/bolt_tpch_benchmark --run_query_verbose=27   --num_drivers=1   --late_materialization_enabled=true --hybrid_join=true --hybrid_sort=true --data_path=/home/jiang.2091/velox_join/data/tpch_parquet/sf3_hive

baseline: _build/Release/bolt/benchmarks/tpch/bolt_tpch_benchmark --run_query_verbose=27   --num_drivers=1   --late_materialization_enabled=false --hybrid_join=false --hybrid_sort=false --data_path=/home/jiang.2091/velox_join/data/tpch_parquet/sf3_hive

| Metric | Target |
|--------|--------|
| Q27 runtime | ≤ 23s (baseline) |
| Code paths | Single unified late-m path |
| N-way support | Works for N ≥ 2 |
| Schema inference | Zero at extraction time |
| Column source lookup | O(1) via pre-computed map |




build command: 
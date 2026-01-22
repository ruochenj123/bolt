/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <memory>
#include <unordered_map>
#include <vector>
#include "bolt/exec/HashTable.h"
#include "bolt/exec/RowContainer.h"  // ProbePayloadContainer is defined here
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::exec {

/// PayloadRegistry: Tracks payload columns from all source operators in an N-way
/// join pipeline. Each source (table scan output, join build side) registers its
/// payloads with a unique sourceId. The final operator (e.g., OrderBy) uses this
/// registry to extract columns from all sources using accumulated rowIds.
///
/// This enables late materialization across multiple joins:
/// T1 JOIN T2 ON k1 → J1 JOIN T3 ON k2 → ... → Sort
///
/// Each match row accumulates rowIds pointing back to original sources.
/// Final extraction uses these rowIds to materialize output columns.
class PayloadRegistry {
 public:
  /// Source type indicator
  enum class SourceType {
    BUILD_SIDE,  // HashJoin build side (HybridContainer)
    PROBE_SIDE,  // HashJoin probe side (ProbePayloadContainer)
  };

  /// A registered source of payload columns
  struct PayloadSource {
    uint8_t sourceId;
    SourceType type;
    
    // For BUILD_SIDE: HybridContainer from HashTable
    std::shared_ptr<BaseHashTable> table;
    
    // For PROBE_SIDE: coalesced probe input
    std::unique_ptr<ProbePayloadContainer> probePayload;
    
    // Column types in this source's payload
    std::vector<TypePtr> columnTypes;
    
    PayloadSource() = default;
    PayloadSource(PayloadSource&&) = default;
    PayloadSource& operator=(PayloadSource&&) = default;
  };

  /// Projection descriptor: maps source column to output column
  struct Projection {
    uint8_t sourceId;             // Which payload source
    column_index_t sourceColumn;  // Column index within source
    column_index_t outputColumn;  // Column index in final output
  };

  PayloadRegistry() = default;
  ~PayloadRegistry() = default;
  
  // Non-copyable, movable
  PayloadRegistry(const PayloadRegistry&) = delete;
  PayloadRegistry& operator=(const PayloadRegistry&) = delete;
  PayloadRegistry(PayloadRegistry&&) = default;
  PayloadRegistry& operator=(PayloadRegistry&&) = default;

  /// Register a build-side payload source (HybridContainer from HashTable).
  /// Returns assigned sourceId.
  uint8_t registerBuildSource(
      std::shared_ptr<BaseHashTable> table,
      const std::vector<TypePtr>& payloadTypes);

  /// Register a probe-side payload source.
  /// Returns assigned sourceId.
  uint8_t registerProbeSource(
      std::unique_ptr<ProbePayloadContainer> probePayload,
      const std::vector<TypePtr>& payloadTypes);

  /// Add a projection from source column to output column
  void addProjection(
      uint8_t sourceId,
      column_index_t sourceColumn,
      column_index_t outputColumn);

  /// Get all projections
  const std::vector<Projection>& projections() const { return projections_; }

  /// Get a payload source by id
  PayloadSource* getSource(uint8_t sourceId);
  const PayloadSource* getSource(uint8_t sourceId) const;

  /// Get number of registered sources
  size_t numSources() const { return sources_.size(); }

  /// Check if a source is registered
  bool hasSource(uint8_t sourceId) const {
    return sources_.find(sourceId) != sources_.end();
  }

  /// Get the next available sourceId
  uint8_t nextSourceId() const { return nextSourceId_; }

  /// Extract a column from a specific source for given rowIds.
  /// @param sourceId  Which source to extract from
  /// @param rowIds    Array of row indices into the source (size=numRows)
  /// @param numRows   Number of rows to extract
  /// @param sourceColumn  Column index within the source
  /// @param output    Output vector to fill
  void extractColumn(
      uint8_t sourceId,
      const uint64_t* rowIds,
      vector_size_t numRows,
      column_index_t sourceColumn,
      VectorPtr& output);

  /// Extract all projected columns for the given rows.
  /// @param rowIdVectors  For each sourceId, the vector of rowIds
  /// @param numRows       Number of output rows
  /// @param output        Output RowVector to populate
  void extractAll(
      const std::unordered_map<uint8_t, const uint64_t*>& rowIdVectors,
      vector_size_t numRows,
      RowVectorPtr output);

  /// Clear all registered sources and projections
  void clear();

 private:
  std::unordered_map<uint8_t, PayloadSource> sources_;
  std::vector<Projection> projections_;
  uint8_t nextSourceId_{0};
};

} // namespace bytedance::bolt::exec

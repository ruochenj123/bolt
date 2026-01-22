/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/exec/PayloadRegistry.h"
#include "bolt/exec/RowContainer.h"  // HybridContainer is defined here
#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::exec {

uint8_t PayloadRegistry::registerBuildSource(
    std::shared_ptr<BaseHashTable> table,
    const std::vector<TypePtr>& payloadTypes) {
  BOLT_CHECK_NOT_NULL(table);
  BOLT_CHECK_NOT_NULL(table->hybridData());
  BOLT_CHECK_LT(nextSourceId_, 255, "Too many payload sources");

  uint8_t sourceId = nextSourceId_++;
  PayloadSource source;
  source.sourceId = sourceId;
  source.type = SourceType::BUILD_SIDE;
  source.table = std::move(table);
  source.columnTypes = payloadTypes;
  sources_[sourceId] = std::move(source);

  LOG(INFO) << "PayloadRegistry: registered BUILD source " << (int)sourceId
            << " with " << payloadTypes.size() << " payload columns";
  return sourceId;
}

uint8_t PayloadRegistry::registerProbeSource(
    std::unique_ptr<ProbePayloadContainer> probePayload,
    const std::vector<TypePtr>& payloadTypes) {
  BOLT_CHECK_NOT_NULL(probePayload);
  BOLT_CHECK_LT(nextSourceId_, 255, "Too many payload sources");

  uint8_t sourceId = nextSourceId_++;
  PayloadSource source;
  source.sourceId = sourceId;
  source.type = SourceType::PROBE_SIDE;
  source.probePayload = std::move(probePayload);
  source.columnTypes = payloadTypes;
  sources_[sourceId] = std::move(source);

  LOG(INFO) << "PayloadRegistry: registered PROBE source " << (int)sourceId
            << " with " << payloadTypes.size() << " payload columns";
  return sourceId;
}

void PayloadRegistry::addProjection(
    uint8_t sourceId,
    column_index_t sourceColumn,
    column_index_t outputColumn) {
  BOLT_CHECK(hasSource(sourceId), "Source {} not registered", sourceId);
  
  Projection proj;
  proj.sourceId = sourceId;
  proj.sourceColumn = sourceColumn;
  proj.outputColumn = outputColumn;
  projections_.push_back(proj);
}

PayloadRegistry::PayloadSource* PayloadRegistry::getSource(uint8_t sourceId) {
  auto it = sources_.find(sourceId);
  return it != sources_.end() ? &it->second : nullptr;
}

const PayloadRegistry::PayloadSource* PayloadRegistry::getSource(uint8_t sourceId) const {
  auto it = sources_.find(sourceId);
  return it != sources_.end() ? &it->second : nullptr;
}

void PayloadRegistry::extractColumn(
    uint8_t sourceId,
    const uint64_t* rowIds,
    vector_size_t numRows,
    column_index_t sourceColumn,
    VectorPtr& output) {
  auto* source = getSource(sourceId);
  BOLT_CHECK_NOT_NULL(source, "Source {} not registered", sourceId);

  if (source->type == SourceType::BUILD_SIDE) {
    // Extract from HybridContainer
    auto* hybridData = source->table->hybridData();
    BOLT_CHECK_NOT_NULL(hybridData);
    
    // Decode rowIds to HybridRowId format
    std::vector<HybridRowId> hybridRowIds(numRows);
    for (vector_size_t i = 0; i < numRows; ++i) {
      // RowId encoding: reserved(8) | batchId(24) | rowInBatch(32)
      uint64_t rowId = rowIds[i];
      uint8_t driverId = rowId >> 56;  // reserved bits used for driverId
      uint64_t localRowId = rowId & ((1ULL << 56) - 1);
      hybridRowIds[i] = {driverId, localRowId};
    }
    
    // HybridContainer::extractColumn needs row pointers for key access,
    // but we only need payloads. Use nullptr for rows since we're extracting
    // payloads only.
    hybridData->extractColumnByRowId(
        hybridRowIds.data(),
        numRows,
        sourceColumn,
        output);
  } else {
    // Extract from ProbePayloadContainer
    BOLT_CHECK_NOT_NULL(source->probePayload);
    source->probePayload->extractColumn(
        rowIds,
        numRows,
        sourceColumn,
        output);
  }
}

void PayloadRegistry::extractAll(
    const std::unordered_map<uint8_t, const uint64_t*>& rowIdVectors,
    vector_size_t numRows,
    RowVectorPtr output) {
  for (const auto& proj : projections_) {
    auto it = rowIdVectors.find(proj.sourceId);
    BOLT_CHECK(
        it != rowIdVectors.end(),
        "RowIds for source {} not provided",
        proj.sourceId);
    
    extractColumn(
        proj.sourceId,
        it->second,
        numRows,
        proj.sourceColumn,
        output->childAt(proj.outputColumn));
  }
}

void PayloadRegistry::clear() {
  sources_.clear();
  projections_.clear();
  nextSourceId_ = 0;
}

} // namespace bytedance::bolt::exec

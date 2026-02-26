/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/exec/HashProbe.h"
#include <common/time/Timer.h>
#include <core/PlanNode.h>
#include <core/QueryConfig.h>
#include <exec/Driver.h>
#include <exec/Operator.h>
#include <algorithm>
#include <unordered_map>
#include "bolt/exec/OperatorUtils.h"
#include "bolt/exec/Task.h"
#include "bolt/expression/FieldReference.h"
#include "bolt/vector/BaseVector.h"
namespace bytedance::bolt::exec {

namespace {

// Batch size used when iterating the row container.
constexpr int kBatchSize = 1024;

// Returns the type for the hash table row. Build side keys first,
// then dependent build side columns.
RowTypePtr makeTableType(
    const RowType* type,
    const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>& keys,
    const std::shared_ptr<const core::HashJoinNode>& joinNode) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::unordered_set<column_index_t> keyChannels(keys.size());
  names.reserve(type->size());
  types.reserve(type->size());
  for (const auto& key : keys) {
    auto channel = type->getChildIdx(key->name());
    names.emplace_back(type->nameOf(channel));
    types.emplace_back(type->childAt(channel));
    keyChannels.insert(channel);
  }
  if (!canDropDuplicates(joinNode)) {
    // For left semi and anti join with no extra filter, hash table does not
    // store dependent columns.
    for (auto i = 0; i < type->size(); ++i) {
      if (keyChannels.find(i) == keyChannels.end()) {
        names.emplace_back(type->nameOf(i));
        types.emplace_back(type->childAt(i));
      }
    }
  }
  return ROW(std::move(names), std::move(types));
}

// Copy values from 'rows' of 'table' according to 'projections' in
// 'result'. Reuses 'result' children where possible.
// 'allowSorting': If false, disables sorting even if the hybridData supports it.
// This is needed when the caller also has probe-side columns that won't be
// reordered - we must keep build-side and probe-side in the same order.
void extractColumns(
    BaseHashTable* table,
    folly::Range<char**> rows,
    folly::Range<const IdentityProjection*> projections,
    memory::MemoryPool* pool,
    const std::vector<TypePtr>& resultTypes,
    std::vector<VectorPtr>& resultVectors,
    bool allowSorting = true) {
  BOLT_CHECK_EQ(resultTypes.size(), resultVectors.size())
  auto hybridData = table->hybridData();
  if (hybridData != nullptr) {
    std::vector<HybridRowId> outputRowIds;
    outputRowIds.resize(rows.size());
    hybridData->getRowIds(rows.data(), rows.size(), outputRowIds);

    // For single container, extract directly without sorting overhead.
    // For multiple containers, sort by containerId for better cache locality.
    // Note: sorting is safe here because the output order of hash join results
    // does not need to match any specific order (SQL doesn't guarantee order).
    // Sorting can be disabled via query config for deterministic testing,
    // or disabled by caller when probe-side columns must stay in sync.
    const bool useSorting = allowSorting && hybridData->shouldUseSorting();

    const char* const* extractRows = rows.data();
    std::vector<HybridRowId>* extractRowIds = &outputRowIds;
    HybridContainer::SortedRows sorted;

    if (useSorting) {
      sorted = hybridData->sortByContainerId(
          rows.data(), folly::Range<const vector_size_t*>{}, outputRowIds);
      extractRows = sorted.rows.data();
      extractRowIds = &sorted.rowIds;
    }

    for (auto projection : projections) {
      const auto resultChannel = projection.outputChannel;
      BOLT_CHECK_LT(resultChannel, resultVectors.size())
      auto& child = resultVectors[resultChannel];
      // TODO: Consider reuse of complex types.
      if (!child || !BaseVector::isVectorWritable(child) ||
          !child->isFlatEncoding()) {
        child =
            BaseVector::create(resultTypes[resultChannel], rows.size(), pool);
      }
      child->resize(rows.size());
      hybridData->extractColumn(
          extractRows,
          extractRowIds->size(),
          projection.inputChannel,
          child,
          *extractRowIds);
    }
  } else {
    for (auto projection : projections) {
      const auto resultChannel = projection.outputChannel;
      BOLT_CHECK_LT(resultChannel, resultVectors.size())

      auto& child = resultVectors[resultChannel];
      // TODO: Consider reuse of complex types.
      if (!child || !BaseVector::isVectorWritable(child) ||
          !child->isFlatEncoding()) {
        child =
            BaseVector::create(resultTypes[resultChannel], rows.size(), pool);
      }
      child->resize(rows.size());
      table->rows()->extractColumn(
          rows.data(), rows.size(), projection.inputChannel, child);
    }
  }
}

BlockingReason fromStateToBlockingReason(ProbeOperatorState state) {
  switch (state) {
    case ProbeOperatorState::kRunning:
      [[fallthrough]];
    case ProbeOperatorState::kFinish:
      return BlockingReason::kNotBlocked;
    case ProbeOperatorState::kWaitForBuild:
      return BlockingReason::kWaitForJoinBuild;
    case ProbeOperatorState::kWaitForPeers:
      return BlockingReason::kWaitForJoinProbe;
    default:
      BOLT_UNREACHABLE("Unexpected state: ", probeOperatorStateName(state));
  }
}

// Generate partition number set from spill partition id set.
SpillPartitionNumSet toPartitionNumSet(
    const SpillPartitionIdSet& partitionIdSet) {
  SpillPartitionNumSet partitionNumSet;
  partitionNumSet.reserve(partitionIdSet.size());
  for (const auto& partitionId : partitionIdSet) {
    partitionNumSet.insert(partitionId.partitionNumber());
  }
  return partitionNumSet;
}
} // namespace

HashProbe::HashProbe(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::HashJoinNode>& joinNode)
    : Operator(
          driverCtx,
          joinNode->outputType(),
          operatorId,
          joinNode->id(),
          "HashProbe",
          joinNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(operatorId)
              : std::nullopt),
      outputBatchSize_{outputBatchRows()},
      joinNode_(std::move(joinNode)),
      joinType_{joinNode_->joinType()},
      nullAware_{joinNode_->isNullAware()},
      probeType_(joinNode_->sources()[0]->outputType()),
      joinBridge_(operatorCtx_->task()->getHashJoinBridgeLocked(
          operatorCtx_->driverCtx()->splitGroupId,
          planNodeId())),
      filterResult_(1),
      outputTableRows_(outputBatchSize_),
      driverId_(static_cast<uint8_t>(driverCtx->driverId)) {
  BOLT_CHECK_NOT_NULL(joinBridge_);

  // N-way late materialization: isNWayLateMInput_ is reserved for future use
  // when probe-side input may come from upstream join results.
  // Currently, probe side always comes from TableScan or other sources.
  isNWayLateMInput_ = false;

  // Check if this probe has downstream build key channels (feeds into another HashBuild)
  bool hasDownstreamHashBuild = 
      driverCtx->downstreamBuildKeyChannels.find(planNodeId()) != 
      driverCtx->downstreamBuildKeyChannels.end();

  // Output is N-way if there's a downstream HashBuild waiting for rowIds
  // (i.e., we're not the final materialization point).
  // When Sort is the materialization point, only intermediate probes (that feed HashBuild)
  // use late-m output. The final probe before Sort uses standard probe with filtering.
  isNWayLateMOutput_ = driverCtx->buildSideLateMEnabled &&
      !driverCtx->materializationPlanNodeId.empty() &&
      driverCtx->materializationPlanNodeId != planNodeId() &&
      hasDownstreamHashBuild;  // Only intermediate probes that feed HashBuild

  // Check if this is the final probe before Sort (Sort is the materialization point)
  // In this case, we use OUTPUT mode but pass sort key columns instead of build key columns
  isFinalProbeBeforeSort_ = driverCtx->buildSideLateMEnabled &&
      driverCtx->sortMaterializationEnabled &&
      !driverCtx->materializationPlanNodeId.empty() &&
      driverCtx->materializationPlanNodeId != planNodeId();

  // Precompute whether this is the final materialization point (HashProbe does materialization)
  // This is false when Sort is the materialization point
  isFinalMaterializationProbe_ = driverCtx->buildSideLateMEnabled &&
      !driverCtx->materializationPlanNodeId.empty() &&
      driverCtx->materializationPlanNodeId == planNodeId() &&
      !driverCtx->sortMaterializationEnabled;
  
  LOG(INFO) << "HashProbe " << planNodeId() << " constructor:"
            << " buildSideLateMEnabled=" << driverCtx->buildSideLateMEnabled
            << ", materializationPlanNodeId=" << driverCtx->materializationPlanNodeId
            << ", isNWayLateMOutput_=" << isNWayLateMOutput_
            << ", isFinalProbeBeforeSort_=" << isFinalProbeBeforeSort_
            << ", isFinalMaterializationProbe_=" << isFinalMaterializationProbe_
            << ", hasDownstreamHashBuild=" << hasDownstreamHashBuild;
}

void HashProbe::initialize() {
  Operator::initialize();
  auto jitRowEqVectors =
      operatorCtx_->driverCtx()->queryConfig().enableJitRowEqVectors();
  BOLT_CHECK(hashers_.empty());
  hashers_ = createVectorHashers(probeType_, joinNode_->leftKeys());

  const auto numKeys = hashers_.size();
  keyChannels_.reserve(numKeys);
  for (auto& hasher : hashers_) {
    keyChannels_.push_back(hasher->channel());
  }

  BOLT_CHECK_NULL(lookup_);
  lookup_ = std::make_unique<HashLookup>(hashers_, jitRowEqVectors);
  auto buildType = joinNode_->sources()[1]->outputType();
  auto tableType =
      makeTableType(buildType.get(), joinNode_->rightKeys(), joinNode_);
  if (joinNode_->filter()) {
    initializeFilter(joinNode_->filter(), probeType_, tableType);
  }

  size_t numIdentityProjections = 0;
  for (auto i = 0; i < probeType_->size(); ++i) {
    auto& name = probeType_->nameOf(i);
    auto outIndex = outputType_->getChildIdxIfExists(name);
    if (!outIndex.has_value()) {
      continue;
    }
    projectedInputColumns_.emplace_back(i, *outIndex);
    if (!isRightJoin(joinType_) && !isFullJoin(joinType_)) {
      identityProjections_.emplace_back(i, *outIndex);
      if (*outIndex == i) {
        ++numIdentityProjections;
      }
    }
  }

  for (column_index_t i = 0; i < outputType_->size(); ++i) {
    auto tableChannel = tableType->getChildIdxIfExists(outputType_->nameOf(i));
    if (tableChannel.has_value()) {
      tableOutputProjections_.emplace_back(tableChannel.value(), i);
      if (!hitSampling_ &&
          (outputType_->childAt(i)->kind() == TypeKind::VARCHAR ||
           outputType_->childAt(i)->kind() == TypeKind::VARBINARY)) {
        hitSampling_ = true;
      }
    }
  }

  if (numIdentityProjections == probeType_->size() &&
      tableOutputProjections_.empty()) {
    isIdentityProjection_ = true;
  }

  if (nullAware_) {
    filterTableResult_.resize(1);
  }

  // N-way late materialization OR final probe before Sort: initialize late-m state
  // This block runs when:
  // - isNWayLateMOutput_: intermediate probe that feeds another HashBuild
  // - isFinalProbeBeforeSort_: final probe before Sort materialization point
  // AND there are columns to track (either probe-side or build-side)
  if ((isNWayLateMOutput_ || isFinalProbeBeforeSort_) && 
      (!projectedInputColumns_.empty() || !tableOutputProjections_.empty())) {
    
    auto* driverCtx = operatorCtx_->driverCtx();
    
    // Initialize ProbePayloadContainer only if there are probe columns in output
    if (!projectedInputColumns_.empty()) {
      probePayloadContainer_ = std::make_shared<ProbePayloadContainer>(pool());
      probePayloadContainer_->setId(driverId_);

      // Initialize allContainers_ with self for single-container mode (intra-pipeline fast path)
      std::unordered_map<uint8_t, ProbePayloadContainer*> selfContainer;
      selfContainer[driverId_] = probePayloadContainer_.get();
      probePayloadContainer_->setAllContainers(selfContainer);

      // Register container in DriverCtx for downstream HashBuild access
      // This enables cross-driver extraction after table merge
      driverCtx->buildSideLateMUpstreamProbePayloads[driverId_] = probePayloadContainer_;
    }

    // Precompute which output channels are keys for downstream HashBuild or Sort
    auto keyMapIt = driverCtx->downstreamBuildKeyChannels.find(planNodeId());
    if (keyMapIt != driverCtx->downstreamBuildKeyChannels.end()) {
      downstreamKeyOutputChannels_ = keyMapIt->second;
    }
    // Also check for sort keys if downstream is an OrderBy
    auto sortKeyMapIt = driverCtx->downstreamSortKeyChannels.find(planNodeId());
    if (sortKeyMapIt != driverCtx->downstreamSortKeyChannels.end()) {
      // Merge sort keys with join keys (sort keys also need to be materialized)
      for (auto channel : sortKeyMapIt->second) {
        downstreamKeyOutputChannels_.insert(channel);
      }
    }
    // If empty, all columns will be materialized (conservative fallback)

    // Precompute probe-side key vs payload (non-key) projections
    if (!projectedInputColumns_.empty()) {
      for (const auto& projection : projectedInputColumns_) {
        if (downstreamKeyOutputChannels_.empty() ||
            downstreamKeyOutputChannels_.count(projection.outputChannel) > 0) {
          probeKeyProjections_.push_back(projection);
        } else {
          // Non-key columns: only stored in ProbePayloadContainer
          probePayloadProjections_.push_back(projection);
        }
      }
    }

    // Precompute build-side key projections and channel mapping
    for (const auto& projection : tableOutputProjections_) {
      bool isKey = downstreamKeyOutputChannels_.empty() ||
          downstreamKeyOutputChannels_.count(projection.outputChannel) > 0;
      if (isKey) {
        buildKeyProjections_.push_back(projection);
        buildKeyChannelMapping_.emplace_back(
            projection.inputChannel, projection.outputChannel);
      }
    }

    // Compute payload schema for ProbePayloadContainer (non-key probe columns only)
    if (!projectedInputColumns_.empty()) {
      std::vector<std::string> payloadNames;
      std::vector<TypePtr> payloadTypes;
      payloadNames.reserve(probePayloadProjections_.size());
      payloadTypes.reserve(probePayloadProjections_.size());

      for (const auto& projection : probePayloadProjections_) {
        payloadNames.push_back(probeType_->nameOf(projection.inputChannel));
        payloadTypes.push_back(probeType_->childAt(projection.inputChannel));
      }
      probePayloadType_ = ROW(std::move(payloadNames), std::move(payloadTypes));
    }
    // Note: columnSourceMap is updated in updateColumnSourceMapForOutput() when table_ is available
  }
}

void HashProbe::initializeFilter(
    const core::TypedExprPtr& filter,
    const RowTypePtr& probeType,
    const RowTypePtr& tableType) {
  std::vector<core::TypedExprPtr> filters = {filter};
  filter_ =
      std::make_unique<ExprSet>(std::move(filters), operatorCtx_->execCtx());

  column_index_t filterChannel = 0;
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  auto numFields = filter_->expr(0)->distinctFields().size();
  names.reserve(numFields);
  types.reserve(numFields);
  for (auto& field : filter_->expr(0)->distinctFields()) {
    const auto& name = field->field();
    auto channel = probeType->getChildIdxIfExists(name);
    if (channel.has_value()) {
      auto channelValue = channel.value();
      filterInputProjections_.emplace_back(channelValue, filterChannel++);
      names.emplace_back(probeType->nameOf(channelValue));
      types.emplace_back(probeType->childAt(channelValue));
      continue;
    }
    channel = tableType->getChildIdxIfExists(name);
    if (channel.has_value()) {
      auto channelValue = channel.value();
      filterTableProjections_.emplace_back(channelValue, filterChannel);
      names.emplace_back(tableType->nameOf(channelValue));
      types.emplace_back(tableType->childAt(channelValue));
      ++filterChannel;
      continue;
    }
    BOLT_FAIL(
        "Join filter field {} not in probe or build input", field->toString());
  }

  filterInputType_ = ROW(std::move(names), std::move(types));
}

void HashProbe::setupSpillRestorForRangePartition(
    const std::optional<SpillPartitionId>& restoredPartitionId) {
  if (reuseSpillReader_) {
    spillInputReader_->reuse();
  } else {
    SpillPartitionId restoreId(
        restoredPartitionId->partitionBitOffset(),
        restoredPartitionId->partitionNumber());
    auto iter = spillPartitionSet_.find(restoreId);
    BOLT_CHECK(iter != spillPartitionSet_.end());
    auto partition = std::move(iter->second);
    BOLT_CHECK_EQ(partition->id(), restoreId);
    spillInputReader_ = partition->createUnorderedReader(pool());
    matchFlagType_ = ROW({"col_matchflag"}, {BOOLEAN()});
  }
  // if this is the last range partition, set reuseSpillReader_ to false
  // since next parition should be normal hash partition
  bool isLastOne = restoredPartitionId->isLastSubRangePartition();
  reuseSpillReader_ = !isLastOne;
  probeRangePartition_ = true;
  // for left/full, if it's not last range partition, do not output unmatched
  // left rows
  includingMiss_ = isLastOne;
  if (isLastOne) {
    SpillPartitionId restoreId(
        restoredPartitionId->partitionBitOffset(),
        restoredPartitionId->partitionNumber());
    auto iter = spillPartitionSet_.find(restoreId);
    BOLT_CHECK(iter != spillPartitionSet_.end());
    spillPartitionSet_.erase(iter);
  }
  if (needLastProbeSideOutput()) {
    if (isLastOne) {
      // no need to spill probe flags
      matchFlagSpiller_.reset();
    } else {
      matchFlagSpiller_ = std::make_unique<Spiller>(
          Spiller::Type::kHashJoinProbeMatchFlag,
          matchFlagType_,
          &(spillConfig_.value()));
      matchFlagSpiller_->setPartitionsSpilled(SpillPartitionNumSet{0});
    }
  }
}

void HashProbe::maybeSetupSpillInput(
    const std::optional<SpillPartitionId>& restoredPartitionId,
    const SpillPartitionIdSet& spillPartitionIds,
    SpillOffsetToBitsSet offsetToJoinBits) {
  if (!reuseSpillReader_) {
    BOLT_CHECK_NULL(spillInputReader_);
  }

  // If 'restoredPartitionId' is not null, then 'table_' is built from the
  // spilled build data. Create an unsorted reader to read the probe inputs from
  // the corresponding spilled probe partition on disk.
  if (restoredPartitionId.has_value()) {
    if (restoredPartitionId->isSubRangePartiton()) {
      setupSpillRestorForRangePartition(restoredPartitionId);
    } else {
      auto iter = spillPartitionSet_.find(restoredPartitionId.value());
      BOLT_CHECK(iter != spillPartitionSet_.end());
      BOLT_CHECK(reuseSpillReader_ == false);
      auto partition = std::move(iter->second);
      BOLT_CHECK_EQ(partition->id(), restoredPartitionId.value());
      spillInputReader_ = partition->createUnorderedReader(
          pool(), spillConfig_->spillUringEnabled);
      spillPartitionSet_.erase(iter);
      probeRangePartition_ = false;
    }
  }

  BOLT_CHECK_NULL(spiller_);
  spillInputPartitionIds_ = spillPartitionIds;
  if (spillInputPartitionIds_.empty()) {
    return;
  }

  // If 'spillInputPartitionIds_' is not empty, then we set up a spiller to
  // spill the incoming probe inputs.
  const auto& spillConfig = spillConfig_.value();
  uint8_t bitOffset = spillInputPartitionIds_.begin()->partitionBitOffset();
  auto search = offsetToJoinBits->find(bitOffset);
  BOLT_CHECK(search != offsetToJoinBits->end());
  LOG(INFO) << __FUNCTION__
            << ": setupSpiller startBit = " << (uint32_t)bitOffset
            << ", partitionBits = " << (uint32_t)search->second;
  auto* tmpConfig = const_cast<common::SpillConfig*>(&spillConfig);
  operatorCtx_->adjustSpillCompressionKind(tmpConfig);
  spiller_ = std::make_unique<Spiller>(
      Spiller::Type::kHashJoinProbe,
      probeType_,
      HashBitRange(bitOffset, bitOffset + search->second),
      &spillConfig,
      spillConfig.maxFileSize);
  spiller_->setSpillConfig(&spillConfig);

  // Set the spill partitions to the corresponding ones at the build side. The
  // hash probe operator itself won't trigger any spilling.
  spiller_->setPartitionsSpilled(toPartitionNumSet(spillInputPartitionIds_));

  spillHashFunction_ = std::make_unique<HashPartitionFunction>(
      spiller_->hashBits(), probeType_, keyChannels_);
  spillInputIndicesBuffers_.resize(spillHashFunction_->numPartitions());
  rawSpillInputIndicesBuffers_.resize(spillHashFunction_->numPartitions());
  numSpillInputs_.resize(spillHashFunction_->numPartitions(), 0);
}

void HashProbe::asyncWaitForHashTable() {
  checkRunning();
  BOLT_CHECK_NULL(table_);

  auto hashBuildResult = joinBridge_->tableOrFuture(&future_);
  if (!hashBuildResult.has_value()) {
    BOLT_CHECK(future_.valid());
    setState(ProbeOperatorState::kWaitForBuild);
    return;
  }

  if (hashBuildResult->hasNullKeys) {
    BOLT_CHECK(nullAware_);
    if (isAntiJoin(joinType_) && !joinNode_->filter()) {
      // Null-aware anti join with null keys on the build side without a filter
      // always returns nothing.
      // The flag must be set on the first (and only) built 'table_'.
      BOLT_CHECK(spillPartitionSet_.empty());
      noMoreInput();
      return;
    }
    buildSideHasNullKeys_ = true;
  }

  table_ = std::move(hashBuildResult->table);
  BOLT_CHECK_NOT_NULL(table_);

  // Compute late-m output paths once now that table_ is available.
  // Check if this table actually has late-m metadata (columnSourceMap).
  // This handles cases where a probe shares DriverCtx with late-m enabled,
  // but its table comes from a base TableScan (no late-m chain).
  const bool tableHasLateMMetadata = table_->hybridData() &&
      !table_->hybridData()->getColumnSourceMap().empty();

  useLateMOutputPath_ = isNWayLateMOutput_ && tableHasLateMMetadata;
  useFinalMaterializationPath_ = isFinalMaterializationProbe_ && tableHasLateMMetadata;

  // Check if pointer reuse mode should be enabled for intermediate late-m output.
  // This requires: config enabled, planner detected same keys for DOWNSTREAM build,
  // intermediate late-m mode, valid container, and a join type that doesn't need right-side iteration.
  // Note: We check downstreamPointerReuseEligibleProbeIds (this probe's ID) not
  // pointerReuseEligibleNodeIds (downstream build's ID).
  const bool joinTypeSupportsPointerReuse = !isRightJoin(joinType_) &&
      !isFullJoin(joinType_) && !isRightSemiFilterJoin(joinType_) &&
      !isRightSemiProjectJoin(joinType_);
  pointerReuseEnabled_ = useLateMOutputPath_ &&
      operatorCtx_->driverCtx()->queryConfig().hybridJoinPointerReuseEnabled() &&
      (operatorCtx_->driverCtx()->downstreamPointerReuseEligibleProbeIds.count(planNodeId()) > 0) &&  // THIS probe feeds reuse-eligible build
      table_->hybridData() && joinTypeSupportsPointerReuse;
  
  maybeSetupSpillInput(
      hashBuildResult->restoredPartitionId,
      hashBuildResult->spillPartitionIds,
      hashBuildResult->offsetToJoinBits);

  if (table_->numDistinct() == 0) {
    if (skipProbeOnEmptyBuild()) {
      if (!needSpillInput()) {
        if (isSpillInput() ||
            operatorCtx_->driverCtx()
                ->queryConfig()
                .hashProbeFinishEarlyOnEmptyBuild()) {
          noMoreInput();
        } else {
          skipInput_ = true;
        }
      }
    }
  } else if (
      (isInnerJoin(joinType_) || isLeftSemiFilterJoin(joinType_) ||
       isRightSemiFilterJoin(joinType_) || isRightSemiProjectJoin(joinType_) ||
       isRightJoin(joinType_)) &&
      table_->hashMode() != BaseHashTable::HashMode::kHash && !isSpillInput() &&
      !hasMoreSpillData()) {
    // Find out whether there are any upstream operators that can accept
    // dynamic filters on all or a subset of the join keys. Create dynamic
    // filters to push down.
    //
    // NOTE: this optimization is not applied in the following cases: (1) if the
    // probe input is read from spilled data and there is no upstream operators
    // involved; (2) if there is spill data to restore, then we can't filter
    // probe inputs solely based on the current table's join keys.
    const auto& buildHashers = table_->hashers();
    auto channels = operatorCtx_->driverCtx()->driver->canPushdownFilters(
        this, keyChannels_);

    // Null aware Right Semi Project join needs to know whether there are any
    // nulls on the probe side. Hence, cannot filter these out.
    const auto nullAllowed = isRightSemiProjectJoin(joinType_) && nullAware_;

    for (auto i = 0; i < keyChannels_.size(); i++) {
      if (channels.find(keyChannels_[i]) != channels.end()) {
        if (auto filter = buildHashers[i]->getFilter(nullAllowed)) {
          dynamicFilters_.emplace(keyChannels_[i], std::move(filter));
        }
      }
    }
  }
}

bool HashProbe::isSpillInput() const {
  return spillInputReader_ != nullptr;
}

void HashProbe::prepareForSpillRestore() {
  checkRunning();
  BOLT_CHECK(spillEnabled());
  BOLT_CHECK(hasMoreSpillData());

  // Reset the internal states which are relevant to the previous probe run.
  noMoreSpillInput_ = false;
  table_.reset();
  spiller_.reset();
  if (!reuseSpillReader_) {
    spillInputReader_.reset();
  }
  spillInputPartitionIds_.clear();
  lastProbeIterator_.reset();

  BOLT_CHECK(promises_.empty() || lastProber_);
  if (!lastProber_) {
    return;
  }
  lastProber_ = false;
  // Notify the hash build operators to build the next hash table.
  joinBridge_->probeFinished();

  // Wake up the peer hash probe operators to wait for table build.
  auto promises = std::move(promises_);
  for (auto& promise : promises) {
    promise.setValue();
  }
}

void HashProbe::addSpillInput() {
  checkRunning();

  if (input_ != nullptr || noMoreSpillInput_) {
    return;
  }
  if (FOLLY_UNLIKELY(!spillInputReader_->nextBatch(input_))) {
    recordSpillReadStats();
    if (probeRangePartition_ && needLastProbeSideOutput()) {
      spillMatchFlagReader_.reset();
      if (matchFlagSpiller_) {
        auto&& spilledFlags = matchFlagSpiller_->finishSpill();
        // for next range partition
        spillMatchFlagReader_ = spilledFlags.createUnorderedReader(pool());
        matchFlagSpiller_.reset();
      }
    }
    DeltaCpuWallTimer timer{[this](const CpuWallTiming& timing) {
      this->stats().wlock()->finishTiming.add(timing);
    }};
    noMoreInputInternal();
    return;
  }

  if (probeRangePartition_ && needLastProbeSideOutput()) {
    if (spillMatchFlagReader_) {
      spillMatchFlagReader_->nextBatch(accumulatedMatchFlag_);
      // probe flags's bits should equals input rows's size
      BOLT_CHECK(accumulatedMatchFlag_->size() == input_->size());
    } else {
      // probe first range partiion
      prepareMatchFlag(input_->size(), accumulatedMatchFlag_);
    }
  }
  DeltaCpuWallTimer timer{[this](const CpuWallTiming& timing) {
    auto selfDelta = operatorCtx_->driver()->processLazyTiming(*this, timing);
    this->stats().wlock()->addInputTiming.add(selfDelta);
  }};
  addInput(std::move(input_));
}

void HashProbe::spillInput(RowVectorPtr& input) {
  BOLT_CHECK(needSpillInput());

  const auto numInput = input->size();
  prepareInputIndicesBuffers(
      input->size(), spiller_->state().spilledPartitionSet());
  const auto singlePartition =
      spillHashFunction_->partition(*input, spillPartitions_);

  vector_size_t numNonSpillingInput = 0;
  for (auto row = 0; row < numInput; ++row) {
    const auto partition = singlePartition.has_value() ? singlePartition.value()
                                                       : spillPartitions_[row];
    if (!spiller_->isSpilled(partition)) {
      rawNonSpillInputIndicesBuffer_[numNonSpillingInput++] = row;
      continue;
    }
    rawSpillInputIndicesBuffers_[partition][numSpillInputs_[partition]++] = row;
  }
  if (numNonSpillingInput == numInput) {
    return;
  }

  // Ensure vector are lazy loaded before spilling.
  for (int32_t i = 0; i < input->childrenSize(); ++i) {
    input->childAt(i)->loadedVector();
  }

  for (int32_t partition = 0; partition < numSpillInputs_.size(); ++partition) {
    const auto numSpillInputs = numSpillInputs_[partition];
    if (numSpillInputs == 0) {
      continue;
    }
    BOLT_CHECK(spiller_->isSpilled(partition));
    spiller_->spill(
        partition,
        wrapAndCombineDict(
            numSpillInputs, spillInputIndicesBuffers_[partition], input));
  }

  if (numNonSpillingInput == 0) {
    input = nullptr;
  } else {
    input = wrapAndCombineDict(
        numNonSpillingInput, nonSpillInputIndicesBuffer_, input);
  }
}

void HashProbe::prepareInputIndicesBuffers(
    vector_size_t numInput,
    const folly::F14FastSet<uint32_t>& spillPartitions) {
  BOLT_DCHECK(spillEnabled());
  const auto maxIndicesBufferBytes = numInput * sizeof(vector_size_t);
  if (nonSpillInputIndicesBuffer_ == nullptr ||
      nonSpillInputIndicesBuffer_->size() < maxIndicesBufferBytes) {
    nonSpillInputIndicesBuffer_ = allocateIndices(numInput, pool());
    rawNonSpillInputIndicesBuffer_ =
        nonSpillInputIndicesBuffer_->asMutable<vector_size_t>();
  }
  for (const auto& partition : spillPartitions) {
    if (spillInputIndicesBuffers_[partition] == nullptr ||
        spillInputIndicesBuffers_[partition]->size() < maxIndicesBufferBytes) {
      spillInputIndicesBuffers_[partition] = allocateIndices(numInput, pool());
      rawSpillInputIndicesBuffers_[partition] =
          spillInputIndicesBuffers_[partition]->asMutable<vector_size_t>();
    }
  }
  std::fill(numSpillInputs_.begin(), numSpillInputs_.end(), 0);
}

BlockingReason HashProbe::isBlocked(ContinueFuture* future) {
  switch (state_) {
    case ProbeOperatorState::kWaitForBuild:
      BOLT_CHECK_NULL(table_);
      if (!future_.valid()) {
        setRunning();
        asyncWaitForHashTable();
      }
      break;
    case ProbeOperatorState::kRunning:
      BOLT_CHECK_NOT_NULL(table_);
      if (spillInputReader_ != nullptr) {
        addSpillInput();
      }
      break;
    case ProbeOperatorState::kWaitForPeers:
      BOLT_CHECK(hasMoreSpillData());
      if (!future_.valid()) {
        setRunning();
      }
      break;
    case ProbeOperatorState::kFinish:
      break;
    default:
      BOLT_UNREACHABLE(probeOperatorStateName(state_));
      break;
  }

  if (future_.valid()) {
    BOLT_CHECK(!isRunning());
    *future = std::move(future_);
  }
  return fromStateToBlockingReason(state_);
}

void HashProbe::clearDynamicFilters() {
  BOLT_CHECK(!hasMoreSpillData());
  BOLT_CHECK(!needSpillInput());

  // The join can be completely replaced with a pushed down
  // filter when the following conditions are met:
  //  * hash table has a single key with unique values,
  //  * build side has no dependent columns.
  if (keyChannels_.size() == 1 && !table_->hasDuplicateKeys() &&
      tableOutputProjections_.empty() && !filter_ && !dynamicFilters_.empty() &&
      !isRightJoin(joinType_)) {
    canReplaceWithDynamicFilter_ = true;
  }

  Operator::clearDynamicFilters();
}

void HashProbe::decodeAndDetectNonNullKeys() {
  nonNullInputRows_.resize(input_->size());
  nonNullInputRows_.setAll();

  for (auto i = 0; i < hashers_.size(); ++i) {
    auto key = input_->childAt(hashers_[i]->channel())->loadedVector();
    hashers_[i]->decode(*key, nonNullInputRows_);
  }

  deselectRowsWithNulls(hashers_, nonNullInputRows_);
  if (isRightSemiProjectJoin(joinType_) &&
      nonNullInputRows_.countSelected() < input_->size()) {
    probeSideHasNullKeys_ = true;
  }
}

void HashProbe::addInput(RowVectorPtr input) {
  if (skipInput_) {
    BOLT_CHECK_NULL(input_);
    return;
  }
  input_ = std::move(input);

  // Reset passingInputRowsInitialized_ as input_ as changed.
  passingInputRowsInitialized_ = false;

  const auto numInput = input_->size();

  if (numInput > 0) {
    noInput_ = false;
  }

  if (canReplaceWithDynamicFilter_) {
    replacedWithDynamicFilter_ = true;
    return;
  }

  bool hasDecoded = false;

  if (needSpillInput()) {
    if (isRightSemiProjectJoin(joinType_) && !probeSideHasNullKeys_) {
      decodeAndDetectNonNullKeys();
      hasDecoded = true;
    }

    spillInput(input_);
    // Check if all the probe input rows have been spilled.
    if (input_ == nullptr) {
      return;
    }
  }

  if (table_->numDistinct() == 0) {
    if (skipProbeOnEmptyBuild()) {
      BOLT_CHECK(needSpillInput());
      input_ = nullptr;
      return;
    }
    // Build side is empty. This state is valid only for anti, left and full
    // joins.
    BOLT_CHECK(
        isAntiJoin(joinType_) || isLeftJoin(joinType_) ||
        isFullJoin(joinType_) || isLeftSemiProjectJoin(joinType_));
    if (isLeftSemiProjectJoin(joinType_) ||
        (isAntiJoin(joinType_) && filter_)) {
      // For anti join with filter and semi project join we need to decode the
      // join keys columns to initialize 'nonNullInputRows_'. The anti join
      // filter evaluation and semi project join output generation will access
      // 'nonNullInputRows_' later.
      decodeAndDetectNonNullKeys();
    }
    return;
  }

  if (!hasDecoded) {
    decodeAndDetectNonNullKeys();
  }
  activeRows_ = nonNullInputRows_;

  // Update statistics for null keys in join operator.
  // Updating here means we will report 0 null keys when build side is empty.
  // If we want more accurate stats, we will have to decode input vector
  // even when not needed. So we tradeoff less accurate stats for more
  // performance.
  {
    auto lockedStats = stats_.wlock();
    lockedStats->numNullKeys +=
        activeRows_.size() - activeRows_.countSelected();
  }

  table_->prepareForJoinProbe(*lookup_.get(), input_, activeRows_, false);

  passingInputRowsInitialized_ = false;
  if (isLeftJoin(joinType_) || isFullJoin(joinType_) || isAntiJoin(joinType_) ||
      isLeftSemiProjectJoin(joinType_)) {
    // Make sure to allocate an entry in 'hits' for every input row to allow for
    // including rows without a match in the output. Also, make sure to
    // initialize all 'hits' to nullptr as HashTable::joinProbe will only
    // process activeRows_.
    auto& hits = lookup_->hits;
    hits.resize(numInput);
    std::fill(hits.data(), hits.data() + numInput, nullptr);
    if (!lookup_->rows.empty()) {
      table_->joinProbe(*lookup_);
    }

    // Update lookup_->rows to include all input rows, not just
    // activeRows_ as we need to include all rows in the output.
    auto& rows = lookup_->rows;
    rows.resize(numInput);
    std::iota(rows.begin(), rows.end(), 0);

    // update and spill probe match flags if needed for left/full join
    if (probeRangePartition_ && needLastProbeSideOutput() && !includingMiss_) {
      // not last range partition and do not have filters, update and spill
      if (!joinNode_->filter()) {
        updateAndSpillProbeMatchFlags(accumulatedMatchFlag_, numInput, true);
      } else {
        // with filter, update tmpMatchFlagForFilter_, do not spill
        prepareMatchFlag(numInput, tmpMatchFlagForFilter_);
        updateAndSpillProbeMatchFlags(tmpMatchFlagForFilter_, numInput, false);
      }
    }
  } else {
    if (lookup_->rows.empty()) {
      input_ = nullptr;
      return;
    }
    lookup_->hits.resize(lookup_->rows.back() + 1);
    table_->joinProbe(*lookup_);
  }
  results_.reset(*lookup_);
}

void HashProbe::prepareOutput(vector_size_t size) {
  // Try to re-use memory for the output vectors that contain build-side data.
  // We expect output vectors containing probe-side data to be null (reset in
  // clearProjectedOutput(). BaseVector::prepareForReuse keeps null
  // children unmodified and makes non-null (build side) children reusable.
  if (output_) {
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, size);
    output_ = std::static_pointer_cast<RowVector>(output);
  } else {
    output_ = BaseVector::create<RowVector>(outputType_, size, pool());
  }
}

void HashProbe::prepareMatchFlag(vector_size_t size, RowVectorPtr& matchFlag) {
  if (matchFlag) {
    VectorPtr flags = std::move(matchFlag);
    BaseVector::prepareForReuse(flags, size);
    matchFlag = std::static_pointer_cast<RowVector>(flags);
  } else {
    matchFlag = BaseVector::create<RowVector>(matchFlagType_, size, pool());
  }
  // set match flag to false
  auto flags = matchFlag->childAt(0)->as<FlatVector<bool>>();
  memset(
      (char*)(flags->asRange().data()), 0x00, BaseVector::byteSize<bool>(size));
}

namespace {
VectorPtr createConstantFalse(vector_size_t size, memory::MemoryPool* pool) {
  return std::make_shared<ConstantVector<bool>>(
      pool, size, false /*isNull*/, BOOLEAN(), false /*value*/);
}
} // namespace

void HashProbe::fillLeftSemiProjectMatchColumn(vector_size_t size) {
  if (emptyBuildSide()) {
    // Build side is empty or all rows have null join keys.
    if (nullAware_ && buildSideHasNullKeys_) {
      matchColumn() = BaseVector::createNullConstant(BOOLEAN(), size, pool());
    } else {
      matchColumn() = createConstantFalse(size, pool());
    }
  } else {
    auto flatMatch = matchColumn()->as<FlatVector<bool>>();
    flatMatch->resize(size);
    auto rawValues = flatMatch->mutableRawValues<uint64_t>();
    for (auto i = 0; i < size; ++i) {
      if (nullAware_) {
        // Null-aware join may produce TRUE, FALSE or NULL.
        if (filter_) {
          if (leftSemiProjectIsNull_.isValid(i)) {
            flatMatch->setNull(i, true);
          } else {
            bool hasMatch = outputTableRows_[i] != nullptr;
            bits::setBit(rawValues, i, hasMatch);
          }
        } else {
          if (!nonNullInputRows_.isValid(i)) {
            // Probe key is null.
            flatMatch->setNull(i, true);
          } else {
            // Probe key is not null.
            bool hasMatch = outputTableRows_[i] != nullptr;
            if (!hasMatch && buildSideHasNullKeys_) {
              flatMatch->setNull(i, true);
            } else {
              bits::setBit(rawValues, i, hasMatch);
            }
          }
        }
      } else {
        bool hasMatch = outputTableRows_[i] != nullptr;
        bits::setBit(rawValues, i, hasMatch);
      }
    }
  }
}

void HashProbe::fillOutput(vector_size_t size) {
  // N-way late materialization paths - flags precomputed in asyncWaitForHashTable()
  if (useLateMOutputPath_) {
    // Intermediate probe: pass rowIds downstream instead of materializing
    fillOutputLateMaterialization(size);
    return;
  }

  if (isFinalProbeBeforeSort_) {
    // Final probe before Sort: use late-m style output (pass rowIds to Sort)
    // but normal probe filtering has already selected matching rows
    fillOutputLateMaterialization(size);
    return;
  }

  if (useFinalMaterializationPath_) {
    // Final probe: do full materialization from all sources
    // Copy columnSourceMap from hash table to DriverCtx for fillOutputFinalMaterialization
    auto* driverCtx = operatorCtx_->driverCtx();
    driverCtx->columnSourceMap = table_->hybridData()->getColumnSourceMap();
    fillOutputFinalMaterialization(size);
    return;
  }

  // Standard path (no late-m or base table)
  prepareOutput(size);
  for (auto projection : projectedInputColumns_) {
    ensureLoadedIfNotAtEnd(projection.inputChannel);
  }

  wrapIndirectChildren(
      projectedInputColumns_,
      input_->children(),
      size,
      outputRowMapping_,
      output_->children());

  if (isLeftSemiProjectJoin(joinType_)) {
    fillLeftSemiProjectMatchColumn(size);
  } else {
    bool wrapInDictionary = false;
    std::map<int64_t, int16_t> addrToIndex;
    // if size too small, no need to sample
    if (hitSampling_ && size > 1024) {
      wrapInDictionary = canWrapInDictionary(size, addrToIndex);
    }
    if (wrapInDictionary) {
      auto numDistinct = addrToIndex.size();
      std::vector<char*> distinctRows(numDistinct);
      for (const auto& [addr, idx] : addrToIndex) {
        distinctRows[idx] = (char*)addr;
      }
      // get dictionary raw value
      RowVectorPtr dictOutput = std::static_pointer_cast<RowVector>(
          BaseVector::create(outputType_, numDistinct, pool()));
      // Disable sorting when there are probe columns to keep build and probe in sync.
      const bool hasProbeColumns = !projectedInputColumns_.empty();
      extractColumns(
          table_.get(),
          folly::Range<char**>(distinctRows.data(), numDistinct),
          tableOutputProjections_,
          pool(),
          outputType_->children(),
          dictOutput->children(),
          /*allowSorting=*/!hasProbeColumns);

      // calculate dictionary index
      BufferPtr indexBuffer;
      auto mapping = initializeRowNumberMapping(indexBuffer, size, pool());
      for (auto i = 0; i < size; ++i) {
        mapping[i] = addrToIndex.find((int64_t)(outputTableRows_[i]))->second;
      }
      for (auto projection : tableOutputProjections_) {
        output_->childAt(projection.outputChannel) = wrapChild(
            size, indexBuffer, dictOutput->childAt(projection.outputChannel));
      }
    } else {
      // Disable sorting when there are probe columns to keep build and probe in sync.
      const bool hasProbeColumns = !projectedInputColumns_.empty();
      extractColumns(
          table_.get(),
          folly::Range<char**>(outputTableRows_.data(), size),
          tableOutputProjections_,
          pool(),
          outputType_->children(),
          output_->children(),
          /*allowSorting=*/!hasProbeColumns);
    }
  }
}

bool HashProbe::canWrapInDictionary(
    vector_size_t size,
    std::map<int64_t, int16_t>& uniqueMap) const {
  int16_t distinctCount = 0;
  vector_size_t nullCount = 0;
  auto sampleSize = size / 10;

  auto sampleMethod = [&](vector_size_t begin, vector_size_t end) {
    for (auto i = begin; i < end; ++i) {
      int64_t addr = (int64_t)(outputTableRows_[i]);
      const auto [it, status] = uniqueMap.insert({addr, distinctCount});
      distinctCount += status;
      nullCount += (addr == 0);
    }
  };
  // sample first 10%
  sampleMethod(0, sampleSize);
  if (distinctCount > sampleSize / 2 || nullCount > sampleSize / 2) {
    return false;
  }
  sampleMethod(sampleSize, size);
  return (distinctCount < size * 0.6 && nullCount < size / 2);
}

RowVectorPtr HashProbe::getBuildSideOutput() {
  outputTableRows_.resize(outputBatchSize_);
  int32_t numOut;
  if (isRightSemiFilterJoin(joinType_)) {
    numOut = table_->listProbedRows(
        &lastProbeIterator_,
        outputBatchSize_,
        RowContainer::kUnlimited,
        outputTableRows_.data());
  } else if (isRightSemiProjectJoin(joinType_)) {
    numOut = table_->listAllRows(
        &lastProbeIterator_,
        outputBatchSize_,
        RowContainer::kUnlimited,
        outputTableRows_.data());
  } else {
    // Must be a right join or full join.
    numOut = table_->listNotProbedRows(
        &lastProbeIterator_,
        outputBatchSize_,
        RowContainer::kUnlimited,
        outputTableRows_.data());
  }
  if (!numOut) {
    return nullptr;
  }

  prepareOutput(numOut);

  // Populate probe-side columns of the output with nulls.
  for (auto projection : projectedInputColumns_) {
    output_->childAt(projection.outputChannel) = BaseVector::createNullConstant(
        outputType_->childAt(projection.outputChannel), numOut, pool());
  }

  extractColumns(
      table_.get(),
      folly::Range<char**>(outputTableRows_.data(), numOut),
      tableOutputProjections_,
      pool(),
      outputType_->children(),
      output_->children());

  if (isRightSemiProjectJoin(joinType_)) {
    // Populate 'match' column.
    if (noInput_) {
      // Probe side is empty. All rows should return 'match = false', even
      // ones with a null join key.
      matchColumn() = createConstantFalse(numOut, pool());
    } else {
      table_->rows()->extractProbedFlags(
          outputTableRows_.data(),
          numOut,
          nullAware_,
          nullAware_ && probeSideHasNullKeys_,
          matchColumn());
    }
  }

  return output_;
}

void HashProbe::clearProjectedOutput() {
  if (!output_ || output_.use_count() != 1) {
    return;
  }
  for (auto& projection : projectedInputColumns_) {
    output_->childAt(projection.outputChannel) = nullptr;
  }
}

bool HashProbe::needLastProbe() const {
  return !skipInput_ &&
      (isRightJoin(joinType_) || isFullJoin(joinType_) ||
       isRightSemiFilterJoin(joinType_) || isRightSemiProjectJoin(joinType_));
}

bool HashProbe::needLastProbeSideOutput() const {
  return isLeftJoin(joinType_) || isFullJoin(joinType_);
}

bool HashProbe::skipProbeOnEmptyBuild() const {
  return isInnerJoin(joinType_) || isLeftSemiFilterJoin(joinType_) ||
      isRightJoin(joinType_) || isRightSemiFilterJoin(joinType_) ||
      isRightSemiProjectJoin(joinType_);
}

bool HashProbe::spillEnabled() const {
  return spillConfig_.has_value();
}

bool HashProbe::hasMoreSpillData() const {
  BOLT_CHECK(spillPartitionSet_.empty() || spillEnabled());
  return !spillPartitionSet_.empty() || needSpillInput();
}

bool HashProbe::needSpillInput() const {
  BOLT_CHECK(spillInputPartitionIds_.empty() || spillEnabled());
  BOLT_CHECK_EQ(spillInputPartitionIds_.empty(), spiller_ == nullptr);

  return !spillInputPartitionIds_.empty();
}

void HashProbe::setState(ProbeOperatorState state) {
  checkStateTransition(state);
  state_ = state;
}

void HashProbe::checkStateTransition(ProbeOperatorState state) {
  BOLT_CHECK_NE(state_, state);
  switch (state) {
    case ProbeOperatorState::kRunning:
      if (!hasMoreSpillData()) {
        BOLT_CHECK_EQ(state_, ProbeOperatorState::kWaitForBuild);
      } else {
        BOLT_CHECK(
            state_ == ProbeOperatorState::kWaitForBuild ||
            state_ == ProbeOperatorState::kWaitForPeers)
      }
      break;
    case ProbeOperatorState::kWaitForPeers:
      BOLT_CHECK(hasMoreSpillData());
      [[fallthrough]];
    case ProbeOperatorState::kWaitForBuild:
      [[fallthrough]];
    case ProbeOperatorState::kFinish:
      BOLT_CHECK_EQ(state_, ProbeOperatorState::kRunning);
      break;
    default:
      BOLT_UNREACHABLE(probeOperatorStateName(state_));
      break;
  }
}

RowVectorPtr HashProbe::getOutput() {
  if (isFinished()) {
    return nullptr;
  }
  checkRunning();

  clearProjectedOutput();
  if (!input_) {
    if (!hasMoreInput()) {
      if (needLastProbe() && lastProber_) {
        auto output = getBuildSideOutput();
        if (output != nullptr) {
          return output;
        }
      }
      if (hasMoreSpillData()) {
        prepareForSpillRestore();
        asyncWaitForHashTable();
      } else {
        setState(ProbeOperatorState::kFinish);
        resetHashTable();
      }
      return nullptr;
    }
    return nullptr;
  }

  const auto inputSize = input_->size();

  if (replacedWithDynamicFilter_) {
    addRuntimeStat("replacedWithDynamicFilterRows", RuntimeCounter(inputSize));
    auto output = Operator::fillOutput(inputSize, nullptr);
    input_ = nullptr;
    return output;
  }

  const bool isLeftSemiOrAntiJoinNoFilter = !filter_ &&
      (isLeftSemiFilterJoin(joinType_) || isLeftSemiProjectJoin(joinType_) ||
       isAntiJoin(joinType_));

  const bool emptyBuildSide = (table_->numDistinct() == 0);

  // Left semi and anti joins are always cardinality reducing, e.g. for a
  // given row of input they produce zero or 1 row of output. Therefore, if
  // there is no extra filter we can process each batch of input in one go.
  auto outputBatchSize = (isLeftSemiOrAntiJoinNoFilter || emptyBuildSide)
      ? inputSize
      : outputBatchSize_;
  auto mapping =
      initializeRowNumberMapping(outputRowMapping_, outputBatchSize, pool());
  outputTableRows_.resize(outputBatchSize);

  for (;;) {
    int numOut = 0;

    if (emptyBuildSide) {
      // When build side is empty, anti and left joins return all probe side
      // rows, including ones with null join keys.
      std::iota(mapping.begin(), mapping.end(), 0);
      std::fill(outputTableRows_.begin(), outputTableRows_.end(), nullptr);
      numOut = inputSize;
    } else if (isAntiJoin(joinType_) && !filter_) {
      if (nullAware_) {
        // When build side is not empty, anti join without a filter returns
        // probe rows with no nulls in the join key and no match in the build
        // side.
        for (auto i = 0; i < inputSize; ++i) {
          if (nonNullInputRows_.isValid(i) &&
              (!activeRows_.isValid(i) || !lookup_->hits[i])) {
            mapping[numOut] = i;
            ++numOut;
          }
        }
      } else {
        for (auto i = 0; i < inputSize; ++i) {
          if (!nonNullInputRows_.isValid(i) ||
              (!activeRows_.isValid(i) || !lookup_->hits[i])) {
            mapping[numOut] = i;
            ++numOut;
          }
        }
      }
    } else {
      if (probeRangePartition_ && needLastProbeSideOutput() && includingMiss_) {
        numOut = table_->listJoinResults(
            results_,
            joinIncludesMissesFromLeft(joinType_),
            mapping,
            folly::Range(outputTableRows_.data(), outputTableRows_.size()),
            accumulatedMatchFlag_->childAt(0).get());
      } else {
        numOut = table_->listJoinResults(
            results_,
            joinIncludesMissesFromLeft(joinType_),
            mapping,
            folly::Range(outputTableRows_.data(), outputTableRows_.size()));
      }
    }

    // We are done processing the input batch if there are no more joined rows
    // to process and the NoMatchDetector isn't carrying forward a row that
    // still needs to be written to the output.
    if (!numOut && !noMatchDetector_.hasLastMissedRow()) {
      if (probeRangePartition_ && needLastProbeSideOutput() &&
          !includingMiss_ && joinNode_->filter()) {
        mergeAndSpillProbeMatchFlags();
      }
      input_ = nullptr;
      return nullptr;
    }
    BOLT_CHECK_LE(numOut, outputTableRows_.size());

    numOut = evalFilter(numOut);

    if (!numOut) {
      continue;
    }

    if (needLastProbe()) {
      // Mark build-side rows that have a match on the join condition.
      table_->rows()->setProbedFlag(outputTableRows_.data(), numOut);
    }

    // Right semi join only returns the build side output when the probe side
    // is fully complete. Do not return anything here.
    if (isRightSemiFilterJoin(joinType_) || isRightSemiProjectJoin(joinType_)) {
      if (results_.atEnd()) {
        input_ = nullptr;
      }
      return nullptr;
    }

    fillOutput(numOut);

    if (isLeftSemiOrAntiJoinNoFilter || emptyBuildSide) {
      input_ = nullptr;
    }
    return output_;
  }
}

void HashProbe::fillFilterInput(vector_size_t size) {
  std::vector<VectorPtr> filterColumns(filterInputType_->size());
  for (auto projection : filterInputProjections_) {
    if (std::any_of(
            projectedInputColumns_.begin(),
            projectedInputColumns_.end(),
            [&](const auto& p) {
              return p.inputChannel == projection.inputChannel;
            })) {
      // If the column is projected to the output, ensure it's loaded if it's
      // lazy in case the filter only loads an incomplete subset of the rows
      // that will be output.
      ensureLoaded(projection.inputChannel);
    } else {
      // If the column isn't projected to the output, the Vector will only be
      // reused if we've broken the input batch into multiple output batches,
      // i.e. if results_ is not at the end of the iterator.
      ensureLoadedIfNotAtEnd(projection.inputChannel);
    }
  }

  wrapIndirectChildren(
      filterInputProjections_,
      input_->children(),
      size,
      outputRowMapping_,
      filterColumns);

  extractColumns(
      table_.get(),
      folly::Range<char**>(outputTableRows_.data(), size),
      filterTableProjections_,
      pool(),
      filterInputType_->children(),
      filterColumns);

  filterInput_ = std::make_shared<RowVector>(
      pool(), filterInputType_, nullptr, size, std::move(filterColumns));
}

void HashProbe::prepareFilterRowsForNullAwareJoin(
    vector_size_t numRows,
    bool filterPropagateNulls) {
  BOLT_CHECK_LE(numRows, kBatchSize);
  if (filterTableInput_ == nullptr) {
    filterTableInput_ =
        BaseVector::create<RowVector>(filterInputType_, kBatchSize, pool());
  }

  if (filterPropagateNulls) {
    nullFilterInputRows_.resizeFill(numRows, false);
    auto* rawNullRows = nullFilterInputRows_.asMutableRange().bits();
    for (auto& projection : filterInputProjections_) {
      filterInputColumnDecodedVector_.decode(
          *filterInput_->childAt(projection.outputChannel), filterInputRows_);
      if (filterInputColumnDecodedVector_.mayHaveNulls()) {
        SelectivityVector nullsInActiveRows(numRows);
        memcpy(
            nullsInActiveRows.asMutableRange().bits(),
            filterInputColumnDecodedVector_.nulls(&filterInputRows_),
            bits::nbytes(numRows));
        // All rows that are not active count as non-null here.
        bits::orWithNegatedBits(
            nullsInActiveRows.asMutableRange().bits(),
            filterInputRows_.asRange().bits(),
            0,
            numRows);
        // NOTE: the false value of a raw null bit indicates null so we OR with
        // negative of the raw bit.
        bits::orWithNegatedBits(
            rawNullRows, nullsInActiveRows.asRange().bits(), 0, numRows);
      }
    }
    nullFilterInputRows_.updateBounds();
    // TODO: consider to skip filtering on 'nullFilterInputRows_' as we know
    // it will never pass the filtering.
  }

  // NOTE: for null-aware anti join, we will skip filtering on the probe rows
  // with null join key columns(s) as we can apply filtering after they cross
  // join with the table rows later.
  if (!nonNullInputRows_.isAllSelected()) {
    auto* rawMapping = outputRowMapping_->asMutable<vector_size_t>();
    for (int i = 0; i < numRows; ++i) {
      if (filterInputRows_.isValid(i) &&
          !nonNullInputRows_.isValid(rawMapping[i])) {
        filterInputRows_.setValid(i, false);
      }
    }
    filterInputRows_.updateBounds();
  }
}

namespace {

const uint64_t* getFlatFilterResult(VectorPtr& result) {
  if (!result->isFlatEncoding()) {
    return nullptr;
  }
  auto* flat = result->asUnchecked<FlatVector<bool>>();
  if (!flat->mayHaveNulls()) {
    return flat->rawValues<uint64_t>();
  }
  if (!flat->rawValues<uint64_t>()) {
    return flat->rawNulls();
  }
  if (result.use_count() != 1) {
    return nullptr;
  }
  auto* values = flat->mutableRawValues<uint64_t>();
  bits::andBits(values, flat->rawNulls(), 0, flat->size());
  return values;
}

} // namespace

void HashProbe::applyFilterOnTableRowsForNullAwareJoin(
    const SelectivityVector& rows,
    SelectivityVector& filterPassedRows,
    std::function<int32_t(char**, int32_t)> iterator) {
  if (!rows.hasSelections()) {
    return;
  }
  auto* tableRows = table_->rows();
  auto* hybridData = table_->hybridData();
  std::vector<HybridRowId> outputRowIds;
  BOLT_CHECK(tableRows, "Should not move rows in hash joins");
  char* data[kBatchSize];
  while (auto numRows = iterator(data, kBatchSize)) {
    filterTableInput_->resize(numRows);
    filterTableInputRows_.resizeFill(numRows, true);
    if (hybridData != nullptr) {
      outputRowIds.resize(numRows);
      hybridData->getRowIds(data, numRows, outputRowIds);

      // For single container, extract directly without sorting.
      // For multiple containers, sort by containerId for better cache locality.
      // const bool useSorting = hybridData->shouldUseSorting();
      const bool useSorting = false;
      const char* const* extractRows = data;
      std::vector<HybridRowId>* extractRowIds = &outputRowIds;
      HybridContainer::SortedRows sorted;

      if (useSorting) {
        sorted = hybridData->sortByContainerId(
            data, folly::Range<const vector_size_t*>{}, outputRowIds);
        extractRows = sorted.rows.data();
        extractRowIds = &sorted.rowIds;
      }

      for (auto& projection : filterTableProjections_) {
        hybridData->extractColumn(
            extractRows,
            extractRowIds->size(),
            projection.inputChannel,
            filterTableInput_->childAt(projection.outputChannel),
            *extractRowIds);
      }
    } else {
      for (auto& projection : filterTableProjections_) {
        tableRows->extractColumn(
            data,
            numRows,
            projection.inputChannel,
            filterTableInput_->childAt(projection.outputChannel));
      }
    }
    rows.applyToSelected([&](vector_size_t row) {
      for (auto& projection : filterInputProjections_) {
        filterTableInput_->childAt(projection.outputChannel) =
            BaseVector::wrapInConstant(
                numRows, row, input_->childAt(projection.inputChannel));
      }
      EvalCtx evalCtx(
          operatorCtx_->execCtx(), filter_.get(), filterTableInput_.get());
      filter_->eval(filterTableInputRows_, evalCtx, filterTableResult_);
      if (auto* values = getFlatFilterResult(filterTableResult_[0])) {
        if (!bits::testSetBits(
                values, 0, numRows, [](vector_size_t) { return false; })) {
          filterPassedRows.setValid(row, true);
        }
      } else {
        decodedFilterTableResult_.decode(
            *filterTableResult_[0], filterTableInputRows_);
        if (decodedFilterTableResult_.isConstantMapping()) {
          if (!decodedFilterTableResult_.isNullAt(0) &&
              decodedFilterTableResult_.valueAt<bool>(0)) {
            filterPassedRows.setValid(row, true);
          }
        } else {
          for (vector_size_t i = 0; i < numRows; ++i) {
            if (!decodedFilterTableResult_.isNullAt(i) &&
                decodedFilterTableResult_.valueAt<bool>(i)) {
              filterPassedRows.setValid(row, true);
              break;
            }
          }
        }
      }
    });
  }
}

SelectivityVector HashProbe::evalFilterForNullAwareJoin(
    vector_size_t numRows,
    bool filterPropagateNulls) {
  auto* rawOutputProbeRowMapping =
      outputRowMapping_->asMutable<vector_size_t>();

  // Subset of probe-side rows with a match that passed the filter.
  SelectivityVector filterPassedRows(input_->size(), false);

  // Subset of probe-side rows with non-null probe key and either no match or
  // no match that passed the filter. We need to combine these with all
  // build-side rows with null keys to see if a filter passes on any of these.
  SelectivityVector nullKeyProbeRows(input_->size(), false);

  // Subset of probe-sie rows with null probe key. We need to combine these
  // with all build-side rows to see if a filter passes on any of these.
  SelectivityVector crossJoinProbeRows(input_->size(), false);

  for (auto i = 0; i < numRows; ++i) {
    // Skip filter input row if it has any null probe side filter column.
    if (filterPropagateNulls && nullFilterInputRows_.isValid(i)) {
      continue;
    }

    const auto probeRow = rawOutputProbeRowMapping[i];
    if (nonNullInputRows_.isValid(probeRow)) {
      if (filterPassed(i)) {
        filterPassedRows.setValid(probeRow, true);
      } else {
        nullKeyProbeRows.setValid(probeRow, true);
      }
    } else {
      crossJoinProbeRows.setValid(probeRow, true);
    }
  }

  if (buildSideHasNullKeys_) {
    BaseHashTable::NullKeyRowsIterator iter;
    nullKeyProbeRows.deselect(filterPassedRows);
    applyFilterOnTableRowsForNullAwareJoin(
        nullKeyProbeRows, filterPassedRows, [&](char** data, int32_t maxRows) {
          return table_->listNullKeyRows(&iter, maxRows, data);
        });
  }
  BaseHashTable::RowsIterator iter;
  crossJoinProbeRows.deselect(filterPassedRows);
  applyFilterOnTableRowsForNullAwareJoin(
      crossJoinProbeRows, filterPassedRows, [&](char** data, int32_t maxRows) {
        return table_->listAllRows(
            &iter, maxRows, RowContainer::kUnlimited, data);
      });
  filterPassedRows.updateBounds();

  return filterPassedRows;
}

int32_t HashProbe::evalFilter(int32_t numRows) {
  if (!filter_) {
    return numRows;
  }

  const bool filterPropagateNulls = filter_->expr(0)->propagatesNulls();
  auto* rawOutputProbeRowMapping =
      outputRowMapping_->asMutable<vector_size_t>();

  filterInputRows_.resizeFill(numRows);

  // Do not evaluate filter on rows with no match to (1) avoid
  // false-positives when filter evaluates to true for rows with NULLs on the
  // build side; (2) avoid errors in filter evaluation that would fail the
  // query unnecessarily.
  // TODO Apply the same to left joins.
  if (isAntiJoin(joinType_) || isLeftSemiProjectJoin(joinType_)) {
    for (auto i = 0; i < numRows; ++i) {
      if (outputTableRows_[i] == nullptr) {
        filterInputRows_.setValid(i, false);
      }
    }
    filterInputRows_.updateBounds();
  }

  fillFilterInput(numRows);

  if (nullAware_) {
    prepareFilterRowsForNullAwareJoin(numRows, filterPropagateNulls);
  }

  EvalCtx evalCtx(operatorCtx_->execCtx(), filter_.get(), filterInput_.get());
  filter_->eval(0, 1, true, filterInputRows_, evalCtx, filterResult_);

  decodedFilterResult_.decode(*filterResult_[0], filterInputRows_);

  int32_t numPassed = 0;
  if (isLeftJoin(joinType_) || isFullJoin(joinType_)) {
    if (probeRangePartition_) {
      if (includingMiss_) {
        auto addMiss = [&](auto row) {
          auto flags =
              accumulatedMatchFlag_->childAt(0)->as<FlatVector<bool>>();
          if (!flags->valueAtFast(row)) {
            outputTableRows_[numPassed] = nullptr;
            rawOutputProbeRowMapping[numPassed++] = row;
          }
        };
        for (auto i = 0; i < numRows; ++i) {
          const bool passed = filterPassed(i);
          noMatchDetector_.advance(
              rawOutputProbeRowMapping[i], passed, addMiss);
          if (passed) {
            outputTableRows_[numPassed] = outputTableRows_[i];
            rawOutputProbeRowMapping[numPassed++] = rawOutputProbeRowMapping[i];
          }
        }

        noMatchDetector_.finishIteration(
            addMiss, results_.atEnd(), outputTableRows_.size() - numPassed);
      } else {
        auto addMiss = [&](auto row) {
          auto flags =
              tmpMatchFlagForFilter_->childAt(0)->as<FlatVector<bool>>();
          BOLT_CHECK(flags->valueAtFast(row));
          flags->set(row, false);
        };
        for (auto i = 0; i < numRows; ++i) {
          const bool passed = filterPassed(i);
          noMatchDetector_.advance(
              rawOutputProbeRowMapping[i], passed, addMiss);
          if (passed) {
            outputTableRows_[numPassed] = outputTableRows_[i];
            rawOutputProbeRowMapping[numPassed++] = rawOutputProbeRowMapping[i];
          }
        }

        noMatchDetector_.finishIteration(
            addMiss, results_.atEnd(), outputTableRows_.size() - numPassed);
      }
    } else {
      // Identify probe rows which got filtered out and add them back with nulls
      // for build side.
      auto addMiss = [&](auto row) {
        outputTableRows_[numPassed] = nullptr;
        rawOutputProbeRowMapping[numPassed++] = row;
      };

      for (auto i = 0; i < numRows; ++i) {
        const bool passed = filterPassed(i);
        noMatchDetector_.advance(rawOutputProbeRowMapping[i], passed, addMiss);
        if (passed) {
          outputTableRows_[numPassed] = outputTableRows_[i];
          rawOutputProbeRowMapping[numPassed++] = rawOutputProbeRowMapping[i];
        }
      }

      noMatchDetector_.finishIteration(
          addMiss, results_.atEnd(), outputTableRows_.size() - numPassed);
    }
  } else if (isLeftSemiFilterJoin(joinType_)) {
    auto addLastMatch = [&](auto row) {
      outputTableRows_[numPassed] = nullptr;
      rawOutputProbeRowMapping[numPassed++] = row;
    };
    for (auto i = 0; i < numRows; ++i) {
      if (filterPassed(i)) {
        leftSemiFilterJoinTracker_.advance(
            rawOutputProbeRowMapping[i], addLastMatch);
      }
    }
    if (results_.atEnd()) {
      leftSemiFilterJoinTracker_.finish(addLastMatch);
    }
  } else if (isLeftSemiProjectJoin(joinType_)) {
    // NOTE: Set output table row to point to a fake string to indicate there
    // is a match for this probe 'row'. 'fillOutput' populates the match
    // column based on the nullable of this pointer.
    static const char* kPassed = "passed";

    if (nullAware_) {
      leftSemiProjectIsNull_.resize(numRows);
      leftSemiProjectIsNull_.clearAll();

      auto addLast = [&](auto row, std::optional<bool> passed) {
        if (passed.has_value()) {
          outputTableRows_[numPassed] =
              passed.value() ? const_cast<char*>(kPassed) : nullptr;
        } else {
          leftSemiProjectIsNull_.setValid(numPassed, true);
        }
        rawOutputProbeRowMapping[numPassed++] = row;
      };

      auto passedRows =
          evalFilterForNullAwareJoin(numRows, filterPropagateNulls);
      for (auto i = 0; i < numRows; ++i) {
        // filterPassed(i) -> TRUE
        // else passed -> NULL
        // else FALSE
        auto probeRow = rawOutputProbeRowMapping[i];
        std::optional<bool> passed = filterPassed(i)
            ? std::optional(true)
            : (passedRows.isValid(probeRow) ? std::nullopt
                                            : std::optional(false));
        leftSemiProjectJoinTracker_.advance(probeRow, passed, addLast);
      }
      leftSemiProjectIsNull_.updateBounds();
      if (results_.atEnd()) {
        leftSemiProjectJoinTracker_.finish(addLast);
      }
    } else {
      auto addLast = [&](auto row, std::optional<bool> passed) {
        outputTableRows_[numPassed] =
            passed.value() ? const_cast<char*>(kPassed) : nullptr;
        rawOutputProbeRowMapping[numPassed++] = row;
      };
      for (auto i = 0; i < numRows; ++i) {
        leftSemiProjectJoinTracker_.advance(
            rawOutputProbeRowMapping[i], filterPassed(i), addLast);
      }
      if (results_.atEnd()) {
        leftSemiProjectJoinTracker_.finish(addLast);
      }
    }
  } else if (isAntiJoin(joinType_)) {
    auto addMiss = [&](auto row) {
      outputTableRows_[numPassed] = nullptr;
      rawOutputProbeRowMapping[numPassed++] = row;
    };
    if (nullAware_) {
      auto passedRows =
          evalFilterForNullAwareJoin(numRows, filterPropagateNulls);
      for (auto i = 0; i < numRows; ++i) {
        auto probeRow = rawOutputProbeRowMapping[i];
        bool passed = passedRows.isValid(probeRow);
        noMatchDetector_.advance(probeRow, passed, addMiss);
      }
    } else {
      for (auto i = 0; i < numRows; ++i) {
        auto probeRow = rawOutputProbeRowMapping[i];
        noMatchDetector_.advance(probeRow, filterPassed(i), addMiss);
      }
    }

    noMatchDetector_.finishIteration(
        addMiss, results_.atEnd(), outputTableRows_.size() - numPassed);
  } else {
    for (auto i = 0; i < numRows; ++i) {
      if (filterPassed(i)) {
        outputTableRows_[numPassed] = outputTableRows_[i];
        rawOutputProbeRowMapping[numPassed++] = rawOutputProbeRowMapping[i];
      }
    }
  }
  return numPassed;
}

void HashProbe::ensureLoadedIfNotAtEnd(column_index_t channel) {
  auto inputChild = input_->childAt(channel);
  bool forceLoaded = input_->containsLazyNotLoaded() &&
      isLazyNotLoaded(*inputChild) && inputChild->containingLazyAndWrapped();
  if (!forceLoaded && results_.atEnd()) {
    return;
  }

  ensureLoaded(channel, forceLoaded);
}

void HashProbe::ensureLoaded(column_index_t channel, bool forceLoaded) {
  if (!forceLoaded &&
      (!filter_ &&
       (isLeftSemiFilterJoin(joinType_) || isLeftSemiProjectJoin(joinType_) ||
        isAntiJoin(joinType_)))) {
    return;
  }
  if (!passingInputRowsInitialized_) {
    passingInputRowsInitialized_ = true;
    passingInputRows_.resize(input_->size());
    if (isLeftJoin(joinType_) || isFullJoin(joinType_) ||
        isLeftSemiProjectJoin(joinType_) || isAntiJoin(joinType_)) {
      passingInputRows_.setAll();
    } else {
      passingInputRows_.clearAll();
      auto hitsSize = lookup_->hits.size();
      auto hits = lookup_->hits.data();
      for (auto i = 0; i < hitsSize; ++i) {
        if (hits[i]) {
          passingInputRows_.setValid(i, true);
        }
      }
    }
    passingInputRows_.updateBounds();
  }

  LazyVector::ensureLoadedRows(input_->childAt(channel), passingInputRows_);
}

void HashProbe::noMoreInput() {
  Operator::noMoreInput();
  noMoreInputInternal();
}

bool HashProbe::hasMoreInput() const {
  return !noMoreInput_ || (spillInputReader_ != nullptr && !noMoreSpillInput_);
}

void HashProbe::noMoreInputInternal() {
  checkRunning();

  // N-way late materialization: coalesce probe payload batches for extraction
  if (isNWayLateMOutput_ && probePayloadContainer_) {
    probePayloadContainer_->coalesceBatches();
  }

  noMoreSpillInput_ = true;
  if (!spillInputPartitionIds_.empty()) {
    // BOLT_CHECK_EQ(
    //  spillInputPartitionIds_.size(), spiller_->spilledPartitionSet().size());
    spiller_->finishSpill(spillPartitionSet_);
    recordSpillStats();
  }

  // Setup spill partition data.
  const bool hasSpillData = hasMoreSpillData();
  if (!needLastProbe() && !hasSpillData) {
    return;
  }

  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<Driver>> peers;
  // The last operator to finish processing inputs is responsible for
  // producing build-side rows based on the join.
  if (!operatorCtx_->task()->allPeersFinished(
          planNodeId(),
          operatorCtx_->driver(),
          hasSpillData ? &future_ : nullptr,
          hasSpillData ? promises_ : promises,
          peers)) {
    if (hasSpillData) {
      BOLT_CHECK(future_.valid());
      setState(ProbeOperatorState::kWaitForPeers);
    }
    DCHECK(promises.empty());
    return;
  }
  // NOTE: if 'hasSpillData' is false, then this is the last built table to
  // probe. Correspondingly, the hash probe operator except the last one can
  // simply finish its processing without waiting the other peers to reach the
  // barrier.
  BOLT_CHECK(promises.empty());
  BOLT_CHECK(hasSpillData || peers.empty());
  lastProber_ = true;
}

void HashProbe::recordSpillStats() {
  BOLT_CHECK_NOT_NULL(spiller_);
  const auto spillStats = spiller_->stats();
  BOLT_CHECK_EQ(spillStats.spillSortTimeUs, 0);
  BOLT_CHECK_EQ(spillStats.spillFillTimeUs, 0);
  Operator::recordSpillStats(spillStats);
}

void HashProbe::recordSpillReadStats() {
  if (spillInputReader_) {
    common::SpillReadStats spillReadStats;
    spillReadStats.spillReadTimeUs = spillInputReader_->getSpillReadTime();
    spillReadStats.spillDecompressTimeUs =
        spillInputReader_->getSpillDecompressTime();
    Operator::recordSpillReadStats(spillReadStats);
  }
}

bool HashProbe::isFinished() {
  return state_ == ProbeOperatorState::kFinish;
}

bool HashProbe::isRunning() const {
  return state_ == ProbeOperatorState::kRunning;
}

void HashProbe::checkRunning() const {
  BOLT_CHECK(isRunning(), probeOperatorStateName(state_));
}

void HashProbe::setRunning() {
  setState(ProbeOperatorState::kRunning);
}

void HashProbe::close() {
  Operator::close();

  // Free up major memory usage.
  joinBridge_.reset();
  spiller_.reset();
  matchFlagSpiller_.reset();
  table_.reset();
  outputRowMapping_.reset();
  output_.reset();
  nonSpillInputIndicesBuffer_.reset();
  spillInputIndicesBuffers_.clear();
  spillInputReader_.reset();
  spillMatchFlagReader_.reset();
}

void HashProbe::updateAndSpillProbeMatchFlags(
    RowVectorPtr& matchFlag,
    vector_size_t size,
    bool doSpill) {
  BOLT_CHECK(matchFlag);
  auto& hits = lookup_->hits;
  BOLT_CHECK_EQ(hits.size(), size);
  auto flags = matchFlag->childAt(0)->as<FlatVector<bool>>();
  for (auto i = 0; i < size; ++i) {
    if (hits[i]) {
      flags->set(i, true);
    }
  }

  if (doSpill) {
    BOLT_CHECK(matchFlagSpiller_);
    matchFlagSpiller_->spill(0, matchFlag);
  }
}

void HashProbe::mergeAndSpillProbeMatchFlags() {
  BOLT_CHECK(
      accumulatedMatchFlag_ && tmpMatchFlagForFilter_ && matchFlagSpiller_);
  BOLT_CHECK(tmpMatchFlagForFilter_->size() == accumulatedMatchFlag_->size());
  auto size = tmpMatchFlagForFilter_->size();
  auto target = accumulatedMatchFlag_->childAt(0)
                    ->as<FlatVector<bool>>()
                    ->asRange()
                    .bits();
  auto right = tmpMatchFlagForFilter_->childAt(0)
                   ->as<FlatVector<bool>>()
                   ->asRange()
                   .bits();
  bits::orBits(
      const_cast<uint64_t*>(target), const_cast<uint64_t*>(right), 0, size);

  matchFlagSpiller_->spill(0, accumulatedMatchFlag_);
}

void HashProbe::resetHashTable() {
  // [morsel] Under morsel-driven mode, not all drivers for a pipeline are
  // created at init phase, and the actual number of drivers might change
  // dynamicly. Therefore, we cannot reset hash table once one hashProbe
  // operator finishes. Otherwise, the task might hang.
  if (operatorCtx_->task()->numDrivers(operatorCtx_->driverCtx()) == 1 &&
      isFinished() &&
      FOLLY_LIKELY(
          !operatorCtx_->driverCtx()->queryConfig().morselDrivenEnabled())) {
    table_.reset();
    joinBridge_->resetHashTable();
  }
}

void HashProbe::fillOutputLateMaterialization(vector_size_t size) {
  // Intermediate probe in N-way chain: pass rowIds downstream instead of materializing
  // Data flow:
  // - For key columns (precomputed): materialize in output for downstream HashBuild
  // - For non-key columns: store in containers only (deferred materialization)

  auto* driverCtx = operatorCtx_->driverCtx();

  // FAST PATH: Pointer reuse mode with no probe payload columns.
  // Just pass raw row pointers directly - minimal overhead.
  if (pointerReuseEnabled_ && probePayloadProjections_.empty()) {
    // Copy columnSourceMap once (needed for final materialization chain)
    if (!columnSourceMapUpdated_) {
      if (table_->hybridData()) {
        driverCtx->columnSourceMap = table_->hybridData()->getColumnSourceMap();
      }
      updateColumnSourceMapForOutput();
    }

    // Pass outputTableRows_ directly - no unwrapping needed.
    // Downstream HashBuild will store these as upstream refs.
    // VirtualRow chain is followed at final materialization, not here.
    driverCtx->buildSideLateMBuildRowPtrs.assign(
        outputTableRows_.begin(), outputTableRows_.begin() + size);
    
    // ProbeRowIds are only needed if there are probe-side payload columns.
    // Since probePayloadProjections_ is empty, we can use dummy values.
    driverCtx->buildSideLateMProbeRowIds.resize(size);
    // No need to populate with real IDs - they won't be used
    
    prepareOutput(size);
    return;
  }
  
  // Log once per operator if we're NOT taking the fast path in reuse mode
  static thread_local bool loggedOnce = false;
  if (pointerReuseEnabled_ && !loggedOnce) {
    LOG(WARNING) << "[HashProbe] planNodeId=" << planNodeId()
                 << " NOT taking fast path: probePayloadProjections_.size()=" 
                 << probePayloadProjections_.size();
    loggedOnce = true;
  }
  // 
  // Note: probeKeyProjections_, probePayloadProjections_, buildKeyProjections_,
  // buildKeyChannelMapping_ are precomputed once in initialize()

  // 0. Unwrap VirtualRow pointers once at the beginning.
  // In pointer reuse mode, outputTableRows_ contains VirtualRow* pointers.
  // We unwrap them here so all subsequent code sees pure row pointers.
  std::vector<char*> buildRowPtrs(outputTableRows_.begin(),
                                   outputTableRows_.begin() + size);
  if (table_->isUsingVirtualRows()) {
    for (vector_size_t i = 0; i < size; ++i) {
      const VirtualRow* vrow = table_->getVirtualRow(buildRowPtrs[i]);
      buildRowPtrs[i] = vrow->sourcePtr;
    }
  }

  // 1. Store only NON-KEY probe columns in ProbePayloadContainer
  //    (key columns are materialized in output, no need to duplicate)
  if (!probePayloadProjections_.empty() && probePayloadContainer_) {
    std::vector<VectorPtr> payloadColumns;
    payloadColumns.reserve(probePayloadProjections_.size());
    for (const auto& projection : probePayloadProjections_) {
      ensureLoadedIfNotAtEnd(projection.inputChannel);
      payloadColumns.push_back(wrapChild(size, outputRowMapping_, input_->childAt(projection.inputChannel)));
    }
    
    auto payloadBatch = std::make_shared<RowVector>(
        pool(),
        probePayloadType_,
        nullptr,
        size,
        std::move(payloadColumns));

    probePayloadContainer_->addBatch(payloadBatch);
  }

  // 2. Set rowIds for downstream HashBuild
  // probeRowIds are sequential indices into probePayloadContainer_.
  // The payload batch was added with consecutive rows 0..size-1 at offset
  // probePayloadContainer_->numRows() - size, so we need sequential IDs.
  driverCtx->buildSideLateMBuildRowPtrs.resize(size);
  driverCtx->buildSideLateMProbeRowIds.resize(size);

  for (vector_size_t i = 0; i < size; ++i) {
    // Use already-unwrapped buildRowPtrs (VirtualRow handled in step 0)
    driverCtx->buildSideLateMBuildRowPtrs[i] = buildRowPtrs[i];
    // probeRowId is a sequential index into probePayloadContainer_
    // (not an index into the input vector)
    uint64_t probeRowId = (static_cast<uint64_t>(driverId_) << 56) |
        ((probeRowIdBase_ + i) & ((1ULL << 56) - 1));
    driverCtx->buildSideLateMProbeRowIds[i] = probeRowId;
  }

  // Increment by the number of output rows added to probePayloadContainer_
  probeRowIdBase_ += size;

  // 3. Copy columnSourceMap from hash table to DriverCtx (once only)
  //    The hash table's columnSourceMap was set up by HashBuild.
  //    We need to copy it before updateColumnSourceMapForOutput() which remaps it.
  if (!columnSourceMapUpdated_) {
    if (table_->hybridData()) {
      driverCtx->columnSourceMap = table_->hybridData()->getColumnSourceMap();
    }
  }
  
  // 4. Update columnSourceMap (once only, tracked by flag)
  updateColumnSourceMapForOutput();

  // 5. For pointer reuse mode: skip output materialization entirely.
  // Data is passed through driverCtx (buildSideLateMBuildRowPtrs/ProbeRowIds).
  // But we must return a non-null RowVector so the pipeline continues.
  // Downstream HashBuild ignores vector content in pointer reuse mode.
  if (pointerReuseEnabled_) {
    prepareOutput(size);
    // Leave all children as nullptr - HashBuild reads from driverCtx
    return;
  }

  // 6. Standard late-m path: Prepare output and fill with selective materialization
  prepareOutput(size);
  
  // 6a. Probe-side KEY columns: materialize directly into output_
  for (const auto& projection : probeKeyProjections_) {
    ensureLoadedIfNotAtEnd(projection.inputChannel);
    output_->childAt(projection.outputChannel) = wrapChild(
        size, outputRowMapping_, input_->childAt(projection.inputChannel));
  }
  // Non-key probe columns: in ProbePayloadContainer, output_ child stays nullptr
  
  // 6b. Build-side KEY columns: extract directly into output_ using precomputed mapping
  if (!buildKeyChannelMapping_.empty()) {
    // Pre-allocate output columns for build keys
    for (const auto& projection : buildKeyProjections_) {
      output_->childAt(projection.outputChannel) = BaseVector::create(
          outputType_->childAt(projection.outputChannel), size, pool());
    }

    // Extract key columns using already-unwrapped buildRowPtrs (from step 0)
    HybridContainer::extractColumnsFromUpstream(
        buildKeyChannelMapping_,
        buildRowPtrs,
        std::vector<uint64_t>{},
        table_->hybridData(),
        nullptr,
        driverCtx->columnSourceMap,
        output_);
  }
  // Non-key build columns: stay nullptr - tracked via columnSourceMap
}

void HashProbe::updateColumnSourceMapForOutput() {
  // Only update once (requires table_ to be available)
  if (columnSourceMapUpdated_) {
    return;
  }
  columnSourceMapUpdated_ = true;

  auto* driverCtx = operatorCtx_->driverCtx();
  auto* hybridData = table_->hybridData();

  // Set the primary upstream hash table - keeps the table alive so row pointers remain valid.
  // Also set the container pointer for extraction.
  if (!driverCtx->primaryUpstreamHashTable) {
    driverCtx->primaryUpstreamHashTable = table_;  // Keep hash table alive
    driverCtx->primaryUpstreamBuildContainer = std::shared_ptr<HybridContainer>(
        hybridData, [](HybridContainer*) {}); // non-deleting, hash table owns it
  }

  // Create new columnSourceMap keyed by outputChannel
  ColumnSourceMap outputChannelMap;

  // For each build-side column in output: remap storageChannel → outputChannel
  for (auto projection : tableOutputProjections_) {
    int32_t storageChannel = projection.inputChannel;
    int32_t outputChannel = projection.outputChannel;

    auto it = driverCtx->columnSourceMap.find(storageChannel);
    if (it != driverCtx->columnSourceMap.end()) {
      // This column comes from upstream (N-way path)
      outputChannelMap[outputChannel] = it->second;
    } else if (hybridData != nullptr) {
      // This column is from the current build table
      if (hybridData->isKey(storageChannel)) {
        outputChannelMap[outputChannel] = ColumnSource{
            ColumnSource::Type::HYBRID_KEY, storageChannel, hybridData};
      } else {
        // Payload column: payloadIndex = storageChannel - numKeys
        int32_t payloadIndex = storageChannel - hybridData->numKeys();
        outputChannelMap[outputChannel] = ColumnSource{
            ColumnSource::Type::HYBRID_PAYLOAD, payloadIndex, hybridData};
      }
    }
  }

  // For probe-side NON-KEY columns: they're stored in ProbePayloadContainer for final materialization
  // Key columns are materialized in output, not stored in container
  if (!probePayloadProjections_.empty() && probePayloadContainer_) {
    // Map non-key probe columns to PROBE_PAYLOAD (using output channel)
    for (size_t i = 0; i < probePayloadProjections_.size(); ++i) {
      int32_t outputChannel = probePayloadProjections_[i].outputChannel;
      outputChannelMap[outputChannel] = ColumnSource{
          ColumnSource::Type::PROBE_PAYLOAD,
          static_cast<int32_t>(i),  // column index in ProbePayloadContainer
          probePayloadContainer_.get()};
    }
  }

  // Replace the columnSourceMap with output-channel-keyed map
  driverCtx->columnSourceMap = std::move(outputChannelMap);
}

void HashProbe::fillOutputFinalMaterialization(vector_size_t size) {
  // Final probe: extract all columns from their sources using columnSourceMap
  auto* driverCtx = operatorCtx_->driverCtx();

  // Build initial row pointers vector for build-side extraction
  std::vector<char*> buildRowPtrs(outputTableRows_.begin(),
                                   outputTableRows_.begin() + size);

  // Pointer reuse mode handling
  std::vector<vector_size_t> expandedProbeMapping;
  std::vector<size_t> intermediateIndices;

  auto* hybridData = table_->hybridData();
  const bool isPointerReuseMode = hybridData && hybridData->isPointerReuseMode();
  const bool useVirtualRows = table_->isUsingVirtualRows();

  std::vector<char*> sourcePtrs;  // For VirtualRow mode: actual R row pointers

  if (useVirtualRows) {
    // VirtualRow mode: hits are VirtualRow*, not R_ptrs
    // No expansion needed - each VirtualRow already has its unique originalIndex
    // Just extract sourcePtr and originalIndex from each VirtualRow
    sourcePtrs.resize(size);
    intermediateIndices.resize(size);
    for (vector_size_t i = 0; i < size; ++i) {
      char* hitPtr = buildRowPtrs[i];
      const VirtualRow* vrow = table_->getVirtualRow(hitPtr);
      sourcePtrs[i] = vrow->sourcePtr;
      intermediateIndices[i] = vrow->originalIndex;
    }
  } else if (isPointerReuseMode) {
    // Old pointer reuse mode: expand using ptrToIndex (fallback)
    std::vector<char*> expandedBuildRowPtrs;
    const auto& ptrToIndex = hybridData->getPtrToIndexMap();
    // Expand: for each hit R_ptr, output one row per intermediate index
    for (vector_size_t i = 0; i < size; ++i) {
      char* ptr = buildRowPtrs[i];
      auto range = ptrToIndex.equal_range(ptr);
      if (range.first == range.second) {
        // Should not happen, but handle gracefully
        expandedProbeMapping.push_back(outputRowMapping_->as<vector_size_t>()[i]);
        expandedBuildRowPtrs.push_back(ptr);
        intermediateIndices.push_back(SIZE_MAX);
      } else {
        for (auto it = range.first; it != range.second; ++it) {
          expandedProbeMapping.push_back(outputRowMapping_->as<vector_size_t>()[i]);
          expandedBuildRowPtrs.push_back(ptr);
          intermediateIndices.push_back(it->second);
        }
      }
    }

    // Update variables to use expanded data
    size = expandedBuildRowPtrs.size();
    buildRowPtrs = std::move(expandedBuildRowPtrs);
    sourcePtrs = buildRowPtrs;  // In old mode, buildRowPtrs ARE the source ptrs
  }

  // Prepare output with (potentially expanded) size
  prepareOutput(size);

  // For probe-side columns: wrap as dictionary over current input
  for (auto projection : projectedInputColumns_) {
    ensureLoadedIfNotAtEnd(projection.inputChannel);
  }

  if (useVirtualRows) {
    // VirtualRow mode: no probe mapping expansion needed
    wrapIndirectChildren(
        projectedInputColumns_,
        input_->children(),
        size,
        outputRowMapping_,
        output_->children());
  } else if (isPointerReuseMode) {
    // Old mode: use expanded probe mapping
    BufferPtr expandedMappingBuffer =
        AlignedBuffer::allocate<vector_size_t>(size, pool());
    auto* expandedMappingData = expandedMappingBuffer->asMutable<vector_size_t>();
    std::copy(expandedProbeMapping.begin(), expandedProbeMapping.end(), expandedMappingData);
    wrapIndirectChildren(
        projectedInputColumns_,
        input_->children(),
        size,
        expandedMappingBuffer,
        output_->children());
  } else {
    wrapIndirectChildren(
        projectedInputColumns_,
        input_->children(),
        size,
        outputRowMapping_,
        output_->children());
  }

  // For build-side columns: use columnSourceMap to extract from correct sources
  const auto& sourceMap = driverCtx->columnSourceMap;

  // Collect channels to extract
  std::vector<column_index_t> channelsToExtract;
  for (auto projection : tableOutputProjections_) {
    channelsToExtract.push_back(projection.inputChannel);
  }

  if (channelsToExtract.empty()) {
    return;
  }

  // Create result vector for extracted columns
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::vector<VectorPtr> children;
  for (auto projection : tableOutputProjections_) {
    names.push_back(outputType_->nameOf(projection.outputChannel));
    types.push_back(outputType_->childAt(projection.outputChannel));
    children.push_back(
        BaseVector::create(types.back(), size, pool()));
  }
  auto extractedType = ROW(std::move(names), std::move(types));
  auto extractedResult = std::make_shared<RowVector>(
      pool(), extractedType, nullptr, size, std::move(children));

  // Extract build-side columns using N-way pre-compute approach
  // Use sourcePtrs (actual R row pointers) for data extraction
  const std::vector<char*>& extractionPtrs = useVirtualRows ? sourcePtrs : buildRowPtrs;
  
  if (useVirtualRows || isPointerReuseMode) {
    // Use intermediateIndices for accurate probe side lookups
    HybridContainer::extractColumnsFromUpstreamWithIndices(
        channelsToExtract,
        extractionPtrs,
        intermediateIndices,
        hybridData,
        nullptr, // currentProbePayload - current probe handled separately
        sourceMap,
        extractedResult);
  } else {
    HybridContainer::extractColumnsFromUpstream(
        channelsToExtract,
        extractionPtrs,
        std::vector<uint64_t>{}, // No level 0 probeRowIds - current probe handled by wrapIndirectChildren
        hybridData,
        nullptr, // currentProbePayload - current probe handled separately
        sourceMap,
        extractedResult);
  }

  // Copy extracted columns to output
  for (size_t i = 0; i < tableOutputProjections_.size(); ++i) {
    output_->childAt(tableOutputProjections_[i].outputChannel) =
        extractedResult->childAt(i);
  }
}

} // namespace bytedance::bolt::exec

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

#include "bolt/exec/OrderBy.h"

#include "bolt/exec/HashProbe.h"
#include "bolt/exec/JoinAggReuse.h"
#include "bolt/exec/OperatorUtils.h"
#include "bolt/exec/SortBuffer.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/radixsort/RadixSortBuffer.h"
#include "bolt/vector/FlatVector.h"
#include "exec/OperatorMetric.h"

namespace bytedance::bolt::exec {

namespace {
CompareFlags fromSortOrderToCompareFlags(const core::SortOrder& sortOrder) {
  return {
      sortOrder.isNullsFirst(),
      sortOrder.isAscending(),
      false,
      CompareFlags::NullHandlingMode::kNullAsValue};
}

bool containsFloatingPointType(const Type& type) {
  switch (type.kind()) {
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      return true;
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
      for (uint32_t child = 0; child < type.size(); ++child) {
        if (containsFloatingPointType(*type.childAt(child))) {
          return true;
        }
      }
      return false;
    default:
      return false;
  }
}
} // namespace

OrderBy::OrderBy(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::OrderByNode>& orderByNode)
    : Operator(
          driverCtx,
          orderByNode->outputType(),
          operatorId,
          orderByNode->id(),
          "OrderBy",
          orderByNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(
                    operatorId,
                    driverCtx->queryConfig().rowBasedSpillMode())
              : std::nullopt) {
  maxOutputRows_ = outputBatchRows(std::nullopt);
  BOLT_CHECK(pool()->trackUsage());

  // Join-sort reuse: the sort buffer stores a reference to the join's build
  // row in place of the deferred build columns.
  sortInputType_ = outputType_;
  if (const auto& plan = driverCtx->joinAggReusePlan; plan != nullptr) {
    deferral_ = plan->forOrderBy(planNodeId());
  }
  if (deferral_ != nullptr) {
    const auto& deferred = *deferral_->rightDeep;
    std::unordered_map<std::string, column_index_t> tableChannels;
    for (size_t i = 0; i < deferred.deferredColumns.size(); ++i) {
      tableChannels[deferred.deferredColumns[i]] =
          deferred.sourceTableChannels[i];
    }
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    for (column_index_t i = 0; i < outputType_->size(); ++i) {
      auto it = tableChannels.find(outputType_->nameOf(i));
      if (it != tableChannels.end()) {
        derefProjections_.emplace_back(it->second, i);
        continue;
      }
      keptChannels_.push_back(i);
      names.push_back(outputType_->nameOf(i));
      types.push_back(outputType_->childAt(i));
    }
    names.push_back(deferred.refColumn);
    types.push_back(BIGINT());
    sortInputType_ = ROW(std::move(names), std::move(types));
  }

  std::vector<column_index_t> sortColumnIndices;
  std::vector<CompareFlags> sortCompareFlags;
  sortColumnIndices.reserve(orderByNode->sortingKeys().size());
  sortCompareFlags.reserve(orderByNode->sortingKeys().size());
  for (int i = 0; i < orderByNode->sortingKeys().size(); ++i) {
    const auto channel =
        exprToChannel(orderByNode->sortingKeys()[i].get(), sortInputType_);
    BOLT_CHECK(
        channel != kConstantChannel,
        "OrderBy doesn't allow constant sorting keys");
    sortColumnIndices.push_back(channel);
    sortCompareFlags.push_back(
        fromSortOrderToCompareFlags(orderByNode->sortingOrders()[i]));
  }
  const auto& queryConfig = driverCtx->queryConfig();
  auto useRadixSort = queryConfig.orderByRadixSortEnabled();
  if (useRadixSort &&
      queryConfig.orderByRadixSortFallbackForFloatingPointKeysEnabled()) {
    for (const auto channel : sortColumnIndices) {
      if (containsFloatingPointType(*sortInputType_->childAt(channel))) {
        useRadixSort = false;
        break;
      }
    }
  }
  if (useRadixSort) {
    sortBuffer_ = std::make_unique<radixsort::RadixSortBuffer>(
        sortInputType_,
        sortColumnIndices,
        sortCompareFlags,
        pool(),
        spillConfig_.has_value() ? &(spillConfig_.value()) : nullptr,
        operatorCtx_->driverCtx()->queryConfig().orderBySpillMemoryThreshold(),
        operatorCtx_.get(),
        &nonReclaimableSection_);
  } else {
    auto hybridSortEnabled = driverCtx->queryConfig().hybridSortEnabled();
    sortBuffer_ = std::make_unique<SortBuffer>(
        sortInputType_,
        sortColumnIndices,
        sortCompareFlags,
        pool(),
        &nonReclaimableSection_,
        spillConfig_.has_value() ? &(spillConfig_.value()) : nullptr,
        operatorCtx_->driverCtx()->queryConfig().orderBySpillMemoryThreshold(),
        operatorCtx_.get(),
        hybridSortEnabled,
        driverCtx->queryConfig().hybridSortMinPayloadBytes());
  }

  this->setRuntimeMetric(
      OperatorMetricKey::kCanUsedToEstimateHashBuildPartitionNum, "true");
  this->setRuntimeMetric(
      OperatorMetricKey::kTotalRowCount, folly::to<std::string>(0));
  this->setRuntimeMetric(
      OperatorMetricKey::kHasBeenProcessedRowCount, folly::to<std::string>(0));
  LOG(INFO) << name() << " construct, output type: " << outputType_->toString()
            << ", sort buffer: " << (useRadixSort ? "radix" : "legacy");
}

void OrderBy::addInput(RowVectorPtr input) {
  {
    // Loading lazy input may need to arbitrate for memory. It is safe to
    // reclaim the existing sort buffer here because addInput hasn't started
    // mutating it yet. Keep the actual SortBuffer::addInput call in the
    // driver's non-reclaimable section: concurrent spill clears RowContainer
    // state, including allocator-backed row pointers.
    ReclaimableSectionGuard guard(this);
    input->loadedVector();
  }
  sortBuffer_->addInput(
      deferral_ != nullptr ? replaceDeferredColumns(input) : input);
}

RowVectorPtr OrderBy::replaceDeferredColumns(const RowVectorPtr& input) {
  if (deferralSource_ == nullptr) {
    const auto& sourceId = deferral_->chainJoinIds.back();
    for (auto* op : operatorCtx_->driver()->operators()) {
      auto* probe = dynamic_cast<HashProbe*>(op);
      if (probe != nullptr && probe->planNodeId() == sourceId) {
        deferralSource_ = probe;
        break;
      }
    }
    BOLT_CHECK_NOT_NULL(deferralSource_);
    deferralTable_ = deferralSource_->joinAggReuseTable();
  }
  BOLT_CHECK(
      deferralSource_->joinAggReuseActive(),
      "join-sort reuse: the join below did not publish its rows");
  auto& channels = deferralSource_->joinAggReuseChannels();
  BOLT_CHECK_EQ(channels.size(), 1);
  BOLT_CHECK_EQ(channels[0].size(), input->size());
  auto refs = BaseVector::create<FlatVector<int64_t>>(
      BIGINT(), input->size(), pool());
  auto* rawRefs = refs->mutableRawValues();
  for (vector_size_t i = 0; i < input->size(); ++i) {
    rawRefs[i] = reinterpret_cast<int64_t>(channels[0][i]);
  }
  channels.clear();
  std::vector<VectorPtr> children;
  children.reserve(keptChannels_.size() + 1);
  for (auto channel : keptChannels_) {
    children.push_back(input->childAt(channel));
  }
  children.push_back(std::move(refs));
  return std::make_shared<RowVector>(
      pool(),
      sortInputType_,
      input->nulls(),
      input->size(),
      std::move(children));
}

RowVectorPtr OrderBy::restoreDeferredColumns(const RowVectorPtr& sorted) {
  const auto size = sorted->size();
  DecodedVector refs(*sorted->childAt(keptChannels_.size()));
  deferralRows_.resize(size);
  for (vector_size_t i = 0; i < size; ++i) {
    deferralRows_[i] = reinterpret_cast<char*>(refs.valueAt<int64_t>(i));
  }
  std::vector<VectorPtr> children(outputType_->size());
  for (size_t i = 0; i < keptChannels_.size(); ++i) {
    children[keptChannels_[i]] = sorted->childAt(i);
  }
  HashProbe::extractTableColumns(
      deferralTable_.get(),
      folly::Range<char**>(deferralRows_.data(), size),
      derefProjections_,
      pool(),
      outputType_->children(),
      children);
  return std::make_shared<RowVector>(
      pool(), outputType_, nullptr, size, std::move(children));
}

void OrderBy::reclaim(
    uint64_t targetBytes,
    memory::MemoryReclaimer::Stats& stats) {
  BOLT_CHECK(canReclaim());
  BOLT_CHECK(!nonReclaimableSection_);

  // TODO: support fine-grain disk spilling based on 'targetBytes' after
  // having row container memory compaction support later.
  sortBuffer_->spill();
  if (noMoreInput_) {
    recordSpillStats();
  }

  // Release the minimum reserved memory.
  pool()->release();
}

void OrderBy::noMoreInput() {
  Operator::noMoreInput();
  sortBuffer_->noMoreInput();
  maxOutputRows_ = outputBatchRows(sortBuffer_->estimateOutputRowSize());
  recordSpillStats();
}

RowVectorPtr OrderBy::getOutput() {
  if (finished_ || !noMoreInput_) {
    return nullptr;
  }

  // When the OrderBy operator has no more output, the sortbuffer memory is
  // cleaned up immediately, without waiting until operator::close() is called.
  // This reduces the peak memory usage of the Pipeline.
  // Immediate cleanup is safe because the data is deep-copied into the output.
  auto guard = folly::makeGuard([&]() {
    if (finished_) {
      sortBuffer_.reset();
      deferralTable_.reset();
    }
  });

  RowVectorPtr output = sortBuffer_->getOutput(maxOutputRows_);
  finished_ = (output == nullptr);
  if (!finished_ && deferral_ != nullptr) {
    output = restoreDeferredColumns(output);
  }
  if (finished_) {
    recordSpillReadStats();
    recordSortStats();
  }

  this->setRuntimeMetric(
      OperatorMetricKey::kTotalRowCount,
      folly::to<std::string>(sortBuffer_->numInputRows()));
  this->setRuntimeMetric(
      OperatorMetricKey::kHasBeenProcessedRowCount,
      folly::to<std::string>(sortBuffer_->numOutputRows()));

  return output;
}

void OrderBy::close() {
  Operator::close();
  sortBuffer_.reset();
  deferralTable_.reset();
}

void OrderBy::recordSpillStats() {
  BOLT_CHECK_NOT_NULL(sortBuffer_);
  auto spillStats = sortBuffer_->spilledStats();
  if (spillStats.has_value()) {
    Operator::recordSpillStats(spillStats.value() - recordedSpillStats_);
    recordedSpillStats_ = spillStats.value();
  }
}

void OrderBy::recordSpillReadStats() {
  auto spillReadStatsOr = sortBuffer_->spillReadStats();
  if (spillReadStatsOr.has_value()) {
    Operator::recordSpillReadStats(spillReadStatsOr.value());
  }
}

void OrderBy::recordSortStats() {
  auto sortStatsOr = sortBuffer_->sortStats();
  if (sortStatsOr.has_value()) {
    Operator::recordSortStats(sortStatsOr.value());
  }
}

} // namespace bytedance::bolt::exec

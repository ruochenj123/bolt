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

#include "SortBuffer.h"
#include <algorithm>
#include "HashTable.h"
#include "Spiller.h"
#include "bolt/exec/MemoryReclaimer.h"
#include "bolt/exec/RowToColumnVector.h"
#include "bolt/jit/RowContainer/RowContainerCodeGenerator.h"

#ifdef ENABLE_META_SORT
#include "bolt/exec/meta/MetaRowSorterApi.h"
#endif
namespace bytedance::bolt::exec {

SortBuffer::SortBuffer(
    const RowTypePtr& input,
    const std::vector<column_index_t>& sortColumnIndices,
    const std::vector<CompareFlags>& sortCompareFlags,
    bolt::memory::MemoryPool* pool,
    tsan_atomic<bool>* nonReclaimableSection,
    const common::SpillConfig* spillConfig,
    uint64_t spillMemoryThreshold,
    OperatorCtx* operatorCtx,
    bool hybridSortEnabled,
    bool lateMaterializationEnabled,
    DriverCtx* driverCtx)
    : input_(input),
      sortCompareFlags_(sortCompareFlags),
      pool_(pool),
      nonReclaimableSection_(nonReclaimableSection),
      spillConfig_(spillConfig),
      spillMemoryThreshold_(spillMemoryThreshold),
      operatorCtx_(operatorCtx),
      hybridSortEnabled_(hybridSortEnabled),
      lateMaterializationEnabled_(lateMaterializationEnabled),
      driverCtx_(driverCtx) {
  BOLT_CHECK_GE(input_->size(), sortCompareFlags_.size());
  BOLT_CHECK_GT(sortCompareFlags_.size(), 0);
  BOLT_CHECK_EQ(sortColumnIndices.size(), sortCompareFlags_.size());
  BOLT_CHECK_NOT_NULL(nonReclaimableSection_);

  // Late materialization and hybrid sort are incompatible.
  // Late-m mode: sort keys come from input, payloads come from upstream containers.
  // Hybrid sort mode: all columns come from input and are stored in HybridContainer.
  // When late-m is enabled, disable hybrid sort to use the late-m code path.
  if (lateMaterializationEnabled_) {
    hybridSortEnabled_ = false;
  }

  // Validate that hybrid sort is not used with row-based spilling
  if (hybridSortEnabled_ && spillConfig_ != nullptr) {
    BOLT_CHECK(
        spillConfig_->rowBasedSpillMode == common::RowBasedSpillMode::DISABLE,
        "Hybrid sort is not compatible with row-based spilling");
  }

  std::vector<TypePtr> sortedColumnTypes;
  std::vector<TypePtr> nonSortedColumnTypes;
  std::vector<std::string> nonSortedColumnNames;
  std::vector<std::string> sortedSpillColumnNames;
  std::vector<TypePtr> sortedSpillColumnTypes;
  sortedColumnTypes.reserve(sortColumnIndices.size());
  nonSortedColumnTypes.reserve(input->size() - sortColumnIndices.size());
  nonSortedColumnNames.reserve(input->size() - sortColumnIndices.size());
  payloadChannels_.reserve(input->size() - sortColumnIndices.size());
  sortedSpillColumnNames.reserve(input->size());
  sortedSpillColumnTypes.reserve(input->size());
  std::unordered_set<column_index_t> sortedChannelSet;
  // Sorted key columns.
  for (column_index_t i = 0; i < sortColumnIndices.size(); ++i) {
    keyColumnMap_.emplace_back(IdentityProjection(i, sortColumnIndices.at(i)));
    columnMap_.emplace_back(IdentityProjection(i, sortColumnIndices.at(i)));
    sortedColumnTypes.emplace_back(input_->childAt(sortColumnIndices.at(i)));
    sortedSpillColumnTypes.emplace_back(
        input_->childAt(sortColumnIndices.at(i)));
    sortedSpillColumnNames.emplace_back(input->nameOf(sortColumnIndices.at(i)));
    sortedChannelSet.emplace(sortColumnIndices.at(i));
  }
  // Non-sorted key columns.
  for (column_index_t i = 0, nonSortedIndex = sortCompareFlags_.size();
       i < input_->size();
       ++i) {
    if (sortedChannelSet.count(i) != 0) {
      continue;
    }
    payloadColumnMap_.emplace_back(nonSortedIndex, i);
    columnMap_.emplace_back(nonSortedIndex, i);
    nonSortedIndex++;
    nonSortedColumnTypes.emplace_back(input_->childAt(i));
    nonSortedColumnNames.emplace_back(input->nameOf(i));
    payloadChannels_.push_back(i);
    sortedSpillColumnTypes.emplace_back(input_->childAt(i));
    sortedSpillColumnNames.emplace_back(input->nameOf(i));
  }
  hybridSortEnabled_ = hybridSortEnabled_ && !nonSortedColumnTypes.empty();
  LOG(ERROR) << "SortBuffer: hybridSortEnabled_=" << hybridSortEnabled_
            << ", nonSortedColumnTypes.size=" << nonSortedColumnTypes.size()
            << ", sortedColumnTypes.size=" << sortedColumnTypes.size();
  if (hybridSortEnabled_) {
    std::vector<TypePtr> rowIdType = {BIGINT()};
    data_ = std::make_unique<RowContainer>(sortedColumnTypes, rowIdType, pool_);
    hybridData_ = std::make_unique<HybridContainer>(
        sortedColumnTypes, nonSortedColumnTypes, data_.get());
    // Set allContainer
    std::unordered_map<uint8_t, HybridContainer*> hybridDataChannel;
    hybridDataChannel[0] = hybridData_.get();
    hybridData_->setAllContainers(hybridDataChannel);

    payloadTypes_ =
        ROW(std::move(nonSortedColumnNames), std::move(nonSortedColumnTypes));
  } else if (lateMaterializationEnabled_) {
    // For late-m: only store sort keys + rowId (for tracking original row index)
    // The non-sort columns will be materialized from upstream containers
    std::vector<TypePtr> rowIdType = {BIGINT()};
    data_ = std::make_unique<RowContainer>(sortedColumnTypes, rowIdType, pool_);
  } else {
    data_ = std::make_unique<RowContainer>(
        sortedColumnTypes, nonSortedColumnTypes, pool_);
  }
  spillerStoreType_ =
      ROW(std::move(sortedSpillColumnNames), std::move(sortedSpillColumnTypes));
  
  // Initialize late materialization state if enabled
  if (lateMaterializationEnabled_ && driverCtx_) {
    // Copy upstream references from DriverCtx - these need to be kept alive
    primaryUpstreamHashTable_ = driverCtx_->primaryUpstreamHashTable;
    upstreamProbePayloads_ = driverCtx_->buildSideLateMUpstreamProbePayloads;
    columnSourceMap_ = driverCtx_->columnSourceMap;
    LOG(INFO) << "SortBuffer: late materialization init: columnSourceMap size=" 
              << columnSourceMap_.size()
              << ", upstreamProbePayloads_.size()=" << upstreamProbePayloads_.size()
              << ", hasHashTable=" << (primaryUpstreamHashTable_ != nullptr);
  }
}

void SortBuffer::addInput(const VectorPtr& input) {
  BOLT_CHECK(!noMoreInput_);
  ensureInputFits(input);

  SelectivityVector allRows(input->size());
  std::vector<char*> rows(input->size());
  for (int row = 0; row < input->size(); ++row) {
    rows[row] = data_->newRow();
  }
  auto* inputRow = input->as<RowVector>();
  MicrosecondTimer timer(&sortColToRowTimeUs_);
  
  // For late materialization: capture row references from DriverCtx for this batch
  if (lateMaterializationEnabled_ && driverCtx_) {
    const auto& buildRowPtrs = driverCtx_->buildSideLateMBuildRowPtrs;
    const auto& probeRowIds = driverCtx_->buildSideLateMProbeRowIds;
    
    LOG(INFO) << "SortBuffer::addInput late-m: buildRowPtrs.size()=" << buildRowPtrs.size()
              << ", probeRowIds.size()=" << probeRowIds.size()
              << ", driverCtx columnSourceMap.size()=" << driverCtx_->columnSourceMap.size()
              << ", driverCtx probePayloads.size()=" << driverCtx_->buildSideLateMUpstreamProbePayloads.size();
    
    // Append row references for this batch
    lateMBuildRowPtrs_.insert(
        lateMBuildRowPtrs_.end(), buildRowPtrs.begin(), buildRowPtrs.end());
    lateMProbeRowIds_.insert(
        lateMProbeRowIds_.end(), probeRowIds.begin(), probeRowIds.end());
    
    // Update columnSourceMap if not already set (might have new entries)
    if (columnSourceMap_.empty() && !driverCtx_->columnSourceMap.empty()) {
      columnSourceMap_ = driverCtx_->columnSourceMap;
    }
    // Keep upstream references alive
    if (!primaryUpstreamHashTable_) {
      primaryUpstreamHashTable_ = driverCtx_->primaryUpstreamHashTable;
    }
    if (upstreamProbePayloads_.empty()) {
      upstreamProbePayloads_ = driverCtx_->buildSideLateMUpstreamProbePayloads;
    }
  }
  
  if (hybridSortEnabled_) {
    auto currentRows = hybridData_->getNumRows();
    
    for (int row = 0; row < input->size(); ++row) {
      // Store RowId: driverId (8 bits) | globalRowId (56 bits)
      uint64_t encodedId = (static_cast<uint64_t>(0) << 56) |
          (static_cast<uint64_t>(row + currentRows) & ((1ULL << 56) - 1));
      data_->storeSingleRowId(encodedId, rows[row]);
    }
    // Store key columns
    for (const auto& columnProjection : keyColumnMap_) {
      DecodedVector decoded(
          *inputRow->childAt(columnProjection.outputChannel), allRows);
      auto kind =
          inputRow->childAt(columnProjection.outputChannel)->type()->kind();
      BOLT_DYNAMIC_TYPE_DISPATCH(
          data_->storeColumn,
          kind,
          decoded,
          input->size(),
          rows,
          columnProjection.inputChannel);
    }
    // Gather payload columns
    std::vector<std::unique_ptr<DecodedVector>> decoders;
    decoders.reserve(payloadColumnMap_.size());
    for (const auto& columnProjection : payloadColumnMap_) {
      decoders.emplace_back(std::make_unique<DecodedVector>(
          *inputRow->childAt(columnProjection.outputChannel), allRows));
    }
    auto payloadInput = wrapColumns(
        input->as<RowVector>(), payloadChannels_, payloadTypes_, pool());
    hybridData_->addPayload(std::move(payloadInput));
  } else if (lateMaterializationEnabled_) {
    // For late-m: only store sort key columns (like hybrid mode)
    // Store rowId for tracking original row index
    auto currentRows = numInputRows_;
    for (int row = 0; row < input->size(); ++row) {
      // Store original row index as rowId (for sorting tracking)
      uint64_t encodedId = (static_cast<uint64_t>(0) << 56) |
          (static_cast<uint64_t>(row + currentRows) & ((1ULL << 56) - 1));
      data_->storeSingleRowId(encodedId, rows[row]);
    }
    // Store only sort key columns
    for (const auto& columnProjection : keyColumnMap_) {
      DecodedVector decoded(
          *inputRow->childAt(columnProjection.outputChannel), allRows);
      auto kind =
          inputRow->childAt(columnProjection.outputChannel)->type()->kind();
      BOLT_DYNAMIC_TYPE_DISPATCH(
          data_->storeColumn,
          kind,
          decoded,
          input->size(),
          rows,
          columnProjection.inputChannel);
    }
  } else {
    for (const auto& columnProjection : columnMap_) {
      DecodedVector decoded(
          *inputRow->childAt(columnProjection.outputChannel), allRows);
      data_->storeColumnVelox(
          decoded,
          input->size(),
          rows,
          columnProjection.inputChannel);
    }
  }

  numInputRows_ += allRows.size();
}

void SortBuffer::noMoreInput() {
  BOLT_CHECK(!noMoreInput_);
  noMoreInput_ = true;

  // No data.
  if (numInputRows_ == 0) {
    return;
  }
  if (hybridData_) {
    hybridData_->coalesceBatches();
  }

  if (spiller_ == nullptr) {
    BOLT_CHECK_EQ(numInputRows_, data_->numRows());
    updateEstimatedOutputRowSize();
    // Sort the pointers to the rows in RowContainer (data_) instead of sorting
    // the rows.
    sortedRows_.resize(numInputRows_);
    RowContainerIterator iter;
    data_->listRows(&iter, numInputRows_, sortedRows_.data());

    MicrosecondTimer timer(&sortInSortTimeUs_);

#ifdef ENABLE_BOLT_JIT
    if (cmp_ == nullptr && operatorCtx_ &&
        operatorCtx_->driverCtx()->queryConfig().enableJitRowCmpRow()) {
      if (data_->JITable(data_->keyTypes())) {
        auto [jitMod, rowRowCmpfn] = data_->codegenCompare(
            data_->keyTypes(),
            sortCompareFlags_,
            bytedance::bolt::jit::CmpType::SORT_LESS,
            true);
        jitModule_ = std::move(jitMod);
        cmp_ = (RowRowCompare)jitModule_->getFuncPtr(rowRowCmpfn);
      }
    }
    if (cmp_) {
      sorter_.sort(sortedRows_.begin(), sortedRows_.end(), cmp_);
    } else {
#endif

#ifdef ENABLE_META_SORT
      MetaRowsSorterWraper<BufferRows>::MetaCodegenSort(
          sortedRows_,
          data_.get(),
          sorter_,
          data_->keyIndices(),
          sortCompareFlags_);
#else
    sorter_.sort(
        sortedRows_.begin(),
        sortedRows_.end(),
        [this](const char* leftRow, const char* rightRow) {
          for (vector_size_t index = 0; index < sortCompareFlags_.size();
               ++index) {
            if (auto result = data_->compare(
                    leftRow, rightRow, index, sortCompareFlags_[index])) {
              return result < 0;
            }
          }
          return false;
        });
#endif

#ifdef ENABLE_BOLT_JIT
    }
#endif
    // For late materialization: reorder row references to match sorted order
    if (lateMaterializationEnabled_ && !lateMBuildRowPtrs_.empty()) {
      sortedBuildRowPtrs_.resize(numInputRows_);
      sortedProbeRowIds_.resize(numInputRows_);
      
      // Get original row indices from stored rowIds and reorder references
      for (size_t i = 0; i < numInputRows_; ++i) {
        uint64_t storedId = data_->getSingleRowId(sortedRows_[i]);
        size_t origIdx = storedId & ((1ULL << 56) - 1);
        sortedBuildRowPtrs_[i] = lateMBuildRowPtrs_[origIdx];
        sortedProbeRowIds_[i] = lateMProbeRowIds_[origIdx];
      }
      
      // Clear original refs to save memory
      lateMBuildRowPtrs_.clear();
      lateMBuildRowPtrs_.shrink_to_fit();
      lateMProbeRowIds_.clear();
      lateMProbeRowIds_.shrink_to_fit();
      
      LOG(INFO) << "SortBuffer: reordered " << numInputRows_ 
                << " row references for late materialization";
    }

  } else {
    // Spill the remaining in-memory state to disk if spilling has been
    // triggered on this sort buffer. This is to simplify query OOM prevention
    // when producing output as we don't support to spill during that stage as
    // for now.
    spill();

    finishSpill();
  }

  // Releases the unused memory reservation after procesing input.
  pool_->release();
}

RowVectorPtr SortBuffer::getOutput(uint32_t maxOutputRows) {
  BOLT_CHECK(noMoreInput_);

  if (numOutputRows_ == numInputRows_) {
    return nullptr;
  }

  prepareOutput(maxOutputRows);
  // bool oldNonReclaimableSection = *nonReclaimableSection_;
  // auto guard = folly::makeGuard([this, oldNonReclaimableSection]() {
  // *nonReclaimableSection_ = oldNonReclaimableSection; });
  // *nonReclaimableSection_ = true;
  MicrosecondTimer timer(&sortOutputTimeUs_);
  if (lateMaterializationEnabled_) {
    getOutputLateMaterialization();
  } else if (spiller_ != nullptr) {
    getOutputWithSpill();
  } else {
    getOutputWithoutSpill();
  }
  return output_;
}

void SortBuffer::spill() {
  BOLT_CHECK_NOT_NULL(
      spillConfig_, "spill config is null when SortBuffer spill is called");

  // Check if sort buffer is empty or not, and skip spill if it is empty.
  if (data_->numRows() == 0) {
    return;
  }
  updateEstimatedOutputRowSize();

  if (operatorCtx_) {
    auto* spillConf = const_cast<common::SpillConfig*>(spillConfig_);
    operatorCtx_->adjustSpillCompressionKind(spillConf);
  }

  if (sortedRows_.empty()) {
    spillInput();
  } else {
    spillOutput();
  }
}

std::optional<uint64_t> SortBuffer::estimateOutputRowSize() const {
  return estimatedOutputRowSize_;
}

void SortBuffer::ensureInputFits(const VectorPtr& input) {
  // Check if spilling is enabled or not.
  if (spillConfig_ == nullptr) {
    return;
  }

  const int64_t numRows = data_->numRows();
  if (numRows == 0) {
    // 'data_' is empty. Nothing to spill.
    return;
  }

  auto [freeRows, outOfLineFreeBytes] = data_->freeSpace();
  const auto outOfLineBytes =
      data_->stringAllocator().retainedSize() - outOfLineFreeBytes;
  const int64_t flatInputBytes = input->usedSize();

  // Test-only spill path.
  if (numRows > 0 && testingTriggerSpill()) {
    spill();
    return;
  }

  // If current memory usage exceeds spilling threshold, trigger spilling.
  const auto currentMemoryUsage = pool_->currentBytes();
  if (spillMemoryThreshold_ != 0 &&
      currentMemoryUsage > spillMemoryThreshold_) {
    spill();
    return;
  }

  const auto minReservationBytes =
      currentMemoryUsage * spillConfig_->minSpillableReservationPct / 100;
  const auto availableReservationBytes = pool_->availableReservation();
  const int64_t estimatedIncrementalBytes =
      data_->sizeIncrement(input->size(), outOfLineBytes ? flatInputBytes : 0);
  if (availableReservationBytes > minReservationBytes) {
    // If we have enough free rows for input rows and enough variable length
    // free space for the vector's flat size, no need for spilling.
    if (freeRows > input->size() &&
        (outOfLineBytes == 0 || outOfLineFreeBytes >= flatInputBytes)) {
      return;
    }

    // If the current available reservation in memory pool is 2X the
    // estimatedIncrementalBytes, no need to spill.
    if (availableReservationBytes > 2 * estimatedIncrementalBytes) {
      return;
    }
  }

  // Try reserving targetIncrementBytes more in memory pool, if succeed, no
  // need to spill.
  const auto targetIncrementBytes = std::max<int64_t>(
      estimatedIncrementalBytes * 2,
      currentMemoryUsage * spillConfig_->spillableReservationGrowthPct / 100);
  {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (pool_->maybeReserve(targetIncrementBytes)) {
      return;
    }
  }
  LOG(WARNING) << "Failed to reserve " << succinctBytes(targetIncrementBytes)
               << " for memory pool " << pool()->name()
               << ", usage: " << succinctBytes(pool()->currentBytes())
               << ", reservation: " << succinctBytes(pool()->reservedBytes());
}

void SortBuffer::updateEstimatedOutputRowSize() {
  const auto optionalRowSize = hybridSortEnabled_
      ? hybridData_->estimateRowSize()
      : data_->estimateRowSize();
  if (!optionalRowSize.has_value() || optionalRowSize.value() == 0) {
    return;
  }

  const auto rowSize = optionalRowSize.value();
  if (!estimatedOutputRowSize_.has_value()) {
    estimatedOutputRowSize_ = rowSize;
  } else if (rowSize > estimatedOutputRowSize_.value()) {
    estimatedOutputRowSize_ = rowSize;
  }
}

void SortBuffer::spillInput() {
  if (spiller_ == nullptr) {
    BOLT_CHECK(!noMoreInput_);
    spiller_ = std::make_unique<Spiller>(
        Spiller::Type::kOrderByInput,
        data_.get(),
        spillerStoreType_,
        data_->keyTypes().size(),
        sortCompareFlags_,
        spillConfig_);
    spiller_->setSpillConfig(spillConfig_);
    if (hybridSortEnabled_ && hybridData_ != nullptr) {
      spiller_->setHybridMode(true, hybridData_.get());
    }

    if (sorter_.getSortAlgo() != SortAlgo::kAuto) {
      spiller_->setSortAlgo(sorter_.getSortAlgo());
    }
  }
  // Coalesce batches BEFORE every spill, not just the first one.
  // After each spill, hybridData_->clear() is called which clears
  // owningInputs_. New data added via addInput() needs to be coalesced before
  // the next spill.
  if (hybridSortEnabled_ && hybridData_ != nullptr) {
    hybridData_->coalesceBatches();
  }
  spiller_->spill();
  LOG(INFO) << (operatorCtx_ ? operatorCtx_->toString() : "SortBuffer")
            << " spill row container, data size: "
            << spiller_->container()->usedBytes()
            << ", num rows: " << spiller_->container()->numRows()
            << ", spill file number: " << spiller_->state().numFinishedFiles(0);
  if (hybridSortEnabled_ && hybridData_ != nullptr) {
    hybridData_->clear();
  } else {
    data_->clear();
  }
}

void SortBuffer::spillOutput() {
  if (spiller_ != nullptr) {
    // Already spilled.
    return;
  }
  if (numOutputRows_ == sortedRows_.size()) {
    // All the output has been produced.
    return;
  }

  spiller_ = std::make_unique<Spiller>(
      Spiller::Type::kOrderByOutput,
      data_.get(),
      spillerStoreType_,
      spillConfig_);
  spiller_->setSpillConfig(spillConfig_);
  if (hybridSortEnabled_ && hybridData_ != nullptr) {
    spiller_->setHybridMode(true, hybridData_.get());
  }

  auto spillRows = std::vector<char*>(
      sortedRows_.begin() + numOutputRows_, sortedRows_.end());
  spiller_->spill(spillRows);
  LOG(INFO) << (operatorCtx_ ? operatorCtx_->toString() : "SortBuffer")
            << " spill output, data size: "
            << spiller_->container()->usedBytes()
            << ", num rows: " << spiller_->container()->numRows()
            << ", spill file number: " << spiller_->state().numFinishedFiles(0);
  if (hybridSortEnabled_ && hybridData_ != nullptr) {
    hybridData_->clear();
  } else {
    data_->clear();
  }
  sortedRows_.clear();
  // Finish right after spilling as the output spiller only spills at most
  // once.
  finishSpill();
}

void SortBuffer::prepareOutput(uint32_t maxOutputRows) {
  BOLT_CHECK_GT(maxOutputRows, 0);
  BOLT_CHECK_GT(numInputRows_, numOutputRows_);

  const size_t batchSize =
      std::min<size_t>(numInputRows_ - numOutputRows_, maxOutputRows);

  if (output_ != nullptr) {
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, batchSize);
    output_ = std::static_pointer_cast<RowVector>(output);
  } else {
    output_ = std::static_pointer_cast<RowVector>(
        BaseVector::create(input_, batchSize, pool_));
  }

  for (auto& child : output_->children()) {
    child->resize(batchSize);
  }

  if (spiller_ != nullptr) {
    spillSources_.resize(maxOutputRows);
    spillSourceRows_.resize(maxOutputRows);
  }

  BOLT_CHECK_GT(output_->size(), 0);
  BOLT_DCHECK_LE(output_->size(), maxOutputRows);
  BOLT_CHECK_LE(output_->size() + numOutputRows_, numInputRows_);
}

void SortBuffer::getOutputWithoutSpill() {
  BOLT_DCHECK_EQ(numInputRows_, sortedRows_.size());
  if (hybridSortEnabled_) {
    std::vector<HybridRowId> outputRowIds;
    outputRowIds.resize(output_->size());
    hybridData_->getRowIds(
        sortedRows_.data() + numOutputRows_, output_->size(), outputRowIds);
    for (const auto& columnProjection : columnMap_) {
      hybridData_->extractColumn(
          sortedRows_.data() + numOutputRows_,
          output_->size(),
          columnProjection.inputChannel,
          output_->childAt(columnProjection.outputChannel),
          outputRowIds);
    }
  } else {
    for (const auto& columnProjection : columnMap_) {
      data_->extractColumn(
          sortedRows_.data() + numOutputRows_,
          output_->size(),
          columnProjection.inputChannel,
          output_->childAt(columnProjection.outputChannel));
    }
  }
  numOutputRows_ += output_->size();
}

void SortBuffer::getOutputWithSpill() {
  BOLT_DCHECK_EQ(sortedRows_.size(), 0);
  if (spillMerger_) {
    BOLT_CHECK_NOT_NULL(spillMerger_);

    int32_t outputRow = 0;
    int32_t outputSize = 0;
    bool isEndOfBatch = false;
    while (outputRow + outputSize < output_->size()) {
      SpillMergeStream* stream = spillMerger_->next();
      BOLT_CHECK_NOT_NULL(stream);

      spillSources_[outputSize] = &stream->current();
      spillSourceRows_[outputSize] = stream->currentIndex(&isEndOfBatch);
      ++outputSize;
      if (FOLLY_UNLIKELY(isEndOfBatch)) {
        // The stream is at end of input batch. Need to copy out the rows before
        // fetching next batch in 'pop'.
        gatherCopy(
            output_.get(),
            outputRow,
            outputSize,
            spillSources_,
            spillSourceRows_,
            columnMap_);
        outputRow += outputSize;
        outputSize = 0;
      }
      // Advance the stream.
      stream->pop();
    }
    BOLT_CHECK_EQ(outputRow + outputSize, output_->size());

    if (FOLLY_LIKELY(outputSize != 0)) {
      gatherCopy(
          output_.get(),
          outputRow,
          outputSize,
          spillSources_,
          spillSourceRows_,
          columnMap_);
    }

    numOutputRows_ += output_->size();
  } else {
    BOLT_CHECK_NOT_NULL(rowBasedSpillMerger_);

    int32_t outputRow = 0;
    bool isEndOfBatch = false;
    std::vector<char*> rows;
    while (outputRow + rows.size() < output_->size()) {
      RowBasedSpillMergeStream* stream = rowBasedSpillMerger_->next();
      BOLT_CHECK_NOT_NULL(stream);

      const auto& currentBatch = stream->current();
      auto index = stream->currentIndex(&isEndOfBatch);
      rows.push_back(currentBatch[index]);
      if (FOLLY_UNLIKELY(isEndOfBatch)) {
        // The stream is at end of input batch. Need to copy out the rows before
        // fetching next batch in 'pop'.
        rowToColumnVector(
            rows.data(),
            rows.size(),
            data_->columns(),
            outputRow,
            output_,
            columnMap_);
        outputRow += rows.size();
        rows.clear();
      }
      // Advance the stream.
      stream->pop();
    }
    BOLT_CHECK_EQ(outputRow + rows.size(), output_->size());

    if (FOLLY_LIKELY(!rows.empty())) {
      rowToColumnVector(
          rows.data(),
          rows.size(),
          data_->columns(),
          outputRow,
          output_,
          columnMap_);
    }

    numOutputRows_ += output_->size();
  }
}

void SortBuffer::getOutputLateMaterialization() {
  // Materialize full rows from upstream containers using sorted row references.
  // The sortedBuildRowPtrs_ point into the final HashProbe's hash table.
  // We need to use HybridContainer::extractColumnsFromUpstream to traverse
  // the N-way chain and extract columns from their actual sources.
  const auto batchSize = output_->size();
  const auto startRow = numOutputRows_;
  
  // Get the row pointers for this batch - these point into the final hash table
  std::vector<char*> batchBuildPtrs(
      sortedBuildRowPtrs_.begin() + startRow,
      sortedBuildRowPtrs_.begin() + startRow + batchSize);
  std::vector<uint64_t> batchProbeIds(
      sortedProbeRowIds_.begin() + startRow,
      sortedProbeRowIds_.begin() + startRow + batchSize);
  
  // Pre-allocate output columns
  for (size_t i = 0; i < output_->childrenSize(); ++i) {
    if (!output_->childAt(i)) {
      output_->childAt(i) = BaseVector::create(
          input_->childAt(i), batchSize, pool_);
    }
    output_->childAt(i)->resize(batchSize);
  }
  
  // Use extractColumnsFromUpstream to traverse the N-way chain and extract all columns.
  // Build channel list for extraction - all output channels
  std::vector<column_index_t> channels;
  for (size_t i = 0; i < output_->childrenSize(); ++i) {
    channels.push_back(static_cast<column_index_t>(i));
  }
  
  // Get the primary upstream container (the hash table we have pointers into)
  HybridContainer* primaryHybrid = nullptr;
  if (primaryUpstreamHashTable_) {
    primaryHybrid = primaryUpstreamHashTable_->hybridData();
  }
  
  // Get the first probe payload container (if any)
  ProbePayloadContainer* probeContainer = nullptr;
  if (!upstreamProbePayloads_.empty()) {
    probeContainer = upstreamProbePayloads_.begin()->second.get();
  }
  
  HybridContainer::extractColumnsFromUpstream(
      channels,
      batchBuildPtrs,
      batchProbeIds,
      primaryHybrid,
      probeContainer,
      columnSourceMap_,
      output_);
  
  numOutputRows_ += batchSize;
}

void SortBuffer::finishSpill() {
  BOLT_CHECK_NULL(spillMerger_);
  auto spillPartition = spiller_->finishSpill();
  if (spillConfig_->rowBasedSpillMode == common::RowBasedSpillMode::DISABLE) {
    spillMerger_ = spillPartition.createOrderedReader(
        pool(), spillConfig_->spillUringEnabled);
  } else {
    rowBasedSpillMerger_ = spillPartition.createRowBasedOrderedReader(
        pool(),
        data_.get(),
        spillConfig_->getJITenabledForSpill(),
        spillConfig_->spillUringEnabled);
  }
}

std::vector<size_t> SortBuffer::getOriginalRowIndices() const {
  std::vector<size_t> indices;
  indices.reserve(sortedRows_.size());
  
  if (hybridSortEnabled_ && data_) {
    // Extract original row index from stored rowId (low 56 bits)
    for (char* row : sortedRows_) {
      uint64_t storedId = data_->getSingleRowId(row);
      size_t originalIndex = storedId & ((1ULL << 56) - 1);
      indices.push_back(originalIndex);
    }
  } else {
    // Non-hybrid mode: rows are stored in order, so we need to find original index
    // by comparing pointers. This is less efficient but provides fallback.
    // For now, just return sequential indices (no reordering supported).
    for (size_t i = 0; i < sortedRows_.size(); ++i) {
      indices.push_back(i);
    }
  }
  
  return indices;
}

} // namespace bytedance::bolt::exec

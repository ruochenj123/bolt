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

#pragma once

#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/exec/Operator.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/SortBufferBase.h"
#include "bolt/exec/Spiller.h"

namespace bytedance::bolt::exec {

class BaseHashTable;
class HashProbe;
struct JoinAggReuseSpec;

/// OrderBy operator implementation: OrderBy stores all its inputs in a
/// RowContainer as the inputs are added. Until all inputs are available,
/// it blocks the pipeline. Once all inputs are available, it sorts pointers
/// to the rows using the RowContainer's compare() function. And finally it
/// constructs and returns the sorted output RowVector using the data in the
/// RowContainer.
///
/// Late Materialization Mode:
/// When configured as the materialization point for N-way late-m joins,
/// the underlying SortBuffer receives sort key columns (materialized) plus
/// row references from upstream. It sorts by keys while tracking the row
/// references, then materializes full rows after sorting is complete.
/// The late-m logic is handled inside SortBuffer for consistency.
///
/// Limitations:
/// * It memcopies twice: 1) input to RowContainer and 2) RowContainer to
/// output.
class OrderBy : public Operator {
 public:
  OrderBy(
      int32_t operatorId,
      DriverCtx* FOLLY_NONNULL driverCtx,
      const std::shared_ptr<const core::OrderByNode>& orderByNode);

  bool needsInput() const override {
    return !finished_;
  }

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* FOLLY_NULLABLE /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

  bool canReclaim() const override {
    // sortBuffer_ may be released
    return Operator::canReclaim() && sortBuffer_ && sortBuffer_->canReclaim();
  }

  void reclaim(uint64_t targetBytes, memory::MemoryReclaimer::Stats& stats)
      override;

  void close() override;

 private:
  // Invoked to record the spilling stats in operator stats after processing all
  // the inputs.
  void recordSpillStats();
  void recordSpillReadStats();
  void recordSortStats();

  // Join-sort reuse (JoinAggReuse.h): replaces the deferred build columns of
  // 'input' by the references to the build rows of the join below, which that
  // join published for these rows.
  RowVectorPtr replaceDeferredColumns(const RowVectorPtr& input);

  // Rebuilds an output batch of 'outputType_' from a sorted batch of
  // 'sortInputType_', reading the deferred columns from the join's table.
  RowVectorPtr restoreDeferredColumns(const RowVectorPtr& sorted);

  std::unique_ptr<SortBufferBase> sortBuffer_;
  common::SpillStats recordedSpillStats_;
  bool finished_ = false;
  vector_size_t maxOutputRows_;

  // Join-sort reuse: the spec; the type the sort buffer stores (outputType_
  // without the deferred columns, plus the reference); for each of its
  // channels but the last, the output channel; and the deferred columns as
  // join table channel -> output channel.
  const JoinAggReuseSpec* deferral_{nullptr};
  RowTypePtr sortInputType_;
  std::vector<column_index_t> keptChannels_;
  std::vector<IdentityProjection> derefProjections_;
  HashProbe* deferralSource_{nullptr};
  std::shared_ptr<BaseHashTable> deferralTable_;
  std::vector<char*> deferralRows_;
};
} // namespace bytedance::bolt::exec

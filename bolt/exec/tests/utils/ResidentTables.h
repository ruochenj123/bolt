/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * Resident ("in-memory") table sources for benchmarks, ported from the velox
 * tree (velox/exec/tests/utils/ResidentTables.h).
 *
 * Each TableScan is replaced, at driver-build time, by a ResidentValues source
 * operator that emits a process-resident list of pre-decoded RowVector
 * batches, partitioned across the pipeline's drivers (driver d of N emits
 * batches d, d+N, ...). The plan is untouched (same node ids), so the query
 * runs as it would on a scan whose decode cost is zero.
 *
 * Unlike the velox version, ResidentValues applies the scan's own filters: the
 * subfield filters and the remaining filter of the HiveTableHandle, and the
 * dynamic filters pushed down by hash joins. Filtering therefore stays in the
 * timed region, plans need no filtersAsNode rewrite (the TPC-DS builder has
 * none), and a hash probe may still replace itself with a pushed filter.
 */
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "bolt/dwio/common/Options.h"
#include "bolt/exec/Driver.h"
#include "bolt/exec/Operator.h"
#include "bolt/expression/Expr.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::exec::test {

/// Process-wide registry: plan node id (of the TableScanNode) -> batches.
class ResidentTableRegistry {
 public:
  static ResidentTableRegistry& instance();

  void set(
      const core::PlanNodeId& scanNodeId,
      std::shared_ptr<const std::vector<RowVectorPtr>> batches);
  std::shared_ptr<const std::vector<RowVectorPtr>> get(
      const core::PlanNodeId& scanNodeId) const;
  void clear();

 private:
  mutable std::mutex mutex_;
  std::unordered_map<
      core::PlanNodeId,
      std::shared_ptr<const std::vector<RowVectorPtr>>>
      tables_;
};

class ResidentValues : public SourceOperator {
 public:
  ResidentValues(
      int32_t operatorId,
      DriverCtx* driverCtx,
      std::shared_ptr<const core::TableScanNode> scanNode,
      std::shared_ptr<const std::vector<RowVectorPtr>> batches,
      size_t numDrivers);

  void initialize() override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return batches_ == nullptr || next_ >= batches_->size();
  }

  bool canAddDynamicFilter() const override {
    return true;
  }

  void addDynamicFilter(
      const core::PlanNodeId& producer,
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) override;

  // Drop our reference as soon as the driver closes: tasks are torn down
  // asynchronously, possibly after the pool owning the batches is gone.
  void close() override;

 private:
  struct ColumnFilter {
    column_index_t channel;
    // Static filters point into the scan node's table handle (kept alive by
    // 'scanNode_'); dynamic ones are owned here.
    const common::Filter* filter;
    std::shared_ptr<common::Filter> owned;
  };

  // Applies all filters to 'batch'. Returns the number of passing rows; their
  // indices are in 'passing_'.
  vector_size_t applyFilters(const RowVectorPtr& batch);

  // Rows of 'batch' listed in 'passing_' as a new batch of flat vectors.
  RowVectorPtr compact(const RowVectorPtr& batch, vector_size_t numPassing);

  const std::shared_ptr<const core::TableScanNode> scanNode_;
  std::shared_ptr<const std::vector<RowVectorPtr>> batches_;
  size_t next_;
  const size_t stride_;

  std::vector<ColumnFilter> filters_;
  // Remaining filter, compiled over 'filterInputType_' (file column names).
  std::unique_ptr<ExprSet> remainingFilter_;
  RowTypePtr filterInputType_;

  std::vector<vector_size_t> passing_;
  int64_t rowsIn_{0};
  int64_t rowsOut_{0};
};

/// Registers the DriverAdapter that performs the TableScan -> ResidentValues
/// substitution for every scan node id present in the registry. Idempotent.
void registerResidentTablesAdapter();
void unregisterResidentTablesAdapter();

/// Loads the tables a plan scans into memory once per process and registers
/// them for the plan's scan nodes. The batches are produced by the real scan
/// (without filters) under 'queryConfigs', so their sizes match a regular run,
/// then deep-copied into a pool owned by the loader.
class ResidentTableLoader {
 public:
  ResidentTableLoader(
      int32_t numThreads,
      int32_t splitsPerFile,
      std::unordered_map<std::string, std::string> queryConfigs);

  ~ResidentTableLoader();

  /// Serves every TableScan of 'plan' from memory: loads missing tables,
  /// drops cached tables 'plan' does not use, registers the batches for the
  /// scan node ids and clears 'dataFiles' (no splits are needed).
  void makeResident(
      const core::PlanNodePtr& plan,
      std::unordered_map<core::PlanNodeId, std::vector<std::string>>&
          dataFiles,
      dwio::common::FileFormat format);

 private:
  std::shared_ptr<const std::vector<RowVectorPtr>> load(
      const core::TableScanNode& scan,
      const std::vector<std::string>& files,
      dwio::common::FileFormat format);

  const int32_t numThreads_;
  const int32_t splitsPerFile_;
  const std::unordered_map<std::string, std::string> queryConfigs_;
  std::shared_ptr<memory::MemoryPool> pool_;
  // (table|file column:type|...) -> batches.
  std::unordered_map<
      std::string,
      std::shared_ptr<const std::vector<RowVectorPtr>>>
      cache_;
};

} // namespace bytedance::bolt::exec::test

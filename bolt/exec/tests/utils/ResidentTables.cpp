/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/exec/tests/utils/ResidentTables.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <numeric>
#include <thread>
#include <unordered_set>

#include <glog/logging.h>

#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/exec/TableScan.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/QueryAssertions.h"
#include "bolt/vector/DecodedVector.h"

namespace bytedance::bolt::exec::test {

namespace {
constexpr const char* kAdapterLabel = "ResidentTables";
bool registered = false;

// Keeps the rows[0, numRows) of 'decoded' that pass 'test' (nulls pass iff
// the filter accepts nulls), compacting them to the front of 'rows'.
template <typename T, typename Test>
vector_size_t filterRows(
    const DecodedVector& decoded,
    const common::Filter& filter,
    vector_size_t* rows,
    vector_size_t numRows,
    Test test) {
  vector_size_t numPassing = 0;
  const bool mayHaveNulls = decoded.mayHaveNulls();
  const bool nullAllowed = filter.testNull();
  for (vector_size_t i = 0; i < numRows; ++i) {
    const auto row = rows[i];
    const bool pass = mayHaveNulls && decoded.isNullAt(row)
        ? nullAllowed
        : test(decoded.valueAt<T>(row));
    rows[numPassing] = row;
    numPassing += pass;
  }
  return numPassing;
}

// Same semantics as a subfield filter in the Hive reader: the filter is tested
// on the physical value (DATE as INTEGER, short DECIMAL as BIGINT).
vector_size_t filterColumn(
    const common::Filter& filter,
    const BaseVector& column,
    vector_size_t* rows,
    vector_size_t numRows) {
  DecodedVector decoded(column);
  switch (column.typeKind()) {
    case TypeKind::BOOLEAN:
      return filterRows<bool>(decoded, filter, rows, numRows, [&](bool v) {
        return filter.testBool(v);
      });
    case TypeKind::TINYINT:
      return filterRows<int8_t>(decoded, filter, rows, numRows, [&](int8_t v) {
        return filter.testInt64(v);
      });
    case TypeKind::SMALLINT:
      return filterRows<int16_t>(
          decoded, filter, rows, numRows, [&](int16_t v) {
            return filter.testInt64(v);
          });
    case TypeKind::INTEGER:
      return filterRows<int32_t>(
          decoded, filter, rows, numRows, [&](int32_t v) {
            return filter.testInt64(v);
          });
    case TypeKind::BIGINT:
      return filterRows<int64_t>(
          decoded, filter, rows, numRows, [&](int64_t v) {
            return filter.testInt64(v);
          });
    case TypeKind::HUGEINT:
      return filterRows<int128_t>(
          decoded, filter, rows, numRows, [&](int128_t v) {
            return filter.testInt128(v);
          });
    case TypeKind::REAL:
      return filterRows<float>(decoded, filter, rows, numRows, [&](float v) {
        return filter.testFloat(v);
      });
    case TypeKind::DOUBLE:
      return filterRows<double>(decoded, filter, rows, numRows, [&](double v) {
        return filter.testDouble(v);
      });
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return filterRows<StringView>(
          decoded, filter, rows, numRows, [&](StringView v) {
            return filter.testStringView(v);
          });
    case TypeKind::TIMESTAMP:
      return filterRows<Timestamp>(
          decoded, filter, rows, numRows, [&](const Timestamp& v) {
            return filter.testTimestamp(v);
          });
    default:
      BOLT_UNSUPPORTED(
          "Resident scan: filter on {} is not supported",
          column.type()->toString());
  }
}

std::shared_ptr<const connector::hive::HiveTableHandle> hiveHandle(
    const core::TableScanNode& scan) {
  auto hive = std::dynamic_pointer_cast<const connector::hive::HiveTableHandle>(
      scan.tableHandle());
  BOLT_CHECK_NOT_NULL(hive, "Resident scan needs a Hive table handle");
  return hive;
}

// Name of the file column behind output column 'name' of 'scan'.
const std::string& fileColumnName(
    const core::TableScanNode& scan,
    const std::string& name) {
  auto it = scan.assignments().find(name);
  BOLT_CHECK(it != scan.assignments().end(), "No assignment for {}", name);
  auto column =
      std::dynamic_pointer_cast<const connector::hive::HiveColumnHandle>(
          it->second);
  BOLT_CHECK_NOT_NULL(column);
  return column->name();
}

void collectScans(
    const core::PlanNodePtr& node,
    std::vector<std::shared_ptr<const core::TableScanNode>>& out) {
  if (auto scan = std::dynamic_pointer_cast<const core::TableScanNode>(node)) {
    out.push_back(scan);
  }
  for (const auto& source : node->sources()) {
    collectScans(source, out);
  }
}

// Batches depend only on the table and the (file column, type) list read.
std::string cacheKey(const core::TableScanNode& scan) {
  std::string key = hiveHandle(scan)->tableName();
  const auto& type = scan.outputType();
  for (column_index_t i = 0; i < type->size(); ++i) {
    key += "|" + fileColumnName(scan, type->nameOf(i)) + ":" +
        type->childAt(i)->toString();
  }
  return key;
}
} // namespace

ResidentTableRegistry& ResidentTableRegistry::instance() {
  // Never destroyed: a loader may still clear it during static destruction.
  static auto* registry = new ResidentTableRegistry();
  return *registry;
}

void ResidentTableRegistry::set(
    const core::PlanNodeId& scanNodeId,
    std::shared_ptr<const std::vector<RowVectorPtr>> batches) {
  std::lock_guard<std::mutex> l(mutex_);
  tables_[scanNodeId] = std::move(batches);
}

std::shared_ptr<const std::vector<RowVectorPtr>> ResidentTableRegistry::get(
    const core::PlanNodeId& scanNodeId) const {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = tables_.find(scanNodeId);
  return it == tables_.end() ? nullptr : it->second;
}

void ResidentTableRegistry::clear() {
  std::lock_guard<std::mutex> l(mutex_);
  tables_.clear();
}

ResidentValues::ResidentValues(
    int32_t operatorId,
    DriverCtx* driverCtx,
    std::shared_ptr<const core::TableScanNode> scanNode,
    std::shared_ptr<const std::vector<RowVectorPtr>> batches,
    size_t numDrivers)
    : SourceOperator(
          driverCtx,
          scanNode->outputType(),
          operatorId,
          scanNode->id(),
          "ResidentValues"),
      scanNode_(std::move(scanNode)),
      batches_(std::move(batches)),
      next_(driverCtx->driverId),
      stride_(numDrivers) {
  auto hive = hiveHandle(*scanNode_);
  // Filters name file columns; map them to output channels.
  std::unordered_map<std::string, column_index_t> fileChannels;
  std::vector<std::string> fileNames;
  for (column_index_t i = 0; i < outputType_->size(); ++i) {
    fileNames.push_back(fileColumnName(*scanNode_, outputType_->nameOf(i)));
    fileChannels[fileNames.back()] = i;
  }
  for (const auto& [subfield, filter] : hive->subfieldFilters()) {
    BOLT_CHECK_EQ(
        subfield.path().size(),
        1,
        "Resident scan: nested subfield filter {}",
        subfield.toString());
    auto it = fileChannels.find(subfield.toString());
    BOLT_CHECK(
        it != fileChannels.end(),
        "Resident scan: filter on {}, which the scan does not output",
        subfield.toString());
    filters_.push_back({it->second, filter.get(), nullptr});
  }
  if (hive->remainingFilter() != nullptr) {
    std::vector<TypePtr> types = outputType_->children();
    filterInputType_ = ROW(std::move(fileNames), std::move(types));
  }
}

void ResidentValues::initialize() {
  Operator::initialize();
  // Compiled here, not in the constructor: the task forbids memory
  // allocations while drivers are created.
  auto hive = hiveHandle(*scanNode_);
  if (hive->remainingFilter() != nullptr) {
    remainingFilter_ = std::make_unique<ExprSet>(
        std::vector<core::TypedExprPtr>{hive->remainingFilter()},
        operatorCtx_->execCtx());
  }
}

void ResidentValues::addDynamicFilter(
    const core::PlanNodeId& /*producer*/,
    column_index_t outputChannel,
    const std::shared_ptr<common::Filter>& filter) {
  filters_.push_back({outputChannel, filter.get(), filter});
}

vector_size_t ResidentValues::applyFilters(const RowVectorPtr& batch) {
  const auto numRows = batch->size();
  if (filters_.empty() && remainingFilter_ == nullptr) {
    return numRows;
  }
  passing_.resize(numRows);
  std::iota(passing_.begin(), passing_.end(), 0);
  vector_size_t numPassing = numRows;
  for (const auto& columnFilter : filters_) {
    if (numPassing == 0) {
      return 0;
    }
    numPassing = filterColumn(
        *columnFilter.filter,
        *batch->childAt(columnFilter.channel),
        passing_.data(),
        numPassing);
  }
  if (remainingFilter_ != nullptr && numPassing > 0) {
    SelectivityVector rows(numRows, false);
    for (vector_size_t i = 0; i < numPassing; ++i) {
      rows.setValid(passing_[i], true);
    }
    rows.updateBounds();
    auto input = std::make_shared<RowVector>(
        pool(), filterInputType_, nullptr, numRows, batch->children());
    EvalCtx evalCtx(
        operatorCtx_->execCtx(), remainingFilter_.get(), input.get());
    std::vector<VectorPtr> results(1);
    remainingFilter_->eval(rows, evalCtx, results);
    DecodedVector decoded(*results[0], rows);
    vector_size_t numKept = 0;
    for (vector_size_t i = 0; i < numPassing; ++i) {
      const auto row = passing_[i];
      passing_[numKept] = row;
      numKept += !decoded.isNullAt(row) && decoded.valueAt<bool>(row);
    }
    numPassing = numKept;
  }
  return numPassing;
}

RowVectorPtr ResidentValues::compact(
    const RowVectorPtr& batch,
    vector_size_t numPassing) {
  // One copy range per run of consecutive passing rows.
  std::vector<BaseVector::CopyRange> ranges;
  for (vector_size_t i = 0; i < numPassing;) {
    vector_size_t end = i + 1;
    while (end < numPassing && passing_[end] == passing_[end - 1] + 1) {
      ++end;
    }
    ranges.push_back({passing_[i], i, end - i});
    i = end;
  }
  std::vector<VectorPtr> children(batch->childrenSize());
  for (size_t c = 0; c < children.size(); ++c) {
    const auto& source = batch->childAt(c);
    auto target = BaseVector::create(source->type(), numPassing, pool());
    target->copyRanges(source.get(), ranges);
    children[c] = std::move(target);
  }
  return std::make_shared<RowVector>(
      pool(), outputType_, nullptr, numPassing, std::move(children));
}

RowVectorPtr ResidentValues::getOutput() {
  while (batches_ != nullptr && next_ < batches_->size()) {
    const auto& batch = (*batches_)[next_];
    next_ += stride_;
    rowsIn_ += batch->size();
    const auto numPassing = applyFilters(batch);
    if (numPassing == 0) {
      continue;
    }
    rowsOut_ += numPassing;
    if (numPassing == batch->size()) {
      // Shared, not copied: the registry holds a reference, so no consumer
      // treats the vectors as writable.
      return std::make_shared<RowVector>(
          pool(), outputType_, nullptr, batch->size(), batch->children());
    }
    return compact(batch, numPassing);
  }
  return nullptr;
}

void ResidentValues::close() {
  addRuntimeStat("residentRowsIn", RuntimeCounter(rowsIn_));
  addRuntimeStat("residentRowsOut", RuntimeCounter(rowsOut_));
  SourceOperator::close();
  batches_.reset();
  remainingFilter_.reset();
}

void registerResidentTablesAdapter() {
  if (registered) {
    return;
  }
  registered = true;
  DriverAdapter adapter{
      kAdapterLabel,
      {},
      [](const DriverFactory& factory, Driver& driver) -> bool {
        auto ops = driver.operators();
        for (size_t i = 0; i < ops.size(); ++i) {
          auto* scan = dynamic_cast<TableScan*>(ops[i]);
          if (scan == nullptr) {
            continue;
          }
          auto batches =
              ResidentTableRegistry::instance().get(scan->planNodeId());
          if (batches == nullptr) {
            continue;
          }
          std::shared_ptr<const core::TableScanNode> scanNode;
          for (const auto& node : factory.planNodes) {
            if (node->id() == scan->planNodeId()) {
              scanNode =
                  std::dynamic_pointer_cast<const core::TableScanNode>(node);
              break;
            }
          }
          BOLT_CHECK_NOT_NULL(scanNode);
          std::vector<std::unique_ptr<Operator>> replacement;
          replacement.push_back(std::make_unique<ResidentValues>(
              scan->operatorId(),
              driver.driverCtx(),
              scanNode,
              batches,
              factory.numDrivers));
          factory.replaceOperators(driver, i, i + 1, std::move(replacement));
        }
        // Velox stops at the first adapter that returns true; let the others
        // run after us.
        return false;
      }};
  DriverFactory::adapters.insert(
      DriverFactory::adapters.begin(), std::move(adapter));
}

void unregisterResidentTablesAdapter() {
  auto& adapters = DriverFactory::adapters;
  adapters.erase(
      std::remove_if(
          adapters.begin(),
          adapters.end(),
          [](const DriverAdapter& a) { return a.label == kAdapterLabel; }),
      adapters.end());
  registered = false;
}

ResidentTableLoader::ResidentTableLoader(
    int32_t numThreads,
    int32_t splitsPerFile,
    std::unordered_map<std::string, std::string> queryConfigs)
    : numThreads_(std::max<int32_t>(1, numThreads)),
      splitsPerFile_(std::max<int32_t>(1, splitsPerFile)),
      queryConfigs_(std::move(queryConfigs)),
      pool_(memory::memoryManager()->addLeafPool("resident_tables")) {
  registerResidentTablesAdapter();
}

ResidentTableLoader::~ResidentTableLoader() {
  // The registry must release the batches while 'pool_' is still alive.
  ResidentTableRegistry::instance().clear();
  cache_.clear();
  unregisterResidentTablesAdapter();
}

void ResidentTableLoader::makeResident(
    const core::PlanNodePtr& plan,
    std::unordered_map<core::PlanNodeId, std::vector<std::string>>& dataFiles,
    dwio::common::FileFormat format) {
  std::vector<std::shared_ptr<const core::TableScanNode>> scans;
  collectScans(plan, scans);
  auto& registry = ResidentTableRegistry::instance();
  registry.clear();

  // Keep only this plan's tables in memory.
  std::unordered_set<std::string> keys;
  for (const auto& scan : scans) {
    keys.insert(cacheKey(*scan));
  }
  for (auto it = cache_.begin(); it != cache_.end();) {
    it = keys.count(it->first) ? std::next(it) : cache_.erase(it);
  }

  for (const auto& scan : scans) {
    const auto key = cacheKey(*scan);
    auto it = cache_.find(key);
    if (it == cache_.end()) {
      auto files = dataFiles.find(scan->id());
      BOLT_CHECK(
          files != dataFiles.end(), "No data files for scan {}", scan->id());
      it = cache_.emplace(key, load(*scan, files->second, format)).first;
    }
    registry.set(scan->id(), it->second);
  }
  dataFiles.clear();
}

std::shared_ptr<const std::vector<RowVectorPtr>> ResidentTableLoader::load(
    const core::TableScanNode& scan,
    const std::vector<std::string>& files,
    dwio::common::FileFormat format) {
  const auto start = std::chrono::steady_clock::now();
  auto hive = hiveHandle(scan);

  // The same scan without filters; the query applies them at runtime.
  auto handle = std::make_shared<connector::hive::HiveTableHandle>(
      hive->connectorId(),
      hive->tableName(),
      hive->isFilterPushdownEnabled(),
      common::SubfieldFilters{},
      nullptr,
      hive->dataColumns(),
      hive->tableParameters());
  // Own id space: the adapter matches scans by plan node id, and the preload
  // scan must never collide with a registered query scan.
  core::PlanNodeId scanId;
  auto preload =
      PlanBuilder(std::make_shared<core::PlanNodeIdGenerator>(1000000))
          .tableScan(scan.outputType(), handle, scan.assignments())
          .capturePlanNodeId(scanId)
          // Loads lazy vectors inside the parallel scan drivers, not serially
          // on this thread (round robin moves whole batches).
          .localPartitionRoundRobin()
          .planNode();

  CursorParameters params;
  params.planNode = preload;
  params.maxDrivers = numThreads_;
  params.queryConfigs = queryConfigs_;
  auto cursor = TaskCursor::create(params);
  auto* task = cursor->task().get();
  const auto splitsPerFile = std::max(splitsPerFile_, 4 * numThreads_);
  for (const auto& path : files) {
    for (auto& split : HiveConnectorTestBase::makeHiveConnectorSplits(
             path, splitsPerFile, format)) {
      task->addSplit(scanId, exec::Split(std::move(split)));
    }
  }
  task->noMoreSplits(scanId);

  std::vector<RowVectorPtr> produced;
  while (cursor->moveNext()) {
    const auto& batch = cursor->current();
    std::vector<VectorPtr> children(batch->childrenSize());
    for (size_t c = 0; c < children.size(); ++c) {
      children[c] = BaseVector::loadedVectorShared(batch->childAt(c));
    }
    produced.push_back(std::make_shared<RowVector>(
        batch->pool(),
        batch->type(),
        nullptr,
        batch->size(),
        std::move(children)));
  }
  waitForTaskCompletion(task);

  // Deep flat copy into our pool, in parallel: the task's pools die with the
  // cursor, and BaseVector::copy allocates every buffer in the target pool.
  auto batches = std::make_shared<std::vector<RowVectorPtr>>(produced.size());
  {
    std::atomic<size_t> nextIndex{0};
    std::vector<std::thread> workers;
    for (int32_t t = 0; t < numThreads_; ++t) {
      workers.emplace_back([&]() {
        for (size_t i = nextIndex++; i < produced.size(); i = nextIndex++) {
          const auto& source = produced[i];
          auto copy = std::static_pointer_cast<RowVector>(BaseVector::create(
              source->type(), source->size(), pool_.get()));
          copy->copy(source.get(), 0, 0, source->size());
          (*batches)[i] = std::move(copy);
        }
      });
    }
    for (auto& worker : workers) {
      worker.join();
    }
  }
  produced.clear();

  int64_t rows = 0;
  for (const auto& batch : *batches) {
    rows += batch->size();
  }
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - start)
                      .count();
  std::cerr << "[resident] loaded " << hive->tableName() << " ("
            << scan.outputType()->size() << " cols): " << rows << " rows, "
            << batches->size() << " batches, " << ms << " ms" << std::endl;
  return batches;
}

} // namespace bytedance::bolt::exec::test

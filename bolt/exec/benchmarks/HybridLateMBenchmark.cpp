/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Benchmark for Hybrid vs Late-Materialization performance
 * Uses synthetic TPC-H data via TpchConnector
 */

#include <chrono>
#include <iostream>
#include <iomanip>

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "bolt/common/memory/Memory.h"
#include "bolt/connectors/tpch/TpchConnector.h"
#include "bolt/connectors/tpch/TpchConnectorSplit.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/parse/TypeResolver.h"

DEFINE_double(scale_factor, 1.0, "TPC-H scale factor");
DEFINE_int32(num_drivers, 4, "Number of drivers for HashBuild");
DEFINE_int32(preferred_batch_rows, 1024, "Preferred output batch rows");
DEFINE_int32(max_batch_rows, 10000, "Max output batch rows");
DEFINE_bool(hybrid_join, true, "Enable hybrid join");
DEFINE_bool(hybrid_sort, true, "Enable hybrid sort");
DEFINE_bool(late_m, false, "Enable late materialization");
DEFINE_bool(pointer_reuse, false, "Enable pointer reuse for N-way late-m");
DEFINE_int32(query, 23, "Query to run (23, 24, 27, or 28)");

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::connector::tpch;

namespace {
// Use kBoltTpchConnectorId to get prefixed column names (c_*, o_*, l_*)
const std::string kTpchConnectorId = kBoltTpchConnectorId;

void registerConnectors() {
  auto tpchConnector =
      connector::getConnectorFactory(TpchConnectorFactory::kTpchConnectorName)
          ->newConnector(
              kTpchConnectorId,
              std::make_shared<config::ConfigBase>(
                  std::unordered_map<std::string, std::string>()));
  connector::registerConnector(tpchConnector);
}

void unregisterConnectors() {
  connector::unregisterConnector(kTpchConnectorId);
}

// Q23-style: HashJoin->OrderBy with both build and probe columns
void runQ23(double scaleFactor, int numDrivers, int preferredBatchRows, int maxBatchRows,
            bool hybridJoin, bool hybridSort, bool lateM) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemScanId, ordersScanId;

  // Build side: orders table
  auto orders = PlanBuilder(planNodeIdGenerator)
                    .tpchTableScan(
                        tpch::Table::TBL_ORDERS,
                        {"o_orderkey", "o_orderdate", "o_totalprice"},
                        scaleFactor,
                        kTpchConnectorId)
                    .capturePlanNodeId(ordersScanId)
                    .planNode();

  // Probe side: lineitem table, then join and sort
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tpchTableScan(
              tpch::Table::TBL_LINEITEM,
              {"l_orderkey", "l_quantity", "l_discount"},
              scaleFactor,
              kTpchConnectorId)
          .capturePlanNodeId(lineitemScanId)
          .hashJoin(
              {"l_orderkey"},
              {"o_orderkey"},
              orders,
              "",
              {"o_orderkey", "o_orderdate", "o_totalprice", "l_quantity", "l_discount"})
          .orderBy({"o_orderkey"}, false)
          .planNode();

  std::unordered_map<std::string, std::string> queryConfigs;
  queryConfigs[core::QueryConfig::kHybridJoinEnabled] = hybridJoin ? "true" : "false";
  queryConfigs[core::QueryConfig::kHybridSortEnabled] = hybridSort ? "true" : "false";
  queryConfigs[core::QueryConfig::kLateMaterializationEnabled] = lateM ? "true" : "false";
  queryConfigs[core::QueryConfig::kHybridJoinPointerReuseEnabled] = FLAGS_pointer_reuse ? "true" : "false";
  queryConfigs[core::QueryConfig::kPreferredOutputBatchRows] = std::to_string(preferredBatchRows);
  queryConfigs[core::QueryConfig::kMaxOutputBatchRows] = std::to_string(maxBatchRows);

  CursorParameters params;
  params.planNode = plan;
  params.maxDrivers = numDrivers;
  params.queryConfigs = queryConfigs;

  auto startTime = std::chrono::steady_clock::now();
  
  auto cursor = TaskCursor::create(params);
  cursor->start();
  
  // Add splits for both tables
  auto task = cursor->task();
  task->addSplit(lineitemScanId, Split(std::make_shared<TpchConnectorSplit>(kTpchConnectorId)));
  task->noMoreSplits(lineitemScanId);
  task->addSplit(ordersScanId, Split(std::make_shared<TpchConnectorSplit>(kTpchConnectorId)));
  task->noMoreSplits(ordersScanId);

  int64_t totalRows = 0;
  while (cursor->moveNext()) {
    totalRows += cursor->current()->size();
  }
  
  task->taskCompletionFuture().wait();
  
  auto endTime = std::chrono::steady_clock::now();
  auto durationMs = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();

  std::cout << "Q23 completed: " << totalRows << " rows in " << durationMs << " ms" << std::endl;
}

// Q24-style: HashJoin->OrderBy with build-only columns
void runQ24(double scaleFactor, int numDrivers, int preferredBatchRows, int maxBatchRows,
            bool hybridJoin, bool hybridSort, bool lateM) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemScanId, ordersScanId;

  // Build side: orders table
  auto orders = PlanBuilder(planNodeIdGenerator)
                    .tpchTableScan(
                        tpch::Table::TBL_ORDERS,
                        {"o_orderkey", "o_orderdate", "o_totalprice"},
                        scaleFactor,
                        kTpchConnectorId)
                    .capturePlanNodeId(ordersScanId)
                    .planNode();

  // Probe side: lineitem table (only join key), then join and sort
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tpchTableScan(
              tpch::Table::TBL_LINEITEM,
              {"l_orderkey"},
              scaleFactor,
              kTpchConnectorId)
          .capturePlanNodeId(lineitemScanId)
          .hashJoin(
              {"l_orderkey"},
              {"o_orderkey"},
              orders,
              "",
              {"o_orderkey", "o_orderdate", "o_totalprice"})  // Build-only columns
          .orderBy({"o_orderkey"}, false)
          .planNode();

  std::unordered_map<std::string, std::string> queryConfigs;
  queryConfigs[core::QueryConfig::kHybridJoinEnabled] = hybridJoin ? "true" : "false";
  queryConfigs[core::QueryConfig::kHybridSortEnabled] = hybridSort ? "true" : "false";
  queryConfigs[core::QueryConfig::kLateMaterializationEnabled] = lateM ? "true" : "false";
  queryConfigs[core::QueryConfig::kHybridJoinPointerReuseEnabled] = FLAGS_pointer_reuse ? "true" : "false";
  queryConfigs[core::QueryConfig::kPreferredOutputBatchRows] = std::to_string(preferredBatchRows);
  queryConfigs[core::QueryConfig::kMaxOutputBatchRows] = std::to_string(maxBatchRows);

  CursorParameters params;
  params.planNode = plan;
  params.maxDrivers = numDrivers;
  params.queryConfigs = queryConfigs;

  auto startTime = std::chrono::steady_clock::now();
  
  auto cursor = TaskCursor::create(params);
  cursor->start();
  
  // Add splits for both tables
  auto task = cursor->task();
  task->addSplit(lineitemScanId, Split(std::make_shared<TpchConnectorSplit>(kTpchConnectorId)));
  task->noMoreSplits(lineitemScanId);
  task->addSplit(ordersScanId, Split(std::make_shared<TpchConnectorSplit>(kTpchConnectorId)));
  task->noMoreSplits(ordersScanId);

  int64_t totalRows = 0;
  while (cursor->moveNext()) {
    totalRows += cursor->current()->size();
  }
  
  task->taskCompletionFuture().wait();
  
  auto endTime = std::chrono::steady_clock::now();
  auto durationMs = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();

  std::cout << "Q24 completed: " << totalRows << " rows in " << durationMs << " ms" << std::endl;
}

// Q27-style: 3-way join (customer × orders × lineitem) → OrderBy
// Pattern: (orders × customer) as build side for lineitem join
void runQ27(double scaleFactor, int numDrivers, int preferredBatchRows, int maxBatchRows,
            bool hybridJoin, bool hybridSort, bool lateM) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId customerScanId, ordersScanId, lineitemScanId;

  // Build side for first join: customer (smallest)
  auto customer = PlanBuilder(planNodeIdGenerator)
                      .tpchTableScan(
                          tpch::Table::TBL_CUSTOMER,
                          {"c_custkey", "c_name", "c_address", "c_nationkey", 
                           "c_phone", "c_acctbal", "c_mktsegment", "c_comment"},
                          scaleFactor,
                          kTpchConnectorId)
                      .capturePlanNodeId(customerScanId)
                      .planNode();

  // First join: orders (probe) × customer (build)
  // Result becomes BUILD side for second join
  auto ordersJoinCustomer =
      PlanBuilder(planNodeIdGenerator)
          .tpchTableScan(
              tpch::Table::TBL_ORDERS,
              {"o_custkey", "o_orderkey", "o_totalprice"},
              scaleFactor,
              kTpchConnectorId)
          .capturePlanNodeId(ordersScanId)
          .hashJoin(
              {"o_custkey"},
              {"c_custkey"},
              customer,
              "",
              {"c_custkey", "c_name", "c_address", "c_nationkey", 
               "c_phone", "c_acctbal", "c_mktsegment", "c_comment",
               "o_orderkey", "o_totalprice"})
          .planNode();

  // Second join: lineitem (probe) × (orders×customer) (build)
  // Then Sort by c_custkey
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tpchTableScan(
              tpch::Table::TBL_LINEITEM,
              {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
               "l_quantity", "l_extendedprice", "l_discount", "l_tax",
               "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
               "l_receiptdate", "l_shipinstruct", "l_shipmode"},
              scaleFactor,
              kTpchConnectorId)
          .capturePlanNodeId(lineitemScanId)
          .hashJoin(
              {"l_orderkey"},
              {"o_orderkey"},
              ordersJoinCustomer,  // <-- Join result as BUILD side
              "",
              {"c_custkey", "c_name", "c_address", "c_nationkey", 
               "c_phone", "c_acctbal", "c_mktsegment", "c_comment",
               "o_totalprice",
               "l_partkey", "l_suppkey", "l_linenumber", "l_quantity",
               "l_extendedprice", "l_discount", "l_tax", "l_returnflag",
               "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate",
               "l_shipinstruct", "l_shipmode"})
          .orderBy({"c_custkey"}, false)
          .planNode();

  std::unordered_map<std::string, std::string> queryConfigs;
  queryConfigs[core::QueryConfig::kHybridJoinEnabled] = hybridJoin ? "true" : "false";
  queryConfigs[core::QueryConfig::kHybridSortEnabled] = hybridSort ? "true" : "false";
  queryConfigs[core::QueryConfig::kLateMaterializationEnabled] = lateM ? "true" : "false";
  queryConfigs[core::QueryConfig::kHybridJoinPointerReuseEnabled] = FLAGS_pointer_reuse ? "true" : "false";
  queryConfigs[core::QueryConfig::kPreferredOutputBatchRows] = std::to_string(preferredBatchRows);
  queryConfigs[core::QueryConfig::kMaxOutputBatchRows] = std::to_string(maxBatchRows);

  CursorParameters params;
  params.planNode = plan;
  params.maxDrivers = numDrivers;
  params.queryConfigs = queryConfigs;

  auto startTime = std::chrono::steady_clock::now();
  
  auto cursor = TaskCursor::create(params);
  cursor->start();
  
  // Add splits for all three tables
  auto task = cursor->task();
  task->addSplit(customerScanId, Split(std::make_shared<TpchConnectorSplit>(kTpchConnectorId)));
  task->noMoreSplits(customerScanId);
  task->addSplit(ordersScanId, Split(std::make_shared<TpchConnectorSplit>(kTpchConnectorId)));
  task->noMoreSplits(ordersScanId);
  task->addSplit(lineitemScanId, Split(std::make_shared<TpchConnectorSplit>(kTpchConnectorId)));
  task->noMoreSplits(lineitemScanId);

  int64_t totalRows = 0;
  while (cursor->moveNext()) {
    totalRows += cursor->current()->size();
  }
  
  task->taskCompletionFuture().wait();
  
  auto endTime = std::chrono::steady_clock::now();
  auto durationMs = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();

  std::cout << "Q27 completed: " << totalRows << " rows in " << durationMs << " ms" << std::endl;
}

// Q28: Single HashJoin with WIDE payload (for testing hybrid benefit)
// lineitem (probe) × orders (build), output ALL columns from both tables
void runQ28(double scaleFactor, int numDrivers, int preferredBatchRows, int maxBatchRows,
            bool hybridJoin, bool hybridSort, bool lateM) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemScanId, ordersScanId;

  // Build side: orders table with ALL columns
  auto orders = PlanBuilder(planNodeIdGenerator)
                    .tpchTableScan(
                        tpch::Table::TBL_ORDERS,
                        {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice",
                         "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority",
                         "o_comment"},
                        scaleFactor,
                        kTpchConnectorId)
                    .capturePlanNodeId(ordersScanId)
                    .planNode();

  // Probe side: lineitem table with ALL columns (except l_comment to fit schema)
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tpchTableScan(
              tpch::Table::TBL_LINEITEM,
              {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
               "l_quantity", "l_extendedprice", "l_discount", "l_tax",
               "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
               "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"},
              scaleFactor,
              kTpchConnectorId)
          .capturePlanNodeId(lineitemScanId)
          .hashJoin(
              {"l_orderkey"},
              {"o_orderkey"},
              orders,
              "",
              {// Build-side (orders) columns - 9 columns
               "o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice",
               "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority",
               "o_comment",
               // Probe-side (lineitem) columns - 15 columns
               "l_partkey", "l_suppkey", "l_linenumber", "l_quantity",
               "l_extendedprice", "l_discount", "l_tax", "l_returnflag",
               "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate",
               "l_shipinstruct", "l_shipmode", "l_comment"})
          .planNode();

  std::unordered_map<std::string, std::string> queryConfigs;
  queryConfigs[core::QueryConfig::kHybridJoinEnabled] = hybridJoin ? "true" : "false";
  queryConfigs[core::QueryConfig::kHybridSortEnabled] = hybridSort ? "true" : "false";
  queryConfigs[core::QueryConfig::kLateMaterializationEnabled] = lateM ? "true" : "false";
  queryConfigs[core::QueryConfig::kHybridJoinPointerReuseEnabled] = FLAGS_pointer_reuse ? "true" : "false";
  queryConfigs[core::QueryConfig::kPreferredOutputBatchRows] = std::to_string(preferredBatchRows);
  queryConfigs[core::QueryConfig::kMaxOutputBatchRows] = std::to_string(maxBatchRows);

  CursorParameters params;
  params.planNode = plan;
  params.maxDrivers = numDrivers;
  params.queryConfigs = queryConfigs;

  auto startTime = std::chrono::steady_clock::now();
  
  auto cursor = TaskCursor::create(params);
  cursor->start();
  
  auto task = cursor->task();
  task->addSplit(lineitemScanId, Split(std::make_shared<TpchConnectorSplit>(kTpchConnectorId)));
  task->noMoreSplits(lineitemScanId);
  task->addSplit(ordersScanId, Split(std::make_shared<TpchConnectorSplit>(kTpchConnectorId)));
  task->noMoreSplits(ordersScanId);

  int64_t totalRows = 0;
  while (cursor->moveNext()) {
    totalRows += cursor->current()->size();
  }
  
  task->taskCompletionFuture().wait();
  
  auto endTime = std::chrono::steady_clock::now();
  auto durationMs = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();

  std::cout << "Q28 completed: " << totalRows << " rows in " << durationMs << " ms" << std::endl;
}

} // namespace

int main(int argc, char** argv) {
  gflags::SetUsageMessage("Hybrid vs Late-M benchmark using TPC-H synthetic data");
  folly::Init init{&argc, &argv, false};

  // Initialize memory manager
  memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  
  // Register functions
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();
  
  // Register TPC-H connector
  registerConnectors();

  std::string config;
  if (!FLAGS_hybrid_join && !FLAGS_hybrid_sort && !FLAGS_late_m) {
    config = "Baseline";
  } else if (FLAGS_hybrid_join && FLAGS_hybrid_sort && !FLAGS_late_m) {
    config = "Hybrid";
  } else if (FLAGS_hybrid_join && FLAGS_hybrid_sort && FLAGS_late_m) {
    config = "Late-M";
  } else {
    config = "Custom";
  }

  std::cout << "=== Configuration ===" << std::endl;
  std::cout << "Mode: " << config << std::endl;
  std::cout << "Scale Factor: " << FLAGS_scale_factor << std::endl;
  std::cout << "Num Drivers: " << FLAGS_num_drivers << std::endl;
  std::cout << "Preferred Batch Rows: " << FLAGS_preferred_batch_rows << std::endl;
  std::cout << "Max Batch Rows: " << FLAGS_max_batch_rows << std::endl;
  std::cout << "Query: Q" << FLAGS_query << std::endl;
  std::cout << "====================" << std::endl;

  if (FLAGS_query == 23) {
    runQ23(FLAGS_scale_factor, FLAGS_num_drivers, FLAGS_preferred_batch_rows, FLAGS_max_batch_rows,
           FLAGS_hybrid_join, FLAGS_hybrid_sort, FLAGS_late_m);
  } else if (FLAGS_query == 24) {
    runQ24(FLAGS_scale_factor, FLAGS_num_drivers, FLAGS_preferred_batch_rows, FLAGS_max_batch_rows,
           FLAGS_hybrid_join, FLAGS_hybrid_sort, FLAGS_late_m);
  } else if (FLAGS_query == 27) {
    runQ27(FLAGS_scale_factor, FLAGS_num_drivers, FLAGS_preferred_batch_rows, FLAGS_max_batch_rows,
           FLAGS_hybrid_join, FLAGS_hybrid_sort, FLAGS_late_m);
  } else if (FLAGS_query == 28) {
    runQ28(FLAGS_scale_factor, FLAGS_num_drivers, FLAGS_preferred_batch_rows, FLAGS_max_batch_rows,
           FLAGS_hybrid_join, FLAGS_hybrid_sort, FLAGS_late_m);
  } else {
    std::cerr << "Unsupported query: " << FLAGS_query << std::endl;
    return 1;
  }

  unregisterConnectors();
  return 0;
}

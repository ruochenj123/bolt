/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/benchmarks/QueryBenchmarkBase.h"
#include "bolt/common/testutil/GPerf.h"
#include "bolt/exec/tests/utils/TpcdsQueryBuilder.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::dwio::common;

DEFINE_string(
    data_path,
    "",
    "Root path of TPC-DS data. Data layout must follow Hive-style partitioning. "
    "Example layout for '-data_path=/data/tpcds10'\n"
    "       /data/tpcds10/catalog_sales\n"
    "If the above is a directory, it contains the data files for "
    "the table. If it is a file, it contains a file system path for each "
    "data file, one per line.");

namespace {
static bool notEmpty(const char* /*flagName*/, const std::string& value) {
  return !value.empty();
}
} // namespace

DEFINE_validator(data_path, &notEmpty);

DEFINE_int32(
    run_query_verbose,
    -1,
    "Run a given query (1-4) and print execution statistics. "
    "1: Sort by 1 key (cs_warehouse_sk), "
    "2: Sort by 2 keys (+cs_ship_mode_sk), "
    "3: Sort by 3 keys (+cs_promo_sk), "
    "4: Sort by 4 keys (+cs_quantity)");

std::shared_ptr<TpcdsQueryBuilder> queryBuilder;

class TpcdsBenchmark : public QueryBenchmarkBase {
 public:
  void runMain(std::ostream& out, RunStats& runStats) override {
    BoltProfilerStart("tpcds.prof");
    if (FLAGS_run_query_verbose == -1) {
      folly::runBenchmarks();
    } else {
      const auto queryPlan = queryBuilder->getQueryPlan(FLAGS_run_query_verbose);
      auto [cursor, actualResults] = run(queryPlan);
      if (!cursor) {
        LOG(ERROR) << "Query terminated with error. Exiting";
        exit(1);
      }
      auto task = cursor->task();
      ensureTaskCompletion(task.get());
      if (FLAGS_include_results) {
        printResults(actualResults, out);
        out << std::endl;
      }
      const auto stats = task->taskStats();
      int64_t rawInputBytes = 0;
      for (auto& pipeline : stats.pipelineStats) {
        auto& first = pipeline.operatorStats[0];
        if (first.operatorType == "TableScan") {
          rawInputBytes += first.rawInputBytes;
        }
      }
      runStats.rawInputBytes = rawInputBytes;
      const auto endTime =
          stats.endTimeMs > 0 ? stats.endTimeMs : stats.executionEndTimeMs;
      out << fmt::format(
                 "Execution time: {}",
                 succinctMillis(endTime - stats.executionStartTimeMs))
          << std::endl;
      out << fmt::format(
                 "Splits total: {}, finished: {}",
                 stats.numTotalSplits,
                 stats.numFinishedSplits)
          << std::endl;
      out << printPlanWithStats(
                 *queryPlan.plan, stats, FLAGS_include_custom_stats, true)
          << std::endl;
    }
    BoltProfilerStop();
  }
};

TpcdsBenchmark benchmark;

// Sort by 1 key: cs_warehouse_sk
BENCHMARK(sort_1key) {
  const auto planContext = queryBuilder->getQueryPlan(1);
  benchmark.run(planContext);
}

// Sort by 2 keys: cs_warehouse_sk, cs_ship_mode_sk
BENCHMARK(sort_2keys) {
  const auto planContext = queryBuilder->getQueryPlan(2);
  benchmark.run(planContext);
}

// Sort by 3 keys: cs_warehouse_sk, cs_ship_mode_sk, cs_promo_sk
BENCHMARK(sort_3keys) {
  const auto planContext = queryBuilder->getQueryPlan(3);
  benchmark.run(planContext);
}

// Sort by 4 keys: cs_warehouse_sk, cs_ship_mode_sk, cs_promo_sk, cs_quantity
BENCHMARK(sort_4keys) {
  const auto planContext = queryBuilder->getQueryPlan(4);
  benchmark.run(planContext);
}

int tpcdsBenchmarkMain() {
  benchmark.initialize();
  queryBuilder =
      std::make_shared<TpcdsQueryBuilder>(toFileFormat(FLAGS_data_format));
  queryBuilder->initialize(FLAGS_data_path);
  if (FLAGS_test_flags_file.empty()) {
    RunStats ignore;
    benchmark.runMain(std::cout, ignore);
  } else {
    benchmark.runAllCombinations();
  }
  benchmark.shutdown();
  queryBuilder.reset();
  return 0;
}

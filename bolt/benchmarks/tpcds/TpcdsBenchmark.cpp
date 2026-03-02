/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/benchmarks/QueryBenchmarkBase.h"
#include "bolt/exec/tests/utils/TpcdsQueryBuilder.h"
#include "bolt/common/testutil/GPerf.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::dwio::common;

DEFINE_string(
    data_path,
    "",
    "Root path of TPC-DS data. Data layout must follow Hive-style partitioning. "
    "Example layout for '-data_path=/data/tpcds_sf10'\n"
    "       /data/tpcds_sf10/store_sales\n"
    "       /data/tpcds_sf10/date_dim\n"
    "       /data/tpcds_sf10/item\n"
    "       /data/tpcds_sf10/customer\n"
    "       etc.\n"
    "If the above are directories, they contain the data files for "
    "each table. If they are files, they contain a file system path for each "
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
    "Run a given query and print execution statistics");

std::shared_ptr<TpcdsQueryBuilder> queryBuilder;

class TpcdsBenchmark : public QueryBenchmarkBase {
 public:
  void runMain(std::ostream& out, RunStats& runStats) override {
    BoltProfilerStart("tpcds.prof");
    if (FLAGS_run_query_verbose == -1) {
      folly::runBenchmarks();
    } else {
      const auto queryPlan =
          queryBuilder->getQueryPlan(FLAGS_run_query_verbose);
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

BENCHMARK(q1) {
  const auto planContext = queryBuilder->getQueryPlan(1);
  benchmark.run(planContext);
}

BENCHMARK(q3) {
  const auto planContext = queryBuilder->getQueryPlan(3);
  benchmark.run(planContext);
}

BENCHMARK(q10) {
  const auto planContext = queryBuilder->getQueryPlan(10);
  benchmark.run(planContext);
}

BENCHMARK(q18) {
  const auto planContext = queryBuilder->getQueryPlan(18);
  benchmark.run(planContext);
}

BENCHMARK(q30) {
  const auto planContext = queryBuilder->getQueryPlan(30);
  benchmark.run(planContext);
}

BENCHMARK(q63) {
  const auto planContext = queryBuilder->getQueryPlan(63);
  benchmark.run(planContext);
}

BENCHMARK(q89) {
  const auto planContext = queryBuilder->getQueryPlan(89);
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

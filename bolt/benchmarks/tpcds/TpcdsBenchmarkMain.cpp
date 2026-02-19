/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "bolt/benchmarks/tpcds/TpcdsBenchmark.h"

int main(int argc, char** argv) {
  std::string kUsage(
      "This program benchmarks TPC-DS sort queries on catalog_sales. "
      "Run 'bolt_tpcds_benchmark -helpon=TpcdsBenchmark' for available options.\n");
  gflags::SetUsageMessage(kUsage);
  folly::Init init{&argc, &argv, false};
  tpcdsBenchmarkMain();
}

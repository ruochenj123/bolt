# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

add_library(bolt_tpcds_benchmark_lib OBJECT TpcdsBenchmark.cpp)

target_link_libraries(
  bolt_tpcds_benchmark_lib
  bolt_query_benchmark
  bolt_testutils
  $<TARGET_NAME_IF_EXISTS:bolt_test_util_gperf>
  Folly::folly
  ${FOLLY_BENCHMARK}
  fmt::fmt
)

add_executable(bolt_tpcds_benchmark TpcdsBenchmarkMain.cpp)

target_link_libraries(bolt_tpcds_benchmark PRIVATE
  bolt_tpcds_benchmark_lib
  bolt_query_benchmark
  bolt_testutils
  $<TARGET_NAME_IF_EXISTS:bolt_test_util_gperf>
  ${FOLLY_BENCHMARK}
)

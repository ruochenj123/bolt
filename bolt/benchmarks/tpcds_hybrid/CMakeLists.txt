# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0
#
# Hand-built TPC-DS plans (25 queries) used for the hybrid-layout / fusion
# experiments. The upstream bolt_tpcds_benchmark (../tpcds) runs serialized
# plans instead; both coexist.

add_library(bolt_tpcds_hybrid_benchmark_lib OBJECT TpcdsHybridBenchmark.cpp)

target_link_libraries(
  bolt_tpcds_hybrid_benchmark_lib
  bolt_query_benchmark
  bolt_testutils
  $<TARGET_NAME_IF_EXISTS:bolt_test_util_gperf>
  Folly::folly
  ${FOLLY_BENCHMARK}
  fmt::fmt
)

add_executable(bolt_tpcds_hybrid_benchmark TpcdsHybridBenchmarkMain.cpp)

target_link_libraries(
  bolt_tpcds_hybrid_benchmark
  PRIVATE
    bolt_tpcds_hybrid_benchmark_lib
    bolt_query_benchmark
    bolt_testutils
    $<TARGET_NAME_IF_EXISTS:bolt_test_util_gperf>
    ${FOLLY_BENCHMARK}
)

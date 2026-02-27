# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

add_library(bolt_tpcds_benchmark_lib TpcdsBenchmark.cpp)

target_link_libraries(
  bolt_tpcds_benchmark_lib
  bolt_query_benchmark
  bolt_aggregates
  bolt_exec
  bolt_exec_test_lib
  bolt_dwio_common
  bolt_dwio_common_exception
  bolt_dwio_parquet_reader
  bolt_dwio_common_test_utils
  bolt_hive_connector
  bolt_exception
  bolt_memory
  bolt_process
  bolt_serialization
  bolt_encode
  bolt_type
  bolt_type_fbhive
  bolt_caching
  bolt_vector_test_lib
  bolt_test_util_gperf
  Folly::folly
  ${FOLLY_BENCHMARK}
  fmt::fmt
)

add_executable(bolt_tpcds_benchmark TpcdsBenchmarkMain.cpp)

target_link_libraries(bolt_tpcds_benchmark bolt_tpcds_benchmark_lib)

#
# Copyright (c) ByteDance Ltd. and/or its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_executable(export_bolt_arrow_bridge_benchmark ExportBoltToArrowBenchmark.cpp)
target_link_libraries(
  export_bolt_arrow_bridge_benchmark
  bolt_arrow_bridge
  bolt_vector_fuzzer
  bolt_vector_test_lib
  bolt_exec_test_lib
  arrow::arrow
  ${FOLLY_BENCHMARK}
)

add_executable(arrow_bolt_bridge_benchmark ArrowToBoltBenchmark.cpp)
target_link_libraries(
  arrow_bolt_bridge_benchmark
  bolt_arrow_bridge
  bolt_vector_fuzzer
  bolt_vector_test_lib
  bolt_exec_test_lib
  arrow::arrow
  ${FOLLY_BENCHMARK}
)

add_executable(arrow_bolt_array_bridge_benchmark ArrowToBoltArrayBenchmark.cpp)
target_link_libraries(
  arrow_bolt_array_bridge_benchmark
  bolt_arrow_bridge
  bolt_vector_fuzzer
  bolt_vector_test_lib
  bolt_exec_test_lib
  arrow::arrow
  ${FOLLY_BENCHMARK}
)

add_executable(import_arrow_bolt_bridge_benchmark ImportArrowToBoltBenchmark.cpp)
target_link_libraries(
  import_arrow_bolt_bridge_benchmark
  bolt_arrow_bridge
  bolt_vector_fuzzer
  bolt_vector_test_lib
  bolt_exec_test_lib
  arrow::arrow
  arrow::libarrow_testing
  ${FOLLY_BENCHMARK}
)

set_target_properties(export_bolt_arrow_bridge_benchmark PROPERTIES LINK_LIBRARY_OVERRIDE "WHOLE_ARCHIVE,bolt_process") # WHOLE_ARCHIVE/plain mix fix

set_target_properties(arrow_bolt_bridge_benchmark PROPERTIES LINK_LIBRARY_OVERRIDE "WHOLE_ARCHIVE,bolt_process") # WHOLE_ARCHIVE/plain mix fix

set_target_properties(arrow_bolt_array_bridge_benchmark PROPERTIES LINK_LIBRARY_OVERRIDE "WHOLE_ARCHIVE,bolt_process") # WHOLE_ARCHIVE/plain mix fix

set_target_properties(import_arrow_bolt_bridge_benchmark PROPERTIES LINK_LIBRARY_OVERRIDE "WHOLE_ARCHIVE,bolt_process") # WHOLE_ARCHIVE/plain mix fix

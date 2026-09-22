/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <climits>
#include <optional>
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
namespace bytedance::bolt::functions::sparksql::test {
using namespace bytedance::bolt::test;
namespace {

class UdfStrToLongArrFunctionTest : public SparkFunctionBaseTest {
 protected:
  void testStrToLongArr(
      const std::vector<std::optional<std::string>>& input,
      const std::vector<std::optional<std::vector<int64_t>>>& output);
};

void UdfStrToLongArrFunctionTest::testStrToLongArr(
    const std::vector<std::optional<std::string>>& input,
    const std::vector<std::optional<std::vector<int64_t>>>& output) {
  auto valueAt = [&input](vector_size_t row) {
    return input[row] ? StringView(*input[row]) : StringView();
  };

  // Creating vectors for input strings
  auto nullAt = [&input](vector_size_t row) { return !input[row].has_value(); };

  auto result = [&] {
    auto inputString =
        makeFlatVector<StringView>(input.size(), valueAt, nullAt);

    auto limits =
        makeFlatVector<int32_t>(input.size(), [](auto row) { return -1; });

    auto rowVector = makeRowVector({inputString, limits});

    // Evaluating the function for each input and seed
    std::string expressionString = "str_to_long_arr(c0)";

    return evaluate<ArrayVector>(expressionString, rowVector);
  }();
  // Creating vectors for output expected vectors
  auto sizeAtOutput = [&output](vector_size_t row) {
    return output[row] ? output[row]->size() : 0;
  };
  auto valueAtOutput = [&output](vector_size_t row, vector_size_t idx) {
    return output[row]->at(idx);
  };
  auto nullAtOutput = [&output](vector_size_t row) {
    return !output[row].has_value();
  };
  auto expectedResult = makeArrayVector<int64_t>(
      output.size(), sizeAtOutput, valueAtOutput, nullAtOutput);

  // Checking the results
  assertEqualVectors(expectedResult, result);
}

TEST_F(UdfStrToLongArrFunctionTest, base) {
  testStrToLongArr(
      {"1,2,3", "a,b,c", "", std::nullopt, "ab,c,1"},
      {{{1L, 2L, 3L}}, emptyArray, {std::nullopt}, {std::nullopt}, {{1L}}});
}

TEST_F(UdfStrToLongArrFunctionTest, corner_case) {
  testStrToLongArr(
      {"1111111111111111111111111111111111111111111111111111111111111",
       "3,-12,45,7,a4,8",
       "+9223372036854775807,9223372036854775807,-9223372036854775808,9223372036854775809,-9223372036854775809"},
      {emptyArray,
       {{3L, -12L, 45L, 7L, 8L}},
       {{LONG_MAX, LONG_MAX, LONG_MIN}}});
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test

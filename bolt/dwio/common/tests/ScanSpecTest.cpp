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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <memory>
#include <optional>

#include "bolt/dwio/common/ScanSpec.h"
#include "bolt/dwio/common/Statistics.h"
#include "bolt/type/filter/NegatedFloatingPointValues.h"

namespace bytedance::bolt::common {

TEST(ScanSpecTest, doubleColumnPruning) {
  std::optional<uint64_t> valueCount = 100;
  std::optional<bool> hasNull = true;
  std::optional<uint64_t> rawSize = std::nullopt;
  std::optional<uint64_t> size = std::nullopt;
  std::optional<double> min = 0;
  std::optional<double> max = 1;
  std::optional<double> sum = std::nullopt;
  using bytedance::bolt::common::NegatedFloatingPointValues;
  using bytedance::bolt::dwio::common::DoubleColumnStatistics;
  // min(col) = 0, max(col) = 1
  auto colStatics = std::make_unique<DoubleColumnStatistics>(
      valueCount, hasNull, rawSize, size, min, max, sum);
  // col not in (0.0, 1.0)
  auto notInList = std::vector<double>{0.0, 1.0};
  auto filter =
      std::make_unique<NegatedFloatingPointValues<double>>(notInList, true);
  // expect return true, because all value from col is between 0.0 and 1.0,
  // shouldn't be filtered
  bool expect =
      testFilter(filter.get(), colStatics.get(), valueCount.value(), DOUBLE());
  EXPECT_TRUE(expect);
}

} // namespace bytedance::bolt::common

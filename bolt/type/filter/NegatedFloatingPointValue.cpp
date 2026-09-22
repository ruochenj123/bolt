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

#include <sstream>
#include "bolt/type/filter/FilterUtil.h"
#include "bolt/type/filter/FloatingPointRange.h"
#include "bolt/type/filter/FloatingPointValues.h"
#include "bolt/type/filter/NegatedFloatingPointValues.h"
namespace bytedance::bolt::common {

template <typename T>
std::unique_ptr<Filter> combineNegatedRangeOnFloatRanges(
    T negatedMin,
    T negatedMax,
    std::vector<std::unique_ptr<FloatingPointRange<T>>> ranges,
    bool nullAllowed) {
  if (ranges.empty()) {
    return std::make_unique<NegatedFloatingPointValues<T>>(
        std::vector<T>{}, nullAllowed);
  }

  // Sort ranges by lower bound
  std::sort(ranges.begin(), ranges.end(), [](const auto& a, const auto& b) {
    return a->lower() < b->lower();
  });

  std::vector<std::unique_ptr<FloatingPointRange<T>>> outRanges;
  const T epsilon = std::numeric_limits<T>::epsilon();
  T currentMin = negatedMin;

  // Process each range
  for (size_t i = 0; i < ranges.size(); i++) {
    const auto& range = ranges[i];

    // Add range before current range if there's a gap
    if (currentMin + epsilon < range->lower()) {
      outRanges.push_back(std::make_unique<FloatingPointRange<T>>(
          currentMin,
          false, /*lowerUnbounded*/
          false, /*lowerExclusive*/
          range->lower() - epsilon,
          false, /*upperUnbounded*/
          false, /*upperExclusive*/
          false /*nullAllowed*/));
    }

    // Update currentMin for next iteration
    currentMin = range->upper() + epsilon;
  }

  // Add final range if there's space after last range
  if (currentMin < negatedMax) {
    outRanges.push_back(std::make_unique<FloatingPointRange<T>>(
        currentMin,
        false, /*lowerUnbounded*/
        false, /*lowerExclusive*/
        negatedMax,
        false, /*upperUnbounded*/
        false, /*upperExclusive*/
        false /*nullAllowed*/));
  }

  return combineFloatRanges(std::move(outRanges), nullAllowed);
}

template <typename T>
NegatedFloatingPointValues<T>::NegatedFloatingPointValues(
    const std::vector<T>& values,
    bool nullAllowed,
    bool nanAllowed)
    : Filter(
          true,
          nullAllowed,
          std::is_same_v<T, double> ? FilterKind::kNegatedDoubleValues
                                    : FilterKind::kNegatedFloatValues),
      nonNegated_(std::make_unique<FloatingPointValues<T>>(
          values,
          !nullAllowed,
          nanAllowed)) {}

template <typename T>
NegatedFloatingPointValues<T>::NegatedFloatingPointValues(
    const NegatedFloatingPointValues& other,
    bool nullAllowed)
    : Filter(true, nullAllowed, other.kind()),
      nonNegated_(
          std::make_unique<FloatingPointValues<T>>(*other.nonNegated_)) {}

template <typename T>
folly::dynamic NegatedFloatingPointValues<T>::serialize() const {
  auto obj = Filter::serializeBase(
      std::is_same_v<T, double> ? "NegatedDoubleValues" : "NegatedFloatValues");
  obj["inner"] = nonNegated_->serialize();
  return obj;
}

template <typename T>
FilterPtr NegatedFloatingPointValues<T>::create(const folly::dynamic& obj) {
  auto min = obj["min"].asDouble();
  auto max = obj["max"].asDouble();
  auto nullAllowed = deserializeNullAllowed(obj);

  std::vector<T> values;
  for (const auto& value : obj["values"]) {
    values.push_back(static_cast<T>(value.asDouble()));
  }
  return std::make_unique<NegatedFloatingPointValues<T>>(values, nullAllowed);
}

template <typename T>
std::unique_ptr<Filter> NegatedFloatingPointValues<T>::clone(
    std::optional<bool> nullAllowed) const {
  return std::make_unique<NegatedFloatingPointValues<T>>(
      *this, nullAllowed.value_or(nullAllowed_));
}

template <typename T>
bool NegatedFloatingPointValues<T>::testDoubleRange(
    double min,
    double max,
    bool hasNull) const {
  // it is not possible to exclude an entire range based on a few blacklisted
  // points.
  return true;
}

template <typename T>
bool NegatedFloatingPointValues<T>::testingEquals(const Filter& other) const {
  if (kind() != other.kind()) {
    return false;
  }
  auto otherFilter = dynamic_cast<const NegatedFloatingPointValues<T>*>(&other);
  return nullAllowed_ == otherFilter->nullAllowed_ &&
      nonNegated_->testingEquals(*otherFilter->nonNegated_);
}

template <typename T>
std::unique_ptr<Filter> NegatedFloatingPointValues<T>::mergeWith(
    const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other->mergeWith(this);

    case FilterKind::kIsNotNull:
      return this->clone(false);

    case FilterKind::kFloatRange:
    case FilterKind::kDoubleRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto otherRange = static_cast<const FloatingPointRange<T>*>(other);

      std::vector<std::unique_ptr<FloatingPointRange<T>>> rangeList;
      rangeList.emplace_back(std::make_unique<FloatingPointRange<T>>(
          otherRange->lower(),
          false, /*lowerUnbounded*/
          false, /*lowerExclusive*/
          otherRange->upper(),
          false, /*upperUnbounded*/
          false, /*upperExclusive*/
          false /*nullAllowed*/));

      return combineNegatedRangeOnFloatRanges(
          nonNegated_->min(),
          nonNegated_->max(),
          std::move(rangeList),
          bothNullAllowed);
    }

    case FilterKind::kNegatedDoubleValues:
    case FilterKind::kNegatedFloatValues: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto otherNegated =
          static_cast<const NegatedFloatingPointValues<T>*>(other);

      if (nonNegated_->min() > otherNegated->nonNegated_->min()) {
        return other->mergeWith(this);
      }

      assert(nonNegated_->min() <= otherNegated->nonNegated_->min());

      // If there's a gap between ranges, create split ranges
      const T epsilon = std::numeric_limits<T>::epsilon();
      if (nonNegated_->max() + epsilon < otherNegated->nonNegated_->min()) {
        std::vector<std::unique_ptr<FloatingPointRange<T>>> outRanges;

        T smallMin = nonNegated_->min();
        T smallMax = nonNegated_->max();
        T bigMin = otherNegated->nonNegated_->min();
        T bigMax = otherNegated->nonNegated_->max();

        // Lower range
        if (smallMin > std::numeric_limits<T>::lowest()) {
          outRanges.emplace_back(std::make_unique<FloatingPointRange<T>>(
              std::numeric_limits<T>::lowest(),
              false, /*lowerUnbounded*/
              false, /*lowerExclusive*/
              smallMin - epsilon,
              false, /*upperUnbounded*/
              false, /*upperExclusive*/
              false /*nullAllowed*/));
        }

        // Middle range (gap)
        if (smallMax < std::numeric_limits<T>::max() &&
            bigMin > std::numeric_limits<T>::lowest()) {
          outRanges.emplace_back(std::make_unique<FloatingPointRange<T>>(
              smallMax + epsilon,
              false, /*lowerUnbounded*/
              false, /*lowerExclusive*/
              bigMin - epsilon,
              false, /*upperUnbounded*/
              false, /*upperExclusive*/
              false /*nullAllowed*/));
        }

        // Upper range
        if (bigMax < std::numeric_limits<T>::max()) {
          outRanges.emplace_back(std::make_unique<FloatingPointRange<T>>(
              bigMax + epsilon,
              false, /*lowerUnbounded*/
              false, /*lowerExclusive*/
              std::numeric_limits<T>::max(),
              false, /*upperUnbounded*/
              false, /*upperExclusive*/
              false /*nullAllowed*/));
        }

        return combineFloatRanges(std::move(outRanges), bothNullAllowed);
      }

      // No gap - merge the ranges
      std::vector<T> mergedValues;
      const auto& otherValues = otherNegated->nonNegated_->values();
      const auto& values = nonNegated_->values();
      std::set_union(
          otherValues.begin(),
          otherValues.end(),
          values.begin(),
          values.end(),
          std::back_inserter(mergedValues));

      return std::make_unique<NegatedFloatingPointValues<T>>(
          mergedValues, // Use values from current range
          bothNullAllowed);
    }

    case FilterKind::kFloatValues:
    case FilterKind::kDoubleValues: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto otherValues = static_cast<const FloatingPointValues<T>*>(other);

      // Convert values to range for merging
      std::vector<std::unique_ptr<FloatingPointRange<T>>> rangeList;
      rangeList.emplace_back(std::make_unique<FloatingPointRange<T>>(
          otherValues->min(),
          false, /*lowerUnbounded*/
          false, /*lowerExclusive*/
          otherValues->max(),
          false, /*upperUnbounded*/
          false, /*upperExclusive*/
          false /*nullAllowed*/));

      return combineNegatedRangeOnFloatRanges(
          nonNegated_->min(),
          nonNegated_->max(),
          std::move(rangeList),
          bothNullAllowed);
    }

    default:
      return nullptr;
  }
}

template <typename T>
std::string NegatedFloatingPointValues<T>::toString() const {
  std::ostringstream out;
  out << (std::is_same_v<T, double> ? "NegatedDoubleValues"
                                    : "NegatedFloatValues")
      << ": " << nonNegated_->toString();
  return out.str();
}

// Explicit template instantiations
template class NegatedFloatingPointValues<float>;
template class NegatedFloatingPointValues<double>;

} // namespace bytedance::bolt::common

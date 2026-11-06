/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/exec/JoinAggReuse.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <optional>
#include <unordered_set>

#include <folly/hash/Hash.h>
#include <glog/logging.h>

#include "bolt/exec/HashProbe.h"
#include "bolt/exec/HashTable.h"
#include "bolt/exec/Task.h"
#include "bolt/type/DecimalUtil.h"
#include "bolt/type/HugeInt.h"

namespace bytedance::bolt::exec {
namespace {

bool isNumericKind(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BIGINT:
    case TypeKind::INTEGER:
    case TypeKind::SMALLINT:
    case TypeKind::TINYINT:
    case TypeKind::DOUBLE:
      return true;
    default:
      return false;
  }
}

// Grouping key types the codes mode can hash (see appendValue).
bool isCodableKind(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::HUGEINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP:
      return true;
    default:
      return false;
  }
}

// Collects the names of input columns referenced by 'expr'.
void collectInputColumns(
    const core::TypedExprPtr& expr,
    std::unordered_set<std::string>& names) {
  if (auto field =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expr)) {
    if (field->isInputColumn()) {
      names.insert(field->name());
      return;
    }
  }
  for (const auto& input : expr->inputs()) {
    collectInputColumns(input, names);
  }
}

bool isConstant(const core::TypedExprPtr& expr) {
  return std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr) !=
      nullptr;
}

bool isBuildColumnOf(const core::HashJoinNode& join, const std::string& name) {
  return join.outputType()->getChildIdxIfExists(name).has_value() &&
      join.sources()[1]->outputType()->getChildIdxIfExists(name).has_value();
}

bool references(
    const core::TypedExprPtr& expr,
    const std::unordered_set<std::string>& names) {
  std::unordered_set<std::string> used;
  collectInputColumns(expr, used);
  for (const auto& name : used) {
    if (names.count(name) > 0) {
      return true;
    }
  }
  return false;
}

// Where a grouping key comes from: the join at chain[chainIndex], as its
// build column 'column' or as its probe key equal to build key 'buildKey'.
struct KeySource {
  size_t chainIndex;
  std::string column;
  int32_t buildKey{-1};
};

// Follows the aggregation input column 'name' down the chain (chain[0] is the
// aggregation's source) through identity projections and the probe side of
// joins, to the first join where it is a build column or a probe key.
std::optional<KeySource> findKeySource(
    const std::vector<core::PlanNodePtr>& chain,
    std::string name) {
  for (size_t k = 0; k < chain.size(); ++k) {
    if (auto project =
            std::dynamic_pointer_cast<const core::ProjectNode>(chain[k])) {
      const auto& names = project->names();
      auto it = std::find(names.begin(), names.end(), name);
      if (it == names.end()) {
        return std::nullopt;
      }
      auto field = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
          project->projections()[it - names.begin()]);
      if (field == nullptr || !field->isInputColumn()) {
        return std::nullopt;
      }
      name = field->name();
      continue;
    }
    const auto& join = static_cast<const core::HashJoinNode&>(*chain[k]);
    if (isBuildColumnOf(join, name)) {
      return KeySource{k, name, -1};
    }
    const auto& leftKeys = join.leftKeys();
    for (size_t i = 0; i < leftKeys.size(); ++i) {
      if (leftKeys[i]->name() == name &&
          leftKeys[i]->type()->equivalent(*join.rightKeys()[i]->type())) {
        return KeySource{k, name, static_cast<int32_t>(i)};
      }
    }
    if (!join.sources()[0]->outputType()->getChildIdxIfExists(name)) {
      return std::nullopt;
    }
  }
  // A column of the chain's input: no join determines it.
  return std::nullopt;
}

// True if build column 'column' of chain[j] is used above it only to pass
// through (identity projections, probe side of the joins in between) into
// grouping keys of 'agg', or not at all: then chain[j] need not extract it.
bool onlyGroupingUse(
    const std::vector<core::PlanNodePtr>& chain,
    size_t j,
    const core::AggregationNode& agg,
    const std::string& column) {
  std::unordered_set<std::string> names{column};
  for (size_t k = j; k-- > 0;) {
    if (auto project =
            std::dynamic_pointer_cast<const core::ProjectNode>(chain[k])) {
      std::unordered_set<std::string> passed;
      for (size_t i = 0; i < project->projections().size(); ++i) {
        const auto& expr = project->projections()[i];
        auto field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expr);
        if (field != nullptr && field->isInputColumn()) {
          if (names.count(field->name()) > 0) {
            passed.insert(project->names()[i]);
          }
        } else if (references(expr, names)) {
          return false;
        }
      }
      names = std::move(passed);
    } else {
      const auto& join = static_cast<const core::HashJoinNode&>(*chain[k]);
      for (const auto& key : join.leftKeys()) {
        if (names.count(key->name()) > 0) {
          return false;
        }
      }
      if (join.filter() != nullptr && references(join.filter(), names)) {
        return false;
      }
      for (auto it = names.begin(); it != names.end();) {
        it = join.outputType()->getChildIdxIfExists(*it) ? std::next(it)
                                                         : names.erase(it);
      }
    }
    if (names.empty()) {
      return true;
    }
  }
  for (const auto& aggregate : agg.aggregates()) {
    if (references(aggregate.call, names) ||
        (aggregate.mask != nullptr && names.count(aggregate.mask->name()))) {
      return false;
    }
  }
  return true;
}

bool analyzeAggregates(
    const core::AggregationNode& agg,
    const RowTypePtr& aggInputType,
    JoinAggReuseSpec& spec) {
  const auto numKeys = agg.groupingKeys().size();
  int32_t slot = 1; // Slot 0 = touched flag.
  for (size_t i = 0; i < agg.aggregates().size(); ++i) {
    const auto& aggregate = agg.aggregates()[i];
    if (aggregate.mask != nullptr || aggregate.distinct ||
        !aggregate.sortingKeys.empty()) {
      return false;
    }
    const auto& fname = aggregate.call->name();
    const auto& inputs = aggregate.call->inputs();
    JoinAggReuseAgg a;
    a.resultType = agg.outputType()->childAt(numKeys + i);
    if (fname == "count") {
      if (inputs.empty() || (inputs.size() == 1 && isConstant(inputs[0]))) {
        a.kind = JoinAggReuseAgg::Kind::kCountStar;
      } else if (inputs.size() == 1) {
        a.kind = JoinAggReuseAgg::Kind::kCount;
      } else {
        return false;
      }
    } else if (
        fname == "sum" || fname == "min" || fname == "max" ||
        fname == "avg") {
      if (inputs.size() != 1) {
        return false;
      }
      a.kind = fname == "sum" ? JoinAggReuseAgg::Kind::kSum
          : fname == "min"    ? JoinAggReuseAgg::Kind::kMin
          : fname == "max"    ? JoinAggReuseAgg::Kind::kMax
                              : JoinAggReuseAgg::Kind::kAvg;
    } else {
      return false;
    }
    if (a.kind != JoinAggReuseAgg::Kind::kCountStar) {
      auto field =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
              inputs[0]);
      if (field == nullptr) {
        return false;
      }
      auto channel = aggInputType->getChildIdxIfExists(field->name());
      if (!channel.has_value()) {
        return false;
      }
      a.inputChannel = static_cast<int32_t>(*channel);
      a.inputType = aggInputType->childAt(*channel);
      a.decimal = a.inputType->isDecimal() &&
          (a.kind == JoinAggReuseAgg::Kind::kSum ||
           a.kind == JoinAggReuseAgg::Kind::kAvg) &&
          a.resultType->kind() == TypeKind::VARBINARY;
      if (a.kind != JoinAggReuseAgg::Kind::kCount && !a.decimal &&
          !isNumericKind(a.inputType)) {
        return false;
      }
    }
    if (a.decimal) {
      // Partial state: VARBINARY (LongDecimalWithOverflowState).
    } else if (a.kind == JoinAggReuseAgg::Kind::kAvg) {
      // Partial avg = ROW(sum DOUBLE, count BIGINT).
      if (a.resultType->kind() != TypeKind::ROW ||
          a.resultType->size() != 2 ||
          a.resultType->childAt(0)->kind() != TypeKind::DOUBLE ||
          a.resultType->childAt(1)->kind() != TypeKind::BIGINT) {
        return false;
      }
    } else if (!isNumericKind(a.resultType)) {
      return false;
    }
    a.slot = slot;
    slot += a.numSlots();
    spec.aggs.push_back(std::move(a));
  }
  spec.numSlots = slot;
  return true;
}

// 'chain': the projections and joins below 'agg' in its pipeline, top-down
// (chain[0] is the aggregation's source).
std::optional<JoinAggReuseSpec> analyzeAggregation(
    const std::shared_ptr<const core::AggregationNode>& agg,
    const std::vector<core::PlanNodePtr>& chain) {
  // Partial step (the final step merges groups several drivers emit), plain
  // grouping, null keys kept.
  if (agg->step() != core::AggregationNode::Step::kPartial ||
      agg->groupingKeys().empty() || !agg->preGroupedKeys().empty() ||
      !agg->globalGroupingSets().empty() || agg->groupId().has_value() ||
      agg->ignoreNullKeys()) {
    return std::nullopt;
  }

  // The join each grouping key comes from.
  std::vector<KeySource> keySources;
  size_t lowest = 0;
  for (const auto& key : agg->groupingKeys()) {
    if (!isCodableKind(key->type())) {
      return std::nullopt;
    }
    auto source = findKeySource(chain, key->name());
    if (!source.has_value()) {
      return std::nullopt;
    }
    lowest = std::max(lowest, source->chainIndex);
    keySources.push_back(std::move(*source));
  }

  // The joins from the lowest source up: inner, not null-aware; a right join
  // only as the single join (its unmatched build rows reach the aggregation
  // with null probe columns, as without reuse).
  JoinAggReuseSpec spec;
  spec.aggregationNodeId = agg->id();
  std::vector<size_t> joinIndexes; // Bottom-up.
  for (size_t k = lowest + 1; k-- > 0;) {
    auto join = std::dynamic_pointer_cast<const core::HashJoinNode>(chain[k]);
    if (join == nullptr) {
      continue;
    }
    if (join->isNullAware() || join->sources().size() != 2 ||
        !(join->isInnerJoin() || join->isRightJoin())) {
      return std::nullopt;
    }
    joinIndexes.push_back(k);
    spec.chainJoinIds.push_back(join->id());
  }
  for (size_t k = 0; k < joinIndexes.size(); ++k) {
    const auto& join =
        static_cast<const core::HashJoinNode&>(*chain[joinIndexes[k]]);
    if (join.isRightJoin() && joinIndexes.size() > 1) {
      return std::nullopt;
    }
  }

  // Sources, bottom-up.
  for (size_t position = 0; position < joinIndexes.size(); ++position) {
    const auto index = joinIndexes[position];
    const auto& join = static_cast<const core::HashJoinNode&>(*chain[index]);
    JoinAggReuseSource source;
    for (size_t g = 0; g < keySources.size(); ++g) {
      const auto& keySource = keySources[g];
      if (keySource.chainIndex != index) {
        continue;
      }
      if (keySource.buildKey >= 0 && join.isRightJoin()) {
        // An unmatched build row has a null probe key.
        return std::nullopt;
      }
      source.groupKeys.push_back(static_cast<int32_t>(g));
      source.groupKeyColumns.push_back(keySource.column);
      source.groupKeyBuildKeys.push_back(keySource.buildKey);
    }
    if (source.groupKeys.empty()) {
      continue;
    }
    // The build row of a source determines its grouping values; a filter
    // would make its matches depend on the probe row as well.
    if (join.filter() != nullptr) {
      return std::nullopt;
    }
    source.joinNodeId = join.id();
    source.chainPosition = static_cast<int32_t>(position);
    source.keysCoverJoinKeys = true;
    for (size_t k = 0; k < join.rightKeys().size(); ++k) {
      bool covered = false;
      for (size_t i = 0; i < source.groupKeys.size(); ++i) {
        covered |= source.groupKeyBuildKeys[i] == static_cast<int32_t>(k) ||
            source.groupKeyColumns[i] == join.rightKeys()[k]->name();
      }
      source.keysCoverJoinKeys &= covered;
    }
    // Build columns needed above the join only as grouping keys are not
    // extracted (null constants); the others are, as without reuse.
    const auto& buildType = join.sources()[1]->outputType();
    for (const auto& name : join.outputType()->names()) {
      if (buildType->getChildIdxIfExists(name).has_value() &&
          onlyGroupingUse(chain, index, *agg, name)) {
        source.nullableBuildColumns.insert(name);
      }
    }
    spec.sources.push_back(std::move(source));
  }

  if (spec.sources.size() > 4 ||
      !analyzeAggregates(*agg, chain[0]->outputType(), spec)) {
    return std::nullopt;
  }
  return spec;
}

// Relaxed atomics on shared slots (rows mode: several drivers update the
// same build row concurrently), plain updates on a driver's own slots.
inline double bitsToDouble(int64_t bits) {
  double d;
  std::memcpy(&d, &bits, sizeof(d));
  return d;
}

inline int64_t doubleToBits(double d) {
  int64_t bits;
  std::memcpy(&bits, &d, sizeof(bits));
  return bits;
}

template <bool kAtomic>
inline void addInt(int64_t* slot, int64_t value) {
  if constexpr (kAtomic) {
    __atomic_fetch_add(slot, value, __ATOMIC_RELAXED);
  } else {
    *slot += value;
  }
}

template <bool kAtomic>
inline void addDouble(int64_t* slot, double value) {
  if constexpr (kAtomic) {
    int64_t current = __atomic_load_n(slot, __ATOMIC_RELAXED);
    while (!__atomic_compare_exchange_n(
        slot,
        &current,
        doubleToBits(bitsToDouble(current) + value),
        true,
        __ATOMIC_RELAXED,
        __ATOMIC_RELAXED)) {
    }
  } else {
    *slot = doubleToBits(bitsToDouble(*slot) + value);
  }
}

template <bool kAtomic, bool kMin>
inline void minMaxInt(int64_t* slot, int64_t value) {
  if constexpr (kAtomic) {
    int64_t current = __atomic_load_n(slot, __ATOMIC_RELAXED);
    while ((kMin ? value < current : value > current) &&
           !__atomic_compare_exchange_n(
               slot,
               &current,
               value,
               true,
               __ATOMIC_RELAXED,
               __ATOMIC_RELAXED)) {
    }
  } else if (kMin ? value < *slot : value > *slot) {
    *slot = value;
  }
}

template <bool kAtomic, bool kMin>
inline void minMaxDouble(int64_t* slot, double value) {
  if constexpr (kAtomic) {
    int64_t current = __atomic_load_n(slot, __ATOMIC_RELAXED);
    while ((kMin ? value < bitsToDouble(current)
                 : value > bitsToDouble(current)) &&
           !__atomic_compare_exchange_n(
               slot,
               &current,
               doubleToBits(value),
               true,
               __ATOMIC_RELAXED,
               __ATOMIC_RELAXED)) {
    }
  } else if (kMin ? value < bitsToDouble(*slot) : value > bitsToDouble(*slot)) {
    *slot = doubleToBits(value);
  }
}

// An integer-kind value of 'decoded' as int64.
inline int64_t readInt(
    const DecodedVector& decoded,
    vector_size_t row,
    TypeKind kind) {
  switch (kind) {
    case TypeKind::BIGINT:
      return decoded.valueAt<int64_t>(row);
    case TypeKind::INTEGER:
      return decoded.valueAt<int32_t>(row);
    case TypeKind::SMALLINT:
      return decoded.valueAt<int16_t>(row);
    default:
      return decoded.valueAt<int8_t>(row);
  }
}

template <bool kAtomic>
void accumulateInto(
    const JoinAggReuseSpec& spec,
    const RowVectorPtr& input,
    const std::vector<int64_t*>& slots,
    std::vector<DecodedVector>& decodedInputs) {
  const auto numRows = input->size();
  decodedInputs.resize(spec.aggs.size());
  for (size_t ai = 0; ai < spec.aggs.size(); ++ai) {
    const auto& a = spec.aggs[ai];
    if (a.kind == JoinAggReuseAgg::Kind::kCountStar) {
      for (vector_size_t i = 0; i < numRows; ++i) {
        addInt<kAtomic>(slots[i] + a.slot, 1);
      }
      continue;
    }
    auto& decoded = decodedInputs[ai];
    decoded.decode(*input->childAt(a.inputChannel));
    const bool mayHaveNulls = decoded.mayHaveNulls();
    if (a.decimal) {
      // As DecimalAggregate::updateNonNullValue: 128-bit sum with an
      // overflow count, and the number of values. Codes mode only.
      if constexpr (kAtomic) {
        BOLT_UNREACHABLE();
      } else {
        const bool isShort = a.inputType->isShortDecimal();
        for (vector_size_t i = 0; i < numRows; ++i) {
          if (mayHaveNulls && decoded.isNullAt(i)) {
            continue;
          }
          const int128_t value = isShort
              ? static_cast<int128_t>(decoded.valueAt<int64_t>(i))
              : decoded.valueAt<int128_t>(i);
          int64_t* s = slots[i] + a.slot;
          int128_t sum = HugeInt::build(
              static_cast<uint64_t>(s[1]), static_cast<uint64_t>(s[0]));
          s[3] += DecimalUtil::addWithOverflow(sum, sum, value);
          s[0] = static_cast<int64_t>(static_cast<uint64_t>(sum));
          s[1] = static_cast<int64_t>(sum >> 64);
          s[2] += 1;
        }
      }
      continue;
    }
    if (a.kind == JoinAggReuseAgg::Kind::kCount) {
      for (vector_size_t i = 0; i < numRows; ++i) {
        if (!mayHaveNulls || !decoded.isNullAt(i)) {
          addInt<kAtomic>(slots[i] + a.slot, 1);
        }
      }
      continue;
    }
    const auto kind = a.inputType->kind();
    const bool isDouble = kind == TypeKind::DOUBLE;
    for (vector_size_t i = 0; i < numRows; ++i) {
      if (mayHaveNulls && decoded.isNullAt(i)) {
        continue;
      }
      int64_t* value = slots[i] + a.slot;
      if (a.kind == JoinAggReuseAgg::Kind::kAvg) {
        addDouble<kAtomic>(
            value,
            isDouble ? decoded.valueAt<double>(i)
                     : static_cast<double>(readInt(decoded, i, kind)));
      } else if (isDouble) {
        const double v = decoded.valueAt<double>(i);
        switch (a.kind) {
          case JoinAggReuseAgg::Kind::kSum:
            addDouble<kAtomic>(value, v);
            break;
          case JoinAggReuseAgg::Kind::kMin:
            minMaxDouble<kAtomic, true>(value, v);
            break;
          default:
            minMaxDouble<kAtomic, false>(value, v);
        }
      } else {
        const int64_t v = readInt(decoded, i, kind);
        switch (a.kind) {
          case JoinAggReuseAgg::Kind::kSum:
            addInt<kAtomic>(value, v);
            break;
          case JoinAggReuseAgg::Kind::kMin:
            minMaxInt<kAtomic, true>(value, v);
            break;
          default:
            minMaxInt<kAtomic, false>(value, v);
        }
      }
      addInt<kAtomic>(value + 1, 1);
    }
  }
}

// Appends a comparable encoding of row 'row' of 'decoded' to 'key'.
void appendValue(
    const DecodedVector& decoded,
    TypeKind kind,
    vector_size_t row,
    std::string& key) {
  if (decoded.isNullAt(row)) {
    key.push_back('\0');
    return;
  }
  key.push_back('\1');
  auto append = [&](const auto& value) {
    key.append(reinterpret_cast<const char*>(&value), sizeof(value));
  };
  switch (kind) {
    case TypeKind::BOOLEAN:
      key.push_back(decoded.valueAt<bool>(row) ? '\1' : '\0');
      break;
    case TypeKind::TINYINT:
      append(decoded.valueAt<int8_t>(row));
      break;
    case TypeKind::SMALLINT:
      append(decoded.valueAt<int16_t>(row));
      break;
    case TypeKind::INTEGER:
      append(decoded.valueAt<int32_t>(row));
      break;
    case TypeKind::BIGINT:
      append(decoded.valueAt<int64_t>(row));
      break;
    case TypeKind::HUGEINT:
      append(decoded.valueAt<int128_t>(row));
      break;
    case TypeKind::REAL:
      append(decoded.valueAt<float>(row));
      break;
    case TypeKind::DOUBLE:
      append(decoded.valueAt<double>(row));
      break;
    case TypeKind::TIMESTAMP:
      append(decoded.valueAt<Timestamp>(row));
      break;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY: {
      const auto value = decoded.valueAt<StringView>(row);
      const uint32_t size = value.size();
      append(size);
      key.append(value.data(), size);
      break;
    }
    default:
      BOLT_UNREACHABLE();
  }
}
} // namespace

std::vector<int64_t> JoinAggReuseSpec::slotInitValues() const {
  std::vector<int64_t> init(numSlots, 0);
  for (const auto& a : aggs) {
    if (a.kind != JoinAggReuseAgg::Kind::kMin &&
        a.kind != JoinAggReuseAgg::Kind::kMax) {
      continue;
    }
    const bool isMin = a.kind == JoinAggReuseAgg::Kind::kMin;
    if (a.inputType->kind() == TypeKind::DOUBLE) {
      init[a.slot] = doubleToBits(
          isMin ? std::numeric_limits<double>::infinity()
                : -std::numeric_limits<double>::infinity());
    } else {
      init[a.slot] = isMin ? std::numeric_limits<int64_t>::max()
                           : std::numeric_limits<int64_t>::min();
    }
  }
  return init;
}

void JoinAggReusePlan::add(JoinAggReuseSpec spec) {
  specs_.push_back(std::make_unique<JoinAggReuseSpec>(std::move(spec)));
  const auto* added = specs_.back().get();
  if (added->rightDeep.has_value()) {
    auto& byConsumer = added->rightDeep->toOrderBy ? byOrderBy_ : byBuild_;
    byConsumer[added->rightDeep->buildJoinNodeId] = added;
  } else {
    byAggregation_[added->aggregationNodeId] = added;
  }
  for (size_t p = 0; p < added->chainJoinIds.size(); ++p) {
    Member member{added, static_cast<int32_t>(p), -1};
    for (size_t s = 0; s < added->sources.size(); ++s) {
      if (added->sources[s].chainPosition == static_cast<int32_t>(p)) {
        member.source = static_cast<int32_t>(s);
      }
    }
    byJoin_[added->chainJoinIds[p]] = member;
  }
}

std::vector<JoinAggReuseSpec> analyzeJoinAggReuse(
    const std::vector<core::PlanNodePtr>& nodes) {
  std::unordered_set<const core::PlanNode*> inPipeline;
  for (const auto& node : nodes) {
    inPipeline.insert(node.get());
  }
  std::vector<JoinAggReuseSpec> specs;
  for (const auto& node : nodes) {
    auto agg = std::dynamic_pointer_cast<const core::AggregationNode>(node);
    if (agg == nullptr) {
      continue;
    }
    // Projections and joins below the aggregation in this pipeline.
    std::vector<core::PlanNodePtr> chain;
    for (auto source = agg->sources()[0];
         inPipeline.count(source.get()) > 0 &&
         (std::dynamic_pointer_cast<const core::ProjectNode>(source) ||
          std::dynamic_pointer_cast<const core::HashJoinNode>(source));
         source = source->sources()[0]) {
      chain.push_back(source);
    }
    if (auto spec = analyzeAggregation(agg, chain)) {
      std::string sources;
      for (const auto& source : spec->sources) {
        sources += (sources.empty() ? "" : ", ") + source.joinNodeId + " (" +
            std::to_string(source.groupKeys.size()) + " keys, " +
            std::to_string(source.nullableBuildColumns.size()) +
            " columns not extracted)";
      }
      LOG(INFO) << "[join-agg-reuse] aggregation " << agg->id()
                << " may reuse joins " << sources << "; "
                << spec->chainJoinIds.size() << " joins in the chain, "
                << spec->aggs.size() << " aggregates";
      specs.push_back(std::move(*spec));
    }
  }
  return specs;
}

namespace {
// Build columns of 'join' in its hash table order: keys, then the other build
// columns (as HashBuild / HashProbe lay out the table).
RowTypePtr joinTableType(const core::HashJoinNode& join) {
  const auto& buildType = join.sources()[1]->outputType();
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::unordered_set<std::string> keys;
  for (const auto& key : join.rightKeys()) {
    names.push_back(key->name());
    types.push_back(key->type());
    keys.insert(key->name());
  }
  for (size_t i = 0; i < buildType->size(); ++i) {
    if (keys.count(buildType->nameOf(i)) == 0) {
      names.push_back(buildType->nameOf(i));
      types.push_back(buildType->childAt(i));
    }
  }
  return ROW(std::move(names), std::move(types));
}

// The columns of 'consumerInput' that the consumer ('consumerId', an OrderBy if
// 'toOrderBy', else a join's build) can carry as a reference to the build row
// of the nearest join below it in the pipeline (J1, through projections):
// J1's build columns passed through unchanged, except 'excluded'.
std::optional<JoinAggReuseSpec> deferBuildColumns(
    const std::vector<core::PlanNodePtr>& nodes,
    const core::PlanNodePtr& consumerInput,
    const std::unordered_set<std::string>& excluded,
    const JoinAggReusePlan& plan,
    const core::PlanNodeId& consumerId,
    bool toOrderBy) {
  // Projections down to the nearest join (J1) below the consumer.
  std::vector<core::PlanNodePtr> chain;
  std::shared_ptr<const core::HashJoinNode> sourceJoin;
  for (auto node = consumerInput;;) {
    if (std::dynamic_pointer_cast<const core::ProjectNode>(node)) {
      chain.push_back(node);
      node = node->sources()[0];
      continue;
    }
    sourceJoin = std::dynamic_pointer_cast<const core::HashJoinNode>(node);
    break;
  }
  const bool inPipeline = std::any_of(
      nodes.begin(), nodes.end(), [&](const core::PlanNodePtr& node) {
        return node.get() == sourceJoin.get();
      });
  if (sourceJoin == nullptr || !inPipeline || !sourceJoin->isInnerJoin() ||
      sourceJoin->isNullAware() || sourceJoin->sources().size() != 2 ||
      plan.forJoin(sourceJoin->id()).spec != nullptr) {
    return std::nullopt;
  }
  chain.push_back(sourceJoin);
  const size_t j = chain.size() - 1;

  // Consumer input columns that are J1 build columns passed through unchanged
  // and that the consumer does not need as values ('excluded').
  const auto sourceTable = joinTableType(*sourceJoin);
  const auto& consumerType = consumerInput->outputType();
  JoinAggReuseSpec spec;
  JoinAggReuseSpec::RightDeep rightDeep;
  rightDeep.buildJoinNodeId = consumerId;
  rightDeep.toOrderBy = toOrderBy;
  rightDeep.refColumn = "$jar_ref$" + sourceJoin->id();
  std::unordered_set<std::string> sourceColumns;
  for (const auto& name : consumerType->names()) {
    if (excluded.count(name) > 0) {
      continue;
    }
    auto source = findKeySource(chain, name);
    if (!source.has_value() || source->chainIndex != j ||
        source->buildKey >= 0) {
      continue;
    }
    rightDeep.deferredColumns.push_back(name);
    rightDeep.sourceTableChannels.push_back(
        sourceTable->getChildIdx(source->column));
    sourceColumns.insert(source->column);
  }
  if (rightDeep.deferredColumns.empty()) {
    return std::nullopt;
  }

  // J1 need not extract a column that only reaches the deferred columns.
  JoinAggReuseSource source;
  source.joinNodeId = sourceJoin->id();
  const std::unordered_set<std::string> deferred(
      rightDeep.deferredColumns.begin(), rightDeep.deferredColumns.end());
  for (const auto& column : sourceColumns) {
    std::unordered_set<std::string> names{column};
    bool onlyDeferred = true;
    for (size_t k = j; k-- > 0 && onlyDeferred;) {
      const auto& project =
          static_cast<const core::ProjectNode&>(*chain[k]);
      std::unordered_set<std::string> passed;
      for (size_t i = 0; i < project.projections().size(); ++i) {
        const auto& expr = project.projections()[i];
        auto field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expr);
        if (field != nullptr && field->isInputColumn()) {
          if (names.count(field->name()) > 0) {
            passed.insert(project.names()[i]);
          }
        } else if (references(expr, names)) {
          onlyDeferred = false;
        }
      }
      names = std::move(passed);
    }
    for (const auto& name : names) {
      onlyDeferred &= deferred.count(name) > 0;
    }
    if (onlyDeferred) {
      source.nullableBuildColumns.insert(column);
    }
  }
  spec.chainJoinIds.push_back(sourceJoin->id());
  spec.sources.push_back(std::move(source));
  spec.rightDeep = std::move(rightDeep);
  LOG(INFO) << "[join-agg-reuse] "
            << (toOrderBy ? "OrderBy " : "build of join ") << consumerId
            << " stores references to rows of join " << sourceJoin->id()
            << " for " << spec.rightDeep->deferredColumns.size()
            << " columns (" << spec.sources[0].nullableBuildColumns.size()
            << " not extracted)";
  return spec;
}
} // namespace

std::optional<JoinAggReuseSpec> analyzeRightDeep(
    const std::vector<core::PlanNodePtr>& nodes,
    const core::PlanNodePtr& consumerNode,
    const JoinAggReusePlan& plan) {
  // A build pipeline: its nodes are the build side of the join that consumes
  // it (the join's HashBuild ends the pipeline).
  auto buildJoin =
      std::dynamic_pointer_cast<const core::HashJoinNode>(consumerNode);
  if (nodes.empty() || buildJoin == nullptr ||
      buildJoin->sources().size() != 2 ||
      buildJoin->sources()[1].get() != nodes.back().get() ||
      plan.forJoin(buildJoin->id()).source >= 0 ||
      !(buildJoin->isInnerJoin() || buildJoin->isLeftJoin() ||
        buildJoin->isRightJoin() || buildJoin->isFullJoin())) {
    return std::nullopt;
  }
  // J2 build columns that are neither J2 keys nor read by J2's filter.
  std::unordered_set<std::string> excluded;
  for (const auto& key : buildJoin->rightKeys()) {
    excluded.insert(key->name());
  }
  if (buildJoin->filter() != nullptr) {
    collectInputColumns(buildJoin->filter(), excluded);
  }
  return deferBuildColumns(
      nodes,
      buildJoin->sources()[1],
      excluded,
      plan,
      buildJoin->id(),
      /*toOrderBy=*/false);
}

std::vector<JoinAggReuseSpec> analyzeJoinSort(
    const std::vector<core::PlanNodePtr>& nodes,
    const JoinAggReusePlan& plan) {
  std::vector<JoinAggReuseSpec> specs;
  for (const auto& node : nodes) {
    auto orderBy = std::dynamic_pointer_cast<const core::OrderByNode>(node);
    if (orderBy == nullptr) {
      continue;
    }
    // The sort keys are compared, so they stay values.
    std::unordered_set<std::string> excluded;
    for (const auto& key : orderBy->sortingKeys()) {
      excluded.insert(key->name());
    }
    if (auto spec = deferBuildColumns(
            nodes,
            orderBy->sources()[0],
            excluded,
            plan,
            orderBy->id(),
            /*toOrderBy=*/true)) {
      specs.push_back(std::move(*spec));
    }
  }
  return specs;
}

namespace {
// True if 'node' is a grouping-only aggregation of 'step' whose grouping keys
// are exactly 'keys' (any order).
bool isDistinctOn(
    const core::PlanNodePtr& node,
    core::AggregationNode::Step step,
    const std::vector<core::FieldAccessTypedExprPtr>& keys) {
  auto agg = std::dynamic_pointer_cast<const core::AggregationNode>(node);
  if (agg == nullptr || agg->step() != step || !agg->aggregates().empty() ||
      agg->groupingKeys().size() != keys.size() ||
      !agg->globalGroupingSets().empty() || agg->groupId().has_value()) {
    return false;
  }
  std::unordered_set<std::string> names;
  for (const auto& key : agg->groupingKeys()) {
    names.insert(key->name());
  }
  for (const auto& key : keys) {
    if (names.count(key->name()) == 0) {
      return false;
    }
  }
  return true;
}

void findRedundantDistincts(
    const core::PlanNodePtr& node,
    std::unordered_set<const core::PlanNode*>& visited,
    std::vector<core::PlanNodeId>& ids) {
  if (!visited.insert(node.get()).second) {
    return;
  }
  for (const auto& source : node->sources()) {
    findRedundantDistincts(source, visited, ids);
  }
  auto join = std::dynamic_pointer_cast<const core::HashJoinNode>(node);
  if (join == nullptr || !join->isRightSemiFilterJoin() ||
      join->filter() != nullptr || join->isNullAware()) {
    return;
  }
  const auto& keys = join->leftKeys();
  const auto& finalAgg = join->sources()[0];
  if (!isDistinctOn(finalAgg, core::AggregationNode::Step::kFinal, keys)) {
    return;
  }
  auto exchange = std::dynamic_pointer_cast<const core::LocalPartitionNode>(
      finalAgg->sources()[0]);
  if (exchange == nullptr || exchange->sources().size() != 1) {
    return;
  }
  const auto& partialAgg = exchange->sources()[0];
  if (!isDistinctOn(partialAgg, core::AggregationNode::Step::kPartial, keys)) {
    return;
  }
  LOG(INFO) << "[join-agg-reuse] right-semi join " << join->id()
            << " makes distinct aggregations " << partialAgg->id() << "/"
            << finalAgg->id() << " pass-through";
  ids.push_back(partialAgg->id());
  ids.push_back(finalAgg->id());
}
} // namespace

std::vector<core::PlanNodeId> findRedundantDistincts(
    const core::PlanNodePtr& root) {
  std::unordered_set<const core::PlanNode*> visited;
  std::vector<core::PlanNodeId> ids;
  findRedundantDistincts(root, visited, ids);
  return ids;
}

JoinAggReuseRowIds::JoinAggReuseRowIds(const BaseHashTable& table)
    : rows(table.rows()) {}

JoinAggReuseState::JoinAggReuseState(
    const JoinAggReuseSpec& spec,
    const BaseHashTable& table,
    memory::MemoryPool* pool)
    : numSlots_(spec.numSlots), rowIds_(table), slots_(256, nullptr) {
  if (!table.rows()->accumulators().empty()) {
    // Reserved and initialized by HashBuild: the accumulator column right
    // after the keys.
    inRowOffset_ = table.rows()->columnAt(table.hashers().size()).offset();
    return;
  }
  const auto init = spec.slotInitValues();
  const bool allZero =
      std::all_of(init.begin(), init.end(), [](int64_t v) { return v == 0; });
  for (auto* container : table.allHybridContainers()) {
    const auto numRows = static_cast<size_t>(container->getNumRows());
    auto buffer = AlignedBuffer::allocate<int64_t>(
        std::max<size_t>(1, numRows * numSlots_), pool);
    auto* raw = buffer->asMutable<int64_t>();
    if (allZero) {
      std::memset(raw, 0, numRows * numSlots_ * sizeof(int64_t));
    } else {
      for (size_t r = 0; r < numRows; ++r) {
        std::memcpy(
            raw + r * numSlots_, init.data(), init.size() * sizeof(int64_t));
      }
    }
    slots_[container->getId()] = raw;
    buffers_.push_back(std::move(buffer));
  }
}

void JoinAggReuseState::addTouched(std::vector<char*>&& rows) {
  std::lock_guard<std::mutex> l(mutex_);
  touched_.push_back(std::move(rows));
}

std::vector<char*> JoinAggReuseState::takeTouched() {
  std::lock_guard<std::mutex> l(mutex_);
  std::vector<char*> all;
  size_t total = 0;
  for (const auto& rows : touched_) {
    total += rows.size();
  }
  all.reserve(total);
  for (auto& rows : touched_) {
    all.insert(all.end(), rows.begin(), rows.end());
  }
  touched_.clear();
  return all;
}

JoinAggReuseCodes::JoinAggReuseCodes(
    BaseHashTable& table,
    const std::vector<IdentityProjection>& projections)
    : table_(table), rowIds_(table), codes_(256) {
  for (auto* container : table.allHybridContainers()) {
    codes_[container->getId()].assign(container->getNumRows(), -1);
  }
  // The grouping columns, extracted into channels 0..n-1.
  const auto columnTypes = table.hybridData()->columnTypes();
  for (size_t i = 0; i < projections.size(); ++i) {
    types_.push_back(columnTypes[projections[i].inputChannel]);
    projections_.emplace_back(
        projections[i].inputChannel, static_cast<column_index_t>(i));
  }
}

void JoinAggReuseCodes::ensure(
    const std::vector<char*>& rows,
    memory::MemoryPool* pool) {
  static thread_local std::vector<char*> missing;
  missing.clear();
  for (auto* row : rows) {
    if (codeOf(row) < 0) {
      missing.push_back(row);
    }
  }
  if (missing.empty()) {
    return;
  }
  // Values and their encodings, without the lock.
  std::vector<VectorPtr> values(types_.size());
  HashProbe::extractTableColumns(
      &table_,
      folly::Range<char**>(missing.data(), missing.size()),
      projections_,
      pool,
      types_,
      values);
  std::vector<DecodedVector> decoded(types_.size());
  for (size_t c = 0; c < types_.size(); ++c) {
    decoded[c].decode(*values[c]);
  }
  static thread_local std::vector<std::string> keys;
  keys.resize(missing.size());
  for (size_t r = 0; r < missing.size(); ++r) {
    keys[r].clear();
    for (size_t c = 0; c < types_.size(); ++c) {
      appendValue(decoded[c], types_[c]->kind(), r, keys[r]);
    }
  }
  std::lock_guard<std::mutex> l(mutex_);
  for (size_t r = 0; r < missing.size(); ++r) {
    const auto id = rowIds_.idOf(missing[r]);
    auto* code = &codes_[JoinAggReuseRowIds::containerOf(id)]
                        [JoinAggReuseRowIds::indexOf(id)];
    if (*code >= 0) {
      continue; // Another driver coded it meanwhile.
    }
    auto [it, inserted] = index_.try_emplace(
        std::move(keys[r]), static_cast<int32_t>(representatives_.size()));
    if (inserted) {
      representatives_.push_back(missing[r]);
    }
    __atomic_store_n(code, it->second, __ATOMIC_RELAXED);
  }
}

void JoinAggReuseCodes::representatives(
    const int32_t* codes,
    int32_t count,
    char** rows) const {
  std::lock_guard<std::mutex> l(mutex_);
  for (int32_t i = 0; i < count; ++i) {
    rows[i] = representatives_[codes[i]];
  }
}

int32_t JoinAggReuseCodes::numCodes() const {
  std::lock_guard<std::mutex> l(mutex_);
  return static_cast<int32_t>(representatives_.size());
}

size_t JoinAggReuseConsumer::GroupKeyHash::operator()(
    const GroupKey& key) const {
  return folly::hash::hash_range(key.codes.begin(), key.codes.end());
}

JoinAggReuseConsumer::JoinAggReuseConsumer(
    const JoinAggReuseSpec& spec,
    std::vector<HashProbe*> sources,
    HashProbe* rowsFrom)
    : spec_(spec), sources_(std::move(sources)), rowsFrom_(rowsFrom) {}

bool JoinAggReuseConsumer::active() const {
  // All sources decide alike (hybrid builds, same configuration).
  return sources_[0]->joinAggReuseActive();
}

bool JoinAggReuseConsumer::rowsMode() const {
  return sources_[0]->joinAggReuseRowsMode();
}

void JoinAggReuseConsumer::addInput(Operator& op, const RowVectorPtr& input) {
  auto& channels = rowsFrom_->joinAggReuseChannels();
  BOLT_CHECK_EQ(channels.size(), sources_.size());
  for (const auto& channel : channels) {
    BOLT_CHECK_EQ(
        channel.size(),
        input->size(),
        "join-agg reuse: aggregation input is not the chain's last output");
  }
  numInputRows_ += input->size();
  if (rowsMode()) {
    addInputRows(op, input);
  } else {
    addInputCodes(op, input);
  }
  channels.clear();
}

void JoinAggReuseConsumer::addInputRows(
    Operator& op,
    const RowVectorPtr& input) {
  if (state_ == nullptr) {
    table_ = sources_[0]->joinAggReuseTable();
    BOLT_CHECK_NOT_NULL(table_);
    // The first driver allocates the slots, charged to its aggregation.
    state_ = table_->joinAggReuseState([&]() {
      return std::make_shared<JoinAggReuseState>(spec_, *table_, op.pool());
    });
  }
  const auto& buildRows = rowsFrom_->joinAggReuseChannels()[0];
  const auto numRows = input->size();
  // Slot addresses; the first driver to touch a build row records it.
  static thread_local std::vector<int64_t*> slots;
  slots.resize(numRows);
  for (vector_size_t i = 0; i < numRows; ++i) {
    int64_t* s = state_->slotsOf(buildRows[i]);
    slots[i] = s;
    if (__atomic_load_n(s, __ATOMIC_RELAXED) == 0 &&
        __atomic_exchange_n(s, 1, __ATOMIC_RELAXED) == 0) {
      touched_.push_back(buildRows[i]);
    }
  }
  accumulateInto<true>(spec_, input, slots, decoded_);
}

void JoinAggReuseConsumer::addInputCodes(
    Operator& op,
    const RowVectorPtr& input) {
  const auto numSources = sources_.size();
  if (codes_.empty()) {
    initValues_ = spec_.slotInitValues();
    initValues_[0] = 1; // Touched.
    for (size_t s = 0; s < numSources; ++s) {
      codes_.push_back(sources_[s]->joinAggReuseCodes());
      sourceTables_.push_back(sources_[s]->joinAggReuseTable());
      BOLT_CHECK_NOT_NULL(codes_.back());
    }
  }
  const auto& channels = rowsFrom_->joinAggReuseChannels();
  for (size_t s = 0; s < numSources; ++s) {
    codes_[s]->ensure(channels[s], op.pool());
  }

  // Group of each row; new groups get their slots at the end.
  const auto numRows = input->size();
  static thread_local std::vector<uint32_t> indexes;
  indexes.resize(numRows);
  GroupKey key;
  for (vector_size_t i = 0; i < numRows; ++i) {
    for (size_t s = 0; s < numSources; ++s) {
      key.codes[s] = codes_[s]->codeOf(channels[s][i]);
    }
    auto [it, inserted] = groupIndex_.try_emplace(
        key, static_cast<uint32_t>(groupKeys_.size()));
    if (inserted) {
      groupKeys_.push_back(key);
      groupSlots_.insert(
          groupSlots_.end(), initValues_.begin(), initValues_.end());
    }
    indexes[i] = it->second;
  }
  static thread_local std::vector<int64_t*> slots;
  slots.resize(numRows);
  for (vector_size_t i = 0; i < numRows; ++i) {
    slots[i] = groupSlots_.data() + indexes[i] * spec_.numSlots;
  }
  accumulateInto<false>(spec_, input, slots, decoded_);
}

void JoinAggReuseConsumer::noMoreInput(Operator& op) {
  op.addRuntimeStat("joinAggReuseInputRows", RuntimeCounter(numInputRows_));
  if (!rowsMode()) {
    op.addRuntimeStat("joinAggReuseGroups", RuntimeCounter(groupKeys_.size()));
    int64_t numCodes = 0;
    for (const auto& codes : codes_) {
      numCodes += codes->numCodes();
    }
    op.addRuntimeStat("joinAggReuseCodes", RuntimeCounter(numCodes));
    return;
  }
  if (state_ != nullptr) {
    state_->addTouched(std::move(touched_));
  }
  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<Driver>> peers;
  emitter_ = op.operatorCtx()->task()->allPeersFinished(
      op.planNodeId(), op.operatorCtx()->driver(), nullptr, promises, peers);
  BOLT_CHECK(promises.empty());
  if (!emitter_) {
    return;
  }
  // All drivers have added their rows (the barrier orders their updates
  // before this point). No state means no driver received a matched row.
  table_ = sources_[0]->joinAggReuseTable();
  if (table_ != nullptr && state_ == nullptr) {
    state_ = table_->peekJoinAggReuseState();
  }
  if (state_ != nullptr) {
    emitRows_ = state_->takeTouched();
  }
  op.addRuntimeStat("joinAggReuseGroups", RuntimeCounter(emitRows_.size()));
}

RowVectorPtr JoinAggReuseConsumer::getOutput(
    Operator& op,
    const RowTypePtr& outputType,
    int32_t maxRows) {
  return rowsMode() ? outputRows(op, outputType, maxRows)
                    : outputCodes(op, outputType, maxRows);
}

RowVectorPtr JoinAggReuseConsumer::outputRows(
    Operator& op,
    const RowTypePtr& outputType,
    int32_t maxRows) {
  if (!emitter_ || emitPosition_ >= emitRows_.size()) {
    return nullptr;
  }
  const auto numOut = static_cast<vector_size_t>(
      std::min<size_t>(maxRows, emitRows_.size() - emitPosition_));
  char** rows = emitRows_.data() + emitPosition_;
  emitPosition_ += numOut;

  auto result = std::static_pointer_cast<RowVector>(
      BaseVector::create(outputType, numOut, op.pool()));
  // Grouping keys, from the join's key rows and HybridContainer.
  HashProbe::extractTableColumns(
      table_.get(),
      folly::Range<char**>(rows, numOut),
      sources_[0]->joinAggReuseKeyProjections(),
      op.pool(),
      outputType->children(),
      result->children());
  std::vector<const int64_t*> slots(numOut);
  for (vector_size_t i = 0; i < numOut; ++i) {
    slots[i] = state_->slotsOf(rows[i]);
  }
  fillAggregates(op, result, slots);
  return result;
}

RowVectorPtr JoinAggReuseConsumer::outputCodes(
    Operator& op,
    const RowTypePtr& outputType,
    int32_t maxRows) {
  if (emitPosition_ >= groupKeys_.size()) {
    return nullptr;
  }
  const auto first = emitPosition_;
  const auto numOut = static_cast<vector_size_t>(
      std::min<size_t>(maxRows, groupKeys_.size() - first));
  emitPosition_ += numOut;

  auto result = std::static_pointer_cast<RowVector>(
      BaseVector::create(outputType, numOut, op.pool()));
  // Grouping keys of each source, from a build row with the group's code.
  std::vector<int32_t> codes(numOut);
  std::vector<char*> rows(numOut);
  for (size_t s = 0; s < sources_.size(); ++s) {
    for (vector_size_t i = 0; i < numOut; ++i) {
      codes[i] = groupKeys_[first + i].codes[s];
    }
    codes_[s]->representatives(codes.data(), numOut, rows.data());
    HashProbe::extractTableColumns(
        sourceTables_[s].get(),
        folly::Range<char**>(rows.data(), numOut),
        sources_[s]->joinAggReuseKeyProjections(),
        op.pool(),
        outputType->children(),
        result->children());
  }
  std::vector<const int64_t*> slots(numOut);
  for (vector_size_t i = 0; i < numOut; ++i) {
    slots[i] = groupSlots_.data() + (first + i) * spec_.numSlots;
  }
  fillAggregates(op, result, slots);
  return result;
}

void JoinAggReuseConsumer::fillAggregates(
    Operator& op,
    const RowVectorPtr& result,
    const std::vector<const int64_t*>& slots) {
  const auto numOut = result->size();
  const auto numKeys = result->childrenSize() - spec_.aggs.size();
  for (size_t ai = 0; ai < spec_.aggs.size(); ++ai) {
    const auto& a = spec_.aggs[ai];
    auto& child = result->childAt(numKeys + ai);
    child = BaseVector::create(a.resultType, numOut, op.pool());
    const bool isCount = a.kind == JoinAggReuseAgg::Kind::kCountStar ||
        a.kind == JoinAggReuseAgg::Kind::kCount;
    const bool inputIsDouble =
        a.inputType != nullptr && a.inputType->kind() == TypeKind::DOUBLE;
    if (a.decimal) {
      // LongDecimalWithOverflowState::serialize: count, overflow, sum low,
      // sum high; null without values.
      auto* flat = child->asUnchecked<FlatVector<StringView>>();
      for (vector_size_t i = 0; i < numOut; ++i) {
        const int64_t* s = slots[i] + a.slot;
        if (s[2] == 0) {
          flat->setNull(i, true);
          continue;
        }
        const int64_t fields[4] = {s[2], s[3], s[0], s[1]};
        char* buffer = flat->getRawStringBufferWithSpace(sizeof(fields));
        std::memcpy(buffer, fields, sizeof(fields));
        flat->setNoCopy(i, StringView(buffer, sizeof(fields)));
      }
      continue;
    }
    if (a.kind == JoinAggReuseAgg::Kind::kAvg) {
      auto* rowChild = child->asUnchecked<RowVector>();
      auto* sums = rowChild->childAt(0)->asUnchecked<FlatVector<double>>();
      auto* counts = rowChild->childAt(1)->asUnchecked<FlatVector<int64_t>>();
      for (vector_size_t i = 0; i < numOut; ++i) {
        const int64_t* s = slots[i] + a.slot;
        if (s[1] == 0) {
          rowChild->setNull(i, true);
          continue;
        }
        sums->set(i, bitsToDouble(s[0]));
        counts->set(i, s[1]);
      }
      continue;
    }
    for (vector_size_t i = 0; i < numOut; ++i) {
      const int64_t* s = slots[i] + a.slot;
      if (!isCount && s[1] == 0) {
        child->setNull(i, true);
        continue;
      }
      const int64_t bits = s[0];
      switch (a.resultType->kind()) {
        case TypeKind::DOUBLE:
          child->asUnchecked<FlatVector<double>>()->set(
              i, inputIsDouble ? bitsToDouble(bits) : static_cast<double>(bits));
          break;
        case TypeKind::BIGINT:
          child->asUnchecked<FlatVector<int64_t>>()->set(i, bits);
          break;
        case TypeKind::INTEGER:
          child->asUnchecked<FlatVector<int32_t>>()->set(
              i, static_cast<int32_t>(bits));
          break;
        case TypeKind::SMALLINT:
          child->asUnchecked<FlatVector<int16_t>>()->set(
              i, static_cast<int16_t>(bits));
          break;
        default:
          child->asUnchecked<FlatVector<int8_t>>()->set(
              i, static_cast<int8_t>(bits));
      }
    }
  }
}

} // namespace bytedance::bolt::exec

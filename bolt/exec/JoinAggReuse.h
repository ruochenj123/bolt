/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <array>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <folly/container/F14Map.h>

#include "bolt/buffer/Buffer.h"
#include "bolt/core/PlanNode.h"
#include "bolt/exec/Operator.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/vector/DecodedVector.h"

namespace bytedance::bolt::exec {

class BaseHashTable;
class HashProbe;
class RowContainer;

/// Join-aggregation reuse: a partial aggregation above hash joins takes its
/// groups from the joins' build rows instead of hashing the grouping columns
/// the joins would extract for it. See docs/join-agg-reuse.md.
///
/// Planning only analyzes the plan: for a partial aggregation whose grouping
/// keys are build columns (or probe keys) of joins below it in the same
/// pipeline, with only projections and inner joins in between, a
/// JoinAggReuseSpec is recorded for the existing operators. The plan and its
/// operators stay as they are.
///
/// At runtime the joins that provide grouping keys (the sources) stop
/// extracting the build columns used above them only as grouping keys (null
/// constants instead) and publish their matched build rows; every join above
/// a source carries the rows up through its output row mapping. The
/// aggregation then identifies each input row's group from these build rows,
/// in one of two modes chosen from the built tables:
///  - rows: one source whose build keys are unique and whose build rows are
///    the groups (TPC-H Q17: the part row of l_partkey). Accumulators are
///    arrays indexed by the build rows' hybrid row ids, shared by all drivers
///    (relaxed atomics); the last driver emits one row per touched build row.
///  - codes: every source's build rows get a dense code for their grouping
///    values (computed once per table); a group is a combination of codes
///    (SSB Q2.1: d_year x p_brand1). Each driver aggregates into its own
///    array (or map) indexed by the combined code and emits its groups.
/// Grouping keys are read from the joins' rows and HybridContainers once per
/// group; the final aggregation downstream merges as usual.
struct JoinAggReuseAgg {
  enum class Kind { kCountStar, kCount, kSum, kMin, kMax, kAvg };
  Kind kind;
  /// Channel of the aggregate's input in the aggregation's input; -1 for
  /// count(*).
  int32_t inputChannel{-1};
  TypePtr inputType;
  /// Partial (intermediate) result type.
  TypePtr resultType;
  /// First accumulator slot of this aggregate.
  int32_t slot{0};
  /// sum/avg over DECIMAL: the partial result is the serialized
  /// LongDecimalWithOverflowState (count, overflow, 128-bit sum).
  bool decimal{false};

  /// count uses one slot; sum/min/max/avg use value + non-null count; a
  /// decimal sum/avg uses sum low, sum high, count and overflow.
  int32_t numSlots() const {
    if (decimal) {
      return 4;
    }
    return (kind == Kind::kCountStar || kind == Kind::kCount) ? 1 : 2;
  }
};

/// A single source with unique build keys whose groups are coarser than its
/// build rows (grouping keys not covering the join keys) uses codes mode up to
/// this many build rows; beyond, it likely touches enough of them that coding
/// costs more than rows mode.
constexpr uint64_t kJoinAggReuseMaxCodedRows = 1 << 20;

/// A join whose build rows provide grouping keys.
struct JoinAggReuseSource {
  core::PlanNodeId joinNodeId;
  /// Position of the join in JoinAggReuseSpec::chainJoinIds.
  int32_t chainPosition{0};
  /// For each of its grouping keys: the index among the aggregation's
  /// grouping keys, and the join output column (a build column) or, when
  /// groupKeyBuildKeys[i] >= 0, a probe join key equal to that build key.
  std::vector<int32_t> groupKeys;
  std::vector<std::string> groupKeyColumns;
  std::vector<int32_t> groupKeyBuildKeys;
  /// The grouping keys include every join key: with unique build keys each
  /// build row is exactly one group.
  bool keysCoverJoinKeys{false};
  /// Build columns used above the join only as grouping keys: not extracted.
  std::unordered_set<std::string> nullableBuildColumns;
};

struct JoinAggReuseSpec {
  core::PlanNodeId aggregationNodeId;
  /// The joins from the lowest source up to the aggregation, bottom-up. Each
  /// publishes build rows for its output rows: those it carries from the join
  /// below, then its own if it is a source.
  std::vector<core::PlanNodeId> chainJoinIds;
  /// Bottom-up; the order of the published row channels.
  std::vector<JoinAggReuseSource> sources;
  std::vector<JoinAggReuseAgg> aggs;
  /// Slot 0 marks a touched group; the aggregates' slots follow.
  int32_t numSlots{1};

  /// Right-deep reuse: the chain's rows go to the build of a join above
  /// (instead of an aggregation), which stores a reference to the build row
  /// of the source join (J1) in place of J1's build columns it only passes on;
  /// the join's probe reads those columns through the reference. The source
  /// does not extract them (nullableBuildColumns). Join-sort reuse is the same
  /// with an OrderBy above instead of a join's build: the sort carries the
  /// reference in place of those columns and reads them through it when it
  /// produces its output.
  struct RightDeep {
    /// The consumer whose input is the chain's output: the join (J2) whose
    /// build input it is, or the OrderBy if 'toOrderBy'.
    core::PlanNodeId buildJoinNodeId;
    bool toOrderBy{false};
    /// Consumer input columns stored as the reference, and for each the
    /// channel of J1's table it comes from.
    std::vector<std::string> deferredColumns;
    std::vector<column_index_t> sourceTableChannels;
    /// Name of the reference column (BIGINT) that replaces them.
    std::string refColumn;
  };
  std::optional<RightDeep> rightDeep;

  /// Initial 64-bit pattern of every slot (min/max start at the extremes).
  std::vector<int64_t> slotInitValues() const;

  /// Decimal sums have no lock-free update: such a spec runs in codes mode,
  /// where each driver has its own slots.
  bool hasDecimal() const {
    for (const auto& a : aggs) {
      if (a.decimal) {
        return true;
      }
    }
    return false;
  }
};

/// The specs of one task, looked up by aggregation and by join.
class JoinAggReusePlan {
 public:
  void add(JoinAggReuseSpec spec);

  const JoinAggReuseSpec* forAggregation(const core::PlanNodeId& id) const {
    auto it = byAggregation_.find(id);
    return it == byAggregation_.end() ? nullptr : it->second;
  }

  struct Member {
    const JoinAggReuseSpec* spec{nullptr};
    /// Position in spec->chainJoinIds.
    int32_t position{-1};
    /// Index in spec->sources, or -1 for a join that only carries rows.
    int32_t source{-1};
  };

  /// The spec in which join 'id' publishes build rows, if any.
  Member forJoin(const core::PlanNodeId& id) const {
    auto it = byJoin_.find(id);
    return it == byJoin_.end() ? Member{} : it->second;
  }

  /// The right-deep spec whose rows the build of join 'id' stores, if any.
  const JoinAggReuseSpec* forBuild(const core::PlanNodeId& id) const {
    auto it = byBuild_.find(id);
    return it == byBuild_.end() ? nullptr : it->second;
  }

  /// The join-sort spec whose rows OrderBy 'id' carries, if any.
  const JoinAggReuseSpec* forOrderBy(const core::PlanNodeId& id) const {
    auto it = byOrderBy_.find(id);
    return it == byOrderBy_.end() ? nullptr : it->second;
  }

  /// A DISTINCT aggregation made redundant by the right-semi join above it:
  /// the join marks each build row once however often its key comes, so the
  /// aggregation passes its input through (projected to its keys).
  void addPassThrough(const core::PlanNodeId& id) {
    passThrough_.insert(id);
  }

  bool isPassThrough(const core::PlanNodeId& id) const {
    return passThrough_.count(id) > 0;
  }

  size_t size() const {
    return specs_.size();
  }

  bool empty() const {
    return specs_.empty() && passThrough_.empty();
  }

 private:
  std::unordered_set<core::PlanNodeId> passThrough_;
  std::vector<std::unique_ptr<JoinAggReuseSpec>> specs_;
  std::unordered_map<core::PlanNodeId, const JoinAggReuseSpec*>
      byAggregation_;
  std::unordered_map<core::PlanNodeId, Member> byJoin_;
  std::unordered_map<core::PlanNodeId, const JoinAggReuseSpec*> byBuild_;
  std::unordered_map<core::PlanNodeId, const JoinAggReuseSpec*> byOrderBy_;
};

/// Finds the join -> aggregation shapes in the ordered nodes of one pipeline.
std::vector<JoinAggReuseSpec> analyzeJoinAggReuse(
    const std::vector<core::PlanNodePtr>& pipelineNodes);

/// Right-deep reuse in one pipeline, if it is the build pipeline of a join
/// ('consumerNode') fed by another join (see JoinAggReuseSpec::RightDeep). Joins that
/// are sources of an aggregation reuse in 'plan' are left alone.
std::optional<JoinAggReuseSpec> analyzeRightDeep(
    const std::vector<core::PlanNodePtr>& pipelineNodes,
    const core::PlanNodePtr& consumerNode,
    const JoinAggReusePlan& plan);

/// Join-sort reuse in one pipeline: OrderBys whose input comes from a join in
/// the pipeline (through projections) carry a reference to its build rows in
/// place of the build columns they only pass on (see
/// JoinAggReuseSpec::RightDeep). Joins in 'plan' are left alone.
std::vector<JoinAggReuseSpec> analyzeJoinSort(
    const std::vector<core::PlanNodePtr>& pipelineNodes,
    const JoinAggReusePlan& plan);

/// The DISTINCT aggregations (partial and final, on exactly the probe keys)
/// below right-semi filter joins in the plan: see addPassThrough.
std::vector<core::PlanNodeId> findRedundantDistincts(
    const core::PlanNodePtr& root);

/// Build rows' hybrid row id -> (container, row index).
struct JoinAggReuseRowIds {
  explicit JoinAggReuseRowIds(const BaseHashTable& table);

  uint64_t idOf(const char* buildRow) const {
    return rows->getSingleRowId(const_cast<char*>(buildRow));
  }

  static int32_t containerOf(uint64_t id) {
    return static_cast<int32_t>(id >> 56);
  }

  static uint64_t indexOf(uint64_t id) {
    return id & ((1ULL << 56) - 1);
  }

  // Reads the row id of any build row: every build container has the same
  // row layout.
  const RowContainer* rows;
};

/// Rows mode: accumulators of one aggregation, shared by its drivers through
/// the join's hash table. In the key rows when the build reserved them there
/// (HashBuild does for a candidate join), else in arrays: row-major per build
/// container, the build row with hybrid row id (c, r) owning slots
/// [r * numSlots, (r + 1) * numSlots) of container c's array.
class JoinAggReuseState {
 public:
  JoinAggReuseState(
      const JoinAggReuseSpec& spec,
      const BaseHashTable& table,
      memory::MemoryPool* pool);

  /// Accumulator slots of 'buildRow'.
  int64_t* slotsOf(const char* buildRow) const {
    if (inRowOffset_ >= 0) {
      return reinterpret_cast<int64_t*>(
          const_cast<char*>(buildRow) + inRowOffset_);
    }
    const auto id = rowIds_.idOf(buildRow);
    return slots_[JoinAggReuseRowIds::containerOf(id)] +
        JoinAggReuseRowIds::indexOf(id) * numSlots_;
  }

  /// Adds the build rows a driver touched first.
  void addTouched(std::vector<char*>&& rows);

  /// All touched build rows (called by the emitting driver).
  std::vector<char*> takeTouched();

 private:
  const int32_t numSlots_;
  const JoinAggReuseRowIds rowIds_;
  // Offset of the slots in the key rows, or -1.
  int32_t inRowOffset_{-1};
  std::vector<BufferPtr> buffers_;
  // By container id.
  std::vector<int64_t*> slots_;
  std::mutex mutex_;
  std::vector<std::vector<char*>> touched_;
};

/// Codes mode: a code for the grouping values of the build rows of one source
/// join, shared by all drivers through the table. Rows get their code when a
/// probe first matches them (ensure), so the work follows the touched build
/// rows, not the build size.
class JoinAggReuseCodes {
 public:
  /// 'projections': table column -> any channel, the source's grouping keys.
  JoinAggReuseCodes(
      BaseHashTable& table,
      const std::vector<IdentityProjection>& projections);

  /// Assigns codes to the rows among 'rows' that have none yet: extracts and
  /// encodes their values without the lock, then registers them under it.
  void ensure(const std::vector<char*>& rows, memory::MemoryPool* pool);

  /// The code of a row 'ensure' has seen.
  int32_t codeOf(const char* buildRow) const {
    const auto id = rowIds_.idOf(buildRow);
    return __atomic_load_n(
        &codes_[JoinAggReuseRowIds::containerOf(id)]
               [JoinAggReuseRowIds::indexOf(id)],
        __ATOMIC_RELAXED);
  }

  /// A build row with the values of each of 'codes'.
  void representatives(const int32_t* codes, int32_t count, char** rows) const;

  int32_t numCodes() const;

 private:
  BaseHashTable& table_;
  const JoinAggReuseRowIds rowIds_;
  std::vector<IdentityProjection> projections_;
  std::vector<TypePtr> types_;
  // By container id, then row index; -1 until coded.
  std::vector<std::vector<int32_t>> codes_;
  mutable std::mutex mutex_;
  folly::F14FastMap<std::string, int32_t> index_;
  std::vector<char*> representatives_;
};

/// The aggregation side: one per HashAggregation operator.
class JoinAggReuseConsumer {
 public:
  /// 'sources': the probes of spec.sources; 'rowsFrom': the topmost join of
  /// the chain, whose output rows reach the aggregation.
  JoinAggReuseConsumer(
      const JoinAggReuseSpec& spec,
      std::vector<HashProbe*> sources,
      HashProbe* rowsFrom);

  /// True when the sources activated the reuse (decided from their tables;
  /// the same for all drivers).
  bool active() const;

  /// Accumulates 'input' (the rows the topmost join published last) into
  /// the groups of their build rows.
  void addInput(Operator& op, const RowVectorPtr& input);

  /// Rows mode: hands this driver's touched rows over and joins the barrier
  /// of the aggregation's drivers; the last one emits. Codes mode: nothing.
  void noMoreInput(Operator& op);

  /// Next batch of partial results; nullptr when done.
  RowVectorPtr getOutput(
      Operator& op,
      const RowTypePtr& outputType,
      int32_t maxRows);

 private:
  bool rowsMode() const;
  void addInputRows(Operator& op, const RowVectorPtr& input);
  void addInputCodes(Operator& op, const RowVectorPtr& input);
  void accumulate(
      const RowVectorPtr& input,
      const std::vector<int64_t*>& slots,
      bool atomic);
  RowVectorPtr outputRows(
      Operator& op,
      const RowTypePtr& outputType,
      int32_t maxRows);
  RowVectorPtr outputCodes(
      Operator& op,
      const RowTypePtr& outputType,
      int32_t maxRows);
  void fillAggregates(
      Operator& op,
      const RowVectorPtr& result,
      const std::vector<const int64_t*>& slots);

  const JoinAggReuseSpec& spec_;
  const std::vector<HashProbe*> sources_;
  HashProbe* const rowsFrom_;
  std::vector<DecodedVector> decoded_;
  int64_t numInputRows_{0};

  // Rows mode.
  std::shared_ptr<BaseHashTable> table_;
  std::shared_ptr<JoinAggReuseState> state_;
  std::vector<char*> touched_;
  bool emitter_{false};
  std::vector<char*> emitRows_;
  size_t emitPosition_{0};

  // Codes mode: a group is the tuple of its sources' codes; each driver
  // keeps its groups in a map, in the order they appear.
  static constexpr size_t kMaxSources = 4;
  struct GroupKey {
    std::array<int32_t, kMaxSources> codes{};
    bool operator==(const GroupKey& other) const {
      return codes == other.codes;
    }
  };
  struct GroupKeyHash {
    size_t operator()(const GroupKey& key) const;
  };
  std::vector<std::shared_ptr<JoinAggReuseCodes>> codes_;
  std::vector<std::shared_ptr<BaseHashTable>> sourceTables_;
  folly::F14FastMap<GroupKey, uint32_t, GroupKeyHash> groupIndex_;
  std::vector<GroupKey> groupKeys_;
  std::vector<int64_t> groupSlots_;
  std::vector<int64_t> initValues_;
};

} // namespace bytedance::bolt::exec

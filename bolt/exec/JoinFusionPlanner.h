/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Fusion v2 planner: detects left-deep PROBE runs and decides, per hop, which
 * build-side output channels are deferred (emitted as row-ref columns) vs
 * immediate (extracted at the hop). See docs/fusion-v2-design.md.
 *
 * All state produced here is IMMUTABLE after planning. Runtime state lives in
 * the run's operators only (v1 failure mode #8: no mutable DriverCtx coupling).
 */
#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "bolt/core/PlanNode.h"

namespace bytedance::bolt::exec {

/// Per-hop fusion instruction for one HashJoin node inside a fused run.
struct FusionHopPlan {
  core::PlanNodeId joinNodeId;
  /// Output channels (of this join's outputType) that carry DEFERRED build
  /// columns: emitted as a row-ref (int64 row pointer) instead of values.
  /// Grouped: all deferred channels of one hop resolve through one ref column.
  std::vector<column_index_t> deferredChannels;
  /// The single extra output channel index carrying this hop's row-ref column.
  /// kNoRefChannel when this hop defers nothing.
  static constexpr column_index_t kNoRefChannel =
      std::numeric_limits<column_index_t>::max();
  column_index_t refChannel{kNoRefChannel};
  /// True for the LAST hop of the run: this probe materializes every live ref
  /// column (its own deferred channels and all upstream ref columns present in
  /// its input) before emitting output.
  bool isMaterializationPoint{false};
  /// For each ref column visible in this hop's INPUT (upstream hops), the input
  /// channel it occupies. Needed by the materialization point.
  std::vector<column_index_t> upstreamRefInputChannels;
  /// Number of hops between this one and the run's materialization point
  /// (0 for the materialization point itself). Used by the adaptive
  /// controller's deferral-cost model.
  int32_t hopsAfter{0};
  /// Plan node id of the run's materialization point (for reading its
  /// observed output cardinality at runtime).
  core::PlanNodeId materializationNodeId;
};

/// Immutable description of all fused runs in one query plan, keyed by join
/// plan-node id. Shared (const) with the operators at construction time.
class FusionPlan {
 public:
  const FusionHopPlan* hopFor(const core::PlanNodeId& id) const {
    auto it = hops_.find(id);
    return it == hops_.end() ? nullptr : &it->second;
  }

  bool empty() const {
    return hops_.empty();
  }

  size_t numRuns() const {
    return numRuns_;
  }

  void addHop(FusionHopPlan hop) {
    hops_.emplace(hop.joinNodeId, std::move(hop));
  }

  void setNumRuns(size_t n) {
    numRuns_ = n;
  }

 private:
  std::unordered_map<core::PlanNodeId, FusionHopPlan> hops_;
  size_t numRuns_{0};
};

class JoinFusionPlanner {
 public:
  /// Detects fused probe runs over the ordered node list of ONE pipeline
  /// (a DriverFactory's planNodes). Nodes are in source->sink order; a run is
  /// a maximal consecutive sequence of HashJoinNodes where node[i+1]'s
  /// sources()[0] == node[i], each join is INNER, has NO filter, and no
  /// deferred-eligible channel is consumed as a later hop's join key.
  /// Returns hops to merge into the query-level FusionPlan.
  static std::vector<FusionHopPlan> planPipeline(
      const std::vector<core::PlanNodePtr>& pipelineNodes);
};

} // namespace bytedance::bolt::exec

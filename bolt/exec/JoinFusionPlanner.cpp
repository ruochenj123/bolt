/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */
#include "bolt/exec/JoinFusionPlanner.h"

#include <unordered_set>

namespace bytedance::bolt::exec {
namespace {

// A join is eligible to be a fused hop in v2.0: inner, no filter, and a plain
// two-source HashJoin. LEFT joins and filter hops arrive in v2.1.
bool hopEligible(const core::HashJoinNode& join) {
  return join.isInnerJoin() && join.filter() == nullptr &&
      join.sources().size() == 2;
}

// Channels of `join`'s output that come from the BUILD side (candidates for
// deferral). A channel is build-sourced iff its name exists in the build
// source's output type. (Velox join output names are unique across sides.)
std::vector<column_index_t> buildSideChannels(const core::HashJoinNode& join) {
  std::vector<column_index_t> out;
  const auto& outputType = join.outputType();
  const auto& buildType = join.sources()[1]->outputType();
  for (column_index_t c = 0; c < outputType->size(); ++c) {
    if (buildType->getChildIdxIfExists(outputType->nameOf(c)).has_value()) {
      out.push_back(c);
    }
  }
  return out;
}

} // namespace

std::vector<FusionHopPlan> JoinFusionPlanner::planPipeline(
    const std::vector<core::PlanNodePtr>& pipelineNodes) {
  std::vector<FusionHopPlan> result;

  // Collect maximal consecutive probe-chained eligible join sequences.
  size_t i = 0;
  while (i < pipelineNodes.size()) {
    // Find start of a run.
    auto first =
        std::dynamic_pointer_cast<const core::HashJoinNode>(pipelineNodes[i]);
    if (!first || !hopEligible(*first)) {
      ++i;
      continue;
    }
    std::vector<std::shared_ptr<const core::HashJoinNode>> run{first};
    size_t j = i + 1;
    while (j < pipelineNodes.size()) {
      auto next =
          std::dynamic_pointer_cast<const core::HashJoinNode>(pipelineNodes[j]);
      if (!next || !hopEligible(*next) ||
          next->sources()[0].get() != pipelineNodes[j - 1].get()) {
        break;
      }
      run.push_back(next);
      ++j;
    }

    if (run.size() >= 2) {
      // Names used as join keys by LATER hops: those channels must be
      // materialized immediately at their producing hop (a deferred ref cannot
      // be hashed as a key). Almost always these are probe-side (fact FKs),
      // but snowflake plans can key on an earlier build column.
      std::unordered_set<std::string> laterKeyNames;
      // Walk hops from last to first so each hop sees keys of hops AFTER it.
      std::vector<FusionHopPlan> hops(run.size());
      for (int k = static_cast<int>(run.size()) - 1; k >= 0; --k) {
        const auto& join = *run[k];
        FusionHopPlan hop;
        hop.joinNodeId = join.id();
        hop.isMaterializationPoint = (k == static_cast<int>(run.size()) - 1);
        for (auto c : buildSideChannels(join)) {
          const auto& name = join.outputType()->nameOf(c);
          if (laterKeyNames.count(name) == 0) {
            hop.deferredChannels.push_back(c);
          }
        }
        // Register THIS hop's probe keys for hops before it.
        for (const auto& key : join.leftKeys()) {
          laterKeyNames.insert(key->name());
        }
        hops[k] = std::move(hop);
      }

      // Upstream ref propagation: hop k's input contains the ref channels of
      // hops [0, k). In v2.0 a ref column keeps its producing hop's OUTPUT
      // channel index through later joins only if later joins pass it through
      // by name; compute input channel by name lookup.
      // Ref column naming: "$fusion_ref$<joinNodeId>" appended as the LAST
      // output channel of the producing hop (HashProbe appends it), so later
      // hops see it as a probe-side passthrough column.
      // At plan time we only record producing-hop ids; the materialization
      // probe resolves input channels by name at initialization.
      result.insert(
          result.end(),
          std::make_move_iterator(hops.begin()),
          std::make_move_iterator(hops.end()));
    }
    i = (run.size() >= 2) ? j : i + 1;
  }
  return result;
}

} // namespace bytedance::bolt::exec

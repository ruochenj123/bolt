# Fusion v2: Deferred-Ref Columns (design)

## Verdict on v1 (see LocalPlanner.cpp catalogue)
v1 ("N-way key-state fusion") kept provenance in side-band state threaded through
DriverCtx/HashBuild/HashProbe/SortBuffer. Eight failure modes on TPC-H SF100;
failures migrate under plan-shape gating (shared state couples chains). Its core
trick (reusing key rows between BUILD-over-build chains via VirtualRow) saves only
key byte-copying — hashing/insertion are repaid in full (<5% measured) — while its
topology (right-deep) is rare in real plans. Disabled; v2 replaces it.

## v2 principle: a deferred column IS a column
A probe in a fusion run does NOT extract build payload columns. It emits, per
deferred build column-group, ONE ref column (BIGINT = build row pointer) in its
ordinary output. Everything downstream that moves columns (dictionary wraps for
duplicate expansion, identity projections, sort, exchange within pipeline) moves
refs correctly with zero new machinery. Materialization = per-hop bulk extract by
row pointer at the run end. No source maps. No multi-level chains. No VirtualRow.
No probe-payload container (probe columns ride the output as today, lazily
dictionary-wrapped by Velox itself).

## Topology: left-deep probe runs (the shape real plans have)
Fusion run = maximal sequence of HashJoin(probe side)-chained operators in ONE
pipeline: scan/fact -> probe D1 -> probe D2 -> ... -> [end]. Each build is a
normal HashBuild pipeline (base or subquery) — never rebuilt, never merged.
This is TPC-H/TPC-DS's actual shape (the v1 right-deep pattern is the rare one).
N:N is supported natively: duplicate expansion wraps ALL probe-side columns —
including earlier hops' ref columns — in the hit dictionary, exactly like any
other column.

## Components
1. JoinFusionPlanner (new, plan-time, unit-testable):
   - detect runs; per hop decide deferrable channels (build columns not needed
     until run end) vs immediate (build columns used as a LATER hop's join key
     or by a join filter -> materialize just those at that hop);
   - emit FusionRunPlan {hops[], deferredSchema}; a node that consumes values
     (agg, filter/project touching a deferred channel, run end) is the
     materialization point. Interposed nodes that merely pass columns through
     need NO special handling (refs are columns).
2. HashProbe deferred-output mode (small, localized):
   - for deferred channels: emit FlatVector<int64_t> of build row pointers
     (hits are already char* rows; ptr==0 => null-extended left-join row);
   - for immediate channels: existing extract path, unchanged.
3. FusionMaterializer (new):
   - at the materialization point: decode any dictionary wrapping over each ref
     column, bulk-extract per hop via the existing prefetch-optimized
     RowContainer/HybridContainer extract-by-row APIs (store-time-flattened
     inputs), replace ref column with real columns.

## Invariants fixed by construction (vs v1 catalogue)
1 VirtualRow-blind paths: no VirtualRow.
2 encoding assumptions: flatten payload batches AT STORE (HybridContainer);
  materializer only ever reads flat.
3 channel tracing through Projects: refs are columns; projections pass them.
4 null types: deferredSchema carries types end-to-end from the planner.
5 probe-key mapping: probe columns are never deferred; non-issue.
6 deepest-member init: no cross-member state to initialize.
7 second extraction path: ONE materializer entry point.
8 state coupling: FusionState is per-run per-driver, owned by the run's
  operators; DriverCtx carries nothing mutable for fusion.

## Memory/liveness (reviewer R3-D1)
Refs pin only the build containers, which live for the probe's duration anyway
(they are the hash tables). Deferral adds no query-lifetime state; freed at the
materialization point per batch. Report query-level peak memory in the eval.

## Rollout
v2.0: inner joins, no join filter in-run (filter hop = run boundary), single
      pipeline, materialize at run end. Covers TPC-H Q3/Q5-style + TPC-DS stars.
v2.1: LEFT joins (null refs), Sort pass-through (sort the refs as data,
      materialize on sort output), filter hops via selective per-hop
      materialization of only filter-referenced columns (cf. selective late
      materialization, Liu et al. VLDB'25 — reviewer R2-D1 tie-in).
v2.2: agg pass-through study (R3-O4).

## Config
fusion_v2_enabled (default false), fusion_v2_max_run_len (safety valve),
per-query stats: runs fused, hops, deferred columns, materialize time.

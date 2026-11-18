# Cross-operator reuse of join state

Status: branch `t2-runtime-reuse`, flag `join_agg_reuse_enabled` (default off).
Code: `bolt/exec/JoinAggReuse.{h,cpp}`, plus hooks in `HashBuild`, `HashProbe`,
`HashAggregation`, `HashTable`, `RowContainer` (`HybridContainer` row id
offset) and `LocalPlanner`. Experiments: `exp/2026-09-28-bolt-t2-reuse/`.

## Idea

A hash join keeps its build side in its hash table: key rows plus, in the
hybrid layout, a row id into columnar payloads. Operators above the join often
rebuild state it already holds:

- an aggregation hashes grouping columns that the join extracted from its build
  rows only to hand them over (join -> aggregation);
- a join whose build input is another join's output copies that join's build
  columns into its own table (right-deep joins);
- a DISTINCT below a right-semi join deduplicates keys the join's table
  deduplicates anyway.

Instead, the downstream operator uses the join's rows. Planning only analyzes
the plan and configures the existing operators; no plan node or operator is
added or removed. What is used is decided when the join's table exists.

## Join -> aggregation

**Where.** A partial aggregation and the joins below it in its pipeline, with
only projections and inner joins in between (`analyzeJoinAggReuse`): every
grouping key is a build column of one of those joins (a *source*) passed
through unchanged, or a probe key equal to a source's build key; aggregates
are `count`, `sum`, `min`, `max`, `avg` over numeric inputs (and `sum`/`avg`
over DECIMAL, which always run in codes mode). At most 4 sources; a right join
only as the single join. Join spilling off.

**Probe side.** A source emits null constants for the build columns used above
it only as grouping keys (`nullableBuildColumns`), extracts the others, and
publishes its matched build rows; every join above it carries the published
rows up through its output row mapping, adding its own if it is a source
(`joinAggReuseChannels()`). Projections compute the aggregate inputs as usual.

**Aggregation side** (`JoinAggReuseConsumer`), one of two modes, chosen by the
source probe from its built table, alike in all drivers:

- *rows*: one source, unique build keys (`hasDuplicateKeys()`: plans carry no
  key constraints; the built table knows), no DECIMAL sums, and grouping keys
  covering the join keys or more than 1M build rows. A build row is a group.
  Accumulators sit in the join's key rows (HashBuild reserves and initializes
  the slots for such a join; codes mode leaves them unused), updated with
  relaxed atomics by all drivers; the last driver (task barrier) emits one
  partial row per touched build row. TPC-H Q3, Q10, Q13, Q17.
- *codes*: otherwise, provided some source skips extracting a build column
  for the grouping keys (`skipsBuildColumns`; with only probe keys as grouping
  keys there is nothing to save and the reuse stays off, TPC-DS Q64's
  `cs_ui`). A group is the tuple of one id per source. A source
  whose build rows are the groups (the rows-mode condition: unique build keys,
  and grouping keys covering the join keys or more than 1M build rows) uses the
  build row itself as the id: no dictionary. Any other source gives its build
  rows a code for their grouping values, lazily when a probe first matches
  them (a dictionary per table, shared by the drivers; values extracted and
  encoded outside its lock). Each driver aggregates in a `GroupingSet` of its
  own, built by `HashAggregation` from the same aggregates, whose grouping
  keys are the ids: one BIGINT column per source appended to the input. The
  hash table thus addresses groups by fixed-width integers (array mode while
  the product of the code ranges is small), and the aggregate functions and
  accumulators are the engine's own, so DECIMAL sums and several sources need
  no shared state. Groups stay in memory until the end of the input (no
  partial flush, no spill); the driver then emits them, reading the grouping
  values from the group's build row or from one build row per code. A DISTINCT
  (no aggregates) keeps the ids of its new groups as the table reports them.
  TPC-H Q5, Q12, SSB Q2.1-Q4.3, TPC-DS star queries (codes); TPC-DS Q11, Q74,
  Q38, Q87 (customer rows).

The final aggregation downstream is unchanged.

## DISTINCT below a right-semi join

`findRedundantDistincts`: a right-semi filter join whose probe input is a
DISTINCT (partial -> local exchange -> final) on exactly its probe keys. Both
aggregations pass their input through, projected to the keys; the join marks
each build row once however often its key comes. TPC-H Q4.

## Right-deep joins

`analyzeRightDeep`: the build pipeline of a join J2 (its consumer node) whose
nearest join below is J1 (inner, in the same pipeline, only projections in
between). J2's build columns that are J1 build columns passed through unchanged
(not J2 keys, not read by J2's filter) are *deferred*: J1 does not extract
them where nothing else uses them and publishes its matched rows; J2's build
stores, instead of them, one BIGINT reference to the J1 row and keeps J1's
table alive; J2's probe reads the deferred columns through the references for
its matches only (`derefRightDeep`). One level: a join whose build stores
references is not itself a source. J2 is not also an aggregation source. TPC-H
Q2, Q7, Q8, Q9, Q21; synthetic Q38 = T x (S x R).

## Limits

- Grouping keys computed by projections (e.g. `year(o_orderdate)`) or coming
  from the pipeline's input (not a join) are not covered.
- Right-deep reuse is one level deep.
- Rows mode emits on one thread.

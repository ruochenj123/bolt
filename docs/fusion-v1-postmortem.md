# Fusion v1 ("N-way key-state fusion") — postmortem

Removed from the tree on 2026-07-23 (superseded by fusion v2, docs/fusion-v2-design.md).
This note preserves what v1 was and why it was removed; it is the raw material for
the paper's lessons-learned discussion.

## What v1 was
Right-deep chains (join output feeding the NEXT join's BUILD side) shared row-oriented
key state across operators: the downstream build wrapped upstream key-row pointers in
VirtualRow shims (to re-chain duplicates without corrupting the upstream table) and
rebuilt a hash table over them; payload reconstruction was deferred via side-band
provenance (ColumnSourceMap / LevelRowRefs on DriverCtx) resolved by multi-level
chain-walking at a materialization point (paper Algorithm 3). A Sort-tail variant
reused key rows when sort keys equaled the final join keys.

## Why it was removed
1. Economics: reusing key state saves only key byte-copying; hashing + insertion
   (the expensive parts of the rebuild) are repaid in full. Measured <5% on the
   identical-vs-distinct-keys experiment. The topology it optimizes (right-deep)
   is also rare in optimizer output; benchmark plans had it only because they were
   hand-written that way.
2. Soundness: on TPC-H SF100 the mechanism exhibited EIGHT distinct failure modes,
   and fixing any one migrated failures to previously working queries (shared
   driver-context state coupled independent chains). Catalogue:
   1. fillFilterInput -> extractColumns not VirtualRow-aware (SIGSEGV)
   2. extract paths assume FLAT children; storage kept dict/lazy encodings
   3. updateSourceMapForNWay could not trace channels through Project nodes
   4. null type propagated into vector creation (Q8/Q3)
   5. probe-KEY columns unmappable when consumed downstream as payload
   6. deepest chain member's columnSourceMap never initialized (Q2/Q20)
   7. a second StringView extraction path with garbage refs
   8. failure migration under plan-shape gating (state coupling)
3. The v2 design eliminates the whole class by carrying deferral state IN-BAND
   (refs as ordinary columns) with zero shared mutable runtime state.

## What replaced each piece
- key-state reuse across builds -> nothing (measured non-payoff); right-deep joins
  run as plain hybrid joins
- side-band provenance + Algorithm-3 chain resolution -> per-hop ref columns,
  single-hop resolution at the run end
- Sort-tail key reuse -> v2.1 sort pass-through (refs sort as data; payload
  materializes after the sort, on fewer rows)

# Sentinel Fixtures

Frozen performance sentinels used by gated perf tests and doctor --full.

## linear_chain
- 1 source, 400 derived in a single chain
- v2 changes d:0001 transform_ref
- Expected impacted: 400 (all)

## wide_fanout
- 1 source, 1 upstream derived, 600 leaf derived
- v2 changes params on d:ROOT
- Expected impacted: 601 (all derived)

## diamond_merge
- Root -> two branches -> reconverge -> 150-node tail
- v2 changes params on d:ROOT
- Expected impacted: 154
- Diamond nodes should report alternative paths

## binding_failure
- 200 sources, 90 derived (80 base + 10 tail)
- bindings omit s:COL199 and s:COL200
- v2 changes params on d:DER001 (unrelated)
- Expected impacted: 12 (DER001 + DER080 + tail)

## many_independent_changes
- 100 independent components, each a 3-node chain
- v2 changes transform_ref on the root derived of each component
- Expected impacted: 300 (all derived)

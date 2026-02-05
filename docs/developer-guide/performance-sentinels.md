# Performance Sentinels (v1)

This document describes the **performance sentinel** test suite: frozen, worst‑case graph shapes used to prevent order‑of‑magnitude regressions in the kernel. Sentinels are not benchmarks; they are regression alarms.

## What Sentinels Are
- **Few and frozen**: five cases only; shapes do not change over time.
- **Pathological on purpose**: they stress depth, breadth, reconvergence, and binding failures.
- **Deterministic**: same inputs, same outputs, same ordering.
- **Gated**: they run only when explicitly requested.

## Sentinel Cases (Frozen Fixtures)
All fixtures live under `fixtures/sentinels/` and are **not parameterized**.

1) **Linear Chain (Depth)**
- 1 source, 400 derived in a single chain
- Change: `d:0001` transform_ref changes
- Expected impacted: 400 (all derived)

2) **Wide Fan‑Out (Breadth)**
- 1 source, 1 upstream derived, 600 leaf derived
- Change: params change on `d:ROOT`
- Expected impacted: 601 (all derived)

3) **Diamond / Reconvergence (Merge)**
- Root -> branches (A, B) -> reconverge (C) -> 150‑node tail
- Change: params change on `d:ROOT`
- Expected impacted: 154

4) **Binding Failure (Adapter)**
- 200 sources, 90 derived (80 base + 1 missing‑binding node + 10 tail)
- Bindings omit `s:COL199` and `s:COL200`
- Change: params change on `d:DER001` (unrelated)
- Expected impacted: 12 (missing‑binding node + tail + direct change)

5) **Many Independent Changes (C * (V+E) Guard)**
- 100 independent components, each a 3‑node chain
- Change: transform_ref change on the root derived of each component
- Expected impacted: 300 (all derived)

## How Sentinels Run
Sentinels execute through the **public API entrypoint** but in **core** mode:
- `detail_level="core"` (no dependency paths, no explanation strings)
- `report_mode="core"` (summary JSON only)

This measures the **kernel compute path**, not report rendering.

Run locally (gated):
```
python -m pytest -m perf --run-perf --benchmark-only
```

## Budgets (Default Caps)
Default caps are defined in `src/cheshbon/_internal/benchmarks.py` and can be overridden via environment variables:

| Sentinel | Default (ms) | Env Override |
|---|---:|---|
| Linear Chain | 1000 | `CHESHBON_MAX_LINEAR_CHAIN_MS` |
| Wide Fan‑Out | 1000 | `CHESHBON_MAX_WIDE_FANOUT_MS` |
| Diamond Merge | 1000 | `CHESHBON_MAX_DIAMOND_MERGE_MS` |
| Binding Failure | 700 | `CHESHBON_MAX_BINDING_FAILURE_MS` |
| Many Independent Changes | 1000 | `CHESHBON_MAX_MANY_INDEPENDENT_CHANGES_MS` |

Budgets are **caps, not targets**. A 10x slowdown is acceptable if still within cap; multi‑second regressions are not.

## Output Artifacts (Optional)
`pytest-benchmark` can emit JSON artifacts for comparison:
```
python -m pytest -m perf --run-perf --benchmark-only \
  --benchmark-save=sentinels-baseline-YYYY-MM-DD \
  --benchmark-storage=benchmarks
```

Note: Benchmark JSON includes `machine_info`. If you commit artifacts, consider scrubbing `machine_info.node`.

## Determinism Guarantees
Sentinel outputs are deterministic:
- `impacted` / `unaffected` lists are sorted by ID
- change events are ordered by `element_id` then per‑element priority
- impact reasons are **order‑insensitive** (event ordering cannot change reasons)

## Cap Audit (Kernel‑Level)
Performance sentinels are complemented by a cap audit of existing limits:

| Cap | Value | Location | Owner | Rationale |
|---|---:|---|---|---|
| Alternative path cap | 10 | `src/cheshbon/kernel/graph.py` | Explanation | Prevents combinatorial path explosion in reporting |
| Alternative path length | shortest + 10 | `src/cheshbon/kernel/graph.py` | Explanation | Keeps path counting bounded |
| Params size limit | 50KB | `src/cheshbon/kernel/spec.py` | Kernel correctness | Prevents params from becoming unbounded payloads |

These caps are **explicit** and must not be relaxed without deliberate review.

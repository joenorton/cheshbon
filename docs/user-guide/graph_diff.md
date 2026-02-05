# Graph Diff (Bundle Lineage)

Cheshbon can compute a structural diff and downstream impact between two **SANS bundle** lineage graphs.

For canonical semantics and invariants, see `docs/GRAPH_DIFF_CONTRACT.md`.

## CLI

```bash
cheshbon graph-diff --bundle-a <bundle_dir> --bundle-b <bundle_dir> --out <output_dir>
```

Outputs:
* `graph_diff.json`
* `impact.json`

## `graph_diff.json`

`graph_diff.json` summarizes raw churn and class-based pairing.

**Raw churn**
* `added_steps`, `removed_steps`: step records `{id, op, transform_id, transform_class_id, payload_sha256}`
* `changed_step_nodes`: step ids present in both graphs but with different payload/op/ids
* `added_edges`, `removed_edges`: edge keys `{src, dst, kind}`

**Class pairing**
* `modified_by_class`: entries keyed by `(transform_class_id, op)` with:
  * `from_steps`: list of `{id, transform_id, payload_sha256}`
  * `to_steps`: list of `{id, transform_id, payload_sha256}`
  * `classification`: `param_value_change` or `rewire_only`

**Counts**
* `counts.raw`: raw churn (added/removed steps, edges, etc.)
* `counts.events`: semantic event counts

**Semantic events**
* `events`: ordered, deterministic event log (`step_added`, `step_removed`, `step_payload_changed`, `step_rewired`, `step_replaced_same_class`)
* replacement suppression: when `step_replaced_same_class` is present for step ids, corresponding `step_added`/`step_removed` events are omitted

## `impact.json`

Impact is computed on **graph B**.

* `seed_steps`: steps in graph B that initiate impact
* `touched_tables`: first-hop outputs of `seed_steps`
* `impacted_tables`: downstream tables, **excluding** `touched_tables`
* `impacted_steps`: downstream steps, **excluding** `seed_steps`
* `seed_reasons`: reasons for each seed step (keys always match `seed_steps`)
* `reasons`: typed list of impact reasons per impacted step
* `paths`: canonical, deterministic paths from a seed to each impacted step
* `context.removed_steps`: removed steps from graph A (for explanation only)
* `context.path_policy`: `bipartite_shortest_then_lex_parent`
* `context.path_representation`: `step_table_step`
* `context.paths_include_seeds`: `true` (seed steps have identity paths)

**Seed reasons (minimal shapes)**
* `changed_step`: `{"reason": "changed_step"}`
* `modified_by_class`: `{"reason": "modified_by_class", "transform_class_id", "op", "from_steps", "to_steps", "classification"}`
* `added_step`: `{"reason": "added_step", "transform_id", "transform_class_id", "op"}`
* `step_rewired`: `{"reason": "step_rewired", "inputs_added", "inputs_removed", "outputs_added", "outputs_removed"}`

**Impact reasons**
* `transitive`: `{"reason": "transitive", "from_step", "via_table"}`

## Pairing Rules (Deterministic)

Steps are paired by `(transform_class_id, op)`:
* If any `transform_id` differs across from/to: `param_value_change`
* If all `transform_id` match but step ids differ: `rewire_only`

Entries and step lists are sorted deterministically by `(op, transform_class_id)` and `id`.

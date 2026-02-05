# SANS Bundle Ingestion

Cheshbon supports first-class ingestion of **SANS run bundles**. This allows Cheshbon to act as the source of truth for lineage and artifact integrity after a data transformation run.

## Overview

When a transformation engine like `sans` executes a plan, it produces a "bundle" of witness artifacts. Cheshbon ingests these to produce deterministic lineage graphs, promoted registries, and normalized run records.

The ingestion process ensures that:
1. The executed plan matches the static IR.
2. Every step in the execution is accounted for in the registry.
3. Data hashes (input/output) are recorded for end-to-end traceability.
4. A **semantic run fingerprint** is generated to uniquely identify the run result.

## Split Identity Model

Cheshbon uses a split identity model to distinguish logic from wiring:

*   **`transform_id` (Semantic)**: Identifies a transform purely by its logic (`op` + `params`). It is content-addressed by the `spec`.
*   **`step_id` (Application)**: Identifies a specific *application* of a transform in a plan (`transform_id` + `inputs` + `outputs`).

This separation allows Cheshbon to detect when the same logic is reused across different tables or when the wiring changes without changing the underlying logic.

## SANS Bundle Structure

A standard SANS bundle directory contains:
* `plan.ir.json`: The static intermediate representation of the transformation plan.
* `runtime.evidence.json`: Execution-time metadata (hashes, row counts, bindings). Optional column-level stats (if present) are used by `run-diff` to annotate value changes; if missing, no value evidence is shown.
* `registry.candidate.json`: The transforms and registry state used during the run. `run-diff` renders spec-level intent from this registry; it is not runtime value evidence.

Supported spec render ops: `compute`, `filter`, `rename`, `select`, `sort`, `aggregate`. Unknown or incomplete specs render as explicit placeholders (e.g., `(unrenderable op: xyz)` or `(incomplete spec: field)`).

## Usage

### 1. Verification

Before ingestion, you should verify the bundle's integrity:

```bash
cheshbon verify bundle <bundle_dir>
```

Cheshbon performs **7 Strict Rules** of verification:
1.  **Plan Hash**: `sha256(plan.ir.json bytes)` must match the hash recorded in `runtime.evidence.json`.
2.  **Index Completeness**: Every step index in the plan must have an entry in `registry.index`.
3.  **Transform ID Consistency**: `registry.index[i]` must equal `plan.steps[i].transform_id`.
4.  **Step ID Integrity**: `plan.steps[i].step_id` must equal `sha256(canon({transform_id, inputs, outputs}))`.
5.  **Artifact Integrity**: Every file referenced in evidence (inputs/outputs) must exist and its `bytes_sha256` must match.
6.  **Registry Completeness**: All `transform_id`s in the index must exist in `registry.transforms`.
7.  **Semantic Integrity**: Every transform must have a `spec`, and `transform_id` must equal `sha256(canon(spec))`.

### 2. Ingestion

To ingest a bundle and materialize Cheshbon artifacts:

```bash
cheshbon ingest sans --bundle <bundle_dir> --out <output_dir>
```

This will create a `cheshbon/` directory under `<output_dir>` containing:
* `graph.json`: A deterministic lineage graph (step + table nodes; produces/consumes edges).
* `registry.json`: The promoted `StrongTransformRegistry` (preserving `spec`).
* `run.json`: A normalized record of the execution, including the semantic fingerprint.

## Bundle `artifacts/graph.json` (v1)

SANS bundles include `artifacts/graph.json`, which Cheshbon ingests with **strict** validation.

**Top-level**
* `schema_version`: `1`
* `producer`: `{name, version}`
* `nodes`: list of step + table nodes
* `edges`: list of `produces` / `consumes` edges

**Step node**
* `id`: `s:<sha256>`
* `kind`: `"step"`
* `op`: operation name
* `transform_class_id`: structural/family id (stable across wiring)
* `transform_id`: semantic transform id
* `inputs`: list of `t:*` table ids
* `outputs`: list of `t:*` table ids
* `payload_sha256`: hash of full step payload

**Table node**
* `id`: `t:<name>`
* `kind`: `"table"`
* `producer`: `s:*` step id or `null`
* `consumers`: list of `s:*` step ids

**Edge**
* `src`: node id
* `dst`: node id
* `kind`: `"produces"` (step -> table) or `"consumes"` (table -> step)

## Semantic Run Fingerprint

The `fingerprint` in `run.json` is a deterministic SHA256 hash of the run's semantic state:
* The `plan_sha256`.
* The ordered list of `(step_id, transform_id)` pairs.
* The `canonical_sha256` of all input tables (keyed by logical name).
* The `canonical_sha256` of all output tables (keyed by logical name).

Identical runs (same plan, same logic, same wiring, same data) will always produce the same fingerprint, regardless of the execution environment or platform.

## Deterministic ID Rules

* **Node IDs**: Fixed as `table:<logical_name>`.
* **Edge IDs**: Deterministic hash of `{step_id, transform_id, inputs, outputs}`.
* **Path Normalization**: All file paths are normalized to use forward slashes (`/`) for cross-platform consistency.

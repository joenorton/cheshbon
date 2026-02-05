# Architecture Reference

## Core Architecture

### Mapping Spec as IR
The `mapping_spec` is treated as a compiler intermediate representation (IR), not executable code. This enables:
- Static analysis without execution
- Deterministic impact computation
- Version control and diffing at the spec level

### Library-First, CLI Wrapper
Pure functions in `src/cheshbon/kernel/`. CLI/reporting/I/O live outside the kernel. No database, no server, no state. Operations are artifact-based and deterministic; file I/O is handled in outer layers.

## Core Invariants

- mapping_spec is the primary IR; execution is downstream and optional
- all edits are transactional: propose -> preview impact -> commit or discard
- ai may suggest bindings or transforms, but never auto-commit
- renames are metadata-only; structural changes are the only impact triggers
- dm domain only; no cross-domain logic in v0
- no visual dag editing until invariants are proven via list-based editor
- impact analysis must explain: what changed, why, and via which dependency path

## Stable Identity System

### Problem
Name-based references create ambiguity: is a rename a new entity or the same entity with a new label?

### Solution
Every source column and derived variable has a stable `id` (`s:NAME`, `d:NAME`) separate from its `name`. This enables:
- Renames are metadata-only (non-impacting)
- Structural changes (ID changes) are explicitly impacting
- Clean diffs without heuristics

### Impact
- `SOURCE_RENAMED` event: ID unchanged, name changed -> no impact
- `DERIVED_INPUTS_CHANGED` event: Input references different IDs -> impact

## Change Event Ontology

### Design Principle
Normalized events, not raw JSON diffs. This prevents:
- Noise from formatting changes
- Ambiguity from structural diff libraries
- False positives from order changes

### Event Types

**Source Events:**
- `SOURCE_ADDED`, `SOURCE_REMOVED`, `SOURCE_RENAMED`

**Derived Events:**
- `DERIVED_ADDED`, `DERIVED_REMOVED`, `DERIVED_RENAMED`
- `DERIVED_INPUTS_CHANGED` (structural)
- `DERIVED_TRANSFORM_REF_CHANGED` (structural: transform_ref changed)
- `DERIVED_TRANSFORM_PARAMS_CHANGED` (structural: params_hash changed, ref unchanged)
- `DERIVED_TYPE_CHANGED` (structural)

**Transform Registry Events:**
- `TRANSFORM_ADDED` (new transform in registry)
- `TRANSFORM_REMOVED` (transform removed from registry)
- `TRANSFORM_IMPL_CHANGED` (registry-level: impl_fingerprint.digest changed)

### Why This Matters
Events are the unit of impact analysis. Each event type has explicit semantics, enabling precise impact computation without guessing.

## Impact Analysis

### Impact Definition
A derived variable is impacted if:
1. **Direct change**: Its inputs, transform_ref, transform params, or type changed
2. **Transitive dependency**: It depends on an impacted variable
3. **Missing input**: A required input ID no longer exists
4. **Missing binding**: A required source ID is not bound in the current extract
5. **Transform implementation changed**: The transform's impl_fingerprint.digest changed (registry-level)
6. **Missing transform**: The transform_ref references a transform not in the registry

### Impact Reasons
- `DIRECT_CHANGE`: Node itself changed
- `TRANSITIVE_DEPENDENCY`: Depends on impacted node
- `MISSING_INPUT`: Required input ID missing from spec
- `DIRECT_CHANGE_MISSING_INPUT`: Both direct change and missing input
- `MISSING_BINDING`: Required source ID not bound in extract

### Why Distinguish Reasons
Different reasons require different actions:
- `DIRECT_CHANGE` -> review the change itself
- `TRANSITIVE_DEPENDENCY` -> review upstream change
- `MISSING_BINDING` -> fix binding layer
- `MISSING_INPUT` -> fix spec or remove dependency

## Dependency Graph

### Structure
- **Nodes**: Source columns (`s:ID`) and derived variables (`d:ID`)
- **Edges**: `depends_on` relationships from `inputs` field
- **Direction**: Source -> Derived, Derived -> Derived

### Operations
- `get_dependents(node_id)`: Direct dependents only
- `get_transitive_dependents(node_id)`: Full closure
- `get_dependency_path(from_id, to_id)`: Shortest path for explanation

### Why Explicit Graph
Enables:
- Transitive closure computation
- Path finding for explanations
- Cycle detection (future)
- Topological sorting (future)

## Binding Layer

### Problem
Raw schema columns change names upstream, but conceptual identity should remain stable. Without bindings, every rename requires spec changes.

### Solution
Separate binding layer maps raw column names -> stable source IDs. This:
- Keeps mapping spec pure (no raw column names)
- Handles raw schema drift without spec changes
- Provides explicit ledger of what's bound/unbound

### Artifacts
- `RawSchema`: Snapshot of raw column names/types
- `Bindings`: Mapping from raw names to source IDs
- `BindingEvent`: Events for binding changes

### Integration
Binding validation happens **before** impact analysis:
1. Validate bindings against raw schema
2. Check for missing bindings (required source IDs not bound)
3. Layer missing binding impact onto spec diff impact

### Why Separate
- Keeps mapping spec ontology frozen
- Binding layer is adapter, not core
- Enables "binding updated" vs "binding not updated" contrast

### Known Gap: Type Incompatibility Detection

**Current State**: The binding layer currently detects:
- Missing bindings (source ID not bound to any raw column) -> `MISSING_BINDING`
- Invalid bindings (bound column not in schema) -> `BINDING_INVALID`

**Gap**: Type incompatibility between raw columns and source columns is **not detected**:
- If a raw column's type changes incompatibly with its bound source column type (e.g., `date` -> `string`), this goes **silent**
- Example: `s:BRTHDT` expects `date`, binding exists (`BRTHDT` -> `s:BRTHDT`), but raw schema has `BRTHDT` as `string`
- This is a **third failure mode** distinct from `MISSING_INPUT` and `MISSING_BINDING`

**Future Implementation Considerations**:
- Should be a `BindingEvent` (e.g., `RAW_COLUMN_TYPE_CHANGED`), not a `ChangeEvent` (spec hasn't changed)
- Should generate an impact reason (e.g., `SOURCE_TYPE_MISMATCH`) parallel to `MISSING_BINDING`
- Requires type compatibility rules (exact match? compatible conversions?)
- Should propagate transitively like `MISSING_BINDING`

## SANS Integration

Cheshbon serves as the **accounting layer** for SANS executions. While SANS is a "dumb" deterministic executor, Cheshbon provides the context, verification, and long-term semantic record.

### The Split Identity Model
To support precise impact analysis and logic reuse, Cheshbon enforces a dual-ID system for transformations:

1.  **`transform_id` (The "What")**: `sha256(canonical_json(op, params))`. This ID is purely semantic. If the logic is the same, the ID is the same, regardless of where it is used.
2.  **`step_id` (The "Where")**: `sha256(canonical_json(transform_id, inputs, outputs))`. This ID represents a specific application of logic to specific data.

### Ingestion as Promotion
Ingestion is the process of "promoting" ephemeral execution witnesses into permanent artifacts:
*   **Witness**: Ephemeral JSON files in a SANS bundle.
*   **Artifact**: Permanent `graph.json`, `registry.json`, and `run.json` in a Cheshbon-managed directory.

### Verification Invariants
Cheshbon will never ingest or promote a bundle that violates its integrity rules. This ensures that the `graph.json` and `run.json` are always a cryptographically accurate reflection of what actually happened to the data.

## Input Semantics

### Order-Agnostic
Inputs are treated as sets, not ordered lists. This prevents false positives from reordering.

### Reference Format
- `s:SOURCE_ID` for source columns
- `d:DERIVED_ID` for derived variables
- `t:TRANSFORM_ID` for transforms

Strict validation ensures all references use this format.

## Constraints Handling

### Current Design
Constraints are ignored for impact calculation in v0. They are part of the spec but do not affect structural dependency impact.

### Rationale
Focus on structural dependency impact first. Constraints can be modeled as derived-like nodes later if needed, but that's out of scope for the kernel.

## Explanation System

### Design
Structured explanation primitives that:
- Resolve IDs to names for readability
- Show full dependency paths (not just nodes)
- Display impact reasons explicitly
- List missing inputs/bindings with canonical names

Rendering (markdown/prose) is handled outside the kernel (reporting layer).

### Why Canonical Names
When showing missing bindings, use the source's canonical name from the spec, not the raw column name (which may not exist). This prevents "how did you know the raw name?" questions.

## Report Modes

Cheshbon emits three report modes with distinct purposes:

- **full**: human-facing markdown + JSON for review and communication.
- **core**: minimal, machine-first JSON for automation/perf.
- **all-details**: machine-first JSON evidence for audit and verification.

All-details reports assert only analytical impact semantics under the Cheshbon kernel contract; they make no claims about code execution, data correctness, or regulatory acceptance.

Report verification (`cheshbon verify report`) validates all-details artifacts (digests + witness invariants). It is not an execution validator.

## Testing Strategy

### Coverage
- Graph building correctness
- Diff event emission
- Impact computation (direct, transitive, missing)
- Binding validation
- Edge cases (unreferenced nodes, missing inputs)

### Philosophy
Tests prove the invariant: given v1 and v2, the kernel computes exactly which outputs are impacted and why, without execution.

## Non-Goals (Explicit)

- No LLM calls
- No execution engine
- No UI
- No database
- No SDTM semantics beyond toy naming
- No multi-domain orchestration

These exclusions keep the kernel focused on proving the single invariant.

## Transform Registry

### Problem
Transforms were opaque string identifiers (`transform_id`), making it impossible to distinguish:
- Transform reference changes (switching from one transform to another)
- Transform parameter changes (same transform, different params)
- Transform implementation changes (same transform ID, but implementation updated)

### Solution
Transforms are first-class artifacts with:
- **Transform Registry**: Separate artifact containing transform definitions
- **Stable IDs**: `t:`-prefixed transform references (e.g., `t:ct_map`)
- **Versioning**: Semantic versioning (informational only)
- **Fingerprinting**: Structured `impl_fingerprint` with algo/source/ref/digest
- **Parameter Storage**: Per-derived-mapping `params` with computed `params_hash`

### Transform Registry Structure

```json
{
  "registry_version": "1.0.0",
  "transforms": [
    {
      "id": "t:ct_map",
      "version": "1.0.0",
      "kind": "builtin",
      "signature": {"inputs": ["string"], "output": "string"},
      "params_schema_hash": "sha256:...",
      "impl_fingerprint": {
        "algo": "sha256",
        "source": "builtin",
        "ref": "cheshbon.kernel.transforms.ct_map",
        "digest": "..."
      }
    }
  ]
}
```

### Derived Variable Transform References

Each derived variable references a transform via `transform_ref` and stores transform-specific parameters:

```json
{
  "id": "d:SEX_CDISC",
  "transform_ref": "t:ct_map",
  "params": {
    "map": {"M": "M", "F": "F", "Male": "M", "Female": "F"}
  }
}
```

**Key Design Decisions:**
- `params_hash` is **computed at load time**, not stored in spec (prevents inconsistencies)
- Transform registry is **separate artifact** (not embedded in mapping spec)
- `impl_fingerprint` is **structured** (not single hash) for sane explanations
- Transform IDs are **globally unique** within a project (no aliases, case-sensitive)

### Change Detection

The system distinguishes three orthogonal transform change types:

1. **`DERIVED_TRANSFORM_REF_CHANGED`**: Transform reference changed (e.g., `t:ct_map` -> `t:normalize`)
2. **`DERIVED_TRANSFORM_PARAMS_CHANGED`**: Parameters changed (same ref, different `params_hash`)
3. **`TRANSFORM_IMPL_CHANGED`**: Implementation changed (registry-level, `impl_fingerprint.digest` changed)

Additionally:
- **`TRANSFORM_ADDED`**: New transform in registry
- **`TRANSFORM_REMOVED`**: Transform removed from registry (hard validation error)

### Parameter Canonicalization

Parameters are canonicalized with explicit rules:
- Object keys sorted recursively
- Arrays preserve order (sets sorted with stable comparator)
- **Floats BANNED** (hard validation error - use strings for decimals)
- Strings normalized to NFC (Unicode)
- Non-JSON types forbidden

This ensures stable hashing across Python versions and environments.

### Validation

The kernel validates that all `transform_ref` values exist in the registry (if provided). Missing transforms are a **hard validation error**, not just an impact event.

## Completeness

The kernel is complete for its stated purpose: proving that change impact is computable without execution.

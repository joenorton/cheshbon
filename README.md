# cheshbon

**Deterministic impact analysis for clinical data mappings**

Diff, verify, and explain SDTM-style mapping changes across specs and registries - deterministically, before submission.

Cheshbon is a kernel-grade engine that answers a single question reliably:

It operates purely on artifacts (specs, bindings, registries) and fails loudly when invariants are violated.

> *Given mapping_spec v1 and v2, determine exactly which derived outputs are impacted-and why-without executing any transforms.*

## What Cheshbon Does

Given two versions of a mapping specification, Cheshbon determines:

1. **What changed** - structural changes, not textual diffs
2. **What's impacted** - direct vs transitive effects on derived variables
3. **Why** - explicit dependency paths explaining each impact
4. **What's unaffected** - outputs that remain valid and do not require regeneration

All analysis is:
- **Deterministic** - same inputs produce identical outputs
- **Execution-free** - no transform execution or data access required
- **Artifact-only** - operates on specs, bindings, registries
- **Reproducible** - verifiable hash chains and canonical JSON

Cheshbon is designed for clinical programmers, data engineers, and platform teams who need deterministic answers about mapping changes in regulated pipelines.

## Quick Start (OSS v1.0)

Install (from source or wheel):

```bash
pip install cheshbon
```

For development:

```bash
pip install -e .
```

Run impact analysis between two specs:

```bash
cheshbon diff --from spec_v1.json --to spec_v2.json
```

Report modes:
- `full`: human-facing markdown + JSON (default).
- `core`: minimal, machine-first JSON for automation/perf.
- `all-details`: machine-first JSON evidence (analysis only).

All-details reports assert only analytical impact semantics under the Cheshbon kernel contract; they make no claims about code execution, data correctness, or regulatory acceptance.

Verify an all-details report against artifacts:

```bash
cheshbon verify report impact.all-details.json --from spec_v1.json --to spec_v2.json
```

Report verification validates all-details artifacts (digests + witness invariants). It is not an execution validator.

Verify kernel-native artifacts (schema + integrity checks):

```bash
cheshbon verify spec spec.json
cheshbon verify registry registry.json
cheshbon verify bindings bindings.json
```

Verify and ingest SANS run bundles:

```bash
# Verify bundle integrity
cheshbon verify bundle <bundle_dir>

# Ingest bundle and materialize artifacts
cheshbon ingest sans --bundle <bundle_dir> --out <out_dir>
```

Run kernel diff directly on two SANS bundles (adapter-only, deterministic):

```bash
cheshbon run-diff --bundle-a <bundle_dir_a> --bundle-b <bundle_dir_b> --out <out_dir>
```

Reports are written to `reports/` in machine-readable (JSON) and human-readable (Markdown) formats.
When available, SANS `vars.graph.json` edge `kind` values (`flow`, `derivation`, `rename`) are preserved: `impact.json` paths are emitted as typed edge hops, and the markdown dependency paths include hop kinds.
Value-level change annotations appear only when runtime evidence provides column stats (preferred) or when small output tables can be scanned; otherwise `value_evidence` is absent or marked unavailable and no values are guessed.

## Public API

Cheshbon provides a minimal public API for programmatic use:

```python
from cheshbon import diff, validate, DiffResult, ValidationResult

# Diff analysis between two specs
result: DiffResult = diff(
    from_spec="spec_v1.json",  # Path or dict
    to_spec="spec_v2.json",     # Path or dict
    registry="registry.json"   # Optional: Path or dict
)

# Access results
print(f"Impacted: {result.impacted_ids}")
print(f"Reasons: {result.reasons}")
print(f"Paths: {result.paths}")

# Validation/preflight checks
validation_result: ValidationResult = validate(
    spec="spec_v1.json",
    registry="registry.json"  # Optional
)
```

The public API exposes only high-level functions that return complete, structured results. Internal implementation details (`_internal` modules) are not accessible from the public namespace.

## Stable API Modules

**Stable API (v1.0 contract):**
- `cheshbon.api` - Core functions and result types:
  - `diff()` - Diff analysis between two specs
  - `validate()` - Validation/preflight checks
  - `DiffResult` - Result model for diff analysis
  - `ValidationResult` - Result model for validation
- `cheshbon.contracts` - Compatibility models:
  - `CompatibilityIssue` - Compatibility issue model
  - `CompatibilityReport` - Compatibility report model

**Root exports (convenience aliases, may change):**
- Functions and types are also exported from `cheshbon` root for convenience
- Root exports are **not part of the v1.0 contract** and may be removed or changed in future versions
- For stable code, import from `cheshbon.api` and `cheshbon.contracts` explicitly

**Version:**
- `__version__` - Package version (available from root)

## The Invariant

> Given `mapping_spec` v1 and v2, the system can determine *exactly which derived outputs are impacted by the change*, and explain *why*, without re-executing the transform.

This is proven through:
- Explicit dependency graphs
- Structural change events (not raw diffs)
- Transitive closure computation
- Binding-aware validation

## Example Output

```
============================================================
MAPPING SPEC CHANGE IMPACT ANALYSIS
============================================================

## Changes Detected

- Source column added: `RFSTDT` (ID: s:RFSTDT)
- Derived variable `AGE` inputs changed (ID: d:AGE)
  - Old: ['s:BRTHDT', 's:RFSTDTC']
  - New: ['s:BRTHDT', 's:RFSTDT']

## Impact Analysis

### Impacted Variables (2)

- **AGE** (ID: d:AGE)
  - Dependency path: AGE
  - Reason: DIRECT_CHANGE
- **AGEGRP** (ID: d:AGEGRP)
  - Dependency path: AGE -> AGEGRP
  - Reason: TRANSITIVE_DEPENDENCY

### Unaffected Variables (2)

- SEX_CDISC (ID: d:SEX_CDISC)
- USUBJID (ID: d:USUBJID)
```

## Key Features

- **Stable IDs**: Identity separate from display names (renames are metadata-only)
- **Precise Impact**: Distinguishes DIRECT_CHANGE vs TRANSITIVE_DEPENDENCY
- **Unresolved References**: Explicitly tracks MISSING_INPUT, MISSING_BINDING, MISSING_TRANSFORM_REF
- **Binding Layer**: Handles raw schema drift without contaminating core ontology
- **Transform Registry**: First-class transform artifacts with fingerprinting (impl_hash, params_hash)
- **Control Plane**: Detects registry-level changes (transform impl changed) even when spec is unchanged
- **No Heuristics**: Everything is explicit and deterministic

## Project Structure

```
.
|-- src/
|   `-- cheshbon/
|       |-- kernel/                 # Core kernel modules
|       |   |-- all_details_builders.py # Shared all-details builders
|       |   |-- bindings.py         # Binding layer (raw schema -> stable IDs)
|       |   |-- binding_impact.py   # Binding-aware impact propagation
|       |   |-- diff.py             # Structural diff -> change events
|       |   |-- explain.py          # Structured explanations (no rendering)
|       |   |-- graph.py            # Dependency graph builder
|       |   |-- hash_utils.py       # Canonicalization and hashing
|       |   |-- impact.py           # Impact analysis
|       |   |-- spec.py             # Mapping spec models (Pydantic)
|       |   |-- transform_registry.py  # Transform registry models
|       |   `-- witness.py          # Witness generation for all-details
|       |-- _internal/              # Internal verification tools
|       |   |-- io/                 # Artifact I/O helpers
|       |   |-- reporting/          # Report rendering helpers
|       |   |-- benchmarks.py       # Perf sentinel benchmarks
|       |   |-- verify_artifacts.py # CLI verify helpers
|       |   `-- report_doctor.py    # All-details report doctor
|       |-- api.py                  # Public API
|       |-- cli.py                  # Main CLI entry point
|       |-- diff.py                 # Diff wrapper + report generation
|       |-- report_all_details.py   # All-details report builder
|       `-- contracts.py            # Compatibility models
|-- fixtures/                       # Golden scenario examples and test fixtures
|-- tests/                          # Pytest test suite
`-- docs/                           # Documentation
```


## Tests

```bash
pytest tests/ -v
```

All tests pass (129+ tests covering kernel, CLI, and golden scenarios).

## Documentation

### Getting Started
- [Quick Start Guide](QUICKSTART.md) - Step-by-step workflow
- [Architecture Reference](docs/architecture.md) - Core design and principles
- [Graph Diff Contract](docs/GRAPH_DIFF_CONTRACT.md) - Canonical graph-diff/impact semantics

### User Guide
- [Context and Glossary](docs/user-guide/00_context_summary.md) - Project context and terminology
- [SANS Ingestion](docs/user-guide/sans_ingestion.md) - SANS bundle ingestion and verification
- [Graph Diff](docs/user-guide/graph_diff.md) - Bundle graph diff + impact outputs
- [Change Events Ontology](docs/user-guide/change_events_ontology.md) - Change event types and impact reasons
- [Binding Layer](docs/user-guide/bindings_layer.md) - Binding layer design and usage
- [What Cheshbon Will Never Do](docs/user-guide/what-cheshbon-will-never-do.md) - Explicit non-goals and scope boundaries

### API Reference
- [v1.0 Contract](docs/api-reference/v1_CONTRACT.md) - Complete v1.0 API contract
- [Kernel Contract](CONTRACT.md) - Core kernel guarantees and exclusions

### Developer Guide
- [Implementation Notes](docs/developer-guide/IMPLEMENTATION_NOTES.md) - Transform registry implementation details
- [Key Questions](docs/developer-guide/KEY_QUESTION_ANSWER.md) - Important design decisions explained
- [Performance Sentinels](docs/developer-guide/performance-sentinels.md) - Frozen perf sentinels and cap audit

### Examples
- [Golden Scenarios](fixtures/README.md) - Golden scenario examples for `cheshbon diff`

## Non-Goals (Explicit)

- No LLM calls
- No execution engine
- No UI
- No database
- No SDTM semantics beyond toy naming
- No multi-domain orchestration

This is a kernel, not a product. It proves the invariant or fails loudly.

## Beyond the Kernel

Cheshbon OSS v1.0 is intentionally artifact-centric.

Workspace management, authoring tools, and orchestration layers live outside the kernel and may appear in future releases or commercial offerings.

Bundle tooling (zip export/import and bundle doctor) is intentionally out of kernel scope and belongs to higher-level tooling outside the kernel.

## Trademark Notice

**cheshbon(TM) is a trademark of Joe Norton. Use of the name in commercial offerings requires permission.**

See [TRADEMARK](TRADEMARK) for more information.

## License

This project is licensed under the Apache License, Version 2.0. See [NOTICE](NOTICE) for copyright information.

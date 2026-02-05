# Quick Start Guide

## Installation

```bash
pip install -e .
```

## Artifact-Centric Workflow (OSS v1.0)

Cheshbon v1.0 operates on artifacts directly - specs, bindings, and registries.

### Basic Diff Analysis

Compare two mapping specs:

```bash
cheshbon diff --from spec_v1.json --to spec_v2.json
```

This generates:
- `reports/impact.md` - Human-readable impact report (full mode)
- `reports/impact.json` - Machine-readable impact data (full/core)
- `reports/impact.all-details.json` - Machine-first analysis evidence (all-details)

Report modes:
- `full`: human-facing markdown + JSON (default).
- `core`: minimal, machine-first JSON for automation/perf.
- `all-details`: machine-first JSON evidence (analysis only).

All-details reports assert only analytical impact semantics under the Cheshbon kernel contract; they make no claims about code execution, data correctness, or regulatory acceptance.

### Health Checks

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

## Programmatic API

For programmatic usage (Python imports), see the [Public API section in README](README.md#public-api).

## Golden Scenarios

See `fixtures/README.md` for 5 golden scenarios demonstrating:

1. **Rename-only** (no impact) - Exit code 0
2. **Params change** (direct + transitive impact) - Exit code 1
3. **Registry impl change** (no spec change) - Exit code 1
4. **Transform removed** (validation failed) - Exit code 2
5. **Ambiguous binding** (validation failure) - Exit code 2

Run examples:
```bash
cheshbon diff --from fixtures/scenario1_rename_no_impact/spec_v1.json --to fixtures/scenario1_rename_no_impact/spec_v2.json
```

## What You'll See

- **Change events**: Normalized structural changes (not raw diffs)
- **Impact analysis**: Exactly which derived variables are impacted and why
- **Dependency paths**: Full chains showing how impact propagates
- **Reason codes**: DIRECT_CHANGE, TRANSITIVE_DEPENDENCY, MISSING_BINDING, MISSING_TRANSFORM_REF
- **Registry events**: TRANSFORM_IMPL_CHANGED, TRANSFORM_ADDED, TRANSFORM_REMOVED
- **Forwardable reports**: Markdown and JSON reports suitable for review

All computed without executing any transforms.

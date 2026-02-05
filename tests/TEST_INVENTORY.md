# Test Inventory

This file is a high-level map of what each test file is validating and why. It is meant as a quick reference for maintenance, not a substitute for reading the tests.

## Core Kernel Correctness
- `test_spec.py` - Spec model validation (IDs, transform_ref format), params hashing, canonicalization of inputs, and change-event stability under reordering.
- `test_graph.py` - DependencyGraph construction, direct/transitive dependents, and path discovery.
- `test_cycle_detection.py` - Cycle detection in derived chains and constraint/derived cycles.
- `test_impact.py` - Impact computation correctness (direct, transitive, missing input, missing transform, alternative paths).
- `test_binding_impact.py` - Binding-aware impact propagation for missing bindings and rename-safe bindings.
- `test_ambiguous_bindings.py` - Ambiguous binding detection and precedence rules (ambiguous beats missing).
- `test_ambiguous_binding_validation_failure.py` - Ambiguous bindings are terminal (validation_failed semantics).
- `test_change_events.py` - Expected change events for a fixture amendment (ontology sanity check).
- `test_transform_history_immutability.py` - Transform history immutability and persistent update semantics.

## Canonicalization and Hashing
- `test_hash_utils.py` - Canonical JSON rules, hash determinism, and hard errors (floats, non-JSON types).
- `test_canonicalization_edge_cases.py` - Edge cases for canonicalization, event orthogonality, registry diff behavior, and determinism.

## Report Generation and Verification
- `test_all_details_report.py` - All-details report determinism, required structure, and reason stability under event ordering.
- `test_diff_report_modes.py` - Report mode behavior (full/core/all-details/off) and fields included/excluded.
- `test_doctor_report.py` - Report verification (digest check, witness invariants, linkage relevance, omission honesty, tamper detection).

## CLI and API Surface
- `test_diff.py` - CLI diff end-to-end scenarios (golden cases), report generation, and exit codes.
- `test_verify_cli.py` - CLI verify commands for report/spec/registry/bindings (success paths and error/warn semantics).
- `test_api.py` - Public API behavior, result shape, determinism, registry-only impact, and export hygiene.
- `test_validate_api.py` - validate() behavior and its contract alignment with diff() (errors vs warnings).
- `test_public_surface.py` - Import behavior, no shadowing, root exports boundaries.
- `test_api_stability.py` - Stable modules (cheshbon.api/contracts) work without root imports.

## Packaging and Boundary Guardrails
- `test_kernel_guardrails.py` - Kernel must remain side-effect-free (no I/O/OS-time/etc.).
- `test_no_backend_imports.py` - OSS package must not import backend/hosted dependencies; _internal not in public API.
- `test_packaging.py` - Package structure and exclusion of backend/frontend.
- `test_schema_stability.py` - JSON schema snapshots (checksums) stay stable unless intentionally updated.
- `test_pydantic_configdict_migration.py` - Pydantic v2 ConfigDict migration behavior for legacy and v0.7 schema models.

## Performance (Gated)
- `perf/test_sentinels_benchmark.py` - Performance sentinel budgets for large graph shapes (marked with `@pytest.mark.perf`).

## Test Infrastructure
- `conftest.py` - Pytest basetemp handling and cleanup hook (Windows hygiene).
- `__init__.py` - Test package marker (no logic).

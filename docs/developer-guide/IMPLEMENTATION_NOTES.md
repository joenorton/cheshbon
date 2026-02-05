# Implementation Notes: Transform Registry and Fingerprinting

## Addressed Issues

### 1. Three Failure Modes - Mitigations

#### a) Canonicalization Edge Cases

**Mitigations implemented:**

- **Mixed-type arrays**: Set-sorting uses stable (type_tag, value) comparator, not Python's default ordering
- **Unicode normalization**: All strings normalized to NFC before hashing
- **None vs missing keys**: Explicitly documented - None is a valid JSON value, missing keys are not represented. They hash differently (by design).
- **Tests added**: `test_canonicalization_edge_cases.py` covers all edge cases

**Tell-tale symptoms to watch for:**
- Diffs firing when "nothing changed" -> check unicode normalization, key ordering
- Hashes changing across runs -> check set-sorting comparator, None semantics

#### b) Registry/Spec Mismatch

**Mitigations implemented:**

- **Hard validation**: `validate_transform_refs()` raises errors for missing transforms
- **Case-sensitive IDs**: Transform IDs must be lowercase (enforced in validator)
- **Global uniqueness**: Registry validates no duplicate IDs on load
- **Tests**: Will surface in test failures - fix fixtures/specs as needed

**Tell-tale symptoms:**
- "missing transform" errors in tests -> check fixture registry entries, case mismatches
- Legacy transforms not migrated -> update all `transform_id` to `transform_ref` with `t:` prefix

#### c) Impl Fingerprint Churn

**Mitigations implemented:**

- **Relative paths only**: `impl_fingerprint.ref` validator rejects absolute paths
- **Structured fingerprint**: Enables sane explanations without path churn
- **Digest-based comparison**: Only digest changes trigger events (not ref changes)

**Tell-tale symptoms:**
- TRANSFORM_IMPL_CHANGED firing on every machine -> check for absolute paths in ref field

### 2. Minimum "Prove It Works" Test Sweep

**Tests added in `test_canonicalization_edge_cases.py`:**

✅ **Canonicalization determinism**
- `test_same_params_same_digest_repeated_runs`
- `test_semantic_object_key_order_independent`
- `test_unicode_normalization_nfc`
- `test_mixed_type_array_set_sorting`
- `test_none_vs_missing_key_semantics`

✅ **Event orthogonality**
- `test_only_transform_ref_changed`
- `test_only_params_changed`
- `test_only_registry_impl_changed`

✅ **Blast radius correctness**
- `test_transform_impl_changed_impacts_all_users`

✅ **No rename noise**
- `test_derived_rename_no_transform_events`

### 3. Params Discipline Check

**Enforcement added:**

- **Pure JSON validation**: `validate_params()` in `DerivedVariable` calls `canonicalize_json()` which bans floats and non-JSON types
- **Size warning**: Warns if params > 10KB (discipline check, not hard error)
- **Schema governance**: `params_schema_hash` in registry enforces shape (future: validate against schema)

**Rule**: Params must be:
- Pure JSON (no floats, no non-JSON types) - **enforced**
- Small (warn if > 10KB) - **warned**
- Schema-governed (params_schema_hash) - **tracked, validation TBD**

### 4. Polish Items

✅ **impl_fingerprint.ref relative paths**: Validator rejects absolute paths
✅ **TRANSFORM_ADDED/REMOVED order-independent**: Registry diff sorts by transform_id for deterministic output
✅ **Explain output enhanced**:
- Old/new transform_ref for ref change
- Old/new params digests for params change
- Impl fingerprint ref + digest old/new for impl change

### 5. Key Question Answer

**Question**: "Nothing in the spec changed, but outputs might change because transform implementation changed"

**Answer**: **YES** - See `docs/KEY_QUESTION_ANSWER.md` for full explanation.

The kernel now unambiguously detects this scenario:
- Spec v1 == Spec v2 (no spec changes)
- Registry v1 != Registry v2 (impl_fingerprint.digest changed)
- Kernel emits: `TRANSFORM_IMPL_CHANGED`
- Impact: All derived vars using that transform_ref marked as impacted

This crosses the threshold from "nice diff tool" to **"control plane"**.

## Transform Events Section for Semantic Audit

See `docs/transform_events_semantic_audit.md` for the complete semantic audit section with:
- Event definitions
- Precedence rules
- Conflict resolution
- Key invariants

## Running Targeted Tests

```bash
# Run canonicalization determinism tests
pytest tests/test_canonicalization_edge_cases.py::TestCanonicalizationDeterminism -v

# Run event orthogonality tests
pytest tests/test_canonicalization_edge_cases.py::TestEventOrthogonality -v

# Run blast radius tests
pytest tests/test_canonicalization_edge_cases.py::TestBlastRadiusCorrectness -v

# Run no-rename-noise tests
pytest tests/test_canonicalization_edge_cases.py::TestNoRenameNoise -v
```

## Pytest Temp Directory Hygiene (Windows)

Preferred approach:
- Set a predictable `--basetemp` in config (see `pyproject.toml`) rather than patching pytest internals.
- If cleanup hardening is needed, do it in a narrow `pytest_sessionfinish` hook and only for the configured basetemp.
- If temp cleanup fails, treat it as a signal of leaked file handles; fix the leak instead of masking it.

## CLI Implementation Status

The `cheshbon` CLI provides artifact-centric analysis:

### Core Commands (OSS v1.0)

✅ **`cheshbon diff <spec1> <spec2>`** - Run impact analysis and generate reports
✅ **`cheshbon verify report <impact.all-details.json>`** - Verify all-details report against artifacts
✅ **`cheshbon verify spec <spec.json>`** - Verify spec artifact (schema + invariants)
✅ **`cheshbon verify registry <registry.json>`** - Verify registry artifact (schema + invariants)
✅ **`cheshbon verify bindings <bindings.json>`** - Verify bindings artifact (schema + invariants)


### Report Generation

The `diff` command generates:
- `reports/impact.md` - Skim-friendly markdown report
- `reports/impact.json` - Structured JSON for automation
- `reports/impact.all-details.json` - Machine-first analysis evidence (all-details)

Report modes:
- `full`: markdown + JSON (default)
- `core`: minimal JSON for automation/perf
- `all-details`: machine-first JSON evidence (analysis only)

All-details reports assert only analytical impact semantics under the Cheshbon kernel contract; they make no claims about code execution, data correctness, or regulatory acceptance.

Report verification validates all-details artifacts (digests + witness invariants). It is not an execution validator.

Exit codes:
- `0` = no impact
- `1` = impact found
- `2` = validation_failed (non-executable)

### Examples

See `examples/` directory for 4 golden scenarios demonstrating all diff capabilities.

## Transform History Tracking

**Purpose**: Append-only metadata for audit trail. Enables answering: "this value changed because the derivation changed on date X, not because data drifted."

**Implementation**:
- `TransformHistory` model: timestamp (ISO 8601), impl_fingerprint snapshot, params_schema_hash snapshot, optional change_reason
- `history` field on `TransformEntry`: append-only list, never deleted
- `add_history_entry()` method: creates immutable snapshot with current timestamp
- `get_history_for_transform()` method: retrieves all history entries

**Usage**:
- History is preserved when loading registries from JSON
- History is append-only: new entries added, old entries never removed
- History is queryable: can retrieve full change timeline for any transform
- History is not used for diffing: diff still compares current impl_fingerprint.digest

**Design Decision**: History is legal armor, not analytics. It's for audit trails and answering "when did this change?" questions, not for trend analysis or UI charts.

## Known Issues to Watch

1. **Canonicalization**: Monitor for false diffs - may need to tighten rules further
2. **Registry validation**: Tests may fail until all fixtures updated
3. **Params discipline**: Size warnings will appear if params bloat - enforce schema validation
4. **Impl fingerprint refs**: Ensure all refs are relative paths in production registries
5. **Transform history**: History entries accumulate over time - consider archival strategy for very old entries (future)

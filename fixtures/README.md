# Fixtures for Cheshbon Kernel

This directory contains example fixtures demonstrating the golden scenarios for `cheshbon diff`.

## Golden Scenarios

### 1. Rename-only (No Impact)
**Scenario**: Only derived variable name changed, ID unchanged
- `spec_v1.json` - `d:USUBJID` with name "USUBJID"
- `spec_v2.json` - `d:USUBJID` with name "SUBJECT_ID" (same ID, different name)
- **Expected**: Exit code 0, no impact

### 2. Params Change (Direct + Transitive Impact)
**Scenario**: Transform params changed, causing direct and transitive impact
- `spec_v1.json` - `d:SEX` with params `{"map": {"M": "M", "F": "F"}}`
- `spec_v2.json` - `d:SEX` with params `{"map": {"M": "M", "F": "F", "U": "UNKNOWN"}}`
- `d:SEX_CDISC` depends on `d:SEX`
- **Expected**: Exit code 1, both `d:SEX` and `d:SEX_CDISC` impacted

### 3. Registry Impl Change (No Spec Change)
**Scenario**: Transform implementation digest changed in registry, spec unchanged
- `spec_v1.json` and `spec_v2.json` - identical specs
- `registry_v1.json` - `t:direct_copy` with digest "aaa..."
- `registry_v2.json` - `t:direct_copy` with digest "bbb..." (impl changed)
- **Expected**: Exit code 1, `d:USUBJID` impacted due to impl change

### 4. Transform Removed (Validation Failed)
**Scenario**: Transform removed from registry, spec still references it
- `spec_v1.json` and `spec_v2.json` - identical specs using `t:direct_copy`
- `registry_v1.json` - contains `t:direct_copy`
- `registry_v2.json` - empty (transform removed)
- **Expected**: Exit code 2, validation failed, but full report generated

### 5. Ambiguous Binding
**Scenario**: Constraint ambiguous binding validation failure
- Demonstrates binding validation edge cases
- **Expected**: Validation error for ambiguous bindings

## Usage

```bash
# Scenario 1: Rename-only
cheshbon diff --from fixtures/scenario1_rename_no_impact/spec_v1.json --to fixtures/scenario1_rename_no_impact/spec_v2.json

# Scenario 2: Params change
cheshbon diff --from fixtures/scenario2_params_change_impact/spec_v1.json --to fixtures/scenario2_params_change_impact/spec_v2.json

# Scenario 3: Registry impl change
cheshbon diff \
  --from fixtures/scenario3_registry_impl_change/spec_v1.json \
  --to fixtures/scenario3_registry_impl_change/spec_v2.json \
  --registry-v1 fixtures/scenario3_registry_impl_change/registry_v1.json \
  --registry-v2 fixtures/scenario3_registry_impl_change/registry_v2.json

# Scenario 4: Transform removed
cheshbon diff \
  --from fixtures/scenario4_transform_removed/spec_v1.json \
  --to fixtures/scenario4_transform_removed/spec_v2.json \
  --registry-v1 fixtures/scenario4_transform_removed/registry_v1.json \
  --registry-v2 fixtures/scenario4_transform_removed/registry_v2.json
```

## Additional Fixtures

The root `fixtures/` directory also contains standalone spec files from the original `specs/` directory for use with demo scripts and manual testing.

## Run-Diff Bundles

Minimal SANS bundle pairs for `cheshbon run-diff` live in:

- `fixtures/run_diff/ex1`
- `fixtures/run_diff/ex2`

These bundles contain `report.json`, `artifacts/plan.ir.json`, and `artifacts/vars.graph.json` only. They are intentionally minimal to exercise the adapter funnel into the kernel diff pipeline.

Each bundle also includes `transform_registry.json` to cover the registry ingestion path.

## Run-Diff Value Evidence Bundles

Bundles with runtime column stats for value-evidence annotations live in:

- `fixtures/run_diff_value/ex1` (label constant HIGH)
- `fixtures/run_diff_value/ex2` (label constant LOW)

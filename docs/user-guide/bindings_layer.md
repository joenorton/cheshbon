# Bindings Layer

## Purpose

Bindings connect the messy reality of raw schema columns to stable source IDs in the mapping spec. This allows the kernel to handle **raw schema drift** without contaminating the pure mapping_spec ontology.

## Key Insight

**Bindings are the adapter that lets stable IDs survive raw schema churn without heuristics.**

When a raw column is renamed upstream (e.g., `RFSTDTC` -> `RFSTDT`), the binding layer can update the mapping (`RFSTDT` -> `s:RFSTDTC`) while keeping the source ID stable. This means:
- The mapping spec doesn't need to change
- Derived variables continue to reference `s:RFSTDTC`
- No impact is generated (binding updated correctly)

## Artifacts

### Raw Schema Snapshot
```json
{
  "table": "RAW_DM",
  "columns": [
    {"name": "RFSTDTC", "type": "date"}
  ]
}
```

### Bindings
```json
{
  "table": "RAW_DM",
  "bindings": {
    "RFSTDTC": "s:RFSTDTC"
  }
}
```

## Three Scenarios

### Scenario 1: Binding Updated (No Impact)
- Raw schema: `RFSTDTC` -> `RFSTDT`
- Binding updated: `RFSTDT` -> `s:RFSTDTC`
- Result: **No impact** - source ID remains bound, mapping spec unchanged

### Scenario 2: Binding Not Updated (Impact)
- Raw schema: `RFSTDTC` -> `RFSTDT`
- Binding not updated: `s:RFSTDTC` has no binding
- Result: **MISSING_BINDING** - `d:AGE` and `d:AGEGRP` impacted

### Scenario 3: Type Incompatibility (Currently Silent - Gap)
- Mapping spec: `s:BRTHDT` has type `date`
- Binding: `BRTHDT` -> `s:BRTHDT` [OK]
- Raw schema v1: `BRTHDT` has type `date` [OK]
- Raw schema v2: `BRTHDT` has type `string` ✗
- **Current behavior**: Silent - no error, no impact, no detection
- **Future behavior**: Should detect `RAW_COLUMN_TYPE_CHANGED` binding event and mark dependents with `SOURCE_TYPE_MISMATCH` impact reason

## Design Principles

1. **Separate ontology**: Binding events are separate from mapping_spec change events
2. **Validation**: Check bindings against schema, check required source IDs against bindings
3. **Explicit failure modes**: `MISSING_BINDING` is distinct from `MISSING_INPUT`
4. **No heuristics**: Bindings are explicit, not inferred

## Event Type Considerations for Type Incompatibility

### Why BindingEvent, Not ChangeEvent?

Type incompatibility is a **binding-layer issue**, not a spec change:
- The mapping spec hasn't changed - `s:BRTHDT` still expects `date`
- The raw column type changed upstream - raw schema now has `string`
- This is analogous to `MISSING_BINDING` (binding layer problem), not `DERIVED_TYPE_CHANGED` (spec change)

### Proposed Event Types

**BindingEvent** (binding layer):
- `RAW_COLUMN_TYPE_CHANGED`: Raw column type changed incompatibly with bound source type
  - Detected when comparing raw schema types against source column types from spec
  - Example: `BRTHDT` changed from `date` -> `string` while bound to `s:BRTHDT` (expects `date`)

**Impact Reason** (impact analysis):
- `SOURCE_TYPE_MISMATCH`: Source column type incompatible with bound raw column type
  - Parallel to `MISSING_BINDING` - both are binding-layer issues that impact derived variables
  - Should propagate transitively like `MISSING_BINDING`

### Design Questions

1. **Type compatibility rules**: What constitutes incompatibility?
   - Exact match required? (`date` ≠ `datetime`)
   - Compatible conversions allowed? (`int` -> `float` OK?)
   - Need a type compatibility matrix

2. **Event granularity**:
   - Detect on binding diff (v1 -> v2 schema comparison)?
   - Or detect on single schema validation (current schema vs spec)?

3. **Impact propagation**:
   - Should type mismatch impact transitively like `MISSING_BINDING`?
   - Or only direct dependents?

## Integration

The binding layer sits **above** the kernel but **below** execution:
- Raw schema + bindings -> validate -> check missing bindings
- Missing bindings -> impact analysis (MISSING_BINDING reason)
- Mapping spec diff -> impact analysis (structural changes)

This keeps the kernel pure while handling the messy boundary.

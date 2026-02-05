# Change Events Ontology

## Normalized Change Events for Current Amendment (v1 -> v2)

```
SOURCE_ADDED: s:RFSTDT
  New: RFSTDT

DERIVED_INPUTS_CHANGED: d:AGE
  Old: ['s:BRTHDT', 's:RFSTDTC']
  New: ['s:BRTHDT', 's:RFSTDT']
  Details: {'old_inputs': ['s:BRTHDT', 's:RFSTDTC'], 'new_inputs': ['s:BRTHDT', 's:RFSTDT']}
```

## Event Types

### Source Column Events
- **SOURCE_RENAMED**: Name changed, ID unchanged (non-impacting metadata)
- **SOURCE_REMOVED**: ID removed from spec (impacting: all dependents get MISSING_INPUT)
- **SOURCE_ADDED**: New ID added (non-impacting for existing derived vars)

### Derived Variable Events
- **DERIVED_RENAMED**: Name changed, ID unchanged (non-impacting metadata)
- **DERIVED_REMOVED**: ID removed (impacting: all dependents get MISSING_INPUT)
- **DERIVED_ADDED**: New ID added (non-impacting for existing derived vars)
- **DERIVED_TRANSFORM_REF_CHANGED**: transform_ref changed (impacting: DIRECT_CHANGE)
- **DERIVED_TRANSFORM_PARAMS_CHANGED**: params_hash changed, transform_ref unchanged (impacting: DIRECT_CHANGE)
- **DERIVED_TYPE_CHANGED**: type changed (impacting: DIRECT_CHANGE)
- **DERIVED_INPUTS_CHANGED**: inputs list changed (impacting: DIRECT_CHANGE, may include MISSING_INPUT if references unresolved)

### Constraint Events
- **CONSTRAINT_RENAMED**: Name changed, ID unchanged (non-impacting metadata)
- **CONSTRAINT_REMOVED**: ID removed (impacting: all dependents get MISSING_INPUT)
- **CONSTRAINT_ADDED**: New ID added (non-impacting for existing derived vars)
- **CONSTRAINT_INPUTS_CHANGED**: inputs list changed (impacting: TRANSITIVE_DEPENDENCY on dependents)
- **CONSTRAINT_EXPRESSION_CHANGED**: expression changed (impacting: TRANSITIVE_DEPENDENCY on dependents)

Constraints are first-class graph nodes with boolean outputs. They participate in the same dependency graph, diff system, and impact logic as derived variables.

### Transform Registry Events
- **TRANSFORM_ADDED**: New transform added to registry (non-impacting for existing derived vars)
- **TRANSFORM_REMOVED**: Transform removed from registry (impacting: all derived vars using it get MISSING_TRANSFORM_REF, hard validation error)
- **TRANSFORM_IMPL_CHANGED**: Transform implementation changed (impl_fingerprint.digest changed, impacting: all derived vars using that transform_ref)

## Impact Reason Codes

- **DIRECT_CHANGE**: The variable itself changed (inputs/type/transform_ref/params)
- **DIRECT_CHANGE_MISSING_INPUT**: The variable changed AND has unresolved input references
- **MISSING_INPUT**: A required input was removed (source/derived ID no longer exists)
- **TRANSITIVE_DEPENDENCY**: Depends on an impacted node (not directly changed)
- **TRANSFORM_IMPL_CHANGED**: Transform implementation changed (registry-level, affects all derived vars using that transform_ref)
- **MISSING_TRANSFORM_REF**: Required transform not found in registry (hard validation error)
- **MISSING_BINDING**: Required source ID not bound to any raw column in extract (binding layer issue)
- **AMBIGUOUS_BINDING**: Multiple raw columns map to the same source ID (cannot determine which to use, terminal failure)

## Design Decisions

1. **Stable IDs**: Identity is separate from display names. Renames are metadata-only.
2. **Order-agnostic inputs**: Input lists are treated as sets. Reordering doesn't trigger changes.
3. **Unresolved references**: Explicitly tracked and reported. Missing inputs are a distinct failure mode.
4. **Transitive closure**: Impact propagates through dependency chains automatically.
5. **Shortest path reporting**: Explanation engine always reports shortest dependency path (epistemic hygiene). Alternative path counts are reported when > 0 (diamond dependency warning).

## Validation

The ontology handles:
- ✅ Structural changes (input ID changes, transform_ref changes, params changes)
- ✅ Missing references (removed sources/derived/constraints, removed transforms)
- ✅ Direct vs transitive impact (distinct reason codes)
- ✅ Changed-but-unreferenced nodes (still marked as impacted)
- ✅ Non-impacting metadata (renames, additions)
- ✅ Transform implementation changes (registry-level, affects all users of transform)
- ✅ Transform parameter changes (same transform, different params)
- ✅ Missing bindings (source ID not bound to any raw column)
- ✅ Ambiguous bindings (multiple raw columns map to same source ID, terminal failure)
- ✅ Constraint changes (constraints are first-class graph nodes with boolean outputs)

This ontology should survive real-world churn because:
- Events are normalized (not raw diffs)
- Identity is stable (IDs don't change)
- Impact reasons are explicit (not inferred)
- Unresolved references are tracked (not hidden)

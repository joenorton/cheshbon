# Key Question: Does the kernel answer "nothing in spec changed, but outputs might change because transform implementation changed"?

## Answer: YES

The kernel now **unambiguously** answers this question through the transform registry system.

## How It Works

### Scenario: Spec Unchanged, Transform Implementation Changed

1. **Spec v1 and v2 are identical** (no spec diff events)
2. **Registry v1 and v2 differ only in impl_fingerprint.digest** for a transform
3. **Kernel emits**: `TRANSFORM_IMPL_CHANGED` event
4. **Impact analysis marks**: All derived vars using that transform_ref as impacted with reason `TRANSFORM_IMPL_CHANGED`

### Example

```python
# Spec v1 == Spec v2 (no changes)
spec_v1 = MappingSpec(...)  # Has d:SEX_CDISC using t:ct_map
spec_v2 = MappingSpec(...)  # Identical to v1

# Registry v1
registry_v1 = TransformRegistry(
    transforms=[
        TransformEntry(
            id="t:ct_map",
            impl_fingerprint=ImplFingerprint(digest="abc123", ...)
        )
    ]
)

# Registry v2 - only impl changed
registry_v2 = TransformRegistry(
    transforms=[
        TransformEntry(
            id="t:ct_map",
            impl_fingerprint=ImplFingerprint(digest="def456", ...)  # Changed!
        )
    ]
)

# Kernel detects:
# - No spec changes (spec_v1 == spec_v2)
# - TRANSFORM_IMPL_CHANGED: t:ct_map (digest abc123 -> def456)
# - Impact: d:SEX_CDISC is impacted with reason TRANSFORM_IMPL_CHANGED
```

## What This Means

**Before**: "Transform changed" was a single mushy event that couldn't distinguish:
- Did the spec change? (ref or params)
- Did the implementation change? (registry-level)

**After**: Three orthogonal events:
1. `DERIVED_TRANSFORM_REF_CHANGED` - spec changed (ref)
2. `DERIVED_TRANSFORM_PARAMS_CHANGED` - spec changed (params)
3. `TRANSFORM_IMPL_CHANGED` - registry changed (impl), spec unchanged

## The Control Plane Threshold

**Question**: "Nothing in the spec changed, but outputs might change because transform implementation changed"

**Answer**: YES - the kernel detects this as `TRANSFORM_IMPL_CHANGED` and marks all affected derived vars as impacted.

This crosses the line from "nice diff tool" to **"control plane"** because:
- The kernel can detect changes outside the spec (registry-level)
- The kernel can explain why outputs changed even when spec didn't change
- The kernel provides actionable information: "transform t:ct_map implementation changed, affecting d:SEX_CDISC"

## Verification

Run the test: `test_transform_impl_changed_impacts_all_users` in `test_canonicalization_edge_cases.py` to see this in action.

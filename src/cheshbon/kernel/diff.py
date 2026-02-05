"""Structural diff between mapping_spec v1 and v2."""

from typing import List, Literal, Optional
from dataclasses import dataclass
import json
from .spec import MappingSpec
from .transform_registry import TransformRegistry


@dataclass
class ChangeEvent:
    """A single change event between two specs."""
    change_type: Literal[
        "SOURCE_RENAMED",  # Source column name changed (ID unchanged)
        "SOURCE_REMOVED",  # Source column removed (ID not in v2)
        "SOURCE_ADDED",  # Source column added (ID not in v1)
        "DERIVED_RENAMED",  # Derived variable name changed (ID unchanged)
        "DERIVED_REMOVED",  # Derived variable removed (ID not in v2)
        "DERIVED_ADDED",  # Derived variable added (ID not in v1)
        "DERIVED_TRANSFORM_REF_CHANGED",  # transform_ref changed
        "DERIVED_TRANSFORM_PARAMS_CHANGED",  # params_hash changed (ref unchanged)
        "DERIVED_TYPE_CHANGED",  # type changed
        "DERIVED_INPUTS_CHANGED",  # inputs list changed (add/remove/reorder)
        "CONSTRAINT_RENAMED",  # Constraint name changed (ID unchanged)
        "CONSTRAINT_REMOVED",  # Constraint removed (ID not in v2)
        "CONSTRAINT_ADDED",  # Constraint added (ID not in v1)
        "CONSTRAINT_INPUTS_CHANGED",  # Constraint inputs list changed
        "CONSTRAINT_EXPRESSION_CHANGED",  # Constraint expression changed
        "TRANSFORM_IMPL_CHANGED",  # Registry-level: impl_fingerprint.digest changed
        "TRANSFORM_ADDED",  # New transform in registry
        "TRANSFORM_REMOVED",  # Transform removed from registry
    ]
    element_id: str  # Stable ID of the element (s:xxx, d:xxx, c:xxx, or t:xxx for transforms)
    old_value: str | None = None  # Old name/value
    new_value: str | None = None  # New name/value
    details: dict | None = None


def diff_specs(spec_v1: MappingSpec, spec_v2: MappingSpec) -> List[ChangeEvent]:
    """
    Compute structural diff between two mapping specs.
    Returns list of change events in canonical format.
    Uses stable IDs to track identity across versions.
    """
    events: List[ChangeEvent] = []
    
    # Build ID-indexed maps
    sources_v1 = {s.id: s for s in spec_v1.sources}
    sources_v2 = {s.id: s for s in spec_v2.sources}
    derived_v1 = {d.id: d for d in spec_v1.derived}
    derived_v2 = {d.id: d for d in spec_v2.derived}
    constraints_v1 = {c.id: c for c in (spec_v1.constraints or [])}
    constraints_v2 = {c.id: c for c in (spec_v2.constraints or [])}
    
    source_ids_v1 = set(sources_v1.keys())
    source_ids_v2 = set(sources_v2.keys())
    derived_ids_v1 = set(derived_v1.keys())
    derived_ids_v2 = set(derived_v2.keys())
    constraint_ids_v1 = set(constraints_v1.keys())
    constraint_ids_v2 = set(constraints_v2.keys())
    
    # Source column changes
    for source_id in source_ids_v1 - source_ids_v2:
        events.append(ChangeEvent(
            change_type="SOURCE_REMOVED",
            element_id=source_id,
            old_value=sources_v1[source_id].name,
            new_value=None
        ))
    
    for source_id in source_ids_v2 - source_ids_v1:
        events.append(ChangeEvent(
            change_type="SOURCE_ADDED",
            element_id=source_id,
            old_value=None,
            new_value=sources_v2[source_id].name
        ))
    
    # Check for source renames (same ID, different name)
    for source_id in source_ids_v1 & source_ids_v2:
        s1 = sources_v1[source_id]
        s2 = sources_v2[source_id]
        if s1.name != s2.name:
            events.append(ChangeEvent(
                change_type="SOURCE_RENAMED",
                element_id=source_id,
                old_value=s1.name,
                new_value=s2.name
            ))
    
    # Derived variable changes
    for derived_id in derived_ids_v1 - derived_ids_v2:
        events.append(ChangeEvent(
            change_type="DERIVED_REMOVED",
            element_id=derived_id,
            old_value=derived_v1[derived_id].name,
            new_value=None
        ))
    
    for derived_id in derived_ids_v2 - derived_ids_v1:
        events.append(ChangeEvent(
            change_type="DERIVED_ADDED",
            element_id=derived_id,
            old_value=None,
            new_value=derived_v2[derived_id].name
        ))
    
    # Check for changes in existing derived variables (same ID)
    for derived_id in derived_ids_v1 & derived_ids_v2:
        d1 = derived_v1[derived_id]
        d2 = derived_v2[derived_id]
        
        # Name change
        if d1.name != d2.name:
            events.append(ChangeEvent(
                change_type="DERIVED_RENAMED",
                element_id=derived_id,
                old_value=d1.name,
                new_value=d2.name
            ))
        
        # Transform ref change
        if d1.transform_ref != d2.transform_ref:
            events.append(ChangeEvent(
                change_type="DERIVED_TRANSFORM_REF_CHANGED",
                element_id=derived_id,
                old_value=d1.transform_ref,
                new_value=d2.transform_ref
            ))
            # Note: When transform_ref changes, params_hash is intentionally not compared.
            # Params are transform-specific and only meaningful in the context of the referenced transform.
            # Comparing params_hash across different transform_refs would be type-unsafe.
            # Spec diff is structural only - it doesn't check registry existence.
            # Even if the new ref is missing in the registry, DERIVED_TRANSFORM_REF_CHANGED is still emitted.
        
        # Transform params change (same ref, different params_hash)
        elif d1.params_hash != d2.params_hash:
            # Only check params if transform_ref is unchanged (type-safety: params are transform-specific)
            events.append(ChangeEvent(
                change_type="DERIVED_TRANSFORM_PARAMS_CHANGED",
                element_id=derived_id,
                old_value=d1.params_hash,
                new_value=d2.params_hash,
                details={"transform_ref": d1.transform_ref}
            ))
        
        # Type change
        if d1.type != d2.type:
            events.append(ChangeEvent(
                change_type="DERIVED_TYPE_CHANGED",
                element_id=derived_id,
                old_value=d1.type,
                new_value=d2.type
            ))
        
        # Inputs change (already canonicalized at parse time, so direct comparison works)
        if d1.inputs != d2.inputs:
            # Use JSON serialization for stable string representation (not str(list(...)))
            events.append(ChangeEvent(
                change_type="DERIVED_INPUTS_CHANGED",
                element_id=derived_id,
                old_value=json.dumps(list(d1.inputs), sort_keys=False),  # Already sorted from canonicalization
                new_value=json.dumps(list(d2.inputs), sort_keys=False),
                details={"old_inputs": list(d1.inputs), "new_inputs": list(d2.inputs)}
            ))
    
    # Constraint changes
    for constraint_id in constraint_ids_v1 - constraint_ids_v2:
        events.append(ChangeEvent(
            change_type="CONSTRAINT_REMOVED",
            element_id=constraint_id,
            old_value=constraints_v1[constraint_id].name,
            new_value=None
        ))
    
    for constraint_id in constraint_ids_v2 - constraint_ids_v1:
        events.append(ChangeEvent(
            change_type="CONSTRAINT_ADDED",
            element_id=constraint_id,
            old_value=None,
            new_value=constraints_v2[constraint_id].name
        ))
    
    # Check for changes in existing constraints (same ID)
    for constraint_id in constraint_ids_v1 & constraint_ids_v2:
        c1 = constraints_v1[constraint_id]
        c2 = constraints_v2[constraint_id]
        
        # Name change
        if c1.name != c2.name:
            events.append(ChangeEvent(
                change_type="CONSTRAINT_RENAMED",
                element_id=constraint_id,
                old_value=c1.name,
                new_value=c2.name
            ))
        
        # Inputs change (already canonicalized at parse time, so direct comparison works)
        if c1.inputs != c2.inputs:
            # Use JSON serialization for stable string representation (not str(list(...)))
            events.append(ChangeEvent(
                change_type="CONSTRAINT_INPUTS_CHANGED",
                element_id=constraint_id,
                old_value=json.dumps(list(c1.inputs), sort_keys=False),  # Already sorted from canonicalization
                new_value=json.dumps(list(c2.inputs), sort_keys=False),
                details={"old_inputs": list(c1.inputs), "new_inputs": list(c2.inputs)}
            ))
        
        # Expression change
        if c1.expression != c2.expression:
            events.append(ChangeEvent(
                change_type="CONSTRAINT_EXPRESSION_CHANGED",
                element_id=constraint_id,
                old_value=c1.expression or "",
                new_value=c2.expression or ""
            ))
    
    return events


def diff_registries(registry_v1: TransformRegistry, registry_v2: TransformRegistry) -> List[ChangeEvent]:
    """Diff two transform registries.
    
    Compares impl_fingerprint.digest for each transform_ref (version is informational,
    impl_fingerprint is authoritative). Emits events for impl changes, additions, and removals.
    
    Args:
        registry_v1: First registry version
        registry_v2: Second registry version
    
    Returns:
        List of ChangeEvent objects
    """
    events: List[ChangeEvent] = []
    
    # Build ID-indexed maps
    transforms_v1 = {t.id: t for t in registry_v1.transforms}
    transforms_v2 = {t.id: t for t in registry_v2.transforms}
    
    transform_ids_v1 = set(transforms_v1.keys())
    transform_ids_v2 = set(transforms_v2.keys())
    
    # Transform added
    for transform_id in transform_ids_v2 - transform_ids_v1:
        version = transforms_v2[transform_id].version
        new_value = version if version else None
        events.append(ChangeEvent(
            change_type="TRANSFORM_ADDED",
            element_id=transform_id,
            old_value=None,
            new_value=new_value
        ))
    
    # Transform removed
    for transform_id in transform_ids_v1 - transform_ids_v2:
        version = transforms_v1[transform_id].version
        old_value = version if version else None
        events.append(ChangeEvent(
            change_type="TRANSFORM_REMOVED",
            element_id=transform_id,
            old_value=old_value,
            new_value=None
        ))
    
    # Check for impl changes (same ID, different impl_fingerprint.digest)
    # Version-only changes are ignored (version is informational)
    # Comparison rule: TRANSFORM_IMPL_CHANGED triggers iff digest changes;
    # non-digest fields (source, ref, algo) are informational and do NOT trigger events
    # Ensure order-independent: sort by transform_id for deterministic output
    for transform_id in sorted(transform_ids_v1 & transform_ids_v2):
        t1 = transforms_v1[transform_id]
        t2 = transforms_v2[transform_id]
        
        # Compare impl_fingerprint.digest ONLY (authoritative, not version, not source/ref)
        if t1.impl_fingerprint.digest != t2.impl_fingerprint.digest:
            details = {
                "old_source": t1.impl_fingerprint.source,
                "new_source": t2.impl_fingerprint.source,
                "old_ref": t1.impl_fingerprint.ref,
                "new_ref": t2.impl_fingerprint.ref,
            }
            if t1.version:
                details["old_version"] = t1.version
            if t2.version:
                details["new_version"] = t2.version
            events.append(ChangeEvent(
                change_type="TRANSFORM_IMPL_CHANGED",
                element_id=transform_id,
                old_value=t1.impl_fingerprint.digest,
                new_value=t2.impl_fingerprint.digest,
                details=details
            ))
    
    return events


def validate_transform_refs(spec: MappingSpec, registry: Optional[TransformRegistry] = None) -> List[str]:
    """Validate that all transform_ref values in spec exist in registry.
    
    This validation does NOT stop processing - it collects errors for reporting.
    The run will be marked as validation_failed but impact analysis continues.
    
    Args:
        spec: Mapping specification to validate
        registry: Transform registry (optional - if None, validation is skipped)
    
    Returns:
        List of error messages for missing transforms (empty if all valid)
    """
    if registry is None:
        return []  # No registry provided, skip validation
    
    errors: List[str] = []
    
    for derived in spec.derived:
        if not registry.has_transform(derived.transform_ref):
            errors.append(
                f"Derived variable '{derived.id}' ({derived.name}) references "
                f"missing transform '{derived.transform_ref}'. "
                f"Transform not found in registry."
            )
    
    return errors

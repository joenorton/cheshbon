"""Compute impacted derived variables from change events.

Impact definition (deterministic predicate):
A derived variable is impacted if ANY of these are true:
1. Any of its input references changed identity (source/derived removed or ID changed)
2. Its transform_ref changed
3. Its transform params changed (params_hash changed)
4. Its type changed
5. Its input list changed (add/remove/reorder)
6. The transform implementation changed (registry-level: impl_fingerprint.digest changed)
7. The transform was removed from registry (TRANSFORM_REMOVED)
8. Transitive closure: if X is impacted, any derived var depending on X is impacted

Everything else (name changes, notes, review status) is non-impacting metadata.
"""

from dataclasses import dataclass, field
from typing import Set, Dict, List, Optional
from .diff import ChangeEvent
from .graph import DependencyGraph
from .spec import MappingSpec
from .transform_registry import TransformRegistry


@dataclass
class ImpactResult:
    """Result of impact analysis."""
    impacted: Set[str]  # Set of derived variable IDs that are impacted
    unaffected: Set[str]  # Set of derived variable IDs that are unaffected
    impact_paths: Dict[str, List[str]]  # For each impacted var ID, the dependency path explaining why
    impact_reasons: Dict[str, str]  # For each impacted var ID, the reason code
    unresolved_references: Dict[str, Set[str]]  # For each impacted var ID, set of missing input IDs (if any)
    missing_bindings: Dict[str, Set[str]]  # For each impacted var ID, set of missing binding source IDs (if any)
    missing_transform_refs: Dict[str, Set[str]]  # For each impacted var ID, set of missing transform references (if any)
    ambiguous_bindings: Dict[str, Set[str]] = field(default_factory=dict)  # For each impacted var ID, set of ambiguous binding source IDs (if any)
    alternative_path_counts: Dict[str, int] = field(default_factory=dict)  # For each impacted var ID, count of alternative dependency paths (diamond warning)
    validation_failed: bool = False  # True if validation errors exist (missing transforms, etc.) - run is not executable
    validation_errors: List[str] = field(default_factory=list)  # List of validation error messages (if validation_failed is True)


def compute_impact(
    spec_v1: MappingSpec,
    spec_v2: MappingSpec,
    graph_v1: DependencyGraph,
    change_events: List[ChangeEvent],
    registry_v2: Optional[TransformRegistry] = None,
    compute_paths: bool = True
) -> ImpactResult:
    """
    Compute which derived outputs are impacted by the changes.
    
    Uses precise impact definition: structural changes only.
    
    Returns:
        ImpactResult with impacted set (IDs), unaffected set (IDs), and optional explanation paths.
    """
    impacted: Set[str] = set()
    impact_paths: Dict[str, List[str]] = {}
    impact_reasons: Dict[str, str] = {}
    unresolved_references: Dict[str, Set[str]] = {}
    missing_transform_refs: Dict[str, Set[str]] = {}

    # Get all derived variable IDs and constraint IDs
    all_derived_ids = spec_v1.get_derived_ids()
    all_constraint_ids = spec_v1.get_constraint_ids()
    
    # Build sets of available source/derived IDs in v2 for unresolved reference detection
    available_source_ids_v2 = spec_v2.get_source_ids()
    available_derived_ids_v2 = spec_v2.get_derived_ids()
    available_ids_v2 = available_source_ids_v2 | available_derived_ids_v2
    
    # Build map of transform_ref -> list of derived var IDs that use it (for registry-level events)
    transform_ref_to_derived: Dict[str, Set[str]] = {}
    for derived in spec_v1.derived:
        transform_ref = derived.transform_ref
        if transform_ref not in transform_ref_to_derived:
            transform_ref_to_derived[transform_ref] = set()
        transform_ref_to_derived[transform_ref].add(derived.id)
    
    reason_priority = {
        "MISSING_TRANSFORM_REF": 100,
        "DIRECT_CHANGE_MISSING_INPUT": 90,
        "MISSING_INPUT": 85,
        "DIRECT_CHANGE": 80,
        "TRANSFORM_REMOVED": 75,
        "TRANSFORM_IMPL_CHANGED": 70,
        "TRANSITIVE_DEPENDENCY": 10,
    }

    def _priority(reason: str) -> int:
        return reason_priority.get(reason, 0)

    def _set_reason(var_id: str, reason: str, path_from: Optional[str] = None) -> None:
        """Set impact reason using deterministic precedence; update path if reason wins."""
        current = impact_reasons.get(var_id)
        if current is not None and _priority(reason) <= _priority(current):
            return
        impact_reasons[var_id] = reason
        if not compute_paths:
            return
        if path_from is None or path_from == var_id:
            impact_paths[var_id] = [var_id]
            return
        path = graph_v1.get_dependency_path(path_from, var_id)
        if path:
            impact_paths[var_id] = path

    def _add_missing_ref(target: Dict[str, Set[str]], var_id: str, ref_id: str) -> None:
        if var_id not in target:
            target[var_id] = set()
        target[var_id].add(ref_id)

    # Process each change event
    for event in change_events:
        if event.change_type == "SOURCE_REMOVED":
            # All derived vars that depend on this source are impacted (MISSING_INPUT)
            source_id = event.element_id
            dependents = graph_v1.get_transitive_dependents(source_id)
            affected_derived = dependents & all_derived_ids
            impacted.update(affected_derived)
            
            for var_id in affected_derived:
                # Direct dependents get MISSING_INPUT, transitive get TRANSITIVE_DEPENDENCY
                if source_id in graph_v1.get_dependencies(var_id):
                    _set_reason(var_id, "MISSING_INPUT", path_from=source_id)
                    _add_missing_ref(unresolved_references, var_id, source_id)
                else:
                    _set_reason(var_id, "TRANSITIVE_DEPENDENCY", path_from=source_id)
        
        elif event.change_type == "SOURCE_RENAMED":
            # Name change doesn't impact (ID unchanged), but if inputs reference changed
            # that would be caught by DERIVED_INPUTS_CHANGED. For now, rename is non-impacting.
            # However, if a derived var's inputs reference the old name somehow, that's an input change.
            # Since we use IDs, a rename shouldn't affect anything unless inputs are also changed.
            pass
        
        elif event.change_type == "DERIVED_REMOVED":
            # The variable is gone, so anything that depended on it is impacted (MISSING_INPUT)
            derived_id = event.element_id
            if derived_id in all_derived_ids:
                dependents = graph_v1.get_transitive_dependents(derived_id)
                affected_derived = dependents & all_derived_ids
                impacted.update(affected_derived)
                
                for var_id in affected_derived:
                    # Direct dependents get MISSING_INPUT, transitive get TRANSITIVE_DEPENDENCY
                    if derived_id in graph_v1.get_dependencies(var_id):
                        _set_reason(var_id, "MISSING_INPUT", path_from=derived_id)
                        _add_missing_ref(unresolved_references, var_id, derived_id)
                    else:
                        _set_reason(var_id, "TRANSITIVE_DEPENDENCY", path_from=derived_id)
        
        elif event.change_type == "DERIVED_TRANSFORM_REF_CHANGED":
            # Transform reference changed - the derived variable itself is impacted (DIRECT_CHANGE)
            derived_id = event.element_id
            if derived_id in all_derived_ids:
                impacted.add(derived_id)
                _set_reason(derived_id, "DIRECT_CHANGE")
                
                # Also impact dependents (TRANSITIVE_DEPENDENCY)
                dependents = graph_v1.get_transitive_dependents(derived_id)
                affected_derived = dependents & all_derived_ids
                impacted.update(affected_derived)
                
                for dep_id in affected_derived:
                    _set_reason(dep_id, "TRANSITIVE_DEPENDENCY", path_from=derived_id)
        
        elif event.change_type == "DERIVED_TRANSFORM_PARAMS_CHANGED":
            # Transform params changed (same ref, different params_hash) - DIRECT_CHANGE
            derived_id = event.element_id
            if derived_id in all_derived_ids:
                impacted.add(derived_id)
                _set_reason(derived_id, "DIRECT_CHANGE")
                
                # Also impact dependents (TRANSITIVE_DEPENDENCY)
                dependents = graph_v1.get_transitive_dependents(derived_id)
                affected_derived = dependents & all_derived_ids
                impacted.update(affected_derived)
                
                for dep_id in affected_derived:
                    _set_reason(dep_id, "TRANSITIVE_DEPENDENCY", path_from=derived_id)
        
        elif event.change_type == "TRANSFORM_IMPL_CHANGED":
            # Registry-level: transform implementation changed (impl_fingerprint.digest changed)
            # Impacts all derived vars using that transform_ref
            transform_ref = event.element_id
            if transform_ref in transform_ref_to_derived:
                affected_derived = transform_ref_to_derived[transform_ref] & all_derived_ids
                impacted.update(affected_derived)
                
                for var_id in affected_derived:
                    _set_reason(var_id, "TRANSFORM_IMPL_CHANGED")
                    
                    # Also impact dependents (TRANSITIVE_DEPENDENCY)
                    dependents = graph_v1.get_transitive_dependents(var_id)
                    transitive_affected = dependents & all_derived_ids
                    impacted.update(transitive_affected)
                    
                    for dep_id in transitive_affected:
                        _set_reason(dep_id, "TRANSITIVE_DEPENDENCY", path_from=var_id)
        
        elif event.change_type == "TRANSFORM_REMOVED":
            # Transform removed from registry - impacts all derived vars referencing it
            # Matching by transform_ref ID (stable ID, not name)
            transform_ref = event.element_id
            if transform_ref in transform_ref_to_derived:
                affected_derived = transform_ref_to_derived[transform_ref] & all_derived_ids
                impacted.update(affected_derived)
                
                for var_id in affected_derived:
                    _set_reason(var_id, "TRANSFORM_REMOVED")
                    
                    # Also impact dependents (TRANSITIVE_DEPENDENCY)
                    dependents = graph_v1.get_transitive_dependents(var_id)
                    transitive_affected = dependents & all_derived_ids
                    impacted.update(transitive_affected)
                    
                    for dep_id in transitive_affected:
                        _set_reason(dep_id, "TRANSITIVE_DEPENDENCY", path_from=var_id)
        
        elif event.change_type == "DERIVED_TYPE_CHANGED":
            # Type change impacts the variable itself (DIRECT_CHANGE)
            derived_id = event.element_id
            if derived_id in all_derived_ids:
                impacted.add(derived_id)
                _set_reason(derived_id, "DIRECT_CHANGE")
                
                # Also impact dependents (TRANSITIVE_DEPENDENCY)
                dependents = graph_v1.get_transitive_dependents(derived_id)
                affected_derived = dependents & all_derived_ids
                impacted.update(affected_derived)
                
                for dep_id in affected_derived:
                    _set_reason(dep_id, "TRANSITIVE_DEPENDENCY", path_from=derived_id)
        
        elif event.change_type == "DERIVED_INPUTS_CHANGED":
            # Inputs changed - the variable itself is impacted (DIRECT_CHANGE)
            derived_id = event.element_id
            if derived_id in all_derived_ids:
                impacted.add(derived_id)
                _set_reason(derived_id, "DIRECT_CHANGE")
                
                # Check for unresolved references in v2
                d1 = spec_v1.get_derived_by_id(derived_id)
                d2 = spec_v2.get_derived_by_id(derived_id)
                if d1 and d2:
                    # Check if any inputs in v2 reference missing IDs
                    missing_inputs = {inp_id for inp_id in d2.inputs if inp_id not in available_ids_v2}
                    if missing_inputs:
                        for missing_id in missing_inputs:
                            _add_missing_ref(unresolved_references, derived_id, missing_id)
                        _set_reason(derived_id, "DIRECT_CHANGE_MISSING_INPUT")
                
                # Also impact dependents (TRANSITIVE_DEPENDENCY)
                dependents = graph_v1.get_transitive_dependents(derived_id)
                affected_derived = dependents & all_derived_ids
                impacted.update(affected_derived)
                
                for dep_id in affected_derived:
                    _set_reason(dep_id, "TRANSITIVE_DEPENDENCY", path_from=derived_id)
        
        elif event.change_type == "CONSTRAINT_REMOVED":
            # Constraint removed - anything that depended on it is impacted (MISSING_INPUT)
            constraint_id = event.element_id
            if constraint_id in all_constraint_ids:
                dependents = graph_v1.get_transitive_dependents(constraint_id)
                # Constraints can be depended on by derived vars or other constraints
                affected_derived = dependents & all_derived_ids
                affected_constraints = dependents & all_constraint_ids
                impacted.update(affected_derived)
                
                for var_id in affected_derived:
                    # Direct dependents get MISSING_INPUT, transitive get TRANSITIVE_DEPENDENCY
                    if constraint_id in graph_v1.get_dependencies(var_id):
                        _set_reason(var_id, "MISSING_INPUT", path_from=constraint_id)
                        _add_missing_ref(unresolved_references, var_id, constraint_id)
                    else:
                        _set_reason(var_id, "TRANSITIVE_DEPENDENCY", path_from=constraint_id)
        
        elif event.change_type == "CONSTRAINT_INPUTS_CHANGED":
            # Constraint inputs changed - the constraint itself is impacted (DIRECT_CHANGE)
            # This impacts anything that depends on the constraint
            constraint_id = event.element_id
            if constraint_id in all_constraint_ids:
                # Mark constraint as changed (though constraints aren't "derived outputs" in the traditional sense)
                # The impact is on anything that depends on this constraint
                dependents = graph_v1.get_transitive_dependents(constraint_id)
                affected_derived = dependents & all_derived_ids
                impacted.update(affected_derived)
                
                for var_id in affected_derived:
                    _set_reason(var_id, "TRANSITIVE_DEPENDENCY", path_from=constraint_id)
        
        elif event.change_type == "CONSTRAINT_EXPRESSION_CHANGED":
            # Constraint expression changed - impacts anything that depends on the constraint
            constraint_id = event.element_id
            if constraint_id in all_constraint_ids:
                dependents = graph_v1.get_transitive_dependents(constraint_id)
                affected_derived = dependents & all_derived_ids
                impacted.update(affected_derived)
                
                for var_id in affected_derived:
                    _set_reason(var_id, "TRANSITIVE_DEPENDENCY", path_from=constraint_id)
        
        # Note: SOURCE_ADDED, DERIVED_ADDED, CONSTRAINT_ADDED, SOURCE_RENAMED, DERIVED_RENAMED, CONSTRAINT_RENAMED
        # are non-impacting for existing derived variables (they don't invalidate existing outputs)
    
    # Unaffected are all derived vars not in impacted
    unaffected = all_derived_ids - impacted
    
    # Check for missing transform refs in v2 (if registry provided)
    # Impact reason precedence: MISSING_TRANSFORM_REF takes precedence over other events
    # (node is non-executable regardless of other changes)
    validation_errors: List[str] = []
    validation_failed = False
    
    if registry_v2 is not None:
        for derived in spec_v2.derived:
            if derived.id in all_derived_ids:  # Only check existing derived vars
                if not registry_v2.has_transform(derived.transform_ref):
                    validation_errors.append(
                        f"Derived variable '{derived.id}' ({derived.name}) references "
                        f"missing transform '{derived.transform_ref}'. "
                        f"Transform not found in registry."
                    )
                    validation_failed = True
                    # Always mark as impacted with MISSING_TRANSFORM_REF
                    # (even if already impacted by other events)
                    impacted.add(derived.id)
                    _set_reason(derived.id, "MISSING_TRANSFORM_REF")
                    _add_missing_ref(missing_transform_refs, derived.id, derived.transform_ref)
    
    # Compute alternative path counts for impacted variables
    # For each impacted variable with a path, count alternative paths from change source to variable
    alternative_path_counts: Dict[str, int] = {}
    if compute_paths:
        for var_id, path in impact_paths.items():
            if len(path) > 1:
                # Path goes from change source (first node) to impacted variable (last node)
                change_source = path[0]
                impacted_var = path[-1]
                alt_count = graph_v1.count_alternative_paths(change_source, impacted_var)
                if alt_count > 0:
                    alternative_path_counts[var_id] = alt_count
    
    return ImpactResult(
        impacted=impacted,
        unaffected=unaffected,
        impact_paths=impact_paths,
        impact_reasons=impact_reasons,
        unresolved_references=unresolved_references,
        missing_bindings={},  # Populated separately by binding layer
        ambiguous_bindings={},  # Populated separately by binding layer
        missing_transform_refs=missing_transform_refs,
        alternative_path_counts=alternative_path_counts,
        validation_failed=validation_failed,
        validation_errors=validation_errors
    )

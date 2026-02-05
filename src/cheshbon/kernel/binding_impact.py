"""Binding-aware impact analysis: checks for missing and ambiguous bindings."""

from typing import Set, Dict
from .spec import MappingSpec
from .graph import DependencyGraph
from .impact import ImpactResult
from .bindings import Bindings, check_missing_bindings, check_ambiguous_bindings


def compute_binding_impact(
    spec: MappingSpec,
    bindings: Bindings,
    graph: DependencyGraph,
    base_impact: ImpactResult,
    compute_paths: bool = True
) -> ImpactResult:
    """
    Compute additional impact from missing and ambiguous bindings.
    
    Missing bindings: A derived variable requires a source ID that's not bound in the current extract.
    Ambiguous bindings: Multiple raw columns map to the same source ID (cannot determine which to use).
    
    Both are terminal failures that must be explicitly resolved. They propagate transitively.
    
    Returns:
        Updated ImpactResult with missing_bindings and ambiguous_bindings populated and impact reasons updated.
    """
    missing_bindings_map = check_missing_bindings(spec, bindings)
    ambiguous_bindings_map = check_ambiguous_bindings(bindings)
    
    if not missing_bindings_map and not ambiguous_bindings_map:
        # No binding issues, return base impact as-is
        return base_impact
    
    # Update impact for missing and ambiguous bindings
    impacted = set(base_impact.impacted)
    impact_reasons = dict(base_impact.impact_reasons)
    missing_bindings = dict(base_impact.missing_bindings)
    ambiguous_bindings = dict(base_impact.ambiguous_bindings)
    
    # Process missing bindings
    for derived_id, missing_source_ids in missing_bindings_map.items():
        # Mark as impacted due to missing binding
        impacted.add(derived_id)
        impact_reasons[derived_id] = "MISSING_BINDING"
        missing_bindings[derived_id] = missing_source_ids
        
        # Also propagate transitively
        dependents = graph.get_transitive_dependents(derived_id)
        all_derived_ids = spec.get_derived_ids()
        affected_derived = dependents & all_derived_ids
        
        for dep_id in affected_derived:
            if dep_id not in impact_reasons or impact_reasons[dep_id] not in ["MISSING_BINDING", "AMBIGUOUS_BINDING"]:
                # Only mark as transitive if not already marked for binding issue
                if dep_id not in impact_reasons:
                    impact_reasons[dep_id] = "TRANSITIVE_DEPENDENCY"
                impacted.add(dep_id)
    
    # Process ambiguous bindings
    # Ambiguous bindings are TERMINAL failures - cannot proceed with execution
    # Find which derived variables depend on ambiguous source IDs
    ambiguous_source_ids = set(ambiguous_bindings_map.keys())
    has_ambiguous = False
    for derived in spec.derived:
        required_source_ids = {inp for inp in derived.inputs if inp.startswith("s:")}
        ambiguous_sources = required_source_ids & ambiguous_source_ids
        if ambiguous_sources:
            has_ambiguous = True
            derived_id = derived.id
            # Mark as impacted due to ambiguous binding
            impacted.add(derived_id)
            # AMBIGUOUS_BINDING takes precedence over MISSING_BINDING
            if impact_reasons.get(derived_id) != "AMBIGUOUS_BINDING":
                impact_reasons[derived_id] = "AMBIGUOUS_BINDING"
            ambiguous_bindings[derived_id] = ambiguous_sources
            
            # Also propagate transitively
            dependents = graph.get_transitive_dependents(derived_id)
            all_derived_ids = spec.get_derived_ids()
            affected_derived = dependents & all_derived_ids
            
            for dep_id in affected_derived:
                if dep_id not in impact_reasons or impact_reasons[dep_id] not in ["MISSING_BINDING", "AMBIGUOUS_BINDING"]:
                    # Only mark as transitive if not already marked for binding issue
                    if dep_id not in impact_reasons:
                        impact_reasons[dep_id] = "TRANSITIVE_DEPENDENCY"
                    impacted.add(dep_id)
    
    # Ambiguous bindings are terminal - set validation_failed
    validation_failed = base_impact.validation_failed or has_ambiguous
    validation_errors = list(base_impact.validation_errors)
    if has_ambiguous:
        for source_id, raw_columns in ambiguous_bindings_map.items():
            validation_errors.append(
                f"Ambiguous binding for source ID '{source_id}': multiple raw columns map to same source "
                f"({', '.join(sorted(raw_columns))}). Cannot determine which to use. Terminal failure."
            )
    
    # Update paths for newly impacted variables
    impact_paths = dict(base_impact.impact_paths)
    if compute_paths:
        for derived_id in list(missing_bindings_map.keys()) + list(ambiguous_bindings.keys()):
            if derived_id not in impact_paths:
                impact_paths[derived_id] = [derived_id]
            
            # Update paths for transitive dependents to show full chain
            dependents = graph.get_transitive_dependents(derived_id)
            all_derived_ids = spec.get_derived_ids()
            affected_derived = dependents & all_derived_ids
            
            for dep_id in affected_derived:
                if dep_id not in impact_paths:
                    path = graph.get_dependency_path(derived_id, dep_id)
                    if path:
                        impact_paths[dep_id] = path
    
    unaffected = base_impact.unaffected - impacted
    
    # Update alternative path counts for newly impacted variables
    alternative_path_counts = dict(base_impact.alternative_path_counts)
    if compute_paths:
        for var_id, path in impact_paths.items():
            if var_id not in alternative_path_counts and len(path) > 1:
                # Path goes from change source (first node) to impacted variable (last node)
                change_source = path[0]
                impacted_var = path[-1]
                alt_count = graph.count_alternative_paths(change_source, impacted_var)
                if alt_count > 0:
                    alternative_path_counts[var_id] = alt_count
    
    return ImpactResult(
        impacted=impacted,
        unaffected=unaffected,
        impact_paths=impact_paths,
        impact_reasons=impact_reasons,
        unresolved_references=base_impact.unresolved_references,
        missing_bindings=missing_bindings,
        ambiguous_bindings=ambiguous_bindings,
        missing_transform_refs=base_impact.missing_transform_refs,
        alternative_path_counts=alternative_path_counts,
        validation_failed=validation_failed,
        validation_errors=validation_errors
    )

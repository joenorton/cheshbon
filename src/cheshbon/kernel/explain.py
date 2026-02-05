"""Structured explanation primitives (no rendering)."""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .diff import ChangeEvent
from .impact import ImpactResult
from .spec import MappingSpec


@dataclass(frozen=True)
class ChangeExplanation:
    change_type: str
    element_id: str
    element_name: Optional[str]
    old_value: Optional[str]
    new_value: Optional[str]
    details: Optional[Dict]


@dataclass(frozen=True)
class MissingReference:
    ref_id: str
    ref_name: Optional[str]


@dataclass(frozen=True)
class ImpactExplanation:
    var_id: str
    var_name: str
    path_ids: Tuple[str, ...]
    path_names: Tuple[str, ...]
    reason: str
    alternative_path_count: int
    missing_inputs: Tuple[MissingReference, ...]
    missing_bindings: Tuple[MissingReference, ...]
    ambiguous_bindings: Tuple[MissingReference, ...]
    missing_transform_refs: Tuple[str, ...]


@dataclass(frozen=True)
class ImpactExplanationResult:
    impacted: Tuple[ImpactExplanation, ...]
    unaffected: Tuple[Tuple[str, str], ...]  # (id, name)


def _resolve_name(spec: MappingSpec, node_id: str) -> Optional[str]:
    if node_id.startswith("s:"):
        source = spec.get_source_by_id(node_id)
        return source.name if source else None
    if node_id.startswith("d:"):
        derived = spec.get_derived_by_id(node_id)
        return derived.name if derived else None
    if node_id.startswith("c:"):
        constraint = spec.get_constraint_by_id(node_id)
        return constraint.name if constraint else None
    return None


def explain_changes_structured(
    change_events: List[ChangeEvent],
    spec_v1: MappingSpec,
    spec_v2: MappingSpec
) -> Tuple[ChangeExplanation, ...]:
    """Return structured explanations of changes (no rendering)."""
    explanations: List[ChangeExplanation] = []
    for event in change_events:
        element_name: Optional[str] = None
        if event.element_id.startswith("s:"):
            source = spec_v1.get_source_by_id(event.element_id) or spec_v2.get_source_by_id(event.element_id)
            element_name = source.name if source else None
        elif event.element_id.startswith("d:"):
            derived = spec_v1.get_derived_by_id(event.element_id) or spec_v2.get_derived_by_id(event.element_id)
            element_name = derived.name if derived else None
        elif event.element_id.startswith("c:"):
            constraint = spec_v1.get_constraint_by_id(event.element_id) or spec_v2.get_constraint_by_id(event.element_id)
            element_name = constraint.name if constraint else None

        explanations.append(ChangeExplanation(
            change_type=event.change_type,
            element_id=event.element_id,
            element_name=element_name,
            old_value=event.old_value,
            new_value=event.new_value,
            details=event.details
        ))
    return tuple(explanations)


def explain_impact_structured(
    impact_result: ImpactResult,
    spec_v1: MappingSpec
) -> ImpactExplanationResult:
    """Return structured impact explanations (no rendering)."""
    impacted_items: List[ImpactExplanation] = []

    for var_id in sorted(impact_result.impacted):
        derived = spec_v1.get_derived_by_id(var_id)
        var_name = derived.name if derived else var_id
        path_ids = tuple(impact_result.impact_paths.get(var_id, [var_id]))
        path_names = tuple(_resolve_name(spec_v1, node_id) or node_id for node_id in path_ids)
        reason = impact_result.impact_reasons.get(var_id, "UNKNOWN")
        alternative_path_count = impact_result.alternative_path_counts.get(var_id, 0)

        missing_inputs = tuple(
            MissingReference(ref_id=missing_id, ref_name=_resolve_name(spec_v1, missing_id))
            for missing_id in sorted(impact_result.unresolved_references.get(var_id, set()))
        )
        missing_bindings = tuple(
            MissingReference(ref_id=missing_id, ref_name=_resolve_name(spec_v1, missing_id))
            for missing_id in sorted(impact_result.missing_bindings.get(var_id, set()))
        )
        ambiguous_bindings = tuple(
            MissingReference(ref_id=missing_id, ref_name=_resolve_name(spec_v1, missing_id))
            for missing_id in sorted(impact_result.ambiguous_bindings.get(var_id, set()))
        )
        missing_transform_refs = tuple(sorted(impact_result.missing_transform_refs.get(var_id, set())))

        impacted_items.append(ImpactExplanation(
            var_id=var_id,
            var_name=var_name,
            path_ids=path_ids,
            path_names=path_names,
            reason=reason,
            alternative_path_count=alternative_path_count,
            missing_inputs=missing_inputs,
            missing_bindings=missing_bindings,
            ambiguous_bindings=ambiguous_bindings,
            missing_transform_refs=missing_transform_refs
        ))

    unaffected_items = tuple(
        (var_id, (spec_v1.get_derived_by_id(var_id).name if spec_v1.get_derived_by_id(var_id) else var_id))
        for var_id in sorted(impact_result.unaffected)
    )

    return ImpactExplanationResult(
        impacted=tuple(impacted_items),
        unaffected=unaffected_items
    )

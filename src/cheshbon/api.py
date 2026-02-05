"""Public API for cheshbon kernel package.

High-level functions that return complete, structured results.
Studio should use these functions instead of importing from _internal.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Union, Literal, Tuple

from pydantic import BaseModel, Field

# Internal imports (not exposed to users)
from cheshbon.kernel.spec import MappingSpec
from cheshbon.codes import ValidationCode
from cheshbon.kernel.transform_registry import TransformRegistry
from cheshbon._internal.io.registry import load_registry_from_path
from cheshbon.kernel.diff import diff_specs, diff_registries, validate_transform_refs, ChangeEvent
from cheshbon.kernel.graph import DependencyGraph
from cheshbon.kernel.impact import compute_impact, ImpactResult
from cheshbon.kernel.bindings import Bindings
from cheshbon.kernel.binding_impact import compute_binding_impact
from cheshbon.kernel.graph_diff import GraphDiff, Impact, diff_graph, impact_from_diff
from cheshbon.kernel.graph_v1 import GraphV1



def _normalize_path(path: Union[str, os.PathLike, Path]) -> Path:
    """Normalize path input to Path object."""
    return Path(path) if not isinstance(path, Path) else path


class DiffResult(BaseModel):
    """Stable result model for diff analysis."""
    validation_failed: bool
    validation_errors: list[str]
    change_summary: dict[str, int]  # Counts by change type
    impacted_ids: list[str]  # Sorted list of impacted variable IDs
    unaffected_ids: list[str] = Field(default_factory=list)  # Sorted list of unaffected variable IDs
    reasons: dict[str, str]  # id -> reason code (DIRECT_CHANGE, TRANSITIVE_DEPENDENCY, etc.)
    paths: dict[str, list[str]]  # id -> dependency chain (for explainability)
    missing_inputs: dict[str, list[str]] = Field(default_factory=dict)  # id -> missing input IDs
    missing_bindings: dict[str, list[str]] = Field(default_factory=dict)  # id -> missing binding source IDs
    ambiguous_bindings: dict[str, list[str]] = Field(default_factory=dict)  # id -> ambiguous binding source IDs
    missing_transform_refs: dict[str, list[str]] = Field(default_factory=dict)  # id -> missing transform refs
    alternative_path_counts: dict[str, int] = Field(default_factory=dict)  # id -> count of alternative paths
    events: list[dict] = Field(default_factory=list)  # Optional: change events if a client wants to render a change log
    binding_issues: dict[str, list[str]] = Field(default_factory=dict)  # Optional: id -> list of missing/ambiguous binding source IDs (only populated when bindings provided)


class ValidationIssue(BaseModel):
    """A single validation issue (error or warning)."""
    code: str  # e.g., "MISSING_INPUT", "DUPLICATE_ID", "CYCLE_DETECTED", "MISSING_TRANSFORM_REF", "MISSING_BINDING", "AMBIGUOUS_BINDING", "INVALID_RAW_COLUMN"
    message: str
    element_id: Optional[str] = None  # ID of element with issue (source_id for bindings, derived_id for missing inputs, etc.)
    missing_id: Optional[str] = None  # For MISSING_INPUT errors
    cycle_path: Optional[List[str]] = None  # For CYCLE_DETECTED errors
    raw_column: Optional[str] = None  # For INVALID_RAW_COLUMN warnings


class ValidationResult(BaseModel):
    """Result of validation/preflight check."""
    ok: bool  # True if no errors (warnings don't block)
    errors: List[ValidationIssue]  # Blocking issues
    warnings: List[ValidationIssue]  # Non-blocking issues


def _load_spec_from_path(path: Path) -> MappingSpec:
    """Load a mapping spec from JSON file."""
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return MappingSpec(**data)


def _load_spec_from_dict(data: Dict) -> MappingSpec:
    """Load a mapping spec from dict."""
    return MappingSpec(**data)


def _load_bindings_from_path(path: Path) -> Bindings:
    """Load bindings from JSON file."""
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return Bindings(table=data["table"], bindings=data["bindings"])


def _load_bindings_from_dict(data: Dict) -> Bindings:
    """Load bindings from dict."""
    return Bindings(table=data["table"], bindings=data["bindings"])


def _load_raw_schema_from_path(path: Path) -> 'RawSchema':
    """Load raw schema from JSON file."""
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    from cheshbon.kernel.bindings import RawSchema, RawColumn
    table = data["table"]
    columns = [RawColumn(name=col["name"], type=col["type"]) for col in data["columns"]]
    return RawSchema(table=table, columns=columns)


def _load_raw_schema_from_dict(data: Dict) -> 'RawSchema':
    """Load raw schema from dict."""
    from cheshbon.kernel.bindings import RawSchema, RawColumn
    table = data["table"]
    columns = [RawColumn(name=col["name"], type=col["type"]) for col in data["columns"]]
    return RawSchema(table=table, columns=columns)


def _change_event_to_dict(event: ChangeEvent) -> Dict:
    """Convert ChangeEvent dataclass to dict."""
    return {
        "change_type": event.change_type,
        "element_id": event.element_id,
        "old_value": event.old_value,
        "new_value": event.new_value,
        "details": event.details,
    }


def _build_diff_result(
    change_events: List[ChangeEvent],
    impact_result: ImpactResult,
    bindings_v2: Optional[Bindings] = None,
    validation_errors: Optional[List[str]] = None,
    detail_level: Literal["full", "core"] = "full"
) -> DiffResult:
    """Build DiffResult from change events and impact result."""
    # Count events by type
    change_summary: Dict[str, int] = {}
    for event in change_events:
        change_summary[event.change_type] = change_summary.get(event.change_type, 0) + 1
    
    # Convert impacted/unaffected sets to sorted lists
    impacted_ids = sorted(list(impact_result.impacted))
    unaffected_ids = sorted(list(impact_result.unaffected))
    
    # Convert paths (already dict[str, list[str]])
    paths = dict(impact_result.impact_paths) if detail_level == "full" else {}
    
    # Convert reasons (already dict[str, str])
    reasons = dict(impact_result.impact_reasons)
    
    # Convert events to dicts
    events = [_change_event_to_dict(event) for event in change_events]

    # Convert missing dependency maps to JSON-friendly dict[str, list[str]]
    missing_inputs = {
        var_id: sorted(list(missing_ids))
        for var_id, missing_ids in impact_result.unresolved_references.items()
    }
    missing_bindings = {
        var_id: sorted(list(missing_ids))
        for var_id, missing_ids in impact_result.missing_bindings.items()
    }
    ambiguous_bindings = {
        var_id: sorted(list(missing_ids))
        for var_id, missing_ids in impact_result.ambiguous_bindings.items()
    }
    missing_transform_refs = {
        var_id: sorted(list(missing_ids))
        for var_id, missing_ids in impact_result.missing_transform_refs.items()
    }

    # Alternative path counts (only populated when paths are computed)
    alternative_path_counts = dict(impact_result.alternative_path_counts) if detail_level == "full" else {}

    # Extract binding issues if bindings were provided
    binding_issues: Dict[str, List[str]] = {}
    if bindings_v2 is not None:
        # Combine missing and ambiguous bindings into a single dict
        # missing_bindings is Dict[str, Set[str]] - var_id -> set of missing source IDs
        for var_id, missing_sources in impact_result.missing_bindings.items():
            if var_id not in binding_issues:
                binding_issues[var_id] = []
            binding_issues[var_id].extend(sorted(missing_sources))
        # ambiguous_bindings is Dict[str, Set[str]] - var_id -> set of ambiguous source IDs
        for var_id, ambiguous_sources in impact_result.ambiguous_bindings.items():
            if var_id not in binding_issues:
                binding_issues[var_id] = []
            binding_issues[var_id].extend(sorted(ambiguous_sources))
        # Remove duplicates and sort
        for var_id in binding_issues:
            binding_issues[var_id] = sorted(list(set(binding_issues[var_id])))
    
    # Combine validation errors: impact_result.validation_errors + validation_errors (dedup + stable sort)
    combined_validation_errors = list(impact_result.validation_errors)
    if validation_errors:
        # Add new errors, deduplicate, and sort
        combined_validation_errors.extend(validation_errors)
        combined_validation_errors = sorted(list(set(combined_validation_errors)))
    
    # Combine validation_failed flag
    validation_failed = impact_result.validation_failed or bool(validation_errors)
    
    return DiffResult(
        validation_failed=validation_failed,
        validation_errors=combined_validation_errors,
        change_summary=change_summary,
        impacted_ids=impacted_ids,
        unaffected_ids=unaffected_ids,
        reasons=reasons,
        paths=paths,
        missing_inputs=missing_inputs,
        missing_bindings=missing_bindings,
        ambiguous_bindings=ambiguous_bindings,
        missing_transform_refs=missing_transform_refs,
        alternative_path_counts=alternative_path_counts,
        events=events,
        binding_issues=binding_issues,
    )


def _diff_internal(
    from_spec: Union[str, os.PathLike, Path, Dict],
    to_spec: Union[str, os.PathLike, Path, Dict],
    from_registry: Optional[Union[str, os.PathLike, Path, Dict]] = None,
    to_registry: Optional[Union[str, os.PathLike, Path, Dict]] = None,
    to_bindings: Optional[Union[str, os.PathLike, Path, Dict]] = None,
    detail_level: Literal["full", "core"] = "full"
) -> Tuple[MappingSpec, MappingSpec, DependencyGraph, DependencyGraph, List[ChangeEvent], ImpactResult, DiffResult, Optional[Bindings], Optional[TransformRegistry], Optional[TransformRegistry]]:
    """Internal diff pipeline that returns intermediate artifacts for report generation."""
    if detail_level not in ("full", "core"):
        raise ValueError("detail_level must be 'full' or 'core'")

    # Load specs (normalize paths first)
    if isinstance(from_spec, dict):
        spec_v1 = _load_spec_from_dict(from_spec)
    else:
        spec_v1 = _load_spec_from_path(_normalize_path(from_spec))

    if isinstance(to_spec, dict):
        spec_v2 = _load_spec_from_dict(to_spec)
    else:
        spec_v2 = _load_spec_from_path(_normalize_path(to_spec))

    # Load registries if provided (normalize paths first)
    # Enforce: either both provided (diff registries) or neither
    if (from_registry is not None) != (to_registry is not None):
        if from_registry is not None:
            raise ValueError(
                "Both from_registry and to_registry must be provided together, or neither. "
                "Got only from_registry."
            )
        else:
            raise ValueError(
                "Both from_registry and to_registry must be provided together, or neither. "
                "Got only to_registry."
            )

    registry_v1: Optional[TransformRegistry] = None
    registry_v2: Optional[TransformRegistry] = None

    if from_registry is not None:
        if isinstance(from_registry, dict):
            registry_v1 = TransformRegistry(**from_registry)
        else:
            registry_v1 = load_registry_from_path(_normalize_path(from_registry))

    if to_registry is not None:
        if isinstance(to_registry, dict):
            registry_v2 = TransformRegistry(**to_registry)
        else:
            registry_v2 = load_registry_from_path(_normalize_path(to_registry))

    # Load bindings if provided (normalize path first)
    # Bindings are evaluated against the 'to' spec
    bindings_v2: Optional[Bindings] = None
    if to_bindings is not None:
        if isinstance(to_bindings, dict):
            bindings_v2 = _load_bindings_from_dict(to_bindings)
        else:
            bindings_v2 = _load_bindings_from_path(_normalize_path(to_bindings))

    # Build dependency graphs
    graph_v1 = DependencyGraph(spec_v1)
    graph_v2 = DependencyGraph(spec_v2)

    # Validate transform refs (collects errors, doesn't stop)
    validation_errors: List[str] = []
    if registry_v2:
        validation_errors = validate_transform_refs(spec_v2, registry_v2)
        if registry_v1:
            validation_errors.extend(validate_transform_refs(spec_v1, registry_v1))

    # Diff specs
    change_events = diff_specs(spec_v1, spec_v2)

    # Diff registries if both provided
    if registry_v1 is not None and registry_v2 is not None:
        registry_events = diff_registries(registry_v1, registry_v2)
        registry_events_sorted = sorted(registry_events, key=lambda e: e.element_id)
        change_events = registry_events_sorted + change_events

    # Deterministic event ordering (group by element_id, then change priority)
    change_type_priority = {
        "SOURCE_REMOVED": 10,
        "SOURCE_ADDED": 20,
        "SOURCE_RENAMED": 30,
        "DERIVED_REMOVED": 10,
        "DERIVED_ADDED": 20,
        "DERIVED_RENAMED": 30,
        "DERIVED_TRANSFORM_REF_CHANGED": 40,
        "DERIVED_TRANSFORM_PARAMS_CHANGED": 50,
        "DERIVED_TYPE_CHANGED": 60,
        "DERIVED_INPUTS_CHANGED": 70,
        "CONSTRAINT_REMOVED": 10,
        "CONSTRAINT_ADDED": 20,
        "CONSTRAINT_RENAMED": 30,
        "CONSTRAINT_INPUTS_CHANGED": 40,
        "CONSTRAINT_EXPRESSION_CHANGED": 50,
        "TRANSFORM_REMOVED": 10,
        "TRANSFORM_ADDED": 20,
        "TRANSFORM_IMPL_CHANGED": 30,
    }

    change_events = sorted(
        change_events,
        key=lambda e: (
            e.element_id,
            change_type_priority.get(e.change_type, 999),
            e.change_type,
            e.old_value or "",
            e.new_value or ""
        )
    )

    # Compute base impact
    impact_result = compute_impact(
        spec_v1=spec_v1,
        spec_v2=spec_v2,
        graph_v1=graph_v1,
        change_events=change_events,
        registry_v2=registry_v2,
        compute_paths=(detail_level == "full")
    )

    # Compute binding-aware impact if bindings provided
    if bindings_v2 is not None:
        impact_result = compute_binding_impact(
            spec=spec_v2,
            bindings=bindings_v2,
            graph=graph_v2,
            base_impact=impact_result,
            compute_paths=(detail_level == "full")
        )

    diff_result = _build_diff_result(
        change_events,
        impact_result,
        bindings_v2,
        validation_errors,
        detail_level=detail_level
    )

    return (
        spec_v1,
        spec_v2,
        graph_v1,
        graph_v2,
        change_events,
        impact_result,
        diff_result,
        bindings_v2,
        registry_v1,
        registry_v2,
    )


def diff(
    from_spec: Union[str, os.PathLike, Path, Dict],
    to_spec: Union[str, os.PathLike, Path, Dict],
    from_registry: Optional[Union[str, os.PathLike, Path, Dict]] = None,
    to_registry: Optional[Union[str, os.PathLike, Path, Dict]] = None,
    to_bindings: Optional[Union[str, os.PathLike, Path, Dict]] = None,
    detail_level: Literal["full", "core"] = "full"
) -> DiffResult:
    """
    High-level diff analysis between two specs.
    """
    _, _, _, _, _, _, diff_result, _, _, _ = _diff_internal(
        from_spec=from_spec,
        to_spec=to_spec,
        from_registry=from_registry,
        to_registry=to_registry,
        to_bindings=to_bindings,
        detail_level=detail_level
    )
    return diff_result


def diff_all_details(
    from_spec: Union[str, os.PathLike, Path, Dict],
    to_spec: Union[str, os.PathLike, Path, Dict],
    from_registry: Optional[Union[str, os.PathLike, Path, Dict]] = None,
    to_registry: Optional[Union[str, os.PathLike, Path, Dict]] = None,
    to_bindings: Optional[Union[str, os.PathLike, Path, Dict]] = None,
    raw_schema: Optional[Union[str, os.PathLike, Path, Dict]] = None,
    caps: Optional[Dict[str, int]] = None
) -> Dict:
    """Generate an all-details report dict (machine-first JSON)."""
    from cheshbon.report_all_details import build_all_details_report

    spec_v1, spec_v2, graph_v1, graph_v2, _, impact_result, diff_result, bindings_v2, registry_v1, registry_v2 = _diff_internal(
        from_spec=from_spec,
        to_spec=to_spec,
        from_registry=from_registry,
        to_registry=to_registry,
        to_bindings=to_bindings,
        detail_level="full"
    )

    return build_all_details_report(
        diff_result=diff_result,
        impact_result=impact_result,
        spec_v1=spec_v1,
        spec_v2=spec_v2,
        graph_v1=graph_v1,
        graph_v2=graph_v2,
        registry_v1=registry_v1,
        registry_v2=registry_v2,
        bindings_v2=bindings_v2,
        raw_schema=raw_schema,
        caps=caps
    )


def validate(
    spec: Union[str, os.PathLike, Path, Dict],
    registry: Optional[Union[str, os.PathLike, Path, Dict]] = None,
    bindings: Optional[Union[str, os.PathLike, Path, Dict]] = None,
    raw_schema: Optional[Union[str, os.PathLike, Path, Dict]] = None
) -> ValidationResult:
    """
    Pure validation/preflight function for a single spec.
    
    Performs the same validations that diff/impact rely on.
    Does NOT mutate or write anything.
    
    Args:
        spec: Spec to validate (Path to JSON or dict)
        registry: Optional transform registry (for transform_ref validation)
        bindings: Optional bindings (for binding presence/ambiguity checks)
        raw_schema: Optional raw schema (required if bindings provided for column validation)
    
    Returns:
        ValidationResult with errors and warnings.
    
    This is READ-ONLY - no side effects, no file writes, no mutations.
    """
    errors: List[ValidationIssue] = []
    warnings: List[ValidationIssue] = []
    
    # 1. Structure Validation (ERROR)
    try:
        if isinstance(spec, dict):
            spec_obj = _load_spec_from_dict(spec)
        else:
            spec_obj = _load_spec_from_path(_normalize_path(spec))
    except Exception as e:
        errors.append(ValidationIssue(
            code=ValidationCode.INVALID_STRUCTURE.value,
            message=f"Failed to parse spec: {str(e)}",
            element_id=None
        ))
        return ValidationResult(ok=False, errors=errors, warnings=warnings)

    # 1b. Params size advisory (WARNING)
    from cheshbon.kernel.hash_utils import canonicalize_json
    for derived in spec_obj.derived:
        if derived.params is None:
            continue
        canonical_str = canonicalize_json(derived.params)
        if len(canonical_str) > 10000:
            warnings.append(ValidationIssue(
                code=ValidationCode.PARAMS_LARGE.value,
                message=(
                    f"Params for derived variable '{derived.id}' are large "
                    f"({len(canonical_str)} bytes). Params should be small and schema-governed."
                ),
                element_id=derived.id
            ))
    
    # 2. Duplicate ID Detection (ERROR)
    # Check for duplicate IDs across sources, derived, and constraints
    all_ids = []
    all_ids.extend([s.id for s in spec_obj.sources])
    all_ids.extend([d.id for d in spec_obj.derived])
    if spec_obj.constraints:
        all_ids.extend([c.id for c in spec_obj.constraints])
    
    seen_ids = set()
    duplicate_ids = set()
    for id_val in all_ids:
        if id_val in seen_ids:
            duplicate_ids.add(id_val)
        seen_ids.add(id_val)
    
    if duplicate_ids:
        for dup_id in sorted(duplicate_ids):
            errors.append(ValidationIssue(
                code=ValidationCode.DUPLICATE_ID.value,
                message=f"Duplicate ID '{dup_id}' found in spec",
                element_id=dup_id
            ))
    
    # 3. Missing Input References & Cycles (ERROR)
    # DependencyGraph construction validates both missing refs and cycles
    try:
        from cheshbon.kernel.graph import DependencyGraph, MissingDependenciesError, CycleDetectedError
        graph = DependencyGraph(spec_obj)
    except MissingDependenciesError as e:
        # Find which elements reference each missing ID
        for missing_id in sorted(e.missing):
            # Find which derived/constraint references this missing ID
            element_id = None
            for derived in spec_obj.derived:
                if missing_id in derived.inputs:
                    element_id = derived.id
                    break
            if not element_id and spec_obj.constraints:
                for constraint in spec_obj.constraints:
                    if missing_id in constraint.inputs:
                        element_id = constraint.id
                        break
            
            errors.append(ValidationIssue(
                code=ValidationCode.MISSING_INPUT.value,
                message=f"Input reference '{missing_id}' not found in spec",
                element_id=element_id,
                missing_id=missing_id
            ))
    except CycleDetectedError as e:
        errors.append(ValidationIssue(
            code=ValidationCode.CYCLE_DETECTED.value,
            message=str(e),
            element_id=None,
            cycle_path=e.cycle
        ))
    except Exception as e:
        # Unknown exception from DependencyGraph
        errors.append(ValidationIssue(
            code=ValidationCode.DEPENDENCY_GRAPH_ERROR.value,
            message=f"Unexpected error building dependency graph: {str(e)}",
            element_id=None
        ))
    
    # 4. Transform Ref Validation (ERROR, if registry provided)
    if registry:
        try:
            if isinstance(registry, dict):
                registry_obj = TransformRegistry(**registry)
            else:
                registry_obj = load_registry_from_path(_normalize_path(registry))
            
            transform_errors = validate_transform_refs(spec_obj, registry_obj)
            for error_msg in transform_errors:
                # Parse error message to extract element_id
                # Format: "Derived variable '{id}' ({name}) references missing transform '{ref}'..."
                element_id = None
                if "Derived variable '" in error_msg:
                    start = error_msg.find("Derived variable '") + len("Derived variable '")
                    end = error_msg.find("'", start)
                    element_id = error_msg[start:end]
                
                errors.append(ValidationIssue(
                    code=ValidationCode.MISSING_TRANSFORM_REF.value,
                    message=error_msg,
                    element_id=element_id
                ))
        except Exception as e:
            errors.append(ValidationIssue(
                code=ValidationCode.REGISTRY_LOAD_ERROR.value,
                message=f"Failed to load registry: {str(e)}",
                element_id=None
            ))
    
    # 5. Binding Validation (WARNINGS, if bindings + raw_schema provided)
    if bindings is not None:
        try:
            if isinstance(bindings, dict):
                bindings_obj = _load_bindings_from_dict(bindings)
            else:
                bindings_obj = _load_bindings_from_path(_normalize_path(bindings))
            
            # Load raw_schema if provided
            raw_schema_obj = None
            if raw_schema is not None:
                try:
                    if isinstance(raw_schema, dict):
                        raw_schema_obj = _load_raw_schema_from_dict(raw_schema)
                    else:
                        raw_schema_obj = _load_raw_schema_from_path(_normalize_path(raw_schema))
                except Exception as e:
                    warnings.append(ValidationIssue(
                        code=ValidationCode.RAW_SCHEMA_LOAD_ERROR.value,
                        message=f"Failed to load raw_schema: {str(e)}. Binding validation skipped.",
                        element_id=None
                    ))
            
            # Check missing bindings
            from cheshbon.kernel.bindings import check_missing_bindings
            missing_bindings_map = check_missing_bindings(spec_obj, bindings_obj)
            for derived_id, missing_source_ids in missing_bindings_map.items():
                for missing_source_id in sorted(missing_source_ids):
                    warnings.append(ValidationIssue(
                        code=ValidationCode.MISSING_BINDING.value,
                        message=f"Derived variable '{derived_id}' requires source '{missing_source_id}' but no binding found",
                        element_id=missing_source_id  # element_id is the source_id (the thing that's wrong)
                    ))
            
            # Check ambiguous bindings
            from cheshbon.kernel.bindings import check_ambiguous_bindings
            ambiguous_bindings_map = check_ambiguous_bindings(bindings_obj)
            for source_id, raw_columns in ambiguous_bindings_map.items():
                warnings.append(ValidationIssue(
                    code=ValidationCode.AMBIGUOUS_BINDING.value,
                    message=f"Source ID '{source_id}' is bound to multiple raw columns: {', '.join(sorted(raw_columns))}",
                    element_id=source_id  # element_id is the source_id (the thing that's wrong)
                ))
            
            # Check invalid raw columns (if raw_schema provided)
            if raw_schema_obj:
                from cheshbon.kernel.bindings import validate_bindings
                binding_events, _ = validate_bindings(raw_schema_obj, bindings_obj)
                for event in binding_events:
                    if event.event_type == "BINDING_INVALID":
                        warnings.append(ValidationIssue(
                            code=ValidationCode.INVALID_RAW_COLUMN.value,
                            message=event.details.get("reason", f"Raw column '{event.old_value}' not found in schema") if event.details else f"Raw column '{event.old_value}' not found in schema",
                            element_id=event.element,  # This is the source_id
                            raw_column=event.old_value
                        ))
        except Exception as e:
            warnings.append(ValidationIssue(
                code=ValidationCode.BINDINGS_LOAD_ERROR.value,
                message=f"Failed to load bindings: {str(e)}. Binding validation skipped.",
                element_id=None
            ))
    
    # Note: Duplicate inputs are already caught by pydantic validation in step 1
    # (DerivedVariable.validate_inputs and ConstraintNode.validate_inputs)
    
    # Sort errors and warnings for deterministic output
    # Sort by: code, then element_id, then missing_id
    def sort_key(issue: ValidationIssue) -> tuple:
        return (
            issue.code,
            issue.element_id or "",
            issue.missing_id or "",
            issue.raw_column or ""
        )
    
    errors_sorted = sorted(errors, key=sort_key)
    warnings_sorted = sorted(warnings, key=sort_key)
    
    ok = len(errors) == 0
    return ValidationResult(ok=ok, errors=errors_sorted, warnings=warnings_sorted)


def verify_sans_bundle(bundle_dir: Union[str, os.PathLike, Path]) -> ValidationResult:
    """
    Verify a SANS run bundle for integrity and consistency.
    """
    from cheshbon._internal.io.sans_bundle import load_bundle
    from cheshbon._internal.verify.sans_bundle import verify_bundle, BundleVerificationError
    
    bundle_dir = _normalize_path(bundle_dir)
    errors: List[ValidationIssue] = []
    
    try:
        bundle = load_bundle(bundle_dir)
        verify_bundle(bundle, bundle_dir)
    except BundleVerificationError as e:
        errors.append(ValidationIssue(
            code=e.code.value,
            message=e.message
        ))
    except FileNotFoundError as e:
        errors.append(ValidationIssue(
            code=ValidationCode.FILE_NOT_FOUND.value,
            message=str(e)
        ))
    except Exception as e:
        errors.append(ValidationIssue(
            code=ValidationCode.INVALID_STRUCTURE.value,
            message=f"Failed to load or verify bundle: {str(e)}"
        ))
        
    return ValidationResult(ok=len(errors) == 0, errors=errors, warnings=[])


def ingest_sans_bundle(
    bundle_dir: Union[str, os.PathLike, Path],
    output_dir: Union[str, os.PathLike, Path]
) -> None:
    """
    Ingest a SANS bundle and write Cheshbon artifacts.
    """
    from cheshbon._internal.io.sans_bundle import load_bundle
    from cheshbon._internal.verify.sans_bundle import verify_bundle
    from cheshbon._internal.ingest.sans import map_bundle_to_run_and_registry
    from cheshbon._internal.canonical_json import canonical_dumps
    from cheshbon._internal.io.graph_bundle import load_graph_from_bundle
    
    bundle_dir = _normalize_path(bundle_dir)
    output_dir = _normalize_path(output_dir)
    
    bundle = load_bundle(bundle_dir)
    verify_bundle(bundle, bundle_dir)
    
    graph = load_graph_from_bundle(bundle_dir)
    run_record, registry = map_bundle_to_run_and_registry(bundle, bundle_dir)
    
    # Write artifacts
    cheshbon_out = output_dir / "cheshbon"
    cheshbon_out.mkdir(parents=True, exist_ok=True)
    
    (cheshbon_out / "graph.json").write_text(canonical_dumps(graph.model_dump()) + "\n", encoding="utf-8")
    (cheshbon_out / "run.json").write_text(canonical_dumps(run_record.model_dump()) + "\n", encoding="utf-8")
    (cheshbon_out / "registry.json").write_text(canonical_dumps(registry.model_dump()) + "\n", encoding="utf-8")


def load_graph_bundle(bundle_dir: Union[str, os.PathLike, Path]) -> GraphV1:
    """Load and validate graph.json from a bundle directory."""
    from cheshbon._internal.io.graph_bundle import load_graph_from_bundle

    bundle_dir = _normalize_path(bundle_dir)
    return load_graph_from_bundle(bundle_dir)


def graph_diff_bundles(
    bundle_a: Union[str, os.PathLike, Path],
    bundle_b: Union[str, os.PathLike, Path],
) -> tuple[GraphDiff, Impact]:
    """Compute graph diff + downstream impact between two bundles."""
    from cheshbon.kernel.hash_utils import compute_canonical_json_sha256

    bundle_a = _normalize_path(bundle_a)
    bundle_b = _normalize_path(bundle_b)

    graph_a_path = bundle_a / "artifacts" / "graph.json"
    graph_b_path = bundle_b / "artifacts" / "graph.json"

    g1 = load_graph_bundle(bundle_a)
    g2 = load_graph_bundle(bundle_b)
    diff = diff_graph(g1, g2)
    diff = diff.model_copy(
        update={
            "graph_a_sha256": compute_canonical_json_sha256(graph_a_path),
            "graph_b_sha256": compute_canonical_json_sha256(graph_b_path),
        }
    )
    impact = impact_from_diff(g2, diff)
    return diff, impact

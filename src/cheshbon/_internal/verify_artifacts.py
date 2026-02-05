"""Artifact verification helpers for CLI commands."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Union

from cheshbon.api import ValidationIssue, ValidationResult, validate
from cheshbon.codes import ValidationCode
from cheshbon.kernel.bindings import (
    Bindings,
    RawColumn,
    RawSchema,
    check_ambiguous_bindings,
    check_missing_bindings,
    validate_bindings,
)
from cheshbon.kernel.spec import MappingSpec
from cheshbon.kernel.transform_registry import TransformRegistry
from cheshbon._internal.io.registry import load_registry_from_path


def _normalize_path(path: Union[str, Path]) -> Path:
    return path if isinstance(path, Path) else Path(path)


def _load_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_spec(spec: Union[str, Path, Dict[str, Any]]) -> MappingSpec:
    if isinstance(spec, dict):
        return MappingSpec(**spec)
    return MappingSpec(**_load_json(_normalize_path(spec)))


def _load_bindings(bindings: Union[str, Path, Dict[str, Any]]) -> Bindings:
    if isinstance(bindings, dict):
        data = bindings
    else:
        data = _load_json(_normalize_path(bindings))
    return Bindings(table=data["table"], bindings=data["bindings"])


def _load_raw_schema(raw_schema: Union[str, Path, Dict[str, Any]]) -> RawSchema:
    if isinstance(raw_schema, dict):
        data = raw_schema
    else:
        data = _load_json(_normalize_path(raw_schema))
    table = data["table"]
    columns = [RawColumn(name=col["name"], type=col["type"]) for col in data["columns"]]
    return RawSchema(table=table, columns=columns)


def verify_spec(
    spec: Union[str, Path, Dict[str, Any]],
    registry: Optional[Union[str, Path, Dict[str, Any]]] = None,
    bindings: Optional[Union[str, Path, Dict[str, Any]]] = None,
    raw_schema: Optional[Union[str, Path, Dict[str, Any]]] = None,
) -> ValidationResult:
    """Verify a mapping spec using the same validation path as diff."""
    return validate(
        spec=spec,
        registry=registry,
        bindings=bindings,
        raw_schema=raw_schema,
    )


def verify_registry(
    registry: Union[str, Path, Dict[str, Any]]
) -> ValidationResult:
    """Verify a registry artifact by parsing with kernel registry model."""
    errors: list[ValidationIssue] = []
    warnings: list[ValidationIssue] = []
    try:
        if isinstance(registry, dict):
            TransformRegistry(**registry)
        else:
            load_registry_from_path(_normalize_path(registry))
    except Exception as e:
        errors.append(ValidationIssue(
            code=ValidationCode.REGISTRY_LOAD_ERROR.value,
            message=f"Failed to load registry: {str(e)}",
            element_id=None,
        ))

    ok = len(errors) == 0
    return ValidationResult(ok=ok, errors=errors, warnings=warnings)


def verify_bindings(
    bindings: Union[str, Path, Dict[str, Any]],
    spec: Optional[Union[str, Path, Dict[str, Any]]] = None,
    raw_schema: Optional[Union[str, Path, Dict[str, Any]]] = None,
) -> ValidationResult:
    """Verify bindings with the same checks used during diff/validation."""
    errors: list[ValidationIssue] = []
    warnings: list[ValidationIssue] = []

    try:
        bindings_obj = _load_bindings(bindings)
    except Exception as e:
        errors.append(ValidationIssue(
            code=ValidationCode.BINDINGS_LOAD_ERROR.value,
            message=f"Failed to load bindings: {str(e)}",
            element_id=None,
        ))
        return ValidationResult(ok=False, errors=errors, warnings=warnings)

    ambiguous_bindings_map = check_ambiguous_bindings(bindings_obj)
    for source_id, raw_columns in ambiguous_bindings_map.items():
        errors.append(ValidationIssue(
            code=ValidationCode.AMBIGUOUS_BINDING.value,
            message=(
                f"Source ID '{source_id}' is bound to multiple raw columns: "
                f"{', '.join(sorted(raw_columns))}"
            ),
            element_id=source_id,
        ))

    if spec is not None:
        try:
            spec_obj = _load_spec(spec)
            missing_bindings_map = check_missing_bindings(spec_obj, bindings_obj)
            for derived_id, missing_source_ids in missing_bindings_map.items():
                for missing_source_id in sorted(missing_source_ids):
                    warnings.append(ValidationIssue(
                        code=ValidationCode.MISSING_BINDING.value,
                        message=(
                            f"Derived variable '{derived_id}' requires source "
                            f"'{missing_source_id}' but no binding found"
                        ),
                        element_id=missing_source_id,
                    ))
        except Exception as e:
            errors.append(ValidationIssue(
                code=ValidationCode.INVALID_STRUCTURE.value,
                message=f"Failed to parse spec: {str(e)}",
                element_id=None,
            ))

    if raw_schema is not None:
        try:
            raw_schema_obj = _load_raw_schema(raw_schema)
            binding_events, _ = validate_bindings(raw_schema_obj, bindings_obj)
            for event in binding_events:
                if event.event_type == "BINDING_INVALID":
                    warnings.append(ValidationIssue(
                        code=ValidationCode.INVALID_RAW_COLUMN.value,
                        message=event.details.get(
                            "reason",
                            f"Raw column '{event.old_value}' not found in schema"
                        ) if event.details else f"Raw column '{event.old_value}' not found in schema",
                        element_id=event.element,
                        raw_column=event.old_value,
                    ))
        except Exception as e:
            warnings.append(ValidationIssue(
                code=ValidationCode.RAW_SCHEMA_LOAD_ERROR.value,
                message=f"Failed to load raw_schema: {str(e)}. Binding validation skipped.",
                element_id=None,
            ))

    def _sort_key(issue: ValidationIssue) -> tuple:
        return (
            issue.code,
            issue.element_id or "",
            issue.missing_id or "",
            issue.raw_column or "",
        )

    errors_sorted = sorted(errors, key=_sort_key)
    warnings_sorted = sorted(warnings, key=_sort_key)
    ok = len(errors_sorted) == 0
    return ValidationResult(ok=ok, errors=errors_sorted, warnings=warnings_sorted)

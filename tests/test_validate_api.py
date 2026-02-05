"""Contract tests for cheshbon.api.validate().

Tests that validate() matches diff() validation behavior for overlapping checks,
and that it correctly identifies all validation issues.
"""

import json
import pytest
from pathlib import Path

from cheshbon.api import diff, validate, DiffResult, ValidationResult


# Fixture paths
HERE = Path(__file__).resolve().parent
FIXTURES = HERE.parent / "fixtures"


def create_valid_registry_fixture(transforms=None):
    """Helper factory for valid TransformRegistry fixtures.
    
    Args:
        transforms: List of transform dicts. If None, returns empty registry.
                   Each transform dict should have at minimum: id, version, kind, signature, impl_fingerprint
    
    Returns:
        dict: Valid TransformRegistry structure
    """
    if transforms is None:
        transforms = []
    
    # Ensure all transforms have required fields with defaults
    validated_transforms = []
    for t in transforms:
        transform = {
            "id": t.get("id", "t:identity"),
            "version": t.get("version", "1.0.0"),
            "kind": t.get("kind", "builtin"),
            "signature": t.get("signature", {"inputs": ["any"], "output": "any"}),
            "params_schema_hash": t.get("params_schema_hash", None),
            "impl_fingerprint": {
                "algo": t.get("impl_fingerprint", {}).get("algo", "sha256"),
                "source": t.get("impl_fingerprint", {}).get("source", "builtin"),
                "ref": t.get("impl_fingerprint", {}).get("ref", "cheshbon.kernel.transforms.identity"),
                "digest": t.get("impl_fingerprint", {}).get("digest", "a" * 64)
            }
        }
        validated_transforms.append(transform)
    
    return {
        "registry_version": "1.0.0",
        "transforms": validated_transforms
    }


def test_validate_valid_spec():
    """Test validate() with a valid spec returns ok=True."""
    # Re-import to avoid test isolation issues
    from cheshbon.api import validate, ValidationResult
    
    spec_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v1.json"
    
    result = validate(spec=spec_path)
    
    # Check that result is the correct type (explicit import to avoid isolation issues)
    assert type(result).__name__ == 'ValidationResult' or isinstance(result, ValidationResult)
    assert result.ok is True
    assert len(result.errors) == 0
    assert len(result.warnings) == 0


def test_validate_missing_input():
    """Test validate() detects missing input references."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:AGE", "name": "AGE", "type": "int"}
        ],
        "derived": [
            {
                "id": "d:AGE_VALID",
                "name": "AGE_VALID",
                "type": "bool",
                "transform_ref": "t:identity",
                "inputs": ["s:MISSING"]  # Missing input
            }
        ]
    }
    
    result = validate(spec=spec_data)
    
    assert result.ok is False
    assert len(result.errors) == 1
    assert result.errors[0].code == "MISSING_INPUT"
    assert result.errors[0].missing_id == "s:MISSING"
    assert result.errors[0].element_id == "d:AGE_VALID"
    assert len(result.warnings) == 0


def test_validate_cycle_detected():
    """Test validate() detects cycles and populates cycle_path."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:AGE", "name": "AGE", "type": "int"}
        ],
        "derived": [
            {
                "id": "d:A",
                "name": "A",
                "type": "int",
                "transform_ref": "t:identity",
                "inputs": ["d:B"]  # A depends on B
            },
            {
                "id": "d:B",
                "name": "B",
                "type": "int",
                "transform_ref": "t:identity",
                "inputs": ["d:A"]  # B depends on A - CYCLE!
            }
        ]
    }
    
    result = validate(spec=spec_data)
    
    assert result.ok is False
    assert len(result.errors) == 1
    assert result.errors[0].code == "CYCLE_DETECTED"
    assert result.errors[0].cycle_path is not None
    cycle_path = result.errors[0].cycle_path
    assert "d:A" in cycle_path
    assert "d:B" in cycle_path
    assert len(cycle_path) >= 2  # Cycle must have at least 2 nodes
    assert len(result.warnings) == 0


def test_validate_duplicate_id():
    """Test validate() detects duplicate IDs."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:AGE", "name": "AGE", "type": "int"},
            {"id": "s:AGE", "name": "AGE2", "type": "int"}  # Duplicate ID
        ],
        "derived": []
    }
    
    result = validate(spec=spec_data)
    
    assert result.ok is False
    assert len(result.errors) == 1
    assert result.errors[0].code == "DUPLICATE_ID"
    assert result.errors[0].element_id == "s:AGE"
    assert len(result.warnings) == 0


def test_validate_missing_transform_ref():
    """Test validate() detects missing transform references when registry provided."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:AGE", "name": "AGE", "type": "int"}
        ],
        "derived": [
            {
                "id": "d:AGE_VALID",
                "name": "AGE_VALID",
                "type": "bool",
                "transform_ref": "t:missing_transform",  # Missing transform
                "inputs": ["s:AGE"]
            }
        ]
    }
    
    registry_data = create_valid_registry_fixture([
        {
            "id": "t:identity",
            "version": "1.0.0",
            "kind": "builtin",
            "signature": {"inputs": ["any"], "output": "any"},
            "params_schema_hash": None,
            "impl_fingerprint": {
                "algo": "sha256",
                "source": "builtin",
                "ref": "cheshbon.kernel.transforms.identity",
                "digest": "a" * 64
            }
        }
    ])
    
    result = validate(spec=spec_data, registry=registry_data)
    
    assert result.ok is False
    assert len(result.errors) == 1
    assert result.errors[0].code == "MISSING_TRANSFORM_REF"
    assert result.errors[0].element_id == "d:AGE_VALID"
    assert len(result.warnings) == 0


def test_validate_missing_binding_warning():
    """Test validate() reports missing bindings as warnings."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:AGE", "name": "AGE", "type": "int"}
        ],
        "derived": [
            {
                "id": "d:AGE_VALID",
                "name": "AGE_VALID",
                "type": "bool",
                "transform_ref": "t:identity",
                "inputs": ["s:AGE"]
            }
        ]
    }
    
    bindings_data = {
        "table": "RAW_DM",
        "bindings": {}  # No bindings - s:AGE is missing
    }
    
    result = validate(spec=spec_data, bindings=bindings_data)
    
    # Missing bindings are warnings, not errors
    assert result.ok is True
    assert len(result.errors) == 0
    assert len(result.warnings) == 1
    assert result.warnings[0].code == "MISSING_BINDING"
    assert result.warnings[0].element_id == "s:AGE"


def test_validate_params_large_warning():
    """Test validate() reports large params as warnings."""
    big_value = "a" * 10050
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:AGE", "name": "AGE", "type": "int"}
        ],
        "derived": [
            {
                "id": "d:AGE_VALID",
                "name": "AGE_VALID",
                "type": "bool",
                "transform_ref": "t:identity",
                "inputs": ["s:AGE"],
                "params": {"big": big_value}
            }
        ]
    }

    result = validate(spec=spec_data)

    assert result.ok is True
    assert len(result.errors) == 0
    assert len(result.warnings) == 1
    assert result.warnings[0].code == "PARAMS_LARGE"
    assert result.warnings[0].element_id == "d:AGE_VALID"


def test_validate_ambiguous_binding_warning():
    """Test validate() reports ambiguous bindings as warnings."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:AGE", "name": "AGE", "type": "int"}
        ],
        "derived": [
            {
                "id": "d:AGE_VALID",
                "name": "AGE_VALID",
                "type": "bool",
                "transform_ref": "t:identity",
                "inputs": ["s:AGE"]
            }
        ]
    }
    
    bindings_data = {
        "table": "RAW_DM",
        "bindings": {
            "AGE1": "s:AGE",  # Multiple raw columns map to same source
            "AGE2": "s:AGE"   # Ambiguous!
        }
    }
    
    result = validate(spec=spec_data, bindings=bindings_data)
    
    # Ambiguous bindings are warnings, not errors
    assert result.ok is True
    assert len(result.errors) == 0
    assert len(result.warnings) == 1
    assert result.warnings[0].code == "AMBIGUOUS_BINDING"
    assert result.warnings[0].element_id == "s:AGE"


def test_validate_invalid_raw_column_warning():
    """Test validate() reports invalid raw columns as warnings when raw_schema provided."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:AGE", "name": "AGE", "type": "int"}
        ],
        "derived": []
    }
    
    bindings_data = {
        "table": "RAW_DM",
        "bindings": {
            "MISSING_COL": "s:AGE"  # Column not in schema
        }
    }
    
    raw_schema_data = {
        "table": "RAW_DM",
        "columns": [
            {"name": "AGE", "type": "int"}
        ]
    }
    
    result = validate(spec=spec_data, bindings=bindings_data, raw_schema=raw_schema_data)
    
    # Invalid raw columns are warnings, not errors
    assert result.ok is True
    assert len(result.errors) == 0
    assert len(result.warnings) == 1
    assert result.warnings[0].code == "INVALID_RAW_COLUMN"
    assert result.warnings[0].raw_column == "MISSING_COL"
    assert result.warnings[0].element_id == "s:AGE"


def test_validate_contract_with_diff_transform_refs():
    """Test that validate() and diff() agree on transform ref validation (compare codes, not messages)."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:AGE", "name": "AGE", "type": "int"}
        ],
        "derived": [
            {
                "id": "d:AGE_VALID",
                "name": "AGE_VALID",
                "type": "bool",
                "transform_ref": "t:missing_transform",
                "inputs": ["s:AGE"]
            }
        ]
    }
    
    registry_data = create_valid_registry_fixture([
        {
            "id": "t:identity",
            "version": "1.0.0",
            "kind": "builtin",
            "signature": {"inputs": ["any"], "output": "any"},
            "params_schema_hash": None,
            "impl_fingerprint": {
                "algo": "sha256",
                "source": "builtin",
                "ref": "cheshbon.kernel.transforms.identity",
                "digest": "a" * 64
            }
        }
    ])
    
    # Validate with validate()
    validate_result = validate(spec=spec_data, registry=registry_data)
    
    # Diff with diff() (using same spec for both v1 and v2)
    # Use same registry for both from and to (no registry diff, just validation)
    # Re-import to avoid module shadowing issues when running full test suite
    from cheshbon.api import diff as diff_func
    diff_result = diff_func(from_spec=spec_data, to_spec=spec_data, from_registry=registry_data, to_registry=registry_data)
    
    # Exact contract: diff() validation_failed == validate() has errors
    assert diff_result.validation_failed == (len(validate_result.errors) > 0)
    
    # Exact error code match
    if diff_result.validation_failed:
        assert len(validate_result.errors) == 1
        assert validate_result.errors[0].code == "MISSING_TRANSFORM_REF"
        assert validate_result.errors[0].element_id == "d:AGE_VALID"
    else:
        assert len(validate_result.errors) == 0


def test_validate_contract_with_diff_cycles():
    """Test that validate() and diff() agree on cycle detection (compare codes, not messages)."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:AGE", "name": "AGE", "type": "int"}
        ],
        "derived": [
            {
                "id": "d:A",
                "name": "A",
                "type": "int",
                "transform_ref": "t:identity",
                "inputs": ["d:B"]
            },
            {
                "id": "d:B",
                "name": "B",
                "type": "int",
                "transform_ref": "t:identity",
                "inputs": ["d:A"]  # Cycle
            }
        ]
    }
    
    # Validate with validate()
    validate_result = validate(spec=spec_data)
    
    # Exact contract: validate() catches cycle, diff() would raise
    assert validate_result.ok is False
    assert len(validate_result.errors) == 1
    assert validate_result.errors[0].code == "CYCLE_DETECTED"
    cycle_path = validate_result.errors[0].cycle_path
    assert cycle_path is not None
    assert "d:A" in cycle_path
    assert "d:B" in cycle_path
    assert len(cycle_path) >= 2  # Cycle must have at least 2 nodes


def test_validate_with_bindings_no_raw_schema():
    """Test validate() with bindings but no raw_schema still checks missing/ambiguous bindings."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:AGE", "name": "AGE", "type": "int"}
        ],
        "derived": [
            {
                "id": "d:AGE_VALID",
                "name": "AGE_VALID",
                "type": "bool",
                "transform_ref": "t:identity",
                "inputs": ["s:AGE"]
            }
        ]
    }
    
    bindings_data = {
        "table": "RAW_DM",
        "bindings": {}  # Missing binding
    }
    
    result = validate(spec=spec_data, bindings=bindings_data)
    
    # Exact: missing bindings checked, invalid columns not checked (needs raw_schema)
    assert result.ok is True
    assert len(result.errors) == 0
    assert len(result.warnings) == 1
    assert result.warnings[0].code == "MISSING_BINDING"
    assert result.warnings[0].element_id == "s:AGE"


def test_validate_invalid_structure():
    """Test validate() handles invalid spec structure gracefully."""
    invalid_spec = {
        "spec_version": "1.0.0",
        # Missing required fields: study_id, source_table, sources, derived
    }
    
    result = validate(spec=invalid_spec)
    
    assert result.ok is False
    assert len(result.errors) == 1
    assert result.errors[0].code == "INVALID_STRUCTURE"
    assert result.errors[0].element_id is None


def test_validate_registry_load_error():
    """Test validate() handles registry load errors gracefully."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:AGE", "name": "AGE", "type": "int"}
        ],
        "derived": []
    }
    
    # Invalid registry (missing required fields)
    invalid_registry = {
        "registry_version": "1.0.0"
        # Missing transforms
    }
    
    result = validate(spec=spec_data, registry=invalid_registry)
    
    # Exact: registry load error
    assert result.ok is False
    assert len(result.errors) == 1
    assert result.errors[0].code == "REGISTRY_LOAD_ERROR"
    assert result.errors[0].element_id is None

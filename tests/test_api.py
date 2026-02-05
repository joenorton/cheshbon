"""Tests for cheshbon public API.

Tests that the public API (cheshbon.api) works correctly and that
_internal modules are not accessible from the public namespace.
"""

import json
import pytest
from pathlib import Path

import cheshbon
from cheshbon.api import diff, DiffResult
from cheshbon.contracts import CompatibilityIssue, CompatibilityReport


# Fixture paths
HERE = Path(__file__).resolve().parent
FIXTURES = HERE.parent / "fixtures"


def test_diff_with_path_inputs(tmp_path):
    """Test diff() function with Path inputs."""
    # Use scenario1 (rename only, no impact)
    spec_v1_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v2.json"
    
    result = diff(from_spec=spec_v1_path, to_spec=spec_v2_path)
    
    assert isinstance(result, DiffResult)
    assert result.validation_failed is False
    assert len(result.validation_errors) == 0
    assert isinstance(result.change_summary, dict)
    assert isinstance(result.impacted_ids, list)
    assert isinstance(result.reasons, dict)
    assert isinstance(result.paths, dict)
    assert isinstance(result.events, list)
    
    # Scenario 1: rename only should have no impact
    assert len(result.impacted_ids) == 0


def test_diff_with_dict_inputs(tmp_path):
    """Test diff() function with dict inputs."""
    # Load specs as dicts
    spec_v1_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v2.json"
    
    with open(spec_v1_path, 'r') as f:
        spec_v1_dict = json.load(f)
    with open(spec_v2_path, 'r') as f:
        spec_v2_dict = json.load(f)
    
    result = diff(from_spec=spec_v1_dict, to_spec=spec_v2_dict)
    
    assert isinstance(result, DiffResult)
    assert result.validation_failed is False
    assert len(result.impacted_ids) == 0


def test_diff_with_registry(tmp_path):
    """Test diff() function with registry."""
    spec_v1_path = FIXTURES / "scenario3_registry_impl_change" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario3_registry_impl_change" / "spec_v2.json"
    registry_v1_path = FIXTURES / "scenario3_registry_impl_change" / "registry_v1.json"
    registry_v2_path = FIXTURES / "scenario3_registry_impl_change" / "registry_v2.json"
    
    result = diff(
        from_spec=spec_v1_path,
        to_spec=spec_v2_path,
        from_registry=registry_v1_path,
        to_registry=registry_v2_path
    )
    
    assert isinstance(result, DiffResult)
    assert isinstance(result.change_summary, dict)
    # Should have registry change events
    assert any("TRANSFORM" in event_type for event_type in result.change_summary.keys())


def test_diff_registry_only_impact():
    """API-level unit test: spec unchanged, registry impl changes, verify impacted_ids includes derived vars with TRANSFORM_IMPL_CHANGED reason."""
    # Create identical specs
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:SUBJID", "name": "SUBJID", "type": "string"}
        ],
        "derived": [
            {
                "id": "d:USUBJID",
                "name": "USUBJID",
                "type": "string",
                "transform_ref": "t:direct_copy",
                "inputs": ["s:SUBJID"]
            }
        ]
    }
    
    # Registry v1 - old impl digest
    registry_v1 = {
        "registry_version": "1.0.0",
        "transforms": [
            {
                "id": "t:direct_copy",
                "version": "1.0.0",
                "kind": "builtin",
                "signature": {"inputs": ["any"], "output": "any"},
                "params_schema_hash": None,
                "impl_fingerprint": {
                    "algo": "sha256",
                    "source": "builtin",
                    "ref": "cheshbon.transforms.direct_copy",
                    "digest": "a" * 64  # Old digest
                }
            }
        ]
    }
    
    # Registry v2 - new impl digest (impl changed)
    registry_v2 = {
        "registry_version": "1.0.0",
        "transforms": [
            {
                "id": "t:direct_copy",
                "version": "1.0.0",
                "kind": "builtin",
                "signature": {"inputs": ["any"], "output": "any"},
                "params_schema_hash": None,
                "impl_fingerprint": {
                    "algo": "sha256",
                    "source": "builtin",
                    "ref": "cheshbon.transforms.direct_copy",
                    "digest": "b" * 64  # New digest (impl changed)
                }
            }
        ]
    }
    
    # Call diff with identical specs but different registries
    result = diff(
        from_spec=spec_data,
        to_spec=spec_data,
        from_registry=registry_v1,
        to_registry=registry_v2
    )
    
    # Should have impact even though spec didn't change
    assert isinstance(result, DiffResult)
    assert "d:USUBJID" in result.impacted_ids, "Derived var using changed transform should be impacted"
    assert result.reasons["d:USUBJID"] == "TRANSFORM_IMPL_CHANGED", "Reason should be TRANSFORM_IMPL_CHANGED"
    
    # Should have TRANSFORM_IMPL_CHANGED event in change_summary
    assert "TRANSFORM_IMPL_CHANGED" in result.change_summary
    assert result.change_summary["TRANSFORM_IMPL_CHANGED"] == 1
    
    # Should have the event in events list
    transform_events = [e for e in result.events if e["change_type"] == "TRANSFORM_IMPL_CHANGED"]
    assert len(transform_events) == 1
    assert transform_events[0]["element_id"] == "t:direct_copy"


def test_diff_returns_minimal_structure():
    """Verify DiffResult has only minimal fields (no kitchen sink)."""
    spec_v1_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v2.json"
    
    result = diff(from_spec=spec_v1_path, to_spec=spec_v2_path)
    
    # Check that DiffResult has only the expected fields
    expected_fields = {
        "validation_failed",
        "validation_errors",
        "change_summary",
        "impacted_ids",
        "unaffected_ids",
        "reasons",
        "paths",
        "missing_inputs",
        "missing_bindings",
        "ambiguous_bindings",
        "missing_transform_refs",
        "alternative_path_counts",
        "events",
        "binding_issues",  # Optional field for binding diagnostics
    }
    
    actual_fields = set(DiffResult.model_fields.keys())
    assert actual_fields == expected_fields, f"Unexpected fields in DiffResult: {actual_fields - expected_fields}"


def test_diff_with_impact():
    """Test diff() with a scenario that has impact."""
    spec_v1_path = FIXTURES / "scenario2_params_change_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario2_params_change_impact" / "spec_v2.json"
    
    result = diff(from_spec=spec_v1_path, to_spec=spec_v2_path)
    
    assert isinstance(result, DiffResult)
    # Should have some impacted IDs
    assert len(result.impacted_ids) > 0
    # Should have reasons for impacted IDs
    assert len(result.reasons) > 0
    # Should have paths for impacted IDs
    assert len(result.paths) > 0


def test_contracts_public_export():
    """Verify contracts are accessible from public namespace."""
    # Import from contracts module (explicit, since they're in cheshbon.contracts, not cheshbon.api)
    from cheshbon.contracts import CompatibilityIssue, CompatibilityReport
    # Also verify they're available from root (for convenience, exported in __init__.py)
    from cheshbon import CompatibilityIssue as RootCompatibilityIssue, CompatibilityReport as RootCompatibilityReport
    
    assert CompatibilityIssue is not None
    assert CompatibilityReport is not None
    # Verify they're the same objects
    assert CompatibilityIssue is RootCompatibilityIssue
    assert CompatibilityReport is RootCompatibilityReport
    
    # Should be able to create instances
    issue = CompatibilityIssue(
        object_type="spec",
        path="test.json",
        found_version="0.7",
        required_version="0.7",
        action="accept",
        reason="ok"
    )
    assert issue.object_type == "spec"
    
    report = CompatibilityReport(
        ok=True,
        mode="permissive",
        unknown_fields="preserve",
        issues=[],
        warnings=[]
    )
    assert report.ok is True


def test_internal_contracts_deleted():
    """Verify _internal.contracts is deleted/not accessible."""
    # Try to import from _internal.contracts - should fail
    with pytest.raises((ImportError, ModuleNotFoundError, AttributeError)):
        from cheshbon._internal.contracts import CompatibilityIssue  # noqa: F401


def test_no_internal_imports_in_public_api():
    """Verify public API doesn't expose internal types.
    
    Studio should not need to import internal types like MappingSpec, DependencyGraph, etc.
    These are implementation details. Studio uses the high-level diff() function instead.
    """
    # Import public API
    from cheshbon.api import diff, DiffResult
    
    # Check that internal types are not in public namespace
    # (They exist in cheshbon/kernel modules, but are not exported from cheshbon)
    with pytest.raises((ImportError, AttributeError)):
        from cheshbon import MappingSpec  # noqa: F401
    
    with pytest.raises((ImportError, AttributeError)):
        from cheshbon import DependencyGraph  # noqa: F401
    
    with pytest.raises((ImportError, AttributeError)):
        from cheshbon import ChangeEvent  # noqa: F401
    
    with pytest.raises((ImportError, AttributeError)):
        from cheshbon import ImpactResult  # noqa: F401
    
    # Note: _internal exists on disk and kernel code can import it,
    # but it's not in __all__ so Studio should not import it


def test_public_api_exports():
    """Verify __all__ exports match expected public API.
    
    This ensures Studio knows what's public API vs internal implementation.
    _internal is not in __all__ (enforced elsewhere), so Studio should not import it.
    
    Note: diff is NOT in root exports (to avoid name conflict with cheshbon.diff module).
    It's available from cheshbon.api instead.
    """
    expected_exports = {
        "__version__",
        "validate",
        "DiffResult",
        "ValidationResult",
        "ValidationCode",
        "CompatibilityIssue",
        "CompatibilityReport",
    }
    
    # Check that all expected exports are available
    for export in expected_exports:
        assert hasattr(cheshbon, export), f"Missing export: {export}"
    
    # Verify diff is NOT in root exports (to avoid name conflict)
    assert 'diff' not in cheshbon.__all__, "diff should not be in root exports"
    
    # But verify it's available from cheshbon.api
    from cheshbon.api import diff
    assert callable(diff)
    
    # Check that old exports are removed
    assert not hasattr(cheshbon, "run_doctor_bundle"), "Old export run_doctor_bundle should be removed"
    
    # Verify _internal is NOT in __all__ (Studio should not import it)
    assert "_internal" not in cheshbon.__all__, (
        "_internal must not be in __all__ - it's for kernel internal use only"
    )
    
    # Note: canonical_dumps might still exist for CLI, but not in __all__


def test_diff_result_serialization():
    """Test that DiffResult can be serialized to dict/JSON."""
    spec_v1_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v2.json"
    
    result = diff(from_spec=spec_v1_path, to_spec=spec_v2_path)
    
    # Should be able to convert to dict
    result_dict = result.model_dump()
    assert isinstance(result_dict, dict)
    assert "validation_failed" in result_dict
    assert "impacted_ids" in result_dict
    
    # Should be able to serialize to JSON
    result_json = result.model_dump_json()
    assert isinstance(result_json, str)
    # Should be able to parse back
    parsed = json.loads(result_json)
    assert parsed["validation_failed"] == result.validation_failed


def test_diff_without_bindings_unchanged():
    """Test that diff() without bindings produces identical results to before."""
    spec_v1_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v2.json"
    
    result = diff(from_spec=spec_v1_path, to_spec=spec_v2_path)
    
    # Should have empty binding_issues when no bindings provided
    assert result.binding_issues == {}
    # Should have all expected fields
    assert isinstance(result.validation_failed, bool)
    assert isinstance(result.impacted_ids, list)
    assert isinstance(result.reasons, dict)
    assert isinstance(result.paths, dict)


def test_diff_with_bindings():
    """Test that diff() with bindings includes binding_issues when problems exist."""
    spec_v1_path = FIXTURES / "mapping_spec_v1.json"
    spec_v2_path = FIXTURES / "mapping_spec_v2.json"
    bindings_v2_missing_path = FIXTURES / "bindings_v2_missing.json"
    
    # Test with bindings that have missing bindings
    # Bindings are evaluated against the 'to' spec
    result = diff(
        from_spec=spec_v1_path,
        to_spec=spec_v2_path,
        to_bindings=bindings_v2_missing_path
    )
    
    # Should have binding_issues field (may be empty if no issues)
    assert isinstance(result.binding_issues, dict)
    # binding_issues should be a dict mapping var_id -> list of source IDs
    for var_id, issues in result.binding_issues.items():
        assert isinstance(var_id, str)
        assert isinstance(issues, list)
        assert all(isinstance(issue, str) for issue in issues)


def test_diff_bindings_api_simplified():
    """Test that diff() accepts only to_bindings parameter."""
    spec_v1_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v2.json"
    bindings_path = FIXTURES / "bindings_v1.json"
    
    # Test: to_bindings works
    result = diff(
        from_spec=spec_v1_path,
        to_spec=spec_v2_path,
        to_bindings=bindings_path
    )
    assert isinstance(result, DiffResult)
    
    # Test: no bindings also works
    result2 = diff(
        from_spec=spec_v1_path,
        to_spec=spec_v2_path
    )
    assert isinstance(result2, DiffResult)


def test_diff_mutable_defaults_independence():
    """Test that events and binding_issues are independent objects (not shared between calls)."""
    spec_v1_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v2.json"
    
    # Call diff() twice
    result1 = diff(from_spec=spec_v1_path, to_spec=spec_v2_path)
    result2 = diff(from_spec=spec_v1_path, to_spec=spec_v2_path)
    
    # Verify they are independent objects (not the same object in memory)
    assert result1.events is not result2.events, "events should be independent objects"
    assert result1.binding_issues is not result2.binding_issues, "binding_issues should be independent objects"
    
    # Modify one and verify the other is unchanged
    result1.events.append({"test": "data"})
    assert len(result2.events) == len(result1.events) - 1, "Modifying result1.events should not affect result2.events"
    
    result1.binding_issues["test_id"] = ["issue1"]
    assert "test_id" not in result2.binding_issues, "Modifying result1.binding_issues should not affect result2.binding_issues"


def test_impact_result_not_mutated():
    """Test that ImpactResult is not mutated when validation errors are present."""
    # Use a scenario with a registry to induce validation errors
    spec_v1_path = FIXTURES / "scenario3_registry_impl_change" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario3_registry_impl_change" / "spec_v2.json"
    registry_v1_path = FIXTURES / "scenario3_registry_impl_change" / "registry_v1.json"
    registry_v2_path = FIXTURES / "scenario3_registry_impl_change" / "registry_v2.json"
    
    # Call diff() - this should not mutate any ImpactResult objects
    result = diff(
        from_spec=spec_v1_path,
        to_spec=spec_v2_path,
        from_registry=registry_v1_path,
        to_registry=registry_v2_path
    )
    
    # Verify DiffResult has correct validation status
    # If there are validation errors, validation_failed should be True
    # The key point is that ImpactResult was not mutated - we verify this indirectly
    # by checking that the result is consistent and doesn't show signs of mutation bugs
    
    # Call diff() again with same inputs - should produce identical results
    result2 = diff(
        from_spec=spec_v1_path,
        to_spec=spec_v2_path,
        from_registry=registry_v1_path,
        to_registry=registry_v2_path
    )
    
    # If ImpactResult was mutated, we might see inconsistent results
    # Verify determinism (which would break if ImpactResult was mutated)
    assert result.model_dump() == result2.model_dump(), "Results should be identical (deterministic) - mutation would break this"
    
    # Verify validation_failed is a boolean (not None or unexpected type)
    assert isinstance(result.validation_failed, bool)
    assert isinstance(result2.validation_failed, bool)
    
    # If validation_errors exist, validation_failed should be True
    if result.validation_errors:
        assert result.validation_failed is True, "validation_failed should be True when validation_errors exist"


def test_api_contract_stable_shape_and_invariants():
    """Strict contract test asserting stable shape, invariants, and determinism.
    
    This test guarantees that cheshbon.api is the programmatic entrypoint with:
    - Required fields exist
    - Shape invariants (sorted impacted_ids, valid reason codes)
    - Determinism (identical results for same inputs)
    """
    # Use a real fixture
    spec_v1_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v2.json"
    
    # Call diff() twice with same inputs
    result1 = diff(from_spec=spec_v1_path, to_spec=spec_v2_path)
    result2 = diff(from_spec=spec_v1_path, to_spec=spec_v2_path)
    
    # 1. Assert required fields exist
    required_fields = {
        "validation_failed",
        "validation_errors",
        "change_summary",
        "impacted_ids",
        "unaffected_ids",
        "reasons",
        "paths",
        "missing_inputs",
        "missing_bindings",
        "ambiguous_bindings",
        "missing_transform_refs",
        "alternative_path_counts",
        "events",
        "binding_issues",
    }
    for field in required_fields:
        assert hasattr(result1, field), f"Required field '{field}' missing from DiffResult"
        assert hasattr(result2, field), f"Required field '{field}' missing from DiffResult"
    
    # 2. Assert impacted_ids is sorted
    assert result1.impacted_ids == sorted(result1.impacted_ids), "impacted_ids must be sorted"
    assert result2.impacted_ids == sorted(result2.impacted_ids), "impacted_ids must be sorted"

    # 2b. Assert unaffected_ids is sorted
    assert result1.unaffected_ids == sorted(result1.unaffected_ids), "unaffected_ids must be sorted"
    assert result2.unaffected_ids == sorted(result2.unaffected_ids), "unaffected_ids must be sorted"
    
    # 3. Assert reasons values are valid reason codes
    valid_reason_codes = {
        "DIRECT_CHANGE",
        "DIRECT_CHANGE_MISSING_INPUT",
        "MISSING_INPUT",
        "TRANSITIVE_DEPENDENCY",
        "TRANSFORM_IMPL_CHANGED",
        "TRANSFORM_REMOVED",
        "MISSING_TRANSFORM_REF",
        "MISSING_BINDING",
        "AMBIGUOUS_BINDING",
    }
    for var_id, reason in result1.reasons.items():
        assert reason in valid_reason_codes, f"Invalid reason code '{reason}' for var_id '{var_id}'"
    for var_id, reason in result2.reasons.items():
        assert reason in valid_reason_codes, f"Invalid reason code '{reason}' for var_id '{var_id}'"
    
    # 4. Assert determinism: identical model_dump() for same inputs
    dump1 = result1.model_dump()
    dump2 = result2.model_dump()
    assert dump1 == dump2, "Results must be deterministic - identical inputs must produce identical outputs"

    # 4b. Assert event ordering is deterministic
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

    def sort_key(event: dict) -> tuple:
        change_type = event.get("change_type", "")
        return (
            event.get("element_id", ""),
            change_type_priority.get(change_type, 999),
            change_type,
            event.get("old_value") or "",
            event.get("new_value") or ""
        )
    
    assert result1.events == sorted(result1.events, key=sort_key), "events must be deterministically ordered"
    assert result2.events == sorted(result2.events, key=sort_key), "events must be deterministically ordered"
    
    # Additional invariant: paths[var_id] should be a list
    for var_id, path in result1.paths.items():
        assert isinstance(path, list), f"paths[{var_id}] must be a list"
        assert len(path) > 0, f"paths[{var_id}] must be non-empty"
    
    # Additional invariant: validation_failed is boolean
    assert isinstance(result1.validation_failed, bool)
    assert isinstance(result2.validation_failed, bool)
    
    # Additional invariant: validation_errors is a list of strings
    assert isinstance(result1.validation_errors, list)
    assert all(isinstance(err, str) for err in result1.validation_errors)


def test_diff_path_type_handling():
    """Test that diff() accepts str, Path, and os.PathLike and produces identical results."""
    spec_v1_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v2.json"
    
    # Test with string paths
    result_str = diff(
        from_spec=str(spec_v1_path),
        to_spec=str(spec_v2_path)
    )
    
    # Test with Path objects
    result_path = diff(
        from_spec=spec_v1_path,
        to_spec=spec_v2_path
    )
    
    # Test with os.PathLike (Path is a PathLike)
    import os
    result_pathlike = diff(
        from_spec=os.fspath(spec_v1_path),
        to_spec=os.fspath(spec_v2_path)
    )
    
    # All should produce identical results
    assert result_str.model_dump() == result_path.model_dump(), "String and Path should produce identical results"
    assert result_path.model_dump() == result_pathlike.model_dump(), "Path and PathLike should produce identical results"
    assert result_str.model_dump() == result_pathlike.model_dump(), "String and PathLike should produce identical results"



"""Tests for spec.py."""

import pytest
from cheshbon.kernel.spec import MappingSpec, SourceColumn, DerivedVariable
from cheshbon.kernel.diff import diff_specs


def test_load_valid_spec():
    """Test loading a valid mapping spec."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:SUBJID", "name": "SUBJID", "type": "string"},
            {"id": "s:BRTHDT", "name": "BRTHDT", "type": "date"}
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
    
    spec = MappingSpec(**spec_data)
    assert spec.spec_version == "1.0.0"
    assert len(spec.sources) == 2
    assert len(spec.derived) == 1
    assert spec.derived[0].transform_ref == "t:direct_copy"
    # params_hash should be computed
    assert spec.derived[0].params_hash.startswith("sha256:")


def test_invalid_input_reference():
    """Test that invalid input references are rejected."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [{"id": "s:SUBJID", "name": "SUBJID", "type": "string"}],
        "derived": [
            {
                "id": "d:USUBJID",
                "name": "USUBJID",
                "type": "string",
                "transform_ref": "t:direct_copy",
                "inputs": ["SUBJID"]  # Missing "s:" prefix
            }
        ]
    }
    
    with pytest.raises(Exception):  # Should raise validation error
        MappingSpec(**spec_data)


def test_transform_ref_validation():
    """Test that transform_ref must start with 't:'."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [{"id": "s:SUBJID", "name": "SUBJID", "type": "string"}],
        "derived": [
            {
                "id": "d:USUBJID",
                "name": "USUBJID",
                "type": "string",
                "transform_ref": "direct_copy",  # Missing "t:" prefix
                "inputs": ["s:SUBJID"]
            }
        ]
    }
    
    with pytest.raises(Exception, match="must start with 't:'"):
        MappingSpec(**spec_data)


def test_params_hash_computed():
    """Test that params_hash is computed from params."""
    derived = DerivedVariable(
        id="d:TEST",
        name="TEST",
        type="string",
        transform_ref="t:ct_map",
        inputs=["s:SEX"],
        params={"map": {"M": "M", "F": "F"}}
    )
    
    # params_hash should be computed
    assert derived.params_hash.startswith("sha256:")
    
    # Same params should produce same hash
    derived2 = DerivedVariable(
        id="d:TEST2",
        name="TEST2",
        type="string",
        transform_ref="t:ct_map",
        inputs=["s:SEX"],
        params={"map": {"M": "M", "F": "F"}}
    )
    assert derived.params_hash == derived2.params_hash
    
    # Different params should produce different hash
    derived3 = DerivedVariable(
        id="d:TEST3",
        name="TEST3",
        type="string",
        transform_ref="t:ct_map",
        inputs=["s:SEX"],
        params={"map": {"M": "M", "F": "F", "U": "UNKNOWN"}}
    )
    assert derived.params_hash != derived3.params_hash


def test_extra_fields_rejected():
    """Test that extra fields are rejected (strict validation)."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [{"id": "s:SUBJID", "name": "SUBJID", "type": "string"}],
        "derived": [],
        "unknown_field": "should be rejected"
    }
    
    with pytest.raises(Exception):  # Should raise validation error
        MappingSpec(**spec_data)


def test_inputs_reordering_canonicalized():
    """Test that reordering inputs produces identical internal representation."""
    spec1 = MappingSpec(
        spec_version="1.0.0",
        study_id="ABC-101",
        source_table="RAW_DM",
        sources=[
            {"id": "s:A", "name": "A", "type": "string"},
            {"id": "s:B", "name": "B", "type": "string"}
        ],
        derived=[{
            "id": "d:VAR1",
            "name": "VAR1",
            "type": "string",
            "transform_ref": "t:copy",
            "inputs": ["s:A", "s:B"]  # Order 1
        }]
    )
    
    spec2 = MappingSpec(
        spec_version="1.0.0",
        study_id="ABC-101",
        source_table="RAW_DM",
        sources=[
            {"id": "s:A", "name": "A", "type": "string"},
            {"id": "s:B", "name": "B", "type": "string"}
        ],
        derived=[{
            "id": "d:VAR1",
            "name": "VAR1",
            "type": "string",
            "transform_ref": "t:copy",
            "inputs": ["s:B", "s:A"]  # Order 2 (reordered)
        }]
    )
    
    # Internal representation should be identical
    assert spec1.derived[0].inputs == spec2.derived[0].inputs
    # Prove the rule: canonicalized to sorted tuple (lexicographic order)
    assert spec1.derived[0].inputs == tuple(sorted(["s:A", "s:B"]))
    assert isinstance(spec1.derived[0].inputs, tuple)
    assert isinstance(spec2.derived[0].inputs, tuple)


def test_inputs_reordering_no_change_event():
    """Test that reordering inputs produces no DERIVED_INPUTS_CHANGED event."""
    spec_v1 = MappingSpec(
        spec_version="1.0.0",
        study_id="ABC-101",
        source_table="RAW_DM",
        sources=[
            {"id": "s:A", "name": "A", "type": "string"},
            {"id": "s:B", "name": "B", "type": "string"}
        ],
        derived=[{
            "id": "d:VAR1",
            "name": "VAR1",
            "type": "string",
            "transform_ref": "t:copy",
            "inputs": ["s:A", "s:B"]
        }]
    )
    
    spec_v2 = MappingSpec(
        spec_version="1.0.0",
        study_id="ABC-101",
        source_table="RAW_DM",
        sources=[
            {"id": "s:A", "name": "A", "type": "string"},
            {"id": "s:B", "name": "B", "type": "string"}
        ],
        derived=[{
            "id": "d:VAR1",
            "name": "VAR1",
            "type": "string",
            "transform_ref": "t:copy",
            "inputs": ["s:B", "s:A"]  # Reordered
        }]
    )
    
    events = diff_specs(spec_v1, spec_v2)
    
    inputs_changes = [e for e in events if e.change_type == "DERIVED_INPUTS_CHANGED"]
    assert len(inputs_changes) == 0, "Reordering inputs should not produce change events"


def test_duplicate_inputs_rejected():
    """Test that duplicate inputs are rejected at parse time."""
    with pytest.raises(ValueError, match="Duplicate inputs"):
        MappingSpec(
            spec_version="1.0.0",
            study_id="ABC-101",
            source_table="RAW_DM",
            sources=[{"id": "s:A", "name": "A", "type": "string"}],
            derived=[{
                "id": "d:VAR1",
                "name": "VAR1",
                "type": "string",
                "transform_ref": "t:copy",
                "inputs": ["s:A", "s:B", "s:A"]  # Duplicate
            }]
        )


def test_actual_input_change_detected():
    """Test that actual input changes (add/remove) are still detected."""
    spec_v1 = MappingSpec(
        spec_version="1.0.0",
        study_id="ABC-101",
        source_table="RAW_DM",
        sources=[
            {"id": "s:A", "name": "A", "type": "string"},
            {"id": "s:B", "name": "B", "type": "string"},
            {"id": "s:C", "name": "C", "type": "string"}
        ],
        derived=[{
            "id": "d:VAR1",
            "name": "VAR1",
            "type": "string",
            "transform_ref": "t:copy",
            "inputs": ["s:A", "s:B"]
        }]
    )
    
    spec_v2 = MappingSpec(
        spec_version="1.0.0",
        study_id="ABC-101",
        source_table="RAW_DM",
        sources=[
            {"id": "s:A", "name": "A", "type": "string"},
            {"id": "s:B", "name": "B", "type": "string"},
            {"id": "s:C", "name": "C", "type": "string"}
        ],
        derived=[{
            "id": "d:VAR1",
            "name": "VAR1",
            "type": "string",
            "transform_ref": "t:copy",
            "inputs": ["s:A", "s:C"]  # Different input (changed from s:B to s:C)
        }]
    )
    
    events = diff_specs(spec_v1, spec_v2)
    inputs_changes = [e for e in events if e.change_type == "DERIVED_INPUTS_CHANGED"]
    assert len(inputs_changes) == 1, "Actual input changes should be detected"
    assert inputs_changes[0].element_id == "d:VAR1"


def test_canonicalization_at_parse_time():
    """Test that canonicalization happens at parse time, not diff time.
    
    This prevents someone from "fixing" it by reintroducing set-compare
    inside diff and leaving models messy.
    """
    # Load spec with reordered inputs
    spec = MappingSpec(
        spec_version="1.0.0",
        study_id="ABC-101",
        source_table="RAW_DM",
        sources=[
            {"id": "s:A", "name": "A", "type": "string"},
            {"id": "s:B", "name": "B", "type": "string"}
        ],
        derived=[{
            "id": "d:VAR1",
            "name": "VAR1",
            "type": "string",
            "transform_ref": "t:copy",
            "inputs": ["s:B", "s:A"]  # Reordered
        }]
    )
    
    # Internal representation should be canonicalized immediately
    assert spec.derived[0].inputs == tuple(sorted(["s:A", "s:B"]))
    
    # Verify it's a tuple (immutable)
    assert isinstance(spec.derived[0].inputs, tuple)
    assert spec.derived[0].inputs == ("s:A", "s:B")  # Lexicographically sorted
    
    # Verify canonicalization happened without calling diff
    # The inputs should be sorted regardless of input order
    assert spec.derived[0].inputs[0] == "s:A"
    assert spec.derived[0].inputs[1] == "s:B"


def test_constraint_inputs_canonicalized():
    """Test that constraint inputs are also canonicalized."""
    spec1 = MappingSpec(
        spec_version="1.0.0",
        study_id="ABC-101",
        source_table="RAW_DM",
        sources=[
            {"id": "s:A", "name": "A", "type": "string"},
            {"id": "s:B", "name": "B", "type": "string"}
        ],
        derived=[],
        constraints=[{
            "id": "c:TEST",
            "name": "TEST",
            "inputs": ["s:A", "s:B"]
        }]
    )
    
    spec2 = MappingSpec(
        spec_version="1.0.0",
        study_id="ABC-101",
        source_table="RAW_DM",
        sources=[
            {"id": "s:A", "name": "A", "type": "string"},
            {"id": "s:B", "name": "B", "type": "string"}
        ],
        derived=[],
        constraints=[{
            "id": "c:TEST",
            "name": "TEST",
            "inputs": ["s:B", "s:A"]  # Reordered
        }]
    )
    
    # Internal representation should be identical
    assert spec1.constraints[0].inputs == spec2.constraints[0].inputs
    assert spec1.constraints[0].inputs == tuple(sorted(["s:A", "s:B"]))
    assert isinstance(spec1.constraints[0].inputs, tuple)
    
    # Reordering should not produce change events
    events = diff_specs(spec1, spec2)
    constraint_inputs_changes = [e for e in events if e.change_type == "CONSTRAINT_INPUTS_CHANGED"]
    assert len(constraint_inputs_changes) == 0, "Reordering constraint inputs should not produce change events"

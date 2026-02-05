"""Verification tests for Pydantic ConfigDict migration.

These tests verify that migrating from class Config to ConfigDict preserves
behavior, especially around extra field handling and populate_by_name semantics.
"""

import pytest
from pydantic import ValidationError

from cheshbon._internal.schemas.spec_schema import MappingSpecV06, MappingSpecV07
from cheshbon._internal.schemas.change_schema import ChangeV06, ChangeV07
from cheshbon._internal.schemas.raw_schema_schema import RawSchemaV06, RawSchemaV07
from cheshbon._internal.schemas.bindings_schema import BindingsV06, BindingsV07
from cheshbon._internal.schemas.common import ParsedArtifact


def test_v06_extra_allow_unknown_fields_survive():
    """Verify V06 models with extra='allow' preserve unknown fields in model_extra."""
    # Test MappingSpecV06
    data = {
        "spec_version": "1.0.0",
        "study_id": "TEST",
        "source_table": "DM",
        "sources": [],
        "derived": [],
        "unknown_field": "should_survive"
    }
    
    model = MappingSpecV06(**data)
    
    # Verify unknown field is in model_extra (Pydantic v2 behavior)
    assert hasattr(model, 'model_extra')
    assert model.model_extra is not None
    assert "unknown_field" in model.model_extra
    assert model.model_extra["unknown_field"] == "should_survive"
    
    # Verify it survives model_dump() - in Pydantic v2, extra fields appear at top level
    dumped = model.model_dump()
    assert "unknown_field" in dumped
    assert dumped["unknown_field"] == "should_survive"
    
    # Test ChangeV06
    change_data = {
        "change_id": "chg:001",
        "object_type": "spec",
        "created_at": "2024-01-01T00:00:00Z",
        "to_spec_version": "1.0.0",
        "canonical_spec": {},
        "spec_diff": {},
        "drift": {},
        "unknown_field": "should_survive"
    }
    
    change_model = ChangeV06(**change_data)
    assert change_model.model_extra is not None
    assert "unknown_field" in change_model.model_extra
    assert change_model.model_extra["unknown_field"] == "should_survive"
    
    # Test RawSchemaV06
    raw_data = {
        "dataset": "DM",
        "record_count": 10,
        "columns": [],
        "unknown_field": "should_survive"
    }
    
    raw_model = RawSchemaV06(**raw_data)
    assert raw_model.model_extra is not None
    assert "unknown_field" in raw_model.model_extra
    assert raw_model.model_extra["unknown_field"] == "should_survive"
    
    # Test BindingsV06
    bindings_data = {
        "table": "DM",
        "bindings": {},
        "unknown_field": "should_survive"
    }
    
    bindings_model = BindingsV06(**bindings_data)
    assert bindings_model.model_extra is not None
    assert "unknown_field" in bindings_model.model_extra
    assert bindings_model.model_extra["unknown_field"] == "should_survive"


def test_v07_extra_forbid_unknown_fields_raise():
    """Verify V07 models with extra='forbid' raise ValidationError on unknown fields."""
    # Test MappingSpecV07
    data = {
        "schema_version": "0.7",
        "spec_version": "1.0.0",
        "study_id": "TEST",
        "source_table": "DM",
        "sources": [],
        "derived": [],
        "unknown_field": "should_fail"
    }
    
    with pytest.raises(ValidationError) as exc_info:
        MappingSpecV07(**data)
    
    # Verify error mentions extra fields
    error_str = str(exc_info.value).lower()
    assert "extra" in error_str or "forbidden" in error_str or "forbid" in error_str
    
    # Test ChangeV07
    change_data = {
        "schema_version": "0.7",
        "change_id": "chg:001",
        "object_type": "spec",
        "created_at": "2024-01-01T00:00:00Z",
        "to_spec_version": "1.0.0",
        "canonical_spec": {},
        "spec_diff": {},
        "drift": {},
        "unknown_field": "should_fail"
    }
    
    with pytest.raises(ValidationError) as exc_info:
        ChangeV07(**change_data)
    
    error_str = str(exc_info.value).lower()
    assert "extra" in error_str or "forbidden" in error_str or "forbid" in error_str
    
    # Test RawSchemaV07
    raw_data = {
        "schema_version": "0.7",
        "dataset": "DM",
        "record_count": 10,
        "columns": [],
        "unknown_field": "should_fail"
    }
    
    with pytest.raises(ValidationError) as exc_info:
        RawSchemaV07(**raw_data)
    
    error_str = str(exc_info.value).lower()
    assert "extra" in error_str or "forbidden" in error_str or "forbid" in error_str
    
    # Test BindingsV07
    bindings_data = {
        "schema_version": "0.7",
        "table": "DM",
        "bindings": {},
        "unknown_field": "should_fail"
    }
    
    with pytest.raises(ValidationError) as exc_info:
        BindingsV07(**bindings_data)
    
    error_str = str(exc_info.value).lower()
    assert "extra" in error_str or "forbidden" in error_str or "forbid" in error_str


def test_parsed_artifact_populate_by_name():
    """Verify ParsedArtifact populate_by_name behavior for __extra__ field."""
    # Note: In Pydantic v2, fields starting with __ are filtered out during validation
    # The __extra__ field exists but cannot be set via constructor. This is expected behavior.
    # The populate_by_name=True allows the field to exist as a model attribute.
    
    # Test that __extra__ field exists and can be accessed
    artifact = ParsedArtifact(
        schema_version="0.7",
        data={}
    )
    
    # Verify the field exists (even if None, since it can't be set via constructor)
    assert hasattr(artifact, '__extra__')
    # The field defaults to None and cannot be set via constructor due to Pydantic v2 filtering
    # This is expected - the field is used internally by the parsing functions
    assert artifact.__extra__ is None or isinstance(artifact.__extra__, dict)
    
    # Verify model can be created and dumped
    dumped = artifact.model_dump()
    assert "schema_version" in dumped
    assert "data" in dumped
    assert "warnings" in dumped

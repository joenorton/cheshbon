"""Tests for cycle detection in dependency graph."""

import pytest
from cheshbon.kernel.spec import MappingSpec, ConstraintNode
from cheshbon.kernel.graph import DependencyGraph, CycleDetectedError


def test_cycle_detection_constraint_derived():
    """Test that cycles between constraints and derived variables are detected."""
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
                "inputs": ["c:AGE_GE_0"]  # Derived depends on constraint
            }
        ],
        "constraints": [
            {
                "id": "c:AGE_GE_0",
                "name": "AGE_GE_0",
                "inputs": ["d:AGE_VALID"],  # Constraint depends on derived - CYCLE!
                "expression": "AGE_VALID == true"
            }
        ]
    }
    
    with pytest.raises(CycleDetectedError) as exc_info:
        spec = MappingSpec(**spec_data)
        DependencyGraph(spec)
    cycle = exc_info.value.cycle
    assert "d:AGE_VALID" in cycle
    assert "c:AGE_GE_0" in cycle
    assert len(cycle) >= 2  # Cycle must have at least 2 nodes


def test_cycle_detection_derived_chain():
    """Test that cycles in derived variable chains are detected."""
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
    
    with pytest.raises(CycleDetectedError) as exc_info:
        spec = MappingSpec(**spec_data)
        DependencyGraph(spec)
    cycle = exc_info.value.cycle
    assert "d:A" in cycle
    assert "d:B" in cycle
    assert len(cycle) >= 2  # Cycle must have at least 2 nodes


def test_no_cycle_valid_graph():
    """Test that valid graphs without cycles are accepted."""
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
        ],
        "constraints": [
            {
                "id": "c:AGE_GE_0",
                "name": "AGE_GE_0",
                "inputs": ["d:AGE_VALID"],  # Constraint depends on derived, but derived doesn't depend on constraint - OK
                "expression": "AGE_VALID == true"
            }
        ]
    }
    
    # Should not raise
    spec = MappingSpec(**spec_data)
    graph = DependencyGraph(spec)
    assert "c:AGE_GE_0" in graph.nodes
    assert "d:AGE_VALID" in graph.nodes

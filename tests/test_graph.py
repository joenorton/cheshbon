"""Tests for graph.py."""

import pytest
from cheshbon.kernel.spec import MappingSpec
from cheshbon.kernel.graph import DependencyGraph


def test_build_graph():
    """Test building dependency graph."""
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
            },
            {
                "id": "d:AGE",
                "name": "AGE",
                "type": "int",
                "transform_ref": "t:age_calc",
                "inputs": ["s:BRTHDT"]
            }
        ]
    }
    
    spec = MappingSpec(**spec_data)
    graph = DependencyGraph(spec)
    
    assert "s:SUBJID" in graph.nodes
    assert "s:BRTHDT" in graph.nodes
    assert "d:USUBJID" in graph.nodes
    assert "d:AGE" in graph.nodes
    
    assert graph.get_dependencies("d:USUBJID") == {"s:SUBJID"}
    assert graph.get_dependencies("d:AGE") == {"s:BRTHDT"}
    assert graph.get_dependents("s:SUBJID") == {"d:USUBJID"}
    assert graph.get_dependents("s:BRTHDT") == {"d:AGE"}


def test_transitive_dependencies():
    """Test transitive dependency tracking."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:BRTHDT", "name": "BRTHDT", "type": "date"}
        ],
        "derived": [
            {
                "id": "d:AGE",
                "name": "AGE",
                "type": "int",
                "transform_ref": "t:age_calc",
                "inputs": ["s:BRTHDT"]
            },
            {
                "id": "d:AGEGRP",
                "name": "AGEGRP",
                "type": "string",
                "transform_ref": "t:bucket",
                "inputs": ["d:AGE"]
            }
        ]
    }
    
    spec = MappingSpec(**spec_data)
    graph = DependencyGraph(spec)
    
    # AGEGRP depends on AGE, which depends on BRTHDT
    # get_dependents returns direct dependents only
    assert graph.get_dependents("s:BRTHDT") == {"d:AGE"}
    # get_transitive_dependents returns all transitive dependents
    assert graph.get_transitive_dependents("s:BRTHDT") == {"d:AGE", "d:AGEGRP"}
    assert graph.get_transitive_dependents("d:AGE") == {"d:AGEGRP"}


def test_dependency_path():
    """Test finding dependency paths."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:BRTHDT", "name": "BRTHDT", "type": "date"}
        ],
        "derived": [
            {
                "id": "d:AGE",
                "name": "AGE",
                "type": "int",
                "transform_ref": "t:age_calc",
                "inputs": ["s:BRTHDT"]
            },
            {
                "id": "d:AGEGRP",
                "name": "AGEGRP",
                "type": "string",
                "transform_ref": "t:bucket",
                "inputs": ["d:AGE"]
            }
        ]
    }
    
    spec = MappingSpec(**spec_data)
    graph = DependencyGraph(spec)
    
    path = graph.get_dependency_path("s:BRTHDT", "d:AGEGRP")
    assert path == ["s:BRTHDT", "d:AGE", "d:AGEGRP"]

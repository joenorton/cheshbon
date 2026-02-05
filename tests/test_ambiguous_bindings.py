"""Tests for ambiguous binding detection."""

import pytest
from cheshbon.kernel.spec import MappingSpec
from cheshbon.kernel.graph import DependencyGraph
from cheshbon.kernel.impact import ImpactResult
from cheshbon.kernel.bindings import Bindings, check_ambiguous_bindings
from cheshbon.kernel.binding_impact import compute_binding_impact


def test_check_ambiguous_bindings():
    """Test detection of ambiguous bindings."""
    bindings = Bindings(
        table="RAW_DM",
        bindings={
            "RFSTDT": "s:RFSTDTC",
            "RFSTDTC": "s:RFSTDTC",  # Multiple raw columns map to same source ID
            "BRTHDT": "s:BRTHDT"
        }
    )
    
    ambiguous = check_ambiguous_bindings(bindings)
    
    assert "s:RFSTDTC" in ambiguous
    assert set(ambiguous["s:RFSTDTC"]) == {"RFSTDT", "RFSTDTC"}
    assert "s:BRTHDT" not in ambiguous


def test_ambiguous_binding_impact():
    """Test impact from ambiguous bindings."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:BRTHDT", "name": "BRTHDT", "type": "date"},
            {"id": "s:RFSTDTC", "name": "RFSTDTC", "type": "date"}
        ],
        "derived": [
            {
                "id": "d:AGE",
                "name": "AGE",
                "type": "int",
                "transform_ref": "t:age_calc",
                "inputs": ["s:BRTHDT", "s:RFSTDTC"]
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
    
    # Base impact (no spec changes)
    base_impact = ImpactResult(
        impacted=set(),
        unaffected={"d:AGE", "d:AGEGRP"},
        impact_paths={},
        impact_reasons={},
        unresolved_references={},
        missing_bindings={},
        ambiguous_bindings={},
        missing_transform_refs={}
    )
    
    # Bindings with ambiguous mapping: both RFSTDT and RFSTDTC map to s:RFSTDTC
    bindings = Bindings(
        table="RAW_DM",
        bindings={
            "BRTHDT": "s:BRTHDT",
            "RFSTDT": "s:RFSTDTC",
            "RFSTDTC": "s:RFSTDTC"  # Ambiguous: multiple raw columns for same source ID
        }
    )
    
    final_impact = compute_binding_impact(spec, bindings, graph, base_impact)
    
    # AGE should be impacted due to ambiguous binding
    assert "d:AGE" in final_impact.impacted
    assert final_impact.impact_reasons["d:AGE"] == "AMBIGUOUS_BINDING"
    assert "s:RFSTDTC" in final_impact.ambiguous_bindings["d:AGE"]
    
    # AGEGRP should be impacted transitively
    assert "d:AGEGRP" in final_impact.impacted
    assert final_impact.impact_reasons["d:AGEGRP"] == "TRANSITIVE_DEPENDENCY"


def test_ambiguous_binding_takes_precedence_over_missing():
    """Test that ambiguous binding takes precedence over missing binding."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:BRTHDT", "name": "BRTHDT", "type": "date"},
            {"id": "s:RFSTDTC", "name": "RFSTDTC", "type": "date"},
            {"id": "s:OTHER", "name": "OTHER", "type": "string"}
        ],
        "derived": [
            {
                "id": "d:AGE",
                "name": "AGE",
                "type": "int",
                "transform_ref": "t:age_calc",
                "inputs": ["s:BRTHDT", "s:RFSTDTC", "s:OTHER"]
            }
        ]
    }
    
    spec = MappingSpec(**spec_data)
    graph = DependencyGraph(spec)
    
    base_impact = ImpactResult(
        impacted=set(),
        unaffected={"d:AGE"},
        impact_paths={},
        impact_reasons={},
        unresolved_references={},
        missing_bindings={},
        ambiguous_bindings={},
        missing_transform_refs={}
    )
    
    # Bindings: s:RFSTDTC is ambiguous, s:OTHER is missing
    bindings = Bindings(
        table="RAW_DM",
        bindings={
            "BRTHDT": "s:BRTHDT",
            "RFSTDT": "s:RFSTDTC",
            "RFSTDTC": "s:RFSTDTC"  # Ambiguous
            # s:OTHER not bound (missing)
        }
    )
    
    final_impact = compute_binding_impact(spec, bindings, graph, base_impact)
    
    # Should be marked as AMBIGUOUS_BINDING (takes precedence)
    assert "d:AGE" in final_impact.impacted
    assert final_impact.impact_reasons["d:AGE"] == "AMBIGUOUS_BINDING"
    assert "s:RFSTDTC" in final_impact.ambiguous_bindings["d:AGE"]
    # But also track missing binding
    assert "s:OTHER" in final_impact.missing_bindings["d:AGE"]

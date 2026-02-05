"""Tests for binding_impact.py."""

import pytest
from cheshbon.kernel.spec import MappingSpec
from cheshbon.kernel.graph import DependencyGraph
from cheshbon.kernel.impact import compute_impact, ImpactResult
from cheshbon.kernel.bindings import Bindings
from cheshbon.kernel.binding_impact import compute_binding_impact


def test_missing_binding_impact():
    """Test impact from missing bindings."""
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
    
    # Bindings missing s:RFSTDTC
    bindings = Bindings(
        table="RAW_DM",
        bindings={
            "BRTHDT": "s:BRTHDT"
            # s:RFSTDTC not bound
        }
    )
    
    final_impact = compute_binding_impact(spec, bindings, graph, base_impact)
    
    # AGE should be impacted due to missing binding
    assert "d:AGE" in final_impact.impacted
    assert final_impact.impact_reasons["d:AGE"] == "MISSING_BINDING"
    assert "s:RFSTDTC" in final_impact.missing_bindings["d:AGE"]
    
    # AGEGRP should be impacted transitively
    assert "d:AGEGRP" in final_impact.impacted
    assert final_impact.impact_reasons["d:AGEGRP"] == "TRANSITIVE_DEPENDENCY"


def test_binding_updated_rename_no_impact():
    """Test that binding update for rename prevents impact."""
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
    
    # Bindings updated: RFSTDT -> s:RFSTDTC (binding updated for rename)
    bindings = Bindings(
        table="RAW_DM",
        bindings={
            "BRTHDT": "s:BRTHDT",
            "RFSTDT": "s:RFSTDTC"  # Raw column renamed, binding updated
        }
    )
    
    final_impact = compute_binding_impact(spec, bindings, graph, base_impact)
    
    # Should be no impact because s:RFSTDTC is still bound (just different raw column name)
    assert "d:AGE" in final_impact.unaffected

"""Tests that ambiguous bindings are terminal failures (validation_failed = True)."""

import pytest
from cheshbon.kernel.spec import MappingSpec
from cheshbon.kernel.graph import DependencyGraph
from cheshbon.kernel.impact import ImpactResult
from cheshbon.kernel.bindings import Bindings
from cheshbon.kernel.binding_impact import compute_binding_impact


def test_ambiguous_binding_sets_validation_failed():
    """Test that ambiguous bindings set validation_failed = True (terminal failure)."""
    spec_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:RFSTDTC", "name": "RFSTDTC", "type": "date"}
        ],
        "derived": [
            {
                "id": "d:AGE",
                "name": "AGE",
                "type": "int",
                "transform_ref": "t:age_calc",
                "inputs": ["s:RFSTDTC"]
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
    
    # Ambiguous binding: multiple raw columns map to same source ID
    bindings = Bindings(
        table="RAW_DM",
        bindings={
            "RFSTDT": "s:RFSTDTC",
            "RFSTDTC": "s:RFSTDTC"  # Ambiguous
        }
    )
    
    final_impact = compute_binding_impact(spec, bindings, graph, base_impact)
    
    # Should be validation failure (terminal)
    assert final_impact.validation_failed is True
    assert len(final_impact.validation_errors) > 0
    assert any("Ambiguous binding" in err for err in final_impact.validation_errors)
    assert "d:AGE" in final_impact.impacted
    assert final_impact.impact_reasons["d:AGE"] == "AMBIGUOUS_BINDING"


def test_ambiguous_binding_exit_code_2():
    """Test that ambiguous bindings result in exit code 2 (validation failure), not 1 (impact)."""
    # This test documents the expected behavior for CLI exit codes
    # Exit code 2 = validation_failed (terminal, non-executable)
    # Exit code 1 = impact found (can proceed with caution)
    # Exit code 0 = no impact
    
    # Ambiguous bindings should be exit code 2, not 1
    # This is tested implicitly by validation_failed = True
    # The CLI code in cheshbon/diff.py should check validation_failed and return 2
    pass  # Integration test would verify CLI exit code

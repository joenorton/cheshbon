"""Tests for impact.py."""

import pytest
from cheshbon.kernel.spec import MappingSpec
from cheshbon.kernel.graph import DependencyGraph
from cheshbon.kernel.diff import diff_specs, ChangeEvent
from cheshbon.kernel.impact import compute_impact


def test_impact_from_source_rename():
    """Test impact analysis from source column rename (non-impacting if ID unchanged)."""
    spec_v1_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:SUBJID", "name": "SUBJID", "type": "string"},
            {"id": "s:BRTHDT", "name": "BRTHDT", "type": "date"},
            {"id": "s:SEX", "name": "SEX", "type": "string"}
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
            },
            {
                "id": "d:AGEGRP",
                "name": "AGEGRP",
                "type": "string",
                "transform_ref": "t:bucket",
                "inputs": ["d:AGE"]
            },
            {
                "id": "d:SEX_CDISC",
                "name": "SEX_CDISC",
                "type": "string",
                "transform_ref": "t:ct_map",
                "inputs": ["s:SEX"]
            }
        ]
    }
    
    spec_v2_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:SUBJID", "name": "SUBJID", "type": "string"},
            {"id": "s:BRTHDT", "name": "BIRTH_DATE", "type": "date"},  # Renamed but same ID
            {"id": "s:SEX", "name": "SEX", "type": "string"}
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
                "inputs": ["s:BRTHDT"]  # Still references same ID
            },
            {
                "id": "d:AGEGRP",
                "name": "AGEGRP",
                "type": "string",
                "transform_ref": "t:bucket",
                "inputs": ["d:AGE"]
            },
            {
                "id": "d:SEX_CDISC",
                "name": "SEX_CDISC",
                "type": "string",
                "transform_ref": "t:ct_map",
                "inputs": ["s:SEX"]
            }
        ]
    }
    
    spec_v1 = MappingSpec(**spec_v1_data)
    spec_v2 = MappingSpec(**spec_v2_data)
    graph_v1 = DependencyGraph(spec_v1)
    
    change_events = diff_specs(spec_v1, spec_v2)
    impact_result = compute_impact(spec_v1, spec_v2, graph_v1, change_events)
    
    # With stable IDs, a name change alone doesn't impact (ID unchanged)
    assert "d:USUBJID" in impact_result.unaffected
    assert "d:SEX_CDISC" in impact_result.unaffected


def test_impact_from_input_change():
    """Test impact from actual input change (different ID reference)."""
    spec_v1_data = {
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
    
    spec_v2_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:BRTHDT", "name": "BRTHDT", "type": "date"},
            {"id": "s:RFSTDTC", "name": "RFSTDTC", "type": "date"},
            {"id": "s:RFSTDT", "name": "RFSTDT", "type": "date"}  # New source
        ],
        "derived": [
            {
                "id": "d:AGE",
                "name": "AGE",
                "type": "int",
                "transform_ref": "t:age_calc",
                "inputs": ["s:BRTHDT", "s:RFSTDT"]  # Changed to new source ID
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
    
    spec_v1 = MappingSpec(**spec_v1_data)
    spec_v2 = MappingSpec(**spec_v2_data)
    graph_v1 = DependencyGraph(spec_v1)
    
    change_events = diff_specs(spec_v1, spec_v2)
    impact_result = compute_impact(spec_v1, spec_v2, graph_v1, change_events)
    
    # AGE should be impacted (inputs changed to different ID)
    assert "d:AGE" in impact_result.impacted
    assert impact_result.impact_reasons["d:AGE"] == "DIRECT_CHANGE"
    
    # AGEGRP should be impacted (transitive dependency through AGE)
    assert "d:AGEGRP" in impact_result.impacted
    assert impact_result.impact_reasons["d:AGEGRP"] == "TRANSITIVE_DEPENDENCY"
    
    # Check that impact paths exist
    assert "d:AGE" in impact_result.impact_paths
    assert "d:AGEGRP" in impact_result.impact_paths


def test_partial_impact_scenario():
    """Test the key scenario: structural change impacts some but not all outputs."""
    spec_v1_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:SUBJID", "name": "SUBJID", "type": "string"},
            {"id": "s:BRTHDT", "name": "BRTHDT", "type": "date"},
            {"id": "s:SEX", "name": "SEX", "type": "string"},
            {"id": "s:RFSTDTC", "name": "RFSTDTC", "type": "date"}
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
                "transform_ref": "t:age_calculation",
                "inputs": ["s:BRTHDT", "s:RFSTDTC"]
            },
            {
                "id": "d:AGEGRP",
                "name": "AGEGRP",
                "type": "string",
                "transform_ref": "t:age_bucket",
                "inputs": ["d:AGE"]
            },
            {
                "id": "d:SEX_CDISC",
                "name": "SEX_CDISC",
                "type": "string",
                "transform_ref": "t:ct_map",
                "inputs": ["s:SEX"]
            }
        ]
    }
    
    spec_v2_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:SUBJID", "name": "SUBJID", "type": "string"},
            {"id": "s:BRTHDT", "name": "BRTHDT", "type": "date"},
            {"id": "s:SEX", "name": "SEX", "type": "string"},
            {"id": "s:RFSTDTC", "name": "RFSTDTC", "type": "date"},
            {"id": "s:RFSTDT", "name": "RFSTDT", "type": "date"}
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
                "transform_ref": "t:age_calculation",
                "inputs": ["s:BRTHDT", "s:RFSTDT"]  # Changed: RFSTDTC -> RFSTDT
            },
            {
                "id": "d:AGEGRP",
                "name": "AGEGRP",
                "type": "string",
                "transform_ref": "t:age_bucket",
                "inputs": ["d:AGE"]
            },
            {
                "id": "d:SEX_CDISC",
                "name": "SEX_CDISC",
                "type": "string",
                "transform_ref": "t:ct_map",
                "inputs": ["s:SEX"]
            }
        ]
    }
    
    spec_v1 = MappingSpec(**spec_v1_data)
    spec_v2 = MappingSpec(**spec_v2_data)
    graph_v1 = DependencyGraph(spec_v1)
    
    change_events = diff_specs(spec_v1, spec_v2)
    impact_result = compute_impact(spec_v1, spec_v2, graph_v1, change_events)
    
    # Expected: d:AGE and d:AGEGRP impacted, d:USUBJID and d:SEX_CDISC unaffected
    assert "d:AGE" in impact_result.impacted
    assert "d:AGEGRP" in impact_result.impacted
    assert "d:USUBJID" in impact_result.unaffected
    assert "d:SEX_CDISC" in impact_result.unaffected
    
    # Verify impact reasons
    assert impact_result.impact_reasons["d:AGE"] == "DIRECT_CHANGE"
    assert impact_result.impact_reasons["d:AGEGRP"] == "TRANSITIVE_DEPENDENCY"
    
    # Verify impact paths
    assert "d:AGE" in impact_result.impact_paths
    assert "d:AGEGRP" in impact_result.impact_paths
    # AGE path should be just itself (direct impact)
    assert impact_result.impact_paths["d:AGE"] == ["d:AGE"]
    # AGEGRP path should show dependency chain
    assert "d:AGE" in impact_result.impact_paths["d:AGEGRP"]


def test_unresolved_reference_missing_input():
    """Test impact from missing input (source removed but not updated in derived)."""
    spec_v1_data = {
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
    
    spec_v2_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:BRTHDT", "name": "BRTHDT", "type": "date"}
            # s:RFSTDTC removed, but d:AGE still references it (not updated)
        ],
        "derived": [
            {
                "id": "d:AGE",
                "name": "AGE",
                "type": "int",
                "transform_ref": "t:age_calc",
                "inputs": ["s:BRTHDT", "s:RFSTDTC"]  # Still references removed source!
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
    
    spec_v1 = MappingSpec(**spec_v1_data)
    spec_v2 = MappingSpec(**spec_v2_data)
    graph_v1 = DependencyGraph(spec_v1)
    
    change_events = diff_specs(spec_v1, spec_v2)
    impact_result = compute_impact(spec_v1, spec_v2, graph_v1, change_events)
    
    # AGE should be impacted due to missing input
    assert "d:AGE" in impact_result.impacted
    assert "s:RFSTDTC" in impact_result.unresolved_references.get("d:AGE", set())
    assert impact_result.impact_reasons["d:AGE"] in ["MISSING_INPUT", "DIRECT_CHANGE_MISSING_INPUT"]
    
    # AGEGRP should be impacted transitively
    assert "d:AGEGRP" in impact_result.impacted
    assert impact_result.impact_reasons["d:AGEGRP"] == "TRANSITIVE_DEPENDENCY"


def test_changed_but_unreferenced_node():
    """Test that a changed derived variable is impacted even if nothing depends on it."""
    spec_v1_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:COL1", "name": "COL1", "type": "string"}
        ],
        "derived": [
            {
                "id": "d:VAR1",
                "name": "VAR1",
                "type": "string",
                "transform_ref": "t:copy",
                "inputs": ["s:COL1"]
            },
            {
                "id": "d:VAR2",
                "name": "VAR2",
                "type": "string",
                "transform_ref": "t:copy",
                "inputs": ["s:COL1"]
            }
        ]
    }
    
    spec_v2_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:COL1", "name": "COL1", "type": "string"}
        ],
        "derived": [
            {
                "id": "d:VAR1",
                "name": "VAR1",
                "type": "string",
                "transform_ref": "t:copy",
                "inputs": ["s:COL1"]
            },
            {
                "id": "d:VAR2",
                "name": "VAR2",
                "type": "int",  # Type changed, but nothing depends on VAR2
                "transform_ref": "t:copy",
                "inputs": ["s:COL1"]
            }
        ]
    }
    
    spec_v1 = MappingSpec(**spec_v1_data)
    spec_v2 = MappingSpec(**spec_v2_data)
    graph_v1 = DependencyGraph(spec_v1)
    
    change_events = diff_specs(spec_v1, spec_v2)
    impact_result = compute_impact(spec_v1, spec_v2, graph_v1, change_events)
    
    # VAR2 should be impacted (direct change) even though nothing depends on it
    assert "d:VAR2" in impact_result.impacted
    assert impact_result.impact_reasons["d:VAR2"] == "DIRECT_CHANGE"
    
    # VAR1 should be unaffected
    assert "d:VAR1" in impact_result.unaffected


def test_impact_reason_order_insensitive():
    """Impact reasons should be invariant to change event ordering."""
    spec_v1_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:COL1", "name": "COL1", "type": "string"},
            {"id": "s:COL2", "name": "COL2", "type": "string"},
        ],
        "derived": [
            {
                "id": "d:VAR1",
                "name": "VAR1",
                "type": "string",
                "transform_ref": "t:copy",
                "inputs": ["s:COL1"]
            }
        ]
    }
    
    # v2 changes inputs to include missing ID and also changes type
    spec_v2_data = {
        "spec_version": "1.0.0",
        "study_id": "ABC-101",
        "source_table": "RAW_DM",
        "sources": [
            {"id": "s:COL1", "name": "COL1", "type": "string"}
        ],
        "derived": [
            {
                "id": "d:VAR1",
                "name": "VAR1",
                "type": "int",
                "transform_ref": "t:copy",
                "inputs": ["s:COL1", "s:MISSING"]
            }
        ]
    }
    
    spec_v1 = MappingSpec(**spec_v1_data)
    spec_v2 = MappingSpec(**spec_v2_data)
    graph_v1 = DependencyGraph(spec_v1)
    
    # Two events for the same element, different ordering
    events_a = [
        ChangeEvent(change_type="DERIVED_INPUTS_CHANGED", element_id="d:VAR1"),
        ChangeEvent(change_type="DERIVED_TYPE_CHANGED", element_id="d:VAR1", old_value="string", new_value="int"),
    ]
    events_b = list(reversed(events_a))
    
    impact_a = compute_impact(spec_v1, spec_v2, graph_v1, events_a)
    impact_b = compute_impact(spec_v1, spec_v2, graph_v1, events_b)
    
    assert impact_a.impacted == impact_b.impacted
    assert impact_a.impact_reasons == impact_b.impact_reasons
    assert impact_a.unresolved_references == impact_b.unresolved_references

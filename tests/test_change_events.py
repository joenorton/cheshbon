"""Test change event ontology for the current amendment scenario."""

from pathlib import Path
from cheshbon.kernel.spec import MappingSpec
from cheshbon.kernel.diff import diff_specs


def test_current_amendment_change_events():
    """Show the normalized change events for the current amendment (v1 -> v2)."""
    # Use path-relative paths for cwd-independence
    HERE = Path(__file__).resolve().parent
    FIXTURES = HERE.parent / "fixtures"
    
    spec_v1 = MappingSpec.model_validate_json((FIXTURES / "mapping_spec_v1.json").read_text())
    spec_v2 = MappingSpec.model_validate_json((FIXTURES / "mapping_spec_v2.json").read_text())
    
    events = diff_specs(spec_v1, spec_v2)
    
    # Expected events for the current amendment:
    # 1. SOURCE_ADDED: s:RFSTDT
    # 2. DERIVED_INPUTS_CHANGED: d:AGE (inputs changed from [s:BRTHDT, s:RFSTDTC] to [s:BRTHDT, s:RFSTDT])
    
    event_types = [e.change_type for e in events]
    assert "SOURCE_ADDED" in event_types
    assert "DERIVED_INPUTS_CHANGED" in event_types
    
    # Verify the details
    source_added = [e for e in events if e.change_type == "SOURCE_ADDED"][0]
    assert source_added.element_id == "s:RFSTDT"
    assert source_added.new_value == "RFSTDT"
    
    inputs_changed = [e for e in events if e.change_type == "DERIVED_INPUTS_CHANGED"][0]
    assert inputs_changed.element_id == "d:AGE"
    assert "s:RFSTDTC" in inputs_changed.details["old_inputs"]
    assert "s:RFSTDT" in inputs_changed.details["new_inputs"]
    
    # Print for validation
    print("\n=== Change Events for Current Amendment ===")
    for event in events:
        print(f"{event.change_type}: {event.element_id}")
        if event.old_value:
            print(f"  Old: {event.old_value}")
        if event.new_value:
            print(f"  New: {event.new_value}")
        if event.details:
            print(f"  Details: {event.details}")
        print()

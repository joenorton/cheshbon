"""Binding layer: connects raw schema columns to stable source IDs."""

from typing import Dict, List, Literal
from dataclasses import dataclass
from .spec import MappingSpec


@dataclass
class RawColumn:
    """A raw column from an extract."""
    name: str
    type: str


@dataclass
class RawSchema:
    """Raw schema snapshot from an extract."""
    table: str
    columns: List[RawColumn]
    
    def get_column_names(self) -> set[str]:
        """Get set of all column names."""
        return {c.name for c in self.columns}


@dataclass
class Bindings:
    """Bindings from raw column names to stable source IDs."""
    table: str
    bindings: Dict[str, str]  # raw_column_name -> source_id (e.g., "RFSTDT" -> "s:RFSTDTC")
    
    def get_bound_source_ids(self) -> set[str]:
        """Get set of all source IDs that are bound."""
        return set(self.bindings.values())
    
    def get_raw_column_for_source(self, source_id: str) -> str | None:
        """Get raw column name for a source ID, or None if not bound."""
        for raw_name, bound_id in self.bindings.items():
            if bound_id == source_id:
                return raw_name
        return None


@dataclass
class BindingEvent:
    """A binding-related event."""
    event_type: Literal[
        "RAW_COLUMN_ADDED",
        "RAW_COLUMN_REMOVED", 
        "RAW_COLUMN_RENAMED",
        "BINDING_ADDED",
        "BINDING_REMOVED",
        "BINDING_CHANGED",
        "BINDING_INVALID"  # Binding references column not in schema
    ]
    element: str  # Column name or source ID
    old_value: str | None = None
    new_value: str | None = None
    details: dict | None = None


def validate_bindings(schema: RawSchema, bindings: Bindings) -> tuple[List[BindingEvent], set[str]]:
    """
    Validate bindings against raw schema.
    Returns:
        - List of binding events (invalidations, etc.)
        - Set of unmapped raw column names (informational)
    """
    events: List[BindingEvent] = []
    
    schema_columns = schema.get_column_names()
    bound_columns = set(bindings.bindings.keys())
    
    # Check for bindings that reference columns not in schema
    for raw_col, source_id in bindings.bindings.items():
        if raw_col not in schema_columns:
            events.append(BindingEvent(
                event_type="BINDING_INVALID",
                element=source_id,
                old_value=raw_col,
                new_value=None,
                details={"reason": f"Raw column '{raw_col}' not found in schema"}
            ))
    
    # Check for schema columns that are unbound (informational)
    unmapped_columns = schema_columns - bound_columns
    
    return events, unmapped_columns


def check_missing_bindings(
    spec: MappingSpec,
    bindings: Bindings
) -> Dict[str, set[str]]:
    """
    Check which source IDs required by derived variables are missing from bindings.
    
    Returns:
        Dict mapping derived variable ID -> set of missing source IDs
    """
    missing: Dict[str, set[str]] = {}
    bound_source_ids = bindings.get_bound_source_ids()
    
    for derived in spec.derived:
        required_source_ids = {inp for inp in derived.inputs if inp.startswith("s:")}
        missing_sources = required_source_ids - bound_source_ids
        if missing_sources:
            missing[derived.id] = missing_sources
    
    return missing


def check_ambiguous_bindings(bindings: Bindings) -> Dict[str, List[str]]:
    """
    Check for ambiguous bindings: multiple raw columns mapping to the same source ID.
    
    This is a distinct failure mode from missing bindings. An ambiguous binding means
    the system cannot determine which raw column should be used for a source ID.
    
    Returns:
        Dict mapping source_id -> List[raw_column_names] for ambiguous cases.
        Empty dict if no ambiguities exist.
        Raw column lists are sorted for stable, reproducible reporting.
    """
    ambiguous: Dict[str, List[str]] = {}
    
    # Build reverse mapping: source_id -> set of raw columns (using set for deterministic detection)
    source_to_raw_columns: Dict[str, set[str]] = {}
    for raw_col, source_id in bindings.bindings.items():
        if source_id not in source_to_raw_columns:
            source_to_raw_columns[source_id] = set()
        source_to_raw_columns[source_id].add(raw_col)
    
    # Find source IDs with multiple raw columns
    # Sort source_ids and raw_columns for stable, reproducible output
    for source_id in sorted(source_to_raw_columns.keys()):
        raw_columns = source_to_raw_columns[source_id]
        if len(raw_columns) > 1:
            # Sort raw columns for stable reporting (not dependent on dict iteration order)
            ambiguous[source_id] = sorted(raw_columns)
    
    return ambiguous


def diff_bindings(bindings_v1: Bindings, bindings_v2: Bindings) -> List[BindingEvent]:
    """
    Compute diff between two binding versions.
    Returns list of binding events.
    """
    events: List[BindingEvent] = []
    
    bindings_v1_dict = bindings_v1.bindings
    bindings_v2_dict = bindings_v2.bindings
    
    v1_keys = set(bindings_v1_dict.keys())
    v2_keys = set(bindings_v2_dict.keys())
    
    # Removed bindings
    for raw_col in v1_keys - v2_keys:
        source_id = bindings_v1_dict[raw_col]
        events.append(BindingEvent(
            event_type="BINDING_REMOVED",
            element=source_id,
            old_value=raw_col,
            new_value=None
        ))
    
    # Added bindings
    for raw_col in v2_keys - v1_keys:
        source_id = bindings_v2_dict[raw_col]
        events.append(BindingEvent(
            event_type="BINDING_ADDED",
            element=source_id,
            old_value=None,
            new_value=raw_col
        ))
    
    # Changed bindings (same source ID, different raw column name = rename)
    for raw_col in v1_keys & v2_keys:
        source_id_v1 = bindings_v1_dict[raw_col]
        source_id_v2 = bindings_v2_dict[raw_col]
        
        if source_id_v1 != source_id_v2:
            # Source ID changed (different binding)
            events.append(BindingEvent(
                event_type="BINDING_CHANGED",
                element=source_id_v1,
                old_value=raw_col,
                new_value=raw_col,
                details={"old_source_id": source_id_v1, "new_source_id": source_id_v2}
            ))
        # If source IDs match but raw column name is different, that's handled by raw schema diff
    
    # Check for source ID renames (same raw column, different source ID)
    # This is the key case: raw column renamed, binding updated to keep same source ID
    v1_by_source = {sid: col for col, sid in bindings_v1_dict.items()}
    v2_by_source = {sid: col for col, sid in bindings_v2_dict.items()}
    
    for source_id in set(v1_by_source.keys()) & set(v2_by_source.keys()):
        raw_col_v1 = v1_by_source[source_id]
        raw_col_v2 = v2_by_source[source_id]
        if raw_col_v1 != raw_col_v2:
            # Same source ID, different raw column = column rename with binding update
            events.append(BindingEvent(
                event_type="RAW_COLUMN_RENAMED",
                element=source_id,
                old_value=raw_col_v1,
                new_value=raw_col_v2,
                details={"source_id": source_id}
            ))
    
    return events

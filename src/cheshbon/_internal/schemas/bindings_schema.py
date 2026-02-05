"""Parser for bindings artifacts (v0.6 legacy, v0.7)."""

from typing import Dict, Any, Set
from pydantic import BaseModel, Field, ValidationError, ConfigDict

from .common import ParsedArtifact


# v0.7 schema model
class BindingsV07(BaseModel):
    """v0.7 bindings schema."""
    schema_version: str = "0.7"
    table: str
    bindings: Dict[str, str]  # raw_column_name -> source_id
    
    model_config = ConfigDict(extra="forbid")  # Reject unknown top-level fields in v0.7


# v0.6 legacy schema model (missing schema_version)
class BindingsV06(BaseModel):
    """v0.6 legacy bindings schema (missing schema_version)."""
    table: str
    bindings: Dict[str, str]  # raw_column_name -> source_id
    
    model_config = ConfigDict(extra="allow")  # Allow unknown fields in legacy (will be captured)


def parse_bindings(obj: Dict[str, Any], unknown_fields: str = "preserve") -> ParsedArtifact:
    """
    Parse bindings with compatibility layer.
    
    Args:
        obj: Raw bindings dict (from JSON)
        unknown_fields: "preserve" or "reject"
        
    Returns:
        ParsedArtifact with normalized in-memory view
        
    Raises:
        ValueError: If unknown_fields="reject" and unknown top-level fields present,
                    or if unsupported schema_version
    """
    warnings = []
    extra_fields = {}
    
    # Determine found version
    found_version = obj.get("schema_version")
    if found_version is None:
        found_version = "missing"
        warnings.append("Missing schema_version, treating as legacy v0.6")
    
    # Check for unsupported versions
    if found_version not in ["missing", "0.6", "0.7"]:
        raise ValueError(f"Unsupported schema_version: {found_version}")
    
    # Try v0.7 first if schema_version is "0.7"
    if found_version == "0.7":
        # Get known fields for v0.7
        known_fields_v07: Set[str] = set(BindingsV07.model_fields.keys())
        
        # Check for unknown top-level fields (top-level only, not recursive)
        unknown_top_level = [k for k in obj.keys() if k not in known_fields_v07]
        if unknown_top_level:
            if unknown_fields == "reject":
                raise ValueError(f"Unknown top-level fields in v0.7 bindings: {unknown_top_level}")
            # preserve mode: extract unknown fields
            for key in unknown_top_level:
                extra_fields[key] = obj[key]
        
        # Parse with known fields only
        known_obj = {k: v for k, v in obj.items() if k in known_fields_v07}
        try:
            model = BindingsV07(**known_obj)
            normalized_data = model.model_dump(exclude={"schema_version"})
            return ParsedArtifact(
                schema_version="0.7",
                data=normalized_data,
                __extra__=extra_fields if extra_fields else None,
                warnings=warnings + ([f"Unknown top-level fields preserved: {list(extra_fields.keys())}"] if extra_fields else [])
            )
        except ValidationError as e:
            raise ValueError(f"Invalid v0.7 bindings structure: {e}")
    
    # Legacy v0.6 parsing (missing or "0.6")
    # Remove schema_version if present (for "0.6" case)
    legacy_obj = {k: v for k, v in obj.items() if k != "schema_version"}
    
    # Get known fields for v0.6
    known_fields_v06: Set[str] = set(BindingsV06.model_fields.keys())
    
    # Check for unknown top-level fields in legacy (top-level only, not recursive)
    unknown_top_level = [k for k in legacy_obj.keys() if k not in known_fields_v06]
    if unknown_top_level:
        if unknown_fields == "reject":
            raise ValueError(f"Unknown top-level field in legacy bindings: {unknown_top_level}")
        # preserve mode: extract unknown fields
        for key in unknown_top_level:
            extra_fields[key] = legacy_obj[key]
    
    # Parse with v0.6 model
    try:
        model = BindingsV06(**legacy_obj)
        normalized_data = model.model_dump()
        
        return ParsedArtifact(
            schema_version="0.7",  # Normalized in parsed result
            data=normalized_data,
            __extra__=extra_fields if extra_fields else None,
            warnings=warnings + ([f"Unknown top-level fields preserved: {list(extra_fields.keys())}"] if extra_fields else [])
        )
    except ValidationError as e:
        raise ValueError(f"Invalid legacy bindings structure: {e}")

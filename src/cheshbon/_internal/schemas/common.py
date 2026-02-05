"""Common types for schema parsing."""

from typing import Dict, List, Any, Optional
from pydantic import BaseModel, ConfigDict


class ParsedArtifact(BaseModel):
    """Result of parsing an artifact with compatibility layer.
    
    This represents a normalized in-memory view. The original bytes
    are never rewritten to disk.
    """
    schema_version: str  # Normalized version (e.g., "0.7") in parsed result, NOT in stored bytes
    data: Dict[str, Any]  # Normalized data object
    __extra__: Optional[Dict[str, Any]] = None  # Unknown top-level fields if preserved (only in parsed result, never written back)
    warnings: List[str] = []  # Warnings from parsing (e.g., missing schema_version)
    
    model_config = ConfigDict(populate_by_name=True)  # Allow __extra__ as a field name

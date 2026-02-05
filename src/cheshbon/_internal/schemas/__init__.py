"""Schema parsers for v0.6 (legacy) and v0.7 artifacts.

This module provides parsers that produce normalized in-memory views
of artifacts. Evidence bytes are never rewritten during import.
"""

from .common import ParsedArtifact
from .spec_schema import parse_spec
from .change_schema import parse_change
from .raw_schema_schema import parse_raw_schema
from .bindings_schema import parse_bindings

__all__ = [
    "ParsedArtifact",
    "parse_spec",
    "parse_change",
    "parse_raw_schema",
    "parse_bindings",
]

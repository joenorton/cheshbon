"""Validation code constants for cheshbon.api.validate().

These constants prevent stringly-typed error codes and ensure
client code uses the correct validation codes.
"""

from enum import Enum


class ValidationCode(str, Enum):
    """Validation error and warning codes."""
    
    # Errors (blocking)
    INVALID_STRUCTURE = "INVALID_STRUCTURE"
    DUPLICATE_ID = "DUPLICATE_ID"
    MISSING_INPUT = "MISSING_INPUT"
    CYCLE_DETECTED = "CYCLE_DETECTED"
    MISSING_TRANSFORM_REF = "MISSING_TRANSFORM_REF"
    DEPENDENCY_GRAPH_ERROR = "DEPENDENCY_GRAPH_ERROR"
    REGISTRY_LOAD_ERROR = "REGISTRY_LOAD_ERROR"
    
    # SANS Ingestion Errors
    HASH_MISMATCH = "HASH_MISMATCH"
    REGISTRY_INDEX_INCOMPLETE = "REGISTRY_INDEX_INCOMPLETE"
    STEP_ID_CONFLICT = "STEP_ID_CONFLICT"
    TRANSFORM_NOT_FOUND = "TRANSFORM_NOT_FOUND"
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    
    # Warnings (non-blocking)
    MISSING_BINDING = "MISSING_BINDING"
    AMBIGUOUS_BINDING = "AMBIGUOUS_BINDING"
    INVALID_RAW_COLUMN = "INVALID_RAW_COLUMN"
    BINDINGS_LOAD_ERROR = "BINDINGS_LOAD_ERROR"
    RAW_SCHEMA_LOAD_ERROR = "RAW_SCHEMA_LOAD_ERROR"
    PARAMS_LARGE = "PARAMS_LARGE"

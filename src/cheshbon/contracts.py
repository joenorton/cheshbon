"""Public compatibility models for cheshbon package."""

from typing import List
from pydantic import BaseModel


class CompatibilityIssue(BaseModel):
    """A compatibility issue found during import."""
    object_type: str  # "change" | "spec" | "raw_schema" | "bindings" | "manifest"
    path: str
    found_version: str  # "0.6" | "missing" | "0.7" | "0.8" | ... (any version string found)
    required_version: str  # "0.7"
    action: str  # "accept" | "migrate" | "reject"
    reason: str  # "missing_schema_version" | "legacy_schema_version" | "unsupported_schema_version" | "unknown_fields_present" | ...


class CompatibilityReport(BaseModel):
    """Compatibility report for artifact import."""
    ok: bool
    mode: str  # "permissive" | "strict"
    unknown_fields: str  # "preserve" | "reject"
    issues: List[CompatibilityIssue]  # sorted
    warnings: List[str]  # sorted

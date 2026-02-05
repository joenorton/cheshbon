"""Normalized run record models."""

from typing import Dict, Optional
from pydantic import BaseModel, Field, ConfigDict


class RunRecordV0(BaseModel):
    """Deterministic record of a specific execution."""
    format: str = "cheshbon.run"
    version: str = "0.1"
    run_id: str
    fingerprint: str  # sha256 of canonical json over plan, steps, and IO hashes
    witnesses: Dict[str, str]  # filename -> sha256
    created_at: str  # ISO 8601

    model_config = ConfigDict(extra="forbid")

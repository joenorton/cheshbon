"""Transform registry: versioned transform definitions with stable fingerprints."""

from typing import Dict, List, Literal, Optional, Union, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
from pydantic import BaseModel, Field, field_validator, ConfigDict, model_validator
from .hash_utils import hash_schema


class StrongTransformEntry(BaseModel):
    """A strong transform entry that preserves semantics (spec)."""
    transform_id: str
    kind: str
    version: str = "0.1"
    spec: Dict[str, Any]
    io_signature: Optional[Dict[str, Any]] = None
    impl_fingerprint: Optional[str] = None

    model_config = ConfigDict(extra="ignore")


class StrongTransformRegistry(BaseModel):
    """A strong transform registry emitted by cheshbon."""
    format: str = "cheshbon.registry"
    version: str = "0.1"
    transforms: List[StrongTransformEntry]

    model_config = ConfigDict(extra="forbid")


class Signature(BaseModel):
    """Transform signature with explicit input/output types.
    
    - inputs: List of input type strings
    - output: Single output type string (not a list)
    - Future: If multi-output needed, add 'outputs' field (mutually exclusive with 'output')
    """
    inputs: List[str] = Field(..., description="List of input type strings")
    output: str = Field(..., description="Single output type string")
    
    model_config = {"extra": "forbid"}


class ImplFingerprint(BaseModel):
    """Structured fingerprint for transform implementation.
    
    Enables sane explanations ("impl changed: file X digest changed")
    instead of just a hash.
    """
    algo: Literal["sha256"] = "sha256"
    source: Literal["builtin", "external_sas", "external_py", "template_sas", "file", "git"]
    ref: str  # Path, module name, git ref, etc.
    digest: str  # SHA256 hash (without prefix)


class TransformHistory(BaseModel):
    """Append-only history entry for a transform.
    
    Tracks immutable snapshots of transform state over time.
    This is legal armor: "this value changed because the derivation changed on date X,
    not because data drifted."
    
    Frozen to prevent accidental mutation of audit trail.
    """
    timestamp: str = Field(..., description="ISO 8601 timestamp when this history entry was created")
    impl_fingerprint: ImplFingerprint = Field(..., description="Snapshot of impl_fingerprint at this time")
    params_schema_hash: Optional[str] = Field(
        None,
        description="Snapshot of params_schema_hash at this time (prefixed with 'sha256:' or null)"
    )
    change_reason: Optional[str] = Field(
        None,
        description="Optional reason for this change (e.g., 'bug fix', 'performance improvement')"
    )
    
    model_config = ConfigDict(frozen=True, extra="forbid")
    
    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        """Validate timestamp is ISO 8601 format."""
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
        except ValueError:
            raise ValueError(f"timestamp must be ISO 8601 format, got '{v}'")
        return v


class TransformEntry(BaseModel):
    """A transform registry entry."""
    id: str  # t:-prefixed, e.g., "t:ct_map"
    version: str  # Semver, e.g., "1.0.0" (informational only)
    kind: Literal["builtin", "external_sas", "external_py", "template_sas"]
    signature: Signature = Field(
        ...,
        description="Transform signature with inputs (list) and output (single type string)"
    )
    params_schema_hash: Optional[str] = Field(
        None,
        description="SHA256 hash of JSON schema for params (prefixed with 'sha256:'), or null for no-params transforms"
    )
    impl_fingerprint: ImplFingerprint
    history: Tuple[TransformHistory, ...] = Field(
        default_factory=tuple,
        description="Append-only history of transform changes (immutable snapshots with timestamps). "
                    "Only modify via add_history_entry() which returns a new TransformEntry."
    )
    
    @model_validator(mode='after')
    def validate_history_immutable(self):
        """History is append-only - entries can only be added, never removed or modified.
        
        This validator ensures history is a tuple (immutable by type) and that
        TransformHistory entries are frozen (cannot be mutated).
        """
        # Tuple type enforces immutability at the container level
        # Frozen TransformHistory entries enforce immutability at the entry level
        # This makes accidental corruption impossible
        return self

    @field_validator('id')
    @classmethod
    def validate_id(cls, v: str) -> str:
        """Validate transform ID starts with 't:' and is globally unique format."""
        if not v.startswith('t:'):
            raise ValueError(f"Transform ID '{v}' must start with 't:' (e.g., 't:ct_map')")
        # Enforce lowercase with underscores (no aliases, no case-insensitive matching)
        if v != v.lower():
            raise ValueError(
                f"Transform ID '{v}' must be lowercase. "
                f"Use lowercase with underscores (e.g., 't:ct_map', not 't:CT_MAP')"
            )
        return v

    @field_validator('params_schema_hash')
    @classmethod
    def validate_params_schema_hash(cls, v: Optional[str]) -> Optional[str]:
        """Validate params_schema_hash format.
        
        If null, transform takes no params. Otherwise must be valid sha256 hash.
        """
        if v is None:
            return None
        if not v.startswith('sha256:'):
            raise ValueError(f"params_schema_hash must start with 'sha256:', got '{v}'")
        if len(v) != 71:  # "sha256:" + 64 hex chars
            raise ValueError(f"params_schema_hash must be 71 characters (sha256: + 64 hex), got {len(v)}")
        return v


    @field_validator('impl_fingerprint', mode='after')
    @classmethod
    def validate_impl_fingerprint(cls, v: ImplFingerprint) -> ImplFingerprint:
        """Validate impl_fingerprint.ref and digest.
        
        - ref is treated as an opaque identifier (no OS/path semantics in kernel)
        - Digest must be 64 hex characters (sha256)
        """
        
        # Validate digest is 64 hex chars (sha256)
        if len(v.digest) != 64 or not all(c in '0123456789abcdef' for c in v.digest.lower()):
            raise ValueError(
                f"impl_fingerprint.digest must be 64 hex characters (sha256), got '{v.digest}' "
                f"(length: {len(v.digest)})"
            )
        
        return v

    def get_impl_hash(self) -> str:
        """Get single hash from impl_fingerprint (backward compatibility)."""
        return f"sha256:{self.impl_fingerprint.digest}"
    
    def add_history_entry(self, timestamp: str, change_reason: Optional[str] = None) -> 'TransformEntry':
        """Append a new history entry with current state.
        
        Creates an immutable snapshot of the transform's current state
        (impl_fingerprint, params_schema_hash) with a timestamp.
        
        Returns a NEW TransformEntry with the appended history (persistent data style).
        The original entry is unchanged (immutability enforced).
        
        Args:
            timestamp: ISO 8601 timestamp string
            change_reason: Optional reason for this change (e.g., 'bug fix', 'performance improvement')
        
        Returns:
            New TransformEntry with history appended
        """
        history_entry = TransformHistory(
            timestamp=timestamp,
            impl_fingerprint=self.impl_fingerprint.model_copy(),
            params_schema_hash=self.params_schema_hash,
            change_reason=change_reason
        )
        # Create new entry with appended history (tuple is immutable, so we create new tuple)
        new_history = self.history + (history_entry,)
        # Return new TransformEntry (persistent data style - original unchanged)
        return self.model_copy(update={'history': new_history})
    
    def get_history_for_transform(self) -> Tuple[TransformHistory, ...]:
        """Get all history entries for this transform (append-only, never deleted).
        
        Returns:
            Tuple of TransformHistory entries in chronological order (oldest first)
        """
        return self.history  # Tuple is already immutable


class TransformRegistry(BaseModel):
    """Transform registry containing all available transforms."""
    registry_version: str
    transforms: List[TransformEntry]

    model_config = {"extra": "forbid"}

    def __init__(self, **data):
        super().__init__(**data)
        # Validate that all transform IDs are unique (no aliases)
        ids = [t.id for t in self.transforms]
        if len(ids) != len(set(ids)):
            duplicates = [id for id in ids if ids.count(id) > 1]
            raise ValueError(
                f"Duplicate transform IDs found: {duplicates}. "
                f"Transform IDs must be globally unique within a project."
            )

    def get_transform(self, transform_ref: str) -> Optional[TransformEntry]:
        """Get transform entry by reference.
        
        Args:
            transform_ref: Transform reference (must start with 't:')
        
        Returns:
            TransformEntry if found, None otherwise
        """
        if not transform_ref.startswith('t:'):
            return None
        
        for transform in self.transforms:
            if transform.id == transform_ref:
                return transform
        return None

    def has_transform(self, transform_ref: str) -> bool:
        """Check if transform exists in registry."""
        return self.get_transform(transform_ref) is not None

    def get_all_ids(self) -> List[str]:
        """Get list of all transform IDs."""
        return [t.id for t in self.transforms]

    @classmethod
    def from_json_bytes(cls, data: bytes) -> "TransformRegistry":
        """Load transform registry from JSON bytes (pure, no I/O)."""
        import json
        payload = json.loads(data)
        return cls(**payload)

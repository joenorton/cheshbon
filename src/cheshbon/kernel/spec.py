"""Pydantic models for mapping_spec with strict validation."""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator, computed_field
import json


class SourceColumn(BaseModel):
    """A source column definition."""
    id: str  # Stable identifier, e.g., "s:BRTHDT"
    name: str
    type: str  # string, int, float, date, datetime, bool


class ConstraintNode(BaseModel):
    """A constraint node: derived node with boolean output.
    
    Constraints are just derived nodes with boolean outputs.
    Examples: AGE >= 0, RFSTDTC present, CT complete.
    
    Once modeled this way, they fall naturally into the same graph,
    the same diff system, the same impact logic.
    """
    id: str  # Stable identifier, e.g., "c:AGE_GE_0"
    name: str
    inputs: tuple[str, ...] = Field(..., description="Canonicalized tuple of source/derived IDs (sorted, no duplicates)")
    expression: Optional[str] = Field(None, description="Constraint expression (stubbed for now, execution not implemented)")
    notes: Optional[str] = None
    
    @field_validator('id')
    @classmethod
    def validate_id(cls, v: str) -> str:
        """Validate constraint ID starts with 'c:' prefix."""
        if not v.startswith('c:'):
            raise ValueError(f"Constraint ID '{v}' must start with 'c:' (e.g., 'c:AGE_GE_0')")
        return v
    
    @field_validator('inputs')
    @classmethod
    def validate_inputs(cls, v: List[str]) -> tuple[str, ...]:
        """Validate and canonicalize input references.
        
        Rules:
        - Must be properly formatted IDs: s: (source), d: (derived), v: (vars), c: (constraint)
          Constraints can depend on sources, derived vars, or other constraints.
        - No duplicates allowed (validation error)
        - Canonicalized to sorted tuple for order-agnostic semantics
        - Canonical order is lexicographic on the ID string (deterministic)
        """
        if not isinstance(v, list):
            v = list(v)
        
        # Validate format - constraints can depend on sources, derived vars, or other constraints
        for inp in v:
            if not (inp.startswith('s:') or inp.startswith('d:') or inp.startswith('v:') or inp.startswith('c:')):
                raise ValueError(
                    f"Input '{inp}' must start with 's:' (source), 'd:' (derived), 'v:' (vars), or 'c:' (constraint)"
                )
        
        # Check for duplicates (linear O(n) algorithm)
        seen = set()
        duplicates = set()
        for inp in v:
            if inp in seen:
                duplicates.add(inp)
            seen.add(inp)
        
        if duplicates:
            # Error message with stable-sorted duplicates for determinism
            raise ValueError(f"Duplicate inputs not allowed: {sorted(duplicates)}")
        
        # Canonicalize: convert to sorted tuple (lexicographic order on ID string)
        return tuple(sorted(v))


class DerivedVariable(BaseModel):
    """A derived variable definition."""
    id: str  # Stable identifier, e.g., "d:AGE"
    name: str
    type: str  # string, int, float, date, datetime, bool
    transform_ref: str  # Transform reference, must start with "t:", e.g., "t:ct_map"
    inputs: tuple[str, ...] = Field(..., description="Canonicalized tuple of source/derived IDs (sorted, no duplicates)")
    params: Optional[Dict[str, Any]] = None  # Transform-specific parameters
    notes: Optional[str] = None

    @field_validator('transform_ref')
    @classmethod
    def validate_transform_ref(cls, v: str) -> str:
        """Validate transform_ref starts with 't:' prefix."""
        if not v.startswith('t:'):
            raise ValueError(f"Transform reference '{v}' must start with 't:' (e.g., 't:ct_map')")
        return v

    @field_validator('inputs')
    @classmethod
    def validate_inputs(cls, v: List[str]) -> tuple[str, ...]:
        """Validate and canonicalize input references.
        
        Rules:
        - Must be properly formatted IDs: s: (source), d: (derived), v: (vars), c: (constraint)
          Derived variables can depend on sources, other derived vars, or constraints.
        - No duplicates allowed (validation error)
        - Canonicalized to sorted tuple for order-agnostic semantics
        - Canonical order is lexicographic on the ID string (deterministic)
        """
        if not isinstance(v, list):
            v = list(v)
        
        # Validate format - derived can depend on sources, derived vars, or constraints
        for inp in v:
            if not (inp.startswith('s:') or inp.startswith('d:') or inp.startswith('v:') or inp.startswith('c:')):
                raise ValueError(
                    f"Input '{inp}' must start with 's:' (source), 'd:' (derived), 'v:' (vars), or 'c:' (constraint)"
                )
        
        # Check for duplicates (linear O(n) algorithm)
        seen = set()
        duplicates = set()
        for inp in v:
            if inp in seen:
                duplicates.add(inp)
            seen.add(inp)
        
        if duplicates:
            # Error message with stable-sorted duplicates for determinism
            raise ValueError(f"Duplicate inputs not allowed: {sorted(duplicates)}")
        
        # Canonicalize: convert to sorted tuple (lexicographic order on ID string)
        return tuple(sorted(v))

    @field_validator('params')
    @classmethod
    def validate_params(cls, v: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Validate params are pure JSON, small, and schema-governed.
        
        Discipline check: params must be:
        - Pure JSON (no floats, no non-JSON types) - **enforced**
        - Small (hard limit: 50KB when serialized) - **enforced**
        - Schema-governed (params_schema_hash in registry enforces shape) - **tracked**
        
        This prevents params from becoming a junk drawer.
        """
        if v is None:
            return v
        
        # Validate JSON types (hash_utils will catch floats and non-JSON types)
        from .hash_utils import canonicalize_json
        try:
            canonical_str = canonicalize_json(v)
            # Hard limit: 50KB measured on canonical JSON string bytes (enforced)
            # Using canonical JSON ensures deterministic measurement across environments
            if len(canonical_str) > 50000:
                raise ValueError(
                    f"Params exceed size limit (50KB). Got {len(canonical_str)} bytes (canonical JSON). "
                    f"Params must be small and schema-governed. Consider refactoring."
                )
        except ValueError:
            raise  # Re-raise ValueError (size limit)
        except Exception as e:
            raise ValueError(f"Params validation failed: {e}")
        
        return v

    @computed_field
    @property
    def params_hash(self) -> str:
        """Compute params hash at load time (kernel-internal, not persisted).
        
        This is computed using hash_utils.hash_params() and is never stored in the spec.
        """
        from .hash_utils import hash_params
        return hash_params(self.params)


class MappingSpec(BaseModel):
    """A mapping specification."""
    spec_version: str
    study_id: str
    source_table: str
    sources: List[SourceColumn]
    derived: List[DerivedVariable]
    constraints: Optional[List[ConstraintNode]] = Field(
        default_factory=list,
        description="Constraint nodes: derived nodes with boolean outputs (first-class graph nodes)"
    )
    review: Optional[dict] = None  # Metadata only - non-impacting

    model_config = {"extra": "forbid"}  # No unknown fields allowed

    def get_source_ids(self) -> set[str]:
        """Get set of all source column IDs."""
        return {s.id for s in self.sources}

    def get_derived_ids(self) -> set[str]:
        """Get set of all derived variable IDs."""
        return {d.id for d in self.derived}
    
    def get_constraint_ids(self) -> set[str]:
        """Get set of all constraint node IDs."""
        return {c.id for c in (self.constraints or [])}

    def get_all_ids(self) -> set[str]:
        """Get set of all variable IDs (sources + derived + constraints)."""
        return self.get_source_ids() | self.get_derived_ids() | self.get_constraint_ids()
    
    def get_source_by_id(self, id: str) -> SourceColumn | None:
        """Get source column by ID."""
        for s in self.sources:
            if s.id == id:
                return s
        return None
    
    def get_derived_by_id(self, id: str) -> DerivedVariable | None:
        """Get derived variable by ID."""
        for d in self.derived:
            if d.id == id:
                return d
        return None
    
    def get_constraint_by_id(self, id: str) -> ConstraintNode | None:
        """Get constraint node by ID."""
        for c in (self.constraints or []):
            if c.id == id:
                return c
        return None

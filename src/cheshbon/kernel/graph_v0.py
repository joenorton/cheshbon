"""Serializable lineage graph models (v0.2)."""

from typing import List, Optional
from pydantic import BaseModel, ConfigDict


class NodeEvidence(BaseModel):
    """Execution-time evidence for a graph node."""
    path: str
    sha256: str
    row_count: Optional[int] = None
    columns: Optional[List[str]] = None
    sorted_by: Optional[List[str]] = None

    model_config = ConfigDict(extra="ignore")


class NodeV0(BaseModel):
    """A logical node instance in the lineage graph."""
    id: str  # artifact:<name> | table:<logical_name>
    name: str
    evidence: Optional[NodeEvidence] = None

    model_config = ConfigDict(extra="ignore")


class EdgeV0(BaseModel):
    """An application of a transform (step) in the graph."""
    id: str  # sha256(...)
    transform_id: str
    step_id: str
    op: str
    inputs: List[str]  # node_ids
    outputs: List[str]  # node_ids
    params: Optional[dict] = None

    model_config = ConfigDict(extra="ignore")


class GraphV0(BaseModel):
    """Deterministic lineage graph."""
    format: str = "cheshbon.graph"
    version: str = "0.2"
    nodes: List[NodeV0]
    edges: List[EdgeV0]

    model_config = ConfigDict(extra="forbid")

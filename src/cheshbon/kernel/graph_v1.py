"""Graph v1 schema models and validation."""

from __future__ import annotations

from typing import Annotated, List, Optional, Union, Literal

from pydantic import BaseModel, ConfigDict, Field


class GraphValidationError(ValueError):
    """Raised when a graph fails schema or consistency validation."""


class Producer(BaseModel):
    name: str
    version: str

    model_config = ConfigDict(extra="forbid")


class StepNode(BaseModel):
    id: str
    kind: Literal["step"]
    op: str
    transform_class_id: str
    transform_id: str
    inputs: List[str] = Field(default_factory=list)
    outputs: List[str] = Field(default_factory=list)
    payload_sha256: str

    model_config = ConfigDict(extra="forbid")


class TableNode(BaseModel):
    id: str
    kind: Literal["table"]
    producer: Optional[str] = None
    consumers: List[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


Node = Annotated[Union[StepNode, TableNode], Field(discriminator="kind")]


class Edge(BaseModel):
    src: str
    dst: str
    kind: Literal["produces", "consumes"]

    model_config = ConfigDict(extra="forbid")


class GraphV1(BaseModel):
    schema_version: int
    producer: Producer
    nodes: List[Node]
    edges: List[Edge]

    model_config = ConfigDict(extra="forbid")


def parse_graph_v1(data: dict) -> GraphV1:
    """Parse and validate a graph dict into GraphV1."""
    graph = GraphV1(**data)
    validate_graph_v1(graph)
    return graph


def validate_graph_v1(graph: GraphV1) -> None:
    """Validate GraphV1 schema + consistency rules (raises GraphValidationError)."""
    if graph.schema_version != 1:
        raise GraphValidationError("unsupported schema_version")

    node_by_id: dict[str, Node] = {}
    for node in graph.nodes:
        if node.id in node_by_id:
            raise GraphValidationError(f"duplicate node id: {node.id}")
        node_by_id[node.id] = node

        if isinstance(node, StepNode):
            if not node.id.startswith("s:"):
                raise GraphValidationError(f"step node id must start with 's:': {node.id}")
            _ensure_unique_list(f"step inputs for {node.id}", node.inputs)
            _ensure_unique_list(f"step outputs for {node.id}", node.outputs)
        elif isinstance(node, TableNode):
            if not node.id.startswith("t:"):
                raise GraphValidationError(f"table node id must start with 't:': {node.id}")
            _ensure_unique_list(f"table consumers for {node.id}", node.consumers)

    edges_seen: set[tuple[str, str, str]] = set()
    produces_by_step: dict[str, set[str]] = {}
    produces_by_table: dict[str, set[str]] = {}
    consumes_by_step: dict[str, set[str]] = {}
    consumes_by_table: dict[str, set[str]] = {}

    for edge in graph.edges:
        key = (edge.src, edge.dst, edge.kind)
        if key in edges_seen:
            raise GraphValidationError(f"duplicate edge: {edge.src} -> {edge.dst} ({edge.kind})")
        edges_seen.add(key)

        if edge.src not in node_by_id or edge.dst not in node_by_id:
            raise GraphValidationError(f"edge references missing node: {edge.src} -> {edge.dst} ({edge.kind})")

        src_node = node_by_id[edge.src]
        dst_node = node_by_id[edge.dst]

        if edge.kind == "produces":
            if not isinstance(src_node, StepNode) or not isinstance(dst_node, TableNode):
                raise GraphValidationError(
                    f"produces edge must be step -> table: {edge.src} -> {edge.dst}"
                )
            produces_by_step.setdefault(edge.src, set()).add(edge.dst)
            produces_by_table.setdefault(edge.dst, set()).add(edge.src)
        elif edge.kind == "consumes":
            if not isinstance(src_node, TableNode) or not isinstance(dst_node, StepNode):
                raise GraphValidationError(
                    f"consumes edge must be table -> step: {edge.src} -> {edge.dst}"
                )
            consumes_by_step.setdefault(edge.dst, set()).add(edge.src)
            consumes_by_table.setdefault(edge.src, set()).add(edge.dst)
        else:
            raise GraphValidationError(f"unsupported edge kind: {edge.kind}")

    for node in graph.nodes:
        if isinstance(node, StepNode):
            for table_id in node.inputs:
                _ensure_node_kind(node_by_id, table_id, TableNode, f"step input for {node.id}")
            for table_id in node.outputs:
                _ensure_node_kind(node_by_id, table_id, TableNode, f"step output for {node.id}")

            expected_inputs = consumes_by_step.get(node.id, set())
            if set(node.inputs) != expected_inputs:
                raise GraphValidationError(
                    f"step inputs mismatch for {node.id}: expected {sorted(expected_inputs)}, "
                    f"got {sorted(set(node.inputs))}"
                )

            expected_outputs = produces_by_step.get(node.id, set())
            if set(node.outputs) != expected_outputs:
                raise GraphValidationError(
                    f"step outputs mismatch for {node.id}: expected {sorted(expected_outputs)}, "
                    f"got {sorted(set(node.outputs))}"
                )

        elif isinstance(node, TableNode):
            if node.producer is not None:
                _ensure_node_kind(node_by_id, node.producer, StepNode, f"table producer for {node.id}")
                expected_producers = produces_by_table.get(node.id, set())
                if expected_producers != {node.producer}:
                    raise GraphValidationError(
                        f"table producer mismatch for {node.id}: expected {sorted(expected_producers)}, "
                        f"got {[node.producer]}"
                    )
            else:
                expected_producers = produces_by_table.get(node.id, set())
                if expected_producers:
                    raise GraphValidationError(
                        f"table producer is null but produces edges exist for {node.id}: "
                        f"{sorted(expected_producers)}"
                    )

            for step_id in node.consumers:
                _ensure_node_kind(node_by_id, step_id, StepNode, f"table consumer for {node.id}")

            expected_consumers = consumes_by_table.get(node.id, set())
            if set(node.consumers) != expected_consumers:
                raise GraphValidationError(
                    f"table consumers mismatch for {node.id}: expected {sorted(expected_consumers)}, "
                    f"got {sorted(set(node.consumers))}"
                )


def _ensure_unique_list(label: str, values: List[str]) -> None:
    if len(values) != len(set(values)):
        raise GraphValidationError(f"duplicate entries in {label}")


def _ensure_node_kind(
    node_by_id: dict[str, Node],
    node_id: str,
    expected_type: type,
    label: str,
) -> None:
    if node_id not in node_by_id:
        raise GraphValidationError(f"{label} references missing node: {node_id}")
    if not isinstance(node_by_id[node_id], expected_type):
        raise GraphValidationError(f"{label} must reference {expected_type.__name__}: {node_id}")

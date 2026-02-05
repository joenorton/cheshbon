"""Graph diff and downstream impact (v1 lineage graphs)."""

from __future__ import annotations

from typing import Annotated, Dict, List, Optional, Set, Tuple, Literal

from pydantic import BaseModel, ConfigDict, Field

from cheshbon._internal.canonical_json import canonical_dumps

from cheshbon.kernel.graph_v1 import GraphV1, StepNode, TableNode


class EdgeKey(BaseModel):
    src: str
    dst: str
    kind: Literal["produces", "consumes"]

    model_config = ConfigDict(extra="forbid")


class ChangedStepNode(BaseModel):
    id: str
    op: str
    from_payload_sha256: str
    to_payload_sha256: str

    model_config = ConfigDict(extra="forbid")


class StepSummary(BaseModel):
    id: str
    transform_id: str
    payload_sha256: str

    model_config = ConfigDict(extra="forbid")


class StepNodeRecord(BaseModel):
    id: str
    op: str
    transform_class_id: str
    transform_id: str
    payload_sha256: str

    model_config = ConfigDict(extra="forbid")

class ModifiedByClass(BaseModel):
    transform_class_id: str
    op: str
    from_steps: List[StepSummary]
    to_steps: List[StepSummary]
    classification: Literal["param_value_change", "rewire_only", "structural_change"]

    model_config = ConfigDict(extra="forbid")


class GraphDiffEventStepAdded(BaseModel):
    type: Literal["step_added"]
    step_id: str
    transform_class_id: str
    op: str

    model_config = ConfigDict(extra="forbid")


class GraphDiffEventStepRemoved(BaseModel):
    type: Literal["step_removed"]
    step_id: str
    transform_class_id: str
    op: str

    model_config = ConfigDict(extra="forbid")


class GraphDiffEventStepPayloadChanged(BaseModel):
    type: Literal["step_payload_changed"]
    step_id: str
    from_payload_sha256: str
    to_payload_sha256: str

    model_config = ConfigDict(extra="forbid")


class GraphDiffEventStepRewired(BaseModel):
    type: Literal["step_rewired"]
    step_id: str
    inputs_added: List[str]
    inputs_removed: List[str]
    outputs_added: List[str]
    outputs_removed: List[str]

    model_config = ConfigDict(extra="forbid")


class GraphDiffEventStepReplacedSameClass(BaseModel):
    type: Literal["step_replaced_same_class"]
    transform_class_id: str
    op: str
    classification: Literal["param_value_change", "rewire_only", "structural_change"]
    from_steps: List[StepSummary]
    to_steps: List[StepSummary]

    model_config = ConfigDict(extra="forbid")


GraphDiffEvent = Annotated[
    GraphDiffEventStepAdded
    | GraphDiffEventStepRemoved
    | GraphDiffEventStepPayloadChanged
    | GraphDiffEventStepRewired
    | GraphDiffEventStepReplacedSameClass,
    Field(discriminator="type"),
]


class GraphDiffCountsRaw(BaseModel):
    added_nodes: int
    removed_nodes: int
    added_steps: int
    removed_steps: int
    changed_step_nodes: int
    modified_by_class: int
    param_value_changes: int
    rewire_only: int
    added_edges: int
    removed_edges: int

    model_config = ConfigDict(extra="forbid")


class GraphDiffCountsEvents(BaseModel):
    total: int
    step_added: int
    step_removed: int
    step_payload_changed: int
    step_rewired: int
    step_replaced_same_class: int

    model_config = ConfigDict(extra="forbid")


class GraphDiffCounts(BaseModel):
    raw: GraphDiffCountsRaw
    events: GraphDiffCountsEvents

    model_config = ConfigDict(extra="forbid")


class GraphDiff(BaseModel):
    graph_a_sha256: Optional[str] = None
    graph_b_sha256: Optional[str] = None
    added_nodes: List[str]
    removed_nodes: List[str]
    added_steps: List[StepNodeRecord]
    removed_steps: List[StepNodeRecord]
    changed_step_nodes: List[ChangedStepNode]
    modified_by_class: List[ModifiedByClass]
    events: List[GraphDiffEvent] = Field(default_factory=list)
    added_edges: List[EdgeKey]
    removed_edges: List[EdgeKey]
    counts: GraphDiffCounts

    model_config = ConfigDict(extra="forbid")


class InducedSubgraph(BaseModel):
    nodes: List[str]
    edges: List[EdgeKey]

    model_config = ConfigDict(extra="forbid")


class SeedReasonChangedStep(BaseModel):
    reason: Literal["changed_step"]

    model_config = ConfigDict(extra="forbid")


class SeedReasonModifiedByClass(BaseModel):
    reason: Literal["modified_by_class"]
    transform_class_id: str
    op: str
    from_steps: List[StepSummary]
    to_steps: List[StepSummary]
    classification: Literal["param_value_change", "rewire_only", "structural_change"]

    model_config = ConfigDict(extra="forbid")


class SeedReasonAddedStep(BaseModel):
    reason: Literal["added_step"]
    transform_id: str
    transform_class_id: str
    op: str

    model_config = ConfigDict(extra="forbid")


class SeedReasonStepRewired(BaseModel):
    reason: Literal["step_rewired"]
    inputs_added: List[str]
    inputs_removed: List[str]
    outputs_added: List[str]
    outputs_removed: List[str]

    model_config = ConfigDict(extra="forbid")


SeedReason = Annotated[
    SeedReasonChangedStep
    | SeedReasonModifiedByClass
    | SeedReasonAddedStep
    | SeedReasonStepRewired,
    Field(discriminator="reason"),
]


class ImpactReasonTransitive(BaseModel):
    reason: Literal["transitive"]
    from_step: str
    via_table: str

    model_config = ConfigDict(extra="forbid")


class ImpactReasonUpstreamRemoved(BaseModel):
    reason: Literal["upstream_removed"]
    removed_step: str

    model_config = ConfigDict(extra="forbid")


ImpactReason = Annotated[
    ImpactReasonTransitive | ImpactReasonUpstreamRemoved,
    Field(discriminator="reason"),
]


class Impact(BaseModel):
    seed_steps: List[str]
    touched_tables: List[str]
    impacted_steps: List[str]
    impacted_tables: List[str]
    reasons: Dict[str, List[ImpactReason]] = Field(default_factory=dict)
    seed_reasons: Dict[str, List[SeedReason]] = Field(default_factory=dict)
    paths: Dict[str, List[str]] = Field(default_factory=dict)
    context: Dict[str, object] = Field(default_factory=dict)
    induced_subgraph: Optional[InducedSubgraph] = None

    model_config = ConfigDict(extra="forbid")


def diff_graph(g1: GraphV1, g2: GraphV1) -> GraphDiff:
    """Compute a structural diff between two GraphV1 instances."""
    nodes1 = {node.id: node for node in g1.nodes}
    nodes2 = {node.id: node for node in g2.nodes}

    ids1 = set(nodes1.keys())
    ids2 = set(nodes2.keys())

    added_nodes = sorted(ids2 - ids1)
    removed_nodes = sorted(ids1 - ids2)

    added_steps = sorted(
        [
            StepNodeRecord(
                id=node.id,
                op=node.op,
                transform_class_id=node.transform_class_id,
                transform_id=node.transform_id,
                payload_sha256=node.payload_sha256,
            )
            for node_id, node in nodes2.items()
            if isinstance(node, StepNode) and node_id not in nodes1
        ],
        key=lambda s: s.id,
    )
    removed_steps = sorted(
        [
            StepNodeRecord(
                id=node.id,
                op=node.op,
                transform_class_id=node.transform_class_id,
                transform_id=node.transform_id,
                payload_sha256=node.payload_sha256,
            )
            for node_id, node in nodes1.items()
            if isinstance(node, StepNode) and node_id not in nodes2
        ],
        key=lambda s: s.id,
    )

    changed_step_nodes: List[ChangedStepNode] = []
    step_ids_1 = {node_id for node_id, node in nodes1.items() if isinstance(node, StepNode)}
    step_ids_2 = {node_id for node_id, node in nodes2.items() if isinstance(node, StepNode)}
    for node_id in sorted(step_ids_1 & step_ids_2):
        n1 = nodes1[node_id]
        n2 = nodes2[node_id]
        if (
            n1.payload_sha256 != n2.payload_sha256
            or n1.transform_id != n2.transform_id
            or n1.op != n2.op
            or n1.transform_class_id != n2.transform_class_id
        ):
            changed_step_nodes.append(
                ChangedStepNode(
                    id=node_id,
                    op=n2.op,
                    from_payload_sha256=n1.payload_sha256,
                    to_payload_sha256=n2.payload_sha256,
                )
            )

    step_rewired_events: List[GraphDiffEvent] = []
    for node_id in sorted(step_ids_1 & step_ids_2):
        n1 = nodes1[node_id]
        n2 = nodes2[node_id]
        inputs_added = sorted(set(n2.inputs) - set(n1.inputs))
        inputs_removed = sorted(set(n1.inputs) - set(n2.inputs))
        outputs_added = sorted(set(n2.outputs) - set(n1.outputs))
        outputs_removed = sorted(set(n1.outputs) - set(n2.outputs))
        if inputs_added or inputs_removed or outputs_added or outputs_removed:
            step_rewired_events.append(
                GraphDiffEventStepRewired(
                    type="step_rewired",
                    step_id=node_id,
                    inputs_added=inputs_added,
                    inputs_removed=inputs_removed,
                    outputs_added=outputs_added,
                    outputs_removed=outputs_removed,
                )
            )

    modified_by_class: List[ModifiedByClass] = []
    steps_by_class_1 = _steps_by_class(nodes1)
    steps_by_class_2 = _steps_by_class(nodes2)

    for transform_class_id, op in sorted(
        set(steps_by_class_1.keys()) & set(steps_by_class_2.keys()),
        key=lambda k: (k[1], k[0]),
    ):
        from_steps = steps_by_class_1[(transform_class_id, op)]
        to_steps = steps_by_class_2[(transform_class_id, op)]

        from_ids = sorted(step.id for step in from_steps)
        to_ids = sorted(step.id for step in to_steps)
        from_transform_ids = sorted(step.transform_id for step in from_steps)
        to_transform_ids = sorted(step.transform_id for step in to_steps)
        from_payloads = sorted(step.payload_sha256 for step in from_steps)
        to_payloads = sorted(step.payload_sha256 for step in to_steps)

        has_id_change = from_ids != to_ids
        has_transform_change = from_transform_ids != to_transform_ids
        has_payload_change = from_payloads != to_payloads

        if not has_id_change:
            continue

        if has_transform_change or has_payload_change:
            classification = "param_value_change"
        else:
            classification = "rewire_only"

        modified_by_class.append(
            ModifiedByClass(
                transform_class_id=transform_class_id,
                op=op,
                from_steps=[
                    StepSummary(
                        id=step.id,
                        transform_id=step.transform_id,
                        payload_sha256=step.payload_sha256,
                    )
                    for step in sorted(from_steps, key=lambda s: s.id)
                ],
                to_steps=[
                    StepSummary(
                        id=step.id,
                        transform_id=step.transform_id,
                        payload_sha256=step.payload_sha256,
                    )
                    for step in sorted(to_steps, key=lambda s: s.id)
                ],
                classification=classification,
            )
        )

    edges1 = {(e.src, e.dst, e.kind) for e in g1.edges}
    edges2 = {(e.src, e.dst, e.kind) for e in g2.edges}

    added_edge_keys = sorted(edges2 - edges1, key=_edge_sort_key)
    removed_edge_keys = sorted(edges1 - edges2, key=_edge_sort_key)

    added_edges = [EdgeKey(src=s, dst=d, kind=k) for (s, d, k) in added_edge_keys]
    removed_edges = [EdgeKey(src=s, dst=d, kind=k) for (s, d, k) in removed_edge_keys]

    covered_from_steps = {
        step.id for modified in modified_by_class for step in modified.from_steps
    }
    covered_to_steps = {
        step.id for modified in modified_by_class for step in modified.to_steps
    }

    events: List[GraphDiffEvent] = []
    for step in added_steps:
        if step.id in covered_to_steps:
            continue
        events.append(
            GraphDiffEventStepAdded(
                type="step_added",
                step_id=step.id,
                transform_class_id=step.transform_class_id,
                op=step.op,
            )
        )
    for step in removed_steps:
        if step.id in covered_from_steps:
            continue
        events.append(
            GraphDiffEventStepRemoved(
                type="step_removed",
                step_id=step.id,
                transform_class_id=step.transform_class_id,
                op=step.op,
            )
        )
    for step in changed_step_nodes:
        events.append(
            GraphDiffEventStepPayloadChanged(
                type="step_payload_changed",
                step_id=step.id,
                from_payload_sha256=step.from_payload_sha256,
                to_payload_sha256=step.to_payload_sha256,
            )
        )
    for modified in modified_by_class:
        events.append(
            GraphDiffEventStepReplacedSameClass(
                type="step_replaced_same_class",
                transform_class_id=modified.transform_class_id,
                op=modified.op,
                classification=modified.classification,
                from_steps=modified.from_steps,
                to_steps=modified.to_steps,
            )
        )
    events.extend(step_rewired_events)
    events = sorted(events, key=_event_sort_key)

    counts = GraphDiffCounts(
        raw=GraphDiffCountsRaw(
            added_nodes=len(added_nodes),
            removed_nodes=len(removed_nodes),
            added_steps=len(added_steps),
            removed_steps=len(removed_steps),
            changed_step_nodes=len(changed_step_nodes),
            modified_by_class=len(modified_by_class),
            param_value_changes=sum(
                1 for item in modified_by_class if item.classification == "param_value_change"
            ),
            rewire_only=sum(1 for item in modified_by_class if item.classification == "rewire_only"),
            added_edges=len(added_edges),
            removed_edges=len(removed_edges),
        ),
        events=GraphDiffCountsEvents(
            total=len(events),
            step_added=sum(1 for event in events if event.type == "step_added"),
            step_removed=sum(1 for event in events if event.type == "step_removed"),
            step_payload_changed=sum(
                1 for event in events if event.type == "step_payload_changed"
            ),
            step_rewired=sum(1 for event in events if event.type == "step_rewired"),
            step_replaced_same_class=sum(
                1 for event in events if event.type == "step_replaced_same_class"
            ),
        ),
    )

    return GraphDiff(
        added_nodes=added_nodes,
        removed_nodes=removed_nodes,
        added_steps=added_steps,
        removed_steps=removed_steps,
        changed_step_nodes=changed_step_nodes,
        modified_by_class=modified_by_class,
        events=events,
        added_edges=added_edges,
        removed_edges=removed_edges,
        counts=counts,
    )


def impact_from_diff(g2: GraphV1, diff: GraphDiff, include_subgraph: bool = True) -> Impact:
    """Compute downstream impact in g2 starting from diff seed steps."""
    step_nodes = {node.id: node for node in g2.nodes if isinstance(node, StepNode)}
    table_nodes = {node.id: node for node in g2.nodes if isinstance(node, TableNode)}

    seed_steps: Set[str] = set()
    seed_reasons: Dict[str, List[SeedReason]] = {}

    def _add_seed_reason(step_id: str, reason: SeedReason) -> None:
        if step_id not in seed_reasons:
            seed_reasons[step_id] = []
        seed_reasons[step_id].append(reason)

    for step in diff.changed_step_nodes:
        seed_steps.add(step.id)
        _add_seed_reason(step.id, SeedReasonChangedStep(reason="changed_step"))

    modified_keys = {(item.transform_class_id, item.op) for item in diff.modified_by_class}

    for modified in diff.modified_by_class:
        for step in modified.to_steps:
            seed_steps.add(step.id)
            _add_seed_reason(
                step.id,
                SeedReasonModifiedByClass(
                    reason="modified_by_class",
                    transform_class_id=modified.transform_class_id,
                    op=modified.op,
                    from_steps=modified.from_steps,
                    to_steps=modified.to_steps,
                    classification=modified.classification,
                ),
            )

    for step in diff.added_steps:
        if (step.transform_class_id, step.op) not in modified_keys:
            seed_steps.add(step.id)
            _add_seed_reason(
                step.id,
                SeedReasonAddedStep(
                    reason="added_step",
                    transform_id=step.transform_id,
                    transform_class_id=step.transform_class_id,
                    op=step.op,
                ),
            )

    for event in diff.events:
        if event.type != "step_rewired" or not event.step_id:
            continue
        inputs_added = event.inputs_added or []
        inputs_removed = event.inputs_removed or []
        outputs_added = event.outputs_added or []
        outputs_removed = event.outputs_removed or []
        seed_steps.add(event.step_id)
        _add_seed_reason(
            event.step_id,
            SeedReasonStepRewired(
                reason="step_rewired",
                inputs_added=inputs_added,
                inputs_removed=inputs_removed,
                outputs_added=outputs_added,
                outputs_removed=outputs_removed,
            ),
        )

    touched_tables: Set[str] = set()
    for step_id in sorted(seed_steps):
        step = step_nodes.get(step_id)
        if step:
            touched_tables.update(step.outputs)

    reached_steps: Set[str] = set()
    reached_tables: Set[str] = set()
    parent_choice: Dict[str, Tuple[str, str]] = {}
    step_distance: Dict[str, int] = {step_id: 0 for step_id in seed_steps if step_id in step_nodes}
    queue = [step_id for step_id in sorted(seed_steps) if step_id in step_nodes]

    while queue:
        step_id = queue.pop(0)
        if step_id in reached_steps:
            continue
        reached_steps.add(step_id)
        step = step_nodes[step_id]
        for table_id in sorted(step.outputs):
            if table_id in reached_tables:
                continue
            reached_tables.add(table_id)
            table = table_nodes.get(table_id)
            if not table:
                continue
            for consumer_id in sorted(table.consumers):
                if consumer_id not in seed_steps:
                    candidate = (step_id, table_id)
                    candidate_distance = step_distance[step_id] + 1
                    current_distance = step_distance.get(consumer_id)
                    current_parent = parent_choice.get(consumer_id)
                    if (
                        current_distance is None
                        or candidate_distance < current_distance
                        or (candidate_distance == current_distance and (current_parent is None or candidate < current_parent))
                    ):
                        step_distance[consumer_id] = candidate_distance
                        parent_choice[consumer_id] = candidate
                if consumer_id not in reached_steps:
                    queue.append(consumer_id)

    seed_steps_sorted = sorted(seed_steps)
    touched_tables_sorted = sorted(touched_tables)
    impacted_steps_sorted = sorted(reached_steps - set(seed_steps_sorted))
    impacted_tables_sorted = sorted(reached_tables - set(touched_tables_sorted))

    reasons: Dict[str, List[ImpactReason]] = {}
    for step_id in impacted_steps_sorted:
        parent = parent_choice.get(step_id)
        if not parent:
            continue
        from_step, via_table = parent
        reasons[step_id] = [
            ImpactReasonTransitive(
                reason="transitive",
                from_step=from_step,
                via_table=via_table,
            )
        ]

    paths: Dict[str, List[str]] = {}

    def _build_path(step_id: str, visiting: Optional[Set[str]] = None) -> List[str]:
        if step_id in paths:
            return paths[step_id]
        if visiting is None:
            visiting = set()
        if step_id in visiting:
            paths[step_id] = [step_id]
            return paths[step_id]
        visiting.add(step_id)
        if step_id in seed_steps:
            paths[step_id] = [step_id]
            visiting.remove(step_id)
            return paths[step_id]
        parent = parent_choice.get(step_id)
        if not parent:
            paths[step_id] = [step_id]
            visiting.remove(step_id)
            return paths[step_id]
        from_step, via_table = parent
        path = _build_path(from_step, visiting) + [via_table, step_id]
        paths[step_id] = path
        visiting.remove(step_id)
        return path

    for step_id in sorted(reached_steps):
        _build_path(step_id)

    induced_subgraph: Optional[InducedSubgraph] = None
    if include_subgraph:
        reached_nodes = reached_steps | reached_tables
        edges = [
            EdgeKey(src=e.src, dst=e.dst, kind=e.kind)
            for e in sorted(g2.edges, key=lambda e: (e.src, e.dst, e.kind))
            if e.src in reached_nodes and e.dst in reached_nodes
        ]
        induced_subgraph = InducedSubgraph(
            nodes=sorted(reached_nodes),
            edges=edges,
        )

    return Impact(
        seed_steps=seed_steps_sorted,
        touched_tables=touched_tables_sorted,
        impacted_steps=impacted_steps_sorted,
        impacted_tables=impacted_tables_sorted,
        reasons={k: reasons[k] for k in sorted(reasons)},
        seed_reasons={k: seed_reasons[k] for k in sorted(seed_reasons) if k in seed_steps},
        paths={k: paths[k] for k in sorted(paths)},
        context={
            "removed_steps": diff.removed_steps,
            "debug_edge_deltas": {
                "added": diff.added_edges,
                "removed": diff.removed_edges,
            },
            "path_policy": "bipartite_shortest_then_lex_parent",
            "path_representation": "step_table_step",
            "paths_include_seeds": True,
        },
        induced_subgraph=induced_subgraph,
    )


def _edge_sort_key(edge: Tuple[str, str, str]) -> Tuple[str, str, str]:
    return edge[0], edge[1], edge[2]


def _event_sort_key(event: GraphDiffEvent) -> Tuple[str, str, str, str, str]:
    return (
        event.type,
        getattr(event, "transform_class_id", "") or "",
        getattr(event, "op", "") or "",
        getattr(event, "step_id", "") or "",
        canonical_dumps(event.model_dump()),
    )


def _steps_by_class(nodes_by_id: Dict[str, object]) -> Dict[Tuple[str, str], List[StepNode]]:
    steps_by_class: Dict[Tuple[str, str], List[StepNode]] = {}
    for node in nodes_by_id.values():
        if isinstance(node, StepNode):
            steps_by_class.setdefault((node.transform_class_id, node.op), []).append(node)
    for key in steps_by_class:
        steps_by_class[key] = sorted(steps_by_class[key], key=lambda s: s.id)
    return steps_by_class

from pathlib import Path

from cheshbon.api import graph_diff_bundles
from cheshbon._internal.canonical_json import canonical_dumps
from cheshbon.kernel.graph_diff import diff_graph, impact_from_diff
from cheshbon.kernel.graph_v1 import GraphV1, validate_graph_v1


def _build_graph(nodes, edges):
    graph = GraphV1(
        schema_version=1,
        producer={"name": "sans", "version": "0.1.0"},
        nodes=nodes,
        edges=edges,
    )
    validate_graph_v1(graph)
    return graph


def test_graph_diff_events_payload_change():
    bundle_a = Path("fixtures/graph_bundles/filter_a")
    bundle_b = Path("fixtures/graph_bundles/filter_b")

    diff, impact = graph_diff_bundles(bundle_a, bundle_b)

    event_types = {event.type for event in diff.events}
    assert "step_payload_changed" in event_types
    assert "step_replaced_same_class" not in event_types
    assert diff.counts.raw.added_steps == sum(
        1 for event in diff.events if event.type == "step_added"
    )
    assert diff.counts.raw.removed_steps == sum(
        1 for event in diff.events if event.type == "step_removed"
    )
    assert diff.counts.raw.changed_step_nodes == sum(
        1 for event in diff.events if event.type == "step_payload_changed"
    )
    assert diff.counts.raw.modified_by_class == sum(
        1 for event in diff.events if event.type == "step_replaced_same_class"
    )
    assert all(
        reason.reason != "edge_added"
        for reasons in impact.seed_reasons.values()
        for reason in reasons
    )


def test_graph_diff_reasons_schema():
    bundle_a = Path("fixtures/graph_bundles/filter_a")
    bundle_b = Path("fixtures/graph_bundles/filter_b")

    _, impact = graph_diff_bundles(bundle_a, bundle_b)

    assert isinstance(impact.reasons["s:select"], list)
    assert impact.reasons["s:select"][0].reason == "transitive"
    # Round-trip validation for discriminated union reasons
    from cheshbon.kernel.graph_diff import Impact as ImpactModel

    ImpactModel.model_validate(impact.model_dump())


def test_graph_diff_determinism_serialization():
    g1 = _build_graph(
        nodes=[
            {
                "id": "s:step1",
                "kind": "step",
                "op": "extract",
                "transform_class_id": "tc:extract",
                "transform_id": "t:extract",
                "inputs": ["t:raw"],
                "outputs": ["t:mid"],
                "payload_sha256": "p1",
            },
            {
                "id": "s:step2",
                "kind": "step",
                "op": "load",
                "transform_class_id": "tc:load",
                "transform_id": "t:load",
                "inputs": ["t:mid"],
                "outputs": ["t:out"],
                "payload_sha256": "p2",
            },
            {"id": "t:raw", "kind": "table", "producer": None, "consumers": ["s:step1"]},
            {"id": "t:mid", "kind": "table", "producer": "s:step1", "consumers": ["s:step2"]},
            {"id": "t:out", "kind": "table", "producer": "s:step2", "consumers": []},
        ],
        edges=[
            {"src": "s:step1", "dst": "t:mid", "kind": "produces"},
            {"src": "s:step2", "dst": "t:out", "kind": "produces"},
            {"src": "t:raw", "dst": "s:step1", "kind": "consumes"},
            {"src": "t:mid", "dst": "s:step2", "kind": "consumes"},
        ],
    )
    g2 = _build_graph(
        nodes=[
            {
                "id": "s:step1",
                "kind": "step",
                "op": "extract",
                "transform_class_id": "tc:extract",
                "transform_id": "t:extract",
                "inputs": ["t:raw"],
                "outputs": ["t:mid"],
                "payload_sha256": "p1_changed",
            },
            {
                "id": "s:step2",
                "kind": "step",
                "op": "load",
                "transform_class_id": "tc:load",
                "transform_id": "t:load",
                "inputs": ["t:mid"],
                "outputs": ["t:out"],
                "payload_sha256": "p2",
            },
            {"id": "t:raw", "kind": "table", "producer": None, "consumers": ["s:step1"]},
            {"id": "t:mid", "kind": "table", "producer": "s:step1", "consumers": ["s:step2"]},
            {"id": "t:out", "kind": "table", "producer": "s:step2", "consumers": []},
        ],
        edges=[
            {"src": "s:step1", "dst": "t:mid", "kind": "produces"},
            {"src": "s:step2", "dst": "t:out", "kind": "produces"},
            {"src": "t:raw", "dst": "s:step1", "kind": "consumes"},
            {"src": "t:mid", "dst": "s:step2", "kind": "consumes"},
        ],
    )

    diff1 = diff_graph(g1, g2)
    impact1 = impact_from_diff(g2, diff1)
    diff2 = diff_graph(g1, g2)
    impact2 = impact_from_diff(g2, diff2)

    assert canonical_dumps(diff1.model_dump()) == canonical_dumps(diff2.model_dump())
    assert canonical_dumps(impact1.model_dump()) == canonical_dumps(impact2.model_dump())


def test_path_policy_lex_parent_choice():
    g1 = _build_graph(
        nodes=[
            {
                "id": "s:a",
                "kind": "step",
                "op": "a",
                "transform_class_id": "tc:a",
                "transform_id": "t:a",
                "inputs": ["t:raw"],
                "outputs": ["t:x"],
                "payload_sha256": "pa",
            },
            {
                "id": "s:b",
                "kind": "step",
                "op": "b",
                "transform_class_id": "tc:b",
                "transform_id": "t:b",
                "inputs": ["t:raw"],
                "outputs": ["t:y"],
                "payload_sha256": "pb",
            },
            {
                "id": "s:c",
                "kind": "step",
                "op": "c",
                "transform_class_id": "tc:c",
                "transform_id": "t:c",
                "inputs": ["t:x", "t:y"],
                "outputs": ["t:z"],
                "payload_sha256": "pc",
            },
            {"id": "t:raw", "kind": "table", "producer": None, "consumers": ["s:a", "s:b"]},
            {"id": "t:x", "kind": "table", "producer": "s:a", "consumers": ["s:c"]},
            {"id": "t:y", "kind": "table", "producer": "s:b", "consumers": ["s:c"]},
            {"id": "t:z", "kind": "table", "producer": "s:c", "consumers": []},
        ],
        edges=[
            {"src": "s:a", "dst": "t:x", "kind": "produces"},
            {"src": "s:b", "dst": "t:y", "kind": "produces"},
            {"src": "s:c", "dst": "t:z", "kind": "produces"},
            {"src": "t:raw", "dst": "s:a", "kind": "consumes"},
            {"src": "t:raw", "dst": "s:b", "kind": "consumes"},
            {"src": "t:x", "dst": "s:c", "kind": "consumes"},
            {"src": "t:y", "dst": "s:c", "kind": "consumes"},
        ],
    )
    g2 = _build_graph(
        nodes=[
            {
                "id": "s:a",
                "kind": "step",
                "op": "a",
                "transform_class_id": "tc:a",
                "transform_id": "t:a",
                "inputs": ["t:raw"],
                "outputs": ["t:x"],
                "payload_sha256": "pa2",
            },
            {
                "id": "s:b",
                "kind": "step",
                "op": "b",
                "transform_class_id": "tc:b",
                "transform_id": "t:b",
                "inputs": ["t:raw"],
                "outputs": ["t:y"],
                "payload_sha256": "pb2",
            },
            {
                "id": "s:c",
                "kind": "step",
                "op": "c",
                "transform_class_id": "tc:c",
                "transform_id": "t:c",
                "inputs": ["t:x", "t:y"],
                "outputs": ["t:z"],
                "payload_sha256": "pc",
            },
            {"id": "t:raw", "kind": "table", "producer": None, "consumers": ["s:a", "s:b"]},
            {"id": "t:x", "kind": "table", "producer": "s:a", "consumers": ["s:c"]},
            {"id": "t:y", "kind": "table", "producer": "s:b", "consumers": ["s:c"]},
            {"id": "t:z", "kind": "table", "producer": "s:c", "consumers": []},
        ],
        edges=[
            {"src": "s:a", "dst": "t:x", "kind": "produces"},
            {"src": "s:b", "dst": "t:y", "kind": "produces"},
            {"src": "s:c", "dst": "t:z", "kind": "produces"},
            {"src": "t:raw", "dst": "s:a", "kind": "consumes"},
            {"src": "t:raw", "dst": "s:b", "kind": "consumes"},
            {"src": "t:x", "dst": "s:c", "kind": "consumes"},
            {"src": "t:y", "dst": "s:c", "kind": "consumes"},
        ],
    )

    diff = diff_graph(g1, g2)
    impact = impact_from_diff(g2, diff)

    assert impact.context["path_policy"] == "bipartite_shortest_then_lex_parent"
    assert impact.context["path_representation"] == "step_table_step"
    assert impact.context["paths_include_seeds"] is True
    assert impact.paths["s:c"] == ["s:a", "t:x", "s:c"]

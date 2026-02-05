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


def test_diff_and_impact_payload_change():
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

    diff = diff_graph(g1, g2)
    assert [node.id for node in diff.changed_step_nodes] == ["s:step1"]
    assert diff.changed_step_nodes[0].from_payload_sha256 == "p1"
    assert diff.changed_step_nodes[0].to_payload_sha256 == "p1_changed"
    assert diff.added_nodes == []
    assert diff.removed_nodes == []

    impact = impact_from_diff(g2, diff)
    assert impact.seed_steps == ["s:step1"]
    assert impact.impacted_steps == ["s:step2"]
    assert impact.touched_tables == ["t:mid"]
    assert impact.impacted_tables == ["t:out"]
    assert set(impact.seed_reasons.keys()) == set(impact.seed_steps)
    reason = impact.reasons["s:step2"][0]
    assert reason.reason == "transitive"
    assert reason.from_step == "s:step1"
    assert reason.via_table == "t:mid"
    assert impact.paths["s:step1"] == ["s:step1"]
    assert impact.paths["s:step2"] == ["s:step1", "t:mid", "s:step2"]
    assert impact.seed_steps == sorted(impact.seed_steps)
    assert impact.impacted_steps == sorted(impact.impacted_steps)
    assert impact.impacted_tables == sorted(impact.impacted_tables)


def test_diff_and_impact_rewire_consumes():
    g1 = _build_graph(
        nodes=[
            {
                "id": "s:a",
                "kind": "step",
                "op": "a",
                "transform_class_id": "tc:a",
                "transform_id": "t:a",
                "inputs": ["t:raw"],
                "outputs": ["t:mid"],
                "payload_sha256": "pa",
            },
            {
                "id": "s:b",
                "kind": "step",
                "op": "b",
                "transform_class_id": "tc:b",
                "transform_id": "t:b",
                "inputs": ["t:mid"],
                "outputs": ["t:outb"],
                "payload_sha256": "pb",
            },
            {
                "id": "s:c",
                "kind": "step",
                "op": "c",
                "transform_class_id": "tc:c",
                "transform_id": "t:c",
                "inputs": ["t:raw"],
                "outputs": ["t:outc"],
                "payload_sha256": "pc",
            },
            {
                "id": "s:d",
                "kind": "step",
                "op": "d",
                "transform_class_id": "tc:d",
                "transform_id": "t:d",
                "inputs": ["t:outc"],
                "outputs": ["t:final"],
                "payload_sha256": "pd",
            },
            {"id": "t:raw", "kind": "table", "producer": None, "consumers": ["s:a", "s:c"]},
            {"id": "t:mid", "kind": "table", "producer": "s:a", "consumers": ["s:b"]},
            {"id": "t:outb", "kind": "table", "producer": "s:b", "consumers": []},
            {"id": "t:outc", "kind": "table", "producer": "s:c", "consumers": ["s:d"]},
            {"id": "t:final", "kind": "table", "producer": "s:d", "consumers": []},
        ],
        edges=[
            {"src": "s:a", "dst": "t:mid", "kind": "produces"},
            {"src": "s:b", "dst": "t:outb", "kind": "produces"},
            {"src": "s:c", "dst": "t:outc", "kind": "produces"},
            {"src": "s:d", "dst": "t:final", "kind": "produces"},
            {"src": "t:raw", "dst": "s:a", "kind": "consumes"},
            {"src": "t:mid", "dst": "s:b", "kind": "consumes"},
            {"src": "t:raw", "dst": "s:c", "kind": "consumes"},
            {"src": "t:outc", "dst": "s:d", "kind": "consumes"},
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
                "outputs": ["t:mid"],
                "payload_sha256": "pa",
            },
            {
                "id": "s:b",
                "kind": "step",
                "op": "b",
                "transform_class_id": "tc:b",
                "transform_id": "t:b",
                "inputs": ["t:raw"],
                "outputs": ["t:outb"],
                "payload_sha256": "pb",
            },
            {
                "id": "s:c",
                "kind": "step",
                "op": "c",
                "transform_class_id": "tc:c",
                "transform_id": "t:c",
                "inputs": ["t:mid"],
                "outputs": ["t:outc"],
                "payload_sha256": "pc",
            },
            {
                "id": "s:d",
                "kind": "step",
                "op": "d",
                "transform_class_id": "tc:d",
                "transform_id": "t:d",
                "inputs": ["t:outc"],
                "outputs": ["t:final"],
                "payload_sha256": "pd",
            },
            {"id": "t:raw", "kind": "table", "producer": None, "consumers": ["s:a", "s:b"]},
            {"id": "t:mid", "kind": "table", "producer": "s:a", "consumers": ["s:c"]},
            {"id": "t:outb", "kind": "table", "producer": "s:b", "consumers": []},
            {"id": "t:outc", "kind": "table", "producer": "s:c", "consumers": ["s:d"]},
            {"id": "t:final", "kind": "table", "producer": "s:d", "consumers": []},
        ],
        edges=[
            {"src": "s:a", "dst": "t:mid", "kind": "produces"},
            {"src": "s:b", "dst": "t:outb", "kind": "produces"},
            {"src": "s:c", "dst": "t:outc", "kind": "produces"},
            {"src": "s:d", "dst": "t:final", "kind": "produces"},
            {"src": "t:raw", "dst": "s:a", "kind": "consumes"},
            {"src": "t:raw", "dst": "s:b", "kind": "consumes"},
            {"src": "t:mid", "dst": "s:c", "kind": "consumes"},
            {"src": "t:outc", "dst": "s:d", "kind": "consumes"},
        ],
    )

    diff = diff_graph(g1, g2)
    impact = impact_from_diff(g2, diff)

    added_keys = [(e.src, e.dst, e.kind) for e in diff.added_edges]
    removed_keys = [(e.src, e.dst, e.kind) for e in diff.removed_edges]
    assert added_keys == sorted(added_keys)
    assert removed_keys == sorted(removed_keys)

    assert impact.seed_steps == sorted(impact.seed_steps)
    assert impact.impacted_steps == ["s:d"]
    assert impact.touched_tables == ["t:outb", "t:outc"]
    assert impact.impacted_tables == ["t:final"]
    assert impact.impacted_tables == sorted(impact.impacted_tables)
    assert set(impact.seed_reasons.keys()) == set(impact.seed_steps)

    assert any(r.reason == "step_rewired" for r in impact.seed_reasons.get("s:b", []))
    assert any(r.reason == "step_rewired" for r in impact.seed_reasons.get("s:c", []))

    d_reason = impact.reasons["s:d"][0]
    assert d_reason.reason == "transitive"
    assert d_reason.from_step == "s:c"
    assert d_reason.via_table == "t:outc"

import json
from pathlib import Path

from cheshbon.kernel.graph_v1 import parse_graph_v1


def test_graph_v1_accepts_transform_class_id():
    graph_path = Path("fixtures/graph_diff/ex1/artifacts/graph.json")
    data = json.loads(graph_path.read_text(encoding="utf-8"))

    graph = parse_graph_v1(data)
    step_nodes = [node for node in graph.nodes if node.kind == "step"]
    assert all(node.transform_class_id for node in step_nodes)

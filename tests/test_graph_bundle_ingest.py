import json
from pathlib import Path

import pytest

from cheshbon.api import load_graph_bundle
from cheshbon.kernel.hash_utils import compute_canonical_json_sha256


def _copy_bundle(src: Path, dest: Path) -> None:
    import shutil
    dest.mkdir()
    for item in src.iterdir():
        if item.is_file():
            shutil.copy(item, dest)
        elif item.is_dir():
            shutil.copytree(item, dest / item.name)


def test_graph_bundle_ingest_success():
    bundle_path = Path("fixtures/graph_bundles/basic")
    graph = load_graph_bundle(bundle_path)
    assert graph.schema_version == 1
    assert len(graph.nodes) > 0
    assert len(graph.edges) > 0


def test_graph_bundle_sha_mismatch(tmp_path):
    src = Path("fixtures/graph_bundles/basic")
    bundle_dir = tmp_path / "bundle"
    _copy_bundle(src, bundle_dir)

    graph_path = bundle_dir / "artifacts" / "graph.json"
    data = json.loads(graph_path.read_text(encoding="utf-8"))
    data["producer"] = "mutated"
    graph_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    with pytest.raises(ValueError) as excinfo:
        load_graph_bundle(bundle_dir)
    assert "hash mismatch" in str(excinfo.value).lower()


def test_graph_bundle_schema_version_failure(tmp_path):
    src = Path("fixtures/graph_bundles/basic")
    bundle_dir = tmp_path / "bundle"
    _copy_bundle(src, bundle_dir)

    graph_path = bundle_dir / "artifacts" / "graph.json"
    data = json.loads(graph_path.read_text(encoding="utf-8"))
    data["schema_version"] = 2
    graph_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    report_path = bundle_dir / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    new_sha = compute_canonical_json_sha256(graph_path)
    for artifact in report.get("artifacts", []):
        if artifact.get("path") == "artifacts/graph.json":
            artifact["sha256"] = new_sha
    report_path.write_text(json.dumps(report, separators=(",", ":")), encoding="utf-8")

    with pytest.raises(ValueError) as excinfo:
        load_graph_bundle(bundle_dir)
    assert "schema_version" in str(excinfo.value)

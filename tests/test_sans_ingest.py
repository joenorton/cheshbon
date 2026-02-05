import pytest
import json
from pathlib import Path
from cheshbon.api import ingest_sans_bundle

def test_ingest_golden_bundle(tmp_path):
    bundle_path = Path("fixtures/sans_bundles/golden")
    output_dir = tmp_path / "output"
    
    ingest_sans_bundle(bundle_path, output_dir)
    
    cheshbon_dir = output_dir / "cheshbon"
    assert (cheshbon_dir / "graph.json").exists()
    assert (cheshbon_dir / "run.json").exists()
    assert (cheshbon_dir / "registry.json").exists()
    
    # Check graph.json content (should be consumed from bundle, not generated)
    graph_text = (cheshbon_dir / "graph.json").read_text(encoding="utf-8")
    bundle_graph_text = (bundle_path / "artifacts" / "graph.json").read_text(encoding="utf-8")
    graph = json.loads(graph_text)
    bundle_graph = json.loads(bundle_graph_text)
    assert graph == bundle_graph
    assert graph["schema_version"] == 1
    assert len(graph["nodes"]) > 0
    assert len(graph["edges"]) > 0
    
    # Check run.json content
    run = json.loads((cheshbon_dir / "run.json").read_text())
    assert run["format"] == "cheshbon.run"
    assert run["run_id"] == "76a5baf5-dcb4-45a7-bcc8-46f72d4fd75b"
    assert "fingerprint" in run
    
    # Check registry.json content
    registry = json.loads((cheshbon_dir / "registry.json").read_text())
    assert registry["format"] == "cheshbon.registry"
    assert len(registry["transforms"]) == 4

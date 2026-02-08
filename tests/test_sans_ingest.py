import pytest
import json
import shutil
from pathlib import Path
from cheshbon.api import ingest_sans_bundle


def _copy_bundle(src: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    for item in src.iterdir():
        if item.is_file():
            shutil.copy(item, dest)
        elif item.is_dir():
            shutil.copytree(item, dest / item.name)


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


def test_ingest_thin_bundle(tmp_path):
    """Thin bundle (no inputs/data/) ingests successfully; run.json has fingerprint."""
    bundle_path = Path("fixtures/demo_low/dl_out")
    output_dir = tmp_path / "output"
    # Patch run_id/created_at only: they are provenance fields required by ingest; thin fixture
    # may omit them. Safe to patch for this test—semantic witness is datasource_inputs.
    bundle_dir = tmp_path / "thin_bundle"
    _copy_bundle(bundle_path, bundle_dir)
    report_path = bundle_dir / "report.json"
    data = json.loads(report_path.read_text())
    data["run_id"] = data.get("run_id") or "thin-test-run-id"
    data["created_at"] = data.get("created_at") or "2026-02-07T00:00:00Z"
    report_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    ingest_sans_bundle(bundle_dir, output_dir)

    cheshbon_dir = output_dir / "cheshbon"
    assert (cheshbon_dir / "graph.json").exists()
    assert (cheshbon_dir / "run.json").exists()
    assert (cheshbon_dir / "registry.json").exists()

    run = json.loads((cheshbon_dir / "run.json").read_text())
    assert run["format"] == "cheshbon.run"
    assert "fingerprint" in run
    assert run["run_id"] == "thin-test-run-id"


def test_ingest_thin_bundle_provenance_does_not_affect_fingerprint(tmp_path):
    """Mutating run_id/created_at in the report must not change run fingerprint (provenance-only)."""
    bundle_path = Path("fixtures/demo_low/dl_out")
    # Patch provenance fields so ingest accepts the bundle
    def ingest_with_provenance(run_id_val: str, created_at_val: str) -> str:
        out = tmp_path / "out" / run_id_val
        out.mkdir(parents=True, exist_ok=True)
        bundle_dir = tmp_path / "copy" / run_id_val
        _copy_bundle(bundle_path, bundle_dir)
        report_path = bundle_dir / "report.json"
        data = json.loads(report_path.read_text())
        data["run_id"] = run_id_val
        data["created_at"] = created_at_val
        report_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        ingest_sans_bundle(bundle_dir, out)
        run = json.loads((out / "cheshbon" / "run.json").read_text())
        return run["fingerprint"]

    fp1 = ingest_with_provenance("run-a", "2026-01-01T00:00:00Z")
    fp2 = ingest_with_provenance("run-b", "2026-12-31T23:59:59Z")
    assert fp1 == fp2, "run_id/created_at are provenance-only; fingerprint must be unchanged"

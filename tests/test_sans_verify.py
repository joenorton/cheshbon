import json
import pytest
from pathlib import Path
from cheshbon.api import verify_sans_bundle
from cheshbon.codes import ValidationCode


def _copy_bundle(src: Path, dest: Path) -> None:
    import shutil
    dest.mkdir()
    for item in src.iterdir():
        if item.is_file():
            shutil.copy(item, dest)
        elif item.is_dir():
            shutil.copytree(item, dest / item.name)


def test_verify_golden_bundle():
    bundle_path = Path("fixtures/sans_bundles/golden")
    result = verify_sans_bundle(bundle_path)
    
    assert result.ok
    assert len(result.errors) == 0

def test_verify_mismatched_plan_hash(tmp_path):
    # Copy golden bundle to tmp_path
    bundle_dir = tmp_path / "corrupt_bundle"
    golden_dir = Path("fixtures/sans_bundles/golden")
    _copy_bundle(golden_dir, bundle_dir)
            
    # Corrupt the plan.ir.json (change semantic content)
    plan_path = bundle_dir / "artifacts" / "plan.ir.json"
    data = json.loads(plan_path.read_text())
    data["steps"][0]["op"] = "compute_modified"
    plan_path.write_text(json.dumps(data, indent=2) + "\n")
    
    result = verify_sans_bundle(bundle_dir)
    
    assert not result.ok
    assert any(e.code == ValidationCode.HASH_MISMATCH.value for e in result.errors)


def test_verify_missing_report(tmp_path):
    bundle_dir = tmp_path / "missing_report"
    bundle_dir.mkdir()

    result = verify_sans_bundle(bundle_dir)

    assert not result.ok
    assert any(e.code == ValidationCode.FILE_NOT_FOUND.value and "report.json" in e.message for e in result.errors)


def test_verify_unsupported_report_version(tmp_path):
    bundle_dir = tmp_path / "unsupported_version"
    golden_dir = Path("fixtures/sans_bundles/golden")
    _copy_bundle(golden_dir, bundle_dir)

    report_path = bundle_dir / "report.json"
    data = json.loads(report_path.read_text())
    data["report_schema_version"] = "9.9"
    report_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    result = verify_sans_bundle(bundle_dir)

    assert not result.ok
    assert any(
        e.code == ValidationCode.INVALID_STRUCTURE.value and "Unsupported report_schema_version" in e.message
        for e in result.errors
    )


def test_verify_thin_bundle_passes():
    """Thin bundle with datasource_inputs and no inputs/data/ verifies successfully."""
    bundle_path = Path("fixtures/demo_low/dl_out")
    result = verify_sans_bundle(bundle_path)
    assert result.ok
    assert len(result.errors) == 0


def test_verify_thin_missing_datasource_inputs_fails(tmp_path):
    """Thin bundle with missing or empty datasource_inputs fails verification."""
    bundle_dir = tmp_path / "thin_bad"
    _copy_bundle(Path("fixtures/demo_low/dl_out"), bundle_dir)
    report_path = bundle_dir / "report.json"
    data = json.loads(report_path.read_text())
    assert data.get("bundle_mode") == "thin"
    data["datasource_inputs"] = []
    report_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    result = verify_sans_bundle(bundle_dir)
    assert not result.ok
    assert any(
        e.code == ValidationCode.INVALID_STRUCTURE.value and "datasource_inputs" in e.message
        for e in result.errors
    )


def test_verify_thin_missing_sha256_fails(tmp_path):
    """Thin bundle with empty sha256 in datasource_inputs entry fails verification."""
    bundle_dir = tmp_path / "thin_bad_sha"
    _copy_bundle(Path("fixtures/demo_low/dl_out"), bundle_dir)
    report_path = bundle_dir / "report.json"
    data = json.loads(report_path.read_text())
    assert data.get("bundle_mode") == "thin" and data.get("datasource_inputs")
    data["datasource_inputs"][0]["sha256"] = ""
    report_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    result = verify_sans_bundle(bundle_dir)
    assert not result.ok
    assert any(
        e.code == ValidationCode.INVALID_STRUCTURE.value and "sha256" in e.message
        for e in result.errors
    )


def test_verify_thin_embedded_true_fails(tmp_path):
    """Thin bundle with embedded=true in datasource_inputs entry fails verification."""
    bundle_dir = tmp_path / "thin_bad_embedded"
    _copy_bundle(Path("fixtures/demo_low/dl_out"), bundle_dir)
    report_path = bundle_dir / "report.json"
    data = json.loads(report_path.read_text())
    assert data.get("bundle_mode") == "thin" and data.get("datasource_inputs")
    data["datasource_inputs"][0]["embedded"] = True
    report_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    result = verify_sans_bundle(bundle_dir)
    assert not result.ok
    assert any(
        e.code == ValidationCode.INVALID_STRUCTURE.value and "embedded" in e.message
        for e in result.errors
    )


def test_verify_thin_size_bytes_zero_fails(tmp_path):
    """Thin bundle with size_bytes==0 in datasource_inputs entry fails verification."""
    bundle_dir = tmp_path / "thin_bad_size"
    _copy_bundle(Path("fixtures/demo_low/dl_out"), bundle_dir)
    report_path = bundle_dir / "report.json"
    data = json.loads(report_path.read_text())
    assert data.get("bundle_mode") == "thin" and data.get("datasource_inputs")
    data["datasource_inputs"][0]["size_bytes"] = 0
    report_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    result = verify_sans_bundle(bundle_dir)
    assert not result.ok
    assert any(
        e.code == ValidationCode.INVALID_STRUCTURE.value and "size_bytes" in e.message
        for e in result.errors
    )

"""Tests for cheshbon diff command."""

import json
import pytest
from pathlib import Path
from cheshbon.diff import run_diff, generate_markdown_report, generate_json_report
from cheshbon.kernel.spec import MappingSpec, SourceColumn, DerivedVariable
from cheshbon.kernel.impact import ImpactResult
from cheshbon.kernel.diff import ChangeEvent


def create_test_spec(study_id: str = "TEST-001", sources=None, derived=None) -> MappingSpec:
    """Helper to create test spec."""
    if sources is None:
        sources = [
            SourceColumn(id="s:SUBJID", name="SUBJID", type="string"),
            SourceColumn(id="s:SEX", name="SEX", type="string"),
        ]
    if derived is None:
        derived = [
            DerivedVariable(
                id="d:USUBJID",
                name="USUBJID",
                type="string",
                transform_ref="t:direct_copy",
                inputs=["s:SUBJID"]
            )
        ]
    
    return MappingSpec(
        spec_version="1.0.0",
        study_id=study_id,
        source_table="RAW_DM",
        sources=sources,
        derived=derived
    )


def test_diff_rename_only_no_impact(tmp_path):
    """Golden scenario 1: rename-only in spec (d: name change) -> no impact."""
    workspace = tmp_path / "workspace"
    reports_dir = workspace / "reports"
    reports_dir.mkdir(parents=True)
    
    # Create two specs: only name changed, ID unchanged
    spec_v1 = create_test_spec(
        derived=[
            DerivedVariable(
                id="d:USUBJID",
                name="USUBJID",
                type="string",
                transform_ref="t:direct_copy",
                inputs=["s:SUBJID"]
            )
        ]
    )
    spec_v2 = create_test_spec(
        derived=[
            DerivedVariable(
                id="d:USUBJID",  # Same ID
                name="SUBJECT_ID",  # Name changed
                type="string",
                transform_ref="t:direct_copy",
                inputs=["s:SUBJID"]
            )
        ]
    )
    
    # Write specs
    spec_dir = workspace / "spec" / "dm"
    spec_dir.mkdir(parents=True)
    spec_v1_path = spec_dir / "v001.json"
    spec_v2_path = spec_dir / "v002.json"
    
    with open(spec_v1_path, 'w', encoding='utf-8') as f:
        json.dump(spec_v1.model_dump(), f, indent=2)
    with open(spec_v2_path, 'w', encoding='utf-8') as f:
        json.dump(spec_v2.model_dump(), f, indent=2)
    
    # Run diff
    exit_code, md_path, json_path = run_diff(spec_v1_path, spec_v2_path, reports_dir)
    
    # Should have no impact (exit code 0)
    assert exit_code == 0
    
    # Verify reports exist
    assert Path(md_path).exists()
    assert Path(json_path).exists()
    
    # Verify JSON report
    with open(json_path, 'r', encoding='utf-8') as f:
        report = json.load(f)
    
    assert report["run_status"] == "no_impact"
    assert len(report["impacted"]) == 0


def test_diff_params_change_impact(tmp_path):
    """Golden scenario 2: params change -> direct + transitive impact."""
    workspace = tmp_path / "workspace"
    reports_dir = workspace / "reports"
    reports_dir.mkdir(parents=True)
    
    # Create specs: params changed on d:SEX, which d:SEX_CDISC depends on
    spec_v1 = create_test_spec(
        derived=[
            DerivedVariable(
                id="d:SEX",
                name="SEX",
                type="string",
                transform_ref="t:ct_map",
                inputs=["s:SEX"],
                params={"map": {"M": "M", "F": "F"}}
            ),
            DerivedVariable(
                id="d:SEX_CDISC",
                name="SEX_CDISC",
                type="string",
                transform_ref="t:direct_copy",
                inputs=["d:SEX"]  # Depends on d:SEX
            )
        ]
    )
    spec_v2 = create_test_spec(
        derived=[
            DerivedVariable(
                id="d:SEX",
                name="SEX",
                type="string",
                transform_ref="t:ct_map",
                inputs=["s:SEX"],
                params={"map": {"M": "M", "F": "F", "U": "UNKNOWN"}}  # Params changed
            ),
            DerivedVariable(
                id="d:SEX_CDISC",
                name="SEX_CDISC",
                type="string",
                transform_ref="t:direct_copy",
                inputs=["d:SEX"]
            )
        ]
    )
    
    # Write specs
    spec_dir = workspace / "spec" / "dm"
    spec_dir.mkdir(parents=True)
    spec_v1_path = spec_dir / "v001.json"
    spec_v2_path = spec_dir / "v002.json"
    
    with open(spec_v1_path, 'w', encoding='utf-8') as f:
        json.dump(spec_v1.model_dump(), f, indent=2)
    with open(spec_v2_path, 'w', encoding='utf-8') as f:
        json.dump(spec_v2.model_dump(), f, indent=2)
    
    # Run diff
    exit_code, md_path, json_path = run_diff(spec_v1_path, spec_v2_path, reports_dir)
    
    # Should have impact (exit code 1)
    assert exit_code == 1
    
    # Verify JSON report
    with open(json_path, 'r', encoding='utf-8') as f:
        report = json.load(f)
    
    assert report["run_status"] == "impacted"
    assert "d:SEX" in report["impacted"]  # Direct impact
    assert "d:SEX_CDISC" in report["impacted"]  # Transitive impact


def test_diff_registry_impl_change_impact(tmp_path):
    """Golden scenario 3: registry impl digest change -> impacts all users with no spec change."""
    workspace = tmp_path / "workspace"
    reports_dir = workspace / "reports"
    registry_dir = workspace / "registry"
    spec_dir = workspace / "spec" / "dm"
    
    for d in [reports_dir, registry_dir, spec_dir]:
        d.mkdir(parents=True)
    
    # Create identical specs
    spec = create_test_spec(
            derived=[
                DerivedVariable(
                    id="d:USUBJID",
                    name="USUBJID",
                    type="string",
                    transform_ref="t:direct_copy",
                    inputs=["s:SUBJID"]
                )
            ]
        )
    
    spec_v1_path = spec_dir / "v001.json"
    spec_v2_path = spec_dir / "v002.json"
    
    with open(spec_v1_path, 'w', encoding='utf-8') as f:
        json.dump(spec.model_dump(), f, indent=2)
    with open(spec_v2_path, 'w', encoding='utf-8') as f:
        json.dump(spec.model_dump(), f, indent=2)
    
    # Create registries with different digests
    registry_v1 = {
        "registry_version": "1.0.0",
        "transforms": [
            {
                "id": "t:direct_copy",
                "version": "1.0.0",
                "kind": "builtin",
                "signature": {"inputs": ["any"], "output": "any"},
                "params_schema_hash": None,
                "impl_fingerprint": {
                    "algo": "sha256",
                    "source": "builtin",
                    "ref": "cheshbon.transforms.direct_copy",
                    "digest": "a" * 64  # Old digest
                }
            }
        ]
    }
    
    registry_v2 = {
        "registry_version": "1.0.0",
        "transforms": [
            {
                "id": "t:direct_copy",
                "version": "1.0.0",
                "kind": "builtin",
                "signature": {"inputs": ["any"], "output": "any"},
                "params_schema_hash": None,
                "impl_fingerprint": {
                    "algo": "sha256",
                    "source": "builtin",
                    "ref": "cheshbon.transforms.direct_copy",
                    "digest": "b" * 64  # New digest (impl changed)
                }
            }
        ]
    }
    
    registry_v1_path = registry_dir / "v001.json"
    registry_v2_path = registry_dir / "v002.json"
    
    with open(registry_v1_path, 'w', encoding='utf-8') as f:
        json.dump(registry_v1, f, indent=2)
    with open(registry_v2_path, 'w', encoding='utf-8') as f:
        json.dump(registry_v2, f, indent=2)
    
    # Run diff
    exit_code, md_path, json_path = run_diff(
        spec_v1_path,
        spec_v2_path,
        reports_dir,
        registry_v1_path=registry_v1_path,
        registry_v2_path=registry_v2_path
    )
    
    # Should have impact (exit code 1) even though spec didn't change
    assert exit_code == 1
    
    # Verify JSON report
    with open(json_path, 'r', encoding='utf-8') as f:
        report = json.load(f)
    
    assert report["run_status"] == "impacted"
    assert "d:USUBJID" in report["impacted"]
    assert any(e["change_type"] == "TRANSFORM_IMPL_CHANGED" for e in report["change_events"])


def test_diff_transform_removed_validation_failed(tmp_path):
    """Golden scenario 4: transform removed -> validation_failed but full report + impacted list."""
    workspace = tmp_path / "workspace"
    reports_dir = workspace / "reports"
    registry_dir = workspace / "registry"
    spec_dir = workspace / "spec" / "dm"
    
    for d in [reports_dir, registry_dir, spec_dir]:
        d.mkdir(parents=True)
    
    # Create spec that uses t:direct_copy
    spec = create_test_spec(
            derived=[
                DerivedVariable(
                    id="d:USUBJID",
                    name="USUBJID",
                    type="string",
                    transform_ref="t:direct_copy",
                    inputs=["s:SUBJID"]
                )
            ]
        )
    
    spec_v1_path = spec_dir / "v001.json"
    spec_v2_path = spec_dir / "v002.json"
    
    with open(spec_v1_path, 'w', encoding='utf-8') as f:
        json.dump(spec.model_dump(), f, indent=2)
    with open(spec_v2_path, 'w', encoding='utf-8') as f:
        json.dump(spec.model_dump(), f, indent=2)
    
    # Create registries: v1 has t:direct_copy, v2 doesn't
    registry_v1 = {
        "registry_version": "1.0.0",
        "transforms": [
            {
                "id": "t:direct_copy",
                "version": "1.0.0",
                "kind": "builtin",
                "signature": {"inputs": ["any"], "output": "any"},
                "params_schema_hash": None,
                "impl_fingerprint": {
                    "algo": "sha256",
                    "source": "builtin",
                    "ref": "cheshbon.transforms.direct_copy",
                    "digest": "a" * 64
                }
            }
        ]
    }
    
    registry_v2 = {
        "registry_version": "1.0.0",
        "transforms": []  # Transform removed
    }
    
    registry_v1_path = registry_dir / "v001.json"
    registry_v2_path = registry_dir / "v002.json"
    
    with open(registry_v1_path, 'w', encoding='utf-8') as f:
        json.dump(registry_v1, f, indent=2)
    with open(registry_v2_path, 'w', encoding='utf-8') as f:
        json.dump(registry_v2, f, indent=2)
    
    # Run diff
    exit_code, md_path, json_path = run_diff(
        spec_v1_path,
        spec_v2_path,
        reports_dir,
        registry_v1_path=registry_v1_path,
        registry_v2_path=registry_v2_path
    )
    
    # Should be validation_failed (exit code 2)
    assert exit_code == 2
    
    # Verify JSON report
    with open(json_path, 'r', encoding='utf-8') as f:
        report = json.load(f)
        
        assert report["run_status"] == "non_executable"
        assert report["validation_failed"] is True
        assert len(report["validation_errors"]) > 0
        # Should still compute impacted list
        assert "d:USUBJID" in report["impacted"]


def test_generate_markdown_report():
    """Test markdown report generation."""
    impact_result = ImpactResult(
        impacted={"d:TEST"},
        unaffected=set(),
        impact_paths={"d:TEST": ["s:INPUT", "d:TEST"]},
        impact_reasons={"d:TEST": "DIRECT_CHANGE"},
        unresolved_references={},
        missing_bindings={},
        missing_transform_refs={},
        validation_failed=False,
        validation_errors=[]
    )
    
    change_events = [
        ChangeEvent(
            change_type="DERIVED_TRANSFORM_PARAMS_CHANGED",
            element_id="d:TEST",
            old_value=None,
            new_value=None
        )
    ]
    
    spec_v1 = create_test_spec()
    spec_v2 = create_test_spec()
    
    md = generate_markdown_report(impact_result, change_events, spec_v1, spec_v2)
    
    assert "# Impact Analysis Report" in md
    assert "d:TEST" in md
    assert "DIRECT_CHANGE" in md


def test_generate_json_report():
    """Test JSON report generation."""
    impact_result = ImpactResult(
        impacted={"d:TEST"},
        unaffected=set(),
        impact_paths={"d:TEST": ["s:INPUT", "d:TEST"]},
        impact_reasons={"d:TEST": "DIRECT_CHANGE"},
        unresolved_references={},
        missing_bindings={},
        missing_transform_refs={},
        validation_failed=False,
        validation_errors=[]
    )
    
    change_events = [
        ChangeEvent(
            change_type="DERIVED_TRANSFORM_PARAMS_CHANGED",
            element_id="d:TEST",
            old_value=None,
            new_value=None
        )
    ]
    
    json_report = generate_json_report(impact_result, change_events)
    
    assert json_report["run_status"] == "impacted"
    assert "d:TEST" in json_report["impacted"]
    assert json_report["summary"]["impacted_count"] == 1

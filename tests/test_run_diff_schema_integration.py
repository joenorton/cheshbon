"""Targeted tests for cheshbon schema integration (schema evidence, lock diff, refusal, hash integrity).

Fixtures: clone fixtures/demo_high/dh_out per test and modify in tmp_path (no committed duplicate trees).
Failures would catch: drop treated as value change; schema diff silently skipped; lock hash misuse;
refusal not short-circuiting.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from cheshbon.adapters.sans_bundle import run_diff_from_bundles


def _demo_high_root() -> Path:
    return Path(__file__).resolve().parent.parent / "fixtures" / "demo_high" / "dh_out"


def _copy_demo_high_to(dest: Path) -> None:
    """Copy fixtures/demo_high/dh_out to dest (for use as bundle A or B)."""
    src = _demo_high_root()
    if not src.exists():
        pytest.skip("fixtures/demo_high/dh_out not found")
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(src, dest)


# --- 1) Schema evidence drop diff ---


def test_schema_evidence_drop_diff_value_evidence_column_no_longer_exists(tmp_path: Path) -> None:
    """Drop in schema.evidence: schema_changes reports removal; value evidence is column_no_longer_exists, not value-changed."""
    _copy_demo_high_to(tmp_path / "a")
    _copy_demo_high_to(tmp_path / "b")

    # B: remove column "label" from table "sorted_high" in schema.evidence.json
    evidence_path = tmp_path / "b" / "artifacts" / "schema.evidence.json"
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert "sorted_high" in evidence.get("tables", {})
    tables = dict(evidence["tables"])
    sorted_high = dict(tables["sorted_high"])
    assert "label" in sorted_high
    del sorted_high["label"]
    tables["sorted_high"] = sorted_high
    evidence["tables"] = tables
    evidence_path.write_text(json.dumps(evidence, indent=2), encoding="utf-8")

    # B: last select step becomes select with drop([label]) so plan reflects projection-removal
    plan_path = tmp_path / "b" / "artifacts" / "plan.ir.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    for step in plan.get("steps", []):
        if step.get("op") == "select" and "sorted_high" in (step.get("outputs") or []):
            step["params"] = step.get("params") or {}
            step["params"]["drop"] = ["label"]
            break
    plan_path.write_text(json.dumps(plan, indent=2), encoding="utf-8")

    # B: change transform_id for v:sorted_high.label so kernel sees DERIVED_TRANSFORM_REF_CHANGED and var is impacted
    vars_path = tmp_path / "b" / "artifacts" / "vars.graph.json"
    vars_graph = json.loads(vars_path.read_text(encoding="utf-8"))
    for node in vars_graph.get("nodes", []):
        if node.get("id") == "v:sorted_high.label":
            node["transform_id"] = "different_transform_so_kernel_sees_change"
            break
    vars_path.write_text(json.dumps(vars_graph, indent=2), encoding="utf-8")

    _md, json_content = run_diff_from_bundles(tmp_path / "a", tmp_path / "b")
    report = json.loads(json_content)

    # Schema changes section reports column removal for that table
    schema_changes = report.get("schema_changes") or {}
    per_table = schema_changes.get("per_table") or []
    sorted_high_changes = next((t for t in per_table if t.get("table") == "sorted_high"), None)
    assert sorted_high_changes is not None
    assert "label" in sorted_high_changes.get("columns_removed", [])

    # Value evidence for that column: reason column_no_longer_exists, not value-changed
    impact_details = report.get("impact_details") or {}
    label_var_id = "v:sorted_high.label"
    if label_var_id in impact_details:
        value_ev = impact_details[label_var_id].get("value_evidence")
        if value_ev is not None:
            assert value_ev.get("failure_reason") == "column_no_longer_exists"
            assert value_ev.get("available") is False
    # If the var is not in impacted (e.g. kernel sees it as removed), at least no value-changed for it
    for var_id, detail in impact_details.items():
        if "sorted_high" in var_id and "label" in var_id:
            value_ev = detail.get("value_evidence")
            if value_ev is not None:
                assert value_ev.get("failure_reason") == "column_no_longer_exists"
                assert value_ev.get("available") is False


# --- 2) Schema lock contract change ---


def test_schema_lock_contract_change_reported(tmp_path: Path) -> None:
    """Lock diff: contract_changed true; schema_lock diff lists changed datasource and column."""
    _copy_demo_high_to(tmp_path / "a")
    _copy_demo_high_to(tmp_path / "b")

    # B: change one column type in schema.lock.json (e.g. USUBJID string -> int)
    lock_path = tmp_path / "b" / "schema.lock.json"
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    for ds in lock.get("datasources", []):
        if ds.get("name") == "lb":
            for col in ds.get("columns", []):
                if col.get("name") == "USUBJID":
                    col["type"] = "int"
                    break
            break
    lock_path.write_text(json.dumps(lock, indent=2), encoding="utf-8")

    # B: remove report hash so cheshbon recomputes from file
    report_path = tmp_path / "b" / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report.pop("schema_lock_sha256", None)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    _md, json_content = run_diff_from_bundles(tmp_path / "a", tmp_path / "b")
    report = json.loads(json_content)

    assert "schema_lock" in report
    assert report["schema_lock"].get("contract_changed") is True
    assert "lb" in (report["schema_lock"].get("datasources_changed") or [])
    per_ds = report["schema_lock"].get("per_datasource") or []
    lb_diff = next((d for d in per_ds if d.get("datasource") == "lb"), None)
    assert lb_diff is not None
    type_changes = lb_diff.get("types_changed") or []
    assert any(t.get("column") == "USUBJID" for t in type_changes)

    assert report.get("contract_changed") is True


# --- 3) Lock hash integrity preference ---


def test_lock_hash_integrity_no_false_contract_change(tmp_path: Path) -> None:
    """Wrong report.schema_lock_sha256: computed hash used; no false contract change when diffing bundle to itself."""
    _copy_demo_high_to(tmp_path / "bundle")
    report_path = tmp_path / "bundle" / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["schema_lock_sha256"] = "0" * 64
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    _md, json_content = run_diff_from_bundles(tmp_path / "bundle", tmp_path / "bundle")
    report = json.loads(json_content)

    assert "schema_lock" in report
    # Same bundle twice: must not report contract changed (computed hashes match)
    assert report["schema_lock"].get("contract_changed") is False
    assert report.get("contract_changed") is not True


# --- 4) Refused bundle with schema artifacts ---


def test_schema_lock_created_by_ignored_contract_unchanged(tmp_path: Path) -> None:
    """Two locks differing only in created_by.git_sha must not set contract_changed; optional provenance_changed true."""
    _copy_demo_high_to(tmp_path / "a")
    _copy_demo_high_to(tmp_path / "b")

    lock_path_b = tmp_path / "b" / "schema.lock.json"
    lock_b = json.loads(lock_path_b.read_text(encoding="utf-8"))
    created = lock_b.get("created_by") or {}
    created["git_sha"] = "different_sha_for_provenance_only"
    lock_b["created_by"] = created
    lock_path_b.write_text(json.dumps(lock_b, indent=2), encoding="utf-8")

    _md, json_content = run_diff_from_bundles(tmp_path / "a", tmp_path / "b")
    report = json.loads(json_content)

    assert "schema_lock" in report
    assert report["schema_lock"].get("contract_changed") is False
    assert report.get("contract_changed") is not True
    assert report["schema_lock"].get("datasources_changed") in (None, [])
    per_ds = report["schema_lock"].get("per_datasource") or []
    assert len(per_ds) == 0
    if report["schema_lock"].get("lock_hash_a") and report["schema_lock"].get("lock_hash_b"):
        assert report["schema_lock"]["lock_hash_a"] == report["schema_lock"]["lock_hash_b"]
    assert report["schema_lock"].get("provenance_changed") is True


def test_refused_bundle_with_schema_artifacts_surfaces_refusal_and_schema_sections(tmp_path: Path) -> None:
    """Refused bundle B with valid schema.lock and schema.evidence: refusal surfaced; schema sections still rendered; diffs skipped."""
    _copy_demo_high_to(tmp_path / "a")
    _copy_demo_high_to(tmp_path / "b")

    report_path = tmp_path / "b" / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["status"] = "refused"
    report["primary_error"] = {
        "code": "E_SCHEMA_REQUIRED",
        "message": "Schema lock required",
        "loc": None,
    }
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    _md, json_content = run_diff_from_bundles(tmp_path / "a", tmp_path / "b")
    report = json.loads(json_content)

    assert "refusal_info" in report
    assert "b" in report["refusal_info"]
    assert report["refusal_info"]["b"]["code"] == "E_SCHEMA_REQUIRED"

    assert report.get("impacted") == []

    assert "schema_lock" in report
    assert "schema_changes" in report or "schema_lock" in report

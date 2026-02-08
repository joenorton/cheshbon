"""Tests for run-diff (SANS bundle adapter into kernel pipeline)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import cheshbon.api as kernel_api
from cheshbon.adapters.sans_bundle import normalize_vars_graph, run_diff_from_bundles


def _write_bundle(
    root: Path,
    plan: dict,
    vars_graph: dict,
    report: dict | None = None,
    registry: dict | None = None,
    registry_candidate: dict | None = None,
    runtime_evidence: dict | None = None,
) -> Path:
    (root / "artifacts").mkdir(parents=True, exist_ok=True)
    (root / "artifacts" / "plan.ir.json").write_text(
        json.dumps(plan, ensure_ascii=False), encoding="utf-8"
    )
    (root / "artifacts" / "vars.graph.json").write_text(
        json.dumps(vars_graph, ensure_ascii=False), encoding="utf-8"
    )
    if registry is not None:
        (root / "transform_registry.json").write_text(
            json.dumps(registry, ensure_ascii=False), encoding="utf-8"
        )
    if registry_candidate is not None:
        (root / "artifacts" / "registry.candidate.json").write_text(
            json.dumps(registry_candidate, ensure_ascii=False), encoding="utf-8"
        )
    if runtime_evidence is not None:
        (root / "artifacts" / "runtime.evidence.json").write_text(
            json.dumps(runtime_evidence, ensure_ascii=False), encoding="utf-8"
        )
    report_payload = report or {"plan_path": "artifacts/plan.ir.json"}
    (root / "report.json").write_text(
        json.dumps(report_payload, ensure_ascii=False), encoding="utf-8"
    )
    return root


def _base_plan(step_outputs: list[str]) -> dict:
    return {
        "steps": [
            {
                "kind": "op",
                "op": "compute",
                "inputs": ["source"],
                "outputs": step_outputs,
                "params": {},
                "transform_id": "tx1",
                "step_id": "step1",
            }
        ],
        "tables": ["source"],
    }


def _vars_graph(
    expr_sha: str,
    transform_id: str = "tx1",
    edges: list[dict] | None = None,
) -> dict:
    return {
        "nodes": [
            {
                "id": "v:t1.a",
                "table_id": "t1",
                "col": "a",
                "origin": "derived",
                "expr_sha256": expr_sha,
                "payload_sha256": "p1",
                "producing_step_id": "step1",
                "transform_id": transform_id,
            }
        ],
        "edges": edges or [],
    }


def _registry_payload(transform_ids: list[str]) -> dict:
    transforms = []
    for transform_id in transform_ids:
        transforms.append(
            {
                "id": f"t:{transform_id}",
                "version": "0.1.0",
                "kind": "external_sas",
                "signature": {"inputs": ["unknown"], "output": "unknown"},
                "params_schema_hash": None,
                "impl_fingerprint": {
                    "algo": "sha256",
                    "source": "external_sas",
                    "ref": transform_id,
                    "digest": "a" * 64,
                },
            }
        )
    return {"registry_version": "1.0.0", "transforms": transforms}


def _registry_candidate_compute(transform_id: str, assignments: list[dict], mode: str = "derive") -> dict:
    return {
        "registry_version": "0.1",
        "index": {"0": transform_id},
        "transforms": [
            {
                "transform_id": transform_id,
                "kind": "compute",
                "spec": {
                    "op": "compute",
                    "params": {
                        "mode": mode,
                        "assignments": assignments,
                    },
                },
            }
        ],
    }


def _runtime_evidence_with_column_stats(table: str, column: str, stats: dict) -> dict:
    return {
        "sans_version": "0.1.0",
        "run_id": "test-run",
        "created_at": "2026-02-01T00:00:00Z",
        "plan_ir": {"path": "artifacts/plan.ir.json", "sha256": "0" * 64},
        "bindings": {},
        "inputs": [],
        "outputs": [],
        "tables": {
            table: {
                "columns": {
                    column: stats
                }
            }
        },
    }


def _runtime_evidence_with_output_stats(table: str, column: str, stats: dict) -> dict:
    return {
        "sans_version": "0.1.0",
        "run_id": "test-run",
        "created_at": "2026-02-01T00:00:00Z",
        "plan_ir": {"path": "artifacts/plan.ir.json", "sha256": "0" * 64},
        "bindings": {},
        "inputs": [],
        "outputs": [
            {
                "name": table,
                "path": f"outputs/{table}.csv",
                "column_stats": {
                    column: stats
                },
            }
        ],
    }


def test_run_diff_uses_kernel(monkeypatch, tmp_path: Path):
    bundle_a = _write_bundle(tmp_path / "a", _base_plan(["t1"]), _vars_graph("e1"))
    bundle_b = _write_bundle(tmp_path / "b", _base_plan(["t1"]), _vars_graph("e1"))

    calls = []
    original = kernel_api._diff_internal

    def wrapper(*args, **kwargs):
        calls.append((args, kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(kernel_api, "_diff_internal", wrapper)

    run_diff_from_bundles(bundle_a, bundle_b)

    assert len(calls) == 1
    _, kwargs = calls[0]
    assert isinstance(kwargs["from_spec"], dict)
    assert isinstance(kwargs["to_spec"], dict)


def test_run_diff_no_heuristic_impact(tmp_path: Path):
    bundle_a = _write_bundle(tmp_path / "a", _base_plan(["t1"]), _vars_graph("e1"))
    bundle_b = _write_bundle(tmp_path / "b", _base_plan(["t1"]), _vars_graph("e2"))

    _md, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    impacted = report["impacted"]
    assert impacted == ["v:t1.a"]


def test_run_diff_plan_wiring_changes_are_detected(tmp_path: Path):
    bundle_a = _write_bundle(tmp_path / "a", _base_plan(["t1"]), _vars_graph("e1"))
    bundle_b = _write_bundle(tmp_path / "b", _base_plan(["t1_alt"]), _vars_graph("e1"))

    _md, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    change_types = [event["change_type"] for event in report["change_events"]]
    assert "DERIVED_TRANSFORM_PARAMS_CHANGED" in change_types


def test_run_diff_registry_impl_change():
    bundle_a = Path("fixtures/run_diff/ex1")
    bundle_b = Path("fixtures/run_diff/ex2")

    _md, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    change_types = [event["change_type"] for event in report["change_events"]]
    assert "TRANSFORM_IMPL_CHANGED" in change_types


def test_vars_graph_edge_kinds_flow():
    vars_graph_path = Path("fixtures/graph_diff/ex1/artifacts/vars.graph.json")
    payload = json.loads(vars_graph_path.read_text(encoding="utf-8"))

    normalized = normalize_vars_graph(payload)
    edge_kinds = {(e["src"], e["dst"]): e["kind"] for e in normalized["edges"]}

    assert edge_kinds[("v:high_value__1.label", "v:high_value__2.label")] == "flow"
    assert edge_kinds[("v:high_value__2.label", "v:high_value.label")] == "flow"
    assert edge_kinds[("v:high_value.label", "v:sorted_high.label")] == "flow"


def test_vars_graph_edge_kinds_rename_ingest(tmp_path: Path):
    plan = _base_plan(["t1"])
    vars_graph = {
        "nodes": [
            {
                "id": "v:t1.a",
                "table_id": "t1",
                "col": "a",
                "origin": "derived",
                "expr_sha256": "e1",
                "payload_sha256": "p1",
                "producing_step_id": "step1",
                "transform_id": "tx1",
            },
            {
                "id": "v:t1.b",
                "table_id": "t1",
                "col": "b",
                "origin": "derived",
                "expr_sha256": "e2",
                "payload_sha256": "p2",
                "producing_step_id": "step1",
                "transform_id": "tx1",
            },
        ],
        "edges": [
            {"src": "v:t1.a", "dst": "v:t1.b", "kind": "rename"},
        ],
    }

    bundle_a = _write_bundle(tmp_path / "a", plan, vars_graph)
    bundle_b = _write_bundle(tmp_path / "b", plan, vars_graph)

    _md, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    assert report["run_status"] == "no_impact"


def test_run_diff_transform_ref_change_no_missing_reason(tmp_path: Path):
    plan = _base_plan(["t1"])
    vars_graph_a = _vars_graph("e1", transform_id="old")
    vars_graph_b = _vars_graph("e1", transform_id="new")

    bundle_a = _write_bundle(
        tmp_path / "a",
        plan,
        vars_graph_a,
        registry=_registry_payload(["old"]),
    )
    bundle_b = _write_bundle(
        tmp_path / "b",
        plan,
        vars_graph_b,
        registry=_registry_payload(["new"]),
    )

    _md, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    details = report["impact_details"]["v:t1.a"]
    assert details["reason"] != "MISSING_TRANSFORM_REF"
    assert details["reason"] == "DIRECT_CHANGE"
    assert report["validation_findings"]["a"] == []
    assert report["validation_findings"]["b"] == []


def test_run_diff_transform_ref_change_renders_assignment(tmp_path: Path):
    plan = _base_plan(["t1"])
    vars_graph_a = {
        "nodes": [
            {
                "id": "v:t1.label",
                "table_id": "t1",
                "col": "label",
                "origin": "derived",
                "expr_sha256": "e1",
                "payload_sha256": "p1",
                "producing_step_id": "step1",
                "transform_id": "old",
            }
        ],
        "edges": [],
    }
    vars_graph_b = {
        "nodes": [
            {
                "id": "v:t1.label",
                "table_id": "t1",
                "col": "label",
                "origin": "derived",
                "expr_sha256": "e2",
                "payload_sha256": "p1",
                "producing_step_id": "step1",
                "transform_id": "new",
            }
        ],
        "edges": [],
    }

    registry_a = _registry_candidate_compute(
        "old",
        [{"target": "label", "expr": {"type": "lit", "value": "HIGH"}}],
        mode="derive",
    )
    registry_b = _registry_candidate_compute(
        "new",
        [{"target": "label", "expr": {"type": "lit", "value": "LOW"}}],
        mode="derive",
    )

    bundle_a = _write_bundle(tmp_path / "a", plan, vars_graph_a, registry_candidate=registry_a)
    bundle_b = _write_bundle(tmp_path / "b", plan, vars_graph_b, registry_candidate=registry_b)

    md_content, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    event = next(
        e for e in report["change_events"] if e["change_type"] == "DERIVED_TRANSFORM_REF_CHANGED"
    )
    details = event["details"]
    assert details["old_render"] == 'label = "HIGH"'
    assert details["new_render"] == 'label = "LOW"'
    assert details["mode"] == "derive"

    assert "- `v:t1.label`: `t:old` -> `t:new`" in md_content
    assert "  - old: `label = \"HIGH\"` (derive)" in md_content
    assert "  - new: `label = \"LOW\"` (derive)" in md_content


def test_run_diff_transform_ref_missing_transform_is_placeholder(tmp_path: Path):
    plan = _base_plan(["t1"])
    vars_graph_a = _vars_graph("e1", transform_id="old")
    vars_graph_b = _vars_graph("e1", transform_id="new")

    registry_b = _registry_candidate_compute(
        "new",
        [{"target": "a", "expr": {"type": "lit", "value": "LOW"}}],
        mode="derive",
    )

    bundle_a = _write_bundle(tmp_path / "a", plan, vars_graph_a)
    bundle_b = _write_bundle(tmp_path / "b", plan, vars_graph_b, registry_candidate=registry_b)

    _md, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    event = next(
        e for e in report["change_events"] if e["change_type"] == "DERIVED_TRANSFORM_REF_CHANGED"
    )
    details = event["details"]
    assert details["old_render"] == "(transform not found: t:old)"


def test_run_diff_transform_ref_no_assignment_for_target(tmp_path: Path):
    plan = _base_plan(["t1"])
    vars_graph_a = _vars_graph("e1", transform_id="old")
    vars_graph_b = _vars_graph("e1", transform_id="new")

    registry_a = _registry_candidate_compute(
        "old",
        [{"target": "other", "expr": {"type": "lit", "value": "HIGH"}}],
        mode="derive",
    )
    registry_b = _registry_candidate_compute(
        "new",
        [{"target": "other", "expr": {"type": "lit", "value": "LOW"}}],
        mode="derive",
    )

    bundle_a = _write_bundle(tmp_path / "a", plan, vars_graph_a, registry_candidate=registry_a)
    bundle_b = _write_bundle(tmp_path / "b", plan, vars_graph_b, registry_candidate=registry_b)

    _md, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    event = next(
        e for e in report["change_events"] if e["change_type"] == "DERIVED_TRANSFORM_REF_CHANGED"
    )
    details = event["details"]
    assert details["old_render"] == "(no assignment for target=a)"


def test_run_diff_transform_added_removed_rendered_in_md(tmp_path: Path):
    plan = _base_plan(["t1"])
    vars_graph = _vars_graph("e1", transform_id="old")

    registry_a = _registry_payload(["old"])
    registry_b = _registry_payload(["new"])

    registry_candidate_a = {
        "registry_version": "0.1",
        "index": {"0": "old"},
        "transforms": [
            {
                "transform_id": "old",
                "kind": "filter",
                "spec": {
                    "op": "filter",
                    "params": {
                        "predicate": {
                            "left": {"name": "value", "type": "col"},
                            "op": ">",
                            "right": {"type": "lit", "value": 250},
                            "type": "binop",
                        }
                    },
                },
            }
        ],
    }
    registry_candidate_b = {
        "registry_version": "0.1",
        "index": {"0": "new"},
        "transforms": [
            {
                "transform_id": "new",
                "kind": "select",
                "spec": {"op": "select", "params": {"cols": ["name", "value"]}},
            }
        ],
    }

    bundle_a = _write_bundle(
        tmp_path / "a",
        plan,
        vars_graph,
        registry=registry_a,
        registry_candidate=registry_candidate_a,
    )
    bundle_b = _write_bundle(
        tmp_path / "b",
        plan,
        vars_graph,
        registry=registry_b,
        registry_candidate=registry_candidate_b,
    )

    md_content, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    change_types = [event["change_type"] for event in report["change_events"]]
    assert "TRANSFORM_ADDED" in change_types
    assert "TRANSFORM_REMOVED" in change_types

    assert "### Transform Added" in md_content
    assert "  - spec: `select name, value`" in md_content
    assert "### Transform Removed" in md_content
    assert "  - spec: `filter(value > 250)`" in md_content


def test_run_diff_missing_transform_side_a_only(tmp_path: Path):
    plan = _base_plan(["t1"])
    vars_graph_a = _vars_graph("e1", transform_id="old")
    vars_graph_b = _vars_graph("e1", transform_id="new")

    bundle_a = _write_bundle(
        tmp_path / "a",
        plan,
        vars_graph_a,
        registry=_registry_payload([]),
    )
    bundle_b = _write_bundle(
        tmp_path / "b",
        plan,
        vars_graph_b,
        registry=_registry_payload(["new"]),
    )

    _md, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    findings_a = report["validation_findings"]["a"]
    findings_b = report["validation_findings"]["b"]

    assert any(
        f["element_id"] == "v:t1.a" and f["transform_id"] == "t:old" and f["side"] == "a"
        for f in findings_a
    )
    assert findings_b == []


def test_run_diff_paths_include_edge_kinds(tmp_path: Path):
    plan = _base_plan(["t1"])
    vars_graph_a = {
        "nodes": [
            {
                "id": "v:t1.a",
                "table_id": "t1",
                "col": "a",
                "origin": "derived",
                "expr_sha256": "e1",
                "payload_sha256": "p1",
                "producing_step_id": "step1",
                "transform_id": "tx1",
            },
            {
                "id": "v:t1.b",
                "table_id": "t1",
                "col": "b",
                "origin": "derived",
                "expr_sha256": "e2",
                "payload_sha256": "p2",
                "producing_step_id": "step1",
                "transform_id": "tx1",
            },
        ],
        "edges": [
            {"src": "v:t1.a", "dst": "v:t1.b", "kind": "derivation"},
        ],
    }
    vars_graph_b = {
        "nodes": [
            {
                "id": "v:t1.a",
                "table_id": "t1",
                "col": "a",
                "origin": "derived",
                "expr_sha256": "e3",
                "payload_sha256": "p1",
                "producing_step_id": "step1",
                "transform_id": "tx1",
            },
            {
                "id": "v:t1.b",
                "table_id": "t1",
                "col": "b",
                "origin": "derived",
                "expr_sha256": "e2",
                "payload_sha256": "p2",
                "producing_step_id": "step1",
                "transform_id": "tx1",
            },
        ],
        "edges": [
            {"src": "v:t1.a", "dst": "v:t1.b", "kind": "derivation"},
        ],
    }

    bundle_a = _write_bundle(tmp_path / "a", plan, vars_graph_a)
    bundle_b = _write_bundle(tmp_path / "b", plan, vars_graph_b)

    _md, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    path = report["impact_details"]["v:t1.b"]["path"]
    assert path == [{"src": "v:t1.a", "dst": "v:t1.b", "kind": "derivation"}]


def test_run_diff_paths_include_rename_edge_kinds(tmp_path: Path):
    plan = _base_plan(["t1"])
    vars_graph_a = {
        "nodes": [
            {
                "id": "v:t1.a",
                "table_id": "t1",
                "col": "a",
                "origin": "derived",
                "expr_sha256": "e1",
                "payload_sha256": "p1",
                "producing_step_id": "step1",
                "transform_id": "tx1",
            },
            {
                "id": "v:t1.b",
                "table_id": "t1",
                "col": "b",
                "origin": "derived",
                "expr_sha256": "e2",
                "payload_sha256": "p2",
                "producing_step_id": "step1",
                "transform_id": "tx1",
            },
        ],
        "edges": [
            {"src": "v:t1.a", "dst": "v:t1.b", "kind": "rename"},
        ],
    }
    vars_graph_b = {
        "nodes": [
            {
                "id": "v:t1.a",
                "table_id": "t1",
                "col": "a",
                "origin": "derived",
                "expr_sha256": "e3",
                "payload_sha256": "p1",
                "producing_step_id": "step1",
                "transform_id": "tx1",
            },
            {
                "id": "v:t1.b",
                "table_id": "t1",
                "col": "b",
                "origin": "derived",
                "expr_sha256": "e2",
                "payload_sha256": "p2",
                "producing_step_id": "step1",
                "transform_id": "tx1",
            },
        ],
        "edges": [
            {"src": "v:t1.a", "dst": "v:t1.b", "kind": "rename"},
        ],
    }

    bundle_a = _write_bundle(tmp_path / "a", plan, vars_graph_a)
    bundle_b = _write_bundle(tmp_path / "b", plan, vars_graph_b)

    _md, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    path = report["impact_details"]["v:t1.b"]["path"]
    assert path == [{"src": "v:t1.a", "dst": "v:t1.b", "kind": "rename"}]


def test_run_diff_value_evidence_constant_flip(tmp_path: Path):
    plan = _base_plan(["high_value__1"])
    vars_graph_a = {
        "nodes": [
            {
                "id": "v:high_value__1.label",
                "table_id": "high_value__1",
                "col": "label",
                "origin": "derived",
                "expr_sha256": "e1",
                "payload_sha256": "p1",
                "producing_step_id": "step1",
                "transform_id": "tx1",
            }
        ],
        "edges": [],
    }
    vars_graph_b = {
        "nodes": [
            {
                "id": "v:high_value__1.label",
                "table_id": "high_value__1",
                "col": "label",
                "origin": "derived",
                "expr_sha256": "e2",
                "payload_sha256": "p1",
                "producing_step_id": "step1",
                "transform_id": "tx1",
            }
        ],
        "edges": [],
    }

    evidence_a = _runtime_evidence_with_column_stats(
        "high_value__1",
        "label",
        {"unique_count": 1, "null_count": 0, "constant_value": "HIGH"},
    )
    evidence_b = _runtime_evidence_with_column_stats(
        "high_value__1",
        "label",
        {"unique_count": 1, "null_count": 0, "constant_value": "LOW"},
    )

    bundle_a = _write_bundle(tmp_path / "a", plan, vars_graph_a, runtime_evidence=evidence_a)
    bundle_b = _write_bundle(tmp_path / "b", plan, vars_graph_b, runtime_evidence=evidence_b)

    _md, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    evidence = report["impact_details"]["v:high_value__1.label"]["value_evidence"]
    assert evidence["available"] is True
    assert evidence["kind"] == "column_constant"
    assert evidence["summary"] == "\"HIGH\" -> \"LOW\""


def test_run_diff_value_evidence_missing(tmp_path: Path):
    bundle_a = _write_bundle(tmp_path / "a", _base_plan(["t1"]), _vars_graph("e1"))
    bundle_b = _write_bundle(tmp_path / "b", _base_plan(["t1"]), _vars_graph("e2"))

    _md, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    evidence = report["impact_details"]["v:t1.a"]["value_evidence"]
    assert evidence["available"] is False
    assert evidence["kind"] == "unknown"
    assert evidence["failure_reason"] == "runtime_evidence_missing"


def test_run_diff_value_evidence_non_constant(tmp_path: Path):
    plan = _base_plan(["t1"])
    vars_graph_a = _vars_graph("e1")
    vars_graph_b = _vars_graph("e2")

    evidence_a = _runtime_evidence_with_column_stats(
        "t1",
        "a",
        {"unique_count": 2, "null_count": 0, "top_values": ["HIGH", "LOW"], "top_counts": [2, 1]},
    )
    evidence_b = _runtime_evidence_with_column_stats(
        "t1",
        "a",
        {"unique_count": 3, "null_count": 0, "top_values": ["HIGH", "MED"], "top_counts": [2, 1]},
    )

    bundle_a = _write_bundle(tmp_path / "a", plan, vars_graph_a, runtime_evidence=evidence_a)
    bundle_b = _write_bundle(tmp_path / "b", plan, vars_graph_b, runtime_evidence=evidence_b)

    _md, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    evidence = report["impact_details"]["v:t1.a"]["value_evidence"]
    assert evidence["available"] is True
    assert evidence["kind"] == "column_stats"
    assert "unique" in evidence["summary"]
    assert evidence["summary"] != "\"HIGH\" -> \"LOW\""


def test_run_diff_value_evidence_tables_constant_flip_intermediate(tmp_path: Path):
    plan = _base_plan(["__t6__"])
    vars_graph_a = {
        "nodes": [
            {
                "id": "v:__t6__.label",
                "table_id": "__t6__",
                "col": "label",
                "origin": "derived",
                "expr_sha256": "e1",
                "payload_sha256": "p1",
                "producing_step_id": "step1",
                "transform_id": "tx1",
            }
        ],
        "edges": [],
    }
    vars_graph_b = {
        "nodes": [
            {
                "id": "v:__t6__.label",
                "table_id": "__t6__",
                "col": "label",
                "origin": "derived",
                "expr_sha256": "e2",
                "payload_sha256": "p1",
                "producing_step_id": "step1",
                "transform_id": "tx1",
            }
        ],
        "edges": [],
    }

    evidence_a = _runtime_evidence_with_column_stats(
        "__t6__",
        "label",
        {"unique_count": 1, "null_count": 0, "constant_value": "HIGH"},
    )
    evidence_b = _runtime_evidence_with_column_stats(
        "__t6__",
        "label",
        {"unique_count": 1, "null_count": 0, "constant_value": "LOW"},
    )

    bundle_a = _write_bundle(tmp_path / "a", plan, vars_graph_a, runtime_evidence=evidence_a)
    bundle_b = _write_bundle(tmp_path / "b", plan, vars_graph_b, runtime_evidence=evidence_b)

    _md, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    evidence = report["impact_details"]["v:__t6__.label"]["value_evidence"]
    assert evidence["available"] is True
    assert evidence["kind"] == "column_constant"
    assert evidence["summary"] == "\"HIGH\" -> \"LOW\""


def test_run_diff_value_evidence_missing_table_reason(tmp_path: Path):
    plan = _base_plan(["t1"])
    vars_graph_a = _vars_graph("e1")
    vars_graph_b = _vars_graph("e2")

    evidence_a = {
        "sans_version": "0.1.0",
        "run_id": "test-run",
        "created_at": "2026-02-01T00:00:00Z",
        "plan_ir": {"path": "artifacts/plan.ir.json", "sha256": "0" * 64},
        "bindings": {},
        "inputs": [],
        "outputs": [],
        "tables": {
            "t1": {
                "columns": {}
            }
        },
    }
    evidence_b = {
        "sans_version": "0.1.0",
        "run_id": "test-run",
        "created_at": "2026-02-01T00:00:00Z",
        "plan_ir": {"path": "artifacts/plan.ir.json", "sha256": "0" * 64},
        "bindings": {},
        "inputs": [],
        "outputs": [],
        "tables": {
            "t1": {
                "columns": {}
            }
        },
    }

    bundle_a = _write_bundle(tmp_path / "a", plan, vars_graph_a, runtime_evidence=evidence_a)
    bundle_b = _write_bundle(tmp_path / "b", plan, vars_graph_b, runtime_evidence=evidence_b)

    _md, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    evidence = report["impact_details"]["v:t1.a"]["value_evidence"]
    assert evidence["available"] is False
    assert evidence["failure_reason"] == "column_not_found"


def test_run_diff_value_evidence_outputs_fallback(tmp_path: Path):
    plan = _base_plan(["t1"])
    vars_graph_a = _vars_graph("e1")
    vars_graph_b = _vars_graph("e2")

    evidence_a = _runtime_evidence_with_output_stats(
        "t1",
        "a",
        {"unique_count": 1, "null_count": 0, "constant_value": "HIGH"},
    )
    evidence_b = _runtime_evidence_with_output_stats(
        "t1",
        "a",
        {"unique_count": 1, "null_count": 0, "constant_value": "LOW"},
    )

    bundle_a = _write_bundle(tmp_path / "a", plan, vars_graph_a, runtime_evidence=evidence_a)
    bundle_b = _write_bundle(tmp_path / "b", plan, vars_graph_b, runtime_evidence=evidence_b)

    _md, json_content = run_diff_from_bundles(bundle_a, bundle_b)
    report = json.loads(json_content)

    evidence = report["impact_details"]["v:t1.a"]["value_evidence"]
    assert evidence["available"] is True
    assert evidence["kind"] == "column_constant"
    assert evidence["summary"] == "\"HIGH\" -> \"LOW\""


def test_run_diff_demo_high_schema_sections():
    """Run-diff with reference bundle fixtures/demo_high (see fixtures/demo_high): schema_lock and schema_changes sections present."""
    bundle_dir = Path(__file__).resolve().parent.parent / "fixtures" / "demo_high" / "dh_out"
    if not bundle_dir.exists():
        import pytest
        pytest.skip("fixtures/demo_high/dh_out not found")
    _md, json_content = run_diff_from_bundles(bundle_dir, bundle_dir)
    report = json.loads(json_content)
    assert "schema_lock" in report
    assert report["schema_lock"]["lock_used_a"] is True
    assert report["schema_lock"]["lock_used_b"] is True
    assert report["schema_lock"].get("contract_changed") is False
    assert "lock_hash_a" in report["schema_lock"]
    assert "lock_hash_b" in report["schema_lock"]


def test_run_diff_refused_bundle_surfaces_refusal_and_skips_diffs(tmp_path: Path):
    """When report.status is refused, output surfaces refusal and skips kernel/value diffs."""
    report_refused_a = {
        "plan_path": "artifacts/plan.ir.json",
        "report_schema_version": "0.3",
        "status": "refused",
        "primary_error": {
            "code": "E_SCHEMA_REQUIRED",
            "message": "Schema lock required",
            "loc": None,
        },
        "artifacts": [],
    }
    report_ok_b = {
        "plan_path": "artifacts/plan.ir.json",
        "report_schema_version": "0.3",
        "status": "ok",
        "primary_error": None,
        "artifacts": [{"name": "plan.ir.json", "path": "artifacts/plan.ir.json", "sha256": "0" * 64}, {"name": "vars.graph.json", "path": "artifacts/vars.graph.json", "sha256": "0" * 64}, {"name": "registry.candidate.json", "path": "artifacts/registry.candidate.json", "sha256": "0" * 64}],
    }
    plan = {"steps": [], "tables": ["source"]}
    vars_graph = {"nodes": [], "edges": []}
    registry = {"registry_version": "0.1", "index": {}, "transforms": []}
    (tmp_path / "a" / "artifacts").mkdir(parents=True, exist_ok=True)
    (tmp_path / "a" / "report.json").write_text(json.dumps(report_refused_a), encoding="utf-8")
    (tmp_path / "b" / "artifacts").mkdir(parents=True, exist_ok=True)
    (tmp_path / "b" / "report.json").write_text(json.dumps(report_ok_b), encoding="utf-8")
    (tmp_path / "b" / "artifacts" / "plan.ir.json").write_text(json.dumps(plan), encoding="utf-8")
    (tmp_path / "b" / "artifacts" / "vars.graph.json").write_text(json.dumps(vars_graph), encoding="utf-8")
    (tmp_path / "b" / "artifacts" / "registry.candidate.json").write_text(json.dumps(registry), encoding="utf-8")
    _md, json_content = run_diff_from_bundles(tmp_path / "a", tmp_path / "b")
    report = json.loads(json_content)
    assert "refusal_info" in report
    assert "a" in report["refusal_info"]
    assert report["refusal_info"]["a"]["code"] == "E_SCHEMA_REQUIRED"
    assert report["impacted"] == []


def test_run_diff_thin_vs_thin():
    """Run-diff two thin bundles (no inputs/data/) produces impact/diff without errors."""
    bundle_low = Path("fixtures/demo_low/dl_out")
    bundle_high = Path("fixtures/demo_high/dh_out")
    if not bundle_low.exists() or not bundle_high.exists():
        pytest.skip("demo_low/dl_out or demo_high/dh_out not found")
    _md, json_content = run_diff_from_bundles(bundle_low, bundle_high)
    report = json.loads(json_content)
    assert "impacted" in report
    assert "change_events" in report
    assert "schema_lock" in report or "refusal_info" in report


def test_run_diff_full_vs_thin():
    """Run-diff full bundle (graph_diff/ex1 has inputs/data/) vs thin bundle (dl_out) runs without errors."""
    bundle_full = Path("fixtures/graph_diff/ex1")
    bundle_thin = Path("fixtures/demo_low/dl_out")
    if not bundle_full.exists() or not bundle_thin.exists():
        pytest.skip("graph_diff/ex1 or demo_low/dl_out not found")
    _md, json_content = run_diff_from_bundles(bundle_full, bundle_thin)
    report = json.loads(json_content)
    assert "impacted" in report
    assert "change_events" in report

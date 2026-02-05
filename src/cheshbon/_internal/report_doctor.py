"""Report doctor: verify all-details artifacts without bundle concerns."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Set

from cheshbon._internal.canonical_json import canonical_dumps
from cheshbon._internal.report_contract import (
    ALL_DETAILS_SCHEMA_VERSION,
    VERIFIER_CONTRACT_VERSION,
    CANONICALIZATION_POLICY_ID,
)
from cheshbon.kernel.spec import MappingSpec
from cheshbon.kernel.transform_registry import TransformRegistry
from cheshbon.kernel.bindings import Bindings
from cheshbon.kernel.graph import DependencyGraph
from cheshbon.kernel.all_details_builders import build_issues_index
from cheshbon.api import diff


def _load_json(path: Union[str, Path]) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _digest_canonical(obj: Any) -> str:
    canonical = canonical_dumps(obj)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _load_spec(path: Union[str, Path]) -> MappingSpec:
    data = _load_json(path)
    return MappingSpec(**data)


def _load_registry(path: Union[str, Path]) -> TransformRegistry:
    data = _load_json(path)
    return TransformRegistry(**data)


def _load_bindings(path: Union[str, Path]) -> Bindings:
    data = _load_json(path)
    return Bindings(table=data["table"], bindings=data["bindings"])


def _bindings_digest(bindings: Bindings) -> str:
    return _digest_canonical({"table": bindings.table, "bindings": dict(bindings.bindings)})


def _core_subset_digest(diff_result: "DiffResult") -> str:
    core_subset = {
        "validation_failed": diff_result.validation_failed,
        "validation_errors": list(diff_result.validation_errors),
        "events": list(diff_result.events),
        "impacted_ids": list(diff_result.impacted_ids),
        "unaffected_ids": list(diff_result.unaffected_ids),
        "reasons": dict(diff_result.reasons),
        "missing_inputs": dict(diff_result.missing_inputs),
        "missing_bindings": dict(diff_result.missing_bindings),
        "missing_transform_refs": dict(diff_result.missing_transform_refs),
    }
    return _digest_canonical(core_subset)


ZERO_DISTANCE_REASONS = {
    "DIRECT_CHANGE",
    "DIRECT_CHANGE_MISSING_INPUT",
    "TRANSFORM_IMPL_CHANGED",
    "TRANSFORM_REMOVED",
    "MISSING_TRANSFORM_REF",
}
NO_EVENT_REASONS = {
    "MISSING_BINDING",
    "AMBIGUOUS_BINDING",
    "MISSING_TRANSFORM_REF",
}

DISTANCE_SAMPLE_N = 50
DISTANCE_SAMPLE_M = min(10, max(1, DISTANCE_SAMPLE_N // 5))
DISTANCE_RULE_ID = "v1:first_last_max_suspicious"


def _expected_witness_ids(diff_result: "DiffResult") -> List[str]:
    return sorted([var_id for var_id in diff_result.impacted_ids if var_id.startswith("d:")])


def _select_distance_ids(
    witnesses: Dict[str, Dict[str, Any]],
    mode: str,
    n: int = DISTANCE_SAMPLE_N,
    m: int = DISTANCE_SAMPLE_M,
) -> List[str]:
    ids = sorted(witnesses.keys())
    if mode == "strict":
        return ids

    must_check: Set[str] = set()
    max_distance = 0
    for var_id, witness in witnesses.items():
        reason = witness.get("reason")
        distance = witness.get("distance", 0)
        predecessor = witness.get("predecessor")
        if reason in ("DIRECT_CHANGE", "DIRECT_CHANGE_MISSING_INPUT"):
            must_check.add(var_id)
        if distance > max_distance:
            max_distance = distance
        if distance == 0 and reason not in ZERO_DISTANCE_REASONS:
            must_check.add(var_id)
        if distance > 0 and reason in ZERO_DISTANCE_REASONS:
            must_check.add(var_id)
        if distance == 0 and predecessor:
            must_check.add(var_id)

    selected: List[str] = sorted(must_check)

    # Include max-distance nodes up to M
    if max_distance > 0:
        max_distance_ids = sorted(
            [var_id for var_id, witness in witnesses.items() if witness.get("distance", 0) == max_distance]
        )
        for var_id in max_distance_ids:
            if var_id not in must_check and len(selected) < n + m:
                selected.append(var_id)
            if len(selected) >= len(must_check) + m:
                break

    # Fill remaining slots with first/last IDs
    remaining = [var_id for var_id in ids if var_id not in selected]
    left = 0
    right = len(remaining) - 1
    while len(selected) < n and left <= right:
        selected.append(remaining[left])
        left += 1
        if len(selected) >= n or left > right:
            break
        selected.append(remaining[right])
        right -= 1

    return selected


def run_doctor_report(
    report_path: Union[str, Path],
    spec_v1_path: Union[str, Path],
    spec_v2_path: Union[str, Path],
    registry_v1_path: Optional[Union[str, Path]] = None,
    registry_v2_path: Optional[Union[str, Path]] = None,
    bindings_path: Optional[Union[str, Path]] = None,
    raw_schema_path: Optional[Union[str, Path]] = None,
    distance_check_mode: str = "sample",
) -> Dict[str, Any]:
    clauses: List[Dict[str, Any]] = []

    # Read report
    try:
        report = _load_json(report_path)
        clauses.append({"id": "report_read", "ok": True, "details": {}})
    except Exception as e:
        clauses.append({"id": "report_read", "ok": False, "details": {"error": str(e)}})
        return {
            "ok": False,
            "clauses": clauses,
            "summary": {
                "total_clauses": len(clauses),
                "ok_clauses": sum(1 for c in clauses if c["ok"]),
                "failed_clauses": sum(1 for c in clauses if not c["ok"]),
                "failed_clause_ids": [c["id"] for c in clauses if not c["ok"]],
            },
        }

    # Header contract
    header_ok = True
    header_details: Dict[str, Any] = {}
    if report.get("report_schema_version") != ALL_DETAILS_SCHEMA_VERSION:
        header_ok = False
        header_details["report_schema_version"] = report.get("report_schema_version")
    if report.get("verifier_contract_version") != VERIFIER_CONTRACT_VERSION:
        header_ok = False
        header_details["verifier_contract_version"] = report.get("verifier_contract_version")
    if report.get("canonicalization_policy_id") != CANONICALIZATION_POLICY_ID:
        header_ok = False
        header_details["canonicalization_policy_id"] = report.get("canonicalization_policy_id")
    clauses.append({"id": "header_contract", "ok": header_ok, "details": header_details})

    # Input digests
    inputs_ok = True
    inputs_details: Dict[str, Any] = {}
    inputs = report.get("inputs", {})
    try:
        expected_inputs = {
            "spec_v1": _digest_canonical(_load_spec(spec_v1_path).model_dump()),
            "spec_v2": _digest_canonical(_load_spec(spec_v2_path).model_dump()),
            "registry_v1": _digest_canonical(_load_registry(registry_v1_path).model_dump()) if registry_v1_path else None,
            "registry_v2": _digest_canonical(_load_registry(registry_v2_path).model_dump()) if registry_v2_path else None,
            "bindings_v2": _bindings_digest(_load_bindings(bindings_path)) if bindings_path else None,
            "raw_schema_v2": _digest_canonical(_load_json(raw_schema_path)) if raw_schema_path else None,
        }
        for key, expected_digest in expected_inputs.items():
            reported = inputs.get(key)
            reported_digest = reported.get("digest") if isinstance(reported, dict) else None
            if expected_digest != reported_digest:
                inputs_ok = False
                inputs_details[key] = {"expected": expected_digest, "reported": reported_digest}
    except Exception as e:
        inputs_ok = False
        inputs_details["error"] = str(e)
    clauses.append({"id": "inputs_digest", "ok": inputs_ok, "details": inputs_details})

    # Core digest (reuse diff_result across checks)
    diff_result = None
    core_ok = True
    core_details: Dict[str, Any] = {}
    try:
        diff_result = diff(
            from_spec=spec_v1_path,
            to_spec=spec_v2_path,
            from_registry=registry_v1_path,
            to_registry=registry_v2_path,
            to_bindings=bindings_path,
            detail_level="core",
        )
        expected_core = _core_subset_digest(diff_result)
        reported_core = report.get("core_digest")
        if expected_core != reported_core:
            core_ok = False
            core_details["expected"] = expected_core
            core_details["reported"] = reported_core
    except Exception as e:
        core_ok = False
        core_details["error"] = str(e)
    clauses.append({"id": "core_digest", "ok": core_ok, "details": core_details})

    # Witness invariants
    witness_ok = True
    witness_details: Dict[str, Any] = {}
    try:
        if diff_result is None:
            raise ValueError("diff_result unavailable for witness verification")

        spec_v1 = _load_spec(spec_v1_path)
        spec_v2 = _load_spec(spec_v2_path)
        graph_v1 = DependencyGraph(spec_v1)
        graph_v2 = DependencyGraph(spec_v2)

        witnesses = report.get("details", {}).get("witnesses", {})
        expected_witness_ids = set(_expected_witness_ids(diff_result))
        witness_ids = set(witnesses.keys())
        if not witness_ids.issubset(expected_witness_ids):
            witness_ok = False
            witness_details["unexpected_witness_ids"] = sorted(witness_ids - expected_witness_ids)

        event_index = report.get("details", {}).get("event_index", [])
        issue_index = report.get("details", {}).get("issues_index", [])
        event_map = {e.get("event_id"): e for e in event_index if e.get("event_id")}
        issue_map = {i.get("issue_id"): i for i in issue_index if i.get("issue_id")}

        allowed_v1_reasons = {"MISSING_INPUT", "DIRECT_CHANGE_MISSING_INPUT", "TRANSITIVE_DEPENDENCY"}

        # Distance check selection
        mode = distance_check_mode if distance_check_mode in ("sample", "strict") else "sample"
        distance_ids = _select_distance_ids(witnesses, mode)
        distance_failed: List[str] = []

        for var_id, witness in witnesses.items():
            reason = witness.get("reason")
            expected_reason = diff_result.reasons.get(var_id)
            if expected_reason != reason:
                witness_ok = False
                witness_details.setdefault("reason_mismatch", []).append(var_id)

            root_ids = witness.get("root_cause_ids") or []
            predecessor = witness.get("predecessor")
            distance = witness.get("distance", 0)
            trig_events = witness.get("triggering_event_ids") or []
            trig_issues = witness.get("triggering_issue_ids") or []

            # Root cause IDs must exist (default v2, v1 only for removed/missing reasons)
            for root_id in root_ids:
                if root_id in graph_v2.nodes:
                    continue
                if reason in allowed_v1_reasons and root_id in graph_v1.nodes:
                    continue
                witness_ok = False
                witness_details.setdefault("invalid_root_cause_id", []).append(root_id)

            # Event/issue linkage sanity
            if reason in NO_EVENT_REASONS:
                if trig_events:
                    witness_ok = False
                    witness_details.setdefault("event_linkage", []).append(var_id)
            else:
                if trig_issues:
                    witness_ok = False
                    witness_details.setdefault("issue_linkage", []).append(var_id)
            if reason in ("MISSING_BINDING", "AMBIGUOUS_BINDING", "MISSING_TRANSFORM_REF"):
                if not trig_issues:
                    witness_ok = False
                    witness_details.setdefault("missing_issue_links", []).append(var_id)

            # Referenced event/issue ids exist
            for eid in trig_events:
                if eid not in event_map:
                    witness_ok = False
                    witness_details.setdefault("missing_event_ids", []).append(eid)
            for iid in trig_issues:
                if iid not in issue_map:
                    witness_ok = False
                    witness_details.setdefault("missing_issue_ids", []).append(iid)

            # Event relevance
            if trig_events:
                derived = spec_v2.get_derived_by_id(var_id) or spec_v1.get_derived_by_id(var_id)
                transform_ref = derived.transform_ref if derived else None
                for eid in trig_events:
                    event = event_map.get(eid, {})
                    event_type = event.get("change_type")
                    element_id = event.get("element_id")
                    if reason in ("DIRECT_CHANGE", "DIRECT_CHANGE_MISSING_INPUT"):
                        if element_id != var_id:
                            witness_ok = False
                            witness_details.setdefault("irrelevant_event", []).append(var_id)
                    elif reason in ("MISSING_INPUT", "TRANSITIVE_DEPENDENCY"):
                        if element_id not in root_ids:
                            witness_ok = False
                            witness_details.setdefault("irrelevant_event", []).append(var_id)
                    elif reason in ("TRANSFORM_IMPL_CHANGED", "TRANSFORM_REMOVED"):
                        if event_type in ("TRANSFORM_IMPL_CHANGED", "TRANSFORM_REMOVED", "TRANSFORM_ADDED"):
                            if element_id not in {transform_ref, var_id}:
                                witness_ok = False
                                witness_details.setdefault("irrelevant_event", []).append(var_id)
                        else:
                            if element_id not in {transform_ref, var_id}:
                                witness_ok = False
                                witness_details.setdefault("irrelevant_event", []).append(var_id)

            # Issue relevance
            if trig_issues:
                derived = spec_v2.get_derived_by_id(var_id) or spec_v1.get_derived_by_id(var_id)
                transform_ref = derived.transform_ref if derived else None
                for iid in trig_issues:
                    issue = issue_map.get(iid, {})
                    issue_type = issue.get("issue_type")
                    element_id = issue.get("element_id")
                    details = issue.get("details", {})
                    if reason in ("MISSING_BINDING", "AMBIGUOUS_BINDING"):
                        if issue_type != reason or element_id not in root_ids or details.get("affected_id") != var_id:
                            witness_ok = False
                            witness_details.setdefault("irrelevant_issue", []).append(var_id)
                    elif reason == "MISSING_TRANSFORM_REF":
                        if issue_type != reason or element_id != transform_ref or details.get("affected_id") != var_id:
                            witness_ok = False
                            witness_details.setdefault("irrelevant_issue", []).append(var_id)

            # Predecessor edge exists
            if predecessor:
                deps_v2 = graph_v2.get_dependencies(var_id)
                deps_v1 = graph_v1.get_dependencies(var_id)
                if predecessor not in deps_v2 and predecessor not in deps_v1:
                    witness_ok = False
                    witness_details.setdefault("invalid_predecessor", []).append(var_id)

            # Distance consistency (bounded)
            if var_id in distance_ids:
                if reason in ZERO_DISTANCE_REASONS and distance != 0:
                    witness_ok = False
                    distance_failed.append(var_id)
                elif distance > 0:
                    ok_distance = False
                    for root_id in root_ids:
                        graph = graph_v2 if root_id in graph_v2.nodes else graph_v1
                        path = graph.get_dependency_path(root_id, var_id)
                        if path and len(path) - 1 == distance:
                            ok_distance = True
                            break
                    if not ok_distance:
                        witness_ok = False
                        distance_failed.append(var_id)
                if distance == 1 and predecessor and root_ids:
                    if predecessor not in root_ids:
                        witness_ok = False
                        witness_details.setdefault("root_predecessor_mismatch", []).append(var_id)

        # Distance check metadata
        if distance_ids:
            witness_details["distance_check_mode"] = mode
            witness_details["distance_check_n"] = len(distance_ids) if mode == "strict" else DISTANCE_SAMPLE_N
            witness_details["distance_check_rule_id"] = "strict:all" if mode == "strict" else DISTANCE_RULE_ID
            witness_details["distance_checked_ids_count"] = len(distance_ids)
            if distance_failed:
                failed_sorted = sorted(set(distance_failed))
                sample = failed_sorted[:2]
                if len(failed_sorted) > 2:
                    sample += failed_sorted[-2:]
                witness_details["distance_failed_ids_sample"] = sample
    except Exception as e:
        witness_ok = False
        witness_details["error"] = str(e)
    clauses.append({"id": "witness_invariants", "ok": witness_ok, "details": witness_details})

    # Accounting invariants (summaries reconcile with witnesses)
    accounting_ok = True
    accounting_details: Dict[str, Any] = {}
    try:
        if diff_result is None:
            raise ValueError("diff_result unavailable for accounting verification")

        details = report.get("details", {})
        witnesses = details.get("witnesses", {})
        summaries = details.get("summaries", {})
        caps = details.get("caps")
        omissions = details.get("omissions", [])

        # Caps contract
        required_caps = {"max_witnesses", "max_root_causes_per_node", "max_trigger_events_per_node", "max_top_roots"}
        if not isinstance(caps, dict) or not required_caps.issubset(caps.keys()):
            accounting_ok = False
            accounting_details["caps_missing"] = True

        # Witness count / omissions honesty
        expected_witness_ids = _expected_witness_ids(diff_result)
        expected_count = len(expected_witness_ids)
        witness_keys = sorted(witnesses.keys())
        max_witnesses = caps.get("max_witnesses") if isinstance(caps, dict) else None
        cap_applied = min(max_witnesses, expected_count) if isinstance(max_witnesses, int) else expected_count
        if len(witness_keys) != cap_applied:
            accounting_ok = False
            accounting_details["witness_count_mismatch"] = True

        # Required omission entry when cap applied
        witness_omission = None
        if isinstance(omissions, list):
            for omission in omissions:
                if omission.get("path") == "details.witnesses":
                    witness_omission = omission
                    break
        if expected_count > cap_applied and witness_omission is None:
            accounting_ok = False
            accounting_details["missing_witness_omission"] = True
        reason_counts: Dict[str, int] = {}
        max_distance = 0
        for witness in witnesses.values():
            reason = witness.get("reason", "UNKNOWN")
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            dist = witness.get("distance", 0)
            if dist > max_distance:
                max_distance = dist

        if summaries.get("reasons") != reason_counts:
            accounting_ok = False
            accounting_details["reasons_mismatch"] = True
        if summaries.get("max_distance") != max_distance:
            accounting_ok = False
            accounting_details["max_distance_mismatch"] = True

        # Event/index counts
        if len(details.get("event_index", [])) != len(diff_result.events):
            accounting_ok = False
            accounting_details["event_index_count_mismatch"] = True
        issues_index, _ = build_issues_index(diff_result)
        if len(details.get("issues_index", [])) != len(issues_index):
            accounting_ok = False
            accounting_details["issues_index_count_mismatch"] = True

        # Omissions shape sanity
        for omission in omissions if isinstance(omissions, list) else []:
            cap = omission.get("cap")
            actual = omission.get("actual")
            omitted_count = omission.get("omitted_count")
            path = omission.get("path")
            sample_ids = omission.get("sample_ids")
            if cap is None or actual is None or omitted_count is None or path is None or sample_ids is None:
                accounting_ok = False
                accounting_details.setdefault("omissions_invalid", []).append(omission)
                continue
            if actual - cap != omitted_count:
                accounting_ok = False
                accounting_details.setdefault("omissions_mismatch", []).append(omission)

            # Enforce witness omissions consistency
            if path == "details.witnesses":
                if isinstance(max_witnesses, int) and cap != max_witnesses:
                    accounting_ok = False
                    accounting_details["witness_cap_mismatch"] = True
                if actual != expected_count:
                    accounting_ok = False
                    accounting_details["witness_actual_mismatch"] = True
                expected_omitted = max(expected_count - len(witness_keys), 0)
                if omitted_count != expected_omitted:
                    accounting_ok = False
                    accounting_details["witness_omitted_count_mismatch"] = True
                omitted_ids = [var_id for var_id in expected_witness_ids if var_id not in witness_keys]
                expected_sample: List[str] = []
                if omitted_ids:
                    expected_sample = [omitted_ids[0]]
                    if len(omitted_ids) > 1:
                        expected_sample.append(omitted_ids[-1])
                if sample_ids != expected_sample:
                    accounting_ok = False
                    accounting_details["witness_sample_mismatch"] = True
    except Exception as e:
        accounting_ok = False
        accounting_details["error"] = str(e)
    clauses.append({"id": "accounting_invariants", "ok": accounting_ok, "details": accounting_details})

    overall_ok = all(clause.get("ok", False) for clause in clauses)
    summary = {
        "total_clauses": len(clauses),
        "ok_clauses": sum(1 for clause in clauses if clause.get("ok", False)),
        "failed_clauses": sum(1 for clause in clauses if not clause.get("ok", False)),
        "failed_clause_ids": [clause["id"] for clause in clauses if not clause.get("ok", False)],
    }
    return {"ok": overall_ok, "clauses": clauses, "summary": summary}

"""All-details report builder (machine-first JSON artifact)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from cheshbon._internal.canonical_json import canonical_dumps
from cheshbon._internal.report_contract import (
    ALL_DETAILS_SCHEMA_VERSION,
    VERIFIER_CONTRACT_VERSION,
    CANONICALIZATION_POLICY_ID,
    DEFAULT_REPORT_CAPS,
)
from cheshbon.diff import generate_core_json_report
from cheshbon.kernel.spec import MappingSpec
from cheshbon.kernel.transform_registry import TransformRegistry
from cheshbon.kernel.bindings import Bindings
from cheshbon.kernel.graph import DependencyGraph
from cheshbon.kernel.impact import ImpactResult
from cheshbon.kernel.witness import compute_witnesses


def _normalize_path(path: Union[str, Path]) -> Path:
    return path if isinstance(path, Path) else Path(path)


def _load_json_from_path(path: Union[str, Path]) -> Dict[str, Any]:
    with open(_normalize_path(path), "r", encoding="utf-8") as f:
        return json.load(f)


def _digest_canonical(obj: Any) -> str:
    canonical = canonical_dumps(obj)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _digest_for_input(value: Optional[Union[str, Path, Dict]]) -> Optional[Dict[str, str]]:
    if value is None:
        return None
    if isinstance(value, dict):
        obj = value
    else:
        obj = _load_json_from_path(value)
    return {
        "digest": _digest_canonical(obj),
        "canonicalization": CANONICALIZATION_POLICY_ID,
    }


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


def build_all_details_report(
    diff_result: "DiffResult",
    impact_result: ImpactResult,
    spec_v1: MappingSpec,
    spec_v2: MappingSpec,
    graph_v1: DependencyGraph,
    graph_v2: DependencyGraph,
    registry_v1: Optional[TransformRegistry] = None,
    registry_v2: Optional[TransformRegistry] = None,
    bindings_v2: Optional[Bindings] = None,
    raw_schema: Optional[Union[str, Path, Dict]] = None,
    caps: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    """Build the all-details report dict."""
    report_caps = dict(DEFAULT_REPORT_CAPS)
    if caps:
        report_caps.update(caps)

    core_report = generate_core_json_report(diff_result)
    run_status = core_report.get("run_status")

    inputs = {
        "spec_v1": _digest_for_input(spec_v1.model_dump()) if spec_v1 else None,
        "spec_v2": _digest_for_input(spec_v2.model_dump()) if spec_v2 else None,
        "registry_v1": _digest_for_input(registry_v1.model_dump()) if registry_v1 else None,
        "registry_v2": _digest_for_input(registry_v2.model_dump()) if registry_v2 else None,
        "bindings_v2": _digest_for_input(
            {"table": bindings_v2.table, "bindings": dict(bindings_v2.bindings)}
        ) if bindings_v2 else None,
        "raw_schema_v2": _digest_for_input(raw_schema) if raw_schema is not None else None,
    }

    witnesses_payload = compute_witnesses(
        diff_result=diff_result,
        impact_result=impact_result,
        spec_v1=spec_v1,
        spec_v2=spec_v2,
        graph_v1=graph_v1,
        graph_v2=graph_v2,
        caps=report_caps,
    )

    report: Dict[str, Any] = {
        "report_schema_version": ALL_DETAILS_SCHEMA_VERSION,
        "verifier_contract_version": VERIFIER_CONTRACT_VERSION,
        "canonicalization_policy_id": CANONICALIZATION_POLICY_ID,
        "inputs": inputs,
        "core_digest": _core_subset_digest(diff_result),
        **core_report,
        "details": {
            "event_index": witnesses_payload["event_index"],
            "issues_index": witnesses_payload["issues_index"],
            "witnesses": witnesses_payload["witnesses"],
            "summaries": witnesses_payload["summaries"],
            "caps": report_caps,
            "omissions": witnesses_payload["omissions"],
        },
    }

    # Ensure run_status is present (core_report already provides it)
    if run_status is None:
        report["run_status"] = "non_executable" if diff_result.validation_failed else (
            "impacted" if diff_result.impacted_ids else "no_impact"
        )

    return report

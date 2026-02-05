"""Adapt SANS bundles into kernel-native inputs and run kernel diff."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from cheshbon.kernel.bindings import Bindings
from cheshbon.kernel.spec import DerivedVariable, MappingSpec
from cheshbon.kernel.transform_registry import (
    ImplFingerprint,
    Signature,
    TransformEntry,
    TransformRegistry,
)


EDGE_KIND_FLOW = "flow"
EDGE_KIND_DERIVATION = "derivation"
EDGE_KIND_RENAME = "rename"
EDGE_KINDS = {EDGE_KIND_FLOW, EDGE_KIND_DERIVATION, EDGE_KIND_RENAME}


@dataclass(frozen=True)
class KernelInputs:
    spec: MappingSpec
    registry: Optional[TransformRegistry]
    bindings: Optional[Bindings]
    vars_graph: Dict[str, Any]


def run_diff_from_bundles(bundle_a: Path, bundle_b: Path) -> Tuple[str, str]:
    """Adapt two SANS bundles and run the kernel diff pipeline."""
    inputs_a = adapt_bundle_to_kernel(bundle_a)
    inputs_b = adapt_bundle_to_kernel(bundle_b)

    from cheshbon import api as kernel_api
    from cheshbon.diff import generate_json_report, generate_markdown_report, _diff_result_to_impact_result
    from cheshbon.run_diff.value_evidence import compute_value_evidence
    from cheshbon.run_diff.transform_render import annotate_transform_events

    spec_a = inputs_a.spec.model_dump()
    spec_b = inputs_b.spec.model_dump()
    reg_a = inputs_a.registry.model_dump() if inputs_a.registry else None
    reg_b = inputs_b.registry.model_dump() if inputs_b.registry else None
    bindings_b = (
        {"table": inputs_b.bindings.table, "bindings": dict(inputs_b.bindings.bindings)}
        if inputs_b.bindings
        else None
    )

    (
        spec_v1,
        spec_v2,
        _graph_v1,
        _graph_v2,
        change_events,
        _impact_result,
        diff_result,
        _bindings_v2,
        registry_v1,
        registry_v2,
    ) = kernel_api._diff_internal(
        from_spec=spec_a,
        to_spec=spec_b,
        from_registry=reg_a,
        to_registry=reg_b,
        to_bindings=bindings_b,
        detail_level="full",
    )

    annotate_transform_events(change_events, bundle_a=Path(bundle_a), bundle_b=Path(bundle_b))

    impact_result = _diff_result_to_impact_result(diff_result)
    edge_kinds = _edge_kind_lookup(inputs_a.vars_graph)
    validation_findings = _collect_validation_findings(
        spec_v1=spec_v1,
        spec_v2=spec_v2,
        registry_v1=registry_v1,
        registry_v2=registry_v2,
    )
    value_evidence = compute_value_evidence(
        bundle_a=bundle_a,
        bundle_b=bundle_b,
        impacted_var_ids=diff_result.impacted_ids,
        change_events=change_events,
    )

    md_content = generate_markdown_report(
        impact_result=impact_result,
        change_events=change_events,
        spec_v1=spec_v1,
        spec_v2=spec_v2,
        edge_kinds=edge_kinds,
        value_evidence=value_evidence,
    )
    json_content = json.dumps(
        generate_json_report(
            impact_result,
            change_events,
            edge_kinds=edge_kinds,
            validation_findings=validation_findings,
            value_evidence=value_evidence,
        ),
        indent=2,
        ensure_ascii=False,
    )
    return md_content, json_content


def adapt_bundle_to_kernel(bundle_dir: Path) -> KernelInputs:
    bundle_dir = Path(bundle_dir)
    plan_path, vars_graph_path, registry_path = _resolve_bundle_paths(bundle_dir)

    plan = _load_json(plan_path)
    vars_graph = normalize_vars_graph(_load_json(vars_graph_path))

    spec = _build_spec_from_vars_graph(vars_graph, plan)
    registry = _build_registry(vars_graph, plan, registry_path)
    bindings = _build_bindings(vars_graph, plan)

    return KernelInputs(spec=spec, registry=registry, bindings=bindings, vars_graph=vars_graph)


def _resolve_bundle_paths(bundle_dir: Path) -> Tuple[Path, Path, Optional[Path]]:
    report_path = bundle_dir / "report.json"
    if not report_path.exists():
        raise FileNotFoundError(f"Missing report.json in {bundle_dir}")

    report = _load_json(report_path)
    plan_relpath = report.get("plan_path") or report.get("planPath")
    if plan_relpath:
        plan_path = bundle_dir / Path(str(plan_relpath))
    else:
        plan_path = bundle_dir / "artifacts" / "plan.ir.json"
    if not plan_path.exists():
        raise FileNotFoundError(f"Missing plan.ir.json at {plan_path}")

    vars_graph_path = bundle_dir / "artifacts" / "vars.graph.json"
    if not vars_graph_path.exists():
        raise FileNotFoundError(f"Missing vars.graph.json at {vars_graph_path}")

    registry_path = _find_registry_path(bundle_dir)
    return plan_path, vars_graph_path, registry_path


def _find_registry_path(bundle_dir: Path) -> Optional[Path]:
    candidates = [
        bundle_dir / "transform_registry.json",
        bundle_dir / "artifacts" / "transform_registry.json",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"Failed to parse JSON at {path}: {exc}") from exc


def normalize_vars_graph(vars_graph: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize vars.graph edges to include explicit edge kinds."""
    nodes = vars_graph.get("nodes", [])
    nodes_by_id: Dict[str, Dict[str, Any]] = {}
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = node.get("id")
        if node_id:
            nodes_by_id[node_id] = node

    normalized_edges: List[Dict[str, Any]] = []
    for edge in vars_graph.get("edges", []):
        if not isinstance(edge, dict):
            continue
        src = edge.get("src")
        dst = edge.get("dst")
        if not src or not dst:
            continue
        kind = edge.get("kind")
        if kind is None:
            kind = _infer_edge_kind(src, dst, nodes_by_id)
        elif kind not in EDGE_KINDS:
            raise ValueError(f"vars.graph edge kind must be one of {sorted(EDGE_KINDS)}, got '{kind}'")
        normalized = dict(edge)
        normalized["kind"] = kind
        normalized_edges.append(normalized)

    normalized_graph = dict(vars_graph)
    normalized_graph["edges"] = normalized_edges
    return normalized_graph


def _infer_edge_kind(
    src: str,
    dst: str,
    nodes_by_id: Dict[str, Dict[str, Any]],
) -> str:
    dst_node = nodes_by_id.get(dst)
    if not dst_node:
        return EDGE_KIND_FLOW
    origin = dst_node.get("origin")
    transform_id = dst_node.get("transform_id")
    if origin == "pass_through" or not transform_id:
        return EDGE_KIND_FLOW
    src_node = nodes_by_id.get(src)
    if src_node:
        if (
            src_node.get("producing_step_id") == dst_node.get("producing_step_id")
            and src_node.get("transform_id") == dst_node.get("transform_id")
        ):
            return EDGE_KIND_DERIVATION
    if origin == "derived":
        return EDGE_KIND_DERIVATION
    return EDGE_KIND_FLOW


def _build_spec_from_vars_graph(vars_graph: Dict[str, Any], plan: Dict[str, Any]) -> MappingSpec:
    nodes = vars_graph.get("nodes", [])
    edges = vars_graph.get("edges", [])
    nodes_by_id: Dict[str, Dict[str, Any]] = {}

    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = node.get("id")
        if not node_id:
            continue
        if node_id in nodes_by_id:
            raise ValueError(f"Duplicate vars.graph node id: {node_id}")
        nodes_by_id[node_id] = node

    inputs_by_id: Dict[str, List[str]] = {node_id: [] for node_id in nodes_by_id}
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        src = edge.get("src")
        dst = edge.get("dst")
        if not src or not dst:
            continue
        if dst not in inputs_by_id:
            raise ValueError(f"vars.graph edge references missing dst node: {dst}")
        if src not in nodes_by_id:
            raise ValueError(f"vars.graph edge references missing src node: {src}")
        inputs_by_id[dst].append(src)

    steps_by_id = _index_steps(plan.get("steps", []))

    derived_vars: List[DerivedVariable] = []
    # origin: pass_through means no local transform; upstream deps may still exist via flow edges.
    for node_id in sorted(nodes_by_id.keys()):
        node = nodes_by_id[node_id]
        table_id, col = _resolve_table_and_col(node)
        inputs = sorted(set(inputs_by_id.get(node_id, [])))

        transform_id = node.get("transform_id")
        transform_ref = f"t:{transform_id}" if transform_id else "t:pass_through"

        params = _build_params(node, steps_by_id)
        if table_id is not None:
            params["table_id"] = table_id
        if col is not None:
            params["col"] = col
        if not params:
            params = None

        name = f"{table_id}.{col}" if table_id and col else node_id

        derived_vars.append(
            DerivedVariable(
                id=node_id,
                name=name,
                type="unknown",
                transform_ref=transform_ref,
                inputs=inputs,
                params=params,
            )
        )

    source_table = _choose_source_table(plan)

    return MappingSpec(
        spec_version="sans.vars_graph.v1",
        study_id="sans_bundle",
        source_table=source_table,
        sources=[],
        derived=derived_vars,
        constraints=[],
    )


def _edge_kind_lookup(vars_graph: Dict[str, Any]) -> Dict[Tuple[str, str], str]:
    edge_kinds: Dict[Tuple[str, str], str] = {}
    for edge in vars_graph.get("edges", []):
        if not isinstance(edge, dict):
            continue
        src = edge.get("src")
        dst = edge.get("dst")
        kind = edge.get("kind")
        if not src or not dst or not kind:
            continue
        edge_kinds[(src, dst)] = kind
    return edge_kinds


def _index_steps(steps: List[Any]) -> Dict[str, Dict[str, Any]]:
    indexed: Dict[str, Dict[str, Any]] = {}
    for step in steps:
        if not isinstance(step, dict):
            continue
        step_id = step.get("step_id")
        if not step_id:
            continue
        indexed[step_id] = step
    return indexed


def _build_params(node: Dict[str, Any], steps_by_id: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    for key in ("producing_step_id", "expr_sha256", "payload_sha256", "origin", "transform_id"):
        if key in node:
            params[key] = node.get(key)

    producing_step_id = node.get("producing_step_id")
    if producing_step_id and producing_step_id in steps_by_id:
        step = steps_by_id[producing_step_id]
        if "inputs" in step:
            params["step_inputs"] = sorted(list(step.get("inputs") or []))
        if "outputs" in step:
            params["step_outputs"] = sorted(list(step.get("outputs") or []))

    return params


def _resolve_table_and_col(node: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    table_id = node.get("table_id")
    col = node.get("col")
    if table_id is not None and col is not None:
        return table_id, col

    node_id = node.get("id")
    parsed = _parse_var_id(node_id)
    if table_id is None:
        table_id = parsed[0]
    if col is None:
        col = parsed[1]
    return table_id, col


def _parse_var_id(node_id: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not node_id or not isinstance(node_id, str):
        return None, None
    if not node_id.startswith("v:"):
        return None, None
    body = node_id[2:]
    if "." not in body:
        return None, None
    table_id, col = body.split(".", 1)
    return table_id, col


def _choose_source_table(plan: Dict[str, Any]) -> str:
    tables = plan.get("tables") or []
    if not tables:
        return "unknown"
    return sorted([str(t) for t in tables])[0]


def _build_bindings(vars_graph: Dict[str, Any], plan: Dict[str, Any]) -> Optional[Bindings]:
    tables = plan.get("tables") or []
    if not tables:
        return None
    table = _choose_source_table(plan)

    bindings: Dict[str, str] = {}
    for node in vars_graph.get("nodes", []):
        if not isinstance(node, dict):
            continue
        node_id = node.get("id")
        if not node_id:
            continue
        table_id, col = _resolve_table_and_col(node)
        if table_id == table and col:
            bindings[col] = node_id

    bindings = {k: bindings[k] for k in sorted(bindings)}
    return Bindings(table=table, bindings=bindings)


def _build_registry(
    vars_graph: Dict[str, Any],
    plan: Dict[str, Any],
    registry_path: Optional[Path],
) -> TransformRegistry:
    if registry_path is not None:
        payload = registry_path.read_bytes()
        try:
            return TransformRegistry.from_json_bytes(payload)
        except Exception as exc:
            raise ValueError(
                f"Failed to load transform registry at {registry_path}: {exc}"
            ) from exc

    transform_ids = _collect_transform_ids(vars_graph, plan)
    entries = [
        TransformEntry(
            id=f"t:{transform_id}",
            version="",
            kind="external_sas",
            signature=Signature(inputs=[], output="unknown"),
            params_schema_hash=None,
            impl_fingerprint=ImplFingerprint(
                algo="sha256",
                source="external_sas",
                ref=transform_id,
                digest=_normalize_digest(transform_id),
            ),
        )
        for transform_id in sorted(transform_ids)
    ]
    return TransformRegistry(registry_version="sans_minimal", transforms=entries)


def _collect_validation_findings(
    spec_v1: MappingSpec,
    spec_v2: MappingSpec,
    registry_v1: Optional[TransformRegistry],
    registry_v2: Optional[TransformRegistry],
) -> Dict[str, List[Dict[str, Any]]]:
    return {
        "a": _missing_transform_findings(spec_v1, registry_v1, side="a"),
        "b": _missing_transform_findings(spec_v2, registry_v2, side="b"),
    }


def _missing_transform_findings(
    spec: MappingSpec,
    registry: Optional[TransformRegistry],
    side: str,
) -> List[Dict[str, Any]]:
    if registry is None:
        return []
    findings: List[Dict[str, Any]] = []
    for derived in sorted(spec.derived, key=lambda d: d.id):
        if registry.has_transform(derived.transform_ref):
            continue
        message = (
            f"Derived variable '{derived.id}' ({derived.name}) references "
            f"missing transform '{derived.transform_ref}'. "
            f"Transform not found in registry."
        )
        findings.append(
            {
                "code": "MISSING_TRANSFORM_REF",
                "side": side,
                "element_id": derived.id,
                "transform_id": derived.transform_ref,
                "message": message,
            }
        )
    return findings


def _collect_transform_ids(vars_graph: Dict[str, Any], plan: Dict[str, Any]) -> List[str]:
    transform_ids: List[str] = []
    for node in vars_graph.get("nodes", []):
        if not isinstance(node, dict):
            continue
        transform_id = node.get("transform_id")
        if transform_id:
            transform_ids.append(str(transform_id))
        else:
            transform_ids.append("pass_through")
    for step in plan.get("steps", []):
        if not isinstance(step, dict):
            continue
        transform_id = step.get("transform_id")
        if transform_id:
            transform_ids.append(str(transform_id))
    return sorted(set(transform_ids))


def _normalize_digest(value: str) -> str:
    if _is_hex64(value):
        return value.lower()
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return digest


def _is_hex64(value: str) -> bool:
    if not isinstance(value, str) or len(value) != 64:
        return False
    return all(ch in "0123456789abcdefABCDEF" for ch in value)

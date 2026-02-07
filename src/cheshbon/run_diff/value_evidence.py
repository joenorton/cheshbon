"""Value-level evidence for run-diff impact output (SANS bundles)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DIRECT_CHANGE_TYPES = {
    "DERIVED_TRANSFORM_REF_CHANGED",
    "DERIVED_TRANSFORM_PARAMS_CHANGED",
    "DERIVED_TYPE_CHANGED",
    "DERIVED_INPUTS_CHANGED",
}

MAX_TOP_VALUES = 5


def compute_value_evidence(
    bundle_a: Path,
    bundle_b: Path,
    impacted_var_ids: Iterable[str],
    change_events: Iterable[Any],
    schema_evidence_a: Optional[Any] = None,
    schema_evidence_b: Optional[Any] = None,
) -> Dict[str, Dict[str, Any]]:
    """Compute value-level evidence for DIRECT_CHANGE vars only.

    When schema_evidence_b is present and the column is not in the target table schema,
    reports "column no longer exists" instead of value changed.
    Returns a dict: var_id -> value_evidence object.
    """
    direct_var_ids = _direct_change_var_ids(change_events)
    impacted_set = set(impacted_var_ids)
    targets = sorted(direct_var_ids & impacted_set)
    if not targets:
        return {}

    bundle_a = Path(bundle_a)
    bundle_b = Path(bundle_b)

    vars_graph_a = _load_json(bundle_a / "artifacts" / "vars.graph.json")
    vars_graph_b = _load_json(bundle_b / "artifacts" / "vars.graph.json")
    report_a = _load_json(bundle_a / "report.json")
    report_b = _load_json(bundle_b / "report.json")
    evidence_a = _load_runtime_evidence(bundle_a, report_a)
    evidence_b = _load_runtime_evidence(bundle_b, report_b)

    node_lookup_a = _index_vars_graph_nodes(vars_graph_a)
    node_lookup_b = _index_vars_graph_nodes(vars_graph_b)

    value_evidence: Dict[str, Dict[str, Any]] = {}

    for var_id in targets:
        table_a, col_a = _resolve_table_col(node_lookup_a, var_id)
        table_b, col_b = _resolve_table_col(node_lookup_b, var_id)
        if not table_a or not col_a or not table_b or not col_b:
            value_evidence[var_id] = _unavailable_value_evidence(
                failure_reason="table_not_found" if not (table_a and table_b) else "column_not_found",
                attempted=["tables", "outputs"],
            )
            continue

        # Schema evidence authoritative: if column not present in B, report "column no longer exists"
        if schema_evidence_b is not None and hasattr(schema_evidence_b, "tables"):
            tables_b = getattr(schema_evidence_b, "tables", {}) or {}
            cols_b = tables_b.get(table_b) if isinstance(tables_b, dict) else None
            if isinstance(cols_b, dict) and col_b not in cols_b:
                value_evidence[var_id] = _unavailable_value_evidence(
                    failure_reason="column_no_longer_exists",
                    attempted=["schema_evidence"],
                )
                continue

        stats_a, failure_a, attempted_a = _get_column_stats(evidence_a, table_a, col_a)
        stats_b, failure_b, attempted_b = _get_column_stats(evidence_b, table_b, col_b)
        if stats_a is None or stats_b is None:
            failure_reason = failure_a or failure_b or "runtime_evidence_missing"
            attempted = _merge_attempted(attempted_a, attempted_b)
            value_evidence[var_id] = _unavailable_value_evidence(
                failure_reason=failure_reason,
                attempted=attempted,
            )
            continue

        value_evidence[var_id] = _build_value_evidence(stats_a, stats_b)

    return value_evidence


def _direct_change_var_ids(change_events: Iterable[Any]) -> set[str]:
    direct_ids: set[str] = set()
    for event in change_events:
        change_type = _event_attr(event, "change_type")
        element_id = _event_attr(event, "element_id")
        if change_type in DIRECT_CHANGE_TYPES and isinstance(element_id, str):
            direct_ids.add(element_id)
    return direct_ids


def _event_attr(event: Any, key: str) -> Any:
    if isinstance(event, dict):
        return event.get(key)
    return getattr(event, key, None)


def _load_json(path: Path) -> Dict[str, Any]:
    import json

    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _load_runtime_evidence(bundle_dir: Path, report: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    artifacts = report.get("artifacts") or []
    runtime_path = None
    for artifact in artifacts:
        if artifact.get("name") == "runtime.evidence.json":
            runtime_path = artifact.get("path")
            break
    if runtime_path is None:
        runtime_path = "artifacts/runtime.evidence.json"
    evidence_path = bundle_dir / Path(str(runtime_path))
    if not evidence_path.exists():
        return None
    return _load_json(evidence_path)


def _index_vars_graph_nodes(vars_graph: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    nodes_by_id: Dict[str, Dict[str, Any]] = {}
    for node in vars_graph.get("nodes", []):
        if not isinstance(node, dict):
            continue
        node_id = node.get("id")
        if isinstance(node_id, str):
            nodes_by_id[node_id] = node
    return nodes_by_id


def _resolve_table_col(nodes_by_id: Dict[str, Dict[str, Any]], var_id: str) -> Tuple[Optional[str], Optional[str]]:
    node = nodes_by_id.get(var_id)
    if node:
        table_id = node.get("table_id")
        col = node.get("col")
        if table_id and col:
            return str(table_id), str(col)
    if var_id.startswith("v:") and "." in var_id:
        body = var_id[2:]
        table_id, col = body.split(".", 1)
        return table_id, col
    return None, None


def _get_column_stats(
    evidence: Optional[Dict[str, Any]],
    table: str,
    column: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], List[str]]:
    attempted: List[str] = []
    if evidence is None:
        return None, "runtime_evidence_missing", attempted

    attempted.append("tables")
    stats, failure_reason = _stats_from_tables(evidence, table, column)
    if stats is not None:
        return stats, None, attempted

    attempted.append("outputs")
    stats = _stats_from_outputs(evidence, table, column)
    if stats is not None:
        return stats, None, attempted

    return None, failure_reason or "column_not_found", attempted


def _stats_from_tables(
    evidence: Dict[str, Any],
    table: str,
    column: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    tables = evidence.get("tables")
    if tables is None or not isinstance(tables, dict):
        return None, "tables_section_missing"

    table_entry = tables.get(table)
    if not isinstance(table_entry, dict):
        return None, "table_not_found"

    columns = table_entry.get("columns")
    if not isinstance(columns, dict):
        return None, "column_not_found"

    stats = columns.get(column)
    if isinstance(stats, dict):
        return _normalize_stats(stats), None

    return None, "column_not_found"


def _stats_from_outputs(
    evidence: Dict[str, Any],
    table: str,
    column: str,
) -> Optional[Dict[str, Any]]:
    column_stats = evidence.get("column_stats")
    if isinstance(column_stats, dict):
        table_stats = column_stats.get(table)
        if isinstance(table_stats, dict):
            stats = table_stats.get(column)
            if isinstance(stats, dict):
                return _normalize_stats(stats)

    outputs = evidence.get("outputs") or []
    for entry in outputs:
        if not isinstance(entry, dict):
            continue
        if entry.get("name") != table:
            continue
        stats = entry.get("column_stats")
        if isinstance(stats, dict) and column in stats and isinstance(stats[column], dict):
            return _normalize_stats(stats[column])

    return None


def _normalize_stats(stats: Dict[str, Any]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    for key in ("unique_count", "null_count", "row_count", "checksum"):
        if key in stats:
            normalized[key] = _normalize_count(stats[key])
    if "top_values" in stats:
        normalized["top_values"] = [_format_value(v) for v in list(stats.get("top_values") or [])[:MAX_TOP_VALUES]]
    if "top_counts" in stats:
        normalized["top_counts"] = list(stats.get("top_counts") or [])[:MAX_TOP_VALUES]
    if "min" in stats:
        normalized["min"] = _format_value(stats.get("min"))
    if "max" in stats:
        normalized["max"] = _format_value(stats.get("max"))
    if "constant_value" in stats:
        normalized["constant_value"] = _format_value(stats.get("constant_value"))
    return normalized


def _build_value_evidence(old_stats: Dict[str, Any], new_stats: Dict[str, Any]) -> Dict[str, Any]:
    old_const = _extract_constant(old_stats)
    new_const = _extract_constant(new_stats)

    if old_const is not None and new_const is not None:
        old_render = _summary_value(old_const)
        new_render = _summary_value(new_const)
        return {
            "available": True,
            "kind": "column_constant",
            "summary": f"{old_render} -> {new_render}",
            "old": _stats_payload(old_stats, constant_value=old_const),
            "new": _stats_payload(new_stats, constant_value=new_const),
        }

    if old_const is not None and new_const is None:
        summary = (
            f"constant {_summary_value(old_const)} -> "
            f"non-constant (unique={_stat_int(new_stats, 'unique_count')})"
        )
        return {
            "available": True,
            "kind": "column_stats",
            "summary": summary,
            "old": _stats_payload(old_stats, constant_value=old_const),
            "new": _stats_payload(new_stats),
        }

    if old_const is None and new_const is not None:
        summary = (
            f"non-constant (unique={_stat_int(old_stats, 'unique_count')}) -> "
            f"constant {_summary_value(new_const)}"
        )
        return {
            "available": True,
            "kind": "column_stats",
            "summary": summary,
            "old": _stats_payload(old_stats),
            "new": _stats_payload(new_stats, constant_value=new_const),
        }

    summary = _stats_summary(old_stats, new_stats)
    return {
        "available": True,
        "kind": "column_stats" if summary else "unknown",
        "summary": summary,
        "old": _stats_payload(old_stats),
        "new": _stats_payload(new_stats),
    }


def _stats_summary(old_stats: Dict[str, Any], new_stats: Dict[str, Any]) -> str:
    old_unique = _stat_int(old_stats, "unique_count")
    new_unique = _stat_int(new_stats, "unique_count")
    old_nulls = _stat_int(old_stats, "null_count")
    new_nulls = _stat_int(new_stats, "null_count")

    old_top = _top_value_summary(old_stats)
    new_top = _top_value_summary(new_stats)

    summary = f"unique {old_unique}->{new_unique}, nulls {old_nulls}->{new_nulls}"
    if old_top or new_top:
        summary += f", top: {old_top} -> {new_top}"
    return summary


def _top_value_summary(stats: Dict[str, Any]) -> str:
    values = stats.get("top_values") or []
    counts = stats.get("top_counts") or []
    items = []
    for idx, value in enumerate(values[:MAX_TOP_VALUES]):
        count = counts[idx] if idx < len(counts) else None
        rendered = _summary_value(value)
        if count is None:
            items.append(rendered)
        else:
            items.append(f"{rendered}({count})")
    return "; ".join(items)


def _extract_constant(stats: Dict[str, Any]) -> Optional[str]:
    constant_value = stats.get("constant_value")
    if constant_value is not None:
        return str(constant_value)

    unique_count = stats.get("unique_count")
    null_count = stats.get("null_count")
    if unique_count == 1 and (null_count == 0 or null_count is None):
        top_values = stats.get("top_values") or []
        if len(top_values) == 1:
            return str(top_values[0])
        min_val = stats.get("min")
        max_val = stats.get("max")
        if min_val is not None and max_val is not None and str(min_val) == str(max_val):
            return str(min_val)
    return None


def _stats_payload(stats: Dict[str, Any], constant_value: Optional[str] = None) -> Dict[str, Any]:
    payload = dict(stats)
    if constant_value is not None:
        payload["constant"] = constant_value
    return payload


def _stat_int(stats: Dict[str, Any], key: str) -> str:
    value = stats.get(key)
    return str(value) if value is not None else "unknown"


def _format_value(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        formatted = format(value, "f")
        if "." in formatted:
            formatted = formatted.rstrip("0").rstrip(".")
        return formatted or "0"
    return str(value)


def _normalize_count(value: Any) -> Any:
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return _format_value(value)
    return value


def _summary_value(value: Any) -> str:
    if isinstance(value, str):
        return json.dumps(value)
    return _format_value(value)


def _merge_attempted(*attempts: List[str]) -> List[str]:
    seen: List[str] = []
    for attempt in attempts:
        for item in attempt:
            if item not in seen:
                seen.append(item)
    return seen


def _unavailable_value_evidence(
    failure_reason: str,
    attempted: Optional[List[str]] = None,
) -> Dict[str, Any]:
    payload = {
        "available": False,
        "kind": "unknown",
        "summary": "",
        "old": {},
        "new": {},
        "failure_reason": failure_reason,
    }
    if attempted:
        payload["attempted"] = attempted
    return payload

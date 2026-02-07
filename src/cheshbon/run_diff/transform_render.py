"""Render transform specs for run-diff change events."""

from __future__ import annotations

import json
from pathlib import Path, PurePosixPath
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class RenderedTransform:
    op: str
    kind: str
    render: str
    structured: Optional[Dict[str, Any]] = None


def annotate_transform_events(
    change_events: Iterable[Any],
    bundle_a: Path,
    bundle_b: Path,
) -> None:
    """Augment transform-related events with spec-level rendering."""
    registry_a = _load_registry_spec_index(bundle_a)
    registry_b = _load_registry_spec_index(bundle_b)

    for event in change_events:
        change_type = _event_attr(event, "change_type")
        if change_type == "DERIVED_TRANSFORM_REF_CHANGED":
            element_id = _event_attr(event, "element_id")
            if not isinstance(element_id, str):
                continue
            target = _parse_var_target(element_id)
            old_transform_id = _event_attr(event, "old_value")
            new_transform_id = _event_attr(event, "new_value")

            old_render, old_mode = _render_target_assignment(old_transform_id, target, registry_a)
            new_render, new_mode = _render_target_assignment(new_transform_id, target, registry_b)

            mode = new_mode or old_mode

            details = dict(_event_attr(event, "details") or {})
            details.update(
                {
                    "target": target,
                    "mode": mode,
                    "old_render": old_render,
                    "new_render": new_render,
                    "old_transform_id": old_transform_id,
                    "new_transform_id": new_transform_id,
                }
            )

            old_transform = _render_transform_by_id(old_transform_id, registry_a)
            new_transform = _render_transform_by_id(new_transform_id, registry_b)
            if old_transform:
                details["old_transform"] = _rendered_transform_payload(old_transform)
            if new_transform:
                details["new_transform"] = _rendered_transform_payload(new_transform)

            _set_event_details(event, details)
            continue

        if change_type in {"TRANSFORM_ADDED", "TRANSFORM_REMOVED"}:
            element_id = _event_attr(event, "element_id")
            if not isinstance(element_id, str):
                continue
            registry = registry_b if change_type == "TRANSFORM_ADDED" else registry_a
            rendered = _render_transform_by_id(element_id, registry)
            if rendered is None:
                render_text = f"(transform not found: {element_id})"
                rendered = RenderedTransform(
                    op="unknown",
                    kind="unknown",
                    render=render_text,
                    structured=None,
                )
            details = dict(_event_attr(event, "details") or {})
            details.update(_rendered_transform_payload(rendered))
            _set_event_details(event, details)


def _load_registry_spec_index(bundle_dir: Path) -> Dict[str, Dict[str, Any]]:
    registry_path = _find_registry_candidate_path(bundle_dir)
    if registry_path is None:
        return {}
    try:
        payload = json.loads(registry_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return _index_registry_specs(payload)


def _find_registry_candidate_path(bundle_dir: Path) -> Optional[Path]:
    report_path = bundle_dir / "report.json"
    report_payload: Dict[str, Any] = {}
    if report_path.exists():
        try:
            report_payload = json.loads(report_path.read_text(encoding="utf-8"))
        except Exception:
            report_payload = {}

    artifacts = report_payload.get("artifacts") or []
    registry_relpath = None
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        if artifact.get("name") == "registry.candidate.json":
            registry_relpath = artifact.get("path")
            break

    if registry_relpath:
        try:
            registry_path = _resolve_bundle_path(bundle_dir, str(registry_relpath))
            if registry_path.exists():
                return registry_path
        except ValueError:
            registry_path = None

    for fallback in ("registry.candidate.json", "artifacts/registry.candidate.json"):
        candidate = bundle_dir / PurePosixPath(fallback)
        if candidate.exists():
            return candidate

    return None


def _resolve_bundle_path(bundle_dir: Path, relpath: str) -> Path:
    normalized = str(PurePosixPath(relpath.replace("\\", "/")))
    posix_path = PurePosixPath(normalized)
    if posix_path.is_absolute() or ".." in posix_path.parts:
        raise ValueError(f"Report path must be bundle-relative: {relpath}")
    return bundle_dir.joinpath(*posix_path.parts)


def _index_registry_specs(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    indexed: Dict[str, Dict[str, Any]] = {}
    transforms = payload.get("transforms") or []
    for transform in transforms:
        if not isinstance(transform, dict):
            continue
        transform_id = transform.get("transform_id") or transform.get("id")
        if not transform_id:
            continue
        entry = {
            "kind": transform.get("kind"),
            "spec": transform.get("spec"),
        }
        transform_id = str(transform_id)
        ids = {transform_id}
        if not transform_id.startswith("t:"):
            ids.add(f"t:{transform_id}")
        for tid in ids:
            indexed[tid] = entry
    return indexed


def _parse_var_target(var_id: str) -> str:
    if not var_id.startswith("v:") or "." not in var_id:
        return ""
    body = var_id[2:]
    _, col = body.split(".", 1)
    return col


def _render_target_assignment(
    transform_id: Optional[str],
    target: str,
    registry_specs: Dict[str, Dict[str, Any]],
) -> Tuple[str, Optional[str]]:
    if not transform_id:
        return "(missing transform id)", None
    entry = _lookup_transform_entry(transform_id, registry_specs)
    if entry is None:
        return f"(transform not found: {transform_id})", None

    kind = entry.get("kind")
    spec = entry.get("spec")
    if kind != "compute" or not isinstance(spec, dict):
        op = spec.get("op") if isinstance(spec, dict) else "unknown"
        return f"(unrenderable transform op={op})", None
    if spec.get("op") != "compute":
        return f"(unrenderable transform op={spec.get('op')})", None

    params = spec.get("params") or {}
    mode = params.get("mode") or "derive"
    assignments = params.get("assignments") or params.get("assign") or []
    if not isinstance(assignments, list):
        return _no_assignment_message(target), mode

    matches: List[Tuple[str, str]] = []
    for assignment in assignments:
        if not isinstance(assignment, dict):
            continue
        target_name = assignment.get("target") or assignment.get("col") or assignment.get("column")
        if not target_name:
            continue
        if str(target_name) != target:
            continue
        rendered_expr = _render_expr(assignment.get("expr"))
        matches.append((str(target_name), rendered_expr))

    if not matches:
        return _no_assignment_message(target), mode

    matches.sort(key=lambda item: (item[0], item[1]))
    rendered_assignments = [f"{target_name} = {expr}" for target_name, expr in matches]
    return "; ".join(rendered_assignments), mode


def _render_transform_by_id(
    transform_id: Optional[str],
    registry_specs: Dict[str, Dict[str, Any]],
) -> Optional[RenderedTransform]:
    if not transform_id:
        return None
    entry = _lookup_transform_entry(transform_id, registry_specs)
    if entry is None:
        return None
    return render_transform(entry)


def render_transform(entry: Dict[str, Any]) -> RenderedTransform:
    kind = entry.get("kind") or "unknown"
    spec = entry.get("spec")
    if not isinstance(spec, dict):
        return RenderedTransform(
            op="unknown",
            kind=str(kind),
            render="(incomplete spec: spec)",
            structured=None,
        )

    op = spec.get("op") or "unknown"
    params = spec.get("params") or {}

    if op == "compute":
        render, structured = _render_compute_transform(params)
        return RenderedTransform(op=op, kind=str(kind), render=render, structured=structured)
    if op == "filter":
        render, structured = _render_filter_transform(params)
        return RenderedTransform(op=op, kind=str(kind), render=render, structured=structured)
    if op == "rename":
        render, structured = _render_rename_transform(params)
        return RenderedTransform(op=op, kind=str(kind), render=render, structured=structured)
    if op == "select":
        render, structured = _render_select_transform(params)
        return RenderedTransform(op=op, kind=str(kind), render=render, structured=structured)
    if op == "sort":
        render, structured = _render_sort_transform(params)
        return RenderedTransform(op=op, kind=str(kind), render=render, structured=structured)
    if op == "aggregate":
        render, structured = _render_aggregate_transform(params)
        return RenderedTransform(op=op, kind=str(kind), render=render, structured=structured)
    if op == "drop":
        render, structured = _render_drop_transform(params)
        return RenderedTransform(op=op, kind=str(kind), render=render, structured=structured)

    return RenderedTransform(
        op=str(op),
        kind=str(kind),
        render=f"(unrenderable op: {op})",
        structured=None,
    )


def _render_compute_transform(params: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
    mode = params.get("mode") or "derive"
    assignments = params.get("assignments") or params.get("assign") or []
    if not isinstance(assignments, list) or not assignments:
        return "(incomplete spec: assignments)", None

    rendered_items: List[Tuple[str, str]] = []
    structured_assignments: List[Dict[str, Any]] = []
    for assignment in assignments:
        if not isinstance(assignment, dict):
            continue
        target_name = assignment.get("target") or assignment.get("col") or assignment.get("column")
        if not target_name:
            continue
        rendered_expr = _render_expr(assignment.get("expr"))
        rendered_items.append((str(target_name), rendered_expr))
        structured_assignments.append(
            {"target": str(target_name), "expr": rendered_expr}
        )

    if not rendered_items:
        return "(incomplete spec: assignments)", None

    rendered_items.sort(key=lambda item: (item[0], item[1]))
    structured_assignments = sorted(
        structured_assignments, key=lambda item: (item["target"], item["expr"])
    )
    rendered_assignments = [f"{target_name} = {expr}" for target_name, expr in rendered_items]
    mode_suffix = " (update!)" if mode == "update" else " (derive)"
    render = f"compute {', '.join(rendered_assignments)}{mode_suffix}"
    return render, {"mode": mode, "assignments": structured_assignments}


def _render_filter_transform(params: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
    predicate = params.get("predicate")
    if predicate is None:
        return "(incomplete spec: predicate)", None
    rendered = _render_expr(predicate)
    return f"filter({rendered})", {"predicate": rendered}


def _render_rename_transform(params: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
    mapping = (
        params.get("mapping")
        or params.get("mappings")
        or params.get("rename")
        or params.get("renames")
    )
    if not isinstance(mapping, list) or not mapping:
        return "(incomplete spec: mapping)", None

    pairs: List[Tuple[str, str]] = []
    structured: List[Dict[str, str]] = []
    for entry in mapping:
        if not isinstance(entry, dict):
            continue
        src = entry.get("from") or entry.get("src") or entry.get("old")
        dst = entry.get("to") or entry.get("dst") or entry.get("new")
        if src is None or dst is None:
            continue
        src_str = str(src)
        dst_str = str(dst)
        pairs.append((src_str, dst_str))
        structured.append({"from": src_str, "to": dst_str})

    if not pairs:
        return "(incomplete spec: mapping)", None

    pairs.sort(key=lambda item: (item[0], item[1]))
    structured = sorted(structured, key=lambda item: (item["from"], item["to"]))
    rendered_pairs = [f"{src} -> {dst}" for src, dst in pairs]
    return f"rename({', '.join(rendered_pairs)})", {"mapping": structured}


def _render_select_transform(params: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
    cols = params.get("cols") or params.get("keep")
    drop_cols = params.get("drop")
    if isinstance(cols, list) and cols:
        col_list = [str(col) for col in cols if col is not None]
        if col_list:
            return f"select {', '.join(col_list)}", {"cols": col_list}
    if isinstance(drop_cols, list) and drop_cols:
        drop_list = [str(c) for c in drop_cols if c is not None]
        if drop_list:
            return f"select drop({', '.join(drop_list)})", {"drop": drop_list}
    if not isinstance(cols, list) or not cols:
        return "(incomplete spec: cols)", None
    col_list = [str(col) for col in cols if col is not None]
    if not col_list:
        return "(incomplete spec: cols)", None
    return f"select {', '.join(col_list)}", {"cols": col_list}


def _render_drop_transform(params: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Projection-removal: drop columns (op=drop)."""
    cols = params.get("drop") or params.get("columns") or params.get("cols")
    if not isinstance(cols, list) or not cols:
        return "(incomplete spec: drop)", None
    col_list = [str(c) for c in cols if c is not None]
    if not col_list:
        return "(incomplete spec: drop)", None
    return f"drop({', '.join(col_list)})", {"drop": col_list}


def _render_sort_transform(params: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
    by = params.get("by") or params.get("cols")
    if not isinstance(by, list) or not by:
        return "(incomplete spec: by)", None

    rendered_cols: List[str] = []
    structured_cols: List[Dict[str, Any]] = []
    for entry in by:
        if isinstance(entry, dict):
            col = entry.get("col") or entry.get("name")
            if col is None:
                continue
            col_str = str(col)
            direction = None
            if entry.get("desc") is True:
                direction = "desc"
            elif entry.get("asc") is True:
                direction = "asc"
            if direction:
                rendered_cols.append(f"{col_str} {direction}")
            else:
                rendered_cols.append(col_str)
            structured_cols.append({"col": col_str, "direction": direction})
        else:
            rendered_cols.append(str(entry))
            structured_cols.append({"col": str(entry), "direction": None})

    if not rendered_cols:
        return "(incomplete spec: by)", None

    nodupkey = params.get("nodupkey")
    render = f"sort by {', '.join(rendered_cols)}"
    if nodupkey is not None:
        render += f" nodupkey({_render_bool(nodupkey)})"
    structured = {"by": structured_cols}
    if nodupkey is not None:
        structured["nodupkey"] = bool(nodupkey)
    return render, structured


def _render_aggregate_transform(params: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
    class_cols = params.get("class") or params.get("class_cols") or params.get("group_by")
    var_cols = params.get("var") or params.get("vars")
    stats = params.get("stats") or params.get("statistics")

    if class_cols is None or var_cols is None or stats is None:
        missing = []
        if class_cols is None:
            missing.append("class")
        if var_cols is None:
            missing.append("var")
        if stats is None:
            missing.append("stats")
        return f"(incomplete spec: {', '.join(missing)})", None

    class_list = [str(col) for col in class_cols if col is not None] if isinstance(class_cols, list) else []
    var_list = [str(col) for col in var_cols if col is not None] if isinstance(var_cols, list) else []
    stat_list = [str(stat) for stat in stats if stat is not None] if isinstance(stats, list) else []

    render = "aggregate"
    render += f" class({', '.join(class_list)})"
    render += f" var({', '.join(var_list)})"
    render += f" stats({', '.join(stat_list)})"
    structured = {"class": class_list, "var": var_list, "stats": stat_list}
    return render, structured


def _rendered_transform_payload(rendered: RenderedTransform) -> Dict[str, Any]:
    payload = {
        "op": rendered.op,
        "kind": rendered.kind,
        "render": rendered.render,
    }
    if rendered.structured is not None:
        payload["structured"] = rendered.structured
    return payload


def _lookup_transform_entry(
    transform_id: str,
    registry_specs: Dict[str, Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if transform_id in registry_specs:
        return registry_specs[transform_id]
    if not transform_id.startswith("t:"):
        return registry_specs.get(f"t:{transform_id}")
    return registry_specs.get(transform_id[2:])


def _no_assignment_message(target: str) -> str:
    if target:
        return f"(no assignment for target={target})"
    return "(no assignment for target)"


def _event_attr(event: Any, key: str) -> Any:
    if isinstance(event, dict):
        return event.get(key)
    return getattr(event, key, None)


def _set_event_details(event: Any, details: Dict[str, Any]) -> None:
    if isinstance(event, dict):
        event["details"] = details
    else:
        event.details = details


def _render_expr(expr: Any, parent_prec: int = 0) -> str:
    if expr is None:
        return "<expr:null>"
    if not isinstance(expr, dict):
        return _render_literal(expr)

    expr_type = expr.get("type") or expr.get("kind")
    if expr_type in {"lit", "literal"}:
        return _render_literal(expr.get("value"))

    if expr_type in {"ident", "identifier", "col", "column"}:
        name = expr.get("name") or expr.get("id") or expr.get("value")
        if isinstance(name, str):
            return name
        return "<expr:ident>"

    if expr_type in {"call", "func", "function"}:
        name = expr.get("name") or expr.get("fn") or expr.get("op")
        args = expr.get("args") or []
        rendered_args = [_render_expr(arg, 0) for arg in args]
        name = name or "call"
        return f"{name}({', '.join(rendered_args)})"

    if expr_type == "if" or (expr.get("op") == "if" and ("args" in expr or "cond" in expr)):
        if "args" in expr:
            args = expr.get("args") or []
            rendered_args = [_render_expr(arg, 0) for arg in args]
        else:
            rendered_args = [
                _render_expr(expr.get("cond"), 0),
                _render_expr(expr.get("then"), 0),
                _render_expr(expr.get("else"), 0),
            ]
        return f"if({', '.join(rendered_args)})"

    if expr_type in {"binop", "binary"}:
        op = expr.get("op") or expr.get("operator")
        prec = _op_precedence(op)
        left_expr = _render_expr(expr.get("left"), prec)
        right_expr = _render_expr(expr.get("right"), prec + 1)
        rendered = f"{left_expr} {op} {right_expr}"
        if prec < parent_prec:
            return f"({rendered})"
        return rendered

    if expr_type in {"lookup", "map_lookup", "map", "index", "subscript"}:
        base = expr.get("name") or expr.get("base") or expr.get("map")
        key = expr.get("key") or expr.get("index")
        base_rendered = base if isinstance(base, str) else _render_expr(base, 100)
        key_rendered = _render_expr(key, 0)
        return f"{base_rendered}[{key_rendered}]"

    if expr_type in {"callable"} and "op" in expr and "args" in expr:
        name = expr.get("op")
        args = expr.get("args") or []
        rendered_args = [_render_expr(arg, 0) for arg in args]
        return f"{name}({', '.join(rendered_args)})"

    if expr_type:
        return f"<expr:{expr_type}>"
    return "<expr>"


def _render_literal(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    return json.dumps(value, ensure_ascii=False)


def _op_precedence(op: Optional[str]) -> int:
    if op in {"*", "/"}:
        return 70
    if op in {"+", "-"}:
        return 60
    if op in {"<", "<=", ">", ">="}:
        return 50
    if op in {"==", "!="}:
        return 40
    return 30


def _render_bool(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return "true" if bool(value) else "false"

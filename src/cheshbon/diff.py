"""Diff command: wrapper around kernel diff/impact with report generation."""

import json
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Literal, Any
from datetime import datetime, timezone

# Import kernel functions (don't modify kernel)
from cheshbon.kernel.spec import MappingSpec
from cheshbon.kernel.transform_registry import TransformRegistry
from cheshbon.kernel.diff import ChangeEvent
from cheshbon.kernel.impact import ImpactResult
from cheshbon._internal.reporting.explain import explain_changes, explain_impact


def load_spec(path: Path) -> MappingSpec:
    """Load a mapping spec from JSON file."""
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return MappingSpec(**data)


# find_latest_specs is imported from spec_draft to avoid duplication


def group_events_by_type(events: List) -> Dict[str, List]:
    """Group change events by type."""
    grouped = {}
    for event in events:
        event_type = event.change_type
        if event_type not in grouped:
            grouped[event_type] = []
        grouped[event_type].append(event)
    return grouped


def generate_markdown_report(
    impact_result: ImpactResult,
    change_events: List,
    spec_v1: MappingSpec,
    spec_v2: MappingSpec,
    edge_kinds: Optional[Dict[tuple[str, str], str]] = None,
    value_evidence: Optional[Dict[str, Dict[str, Any]]] = None,
    registry_v1: Optional[TransformRegistry] = None,
    registry_v2: Optional[TransformRegistry] = None,
    refusal_info: Optional[Dict[str, Any]] = None,
    schema_lock_section: Optional[Dict[str, Any]] = None,
    schema_changes_section: Optional[Dict[str, Any]] = None,
    schema_changes_lines: Optional[List[str]] = None,
    contract_changed: bool = False,
) -> str:
    """Generate markdown impact report."""
    lines = []
    
    # Top section: skim-friendly summary
    lines.append("# Impact Analysis Report")
    lines.append("")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    
    # Refusal (run-diff: bundle refused -> run did not execute)
    if refusal_info:
        lines.append("## [!] Bundle Refused")
        lines.append("")
        for key in ("a", "b"):
            part = refusal_info.get(key)
            if isinstance(part, dict):
                code = part.get("code", "")
                message = part.get("message", "")
                loc = part.get("loc")
                lines.append(f"- **Bundle {key.upper()}**: `{code}` — {message}")
                if loc:
                    lines.append(f"  - loc: {loc}")
        lines.append("")
        lines.append("Kernel and value diffs skipped (run did not execute).")
        lines.append("")
    
    # Schema lock section (run-diff)
    if schema_lock_section:
        lines.append("## Schema Lock")
        lines.append("")
        lines.append(f"- Lock used (A): {schema_lock_section.get('lock_used_a', False)}")
        lines.append(f"- Lock used (B): {schema_lock_section.get('lock_used_b', False)}")
        if schema_lock_section.get("lock_hash_a") is not None:
            lines.append(f"- Schema contract hash A: `{schema_lock_section['lock_hash_a']}`")
        if schema_lock_section.get("lock_hash_b") is not None:
            lines.append(f"- Schema contract hash B: `{schema_lock_section['lock_hash_b']}`")
        lines.append(f"- Contract changed: {schema_lock_section.get('contract_changed', False)}")
        if schema_lock_section.get("provenance_changed"):
            lines.append("- Provenance changed (e.g. created_by); contract unchanged.")
        if schema_lock_section.get("datasources_changed"):
            lines.append(f"- Datasources changed: {', '.join(schema_lock_section['datasources_changed'])}")
        lines.append("")
    
    # Schema changes (evidence diff)
    if schema_changes_lines:
        lines.append("## Schema Changes (evidence)")
        lines.append("")
        for line in schema_changes_lines:
            lines.append(f"- {line}")
        lines.append("")
    elif schema_changes_section and schema_changes_section.get("per_table"):
        lines.append("## Schema Changes (evidence)")
        lines.append("")
        for t in schema_changes_section["per_table"]:
            parts = []
            if t.get("columns_added"):
                parts.append("+" + ",".join(t["columns_added"]))
            if t.get("columns_removed"):
                parts.append("-" + ",".join(t["columns_removed"]))
            for c in t.get("types_changed", []):
                parts.append(f"{c.get('column', '')} {c.get('old_type', '')}->{c.get('new_type', '')}")
            lines.append(f"- {t.get('table', '')}: " + " ".join(parts))
        lines.append("")
    
    if contract_changed:
        lines.append("## Contract Changed")
        lines.append("")
        lines.append("Schema lock hash differs; downstream diffs may reflect contract change.")
        lines.append("")
    
    # Run status
    if impact_result.validation_failed:
        lines.append("## [!] Run Status: NON-EXECUTABLE")
    elif impact_result.impacted:
        lines.append("## [!] Run Status: IMPACTED")
    else:
        lines.append("## [OK] Run Status: NO IMPACT")
    lines.append("")
    
    # Counts
    spec_events = [e for e in change_events if not e.change_type.startswith("TRANSFORM_")]
    registry_events = [e for e in change_events if e.change_type.startswith("TRANSFORM_")]
    
    lines.append("### Summary")
    lines.append("")
    lines.append(f"- **Change Events**: {len(change_events)}")
    lines.append(f"  - Spec events: {len(spec_events)}")
    lines.append(f"  - Registry events: {len(registry_events)}")
    lines.append(f"- **Impacted Nodes**: {len(impact_result.impacted)}")
    lines.append(f"- **Unaffected Nodes**: {len(impact_result.unaffected)}")
    lines.append(f"- **Missing Bindings**: {sum(len(s) for s in impact_result.missing_bindings.values())}")
    lines.append(f"- **Missing Transforms**: {sum(len(s) for s in impact_result.missing_transform_refs.values())}")
    lines.append("")
    
    # What changed (grouped)
    if spec_events:
        lines.append("## Spec Changes")
        lines.append("")
        grouped = group_events_by_type(spec_events)
        for event_type, events in sorted(grouped.items()):
            lines.append(f"### {event_type.replace('_', ' ').title()}")
            lines.append("")
            for event in events:
                if event.change_type == "DERIVED_TRANSFORM_REF_CHANGED":
                    if event.old_value and event.new_value:
                        lines.append(f"- `{event.element_id}`: `{event.old_value}` -> `{event.new_value}`")
                    elif event.old_value:
                        lines.append(f"- `{event.element_id}`: removed (`{event.old_value}`)")
                    elif event.new_value:
                        lines.append(f"- `{event.element_id}`: added (`{event.new_value}`)")
                    else:
                        lines.append(f"- `{event.element_id}`")
                    if event.details:
                        mode = event.details.get("mode")
                        mode_suffix = ""
                        if mode == "update":
                            mode_suffix = " (update!)"
                        elif mode == "derive":
                            mode_suffix = " (derive)"
                        old_render = event.details.get("old_render")
                        new_render = event.details.get("new_render")
                        if old_render:
                            lines.append(f"  - old: `{old_render}`{mode_suffix}")
                        if new_render:
                            lines.append(f"  - new: `{new_render}`{mode_suffix}")
                        old_transform = event.details.get("old_transform") if isinstance(event.details, dict) else None
                        new_transform = event.details.get("new_transform") if isinstance(event.details, dict) else None
                        if isinstance(old_transform, dict):
                            old_full = old_transform.get("render")
                            if old_full:
                                lines.append(f"  - old transform: `{old_full}`")
                        if isinstance(new_transform, dict):
                            new_full = new_transform.get("render")
                            if new_full:
                                lines.append(f"  - new transform: `{new_full}`")
                else:
                    if event.old_value and event.new_value:
                        lines.append(f"- `{event.element_id}`: `{event.old_value}` -> `{event.new_value}`")
                    elif event.old_value:
                        lines.append(f"- `{event.element_id}`: removed (`{event.old_value}`)")
                    elif event.new_value:
                        lines.append(f"- `{event.element_id}`: added (`{event.new_value}`)")
                    else:
                        lines.append(f"- `{event.element_id}`")
            lines.append("")
    
    if registry_events:
        lines.append("## Registry Changes")
        lines.append("")
        grouped = group_events_by_type(registry_events)
        for event_type, events in sorted(grouped.items()):
            lines.append(f"### {event_type.replace('_', ' ').title()}")
            lines.append("")
            for event in events:
                lines.append(f"- `{event.element_id}`")
                if event.change_type in {"TRANSFORM_ADDED", "TRANSFORM_REMOVED"} and event.details:
                    render = event.details.get("render") if isinstance(event.details, dict) else None
                    if render:
                        lines.append(f"  - spec: `{render}`")
            lines.append("")
    
    # Validation errors
    if impact_result.validation_failed:
        lines.append("## Validation Errors")
        lines.append("")
        lines.append("**Run is not executable due to:**")
        lines.append("")
        for error in impact_result.validation_errors:
            lines.append(f"- {error}")
        lines.append("")
    
    # Impacted variables table
    if impact_result.impacted:
        lines.append("## Impacted Variables")
        lines.append("")
        if value_evidence is not None:
            lines.append("| ID | Name | Reason | Dependency Path | Value Change |")
            lines.append("|----|------|--------|-----------------|--------------|")
        else:
            lines.append("| ID | Name | Reason | Dependency Path |")
            lines.append("|----|------|--------|-----------------|")
        
        for var_id in sorted(impact_result.impacted):
            derived = spec_v1.get_derived_by_id(var_id) or spec_v2.get_derived_by_id(var_id)
            var_name = derived.name if derived else var_id
            
            reason = impact_result.impact_reasons.get(var_id, "UNKNOWN")
            
            # Format dependency path
            path = impact_result.impact_paths.get(var_id, [var_id])
            path_names = []
            for node_id in path:
                if node_id.startswith("s:"):
                    source = spec_v1.get_source_by_id(node_id) or spec_v2.get_source_by_id(node_id)
                    path_names.append(source.name if source else node_id)
                elif node_id.startswith("d:"):
                    derived_node = spec_v1.get_derived_by_id(node_id) or spec_v2.get_derived_by_id(node_id)
                    path_names.append(derived_node.name if derived_node else node_id)
                else:
                    path_names.append(node_id)
            if edge_kinds and len(path_names) > 1:
                max_nodes = 5
                path_ids_display = path[:max_nodes]
                path_names_display = path_names[:max_nodes]
                path_str = path_names_display[0]
                for idx in range(len(path_names_display) - 1):
                    src_id = path_ids_display[idx]
                    dst_id = path_ids_display[idx + 1]
                    kind = edge_kinds.get((src_id, dst_id))
                    if kind:
                        path_str += f" -({kind})-> {path_names_display[idx + 1]}"
                    else:
                        path_str += f" -> {path_names_display[idx + 1]}"
                if len(path_names) > max_nodes:
                    path_str += " -> ..."
            else:
                path_str = " -> ".join(path_names[:5])  # Limit length
                if len(path_names) > 5:
                    path_str += " -> ..."
            
            if value_evidence is not None:
                value_summary = ""
                evidence = value_evidence.get(var_id)
                if evidence:
                    if evidence.get("available"):
                        value_summary = evidence.get("summary") or ""
                    else:
                        value_summary = "(no value evidence)"
                lines.append(f"| `{var_id}` | {var_name} | {reason} | {path_str} | {value_summary} |")
            else:
                lines.append(f"| `{var_id}` | {var_name} | {reason} | {path_str} |")
        
        lines.append("")
        
        # Missing details
        has_missing = False
        for var_id in sorted(impact_result.impacted):
            missing_inputs = impact_result.unresolved_references.get(var_id, set())
            missing_bindings = impact_result.missing_bindings.get(var_id, set())
            missing_transforms = impact_result.missing_transform_refs.get(var_id, set())
            
            if missing_inputs or missing_bindings or missing_transforms:
                if not has_missing:
                    lines.append("### Missing Dependencies")
                    lines.append("")
                    has_missing = True
                
                derived = spec_v1.get_derived_by_id(var_id) or spec_v2.get_derived_by_id(var_id)
                var_name = derived.name if derived else var_id
                lines.append(f"**{var_name}** (`{var_id}`):")
                
                if missing_inputs:
                    lines.append(f"- Missing inputs: {', '.join(sorted(missing_inputs))}")
                if missing_bindings:
                    lines.append(f"- Missing bindings: {', '.join(sorted(missing_bindings))}")
                if missing_transforms:
                    lines.append(f"- Missing transforms: {', '.join(sorted(missing_transforms))}")
                lines.append("")
    else:
        lines.append("## Impacted Variables")
        lines.append("")
        lines.append("None.")
        lines.append("")
    
    # Next actions
    lines.append("## Next Actions")
    lines.append("")
    
    actions = []
    if impact_result.missing_transform_refs:
        actions.append("- **Restore transform** or pin registry version")
    if impact_result.missing_bindings:
        actions.append("- **Bind missing sources** ")
    if any(e.change_type == "SOURCE_RENAMED" for e in change_events):
        actions.append("- **Apply rename** using `cheshbon bindings apply-rename --old <old> --new <new>`")
    if impact_result.impacted and not impact_result.validation_failed:
        actions.append("- **Review impacted variables** and update downstream dependencies")
    
    if actions:
        for action in actions:
            lines.append(action)
    else:
        lines.append("No actions required.")
    
    lines.append("")
    
    # Detailed explanations (from kernel)
    lines.append("---")
    lines.append("")
    lines.append("## Detailed Explanations")
    lines.append("")
    lines.append(explain_changes(change_events, spec_v1, spec_v2))
    lines.append("")
    lines.append(explain_impact(impact_result, spec_v1))
    
    return "\n".join(lines)


def _build_path_edges(
    path_nodes: List[str],
    edge_kinds: Optional[Dict[tuple[str, str], str]] = None,
) -> List[Dict[str, str]]:
    if len(path_nodes) < 2:
        return []
    edges: List[Dict[str, str]] = []
    for idx in range(len(path_nodes) - 1):
        src = path_nodes[idx]
        dst = path_nodes[idx + 1]
        if edge_kinds is not None:
            kind = edge_kinds.get((src, dst))
            if kind is None:
                return []
        else:
            kind = "unknown"
        edges.append({"src": src, "dst": dst, "kind": kind})
    return edges


def generate_json_report(
    impact_result: ImpactResult,
    change_events: List,
    edge_kinds: Optional[Dict[tuple[str, str], str]] = None,
    validation_findings: Optional[Dict[str, List[Dict[str, str]]]] = None,
    value_evidence: Optional[Dict[str, Dict[str, Any]]] = None,
    refusal_info: Optional[Dict[str, Any]] = None,
    schema_lock_section: Optional[Dict[str, Any]] = None,
    schema_changes_section: Optional[Dict[str, Any]] = None,
    contract_changed: bool = False,
) -> Dict:
    """Generate JSON impact report."""
    # Convert sets to lists for JSON serialization
    report = {
        "run_status": "non_executable" if impact_result.validation_failed else ("impacted" if impact_result.impacted else "no_impact"),
        "validation_failed": impact_result.validation_failed,
        "validation_errors": impact_result.validation_errors,
        "summary": {
            "total_events": len(change_events),
            "spec_events": len([e for e in change_events if not e.change_type.startswith("TRANSFORM_")]),
            "registry_events": len([e for e in change_events if e.change_type.startswith("TRANSFORM_")]),
            "impacted_count": len(impact_result.impacted),
            "unaffected_count": len(impact_result.unaffected),
            "missing_bindings_count": sum(len(s) for s in impact_result.missing_bindings.values()),
            "missing_transforms_count": sum(len(s) for s in impact_result.missing_transform_refs.values())
        },
        "change_events": [
            {
                "change_type": e.change_type,
                "element_id": e.element_id,
                "old_value": e.old_value,
                "new_value": e.new_value,
                "details": e.details
            }
            for e in change_events
        ],
        "impacted": sorted(list(impact_result.impacted)),
        "unaffected": sorted(list(impact_result.unaffected)),
        "impact_details": {
            var_id: {
                "reason": impact_result.impact_reasons.get(var_id, "UNKNOWN"),
                "path": _build_path_edges(
                    impact_result.impact_paths.get(var_id, []),
                    edge_kinds=edge_kinds,
                ),
                "missing_inputs": sorted(list(impact_result.unresolved_references.get(var_id, set()))),
                "missing_bindings": sorted(list(impact_result.missing_bindings.get(var_id, set()))),
                "missing_transforms": [
                    {"side": "b", "transform_id": transform_id}
                    for transform_id in sorted(list(impact_result.missing_transform_refs.get(var_id, set())))
                ],
                **({"value_evidence": value_evidence.get(var_id)} if value_evidence and var_id in value_evidence else {}),
            }
            for var_id in impact_result.impacted
        },
    }
    if validation_findings is not None:
        report["validation_findings"] = validation_findings
    if refusal_info is not None:
        report["refusal_info"] = refusal_info
    if schema_lock_section is not None:
        report["schema_lock"] = schema_lock_section
    if schema_changes_section is not None:
        report["schema_changes"] = schema_changes_section
    if contract_changed:
        report["contract_changed"] = True
    return report


def generate_core_json_report(diff_result: "DiffResult") -> Dict:
    """Generate a minimal JSON report (no paths, no explanations)."""
    total_events = sum(diff_result.change_summary.values())
    spec_events = sum(
        count for event_type, count in diff_result.change_summary.items()
        if not event_type.startswith("TRANSFORM_")
    )
    registry_events = total_events - spec_events

    return {
        "run_status": "non_executable" if diff_result.validation_failed else ("impacted" if diff_result.impacted_ids else "no_impact"),
        "validation_failed": diff_result.validation_failed,
        "validation_errors": diff_result.validation_errors,
        "summary": {
            "total_events": total_events,
            "spec_events": spec_events,
            "registry_events": registry_events,
            "impacted_count": len(diff_result.impacted_ids),
            "unaffected_count": len(diff_result.unaffected_ids),
            "missing_bindings_count": sum(len(s) for s in diff_result.missing_bindings.values()),
            "missing_transforms_count": sum(len(s) for s in diff_result.missing_transform_refs.values())
        },
        "change_events": list(diff_result.events),
        "impacted": list(diff_result.impacted_ids),
        "unaffected": list(diff_result.unaffected_ids),
        "reasons": dict(diff_result.reasons),
        "missing_inputs": dict(diff_result.missing_inputs),
        "missing_bindings": dict(diff_result.missing_bindings),
        "ambiguous_bindings": dict(diff_result.ambiguous_bindings),
        "missing_transform_refs": dict(diff_result.missing_transform_refs),
    }


def _diff_result_to_change_events(diff_result: "DiffResult") -> List[ChangeEvent]:
    """Convert DiffResult event dicts to ChangeEvent objects."""
    events: List[ChangeEvent] = []
    for event in diff_result.events:
        events.append(ChangeEvent(
            change_type=event.get("change_type"),
            element_id=event.get("element_id"),
            old_value=event.get("old_value"),
            new_value=event.get("new_value"),
            details=event.get("details")
        ))
    return events


def _diff_result_to_impact_result(diff_result: "DiffResult") -> ImpactResult:
    """Convert DiffResult to ImpactResult for report rendering."""
    return ImpactResult(
        impacted=set(diff_result.impacted_ids),
        unaffected=set(diff_result.unaffected_ids),
        impact_paths=dict(diff_result.paths),
        impact_reasons=dict(diff_result.reasons),
        unresolved_references={k: set(v) for k, v in diff_result.missing_inputs.items()},
        missing_bindings={k: set(v) for k, v in diff_result.missing_bindings.items()},
        ambiguous_bindings={k: set(v) for k, v in diff_result.ambiguous_bindings.items()},
        missing_transform_refs={k: set(v) for k, v in diff_result.missing_transform_refs.items()},
        alternative_path_counts=dict(diff_result.alternative_path_counts),
        validation_failed=diff_result.validation_failed,
        validation_errors=list(diff_result.validation_errors)
    )


def run_diff(
    spec_v1_path: Path,
    spec_v2_path: Path,
    output_dir: Optional[Path] = None,
    registry_v1_path: Optional[Path] = None,
    registry_v2_path: Optional[Path] = None,
    bindings_path: Optional[Path] = None,
    from_bindings_path: Optional[Path] = None,
    to_bindings_path: Optional[Path] = None,
    return_content: bool = False,
    report_mode: Literal["full", "core", "all-details", "off"] = "full"
) -> Tuple[int, str, str]:
    """
    Run diff analysis and generate reports.
    
    This is a thin wrapper over cheshbon.api.diff() that generates file outputs.
    The actual diff logic lives in api.diff() to keep CLI and API in sync.
    
    Returns:
        Tuple of (exit_code, md_path, json_path)
        Exit codes: 0 = no impact, 1 = impact found, 2 = validation_failed
    """
    # Import API (single source of truth)
    from .api import diff, DiffResult
    
    if report_mode not in ("full", "core", "all-details", "off"):
        raise ValueError("report_mode must be 'full', 'core', 'all-details', or 'off'")
    
    detail_level: Literal["full", "core"] = "full" if report_mode in ("full", "all-details") else "core"
    
    # Call API to get DiffResult (single source of truth for logic)
    # Determine bindings argument - bindings are evaluated against the 'to' spec
    # Prefer to_bindings_path, fall back to bindings_path if provided
    to_bindings_arg = to_bindings_path if to_bindings_path else (bindings_path if bindings_path else None)
    
    # Determine registry arguments - both must be provided together or neither
    from_registry_arg = registry_v1_path if registry_v1_path and registry_v1_path.exists() else None
    to_registry_arg = registry_v2_path if registry_v2_path and registry_v2_path.exists() else None
    
    # If only one registry path exists, use it for both (single registry mode)
    if from_registry_arg is None and to_registry_arg is None:
        # No registries provided
        pass
    elif from_registry_arg is None and to_registry_arg is not None:
        # Only to_registry provided - use it for both (no diff, just validation)
        from_registry_arg = to_registry_arg
    elif from_registry_arg is not None and to_registry_arg is None:
        # Only from_registry provided - use it for both (no diff, just validation)
        to_registry_arg = from_registry_arg
    
    if report_mode == "all-details":
        from .api import diff_all_details
        from ._internal.canonical_json import canonical_dumps

        report_dict = diff_all_details(
            from_spec=spec_v1_path,
            to_spec=spec_v2_path,
            from_registry=from_registry_arg,
            to_registry=to_registry_arg,
            to_bindings=to_bindings_arg,
        )
        run_status = report_dict.get("run_status")
        if run_status == "non_executable":
            exit_code = 2
        elif run_status == "impacted":
            exit_code = 1
        else:
            exit_code = 0

        report_json_str = canonical_dumps(report_dict)
        if return_content:
            return exit_code, "", report_json_str
        if output_dir is None:
            raise ValueError("output_dir must be specified when return_content is False")
        output_dir.mkdir(parents=True, exist_ok=True)
        json_path = output_dir / "impact.all-details.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            f.write(report_json_str + "\n")
        return exit_code, "", str(json_path)

    # Call API
    result: DiffResult = diff(
        from_spec=spec_v1_path,
        to_spec=spec_v2_path,
        from_registry=from_registry_arg,
        to_registry=to_registry_arg,
        to_bindings=to_bindings_arg,
        detail_level=detail_level
    )
    
    # Determine exit code
    if result.validation_failed:
        exit_code = 2
    elif result.impacted_ids:
        exit_code = 1
    else:
        exit_code = 0

    if report_mode == "off":
        if return_content:
            return exit_code, "", ""
        return exit_code, "", ""

    if report_mode == "core":
        json_content_dict = generate_core_json_report(result)
        json_content_str = json.dumps(json_content_dict, indent=2, ensure_ascii=False)
        if return_content:
            return exit_code, "", json_content_str
        if output_dir is None:
            raise ValueError("output_dir must be specified when return_content is False")
        output_dir.mkdir(parents=True, exist_ok=True)
        json_path = output_dir / "impact.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            f.write(json_content_str)
        return exit_code, "", str(json_path)

    # Full report generation
    spec_v1 = load_spec(spec_v1_path)
    spec_v2 = load_spec(spec_v2_path)
    change_events = _diff_result_to_change_events(result)
    impact_result = _diff_result_to_impact_result(result)

    md_content = generate_markdown_report(
        impact_result,
        change_events,
        spec_v1,
        spec_v2
    )

    json_content_dict = generate_json_report(impact_result, change_events)
    json_content_str = json.dumps(json_content_dict, indent=2, ensure_ascii=False)

    if return_content:
        return exit_code, md_content, json_content_str

    if output_dir is None:
        raise ValueError("output_dir must be specified when return_content is False")

    output_dir.mkdir(parents=True, exist_ok=True)
    md_path = output_dir / "impact.md"
    json_path = output_dir / "impact.json"

    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(md_content)

    with open(json_path, 'w', encoding='utf-8') as f:
        f.write(json_content_str)

    return exit_code, str(md_path), str(json_path)

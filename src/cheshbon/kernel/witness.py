"""Witness computation for all-details reports (pure logic)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from cheshbon.kernel.spec import MappingSpec
from cheshbon.kernel.graph import DependencyGraph
from cheshbon.kernel.impact import ImpactResult
from cheshbon.kernel.all_details_builders import build_event_index, build_issues_index


def _apply_cap(items: List[str], cap: int, path: str, omissions: List[Dict[str, Any]]) -> List[str]:
    if cap <= 0 or len(items) <= cap:
        return items
    kept = items[:cap]
    omitted = items[cap:]
    sample = []
    if omitted:
        sample = [omitted[0]]
        if len(omitted) > 1:
            sample.append(omitted[-1])
    omissions.append({
        "path": path,
        "cap": cap,
        "actual": len(items),
        "omitted_count": len(omitted),
        "sample_ids": sample,
    })
    return kept


def compute_witnesses(
    diff_result: "DiffResult",
    impact_result: ImpactResult,
    spec_v1: MappingSpec,
    spec_v2: MappingSpec,
    graph_v1: DependencyGraph,
    graph_v2: DependencyGraph,
    caps: Dict[str, int],
) -> Dict[str, Any]:
    """Compute witnesses and summaries for all-details."""
    omissions: List[Dict[str, Any]] = []

    event_index, event_ids_by_element = build_event_index(diff_result.events)
    issues_index, issue_id_map = build_issues_index(diff_result)

    max_witnesses = caps.get("max_witnesses", 100000)
    max_root_causes = caps.get("max_root_causes_per_node", 16)
    max_trigger_events = caps.get("max_trigger_events_per_node", 16)
    max_top_roots = caps.get("max_top_roots", 50)

    witnesses: Dict[str, Dict[str, Any]] = {}
    impacted_ids = sorted(diff_result.impacted_ids)
    impacted_ids = _apply_cap(impacted_ids, max_witnesses, "details.witnesses", omissions)

    for var_id in impacted_ids:
        reason = diff_result.reasons.get(var_id, "UNKNOWN")
        path = diff_result.paths.get(var_id, [])

        # Root cause selection
        if reason in ("DIRECT_CHANGE", "DIRECT_CHANGE_MISSING_INPUT", "TRANSFORM_IMPL_CHANGED", "TRANSFORM_REMOVED", "MISSING_TRANSFORM_REF"):
            root_cause_ids = [var_id]
        elif reason == "MISSING_INPUT":
            missing_ids = diff_result.missing_inputs.get(var_id, [])
            if missing_ids:
                root_cause_ids = sorted(missing_ids)
            elif path:
                root_cause_ids = [path[0]]
            else:
                root_cause_ids = [var_id]
        elif reason == "MISSING_BINDING":
            root_cause_ids = sorted(diff_result.missing_bindings.get(var_id, []))
        elif reason == "AMBIGUOUS_BINDING":
            root_cause_ids = sorted(diff_result.ambiguous_bindings.get(var_id, []))
        elif reason == "TRANSITIVE_DEPENDENCY":
            root_cause_ids = [path[0]] if path else [var_id]
        else:
            root_cause_ids = [var_id]

        root_cause_ids = _apply_cap(
            root_cause_ids,
            max_root_causes,
            f"details.witnesses.{var_id}.root_cause_ids",
            omissions,
        )

        # Distance + predecessor
        if reason in ("DIRECT_CHANGE", "DIRECT_CHANGE_MISSING_INPUT", "TRANSFORM_IMPL_CHANGED", "TRANSFORM_REMOVED", "MISSING_TRANSFORM_REF"):
            distance = 0
            predecessor = None
        elif reason in ("MISSING_BINDING", "AMBIGUOUS_BINDING"):
            distance = 1
            predecessor = root_cause_ids[0] if root_cause_ids else None
        elif path and len(path) > 1:
            distance = len(path) - 1
            predecessor = path[-2]
        else:
            distance = 0
            predecessor = None

        # Triggering events
        triggering_event_ids: List[str] = []
        if reason in ("DIRECT_CHANGE", "DIRECT_CHANGE_MISSING_INPUT"):
            triggering_event_ids = event_ids_by_element.get(var_id, [])
        elif reason == "MISSING_INPUT":
            for root_id in root_cause_ids:
                triggering_event_ids.extend(event_ids_by_element.get(root_id, []))
        elif reason == "TRANSITIVE_DEPENDENCY":
            if root_cause_ids:
                triggering_event_ids = event_ids_by_element.get(root_cause_ids[0], [])
        elif reason in ("TRANSFORM_IMPL_CHANGED", "TRANSFORM_REMOVED"):
            derived = spec_v1.get_derived_by_id(var_id) or spec_v2.get_derived_by_id(var_id)
            transform_ref = derived.transform_ref if derived else None
            if transform_ref:
                triggering_event_ids = event_ids_by_element.get(transform_ref, [])

        triggering_event_ids = sorted(set(triggering_event_ids))
        triggering_event_ids = _apply_cap(
            triggering_event_ids,
            max_trigger_events,
            f"details.witnesses.{var_id}.triggering_event_ids",
            omissions,
        )

        # Triggering issues (non-change causes)
        triggering_issue_ids: List[str] = []
        if reason in ("MISSING_BINDING", "AMBIGUOUS_BINDING"):
            for root_id in root_cause_ids:
                issue_id = issue_id_map.get((reason, root_id, var_id))
                if issue_id:
                    triggering_issue_ids.append(issue_id)
        elif reason == "MISSING_TRANSFORM_REF":
            derived = spec_v2.get_derived_by_id(var_id) or spec_v1.get_derived_by_id(var_id)
            transform_ref = derived.transform_ref if derived else None
            if transform_ref:
                issue_id = issue_id_map.get((reason, transform_ref, var_id))
                if issue_id:
                    triggering_issue_ids.append(issue_id)

        triggering_issue_ids = sorted(set(triggering_issue_ids))
        triggering_issue_ids = _apply_cap(
            triggering_issue_ids,
            max_trigger_events,
            f"details.witnesses.{var_id}.triggering_issue_ids",
            omissions,
        )

        witnesses[var_id] = {
            "reason": reason,
            "root_cause_ids": root_cause_ids,
            "distance": distance,
            "predecessor": predecessor,
            "triggering_event_ids": triggering_event_ids,
            "triggering_issue_ids": triggering_issue_ids,
        }

    # Summaries (based on included witnesses)
    reason_counts: Dict[str, int] = {}
    max_distance = 0
    root_counts: Dict[str, int] = {}
    for witness in witnesses.values():
        reason = witness.get("reason", "UNKNOWN")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        distance = witness.get("distance", 0)
        if distance > max_distance:
            max_distance = distance
        for root_id in witness.get("root_cause_ids", []):
            root_counts[root_id] = root_counts.get(root_id, 0) + 1

    top_roots = sorted(
        [{"id": root_id, "impacted_count": count} for root_id, count in root_counts.items()],
        key=lambda x: (-x["impacted_count"], x["id"]),
    )
    if len(top_roots) > max_top_roots:
        omitted = top_roots[max_top_roots:]
        omissions.append({
            "path": "details.summaries.top_root_causes",
            "cap": max_top_roots,
            "actual": len(top_roots),
            "omitted_count": len(omitted),
            "sample_ids": [o["id"] for o in omitted[:2]] if omitted else [],
        })
        top_roots = top_roots[:max_top_roots]

    events_by_type: Dict[str, int] = {}
    for event in diff_result.events:
        change_type = event.get("change_type", "UNKNOWN")
        events_by_type[change_type] = events_by_type.get(change_type, 0) + 1

    return {
        "event_index": event_index,
        "issues_index": issues_index,
        "witnesses": witnesses,
        "summaries": {
            "reasons": reason_counts,
            "events_by_type": events_by_type,
            "max_distance": max_distance,
            "top_root_causes": top_roots,
        },
        "omissions": omissions,
    }

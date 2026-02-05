"""Shared builders for all-details event and issue indexes."""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Tuple

from cheshbon._internal.canonical_json import canonical_dumps


def _short_digest(obj: Any) -> str:
    canonical = canonical_dumps(obj)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return digest[:8]


def build_event_index(events: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, List[str]]]:
    """Build event index entries and a lookup map from element_id -> [event_id]."""
    event_index: List[Dict[str, Any]] = []
    event_ids_by_element: Dict[str, List[str]] = {}
    for seq, event in enumerate(events, start=1):
        event_id = f"evt:{_short_digest(event)}"
        event_index.append({
            "event_id": event_id,
            "event_seq": seq,
            **event,
        })
        element_id = event.get("element_id", "")
        if element_id:
            event_ids_by_element.setdefault(element_id, []).append(event_id)
    # Stable order for lookups
    for element_id in event_ids_by_element:
        event_ids_by_element[element_id] = sorted(event_ids_by_element[element_id])
    return event_index, event_ids_by_element


def build_issues_index(
    diff_result: "DiffResult",
) -> Tuple[List[Dict[str, Any]], Dict[Tuple[str, str, str], str]]:
    """Build issue index entries and a lookup map keyed by (issue_type, element_id, affected_id)."""
    issues_index: List[Dict[str, Any]] = []
    issue_id_map: Dict[Tuple[str, str, str], str] = {}
    seq = 1

    def add_issue(issue_type: str, element_id: str, affected_id: str, details: Dict[str, Any]) -> None:
        nonlocal seq
        issue_core = {
            "issue_type": issue_type,
            "element_id": element_id,
            "details": details,
        }
        issue_id = f"iss:{_short_digest(issue_core)}"
        issues_index.append({
            "issue_id": issue_id,
            "issue_seq": seq,
            **issue_core,
        })
        issue_id_map[(issue_type, element_id, affected_id)] = issue_id
        seq += 1

    # Missing bindings: derived_id -> [source_ids]
    for derived_id, source_ids in diff_result.missing_bindings.items():
        for source_id in sorted(source_ids):
            add_issue(
                "MISSING_BINDING",
                source_id,
                derived_id,
                {"affected_id": derived_id, "source_id": source_id},
            )

    # Ambiguous bindings: derived_id -> [source_ids]
    for derived_id, source_ids in diff_result.ambiguous_bindings.items():
        for source_id in sorted(source_ids):
            add_issue(
                "AMBIGUOUS_BINDING",
                source_id,
                derived_id,
                {"affected_id": derived_id, "source_id": source_id},
            )

    # Missing transform refs: derived_id -> [transform_ref_ids]
    for derived_id, transform_ids in diff_result.missing_transform_refs.items():
        for transform_id in sorted(transform_ids):
            add_issue(
                "MISSING_TRANSFORM_REF",
                transform_id,
                derived_id,
                {"affected_id": derived_id, "transform_ref": transform_id},
            )

    return issues_index, issue_id_map

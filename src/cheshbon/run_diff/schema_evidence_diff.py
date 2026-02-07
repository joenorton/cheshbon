"""Schema evidence diff: compare schema.evidence.json between two runs (per-table +cols, -cols, type changes)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from cheshbon._internal.io.sans_bundle import (
    ARTIFACT_NAME_SCHEMA_EVIDENCE,
    SchemaEvidence,
    _resolve_artifact_path,
)
from cheshbon._internal.io.sans_bundle import SansReport


def load_schema_evidence(bundle_dir: Path, report: SansReport) -> Optional[SchemaEvidence]:
    """Load schema.evidence.json from bundle when present (report.artifacts only when indexed)."""
    path = _resolve_artifact_path(bundle_dir, report, ARTIFACT_NAME_SCHEMA_EVIDENCE)
    if not path or not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return SchemaEvidence.from_raw(data)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


@dataclass
class TableSchemaChange:
    """Per-table schema changes (no rename inference in v-next)."""

    table: str
    columns_added: List[str] = field(default_factory=list)
    columns_removed: List[str] = field(default_factory=list)
    types_changed: List[tuple] = field(default_factory=list)  # (col, old_type, new_type)


@dataclass
class SchemaEvidenceDiffResult:
    """Result of diffing two schema evidence files."""

    per_table: List[TableSchemaChange] = field(default_factory=list)


def diff_schema_evidence(
    evidence_a: Optional[SchemaEvidence],
    evidence_b: Optional[SchemaEvidence],
) -> SchemaEvidenceDiffResult:
    """
    Compare schema evidence: per table columns added, removed, type changed.
    No rename inference.
    """
    result = SchemaEvidenceDiffResult()
    tables_a = (evidence_a.tables if evidence_a else {}) or {}
    tables_b = (evidence_b.tables if evidence_b else {}) or {}
    all_tables = sorted(set(tables_a.keys()) | set(tables_b.keys()))

    for table in all_tables:
        cols_a = tables_a.get(table, {}) or {}
        cols_b = tables_b.get(table, {}) or {}
        added = sorted(set(cols_b.keys()) - set(cols_a.keys()))
        removed = sorted(set(cols_a.keys()) - set(cols_b.keys()))
        type_changes: List[tuple] = []
        for c in sorted(set(cols_a.keys()) & set(cols_b.keys())):
            if cols_a.get(c) != cols_b.get(c):
                type_changes.append((c, cols_a.get(c, ""), cols_b.get(c, "")))

        if added or removed or type_changes:
            result.per_table.append(
                TableSchemaChange(
                    table=table,
                    columns_added=added,
                    columns_removed=removed,
                    types_changed=type_changes,
                )
            )

    return result


def schema_evidence_section_for_report(diff_result: SchemaEvidenceDiffResult) -> Dict[str, Any]:
    """Compact dict for JSON report: schema changes section."""
    section: Dict[str, Any] = {}
    if diff_result.per_table:
        section["per_table"] = [
            {
                "table": t.table,
                "columns_added": t.columns_added,
                "columns_removed": t.columns_removed,
                "types_changed": [
                    {"column": c[0], "old_type": c[1], "new_type": c[2]}
                    for c in t.types_changed
                ],
            }
            for t in diff_result.per_table
        ]
    return section


def format_schema_evidence_diff_compact(diff_result: SchemaEvidenceDiffResult) -> List[str]:
    """Human-readable compact lines for markdown (e.g. 'table X: +a, -b, c type str->int')."""
    lines: List[str] = []
    for t in diff_result.per_table:
        parts: List[str] = []
        if t.columns_added:
            parts.append("+" + ",".join(t.columns_added))
        if t.columns_removed:
            parts.append("-" + ",".join(t.columns_removed))
        for col, old_t, new_t in t.types_changed:
            parts.append(f"{col} {old_t}->{new_t}")
        if parts:
            lines.append(f"{t.table}: " + " ".join(parts))
    return lines

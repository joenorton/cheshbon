"""Schema lock diff: compare schema.lock.json between two bundles.

Contract comparison uses a normalized view (datasource name, kind, columns, rules only);
created_by and other provenance metadata are excluded so they do not trigger contract_changed.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from cheshbon._internal.canonical_json import canonical_dumps
from cheshbon._internal.io.sans_bundle import (
    ARTIFACT_NAME_SCHEMA_LOCK,
    SchemaLock,
    _resolve_artifact_path,
)
from cheshbon._internal.io.sans_bundle import SansReport

def _contract_view_from_raw(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Build normalized contract view: datasource name, kind, columns (name+type), rules only.
    Excludes created_by and other provenance. Deterministic order (datasources by name, columns by name).
    """
    out_datasources: List[Dict[str, Any]] = []
    ds_list = raw.get("datasources")
    if not isinstance(ds_list, list):
        return {"datasources": []}
    for item in ds_list:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        if name is None:
            continue
        # columns: list of {name, type} -> normalized sorted list
        cols_raw = item.get("columns")
        if isinstance(cols_raw, list):
            cols = sorted(
                ({"name": str(c.get("name", "")), "type": str(c.get("type", ""))} for c in cols_raw if isinstance(c, dict) and c.get("name") is not None),
                key=lambda x: (x["name"], x["type"]),
            )
        elif isinstance(cols_raw, dict):
            cols = sorted(
                [{"name": str(c), "type": str(t)} for c, t in cols_raw.items()],
                key=lambda x: (x["name"], x["type"]),
            )
        else:
            cols = []
        ds_contract: Dict[str, Any] = {"name": str(name), "columns": cols}
        if "kind" in item:
            ds_contract["kind"] = str(item["kind"])
        rules = item.get("rules")
        if isinstance(rules, dict):
            ds_contract["rules"] = dict(sorted(rules.items()))
        out_datasources.append(ds_contract)
    out_datasources.sort(key=lambda d: d.get("name", ""))
    return {"datasources": out_datasources}


def compute_schema_contract_sha256(raw: Dict[str, Any]) -> str:
    """Hash of normalized contract view only (excludes created_by etc.)."""
    view = _contract_view_from_raw(raw)
    return hashlib.sha256(canonical_dumps(view).encode("utf-8")).hexdigest()


def compute_lock_provenance_sha256(raw: Dict[str, Any]) -> str:
    """Hash of full canonical lock JSON (includes created_by; for provenance_changed)."""
    return hashlib.sha256(canonical_dumps(raw).encode("utf-8")).hexdigest()


def _lock_to_canonical_dict(lock: SchemaLock) -> Dict[str, Any]:
    """Stable dict for hashing: datasources with sorted keys and column dicts sorted (legacy fallback)."""
    out: Dict[str, Any] = {}
    for ds_id in sorted(lock.datasources.keys()):
        ds = lock.datasources[ds_id]
        out[ds_id] = {"columns": dict(sorted(ds.columns.items()))}
    return out


def load_schema_lock(bundle_dir: Path, report: SansReport) -> Optional[SchemaLock]:
    """Load schema.lock.json from bundle when present (report.artifacts or root fallback)."""
    path = _resolve_artifact_path(bundle_dir, report, ARTIFACT_NAME_SCHEMA_LOCK)
    if not path or not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return SchemaLock.from_raw(data)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


def load_schema_lock_and_raw(bundle_dir: Path, report: SansReport) -> Tuple[Optional[SchemaLock], Optional[Dict[str, Any]]]:
    """Load schema.lock.json and return (SchemaLock, raw dict) for contract/provenance hashing."""
    path = _resolve_artifact_path(bundle_dir, report, ARTIFACT_NAME_SCHEMA_LOCK)
    if not path or not path.exists():
        return None, None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return SchemaLock.from_raw(raw), raw
    except (json.JSONDecodeError, TypeError, ValueError):
        return None, None


@dataclass
class DatasourceSchemaChange:
    """Per-datasource schema changes."""

    datasource: str
    columns_added: List[str] = field(default_factory=list)
    columns_removed: List[str] = field(default_factory=list)
    types_changed: List[tuple] = field(default_factory=list)  # (col, old_type, new_type)


@dataclass
class SchemaLockDiffResult:
    """Result of diffing two schema locks."""

    lock_hash_a: Optional[str] = None  # schema_contract_sha256 (semantic only)
    lock_hash_b: Optional[str] = None
    contract_changed: bool = False  # semantic contract only (excludes created_by etc.)
    provenance_changed: bool = False  # full lock (e.g. created_by) differed
    lock_provenance_sha256_a: Optional[str] = None
    lock_provenance_sha256_b: Optional[str] = None
    datasources_changed: List[str] = field(default_factory=list)
    per_datasource: List[DatasourceSchemaChange] = field(default_factory=list)
    lock_used_a: bool = False
    lock_used_b: bool = False
    schema_lock_sha256_a: Optional[str] = None  # from report if present (provenance)
    schema_lock_sha256_b: Optional[str] = None


def diff_schema_locks(
    lock_a: Optional[SchemaLock],
    lock_b: Optional[SchemaLock],
    report_a: Optional[Any] = None,
    report_b: Optional[Any] = None,
    raw_a: Optional[Dict[str, Any]] = None,
    raw_b: Optional[Dict[str, Any]] = None,
) -> SchemaLockDiffResult:
    """
    Compare two schema locks. contract_changed uses normalized contract view only
    (datasource name/kind/columns/rules); created_by and other provenance are excluded.
    provenance_changed is true when full lock content (e.g. created_by) differs.
    """
    result = SchemaLockDiffResult()
    if report_a and hasattr(report_a, "schema_lock_sha256") and report_a.schema_lock_sha256:
        result.schema_lock_sha256_a = report_a.schema_lock_sha256
    if report_b and hasattr(report_b, "schema_lock_sha256") and report_b.schema_lock_sha256:
        result.schema_lock_sha256_b = report_b.schema_lock_sha256

    result.lock_used_a = lock_a is not None
    result.lock_used_b = lock_b is not None

    # Contract hash: from normalized view only (excludes created_by)
    if raw_a is not None:
        result.lock_hash_a = compute_schema_contract_sha256(raw_a)
        result.lock_provenance_sha256_a = compute_lock_provenance_sha256(raw_a)
    elif lock_a is not None:
        result.lock_hash_a = hashlib.sha256(
            canonical_dumps(_lock_to_canonical_dict(lock_a)).encode("utf-8")
        ).hexdigest()
    if raw_b is not None:
        result.lock_hash_b = compute_schema_contract_sha256(raw_b)
        result.lock_provenance_sha256_b = compute_lock_provenance_sha256(raw_b)
    elif lock_b is not None:
        result.lock_hash_b = hashlib.sha256(
            canonical_dumps(_lock_to_canonical_dict(lock_b)).encode("utf-8")
        ).hexdigest()

    if result.lock_hash_a and result.lock_hash_b:
        result.contract_changed = result.lock_hash_a != result.lock_hash_b
    if result.lock_provenance_sha256_a and result.lock_provenance_sha256_b:
        result.provenance_changed = result.lock_provenance_sha256_a != result.lock_provenance_sha256_b

    if lock_a is None and lock_b is None:
        return result

    # Per-datasource diff (structural only; no created_by)
    all_ds = set()
    if lock_a:
        all_ds.update(lock_a.datasources.keys())
    if lock_b:
        all_ds.update(lock_b.datasources.keys())

    for ds_id in sorted(all_ds):
        ds_a = lock_a.datasources.get(ds_id) if lock_a else None
        ds_b = lock_b.datasources.get(ds_id) if lock_b else None
        cols_a = (ds_a.columns if ds_a else {}) or {}
        cols_b = (ds_b.columns if ds_b else {}) or {}

        added = sorted(set(cols_b.keys()) - set(cols_a.keys()))
        removed = sorted(set(cols_a.keys()) - set(cols_b.keys()))
        type_changes: List[tuple] = []
        for c in sorted(set(cols_a.keys()) & set(cols_b.keys())):
            if cols_a.get(c) != cols_b.get(c):
                type_changes.append((c, cols_a.get(c, ""), cols_b.get(c, "")))

        if added or removed or type_changes:
            result.datasources_changed.append(ds_id)
            result.per_datasource.append(
                DatasourceSchemaChange(
                    datasource=ds_id,
                    columns_added=added,
                    columns_removed=removed,
                    types_changed=type_changes,
                )
            )

    return result


def schema_lock_section_for_report(diff_result: SchemaLockDiffResult) -> Dict[str, Any]:
    """Compact dict for JSON report: schema lock section."""
    section: Dict[str, Any] = {
        "lock_used_a": diff_result.lock_used_a,
        "lock_used_b": diff_result.lock_used_b,
        "contract_changed": diff_result.contract_changed,
    }
    if diff_result.lock_hash_a is not None:
        section["schema_contract_sha256_a"] = diff_result.lock_hash_a
        section["lock_hash_a"] = diff_result.lock_hash_a  # backward compat
    if diff_result.lock_hash_b is not None:
        section["schema_contract_sha256_b"] = diff_result.lock_hash_b
        section["lock_hash_b"] = diff_result.lock_hash_b
    if diff_result.provenance_changed:
        section["provenance_changed"] = True
    if diff_result.lock_provenance_sha256_a is not None:
        section["lock_provenance_sha256_a"] = diff_result.lock_provenance_sha256_a
    if diff_result.lock_provenance_sha256_b is not None:
        section["lock_provenance_sha256_b"] = diff_result.lock_provenance_sha256_b
    if diff_result.datasources_changed:
        section["datasources_changed"] = diff_result.datasources_changed
    if diff_result.per_datasource:
        section["per_datasource"] = [
            {
                "datasource": d.datasource,
                "columns_added": d.columns_added,
                "columns_removed": d.columns_removed,
                "types_changed": [
                    {"column": c[0], "old_type": c[1], "new_type": c[2]}
                    for c in d.types_changed
                ],
            }
            for d in diff_result.per_datasource
        ]
    return section

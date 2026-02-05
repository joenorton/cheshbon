"""Load and verify graph.json from a bundle."""

from __future__ import annotations

import json
import posixpath
from pathlib import Path
from typing import Any, Dict

from cheshbon.kernel.graph_v1 import GraphV1, parse_graph_v1
from cheshbon.kernel.hash_utils import compute_canonical_json_sha256


class GraphBundleError(ValueError):
    """Raised when bundle graph ingestion fails."""


def load_graph_from_bundle(bundle_dir: Path) -> GraphV1:
    """Load graph.json from bundle, verify hash, and validate schema."""
    report_path = bundle_dir / "report.json"
    if not report_path.exists():
        raise GraphBundleError("Missing report.json in bundle")

    report_data = json.loads(report_path.read_text(encoding="utf-8"))
    artifact_entry = _find_graph_artifact(report_data)
    expected_sha = artifact_entry.get("sha256")
    if not isinstance(expected_sha, str) or not expected_sha:
        raise GraphBundleError("artifacts/graph.json entry missing sha256 in report.json")

    graph_path = bundle_dir / "artifacts" / "graph.json"
    if not graph_path.exists():
        raise GraphBundleError("Missing artifacts/graph.json in bundle")

    actual_sha = compute_canonical_json_sha256(graph_path)
    if actual_sha != expected_sha:
        raise GraphBundleError(
            f"graph.json hash mismatch. Expected {expected_sha}, got {actual_sha}"
        )

    graph_data = json.loads(graph_path.read_text(encoding="utf-8"))
    return parse_graph_v1(graph_data)


def _find_graph_artifact(report_data: Dict[str, Any]) -> Dict[str, Any]:
    artifacts = report_data.get("artifacts")
    if not isinstance(artifacts, list):
        raise GraphBundleError("report.json missing artifacts list")

    for entry in artifacts:
        if not isinstance(entry, dict):
            continue
        path_val = entry.get("path")
        if not isinstance(path_val, str):
            continue
        normalized = posixpath.normpath(path_val.replace("\\", "/"))
        if normalized == "artifacts/graph.json" and path_val.replace("\\", "/") == "artifacts/graph.json":
            return entry

    raise GraphBundleError("report.json missing artifacts/graph.json entry")

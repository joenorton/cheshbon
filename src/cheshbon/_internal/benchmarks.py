"""Performance sentinel benchmarks (doctor --full)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from time import perf_counter
from typing import Tuple

from cheshbon.diff import run_diff


def _find_repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "fixtures").is_dir():
            return parent
    raise FileNotFoundError("Could not locate repo root with fixtures/")


def _budget_from_env(var_name: str, default_ms: float) -> float:
    raw = os.getenv(var_name)
    if not raw:
        return default_ms
    try:
        return float(raw)
    except ValueError:
        return default_ms


SENTINEL_ROOT = _find_repo_root() / "fixtures" / "sentinels"

MAX_LINEAR_CHAIN_MS = _budget_from_env("CHESHBON_MAX_LINEAR_CHAIN_MS", 1000.0)
MAX_WIDE_FANOUT_MS = _budget_from_env("CHESHBON_MAX_WIDE_FANOUT_MS", 1000.0)
MAX_DIAMOND_MERGE_MS = _budget_from_env("CHESHBON_MAX_DIAMOND_MERGE_MS", 1000.0)
MAX_BINDING_FAILURE_MS = _budget_from_env("CHESHBON_MAX_BINDING_FAILURE_MS", 700.0)
MAX_MANY_INDEPENDENT_CHANGES_MS = _budget_from_env("CHESHBON_MAX_MANY_INDEPENDENT_CHANGES_MS", 1000.0)


def _run_sentinel(case: str, bindings: bool = False) -> Tuple[float, str]:
    case_dir = SENTINEL_ROOT / case
    spec_v1 = case_dir / "spec_v1.json"
    spec_v2 = case_dir / "spec_v2.json"
    bindings_path = case_dir / "bindings.json" if bindings else None

    start = perf_counter()
    _, _, json_str = run_diff(
        spec_v1,
        spec_v2,
        bindings_path=bindings_path,
        return_content=True,
        report_mode="core"
    )
    elapsed_ms = (perf_counter() - start) * 1000.0
    return elapsed_ms, json_str


def run_sentinel_case(case: str, bindings: bool = False) -> Tuple[float, dict]:
    """Run sentinel case and return elapsed ms plus parsed JSON report."""
    elapsed_ms, json_str = _run_sentinel(case, bindings)
    report = json.loads(json_str)
    return elapsed_ms, report


def benchmark_sentinel_linear_chain() -> float:
    elapsed_ms, _ = _run_sentinel("linear_chain")
    return elapsed_ms


def benchmark_sentinel_wide_fanout() -> float:
    elapsed_ms, _ = _run_sentinel("wide_fanout")
    return elapsed_ms


def benchmark_sentinel_diamond_merge() -> float:
    elapsed_ms, _ = _run_sentinel("diamond_merge")
    return elapsed_ms


def benchmark_sentinel_binding_failure() -> float:
    elapsed_ms, _ = _run_sentinel("binding_failure", bindings=True)
    return elapsed_ms


def benchmark_sentinel_many_independent_changes() -> float:
    elapsed_ms, _ = _run_sentinel("many_independent_changes")
    return elapsed_ms

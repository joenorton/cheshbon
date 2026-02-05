"""Performance sentinels (gated)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from cheshbon.diff import run_diff
from cheshbon._internal.benchmarks import (
    MAX_LINEAR_CHAIN_MS,
    MAX_WIDE_FANOUT_MS,
    MAX_DIAMOND_MERGE_MS,
    MAX_BINDING_FAILURE_MS,
    MAX_MANY_INDEPENDENT_CHANGES_MS,
)

SENTINEL_ROOT = Path(__file__).resolve().parents[2] / "fixtures" / "sentinels"


def _run_case(case: str, bindings: bool = False):
    case_dir = SENTINEL_ROOT / case
    spec_v1 = case_dir / "spec_v1.json"
    spec_v2 = case_dir / "spec_v2.json"
    bindings_path = case_dir / "bindings.json" if bindings else None
    return run_diff(
        spec_v1,
        spec_v2,
        bindings_path=bindings_path,
        return_content=True,
        report_mode="core"
    )


def _assert_budget(benchmark, max_ms: float) -> None:
    mean_ms = benchmark.stats.stats.mean * 1000.0
    assert mean_ms < max_ms, f"Mean {mean_ms:.2f} ms exceeded budget {max_ms:.2f} ms"


@pytest.mark.perf
def test_linear_chain_sentinel(benchmark):
    result = benchmark.pedantic(lambda: _run_case("linear_chain"), rounds=3, iterations=1)
    exit_code, _, json_str = result
    report = json.loads(json_str)

    assert exit_code == 1
    assert report["summary"]["impacted_count"] == 400
    assert report["summary"]["unaffected_count"] == 0

    _assert_budget(benchmark, MAX_LINEAR_CHAIN_MS)


@pytest.mark.perf
def test_wide_fanout_sentinel(benchmark):
    result = benchmark.pedantic(lambda: _run_case("wide_fanout"), rounds=3, iterations=1)
    exit_code, _, json_str = result
    report = json.loads(json_str)

    assert exit_code == 1
    assert report["summary"]["impacted_count"] == 601

    _assert_budget(benchmark, MAX_WIDE_FANOUT_MS)


@pytest.mark.perf
def test_diamond_merge_sentinel(benchmark):
    result = benchmark.pedantic(lambda: _run_case("diamond_merge"), rounds=3, iterations=1)
    exit_code, _, json_str = result
    report = json.loads(json_str)

    assert exit_code == 1
    assert report["summary"]["impacted_count"] == 154

    _assert_budget(benchmark, MAX_DIAMOND_MERGE_MS)


@pytest.mark.perf
def test_binding_failure_sentinel(benchmark):
    result = benchmark.pedantic(lambda: _run_case("binding_failure", bindings=True), rounds=3, iterations=1)
    exit_code, _, json_str = result
    report = json.loads(json_str)

    assert exit_code == 1
    assert report["summary"]["impacted_count"] == 12
    assert report["summary"]["missing_bindings_count"] >= 2

    _assert_budget(benchmark, MAX_BINDING_FAILURE_MS)


@pytest.mark.perf
def test_many_independent_changes_sentinel(benchmark):
    result = benchmark.pedantic(lambda: _run_case("many_independent_changes"), rounds=3, iterations=1)
    exit_code, _, json_str = result
    report = json.loads(json_str)

    assert exit_code == 1
    assert report["summary"]["impacted_count"] == 300
    assert report["summary"]["unaffected_count"] == 0

    _assert_budget(benchmark, MAX_MANY_INDEPENDENT_CHANGES_MS)

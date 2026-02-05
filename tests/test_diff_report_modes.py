"""Tests for diff report modes (full/core/off)."""

import json
from pathlib import Path

from cheshbon.api import diff
from cheshbon.diff import run_diff


HERE = Path(__file__).resolve().parent
FIXTURES = HERE.parent / "fixtures"


def test_api_diff_core_excludes_paths():
    spec_v1_path = FIXTURES / "scenario2_params_change_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario2_params_change_impact" / "spec_v2.json"

    result = diff(from_spec=spec_v1_path, to_spec=spec_v2_path, detail_level="core")

    assert result.impacted_ids, "core should still compute impacted IDs"
    assert result.reasons, "core should still compute reasons"
    assert result.paths == {}, "core should not compute dependency paths"
    assert result.alternative_path_counts == {}, "core should not compute alternative paths"


def test_run_diff_core_report_mode():
    spec_v1_path = FIXTURES / "scenario2_params_change_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario2_params_change_impact" / "spec_v2.json"

    exit_code, report_md, report_json = run_diff(
        spec_v1_path,
        spec_v2_path,
        return_content=True,
        report_mode="core"
    )

    assert exit_code == 1
    assert report_md == ""
    parsed = json.loads(report_json)
    assert "impact_details" not in parsed
    assert "paths" not in parsed
    assert "reasons" in parsed


def test_run_diff_all_details_report_mode():
    spec_v1_path = FIXTURES / "scenario2_params_change_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario2_params_change_impact" / "spec_v2.json"

    exit_code, report_md, report_json = run_diff(
        spec_v1_path,
        spec_v2_path,
        return_content=True,
        report_mode="all-details"
    )

    assert exit_code == 1
    assert report_md == ""
    parsed = json.loads(report_json)
    assert "details" in parsed
    assert "witnesses" in parsed["details"]


def test_run_diff_off_report_mode():
    spec_v1_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v2.json"

    exit_code, report_md, report_json = run_diff(
        spec_v1_path,
        spec_v2_path,
        return_content=True,
        report_mode="off"
    )

    assert exit_code == 0
    assert report_md == ""
    assert report_json == ""

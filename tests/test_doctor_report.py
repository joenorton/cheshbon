"""Tests for report doctor (all-details verification)."""

from pathlib import Path

from cheshbon.api import diff_all_details
from cheshbon._internal.report_doctor import run_doctor_report
from cheshbon._internal.canonical_json import canonical_dumps

HERE = Path(__file__).resolve().parent
FIXTURES = HERE.parent / "fixtures"


def _write_report(tmp_path: Path, report: dict) -> Path:
    report_path = tmp_path / "impact.all-details.json"
    report_path.write_text(canonical_dumps(report) + "\n", encoding="utf-8")
    return report_path


def test_doctor_report_ok(tmp_path: Path):
    spec_v1_path = FIXTURES / "scenario2_params_change_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario2_params_change_impact" / "spec_v2.json"

    report = diff_all_details(from_spec=spec_v1_path, to_spec=spec_v2_path)
    report_path = _write_report(tmp_path, report)

    result = run_doctor_report(
        report_path=report_path,
        spec_v1_path=spec_v1_path,
        spec_v2_path=spec_v2_path,
    )

    assert result["ok"] is True


def test_doctor_report_detects_tamper(tmp_path: Path):
    spec_v1_path = FIXTURES / "scenario2_params_change_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario2_params_change_impact" / "spec_v2.json"

    report = diff_all_details(from_spec=spec_v1_path, to_spec=spec_v2_path)
    # Tamper with a witness predecessor if any witnesses exist
    witnesses = report.get("details", {}).get("witnesses", {})
    assert witnesses, "Expected at least one witness to tamper with"
    first_key = sorted(witnesses.keys())[0]
    witnesses[first_key]["predecessor"] = "d:FAKE"

    report_path = _write_report(tmp_path, report)

    result = run_doctor_report(
        report_path=report_path,
        spec_v1_path=spec_v1_path,
        spec_v2_path=spec_v2_path,
    )

    assert result["ok"] is False
    assert "witness_invariants" in result["summary"]["failed_clause_ids"]
    core_clause = next(c for c in result["clauses"] if c["id"] == "core_digest")
    assert core_clause["ok"] is True


def test_doctor_report_reason_mismatch(tmp_path: Path):
    spec_v1_path = FIXTURES / "scenario2_params_change_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario2_params_change_impact" / "spec_v2.json"

    report = diff_all_details(from_spec=spec_v1_path, to_spec=spec_v2_path)
    witnesses = report.get("details", {}).get("witnesses", {})
    assert witnesses, "Expected at least one witness to tamper with"
    first_key = sorted(witnesses.keys())[0]
    current_reason = witnesses[first_key].get("reason")
    witnesses[first_key]["reason"] = "TRANSITIVE_DEPENDENCY" if current_reason != "TRANSITIVE_DEPENDENCY" else "DIRECT_CHANGE"

    report_path = _write_report(tmp_path, report)
    result = run_doctor_report(
        report_path=report_path,
        spec_v1_path=spec_v1_path,
        spec_v2_path=spec_v2_path,
    )

    assert result["ok"] is False
    assert "witness_invariants" in result["summary"]["failed_clause_ids"]


def test_doctor_report_invalid_root_cause_id(tmp_path: Path):
    spec_v1_path = FIXTURES / "scenario2_params_change_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario2_params_change_impact" / "spec_v2.json"

    report = diff_all_details(from_spec=spec_v1_path, to_spec=spec_v2_path)
    witnesses = report.get("details", {}).get("witnesses", {})
    assert witnesses, "Expected at least one witness to tamper with"
    first_key = sorted(witnesses.keys())[0]
    witnesses[first_key]["root_cause_ids"] = ["d:FAKE"]

    report_path = _write_report(tmp_path, report)
    result = run_doctor_report(
        report_path=report_path,
        spec_v1_path=spec_v1_path,
        spec_v2_path=spec_v2_path,
    )

    assert result["ok"] is False
    assert "witness_invariants" in result["summary"]["failed_clause_ids"]


def test_doctor_report_irrelevant_event_linkage(tmp_path: Path):
    spec_v1_path = FIXTURES / "scenario2_params_change_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario2_params_change_impact" / "spec_v2.json"

    report = diff_all_details(from_spec=spec_v1_path, to_spec=spec_v2_path)
    event_index = report.get("details", {}).get("event_index", [])
    assert event_index, "Expected events to tamper with"
    # Tamper event element_id to make linkage irrelevant.
    event_index[0]["element_id"] = "d:FAKE"

    report_path = _write_report(tmp_path, report)
    result = run_doctor_report(
        report_path=report_path,
        spec_v1_path=spec_v1_path,
        spec_v2_path=spec_v2_path,
    )

    assert result["ok"] is False
    assert "witness_invariants" in result["summary"]["failed_clause_ids"]


def test_doctor_report_missing_omissions_for_caps(tmp_path: Path):
    spec_v1_path = FIXTURES / "scenario2_params_change_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario2_params_change_impact" / "spec_v2.json"

    report = diff_all_details(
        from_spec=spec_v1_path,
        to_spec=spec_v2_path,
        caps={"max_witnesses": 1},
    )
    # Remove omissions even though cap is applied.
    report["details"]["omissions"] = []

    report_path = _write_report(tmp_path, report)
    result = run_doctor_report(
        report_path=report_path,
        spec_v1_path=spec_v1_path,
        spec_v2_path=spec_v2_path,
    )

    assert result["ok"] is False
    assert "accounting_invariants" in result["summary"]["failed_clause_ids"]


def test_doctor_report_dishonest_omission_actual(tmp_path: Path):
    spec_v1_path = FIXTURES / "scenario2_params_change_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario2_params_change_impact" / "spec_v2.json"

    report = diff_all_details(
        from_spec=spec_v1_path,
        to_spec=spec_v2_path,
        caps={"max_witnesses": 1},
    )
    omissions = report.get("details", {}).get("omissions", [])
    witness_omission = next(o for o in omissions if o.get("path") == "details.witnesses")
    witness_omission["actual"] = witness_omission["cap"]

    report_path = _write_report(tmp_path, report)
    result = run_doctor_report(
        report_path=report_path,
        spec_v1_path=spec_v1_path,
        spec_v2_path=spec_v2_path,
    )

    assert result["ok"] is False
    assert "accounting_invariants" in result["summary"]["failed_clause_ids"]


def test_doctor_report_missing_caps(tmp_path: Path):
    spec_v1_path = FIXTURES / "scenario2_params_change_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario2_params_change_impact" / "spec_v2.json"

    report = diff_all_details(from_spec=spec_v1_path, to_spec=spec_v2_path)
    report["details"].pop("caps", None)

    report_path = _write_report(tmp_path, report)
    result = run_doctor_report(
        report_path=report_path,
        spec_v1_path=spec_v1_path,
        spec_v2_path=spec_v2_path,
    )

    assert result["ok"] is False
    assert "accounting_invariants" in result["summary"]["failed_clause_ids"]


def test_doctor_report_issue_linkage_mismatch(tmp_path: Path):
    spec_v1_path = FIXTURES / "scenario5_ambiguous_binding" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario5_ambiguous_binding" / "spec_v2.json"
    bindings_path = FIXTURES / "scenario5_ambiguous_binding" / "bindings_v2.json"

    report = diff_all_details(
        from_spec=spec_v1_path,
        to_spec=spec_v2_path,
        to_bindings=bindings_path,
    )
    witnesses = report.get("details", {}).get("witnesses", {})
    issues = report.get("details", {}).get("issues_index", [])
    assert witnesses and issues, "Expected witnesses and issues to tamper with"

    # Tamper the first issue entry to no longer match the witness linkage.
    issues[0]["element_id"] = "s:FAKE"

    report_path = _write_report(tmp_path, report)
    result = run_doctor_report(
        report_path=report_path,
        spec_v1_path=spec_v1_path,
        spec_v2_path=spec_v2_path,
        bindings_path=bindings_path,
    )

    assert result["ok"] is False
    assert "witness_invariants" in result["summary"]["failed_clause_ids"]

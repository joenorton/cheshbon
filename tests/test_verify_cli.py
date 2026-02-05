"""CLI tests for verify subcommands."""

import json
from pathlib import Path
import sys

import pytest

from cheshbon import cli


def _run_cli(args, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["cheshbon"] + args)
    return cli.main()


def _write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def test_verify_spec_ok(monkeypatch, capsys):
    spec_path = Path("fixtures/scenario1_rename_no_impact/spec_v1.json")
    _run_cli(["verify", "spec", str(spec_path)], monkeypatch)
    out = capsys.readouterr().out
    assert "Status: OK" in out
    assert "Errors: 0" in out


def test_verify_registry_ok(monkeypatch, capsys):
    registry_path = Path("fixtures/scenario3_registry_impl_change/registry_v1.json")
    _run_cli(["verify", "registry", str(registry_path)], monkeypatch)
    out = capsys.readouterr().out
    assert "Status: OK" in out
    assert "Errors: 0" in out


def test_verify_spec_with_registry_missing_transform_fails(monkeypatch, capsys):
    spec_path = Path("fixtures/scenario4_transform_removed/spec_v2.json")
    registry_path = Path("fixtures/scenario4_transform_removed/registry_v2.json")
    with pytest.raises(SystemExit) as excinfo:
        _run_cli(
            ["verify", "spec", str(spec_path), "--registry", str(registry_path)],
            monkeypatch,
        )
    assert excinfo.value.code == 1
    out = capsys.readouterr().out
    assert "Status: FAILED" in out


def test_verify_bindings_ambiguous_fails(monkeypatch, capsys):
    bindings_path = Path("fixtures/scenario5_ambiguous_binding/bindings_v2.json")
    with pytest.raises(SystemExit) as excinfo:
        _run_cli(["verify", "bindings", str(bindings_path)], monkeypatch)
    assert excinfo.value.code == 1
    out = capsys.readouterr().out
    assert "Status: FAILED" in out


def test_verify_bindings_missing_binding_warns(monkeypatch, capsys, tmp_path):
    bindings_path = tmp_path / "bindings.json"
    _write_json(
        bindings_path,
        {
            "table": "RAW_DM",
            "bindings": {
                "BRTHDT": "s:BRTHDT"
            },
        },
    )
    spec_path = Path("fixtures/scenario5_ambiguous_binding/spec_v2.json")
    _run_cli(
        ["verify", "bindings", str(bindings_path), "--spec", str(spec_path)],
        monkeypatch,
    )
    out = capsys.readouterr().out
    assert "Status: OK" in out
    assert "Errors: 0" in out
    assert "Warnings: 1" in out


def test_verify_bindings_with_raw_schema_warns(monkeypatch, capsys, tmp_path):
    bindings_path = tmp_path / "bindings.json"
    raw_schema_path = tmp_path / "raw_schema.json"
    _write_json(
        bindings_path,
        {
            "table": "RAW_DM",
            "bindings": {
                "BRTHDT": "s:BRTHDT",
                "RFSTDTC": "s:RFSTDTC",
            },
        },
    )
    _write_json(
        raw_schema_path,
        {
            "table": "RAW_DM",
            "columns": [
                {"name": "BRTHDT", "type": "date"},
            ],
        },
    )
    _run_cli(
        ["verify", "bindings", str(bindings_path), "--raw-schema", str(raw_schema_path)],
        monkeypatch,
    )
    out = capsys.readouterr().out
    assert "Status: OK" in out
    assert "Errors: 0" in out
    assert "Warnings: 1" in out


def test_verify_spec_with_bindings_and_raw_schema_warns(monkeypatch, capsys, tmp_path):
    bindings_path = tmp_path / "bindings.json"
    raw_schema_path = tmp_path / "raw_schema.json"
    _write_json(
        bindings_path,
        {
            "table": "RAW_DM",
            "bindings": {
                "BRTHDT": "s:BRTHDT",
                "UNKNOWN": "s:EXTRA",
            },
        },
    )
    _write_json(
        raw_schema_path,
        {
            "table": "RAW_DM",
            "columns": [
                {"name": "BRTHDT", "type": "date"},
            ],
        },
    )
    spec_path = Path("fixtures/scenario5_ambiguous_binding/spec_v2.json")
    _run_cli(
        [
            "verify",
            "spec",
            str(spec_path),
            "--bindings",
            str(bindings_path),
            "--raw-schema",
            str(raw_schema_path),
        ],
        monkeypatch,
    )
    out = capsys.readouterr().out
    assert "Status: OK" in out
    assert "Errors: 0" in out
    assert "Warnings: 2" in out

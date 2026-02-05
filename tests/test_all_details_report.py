"""Tests for all-details report generation and determinism."""

import json
import random
from pathlib import Path

from cheshbon.api import diff_all_details
from cheshbon._internal.canonical_json import canonical_dumps
from cheshbon.kernel.spec import MappingSpec
from cheshbon.kernel.diff import diff_specs
from cheshbon.kernel.graph import DependencyGraph
from cheshbon.kernel.impact import compute_impact

HERE = Path(__file__).resolve().parent
FIXTURES = HERE.parent / "fixtures"


def test_all_details_deterministic():
    spec_v1_path = FIXTURES / "scenario2_params_change_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario2_params_change_impact" / "spec_v2.json"

    report1 = diff_all_details(from_spec=spec_v1_path, to_spec=spec_v2_path)
    report2 = diff_all_details(from_spec=spec_v1_path, to_spec=spec_v2_path)

    assert canonical_dumps(report1) == canonical_dumps(report2)


def test_all_details_deterministic_realish_pair():
    spec_v1_path = FIXTURES / "scenario2_params_change_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario2_params_change_impact" / "spec_v2.json"

    report1 = diff_all_details(from_spec=spec_v1_path, to_spec=spec_v2_path)
    report2 = diff_all_details(from_spec=spec_v1_path, to_spec=spec_v2_path)

    json1 = canonical_dumps(report1)
    json2 = canonical_dumps(report2)

    assert json1 == json2


def test_all_details_has_witnesses():
    spec_v1_path = FIXTURES / "scenario2_params_change_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario2_params_change_impact" / "spec_v2.json"

    report = diff_all_details(from_spec=spec_v1_path, to_spec=spec_v2_path)

    assert "details" in report
    assert "witnesses" in report["details"]
    assert isinstance(report["details"]["witnesses"], dict)


def test_impact_reason_order_insensitive():
    spec_v1_path = FIXTURES / "scenario2_params_change_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario2_params_change_impact" / "spec_v2.json"

    spec_v1 = MappingSpec(**json.loads(Path(spec_v1_path).read_text(encoding="utf-8")))
    spec_v2 = MappingSpec(**json.loads(Path(spec_v2_path).read_text(encoding="utf-8")))

    change_events = diff_specs(spec_v1, spec_v2)
    graph_v1 = DependencyGraph(spec_v1)

    impact_a = compute_impact(
        spec_v1=spec_v1,
        spec_v2=spec_v2,
        graph_v1=graph_v1,
        change_events=change_events,
        registry_v2=None,
        compute_paths=True,
    )

    shuffled = list(change_events)
    random.Random(1337).shuffle(shuffled)
    impact_b = compute_impact(
        spec_v1=spec_v1,
        spec_v2=spec_v2,
        graph_v1=graph_v1,
        change_events=shuffled,
        registry_v2=None,
        compute_paths=True,
    )

    assert impact_a.impact_reasons == impact_b.impact_reasons

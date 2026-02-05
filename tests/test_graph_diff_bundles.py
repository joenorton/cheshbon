from pathlib import Path

from cheshbon.api import graph_diff_bundles


def test_graph_diff_bundles_payload_change():
    bundle_a = Path("fixtures/graph_bundles/filter_a")
    bundle_b = Path("fixtures/graph_bundles/filter_b")

    diff, impact = graph_diff_bundles(bundle_a, bundle_b)

    assert diff.graph_a_sha256 != diff.graph_b_sha256
    assert len(diff.changed_step_nodes) == 1
    assert diff.changed_step_nodes[0].id == "s:filter"

    assert impact.seed_steps == ["s:filter"]
    assert impact.impacted_steps == ["s:select", "s:sort"]
    assert impact.touched_tables == ["t:high_value__2"]
    assert impact.impacted_tables == ["t:high_value", "t:sorted_high"]
    assert set(impact.seed_reasons.keys()) == set(impact.seed_steps)

    select_reason = impact.reasons["s:select"][0]
    assert select_reason.reason == "transitive"
    assert select_reason.from_step == "s:filter"
    assert select_reason.via_table == "t:high_value__2"

    sort_reason = impact.reasons["s:sort"][0]
    assert sort_reason.reason == "transitive"
    assert sort_reason.from_step == "s:select"
    assert sort_reason.via_table == "t:high_value"

    assert impact.paths["s:filter"] == ["s:filter"]
    assert impact.paths["s:select"] == ["s:filter", "t:high_value__2", "s:select"]

from pathlib import Path

from cheshbon.api import graph_diff_bundles


def test_modified_transform_pairing_step_id_change():
    bundle_a = Path("fixtures/graph_bundles/compute_change_a")
    bundle_b = Path("fixtures/graph_bundles/compute_change_b")

    diff, impact = graph_diff_bundles(bundle_a, bundle_b)

    assert diff.graph_a_sha256 != diff.graph_b_sha256

    modified = {(m.transform_class_id, m.op): m for m in diff.modified_by_class}
    assert ("tc:compute", "compute") in modified
    assert [s.id for s in modified[("tc:compute", "compute")].from_steps] == ["s:compute_v1"]
    assert [s.id for s in modified[("tc:compute", "compute")].to_steps] == ["s:compute_v2"]
    assert modified[("tc:compute", "compute")].classification == "rewire_only"

    assert ("tc:filter", "filter") in modified
    assert [s.id for s in modified[("tc:filter", "filter")].from_steps] == ["s:filter_v1"]
    assert [s.id for s in modified[("tc:filter", "filter")].to_steps] == ["s:filter_v2"]
    assert modified[("tc:filter", "filter")].classification == "rewire_only"

    event_types = [event.type for event in diff.events]
    assert event_types.count("step_replaced_same_class") == 2
    assert all(
        event.type != "step_added" or event.step_id not in {"s:compute_v2", "s:filter_v2"}
        for event in diff.events
    )
    assert all(
        event.type != "step_removed" or event.step_id not in {"s:compute_v1", "s:filter_v1"}
        for event in diff.events
    )

    seed_reasons = impact.seed_reasons.get("s:filter_v2", [])
    assert any(
        reason.reason == "modified_by_class"
        and reason.transform_class_id == "tc:filter"
        and reason.classification == "rewire_only"
        and [s.id for s in reason.to_steps] == ["s:filter_v2"]
        for reason in seed_reasons
    )
    assert set(impact.seed_reasons.keys()) == set(impact.seed_steps)

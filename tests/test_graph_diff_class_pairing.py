from pathlib import Path

from cheshbon.api import graph_diff_bundles


def test_modified_by_class_param_value_change():
    bundle_a = Path("fixtures/graph_diff/ex1")
    bundle_b = Path("fixtures/graph_diff/ex2")

    diff, impact = graph_diff_bundles(bundle_a, bundle_b)

    entry = next(m for m in diff.modified_by_class if m.op == "compute")
    assert entry.classification == "param_value_change"
    assert [s.id for s in entry.from_steps] == [
        "s:b9a012e46daa4899e0cbf07b6cf8bad9e0cfde365452f8f39d5df8618e0c9f04"
    ]
    assert [s.id for s in entry.to_steps] == [
        "s:1ccc13ed7f2ba1d8dcae333ba6a18ed098da944009f7a3e810930a860488601f"
    ]

    expected_steps = {
        "s:977b7390ce7cf819f15b1aa0823523930b3f85d861a67cef04b126402efb1742",
        "s:e1bafba763900b9b4669993a01eb85e324483236aa9ffc7beef4cde19466a720",
        "s:b725a1317e2513112563fe4d6126100efe2af4b48464b226060274fd7128f146",
    }
    assert expected_steps.issubset(set(impact.impacted_steps))

    assert impact.touched_tables == ["t:high_value__1"]

    expected_tables = {
        "t:high_value__2",
        "t:high_value",
        "t:sorted_high",
    }
    assert expected_tables.issubset(set(impact.impacted_tables))
    assert set(impact.seed_reasons.keys()) == set(impact.seed_steps)

"""Unit tests for transform spec rendering."""

from cheshbon.run_diff.transform_render import render_transform


def test_render_filter_predicate():
    entry = {
        "kind": "filter",
        "spec": {
            "op": "filter",
            "params": {
                "predicate": {
                    "left": {"name": "value", "type": "col"},
                    "op": ">",
                    "right": {"type": "lit", "value": 250},
                    "type": "binop",
                }
            },
        },
    }
    rendered = render_transform(entry)
    assert rendered.render == "filter(value > 250)"
    assert rendered.structured == {"predicate": "value > 250"}


def test_render_rename_mapping():
    entry = {
        "kind": "rename",
        "spec": {
            "op": "rename",
            "params": {
                "mapping": [
                    {"from": "C", "to": "D"},
                    {"from": "A", "to": "B"},
                ]
            },
        },
    }
    rendered = render_transform(entry)
    assert rendered.render == "rename(A -> B, C -> D)"
    assert rendered.structured == {
        "mapping": [{"from": "A", "to": "B"}, {"from": "C", "to": "D"}]
    }


def test_render_select_order_preserved():
    entry = {
        "kind": "select",
        "spec": {"op": "select", "params": {"cols": ["b", "a"]}},
    }
    rendered = render_transform(entry)
    assert rendered.render == "select b, a"
    assert rendered.structured == {"cols": ["b", "a"]}

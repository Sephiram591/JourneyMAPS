"""Focused tests for the JourneyMAPS container adapter extension."""

from __future__ import annotations

from jmaps.journey.containers import (
    MISSING_ENV_VALUE,
    contains_missing_environment_values,
    deserialize_reduced_environment,
    register_container_adapter,
    register_sequence_container,
    serialize_reduced_environment,
)
from jmaps.journey.schema import EnvNode, EnvTree


def make_tree(path: tuple[object, ...], *, marker: str = "used") -> EnvTree:
    root = EnvNode("<root>")
    node = root
    for selector in path:
        node = node.ensure_child(selector)
    setattr(node, marker, True)
    return EnvTree(0, root)


def test_indexed_list_path_and_sparse_round_trip() -> None:
    env = {
        "records": [
            {"x": 1, "y": 2},
            {"x": 3, "y": 4},
        ]
    }
    paths = make_tree(("records", 1, "x")).get_used_leaves(env)
    assert paths == {("records", 1, "x")}

    schema, payload = serialize_reduced_environment(env, paths)
    restored = deserialize_reduced_environment(schema, payload)

    assert type(restored["records"]) is list
    assert len(restored["records"]) == 2
    assert restored["records"][0] is MISSING_ENV_VALUE
    assert restored["records"][1] == {"x": 3}
    assert contains_missing_environment_values(restored)


def test_negative_index_is_canonicalized() -> None:
    env = {"records": [{"x": 1}, {"x": 3}]}
    paths = make_tree(("records", -1, "x")).get_used_leaves(env)
    assert paths == {("records", 1, "x")}


def test_complete_tuple_round_trip() -> None:
    env = {"coords": (10, 20)}
    paths = make_tree(("coords",)).get_used_leaves(env)
    schema, payload = serialize_reduced_environment(env, paths)
    restored = deserialize_reduced_environment(schema, payload)

    assert restored == env
    assert type(restored["coords"]) is tuple
    assert not contains_missing_environment_values(restored)


def test_empty_list_retains_type() -> None:
    env = {"empty": []}
    paths = make_tree(("empty",)).get_used_leaves(env)
    schema, payload = serialize_reduced_environment(env, paths)
    restored = deserialize_reduced_environment(schema, payload)

    assert restored == env
    assert type(restored["empty"]) is list


def test_post_registered_sequence_class() -> None:
    class VendorVector:
        def __init__(self, values):
            self.values = tuple(values)

        def __len__(self):
            return len(self.values)

        def __getitem__(self, index):
            return self.values[index]

    register_sequence_container(
        VendorVector,
        constructor=VendorVector,
        type_id="tests.VendorVector.v1",
    )

    env = {"vector": VendorVector([4, 5, 6])}
    schema, payload = serialize_reduced_environment(env, {("vector", 1)})
    restored = deserialize_reduced_environment(schema, payload)

    assert type(restored["vector"]) is VendorVector
    assert restored["vector"][0] is MISSING_ENV_VALUE
    assert restored["vector"][1] == 5
    assert restored["vector"][2] is MISSING_ENV_VALUE


def test_post_registered_keyed_class() -> None:
    class VendorBox:
        def __init__(self, entries, label):
            self.entries = dict(entries)
            self.label = label

    register_container_adapter(
        VendorBox,
        type_id="tests.VendorBox.v1",
        iter_children=lambda value: value.entries.items(),
        get_child=lambda value, key: value.entries[key],
        get_metadata=lambda value: {"label": value.label},
        build=lambda children, metadata: VendorBox(
            children,
            metadata["label"],
        ),
    )

    env = {"box": VendorBox({"left": 8, "right": 9}, "sample")}
    schema, payload = serialize_reduced_environment(
        env,
        {("box", "right")},
    )
    restored = deserialize_reduced_environment(schema, payload)

    assert type(restored["box"]) is VendorBox
    assert restored["box"].entries == {"right": 9}
    assert restored["box"].label == "sample"


def test_complete_container_use_expands_unknown_siblings() -> None:
    env = {"records": ({"x": 1}, {"x": 2})}

    root = EnvNode("<root>")
    records = root.ensure_child("records")
    records.ensure_child(0).ensure_child("x")
    records.mark_used()
    tree = EnvTree(0, root)

    assert tree.get_used_leaves(env) == {
        ("records", 0, "x"),
        ("records", 1, "x"),
    }

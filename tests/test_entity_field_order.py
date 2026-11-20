from copy import deepcopy
from importlib import import_module

import pytest
from kahi.KahiBase import KahiBase


@pytest.mark.parametrize("entity,template,builder", [
    ("works", "empty_work", "_project"),
    ("events", "empty_event", "_event"),
    ("projects", "empty_project", "_project"),
    ("patents", "empty_patent", "_patent"),
])
def test_snapshot_creation_and_merge_follow_base_order(entity, template, builder):
    name = "Kahi_minciencias_opendata_" + entity
    module = import_module("kahi_minciencias_opendata_" + entity + "." + name)
    plugin = getattr(module, name).__new__(getattr(module, name))
    expected = getattr(KahiBase(), template)()
    source = dict(reversed(list(deepcopy(expected).items())))
    source["_id"] = "a" * 64 if entity == "works" else "snapshot-id"
    arguments = [source, {}, {}]
    if entity == "works":
        arguments.extend([{}, {"unresolved_institution_evidence": 0}])
    incoming = getattr(plugin, builder)(*arguments)
    assert [key for key in incoming if key != "_id"] == list(expected)
    existing = dict(reversed(list(incoming.items())))
    existing["extra"] = {"z": 2, "a": 1}
    before = deepcopy(existing)
    merged = plugin._merge(existing, incoming)
    assert list(merged) == ["_id", *expected, "extra"]
    assert merged == before
    assert existing == before

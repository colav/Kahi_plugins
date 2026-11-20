import pytest

from kahi_impactu_utils.Utils import compare_author
from kahi_unicity_person_graph.Kahi_unicity_person_graph import (
    CandidateEdgeBuilder, ComponentResolver,
)
from kahi_unicity_person_graph.graph import (
    CandidateGroup, EvidenceEdge, build_evidence_components,
)


DOI = "https://doi.org/10.1097/01.hs9.0000850748.95462.8b"
ORCID = "https://orcid.org/0000-0003-2895-3084"


def person(pid, name, first_names, last_names, aliases=(), ids=()):
    return {
        "_id": pid, "full_name": name, "first_names": first_names,
        "last_names": last_names, "aliases": list(aliases),
        "external_ids": list(ids), "initials": "R",
        "updated": [{"source": "staff" if pid == "a" else "openalex", "time": 1}],
        "affiliations": [],
    }


def profiles():
    return {
        "a": person("a", "Rigoberto Gomez Gutierrez", ["Rigoberto"],
                    ["Gomez", "Gutierrez"], ["r. gomez"]),
        "b": person("b", "Roald Gómez", ["Roald"], ["Gómez"], ["r. gomez"]),
    }


def test_rigoberto_roald_is_rejected_without_a_third_profile():
    snapshot = profiles()
    assert compare_author(snapshot["a"], snapshot["b"], 15) is True
    result = CandidateEdgeBuilder(compare_author, 20, 20).add_groups(
        [CandidateGroup("doi", DOI, ("a", "b"), 0, 15)], snapshot,
    ).finish()
    assert result.edges == ()
    assert result.rejected_given_name_conflicts == 1


@pytest.mark.parametrize("source,key", [
    ("orcid", ORCID), ("scienti", {"COD_RH": "0001336799"}),
])
def test_shared_identifiers_cannot_override_complete_name_conflict(source, key):
    snapshot = profiles()
    for person in snapshot.values():
        person["external_ids"] = [{"source": source, "id": key}]
    result = CandidateEdgeBuilder(compare_author, 20, 20).add_groups(
        [CandidateGroup(source, key, ("a", "b"), 0)], snapshot,
    ).finish()
    assert result.edges == ()
    assert result.rejected_given_name_conflicts == 1


@pytest.mark.parametrize("aliases", [["r. gomez"], ["Rigoberto Gomez Gutierrez"]])
def test_aliases_do_not_erase_different_explicit_given_names(aliases):
    snapshot = profiles()
    for person in snapshot.values():
        person["aliases"] = aliases
    assert ComponentResolver._has_name_conflict(snapshot["a"], snapshot["b"])


@pytest.mark.parametrize("name,first_names", [
    ("R Gomez", ["R"]), ("R. Gomez", ["R."]),
    ("Rigoberto Gomez", ["Rigoberto"]),
])
def test_initials_and_truncated_compatible_names_remain_possible(name, first_names):
    left = profiles()["a"]
    right = person("b", name, first_names, ["Gomez"], ["r. gomez"])
    assert not ComponentResolver._has_name_conflict(left, right)
    result = CandidateEdgeBuilder(compare_author, 20, 20).add_groups(
        [CandidateGroup("doi", DOI, ("a", "b"), 0, 4)],
        {"a": left, "b": right},
    ).finish()
    assert len(result.edges) == 1


def test_reordered_full_names_with_inconsistent_split_are_not_new_conflicts():
    left = person("a", "John Willmer Escobar", ["John", "Willmer"], ["Escobar"])
    right = person("b", "Escobar John Willmer", ["Escobar"], ["John", "Willmer"],
                   ["John Willmer Escobar"])
    assert not ComponentResolver._has_name_conflict(left, right)


def test_missing_secondary_given_name_is_not_a_contradiction():
    left = person("a", "Juan Carlos Perez", ["Juan", "Carlos"], ["Perez"])
    right = person("b", "Juan Perez", ["Juan"], ["Perez"])
    assert not ComponentResolver._has_name_conflict(left, right)


def test_conflicting_complete_compound_given_names_are_detected():
    left = person("a", "Juan Carlos Perez", ["Juan", "Carlos"], ["Perez"], ["J Perez"])
    right = person("b", "Juan Camilo Perez Gomez", ["Juan", "Camilo"],
                   ["Perez", "Gomez"], ["J Perez"])
    assert ComponentResolver._has_name_conflict(left, right)


def test_stale_given_name_not_present_in_full_name_does_not_supply_evidence():
    left = profiles()["a"]
    left["first_names"] = ["Ricardo"]
    right = person("b", "R Gomez", ["R"], ["Gomez"], ["r. gomez"])
    features = [ComponentResolver._name_features(p) for p in [left, right]]
    assert features[0]["complete_given_names"] == set()
    assert not ComponentResolver._contradictory_complete_given_names(*features)


def test_exact_full_names_keep_priority_over_inconsistent_split_fields():
    left = profiles()["a"]
    right = person("b", left["full_name"], ["Roald"], ["Gomez"])
    assert not ComponentResolver._has_name_conflict(left, right)


def test_cluster_guard_also_rejects_previously_scored_weak_edge():
    snapshot = profiles()
    edge = EvidenceEdge("a", "b", "high", 75,
                        ({"source": "doi", "key": DOI},),
                        ({"source": "doi", "key": DOI},))
    component = build_evidence_components([edge])[0]
    result = ComponentResolver(compare_author, timestamp=1).resolve(component, snapshot)
    assert result.plans == ()
    assert result.rejected_given_name_conflicts == 1
    assert result.rejected_weak_bridges == 1

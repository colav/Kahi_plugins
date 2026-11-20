from copy import deepcopy

from bson import ObjectId
import pytest

from test_person_snapshot_ingestion import Plugin, config, person_snapshot, prepare
from kahi_minciencias_opendata_person.identity import canonical_orcid, compatible_names


ORCID = "0000-0002-1825-0097"


def add_source_orcid(source, identifier=ORCID):
    source.persons_final.update_one({}, {"$push": {"external_ids": {
        "source": "orcid", "provenance": "scienti", "id": identifier,
    }}})


def existing_person(name="Ada Lovelace", identifier=ORCID):
    entry = {"_id": ObjectId(), **Plugin.empty_person(None)}
    entry.update(full_name=name, first_names=[name.split()[0]], last_names=[name.split()[-1]])
    entry["external_ids"] = [{"source": "orcid", "provenance": "staff", "id": identifier}]
    return entry


def test_cod_rh_has_priority_over_a_different_orcid_candidate(monkeypatch):
    target, source = prepare(monkeypatch)
    cod_rh_person = existing_person(identifier="0000-0001-5109-3700")
    cod_rh_person["external_ids"] += person_snapshot()["external_ids"]
    orcid_person = existing_person()
    target.person.insert_many([cod_rh_person, orcid_person])
    add_source_orcid(source)
    summary = Plugin(config()).run()
    assert summary["updated"] == 1 and summary["matched_orcid"] == 0
    assert target.person.find_one({"_id": cod_rh_person["_id"]})["keywords"] == ["Matemáticas"]
    assert target.person.find_one({"_id": orcid_person["_id"]}) == orcid_person


@pytest.mark.parametrize("identifier", [ORCID, "https://orcid.org/" + ORCID,
                                        "http://www.orcid.org/" + ORCID + "/", ORCID.replace("-", "")])
def test_orcid_match_preserves_objectid_and_adds_cod_rh(monkeypatch, identifier):
    target, source = prepare(monkeypatch)
    original = existing_person("Áda Lovelace", identifier)
    target.person.insert_one(original)
    add_source_orcid(source)
    plugin = Plugin(config())
    first = plugin.run()
    assert first["matched_orcid"] == 1 and first["inserted"] == 0
    assert target.person.count_documents({}) == 1
    actual = target.person.find_one({"_id": original["_id"]})
    assert any(item.get("id", {}).get("COD_RH") == "0000000001"
               for item in actual["external_ids"] if isinstance(item.get("id"), dict))
    # Exercise a repeated batch, not only the completed checkpoint shortcut.
    counts = {"inserted": 0, "updated": 0, "removed_legacy_related_works": 0}
    plugin._process_batch(list(source.persons_final.find()), counts)
    assert counts["updated"] == 1 and counts["matched_orcid"] == 0
    assert target.person.find_one({"_id": original["_id"]}) == actual


def test_different_name_with_same_orcid_does_not_overwrite_existing(monkeypatch, caplog):
    target, source = prepare(monkeypatch)
    original = existing_person("Ludovic Montoya Vergara")
    target.person.insert_one(original)
    add_source_orcid(source)
    summary = Plugin(config()).run()
    assert summary["orcid_conflicts"] == 1 and summary["inserted"] == 1
    assert target.person.find_one({"_id": original["_id"]}) == original
    assert target.person.count_documents({}) == 2
    assert "ORCID match rejected" in caplog.text


def test_ambiguous_same_name_orcid_is_not_chosen_arbitrarily(monkeypatch):
    target, source = prepare(monkeypatch)
    originals = [existing_person(), existing_person()]
    target.person.insert_many(originals)
    add_source_orcid(source)
    summary = Plugin(config()).run()
    assert summary["orcid_conflicts"] == 1 and summary["matched_orcid"] == 0
    for person in originals:
        assert target.person.find_one({"_id": person["_id"]}) == person


def test_invalid_checksum_is_not_used_for_matching(monkeypatch):
    target, source = prepare(monkeypatch)
    invalid = ORCID[:-1] + "8"
    target.person.insert_one(existing_person(identifier=invalid))
    add_source_orcid(source, invalid)
    summary = Plugin(config()).run()
    assert canonical_orcid(invalid) is None
    assert summary["inserted"] == 1 and summary["matched_orcid"] == 0


@pytest.mark.parametrize("name", ["", None, "   ", " . "])
def test_nameless_source_is_skipped_and_counted(monkeypatch, caplog, name):
    target, source = prepare(monkeypatch)
    source.persons_final.update_one({}, {"$set": {"full_name": name}})
    summary = Plugin(config()).run()
    assert summary["processed"] == 1 and summary["skipped_empty_name"] == 1
    assert summary["inserted"] == summary["updated"] == 0
    assert target.person.count_documents({}) == 0
    assert "Skipping person without a name" in caplog.text


def test_source_cod_rh_is_not_matched_against_mongodb_id(monkeypatch):
    target, _ = prepare(monkeypatch)
    unrelated = {"_id": "0000000001", "full_name": "Unrelated Person"}
    target.person.insert_one(unrelated)
    summary = Plugin(config()).run()
    assert summary["inserted"] == 1
    assert target.person.find_one({"_id": unrelated["_id"]}) == unrelated
    assert isinstance(target.person.find_one({"external_ids.id.COD_RH": "0000000001"})["_id"], ObjectId)


def test_matched_non_objectid_requires_explicit_migration(monkeypatch):
    target, _ = prepare(monkeypatch)
    target.person.insert_one(person_snapshot())
    with pytest.raises(RuntimeError, match="MongoDB ObjectId"):
        Plugin(config()).run()
    assert target.person.count_documents({}) == 1


def set_group_parents(target, group="group-target", parents=("institution-target",)):
    target.affiliations.update_one({"_id": group}, {"$set": {
        "relations": [{"id": value, "name": "Source label"} for value in parents],
    }})


def test_group_institution_added_without_inheriting_membership_dates(monkeypatch):
    target, source = prepare(monkeypatch)
    source.persons_final.update_one({}, {"$set": {"affiliations": [person_snapshot()["affiliations"][0]]}})
    set_group_parents(target)
    summary = Plugin(config()).run()
    person = target.person.find_one()
    institution = person["affiliations"][1]
    assert institution["id"] == "institution-target"
    assert institution["start_date"] is None and institution["end_date"] == -1
    assert institution["years"] == [] and institution["position"] == ""
    assert person["affiliations"][0]["start_date"] == 1
    assert summary["indirect_institutions_added"] == 1
    assert [key for key in person if key != "_id"] == list(Plugin.empty_person(None))


def test_existing_staff_institution_history_is_not_duplicated_or_changed(monkeypatch):
    target, source = prepare(monkeypatch)
    source.persons_final.update_one({}, {"$set": {"affiliations": [person_snapshot()["affiliations"][0]]}})
    set_group_parents(target)
    original = existing_person()
    original["external_ids"] = person_snapshot()["external_ids"]
    original["affiliations"] = [{"id": "institution-target", "name": "Staff university",
                                 "types": [], "start_date": 42, "end_date": 99, "years": [2004, 2005]}]
    before = deepcopy(original["affiliations"])
    target.person.insert_one(original)
    summary = Plugin(config()).run()
    actual = target.person.find_one()
    assert [item for item in actual["affiliations"] if item["id"] == "institution-target"] == before
    assert summary["indirect_institutions_added"] == 0


def test_two_groups_share_one_matrix_and_missing_or_unit_relations_are_ignored(monkeypatch):
    target, source = prepare(monkeypatch)
    target.affiliations.insert_many([
        {"_id": "group-two", "external_ids": [{"source": "minciencias", "id": "COL0000002"}],
         "names": [{"name": "Second group"}], "types": [{"type": "group"}]},
        {"_id": "faculty", "names": [{"name": "Faculty"}], "types": [{"type": "faculty"}]},
    ])
    set_group_parents(target, parents=("institution-target", "faculty", "missing"))
    set_group_parents(target, "group-two")
    group = deepcopy(person_snapshot()["affiliations"][0])
    second = {**group, "id": "COL0000002"}
    repeated_period = {**group, "start_date": 123}
    source.persons_final.update_one({}, {"$set": {"affiliations": [group, second, repeated_period]}})
    summary = Plugin(config()).run()
    actual = target.person.find_one()["affiliations"]
    assert sum(item["id"] == "institution-target" for item in actual) == 1
    assert sum(item["id"] == "group-target" for item in actual) == 2
    assert not any(item["id"] in {"faculty", "missing"} for item in actual)
    assert summary["indirect_institutions_added"] == 1


@pytest.mark.parametrize("batch_size", [1, 50])
def test_same_orcid_and_name_across_cod_rh_keeps_metadata_and_is_retry_safe(monkeypatch, batch_size):
    target, source = prepare(monkeypatch)
    first = source.persons_final.find_one()
    first["external_ids"].append({"source": "orcid", "provenance": "scienti", "id": ORCID})
    first["keywords"] = ["First keyword"]
    second = deepcopy(first)
    second["_id"] = "0000000002"
    second["external_ids"][0]["id"]["COD_RH"] = second["_id"]
    second["keywords"] = ["Second keyword"]
    second["related_works"][0]["id"] = "https://doi.org/10.1000/second"
    source.persons_final.replace_one({}, first)
    source.persons_final.insert_one(second)
    source.scienti_final_release_audits.update_one({}, {"$set": {"evidence.persons.documents": 2}})
    plugin = Plugin(config(batch_size=batch_size))
    summary = plugin.run()
    assert summary["inserted"] == summary["updated"] == 1
    assert target.person.count_documents({}) == 1
    actual = target.person.find_one()
    assert len(actual["related_works"]) == 2
    assert set(actual["keywords"]) == {"First keyword", "Second keyword"}
    codes = {item["id"]["COD_RH"] for item in actual["external_ids"] if isinstance(item.get("id"), dict)}
    assert codes == {"0000000001", "0000000002"}
    counts = {"inserted": 0, "updated": 0, "removed_legacy_related_works": 0}
    plugin._process_batch([first, second], counts)
    assert target.person.find_one() == actual


@pytest.mark.parametrize("left,right,expected", [
    ("María Antonia Pérez López", "Maria Antonia Perez Lopez", True),
    ("Pérez López María Antonia", "Maria Antonia Perez Lopez", True),
    ("Maria Antonia Perez Lopez", "Maria Antnia Perez Lopez", True),
    ("Ludovic Montoya Vergara", "German Montoya Vergara", False),
    ("Ana", "Anita", False),
    ("", "Ada Lovelace", False),
])
def test_orcid_names_are_compared_conservatively(left, right, expected):
    assert compatible_names({"full_name": left}, {"full_name": right}) is expected

import copy
import importlib

import mongomock
import pytest
from bson import ObjectId


plugin_module = importlib.import_module(
    "kahi_minciencias_opendata_person.Kahi_minciencias_opendata_person"
)
Plugin = plugin_module.Kahi_minciencias_opendata_person
legacy_module = importlib.import_module(
    "kahi_minciencias_opendata_person.legacy"
)
ENTITIES = ("works", "projects", "patents", "events", "persons", "affiliations")


def test_legacy_reuses_recognized_researcher_indexes_with_custom_names():
    collection = mongomock.MongoClient().dam.recognized_researchers
    collection.create_index([("cod_rh", 1)], name="ix_cod_rh")
    collection.create_index(
        [("documento_normalizado", 1)], name="ix_documento_normalizado")

    assert legacy_module.ensure_query_index(collection, [("cod_rh", 1)]) == "ix_cod_rh"
    assert legacy_module.ensure_query_index(
        collection, [("documento_normalizado", 1)]) == "ix_documento_normalizado"
    assert len(list(collection.list_indexes())) == 3


def person_snapshot():
    return {
        "_id": "0000000001",
        "updated": [{"source": "minciencias", "time": 1}],
        "full_name": "Ada Lovelace",
        "first_names": ["Ada"],
        "last_names": ["Lovelace"],
        "initials": "A",
        "aliases": ["Augusta Ada Lovelace"],
        "affiliations": [{
            "id": "COL0000001", "name": "Grupo fuente",
            "types": [{"source": "scienti", "type": "group"}],
            "start_date": 1, "end_date": -1, "position": "Investigadora",
        }, {
            "id": "", "name": "Universidad Exacta", "types": [],
            "start_date": 2, "end_date": 3, "position": "Docente",
        }],
        "keywords": ["Matemáticas"],
        "external_ids": [{
            "provenance": "minciencias", "source": "scienti",
            "id": {"COD_RH": "0000000001"},
        }],
        "sex": "Mujer", "marital_status": None,
        "ranking": [{"source": "minciencias", "rank": "Emérito", "date": 1}],
        "birthplace": {"country": "Colombia"}, "birthdate": -1,
        "degrees": [], "subjects": [], "citations_count": [],
        "products_count": 0,
        "related_works": [{
            "provenance": "minciencias", "source": "doi",
            "id": "https://doi.org/10.1000/example", "author_count": 2,
        }],
    }


def config(**source_changes):
    source = {
        "database_url": "mongodb://source", "database_name": "dam",
        "release_name": "release_six", "batch_size": 50, "verbose": 0,
    }
    source.update(source_changes)
    return {
        "database_url": "mongodb://target", "database_name": "kahi",
        "minciencias_opendata_person": source,
    }


def test_legacy_mode_uses_raw_html_defaults(monkeypatch):
    captured = {}

    class DummyLegacy:
        def __init__(self, value):
            captured.update(value["minciencias_opendata_person"])

        def run(self):
            return "legacy"

    monkeypatch.setattr(
        legacy_module, "Kahi_minciencias_opendata_person", DummyLegacy
    )
    plugin = Plugin(config(source_mode="legacy"))

    assert plugin.run() == "legacy"
    assert captured["researchers"] == "cvlac_data"
    assert captured["groups_production"] == "gruplac_production_data"
    assert captured["recognized_researchers"] == "recognized_researchers"
    assert captured["cvlac_html_profiles"] == (
        "cvlac_stage_raw_full_corrected_20260823"
    )


def test_legacy_raw_html_parser_extracts_identity_without_experience():
    html = """
    <a name="datos_generales"></a><table>
      <tr><td>Nombre</td><td>Ada Lovelace</td></tr>
      <tr><td>Sexo</td><td>Mujer</td></tr>
    </table>
    <a name="red_identificadores"></a><table>
      <tr><td><a href="https://orcid.org/0000-0001">ORCID</a></td></tr>
    </table>
    <a name="experiencia"></a><table><tr><td>Empresa que no debe importarse</td></tr></table>
    """

    profile = legacy_module.cvlac_profile_from_html(
        {"_id": "0000000001", "html": html}
    )

    assert profile["datos_generales"]["Nombre"] == "Ada Lovelace"
    assert profile["red_identificadores"]["ORCID"] == "https://orcid.org/0000-0001"
    assert "experiencia" not in profile


def test_legacy_reads_capitalized_sex_and_enriches_existing_without_html():
    client = mongomock.MongoClient()
    db = client.kahi
    person = person_snapshot()
    person["sex"] = ""
    db.person.insert_one(person)
    author = {"id_persona_pr": "0000000001"}

    legacy_module.process_one(
        [author], db, db.person, person_snapshot(),
        {"id_persona_pr": "0000000001", "datos_generales": {
            "Nombre": "Ada Lovelace", "Sexo": "Mujer"
        }},
        client.dam.production, False, client.dam.html, 0,
    )
    assert db.person.find_one()["sex"]

    db.person.update_one({}, {"$set": {"sex": ""}})
    legacy_module.process_one(
        [author], db, db.person, person_snapshot(), None,
        client.dam.production, False, client.dam.html, 0,
    )
    updated = db.person.find_one()
    assert any(item["source"] == "minciencias" for item in updated["updated"])


def test_legacy_does_not_create_nameless_person_without_html():
    client = mongomock.MongoClient()
    legacy_module.process_one(
        [{"id_persona_pr": "0000000002"}], client.kahi,
        client.kahi.person, person_snapshot(), None,
        client.dam.production, False, client.dam.html, 0,
    )
    assert client.kahi.person.count_documents({}) == 0


def recognized_counters():
    return {
        "processed": 0, "inserted": 0, "updated": 0,
        "cod_rh_added": 0, "typed_documents_added": 0,
        "typed_documents_confirmed": 0, "documents_without_type": 0,
        "identity_conflicts": 0, "ambiguous": 0, "invalid": 0,
    }


def legacy_recognized_plugin(client):
    plugin = legacy_module.Kahi_minciencias_opendata_person.__new__(
        legacy_module.Kahi_minciencias_opendata_person
    )
    plugin.collection = client.kahi.person
    return plugin


def test_legacy_recognized_inserts_typed_document_and_cod_rh_idempotently():
    client = mongomock.MongoClient()
    plugin = legacy_recognized_plugin(client)
    record = {
        "cod_rh": "0000000001", "documento_normalizado": "12345",
        "tipo_consolidado": "Cédula de ciudadanía",
        "nombre_completo": "Ada Lovelace",
    }

    plugin._enrich_recognized_person(record, recognized_counters())
    plugin._enrich_recognized_person(record, recognized_counters())

    assert plugin.collection.count_documents({}) == 1
    person = plugin.collection.find_one()
    assert sum(item["source"] == "scienti"
               for item in person["external_ids"]) == 1
    assert sum(item["source"] == "Cédula de Ciudadanía"
               for item in person["external_ids"]) == 1


def test_legacy_recognized_uses_existing_document_type_to_add_cod_rh():
    client = mongomock.MongoClient()
    person = person_snapshot()
    person["external_ids"] = [{
        "provenance": "staff", "source": "Pasaporte", "id": "AB123"
    }]
    client.kahi.person.insert_one(person)
    plugin = legacy_recognized_plugin(client)

    plugin._enrich_recognized_person({
        "cod_rh": "0000000001", "documento_normalizado": "AB123",
        "tipo_consolidado": None, "nombre_completo": "Ada Lovelace",
    }, recognized_counters())

    updated = plugin.collection.find_one()
    assert any(item.get("id", {}).get("COD_RH") == "0000000001"
               for item in updated["external_ids"]
               if isinstance(item.get("id"), dict))
    assert sum(item["source"] == "Pasaporte"
               for item in updated["external_ids"]) == 1


def test_legacy_recognized_does_not_publish_document_without_type():
    client = mongomock.MongoClient()
    plugin = legacy_recognized_plugin(client)

    plugin._enrich_recognized_person({
        "cod_rh": "0000000001", "documento_normalizado": "12345",
        "tipo_consolidado": None, "nombre_completo": "Ada Lovelace",
    }, recognized_counters())

    person = plugin.collection.find_one()
    assert [item["source"] for item in person["external_ids"]] == ["scienti"]


def test_legacy_recognized_does_not_join_conflicting_document_and_cod_rh():
    client = mongomock.MongoClient()
    first = person_snapshot()
    first["_id"] = "first"
    second = person_snapshot()
    second["_id"] = "second"
    second["external_ids"] = [{
        "source": "Cédula de Ciudadanía", "id": "12345",
    }]
    second["full_name"] = "Otra Persona"
    client.kahi.person.insert_many([first, second])
    plugin = legacy_recognized_plugin(client)
    counters = recognized_counters()

    plugin._enrich_recognized_person({
        "cod_rh": "0000000001", "documento_normalizado": "12345",
        "tipo_consolidado": "Cédula de ciudadanía",
        "nombre_completo": "Ada Lovelace",
    }, counters)

    assert counters["identity_conflicts"] == 1
    assert not plugin._person_has_document(
        plugin.collection.find_one({"_id": "first"}),
        "12345", "Cédula de Ciudadanía",
    )
    assert plugin._person_codes(plugin.collection.find_one({"_id": "second"})) == set()


def test_legacy_recognized_requires_compatible_name_for_document_only_match():
    client = mongomock.MongoClient()
    existing = person_snapshot()
    existing["external_ids"] = [{
        "source": "Cédula de Ciudadanía", "id": "12345",
    }]
    existing["full_name"] = "Otra Persona"
    client.kahi.person.insert_one(existing)
    plugin = legacy_recognized_plugin(client)
    counters = recognized_counters()

    plugin._enrich_recognized_person({
        "cod_rh": "0000000001", "documento_normalizado": "12345",
        "tipo_consolidado": "Cédula de ciudadanía",
        "nombre_completo": "Ada Lovelace",
    }, counters)

    assert plugin.collection.count_documents({}) == 1
    assert counters["identity_conflicts"] == 1
    assert plugin._person_codes(plugin.collection.find_one()) == set()


def prepare(monkeypatch, *, entity_count=6):
    clients = {
        "mongodb://target": mongomock.MongoClient(),
        "mongodb://source": mongomock.MongoClient(),
    }
    monkeypatch.setattr(plugin_module, "MongoClient", lambda uri: clients[uri])
    target = clients["mongodb://target"].kahi
    source = clients["mongodb://source"].dam
    target.affiliations.insert_many([{
        "_id": "group-target", "external_ids": [
            {"source": "minciencias", "id": "COL0000001"}
        ],
        "names": [{"lang": "es", "name": "Grupo canónico"}],
        "types": [{"source": "minciencias", "type": "group"}],
    }, {
        "_id": "institution-target", "external_ids": [],
        "names": [{"lang": "es", "name": "Universidad Exacta"}],
        "types": [{"source": "ror", "type": "education"}],
    }])
    source.persons_final.insert_one(person_snapshot())
    collections = {entity: "final_{}".format(entity) for entity in ENTITIES}
    collections["persons"] = "persons_final"
    runs = {entity: "run_{}".format(entity) for entity in ENTITIES}
    source.scienti_final_release_publications.insert_one({
        "_id": "release_six", "status": "published",
        "entity_count": entity_count, "audit": "release_six_audit",
        "collections": collections, "materialization_runs": runs,
    })
    source.scienti_final_release_audits.insert_one({
        "_id": "release_six_audit", "release_name": "release_six",
        "status": "passed", "critical_anomalies": 0,
        "collections": collections, "materialization_runs": runs,
        "evidence": {"persons": {
            "collection": "persons_final", "documents": 1,
            "materialization_run": "run_persons",
        }},
    })
    return target, source


def test_streams_snapshot_resolves_affiliations_and_is_idempotent(monkeypatch):
    target, source = prepare(monkeypatch)
    source_indexes = source.persons_final.index_information()

    first = Plugin(config()).run()
    repeated = Plugin(config()).run()

    person = target.person.find_one({"external_ids.id.COD_RH": "0000000001"})
    assert isinstance(person["_id"], ObjectId)
    assert first["inserted"] == 1
    assert repeated == first
    assert person["affiliations"][0]["id"] == "group-target"
    assert person["affiliations"][0]["name"] == "Grupo canónico"
    assert person["affiliations"][1]["id"] == "institution-target"
    assert person["related_works"] == person_snapshot()["related_works"]
    assert len(person["affiliations"]) == 2
    assert source.persons_final.index_information() == source_indexes


def test_replaces_legacy_minciencias_works_but_preserves_other_sources(monkeypatch):
    target, _ = prepare(monkeypatch)
    existing = person_snapshot()
    existing["_id"] = ObjectId()
    existing["related_works"] = [{
        "provenance": "minciencias", "source": "scienti",
        "id": {"COD_RH": "0000000001", "COD_PRODUCTO": "5"},
    }, {
        "provenance": "openalex", "source": "openalex", "id": "W1",
    }]
    existing["citations_count"] = [{"source": "openalex", "count": 7}]
    existing["products_count"] = 8
    target.person.insert_one(existing)

    summary = Plugin(config()).run()
    person = target.person.find_one({"_id": existing["_id"]})

    assert summary["removed_legacy_related_works"] == 1
    assert {item["source"] for item in person["related_works"]} == {"doi", "openalex"}
    assert person["citations_count"] == [{"source": "openalex", "count": 7}]
    assert person["products_count"] == 8


def test_rejects_legacy_options_and_non_explicit_release(monkeypatch):
    prepare(monkeypatch)
    with pytest.raises(ValueError, match="legacy person source options"):
        Plugin(config(groups_production="gruplac_production_data"))
    with pytest.raises(ValueError, match="explicit immutable release_name"):
        Plugin(config(release_name="current"))


def test_rejects_four_entity_release_and_missing_target_group(monkeypatch):
    prepare(monkeypatch, entity_count=4)
    with pytest.raises(RuntimeError, match="six-entity person snapshot"):
        Plugin(config())

    target, source = prepare(monkeypatch)
    target.affiliations.delete_one({"_id": "group-target"})
    with pytest.raises(RuntimeError, match="groups must be imported"):
        Plugin(config()).run()
    assert target.person.count_documents({}) == 0


def test_checkpoint_resumes_and_records_source_identity(monkeypatch):
    target, source = prepare(monkeypatch)
    invalid = copy.deepcopy(person_snapshot())
    invalid["_id"] = "0000000002"
    invalid["external_ids"][0]["id"]["COD_RH"] = "0000000002"
    invalid.pop("birthdate")
    source.persons_final.insert_one(invalid)
    source.scienti_final_release_audits.update_one(
        {"_id": "release_six_audit"},
        {"$set": {"evidence.persons.documents": 2}},
    )

    with pytest.raises(RuntimeError, match="does not follow the Kahi schema"):
        Plugin(config(batch_size=1)).run()
    run = target.minciencias_opendata_person_runs.find_one()
    assert run["last_id"] == "0000000001"
    assert run["contract_version"] == plugin_module.CONTRACT_VERSION

    invalid["birthdate"] = -1
    source.persons_final.replace_one({"_id": "0000000002"}, invalid)
    summary = Plugin(config(batch_size=1)).run()
    assert summary["processed"] == 2
    assert summary["source_collection"] == "persons_final"


def test_rejects_ambiguous_target_cod_rh(monkeypatch):
    target, _ = prepare(monkeypatch)
    external_ids = person_snapshot()["external_ids"]
    target.person.insert_many([
        {"_id": "duplicate-1", "external_ids": copy.deepcopy(external_ids)},
        {"_id": "duplicate-2", "external_ids": copy.deepcopy(external_ids)},
    ])

    with pytest.raises(RuntimeError, match="resolves to multiple Kahi people"):
        Plugin(config()).run()


def test_resolves_duplicate_cod_rh_by_snapshot_name(monkeypatch):
    target, _ = prepare(monkeypatch)
    external_ids = person_snapshot()["external_ids"]
    matching_id = ObjectId()
    target.person.insert_many([{
        "_id": ObjectId(), "full_name": "Different Person",
        "external_ids": copy.deepcopy(external_ids),
    }, {
        "_id": matching_id, "full_name": "Áda Lovelace",
        "external_ids": copy.deepcopy(external_ids),
    }])

    summary = Plugin(config()).run()

    assert summary["resolved_duplicate_cod_rh"] == 1
    assert target.person.find_one({"_id": matching_id})["keywords"] == [
        "Matemáticas"
    ]


def test_resolves_same_name_duplicate_by_stronger_evidence(monkeypatch):
    target, _ = prepare(monkeypatch)
    external_ids = person_snapshot()["external_ids"]
    enriched_id = ObjectId()
    target.person.insert_many([{
        "_id": ObjectId(), "full_name": "Ada Lovelace",
        "updated": [{"source": "staff"}],
        "external_ids": copy.deepcopy(external_ids), "affiliations": [],
    }, {
        "_id": enriched_id, "full_name": "Ada Lovelace",
        "updated": [{"source": "staff"}, {"source": "scienti"}],
        "external_ids": copy.deepcopy(external_ids) + [{
            "source": "Cédula de Ciudadanía", "id": "123"
        }],
        "affiliations": [{"id": "institution-target"}],
    }])

    summary = Plugin(config()).run()

    assert summary["resolved_duplicate_cod_rh"] == 1
    assert target.person.find_one({"_id": enriched_id})["keywords"] == [
        "Matemáticas"
    ]


def test_resolves_same_person_when_snapshot_omits_middle_name(monkeypatch):
    target, source = prepare(monkeypatch)
    source.persons_final.update_one(
        {"_id": "0000000001"}, {"$set": {"full_name": "Ada Lovelace"}}
    )
    external_ids = person_snapshot()["external_ids"]
    enriched_id = ObjectId()
    target.person.insert_many([{
        "_id": ObjectId(), "full_name": "Ada Augusta Lovelace",
        "updated": [{"source": "staff"}],
        "external_ids": copy.deepcopy(external_ids), "affiliations": [],
    }, {
        "_id": enriched_id, "full_name": "Ada Augusta Lovelace",
        "updated": [{"source": "staff"}, {"source": "scienti"}],
        "external_ids": copy.deepcopy(external_ids) + [{
            "source": "Cédula de Ciudadanía", "id": "123"
        }],
        "affiliations": [],
    }])

    summary = Plugin(config()).run()

    assert summary["resolved_duplicate_cod_rh"] == 1
    assert target.person.find_one({"_id": enriched_id})["keywords"] == [
        "Matemáticas"
    ]

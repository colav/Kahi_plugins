"""ScienTI's first group institution belongs to the source author in the work."""
from copy import deepcopy

from bson import ObjectId
from kahi.KahiBase import KahiBase
import mongomock
import pytest

import kahi_scienti_works.parser as parser_module
import kahi_scienti_works.process_one as process_module


@pytest.fixture
def case(monkeypatch):
    monkeypatch.setattr(parser_module, "lang_poll", lambda *_args, **_kwargs: "es")
    monkeypatch.setattr(process_module, "lang_poll", lambda *_args, **_kwargs: "es")
    db = mongomock.MongoClient().scienti_test
    owner_id, coauthor_id = ObjectId(), ObjectId()
    raw = {
        "COD_RH": "RH1", "COD_PRODUCTO": "P1",
        "TXT_NME_PROD": "Publicación de prueba", "TXT_RESUMEN_PROD": "",
        "NRO_ANO_PRESENTA": 2000,
        "product_type": [{"TXT_NME_TIPO_PRODUCTO": "Artículo", "NRO_NIVEL": 1,
                          "TPO_CLASE": "A", "COD_TIPO_PRODUCTO": "111"}],
        "author": [{"TXT_TOTAL_NAMES": "Ana Pérez", "COD_RH": "RH1",
                    "TPO_DOCUMENTO_IDENT": ""}],
        "group": [{"COD_ID_GRUPO": "GROUP1", "NRO_ID_GRUPO": "GROUP1",
                   "NME_GRUPO": "Grupo fuente",
                   "institution": [{"COD_INST": "INST1", "NME_INST": "Universidad fuente"},
                                   {"COD_INST": "INST2", "NME_INST": "Otra institución"}]}],
    }
    institution = {"id": "source-university", "name": "Universidad fuente",
                   "types": [{"source": "ror", "type": "education"}]}
    other_aff = {"id": "other-university", "name": "Universidad previa", "types": []}
    owner = {"_id": owner_id, "full_name": "Ana Pérez", "first_names": ["Ana"],
             "last_names": ["Pérez"], "initials": "A",
             "external_ids": [{"provenance": "scienti", "source": "scienti",
                               "id": {"COD_RH": "RH1"}}],
             "affiliations": [deepcopy(other_aff), dict(institution, years=[2050]),
                              {"id": "source-faculty", "name": "Facultad", "types": [
                                  {"type": "faculty"}]}]}
    coauthor = {"_id": coauthor_id, "full_name": "Bruno Gómez", "first_names": ["Bruno"],
                "last_names": ["Gómez"], "initials": "B", "external_ids": [], "affiliations": []}
    db.person.insert_many([deepcopy(owner), coauthor])
    db.affiliations.insert_many([
        {"_id": institution["id"], "names": [{"name": institution["name"], "lang": "es", "source": "ror"}],
         "types": institution["types"], "external_ids": [{"source": "scienti", "id": "INST1"}]},
        {"_id": "source-group", "names": [{"name": "Grupo fuente", "lang": "es", "source": "scienti"}],
         "types": [{"type": "group", "source": "scienti"}],
         "external_ids": [{"source": "scienti", "id": "GROUP1"}]},
        {"_id": "source-faculty", "names": [{"name": "Facultad", "lang": "es", "source": "staff"}],
         "types": [{"type": "faculty"}], "relations": [{"id": "source-university"}]},
    ])
    work = KahiBase.empty_work(None)
    work.update({"_id": ObjectId(), "year_published": 2000,
                 "updated": [{"source": "openalex", "time": 1}],
                 "titles": [{"title": "Publicación de prueba", "lang": "es", "source": "openalex"},
                            {"title": "Publicación de prueba", "lang": "es", "source": "scienti"}],
                 "external_ids": [{"source": "scienti", "provenance": "scienti",
                                   "id": {"COD_RH": "RH1", "COD_PRODUCTO": "P1"}}],
                 "authors": [{"id": owner_id, "full_name": owner["full_name"],
                              "affiliations": [deepcopy(other_aff)]},
                             {"id": coauthor_id, "full_name": coauthor["full_name"],
                              "affiliations": [deepcopy(other_aff)], "type": "coauthor"}],
                 "author_count": 2})
    return db, raw, work, institution, owner


def update(db, raw, work):
    db.works.replace_one({"_id": work["_id"]}, deepcopy(work), upsert=True)
    process_module.process_one_update(deepcopy(raw), deepcopy(work), db, db.works, KahiBase.empty_work(None))
    return db.works.find_one({"_id": work["_id"]})


@pytest.mark.parametrize("author_match", ["id", "name"])
def test_first_update_keeps_source_institution_only_for_source_author(case, monkeypatch, author_match):
    db, raw, work, institution, owner = case
    if author_match == "name":
        alias = deepcopy(owner)
        alias.update({"_id": ObjectId(), "external_ids": []})
        db.person.insert_one(alias)
        work["authors"][0]["id"] = alias["_id"]
        monkeypatch.setattr(process_module, "split_names_fix", lambda *_args: None)
    result = update(db, raw, work)
    assert result["authors"][0]["id"] == owner["_id"]
    assert result["authors"][0]["affiliations"] == [
        work["authors"][0]["affiliations"][0],
        {"id": "source-group", "name": "Grupo fuente", "types": [{"type": "group", "source": "scienti"}]},
        institution,
    ]
    assert result["authors"][1] == work["authors"][1]
    assert db.person.find_one({"_id": owner["_id"]}) == owner
    assert result["year_published"] == 2000  # No new staff/year filtering.


def test_retry_repairs_only_source_author_institution_and_is_idempotent(case):
    db, raw, work, institution, _owner = case
    work["updated"].append({"source": "scienti", "time": 2})
    work["authors"][0]["type"] = "author"
    expected = deepcopy(work)
    expected["authors"][0]["affiliations"].append(institution)
    result = update(db, raw, work)
    assert result == expected
    assert update(db, raw, result) == expected


def test_retry_does_not_replace_existing_institution_metadata(case):
    db, raw, work, institution, _owner = case
    work["updated"].append({"source": "scienti", "time": 2})
    work["authors"][0]["affiliations"].append(dict(institution, years=[2000], name="Nombre previo"))
    assert update(db, raw, work) == work


@pytest.mark.parametrize("mode", ["unknown", "by_name", "company"])
def test_source_institution_is_not_lost_due_to_resolution_or_type(case, mode):
    db, raw, work, institution, _owner = case
    if mode == "unknown":
        db.affiliations.delete_one({"_id": institution["id"]})
        institution = {"id": "", "name": raw["group"][0]["institution"][0]["NME_INST"], "types": []}
    elif mode == "by_name":
        db.affiliations.update_one({"_id": institution["id"]}, {"$set": {"external_ids": []}})
    else:
        institution["types"] = [{"type": "company", "source": "ror"}]
        db.affiliations.update_one({"_id": institution["id"]}, {"$set": {"types": institution["types"]}})
    # An unrelated unresolved affiliation must not suppress the institution.
    work["authors"][0]["affiliations"].append({"id": "", "name": "Afiliación previa desconocida", "types": []})
    work["updated"].append({"source": "scienti", "time": 2})
    expected = deepcopy(work)
    expected["authors"][0]["affiliations"].append(institution)
    result = update(db, raw, work)
    assert result == expected
    assert update(db, raw, result) == expected


def test_no_institution_does_not_import_profile_affiliations(case):
    db, raw, work, _institution, _owner = case
    del raw["group"][0]["institution"]
    work["updated"].append({"source": "scienti", "time": 2})
    assert update(db, raw, work) == work


def test_retry_does_not_assign_source_institution_to_another_author(case):
    db, raw, work, _institution, _owner = case
    work["authors"] = work["authors"][1:]
    work["author_count"] = 1
    work["updated"].append({"source": "scienti", "time": 2})
    assert update(db, raw, work) == work


def test_new_work_already_keeps_the_parsed_source_institution(case, monkeypatch):
    db, raw, _work, institution, owner = case
    # mongomock does not implement $type in the insert-only update pipeline.
    # Keep the complete parsing/linking path, substituting only the memory write.
    monkeypatch.setattr(process_module, "insert_only_update", lambda entry: {"$setOnInsert": entry})
    process_module.process_one_insert(deepcopy(raw), db, db.works, KahiBase.empty_work(None), None)
    result = db.works.find_one({})
    assert result["authors"][0]["id"] == owner["_id"]
    assert institution in result["authors"][0]["affiliations"]

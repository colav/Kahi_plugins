"""The advisors of a directed work stay as authors of type ``advisor``."""
from copy import deepcopy

from bson import ObjectId
from kahi.KahiBase import KahiBase
import mongomock
import pytest

import kahi_scienti_works.parser as parser_module
import kahi_scienti_works.process_one as process_module
from kahi_scienti_works.process_one import apply_thesis_roles, thesis_roles


def person(cod_rh, full_name):
    first, *last = full_name.split()
    return {"_id": ObjectId(), "full_name": full_name, "first_names": [first],
            "last_names": last, "initials": first[0], "affiliations": [],
            "external_ids": [{"provenance": "scienti", "source": "scienti",
                              "id": {"COD_RH": cod_rh}}]}


@pytest.fixture
def thesis(monkeypatch):
    monkeypatch.setattr(parser_module, "lang_poll", lambda *_args, **_kwargs: "es")
    monkeypatch.setattr(process_module, "lang_poll", lambda *_args, **_kwargs: "es")
    db = mongomock.MongoClient().scienti_test
    tutor, cotutor, student = (person("RH1", "Juan Bedoya Molina"),
                               person("RH2", "Jenny Aristizabal Nieto"),
                               person("RH3", "Yenifer Giraldo Pérez"))
    db.person.insert_many([deepcopy(tutor), deepcopy(cotutor), deepcopy(student)])
    raw = {
        "COD_RH": "RH1", "COD_PRODUCTO": "68",
        "TXT_NME_PROD": "Radiografía amorosa del conflicto armado", "TXT_RESUMEN_PROD": "",
        "NRO_ANO_PRESENTA": 2022,
        "product_type": [{"TXT_NME_TIPO_PRODUCTO": "Trabajos dirigidos/Tutorías", "NRO_NIVEL": 0,
                          "TPO_CLASE": None, "COD_TIPO_PRODUCTO": "6"}],
        "author": [{"TXT_TOTAL_NAMES": "Juan Bedoya Molina", "COD_RH": "RH1",
                    "TPO_DOCUMENTO_IDENT": ""}],
        "re_author_others": [
            {"TPO_PARTICIPACION": "TUT", "author": [{"COD_RH": "RH1"}]},
            {"TPO_PARTICIPACION": "COT", "author": [{"COD_RH": "RH2"}]},
            {"TPO_PARTICIPACION": "ORI", "author": [{"COD_RH": "RH3"}]},
            {"TPO_PARTICIPACION": "ORI", "author": [{"COD_RH": None}]},
        ],
    }
    work = KahiBase.empty_work(None)
    work.update({"_id": ObjectId(), "year_published": 2022,
                 "updated": [{"source": "openalex", "time": 1}],
                 "titles": [{"title": raw["TXT_NME_PROD"], "lang": "es", "source": "openalex"}],
                 "types": [{"provenance": "openalex", "source": "openalex",
                            "type": "dissertation", "level": None}],
                 "authors": [{"id": student["_id"], "full_name": student["full_name"],
                              "affiliations": []}],
                 "author_count": 1})
    return db, raw, work, tutor, cotutor, student


def update(db, raw, work):
    db.works.replace_one({"_id": work["_id"]}, deepcopy(work), upsert=True)
    process_module.process_one_update(
        deepcopy(raw), deepcopy(work), db, db.works, KahiBase.empty_work(None))
    return db.works.find_one({"_id": work["_id"]})


def roles(work):
    return [(author["full_name"], author.get("type")) for author in work["authors"]]


def test_roles_come_from_the_participation_type(thesis):
    _db, raw, *_ = thesis
    assert thesis_roles(raw) == {"RH1": "advisor", "RH2": "advisor", "RH3": "author"}
    assert thesis_roles({"re_author_others": [{"author_others": [{"COD_RH_REF": "RH9"}]}]}) == {}


def test_thesis_inserted_by_another_source_gains_its_advisors(thesis):
    db, raw, work, tutor, cotutor, student = thesis

    result = update(db, raw, work)

    assert roles(result) == [("Yenifer Giraldo Pérez", "author"),
                             ("Juan Bedoya Molina", "advisor"),
                             ("Jenny Aristizabal Nieto", "advisor")]
    assert [author["id"] for author in result["authors"]] == [
        student["_id"], tutor["_id"], cotutor["_id"]]
    assert result["author_count"] == 3


def test_second_cvlac_of_the_same_thesis_adds_its_owner(thesis):
    db, raw, work, tutor, cotutor, _student = thesis
    first = update(db, raw, work)
    first["authors"] = [author for author in first["authors"] if author["id"] != cotutor["_id"]]
    second = deepcopy(raw)
    second.update({"COD_RH": "RH2", "COD_PRODUCTO": "9",
                   "author": [{"TXT_TOTAL_NAMES": "Jenny Aristizabal Nieto", "COD_RH": "RH2",
                               "TPO_DOCUMENTO_IDENT": ""}]})

    result = update(db, second, first)

    assert ("Jenny Aristizabal Nieto", "advisor") in roles(result)
    assert result["author_count"] == 3
    assert update(db, second, result) == result


def test_author_type_survives_the_author_merge(thesis):
    db, raw, work, tutor, _cotutor, student = thesis
    work["authors"] = [
        {"id": tutor["_id"], "full_name": "Juan Bedoya", "affiliations": [], "type": "advisor"},
        {"id": student["_id"], "full_name": student["full_name"], "affiliations": [],
         "type": "author"},
    ]
    article = deepcopy(raw)
    article.pop("re_author_others")

    result = update(db, article, work)

    assert roles(result) == [("Juan Bedoya Molina", "advisor"),
                             ("Yenifer Giraldo Pérez", "author")]


def test_type_set_by_another_source_is_not_overridden(thesis):
    db, raw, _work, tutor, _cotutor, _student = thesis
    authors = [{"id": tutor["_id"], "full_name": tutor["full_name"], "affiliations": [],
                "type": "author"}]
    apply_thesis_roles(raw, authors, db)
    assert authors[0]["type"] == "author"


def test_works_without_advisor_roles_are_left_alone(thesis):
    db, raw, _work, *_ = thesis
    raw["re_author_others"] = [{"author_others": [{"COD_RH_REF": "RH2"}]}]
    authors = []
    assert apply_thesis_roles(raw, authors, db) is False and authors == []

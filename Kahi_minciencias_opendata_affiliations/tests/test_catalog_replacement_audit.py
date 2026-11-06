import copy

import mongomock
import pytest

from test_snapshot_ingestion import Plugin
from test_snapshot_migration_activation import audit


def fixture():
    client = mongomock.MongoClient()
    db = client.audit
    provisional_name = "INTERTELCO S.A.S"
    helper = Plugin.__new__(Plugin)
    provisional = {"_id": helper.aval_institution_id(provisional_name),
                   "names": [{"name": provisional_name}], "external_ids": []}
    db.legacy.insert_one(provisional)
    canonical = {"_id": "000000000241", "names": [{"name": "INTER-TELCO S.A.S"}],
                 "external_ids": [{"source": "COD_INST", "id": "000000000241"},
                                  {"source": "ID_INSTITUCION", "id": "1182"},
                                  {"source": "nit", "id": "900382516"}]}
    db.affiliations.insert_one(canonical)
    catalog = {"000000000241": {"NME_INST": "INTER-TELCO S.A.S", "ID_INSTITUCION": 1182,
                                 "TXT_NIT": "900382516"}}
    reader = audit.verification_reader(db, "run", "resolutions", catalog)
    group = {"_id": "COL0159366", "relations": [{"id": canonical["_id"]}]}
    source = {"relations": [{"name": "FUERZA AÉREA COLOMBIANA; INTERTELCO S.A.S"}]}
    return db, reader, provisional, canonical, group, source


def test_intertelco_fallback_is_replaced_by_unique_catalog_identity():
    db, reader, provisional, canonical, group, source = fixture()
    proof = audit.explain_removed_relation(reader, group, source, provisional["_id"], db.legacy)
    assert proof == {"reason": "catalog_verified_punctuation_replacement", "replacement_id": canonical["_id"],
                     "cod_inst": "000000000241", "catalog_name": "INTER-TELCO S.A.S"}


@pytest.mark.parametrize("conflict", ["catalog_name", "duplicate_catalog_name", "duplicate_target_code",
                                      "nit", "internal_id", "uncorroborated", "registered_provisional", "source"])
def test_equivalence_requires_more_than_text_similarity(conflict):
    db, reader, provisional, canonical, group, source = fixture()
    if conflict == "catalog_name":
        reader.scienti_catalog["000000000241"]["NME_INST"] = "Other company"
    elif conflict == "duplicate_catalog_name":
        reader.scienti_catalog["other"] = copy.deepcopy(reader.scienti_catalog["000000000241"])
    elif conflict == "duplicate_target_code":
        duplicate = copy.deepcopy(canonical)
        duplicate["_id"] = "other"
        db.affiliations.insert_one(duplicate)
    elif conflict in {"nit", "internal_id"}:
        field = "nit" if conflict == "nit" else "ID_INSTITUCION"
        for ext in canonical["external_ids"]:
            if ext["source"] == field:
                ext["id"] = "123456789"
        db.affiliations.replace_one({"_id": canonical["_id"]}, canonical)
    elif conflict == "uncorroborated":
        canonical["external_ids"] = canonical["external_ids"][:1]
        db.affiliations.replace_one({"_id": canonical["_id"]}, canonical)
    elif conflict == "registered_provisional":
        db.legacy.update_one({"_id": provisional["_id"]}, {"$set": {
            "external_ids": [{"source": "COD_INST", "id": "another-code"}],
        }})
    else:
        source["relations"] = [{"name": "Another declared company"}]
    assert audit.explain_removed_relation(reader, group, source, provisional["_id"], db.legacy) is None

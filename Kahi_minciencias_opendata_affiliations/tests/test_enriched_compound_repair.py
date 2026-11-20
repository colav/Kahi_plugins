"""Regression cases: shared aliases must not distribute identities across groups."""
import copy

import pytest

from test_snapshot_ingestion import Plugin, config, group_snapshot, prepare_databases


OWNERS = [
    ("IUA_c197caa76770", "000000000243", "HOSPITAL UNIVERSITARIO CLINICA SAN RAFAEL",
     "HOSPITAL UNIVERSITARIO CLÍNICA SAN RAFAEL; UNIVERSIDAD MILITAR NUEVA GRANADA - UNIMILITAR", "860015888", 691),
    ("IUA_b9c9abc19bbb", "000000010865", "Fundación ALMA",
     "UNIVERSIDAD DE ANTIOQUIA; FUNDACIÓN HOSPITALARIA SAN VICENTE DE PAÚL; FUNDACION HOSPITAL SAN VICENTE DE PAUL - RIONEGRO; HOSPITAL ALMA MÁTER DE ANTIOQUIA", "860531851", 5143),
    ("IUA_bdd16810923b", "000000013620", "microscopIA SAS",
     "ASOCIACIÓN COLOMBIANA DE MICROSCOPÍA; CENTRO DE MICROSCOPÍA Y MICROANÁLISIS; CENTRO INTERNACIONAL DE FISICA - CIF; EDITIC SAS; FUNDACION TERRAZUL CENTRO DE DESARROLLO TECNOLOGICO", "", 490703),
]
CASES = [
    ("COL0075749", 0, True), ("COL0094314", 1, False),
    ("COL0097085", 0, True), ("COL0107771", 0, True),
    ("COL0160044", 2, False), ("COL0171039", 2, False),
    ("COL0200319", 1, True), ("COL0202331", 0, True),
    ("COL0221032", 2, True), ("COL0222281", 0, True),
]
OTHERS = [
    ("hospital-alma-mater", "000000006328", "Hospital Alma Máter de Antioquia"),
    ("microscopy-center", "000000010194", "Centro de Microscopía y Microanálisis"),
    ("military-university", "000000000801", "UNIVERSIDAD MILITAR NUEVA GRANADA"),
    ("corpas", "000000000802", "FUNDACIÓN UNIVERSITARIA JUAN N. CORPAS"),
]


def fixture(monkeypatch, *, compound_source=False):
    target, source = prepare_databases(monkeypatch)
    cfg = config()
    cfg["minciencias_opendata_affiliations"].update(batch_size=1, scienti_databases=[{
        "database_url": "mongodb://source", "database_name": "dam",
        "institution_collection_name": "institution",
    }])
    for identifier, code, name, compound, nit, internal in OWNERS:
        entry = {"_id": identifier, **Plugin.empty_affiliation(None)}
        entry["names"] = [{"source": "minciencias", "name": compound}, {"source": "scienti", "name": name}]
        entry["aliases"] = [compound]
        entry["external_ids"] = [{"source": "minciencias", "id": identifier},
                                 {"source": "COD_INST", "id": code},
                                 {"source": "ID_INSTITUCION", "id": str(internal)}]
        if nit:
            entry["external_ids"].append({"source": "nit", "id": nit})
        entry["types"] = [{"source": "minciencias", "type": "other"}]
        entry["campuses"] = [{"name": "Preserved existing SNIES data"}]
        target.affiliations.insert_one(entry)
        source.institution.insert_one({"COD_INST": code, "COD_INST_MACRO": code,
                                       "NME_INST": name, "TXT_NIT": nit,
                                       "ID_INSTITUCION": internal})
    for identifier, code, name in OTHERS:
        entry = {"_id": identifier, **Plugin.empty_affiliation(None)}
        entry["names"] = [{"source": "scienti", "name": name}]
        entry["external_ids"] = [{"source": "COD_INST", "id": code}]
        target.affiliations.insert_one(entry)
        source.institution.insert_one({"COD_INST": code, "COD_INST_MACRO": code, "NME_INST": name})
    source.affiliations_final.delete_many({})
    for group_id, owner_index, declared in CASES:
        identifier, _, name, compound, *_ = OWNERS[owner_index]
        snapshot = group_snapshot()
        snapshot["_id"] = group_id
        snapshot["external_ids"][0]["id"] = group_id
        labels = [name] if declared else [OTHERS[0 if owner_index == 1 else 1][2]]
        if group_id == "COL0075749":
            labels.append(OTHERS[2][2])
        if group_id == "COL0202331":
            labels.append(OTHERS[3][2])
        if compound_source and len(labels) > 1:
            labels = ["; ".join(labels)]
        snapshot["relations"] = [{"id": "", "name": value, "types": []} for value in labels]
        source.affiliations_final.insert_one(snapshot)
        entry = {"_id": group_id, **Plugin.empty_affiliation(None)}
        entry["names"] = copy.deepcopy(snapshot["names"])
        entry["external_ids"] = copy.deepcopy(snapshot["external_ids"])
        entry["relations"] = [{"id": identifier, "name": compound},
                              {"id": "canonical-argos", "name": "CEMENTOS ARGOS SA"}]
        target.affiliations.insert_one(entry)
    source.scienti_final_release_audits.update_one({}, {"$set": {"evidence.affiliations.documents": 10}})
    return target, source, cfg


@pytest.mark.parametrize("compound_source", [False, True])
def test_preserves_seven_correct_links_and_removes_three_unrelated_links(monkeypatch, compound_source):
    target, _, cfg = fixture(monkeypatch, compound_source=compound_source)
    originals = {row[0]: target.affiliations.find_one({"_id": row[0]}) for row in OWNERS}
    result = Plugin(cfg).run()
    for group_id, owner, declared in CASES:
        group = target.affiliations.find_one({"_id": group_id})
        ids = {row["id"] for row in group["relations"]}
        assert (OWNERS[owner][0] in ids) == declared
        assert "canonical-argos" in ids
        assert list(group) == ["_id", *Plugin.empty_affiliation(None)]
        assert all(";" not in row["name"] for row in group["relations"])
    assert "military-university" in {r["id"] for r in target.affiliations.find_one({"_id": "COL0075749"})["relations"]}
    corpas = {r["id"] for r in target.affiliations.find_one({"_id": "COL0202331"})["relations"]}
    assert "corpas" in corpas and "military-university" not in corpas
    for identifier, before in originals.items():
        after = target.affiliations.find_one({"_id": identifier})
        assert list(after) == list(before)
        assert after["external_ids"] == before["external_ids"]
        assert after["campuses"] == before["campuses"]
        assert all(";" not in row["name"] for row in after["names"])
        assert after["aliases"] == []
    audit = result["institution_resolution"]
    assert audit["enriched_compound_institutions_repaired"] == 3
    assert audit["enriched_compound_links_kept"] == 7
    assert audit["enriched_compound_links_removed"] == 3
    before = list(target.affiliations.find())
    assert Plugin(cfg).run() == result
    monkeypatch.setattr(Plugin, "implementation_fingerprint", staticmethod(lambda: "upgrade"))
    again = Plugin(cfg).run()
    assert again["institution_resolution"]["enriched_compound_links_removed"] == 3
    assert list(target.affiliations.find()) == before


def test_resume_recovers_evidence_if_interrupted_after_cleaning_labels(monkeypatch):
    target, _, cfg = fixture(monkeypatch)
    update = target.affiliations.update_one
    crashed = False

    def interrupt(query, change, *args, **kwargs):
        nonlocal crashed
        result = update(query, change, *args, **kwargs)
        if "names" in change.get("$set", {}) and not crashed:
            crashed = True
            raise RuntimeError("interrupted after label update")
        return result

    with monkeypatch.context() as scoped:
        scoped.setattr(target.affiliations, "update_one", interrupt)
        with pytest.raises(RuntimeError, match="after label update"):
            Plugin(cfg).run()
    summary = Plugin(cfg).run()["institution_resolution"]
    assert summary["enriched_compound_links_kept"] == 7
    assert summary["enriched_compound_links_removed"] == 3
    assert target.minciencias_opendata_affiliations_institution_resolutions.count_documents({
        "kind": "enriched_compound_repair", "committed": True,
    }) == 3


def test_partial_group_checkpoint_retains_removed_link_evidence(monkeypatch):
    target, _, cfg = fixture(monkeypatch)
    first = Plugin(cfg)
    process = first.process_snapshot_one

    def fail(reg, *args):
        if reg["_id"] == "COL0097085":
            raise RuntimeError("interrupted after GRIMPA")
        return process(reg, *args)

    first.process_snapshot_one = fail
    with pytest.raises(RuntimeError, match="after GRIMPA"):
        first.run()
    assert OWNERS[1][0] not in {r["id"] for r in target.affiliations.find_one({"_id": "COL0094314"})["relations"]}
    assert Plugin(cfg).run()["institution_resolution"]["enriched_compound_links_removed"] == 3


@pytest.mark.parametrize("conflict", ["nit", "duplicate_code", "authority", "unknown_catalog", "mixed_atomic_names"])
def test_conflicting_registry_evidence_stays_protected_and_blocks_completion(monkeypatch, conflict):
    target, source, cfg = fixture(monkeypatch)
    identifier, code, *_ = OWNERS[0]
    if conflict == "nit":
        target.affiliations.update_one({"_id": identifier}, {"$push": {"external_ids": {"source": "nit", "id": "123456789"}}})
    elif conflict == "duplicate_code":
        target.affiliations.update_one({"_id": "canonical-argos"}, {"$push": {"external_ids": {"source": "COD_INST", "id": code}}})
    elif conflict == "authority":
        target.affiliations.update_one({"_id": identifier}, {"$push": {"external_ids": {"source": "ror", "id": "other"}}})
    elif conflict == "unknown_catalog":
        source.institution.delete_one({"COD_INST": code})
    else:
        target.affiliations.update_one({"_id": identifier}, {"$push": {"names": {"name": "Otra institución"}}})
    before = target.affiliations.find_one({"_id": identifier})
    with pytest.raises(RuntimeError):
        Plugin(cfg).run()
    after = target.affiliations.find_one({"_id": identifier})
    assert after["names"] == before["names"]
    assert after["external_ids"] == before["external_ids"]

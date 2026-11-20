import importlib

import mongomock
import pytest

from kahi_lens_patents.merging import (
    choose_survivor,
    clusters,
    inventor_tokens,
    is_lens_inventor,
    merge_documents,
    same_inventor,
)
from kahi_lens_patents.matching import (
    LensIndex,
    content_overlap,
    match_patent,
    normalize_text,
    number_keys,
)

plugin_module = importlib.import_module("kahi_lens_patents.Kahi_lens_patents")
Plugin = plugin_module.Kahi_lens_patents
PATENT_TYPE = {"provenance": "scienti", "source": "impactu", "type": "Patente"}


@pytest.mark.parametrize("raw, country, expected", [
    ("NC2023/0017292", None, "CO2023017292"),
    ("NC2018/0001325", "CO", "CO2018001325"),
    ("PCT/IB2019/053356", None, "IB2019053356"),
    ("US 7,041,647", None, "US7041647"),
    ("US20250127805A1", None, "US20250127805"),
    ("16146449", "US", "US16146449"),
    ("12-163128", "CO", "CO12163128"),
    ("WO2023/275652", None, "WO2023275652"),
    ("MX/a/2024/002278", None, "MX2024002278"),
    ("13-192338-00000-0000", "CO", "CO13192338"),
    ("15-017263-00000-000", "CO", "CO15017263"),
    ("9-108636", "CO", "CO09108636"),
    ("US008303926B1", None, "US8303926"),
])
def test_number_keys(raw, country, expected):
    assert expected in number_keys(raw, country)


def test_number_without_country_has_no_bare_key():
    assert number_keys("37501", None) == set()


def lens_record(lens_id, doc_number, title, jurisdiction="CO", year=2022,
                abstract=None, inventors=("PEREZ GOMEZ JUAN",), family=None):
    members = [{"lens_id": member, "document_id": {"jurisdiction": "CO", "doc_number": number}}
               for member, number in (family or [(lens_id, doc_number)])]
    return {
        "lens_id": lens_id, "jurisdiction": jurisdiction, "doc_number": doc_number,
        "date_published": f"{year}-01-01",
        "abstract": abstract or [],
        "families": {"simple_family": {"members": members}},
        "biblio": {
            "invention_title": [{"text": title, "lang": "es"}],
            "application_reference": {"jurisdiction": jurisdiction, "doc_number": doc_number,
                                      "date": f"{year}-01-01"},
            "parties": {"inventors": [{"extracted_name": {"value": name}} for name in inventors]},
        },
    }


def test_generic_subset_title_is_not_a_fuzzy_match():
    index = LensIndex([lens_record("L1", "2013000001", "Sistema de tratamiento de aguas", year=2016)])
    patent = {"titles": ["Sistema y metodo de tratamiento de aguas residuales en nucleos "
                         "descentralizados"], "year": 2013, "authors": []}
    assert match_patent(index, patent)["lens_id"] is None
    assert content_overlap(normalize_text(patent["titles"][0]),
                           "sistema de tratamiento de aguas") == 0.0


def test_exact_title_ambiguous_across_families_is_skipped():
    title = "Dispositivo de anclaje para postes de concreto"
    index = LensIndex([lens_record("L1", "2010000001", title, year=2010),
                       lens_record("L2", "2020000002", title, year=2020)])
    assert match_patent(index, {"titles": [title], "year": None})["reason"] == "ambiguous_title"
    assert match_patent(index, {"titles": [title], "year": 2019})["lens_id"] == "L2"


def test_inventor_rule_needs_inventor_and_year():
    lens = lens_record("L1", "2016000001", "Dispositivo modular de ventilacion para viviendas "
                       "de interes social", year=2017, inventors=("GOMEZ RUIZ ANA MARIA",))
    index = LensIndex([lens])
    patent = {"titles": ["Dispositivo modular de renovacion de aire para viviendas de interes "
                         "social"], "year": 2016, "authors": ["Ana Maria Gomez Ruiz"]}
    assert match_patent(index, patent)["matched_by"] == "inventor_title"
    assert match_patent(index, {**patent, "authors": ["Pedro Lopez"]})["lens_id"] is None
    assert match_patent(index, {**patent, "year": 2005})["lens_id"] is None


def prepare(monkeypatch, lens_docs, patents, snapshot):
    clients = {"mongodb://kahi": mongomock.MongoClient(), "mongodb://lens": mongomock.MongoClient()}
    monkeypatch.setattr(plugin_module, "MongoClient", lambda uri: clients[uri])
    kahi = clients["mongodb://kahi"].kahi
    lens = clients["mongodb://lens"]
    lens.lens.patents.insert_many([{"_id": doc["lens_id"], "lens": doc} for doc in lens_docs])
    lens.dam.patents_final.insert_many(snapshot or [{"_id": "unused"}])
    kahi.patents.insert_many(patents or [{"_id": "not-a-patent", "types": []}])
    kahi.minciencias_opendata_patents_runs.insert_one({
        "_id": "patents:release", "status": "complete", "finished_at": 1,
        "source_collection": "patents_final",
    })
    config = {
        "database_url": "mongodb://kahi", "database_name": "kahi",
        "lens_patents": {"database_url": "mongodb://lens", "database_name": "lens",
                         "collection_name": "patents", "snapshot_database_name": "dam"},
    }
    return kahi, config


def kahi_patent(identifier, title):
    return {
        "_id": identifier, "titles": [{"title": title, "lang": "es", "source": "minciencias"}],
        "updated": [{"source": "minciencias", "time": 1}], "abstracts": [],
        "types": [PATENT_TYPE],
        "external_ids": [{"provenance": "minciencias", "source": "minciencias", "id": identifier}],
        "external_urls": [], "authors": [{"full_name": "Juan Perez Gomez"}],
    }


def snapshot_doc(identifier, number, year=2022):
    return {"_id": identifier, "source_metadata": {"occurrences": [{"metadata": {
        "registration_number": number, "country": "Colombia", "year": year}}]}}


def test_plugin_enriches_matched_patents_and_is_idempotent(monkeypatch):
    abstract = [{"text": "<p>Un dispositivo H<sub>2</sub>O para medir</p>", "lang": "es"},
                {"text": "A device to measure", "lang": "en"}]
    lens_docs = [
        lens_record("010-1", "2022007666", "Maquina dispensadora de alimentos", abstract=abstract),
        lens_record("010-2", "2021000001", "Patente que solo esta en Lens"),
    ]
    patents = [kahi_patent("P1", "Máquina dispensadora con inteligencia artificial"),
               kahi_patent("P2", "Patente de modelo de utilidad")]
    snapshot = [snapshot_doc("P1", "NC2022/0007666"), snapshot_doc("P2", None)]
    kahi, config = prepare(monkeypatch, lens_docs, patents, snapshot)

    Plugin(config).run()
    patent = kahi.patents.find_one({"_id": "P1"})
    assert {"provenance": "lens", "source": "lens", "id": "010-1"} in patent["external_ids"]
    assert patent["external_urls"] == [{"provenance": "lens", "source": "lens",
                                        "url": "https://www.lens.org/lens/patent/010-1"}]
    assert [item["lang"] for item in patent["abstracts"]] == ["es", "en"]
    assert set(patent["abstracts"][0]["abstract"]) == {"Un", "dispositivo", "H", "2", "O",
                                                       "para", "medir"}
    assert [item["source"] for item in patent["updated"]] == ["minciencias", "lens"]
    assert kahi.patents.count_documents({}) == 2
    assert kahi.patents.find_one({"_id": "P2"})["external_ids"][0]["source"] == "minciencias"
    audit = kahi.lens_patents_matches.find_one({"_id": "P1"})
    assert audit["matched_by"] == "number" and audit["evidence"] == ["CO2022007666"]

    Plugin(config).run()
    assert kahi.patents.find_one({"_id": "P1"}) == patent


def test_plugin_removes_lens_entries_that_no_longer_match(monkeypatch):
    lens_docs = [lens_record("010-1", "2022007666", "Maquina dispensadora de alimentos")]
    stale = kahi_patent("P1", "Titulo sin relacion con nada de lens")
    stale["external_ids"].append({"provenance": "lens", "source": "lens", "id": "old"})
    stale["abstracts"] = [{"abstract": {"x": [0]}, "lang": "es", "source": "lens",
                           "provenance": "lens"}]
    stale["updated"].append({"source": "lens", "time": 5})
    kahi, config = prepare(monkeypatch, lens_docs, [stale], [snapshot_doc("P1", None)])

    Plugin(config).run()
    patent = kahi.patents.find_one({"_id": "P1"})
    assert [item["source"] for item in patent["external_ids"]] == ["minciencias"]
    assert patent["abstracts"] == [] and patent["updated"][-1]["source"] == "minciencias"


def test_plugin_requires_a_complete_patents_run(monkeypatch):
    kahi, config = prepare(monkeypatch, [lens_record("010-1", "1", "Titulo cualquiera largo")],
                           [], [])
    kahi.minciencias_opendata_patents_runs.delete_many({})
    with pytest.raises(RuntimeError, match="no complete snapshot run"):
        Plugin(config).run()


LENS_INVENTORS = ["PARRA MORENO HECTOR JULIO", "DUQUE LUNA JHONY EDWARD", "PEÑA TORRES JESSICA"]


def member(identifier, matched_by, *names, author_ids=(), owner_ids=()):
    return {"_id": identifier, "matched_by": matched_by, "names": list(names),
            "author_ids": list(author_ids), "owner_ids": list(owner_ids)}


def test_inventor_names_need_more_than_shared_given_names():
    assert same_inventor(inventor_tokens("Hector Julio PARRA MORENO"),
                         inventor_tokens("Héctor Parra Moreno"))
    assert not same_inventor(inventor_tokens("Juan Carlos Perez"),
                             inventor_tokens("Juan Carlos Gomez"))
    assert is_lens_inventor([inventor_tokens("Jonny Edward Duque Luna")], LENS_INVENTORS)
    assert not is_lens_inventor([inventor_tokens("Hector Julio Gomez")], LENS_INVENTORS)


def test_clusters_require_number_inventor_or_owner_evidence():
    members = [
        member("by-number", "number", "Hector Julio PARRA MORENO", author_ids=["0001317393"]),
        member("same-inventor", "title_exact", "Hector Julio PARRA MORENO"),
        member("lens-inventor", "title_exact", "Jonny Edward Duque Luna"),
        member("official", "title_exact", owner_ids=["0001317393"]),
        member("title-only", "title_exact", "Pedro Pablo Ruiz Diaz"),
        member("no-evidence", "title_fuzzy"),
    ]
    found = clusters(members, LENS_INVENTORS)
    assert len(found) == 1
    assert sorted(found[0]["ids"]) == ["by-number", "lens-inventor", "official", "same-inventor"]
    assert found[0]["evidence"]["lens-inventor"] == ["lens_inventor"]
    assert found[0]["evidence"]["official"] == ["shared_owner"]
    assert "number" in found[0]["evidence"]["by-number"]
    assert choose_survivor(members[:4]) == "by-number"
    # Two title-only patents of different people stay apart.
    assert clusters([members[4], member("other", "title_exact", "Ana Maria Gomez Ruiz")],
                    LENS_INVENTORS) == []


def test_merge_documents_unions_people_groups_and_identifiers():
    survivor = {"_id": "a", "titles": [{"title": "Trampa para Aedes"}],
                "authors": [{"id": "1", "full_name": "Hector Parra"}], "author_count": 1,
                "groups": [{"id": "G1"}], "external_ids": [{"source": "minciencias", "id": "a"}],
                "updated": [{"source": "minciencias", "time": 1}, {"source": "lens", "time": 2}]}
    other = {"_id": "b", "titles": [{"title": "TRAMPA PARA AEDES"}],
             "authors": [{"id": "1", "full_name": "Hector Julio Parra"},
                         {"id": "2", "full_name": "Jonny Duque"}],
             "groups": [{"id": "G1"}, {"id": "G2"}],
             "external_ids": [{"source": "minciencias", "id": "b"}],
             "updated": [{"source": "minciencias", "time": 1}]}
    merged = merge_documents(survivor, [other])
    assert [author["id"] for author in merged["authors"]] == ["1", "2"]
    assert merged["author_count"] == 2 and len(merged["titles"]) == 1
    assert [group["id"] for group in merged["groups"]] == ["G1", "G2"]
    assert [item["id"] for item in merged["external_ids"]] == ["a", "b"]
    assert [item["source"] for item in merged["updated"]] == ["minciencias", "lens"]


def duplicated(monkeypatch, **options):
    lens_docs = [lens_record("010-1", "2013095397", "Trampa para captura y monitoreo de Aedes "
                             "aegypti", inventors=LENS_INVENTORS)]
    first = kahi_patent("P1", "Trampa para captura y monitoreo de Aedes aegypti")
    first["authors"] = [{"id": "0001317393", "full_name": "Hector Julio PARRA MORENO"}]
    second = kahi_patent("P2", "TRAMPA PARA CAPTURA Y MONITOREO DE AEDES AEGYPTI")
    second["authors"] = [{"id": "0000075183", "full_name": "Jonny Edward Duque Luna"}]
    official = kahi_patent("P3", "Trampa para captura y monitoreo de Aedes aegypti")
    official["authors"] = []
    official["groups"] = [{"id": "COL1", "name": "Grupo"}]
    stranger = kahi_patent("P4", "Trampa para captura y monitoreo de Aedes aegypti")
    stranger["authors"] = [{"id": "9", "full_name": "Pedro Pablo Ruiz Diaz"}]
    snapshot = [snapshot_doc("P1", "NC2013/0095397"), snapshot_doc("P2", None),
                snapshot_doc("P3", None), snapshot_doc("P4", None)]
    snapshot[0]["authors"] = [{"id": "0001317393"}]
    snapshot[2]["source_metadata"]["occurrences"][0]["metadata"]["owner_ids"] = ["0001317393"]
    kahi, config = prepare(monkeypatch, lens_docs, [first, second, official, stranger], snapshot)
    config["lens_patents"].update(options)
    return kahi, config


def test_plugin_merges_proven_duplicates_and_keeps_them_recoverable(monkeypatch):
    kahi, config = duplicated(monkeypatch, merge_duplicates=True)

    Plugin(config).run()

    assert sorted(doc["_id"] for doc in kahi.patents.find()) == ["P1", "P4"]
    survivor = kahi.patents.find_one({"_id": "P1"})
    assert [author["id"] for author in survivor["authors"]] == ["0001317393", "0000075183"]
    assert survivor["author_count"] == 2 and survivor["groups"] == [{"id": "COL1", "name": "Grupo"}]
    assert [item["id"] for item in survivor["external_ids"]] == ["P1", "P2", "P3", "010-1"]
    assert len([item for item in survivor["external_ids"] if item["source"] == "lens"]) == 1
    assert len(survivor["titles"]) == 1
    merge = kahi.lens_patents_merges.find_one({"_id": "P2"})
    assert merge["merged_into"] == "P1" and merge["evidence"] == ["lens_inventor"]
    assert merge["document"]["authors"][0]["full_name"] == "Jonny Edward Duque Luna"
    assert kahi.lens_patents_merges.find_one({"_id": "P3"})["evidence"] == ["shared_owner"]
    assert kahi.lens_patents_matches.find_one({"_id": "P2"})["merged_into"] == "P1"
    assert "merged_into" not in kahi.lens_patents_matches.find_one({"_id": "P4"})

    Plugin(config).run()
    assert kahi.patents.find_one({"_id": "P1"}) == survivor
    assert kahi.patents.count_documents({}) == 2


def test_merge_is_off_by_default_and_dry_run_deletes_nothing(monkeypatch):
    kahi, config = duplicated(monkeypatch)
    Plugin(config).run()
    assert kahi.patents.count_documents({}) == 4
    assert "lens_patents_merges" not in kahi.list_collection_names()

    config["lens_patents"]["merge_dry_run"] = True
    Plugin(config).run()
    assert kahi.patents.count_documents({}) == 4
    assert sorted(doc["_id"] for doc in kahi.lens_patents_merges_dry_run.find()) == ["P2", "P3"]
    assert "lens_patents_merges" not in kahi.list_collection_names()

import mongomock
import pytest
from concurrent.futures import ThreadPoolExecutor
from kahi.KahiBase import KahiBase

from kahi_minciencias_opendata_affiliations.Kahi_minciencias_opendata_affiliations import (
    Kahi_minciencias_opendata_affiliations,
    is_strict_institution_match,
    legacy_group_ranking,
    normalize_scienti_nit,
    replace_legacy_group_name,
    replace_legacy_group_ranking,
)


def plugin_without_database():
    plugin = Kahi_minciencias_opendata_affiliations.__new__(
        Kahi_minciencias_opendata_affiliations)
    plugin.institution_match_cache = {}
    return plugin


def test_legal_suffix_variant_is_a_strict_match():
    plugin = plugin_without_database()
    match = plugin.institution_match_score(
        {"names": [{"name": "Cementos Argos"}]},
        "cementos argos s a",
    )

    assert is_strict_institution_match(match)


def test_weakly_related_institution_is_not_a_strict_match():
    plugin = plugin_without_database()
    match = plugin.institution_match_score(
        {"names": [{"name": "Universidad de Antioquia"}]},
        "universidad industrial de santander",
    )

    assert not is_strict_institution_match(match)


def test_cataloged_institution_outranks_legacy_iua():
    plugin = plugin_without_database()
    match = plugin.institution_match_score(
        {"names": [{"name": "Cementos Argos"}]},
        "cementos argos",
    )
    catalog = {
        "_id": "01gyh5y52",
        "names": [{"name": "Cementos Argos"}],
        "addresses": [{"city": "Medellín", "state": "Antioquia"}],
        "relations": [],
        "external_ids": [{
            "provenance": "scienti", "source": "COD_INST",
            "id": "000000009892",
        }],
    }
    synthetic = {
        "_id": "IUA123456",
        "names": [{"name": "Cementos Argos"}],
        "addresses": [{"city": "Medellín", "state": "Antioquia"}],
        "relations": [],
        "external_ids": [],
    }
    reg = {"nme_municipio_gr": "Medellín", "nme_departamento_gr": "Antioquia"}

    assert plugin.institution_candidate_rank(catalog, match, reg) > plugin.institution_candidate_rank(
        synthetic, match, reg)


def test_exact_alias_wins_a_fuzzy_score_tie():
    plugin = plugin_without_database()
    match = plugin.institution_match_score({"names": [
        {"name": "Fundación Universitaria de Ciencias de la Salud"},
        {"name": "Fundación Universitaria de Ciencias de la Salud - FUCS"},
    ]}, "fundacion universitaria de ciencias de la salud fucs")
    assert match["normalized_name"] == match["normalized_source_name"]


def test_cod_inst_is_used_before_the_unrelated_name():
    plugin = plugin_without_database()
    institution = {
        "_id": "canonical", "types": [{"type": "education"}],
        "external_ids": [{"source": "COD_INST", "id": "000000000410"}],
    }
    collection = type("Collection", (), {})()
    cursor = type("Cursor", (list,), {"limit": lambda self, value: self})
    collection.find = lambda query: cursor([institution])
    plugin.collection = collection
    assert plugin.find_matching_institution(
        "unrelated name", cod_inst="000000000410") is institution


def test_text_match_returns_complete_document_without_search_score():
    plugin = configured_catalog_plugin()
    canonical = insert_canonical(plugin, "canonical", "Institucion Canonica", [])
    canonical["external_urls"] = ["https://example.org"]
    canonical["description"] = [{"lang": "es", "text": "Descripción"}]
    plugin.collection.replace_one({"_id": canonical["_id"]}, canonical)
    projected = {
        "_id": canonical["_id"],
        "names": canonical["names"],
        "types": canonical["types"],
        "addresses": [],
        "external_ids": [],
        "relations": [],
        "score": 8.0,
    }
    plugin.get_institution_candidates = lambda name: [projected]

    matched = plugin.find_matching_institution("Institucion Canonica")

    assert matched["external_urls"] == ["https://example.org"]
    assert matched["description"] == [{"lang": "es", "text": "Descripción"}]
    assert "score" not in matched


def test_materialization_preserves_every_standard_affiliation_field():
    plugin = configured_catalog_plugin()
    partial = {
        "_id": "canonical",
        "names": [{"source": "ror", "lang": "es", "name": "Institución"}],
        "types": [{"source": "ror", "type": "education"}],
        "external_ids": [],
        "score": 7.5,
    }
    persisted = plugin.empty_affiliation()
    persisted.update({
        "_id": "canonical",
        "names": partial["names"],
        "types": partial["types"],
        "external_urls": ["https://example.org"],
        "products_count": 17,
    })
    plugin.collection.insert_one(persisted)
    plugin.find_matching_institution = lambda *args, **kwargs: partial
    source = {
        "COD_INST": "000000000001", "COD_INST_MACRO": "000000000001",
        "NME_INST": "Institución",
    }
    plugin.scienti_catalog = {source["COD_INST"]: source}

    plugin.materialize_scienti_institution(source)
    stored = plugin.collection.find_one({"_id": "canonical"})

    assert set(plugin.empty_affiliation()).issubset(stored)
    assert stored["external_urls"] == ["https://example.org"]
    assert stored["products_count"] == 17
    assert "score" not in stored


def test_campus_name_is_preserved_for_disambiguation():
    plugin = plugin_without_database()
    assert plugin.normalize_inst_aval(
        "Universidad Santo Tomás Seccional Bucaramanga"
    ) == "universidad santo tomas seccional bucaramanga"


@pytest.mark.parametrize("label,parent", [
    (
        "UNIVERSIDAD MANUELA BELTRAN SECCIONAL BUCARAMANGA "
        "(UNIVERSIDAD MANUELA BELTRAN)",
        "UNIVERSIDAD MANUELA BELTRAN",
    ),
    (
        "UNIVERSIDAD PONTIFICIA BOLIVARIANA SECCIONAL BUCARAMANGA "
        "(UNIVERSIDAD PONTIFICIA BOLIVARIANA)",
        "UNIVERSIDAD PONTIFICIA BOLIVARIANA",
    ),
    (
        "PONTIFICIA UNIVERSIDAD JAVERIANA PUJ SEDE CALI "
        "(PONTIFICIA UNIVERSIDAD JAVERIANA)",
        "PONTIFICIA UNIVERSIDAD JAVERIANA",
    ),
    (
        "UNIVERSIDAD SANTO TOMAS SECCIONAL BUCARAMANGA "
        "(UNIVERSIDAD SANTO TOMAS)",
        "UNIVERSIDAD SANTO TOMAS",
    ),
    (
        "UNIVERSIDAD SANTO TOMAS VILLAVICENCIO "
        "(UNIVERSIDAD SANTO TOMAS)",
        "UNIVERSIDAD SANTO TOMAS",
    ),
    (
        "UNIVERSIDAD DEL SINU SECCIONAL CARTAGENA "
        "(UNIVERSIDAD DEL SINU ELIAS BECHARA ZAINUM)",
        "UNIVERSIDAD DEL SINU ELIAS BECHARA ZAINUM",
    ),
    (
        "UNIVERSIDAD NACIONAL DE COLOMBIA SEDE BOGOTA "
        "(UNIVERSIDAD NACIONAL DE COLOMBIA)",
        "UNIVERSIDAD NACIONAL DE COLOMBIA",
    ),
    (
        "UNIVERSIDAD DE SAN BUENAVENTURA CALI "
        "(UNIVERSIDAD DE SAN BUENAVENTURA)",
        "UNIVERSIDAD DE SAN BUENAVENTURA",
    ),
    (
        "UNIVERSIDAD SANTO TOMAS SECCIONAL TUNJA "
        "(UNIVERSIDAD SANTO TOMAS)",
        "UNIVERSIDAD SANTO TOMAS",
    ),
    (
        "UNIVERSIDAD PONTIFICIA BOLIVARIANA SECCIONAL MONTERIA "
        "(UNIVERSIDAD PONTIFICIA BOLIVARIANA)",
        "UNIVERSIDAD PONTIFICIA BOLIVARIANA",
    ),
    (
        "UNIVERSIDAD NACIONAL DE COLOMBIA SEDE MEDELLIN "
        "(UNIVERSIDAD NACIONAL DE COLOMBIA)",
        "UNIVERSIDAD NACIONAL DE COLOMBIA",
    ),
    (
        "UNIVERSIDAD COOPERATIVA DE COLOMBIA CAMPUS NEIVA "
        "(UNIVERSIDAD COOPERATIVA DE COLOMBIA)",
        "UNIVERSIDAD COOPERATIVA DE COLOMBIA",
    ),
    (
        "UNIVERSIDAD NACIONAL DE COLOMBIA SEDE MANIZALES "
        "(UNIVERSIDAD NACIONAL DE COLOMBIA)",
        "UNIVERSIDAD NACIONAL DE COLOMBIA",
    ),
    (
        "UNIVERSIDAD SANTO TOMAS MEDELLIN (UNIVERSIDAD SANTO TOMAS)",
        "UNIVERSIDAD SANTO TOMAS",
    ),
    (
        "UNIVERSIDAD PONTIFICIA BOLIVARIANA SEDE PALMIRA "
        "(UNIVERSIDAD PONTIFICIA BOLIVARIANA)",
        "UNIVERSIDAD PONTIFICIA BOLIVARIANA",
    ),
])
def test_declared_campus_parent_is_extracted(label, parent):
    plugin = plugin_without_database()
    assert plugin.declared_campus_parent_name(label) == parent


def test_non_campus_unit_is_not_collapsed_to_parent():
    plugin = plugin_without_database()
    assert plugin.declared_campus_parent_name(
        "CLINICA UNIVERSITARIA BOLIVARIANA UNIVERSIDAD PONTIFICIA "
        "BOLIVARIANA (UNIVERSIDAD PONTIFICIA BOLIVARIANA)"
    ) == ""


def test_legacy_ranking_is_added_to_an_existing_scienti_group():
    entry = {
        "ranking": [{"source": "scienti", "rank": "C"}],
    }
    record = {
        "nme_clasificacion_gr": "A1", "orden_clas_gr": "1",
        "ano_convo": "2024",
    }
    replace_legacy_group_ranking(entry, record)
    assert [item["source"] for item in entry["ranking"]] == [
        "scienti", "minciencias",
    ]
    assert entry["ranking"][1]["rank"] == "A1"


def test_legacy_ranking_refresh_is_idempotent_and_preserves_other_sources():
    entry = {
        "ranking": [
            {"source": "scienti", "rank": "C"},
            {"source": "minciencias", "rank": "B"},
        ],
    }
    record = {
        "nme_clasificacion_gr": "A", "orden_clas_gr": "2",
        "ano_convo": "2024",
    }
    replace_legacy_group_ranking(entry, record)
    replace_legacy_group_ranking(entry, record)
    assert entry["ranking"] == [
        {"source": "scienti", "rank": "C"},
        legacy_group_ranking(record),
    ]


def test_existing_group_update_persists_the_legacy_ranking():
    collection = mongomock.MongoClient().db.affiliations
    entry = KahiBase().empty_affiliation()
    entry.update({
        "_id": "COL0000001",
        "updated": [{"source": "scienti", "time": 1}],
        "names": [{"source": "scienti", "lang": "es", "name": "Grupo"}],
        "types": [{"source": "scienti", "type": "group"}],
        "external_ids": [{"source": "minciencias", "id": "COL0000001"}],
        "ranking": [{"source": "scienti", "rank": "C"}],
    })
    collection.insert_one(entry)
    plugin = plugin_without_database()
    plugin.collection = collection
    plugin.inserted_cod_grupo = []
    plugin.missing_group_names = {}
    record = {
        "cod_grupo_gr": "COL0000001", "nme_grupo_gr": "Grupo",
        "nme_clasificacion_gr": "A1", "orden_clas_gr": "1",
        "ano_convo": "2024",
    }
    plugin.process_one(record, collection, plugin.empty_affiliation(), 0)
    plugin.process_one(record, collection, plugin.empty_affiliation(), 0)
    stored = collection.find_one({"_id": "COL0000001"})
    assert [item["source"] for item in stored["ranking"]] == [
        "scienti", "minciencias",
    ]
    assert stored["ranking"][1] == legacy_group_ranking(record)
    assert stored["names"] == [
        {"source": "scienti", "lang": "es", "name": "Grupo"},
        {"source": "minciencias", "lang": "es", "name": "Grupo"},
    ]


def test_legacy_name_refresh_preserves_names_from_other_sources():
    entry = {"names": [
        {"source": "scienti", "lang": "es", "name": "Nombre ScienTI"},
        {"source": "minciencias", "lang": "es", "name": "Nombre anterior"},
    ]}
    replace_legacy_group_name(entry, "Nombre convocatoria reciente")
    replace_legacy_group_name(entry, "Nombre convocatoria reciente")
    assert entry["names"] == [
        {"source": "scienti", "lang": "es", "name": "Nombre ScienTI"},
        {
            "source": "minciencias", "lang": "es",
            "name": "Nombre convocatoria reciente",
        },
    ]


def test_blank_legacy_name_does_not_remove_existing_evidence():
    entry = {"names": [
        {"source": "minciencias", "lang": "es", "name": "Nombre anterior"},
    ]}
    replace_legacy_group_name(entry, "")
    assert entry["names"][0]["name"] == "Nombre anterior"


def configured_catalog_plugin():
    plugin = plugin_without_database()
    plugin.collection = mongomock.MongoClient().db.affiliations
    plugin.materialized_scienti_codes = {}
    return plugin


def insert_canonical(plugin, identifier, name, external_ids):
    entry = plugin.empty_affiliation()
    entry.update({
        "_id": identifier,
        "names": [{"source": "ror", "lang": "es", "name": name}],
        "types": [{"source": "ror", "type": "education"}],
        "external_ids": external_ids,
    })
    plugin.collection.insert_one(entry)
    return entry


@pytest.mark.parametrize("name,ror_id", [
    ("UNIVERSIDAD DE SUCRE", "04fbb7514"),
    ("UNIVERSIDAD MILITAR NUEVA GRANADA", "05n0gsn30"),
    ("UNIVERSIDAD DEL MAGDALENA", "038mvjn28"),
    ("UNIVERSIDAD DE PAMPLONA", "04dfr7a85"),
    ("UNIVERSIDAD DE LA SALLE", "0474gxy81"),
])
def test_selected_names_keep_full_form_and_outrank_iua(name, ror_id):
    plugin = configured_catalog_plugin()
    insert_canonical(plugin, ror_id, name.title(), [
        {"provenance": "ror", "source": "ror", "id": ror_id},
    ])
    iua = plugin.empty_affiliation()
    iua.update({
        "_id": "IUA_test",
        "names": [{"source": "minciencias", "lang": "es", "name": name}],
        "types": [{"source": "minciencias", "type": "other"}],
    })
    plugin.collection.insert_one(iua)

    assert plugin.normalize_inst_aval(name) == name.lower()
    assert plugin.find_matching_institution(name)["_id"] == ror_id


def test_colombian_la_salle_rejects_us_homonym():
    plugin = configured_catalog_plugin()
    colombian = insert_canonical(
        plugin, "0474gxy81", "Universidad de La Salle", [
            {"provenance": "ror", "source": "ror", "id": "0474gxy81"},
        ])
    colombian["addresses"] = [{"country": "Colombia", "country_code": "CO"}]
    plugin.collection.replace_one({"_id": colombian["_id"]}, colombian)
    foreign = insert_canonical(
        plugin, "02beve479", "Universidad de La Salle", [
            {"provenance": "ror", "source": "ror", "id": "02beve479"},
        ])
    foreign["addresses"] = [{"country": "United States", "country_code": "US"}]
    plugin.collection.replace_one({"_id": foreign["_id"]}, foreign)

    assert plugin.find_matching_institution(
        "UNIVERSIDAD DE LA SALLE")["_id"] == "0474gxy81"


def test_scienti_campus_group_relation_keeps_only_canonical_root():
    plugin = configured_catalog_plugin()
    insert_canonical(plugin, "059yx9a68", "Universidad Nacional de Colombia", [
        {"provenance": "ror", "source": "ror", "id": "059yx9a68"},
        {"provenance": "snies", "source": "snies", "id": "1101"},
        {"provenance": "ror", "source": "nit", "id": "899999063"},
    ])
    root = {
        "COD_INST": "625100000880", "COD_INST_MACRO": "625100000880",
        "NME_INST": "UNIVERSIDAD NACIONAL DE COLOMBIA", "COD_IES": "1101",
        "TXT_NIT": "899999063", "TXT_CIUDAD_INST": "Bogotá",
    }
    campus = {
        "COD_INST": "625100001102", "COD_INST_MACRO": "625100000880",
        "NME_INST": "UNIVERSIDAD NACIONAL DE COLOMBIA",
        "TXT_CIUDAD_INST": "Medellín",
    }
    plugin.scienti_catalog = {root["COD_INST"]: root, campus["COD_INST"]: campus}
    plugin.scienti_name_index = plugin.index_scienti_catalog(plugin.scienti_catalog)
    group = plugin.empty_affiliation()
    plugin.add_aval_institution_relations({
        "inst_aval": "UNIVERSIDAD NACIONAL DE COLOMBIA",
        "nme_departamento_gr": "Antioquia", "nme_municipio_gr": "Medellín",
    }, group, plugin.collection)

    assert plugin.collection.find_one({"_id": "625100001102"}) is not None
    assert {item["id"] for item in group["relations"]} == {"059yx9a68"}


def test_parenthetical_unal_campus_keeps_only_canonical_root():
    plugin = configured_catalog_plugin()
    insert_canonical(plugin, "059yx9a68", "Universidad Nacional de Colombia", [
        {"provenance": "ror", "source": "ror", "id": "059yx9a68"},
    ])
    insert_canonical(plugin, "030kw0b65", "Universidad de Medellín", [
        {"provenance": "ror", "source": "ror", "id": "030kw0b65"},
    ])
    group = plugin.empty_affiliation()

    plugin.add_aval_institution_relations({
        "inst_aval": (
            "UNIVERSIDAD NACIONAL DE COLOMBIA SEDE MEDELLIN "
            "(UNIVERSIDAD NACIONAL DE COLOMBIA)"
        ),
        "nme_departamento_gr": "Antioquia",
        "nme_municipio_gr": "Medellín",
    }, group, plugin.collection)

    assert {item["id"] for item in group["relations"]} == {"059yx9a68"}
    assert plugin.collection.count_documents({"_id": {"$regex": "^IUA_"}}) == 0


def test_embedded_snies_campus_materializes_as_principal_only():
    plugin = configured_catalog_plugin()
    principal = insert_canonical(
        plugin, "03bp5hc83", "Universidad de Antioquia", [
            {"provenance": "ror", "source": "ror", "id": "03bp5hc83"},
            {"provenance": "snies", "source": "snies", "id": "1201"},
        ])
    principal["campuses"] = [{
        "id": "snies_1223", "name": "Universidad de Antioquia",
        "external_ids": [{
            "provenance": "snies", "source": "snies", "id": "1223",
        }],
        "addresses": [{"city": "Turbo", "state": "Antioquia"}],
    }]
    plugin.collection.replace_one({"_id": principal["_id"]}, principal)
    source = {
        "COD_INST": "000000001850", "COD_INST_MACRO": "000000001850",
        "COD_IES": "1223", "NME_INST": "UNIVERSIDAD DE ANTIOQUIA",
        "TXT_CIUDAD_INST": "Turbo",
    }
    plugin.scienti_catalog = {source["COD_INST"]: source}

    institution = plugin.materialize_scienti_institution(source)

    assert institution["_id"] == "03bp5hc83"
    assert plugin.collection.find_one({"_id": "000000001850"}) is None
    assert not any(item.get("id") == "000000001850"
                   for item in institution["external_ids"])


def test_colombian_univalle_catalog_evidence_rejects_homonymous_ror():
    plugin = configured_catalog_plugin()
    insert_canonical(plugin, "02xw8cw23", "Universidad del Valle", [
        {"provenance": "ror", "source": "ror", "id": "02xw8cw23"},
    ])
    insert_canonical(plugin, "00jb9vg53", "Universidad del Valle", [
        {"provenance": "ror", "source": "ror", "id": "00jb9vg53"},
        {"provenance": "ror", "source": "nit", "id": "890399010"},
        {"provenance": "snies", "source": "snies", "id": "1203"},
    ])
    source = {
        "COD_INST": "625100001203", "COD_INST_MACRO": "625100001203",
        "NME_INST": "UNIVERSIDAD DEL VALLE", "TXT_NIT": "890399010",
        "COD_IES": "1203", "TXT_CIUDAD_INST": "Cali",
    }
    plugin.scienti_catalog = {source["COD_INST"]: source}
    plugin.scienti_name_index = plugin.index_scienti_catalog(plugin.scienti_catalog)

    institution = plugin.get_or_create_aval_institution(
        "UNIVERSIDAD DEL VALLE", plugin.collection,
        {"nme_departamento_gr": "Valle del Cauca", "nme_municipio_gr": "Cali"})

    assert institution["_id"] == "00jb9vg53"
    assert any(item.get("source") == "COD_INST" and
               item.get("id") == "625100001203"
               for item in institution["external_ids"])


def test_unmatched_scienti_institution_uses_exact_cod_inst_as_id():
    plugin = configured_catalog_plugin()
    source = {
        "COD_INST": "000000009999", "COD_INST_MACRO": "000000009999",
        "NME_INST": "Institución de Prueba Única",
    }
    plugin.scienti_catalog = {source["COD_INST"]: source}
    plugin.scienti_name_index = plugin.index_scienti_catalog(plugin.scienti_catalog)
    plugin.find_matching_institution = lambda *args, **kwargs: None

    institution = plugin.get_or_create_aval_institution(
        source["NME_INST"], plugin.collection, {})

    assert institution["_id"] == "000000009999"


def test_invalid_nits_are_rejected():
    for value in [None, "", "0", "000000000", "000000010", "111111111",
                  "123456789", "999999999", "123-4"]:
        assert normalize_scienti_nit(value) == ""
    assert normalize_scienti_nit("901167074-2") == "901167074"


def test_placeholder_nit_does_not_merge_unrelated_institutions():
    plugin = configured_catalog_plugin()
    insert_canonical(plugin, "snies_9935", "Unidad Tecnológica", [
        {"provenance": "legacy", "source": "nit", "id": "000000000"},
    ])
    plugin.find_matching_institution = lambda *args, **kwargs: None
    source = {
        "COD_INST": "543000000880", "COD_INST_MACRO": "543000000880",
        "NME_INST": "Programa de Desarrollo y Paz", "TXT_NIT": "000000000",
    }
    plugin.scienti_catalog = {source["COD_INST"]: source}

    institution = plugin.materialize_scienti_institution(source)

    assert institution["_id"] == "543000000880"
    assert not any(item.get("source") == "nit"
                   for item in institution["external_ids"])
    assert plugin.collection.find_one({"_id": "snies_9935"})["names"] == [
        {"source": "ror", "lang": "es", "name": "Unidad Tecnológica"},
    ]


def test_different_cod_inst_is_not_merged_by_fuzzy_name():
    plugin = configured_catalog_plugin()
    canonical = insert_canonical(plugin, "02sqgkj21", "Universidad de La Sabana", [
        {"provenance": "ror", "source": "ror", "id": "02sqgkj21"},
        {"provenance": "scienti", "source": "COD_INST", "id": "000000001675"},
        {"provenance": "snies", "source": "nit", "id": "860075558"},
    ])
    source = {
        "COD_INST": "000000009620", "COD_INST_MACRO": "000000009620",
        "NME_INST": "CLÍNICA UNIVERSIDAD DE LA SABANA",
        "TXT_NIT": "832003167",
    }
    plugin.scienti_catalog = {source["COD_INST"]: source}
    plugin.find_matching_institution = lambda *args, **kwargs: plugin.collection.find_one(
        {"_id": canonical["_id"]})

    institution = plugin.materialize_scienti_institution(source)

    assert institution["_id"] == source["COD_INST"]
    assert plugin.affiliation_cod_inst_codes(institution) == {source["COD_INST"]}
    assert institution["relations"] == []
    assert plugin.affiliation_cod_inst_codes(
        plugin.collection.find_one({"_id": canonical["_id"]})) == {"000000001675"}


def test_exact_duplicate_code_gets_canonical_parent_without_code_fusion():
    plugin = configured_catalog_plugin()
    canonical = insert_canonical(
        plugin, "04cmc9894", "Universidad Pedagógica Nacional", [
            {"provenance": "ror", "source": "ror", "id": "04cmc9894"},
            {"provenance": "scienti", "source": "COD_INST", "id": "003300000884"},
        ])
    source = {
        "COD_INST": "000000011714", "COD_INST_MACRO": "000000011714",
        "NME_INST": "UNIVERSIDAD PEDAGÓGICA NACIONAL",
    }
    plugin.scienti_catalog = {source["COD_INST"]: source}
    plugin.find_matching_institution = lambda *args, **kwargs: plugin.collection.find_one(
        {"_id": canonical["_id"]})

    institution = plugin.materialize_scienti_institution(source)

    assert institution["_id"] == source["COD_INST"]
    assert plugin.affiliation_cod_inst_codes(institution) == {source["COD_INST"]}
    assert [item["id"] for item in institution["relations"]] == [canonical["_id"]]
    assert [item["_id"] for item in plugin.institution_with_canonical_parents(
        institution, plugin.collection)] == [canonical["_id"]]


def test_shared_nit_does_not_merge_distinct_cod_inst():
    plugin = configured_catalog_plugin()
    first = plugin.empty_affiliation()
    first.update({
        "_id": "000000000843",
        "names": [{"source": "scienti", "lang": "es", "name": "Gobernación del Meta"}],
        "types": [{"source": "scienti", "type": "other"}],
        "external_ids": [
            {"provenance": "scienti", "source": "COD_INST", "id": "000000000843"},
            {"provenance": "scienti", "source": "nit", "id": "892000148"},
        ],
    })
    plugin.collection.insert_one(first)
    source_first = {
        "COD_INST": "000000000843", "NME_INST": "GOBERNACION DEL META",
        "TXT_NIT": "892000148",
    }
    source_second = {
        "COD_INST": "584100000889", "COD_INST_MACRO": "584100000889",
        "NME_INST": "SECRETARÍA DE SALUD DEL META", "TXT_NIT": "892000148",
    }
    plugin.scienti_catalog = {
        source_first["COD_INST"]: source_first,
        source_second["COD_INST"]: source_second,
    }
    plugin.unique_scienti_nits = plugin.catalog_unique_nits(plugin.scienti_catalog)
    plugin.find_matching_institution = lambda *args, **kwargs: plugin.collection.find_one(
        {"_id": first["_id"]})

    institution = plugin.materialize_scienti_institution(source_second)

    assert institution["_id"] == source_second["COD_INST"]
    assert plugin.affiliation_cod_inst_codes(institution) == {source_second["COD_INST"]}
    assert institution["relations"] == []


def test_parallel_materialization_keeps_one_cod_inst_per_document():
    plugin = configured_catalog_plugin()
    canonical = insert_canonical(plugin, "canonical", "Institución Canónica", [
        {"provenance": "ror", "source": "ror", "id": "canonical"},
    ])
    sources = [{
        "COD_INST": code, "COD_INST_MACRO": code,
        "NME_INST": "INSTITUCIÓN CANÓNICA", "TXT_NIT": nit,
    } for code, nit in [
        ("000000000001", "901000001"),
        ("000000000002", "901000002"),
    ]]
    plugin.scienti_catalog = {item["COD_INST"]: item for item in sources}
    plugin.unique_scienti_nits = plugin.catalog_unique_nits(plugin.scienti_catalog)
    plugin.find_matching_institution = lambda *args, **kwargs: plugin.collection.find_one(
        {"_id": canonical["_id"]})

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(plugin.materialize_scienti_institution, sources))

    documents = list(plugin.collection.find({
        "external_ids.source": "COD_INST",
    }))
    assert all(len(plugin.affiliation_cod_inst_codes(item)) == 1
               for item in documents)
    assert set().union(*(plugin.affiliation_cod_inst_codes(item)
                         for item in documents)) == {
        "000000000001", "000000000002",
    }

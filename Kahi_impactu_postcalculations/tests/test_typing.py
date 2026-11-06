from kahi_impactu_type_catalog import get_impactu_catalog
from kahi_impactu_postcalculations.typing import (
    build_type_lookup,
    clear_impactu_types,
    functors,
    process_minciencias,
    process_type,
)


def test_clear_impactu_types_removes_only_computed_types():
    class Result:
        modified_count = 2

    class Collection:
        def update_many(self, query, update):
            assert query == {"types.source": "impactu"}
            assert update == {"$pull": {"types": {"source": "impactu"}}}
            return Result()

    assert clear_impactu_types({"works": Collection()}).modified_count == 2


def mapping(source, native_type, impactu_type, entity="works"):
    return {
        "source": source,
        "type": native_type,
        "type_impactu": impactu_type,
        "entity": entity,
    }


def test_process_minciencias_maps_single_type():
    lookup = build_type_lookup(
        [mapping("minciencias", "Capítulo de libro", "book-chapter")],
        "minciencias",
    )
    work = {"types": [{"type": "Capítulo de libro"}]}
    assert process_minciencias(work, lookup)["type"] == "book-chapter"


def test_process_minciencias_accepts_normalized_single_type():
    lookup = build_type_lookup(
        [
            mapping(
                "minciencias",
                "Nuevo conocimiento: Capítulos de libro",
                "Capítulo de libro",
            )
        ],
        "minciencias",
    )
    work = {"types": [{"type": "Capítulo de libro"}]}
    assert process_minciencias(work, lookup)["type"] == "Capítulo de libro"


def test_process_minciencias_prefers_two_level_mapping():
    lookup = build_type_lookup(
        [
            mapping(
                "minciencias",
                "Producción bibliográfica: Capítulo de libro",
                "research-article",
            ),
        ],
        "minciencias",
    )
    work = {
        "types": [
            {"type": "Producción bibliográfica"},
            {"type": "Capítulo de libro"},
        ]
    }
    assert process_minciencias(work, lookup)["type"] == "research-article"


def test_crossref_functor_uses_lookup():
    lookup = build_type_lookup(
        [mapping("crossref", "journal-article", "article")],
        "crossref",
    )
    work = {"types": [{"type": "journal-article"}]}
    assert functors["crossref"](work, lookup)["type"] == "article"


def test_process_type_is_idempotent():
    calls = []

    class Collection:
        def update_one(self, query, update):
            calls.append((query, update))

    types = [mapping("crossref", "journal-article", "article")]
    work = {"_id": "work-1", "types": [{"type": "journal-article"}]}
    process_type({"works": Collection()}, work, "crossref", types, False)
    assert calls == [
        (
            {"_id": "work-1"},
            {
                "$addToSet": {
                    "types": {
                        "provenance": "crossref",
                        "source": "impactu",
                        "type": "article",
                    }
                }
            },
        )
    ]


def test_build_type_lookup_returns_mapping_result():
    lookup = build_type_lookup(
        [mapping("crossref", "journal-article", "article")],
        "crossref",
    )
    assert lookup["by_type"]["journal-article"] == ("article",)


def test_cvlac_lookup_derives_hierarchy_without_scienti_codes():
    lookup = build_type_lookup(
        [mapping(
            "scienti",
            "1: Producción bibliográfica 11: Artículo 111: Publicado en revista especializada",
            "Artículo de revista",
        )],
        "cvlac",
    )
    work = {"types": [{
        "source": "cvlac",
        "type": "Producción bibliográfica - Artículo - Publicado en revista especializada",
    }]}
    assert functors["cvlac"](work, lookup)["type"] == "Artículo de revista"


def test_gruplac_lookup_rejects_ambiguous_leaf():
    lookup = build_type_lookup(
        [
            mapping("scienti", "1: Uno 11: Otro", "Otro"),
            mapping("scienti", "2: Dos 21: Otro", "Código fuente"),
        ],
        "gruplac",
    )
    work = {"types": [{"source": "gruplac", "type": "Otro"}]}
    assert functors["gruplac"](work, lookup) == {}


def test_process_type_warnings_depend_on_verbose(capsys):
    class Collection:
        def update_one(self, _query, _update):
            raise AssertionError("update_one should not be called")

    types = [mapping("crossref", "journal-article", "article")]
    work = {"_id": "work-1", "types": [{"type": "unknown"}]}
    db = {"works": Collection()}
    process_type(db, work, "crossref", types, False)
    assert capsys.readouterr().out == ""
    process_type(db, work, "crossref", types, True)
    assert "work-1" in capsys.readouterr().out


def test_shared_catalog_drives_kahi_type_lookup():
    catalog = get_impactu_catalog()
    redcol_lookup = build_type_lookup(catalog, "redcol")
    ciarp_lookup = build_type_lookup(catalog, "ciarp")

    redcol_work = {"types": [{"type": "td"}]}
    assert functors["redcol"](redcol_work, redcol_lookup)["type"] == (
        "Tesis de posgrado"
    )

    ciarp_work = {
        "types": [{"type": "Direccion de trabajo de grado de doctorado"}]
    }
    assert functors["ciarp"](ciarp_work, ciarp_lookup)["type"] == (
        "Tesis de posgrado"
    )

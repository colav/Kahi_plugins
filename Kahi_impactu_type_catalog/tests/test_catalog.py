import json
from pathlib import Path

from kahi_impactu_type_catalog import get_impactu_catalog


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
DATA = PACKAGE_ROOT / "kahi_impactu_type_catalog" / "data"


def test_runtime_loader_reads_the_versioned_catalog():
    catalog = get_impactu_catalog()

    assert catalog.version == "1.2.0"
    assert len(catalog) == 800
    assert catalog.lookup("redcol", "td")["type_impactu"] == "Tesis de posgrado"
    assert catalog.lookup("coar", "c_12cc")["entity"] == "works"
    assert catalog.lookup("eu-repo", "conferencePaper")["entity"] == "works"


def test_schema_and_workbook_are_distributed_with_the_catalog():
    payload = json.loads(
        (DATA / "impactu_type_catalog_v1.json").read_text(encoding="utf-8")
    )

    assert (DATA / "impactu_type_catalog.schema.json").is_file()
    assert (DATA / "Tipos_ImpactU_Definitivo.xlsx").is_file()
    assert payload["source"]["workbook"] == "Tipos_ImpactU_Definitivo.xlsx"


def test_origin_and_parent_disambiguate_scraping_from_other_vocabularies():
    catalog = get_impactu_catalog()
    parent = "Articulos"
    native = "Producción bibliográfica - Artículo - Caso clínico"
    assert catalog.lookup_context("minciencias", "scraping", parent, native)["type_impactu"] == "Artículo de revista"
    assert catalog.lookup_context("scienti", "scraping", parent, native) is None
    assert catalog.lookup_context("minciencias", "dumps", parent, native) is None
    assert catalog.lookup_context("minciencias", "socrata",
        "Apropiación social del conocimiento y divulgación pública de la ciencia",
        "Artículos")["type_impactu"] == "Publicaciones editoriales no especializadas"
    assert catalog.lookup_scraping("Otro contexto", native) is None
    returned = catalog.lookup_scraping(parent, native)
    returned["type_impactu"] = "Libro"
    assert catalog.lookup_scraping(parent, native)["type_impactu"] == "Artículo de revista"


def test_coded_dumps_identities_remain_executable():
    catalog = get_impactu_catalog()
    for mapping in catalog.payload["mappings"]:
        if mapping["source"] == "scienti":
            assert catalog.lookup_context("scienti", "dumps", "", mapping["type"]) == mapping
            assert catalog.lookup_context("scienti", "socrata", "", mapping["type"]) is None

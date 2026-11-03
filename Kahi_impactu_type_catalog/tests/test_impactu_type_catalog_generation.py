import importlib.util
import hashlib
import json
from pathlib import Path

from openpyxl import load_workbook
import pytest


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = PACKAGE_ROOT / "scripts" / "generate_impactu_type_catalog.py"
WORKBOOK = (
    PACKAGE_ROOT
    / "kahi_impactu_type_catalog"
    / "data"
    / "Tipos_ImpactU_Definitivo.xlsx"
)
CATALOG = (
    PACKAGE_ROOT
    / "kahi_impactu_type_catalog"
    / "data"
    / "impactu_type_catalog_v1.json"
)


def load_generator():
    spec = importlib.util.spec_from_file_location("impactu_catalog_generator", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def mapping_index(catalog):
    return {
        (mapping["source"], mapping["type"]): mapping
        for mapping in catalog["mappings"]
    }


def test_committed_catalog_is_reproducible():
    generator = load_generator()
    generated = generator.build_catalog(WORKBOOK, "1.2.0")
    assert CATALOG.read_text(encoding="utf-8") == generator.serialized_catalog(
        generated
    )


def test_catalog_metadata_and_keys_are_valid():
    generator = load_generator()
    catalog = json.loads(CATALOG.read_text(encoding="utf-8"))
    keys = [
        (generator.exact_key(item["source"]), generator.exact_key(item["type"]))
        for item in catalog["mappings"]
    ]

    assert catalog["schema_version"] == "impactu-type-routing-v1"
    assert catalog["catalog_version"] == "1.2.0"
    assert catalog["source"]["input_rows"] == 982
    assert catalog["source"]["rows"] == len(catalog["mappings"]) == 800
    assert catalog["source"]["duplicates_removed"] == 0
    assert catalog["source"]["scraping_rows"] == len(catalog["scraping_mappings"]) == 180
    assert catalog["source"]["unmapped_rows"] == len(catalog["unmapped"]) == 2
    assert len(keys) == len(set(keys))


def test_original_800_mappings_are_unchanged():
    catalog = json.loads(CATALOG.read_text(encoding="utf-8"))
    digest = hashlib.sha256(json.dumps(catalog["mappings"], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    assert digest == "39cdefe301add0370f372f468fc329000f48de13432d506d0d0843c72dc86b47"


def test_unified_workbook_has_only_the_agreed_editorial_columns():
    book = load_workbook(WORKBOOK, read_only=True, data_only=True)
    try:
        assert book.sheetnames == ["Catalogo"]
        assert tuple(c.value for c in book.active[1]) == (
            "Fuente", "Origen", "Parent", "Type", "Tipo ImpactU", "Entidad", "Tipo ImpactU Sugerido")
        assert book.active.max_row == 983
        assert book.active.max_column == 7
    finally:
        book.close()


def test_generated_artifact_satisfies_its_distributed_schema():
    import jsonschema
    schema = json.loads(CATALOG.with_name("impactu_type_catalog.schema.json").read_text())
    jsonschema.validate(json.loads(CATALOG.read_text()), schema)


@pytest.mark.parametrize("change", ["duplicate", "scienti", "unknown_target"])
def test_editorial_errors_fail_before_publishing(tmp_path, change):
    import shutil
    target = tmp_path / WORKBOOK.name
    shutil.copy2(WORKBOOK.with_name("legacy_type_identities_v1.json"), tmp_path)
    book = load_workbook(WORKBOOK)
    sheet = book.active
    row = next(r for r in sheet.iter_rows(min_row=2) if r[1].value == "scraping")
    if change == "duplicate":
        sheet.append([c.value for c in row])
    elif change == "scienti":
        row[0].value = "scienti"
    else:
        row[6].value = "Tipo inventado"
    book.save(target)
    book.close()
    with pytest.raises(ValueError):
        load_generator().build_catalog(target)


def test_recent_type_decisions_are_in_the_catalog():
    catalog = json.loads(CATALOG.read_text(encoding="utf-8"))
    mappings = mapping_index(catalog)

    assert mappings[(
        "minciencias",
        "Apropiación social del conocimiento y divulgación pública de la ciencia: Libros de Formación",
    )]["type_impactu"] == "Libro"
    assert mappings[(
        "ciarp_udea",
        "Direccion de trabajo de grado de doctorado",
    )]["type_impactu"] == "Tesis de posgrado"
    assert mappings[("redcol", "td")]["type_impactu"] == "Tesis de posgrado"
    assert mappings[("redcol", "tm")]["type_impactu"] == "Tesis de posgrado"
    assert mappings[("redcol", "tp")]["type_impactu"] == "Tesis de pregrado"
    assert mappings[("redcol", "apo")]["type_impactu"] == "Otro"
    assert "Docencia" not in catalog["impactu_types"]

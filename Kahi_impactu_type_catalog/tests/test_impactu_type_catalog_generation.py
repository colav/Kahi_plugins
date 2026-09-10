import importlib.util
import json
from pathlib import Path


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
    generated = generator.build_catalog(WORKBOOK, "1.1.0")
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
    assert catalog["catalog_version"] == "1.1.0"
    assert catalog["source"]["input_rows"] == 807
    assert catalog["source"]["rows"] == len(catalog["mappings"]) == 800
    assert catalog["source"]["duplicates_removed"] == 5
    assert catalog["source"]["unmapped_rows"] == len(catalog["unmapped"]) == 2
    assert len(keys) == len(set(keys))


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

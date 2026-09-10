import json
from pathlib import Path

from kahi_impactu_type_catalog import get_impactu_catalog


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
DATA = PACKAGE_ROOT / "kahi_impactu_type_catalog" / "data"


def test_runtime_loader_reads_the_versioned_catalog():
    catalog = get_impactu_catalog()

    assert catalog.version == "1.1.0"
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

# ImpactU type catalog

`Kahi_impactu_type_catalog` is the shared source of ImpactU type mappings for
Kahi and Yuku. `Tipos_ImpactU_Definitivo.xlsx` remains the editorial source;
runtime consumers read only the generated, versioned JSON artifact.

The generator reads `ALL`, `COAR`, `REDCOL`, and `INFO-EU-REPO`. Auxiliary
sheets receive the fixed source names `coar`, `redcol`, and `eu-repo`.
Equivalent duplicate keys are collapsed; conflicting duplicate keys stop the
generation. Rows without `Tipo ImpactU` are preserved under `unmapped` and are
not exposed as executable mappings.

Generate or update the catalog:

```bash
python scripts/generate_impactu_type_catalog.py --catalog-version 1.1.0
```

Verify that the committed artifact matches the workbook:

```bash
python scripts/generate_impactu_type_catalog.py --catalog-version 1.1.0 --check
```

The JSON intentionally has no generation timestamp, so identical workbook
bytes and catalog version produce identical output bytes.

Runtime usage:

```python
from kahi_impactu_type_catalog import get_impactu_catalog

mapping = get_impactu_catalog().lookup("redcol", "td")
```

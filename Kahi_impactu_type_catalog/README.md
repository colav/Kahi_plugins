# ImpactU type catalog

`Kahi_impactu_type_catalog` is the shared source of ImpactU type mappings for
Kahi and Yuku. `Tipos_ImpactU_Definitivo.xlsx` remains the editorial source;
runtime consumers read only the generated, versioned JSON artifact.

Catalog 1.2.0 uses one sheet, `Catalogo`, with seven columns:
`Fuente`, `Origen`, `Parent`, `Type`, `Tipo ImpactU`, `Entidad`, and
`Tipo ImpactU Sugerido`. Its 982 rows contain the original 800 mappings,
two originally unmapped types, and 180 reviewed scraping classifications.
`socrata` identifies open-data classifications, `scraping` identifies CvLAC/
GrupLAC HTML, and `dumps` identifies the original `scienti_***_2024` databases.
Other providers retain their original source name as origin.

Scraping entries use source `minciencias`; `scienti` is reserved for dumps.
Exact source, parent, type and entity matches may combine origins with `;`.
The original 800 executable mappings are unchanged. The identity-only sidecar
`legacy_type_identities_v1.json` restores their original native strings,
including coded dumps paths; it contains no ImpactU assignments.

Edit `Tipo ImpactU Sugerido` to review assignments. For original rows, a
suggestion overrides `Tipo ImpactU`; for scraping rows it supplies the approved
target. Combined-origin rows retain the original assignment for Socrata and
use the suggestion for scraping. Duplicate identities, invalid origins and
noncanonical scraping targets stop generation. Rows without a target remain
under `unmapped`. The generator also supports the previous four-sheet format.

Generate or update the catalog:

```bash
python scripts/generate_impactu_type_catalog.py --catalog-version 1.2.0
```

Verify that the committed artifact matches the workbook:

```bash
python scripts/generate_impactu_type_catalog.py --catalog-version 1.2.0 --check
```

The JSON intentionally has no generation timestamp, so identical workbook
bytes and catalog version produce identical output bytes.

Runtime usage:

```python
from kahi_impactu_type_catalog import get_impactu_catalog

mapping = get_impactu_catalog().lookup("redcol", "td")
scraped = get_impactu_catalog().lookup_context(
    "minciencias", "scraping", "Articulos",
    "Producción bibliográfica - Artículo - Caso clínico",
)
```

The original `lookup(source, type)` and catalog length (800) retain their API.
Scraping mappings are indexed separately by parent/type, exposed through
`lookup_scraping` and `lookup_context`, and included in the artifact identity.
Yuku reads published scraping assignments from this shared artifact; its
local JSON preserves observations, examples and decision history.

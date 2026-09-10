"""Shared ImpactU type catalog."""

from kahi_impactu_type_catalog._version import __version__
from kahi_impactu_type_catalog.catalog import (
    CATALOG_ENTITIES,
    CATALOG_SCHEMA_VERSION,
    ImpactuTypeCatalog,
    exact_key,
    get_impactu_catalog,
)

__all__ = [
    "__version__",
    "CATALOG_ENTITIES",
    "CATALOG_SCHEMA_VERSION",
    "ImpactuTypeCatalog",
    "exact_key",
    "get_impactu_catalog",
]

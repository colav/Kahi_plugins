"""Runtime loader for the exact, versioned ImpactU type catalog."""

from __future__ import annotations

from functools import lru_cache
import html
import json
import pkgutil
import re
from typing import Any
import unicodedata


CATALOG_RESOURCE = "impactu_type_catalog_v1.json"
CATALOG_SCHEMA_VERSION = "impactu-type-routing-v1"
CATALOG_ENTITIES = frozenset({"works", "projects", "patents", "events"})


def exact_key(value: Any) -> str:
    """Normalize presentation only; accents and semantic words remain significant."""
    text = html.unescape(str(value or "")).replace("\xa0", " ")
    text = unicodedata.normalize("NFKC", text).casefold()
    return re.sub(r"\s+", " ", text).strip()


class ImpactuTypeCatalog:
    """Validated index of exact source-type mappings."""

    def __init__(self, payload):
        if payload.get("schema_version") != CATALOG_SCHEMA_VERSION:
            raise ValueError("unsupported ImpactU catalog schema")
        mappings = payload.get("mappings")
        if not isinstance(mappings, list) or not mappings:
            raise ValueError("ImpactU catalog has no mappings")
        source = payload.get("source") or {}
        declared_rows = int(source.get("rows") or 0)
        if declared_rows != len(mappings):
            raise ValueError("ImpactU catalog row count does not match its metadata")
        declared_entities = set(payload.get("entities") or [])
        if declared_entities != CATALOG_ENTITIES:
            raise ValueError("ImpactU catalog entity declaration is invalid")

        index = {}
        actual_types = set()
        for position, raw in enumerate(mappings, start=1):
            if not isinstance(raw, dict):
                raise ValueError(
                    "invalid ImpactU mapping at position {}".format(position)
                )
            mapping = {
                "source": str(raw.get("source") or "").strip(),
                "type": str(raw.get("type") or "").strip(),
                "type_impactu": str(raw.get("type_impactu") or "").strip(),
                "entity": str(raw.get("entity") or "").strip(),
            }
            if not all(mapping.values()) or mapping["entity"] not in CATALOG_ENTITIES:
                raise ValueError(
                    "invalid ImpactU mapping at position {}".format(position)
                )
            key = (exact_key(mapping["source"]), exact_key(mapping["type"]))
            if key in index:
                raise ValueError("duplicate ImpactU mapping: {!r}".format(key))
            index[key] = mapping
            actual_types.add(mapping["type_impactu"])

        if set(payload.get("impactu_types") or []) != actual_types:
            raise ValueError("ImpactU catalog type declaration is invalid")
        unmapped = payload.get("unmapped")
        if not isinstance(unmapped, list):
            raise ValueError("ImpactU catalog unmapped declaration is invalid")
        if int(source.get("unmapped_rows") or 0) != len(unmapped):
            raise ValueError("ImpactU catalog unmapped count does not match metadata")

        self.payload = payload
        self._index = index
        self.version = str(payload.get("catalog_version") or "")
        self.source_sha256 = str(source.get("sha256") or "")

    def lookup(self, source: Any, native_type: Any):
        mapping = self._index.get((exact_key(source), exact_key(native_type)))
        return dict(mapping) if mapping else None

    def classify_minciencias(self, product_class: Any, typology: Any):
        native_type = "{}: {}".format(
            str(product_class or "").strip(), str(typology or "").strip()
        )
        return self.lookup("minciencias", native_type)

    def __len__(self):
        return len(self._index)


def _read_catalog_payload():
    content = pkgutil.get_data(
        "kahi_impactu_type_catalog", "data/{}".format(CATALOG_RESOURCE)
    )
    if content is None:
        raise RuntimeError("bundled ImpactU catalog was not found")
    return json.loads(content.decode("utf-8"))


@lru_cache(maxsize=1)
def get_impactu_catalog():
    return ImpactuTypeCatalog(_read_catalog_payload())

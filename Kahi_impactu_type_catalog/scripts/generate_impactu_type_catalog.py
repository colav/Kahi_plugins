#!/usr/bin/env python3
"""Generate the canonical ImpactU type catalog from the maintained workbook."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
from pathlib import Path
import re
import unicodedata

from openpyxl import load_workbook


SCHEMA_VERSION = "impactu-type-routing-v1"
DEFAULT_CATALOG_VERSION = "1.1.0"
ENTITIES = frozenset({"works", "projects", "patents", "events"})
SHEET_SPECS = (
    ("ALL", None),
    ("COAR", "coar"),
    ("REDCOL", "redcol"),
    ("INFO-EU-REPO", "eu-repo"),
)
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKBOOK = (
    PACKAGE_ROOT
    / "kahi_impactu_type_catalog"
    / "data"
    / "Tipos_ImpactU_Definitivo.xlsx"
)
DEFAULT_OUTPUT = (
    PACKAGE_ROOT
    / "kahi_impactu_type_catalog"
    / "data"
    / "impactu_type_catalog_v1.json"
)


def display_text(value):
    """Return a stable textual representation without changing semantics."""
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


def exact_key(value):
    """Normalize presentation for duplicate detection and exact runtime lookup."""
    text = html.unescape(display_text(value))
    text = unicodedata.normalize("NFKC", text).casefold()
    return re.sub(r"\s+", " ", text).strip()


def first_header_index(headers, name, sheet_name):
    """Use the first matching header, matching pandas' duplicate-column behavior."""
    for index, value in enumerate(headers):
        if display_text(value) == name:
            return index
    raise ValueError("Sheet {!r} has no {!r} column".format(sheet_name, name))


def validate_catalog_version(value):
    value = display_text(value)
    if not re.fullmatch(r"0|[1-9]\d*\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)", value):
        raise ValueError("catalog version must use MAJOR.MINOR.PATCH")
    return value


def workbook_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def build_catalog(workbook_path, catalog_version=DEFAULT_CATALOG_VERSION):
    """Build a validated catalog with deterministic ordering."""
    workbook_path = Path(workbook_path).resolve()
    catalog_version = validate_catalog_version(catalog_version)
    workbook = load_workbook(workbook_path, read_only=True, data_only=True)
    mappings_by_key = {}
    unmapped = []
    sheet_metadata = []
    input_rows = 0
    duplicate_rows = 0

    try:
        for sheet_name, fixed_source in SHEET_SPECS:
            if sheet_name not in workbook.sheetnames:
                raise ValueError("Workbook has no {!r} sheet".format(sheet_name))
            sheet = workbook[sheet_name]
            headers = [cell.value for cell in sheet[1]]
            indexes = {
                field: first_header_index(headers, field, sheet_name)
                for field in ("Tipo", "Tipo ImpactU", "Entidad")
            }
            if fixed_source is None:
                indexes["Fuente"] = first_header_index(
                    headers, "Fuente", sheet_name
                )

            sheet_input_rows = 0
            sheet_mapping_rows = 0
            sheet_unmapped_rows = 0
            for row_number, row in enumerate(
                sheet.iter_rows(min_row=2, values_only=True), start=2
            ):
                native_type = display_text(row[indexes["Tipo"]])
                type_impactu = display_text(row[indexes["Tipo ImpactU"]])
                entity = display_text(row[indexes["Entidad"]])
                source = (
                    fixed_source
                    if fixed_source is not None
                    else display_text(row[indexes["Fuente"]])
                )

                if not any((native_type, type_impactu, entity)):
                    continue
                sheet_input_rows += 1
                input_rows += 1
                if not source or not native_type or not entity:
                    raise ValueError(
                        "Incomplete identity at {}!{}".format(
                            sheet_name, row_number
                        )
                    )
                if entity not in ENTITIES:
                    raise ValueError(
                        "Invalid entity {!r} at {}!{}".format(
                            entity, sheet_name, row_number
                        )
                    )
                if not type_impactu:
                    unmapped.append(
                        {
                            "sheet": sheet_name,
                            "row": row_number,
                            "source": source,
                            "type": native_type,
                            "entity": entity,
                            "reason": "missing_type_impactu",
                        }
                    )
                    sheet_unmapped_rows += 1
                    continue

                mapping = {
                    "source": source,
                    "type": native_type,
                    "type_impactu": type_impactu,
                    "entity": entity,
                }
                key = (exact_key(source), exact_key(native_type))
                previous = mappings_by_key.get(key)
                if previous is not None:
                    previous_target = (
                        exact_key(previous["type_impactu"]),
                        exact_key(previous["entity"]),
                    )
                    current_target = (
                        exact_key(type_impactu),
                        exact_key(entity),
                    )
                    if previous_target != current_target:
                        raise ValueError(
                            "Conflicting mapping for {!r} at {}!{}".format(
                                key, sheet_name, row_number
                            )
                        )
                    duplicate_rows += 1
                    continue
                mappings_by_key[key] = mapping
                sheet_mapping_rows += 1

            sheet_metadata.append(
                {
                    "name": sheet_name,
                    "source": fixed_source or "Fuente column",
                    "input_rows": sheet_input_rows,
                    "mapping_rows": sheet_mapping_rows,
                    "unmapped_rows": sheet_unmapped_rows,
                }
            )
    finally:
        workbook.close()

    mappings = sorted(
        mappings_by_key.values(),
        key=lambda item: (
            exact_key(item["source"]),
            exact_key(item["type"]),
            item["source"],
            item["type"],
        ),
    )
    unmapped.sort(
        key=lambda item: (
            exact_key(item["source"]),
            exact_key(item["type"]),
            item["sheet"],
            item["row"],
        )
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "catalog_version": catalog_version,
        "source": {
            "workbook": workbook_path.name,
            "sha256": workbook_sha256(workbook_path),
            "sheets": sheet_metadata,
            "input_rows": input_rows,
            "rows": len(mappings),
            "duplicates_removed": duplicate_rows,
            "unmapped_rows": len(unmapped),
        },
        "entities": sorted(ENTITIES),
        "impactu_types": sorted(
            {item["type_impactu"] for item in mappings}, key=exact_key
        ),
        "mappings": mappings,
        "unmapped": unmapped,
    }


def serialized_catalog(catalog):
    return (
        json.dumps(catalog, ensure_ascii=False, indent=2, sort_keys=False)
        + "\n"
    )


def write_catalog(catalog, output_path):
    output_path = Path(output_path).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(serialized_catalog(catalog), encoding="utf-8")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workbook", type=Path, default=DEFAULT_WORKBOOK)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--catalog-version", default=DEFAULT_CATALOG_VERSION
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail when the existing output differs from a fresh generation.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    catalog = build_catalog(args.workbook, args.catalog_version)
    rendered = serialized_catalog(catalog)
    if args.check:
        if not args.output.exists() or args.output.read_text(
            encoding="utf-8"
        ) != rendered:
            raise SystemExit("catalog is missing or out of date")
    else:
        write_catalog(catalog, args.output)
    print(
        "catalog_version={} mappings={} unmapped={} duplicates_removed={}".format(
            catalog["catalog_version"],
            catalog["source"]["rows"],
            catalog["source"]["unmapped_rows"],
            catalog["source"]["duplicates_removed"],
        )
    )


if __name__ == "__main__":
    main()

"""Deterministic Spanish normalization for affiliation addresses."""

from copy import deepcopy
import json
from pathlib import Path
import re
import unicodedata

from babel import Locale
from pymongo import UpdateOne


CATALOG_PATH = Path(__file__).with_name("data") / "geographic_aliases_es.json"
UNKNOWN_VALUES = {"", "no disponible", "not available", "unknown"}


def normalized_key(value):
    """Return an accent- and punctuation-insensitive lookup key."""
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(character for character in text if not unicodedata.combining(character))
    return re.sub(r"[^a-z0-9]+", " ", text.casefold()).strip()


class SpanishGeographicNormalizer:
    """Normalize geographic labels without changing the address schema."""

    def __init__(self, catalog_path=CATALOG_PATH):
        with open(catalog_path, encoding="utf-8") as catalog_file:
            self.catalog = json.load(catalog_file)

        spanish = Locale.parse("es").territories
        english = Locale.parse("en").territories
        self.country_aliases = {}
        self.country_names = {}
        for code, spanish_name in spanish.items():
            if len(code) != 2 or not code.isalpha():
                continue
            self.country_names[code] = spanish_name
            self.country_aliases[normalized_key(spanish_name)] = code
            english_name = english.get(code)
            if english_name:
                self.country_aliases[normalized_key(english_name)] = code

        self.special_country_aliases = {
            normalized_key(alias): value
            for alias, value in self.catalog["country_aliases"].items()
        }
        self.colombian_departments = {}
        for department in self.catalog["colombian_departments"]:
            key = normalized_key(department)
            self.colombian_departments[key] = department
            self.colombian_departments[f"{key} department"] = department
        for alias, department in self.catalog["colombian_department_aliases"].items():
            self.colombian_departments[normalized_key(alias)] = department

        self.city_aliases = {
            code: {
                normalized_key(alias): canonical
                for alias, canonical in aliases.items()
            }
            for code, aliases in self.catalog["city_aliases_by_country"].items()
        }

    def normalize_country(self, country, country_code=""):
        """Return ``(Spanish name, ISO-like code, recognized)``."""
        key = normalized_key(country)
        if key in self.special_country_aliases:
            value = self.special_country_aliases[key]
            return value["name"], value.get("code", ""), True
        code = self.country_aliases.get(key)
        if code:
            return self.country_names[code], code, True

        normalized_code = str(country_code or "").strip().upper()
        # Use the stored ISO code as a deterministic fallback for source aliases
        # not covered by CLDR (for example, Hong Kong or Myanmar).
        if normalized_code in self.country_names:
            return self.country_names[normalized_code], normalized_code, True
        if key in UNKNOWN_VALUES:
            return country, normalized_code, True
        return country, normalized_code, False

    def normalize_address(self, address):
        """Return a normalized address and any unresolved-value diagnostics."""
        normalized = deepcopy(address)
        country, country_code, country_recognized = self.normalize_country(
            address.get("country", ""), address.get("country_code", "")
        )
        normalized["country"] = country
        issues = []
        if not country_recognized and normalized_key(address.get("country")):
            issues.append(("country", address.get("country")))

        state = address.get("state", "")
        state_key = normalized_key(state)
        if country_code == "CO" and state_key not in UNKNOWN_VALUES:
            canonical_state = self.colombian_departments.get(state_key)
            if canonical_state:
                normalized["state"] = canonical_state
            elif state_key:
                issues.append(("colombian_state", state))

        city = address.get("city", "")
        city_key = normalized_key(city)
        canonical_city = self.city_aliases.get(country_code, {}).get(city_key)
        if canonical_city:
            normalized["city"] = canonical_city
        return normalized, issues

    def normalize_addresses(self, addresses):
        """Normalize an address list while preserving order and extra fields."""
        normalized_addresses = []
        issues = []
        changed_addresses = 0
        for address in addresses or []:
            if not isinstance(address, dict):
                normalized_addresses.append(address)
                continue
            normalized, address_issues = self.normalize_address(address)
            normalized_addresses.append(normalized)
            issues.extend(address_issues)
            if normalized != address:
                changed_addresses += 1
        return normalized_addresses, changed_addresses, issues


def normalize_affiliation_addresses(
    collection,
    dry_run=False,
    batch_size=1000,
    verbose=0,
    normalizer=None,
):
    """Normalize all affiliation addresses using bounded bulk updates."""
    if batch_size < 1:
        raise ValueError("geographic_batch_size must be greater than zero")
    normalizer = normalizer or SpanishGeographicNormalizer()
    report = {
        "documents_scanned": 0,
        "documents_changed": 0,
        "addresses_scanned": 0,
        "addresses_changed": 0,
        "unresolved_countries": 0,
        "unrecognized_colombian_states": 0,
        "issue_samples": [],
    }
    operations = []
    cursor = collection.find(
        {"addresses.0": {"$exists": True}}, {"addresses": 1}
    ).batch_size(batch_size)
    for affiliation in cursor:
        addresses = affiliation.get("addresses", [])
        normalized, changed_count, issues = normalizer.normalize_addresses(addresses)
        report["documents_scanned"] += 1
        report["addresses_scanned"] += len(addresses)
        report["addresses_changed"] += changed_count
        for issue_type, value in issues:
            if issue_type == "country":
                report["unresolved_countries"] += 1
            else:
                report["unrecognized_colombian_states"] += 1
            sample = {"affiliation_id": affiliation["_id"], issue_type: value}
            if len(report["issue_samples"]) < 20 and sample not in report["issue_samples"]:
                report["issue_samples"].append(sample)
        if not changed_count:
            continue
        report["documents_changed"] += 1
        if not dry_run:
            operations.append(UpdateOne(
                {"_id": affiliation["_id"]}, {"$set": {"addresses": normalized}}
            ))
            if len(operations) >= batch_size:
                collection.bulk_write(operations, ordered=False)
                operations = []
        if verbose and report["documents_changed"] % 1000 == 0:
            print(
                "INFO: geographic normalization: "
                f"{report['documents_changed']} affiliations changed"
            )
    if operations:
        collection.bulk_write(operations, ordered=False)
    return report

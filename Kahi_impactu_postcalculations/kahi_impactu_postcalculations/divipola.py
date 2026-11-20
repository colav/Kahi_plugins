"""Map Colombian addresses to DIVIPOLA and keep research groups in Colombia.

For every address in Colombia the municipality and department are identified in
DIVIPOLA (DANE) and the address gets the common name of the city, the name of
the department, their DANE codes (``city_code``, ``state_code``) and, when it
had none, the coordinates of the municipality. A department that contradicts a
municipality with a unique name is corrected ("Bogotá" in "Nariño").

A research group inherits one address per related institution, so groups ended
up with foreign cities. A group keeps only Colombian addresses, and each city
once. Addresses whose city cannot be placed in Colombia (empty, "No disponible",
a city of another country tagged as Colombian) are dropped when the group has a
located one; a group with none keeps a single address that says only Colombia.

A Colombian address whose city is not a Colombian municipality and whose
department is not a Colombian department ("Yawan, Badahsan, Colombia": a place
of another country resolved from a Colombian code) of an institution keeps only
the country: its city, department and coordinates are emptied.
"""

import json
from pathlib import Path

from pymongo import UpdateOne

from kahi_impactu_postcalculations.geographic_normalization import (
    CATALOG_PATH,
    UNKNOWN_VALUES,
    normalized_key,
)

DIVIPOLA_PATH = Path(__file__).with_name("data") / "divipola_municipios.json"
# Name people use, when it differs from the official one.
COMMON_CITY_NAMES = {
    "11001": "Bogotá",
    "76001": "Cali",
    "13001": "Cartagena",
    "54001": "Cúcuta",
}
# Other spellings found in the sources.
CITY_ALIASES = {
    "bogota": "11001", "bogota d c": "11001", "bogota dc": "11001",
    "santa fe de bogota": "11001", "santafe de bogota": "11001",
    "cali": "76001", "cartagena": "13001", "cucuta": "54001",
}
# Municipality meant when the name exists in several departments and none is given.
DEFAULT_CITY_CODES = {
    "armenia": "63001",    # Armenia, Quindío
    "rionegro": "05615",   # Rionegro, Antioquia
    "florencia": "18001",  # Florencia, Caquetá
}
# The sources write this country on addresses that had none.
MISASSIGNED_COUNTRY_CODES = {"AF"}
LOWERCASE_WORDS = {"de", "del", "la", "las", "los", "el", "y"}
GROUP_TYPE = "group"
# Empty values of the address schema, as the affiliation plugins write them.
EMPTY_ADDRESS = {"lat": "", "lng": "", "postcode": "", "state": "", "city": ""}


def _title(name):
    words = name.lower().split()
    return " ".join(
        word if position and word in LOWERCASE_WORDS else word[:1].upper() + word[1:]
        for position, word in enumerate(words)
    )


class Divipola:
    """Lookup of Colombian municipalities by name and department."""

    def __init__(self, path=DIVIPOLA_PATH, catalog_path=CATALOG_PATH):
        with open(path, encoding="utf-8") as handle:
            rows = json.load(handle)["municipalities"]
        with open(catalog_path, encoding="utf-8") as handle:
            catalog = json.load(handle)
        departments = {normalized_key(name): name for name in catalog["colombian_departments"]}
        self.by_code = {}
        self.by_name = {}
        self.state_codes = {}
        for row in rows:
            state = departments.get(normalized_key(row["state"]), _title(row["state"]))
            entry = {
                "city_code": row["city_code"],
                "city": COMMON_CITY_NAMES.get(row["city_code"], _title(row["city"])),
                "state_code": row["state_code"],
                "state": state,
                "lat": row["lat"],
                "lng": row["lng"],
            }
            self.by_code[row["city_code"]] = entry
            self.by_name.setdefault(normalized_key(row["city"]), []).append(entry)
            self.state_codes[normalized_key(row["state"])] = row["state_code"]
            self.state_codes[normalized_key(state)] = row["state_code"]
        for alias in catalog.get("colombian_department_aliases") or {}:
            target = normalized_key(catalog["colombian_department_aliases"][alias])
            if target in self.state_codes:
                self.state_codes[normalized_key(alias)] = self.state_codes[target]

    def state_code(self, state):
        key = normalized_key(state)
        if key.endswith(" department"):
            key = key[: -len(" department")]
        return self.state_codes.get(key)

    def find(self, city, state):
        """Return the municipality of ``city`` (in ``state`` when given), or ``None``."""
        key = normalized_key(city)
        if not key:
            return None
        if key in CITY_ALIASES:
            return self.by_code[CITY_ALIASES[key]]
        candidates = self.by_name.get(key) or []
        if not candidates:
            return None
        state_code = self.state_code(state)
        for candidate in candidates:
            if candidate["state_code"] == state_code:
                return candidate
        if len(candidates) == 1:
            return candidates[0]
        if key in DEFAULT_CITY_CODES:
            return self.by_code[DEFAULT_CITY_CODES[key]]
        return None


def _empty(value):
    return value is None or normalized_key(value) in UNKNOWN_VALUES


def normalize_colombian_addresses(addresses, divipola, is_group=False):
    """
    Return the addresses mapped to DIVIPOLA and what happened to them.

    Returns
    -------
    tuple
        ``(addresses, counts)``; ``counts`` has ``mapped``, ``unresolved``
        (Colombian city not found or ambiguous), ``country_fixed``,
        ``foreign_removed`` and ``duplicates_removed``.
    """
    counts = {"mapped": 0, "unresolved": 0, "country_fixed": 0, "foreign_removed": 0,
              "duplicates_removed": 0, "unlocated_removed": 0, "groups_without_city": 0,
              "foreign_place_cleared": 0}
    result = []
    for address in addresses or []:
        if not isinstance(address, dict):
            result.append(address)
            continue
        address = dict(address)
        code = address.get("country_code") or ""
        found = None
        if code in MISASSIGNED_COUNTRY_CODES or (is_group and not code):
            # The sources write that country on addresses that had none, and a group is
            # Colombian: when the city is a Colombian municipality the address is in Colombia.
            found = divipola.find(address.get("city"), address.get("state"))
            if found:
                address["country"], address["country_code"] = "Colombia", "CO"
                code = "CO"
                counts["country_fixed"] += 1
        if is_group and code and code != "CO":
            counts["foreign_removed"] += 1
            continue
        if code == "CO":
            found = found or divipola.find(address.get("city"), address.get("state"))
            if found:
                address.update(city=found["city"], state=found["state"],
                               city_code=found["city_code"], state_code=found["state_code"])
                if _empty(address.get("lat")) or _empty(address.get("lng")):
                    address["lat"], address["lng"] = found["lat"], found["lng"]
                counts["mapped"] += 1
            elif not _empty(address.get("city")):
                state = address.get("state")
                # Groups drop such addresses below; other affiliations keep the country.
                if not is_group and not _empty(state) and divipola.state_code(state) is None:
                    # A place of another country under a Colombian address: keep only the country.
                    address.update(city="", state="", lat="", lng="")
                    counts["foreign_place_cleared"] += 1
                else:
                    counts["unresolved"] += 1
        result.append(address)
    if is_group:
        unique, seen = [], {}
        for address in result:
            if not isinstance(address, dict):
                unique.append(address)
                continue
            key = (address.get("country_code") or "",
                   address.get("city_code") or normalized_key(address.get("city")),
                   address.get("state_code") or normalized_key(address.get("state")))
            if key in seen:
                for field, value in address.items():
                    if _empty(seen[key].get(field)) and not _empty(value):
                        seen[key][field] = value
                counts["duplicates_removed"] += 1
                continue
            seen[key] = address
            unique.append(address)
        located = [address for address in unique
                   if isinstance(address, dict) and address.get("city_code")]
        if located:
            counts["unlocated_removed"] = len(unique) - len(located)
            result = located
        elif unique or counts["foreign_removed"]:
            # Nothing places the group in a city: say only that it is in Colombia.
            counts["unlocated_removed"] = max(len(unique) - 1, 0)
            counts["groups_without_city"] = 1
            result = [{**EMPTY_ADDRESS, "country": "Colombia", "country_code": "CO"}]
        else:
            result = unique
        counts["unresolved"] = 0
    return result, counts


def map_affiliation_addresses(collection, dry_run=False, batch_size=1000, divipola=None):
    """Apply :func:`normalize_colombian_addresses` to every affiliation."""
    divipola = divipola or Divipola()
    report = {"documents_scanned": 0, "documents_changed": 0, "mapped": 0, "unresolved": 0,
              "country_fixed": 0, "foreign_removed": 0, "duplicates_removed": 0,
              "unlocated_removed": 0, "groups_without_city": 0, "foreign_place_cleared": 0,
              "unresolved_samples": []}
    operations = []
    cursor = collection.find({"addresses.0": {"$exists": True}},
                             {"addresses": 1, "types.type": 1}).batch_size(batch_size)
    for affiliation in cursor:
        addresses = affiliation.get("addresses") or []
        is_group = any(item.get("type") == GROUP_TYPE for item in affiliation.get("types") or [])
        normalized, counts = normalize_colombian_addresses(addresses, divipola, is_group)
        report["documents_scanned"] += 1
        for key, value in counts.items():
            report[key] += value
        if counts["unresolved"] and len(report["unresolved_samples"]) < 20:
            report["unresolved_samples"].extend(
                {"affiliation_id": affiliation["_id"], "city": item.get("city"),
                 "state": item.get("state")}
                for item in normalized
                if isinstance(item, dict) and item.get("country_code") == "CO"
                and not item.get("city_code") and not _empty(item.get("city")))
        if normalized == addresses:
            continue
        report["documents_changed"] += 1
        if not dry_run:
            operations.append(UpdateOne(
                {"_id": affiliation["_id"]}, {"$set": {"addresses": normalized}}))
            if len(operations) >= batch_size:
                collection.bulk_write(operations, ordered=False)
                operations = []
    if operations:
        collection.bulk_write(operations, ordered=False)
    report["unresolved_samples"] = report["unresolved_samples"][:20]
    return report

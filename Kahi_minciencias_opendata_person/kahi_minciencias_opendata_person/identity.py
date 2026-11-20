"""Local, deterministic identity checks for the snapshot importer."""

import re
import unicodedata

from thefuzz import fuzz
from kahi_impactu_utils.Utils import parse_orcid_id_from_url


def normalized_name(value):
    value = unicodedata.normalize("NFKD", str(value or ""))
    value = "".join(char for char in value if not unicodedata.combining(char))
    return " ".join(re.sub(r"[^a-z0-9]+", " ", value.casefold()).split())


def canonical_orcid(value):
    if not isinstance(value, str):
        return None
    url = parse_orcid_id_from_url(value)
    if not url or not re.fullmatch(r"https://orcid\.org/\d{4}-\d{4}-\d{4}-\d{3}[\dX]", url):
        return None
    digits = url.rsplit("/", 1)[-1].replace("-", "")
    total = 0
    for digit in digits[:15]:
        total = (total + int(digit)) * 2
    check = (12 - total % 11) % 11
    if digits[-1] != ("X" if check == 10 else str(check)):
        return None
    return url.rsplit("/", 1)[-1]


def person_orcids(document):
    return {
        identifier
        for item in document.get("external_ids", [])
        if str(item.get("source") or "").casefold() == "orcid"
        for identifier in [canonical_orcid(item.get("id"))]
        if identifier
    }


def orcid_variants(identifier):
    identifiers = {identifier, identifier.replace("-", "")}
    if identifier.endswith("X"):
        identifiers |= {item[:-1] + "x" for item in list(identifiers)}
    return {
        value
        for item in identifiers
        for value in (item, *(prefix + item + suffix
                              for prefix in ("https://orcid.org/", "http://orcid.org/",
                                             "https://www.orcid.org/", "http://www.orcid.org/")
                              for suffix in ("", "/")))
    }


def compatible_names(left, right):
    a = normalized_name(left.get("full_name"))
    b = normalized_name(right.get("full_name"))
    if not a or not b:
        return False
    a_tokens, b_tokens = a.split(), b.split()
    if sorted(a_tokens) == sorted(b_tokens):
        return True
    if min(len(a_tokens), len(b_tokens)) < 2 or fuzz.token_sort_ratio(a, b) < 92:
        return False
    # Reject contradictory complete components even in otherwise long names.
    for field in ("first_names", "last_names"):
        first = {word for item in left.get(field, []) for word in normalized_name(item).split()}
        second = {word for item in right.get(field, []) for word in normalized_name(item).split()}
        first, second = first - second, second - first
        if first and second and any(
                max(fuzz.ratio(word, other) for other in second) < 85
                for word in first):
            return False
    return bool(set(a_tokens) & set(b_tokens))

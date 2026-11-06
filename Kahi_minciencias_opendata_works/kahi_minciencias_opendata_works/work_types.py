"""Conservative book/chapter identity checks without changing work models."""

from functools import lru_cache
from hashlib import sha256
import json

from kahi_impactu_utils.Work import normalize_work_title


_BOOK = frozenset({"libro", "book", "edited book", "monograph", "whole book"})
_CHAPTER = frozenset({
    "capitulo", "capitulo de libro", "capitulo en libro", "capitulos de libros",
    "book chapter", "bookchapter", "book section", "book part", "chapter",
    "chapter in book",
})


def _canonical_family(label):
    key = normalize_work_title(label)
    if key in _BOOK:
        return "book"
    if key in _CHAPTER:
        return "chapter"
    return None


@lru_cache(maxsize=1)
def _catalog_aliases():
    """Use curated exact labels; 'libro ... por capítulos' can be a book."""
    aliases = {}
    try:
        from kahi_impactu_type_catalog.catalog import get_impactu_catalog
    except ModuleNotFoundError as error:
        if error.name not in {"kahi_impactu_type_catalog", "kahi_impactu_type_catalog.catalog"}:
            raise
        return aliases
    catalog = get_impactu_catalog().payload
    for row in [*catalog.get("mappings", []), *catalog.get("scraping_mappings", [])]:
        family = _canonical_family(row.get("type_impactu", ""))
        aliases.setdefault(normalize_work_title(row.get("type", "")), set()).add(family or "other")
    # Source-independent aliases are safe only when the curated label never
    # means a non-book publication in another provider/context.
    return {label: families for label, families in aliases.items() if "other" not in families}


def book_families(document):
    families = set()
    aliases = _catalog_aliases()
    for entry in document.get("types") or []:
        label = entry.get("type", "") if isinstance(entry, dict) else entry
        if not isinstance(label, str):
            continue
        direct = _canonical_family(label)
        if direct:
            families.add(direct)
        else:
            families.update(aliases.get(normalize_work_title(label), ()))
    return frozenset(families)


def book_types_compatible(incoming, owner):
    left, right = book_families(incoming), book_families(owner)
    if not left and not right:
        # No identifiable book/chapter evidence: retain the other existing
        # identity checks, without guessing a family from title or ISBN.
        return True
    # A known book family must agree on both sides. Missing or contradictory
    # classification cannot bypass the veto through another matching route.
    return len(left) == len(right) == 1 and left == right


@lru_cache(maxsize=1)
def book_type_policy():
    aliases = {label: sorted(families) for label, families in _catalog_aliases().items()}
    digest = sha256(json.dumps(aliases, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return {"book_type_policy": "book-chapter-veto-v1", "book_type_aliases_digest": digest}

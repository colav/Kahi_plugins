"""Decide which works keep a shared DOI and which person keeps a shared ORCID iD.

The decision uses what the identifier resolver stored for the DOI (existence,
type, titles and authors as registered) and is deliberately conservative: a
work loses the DOI only when the DOI does not exist, identifies a container
(journal, issue, proceedings volume) or is registered for a different work
with no author in common. Anything else is left untouched and reported.
"""

import html
import re

from rapidfuzz import fuzz
from unidecode import unidecode

TITLE_MATCH = 90
# Below a match but too close to call it another work.
TITLE_SIMILAR = 70
MIN_MAIN_TITLE_CHARS = 12
MAX_NAMES = 60
# The DOI identifies a container, never one of the works inside it.
# The work that keeps a DOI several works own: best source first.
SOURCE_PRIORITY = ("openalex", "ciarp", "scienti", "minciencias", "scholar", "dspace")
# Reasons a work is left untouched that a single owner of the title settles.
SETTLED_BY_OWNER = ("shared_author", "no_author_evidence")
CONTAINER_TYPES = {
    "journal", "journal-issue", "journal-volume", "proceedings",
    "proceedings-series", "book-series", "book-set",
}
# Without registered authors these are edited volumes or proceedings.
BOOK_TYPES = {"book", "edited-book", "monograph", "reference-book"}

TAG_RE = re.compile(r"<[^>]+>")
LATEX_RE = re.compile(r"\\[a-zA-Z]+|[${}^_]")
SUBTITLE_RE = re.compile(r"\s*(?::|\s[-–—]\s|\.\s|\?\s|\()")


def _plain(text):
    return LATEX_RE.sub(" ", TAG_RE.sub(" ", html.unescape(str(text or ""))))


def normalize_title(text):
    """Title without markup, accents, case or punctuation."""
    return " ".join(re.sub(r"[^a-z0-9 ]", " ", unidecode(_plain(text)).lower()).split())


def main_title(text):
    """The part of a title before its subtitle."""
    return normalize_title(SUBTITLE_RE.split(_plain(text).strip(), maxsplit=1)[0])


def title_score(work_titles, registry_titles):
    """Best 0-100 similarity between the titles of a work and of the registry."""
    best = 0
    for work_title in work_titles:
        left = normalize_title(work_title)
        if not left:
            continue
        for registry_title in registry_titles:
            right = normalize_title(registry_title)
            if not right:
                continue
            score = fuzz.token_sort_ratio(left, right)
            if min(len(left), len(right)) >= MIN_MAIN_TITLE_CHARS:
                left_main, right_main = main_title(work_title), main_title(registry_title)
                if left_main == right or right_main == left or (
                        left_main == right_main and len(left_main) >= MIN_MAIN_TITLE_CHARS):
                    score = max(score, 95)
            best = max(best, score)
    return round(best)


def name_tokens(name):
    return re.sub(r"[^a-z ]", " ", unidecode(str(name or "")).lower()).split()


def same_author(left, right):
    """Two names share two words, or one word and an initial of the rest."""
    left_tokens, right_tokens = name_tokens(left), name_tokens(right)
    common = ({token for token in left_tokens if len(token) >= 3}
              & {token for token in right_tokens if len(token) >= 3})
    if len(common) >= 2:
        return True
    if len(common) == 1:
        left_rest = [token for token in left_tokens if token not in common]
        right_rest = [token for token in right_tokens if token not in common]
        return any(a[0] == b[0] for a in left_rest for b in right_rest)
    return False


def shares_author(left_names, right_names):
    return any(same_author(left, right)
               for left in left_names[:MAX_NAMES] for right in right_names[:MAX_NAMES])


def work_titles(work):
    return [item.get("title") for item in work.get("titles") or [] if item.get("title")]


def work_authors(work):
    return [author.get("full_name") for author in work.get("authors") or []
            if author.get("full_name")]


def work_sources(work):
    return {item.get("source") for item in work.get("updated") or []} - {"disputes", None}


def owner_rank(work):
    """Sort key of the owners of a DOI: best source, then more sources, then older id."""
    sources = work_sources(work)
    best = min((SOURCE_PRIORITY.index(source) for source in sources
                if source in SOURCE_PRIORITY), default=len(SOURCE_PRIORITY))
    return best, -len(sources), str(work.get("_id"))


def settle_owners(works, decisions):
    """
    Leave one owner per DOI once the title says who owns it.

    The best owner (see ``owner_rank``) keeps the DOI; any other work with the
    registered title loses it (works are never merged). A work with another
    title that was left untouched for lack of author evidence loses the DOI
    too: the title is enough. Similar titles (70-89) stay untouched.
    """
    owners = [index for index, decision in enumerate(decisions)
              if decision["action"] == "kept"]
    if not owners:
        return decisions
    winner = min(owners, key=lambda index: owner_rank(works[index]))
    for index, decision in enumerate(decisions):
        if index == winner:
            continue
        if decision["action"] == "kept":
            decision.update(action="removed", reason="lower_priority_source")
        elif decision["action"] == "ambiguous" and decision["reason"] in SETTLED_BY_OWNER:
            decision.update(action="removed", reason="title_owner")
    return decisions


def registry_titles(registry):
    titles = [title for title in registry.get("titles") or [] if title]
    return titles + [f"{title}: {subtitle}" for title in titles
                     for subtitle in registry.get("subtitles") or [] if subtitle]


def decide(registry, works):
    """
    Decide, for every work that holds a shared DOI, whether it keeps it.

    Parameters
    ----------
    registry : dict
        Resolver document of the DOI with status ``ok`` or ``not_found``.
    works : list of dict
        Kahi works that have that DOI (``titles`` and ``authors`` are read).

    Returns
    -------
    list of dict
        One per work, in the same order: ``action`` ("kept", "removed" or
        "ambiguous"), ``reason`` and ``title_score``.
    """
    if registry.get("status") == "not_found":
        return [{"action": "removed", "reason": "doi_not_found", "title_score": 0}
                for _ in works]
    titles = registry_titles(registry)
    scores = [title_score(work_titles(work), titles) for work in works]
    registered_authors = [name for name in registry.get("authors") or [] if name]
    doi_type = str(registry.get("type") or "").lower()
    container = doi_type in CONTAINER_TYPES or (
        doi_type in BOOK_TYPES and not registered_authors)
    # The registry lists at most some of the authors of a large collaboration.
    authors_complete = int(registry.get("author_count") or 0) <= len(registered_authors)
    reference_authors = registered_authors or [
        name for work, score in zip(works, scores) if score >= TITLE_MATCH
        for name in work_authors(work)
    ]
    decisions = []
    for work, score in zip(works, scores):
        authors = work_authors(work)
        if score >= TITLE_MATCH:
            action, reason = "kept", "title_match"
        elif container:
            action, reason = "removed", "container_doi"
        elif not titles and not reference_authors:
            action, reason = "ambiguous", "no_registry_metadata"
        elif score >= TITLE_SIMILAR:
            action, reason = "ambiguous", "similar_title"
        elif authors and reference_authors and shares_author(authors, reference_authors):
            action, reason = "ambiguous", "shared_author"
        elif not authors or not reference_authors or not authors_complete:
            action, reason = "ambiguous", "no_author_evidence"
        else:
            action, reason = "removed", "other_work_doi"
        decisions.append({"action": action, "reason": reason, "title_score": score})
    return settle_owners(works, decisions)


def _registry_names(registry):
    return [registry.get("given_names"), registry.get("family_name"),
            registry.get("credit_name")] + list(registry.get("other_names") or [])


def orcid_score(full_name, registry):
    """
    Compare a person's name with the public names of an ORCID record.

    Returns
    -------
    dict
        ``score`` (registered words found in the name, plus one half when a
        remaining word starts with a registered initial), ``has_given``,
        ``has_family`` and ``extra`` (words of the name the record does not have).
    """
    tokens = [token for name in _registry_names(registry) for token in name_tokens(name)]
    words = {token for token in tokens if len(token) >= 3}
    initials = {token for token in tokens if len(token) == 1}
    given = {token for token in name_tokens(registry.get("given_names")) if len(token) >= 3}
    family = {token for token in name_tokens(registry.get("family_name")) if len(token) >= 3}
    person = {token for token in name_tokens(full_name) if len(token) >= 3}
    extra = person - words
    by_initial = {token for token in extra if token[0] in initials}
    return {
        "score": len(person & words) + (0.5 if by_initial else 0),
        "has_given": bool(person & given) or bool(by_initial) or not given,
        "has_family": bool(person & family) or not family,
        "extra": sorted(extra - by_initial),
    }


def decide_orcid(registry, people):
    """
    Decide which of the people that hold an ORCID iD keeps it.

    The owner is the only person with the most registered words in the name
    (at least two, with a given name and a family name). Another person loses
    the iD only when the name has a word the record does not have; a name that
    fits inside the registered one may be a second profile of the owner and is
    left untouched.

    Parameters
    ----------
    registry : dict
        Resolver document of the ORCID iD.
    people : list of dict
        Kahi people that hold it (``full_name`` is read).

    Returns
    -------
    list of dict
        One per person, in the same order: ``action``, ``reason`` and ``score``.
    """
    if registry.get("status") != "ok":
        return [{"action": "ambiguous", "reason": "orcid_not_found", "score": 0}
                for _ in people]
    if not any(name_tokens(name) for name in _registry_names(registry)):
        return [{"action": "ambiguous", "reason": "no_registry_name", "score": 0}
                for _ in people]
    scores = [orcid_score(person.get("full_name"), registry) for person in people]
    best = max(item["score"] for item in scores)
    leaders = [item for item in scores if item["score"] == best]
    owner = leaders[0] if len(leaders) == 1 else None
    if owner and not (best >= 2 and owner["has_given"] and owner["has_family"]):
        owner = None
    decisions = []
    for item in scores:
        if owner is None:
            action, reason = "ambiguous", "tie" if len(leaders) > 1 else "no_owner"
        elif item is owner:
            action, reason = "kept", "name_match"
        elif item["extra"]:
            action, reason = "removed", "other_person_orcid"
        else:
            action, reason = "ambiguous", "possible_duplicate_person"
        decisions.append({"action": action, "reason": reason, "score": item["score"]})
    return decisions

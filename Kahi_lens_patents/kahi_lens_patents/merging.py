"""Decide which Kahi patents matched to the same Lens record are one patent.

MinCiencias publishes a patent once per CvLAC that declares it (each with its
owner as the only author) and once per official measurement record (no
authors, only the owner's COD_RH). Sharing a Lens record is not enough to
merge them, because a title-only match can join different patents; this
module asks for hard evidence:

- the patent matched Lens by number or family, or
- one of its authors is an inventor of the Lens record, or
- it shares an inventor (or its owner) with another patent of the group.
"""

from collections import defaultdict

from kahi_lens_patents.matching import normalize_text

HARD_RULES = ("number", "family")
PARTICLES = {"del", "los", "las", "von", "van", "der", "dos", "das"}


def inventor_tokens(name):
    """Words of a name that can identify a person (no initials or particles)."""
    return frozenset(
        token for token in normalize_text(name).split()
        if len(token) >= 3 and token not in PARTICLES
    )


def same_inventor(left, right):
    """Two names agree on three words, or the shorter one is contained in the other."""
    shared = left & right
    if len(shared) >= 3:
        return True
    return len(shared) >= 2 and (left <= right or right <= left)


def is_lens_inventor(names, lens_inventors):
    """One of ``names`` is an inventor of the Lens record.

    Lens writes ``SURNAME SURNAME GIVEN``: the first surname has to agree.
    """
    for inventor in lens_inventors:
        tokens = inventor_tokens(inventor)
        first = normalize_text(inventor).split()[:1]
        for name in names:
            shared = name & tokens
            if len(shared) >= 3 or (len(shared) >= 2 and first and first[0] in shared):
                return True
    return False


def clusters(members, lens_inventors):
    """
    Group the patents of one Lens record that are the same patent.

    Parameters
    ----------
    members : list of dict
        One per Kahi patent: ``_id``, ``matched_by``, ``names`` (author full
        names), ``author_ids`` and ``owner_ids`` (COD_RH from the snapshot).
    lens_inventors : list of str
        Inventor names of the Lens record.

    Returns
    -------
    list of dict
        One per group of two or more patents: ``ids`` and ``evidence`` (per
        patent, why it belongs to the group).
    """
    parent = {member["_id"]: member["_id"] for member in members}
    evidence = defaultdict(set)

    def find(item):
        while parent[item] != item:
            parent[item] = parent[parent[item]]
            item = parent[item]
        return item

    def union(left, right):
        parent[find(left)] = find(right)

    tokens = {member["_id"]: [inventor_tokens(name) for name in member.get("names") or []]
              for member in members}
    tokens = {key: [name for name in names if name] for key, names in tokens.items()}
    anchored = []
    for member in members:
        if member.get("matched_by") in HARD_RULES:
            evidence[member["_id"]].add(member["matched_by"])
            anchored.append(member["_id"])
        elif is_lens_inventor(tokens[member["_id"]], lens_inventors):
            evidence[member["_id"]].add("lens_inventor")
            anchored.append(member["_id"])
    for other in anchored[1:]:
        union(anchored[0], other)
    for position, left in enumerate(members):
        for right in members[position + 1:]:
            reason = None
            if any(same_inventor(a, b) for a in tokens[left["_id"]] for b in tokens[right["_id"]]):
                reason = "shared_inventor"
            elif (set(left.get("owner_ids") or []) & set(right.get("author_ids") or [])
                  or set(right.get("owner_ids") or []) & set(left.get("author_ids") or [])
                  or set(left.get("owner_ids") or []) & set(right.get("owner_ids") or [])):
                reason = "shared_owner"
            if reason:
                evidence[left["_id"]].add(reason)
                evidence[right["_id"]].add(reason)
                union(left["_id"], right["_id"])
    groups = defaultdict(list)
    for member in members:
        groups[find(member["_id"])].append(member["_id"])
    return [
        {"ids": ids, "evidence": {item: sorted(evidence[item]) for item in ids}}
        for ids in groups.values() if len(ids) > 1
    ]


def choose_survivor(members):
    """The patent that stays: matched by number or family, then most authors, then lowest id."""
    def rank(member):
        return (member.get("matched_by") not in HARD_RULES,
                -len(member.get("names") or []), str(member["_id"]))
    return min(members, key=rank)["_id"]


def _unique(items, key):
    seen, result = set(), []
    for item in items:
        marker = key(item)
        if marker in seen:
            continue
        seen.add(marker)
        result.append(item)
    return result


def _author_key(author):
    return str(author.get("id") or "") or inventor_tokens(author.get("full_name"))


def merge_documents(survivor, others):
    """Fields of ``survivor`` after absorbing the ``others`` (same patent)."""
    documents = [survivor] + list(others)

    def gather(field):
        return [item for document in documents for item in document.get(field) or []]

    def lens_last(items):
        # Same order the enrichment writes, so a second run finds nothing to change.
        return ([item for item in items if item.get("source") != "lens"]
                + [item for item in items if item.get("source") == "lens"])

    authors = _unique(gather("authors"), _author_key)
    return {
        "titles": _unique(gather("titles"), lambda item: normalize_text(item.get("title"))),
        "types": _unique(gather("types"), repr),
        "external_ids": lens_last(_unique(gather("external_ids"), repr)),
        "external_urls": lens_last(_unique(gather("external_urls"), lambda item: item.get("url"))),
        "abstracts": lens_last(_unique(gather("abstracts"),
                                       lambda item: (item.get("source"), item.get("lang")))),
        "authors": authors,
        "author_count": len(authors),
        "groups": _unique(gather("groups"), lambda item: item.get("id") or item.get("name")),
        "ranking": _unique(gather("ranking"), repr),
        "updated": _unique(gather("updated"), lambda item: item.get("source")),
    }

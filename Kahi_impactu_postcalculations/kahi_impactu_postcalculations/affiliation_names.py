"""Show every institution with its Spanish name wherever the name is copied.

Each plugin copies an affiliation name when it inserts a work, a person or a
relation, and each one picks it differently (the first name, the last English
or Spanish one...). This stage rewrites those copies with one rule:

1. the ROR name in Spanish;
2. else the SNIES name in Spanish;
3. else, for institutions, the MinCiencias or staff name in Spanish;
4. else the copy is left as it is.

Wikidata and ScienTI names are not used: Wikidata translates and ScienTI tags
upper-case and English names as Spanish. Groups, departments and faculties
keep their own names; only a ROR or SNIES name can replace them.
"""

from pymongo import UpdateOne

UNIT_TYPES = {"group", "department", "faculty"}
INSTITUTION_SOURCES = ("minciencias", "staff")
BATCH_SIZE = 1000
# Collections whose authors carry a copy of the affiliation name.
AUTHOR_COLLECTIONS = ("works", "patents", "projects", "events")


def _named(names, source):
    for item in names or []:
        if item.get("source") == source and item.get("lang") == "es" and item.get("name"):
            return item["name"]
    return None


def spanish_name(affiliation):
    """Spanish name of an affiliation by the rule above, or ``None`` when there is none."""
    names = affiliation.get("names") or []
    name = _named(names, "ror") or _named(names, "snies")
    if name:
        return name
    types = {item.get("type") for item in affiliation.get("types") or []}
    if types & UNIT_TYPES:
        return None
    for source in INSTITUTION_SOURCES:
        name = _named(names, source)
        if name:
            return name
    return None


def spanish_names(affiliations):
    """Return ``{affiliation id: Spanish name}`` for the affiliations that have one."""
    names = {}
    for affiliation in affiliations.find({"names.lang": "es"}, {"names": 1, "types.type": 1}):
        name = spanish_name(affiliation)
        if name:
            names[affiliation["_id"]] = name
    return names


def _fixes(items, path, names):
    """``{dotted path: name}`` for the entries of ``items`` whose name is not the Spanish one."""
    fixes = {}
    for position, item in enumerate(items or []):
        if not isinstance(item, dict):
            continue
        name = names.get(item.get("id"))
        if name and item.get("name") != name:
            fixes[f"{path}.{position}.name"] = name
    return fixes


def _rewrite(collection, query, projection, document_fixes):
    documents = entries = 0
    operations = []
    for document in collection.find(query, projection):
        fixes = document_fixes(document)
        if not fixes:
            continue
        documents += 1
        entries += len(fixes)
        operations.append(UpdateOne({"_id": document["_id"]}, {"$set": fixes}))
        if len(operations) >= BATCH_SIZE:
            collection.bulk_write(operations, ordered=False)
            operations = []
    if operations:
        collection.bulk_write(operations, ordered=False)
    return {"documents": documents, "names": entries}


def normalize_affiliation_names(db, author_collections=AUTHOR_COLLECTIONS):
    """
    Rewrite the copied affiliation names with the Spanish name of the affiliation.

    Covers ``affiliations.relations``, ``person.affiliations`` and the author
    affiliations of works, patents, projects and events. Running it again
    changes nothing.

    Returns
    -------
    dict
        Per collection: documents and names changed.
    """
    names = spanish_names(db["affiliations"])
    report = {"spanish_names": len(names)}
    report["affiliations"] = _rewrite(
        db["affiliations"], {"relations.0": {"$exists": True}},
        {"relations.id": 1, "relations.name": 1},
        lambda document: _fixes(document.get("relations"), "relations", names))
    report["person"] = _rewrite(
        db["person"], {"affiliations.0": {"$exists": True}},
        {"affiliations.id": 1, "affiliations.name": 1},
        lambda document: _fixes(document.get("affiliations"), "affiliations", names))

    def author_fixes(document):
        fixes = {}
        for position, author in enumerate(document.get("authors") or []):
            if isinstance(author, dict):
                fixes.update(_fixes(
                    author.get("affiliations"), f"authors.{position}.affiliations", names))
        return fixes

    existing = set(db.list_collection_names())
    for collection_name in author_collections:
        if collection_name not in existing:
            continue
        report[collection_name] = _rewrite(
            db[collection_name], {"authors.affiliations.0": {"$exists": True}},
            {"authors.affiliations.id": 1, "authors.affiliations.name": 1}, author_fixes)
    return report

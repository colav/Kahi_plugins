"""Bring the ImpactU news collections into the release without identity documents.

The news collections (``impactu_news``) link each professor to the people of
Kahi through ``professor_id``, an identity document number. A release cannot
carry identity documents, so this stage copies the three collections into the
release database and, in the professors collection, replaces ``professor_id``
with ``person_id``: the ``_id`` of the release person that holds that document
in ``external_ids``. That ``_id`` already follows the COD_RH, ORCID, ``_id_old``
order of the persistent person identifiers.

* A professor with no person, or whose document is held by several people with
  different ``_id``, keeps its name and news with ``person_id: None``.
* The source database is only read. The release copies are rebuilt on every
  run, so the stage can be repeated, but it must run while ``person`` still has
  the identity documents.
"""

from pymongo import ASCENDING

NEWS_COLLECTIONS = (
    "news_media_collection",
    "news_professors_collection",
    "news_urls_collection",
)
PROFESSORS_COLLECTION = "news_professors_collection"
IDENTITY_FIELD = "professor_id"
IDENTITY_SOURCES = (
    "Cédula de Ciudadanía",
    "Cédula de Extranjería",
    "Passport",
    "Pasaporte",
)
BATCH_SIZE = 1000


def _document_number(value):
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip()
    return text or None


def identity_index(person_collection):
    """Return ``{document number: {person _id, ...}}`` for the release people."""
    index = {}
    query = {"external_ids.source": {"$in": list(IDENTITY_SOURCES)}}
    for person in person_collection.find(query, {"external_ids": 1}):
        for item in person.get("external_ids") or []:
            if not isinstance(item, dict) or item.get("source") not in IDENTITY_SOURCES:
                continue
            number = _document_number(item.get("id"))
            if number:
                index.setdefault(number, set()).add(person["_id"])
    return index


def _replace_collection(target, documents):
    target.drop()
    batch, written = [], 0
    for document in documents:
        batch.append(document)
        if len(batch) >= BATCH_SIZE:
            target.insert_many(batch, ordered=True)
            written += len(batch)
            batch = []
    if batch:
        target.insert_many(batch, ordered=True)
        written += len(batch)
    return written


def _has_news(professor):
    return bool(professor.get("classified_urls_ids"))


def _linked_professors(source, index, report):
    for professor in source.find({}):
        number = _document_number(professor.pop(IDENTITY_FIELD, None))
        people = index.get(number, set()) if number else set()
        if len(people) == 1:
            professor["person_id"] = next(iter(people))
            report["enlazados"] += 1
            report["con_noticias_enlazados"] += _has_news(professor)
        else:
            professor["person_id"] = None
            report["ambiguos" if len(people) > 1 else "sin_persona"] += 1
            if len(people) > 1:
                report["ejemplos_ambiguos"].append(professor["_id"])
        report["con_noticias"] += _has_news(professor)
        yield professor


def verify_news(news_db, db, report, numbers):
    """Raise when the release copies lost documents or still carry identity documents."""
    for name in NEWS_COLLECTIONS:
        expected = news_db[name].count_documents({})
        copied = db[name].count_documents({})
        if expected != copied:
            raise RuntimeError(f"News count mismatch for {name}: {expected} != {copied}")
    professors = db[PROFESSORS_COLLECTION]
    if professors.count_documents({IDENTITY_FIELD: {"$exists": True}}):
        raise RuntimeError(f"{PROFESSORS_COLLECTION} still has {IDENTITY_FIELD}")
    for name in NEWS_COLLECTIONS:
        for document in db[name].find({}):
            leaked = {
                key for key, value in document.items()
                if key != "_id" and _document_number(value) in numbers
            }
            if leaked:
                raise RuntimeError(
                    f"{name} {document['_id']} still carries an identity document in {sorted(leaked)}"
                )
    person_ids = professors.distinct("person_id", {"person_id": {"$ne": None}})
    found = db["person"].count_documents({"_id": {"$in": person_ids}})
    if found != len(person_ids):
        raise RuntimeError(
            f"{len(person_ids) - found} person_id values are not in person"
        )
    report["personas_distintas"] = len(person_ids)


def link_news(news_db, db):
    """
    Copy the news collections into ``db`` with professors linked by ``person_id``.

    Parameters
    ----------
    news_db : pymongo.database.Database
        Database with the news collections (only read).
    db : pymongo.database.Database
        Release database; its ``person`` must still hold the identity documents.

    Returns
    -------
    dict
        Documents per collection and the coverage of the professors link.
    """
    missing = sorted(set(NEWS_COLLECTIONS) - set(news_db.list_collection_names()))
    if missing:
        raise RuntimeError(f"Missing news collections in {news_db.name}: {missing}")
    if news_db.name == db.name and news_db.client.address == db.client.address:
        raise RuntimeError("The news source and the release must be different databases")

    numbers = {
        number for number in (
            _document_number(value)
            for value in news_db[PROFESSORS_COLLECTION].distinct(IDENTITY_FIELD)
        ) if number
    }
    index = identity_index(db["person"])
    if numbers and not index:
        raise RuntimeError(
            "person has no identity documents: run this stage before removing them"
        )

    report = {
        "documentos": {},
        "enlazados": 0,
        "sin_persona": 0,
        "ambiguos": 0,
        "con_noticias": 0,
        "con_noticias_enlazados": 0,
        "ejemplos_ambiguos": [],
    }
    for name in NEWS_COLLECTIONS:
        if name == PROFESSORS_COLLECTION:
            documents = _linked_professors(news_db[name], index, report)
        else:
            documents = news_db[name].find({})
        report["documentos"][name] = _replace_collection(db[name], documents)
    db[PROFESSORS_COLLECTION].create_index([("person_id", ASCENDING)])

    verify_news(news_db, db, report, numbers)
    report["ejemplos_ambiguos"] = [str(item) for item in report["ejemplos_ambiguos"][:5]]
    return report

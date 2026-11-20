import mongomock

from kahi_impactu_postcalculations.author_reconciliation import (
    normalized_author_name,
    reconcile_collection_authors,
    reconcile_document_authors,
)


def test_normalized_name_ignores_case_accents_and_punctuation():
    assert normalized_author_name(" Hernández,  Arias ") == "hernandez arias"


def test_reconciles_unique_same_document_author_and_preserves_metadata():
    authors = [
        {"id": "", "full_name": "JENNIFER ANDREA HERNANDEZ ARIAS",
         "affiliations": [{"id": "A"}]},
        {"id": "0002121589", "full_name": "Jennifer Andrea Hernández Arias",
         "affiliations": []},
    ]

    result, resolved, ambiguous, conflicts = reconcile_document_authors(authors)

    assert resolved == 1
    assert ambiguous == 0
    assert conflicts == 0
    assert result == [{
        "id": "0002121589",
        "full_name": "Jennifer Andrea Hernández Arias",
        "affiliations": [{"id": "A"}],
    }]


def test_skips_ambiguous_same_name_ids():
    authors = [
        {"id": "", "full_name": "Ana Pérez"},
        {"id": "1", "full_name": "Ana Perez"},
        {"id": "2", "full_name": "ANA PEREZ"},
    ]

    result, resolved, ambiguous, conflicts = reconcile_document_authors(authors)

    assert result == authors
    assert resolved == 0
    assert ambiguous == 1
    assert conflicts == 0


def test_skips_different_nonempty_affiliations():
    authors = [
        {"id": "", "full_name": "Ana Pérez", "affiliations": [{"id": "A"}]},
        {"id": "1", "full_name": "ANA PEREZ", "affiliations": [{"id": "B"}]},
    ]

    result, resolved, ambiguous, conflicts = reconcile_document_authors(authors)

    assert result == authors
    assert resolved == 0
    assert ambiguous == 0
    assert conflicts == 1


def test_collection_dry_run_is_idempotent_and_adds_no_fields():
    collection = mongomock.MongoClient().db.events
    collection.insert_one({
        "_id": "event",
        "authors": [
            {"id": "", "full_name": "Ana Pérez"},
            {"id": "1", "full_name": "ANA PEREZ"},
        ],
    })

    report = reconcile_collection_authors(collection, dry_run=True)

    assert report["authors_resolved"] == 1
    assert collection.find_one()["authors"][0]["id"] == ""


def test_collection_apply_updates_existing_author_count_only():
    collection = mongomock.MongoClient().db.works
    collection.insert_many([
        {"_id": "with-count", "author_count": 2, "authors": [
            {"id": "", "full_name": "Ana Pérez"},
            {"id": "1", "full_name": "ANA PEREZ"},
        ]},
        {"_id": "without-count", "authors": [
            {"id": "", "full_name": "Luis Gómez"},
            {"id": "2", "full_name": "LUIS GOMEZ"},
        ]},
    ])

    first = reconcile_collection_authors(collection, dry_run=False)
    second = reconcile_collection_authors(collection, dry_run=False)

    assert first["authors_resolved"] == 2
    assert second["authors_resolved"] == 0
    assert collection.find_one({"_id": "with-count"})["author_count"] == 1
    assert "author_count" not in collection.find_one({"_id": "without-count"})

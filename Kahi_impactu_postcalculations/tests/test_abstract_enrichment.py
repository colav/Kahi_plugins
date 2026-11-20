import mongomock

from kahi_impactu_postcalculations.abstract_enrichment import (
    enrich_missing_abstracts,
    normalize_doi,
    title_similarity,
)


def databases():
    client = mongomock.MongoClient()
    return client.kahi.works, client.impactu_abstract_resolver.resolved, client.impactu_abstract_resolver.kahi_enrichment_runs


def resolved_document(**overrides):
    document = {
        "doi": "https://doi.org/10.1000/example",
        "abstract": "Resumen recuperado",
        "abstract_inverted_index": {"Resumen": [0], "recuperado": [1]},
        "source": "doaj",
        "rights": {"can_store": True},
        "source_titles": [{"title": "Análisis de biodiversidad en Colombia"}],
        "publication_status": "REVIEW_REQUIRED",
    }
    document.update(overrides)
    return document


def test_normalizes_common_doi_representations():
    expected = "10.1000/example"
    assert normalize_doi("https://doi.org/10.1000/EXAMPLE") == expected
    assert normalize_doi("doi:10.1000/example") == expected
    assert normalize_doi("10.1000/example") == expected


def test_title_similarity_accepts_reordered_nonexact_title():
    assert title_similarity(
        "Colombia: análisis de biodiversidad",
        "Análisis de biodiversidad en Colombia",
    ) >= 90


def test_dry_run_reports_match_without_modifying_work():
    works, resolved, runs = databases()
    works.insert_one({
        "_id": "W1",
        "doi": "10.1000/example",
        "titles": [{"title": "Análisis de biodiversidad en Colombia"}],
        "abstracts": [],
    })
    resolved.insert_one(resolved_document())

    report = enrich_missing_abstracts(
        works, resolved, runs, dry_run=True, title_threshold=90
    )

    assert report["works_changed"] == 1
    assert works.find_one({"_id": "W1"})["abstracts"] == []
    run = runs.find_one({"_id": report["run_id"]})
    assert run["status"] == "COMPLETED"
    assert run["stats"]["abstracts_extracted"] == 1
    assert run["stats"]["publication_status_counts"] == {"REVIEW_REQUIRED": 1}
    assert run["stats"]["display_permission_counts"] == {"allowed": 0, "not_allowed": 1}


def test_apply_adds_ordered_abstract_metadata_and_is_idempotent():
    works, resolved, runs = databases()
    works.insert_one({
        "_id": "W1",
        "external_ids": [{"source": "doi", "id": "https://doi.org/10.1000/example"}],
        "titles": [{"title": "Análisis de biodiversidad en Colombia"}],
    })
    resolved.insert_one(resolved_document())

    first = enrich_missing_abstracts(
        works, resolved, runs, dry_run=False, title_threshold=90
    )
    second = enrich_missing_abstracts(
        works, resolved, runs, dry_run=False, title_threshold=90
    )

    assert first["works_changed"] == 1
    assert second["works_scanned"] == 0
    assert works.find_one({"_id": "W1"})["abstracts"] == [{
        "abstract": {"Resumen": [0], "recuperado": [1]},
        "lang": "",
        "source": "doaj",
        "provenance": "impactu_abstract_resolver",
    }]


def test_rejects_different_title_and_forbidden_storage():
    works, resolved, runs = databases()
    works.insert_many([
        {"_id": "different", "doi": "10.1000/example", "titles": [{"title": "Química cuántica"}]},
        {"_id": "forbidden", "doi": "10.2000/example", "titles": [{"title": "Título permitido"}]},
    ])
    resolved.insert_many([
        resolved_document(),
        resolved_document(
            doi="https://doi.org/10.2000/example",
            source_titles=[{"title": "Título permitido"}],
            rights={"can_store": False},
        ),
    ])

    report = enrich_missing_abstracts(
        works, resolved, runs, dry_run=False, title_threshold=90
    )

    assert report["works_changed"] == 0
    assert report["title_rejections"] == 1
    assert report["doi_not_found"] == 1
    assert all("abstracts" not in work for work in works.find())


def test_ignores_plain_abstract_without_inverted_index():
    works, resolved, runs = databases()
    works.insert_one({
        "_id": "W1",
        "doi": "10.1000/example",
        "titles": [{"title": "Análisis de biodiversidad en Colombia"}],
    })
    resolved.insert_one(resolved_document(abstract_inverted_index=None))

    report = enrich_missing_abstracts(
        works, resolved, runs, dry_run=False, title_threshold=90
    )

    assert report["works_changed"] == 0
    assert report["resolver_without_inverted_abstract"] == 1
    assert "abstracts" not in works.find_one({"_id": "W1"})

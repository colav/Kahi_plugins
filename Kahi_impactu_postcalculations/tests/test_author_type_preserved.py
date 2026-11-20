"""Authors keep their ``type`` (advisor, author) through postcalculations."""
from pathlib import Path
import re

from kahi_impactu_postcalculations import denormalization
from kahi_impactu_postcalculations.author_reconciliation import reconcile_document_authors

SOURCE = Path(denormalization.__file__).read_text()
# Every stage that rewrites the authors array, by its ``"input": "$authors"`` line.
AUTHOR_MAPS = [match.start() for match in re.finditer(r'"input": "\$authors",', SOURCE)]


def test_every_stage_that_rewrites_authors_starts_from_the_whole_author():
    assert len(AUTHOR_MAPS) >= 8
    for position in AUTHOR_MAPS:
        stage = SOURCE[position:position + 1500]
        line = SOURCE.count("\n", 0, position) + 1
        merge = stage.find('"$mergeObjects": [')
        assert merge != -1, f"denormalization.py:{line} rebuilds authors without $mergeObjects"
        first_operand = stage[merge:].split("[", 1)[1].split(",", 1)[0].strip()
        assert first_operand == '"$$author"', (
            f"denormalization.py:{line} does not keep the original author fields "
            f"(first $mergeObjects operand is {first_operand})")


def test_person_fields_merged_into_authors_do_not_include_type():
    lookup = SOURCE[SOURCE.index('"as": "authors_data"'):][:600]
    assert '"type"' not in lookup


def test_blank_advisor_merged_into_the_identified_author_keeps_the_advisor_type():
    authors = [
        {"id": "p1", "full_name": "Gladys Basto Hernández", "affiliations": []},
        {"id": "", "full_name": "Gladys Basto Hernandez", "type": "advisor", "affiliations": []},
        {"id": "p2", "full_name": "María Londoño", "type": "author", "affiliations": []},
    ]
    result, resolved, _ambiguous, _conflicts = reconcile_document_authors(authors)
    assert resolved == 1
    assert [(author["id"], author.get("type")) for author in result] == [
        ("p1", "advisor"), ("p2", "author")]


def test_reconciliation_never_drops_an_advisor_without_a_twin():
    authors = [
        {"id": "p2", "full_name": "Stefania Ramos Franco", "type": "author", "affiliations": []},
        {"id": "", "full_name": "María Claudia Mejía Gil", "type": "advisor", "affiliations": []},
    ]
    result, resolved, _ambiguous, _conflicts = reconcile_document_authors(authors)
    assert resolved == 0 and result == authors

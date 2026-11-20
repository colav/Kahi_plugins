import importlib.util
from pathlib import Path

from bson import ObjectId


SCRIPT = Path(__file__).parents[1] / "scripts" / "plan_minciencias_work_dedup.py"
SPEC = importlib.util.spec_from_file_location("dedup_plan", SCRIPT)
dedup_plan = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(dedup_plan)


def work(title, year=2020, date=1577854800, source_id=None, authors=None, doi=""):
    return {
        "_id": ObjectId(),
        "titles": [{"title": title}],
        "year_published": year,
        "date_published": date,
        "source": {"id": source_id} if source_id else {},
        "authors": [{"id": value} for value in (authors or [])],
        "doi": doi,
    }


def test_accepts_unique_doi_bridge_with_long_exact_title():
    source_id = ObjectId()
    loser = work(
        "A sufficiently long bibliographic title shared by both records",
        source_id=source_id,
        authors=["person-1"],
    )
    survivor = work(
        "A sufficiently long bibliographic title shared by both records",
        source_id=source_id,
        authors=["person-1"],
    )

    evidence, rejection, similarity = dedup_plan.evaluate_pair(
        loser, survivor, "10.1234/example"
    )

    assert evidence == "exact_long_title_shared_author"
    assert rejection is None
    assert similarity == 1.0


def test_rejects_conflicting_year_even_when_title_matches():
    loser = work("A sufficiently long bibliographic title shared by both records", year=2019)
    survivor = work("A sufficiently long bibliographic title shared by both records", year=2020)

    evidence, rejection, _ = dedup_plan.evaluate_pair(loser, survivor, "10.1234/example")

    assert evidence is None
    assert rejection == "year_conflict"


def test_rejects_exact_title_without_shared_author():
    loser = work(
        "A sufficiently long bibliographic title shared by both records",
        authors=["person-1"],
    )
    survivor = work(
        "A sufficiently long bibliographic title shared by both records",
        authors=["person-2"],
    )

    evidence, rejection, similarity = dedup_plan.evaluate_pair(
        loser, survivor, "10.1234/example"
    )

    assert evidence is None
    assert rejection == "exact_title_without_shared_author"
    assert similarity == 1.0


def test_fuzzy_title_requires_shared_author():
    loser = work(
        "Neural networks for bibliographic entity reconciliation in Colombia",
        authors=["person-1"],
    )
    survivor = work(
        "Neural networks for bibliographic entity reconciliaton in Colombia",
        authors=["person-2"],
    )

    evidence, rejection, similarity = dedup_plan.evaluate_pair(
        loser, survivor, "10.1234/example"
    )

    assert similarity >= dedup_plan.FUZZY_TITLE_THRESHOLD
    assert evidence is None
    assert rejection == "insufficient_title_author"


def test_rejects_source_conflict_before_title_evidence():
    loser = work(
        "A sufficiently long bibliographic title shared by both records",
        source_id=ObjectId(),
    )
    survivor = work(
        "A sufficiently long bibliographic title shared by both records",
        source_id=ObjectId(),
    )

    evidence, rejection, _ = dedup_plan.evaluate_pair(loser, survivor, "10.1234/example")

    assert evidence is None
    assert rejection == "source_conflict"


def test_rejects_pseudo_doi():
    loser = work("A sufficiently long bibliographic title shared by both records")
    survivor = work("A sufficiently long bibliographic title shared by both records")

    evidence, rejection, _ = dedup_plan.evaluate_pair(
        loser, survivor, "10.1002/(ISSN)1099-0690"
    )

    assert evidence is None
    assert rejection == "invalid_doi"

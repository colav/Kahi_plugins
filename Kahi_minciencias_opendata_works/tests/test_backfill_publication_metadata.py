from collections import Counter
import importlib.util
from pathlib import Path

from bson import ObjectId


SCRIPT = (
    Path(__file__).parents[1]
    / "scripts"
    / "backfill_publication_metadata_from_previous.py"
)
SPEC = importlib.util.spec_from_file_location("metadata_backfill", SCRIPT)
backfill = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(backfill)


def work(title, title_source, external_ids=None, authors=None, **fields):
    document = {
        "titles": [{"title": title, "source": title_source}],
        "external_ids": external_ids or [],
        "authors": authors or [],
    }
    document.update(fields)
    return document


def test_identity_requires_rich_evidence_beyond_minciencias_id():
    product_id = {
        "source": "scienti",
        "provenance": "minciencias",
        "id": {"COD_RH": "1", "COD_PRODUCTO": "2"},
    }
    previous = work("Generic title", "minciencias", [product_id])
    target = work("Generic title", "minciencias", [product_id])

    assert backfill.strong_identity_evidence(previous, target) is None


def test_identity_accepts_exact_long_title_from_rich_source():
    title = "A sufficiently distinctive publication title for matching"
    previous = work(title, "openalex")
    target = work(title.upper(), "minciencias")

    assert backfill.strong_identity_evidence(
        previous, target) == "exact_rich_title"


def test_proposal_fills_only_missing_values_and_remaps_source():
    old_source = ObjectId()
    new_source = ObjectId()
    previous = {
        "year_published": 2020,
        "date_published": 1577836800,
        "source": {"id": old_source},
    }
    target = {
        "year_published": None,
        "date_published": None,
        "source": {},
    }

    fields = backfill.propose_fields(
        previous,
        target,
        {old_source: {"id": new_source, "name": "Journal"}},
    )

    assert fields == {
        "year_published": 2020,
        "date_published": 1577836800,
        "source": {"id": new_source, "name": "Journal"},
    }


def test_proposal_never_overwrites_populated_values():
    previous = {
        "year_published": 2020,
        "date_published": 1577836800,
        "source": {"id": ObjectId()},
    }
    target = {
        "year_published": 2021,
        "date_published": 1609459200,
        "source": {"id": ObjectId(), "name": "Existing"},
    }

    assert backfill.propose_fields(previous, target, {}) == {}


def test_conflicting_proposals_are_excluded():
    proposals = {}
    counters = Counter()
    backfill.merge_proposal(
        proposals, "target", "p1", "old1",
        {"year_published": 2020}, "exact_rich_title", counters)
    backfill.merge_proposal(
        proposals, "target", "p2", "old2",
        {"year_published": 2021}, "exact_rich_title", counters)

    assert backfill.finalize_proposals(proposals, counters) == []
    assert counters["conflict_year_published"] == 1

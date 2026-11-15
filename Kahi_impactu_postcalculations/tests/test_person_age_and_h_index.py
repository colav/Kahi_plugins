"""Person age replaces the birth date; h-index and h5-index definitions."""
from datetime import datetime

import mongomock
import mongomock.collection
import pytest

from kahi_impactu_postcalculations import denormalization
from kahi_impactu_postcalculations.denormalization import (
    DENORMALIZATION_PIPELINES,
    _build_citations_pipeline,
    _compute_h_index,
    completed_years,
    set_person_age,
)

REFERENCE = datetime(2026, 11, 14)


def timestamp(year, month, day):
    return int((datetime(year, month, day) - datetime(1970, 1, 1)).total_seconds())


@pytest.fixture(autouse=True)
def bulk_write_without_sort(monkeypatch):
    builder = mongomock.collection.BulkOperationBuilder
    original = builder.add_update

    def add_update(self, *args, sort=None, **kwargs):
        return original(self, *args, **kwargs)

    monkeypatch.setattr(builder, "add_update", add_update)


@pytest.mark.parametrize("birthdate, expected", [
    (timestamp(1975, 6, 30), 51),
    (timestamp(1975, 11, 14), 51),      # birthday today
    (timestamp(1975, 11, 15), 50),      # birthday tomorrow
    (timestamp(1960, 2, 29), 66),       # before 1970
    (timestamp(2011, 11, 14), 15),      # youngest accepted
    (timestamp(2011, 11, 15), None),    # 14 years old
    (timestamp(1926, 11, 14), 100),     # oldest accepted
    (timestamp(1925, 11, 14), None),    # 101 years old
    (-2208971024, None),                # 1900, typo in the source
    (2576638800, None),                 # 2051, in the future
    (-1, None), (None, None), ("1975-06-30", None), (True, None), (10 ** 20, None),
])
def test_completed_years(birthdate, expected):
    assert completed_years(birthdate, REFERENCE) == expected


def test_age_is_published_and_the_birth_date_is_cleared():
    person = mongomock.MongoClient().kahi.person
    person.insert_many([
        {"_id": "valid", "birthdate": timestamp(1975, 6, 30)},
        {"_id": "typo", "birthdate": -2208971024, "age": None},
        {"_id": "text", "birthdate": "1975-06-30"},
        {"_id": "empty", "birthdate": -1},
        {"_id": "null", "birthdate": None},
        {"_id": "done", "birthdate": -1, "age": 40},
    ])

    set_person_age(person, REFERENCE)

    found = {doc["_id"]: (doc["age"], doc["birthdate"]) for doc in person.find()}
    assert found == {"valid": (51, -1), "typo": (None, -1), "text": (None, -1),
                     "empty": (None, -1), "null": (None, -1), "done": (40, -1)}

    set_person_age(person, datetime(2030, 1, 1))
    assert {doc["_id"]: (doc["age"], doc["birthdate"]) for doc in person.find()} == found


def test_age_runs_last_among_the_person_stages():
    assert DENORMALIZATION_PIPELINES["person"][-1] is set_person_age


@pytest.mark.parametrize("citations, expected", [
    ([], 0), ([0, 0], 0), ([1], 1), ([10, 8, 5, 4, 3], 4), ([25, 8, 5, 3, 3], 3),
    ([3, 3, 3], 3), ([None, 5, None, 2], 2), ([100], 1), ([4, 4, 4, 4, 4], 4),
])
def test_h_index(citations, expected):
    assert _compute_h_index(citations) == expected


def test_h5_window_is_the_five_complete_years_before_the_run(monkeypatch):
    class Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 14)

    captured = {}

    class Collection:
        database = {"person": None}

        def aggregate(self, pipeline, **_kwargs):
            captured["pipeline"] = pipeline
            return []

    monkeypatch.setattr(denormalization, "datetime", Frozen)
    denormalization._set_h_index_metrics(Collection(), "person", "authors.id")

    condition = captured["pipeline"][1]["$project"]["h5_citations"]["$map"]["input"][
        "$filter"]["cond"]["$and"]
    assert condition == [{"$gte": ["$$w.year_published", 2021]},
                         {"$lte": ["$$w.year_published", 2025]}]
    assert captured["pipeline"] == _build_citations_pipeline("authors.id", 2021, 2025)
    assert captured["pipeline"][0]["$lookup"]["foreignField"] == "authors.id"

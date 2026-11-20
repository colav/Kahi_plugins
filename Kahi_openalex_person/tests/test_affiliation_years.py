"""Identity, last-known institution years and concurrent merges on real MongoDB."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from importlib import import_module
import os
from uuid import uuid4

import pytest
from pymongo import MongoClient

from kahi.KahiBase import KahiBase
from kahi_openalex_person.affiliation_years import (
    InstitutionResolver, canonical_orcid, collect_affiliations, resolve_person,
)

plugin = import_module("kahi_openalex_person.Kahi_openalex_person")
ORCID = "0000-0003-2895-3084"
UDEA = {"id": "https://openalex.org/I101", "ror": "https://ror.org/03bp5hc83"}
UNAL = {"id": "https://openalex.org/I102", "ror": "https://ror.org/059yx9a68"}


def author(number=1, years=None, orcid=ORCID):
    identifier = f"https://openalex.org/A{number}"
    return {"id": identifier, "display_name": "Ana Pérez",
            "display_name_alternatives": ["Ana Perez"],
            "ids": {"openalex": identifier, "orcid": f"https://orcid.org/{orcid}" if orcid else None},
            "affiliations": [{"institution": deepcopy(UDEA), "years": [2023, 2025] if years is None else years}],
            "last_known_institutions": [deepcopy(UDEA)]}


def person(number=1, years=None, orcid=ORCID, openalex=False):
    entry = KahiBase.empty_person(None)
    entry.update(_id=f"person-{number}", full_name="Ana Pérez", first_names=["Ana"], last_names=["Pérez"])
    entry["external_ids"] = [{"source": "orcid", "id": f"https://orcid.org/{orcid}"}] if orcid else []
    if openalex:
        entry["external_ids"].append({"source": "openalex", "id": f"A{number}"})
    entry["updated"] = [{"source": "openalex" if openalex else "staff", "time": 1}]
    entry["affiliations"] = [{"id": "03bp5hc83", "name": "Universidad de Antioquia", "types": [],
                              "start_date": 1640995200, "end_date": -1,
                              "years": [2022, 2023, 2024] if years is None else years}]
    return entry


@pytest.fixture
def db():
    url = os.environ.get("KAHI_TEST_MONGODB_URL")
    if not url:
        pytest.skip("Set KAHI_TEST_MONGODB_URL for isolated MongoDB tests")
    client = MongoClient(url, serverSelectionTimeoutMS=5000)
    name = "kahi_openalex_years_test_" + uuid4().hex
    database = client[name]
    database.person.create_index("external_ids.id")
    database.affiliations.create_index("external_ids.id")
    for institution in (UDEA, UNAL):
        database.affiliations.insert_one({
            "_id": institution["ror"].rsplit("/", 1)[-1],
            "names": [{"name": "University", "lang": "en"}, {"name": "Universidad", "lang": "es"}],
            "types": [{"source": "ror", "type": "education"}],
            "external_ids": [{"source": "ror", "id": institution["ror"]},
                             {"source": "openalex", "id": institution["id"]}],
        })
    try:
        yield database
    finally:
        client.drop_database(name)
        client.close()


@pytest.fixture(autouse=True)
def forbid_http(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("Affiliation years must come from the local dump, with no HTTP requests")
    monkeypatch.setattr("requests.sessions.Session.request", forbidden)
    monkeypatch.setattr(plugin, "split_names", lambda _name: {
        "first_names": ["Ana"], "last_names": ["Pérez"], "initials": "A P"})


def run(db, source, *, skip=False, resolver=None):
    if resolver is None:
        resolver = InstitutionResolver(db.affiliations)
        resolver.prefetch([source])
    return plugin.process_one(source, db.client, db.name, KahiBase.empty_person(None),
                              db.works, skip, resolver)


def test_staff_union_keeps_dates_metadata_and_other_unit_histories(db):
    original = person()
    original["affiliations"].extend([
        {"id": "faculty", "types": [{"type": "faculty"}], "years": [2022]},
        {"id": "department", "types": [{"type": "department"}], "years": [2023]},
        {"id": "group", "types": [{"type": "group"}]},
    ])
    original["affiliations"][0]["extra"] = "preserved"
    db.person.insert_one(original)
    run(db, author())
    result = db.person.find_one({"_id": original["_id"]})
    expected = deepcopy(original["affiliations"])
    expected[0]["years"] = [2022, 2023, 2024, 2025]
    assert result["affiliations"] == expected
    assert [key for key in result if key != "_id"] == [key for key in original if key != "_id"]
    assert list(result["affiliations"][0]) == list(original["affiliations"][0])
    assert db.works.count_documents({}) == 0


def test_new_author_imports_only_last_known_institutions_with_historical_years(db):
    source = author(orcid=None)
    source["affiliations"].append({"institution": UNAL, "years": [2020, 2019, 2020]})
    template = KahiBase.empty_person(None)
    before = deepcopy(template)
    plugin.process_one_insert(None, source, db.client, db.name, template, [])
    assert template == before
    result = db.person.find_one()
    assert [key for key in result if key != "_id"] == list(before)
    affiliations = {item["id"]: item for item in result["affiliations"]}
    assert len(result["affiliations"]) == 1
    assert affiliations["03bp5hc83"]["years"] == [2023, 2025]
    assert "059yx9a68" not in affiliations
    assert affiliations["03bp5hc83"]["start_date"] == -1


def test_existing_non_last_known_affiliation_keeps_its_metadata_and_years(db):
    original = person()
    original["affiliations"].append({
        "id": "059yx9a68", "name": "Universidad Nacional", "types": [],
        "start_date": 1514764800, "end_date": 1546300800, "years": [2018],
    })
    db.person.insert_one(original)
    source = author()
    source["affiliations"].append({"institution": UNAL, "years": [2019, 2020]})
    run(db, source)
    result = db.person.find_one()
    assert len(result["affiliations"]) == 2
    assert result["affiliations"][0]["years"] == [2022, 2023, 2024, 2025]
    assert result["affiliations"][1] == original["affiliations"][1]


@pytest.mark.parametrize("last_known", [None, [], "missing"])
def test_no_last_known_institutions_does_not_import_historical_affiliations(db, last_known):
    source = author(orcid=None)
    if last_known == "missing":
        source.pop("last_known_institutions")
    else:
        source["last_known_institutions"] = last_known
    run(db, source)
    assert db.person.find_one()["affiliations"] == []


def test_no_last_known_institutions_preserves_existing_staff_affiliations(db):
    original = person()
    db.person.insert_one(original)
    source = author()
    source["last_known_institutions"] = []
    run(db, source)
    assert db.person.find_one()["affiliations"] == original["affiliations"]


def test_multiple_last_known_institutions_union_duplicate_historical_records_once(db):
    source = author(orcid=None)
    source["last_known_institutions"].extend([deepcopy(UNAL), deepcopy(UDEA)])
    source["affiliations"].extend([
        {"institution": {"ror": "03bp5hc83"}, "years": [2024, 2023]},
        {"institution": UNAL, "years": [2020, 2019, 2020]},
    ])
    run(db, source)
    affiliations = db.person.find_one()["affiliations"]
    assert [item["id"] for item in affiliations] == ["03bp5hc83", "059yx9a68"]
    assert affiliations[0]["years"] == [2023, 2024, 2025]
    assert affiliations[1]["years"] == [2019, 2020]


def test_contradictory_historical_institution_ids_do_not_supply_last_known_years(db, caplog):
    source = author(orcid=None, years=[2025])
    source["affiliations"].append({
        "institution": {"id": UDEA["id"], "ror": UNAL["ror"]}, "years": [2020],
    })
    run(db, source)
    affiliations = db.person.find_one()["affiliations"]
    assert len(affiliations) == 1
    assert affiliations[0]["years"] == [2025]
    assert "ambiguous_institution" in caplog.text


def test_resume_enriches_years_without_loading_related_works_and_is_idempotent(db, monkeypatch):
    db.person.insert_one(person(openalex=True))
    monkeypatch.setattr(plugin, "get_related_works", lambda *_args: pytest.fail("resume loaded works"))
    run(db, author(), skip=True)
    first = db.person.find_one()
    assert first["affiliations"][0]["years"] == [2022, 2023, 2024, 2025]
    run(db, author(), skip=True)
    assert db.person.find_one() == first


@pytest.mark.parametrize("values,expected", [
    ([4, 3025, True, 2023.5, "invalid", None, 2023, "2024", 2023], [2023, 2024]),
    ([], []), (None, []), ([1800, 1799], [1800]),
])
def test_invalid_incoming_years_are_ignored(db, values, expected):
    source = author(orcid=None)
    source["affiliations"][0]["years"] = values
    run(db, source)
    assert db.person.find_one()["affiliations"][0]["years"] == expected


def test_empty_incoming_years_never_erase_staff_years(db):
    db.person.insert_one(person())
    run(db, author(years=[]))
    assert db.person.find_one()["affiliations"][0]["years"] == [2022, 2023, 2024]


def test_missing_years_evidence_does_not_disable_existing_date_based_eligibility(db):
    original = person()
    del original["affiliations"][0]["years"]
    db.person.insert_one(original)
    run(db, author(years=[]))
    assert db.person.find_one()["affiliations"] == original["affiliations"]


def test_last_known_only_institution_has_an_empty_years_list(db):
    source = author(orcid=None)
    source["affiliations"] = None
    run(db, source)
    assert db.person.find_one()["affiliations"][0]["years"] == []


def test_last_known_institution_gets_historical_years_resolved_by_bare_ror_only(db):
    source = author(orcid=None)
    source["affiliations"] = [{"institution": {"ror": "059yx9a68"}, "years": [2020]}]
    source["last_known_institutions"] = [{"id": UNAL["id"]}]
    run(db, source)
    assert db.person.find_one()["affiliations"][0]["id"] == "059yx9a68"
    assert db.person.find_one()["affiliations"][0]["years"] == [2020]


@pytest.mark.parametrize("source", ["openalex", "orcid"])
def test_ambiguous_person_identifier_logs_candidates_and_skips_author(db, caplog, source):
    first, second = person(), person(2)
    if source == "openalex":
        for record in (first, second):
            record["external_ids"] = [{"source": "openalex", "id": "A1"}]
    db.person.insert_many([first, second])
    before = list(db.person.find())
    run(db, author())
    assert list(db.person.find()) == before
    assert f"ambiguous_{source}" in caplog.text
    assert "person-1" in caplog.text and "person-2" in caplog.text


def test_orcid_cannot_attach_another_openalex_id(db, caplog):
    db.person.insert_one(person(2, openalex=True))
    before = db.person.find_one()
    run(db, author(1))
    assert db.person.find_one() == before
    assert db.person.count_documents({}) == 1
    assert "orcid_has_other_openalex_id" in caplog.text


def test_existing_openalex_person_with_conflicting_orcid_is_not_enriched(db, caplog):
    db.person.insert_one(person(openalex=True, orcid="0000-0003-4072-6140"))
    before = db.person.find_one()
    run(db, author())
    assert db.person.find_one() == before
    assert "conflicting_orcid" in caplog.text


@pytest.mark.parametrize("value", [ORCID, f"https://orcid.org/{ORCID}", f"http://orcid.org/{ORCID}"])
def test_orcid_variants_find_the_same_staff_person(db, value):
    original = person()
    original["external_ids"][0]["id"] = value
    db.person.insert_one(original)
    run(db, author())
    assert db.person.count_documents({}) == 1
    assert db.person.find_one()["affiliations"][0]["years"] == [2022, 2023, 2024, 2025]


@pytest.mark.parametrize("value", [None, "garbage", "0000-0003-2895-3085", "abcd-abcd-abcd-abcd"])
def test_malformed_orcid_is_not_an_identity_bridge(value):
    assert canonical_orcid(value) is None


def test_wrong_identifier_namespace_cannot_match_a_person(db):
    original = person(orcid=None)
    original["external_ids"] = [{"source": "scopus", "id": "A1"}]
    db.person.insert_one(original)
    resolved, valid = resolve_person(db.person, author(orcid=None))
    assert resolved is None and valid


def test_conflicting_ror_and_openalex_institution_ids_skip_only_that_institution(db, caplog):
    source = author(orcid=None)
    source["affiliations"] = [
        {"institution": {"id": UDEA["id"], "ror": UNAL["ror"]}, "years": [2020]},
        {"institution": UNAL, "years": [2023]},
    ]
    source["last_known_institutions"] = [
        {"id": UDEA["id"], "ror": UNAL["ror"]}, deepcopy(UNAL),
    ]
    run(db, source)
    assert [item["id"] for item in db.person.find_one()["affiliations"]] == ["059yx9a68"]
    assert "ambiguous_institution" in caplog.text


def test_unresolved_institution_is_logged_without_guessing_or_creating_it(db, caplog):
    source = author(orcid=None)
    source["affiliations"] = [{"institution": {"id": "https://openalex.org/I9999"}, "years": [2020]}]
    source["last_known_institutions"] = [{"id": "https://openalex.org/I9999"}]
    run(db, source)
    assert db.person.find_one()["affiliations"] == []
    assert "unresolved_institution" in caplog.text
    assert db.affiliations.count_documents({}) == 2


def test_cache_avoids_repeated_institution_database_reads(db):
    class CountingCollection:
        calls = 0

        def find(self, *args, **kwargs):
            self.calls += 1
            return db.affiliations.find(*args, **kwargs)

    collection = CountingCollection()
    resolver = InstitutionResolver(collection)
    for _ in range(10):
        collect_affiliations(author(), resolver)
    assert collection.calls == 1


def test_prefetch_resolves_a_batch_with_one_read_and_reuses_cache(db):
    class CountingCollection:
        calls = 0

        def find(self, *args, **kwargs):
            self.calls += 1
            return db.affiliations.find(*args, **kwargs)

    source = author()
    source["affiliations"].append({"institution": UNAL, "years": [2020]})
    collection = CountingCollection()
    resolver = InstitutionResolver(collection)
    resolver.prefetch([source] * 100)
    for _ in range(10):
        result = collect_affiliations(source, resolver)
        assert len(result) == 1
        assert result[0]["years"] == [2023, 2025]
    resolver.prefetch([source] * 100)
    assert collection.calls == 1


def test_prefetch_and_on_demand_resolution_make_identical_conflict_decisions(db):
    source = author()
    source["affiliations"].append({"institution": {"ror": UNAL["ror"], "id": UDEA["id"]}, "years": [2020]})
    source["affiliations"].append({"institution": {"id": "https://openalex.org/I9999"}, "years": [2021]})
    direct = InstitutionResolver(db.affiliations)
    prefetched = InstitutionResolver(db.affiliations)
    prefetched.prefetch([source])
    assert collect_affiliations(source, direct) == collect_affiliations(source, prefetched)


def test_invalid_years_container_does_not_stop_import(db, caplog):
    source = author()
    source["affiliations"][0]["years"] = 2020
    run(db, source)
    assert db.person.find_one()["affiliations"][0]["years"] == []
    assert "invalid_years" in caplog.text


def test_equivalent_identifier_formats_are_not_appended_again(db):
    original = person(openalex=True)
    original["external_ids"][0]["id"] = ORCID
    db.person.insert_one(original)
    run(db, author())
    assert db.person.find_one()["external_ids"] == original["external_ids"]


def test_plugin_streams_small_batches_and_enriches_all_new_authors(db, monkeypatch):
    records = [author(number, orcid=None) for number in range(1, 8)]
    db.source_authors.insert_many(records)
    batches = []

    class TrackingResolver(InstitutionResolver):
        def prefetch(self, batch):
            batches.append(len(batch))
            return super().prefetch(batch)

    monkeypatch.setattr(plugin, "InstitutionResolver", TrackingResolver)
    instance = plugin.Kahi_openalex_person.__new__(plugin.Kahi_openalex_person)
    instance.config = {"database_name": db.name}
    instance.mongodb_url = os.environ["KAHI_TEST_MONGODB_URL"]
    instance.openalex_collection = db.source_authors
    instance.openalex_collection_works = db.works
    instance.n_jobs, instance.verbose, instance.batch_size = 3, 0, 2
    instance.skip_previously_processed = False
    instance.process_openalex()
    assert batches == [2, 2, 2, 1]
    assert db.person.count_documents({}) == 7
    for result in db.person.find():
        assert result["affiliations"][0]["years"] == [2023, 2025]
        assert len([item for item in result["external_ids"] if item["source"] == "openalex"]) == 1


def test_concurrent_updates_with_stale_snapshots_keep_all_years_and_one_new_institution(db):
    original = person(openalex=True, years=[2000])
    db.person.insert_one(original)
    resolver = InstitutionResolver(db.affiliations)

    def update(index):
        source = author(years=[2001 + index])
        source["affiliations"].append({"institution": UNAL, "years": [2001 + index]})
        source["last_known_institutions"].append(deepcopy(UNAL))
        plugin.process_one_update(deepcopy(original), source, db.client, db.name,
                                  KahiBase.empty_person(None), [], resolver)

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(update, range(24)))
    result = db.person.find_one()
    by_id = {item["id"]: item for item in result["affiliations"]}
    assert len(result["affiliations"]) == 2
    assert by_id["03bp5hc83"]["years"] == list(range(2000, 2025))
    assert by_id["059yx9a68"]["years"] == list(range(2001, 2025))


def test_concurrent_orcid_binding_rejects_second_openalex_id(db, caplog):
    original = person()
    db.person.insert_one(original)
    for number in (1, 2):
        plugin.process_one_update(deepcopy(original), author(number), db.client, db.name,
                                  KahiBase.empty_person(None), [])
    result = db.person.find_one()
    assert [item["id"] for item in result["external_ids"] if item["source"] == "openalex"] == ["https://openalex.org/A1"]
    assert "person_identity_changed_during_update" in caplog.text

from unittest.mock import MagicMock
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore, Lock
from time import sleep

from bson import ObjectId

import kahi_openalex_works.Kahi_openalex_works as plugin_module
import kahi_openalex_works.process_one as process_module


class FakeCursor:
    def __init__(self, documents):
        self.documents = documents
        self.limit_value = None

    def sort(self, *_args):
        return self

    def hint(self, *_args):
        return self

    def limit(self, value):
        self.limit_value = value
        return self

    def __iter__(self):
        return iter(self.documents[:self.limit_value])


class FakeOpenAlexCollection:
    def __init__(self, documents):
        self.documents = documents
        self.count_query = None
        self.find_queries = []

    def count_documents(self, query):
        self.count_query = query
        return len(self.documents)

    def find(self, query):
        self.find_queries.append(query)
        last_id = query.get("_id", {}).get("$gt")
        documents = self.documents
        if last_id is not None:
            documents = [doc for doc in documents if doc["_id"] > last_id]
        return FakeCursor(documents)


class FakeParallel:
    kwargs = None

    def __init__(self, **kwargs):
        type(self).kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def __call__(self, tasks):
        return (task() for task in tasks)


def immediate_delayed(function):
    def bind(*args, **kwargs):
        return lambda: function(*args, **kwargs)
    return bind


def test_process_openalex_paginates_and_consumes_unordered_generator(monkeypatch):
    processed = []
    documents = [
        {"_id": value, "doi": None, "title": f"Work {value}", "type": "article"}
        for value in range(1, 8)
    ]
    instance = object.__new__(plugin_module.Kahi_openalex_works)
    instance.task = None
    instance.openalex_collection = FakeOpenAlexCollection(documents)
    instance.config = {"openalex_works": {"page_size": 3}}
    instance.n_jobs = 48
    instance.verbose = 0
    instance.backend = "threading"
    instance.client = object()
    instance.es_handler = object()
    instance.es_semaphore = BoundedSemaphore(10)
    instance.empty_work = lambda: {}

    monkeypatch.setattr(plugin_module, "Parallel", FakeParallel)
    monkeypatch.setattr(plugin_module, "delayed", immediate_delayed)
    monkeypatch.setattr(
        plugin_module,
        "process_one",
        lambda paper, *_args, **_kwargs: processed.append(paper["_id"]),
    )

    instance.process_openalex()

    assert processed == list(range(1, 8))
    assert FakeParallel.kwargs["backend"] == "threading"
    assert FakeParallel.kwargs["return_as"] == "generator_unordered"
    assert instance.openalex_collection.count_query["title"] == {
        "$type": "string",
        "$regex": r"\S",
    }
    assert instance.openalex_collection.find_queries[0]["title"] == {
        "$type": "string",
        "$regex": r"\S",
    }


def test_process_openalex_multiprocessing_uses_supported_return_mode(monkeypatch):
    processed = []
    documents = [
        {"_id": value, "doi": None, "title": f"Work {value}", "type": "article"}
        for value in range(1, 4)
    ]
    instance = object.__new__(plugin_module.Kahi_openalex_works)
    instance.task = None
    instance.openalex_collection = FakeOpenAlexCollection(documents)
    instance.config = {"openalex_works": {"page_size": 2}}
    instance.n_jobs = 2
    instance.verbose = 0
    instance.backend = "multiprocessing"
    instance.client = None
    instance.es_handler = None
    instance.es_semaphore = None
    instance.empty_work = lambda: {}

    monkeypatch.setattr(plugin_module, "Parallel", FakeParallel)
    monkeypatch.setattr(plugin_module, "delayed", immediate_delayed)
    monkeypatch.setattr(
        plugin_module,
        "process_one",
        lambda paper, *_args, **_kwargs: processed.append(paper["_id"]),
    )

    instance.process_openalex()

    assert processed == [1, 2, 3]
    assert FakeParallel.kwargs["backend"] == "multiprocessing"
    assert "return_as" not in FakeParallel.kwargs


def outcome_process_one(paper, *_args, **_kwargs):
    return "nuevos" if paper["_id"] % 2 else "actualizados"


def progress_instance(backend, task=None):
    documents = [
        {"_id": value, "doi": None, "title": f"Work {value}", "type": "article"}
        for value in range(1, 4)
    ]
    instance = object.__new__(plugin_module.Kahi_openalex_works)
    instance.task = task
    instance.openalex_collection = FakeOpenAlexCollection(documents)
    instance.config = {"openalex_works": {"page_size": 2}}
    instance.n_jobs = 2
    instance.verbose = 0
    instance.backend = backend
    instance.client = None
    instance.es_handler = None
    instance.es_semaphore = None
    instance.empty_work = lambda: {}
    return instance


def test_process_openalex_threading_reports_outcomes(monkeypatch, capsys):
    monkeypatch.setattr(plugin_module, "Parallel", FakeParallel)
    monkeypatch.setattr(plugin_module, "delayed", immediate_delayed)
    monkeypatch.setattr(plugin_module, "process_one", outcome_process_one)

    progress_instance("threading").process_openalex()

    lines = capsys.readouterr().out.splitlines()
    assert FakeParallel.kwargs["verbose"] == 0
    assert lines[0] == ("[openalex_works:sin_doi] START total=3 | "
                        "backend=threading num_jobs=2 page_size=2")
    assert lines[-1].startswith("[openalex_works:sin_doi] DONE 3 reg en ")
    assert lines[-1].endswith("| nuevos=2 actualizados=1")


def test_process_openalex_multiprocessing_counts_completed_in_parent(monkeypatch, capsys):
    # Real joblib: the children cannot step, so the parent counts the
    # completed batches and aggregates the returned outcomes at the end.
    monkeypatch.setattr(plugin_module, "process_one", outcome_process_one)

    progress_instance("multiprocessing", task="doi").process_openalex()

    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == ("[openalex_works:doi] START total=3 | "
                        "backend=multiprocessing num_jobs=2 page_size=2")
    assert lines[-1].startswith("[openalex_works:doi] DONE 3 reg en ")
    assert lines[-1].endswith("| nuevos=2 actualizados=1")


def test_report_completed_tasks_steps_only_new_completions():
    parallel = MagicMock(n_completed_tasks=0)
    progress = plugin_module.ProgressLogger("test", total=30)
    plugin_module.report_completed_tasks(parallel, progress)
    for completed in (10, 10, 20, 30):
        parallel.n_completed_tasks = completed
        parallel.print_progress()
    assert progress.processed == 30


class FakePersonCollection:
    full_name = "database.person"

    def __init__(self, people):
        self.people = people
        self.queries = []

    def find(self, query, _projection):
        names = set(query["full_name"]["$in"])
        self.queries.append(names)
        return [person for person in self.people if person["full_name"] in names]


def test_author_cache_is_bounded_and_reuses_hits(monkeypatch):
    people = [
        {"_id": value, "full_name": name, "external_ids": [], "affiliations": []}
        for value, name in enumerate(["Ada", "Grace", "Linus"], start=1)
    ]
    collection = FakePersonCollection(people)
    process_module._AUTHOR_NAME_CACHE.clear()
    monkeypatch.setattr(process_module, "_AUTHOR_NAME_CACHE_MAX_SIZE", 2)

    process_module.get_author_name_candidates(collection, ["Ada", "Grace"])
    process_module.get_author_name_candidates(collection, ["Grace"])
    process_module.get_author_name_candidates(collection, ["Linus"])

    assert len(process_module._AUTHOR_NAME_CACHE) == 2
    assert (collection.full_name, "Ada") not in process_module._AUTHOR_NAME_CACHE
    assert collection.queries == [{"Ada", "Grace"}, {"Linus"}]


def test_threading_reuses_supplied_clients(monkeypatch):
    client = MagicMock()
    client.__getitem__.return_value.__getitem__.return_value.find_one.return_value = None
    monkeypatch.setattr(
        process_module,
        "get_process_mongo_client",
        MagicMock(side_effect=AssertionError("must not create a process client")),
    )

    process_module.process_one(
        {"doi": None},
        {"database_name": "kahi", "database_url": "localhost:27017", "openalex_works": {}},
        {},
        client,
        None,
        "threading",
    )

    client.__getitem__.assert_called_once_with("kahi")
    process_module.get_process_mongo_client.assert_not_called()


def openalex_work():
    return {
        "id": "https://openalex.org/W1",
        "doi": None,
        "title": "A work",
        "publication_year": 2024,
        "authorships": [],
        "primary_location": None,
        "biblio": {
            "volume": None,
            "issue": None,
            "first_page": None,
            "last_page": None,
        },
    }


def test_elasticsearch_match_updates_existing_work(monkeypatch):
    matched_id = ObjectId()
    collection = MagicMock()
    collection.find_one.side_effect = [None, {"_id": matched_id}]
    db = MagicMock()
    db.__getitem__.return_value = collection
    client = MagicMock()
    client.__getitem__.return_value = db
    es_handler = MagicMock()
    es_handler.search_work.return_value = {"_id": str(matched_id)}
    update = MagicMock()
    insert = MagicMock()
    monkeypatch.setattr(process_module, "process_one_update", update)
    monkeypatch.setattr(process_module, "process_one_insert", insert)

    outcome = process_module.process_one(
        openalex_work(),
        {"database_name": "kahi", "openalex_works": {}},
        {},
        client,
        es_handler,
        "threading",
        es_semaphore=BoundedSemaphore(1),
    )

    assert outcome == "actualizados"
    update.assert_called_once()
    insert.assert_not_called()
    assert collection.find_one.call_args_list[1].args[0] == {"_id": matched_id}


def test_missing_elasticsearch_match_inserts_work(monkeypatch):
    collection = MagicMock()
    collection.find_one.return_value = None
    db = MagicMock()
    db.__getitem__.return_value = collection
    client = MagicMock()
    client.__getitem__.return_value = db
    es_handler = MagicMock()
    es_handler.search_work.return_value = None
    update = MagicMock()
    insert = MagicMock(return_value="nuevos")
    monkeypatch.setattr(process_module, "process_one_update", update)
    monkeypatch.setattr(process_module, "process_one_insert", insert)

    outcome = process_module.process_one(
        openalex_work(),
        {"database_name": "kahi", "openalex_works": {}},
        {},
        client,
        es_handler,
        "threading",
        es_semaphore=BoundedSemaphore(1),
    )

    assert outcome == "nuevos"
    update.assert_not_called()
    insert.assert_called_once()
    assert insert.call_args.kwargs["es_semaphore"] is not None


def test_missing_publication_year_is_sent_as_numeric_sentinel(monkeypatch):
    collection = MagicMock()
    collection.find_one.return_value = None
    db = MagicMock()
    db.__getitem__.return_value = collection
    client = MagicMock()
    client.__getitem__.return_value = db
    es_handler = MagicMock()
    es_handler.search_work.return_value = None
    monkeypatch.setattr(process_module, "process_one_insert", MagicMock())
    work = openalex_work()
    work["publication_year"] = None

    process_module.process_one(
        work,
        {"database_name": "kahi", "openalex_works": {}},
        {},
        client,
        es_handler,
        "threading",
        es_semaphore=BoundedSemaphore(1),
    )

    assert es_handler.search_work.call_args.kwargs["year"] == "0"


def test_es_operations_respect_concurrency_limit():
    semaphore = BoundedSemaphore(2)
    state_lock = Lock()
    active = 0
    maximum_active = 0

    def operation():
        nonlocal active, maximum_active
        with state_lock:
            active += 1
            maximum_active = max(maximum_active, active)
        sleep(0.02)
        with state_lock:
            active -= 1

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(
                process_module.run_es_operation,
                semaphore,
                operation,
            )
            for _ in range(16)
        ]
        for future in futures:
            future.result()

    assert maximum_active == 2


def test_elasticsearch_insert_skips_entry_without_titles():
    es_handler = MagicMock()

    process_module.index_work_in_elasticsearch(
        {"titles": []},
        ObjectId(),
        es_handler,
        BoundedSemaphore(1),
    )

    es_handler.insert_work.assert_not_called()


def test_elasticsearch_insert_uses_numeric_sentinel_for_missing_year():
    es_handler = MagicMock()
    entry = {
        "titles": [{"title": "Work"}],
        "source": {},
        "year_published": None,
        "bibliographic_info": {},
        "authors": [],
    }

    process_module.index_work_in_elasticsearch(
        entry,
        ObjectId(),
        es_handler,
        BoundedSemaphore(1),
    )

    assert es_handler.insert_work.call_args.kwargs["work"]["year"] == 0


def test_update_persists_open_access_and_uses_its_actual_url(monkeypatch):
    entry = {
        "titles": [],
        "external_ids": [],
        "types": [],
        "open_access": {
            "is_open_access": True,
            "open_access_status": "gold",
        },
        "external_urls": [
            {"source": "publisher", "url": "https://publisher.example"},
            {"source": "open_access", "url": "https://open.example/article"},
        ],
        "citations_count": [],
        "subjects": [],
        "authors": [],
    }
    work = {
        "_id": ObjectId(),
        "updated": [],
        "titles": [],
        "external_ids": [],
        "types": [],
        "open_access": {},
        "external_urls": [],
        "citations_count": [],
        "citations_by_year": [],
        "subjects": [],
        "authors": [],
        "bibliographic_info": {},
    }
    collection = MagicMock()
    monkeypatch.setattr(process_module, "parse_openalex", lambda *_args, **_kwargs: entry)

    process_module.process_one_update(
        {}, work, MagicMock(), collection, {},
    )

    update = collection.update_one.call_args.args[1]["$set"]
    assert update["open_access"] == entry["open_access"]
    assert update["external_urls"] == [{
        "provenance": "openalex",
        "source": "open_access",
        "url": "https://open.example/article",
    }]
    assert "open_acess" not in update


def test_update_persists_openalex_abstracts_and_topics(monkeypatch):
    entry = {
        "titles": [], "external_ids": [], "types": [], "open_access": {},
        "external_urls": [], "citations_count": [], "subjects": [],
        "authors": [],
        "abstracts": [{
            "abstract": {"texto": [0]}, "lang": "es", "source": "openalex"
        }],
        "primary_topic": {"id": "https://openalex.org/T1"},
        "topics": [{"id": "https://openalex.org/T1"}],
    }
    work = {
        "_id": ObjectId(), "updated": [], "titles": [], "external_ids": [],
        "types": [], "open_access": {}, "external_urls": [],
        "citations_count": [], "citations_by_year": [], "subjects": [],
        "authors": [], "abstracts": [], "primary_topic": None, "topics": [],
        "bibliographic_info": {},
    }
    collection = MagicMock()
    monkeypatch.setattr(process_module, "parse_openalex", lambda *_a, **_k: entry)

    process_module.process_one_update({}, work, MagicMock(), collection, {})

    update = collection.update_one.call_args.args[1]["$set"]
    assert update["abstracts"] == entry["abstracts"]
    assert update["primary_topic"] == entry["primary_topic"]
    assert update["topics"] == entry["topics"]


def test_rerun_repairs_only_missing_openalex_metadata(monkeypatch):
    entry = {
        "abstracts": [{"abstract": {"word": [0]}, "source": "openalex"}],
        "primary_topic": {"id": "https://openalex.org/T2"},
        "topics": [{"id": "https://openalex.org/T2"}],
    }
    work = {
        "_id": ObjectId(), "updated": [{"source": "openalex", "time": 1}],
        "abstracts": [], "primary_topic": {}, "topics": [],
    }
    collection = MagicMock()
    monkeypatch.setattr(process_module, "parse_openalex", lambda *_a, **_k: entry)

    process_module.process_one_update({}, work, MagicMock(), collection, {})

    update = collection.update_one.call_args.args[1]["$set"]
    assert set(update) == {"abstracts", "primary_topic", "topics"}
    assert len(work["updated"]) == 1


def test_rerun_repairs_core_metadata_without_overwriting(monkeypatch):
    source_id = ObjectId()
    entry = {
        "doi": "10.1000/incoming",
        "date_published": 123456,
        "year_published": 2024,
        "source": {"external_ids": [{"id": "https://openalex.org/S1"}]},
        "abstracts": [],
        "primary_topic": {},
        "topics": [],
    }
    work = {
        "_id": ObjectId(),
        "updated": [{"source": "openalex", "time": 1}],
        "doi": "10.1000/existing",
        "date_published": None,
        "year_published": None,
        "source": {},
        "abstracts": [],
        "primary_topic": {},
        "topics": [],
    }
    sources = MagicMock()
    sources.find_one.return_value = {
        "_id": source_id,
        "names": [{"name": "Journal", "lang": "en"}],
    }
    db = {"sources": sources}
    collection = MagicMock()
    monkeypatch.setattr(
        process_module, "parse_openalex", lambda *_a, **_k: entry)

    process_module.process_one_update({}, work, db, collection, {})

    fields = collection.update_one.call_args.args[1]["$set"]
    assert fields["date_published"] == 123456
    assert fields["year_published"] == 2024
    assert fields["source"] == {"id": source_id, "name": "Journal"}
    assert "doi" not in fields


class FakeExternalIdPersonCollection:
    def __init__(self, people):
        self.people = people

    def find(self, _query, _projection):
        return self.people


def test_multiple_external_ids_choose_person_with_most_matches():
    selected_id = ObjectId()
    people = [
        {
            "_id": selected_id,
            "full_name": "Ana López",
            "external_ids": [
                {"source": "openalex", "id": "A1"},
                {"source": "orcid", "id": "O1"},
            ],
            "affiliations": [],
        },
        {
            "_id": ObjectId(),
            "full_name": "Otra Ana",
            "external_ids": [{"source": "openalex", "id": "A1"}],
            "affiliations": [],
        },
    ]

    person = process_module.resolve_person_by_external_ids(
        FakeExternalIdPersonCollection(people),
        [
            {"source": "openalex", "id": "A1"},
            {"source": "orcid", "id": "O1"},
        ],
    )

    assert person["_id"] == selected_id


def test_existing_author_matches_by_person_id_despite_different_order():
    ana_id = ObjectId()
    existing = [
        {"id": ObjectId(), "full_name": "Luis Pérez", "affiliations": []},
        {"id": ana_id, "full_name": "Ana López", "affiliations": []},
    ]

    matched = process_module.match_existing_work_author(
        MagicMock(),
        {"full_name": "Ana López", "affiliations": []},
        existing,
        person={"_id": ana_id},
    )

    assert matched is existing[1]


def test_existing_author_matches_unique_normalized_name():
    existing = [
        {"id": ObjectId(), "full_name": "Ana María López", "affiliations": []},
        {"id": ObjectId(), "full_name": "Luis Pérez", "affiliations": []},
    ]

    matched = process_module.match_existing_work_author(
        MagicMock(),
        {"full_name": "  ANA MARIA  LOPEZ ", "affiliations": []},
        existing,
    )

    assert matched is existing[0]


def test_ambiguous_name_is_disambiguated_by_affiliation(monkeypatch):
    expected_affiliation = ObjectId()
    existing = [
        {
            "id": ObjectId(),
            "full_name": "Juan Pérez",
            "affiliations": [{"id": ObjectId()}],
        },
        {
            "id": ObjectId(),
            "full_name": "Juan Perez",
            "affiliations": [{"id": expected_affiliation}],
        },
    ]
    monkeypatch.setattr(
        process_module,
        "get_affiliation_ids_by_external_id",
        lambda *_args: {"ROR1": expected_affiliation},
    )

    matched = process_module.match_existing_work_author(
        MagicMock(),
        {
            "full_name": "Juan Pérez",
            "affiliations": [{"external_ids": [{"id": "ROR1"}]}],
        },
        existing,
    )

    assert matched is existing[1]


def test_ambiguous_name_without_affiliation_is_not_guessed(monkeypatch):
    existing = [
        {"id": ObjectId(), "full_name": "Juan Pérez", "affiliations": []},
        {"id": ObjectId(), "full_name": "Juan Perez", "affiliations": []},
    ]
    monkeypatch.setattr(
        process_module,
        "get_affiliation_ids_by_external_id",
        lambda *_args: {},
    )

    matched = process_module.match_existing_work_author(
        MagicMock(),
        {"full_name": "Juan Pérez", "affiliations": []},
        existing,
    )

    assert matched is None

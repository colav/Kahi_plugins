from bson import ObjectId

import mongomock

from kahi_elasticsearch_works.Kahi_elasticsearch_works import Kahi_elasticsearch_works


class BulkRecorder:
    def __init__(self):
        self.entries = []

    def insert_bulk(self, entries):
        self.entries.extend(entries)


def test_bulk_insert_accepts_sparse_work_and_uses_mohan_page_field_names():
    collection = mongomock.MongoClient().db.works
    work_id = ObjectId()
    collection.insert_one(
        {
            "_id": work_id,
            "titles": [{"title": "Trabajo mínimo"}],
            "authors": [{"full_name": "Ada Uno"}],
            "bibliographic_info": {"start_page": "10", "end_page": "20"},
        }
    )
    plugin = object.__new__(Kahi_elasticsearch_works)
    plugin.collection = collection
    plugin.index = "test"
    plugin.bulk_size = 100
    plugin.progress_interval = 100000
    plugin.verbose = 0
    plugin.es_client = BulkRecorder()

    plugin.bulk_insert()

    source = plugin.es_client.entries[0]["_source"]
    assert source["page_start"] == "10"
    assert source["page_end"] == "20"
    assert "start_page" not in source
    assert source["source"] == ""


def test_bulk_insert_reports_low_verbosity_progress(capsys):
    collection = mongomock.MongoClient().db.works
    collection.insert_many([
        {"titles": [{"title": "Uno"}], "authors": []},
        {"titles": [], "authors": []},
        {"titles": [{"title": "Tres"}], "authors": []},
    ])
    plugin = object.__new__(Kahi_elasticsearch_works)
    plugin.collection = collection
    plugin.index = "test"
    plugin.bulk_size = 2
    plugin.progress_interval = 2
    plugin.verbose = 1
    plugin.es_client = BulkRecorder()

    plugin.bulk_insert()

    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == "[elasticsearch_works:bulk_insert] START total=3 | index=test"
    # Los registros cuentan al descartarse o al enviarse su lote.
    assert lines[1].startswith("[elasticsearch_works:bulk_insert] 3/3 (100.0%)")
    assert lines[1].endswith("| omitidos=1 nuevos=2")
    assert lines[2].startswith("[elasticsearch_works:bulk_insert] DONE 3 reg en ")
    assert lines[2].endswith("| omitidos=1 nuevos=2")
    assert len(lines) == 3


def sample_plugin(collection, verbose):
    plugin = object.__new__(Kahi_elasticsearch_works)
    plugin.collection = collection
    plugin.index = "test"
    plugin.bulk_size = 100
    plugin.progress_interval = None
    plugin.verbose = verbose
    plugin.es_client = BulkRecorder()
    return plugin


def test_bulk_insert_zero_verbose_is_silent(capsys):
    collection = mongomock.MongoClient().db.works
    collection.insert_many([{"titles": [{"title": "Uno"}], "authors": []}])
    plugin = sample_plugin(collection, verbose=0)

    plugin.bulk_insert()

    assert len(plugin.es_client.entries) == 1
    assert capsys.readouterr().out == ""


def test_bulk_insert_default_interval_uses_standard(capsys):
    collection = mongomock.MongoClient().db.works
    collection.insert_many(
        [{"titles": [{"title": str(i)}], "authors": []} for i in range(1500)])
    plugin = sample_plugin(collection, verbose=1)

    plugin.bulk_insert()

    lines = capsys.readouterr().out.splitlines()
    assert len(plugin.es_client.entries) == 1500
    assert lines[1].startswith("[elasticsearch_works:bulk_insert] 1000/1500")
    assert lines[2].startswith("[elasticsearch_works:bulk_insert] DONE 1500 reg en ")
    assert len(lines) == 3


class IndicesRecorder:
    def __init__(self):
        self.deleted = []

    def delete(self, index, ignore_unavailable):
        self.deleted.append(index)


def test_delete_reports_start_and_done(capsys):
    plugin = sample_plugin(None, verbose=1)
    plugin.es_client = type("Client", (), {"indices": IndicesRecorder()})()

    plugin.delete()

    lines = capsys.readouterr().out.splitlines()
    assert plugin.es_client.indices.deleted == ["test"]
    assert lines[0] == "[elasticsearch_works:delete] START total=? | index=test"
    assert lines[1].startswith("[elasticsearch_works:delete] DONE 0 reg en ")


def test_bulk_insert_sends_last_batch_when_final_record_has_no_title():
    collection = mongomock.MongoClient().db.works
    collection.insert_many(
        [{"titles": [{"title": str(i)}], "authors": []} for i in range(5)]
        + [{"titles": [], "authors": []}])
    plugin = sample_plugin(collection, verbose=0)
    plugin.bulk_size = 2

    plugin.bulk_insert()

    assert sorted(entry["_source"]["title"] for entry in plugin.es_client.entries) == [
        "0", "1", "2", "3", "4"]

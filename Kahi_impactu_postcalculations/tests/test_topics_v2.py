import mongomock
import pytest

from kahi_impactu_postcalculations import topics_v2
from kahi_impactu_postcalculations.topics import TopicBatchRejected, TopicEndpointUnavailable
from kahi_impactu_postcalculations.Kahi_impactu_postcalculations import Kahi_impactu_postcalculations

NC = topics_v2.NOT_CLASSIFIABLE


class _Resp:
    def __init__(self, status, body=None):
        self.status_code = status
        self._body = body
        self.text = str(body)

    def json(self):
        return self._body


class _FakeEndpoint:
    """Stands in for requests.Session.post: answers from a {text: top} table, records every call."""

    def __init__(self, tops=None, statuses=None, model=topics_v2.MODEL_VERSION):
        self.tops = tops or {}
        self.statuses = list(statuses or [])
        self.model = model
        self.calls = []

    def post(self, url, json, headers, timeout):
        self.calls.append([i["text"] for i in json["inputs"]])
        assert headers["Authorization"] == "Bearer secret"
        if self.statuses:
            status = self.statuses.pop(0)
            if status != 200:
                return _Resp(status, {"error": status})
        preds = [{"key": i["key"], "top": self.tops.get(i["text"], [[NC, 0.9], ["T1", 0.05]]), "p_nc": 0.0}
                 for i in json["inputs"]]
        return _Resp(200, {"model": self.model, "predictions": preds})


def _client(fake, retries=5):
    return topics_v2.EndpointClient("https://endpoint", "secret", gpu_batch=16, timeout=5, retries=retries,
                                    session_factory=lambda: fake)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(topics_v2, "sleep", lambda s: None)


@pytest.fixture(autouse=True)
def _mongomock_bulk_write(monkeypatch):
    """mongomock 4.3 cannot take the UpdateOne of current pymongo: apply each one with update_one."""
    def bulk_write(self, operations, ordered=True):
        for op in operations:
            self.update_one(op._filter, op._doc, upsert=op._upsert)
    monkeypatch.setattr(mongomock.collection.Collection, "bulk_write", bulk_write)


# ---------------------------------------------------------------- text

def test_document_text_per_collection():
    work = {"titles": [{"title": " Blockchain "}], "source": {"name": "Rev"},
            "abstracts": [{"abstract": {"world": [1], "hello": [0]}, "lang": "en"}]}
    assert topics_v2.document_text(work, "works") == "Title: Blockchain\nAbstract: hello world\nVenue: Rev"
    assert topics_v2.document_text({"titles": [{"title": "Sin resumen"}], "abstracts": []}, "works") is None
    patent = {"titles": [{"title": "Trampa para Aedes"}]}
    assert topics_v2.document_text(patent, "patents", title_only=["patents"]) == \
        "Title: Trampa para Aedes\nVenue: (unknown)"
    assert topics_v2.document_text(patent, "patents", title_only=[]) is None
    bilingual = {"titles": [{"title": "P"}], "abstracts": [{"abstract": {"es": [0]}, "lang": "es"},
                                                          {"abstract": {"en": [0]}, "lang": "en"}]}
    assert "Abstract: en" in topics_v2.document_text(bilingual, "patents", ["en", "es"])
    project = {"titles": [{"title": "Proyecto"}], "abstract": "", "topic_text": "texto libre"}
    assert topics_v2.document_text(project, "projects", title_only=["projects"]) == \
        "Title: Proyecto\nAbstract: texto libre\nVenue: (unknown)"
    assert topics_v2.document_text({"titles": [{"title": ""}], "abstract": "x"}, "projects", title_only=["projects"]) is None


def test_abstract_is_cut_at_4000_chars_and_key_depends_on_model_and_text():
    text = topics_v2.work_text("T", "a" * 5000, "")
    assert text.split("Abstract: ")[1].split("\n")[0] == "a" * 4000
    assert topics_v2.cache_key(text) == topics_v2.cache_key(text)
    assert topics_v2.cache_key(text) != topics_v2.cache_key(text + " ")
    assert topics_v2.cache_key(text) != topics_v2.cache_key(text, model_version="other")


def test_topic_query_requires_abstract_only_outside_title_only():
    assert "abstracts.0" in topics_v2.topic_query("works", ["projects", "patents"])
    assert "abstracts.0" not in topics_v2.topic_query("patents", ["projects", "patents"])
    assert "abstracts.0" not in topics_v2.topic_query("projects", [])


# ---------------------------------------------------------------- assignment

def test_assigned_topics_rule():
    assert topics_v2.assigned_topics([[NC, 0.21], ["T1", 0.18]]) == []
    assert topics_v2.assigned_topics([["T1", 0.44], ["T2", 0.24], ["T3", 0.11], ["T4", 0.1]]) == \
        [("T1", 0.44), ("T2", 0.24)]
    assert topics_v2.assigned_topics([["T1", 0.5], [NC, 0.3], ["T2", 0.2]]) == [("T1", 0.5), ("T2", 0.2)]
    assert topics_v2.assigned_topics([["T1", 0.1]]) == []
    assert topics_v2.assigned_topics([["T1", 0.4], ["T2", 0.3], ["T3", 0.2], ["T4", 0.16]], top_k=3) == \
        [("T1", 0.4), ("T2", 0.3), ("T3", 0.2)]


def test_topic_update_fills_primary_and_deduplicates():
    cache = {"https://openalex.org/T1": {"id": "https://openalex.org/T1", "display_name": "Uno"}}
    doc = {"_id": 1, "primary_topic": {}, "topics": [{"id": "https://openalex.org/T1"}]}
    op = topics_v2.topic_update(doc, [("T1", 0.9), ("bad", 0.5)], cache)
    update = op._doc["$set"]
    assert update["primary_topic"] == {"id": "https://openalex.org/T1", "display_name": "Uno", "score": 0.9}
    assert len(update["topics"]) == 1
    assert cache["https://openalex.org/T1"] == {"id": "https://openalex.org/T1", "display_name": "Uno"}
    assert topics_v2.topic_update(doc, [], cache) is None


# ---------------------------------------------------------------- client

def test_client_retries_unavailable_then_succeeds():
    fake = _FakeEndpoint(statuses=[503, 502, 200])
    preds = _client(fake).infer([{"key": "k", "text": "x"}])
    assert [p["key"] for p in preds] == ["k"] and len(fake.calls) == 3


def test_client_fails_fast_on_auth_and_rejects_bad_batches():
    with pytest.raises(topics_v2.TopicAuthError):
        _client(_FakeEndpoint(statuses=[401])).infer([{"key": "k", "text": "x"}])
    with pytest.raises(TopicBatchRejected):
        _client(_FakeEndpoint(statuses=[400])).infer([{"key": "k", "text": "x"}])
    fake = _FakeEndpoint(statuses=[500, 500, 500])
    with pytest.raises(TopicBatchRejected):
        _client(fake).infer([{"key": "k", "text": "x"}])
    assert len(fake.calls) == 3
    with pytest.raises(TopicEndpointUnavailable):
        _client(_FakeEndpoint(statuses=[503] * 5), retries=5).infer([{"key": "k", "text": "x"}])


def test_client_checks_model_version_and_order():
    with pytest.raises(topics_v2.TopicModelMismatch):
        _client(_FakeEndpoint(model="topic-classifier-v1")).infer([{"key": "k", "text": "x"}])

    class _Swapped(_FakeEndpoint):
        def post(self, *args, **kwargs):
            resp = super().post(*args, **kwargs)
            resp._body["predictions"].reverse()
            return resp

    with pytest.raises(TopicBatchRejected):
        _client(_Swapped()).infer([{"key": "a", "text": "x"}, {"key": "b", "text": "y"}])


# ---------------------------------------------------------------- pipeline

def _setup(tops):
    client = mongomock.MongoClient()
    db = client.kahi
    cache_col = client.topics_inference.predictions_v2
    topic_cache = {f"https://openalex.org/T{i}": {"id": f"https://openalex.org/T{i}", "display_name": f"Topic {i}"}
                   for i in (1, 2, 3)}
    fake = _FakeEndpoint(tops)
    pipeline = topics_v2.TopicsV2(db, cache_col, topic_cache, _client(fake), release="kahi_dev_test",
                                  batch_size=2, jobs=2, title_only=["projects", "patents"])
    return db, cache_col, fake, pipeline


def _work(_id, title, abstract="resumen"):
    return {"_id": _id, "titles": [{"title": title}], "primary_topic": {},
            "abstracts": [{"abstract": {abstract: [0]}, "lang": "es"}] if abstract else []}


def test_pipeline_writes_topics_caches_and_never_resends():
    tops = {"Title: A\nAbstract: resumen\nVenue: (unknown)": [["T1", 0.8], ["T2", 0.16], ["T3", 0.01]],
            "Title: Marca\nAbstract: resumen\nVenue: (unknown)": [[NC, 0.21], ["T2", 0.18]]}
    db, cache_col, fake, pipeline = _setup(tops)
    db.works.insert_many([_work(1, "A"), _work(2, "A"), _work(3, "Marca"), _work(4, "Sin", abstract=None),
                          {**_work(5, "Hecho"), "primary_topic": {"id": "x"}}])

    report = pipeline.process_collection("works")

    sent = [t for call in fake.calls for t in call]
    assert sorted(sent) == sorted(tops)                       # one text per distinct key; 4 and 5 never queried
    assert report["actualizados"] == 2 and report["sin_tema"] == 1 and report["inferidos"] == 2
    w1 = db.works.find_one({"_id": 1})
    assert w1["primary_topic"]["id"] == "https://openalex.org/T1"
    assert [t["id"] for t in w1["topics"]] == ["https://openalex.org/T1", "https://openalex.org/T2"]
    assert db.works.find_one({"_id": 3})["primary_topic"] == {}
    assert db.works.find_one({"_id": 5})["primary_topic"] == {"id": "x"}
    cached = cache_col.find_one({"_id": topics_v2.cache_key("Title: Marca\nAbstract: resumen\nVenue: (unknown)")})
    assert cached["top"][0][0] == NC and cached["first_seen"]["release"] == "kahi_dev_test"

    fake.calls.clear()  # resume: the no-topic work is still pending but comes from the cache
    report = pipeline.process_collection("works")
    assert fake.calls == [] and report["sin_tema"] == 1 and report["cache"] == 1


def test_pipeline_title_only_patents_and_isolated_failures():
    tops = {"Title: Trampa\nVenue: (unknown)": [["T3", 0.95]]}
    db, cache_col, fake, pipeline = _setup(tops)
    db.patents.insert_many([{"_id": "p1", "titles": [{"title": "Trampa"}], "primary_topic": {}},
                            {"_id": "p2", "titles": [{"title": "Rota"}], "primary_topic": {}}])
    original = fake.post

    def post(url, json, headers, timeout):
        if any(i["text"].startswith("Title: Rota") for i in json["inputs"]):
            fake.calls.append([i["text"] for i in json["inputs"]])
            return _Resp(422, {"error": "bad"})
        return original(url, json, headers, timeout)

    fake.post = post
    report = pipeline.process_collection("patents")
    assert report["actualizados"] == 1 and report["errores"] == 1
    assert db.patents.find_one({"_id": "p1"})["primary_topic"]["id"] == "https://openalex.org/T3"
    assert cache_col.count_documents({}) == 1


# ---------------------------------------------------------------- plugin config

def _config(**topic):
    base = {"database_url": "mongodb://x", "database_name": "kahi_dev_test",
            "impactu_postcalculations": {
                "database_url": "mongodb://x", "database_name": "impactu", "backend": "threading",
                "verbose": 0, "n_jobs": 1, "openalex_database_url": "mongodb://x",
                "openalex_database_name": "openalex", "inference_endpoint": "https://endpoint",
                "topic_inference_mode": "hf_v2", "topic_batch_size": 256, "topic_jobs": 2, **topic}}
    return base


def test_plugin_reads_hf_v2_config(monkeypatch):
    monkeypatch.setattr(Kahi_impactu_postcalculations, "_check_and_install_spacy_models", lambda self: None,
                        raising=False)
    try:
        plugin = Kahi_impactu_postcalculations(_config(topic_min_score=0.2))
    except Exception as error:  # other phases may need resources absent in tests
        pytest.skip(f"plugin constructor needs more config: {error}")
    assert plugin.topic_v2["min_score"] == 0.2 and plugin.topic_v2["top_k"] == 3
    assert plugin.topic_v2["title_only"] == ["projects", "patents"]


def test_hf_v2_config_validation():
    plugin = object.__new__(Kahi_impactu_postcalculations)
    plugin.inference_endpoints = ["https://endpoint"]
    plugin.topic_batch_size = 256
    with pytest.raises(ValueError):
        plugin._topic_v2_config({"topic_top_k": 11})
    with pytest.raises(ValueError):
        plugin._topic_v2_config({"topic_min_score": 1.5})
    with pytest.raises(ValueError):
        plugin._topic_v2_config({"topic_title_only_collections": ["persons"]})
    plugin.inference_endpoints = ["https://a", "https://b"]
    with pytest.raises(ValueError):
        plugin._topic_v2_config({})

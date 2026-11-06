from types import SimpleNamespace

import pytest

from kahi_impactu_postcalculations.Kahi_impactu_postcalculations import (
    Kahi_impactu_postcalculations,
)


class _Admin:
    def command(self, name):
        assert name == "ping"
        return {"ok": 1}


class _Cursor(list):
    def limit(self, count):
        return _Cursor(self[:count])


class _Works:
    def aggregate(self, pipeline):
        return [{"preflight_sort": [1, 2]}]

    def find(self, query, projection):
        return _Cursor([{
            "titles": [{"title": "Prueba"}],
            "abstracts": [{"abstract": {"texto": [0]}}],
            "source": {},
        }])


class _Topics:
    def estimated_document_count(self):
        return 1

    def count_documents(self, query, limit=0):
        return int(query.get("id") == "https://openalex.org/T123")


class _Database:
    def __init__(self, collections):
        self.collections = collections
        self.client = SimpleNamespace(admin=_Admin())

    def list_collection_names(self):
        return list(self.collections)

    def __getitem__(self, name):
        return self.collections[name]


class _Response:
    def raise_for_status(self):
        return None

    def json(self):
        return [[{"topic_id": 123, "topic_score": 0.9}]]


class _NoClassificationResponse(_Response):
    def json(self):
        return [[{"topic_id": -1, "topic_score": 0.0}]]


def _plugin():
    plugin = object.__new__(Kahi_impactu_postcalculations)
    plugin.database_name = "input"
    plugin.impactu_database_name = "output"
    plugin.openalex_database_name = "openalex"
    plugin.type_catalog = SimpleNamespace(payload={"mappings": [{}]})
    plugin.openalex_enrichment_enabled = True
    plugin.top_words_enabled = True
    plugin.top_words_strategy = "works_scan"
    plugin.es_model = lambda text: [text]
    plugin.en_model = lambda text: [text]
    plugin.topics_enabled = True
    plugin.inference_endpoint = "http://topics/invocations"
    plugin.inference_endpoints = ["http://topics/invocations"]
    plugin.topic_request_timeout = 300
    plugin.check_topic_inference_service = lambda: None
    return plugin


def test_preflight_validates_enabled_phases(monkeypatch):
    plugin = _plugin()
    source = _Database({
        "works": _Works(), "person": object(), "affiliations": object(),
    })
    output = _Database({})
    openalex = _Database({"topics": _Topics(), "works": object()})
    monkeypatch.setattr(
        "kahi_impactu_postcalculations.Kahi_impactu_postcalculations."
        "request_topic_inference_batch",
        lambda *args: _Response(),
    )

    plugin.preflight(source, output, openalex)


def test_preflight_fails_before_processing_when_collection_is_missing():
    plugin = _plugin()
    source = _Database({"works": _Works(), "person": object()})

    with pytest.raises(RuntimeError, match="affiliations"):
        plugin.preflight(
            source, _Database({}),
            _Database({"topics": _Topics(), "works": object()}),
        )


def test_preflight_accepts_model_no_classification_marker(monkeypatch):
    plugin = _plugin()
    source = _Database({
        "works": _Works(), "person": object(), "affiliations": object(),
    })
    monkeypatch.setattr(
        "kahi_impactu_postcalculations.Kahi_impactu_postcalculations."
        "request_topic_inference_batch",
        lambda *args: _NoClassificationResponse(),
    )

    plugin.preflight(
        source, _Database({}),
        _Database({"topics": _Topics(), "works": object()}),
    )

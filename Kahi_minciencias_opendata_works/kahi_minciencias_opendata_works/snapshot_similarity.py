"""Elasticsearch candidate retrieval for the non-DOI snapshot partition."""

from collections import OrderedDict
from copy import deepcopy
from hashlib import sha256

from bson import BSON
from mohan.Similarity import Similarity

from kahi_minciencias_opendata_works.process_one import build_es_work


def _es_work(document):
    work = build_es_work(document)
    if work:
        # Match the current bulk indexer and Mohan's search field names.
        work["page_start"] = work.pop("first_page")
        work["page_end"] = work.pop("last_page")
    return work


class SnapshotSimilarity:
    """Reuse Mohan's query/index contract; matching stays in the importer."""

    def __init__(self, config):
        auth = ((config["es_user"], config["es_password"])
                if config.get("es_user") else None)
        self.handler = Similarity(config["es_index"], es_uri=config["es_url"], es_auth=auth)
        self._indexed = OrderedDict()

    def candidates(self, document):
        work = _es_work(document)
        if not work:
            return [], 0
        hits, queries = {}, 0
        titles = dict.fromkeys(item.get("title") for item in document.get("titles", [])
                               if item.get("title"))
        for title in titles:
            result = self.handler.search_work(
                title=title, source=work["source"], year=str(work["year"]),
                authors=work["authors"], volume=work["volume"], issue=work["issue"],
                page_start=work["page_start"], page_end=work["page_end"],
                use_es_thold=True, es_thold=0, hits=20,
            )
            queries += 1
            for hit in result or []:
                if hit.get("_id") is not None:
                    hits[str(hit["_id"])] = hit
        return list(hits), queries

    def sync(self, document):
        work = _es_work(document)
        if not work:
            return
        identifier = str(document["_id"])
        fingerprint = sha256(BSON.encode(work)).hexdigest()
        if self._indexed.get(identifier) == fingerprint:
            return
        # Mohan refreshes after this write. Another source row can therefore
        # find this work before its next insertion decision. Errors propagate
        # so an unavailable index never silently becomes "no candidates".
        self.handler.insert_work(_id=identifier, work=deepcopy(work))
        self._indexed[identifier] = fingerprint
        self._indexed.move_to_end(identifier)
        if len(self._indexed) > 4096:
            self._indexed.popitem(last=False)

"""Topic inference with the OpenAlex topic classifier v2 behind a Hugging Face Inference Endpoint
(``topic_inference_mode: hf_v2``), with a persistent, cross-release prediction cache.

Each document becomes the exact text the model was trained on (``work_text`` of
ourresearch/openalex-topic-classification v2); the cache key is the sha256 of model version, text
recipe and text. A text already in the cache is never sent again: not in a resumed run, not in the
next release. The cache keeps the raw top-10 of every text, so the assignment rule (``top_k``,
``min_score``) can change later without new inference.

Endpoint contract (handler.py of the private model repo):
  POST <endpoint>  Authorization: Bearer <token>
       {"inputs": [{"key": "<sha256>", "text": "..."}], "parameters": {"top": 10, "batch": 16}}
  -> {"model": "topic-classifier-v2@98df3b9", "runtime": {...},
      "predictions": [{"key": "...", "top": [["T14423", 0.91], ...], "p_nc": 0.01}, ...]}  # input order
"""
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from threading import Event, Lock, local
from time import sleep

import requests
from pymongo import UpdateOne

from kahi.progress import ProgressLogger
from kahi_impactu_postcalculations.topics import (
    TopicBatchRejected,
    TopicEndpointUnavailable,
    get_openalex_topic,
)

MODEL_VERSION = "topic-classifier-v2@98df3b9"
# Bump whenever document_text/work_text changes: it is part of the cache key.
TEXT_RECIPE = "work_text-v1"
NOT_CLASSIFIABLE = "NOT_CLASSIFIABLE"
CACHE_TOP = 10  # predictions kept per text in the cache
PENDING_TOPIC = {"primary_topic": {"$in": [{}, None]}}
PROJECTION = {"titles": 1, "abstracts": 1, "abstract": 1, "topic_text": 1, "source": 1,
              "primary_topic": 1, "topics": 1}


class TopicAuthError(RuntimeError):
    """The endpoint rejected the token (HTTP 401/403): retrying cannot help."""


class TopicModelMismatch(RuntimeError):
    """The endpoint serves another model version than the configured one."""


# ---------------------------------------------------------------- text and cache key

def inverted_to_text(index):
    """OpenAlex-style abstract_inverted_index -> plain text."""
    if not isinstance(index, dict) or not index:
        return ""
    positions = {}
    for word, places in index.items():
        for place in places or []:
            positions[place] = word
    return " ".join(positions[p] for p in sorted(positions))


def work_text(title, abstract, venue):
    """The exact input the v2 student was trained on (v2/student/model.py upstream)."""
    parts = [f"Title: {title or '(none)'}"]
    if abstract:
        parts.append(f"Abstract: {abstract[:4000]}")
    parts.append(f"Venue: {venue or '(unknown)'}")
    return "\n".join(parts)


def cache_key(text, model_version=MODEL_VERSION):
    return hashlib.sha256(f"{model_version}\n{TEXT_RECIPE}\n{text}".encode("utf-8")).hexdigest()


def document_text(doc, collection, abstract_langs=(), title_only=()):
    """Model text of a Kahi document, or None when it must not be sent.

    A title is always required. Collections in ``title_only`` are sent with the title alone when
    they have no abstract; the others need an abstract. ``projects`` carry a plain-text
    ``abstract`` (else ``topic_text``); works and patents a list of inverted indexes, of which the
    first in ``abstract_langs`` order is used.
    """
    titles = doc.get("titles") or []
    title = titles[0].get("title") if titles and isinstance(titles[0], dict) else None
    if not isinstance(title, str) or not title.strip():
        return None
    if "abstracts" not in doc and ("abstract" in doc or "topic_text" in doc):
        abstract = next((doc[f] for f in ("abstract", "topic_text")
                         if isinstance(doc.get(f), str) and doc[f].strip()), "")
    else:
        abstracts = [a for a in (doc.get("abstracts") or []) if isinstance(a, dict)]
        order = {lang: i for i, lang in enumerate(abstract_langs or [])}
        abstracts.sort(key=lambda a: order.get(a.get("lang"), len(order)))
        abstract = inverted_to_text(abstracts[0].get("abstract")) if abstracts else ""
    abstract = abstract.strip()
    if not abstract and collection not in title_only:
        return None
    venue = ((doc.get("source") or {}).get("name") or "").strip()
    return work_text(title.strip(), abstract, venue)


def topic_query(collection, title_only=()):
    query = {**PENDING_TOPIC, "titles.0": {"$exists": True}}
    if collection not in title_only and collection != "projects":
        query["abstracts.0"] = {"$exists": True}
    return query


# ---------------------------------------------------------------- assignment

def assigned_topics(top, top_k=3, min_score=0.15):
    """Topics to write: none when NOT_CLASSIFIABLE ranks first; else up to ``top_k`` with p >= min_score."""
    if not top or top[0][0] == NOT_CLASSIFIABLE:
        return []
    return [(tid, p) for tid, p in top[:top_k] if tid != NOT_CLASSIFIABLE and p >= min_score]


def topic_update(doc, assigned, topic_cache):
    """UpdateOne that fills an empty primary_topic and appends the new topics, or None."""
    current = list(doc.get("topics") or [])
    current_ids = {t.get("id") for t in current if isinstance(t, dict)}
    primary = doc.get("primary_topic") or {}
    for tid, score in assigned:
        if not (isinstance(tid, str) and tid.startswith("T") and tid[1:].isdigit()):
            continue
        topic = get_openalex_topic(topic_cache, {"topic_id": tid[1:], "topic_score": score})
        if not primary:
            primary = topic
        if topic.get("id") not in current_ids:
            current.append(topic)
            current_ids.add(topic.get("id"))
    if not primary:
        return None
    return UpdateOne({"_id": doc["_id"], **PENDING_TOPIC},
                     {"$set": {"primary_topic": primary, "topics": current}})


# ---------------------------------------------------------------- endpoint client

class EndpointClient:
    """Thread-safe client: one requests.Session per thread, token read once from the environment."""

    def __init__(self, url, token, model_version=MODEL_VERSION, gpu_batch=16, timeout=300, retries=240,
                 session_factory=requests.Session):
        if not url or not token:
            raise ValueError("hf_v2 needs an endpoint URL and a token")
        self.session_factory = session_factory
        self.url = url
        self.headers = {"Authorization": f"Bearer {token}"}
        self.model_version = model_version
        self.gpu_batch = gpu_batch
        self.timeout = timeout
        self.retries = retries
        self._local = local()

    def _session(self):
        if not hasattr(self._local, "session"):
            self._local.session = self.session_factory()
        return self._local.session

    def infer(self, items, retries=None):
        """POST items [{key, text}] and return predictions in the same order.

        401/403 -> TopicAuthError; 400/413/422 or a repeated 500 -> TopicBatchRejected (split it);
        network errors and 502/503/504 (e.g. waking from scale-to-zero) are retried, then
        TopicEndpointUnavailable."""
        retries = retries or self.retries
        payload = {"inputs": items, "parameters": {"top": CACHE_TOP, "batch": self.gpu_batch}}
        last, errors_500 = None, 0
        for attempt in range(1, retries + 1):
            try:
                r = self._session().post(self.url, json=payload, headers=self.headers, timeout=self.timeout)
            except requests.RequestException as error:
                last = error
            else:
                if r.status_code == 200:
                    return self._validate(items, r)
                last = RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
                if r.status_code in (401, 403):
                    raise TopicAuthError(f"Topic endpoint rejected the token: {last}")
                if r.status_code in (400, 413, 422):
                    raise TopicBatchRejected(f"Topic endpoint rejected the batch: {last}")
                if r.status_code == 500:
                    errors_500 += 1
                    if errors_500 >= 3:
                        raise TopicBatchRejected(f"Topic endpoint failed the batch: {last}")
            if attempt < retries:
                print(f"WARNING: topic endpoint attempt {attempt}/{retries} failed ({last}); retrying", flush=True)
                sleep(min(attempt, 30))
        raise TopicEndpointUnavailable(f"Topic endpoint unavailable after {retries} attempts") from last

    def _validate(self, items, response):
        try:
            body = response.json()
        except ValueError as error:
            raise TopicBatchRejected("Topic endpoint returned invalid JSON") from error
        if not isinstance(body, dict) or "predictions" not in body:
            raise TopicBatchRejected("Topic endpoint response has no predictions")
        if body.get("model") != self.model_version:
            raise TopicModelMismatch(f"endpoint serves {body.get('model')!r}, expected {self.model_version!r}")
        preds = body["predictions"]
        if not isinstance(preds, list) or [p.get("key") for p in preds] != [i["key"] for i in items]:
            raise TopicBatchRejected("Topic endpoint response size/order mismatch")
        return preds


# ---------------------------------------------------------------- pipeline

class TopicsV2:
    def __init__(self, db, cache_col, topic_cache, client, release, top_k=3, min_score=0.15,
                 batch_size=256, jobs=2, abstract_langs=None, title_only=("projects", "patents")):
        self.db = db
        self.cache_col = cache_col
        self.topic_cache = topic_cache
        self.client = client
        self.release = release
        self.top_k = top_k
        self.min_score = min_score
        self.batch_size = batch_size
        self.jobs = jobs
        self.abstract_langs = abstract_langs or {}
        self.title_only = tuple(title_only)

    def _infer_resilient(self, items):
        """{key: prediction} for the items that could be inferred, plus the keys that failed alone."""
        try:
            return {p["key"]: p for p in self.client.infer(items)}, set()
        except TopicBatchRejected:
            if len(items) == 1:
                print(f"ERROR: topic inference skipped irrecoverable text key={items[0]['key']}", flush=True)
                return {}, {items[0]["key"]}
            mid = len(items) // 2
            print(f"WARNING: splitting rejected topic batch {len(items)} into {mid}+{len(items) - mid}", flush=True)
            left, lf = self._infer_resilient(items[:mid])
            right, rf = self._infer_resilient(items[mid:])
            return {**left, **right}, lf | rf

    def process_batch(self, collection, docs):
        """Infer (or reuse) and persist topics for one batch. Returns per-document outcomes and counters."""
        langs = self.abstract_langs.get(collection) or []
        keyed, texts = [], {}
        for doc in docs:
            text = document_text(doc, collection, langs, self.title_only)
            key = cache_key(text) if text is not None else None
            keyed.append((doc, key))
            if key is not None:
                texts[key] = text
        cached = {c["_id"]: c for c in self.cache_col.find({"_id": {"$in": list(texts)}}, {"top": 1})}
        misses = [{"key": k, "text": t} for k, t in texts.items() if k not in cached]
        new, failed = self._infer_resilient(misses) if misses else ({}, set())
        if new:
            now = datetime.now(timezone.utc)
            first = {key: (doc["_id"]) for doc, key in keyed if key in new}
            self.cache_col.bulk_write([
                UpdateOne({"_id": k}, {"$setOnInsert": {
                    "model": self.client.model_version, "recipe": TEXT_RECIPE,
                    "top": p["top"], "p_nc": p.get("p_nc"), "created": now,
                    "first_seen": {"release": self.release, "collection": collection, "doc_id": first[k]},
                }}, upsert=True)
                for k, p in new.items()
            ], ordered=False)
        tops = {k: c["top"] for k, c in cached.items()}
        tops.update({k: p["top"] for k, p in new.items()})
        ops, outcomes = [], []
        for doc, key in keyed:
            if key is None:
                outcomes.append("omitidos")
            elif key in failed or key not in tops:
                outcomes.append("errores")
            else:
                op = topic_update(doc, assigned_topics(tops[key], self.top_k, self.min_score), self.topic_cache)
                if op is None:
                    outcomes.append("sin_tema")
                else:
                    ops.append(op)
                    outcomes.append("actualizados")
        if ops:
            self.db[collection].bulk_write(ops, ordered=False)
        return outcomes, len(new), len(cached)

    def process_collection(self, collection):
        """All pending documents of ``collection``. Ids are listed first, so no cursor stays open for hours."""
        query = topic_query(collection, self.title_only)
        ids = [d["_id"] for d in self.db[collection].find(query, {"_id": 1})]
        progress = ProgressLogger(f"impactu_postcalculations:topics:{collection}", total=len(ids))
        progress.start(f"modelo={self.client.model_version} lote={self.batch_size} trabajadores={self.jobs}")
        chunks = iter([ids[s:s + self.batch_size] for s in range(0, len(ids), self.batch_size)])
        chunk_lock, stats_lock, stop = Lock(), Lock(), Event()
        stats = {"inferidos": 0, "cache": 0}

        def worker():
            while not stop.is_set():
                with chunk_lock:
                    chunk = next(chunks, None)
                if chunk is None:
                    return
                try:
                    docs = list(self.db[collection].find({"_id": {"$in": chunk}, **PENDING_TOPIC}, PROJECTION))
                    outcomes, n_new, n_cached = self.process_batch(collection, docs)
                except BaseException:
                    stop.set()
                    raise
                with stats_lock:
                    stats["inferidos"] += n_new
                    stats["cache"] += n_cached
                for outcome in set(outcomes):
                    progress.step(outcome, outcomes.count(outcome))
                if len(chunk) > len(docs):  # already done by someone else since the id listing
                    progress.step("existentes", len(chunk) - len(docs))

        with ThreadPoolExecutor(max_workers=self.jobs) as executor:
            for future in as_completed([executor.submit(worker) for _ in range(self.jobs)]):
                future.result()
        progress.done(**{f"textos_{k}": v for k, v in stats.items()})
        return {**progress.counts, **stats}


def token_from_env(name):
    token = os.environ.get(name or "")
    if not token:
        raise RuntimeError(f"hf_v2 topic inference needs the token in the environment variable {name!r}")
    return token

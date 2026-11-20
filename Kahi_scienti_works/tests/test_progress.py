import kahi_scienti_works.Kahi_scienti_works as plugin_module


DOCS = {
    1: {"_id": 1, "outcome": "nuevos"},
    2: {"_id": 2, "outcome": None},  # DOI conflict skipped by the decorator
    3: {"_id": 3, "outcome": "actualizados"},
}


class FakeScienti:
    def __init__(self, groups):
        self.groups = groups

    def aggregate(self, _pipeline, **_kwargs):
        return iter(self.groups)

    def find_one(self, query):
        return DOCS[query["_id"]]

    def find(self, query):
        if "_id" in query:
            return [DOCS[i] for i in query["_id"]["$in"]]
        return [DOCS[3]]

    def count_documents(self, _query):
        return 1


class FakeClient:
    def __init__(self, scienti):
        self.scienti = scienti

    def __getitem__(self, _name):
        return {"product": self.scienti}

    def close(self):
        pass


def fake_process_one(reg, *_args, **_kwargs):
    return reg["outcome"]


def run_scienti(monkeypatch, task, groups):
    monkeypatch.setattr(plugin_module, "MongoClient", lambda _url: FakeClient(FakeScienti(groups)))
    monkeypatch.setattr(plugin_module, "process_one", fake_process_one)
    instance = object.__new__(plugin_module.Kahi_scienti_works)
    instance.task = task
    instance.n_jobs = 2
    instance.verbose = 0
    instance.es_handler = None
    instance.es_semaphore = None
    instance.empty_work = lambda: {}
    instance.process_scienti(None, None, {
        "database_url": "unused",
        "database_name": "scienti_udea_2024",
        "collection_name": "product",
    })


def test_doi_task_counts_records_of_each_doi_group(monkeypatch, capsys):
    run_scienti(monkeypatch, "doi", [
        {"_id": "10.1/a", "ids": [1, 2]},
        {"_id": "10.1/b", "ids": [3]},
    ])
    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == ("[scienti_works:doi_scienti_udea_2024] START total=3 | "
                        "collection=product doi_groups=2 num_jobs=2")
    assert lines[-1].startswith("[scienti_works:doi_scienti_udea_2024] DONE 3 reg en ")
    for count in ("nuevos=1", "omitidos=1", "actualizados=1"):
        assert count in lines[-1]


def test_no_doi_task_reports_each_phase(monkeypatch, capsys):
    run_scienti(monkeypatch, None, [
        {"_id": {"doi": "0", "web_doi": None}, "ids": [1, 2]},
        {"_id": {"doi": "10.1234/abc", "web_doi": None}, "ids": [3]},
    ])
    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == ("[scienti_works:doi_invalido_scienti_udea_2024] START total=2 | "
                        "collection=product num_jobs=2")
    assert lines[1].startswith("[scienti_works:doi_invalido_scienti_udea_2024] DONE 2 reg en ")
    assert "nuevos=1" in lines[1] and "omitidos=1" in lines[1]
    assert lines[2] == ("[scienti_works:sin_doi_scienti_udea_2024] START total=1 | "
                        "collection=product num_jobs=2")
    assert lines[3].startswith("[scienti_works:sin_doi_scienti_udea_2024] DONE 1 reg en ")
    assert lines[3].endswith("| actualizados=1")
    assert len(lines) == 4

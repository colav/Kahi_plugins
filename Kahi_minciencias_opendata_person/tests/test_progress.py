import copy

import mongomock
import pytest

from test_person_snapshot_ingestion import (
    Plugin, config, legacy_module, legacy_recognized_plugin, person_snapshot,
    prepare,
)


def add_second_person(source, **changes):
    second = copy.deepcopy(person_snapshot())
    second["_id"] = "0000000002"
    second["external_ids"][0]["id"]["COD_RH"] = "0000000002"
    second.update(changes)
    source.persons_final.insert_one(second)
    source.scienti_final_release_audits.update_one(
        {"_id": "release_six_audit"},
        {"$set": {"evidence.persons.documents": 2}},
    )
    return second


def test_snapshot_progress_counts_outcomes(monkeypatch, capsys):
    _, source = prepare(monkeypatch)
    add_second_person(source, full_name="")

    Plugin(config(batch_size=1, verbose=1)).run()

    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == (
        "[minciencias_opendata_person] START total=2 | "
        "release=release_six batch_size=1"
    )
    assert len(lines) == 2
    assert lines[1].startswith("[minciencias_opendata_person] DONE 2 reg en ")
    assert lines[1].endswith("| nuevos=1 omitidos=1")


def test_snapshot_progress_resumes_from_checkpoint(monkeypatch, capsys):
    _, source = prepare(monkeypatch)
    second = add_second_person(source)
    source.persons_final.update_one(
        {"_id": "0000000002"}, {"$unset": {"birthdate": ""}})
    with pytest.raises(RuntimeError, match="does not follow the Kahi schema"):
        Plugin(config(batch_size=1, verbose=1)).run()
    assert "DONE" not in capsys.readouterr().out

    source.persons_final.replace_one({"_id": "0000000002"}, second)
    Plugin(config(batch_size=1, verbose=1)).run()

    lines = capsys.readouterr().out.splitlines()
    assert lines[0].startswith(
        "[minciencias_opendata_person] START total=2 | reanuda_desde=1 | ")
    assert lines[-1].startswith("[minciencias_opendata_person] DONE 2 reg en ")
    assert lines[-1].endswith("| nuevos=1")


def test_snapshot_progress_is_silent_without_verbose(monkeypatch, capsys):
    prepare(monkeypatch)
    Plugin(config()).run()
    assert capsys.readouterr().out == ""


def test_legacy_recognized_progress_uses_interval_and_outcomes(capsys):
    client = mongomock.MongoClient()
    plugin = legacy_recognized_plugin(client)
    plugin.verbose = 1
    plugin.progress_interval = 2
    plugin.recognized_researchers = client.dam.recognized_researchers
    record = {
        "cod_rh": "0000000001", "documento_normalizado": "12345",
        "tipo_consolidado": "Cédula de ciudadanía",
        "nombre_completo": "Ada Lovelace",
    }
    plugin.recognized_researchers.insert_many([
        dict(record), dict(record), dict(record, cod_rh="invalid"),
    ])

    counters = plugin.process_recognized_researchers()

    lines = capsys.readouterr().out.splitlines()
    name = "[minciencias_opendata_person:legacy_recognized]"
    assert counters["processed"] == 3
    assert lines[0] == "{} START total=3".format(name)
    assert lines[1].startswith("{} 2/3 (66.7%)".format(name))
    assert lines[1].endswith("| nuevos=1 actualizados=1")
    assert lines[2].startswith("{} DONE 3 reg en ".format(name))
    assert "nuevos=1 actualizados=1 omitidos=1 cod_rh_added=1" in lines[2]
    assert "invalid=1" in lines[2]


def test_legacy_parallel_counts_completed_authors(monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(
        legacy_module, "process_one", lambda *args: calls.append(args[0]))
    monkeypatch.setattr(
        legacy_module, "cvlac_profile_from_html", lambda document: None)
    client = mongomock.MongoClient()
    plugin = legacy_recognized_plugin(client)
    plugin.backend = "threading"
    plugin.n_jobs = 2
    plugin.verbose = 0
    plugin.cvlac_html_profiles = client.dam.html
    plugin.groups_production = client.dam.production

    plugin._process_parallel(
        ((author, author) for author in ["1", "2", "3"]),
        None, None, True, None, "legacy_html_only", 3,
    )

    lines = capsys.readouterr().out.splitlines()
    name = "[minciencias_opendata_person:legacy_html_only]"
    assert sorted(calls) == ["1", "2", "3"]
    assert lines[0] == "{} START total=3 | backend=threading num_jobs=2".format(name)
    assert lines[-1].startswith("{} DONE 3 reg en ".format(name))

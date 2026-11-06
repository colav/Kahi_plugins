from importlib import import_module

from kahi_scienti_affiliations.Kahi_scienti_affiliations import Kahi_scienti_affiliations


progress_module = import_module("kahi.progress")


def plugin(verbose=1):
    instance = Kahi_scienti_affiliations.__new__(Kahi_scienti_affiliations)
    instance.verbose = verbose
    instance.progress_interval = 2
    instance.progress_seconds = 60
    return instance


def test_interval_counts_completed_records_including_continue(capsys):
    visited = []
    for record in plugin()._progress(range(3), "groups source.product", total=3):
        visited.append(record)
        continue
    lines = capsys.readouterr().out.splitlines()
    assert visited == [0, 1, 2]
    assert len(lines) == 3
    assert lines[0] == "[scienti_affiliations:groups_source_product] START total=3"
    assert lines[1].startswith("[scienti_affiliations:groups_source_product] 2/3 (66.7%)")
    assert lines[2].startswith("[scienti_affiliations:groups_source_product] DONE 3 reg en ")


def test_time_threshold_reports_before_count_threshold(monkeypatch, capsys):
    ticks = iter([0, 61, 62])
    monkeypatch.setattr(progress_module, "monotonic", lambda: next(ticks))
    instance = plugin()
    instance.progress_interval = 1000
    assert list(instance._progress(["item"], "institutions", total=1)) == ["item"]
    lines = capsys.readouterr().out.splitlines()
    assert lines[1].startswith("[scienti_affiliations:institutions] 1/1 (100.0%)")
    assert lines[2] == "[scienti_affiliations:institutions] DONE 1 reg en 1m02s"


def test_default_interval_uses_standard(capsys):
    instance = plugin()
    instance.progress_interval = None
    assert list(instance._progress(range(1500), "groups", total=1500)) == list(range(1500))
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 3
    assert lines[1].startswith("[scienti_affiliations:groups] 1000/1500")


def test_zero_verbose_is_silent(capsys):
    assert list(plugin(0)._progress(range(3), "institutions", total=3)) == [0, 1, 2]
    assert capsys.readouterr().out == ""


def test_empty_phase_reports_completion_without_division_by_zero(capsys):
    assert list(plugin()._progress([], "groups", total=0)) == []
    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == "[scienti_affiliations:groups] START total=?"
    assert lines[-1].startswith("[scienti_affiliations:groups] DONE 0 reg")


def test_failed_record_is_not_counted_as_completed(capsys):
    records = plugin()._progress(["bad"], "institutions", total=1)
    assert next(records) == "bad"
    records.close()
    assert "DONE" not in capsys.readouterr().out

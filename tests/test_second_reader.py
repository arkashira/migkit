"""The second reading's answers, turned into migkit's verdicts.

The reader runs out of process; these pin the translation and the refusals,
which need no server. The live measurement - that it writes nothing to
either side, how fast it is, where its equality differs from migkit's - is
in its own file.
"""
from migkit import second_reader as sr
from migkit.config import Endpoint
from tests.test_the_report_does_not_name_its_tools import TOOLS


def _row(table, status, name="count", src=3, dst=3):
    return {"source_table_name": table, "validation_name": name,
            "validation_status": status, "source_agg_value": src,
            "target_agg_value": dst}


def _clean(results):
    said = " ".join(f"{r.check} {r.detail}" for r in results).lower()
    return not [t for t in TOOLS + ("dvt", "data-validation") if t in said]


def test_agreeing_measures_are_ok_per_table():
    got = sr.findings({"ok": True, "results": [
        _row("public.a", "success"), _row("public.a", "success", "sum__v"),
        _row("public.b", "success")]}, "app")
    assert [(r.scope, r.status) for r in got] == [("app.public.a", "ok"),
                                                  ("app.public.b", "ok")]
    assert _clean(got)


def test_one_differing_measure_makes_the_table_differ():
    got = sr.findings({"ok": True, "results": [
        _row("public.a", "success"),
        _row("public.a", "fail", "sum__v", 10, 11)]}, "app")
    assert [r.status for r in got] == ["diff"]
    assert "source 10 target 11" in got[0].detail, got[0].detail


def test_a_reading_that_did_not_happen_is_never_a_pass():
    for answer in ({"ok": False, "error": "connection refused"},
                   {"ok": True, "results": []}, {}):
        got = sr.findings(answer, "app")
        assert [r.status for r in got] == ["error"], (answer, got)
        assert _clean(got)


def test_an_engine_it_cannot_read_is_refused():
    import pytest
    with pytest.raises(SystemExit):
        sr.connection("redis", Endpoint(host="h", port=1), "0")


def test_not_installed_is_an_answer_not_a_crash(monkeypatch):
    monkeypatch.setenv("MIGKIT_SECOND_READER_PYTHON", "/nonexistent/python")
    got = sr.run({"tables": ["public.a"]})
    assert got["ok"] is False and "doctor --install" in got["error"]

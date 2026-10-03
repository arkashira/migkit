"""One verdict, one exit code (audit V1, V2, V6, V10, DU2, LG11).

"Same" was decided in four places that disagreed. Measured before this:
the report's renderer rewrote a `diff` whose detail said no row differed
into an `ok`, in the very rows `check` counted its exit code from - the
console said "verdict: different", then "all green", and exited 0; an
empty answer set read `same`; a `warn` read `same` in `verdict.json` while
the check exited 1 on it; `--only` with a typo dropped the name, compared
nothing and passed. Now `verdict.decide` is the one rule and
`verdict.VERDICTS` the one table, and every reader here is held to them.
"""
import json
import sqlite3

import pytest

from migkit import verdict


class _Hop:
    name = "h"
    engine = "postgres"

    def __init__(self, tmp=None, options=None):
        self._tmp = tmp
        self.options = options or {}

    def report_dir(self, db=None):
        return self._tmp


def _row(status, scope="db", check="data", detail="", **kw):
    return {"check": check, "scope": scope, "status": status,
            "detail": detail, **kw}


# ---- the one rule -----------------------------------------------------------

def test_nothing_compared_is_never_same():
    got = verdict.decide([])
    assert got.status == "incomplete" and got.exit == 3
    assert "nothing was compared" in got.why
    # the envelope a CI job reads says the same
    assert verdict.summarize(_Hop(), [])["status"] == "incomplete"


def test_a_difference_is_said_whatever_else_ran():
    got = verdict.decide([_row("ok"), _row("skip"), _row("diff", "db.t")])
    assert got.status == "different" and got.exit == 1
    assert "db.t" in got.why


def test_an_answer_not_taken_is_an_error_not_a_pass():
    got = verdict.decide([_row("ok"), _row("error", "db.t")])
    assert got.status == "error" and got.exit == 3
    # a status the table has no word for is never counted as a pass
    odd = verdict.decide([_row("ok"), _row("stale?", "db.u")])
    assert odd.status == "error" and "stale?" in odd.why


def test_a_warn_is_something_to_read_not_same():
    got = verdict.decide([_row("ok"), _row("warn", "db grants")])
    assert got.status == "incomplete" and got.exit == 3
    assert "db grants" in got.why


def test_a_slower_target_stops_the_check_only_where_the_hop_says():
    slow = _row("warn", "db performance", check="deep")
    free = verdict.decide([_row("ok"), slow], _Hop())
    assert free.status == "same" and free.severity[1] == "note"
    gated = verdict.decide([_row("ok"), dict(slow)],
                           _Hop(options={"performance": "gate"}))
    assert gated.status == "incomplete"


def test_only_skips_compare_nothing():
    got = verdict.decide([_row("skip"), _row("skip", "db2")])
    assert got.status == "incomplete" and "skipped" in got.why
    # a skip among compared answers is said, not hidden
    some = verdict.decide([_row("ok"), _row("skip", "db2", check="params")])
    assert some.status == "same" and "db2" in some.why


def test_a_narrowed_run_is_incomplete_and_ends_so():
    got = verdict.decide([_row("ok")], coverage={"checks": ["data"]})
    assert got.status == "incomplete" and got.exit == 3
    assert got.why.startswith("the run was narrowed to")


def test_answers_reused_from_an_earlier_run_are_not_fresh():
    """`check --resume` kept an `ok` however old: the source's own count of
    changes decides whether it still holds."""
    moved = verdict.decide([_row("ok", reused_from="2026-10-01T00:00:00",
                                 reuse_held=False)])
    assert moved.status == "incomplete" and "reused" in moved.why
    held = verdict.decide([_row("ok", reused_from="2026-10-01T00:00:00",
                                reuse_held=True)])
    assert held.status == "same"


def test_a_resumed_answer_is_held_by_the_source_s_count():
    from migkit import cli
    assert cli._held_since([10, 50.0], [10, 80.0])
    assert not cli._held_since([10, 50.0], [11, 80.0])
    # a count reset in between (the time it covers shrank) holds nothing
    assert not cli._held_since([10, 50.0], [10, 5.0])
    assert not cli._held_since(None, [10, 80.0])


def test_the_database_an_answer_is_about_is_read_one_way():
    """`--only --db shop` threw away `shop_archive`'s answers: it kept
    those not starting with `shop`."""
    assert verdict.db_of("shop_archive.public.t") == "shop_archive"
    assert verdict.db_of("shop narrowing") == "shop"
    assert verdict.db_of("shop_archive") != "shop"


def test_one_table_of_exit_codes():
    assert {k: v.exit for k, v in verdict.VERDICTS.items()} == {
        "same": 0, "different": 1, "error": 3, "incomplete": 3}
    assert verdict.EXIT == {"done": 0, "usage": 2, "failed": 3,
                            "refused": 4, "interrupted": 130}
    # a check that found a difference did its work; one that could not
    # compare, was refused or stopped did not
    assert verdict.ran(0) and verdict.ran(1)
    assert not any(verdict.ran(c) for c in (2, 3, 4, 130))


def test_the_manual_s_table_is_the_one_the_code_ends_by():
    got = dict(verdict.exit_codes())
    assert sorted(got) == [0, 1, 2, 3, 4, 130]
    assert [w for w, _ in got[3]] == ["could not verify", "incomplete",
                                      "failed"]
    # every verdict and every end is said in words
    assert set(verdict.MEANS) == set(verdict.VERDICTS) | set(verdict.EXIT)


# ---- the report reads, and changes nothing ---------------------------------

def test_the_report_changes_no_answer(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.report import render
    hop = Hop(name="r", engine="mssql",
              source=Endpoint(host="10.0.0.1", user="u", password="p"),
              target=Endpoint(host="10.0.0.2", user="u", password="p"))
    rows = [_row("diff", "shop", detail="t1 missing=0 extra=0 changed=0;"
                                        " t2 src=10 dst=7")]
    html = render(hop, rows)
    assert rows[0]["status"] == "diff", rows
    assert "DIFFERENT" in html and "PASS" not in html and "SAME" not in html
    from migkit import report
    assert not hasattr(report, "_reclassify")


def test_the_report_shows_a_skip_as_a_skip(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.report import render
    hop = Hop(name="r", engine="postgres",
              source=Endpoint(host="10.0.0.1", user="u", password="p"),
              target=Endpoint(host="10.0.0.2", user="u", password="p"))
    html = render(hop, [_row("skip", "shop", check="params")])
    assert "INCOMPLETE" in html and "SAME" not in html


# ---- the command ends as the verdict does -----------------------------------

@pytest.fixture
def lite(tmp_path, monkeypatch):
    import migkit.config as cfg
    for name, rows in (("a.db", 3), ("b.db", 3)):
        con = sqlite3.connect(tmp_path / name)
        con.execute("create table t (id integer primary key, v text)")
        con.executemany("insert into t values (?, ?)",
                        [(i, f"v{i}") for i in range(rows)])
        con.commit()
        con.close()
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 'a.db'}, user: x, password: x}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}, user: x, password: x}}\n"
        "    databases: [main]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return tmp_path


def _run(*argv):
    from click.testing import CliRunner

    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, got.output


def test_the_check_ends_as_its_verdict_says(lite):
    got, said = _run("check", "lite")
    env = json.loads((lite / "reports" / "lite" / "verdict.json")
                     .read_text())
    assert (got.exit_code, env["status"]) == (0, "same"), said
    con = sqlite3.connect(lite / "b.db")
    con.execute("update t set v = 'changed' where id = 1")
    con.commit()
    con.close()
    got, said = _run("check", "lite")
    env = json.loads((lite / "reports" / "lite" / "verdict.json")
                     .read_text())
    assert (got.exit_code, env["status"]) == (1, "different"), said
    assert "all green" not in said, said
    # the report rendered from it says the same, and summary.json kept
    # the answer as the check took it
    summary = json.loads((lite / "reports" / "lite" / "summary.json")
                         .read_text())
    assert any(r["status"] == "diff" for r in summary), summary
    assert "DIFFERENT" in (lite / "reports" / "lite" / "report.html"
                           ).read_text()


def test_a_mistyped_only_is_a_usage_error_not_a_pass(lite):
    got, said = _run("check", "lite", "--only", "countz")
    assert got.exit_code == 2, said
    assert "countz" in said and "all green" not in said, said


def test_a_narrowed_check_ends_incomplete(lite):
    got, said = _run("check", "lite", "--only", "counts")
    assert got.exit_code == 3, said
    assert "the run was narrowed to" in said, said


def test_the_integration_task_passes_only_what_the_command_passes():
    from migkit import integrations

    def res(code, status):
        return {"exit": code, "status": status}
    assert integrations.passed(res(0, "same"))
    assert integrations.passed(res(0, None))
    # a verdict that is not same fails the task, whatever the exit said
    assert not integrations.passed(res(0, "incomplete"))
    assert not integrations.passed(res(3, "error"))
    assert not integrations.passed(res(1, "different"))
    assert integrations.passed(res(1, "different"), fail_on_difference=False)
    assert not integrations.passed(res(3, "incomplete"),
                                   fail_on_difference=False)


def test_a_yes_no_variable_says_a_mistake(monkeypatch):
    from migkit import schedule
    assert schedule.flag("X", environ={"X": "false"}) is False
    assert schedule.flag("X", environ={"X": "on"}) is True
    assert schedule.flag("X", True, environ={}) is True
    with pytest.raises(verdict.Usage, match="X=flase"):
        schedule.flag("X", environ={"X": "flase"})

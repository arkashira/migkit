"""A check reuses an answer only where it was taken under the hop's scope.

Measured at ee1032d: `check --only counts` merged the previous run's other
answers into `summary.json` whatever scope they were taken under; `check
--resume` caught a changed `include` only because the source's shape it
listed changed with it, said "the source's schema changed", and missed a
changed row filter, column rule, rename or database name altogether; and
the same findings under another scope read "identical to the previous
run". Every answer now carries the scope it was taken under, and one
taken under another is taken again, said. Two SQLite files: no server.
"""
import json
import sqlite3

import pytest
from click.testing import CliRunner

from migkit import cli

HOP = ("hops:\n  lite:\n    engine: sqlite\n"
       "    source: {{host: {a}, user: x, password: x}}\n"
       "    target: {{host: {b}, user: x, password: x}}\n"
       "    databases: [main]\n")


@pytest.fixture
def lite(tmp_path, monkeypatch):
    import migkit.config as cfg
    for name in ("a.db", "b.db"):
        con = sqlite3.connect(tmp_path / name)
        con.executescript(
            "create table orders (id integer primary key, v text);"
            " insert into orders values (1, 'a'), (2, 'b');"
            " create table people (id integer primary key);"
            " insert into people values (1);")
        con.commit()
        con.close()
    conf = tmp_path / "hops.yaml"
    conf.write_text(HOP.format(a=tmp_path / "a.db", b=tmp_path / "b.db"))
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return tmp_path


def _takes_every_table_by_name(where):
    """The same tables, rows and columns - the scope said another way, so
    the answers are alike and only the scope they are about differs."""
    conf = where / "hops.yaml"
    conf.write_text(conf.read_text() + "    include: [orders, people]\n")


def _check(*extra):
    got = CliRunner().invoke(cli.main, ["check", "lite", *extra])
    return got, " ".join((got.output + str(got.exception or "")).split())


def _records(where):
    return json.loads((where / "reports" / "lite" / "summary.json")
                      .read_text())


def _scope_now():
    from migkit.config import get_hop
    return get_hop("lite").scope_fingerprint()


def test_every_answer_carries_the_scope_it_was_taken_under(lite):
    got, said = _check()
    assert got.exit_code == 0, said
    assert {r.get("scope_fingerprint") for r in _records(lite)} == \
        {_scope_now()}


def test_a_resumed_check_takes_again_what_another_scope_answered(lite):
    got, said = _check()
    assert got.exit_code == 0, said
    _takes_every_table_by_name(lite)
    got, said = _check("--resume")
    assert got.exit_code == 0, said
    assert "the hop's scope changed since the run being resumed" in said, \
        said
    records = _records(lite)
    assert not [r for r in records if r.get("reused_from")], records
    assert {r.get("scope_fingerprint") for r in records} == {_scope_now()}
    # under the same scope, the answers are reused as they always were
    got, said = _check("--resume")
    assert any(r.get("reused_from") for r in _records(lite)), said


def test_a_narrowed_check_keeps_no_answer_of_another_scope(lite):
    got, said = _check()
    assert got.exit_code == 0, said
    before = {r["check"] for r in _records(lite)}
    assert "data" in before, before
    _takes_every_table_by_name(lite)
    got, said = _check("--only", "counts")
    assert "taken under another scope than the hop's now" in said, said
    records = _records(lite)
    assert {r["check"] for r in records} == {"counts"}, records
    assert {r.get("scope_fingerprint") for r in records} == {_scope_now()}
    # a narrowed run is never `same`
    assert got.exit_code != 0, said


def test_the_same_findings_about_another_scope_are_not_identical(lite):
    got, said = _check()
    assert got.exit_code == 0, said
    got, said = _check()
    assert "identical to the previous run" in said, said
    _takes_every_table_by_name(lite)
    got, said = _check()
    assert got.exit_code == 0, said
    assert "identical to the previous run" not in said, said
    env = json.loads((lite / "reports" / "lite" / "verdict.json")
                     .read_text())
    assert env["scope_fingerprint"] == _scope_now()
    assert env["scope"] == {"include": ["orders", "people"]}

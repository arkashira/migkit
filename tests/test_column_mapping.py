"""Columns kept, dropped and renamed on the way (backlog 9).

`mapping` read `where` and `tables` only. DMS and DTS both let a task drop
a column or give it another name on the target. `mapping.columns` does
that now, and the copy, the comparison and the schema check read it
through one function (`_mapped_types`), so the check compares a renamed
column with its new name and does not report a dropped one as missing.
"""
import sqlite3

import pytest


@pytest.fixture
def lite(tmp_path, monkeypatch):
    import migkit.config as cfg
    src = tmp_path / "a.db"
    con = sqlite3.connect(src)
    con.executescript(
        "create table people (id integer primary key, full_name text,"
        " secret text, age integer);"
        " insert into people values (1, 'Ann', 'x', 30), (2, 'Bo', 'y', 41);")
    con.commit()
    con.close()
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {src}, user: x, password: x}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}, user: x, password: x}}\n"
        "    databases: [main]\n"
        "    mapping:\n      columns:\n        people:\n"
        "          drop: [secret]\n          rename: {full_name: name}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return tmp_path


def _run(*argv):
    from click.testing import CliRunner

    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


def test_the_target_is_built_and_filled_under_the_mapping(lite):
    got, said = _run("move", "lite", "--mode", "full", "--go")
    assert got.exit_code == 0, said
    con = sqlite3.connect(lite / "b.db")
    cols = [r[1] for r in con.execute("pragma table_info(people)")]
    rows = con.execute("select id, name, age from people order by id"
                       ).fetchall()
    con.close()
    assert "secret" not in cols and "full_name" not in cols, cols
    assert "name" in cols, cols
    assert rows == [(1, "Ann", 30), (2, "Bo", 41)], rows


def test_the_check_compares_through_the_mapping(lite):
    _run("move", "lite", "--mode", "full", "--go")
    got, said = _run("check", "lite", "--only", "schema,data")
    assert got.exit_code == 0, said
    assert "secret" not in said, said
    assert "DIFF" not in said.upper().replace("DIFFER", ""), said
    # and a changed renamed value is still caught
    con = sqlite3.connect(lite / "b.db")
    con.execute("update people set name = 'Anne' where id = 1")
    con.commit()
    con.close()
    got, said = _run("check", "lite", "--only", "data")
    assert got.exit_code != 0, said


def test_the_rules_read_by_suffix():
    from migkit.config import Endpoint, Hop
    hop = Hop(name="h", engine="sqlite",
              source=Endpoint(host="a", port=0, user="", password=""),
              target=Endpoint(host="b", port=0, user="", password=""),
              mapping={"columns": {"people": {"keep": ["id", "name"],
                                              "rename": {"name": "nm"}}}})
    assert hop.column_rules("main", "people") == {
        "keep": ["id", "name"], "drop": [], "rename": {"name": "nm"}}
    assert hop.column_rules("main", "other") == {}

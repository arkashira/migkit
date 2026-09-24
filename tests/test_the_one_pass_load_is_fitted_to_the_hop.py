"""The one-pass cross-engine load is used only for a hop it can carry.

It was chosen for every cross-engine hop once installed, and the load file
migkit writes for it reads MySQL into PostgreSQL, the whole database, with
nothing else. So on a machine that had it:
* a PostgreSQL-to-MySQL hop, or any other pair, was read from the wrong
  server as the wrong kind
* a hop with an exclude list had the target's own tables loaded over
* a table, column or row mapping was not read at all

The decision is now made from the hop, and anything the load cannot carry
goes through the table copier, which carries all of it.
"""
import sqlite3

from click.testing import CliRunner

from migkit import movers
from migkit.config import Endpoint, Hop


def _hop(**kw):
    options = kw.pop("options", {"source_engine": "mysql",
                                 "target_engine": "postgres"})
    return Hop(name="h", engine="hetero", options=options,
               source=Endpoint(host="10.0.0.1", port=1, user="u",
                               password="CHANGE_ME"),
               target=Endpoint(host="10.0.0.2", port=2, user="u",
                               password="CHANGE_ME"),
               databases=["appdb"], **kw)


def test_a_plain_mysql_to_postgres_hop_keeps_it():
    assert movers.fitted(_hop(), "hetero", "pgloader") == ("pgloader", None)


def test_another_pair_takes_the_table_copier():
    via, why = movers.fitted(_hop(options={"source_engine": "postgres",
                                           "target_engine": "mysql"}),
                             "hetero", "pgloader")
    assert via == "builtin" and "MySQL into PostgreSQL only" in why, why


def test_an_exclude_list_takes_the_table_copier():
    via, why = movers.fitted(_hop(exclude=["audit"]), "hetero", "pgloader")
    assert via == "builtin" and "excludes" in why, why


def test_a_mapping_takes_the_table_copier():
    for mapping in ({"tables": {"a": "b"}}, {"columns": {"a": {"drop":
                                                              ["x"]}}},
                    {"where": {"a": "id > 1"}}):
        via, why = movers.fitted(_hop(mapping=mapping), "hetero", "pgloader")
        assert via == "builtin" and "mapping" in why, (mapping, why)


def test_other_paths_are_not_touched():
    assert movers.fitted(_hop(exclude=["audit"]), "mysql", "mydumper") == \
        ("mydumper", None)


def test_a_move_on_a_machine_that_has_it_still_moves(tmp_path, monkeypatch):
    """A SQLite pair, with the load installed: it used to be handed a load
    file for MySQL and PostgreSQL."""
    import migkit.config as cfg
    from migkit import cli
    for name, rows in (("a.db", "(1, 'x'), (2, 'y')"), ("b.db", None)):
        con = sqlite3.connect(tmp_path / name)
        con.execute("create table t (id integer primary key, v text)")
        if rows:
            con.execute(f"insert into t values {rows}")
        con.commit()
        con.close()
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  lite:\n    engine: hetero\n"
        "    options: {source_engine: sqlite, target_engine: sqlite}\n"
        f"    source: {{host: {tmp_path / 'a.db'}, user: x, password: x}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}, user: x, password: x}}\n"
        "    databases: [main]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    real = movers.which
    monkeypatch.setattr(movers, "which",
                        lambda n: "/usr/local/bin/pgloader"
                        if n == "pgloader" else real(n))
    got = CliRunner().invoke(cli.main, ["move", "lite", "--go"])
    said = got.output + str(got.exception or "")
    assert got.exit_code == 0, said
    assert "MySQL into PostgreSQL only" in said, said
    con = sqlite3.connect(tmp_path / "b.db")
    assert con.execute("select count(*) from t").fetchone()[0] == 2
    con.close()

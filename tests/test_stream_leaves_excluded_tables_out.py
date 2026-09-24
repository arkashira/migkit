"""The streaming pipeline leaves out what the hop excludes, on MySQL too.

The connector's `table.exclude.list` is worked out from the source's table
list. On MySQL that list came from `neutral_tables`, which has already
dropped the excluded tables. So the exclude list was worked out over a list
with nothing excluded in it, came back empty, and the connector streamed
the tables the hop leaves alone into a target that owns them.
PostgreSQL's list had the opposite fault: it did not drop them anywhere,
so the cross-engine move copied them. Each engine now keeps both lists
apart. `_all_tables` is for working out what to leave out, and
`neutral_tables` is for everything else.
"""
import json

from migkit.config import Endpoint, Hop


def _hop(engine, tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return Hop(name="s", engine=engine,
               source=Endpoint(host="10.0.0.1", port=1, user="u",
                               password="CHANGE_ME"),
               target=Endpoint(host="10.0.0.2", port=2, user="u",
                               password="CHANGE_ME"),
               databases=["shop"], exclude=["audit_log"])


def _excluded_by_the_connector(out):
    src = json.loads((out / "source-connector.json").read_text())
    return src["config"].get("table.exclude.list")


def test_mysql_stream_leaves_the_excluded_table_out(tmp_path, monkeypatch):
    from migkit import movers
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setattr(MySQLEngine, "_all_tables",
                        lambda self, side, db: ["orders", "audit_log"])
    out = movers.stream_codegen(_hop("mysql", tmp_path, monkeypatch),
                                ["shop"], "mysql")
    assert _excluded_by_the_connector(out) == "shop\\.audit_log"


def test_postgres_stream_leaves_the_excluded_table_out(tmp_path,
                                                       monkeypatch):
    from migkit import movers
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.setattr(PostgresEngine, "_all_tables",
                        lambda self, side, db: ["public.orders",
                                                "public.audit_log"])
    out = movers.stream_codegen(_hop("postgres", tmp_path, monkeypatch),
                                ["shop"], "postgres")
    assert _excluded_by_the_connector(out) == "public\\.audit_log"


def test_the_postgres_table_list_drops_what_the_hop_excludes(tmp_path,
                                                            monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop("postgres", tmp_path, monkeypatch))
    monkeypatch.setattr(eng, "_psql", lambda side, db, sql:
                        "public.orders\npublic.audit_log\nother.audit_log")
    assert eng.neutral_tables("src", "shop") == ["public.orders"]
    assert len(eng._all_tables("src", "shop")) == 3

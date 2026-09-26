"""The checks of a SQL Server hop reach the server, and a rollback puts its
identities back.

Reading rows through the driver gave the engine a second method named
`_q`, which quotes a name, and it replaced the one that runs a statement
through the SQL Server client: every check of a SQL Server hop - counts,
identities, schema, data - stopped on `_q() takes 2 positional arguments
but 4 were given` before asking the server anything. The client's runner
has a name of its own now. It also asks the target under the name the hop
maps the database to; it used to ask under the source's name, so a hop
with `db_map` compared the source with a database of the same name on the
target, or with none.

These run without a server: the client's replies are the ones it gives,
as text, and the driver's rows are given as the driver returns them. The
same checks against a server are in
`test_sql_server_checks_against_a_server.py`.
"""
import types

from migkit.config import Endpoint, Hop


def _engine():
    from migkit.engines.mssql import MSSQLEngine
    hop = Hop(name="ms", engine="mssql",
              source=Endpoint(host="10.0.0.1", port=1433, user="sa",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=1433, user="sa",
                              password="CHANGE_ME"),
              databases=["shop"], db_map={"shop": "shop_new"})
    return MSSQLEngine(hop)


def test_the_counts_are_asked_of_each_side_under_its_own_name(monkeypatch):
    from migkit.engines import mssql
    asked = []

    def run(argv, env=None, **kw):
        server, db = argv[argv.index("-S") + 1], argv[argv.index("-d") + 1]
        asked.append((server, db))
        rows = "dbo.orders|3\ndbo.lines|7\n" if server.startswith(
            "10.0.0.1") else "dbo.orders|2\ndbo.lines|7\n"
        return types.SimpleNamespace(stdout=rows, returncode=0)
    monkeypatch.setattr(mssql, "which", lambda name: "/usr/bin/" + name)
    monkeypatch.setattr(mssql, "run", run)
    got = _engine().check_counts("shop")
    assert [(r.status, r.detail) for r in got] == [
        ("diff", "dbo.orders src=3 dst=2")], got
    assert asked == [("10.0.0.1,1433", "shop"), ("10.0.0.2,1433", "shop_new")]


def test_a_rollback_reseeds_each_identity_the_snapshot_found(tmp_path,
                                                             monkeypatch):
    eng = _engine()

    def rows(side, db, sql, args=None):
        assert side == "dst"
        if "identity_columns" in sql:
            return [["dbo.orders", 41], ["dbo.never_used", None],
                    ["o'brien.t", 3]]
        return [["dbo.v_open", "VIEW", "create view dbo.v_open as\n"
                                       "select id from dbo.orders"]]
    monkeypatch.setattr(eng, "_rows", rows)
    eng.snapshot_state("shop", tmp_path)
    assert eng.restore_sequences("shop", tmp_path) == [
        "dbcc checkident ('[dbo].[orders]', reseed, 41);",
        "dbcc checkident ('[o''brien].[t]', reseed, 3);"]
    assert (tmp_path / "dst-schema.sql").read_text() == (
        "-- dbo.v_open (view)\ncreate view dbo.v_open as\n"
        "select id from dbo.orders\ngo\n")
    # a repair of identities only takes only them
    only = tmp_path / "only"
    only.mkdir()
    eng.snapshot_state("shop", only, kind="sequences")
    assert not (only / "dst-schema.sql").exists()
    assert (only / "dst-identity.txt").read_text() == \
        "dbo.orders|41\no'brien.t|3\n"

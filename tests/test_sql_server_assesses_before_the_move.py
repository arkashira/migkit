"""SQL Server's assessment says what PostgreSQL's and MySQL's do before
anything moves (P0-PARITY.all): both servers' versions read and held to
each other, which product answers on each side, the readiness checks of its
own, the deep checks that predict trouble, the room the target has, every
other writer on it, and the work the move leaves for hands.

Without a server: a scripted one (`tests/mssql_stub.py`) answers the
catalogue reads. Every statement here is new and has not run on a server:
none of these items is in `PROVES`; each waits in
`capabilities.UNPROVED` for the test that runs it on one.
"""
import pytest

from tests.mssql_stub import Server, engine


def _both(server, pattern, src, dst):
    return server.on(pattern, src, "src").on(pattern, dst, "dst")


def _assess(monkeypatch, tmp_path, server, **kw):
    eng = engine(monkeypatch, tmp_path, server, **kw)
    eng._bulk_path_rows = lambda: []
    return eng


def _rows(items, item):
    return [i for i in items if i["item"] == item]


def test_both_versions_are_read_and_held_to_each_other(monkeypatch, tmp_path):
    s = _both(Server(), "ProductVersion'\\) as nvarchar\\(128\\)\\)$",
              [("16.0.4135.4",)], [("15.0.2000.5",)])
    eng = _assess(monkeypatch, tmp_path, s)
    assert eng._server_versions() == ("16.0.4135.4", "15.0.2000.5")
    (row,) = _rows(eng.assess(), "server version match")
    assert row["level"] == "warn" and "src 16.0.4135.4 / dst 15.0.2000.5" \
        in row["detail"]


def test_a_version_a_side_would_not_say_is_unknown_not_clean(monkeypatch,
                                                              tmp_path):
    s = Server().on("ProductVersion'\\) as nvarchar\\(128\\)\\)$",
                    lambda side, sql: [("16.0.4135.4",)] if side == "src"
                    else [])
    eng = _assess(monkeypatch, tmp_path, s)
    (row,) = _rows(eng.assess(), "server version match")
    assert row["level"] == "warn" and "unknown, not clean" in row["detail"]


def _probe(banner, edition, babelfish=None, product="16.0.4135.4"):
    return [(banner, edition, "Enterprise Edition", product, babelfish)]


def test_the_product_that_answers_is_named_from_what_it_says(monkeypatch,
                                                             tmp_path):
    cases = {
        "SQL Server": _probe("Microsoft SQL Server 2022 (RTM-CU14) - "
                             "16.0.4135.4 (X64)", 3),
        "Azure SQL Edge": _probe("Microsoft Azure SQL Edge Developer (RTM) -"
                                 " 15.0.2000.1574 (ARM64)", 9),
        "Azure SQL Database": _probe("Microsoft SQL Azure (RTM) - 12.0.2000.8",
                                     5),
        "Azure SQL Managed Instance": _probe(
            "Microsoft SQL Azure (RTM) - 12.0.2000.8", 8),
        "Babelfish": _probe("Babelfish for PostgreSQL with SQL Server"
                            " Compatibility - 12.0.2000.8", 2,
                            babelfish="3.4.0"),
    }
    names = {}
    for label, rows in cases.items():
        s = Server().on("@@version", rows)
        eng = _assess(monkeypatch, tmp_path, s)
        names[label] = eng._brands()[0].name
    assert names == {"SQL Server": "mssql", "Azure SQL Edge":
                     "azure-sql-edge", "Azure SQL Database":
                     "azure-sql-database", "Azure SQL Managed Instance":
                     "azure-sql-managed-instance", "Babelfish": "babelfish"}


def test_a_product_that_said_nothing_is_not_named_by_its_silence(
        monkeypatch, tmp_path):
    s = Server().on("@@version", [("something else entirely", 1, "x", "1.0",
                                   None)])
    eng = _assess(monkeypatch, tmp_path, s)
    assert eng._brands()[0].name == "unknown"
    rows = [i for i in eng._brand_rows() if i["item"] == "source brand"]
    assert rows and rows[0]["level"] == "warn"
    # a probe that fails is unidentified, and takes nothing down
    s = Server().on("@@version", lambda side, sql: 1 / 0)
    assert _assess(monkeypatch, tmp_path, s)._brands()[0].name == "unknown"


def test_different_products_behind_one_wire_are_said_as_a_pairing(
        monkeypatch, tmp_path):
    s = Server().on("@@version", lambda side, sql: _probe(
        "Microsoft SQL Server 2022 - 16.0", 3) if side == "src" else
        _probe("Microsoft SQL Azure (RTM) - 12.0.2000.8", 5))
    eng = _assess(monkeypatch, tmp_path, s)
    pairing = [i for i in eng._brand_rows() if i["item"] == "brand pairing"]
    assert pairing and "different software behind the same protocol" \
        in pairing[0]["detail"]


def test_the_change_feed_is_asked_of_the_source(monkeypatch, tmp_path):
    s = Server()
    s.on("sys.change_tracking_databases", [(0,)])
    eng = _assess(monkeypatch, tmp_path, s)
    (row,) = eng._assess_tracking("appdb")
    assert row["level"] == "warn" and row["item"] == "change tracking"
    assert "alter database" in row["detail"].lower()
    # on for the database, a table the hop moves not followed
    s = Server()
    s.on("sys.change_tracking_databases", [(1,)])
    s.on("sys.change_tracking_tables", [("dbo.a",)])
    eng = _assess(monkeypatch, tmp_path, s)
    eng.neutral_tables = lambda side, db: ["dbo.a", "dbo.b"]
    (row,) = eng._assess_tracking("appdb")
    assert row["level"] == "warn" and "dbo.b" in row["detail"]
    assert "enable_change_tracking" in row["detail"].lower().replace(
        " ", "_") or "change_tracking" in row["detail"].lower()
    # on, and following every table
    eng.neutral_tables = lambda side, db: ["dbo.a"]
    (row,) = eng._assess_tracking("appdb")
    assert row["level"] == "pass"


def test_a_collation_and_a_level_that_differ_are_warned(monkeypatch,
                                                         tmp_path):
    s = Server().on("databasepropertyex", lambda side, sql: [
        ("Latin1_General_CI_AS", 150)] if side == "src" else
        [("Latin1_General_BIN2", 150)])
    eng = _assess(monkeypatch, tmp_path, s)
    got = {r["item"]: r for r in eng._assess_alike("appdb")}
    assert got["collation matches"]["level"] == "warn"
    assert "src Latin1_General_CI_AS / dst Latin1_General_BIN2" \
        in got["collation matches"]["detail"]
    assert got["compatibility level matches"]["level"] == "pass"


def test_what_the_move_leaves_for_hands_is_counted_and_named(monkeypatch,
                                                              tmp_path):
    s = Server()
    s.on("is_memory_optimized = 1", [("dbo.hot",)])
    s.on("sys.fulltext_catalogs", [("ft_docs",)])
    s.on("not exists \\(select 1 from sys\\.indexes", [("dbo.keyless",)])
    eng = _assess(monkeypatch, tmp_path, s)
    inv = eng._handwork()
    rows = {r["item"]: r for r in inv.rows()}
    assert rows["tables with no primary key or unique index"]["level"] == \
        "warn"
    assert "dbo.keyless" in rows[
        "tables with no primary key or unique index"]["detail"]
    assert "memory-optimized tables: 1 (dbo.hot)" in rows[
        "objects whose contents no data mover copies"]["detail"]
    assert "full-text catalogs: 1 (ft_docs)" in rows[
        "objects whose contents no data mover copies"]["detail"]
    assert inv.summary()[0]["item"].endswith("items need hands")


def test_a_probe_that_could_not_run_is_unknown_never_zero(monkeypatch,
                                                           tmp_path):
    def refuse(side, sql):
        raise RuntimeError("the source refused a statement: denied")
    s = Server().on("sys.fulltext_catalogs", refuse)
    eng = _assess(monkeypatch, tmp_path, s)
    inv = eng._handwork()
    assert inv.unknowns() == 1
    unknown = [r for r in inv.rows() if "UNKNOWN" in r["detail"]]
    assert unknown and "full-text catalogs" in unknown[0]["detail"]


def test_the_predictive_deep_checks_run_in_the_assessment(monkeypatch,
                                                           tmp_path):
    from migkit.engines.base import Result
    assert "_capacity_gaps" in type(
        engine(monkeypatch, tmp_path, Server())).PREFLIGHT
    s = Server()
    eng = _assess(monkeypatch, tmp_path, s)
    monkeypatch.setattr(type(eng), "_mojibake", lambda self, db: Result(
        "deep", f"{db} mojibake", "ok", "none"))
    eng.neutral_tables = lambda side, db: ["dbo.t"]
    eng.neutral_columns = lambda side, db, t: (
        [("note", "varchar(50)"), ("n", "bigint")] if side == "src"
        else [("note", "varchar(20)"), ("n", "int")])

    def count(side, sql):
        # three rows past 20 characters, one past an int
        return [(3, 41)] if "len(" in sql else [(1, 5000000000)]
    s.on("select 1", [(1,)])
    s.on("select count\\(\\*\\), max", count)
    items = [i for i in eng._preflight_items()
             if i["scope"] == "before the move"]
    cap = [i for i in items if "target capacity" in i["item"]]
    assert cap and cap[0]["level"] == "fail"
    assert "3 rows, largest 41 against the target's 20" in cap[0]["detail"]
    assert "1 rows, largest 5000000000 against the target's 2147483647" \
        in cap[0]["detail"]
    asked = s.sent("len\\(")
    assert asked and "where len([note]) > 20" in asked[0]


def test_the_capacity_probe_is_t_sql(monkeypatch, tmp_path):
    eng = engine(monkeypatch, tmp_path, Server())
    assert eng._capacity_probe("[a]", ("chars", 20)) == (
        "len([a]) > 20", "max(len([a]))")
    where, worst = eng._capacity_probe("[a]", ("int", -32768, 32767))
    assert where == "([a] < -32768 or [a] > 32767)" and "decimal(38, 0)" \
        in worst
    assert eng._capacity_probe("[a]", ("numeric", 8, 2))[0] == \
        "abs([a]) >= 1000000"
    with pytest.raises(ValueError):
        eng._capacity_probe("[a]", ("bytes", 5))


def test_the_room_a_server_has_is_asked_and_left_out_when_it_is_not_said(
        monkeypatch, tmp_path):
    s = Server()
    s.on("dm_os_sys_info", [(8,)])
    s.on("@@max_connections", [(32000,)])
    s.on("dm_exec_requests", [(3,)])
    eng = engine(monkeypatch, tmp_path, s)
    assert eng.capacity("dst", "appdb") == {
        "cpus": 8, "free_connections": 32000, "running": 3}

    def refuse(side, sql):
        raise RuntimeError("the target refused a statement: VIEW SERVER"
                           " STATE permission was denied")
    s = Server().on("dm_os_sys_info", refuse).on("@@max_connections",
                                                  [(100,)])
    eng = engine(monkeypatch, tmp_path, s)
    # a key it will not answer is left out - not readable, never idle
    assert eng.capacity("dst", "appdb") == {"free_connections": 100}


def test_the_free_disk_is_the_least_of_the_volumes_and_none_when_unsaid(
        monkeypatch, tmp_path):
    eng = engine(monkeypatch, tmp_path,
                 Server().on("dm_os_volume_stats", [(5 * 2 ** 30,)]))
    assert eng.free_bytes("dst", "appdb") == 5 * 2 ** 30
    eng = engine(monkeypatch, tmp_path,
                 Server().on("dm_os_volume_stats", [(None,)]))
    assert eng.free_bytes("dst", "appdb") is None


def test_every_other_writer_on_the_target_is_named(monkeypatch, tmp_path):
    s = Server()
    s.on("dm_hadr_database_replica_states", [(1,)])
    s.on("is_subscribed = 1", [(0,)])
    s.on("log_shipping_secondary_databases", [(1,)])
    eng = engine(monkeypatch, tmp_path, s)
    assert eng.stream_writers("appdb") == [
        ("the availability group applying the primary's changes", False),
        ("log shipping restoring logs", False)]

    def refuse(side, sql):
        raise RuntimeError("the target refused a statement: denied")
    # one the server will not show is not one that is there
    eng = engine(monkeypatch, tmp_path,
                 Server().on("log_shipping_secondary_databases", refuse))
    assert eng.stream_writers("appdb") == []

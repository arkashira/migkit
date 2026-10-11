"""SQL Server reads the hop's scope the way PostgreSQL and MySQL do, in the
copy and in the check (P0-PARITY.scope): the tables its copy lists, the
objects beside its tables, the finding that names its table, the rows a row
filter selects and the names a hop gives its tables.

Without a server: a scripted one (`tests/mssql_stub.py`) answers the
catalogue reads, and what is held is which statement the engine sends and
what it makes of the rows that come back. The statements that carry a row
filter are new and have not run on a server: `scope.row-filter` and
`scope.names` wait in `capabilities.UNPROVED` for the test that runs them
on one.
"""
from migkit.catalog import Catalog, Column
from tests.mssql_stub import Server, engine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("scope.copy", "mssql"),
    ("scope.objects", "mssql"),
    ("verify.structure.objects", "mssql"),
    ("verify.table", "mssql"),
]


def test_the_copy_lists_the_hops_tables_only(monkeypatch, tmp_path):
    server = Server().on("sys.tables", [("dbo.a",), ("dbo.b",), ("dbo.c",)])

    def listed(**kw):
        eng = engine(monkeypatch, tmp_path, server, **kw)
        return [t for _, t in eng.list_move_tables("appdb")]
    assert listed() == ["a", "b", "c"]
    assert listed(exclude=["appdb.dbo.b"]) == ["a", "c"]
    assert listed(include=["appdb.dbo.a", "appdb.dbo.c"]) == ["a", "c"]


def _catalog(*tables):
    return Catalog(columns={t: [Column("id", "int")] for t in tables})


def _view(name, kind="VIEW", digest="AA"):
    schema, _, leaf = name.partition(".")
    return (name, kind, digest, schema, leaf)


def test_the_objects_beside_the_tables_are_the_hops_by_their_kind(
        monkeypatch, tmp_path):
    """A view, a routine and a sequence are taken by the hop's `objects`
    as every other inventory takes them; what it leaves out is the
    target's own, and no finding."""
    both = [_view("dbo.v_orders"), _view("dbo.p_close", "SQL_STORED_PROCEDURE"),
            _view("dbo.seq_a", "SEQUENCE_OBJECT")]
    server = Server().on("from sys.objects o", lambda side, sql: both
                         if side == "src" else both[:1])
    eng = engine(monkeypatch, tmp_path, server)
    assert sorted(eng._objects("src", "appdb")) == sorted(
        (r[0], r[1]) for r in both)
    # no procedures and no sequences carried: the target's lack is not a
    # finding, and nothing of either kind is read
    eng = engine(monkeypatch, tmp_path, server,
                 objects={"procedures": False, "sequences": False})
    assert sorted(eng._objects("src", "appdb")) == [
        ("dbo.v_orders", "VIEW")]
    # only the named view carried
    eng = engine(monkeypatch, tmp_path, server,
                 objects={"views": ["v_other"], "procedures": True,
                          "sequences": False})
    assert sorted(eng._objects("src", "appdb")) == [
        ("dbo.p_close", "SQL_STORED_PROCEDURE")]


def test_an_object_missing_on_the_target_is_a_finding_of_its_own(
        monkeypatch, tmp_path):
    both = [_view("dbo.v_orders"), _view("dbo.p_close", "SQL_STORED_PROCEDURE")]
    server = Server().on("from sys.objects o", lambda side, sql: both
                         if side == "src" else both[:1])
    eng = engine(monkeypatch, tmp_path, server)
    eng.shape_catalog = lambda side, db: _catalog("dbo.orders")
    got = {r.scope: r for r in eng.check_schema("appdb")}
    assert got["appdb"].status == "ok"
    objects = got["appdb objects"]
    assert objects.status == "diff"
    assert "missing SQL_STORED_PROCEDURE dbo.p_close" in objects.detail
    assert objects.category == "structure.objects"
    # a hop that carries no procedures does not ask after them
    eng = engine(monkeypatch, tmp_path, server,
                 objects={"procedures": False})
    eng.shape_catalog = lambda side, db: _catalog("dbo.orders")
    got = {r.scope: r for r in eng.check_schema("appdb")}
    assert got["appdb objects"].status == "ok", got["appdb objects"].detail


def test_a_finding_about_one_table_names_the_table(monkeypatch, tmp_path):
    def answer(side, sql):
        if "sys.tables" in sql:
            return [("dbo.orders",)]
        if "count_big" in sql:
            return [(3, 10)] if side == "src" else [(4, 11)]
        return []
    server = Server().on("sys.tables|count_big", answer)
    eng = engine(monkeypatch, tmp_path, server)
    eng._row_json = lambda db, t: ("x", [])
    eng._health = lambda side, db=None: None
    eng._drilldown = lambda db, t: (0, 1, 0)
    eng._resolve_inflight = lambda db, bad, stream=None: (bad, [], [])
    (one,) = eng.check_data("appdb", table="dbo.orders")
    assert one.status == "diff" and one.table == "dbo.orders"
    assert one.rows_apart == {"dbo.orders": {
        "missing": 0, "extra": 1, "changed": 0, "exact": True}}
    # the whole database's finding names no one table
    (whole,) = [r for r in eng.check_data("appdb") if r.check == "data"]
    assert whole.table == ""


def test_the_rows_a_filter_selects_are_the_ones_counted_and_hashed(
        monkeypatch, tmp_path):
    """The rows the filter leaves out are the target's own: counted by the
    filter on both sides, and the target's rows outside it are said."""
    where = {"dbo.orders": "region = 'TH'"}

    def answer(side, sql):
        if "sys.partitions" in sql:
            return [("dbo.orders", 100), ("dbo.lines", 5)]
        if "count_big" in sql and "region = 'TH'" in sql:
            if "= 0" in sql:
                return [(2,)] if side == "dst" else [(0,)]
            return [(40,)]
        return []
    server = Server().on("sys.partitions|count_big", answer)
    eng = engine(monkeypatch, tmp_path, server, mapping={"where": where})
    (got,) = eng.check_counts("appdb")
    assert got.status == "diff"
    assert "dbo.orders dst holds 2 rows the hop's row filter excludes" \
        in got.detail
    # counted by the filter, not by the whole table's 100 rows
    assert "src=100" not in got.detail
    counted = server.sent(r"count_big\(\*\) from \[dbo\]\.\[orders\] t where"
                          r" region = 'TH'$")
    assert len(counted) == 2
    # a hop with no filter counts as it did
    server.asked.clear()
    eng = engine(monkeypatch, tmp_path, server)
    (got,) = eng.check_counts("appdb")
    assert got.status == "ok"
    assert not server.sent("region")


def test_the_hash_pass_reads_the_filter_on_both_sides(monkeypatch, tmp_path):
    where = {"dbo.orders": "region = 'TH'"}

    def answer(side, sql):
        if "sys.tables" in sql:
            return [("dbo.orders",)]
        if "rows_hashed" in sql:
            return [(3, 7)]
        if "= 0" in sql:
            return [(0,)]
        return []
    server = Server().on("sys.tables|rows_hashed|count_big", answer)
    eng = engine(monkeypatch, tmp_path, server, mapping={"where": where})
    eng._row_json = lambda db, t: ("x", [])
    eng._health = lambda side, db=None: None
    (got,) = [r for r in eng.check_data("appdb") if r.check == "data"]
    assert got.status == "ok", got.detail
    hashed = server.sent("rows_hashed")
    assert len(hashed) == 2
    assert all("where region = 'TH'" in s for s in hashed)
    # the same pass with no filter reads the whole table
    server.asked.clear()
    eng = engine(monkeypatch, tmp_path, server)
    eng._row_json = lambda db, t: ("x", [])
    eng._health = lambda side, db=None: None
    eng.check_data("appdb")
    assert not any(" where " in s for s in server.sent("rows_hashed"))


def test_a_table_the_hop_renames_is_compared_by_the_pair(monkeypatch,
                                                         tmp_path):
    """Hashed whole under the source's name, the table it was renamed to
    was never compared and nothing said so: the pair reads the name
    rules, in the check, the schema and the repair."""
    renames = {"tables": {"dbo.orders": "dbo.orders_new"}}
    server = Server().on("rows_hashed", [(1, 1)]).on(
        "sys.tables", [("dbo.orders",), ("dbo.lines",)])
    eng = engine(monkeypatch, tmp_path, server, mapping=renames)
    assert eng.through_pair("appdb", "dbo.orders")
    assert not eng.through_pair("appdb", "dbo.lines")
    assert eng._target_name("appdb", "dbo.orders") == "dbo.orders_new"
    assert eng._target_name("appdb", "dbo.lines") == "dbo.lines"
    asked = []

    class Pair:
        def check_data(self, db, table, stream=None):
            asked.append(table)
            return []
    eng.columns_pair = lambda: Pair()
    eng.check_data("appdb", table="dbo.orders")
    assert asked == ["dbo.orders"]
    # the whole database's pass leaves the renamed table to the pair
    eng._row_json = lambda db, t: ("x", [])
    eng._health = lambda side, db=None: None
    eng.mapped_tables = lambda db: ["dbo.orders"]
    eng.check_data("appdb")
    assert asked == ["dbo.orders", "dbo.orders"]
    hashed = server.sent("rows_hashed")
    assert hashed and not any("orders" in s for s in hashed)


def test_a_renamed_table_is_counted_under_its_new_name(monkeypatch, tmp_path):
    renames = {"tables": {"dbo.orders": "dbo.orders_new"}}

    def answer(side, sql):
        return [("dbo.orders", 7)] if side == "src" else \
            [("dbo.orders_new", 7)]
    server = Server().on("sys.partitions", answer)
    eng = engine(monkeypatch, tmp_path, server, mapping=renames)
    (got,) = eng.check_counts("appdb")
    assert got.status == "ok", got.detail

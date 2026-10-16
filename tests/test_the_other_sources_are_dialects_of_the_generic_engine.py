"""Netezza, SAP HANA, Informix, Progress OpenEdge, Vertica, Exasol and
Databricks are dialects of the generic engine (backlog W5): each a row of
`dbapi.DIALECTS` read by one connection and one engine. A stand-in driver
per dialect answers its own catalogue (the real servers are reached by
nothing here), and what is proved is migkit's side: the scope read off the
dialect's catalogue, a keyed read, a page in the dialect's way, a bulk way
chosen from a fact, and the driver missing named."""
import sys
import types
from types import SimpleNamespace

import pytest

from migkit import decide, engines
from migkit.engines import dbapi
from migkit.engines.generic import GenericEngine

PROVES = []

NAMES = ("netezza", "saphana", "informix", "progress", "vertica", "exasol",
         "databricks")


class Cursor:
    """A DB-API cursor over an in-memory table, answering the dialect's
    catalogue questions with what the stand-in server holds."""

    def __init__(self, server):
        self.server, self.description, self.rows = server, None, []

    def execute(self, sql, params=()):
        self.server.seen.append(sql)
        up = " ".join(sql.split())
        if up == self.server.dialect.tables_sql:
            self.rows = [("app", "orders"), ("app", "tmp_scratch"),
                         (self.server.dialect.system_schemas[0], "kept")]
        elif up.startswith(("SELECT CURRENT_SCHEMA", "SELECT current_schema",
                            "SELECT CURRENT_SCHEMA()")):
            self.rows = [("app",)]
        elif "count(*)" in up.lower() and "orders" in up.lower():
            self.rows = [(len(self.server.table),)]
        elif up.startswith("SELECT * FROM") or " FIRST " in up or \
                " TOP " in up:
            self.rows = self.server.table
        elif up.startswith("SELECT COUNT(*)"):
            self.rows = [(1 if self.server.may else 0,)]
        else:
            self.rows = []
        self.description = [("c",)] if self.rows else None

    def fetchall(self):
        return list(self.rows)

    def close(self):
        pass


class Server:
    def __init__(self, dialect, may=True):
        self.dialect, self.may, self.seen = dialect, may, []
        self.table = [(1, "a"), (2, "b"), (3, "c")]

    def cursor(self):
        return Cursor(self)

    def commit(self):
        pass

    def close(self):
        pass


def engine(name, tmp_path, server, landing=""):
    cls = engines._class_for(name)
    eng = cls.__new__(cls)
    opts = {"url": f"{name}://u:p@db1.example.com/app", "stage": landing}
    eng.hop = SimpleNamespace(
        name="g", report_dir=lambda db: tmp_path, engine=name,
        options={"key": "id"}, target=SimpleNamespace(options=opts),
        source=SimpleNamespace(options=opts))
    eng._conns = {"src": dbapi.DialectConnection(
        dbapi.DIALECTS[name], server), "dst": dbapi.DialectConnection(
        dbapi.DIALECTS[name], server)}
    eng.in_scope = lambda db, t: not t.endswith("tmp_scratch")
    return eng


def test_each_dialect_is_a_name_the_generic_engine_serves():
    for name in NAMES:
        cls = engines._class_for(name)
        assert issubclass(cls, GenericEngine)
        assert issubclass(cls, dbapi.DialectEngine)
        assert engines.canonical(name) == name
        assert name in dbapi.DIALECTS
    assert engines.canonical("hana") == "saphana"
    assert engines.SCHEMES["vertica"] == "vertica"


@pytest.mark.parametrize("name", NAMES)
def test_the_scope_is_read_off_the_dialects_catalogue(name, tmp_path):
    eng = engine(name, tmp_path, Server(dbapi.DIALECTS[name]))
    got = eng._tables()
    assert got == ["app.orders"], got


@pytest.mark.parametrize("name", NAMES)
def test_a_name_is_quoted_and_folded_the_dialects_way(name):
    d = dbapi.DIALECTS[name]
    mark = d.quote_mark
    assert d.quote("id") == f"{mark}{'ID' if d.fold == 'upper' else 'id'}" \
        f"{mark}"
    assert d.quote("Mixed") == f"{mark}Mixed{mark}"
    assert d.quote(f"A{mark}b") == f"{mark}A{mark}{mark}b{mark}"


@pytest.mark.parametrize("name", NAMES)
def test_a_read_is_paged_the_dialects_way(name):
    d = dbapi.DIALECTS[name]
    got = d.page("SELECT * FROM t", None, 10)
    if d.paging == "limit":
        assert got == "SELECT * FROM t LIMIT 10"
        assert d.page("SELECT * FROM t", 5, 10).endswith("LIMIT 10 OFFSET 5")
    elif d.paging == "first":
        assert got == "SELECT FIRST 10 * FROM t"
        assert d.page("SELECT * FROM t", 5, 10) == \
            "SELECT SKIP 5 FIRST 10 * FROM t"
    else:
        assert got == "SELECT TOP 10 * FROM t"


@pytest.mark.parametrize("name", NAMES)
def test_a_held_sample_is_read_through_the_dialects_page(name, tmp_path):
    server = Server(dbapi.DIALECTS[name])
    eng = engine(name, tmp_path, server)
    eng.SAMPLE_ROWS = 2
    assert eng._held_bytes("app.orders") is not None
    page = [s for s in server.seen if s.startswith("SELECT") and
            ("LIMIT 2" in s or "FIRST 2" in s or "TOP 2" in s)]
    assert page, server.seen


def test_a_declared_type_is_parsed_into_the_classes_every_dialect_shares():
    from sqeleton.abcs import database_types as t
    vertica = dbapi.DIALECTS["vertica"]
    assert isinstance(vertica.parse_type(("s", "t"), "a", "int"), t.Integer)
    dec = vertica.parse_type(("s", "t"), "a", "numeric(10,3)")
    assert isinstance(dec, t.Decimal) and dec.precision == 3
    # Informix reports a type as a number
    informix = dbapi.DIALECTS["informix"]
    assert isinstance(informix.parse_type(("s", "t"), "a", "2"), t.Integer)
    assert isinstance(informix.parse_type(("s", "t"), "a", "10"),
                      t.Timestamp)
    assert isinstance(vertica.parse_type(("s", "t"), "a", "geometry"),
                      t.UnknownColType)


def test_a_keyed_read_is_asked_with_the_dialects_catalogue_and_quoting(
        tmp_path):
    conn = dbapi.DialectConnection(dbapi.DIALECTS["saphana"],
                                   Server(dbapi.DIALECTS["saphana"]))
    sql = conn.select_table_schema(("app", "orders"))
    assert "SYS.TABLE_COLUMNS" in sql and "'APP'" in sql and "'ORDERS'" in sql
    for name, written in (("netezza", '"ID" = ?'), ("databricks",
                                                    "`id` = ?")):
        eng = engine(name, tmp_path, Server(dbapi.DIALECTS[name]))
        where, params = eng._keyed_where("src", "qmark", ["id"], [7])
        assert (where, params) == (written, (7,))


def test_the_driver_missing_is_named_and_nothing_is_read(tmp_path):
    for name in NAMES:
        d = dbapi.DIALECTS[name]
        saved = sys.modules.get(d.driver)
        sys.modules[d.driver] = None
        try:
            with pytest.raises(SystemExit) as e:
                d.open(f"{name}://u:p@db1.example.com/app", "source")
        finally:
            if saved is not None:
                sys.modules[d.driver] = saved
            else:
                sys.modules.pop(d.driver, None)
        said = str(e.value)
        assert d.pip in said and "not installed" in said, said
        assert "reladiff" not in said and "sqeleton" not in said


def test_an_address_the_driver_refuses_is_said_not_raised(tmp_path):
    d = dbapi.DIALECTS["vertica"]
    boom = types.ModuleType("vertica_python")

    def connect(**kw):
        raise OSError("connection refused by 10.0.0.9")
    boom.connect = connect
    sys.modules["vertica_python"] = boom
    try:
        with pytest.raises(SystemExit) as e:
            d.open("vertica://u:p@10.0.0.9/app", "target")
    finally:
        sys.modules.pop("vertica_python")
    assert "cannot open the target Vertica address" in str(e.value)


def _facts(**kw):
    base = {"way": None, "landing": "", "writer": False, "streams": False,
            "tool": False, "may": True, "rows": 10, "empty": True}
    return {**base, **kw}


def test_the_client_way_is_taken_when_every_need_holds():
    got = decide.dialect_bulk(_facts(way="client", streams=True))
    assert got.path == "client"


@pytest.mark.parametrize("fact,why", [
    ({"may": None}, "grants were not read"),
    ({"may": False}, "may not run"),
    ({"rows": 0}, "no rows"),
    ({"empty": False}, "holds rows already")])
def test_a_bulk_way_gives_way_to_the_copier_on_each_fact_that_fails(fact,
                                                                    why):
    got = decide.dialect_bulk(_facts(way="client", streams=True, **fact))
    assert got.path == "copier"
    assert why in got.reason


def test_a_landing_way_needs_a_store_and_a_writer_for_it():
    held = _facts(way="landing", landing="s3://bucket/x", writer=True)
    assert decide.dialect_bulk(held).path == "landing"
    for fact, why in (({"landing": ""}, "no landing store"),
                      ({"writer": False}, "nothing here writes")):
        got = decide.dialect_bulk({**held, **fact})
        assert got.path == "copier" and why in got.reason


def test_a_tool_way_needs_its_client_here_and_progress_has_no_way():
    assert decide.dialect_bulk(_facts(way="tool", tool=True)).path == "tool"
    assert decide.dialect_bulk(_facts(way="tool")).path == "copier"
    got = decide.dialect_bulk(_facts(way=None, none_why=dbapi.DIALECTS[
        "progress"].no_bulk))
    assert got.path == "copier" and "no bulk loader" in got.reason


@pytest.mark.parametrize("name,kind", [
    ("vertica", "client"), ("netezza", "client"), ("saphana", "landing"),
    ("exasol", "landing"), ("databricks", "landing"),
    ("informix", "tool"), ("progress", None)])
def test_each_dialects_bulk_kind_is_the_one_its_server_offers(name, kind):
    bulk = dbapi.DIALECTS[name].bulk
    assert (bulk.kind if bulk else None) == kind


def test_the_way_is_chosen_from_what_the_target_answers(tmp_path,
                                                        monkeypatch):
    # the client way stands on the driver loading on this machine
    monkeypatch.setitem(sys.modules, dbapi.DIALECTS["vertica"].driver,
                        types.ModuleType("driver"))
    server = Server(dbapi.DIALECTS["vertica"], may=True)
    eng = engine("vertica", tmp_path, server)
    eng._on = lambda which, table, db="-": table
    counts = iter([3, 0])
    eng._count = lambda side, table: next(counts)
    eng.hop.options = {"key": "id"}
    got = eng.bulk_way("app.orders")
    assert got.path == "client"
    refused = Server(dbapi.DIALECTS["vertica"], may=False)
    eng2 = engine("vertica", tmp_path, refused)
    eng2._on = lambda which, table, db="-": table
    assert eng2.bulk_way("app.orders").path == "copier"


def test_a_bulk_statement_names_the_table_and_columns_in_the_dialects_way(
        tmp_path):
    eng = engine("vertica", tmp_path, Server(dbapi.DIALECTS["vertica"]))
    eng._on = lambda which, table, db="-": table
    eng._path = lambda conn, t: tuple(t.split("."))
    sql = eng._bulk_statement("app.orders", ["id", "name"], "/tmp/x.csv")
    assert sql.startswith('COPY "app"."orders" ("id", "name") FROM LOCAL'
                          " '/tmp/x.csv'")


def test_a_repair_is_refused_with_its_reason_and_writes_nothing(tmp_path):
    eng = engine("exasol", tmp_path, Server(dbapi.DIALECTS["exasol"]))
    with pytest.raises(SystemExit) as e:
        eng._apply_rows_borrowed("app", SimpleNamespace(scope="t.orders"))
    assert "nothing was changed" in str(e.value)


class Checkpoint(dict):
    saves = 0

    def save(self):
        self.saves += 1


def _bulk_engine(tmp_path, landed):
    eng = engine("vertica", tmp_path, Server(dbapi.DIALECTS["vertica"]))
    ran = []
    eng.bulk_way = lambda table, log=None: decide.dialect_bulk(
        _facts(way="client", streams=True))
    eng._copy_columns = lambda table: ["id", "name"]
    eng._on = lambda which, table, db="-": table
    eng._bulk_statement = lambda table, cols, where: f"COPY {table}"
    eng._run_bulk = lambda kind, statement: ran.append((kind, statement))
    eng._count = lambda side, table: landed
    eng._close = lambda: None
    path = tmp_path / "rows.csv"
    path.write_text("1,a\n2,b\n3,c\n")
    eng._land_source = lambda table, cols, kind, chunk=None: (str(path), 3)
    return eng, ran


def test_a_table_loaded_in_bulk_is_counted_before_it_is_done(tmp_path):
    eng, ran = _bulk_engine(tmp_path, landed=3)
    ck, said = Checkpoint(), []
    eng.move_table("app", "app", "orders", 100, ck, said.append)
    assert ran == [("client", "COPY app.orders")]
    assert ck["app.orders"] == {"done": True, "rows": 3} and ck.saves == 1
    assert any("loaded by the client bulk way" in m for m in said)


def test_a_bulk_load_that_lands_short_is_not_marked_done(tmp_path):
    eng, ran = _bulk_engine(tmp_path, landed=2)
    ck = Checkpoint()
    with pytest.raises(SystemExit) as e:
        eng.move_table("app", "app", "orders", 100, ck, lambda m: None)
    assert "2 rows on the target for 3" in str(e.value)
    assert not ck.get("app.orders", {}).get("done") and ck.saves == 0


def test_a_table_the_climb_gives_to_the_copier_is_copied_by_the_parent(
        tmp_path, monkeypatch):
    eng, ran = _bulk_engine(tmp_path, landed=0)
    eng.bulk_way = lambda table, log=None: decide.dialect_bulk(
        _facts(way="client", streams=True, empty=False))
    called = []
    monkeypatch.setattr(GenericEngine, "move_table",
                        lambda self, *a: called.append(a))
    eng.move_table("app", "app", "orders", 100, Checkpoint(), None)
    assert called and not ran


def test_a_client_way_without_its_driver_gives_way_to_the_copier(
        monkeypatch):
    import importlib

    from migkit.config import Endpoint, Hop
    from migkit.engines import dbapi
    real = importlib.import_module

    def fake(name, *a, **k):
        if name == dbapi.DIALECTS["vertica"].driver:
            raise ImportError(name)
        return real(name, *a, **k)
    monkeypatch.setattr(importlib, "import_module", fake)
    hop = Hop(name="v", engine="vertica",
              source=Endpoint(host="10.0.0.1", port=5433, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=5433, user="u",
                              password="CHANGE_ME"), databases=["d"])
    eng = dbapi.VerticaEngine(hop)
    monkeypatch.setattr(decide, "record", lambda *a, **k: None)
    got = eng.bulk_way("s.t")
    assert got.path == "copier", got
    assert "needs its Python driver" in got.reason, got.reason


def test_the_bulk_landing_streams_the_cursors_batches_and_counts_them(
        tmp_path):
    from types import SimpleNamespace

    from migkit.engines import dbapi
    batches = [[(1, "a"), (2, "b")], [(3, "c"), (4, "d")], [(5, "e")], []]
    asked = []

    class Cursor:
        def execute(self, sql):
            asked.append(sql)

        def fetchmany(self, size):
            asked.append(size)
            return batches.pop(0)

        def close(self):
            pass
    eng = dbapi.VerticaEngine.__new__(dbapi.VerticaEngine)
    conn = SimpleNamespace(_conn=SimpleNamespace(cursor=Cursor),
                           dialect=SimpleNamespace(quote=lambda c: c))
    eng._connect = lambda side: conn
    eng._filter = lambda table: ""
    eng._on = lambda side, table: table
    eng._from = lambda conn, table: table
    eng._masked = lambda table, cols, rows: rows
    where, n = eng._land_source("s.t", ["id", "v"], "client", chunk=2)
    try:
        assert n == 5
        assert open(where).read().count("\n") == 5
        assert asked[1:] == [2, 2, 2, 2]
    finally:
        import os
        os.unlink(where)

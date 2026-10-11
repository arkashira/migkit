"""Teradata as a side of a pair (backlog ER3), held to what does not need a
system: no Teradata runs in a container, and ClearScape is an account
migkit may not use here - `test_teradata_against_a_server.py` proves it on
a Vantage Express VM where one is named.

What is held: each `DBC.ColumnsV` type as the type it is declared and the
class it is compared as; how a session is opened (ANSI mode, the data
encrypted, the endpoint's TLS); which index is a table's key; a read in
the driver's parallel sessions where a table is large, and one that cannot
start switched from and said; a key read in pages that orders and resumes
text the same way; PERIOD, INTERVAL and TIME WITH TIME ZONE read as the
values they are and written back through their slots; a batch bulk-loaded
into a table of its own and merged in, and a bulk load that dropped a row
- as FastLoad does with a duplicate, without failing - switched from and
written by statements instead; the same-engine digest's SQL keeping case,
trailing blanks and NULL apart, and the pair saying "different" where the
two systems' sums differ; what the deep check finds on a target; the
ledger a tail's batch lands with; users and rights; and what the DBA is
asked for.
"""
import datetime
import re
import sys
import types
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("verify.users", "teradata"),
    ("cutover.two-way", "teradata"),
    ("security.tls", "teradata"),
]


class DriverError(Exception):
    """An error as the driver raises one."""


DriverError.__module__ = "teradatasql"


class _Cursor:
    def __init__(self, conn):
        self.conn, self.rows, self.rowcount = conn, [], -1
        self.description = None

    def execute(self, sql, params=()):
        self.conn.fake.said.append((self.conn.host, sql, tuple(params or ())))
        got = self.conn.fake.answer(self.conn.host, sql, tuple(params or ()))
        if isinstance(got, Exception):
            raise got
        if isinstance(got, int):
            self.rowcount, self.rows = got, []
            return
        self.rows = [list(r) for r in got]
        self.rowcount = len(self.rows)

    def executemany(self, sql, seq):
        seq = [tuple(r) for r in seq]
        self.conn.fake.said.append((self.conn.host, "MANY " + sql, seq))
        got = self.conn.fake.answer(self.conn.host, "MANY " + sql, seq)
        if isinstance(got, Exception):
            raise got

    def fetchall(self):
        rows, self.rows = self.rows, []
        return rows

    def fetchmany(self, n):
        rows, self.rows = self.rows[:n], self.rows[n:]
        return rows

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None


class _Conn:
    def __init__(self, fake, params):
        self.fake, self.params = fake, params
        self.host = params["host"]
        self.autocommit = True

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.fake.said.append((self.host, "COMMIT", ()))

    def rollback(self):
        self.fake.said.append((self.host, "ROLLBACK", ()))

    def close(self):
        pass


class Fake:
    """A Teradata driver that answers from `answer(host, sql, args)`."""

    def __init__(self, answer):
        self.answer, self.said, self.params = answer, [], []

    def module(self):
        mod = types.ModuleType("teradatasql")
        mod.connect = self.connect
        mod.OperationalError = DriverError
        return mod

    def connect(self, **params):
        self.params.append(params)
        return _Conn(self, params)

    def sql(self, host=None):
        return [s for h, s, _ in self.said if host is None or h == host]


SRC, DST = "10.0.0.7", "10.0.0.8"

#: DBC.ColumnsV rows: name, type, length, digits, fraction, charset, case,
#: nullable, default, identity
ORDERS = [
    ("id", "I", 4, None, None, None, None, "N", None, None),
    ("code", "CF", 10, None, None, 1, "N", "Y", None, None),
    ("name", "CV", 200, None, None, 2, "C", "Y", None, None),
    ("amount", "D", 8, 12, 2, None, None, "Y", None, None),
    ("ratio", "N", 18, -128, -128, None, None, "Y", None, None),
    ("span", "PD", 8, None, None, None, None, "Y", None, None),
    ("wait", "DS", 12, 4, 6, None, None, "Y", None, None),
    ("at_tz", "TZ", 12, None, 6, None, None, "Y", None, None),
    ("blob", "BV", 16, None, None, None, None, "Y", None, None),
]


def _hop(**src_options):
    hop = Hop(name="td", engine="teradata",
              source=Endpoint(host=SRC, user="MIGKIT", password="CHANGE_ME",
                              options=src_options),
              target=Endpoint(host=DST, user="MIGKIT", password="CHANGE_ME"),
              databases=["shop"])
    hop.report_dir = lambda db=None: __import__("pathlib").Path("/tmp")
    return hop


def _catalog(host, sql, args, columns=ORDERS, rows=None, sums=None,
             key="K", nullable_key=False, sessions=True):
    """The DBC views of a database holding `orders`, and what a test adds."""
    if "from DBC.TablesV" in sql:
        return [("orders",), ("migkit_origin",),
                ("migkit_stage_orders_0a1b2c3d4e5f",)]
    if "from DBC.ColumnsV" in sql:
        cols = list(columns)
        if nullable_key:
            cols = [c if c[0] != "id" else c[:7] + ("Y",) + c[8:]
                    for c in cols]
        return cols
    if "from DBC.IndicesV" in sql and "'K', 'P', 'Q'" in sql:
        return [(key, "Y", 1, "id", 1)]
    if "from DBC.IndicesV" in sql:
        return []
    if "DBC.TableSizeV" in sql:
        return [("orders", 5_000_000)]
    if "DBC.TableStatsV" in sql:
        return [("orders", rows if rows is not None else 10)]
    if "DBC.SessionInfoV" in sql:
        return [(1,)] if sessions else DriverError("3523 no SELECT access")
    return None


@pytest.fixture
def fake(monkeypatch):
    holder = {}

    def answer(host, sql, args):
        got = holder["answer"](host, sql, args)
        return [] if got is None else got
    f = Fake(answer)
    holder["answer"] = _catalog
    f.set = lambda fn: holder.__setitem__("answer", fn)
    monkeypatch.setitem(sys.modules, "teradatasql", f.module())
    return f


def _engine(**src_options):
    from migkit.engines.teradata import TeradataEngine
    return TeradataEngine(_hop(**src_options))


# ---- types ------------------------------------------------------------------

def test_each_type_is_declared_and_compared_as_what_it_is(fake):
    from migkit import canon
    eng = _engine()
    got = dict(eng.neutral_columns("src", "shop", "orders"))
    assert got == {
        "id": "INTEGER",
        "code": "CHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC",
        "name": "VARCHAR(100) CHARACTER SET UNICODE",
        "amount": "DECIMAL(12,2)",
        "ratio": "NUMBER",
        "span": "PERIOD(DATE)",
        "wait": "INTERVAL DAY(4) TO SECOND(6)",
        "at_tz": "TIME(6) WITH TIME ZONE",
        "blob": "VARBYTE(16)"}, got
    classes = {n: canon.type_class("teradata", t) for n, t in got.items()}
    assert classes == {"id": "integer", "code": "text", "name": "text",
                       "amount": "decimal", "ratio": "decimal",
                       "span": "text", "wait": "interval", "at_tz": "time",
                       "blob": "bytes"}
    assert canon.time_meaning("teradata", got["at_tz"]) == canon.INSTANT
    assert canon.time_meaning("teradata", "TIMESTAMP(6)") == canon.WALL
    # a type migkit has no name for is said as uncomparable, never guessed
    from migkit.engines.teradata import declared_of
    assert declared_of("UT", 0, None, None, None, None) is None
    assert canon.type_class("teradata", "UDT UT") is None
    # the tables migkit keeps in a database are none of the application's
    assert eng.neutral_tables("src", "shop") == ["orders"]


@pytest.mark.parametrize("declared,text,value,back", [
    ("INTERVAL DAY(4) TO SECOND(6)", "-5 04:05:06.500000",
     (0, -5, -(4 * 3600 + 5 * 60 + 6) * 10 ** 9 - 5 * 10 ** 8),
     "-5 04:05:06.5"),
    ("INTERVAL YEAR(2) TO MONTH", " 12-03", (147, 0, 0), "12-03"),
    ("INTERVAL HOUR(2) TO MINUTE", " 4:05", (0, 0, (4 * 60 + 5) * 60 * 10 ** 9),
     "4:05"),
    ("INTERVAL SECOND(2,3)", " 6.250", (0, 0, 6_250_000_000), "6.25"),
])
def test_an_interval_is_read_and_written_as_its_parts(declared, text, value,
                                                      back):
    from migkit.engines.teradata import interval_text, parse_interval
    got = parse_interval(declared, text)
    assert tuple(got) == value
    assert interval_text(declared, got) == back
    assert parse_interval(declared, interval_text(declared, got)) == got


def test_an_interval_a_type_has_no_room_for_is_refused():
    from migkit import intervals
    from migkit.engines.teradata import interval_text
    with pytest.raises(SystemExit, match="holds months"):
        interval_text("INTERVAL DAY(4) TO SECOND(6)",
                      intervals.Interval(1, 2, 0))
    with pytest.raises(SystemExit, match="fraction of a second"):
        interval_text("INTERVAL HOUR(2) TO MINUTE",
                      intervals.Interval(0, 0, 60 * 10 ** 9 + 1))
    with pytest.raises(SystemExit, match="days or time"):
        interval_text("INTERVAL YEAR(2) TO MONTH",
                      intervals.Interval(0, 1, 0))


def test_a_period_is_carried_as_its_begin_and_its_end():
    from migkit.engines.teradata import period_literal, period_text
    got = period_text("('2005-02-03', '2006-02-04')")
    assert got == "[2005-02-03, 2006-02-04)"
    assert period_literal(got) == "('2005-02-03', '2006-02-04')"
    # two different periods are two texts
    assert period_text("('2005-02-03', '2006-02-05')") != got


def test_values_are_read_as_the_types_mean_them(fake):
    tz = datetime.timezone(datetime.timedelta(hours=7))

    def answer(host, sql, args):
        got = _catalog(host, sql, args)
        if got is not None:
            return got
        if sql.startswith("select") and '"orders"' in sql:
            return [[1, "AB        ", "x", Decimal("1.50"), Decimal("2"),
                     "('2024-01-01', '2024-02-01')", " 1 00:00:01.000000",
                     datetime.time(10, 0, tzinfo=tz), b"\x00\x01"]]
        return None
    fake.set(answer)
    from migkit import canon
    eng = _engine()
    cols = [(n, canon.type_class("teradata", t))
            for n, t in eng.neutral_columns("src", "shop", "orders")]
    (row,) = [r for b in eng.neutral_batches("src", "shop", "orders", cols)
              for r in b]
    assert row[1] == "AB"                         # CHAR, unpadded
    assert row[5] == "[2024-01-01, 2024-02-01)"   # PERIOD
    assert tuple(row[6]) == (0, 1, 10 ** 9)       # INTERVAL, three parts
    assert row[7] == datetime.time(3, 0)          # the instant, in UTC
    # the same clock in another zone is another instant
    assert row[7] != datetime.time(10, 0)


# ---- sessions --------------------------------------------------------------

def test_a_session_is_ansi_encrypted_and_as_tls_the_endpoint_says(fake):
    eng = _engine(sslmode="verify-full", sslrootcert="/etc/ca.pem")
    eng.neutral_tables("src", "shop")
    p = fake.params[-1]
    assert p["tmode"] == "ANSI" and p["encryptdata"] == "true"
    assert p["sslmode"] == "VERIFY-FULL" and p["sslca"] == "/etc/ca.pem"
    assert p["dbs_port"] == "1025" and p["database"] == "shop"
    eng.neutral_tables("dst", "shop")
    assert "sslmode" not in fake.params[-1]


def test_without_the_driver_it_says_how_to_put_it_in_place(monkeypatch):
    monkeypatch.setitem(sys.modules, "teradatasql", None)
    with pytest.raises(SystemExit) as e:
        _engine()._connect("src", "shop")
    assert "migkit doctor --install" in str(e.value)
    assert "teradatasql" not in str(e.value)


# ---- the key ---------------------------------------------------------------

def test_the_key_is_the_primary_key_or_a_unique_index_of_no_nulls(fake):
    eng = _engine()
    assert eng.neutral_key("src", "shop", "orders") == ["id"]
    fake.set(lambda h, s, a: _catalog(h, s, a, key="P"))
    assert _engine().neutral_key("src", "shop", "orders") == ["id"]
    # a unique primary index over a column that takes NULL is no key: a
    # row with a NULL there would be missed by every read by key
    fake.set(lambda h, s, a: _catalog(h, s, a, key="P", nullable_key=True))
    assert _engine().neutral_key("src", "shop", "orders") == []


# ---- reading ---------------------------------------------------------------

def test_a_large_table_is_read_in_parallel_sessions_and_a_small_one_not(
        fake):
    from migkit import canon
    fake.set(lambda h, s, a: _catalog(h, s, a, rows=250_000))
    eng = _engine()
    cols = [("id", "integer")]
    eng.neutral_digest("src", "shop", "orders", cols)
    reads = [s for s in fake.sql(SRC) if '"orders"' in s and "select" in s]
    assert reads and reads[-1].startswith("{fn teradata_try_fastexport}")
    # a sum needs no order, and parallel sessions read quickest without one
    assert "order by" not in reads[-1]
    fake.said.clear()
    fake.set(lambda h, s, a: _catalog(h, s, a, rows=900))
    eng = _engine()
    eng.neutral_digest("src", "shop", "orders", cols)
    reads = [s for s in fake.sql(SRC) if '"orders"' in s and "select" in s]
    assert reads and not reads[-1].startswith("{fn")
    assert canon.type_class("teradata", "INTEGER") == "integer"


def test_parallel_sessions_that_cannot_start_are_switched_from(fake):
    def answer(host, sql, args):
        got = _catalog(host, sql, args, rows=250_000)
        if got is not None:
            return got
        if sql.startswith("{fn teradata_try_fastexport}"):
            return DriverError("[Error 3695] FastExport could not begin")
        if '"orders"' in sql:
            return [[1], [2]]
        return None
    fake.set(answer)
    eng = _engine()
    said = []
    eng.lend_log(said.append)
    got = eng.neutral_digest("src", "shop", "orders", [("id", "integer")])
    assert got[0] == 2
    assert any("switching to one statement's rows" in s for s in said), said
    reads = [s for s in fake.sql(SRC) if '"orders"' in s]
    assert reads[-2].startswith("{fn teradata_try_fastexport}")
    assert not reads[-1].startswith("{fn")


def test_a_page_by_key_orders_and_resumes_text_the_same_way(fake):
    cols = [("code", "CF", 10, None, None, 1, "N", "N", None, None),
            ("id", "I", 4, None, None, None, None, "N", None, None)]

    def answer(host, sql, args):
        if "from DBC.ColumnsV" in sql:
            return cols
        if "from DBC.IndicesV" in sql and "'K'" in sql:
            return [("K", "Y", 1, "code", 1), ("K", "Y", 1, "id", 2)]
        return _catalog(host, sql, args)
    fake.set(answer)
    eng = _engine()
    eng.neutral_read("src", "shop", "orders",
                     [("code", "text"), ("id", "integer")],
                     after=("a", 5), limit=100)
    (sql,) = [s for s in fake.sql(SRC) if s.startswith("select top")]
    assert sql.startswith("select top 100 ")
    # ordered and compared alike: a NOT CASESPECIFIC column orders without
    # case, and a page resumed with case would pass rows over
    assert 'order by "code" (CASESPECIFIC), "id"' in sql
    assert '("code" (CASESPECIFIC) > ?)' in sql
    assert '"code" (CASESPECIFIC) = ? and "id" > ?' in sql


# ---- writing ---------------------------------------------------------------

def _rows(n):
    return [[i, "c", "n", Decimal("1.00"), Decimal("2"),
             "[2024-01-01, 2024-02-01)", None, None, b"\x01"]
            for i in range(n)]


def _cols(eng):
    from migkit import canon
    return [(n, canon.type_class("teradata", t))
            for n, t in eng.neutral_columns("dst", "shop", "orders")]


def test_a_small_batch_is_deleted_by_key_and_inserted_in_one_transaction(
        fake):
    eng = _engine()
    eng.neutral_write("dst", "shop", "orders", _cols(eng), _rows(3))
    said = fake.sql(DST)
    delete = [s for s in said if s.startswith("delete from")]
    assert delete == ['delete from "shop"."orders" where ("id" = ?) or'
                      ' ("id" = ?) or ("id" = ?)']
    (insert,) = [s for s in said if s.startswith("MANY")]
    # bytes bound as VARBYTE, so a NULL of them is taken; a PERIOD and an
    # INTERVAL through a CAST from their text
    assert insert.startswith("MANY {fn teradata_parameter(9,VARBYTE(16))}"
                             "insert into")
    assert "cast(? as PERIOD(DATE))" in insert
    assert "cast(? as INTERVAL DAY(4) TO SECOND(6))" in insert
    (_, _, values), = [x for x in fake.said if x[1].startswith("MANY")]
    assert values[0][5] == "('2024-01-01', '2024-02-01')"
    assert said[-1] == "COMMIT"


def test_a_large_batch_is_bulk_loaded_and_a_dropped_row_is_caught(
        fake, monkeypatch):
    from migkit.engines import teradata
    monkeypatch.setattr(teradata, "BULK_ROWS", 3)
    columns = [c for c in ORDERS if c[0] in ("id", "name", "amount")]
    landed = {"n": 4}

    def answer(host, sql, args):
        if "from DBC.ColumnsV" in sql:
            return columns
        got = _catalog(host, sql, args)
        if got is not None:
            return got
        if sql.startswith("{fn teradata_nativesql}"):
            return [("",)]
        if sql.startswith("select count(*) from") and "migkit_stage_" in sql:
            return [(landed["n"],)]
        return None
    fake.set(answer)
    eng = _engine()
    cols = [("id", "integer"), ("name", "text"), ("amount", "decimal")]
    rows = [[i, f"n{i}", Decimal("1.00")] for i in range(4)]
    eng.neutral_write("dst", "shop", "orders", cols, rows)
    said = fake.sql(DST)
    stage = [s for s in said if s.startswith("create multiset table")]
    assert stage and "with no data no primary index" in stage[0]
    load = [s for s in said if s.startswith("MANY {fn teradata_require"
                                            "_fastload}insert into")]
    assert load and "migkit_stage_orders_" in load[0]
    # its errors and warnings asked after the batch and after the commit
    asked = [s for s in said if s.startswith("{fn teradata_nativesql}")]
    assert len(asked) == 4
    assert any(s.startswith('delete from "shop"."orders" where exists')
               for s in said)
    assert any(s.startswith('insert into "shop"."orders"') and
               "migkit_stage_" in s for s in said)
    assert said[-1].startswith("drop table") and "migkit_stage_" in said[-1]
    # a duplicate row FastLoad dropped without failing: three landed of
    # four, and the batch goes by statements instead - said
    fake.said.clear()
    landed["n"] = 3
    eng = _engine()
    lines = []
    eng.lend_log(lines.append)
    eng.neutral_write("dst", "shop", "orders", cols, rows)
    said = fake.sql(DST)
    assert any("kept 3 of 4 rows" in s for s in lines), lines
    assert any("switching to deletes by key and inserts" in s
               for s in lines), lines
    assert any(s.startswith("MANY insert into") for s in said), said
    assert any(s.startswith("drop table") for s in said)


def test_a_table_is_made_multiset_with_its_text_casespecific(fake):
    from migkit import canon
    eng = _engine()
    ddl = eng.neutral_create_sql("dst", "shop", "t", [
        ("id", "integer", ()), ("name", "text", (40,)),
        ("amount", "decimal", (12, 2))], key=("id",))
    assert ddl.startswith('create multiset table "shop"."t"')
    assert '"id" BIGINT not null' in ddl
    assert "VARCHAR(40) CHARACTER SET UNICODE CASESPECIFIC" in ddl
    assert 'primary key ("id")' in ddl
    keyless = eng.neutral_create_sql("dst", "shop", "t",
                                     [("a", "integer", ())])
    assert keyless.endswith("no primary index")
    assert canon.ddl_type("teradata", "text", (), True).startswith(
        "VARCHAR(4000)")


# ---- the same-engine digest -----------------------------------------------

def test_the_rows_hash_keeps_case_blanks_and_null_apart(fake):
    eng = _engine()
    sql = eng.hash_sums_sql("src", "shop", "orders",
                            ["id", "name", "span"], ["s0", "s1", "s2", "s3"],
                            where="id > 5")
    assert 'char2hexint("name")' in sql
    assert 'begin("span"), end("span")' in sql
    assert "case when \"id\" is null then '1' else '0' end" in sql
    assert sql.count("hashbucket(hashrow('s") == 4
    assert sql.endswith("where (id > 5)")
    # a column HASHROW cannot take leaves it to the fold
    fake.set(lambda h, s, a: _catalog(h, s, a, columns=ORDERS + [
        ("doc", "JN", 100, None, None, None, None, "Y", None, None)]))
    eng = _engine()
    assert eng.hash_sums_sql("src", "shop", "orders", ["id", "doc"],
                             ["s0"]) is None


def test_the_pair_says_different_where_the_systems_sums_differ(
        fake, monkeypatch, tmp_path):
    from migkit.engines.hetero import HeteroEngine
    columns = [c for c in ORDERS if c[0] in ("id", "name")]
    sums = {SRC: (3, 11, 12, 13, 14), DST: (3, 11, 12, 13, 14)}

    def answer(host, sql, args):
        if "from DBC.ColumnsV" in sql:
            return columns
        got = _catalog(host, sql, args)
        if got is not None:
            return got
        if "hashbucket(hashrow(" in sql:
            return [sums[host]]
        return None
    fake.set(answer)
    monkeypatch.setattr(HeteroEngine, "_drill", lambda self, *a, **k: "")

    def engine():
        eng = _engine()
        eng.hop.report_dir = lambda db=None: tmp_path
        return eng
    pair = engine()._as_pair()
    (row,) = [r for r in pair._neutral_rows("shop") if r[0] == "shop.orders"]
    assert row[1] == "ok", row
    sums[DST] = (3, 11, 12, 13, 15)
    pair = engine()._as_pair()
    (row,) = [r for r in pair._neutral_rows("shop") if r[0] == "shop.orders"]
    assert row[1] == "diff", row
    # the same count and one salt's sum apart is a difference: a row's value
    # changed, never passed for its count
    assert "match but the contents do not" in row[2]


# ---- the target ------------------------------------------------------------

def test_the_deep_check_finds_a_table_left_mid_load_and_leftovers(fake):
    def answer(host, sql, args):
        if "from DBC.TablesV" in sql:
            return [("orders",), ("lines",), ("migkit_stage_x_1",),
                    ("orders_ERR_1",)]
        if sql.startswith('select top 1 1 from "shop"."lines"'):
            return DriverError("[Error 2652] Operation not allowed:"
                               " shop.lines is being Loaded.")
        return _catalog(host, sql, args)
    fake.set(answer)
    got = {r.scope: r for r in _engine().check_deep("shop")}
    assert got["shop target tables"].status == "diff"
    assert "lines" in got["shop target tables"].detail
    assert got["shop load leftovers"].status == "diff"
    assert "orders_ERR_1" in got["shop load leftovers"].detail
    fake.set(lambda h, s, a: [("orders",)] if "from DBC.TablesV" in s
             else _catalog(h, s, a))
    got = {r.scope: r for r in _engine().check_deep("shop")}
    assert got["shop target tables"].status == "ok"
    # beside it, the text stored twice as UTF-8 on the source: asked, its
    # answer whatever the stand-in gives
    assert "shop mojibake" in got
    # and the two systems' tables held to each other
    # (`tests/test_teradata_deep.py` reads each of those)
    assert {"shop target tables", "shop mojibake", "shop keys",
            "shop columns", "shop narrowing"} <= set(got)


def test_a_tail_batch_lands_with_its_line_in_the_ledger(fake):
    from migkit import twoway
    held = {}

    def answer(host, sql, args):
        if sql.startswith('update "shop"."migkit_origin"'):
            return 1 if held else 0
        if sql.startswith('insert into "shop"."migkit_origin"'):
            held["seen"] = args[1]
            return 1
        if sql.startswith('select seen from "shop"."migkit_origin"'):
            return [(held.get("seen"),)] if held else []
        if sql.startswith('update "shop"."orders"'):
            return 1
        return _catalog(host, sql, args)
    fake.set(answer)
    eng = _engine()
    eng.__dict__["_origin_made"] = True
    eng._batch_seen = '{"token": {"a": 1}, "batch": 7}'
    with eng._apply_session("dst", "shop"):
        eng._apply_upsert("dst", "shop", "orders", {"id": 1}, {"name": "x"})
    said = fake.sql(DST)
    assert said.index([s for s in said if "migkit_origin" in s][0]) < \
        said.index([s for s in said if 'update "shop"."orders"' in s][0])
    assert said[-1] == "COMMIT"
    assert twoway.batch_seen(eng) == held["seen"]
    assert eng.origin_seen("dst", "shop") == {"token": {"a": 1}, "batch": 7}


# ---- users, rights and what the DBA is asked for ------------------------

def test_users_and_rights_are_compared_and_granted(fake, tmp_path):
    from migkit import users

    def answer(host, sql, args):
        if "DBC.AllRightsV" in sql:
            base = [("ANA", "shop", "orders", "All", "R"),
                    ("ANA", "shop", "All", "All", "I"),
                    ("DBC", "shop", "All", "All", "R")]
            return base if host == SRC else base[:1]
        if "DBC.AllRoleRightsV" in sql:
            return [("READERS", "shop", "All", "All", "R")] \
                if host == SRC else []
        if "DBC.RoleMembersV" in sql:
            return [("READERS", "ANA")] if host == SRC else []
        return _catalog(host, sql, args)
    fake.set(answer)
    hop = _hop()
    hop.report_dir = lambda db=None: tmp_path
    lines = []
    out, s = users.compare(hop, lines.append)
    assert out["missing_on_target"] == ["role:READERS"]
    assert out["grants_missing"] == ["user:ANA"]
    assert "user:DBC" not in s
    assert s["user:ANA"]["grants"] == ['GRANT INSERT ON "shop"',
                                       'GRANT SELECT ON "shop"."orders"']
    said = []
    users.create(hop, False, {}, said.append)
    text = "\n".join(said)
    assert 'CREATE ROLE "READERS"' in text
    assert 'GRANT INSERT ON "shop" TO "ANA"' in text
    assert 'GRANT "READERS" TO "ANA"' in text
    assert "nothing done" in text


def test_the_dba_is_asked_for_what_is_missing(fake):
    from migkit import dba

    def answer(host, sql, args):
        if sql == "select user":
            return [("MIGKIT",)]
        if "not (exists" in sql:
            return [("orders",)]
        if "select 1 from (select 1 as x) d" in sql:
            return [] if args[1] in ("DT", "ST") else [(1,)]
        return _catalog(host, sql, args, sessions=False)
    fake.set(answer)
    eng = _engine()
    got = dba._teradata(eng.hop, eng, "src", "source", ["shop"], None,
                        True, False, False, False)
    read = next(n for n in got if n.what == "read every table of shop")
    assert read.ok is False and read.fix == [
        'grant select on "shop" to "MIGKIT";']
    sessions = next(n for n in got if "DBC.SessionInfoV" in n.what)
    assert sessions.ok is False and "single statements" in sessions.without
    got = dba._teradata(eng.hop, eng, "dst", "target", ["shop"], None,
                        False, True, False, False)
    write = next(n for n in got if n.what.startswith("make and write"))
    assert write.ok is False
    assert write.fix == ['grant drop table, statistics on "shop" to'
                         ' "MIGKIT";']


def test_the_drivers_every_option_is_used_or_explained():
    from migkit import tooluse
    import migkit.engines.teradata  # noqa: F401
    assert tooluse.unexplained("teradatasql") == []
    assert tooluse.stale("teradatasql") == []
    used = tooluse.used("teradatasql", "escape")
    assert {"teradata_try_fastexport", "teradata_require_fastload",
            "teradata_get_errors", "teradata_get_warnings",
            "teradata_parameter"} <= set(used), used
    assert re.search(r"LicenseRef", tooluse.REGISTRY["teradatasql"].licence)

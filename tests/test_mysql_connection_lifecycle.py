from types import SimpleNamespace
import pytest
from migkit import checkpoint as _cp
from migkit.engines.base import Result
from migkit.engines.mysql import MySQLEngine


def test_composite_key_range_splitting_covers_duplicate_prefixes():
    engine = object.__new__(MySQLEngine)
    engine.hop = SimpleNamespace(
        slice=2,
        report_dir=lambda db: None,
        source=SimpleNamespace(user="root"),
        target=SimpleNamespace(user="root"),
    )
    engine._d = lambda side, db: db
    engine._scope = lambda side, db, table: f"`{db}`.`{table}`"

    rows_data = [
        ("a", 1),
        ("a", 2),
        ("a", 3),
        ("b", 1),
        ("b", 2),
        ("c", 1),
    ]

    executed_queries = []

    def mock_q(side, sql, args=None):
        executed_queries.append((sql, args))
        if "limit" in sql:
            offset = args[-1]
            if "where" in sql:
                after_id, after_seq = args[0], args[1]
                filtered = [r for r in rows_data if (r[0], r[1]) > (after_id, after_seq)]
            else:
                filtered = rows_data
            if offset < len(filtered):
                return [filtered[offset]]
            return []
        return [(len(rows_data),)]

    engine._q = mock_q
    pk_ranges, col = engine._checksum_ranges("shop", "items", ["id", "seq"])

    assert col == "(`id`, `seq`)"
    assert len(pk_ranges) == 4
    assert pk_ranges[0][0] is None
    assert pk_ranges[-1][1] is None

    first_bound = "(CONVERT(X'61' USING utf8mb4), 2)"
    second_bound = "(CONVERT(X'62' USING utf8mb4), 1)"
    third_bound = "(CONVERT(X'63' USING utf8mb4), 1)"
    assert pk_ranges[0][1] == first_bound
    assert pk_ranges[1] == (first_bound, second_bound)
    assert pk_ranges[2] == (second_bound, third_bound)
    assert pk_ranges[3] == (third_bound, None)

    clauses = []
    for lo, hi in pk_ranges:
        w = _cp.where(col, lo, hi)
        clauses.append(f"where {w}")

    assert clauses[0] == f"where (`id`, `seq`) < {first_bound}"
    assert clauses[1] == f"where (`id`, `seq`) >= {first_bound} and (`id`, `seq`) < {second_bound}"
    assert clauses[2] == f"where (`id`, `seq`) >= {second_bound} and (`id`, `seq`) < {third_bound}"
    assert clauses[3] == f"where (`id`, `seq`) >= {third_bound}"

    for c in clauses:
        assert "``" not in c
        assert "`(`" not in c

    where_order = [q[0] for q in executed_queries if "where" in q[0]]
    assert len(where_order) >= 1
    assert "where (`id`, `seq`) > (%s, %s) order by `id`, `seq` limit %s, 1" in where_order[0]


def test_diff_table_clause_handles_composite_and_backtick_quoted():
    engine = object.__new__(MySQLEngine)

    col = "(`id`, `seq`)"
    if not col:
        c = ""
    elif col.startswith("`") or col.startswith("("):
        c = col
    else:
        c = f"`{col}`"
    w = _cp.where(c, "(1, 10)", "(2, 20)")
    clause_str = f"where {w}" if w else ""
    assert clause_str == "where (`id`, `seq`) >= (1, 10) and (`id`, `seq`) < (2, 20)"

    col_quoted = "`already_quoted`"
    if not col_quoted:
        cq = ""
    elif col_quoted.startswith("`") or col_quoted.startswith("("):
        cq = col_quoted
    else:
        cq = f"`{col_quoted}`"
    w_quoted = _cp.where(cq, 10, 20)
    assert w_quoted == "`already_quoted` >= 10 and `already_quoted` < 20"


def test_cancel_query_terminates_idle_server_session_with_kill_connection():
    engine = object.__new__(MySQLEngine)
    engine.hop = SimpleNamespace(
        source=SimpleNamespace(user="miguser"),
        target=SimpleNamespace(user="miguser"),
    )

    killed = []
    processlist_calls = []

    class MockCursor:
        def __init__(self):
            self.iteration = 0

        def execute(self, sql, args=None):
            if sql.startswith("KILL"):
                killed.append((sql, args))
            elif "select COMMAND, INFO, USER" in sql:
                processlist_calls.append(("initial", args))
            elif "select ID from information_schema.PROCESSLIST" in sql:
                self.iteration += 1
                processlist_calls.append(("check_gone", args))

        def fetchone(self):
            if processlist_calls and processlist_calls[-1][0] == "initial":
                return ("Sleep", None, "miguser")
            return None

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class MockConn:
        _read_timeout = 15

        def cursor(self):
            return MockCursor()

        def close(self):
            pass

    engine._conn = lambda side, retry=True: MockConn()

    engine._cancel_query("src", 42, "select * from shop.orders")

    assert ("KILL CONNECTION 42", None) in killed
    assert len(killed) == 1
    assert any(c[0] == "check_gone" for c in processlist_calls)


def test_cancel_query_uses_kill_query_for_active_statement():
    engine = object.__new__(MySQLEngine)
    engine.hop = SimpleNamespace(
        source=SimpleNamespace(user="miguser"),
        target=SimpleNamespace(user="miguser"),
    )

    stmt = "select /* checksum */ count(*) from shop.orders"
    killed = []
    checks = []

    class MockCursor:
        def __init__(self):
            self.checked = False

        def execute(self, sql, args=None):
            if sql.startswith("KILL"):
                killed.append(sql)
            elif "from information_schema.PROCESSLIST" in sql:
                checks.append(sql)

        def fetchone(self):
            if len(checks) == 1:
                return ("Query", stmt, "miguser")
            return ("Sleep", None)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class MockConn:
        _read_timeout = 15

        def cursor(self):
            return MockCursor()

        def close(self):
            pass

    engine._conn = lambda side, retry=True: MockConn()

    engine._cancel_query("src", 99, stmt)

    assert killed == ["KILL QUERY 99"]


def test_cancel_query_escalates_to_kill_connection_when_unresponsive(monkeypatch):
    import time
    engine = object.__new__(MySQLEngine)
    engine.hop = SimpleNamespace(
        source=SimpleNamespace(user="miguser"),
        target=SimpleNamespace(user="miguser"),
    )

    stmt = "select long_running from big_table"
    killed = []

    class MockCursor:
        def __init__(self):
            self.killed_conn = False

        def execute(self, sql, args=None):
            if "KILL CONNECTION" in sql:
                self.killed_conn = True
                killed.append(sql)
            elif sql.startswith("KILL"):
                killed.append(sql)

        def fetchone(self):
            if self.killed_conn:
                return None
            return ("Query", stmt, "miguser")

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class MockConn:
        _read_timeout = 15

        def cursor(self):
            return MockCursor()

        def close(self):
            pass

    engine._conn = lambda side, retry=True: MockConn()

    real_monotonic = time.monotonic
    times = [100.0, 100.0, 101.0, 116.0, 117.0, 122.0]
    time_idx = [0]

    def mock_monotonic():
        idx = min(time_idx[0], len(times) - 1)
        time_idx[0] += 1
        return times[idx]

    monkeypatch.setattr(time, "monotonic", mock_monotonic)
    monkeypatch.setattr(time, "sleep", lambda s: None)

    engine._cancel_query("src", 77, stmt)

    assert "KILL QUERY 77" in killed
    assert "KILL CONNECTION 77" in killed


def test_cancel_query_skips_unowned_session():
    engine = object.__new__(MySQLEngine)
    engine.hop = SimpleNamespace(
        source=SimpleNamespace(user="my_migrator_user"),
        target=SimpleNamespace(user="my_migrator_user"),
    )

    killed = []

    class MockCursor:
        def execute(self, sql, args=None):
            if sql.startswith("KILL"):
                killed.append(sql)

        def fetchone(self):
            return ("Sleep", None, "other_system_user")

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class MockConn:
        _read_timeout = 15

        def cursor(self):
            return MockCursor()

        def close(self):
            pass

    engine._conn = lambda side, retry=True: MockConn()

    engine._cancel_query("src", 88, "select 1")

    assert len(killed) == 0


def test_reconnection_reconciles_idle_connection_and_preserves_work(monkeypatch):
    import pymysql.err
    from migkit import util

    engine = object.__new__(MySQLEngine)
    engine.hop = SimpleNamespace(
        source=SimpleNamespace(user="root"),
        target=SimpleNamespace(user="root"),
    )

    attempts = [0]
    cancelled_threads = []

    class Connection:
        def __init__(self, thread_num):
            self._tid = thread_num

        def cursor(self):
            tid = self._tid

            class Cursor:
                def mogrify(self, sql, args=None):
                    return sql

                def execute(self, sql, args=None):
                    attempts[0] += 1
                    if attempts[0] == 1:
                        raise pymysql.err.OperationalError(2013, "Lost connection to MySQL server during query")
                    return [(1, "ok")]

                def fetchall(self):
                    return [(1, "ok")]

                def __enter__(self):
                    return self

                def __exit__(self, *a):
                    pass

            return Cursor()

        def thread_id(self):
            return self._tid

        def close(self):
            pass

    connections = [Connection(101), Connection(102)]

    def mock_conn(side, retry=True):
        return connections.pop(0)

    engine._conn = mock_conn
    engine._cancel_query = lambda side, tid, stmt: cancelled_threads.append(tid)
    monkeypatch.setattr(util, "_backoff", lambda attempt, base=0.8: 0.0)

    rows = engine._q("src", "select 1")
    assert rows == [(1, "ok")]
    assert attempts[0] == 2
    assert cancelled_threads == [101]


def test_lob_check_reports_error_result_on_failure():
    engine = object.__new__(MySQLEngine)
    engine._d = lambda side, db: db
    engine._scoped_rows = lambda db, r: r
    engine.LOB_TYPES = ["text"]
    engine._q = lambda side, sql, *args: (_ for _ in ()).throw(RuntimeError("disk full"))

    res = engine._lob_check("shop")
    assert isinstance(res, Result)
    assert res.status == "error"
    assert "could not size the large columns" in res.detail


def test_charset_counts_reports_error_result_on_failure():
    engine = object.__new__(MySQLEngine)
    engine._d = lambda side, db: db
    engine._scoped_rows = lambda db, r: r
    engine._pk_cols = lambda db, t: ["id"]
    engine.hop = SimpleNamespace(
        slice=1000,
        report_dir=lambda db: None,
    )

    def fail_counts(db, cols):
        raise OSError("connection reset by peer")

    engine._charset_counts = fail_counts
    engine._q = lambda side, sql, *args: [("t", "c")]

    res = []
    try:
        tcols = engine._scoped_rows("db", engine._q("src", "query"))
        counts = engine._charset_counts("db", tcols[:30])
    except Exception as e:
        res.append(Result("deep", "db charset", "error",
                          "replacement-character scan incomplete: " + type(e).__name__))

    assert len(res) == 1
    assert res[0].status == "error"
    assert "replacement-character scan incomplete: OSError" in res[0].detail

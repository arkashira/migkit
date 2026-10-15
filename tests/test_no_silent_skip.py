from types import SimpleNamespace
import pytest

from migkit.engines.mysql import MySQLEngine
from migkit.engines.base import Result


def _make_engine(tmp_path):
    engine = object.__new__(MySQLEngine)
    engine.hop = SimpleNamespace(
        name="testhop",
        options={},
        slice=1000000,
        report_dir=lambda db: tmp_path,
        target_table_in=lambda db, t: t,
        masks=lambda db, t, columns=(): (),
        excluded=lambda db, t: False,
    )
    engine._d = lambda side, db: db
    engine._my_ident = lambda name: f"`{name}`"
    engine._quote_ident = lambda name: f"`{name}`"
    engine._scope = lambda side, db, table: f"`{db}`.`{table}`"
    engine._scoped_rows = lambda db, rows: rows
    return engine


def test_lob_check_fails_on_column_query_error(tmp_path):
    engine = _make_engine(tmp_path)
    engine.LOB_TYPES = ("blob", "text")
    engine._pk_cols = lambda db, table: ["id"]
    engine._checksum_ranges = lambda db, table, pks: ([(None, None)], "id")

    def query(side, sql, args=None):
        if "information_schema.columns" in sql:
            return [("articles", "content")]
        if "@@max_allowed_packet" in sql:
            return [(67108864,)]
        if "coalesce(max(length(" in sql:
            raise RuntimeError("query timeout reading LOB")
        return []

    engine._q = query
    engine._read_pool = lambda db: SimpleNamespace(
        __enter__=lambda s: SimpleNamespace(
            submit=lambda fn, *a, **k: SimpleNamespace(result=lambda: fn(*a, **k))
        ),
        __exit__=lambda *a: None,
    )

    res = engine._lob_check("testdb")
    assert res.status in ("error", "incomplete"), f"Expected error/incomplete but got {res.status}"
    assert "articles" in res.detail
    assert "content" in res.detail or "articles" in res.detail


def test_charset_check_fails_on_column_query_error(tmp_path, monkeypatch):
    engine = _make_engine(tmp_path)
    engine._pk_cols = lambda db, table: ["id"]
    engine._checksum_ranges = lambda db, table, pks: ([(None, None)], "id")

    from migkit import sizing
    monkeypatch.setattr(sizing, "estimate", lambda *a, **k: sizing.Plan(1, 1, {}))
    monkeypatch.setattr(sizing, "resources", lambda *a, **k: {})

    def query(side, sql, args=None):
        if "character_set_name is not null" in sql:
            return [("users", "bio", "utf8mb4")]
        if "data_type in ('char'" in sql:
            return [("users", "bio")]
        if "0xEFBFBD" in sql:
            raise RuntimeError("deadlock or timeout on charset scan")
        return []

    engine._q = query

    res = engine._charset_check("testdb")
    assert res.status in ("error", "incomplete"), f"Expected error/incomplete but got {res.status}"
    assert "users" in res.detail or "bio" in res.detail


def test_nullempty_check_fails_on_column_error(tmp_path):
    engine = _make_engine(tmp_path)
    engine._text_columns = lambda db: [("accounts", "nickname")]

    def query(side, sql, args=None):
        if "nickname" in sql:
            raise RuntimeError("connection reset while scanning null/empty")
        return []

    engine._q = query

    results = engine._deep_nullempty("testdb", "testdb")
    assert len(results) >= 1
    res = results[0]
    assert res.status in ("error", "incomplete"), f"Expected error/incomplete but got {res.status}"
    assert "accounts.nickname" in res.detail or "nickname" in res.detail


def test_float_check_fails_on_column_error(tmp_path):
    engine = _make_engine(tmp_path)
    engine.FLOATY = ("float", "double")

    def query(side, sql, args=None):
        if "data_type in (%s, %s)" in sql:
            return [("metrics", "score")]
        if "coalesce(sum(`score`)" in sql:
            raise RuntimeError("disk full during float aggregation")
        return []

    engine._q = query

    results = engine._deep_float("testdb", "testdb")
    assert len(results) >= 1
    res = results[0]
    assert res.status in ("error", "incomplete"), f"Expected error/incomplete but got {res.status}"
    assert "metrics.score" in res.detail or "score" in res.detail


def test_partition_check_fails_on_partition_query_error(tmp_path):
    engine = _make_engine(tmp_path)
    engine.partition_scheme = lambda side, db, sch: {
        "orders": {"m": "RANGE", "e": "id", "p": {"p0": "100", "p1": "200"}}
    }
    engine._all_tables = lambda side, db: ["orders"]
    engine._cols = lambda db, t: ["id", "amount"]
    engine.digest_salt = lambda: "salt"
    engine._summed = lambda expr, salt: "sum(1)"

    def query(side, sql, args=None):
        if "partition (`p" in sql:
            raise RuntimeError("lost connection reading partition p0")
        return []

    engine._q = query

    res = engine._deep_partitions("testdb", "testdb")
    assert res.status in ("error", "incomplete"), f"Expected error/incomplete but got {res.status}"
    assert "orders" in res.detail or "partition" in res.detail


def test_partition_digest_error_cannot_silently_pass(tmp_path):
    engine = _make_engine(tmp_path)
    engine.partition_scheme = lambda side, db, sch: {
        "orders": {"m": "RANGE", "e": "id", "p": {"p0": "100"}}
    }
    engine._all_tables = lambda side, db: ["orders"]
    # _cols raises an error so digest cannot be computed
    def failing_cols(db, t):
        raise RuntimeError("cannot read column metadata for digest")
    engine._cols = failing_cols

    # But counting partitions succeeds and counts match (5 == 5)
    def query(side, sql, args=None):
        if "partition (`p0`)" in sql:
            return [(0, 5, None)]
        return []
    engine._q = query

    res = engine._deep_partitions("testdb", "testdb")
    assert res.status in ("error", "incomplete"), f"Expected error/incomplete but got {res.status}"
    assert "orders" in res.detail or "digest" in res.detail


def test_check_deep_surfaces_failures(tmp_path):
    engine = _make_engine(tmp_path)
    engine._planner_stats = lambda db: Result("deep", f"{db} stats", "ok", "stats match")
    engine._lob_check = lambda db: Result("deep", f"{db} lobs", "ok", "lobs match")
    engine._invalid_indexes = lambda db: None
    engine._collation_versions = lambda db: None
    engine._mojibake = lambda db: None
    engine._duplicate_keys = lambda db: None
    engine._temporal_meaning = lambda db: None
    engine._time_zone_rules = lambda db: None
    engine._capacity_gaps = lambda db: None
    engine._errant_transactions = lambda db: None
    engine._fk_orphans = lambda db: Result("deep", f"{db} fk", "ok", "fk ok")
    engine._deep_partitions = lambda db, ddb: Result("deep", f"{db} partitions", "ok", "partitions ok")
    engine._deep_unenforced_checks = lambda db, ddb: []
    engine._check_grants = lambda db: []
    engine._deep_ownership = lambda db, ddb: []
    engine._deep_triggers = lambda db, ddb: []

    # Inject failure into _deep_nullempty
    engine._text_columns = lambda db: [("accounts", "nickname")]
    def failing_query(side, sql, args=None):
        if "nickname" in sql:
            raise RuntimeError("query timeout on nickname")
        return []
    engine._q = failing_query

    # Also mock _charset_check to return error
    engine._charset_check = lambda db, ddb: Result("deep", f"{db} charset", "error", "could not read users.bio")

    results = engine.check_deep("testdb")
    ne = next((r for r in results if "nullempty" in r.scope), None)
    assert ne is not None
    assert ne.status in ("error", "incomplete")
    assert "nickname" in ne.detail

    cs = next((r for r in results if "charset" in r.scope), None)
    assert cs is not None
    assert cs.status in ("error", "incomplete")
    assert "users.bio" in cs.detail



"""Unit tests for the 0.3.0 verification/mover innovations - parsers and
selection logic only, no database needed."""
import migkit.movers as movers
from migkit.config import Endpoint, Hop
from migkit.engines.postgres import PostgresEngine


def _hop(engine="postgres", options=None):
    return Hop(name="t", engine=engine,
               source=Endpoint(host="s", port=1, user="u", password="p"),
               target=Endpoint(host="t", port=2, user="u", password="p"),
               options=options or {})


def test_decoding_parser_extracts_pks():
    lines = [
        ("0/1A", "BEGIN 123"),
        ("0/1B", "table public.orders: UPDATE: id[bigint]:5"
                 " note[text]:'a b''c' amount[numeric]:1.50"),
        ("0/1C", "table public.orders: DELETE: id[bigint]:9"),
        ("0/1D", "table public.pair: INSERT: a[integer]:1 b[integer]:2"
                 " v[text]:'x'"),
        ("0/1E", "table public.nopk: INSERT: v[text]:'y'"),
        ("0/1F", "COMMIT 123"),
    ]
    pks = {"public.orders": ["id"], "public.pair": ["a", "b"],
           "public.nopk": None}
    touched, nopk, last = PostgresEngine._parse_decoding(lines, pks.get)
    assert touched["public.orders"] == {"5", "9"}
    assert touched["public.pair"] == {"1\t2"}
    assert nopk == {"public.nopk"}
    assert last == "0/1F"


def test_row_hash_expr_option():
    assert PostgresEngine(_hop())._row_hash_expr() == "md5(t::text)"
    eng = PostgresEngine(_hop(options={"checksum": "jsonb"}))
    assert eng._row_hash_expr() == "md5(to_jsonb(t)::text)"


def test_movers_pick_prefers_installed(monkeypatch):
    have = {"pg_dump", "pg_restore", "mydumper", "myloader",
            "pgloader", "mongodump", "mongorestore"}
    monkeypatch.setattr(movers, "which",
                        lambda n: f"/bin/{n}" if n in have else None)
    assert movers.pick("postgres") == "pgdump"
    assert movers.pick("mysql") == "mydumper"
    assert movers.pick("hetero") == "pgloader"
    assert movers.pick("mongodb") == "mongodump"
    assert movers.pick("postgres", table="x.y") == "builtin"
    monkeypatch.setattr(movers, "which", lambda n: None)
    assert movers.pick("postgres") == "builtin"


def test_movers_supported_matrix():
    assert movers.supported("postgres", "pgdump")
    assert not movers.supported("mysql", "pgdump")
    assert movers.supported("hetero", "debezium")
    assert not movers.supported("mongodb", "debezium")
    assert movers.supported("postgres", "builtin")


def test_debezium_codegen_writes_configs(tmp_path):
    import json
    import migkit.config as cfg
    cfg.REPORTS = tmp_path / "reports"
    hop = _hop("mysql")
    out = movers.debezium_codegen(hop, ["shop"], "mysql")
    files = {f.name for f in out.iterdir()}
    assert {"docker-compose.yml", "source-connector.json",
            "sink-connector.json", "README-debezium.md"} <= files
    src = json.loads((out / "source-connector.json").read_text())
    assert "MySqlConnector" in src["config"]["connector.class"]
    assert src["config"]["database.hostname"] == "s"
    sink = json.loads((out / "sink-connector.json").read_text())
    assert sink["config"]["insert.mode"] == "upsert"
    assert (out.stat().st_mode & 0o777) == 0o700


def test_pt_sync_sql_guardrails(monkeypatch):
    from migkit.engines import mysql as m
    eng = m.MySQLEngine(_hop("mysql"))
    monkeypatch.setattr(m, "which", lambda n: "/bin/pt-table-sync")
    # composite pk and oversized batches must fall back to builtin
    assert eng._pt_sync_sql("d", "t", ["a", "b"], [["1"]]) is None
    assert eng._pt_sync_sql("d", "t", ["a"], []) is None
    big = [[str(i)] for i in range(1001)]
    assert eng._pt_sync_sql("d", "t", ["a"], big) is None


def test_engines_override_deep():
    from migkit.engines.base import Engine
    from migkit.engines.kafka import KafkaEngine
    from migkit.engines.mssql import MSSQLEngine
    from migkit.engines.redis import RedisEngine
    for cls in (KafkaEngine, MSSQLEngine, RedisEngine):
        assert cls.check_deep is not Engine.check_deep, cls


def test_delta_available_on_three_engines():
    from migkit.engines.mongodb import MongoEngine
    from migkit.engines.mysql import MySQLEngine
    assert hasattr(PostgresEngine, "delta_verify")
    assert hasattr(MySQLEngine, "delta_verify")
    assert hasattr(MongoEngine, "delta_verify")
    assert hasattr(PostgresEngine, "delta_teardown")


def test_mssql_counts_ride_along():
    from migkit.engines.mssql import MSSQLEngine
    assert MSSQLEngine.counts_from_data


def test_standby_source_is_guarded(monkeypatch):
    """A read-replica source must not crash consistent/delta/fence; it must
    degrade cleanly. This is the 'app pointed at the reader endpoint' class
    of outage, caught by the tool instead of at runtime."""
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop())
    monkeypatch.setattr(eng, "_in_recovery", lambda side, db="postgres": True)
    # no source LSN -> no fence (returns None, never raises)
    assert eng.src_lsn("db") is None
    assert eng.fence_wait("db", None) is None
    # delta verify returns a clean error Result, not an exception
    r = eng.delta_verify("db")
    assert r[0].status == "error"
    assert "read replica" in r[0].detail

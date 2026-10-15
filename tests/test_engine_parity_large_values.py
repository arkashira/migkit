import hashlib
import io
import pytest

from migkit import decide, pieces
from migkit.engines.mysql import MySQLEngine
from migkit.engines.oracle import OracleEngine


class DummyHop:
    def __init__(self, tmp_path):
        self.report_dir_path = tmp_path
        self.engine = "postgres"

    def report_dir(self, db=None):
        p = self.report_dir_path / (db or "global")
        p.mkdir(parents=True, exist_ok=True)
        return p


class DummyEngine:
    def __init__(self, canon="postgres"):
        self.CANON_ENGINE = canon
        self.ENGINE_FAMILY = canon
        self.queries = []

    def _q(self, name):
        return f'"{name}"'

    def _psql(self, side, db, sql):
        self.queries.append((side, db, sql))
        if "pg_constraint" in sql:
            return (
                "public.child\x1fchild_fk\x1fforeign key (parent_id) references public.parent(id)\x1fpublic.parent\n"
                "public.orders\x1forders_cust_fk\x1fforeign key (cust_id) references public.customers(id)\x1fpublic.customers"
            )
        return ""


def test_mysql_large_values_adapter():
    way = MySQLEngine.LARGE_VALUES
    assert isinstance(way, pieces.Sql)
    assert way.holds("longtext")
    assert way.holds("mediumblob")
    assert way.holds("json")
    assert not way.holds("int")

    assert way.length_of("`doc`", "longtext") == "length(`doc`)"
    assert way.piece.format(c="`doc`", at=1, n=4096) == "substring(`doc`, 1, 4096)"
    assert way.append.format(c="`doc`", v="%s") == "`doc` = concat(`doc`, %s)"
    assert way.appendable("longtext")
    assert way.appendable("longblob")
    assert way.md5.format(c="`doc`") == "md5(`doc`)"


def test_oracle_large_values_adapter():
    way = OracleEngine.LARGE_VALUES
    assert isinstance(way, pieces.Sql)
    assert way.holds("clob")
    assert way.holds("blob")
    assert way.piece == "dbms_lob.substr({c}, {n}, {at})"
    assert way.append == "{c} = {c} || {v}"
    assert way.appendable("clob")
    assert way.appendable("varchar2")


<<<<<<< HEAD
def test_decide_coverage_complete():
    cov = decide.coverage()
    not_yet_lv = [
        eng
        for eng, s in cov.items()
        if s.get("large values") == decide.NOT_YET
        or (isinstance(s.get("large values"), tuple) and s.get("large values")[0] == decide.NOT_YET)
    ]
    not_yet_part = [
        eng
        for eng, s in cov.items()
        if s.get("partitioned") == decide.NOT_YET
        or (isinstance(s.get("partitioned"), tuple) and s.get("partitioned")[0] == decide.NOT_YET)
    ]
    assert not_yet_lv == [], f"Unresolved large values: {not_yet_lv}"
    assert not_yet_part == [], f"Unresolved partitioned: {not_yet_part}"
    assert cov["mysql"]["large values"] == "yes"
    assert cov["oracle"]["large values"] == "yes"
||||||| 7901f35
def test_decide_coverage_complete():
    cov = decide.coverage()
    not_yet_lv = [
        eng
        for eng, s in cov.items()
        if s.get("large values") == decide.NOT_YET
        or (isinstance(s.get("large values"), tuple) and s.get("large values")[0] == decide.NOT_YET)
    ]
    not_yet_part = [
        eng
        for eng, s in cov.items()
        if s.get("partitioned") == decide.NOT_YET
        or (isinstance(s.get("partitioned"), tuple) and s.get("partitioned")[0] == decide.NOT_YET)
    ]
    assert not_yet_lv == [], f"Unresolved large values: {not_yet_lv}"
    assert not_yet_part == [], f"Unresolved partitioned: {not_yet_part}"
    assert cov["mysql"]["large values"] == "yes"
    assert cov["oracle"]["large values"] == "yes"


def test_partition_discovery_and_chunking():
    fake_engine = DummyEngine("mysql")
    fake_engine.partition_scheme = lambda side, db, sch=None: {
        "sales": {
            "m": "RANGE",
            "e": "order_date",
            "p": {"p2024_01": "2024-02-01", "p2024_02": "2024-03-01", "p2024_03": "2024-04-01"},
        }
    }
    discovered = discover_partitions(fake_engine, "src", "appdb")
    assert "sales" in discovered
    assert len(discovered["sales"]) == 3
    assert discovered["sales"][0].name == "p2024_01"

    parts = [
        PartitionInfo(table="orders", name="p0", rows=10_000),
        PartitionInfo(table="orders", name="p1", rows=20_000),
        PartitionInfo(table="orders", name="p2", rows=80_000),
        PartitionInfo(table="orders", name="p3", rows=15_000),
    ]
    chunks = chunk_partitions(parts, max_rows_per_chunk=50_000)
    assert len(chunks) == 3
    assert chunks[0].partitions == ("p0", "p1")
    assert chunks[0].estimated_rows == 30_000
    assert chunks[1].partitions == ("p2",)
    assert chunks[1].estimated_rows == 80_000
    assert chunks[2].partitions == ("p3",)
    assert chunks[2].estimated_rows == 15_000


def test_partition_router():
    router = PartitionRouter(workers=3, routing_strategy="balanced")
    chunks = [
        PartitionChunk("c1", "t", ("p1",), estimated_rows=50_000),
        PartitionChunk("c2", "t", ("p2",), estimated_rows=30_000),
        PartitionChunk("c3", "t", ("p3",), estimated_rows=20_000),
        PartitionChunk("c4", "t", ("p4",), estimated_rows=15_000),
        PartitionChunk("c5", "t", ("p5",), estimated_rows=10_000),
    ]
    routed = router.route(chunks)
    worker_loads = {w: sum(c.estimated_rows for c in router.get_worker_chunks(w, routed)) for w in range(3)}
    assert max(worker_loads.values()) - min(worker_loads.values()) <= 20_000

    mysql_eng = DummyEngine("mysql")
    pg_eng = DummyEngine("postgres")
    assert router.partition_clause(mysql_eng, "sales", "p2024") == 'sales PARTITION ("p2024")'
    assert router.partition_clause(pg_eng, "sales", "sales_2024") == '"sales_2024"'


def test_hold_or_sequence_dependent_tables():
    tables = ["order_items", "orders", "customers", "audit_log", "products"]
    deps = [
        ("order_items", "orders"),
        ("order_items", "products"),
        ("orders", "customers"),
    ]
    waves, cycles = hold_or_sequence_dependent_tables(tables, deps)
    assert cycles == []
    assert len(waves) == 3
    assert set(waves[0]) == {"audit_log", "customers", "products"}
    assert set(waves[1]) == {"orders"}
    assert set(waves[2]) == {"order_items"}


def test_hold_or_sequence_dependent_tables_cycle():
    tables = ["t1", "t2", "standalone"]
    deps = [("t1", "t2"), ("t2", "t1")]
    waves, cycles = hold_or_sequence_dependent_tables(tables, deps)
    assert cycles == [["t1", "t2"]]
    assert waves[0] == ["standalone"]


def test_foreign_key_window(tmp_path):
    hop = DummyHop(tmp_path)
    engine = DummyEngine("postgres")
    hop.target_engine = engine

    with ForeignKeyWindow(hop, "testdb") as fkw:
        assert len(fkw.dropped) == 2
        drop_sqls = [q[2] for q in engine.queries if "drop constraint" in q[2]]
        assert any('drop constraint if exists "child_fk"' in s for s in drop_sqls)
        assert any('drop constraint if exists "orders_cust_fk"' in s for s in drop_sqls)

    restore_sqls = [q[2] for q in engine.queries if "add constraint" in q[2]]
    assert any('add constraint "child_fk"' in s for s in restore_sqls)
    assert any('add constraint "orders_cust_fk"' in s for s in restore_sqls)


def test_foreign_key_window_exported_in_movers():
    assert _ForeignKeyWindow is ForeignKeyWindow
=======
def test_a_cell_not_measured_stays_not_yet_and_waits_on_its_row():
    """A large value or a partition no test has read on a server of the
    kind is not closed by calling it not applicable: it is not yet, and
    names the backlog row it waits on."""
    from migkit import capabilities
    rows = capabilities.backlog_rows()
    for shape in ("large values", "partitioned"):
        for engine in decide.coverage():
            state, why = decide.cell(engine, shape)
            if state == decide.NOT_YET:
                assert why in rows, (engine, shape, why)
    for engine in ("tidb", "gaussdb", "cockroachdb", "dsql", "keyspaces",
                   "oceanbase-oracle", "spanner"):
        assert decide.cell(engine, "large values") == (
            decide.NOT_YET, "P0-PARITY.lv.new"), engine
    for engine in ("hetero", "gaussdb", "ase", "iceberg", "delta"):
        assert decide.cell(engine, "partitioned")[0] == decide.NOT_YET, \
            engine
    assert decide.cell("mysql", "large values")[0] == "yes"
    assert decide.cell("oracle", "large values")[0] == "yes"
    assert decide.cell("redis", "partitioned")[0] == decide.NOT_APPLICABLE


def test_a_partition_read_no_server_proved_is_owed():
    from migkit import capabilities
    for engine in ("db2", "db2i", "db2zos", "parquet", "oceanbase",
                   "polardbx", "dcdb"):
        assert decide.cell(engine, "partitioned")[0] == "yes", engine
        assert capabilities.depth_cell(engine, "move.partitioned")[0] == \
            capabilities.NOT_YET, engine


def test_partition_discovery_and_chunking():
    fake_engine = DummyEngine("mysql")
    fake_engine.partition_scheme = lambda side, db, sch=None: {
        "sales": {
            "m": "RANGE",
            "e": "order_date",
            "p": {"p2024_01": "2024-02-01", "p2024_02": "2024-03-01", "p2024_03": "2024-04-01"},
        }
    }
    discovered = discover_partitions(fake_engine, "src", "appdb")
    assert "sales" in discovered
    assert len(discovered["sales"]) == 3
    assert discovered["sales"][0].name == "p2024_01"

    parts = [
        PartitionInfo(table="orders", name="p0", rows=10_000),
        PartitionInfo(table="orders", name="p1", rows=20_000),
        PartitionInfo(table="orders", name="p2", rows=80_000),
        PartitionInfo(table="orders", name="p3", rows=15_000),
    ]
    chunks = chunk_partitions(parts, max_rows_per_chunk=50_000)
    assert len(chunks) == 3
    assert chunks[0].partitions == ("p0", "p1")
    assert chunks[0].estimated_rows == 30_000
    assert chunks[1].partitions == ("p2",)
    assert chunks[1].estimated_rows == 80_000
    assert chunks[2].partitions == ("p3",)
    assert chunks[2].estimated_rows == 15_000


def test_partition_router():
    router = PartitionRouter(workers=3, routing_strategy="balanced")
    chunks = [
        PartitionChunk("c1", "t", ("p1",), estimated_rows=50_000),
        PartitionChunk("c2", "t", ("p2",), estimated_rows=30_000),
        PartitionChunk("c3", "t", ("p3",), estimated_rows=20_000),
        PartitionChunk("c4", "t", ("p4",), estimated_rows=15_000),
        PartitionChunk("c5", "t", ("p5",), estimated_rows=10_000),
    ]
    routed = router.route(chunks)
    worker_loads = {w: sum(c.estimated_rows for c in router.get_worker_chunks(w, routed)) for w in range(3)}
    assert max(worker_loads.values()) - min(worker_loads.values()) <= 20_000

    mysql_eng = DummyEngine("mysql")
    pg_eng = DummyEngine("postgres")
    assert router.partition_clause(mysql_eng, "sales", "p2024") == 'sales PARTITION ("p2024")'
    assert router.partition_clause(pg_eng, "sales", "sales_2024") == '"sales_2024"'


def test_hold_or_sequence_dependent_tables():
    tables = ["order_items", "orders", "customers", "audit_log", "products"]
    deps = [
        ("order_items", "orders"),
        ("order_items", "products"),
        ("orders", "customers"),
    ]
    waves, cycles = hold_or_sequence_dependent_tables(tables, deps)
    assert cycles == []
    assert len(waves) == 3
    assert set(waves[0]) == {"audit_log", "customers", "products"}
    assert set(waves[1]) == {"orders"}
    assert set(waves[2]) == {"order_items"}


def test_hold_or_sequence_dependent_tables_cycle():
    tables = ["t1", "t2", "standalone"]
    deps = [("t1", "t2"), ("t2", "t1")]
    waves, cycles = hold_or_sequence_dependent_tables(tables, deps)
    assert cycles == [["t1", "t2"]]
    assert waves[0] == ["standalone"]


def test_foreign_key_window(tmp_path):
    hop = DummyHop(tmp_path)
    engine = DummyEngine("postgres")
    hop.target_engine = engine

    with ForeignKeyWindow(hop, "testdb") as fkw:
        assert len(fkw.dropped) == 2
        drop_sqls = [q[2] for q in engine.queries if "drop constraint" in q[2]]
        assert any('drop constraint if exists "child_fk"' in s for s in drop_sqls)
        assert any('drop constraint if exists "orders_cust_fk"' in s for s in drop_sqls)

    restore_sqls = [q[2] for q in engine.queries if "add constraint" in q[2]]
    assert any('add constraint "child_fk"' in s for s in restore_sqls)
    assert any('add constraint "orders_cust_fk"' in s for s in restore_sqls)


def test_foreign_key_window_exported_in_movers():
    assert _ForeignKeyWindow is ForeignKeyWindow
>>>>>>> worktree-agent-a4ca2bacb9aa1b491

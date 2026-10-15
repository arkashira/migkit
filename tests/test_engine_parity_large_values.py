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
    for engine in ("gaussdb", "cockroachdb", "dsql", "keyspaces",
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

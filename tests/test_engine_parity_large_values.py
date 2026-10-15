import hashlib
import io
import pytest

from migkit import decide, pieces
from migkit.engines.mysql import MySQLEngine
from migkit.engines.oracle import OracleEngine
<<<<<<< HEAD
from migkit.lob_streamer import LobChunk, LobStreamSummary, LobStreamer, iter_lob_chunks
||||||| d45f7f2
from migkit.fk_window import ForeignKeyWindow, hold_or_sequence_dependent_tables
from migkit.lob_streamer import LobChunk, LobStreamSummary, LobStreamer, iter_lob_chunks
from migkit.movers import _ForeignKeyWindow
from migkit.partitions import (
    PartitionChunk,
    PartitionInfo,
    PartitionRouter,
    chunk_partitions,
    discover_partitions,
)
=======
from migkit.fk_window import ForeignKeyWindow, hold_or_sequence_dependent_tables
from migkit.movers import _ForeignKeyWindow
from migkit.partitions import (
    PartitionChunk,
    PartitionInfo,
    PartitionRouter,
    chunk_partitions,
    discover_partitions,
)
>>>>>>> worktree-agent-a0cf3bd8e028add14


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

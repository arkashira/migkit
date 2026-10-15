import hashlib
import io
import pytest

from migkit import decide, pieces
from migkit.engines.mysql import MySQLEngine
from migkit.engines.oracle import OracleEngine
from migkit.lob_streamer import LobChunk, LobStreamSummary, LobStreamer, iter_lob_chunks


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


def test_lob_chunks_generator_bytes():
    payload = b"abcdefghijklmnopqrstuvwxyz012345"
    chunks = list(iter_lob_chunks(payload, chunk_size=10))
    assert len(chunks) == 4
    assert chunks[0].data == b"abcdefghij"
    assert chunks[0].offset == 1
    assert chunks[0].length == 10
    assert not chunks[0].is_last
    assert chunks[1].data == b"klmnopqrst"
    assert chunks[2].data == b"uvwxyz0123"
    assert chunks[3].data == b"45"
    assert chunks[3].offset == 31
    assert chunks[3].length == 2
    assert chunks[3].is_last
    assert b"".join(c.data for c in chunks) == payload


def test_lob_chunks_generator_empty():
    chunks = list(iter_lob_chunks(b"", chunk_size=100))
    assert len(chunks) == 1
    assert chunks[0].length == 0
    assert chunks[0].data == b""
    assert chunks[0].is_last


def test_lob_chunks_generator_stream():
    stream = io.BytesIO(b"0123456789" * 10)
    chunks = list(iter_lob_chunks(stream, chunk_size=25))
    assert len(chunks) == 4
    assert sum(c.length for c in chunks) == 100
    assert all(c.checksum == hashlib.md5(c.data, usedforsecurity=False).hexdigest() for c in chunks)


def test_lob_chunks_generator_callable():
    store = {1: b"CHUNK1", 7: b"CHUNK2", 13: b"END"}

    def reader(at, n):
        return store.get(at, b"")

    chunks = list(iter_lob_chunks(reader, chunk_size=6, total_size=15))
    assert len(chunks) == 3
    assert b"".join(c.data for c in chunks) == b"CHUNK1CHUNK2END"


def test_lob_streamer_stream_between():
    streamer = LobStreamer(chunk_size=16)
    source_data = b"migkit-cross-engine-large-value-streaming-test-bytes" * 5
    read_stream = io.BytesIO(source_data)
    written = []

    def writer(chunk: LobChunk):
        written.append(chunk.data)

    summary = streamer.stream_between(read_stream, writer)
    assert summary.total_bytes == len(source_data)
    assert summary.digest == hashlib.md5(source_data, usedforsecurity=False).hexdigest()
    assert b"".join(written) == source_data

    target_summary = LobStreamSummary(
        total_bytes=len(b"".join(written)),
        chunk_count=len(written),
        digest=hashlib.md5(b"".join(written), usedforsecurity=False).hexdigest(),
    )
    assert streamer.verify_parity(summary, target_summary)


def test_lob_streamer_sql_dialects():
    streamer = LobStreamer()
    mysql_eng = DummyEngine(canon="mysql")
    oracle_eng = DummyEngine(canon="oracle")
    mssql_eng = DummyEngine(canon="mssql")
    pg_eng = DummyEngine(canon="postgres")

    assert streamer.read_piece_sql(mysql_eng, "content", 1, 1024) == 'substring("content", 1, 1024)'
    assert streamer.read_piece_sql(oracle_eng, "content", 1, 1024) == 'dbms_lob.substr("content", 1024, 1)'
    assert streamer.read_piece_sql(mssql_eng, "content", 1, 1024) == 'substring("content", 1, 1024)'
    assert streamer.read_piece_sql(pg_eng, "content", 1, 1024) == 'substr("content", 1, 1024)'

    assert streamer.append_piece_sql(mysql_eng, "content", "?") == '"content" = concat("content", ?)'
    assert streamer.append_piece_sql(mssql_eng, "content", "?") == '"content".write(?, null, null)'
    assert streamer.append_piece_sql(oracle_eng, "content", ":1") == '"content" = "content" || :1'


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

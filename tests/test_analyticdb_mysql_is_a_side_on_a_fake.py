"""AnalyticDB for MySQL as a side, on a fake answering its catalogue (backlog
P0-CLOUDS.more): spread, external-table load and per-table binary log as rungs on its facts. No server is reached."""
import types

import pytest

from migkit import decide, managed, users
from migkit.engines import analyticdb


@pytest.fixture
def eng(monkeypatch):
    got = analyticdb.AnalyticDBMySQLEngine.__new__(analyticdb.AnalyticDBMySQLEngine)
    got.hop = types.SimpleNamespace(
        engine="analyticdb-mysql", source=types.SimpleNamespace(options={}),
        target=types.SimpleNamespace(options={}))
    got.said = []
    monkeypatch.setattr(decide, "record",
                        lambda hop, choice, log=None, **kw: got.said.append(
                            choice))
    monkeypatch.setattr(type(got), "_d", lambda self, side, db: db,
                        raising=False)
    return got


def test_the_catalogue_row_is_kept_for_the_engine():
    assert "alibaba-analyticdb-mysql" in managed.services()


def test_its_accounts_are_read_as_its_wires():
    hop = types.SimpleNamespace(engine="analyticdb-mysql")
    assert users._speaking(hop).engine == "mysql"


def test_a_small_table_is_kept_whole_and_a_large_one_hashed(eng):
    eng._q = lambda side, sql, args=None: [(10,)]
    assert eng.spread_of("app", "t", ["id"]) == "distributed by broadcast"
    eng._q = lambda side, sql, args=None: [(10 ** 7,)]
    assert eng.spread_of("app", "t", ["id"]) == "distributed by hash(`id`)"
    assert eng.spread_of("app", "t", []) == ""


def test_it_is_followed_only_where_every_table_keeps_a_binary_log(eng):
    eng.neutral_tables = lambda side, db: ["a", "b"]
    kept = {"a"}
    eng._q = lambda side, sql, args=None: [(
        "t", "create table x (id int) binlog=true" if any(
            f"`{t}`" in sql for t in kept) else "create table x (id int)")]
    got = eng.follow_way("app")
    assert got.path is None and "only 1 of the 2 tables" in got.reason
    with pytest.raises(SystemExit, match="no way to follow"):
        eng.tail_start("app", None)
    kept.add("b")
    assert eng.follow_way("app").path == "binlog"


def test_the_external_table_load_is_taken_on_facts_and_said_otherwise(eng):
    eng._table_rows = lambda side, db, t: 5 if side == "src" else 0
    eng._may_load = lambda db: True
    assert eng.load_way("app", "t").path == "copier"
    assert "no landing store" in eng.said[-1].reason
    eng.hop.target.options = {"stage": "oss://bucket/in"}
    eng._landing_writable = lambda url: True
    assert eng.load_way("app", "t").path == "landing"
    eng._table_rows = lambda side, db, t: 5
    assert eng.load_way("app", "t").path == "copier"
    assert "twice" in eng.said[-1].reason
    sql = eng.landing_sql("app", "t", ["id", "v"], "oss://bucket/in/t.csv")
    assert "engine='OSS'" in sql[0] and sql[1].startswith("insert into")
    assert sql[2].startswith("drop table")


def test_a_landed_value_keeps_null_apart_from_its_text():
    from migkit.engines.analyticdb import landing_text
    assert landing_text(None) == "\\N"
    assert landing_text("\\N") == "\\\\N"
    assert landing_text(b"\x01") == "01"


class FakeStore:
    """An object store's client: parts kept as sent, calls in order."""

    def __init__(self, fail_on=None):
        self.calls, self.parts, self.put, self.fail_on = [], [], None, fail_on

    def put_object(self, Bucket, Key, Body):
        self.calls.append("put")
        self.put = (Key, Body)

    def create_multipart_upload(self, Bucket, Key):
        self.calls.append("create")
        return {"UploadId": "u1"}

    def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):
        self.calls.append("part")
        if self.fail_on == PartNumber:
            raise OSError("store refused")
        self.parts.append(Body)
        return {"ETag": f"e{PartNumber}"}

    def complete_multipart_upload(self, Bucket, Key, UploadId,
                                  MultipartUpload):
        self.calls.append("complete")
        assert [p["PartNumber"] for p in MultipartUpload["Parts"]] == list(
            range(1, len(self.parts) + 1))

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self.calls.append("abort")


def _wide_rows(n, held):
    """`n` rows of about 100 bytes, `held[0]` the most rows pulled and
    not yet sent."""
    for i in range(n):
        held[0] = i
        yield (i, "x" * 90, None)


def test_a_csv_larger_than_the_piece_goes_up_in_pieces_not_whole():
    """13 MB of rows against the 8 MiB piece: a multipart upload whose
    parts hold a piece and a row, not the file, and whose bytes are the
    file a one-shot write would have made."""
    from migkit import pieces
    store, held = FakeStore(), [0]
    got = analyticdb.land_csv("s3://bucket/in", "t", _wide_rows(130_000, held),
                              {}, client=store)
    assert got.startswith("s3://bucket/in/t-") and got.endswith(".csv")
    assert store.calls[0] == "create" and store.calls[-1] == "complete"
    assert len(store.parts) >= 2 and "put" not in store.calls
    for body in store.parts[:-1]:
        assert pieces.PIECE <= len(body) < pieces.PIECE + 1024
    whole = b"".join(store.parts)
    assert whole.startswith(b"0,") and whole.count(b"\n") == 130_000
    assert whole.endswith(b"129999," + b"x" * 90 + b",\\N\n")
    assert max(map(len, store.parts)) < len(whole) // 1.4


def test_a_csv_within_one_piece_is_one_request_and_an_empty_one_too():
    store = FakeStore()
    analyticdb.land_csv("oss://bucket", "t", iter([(1, "a"), (2, None)]), {},
                        client=store)
    assert store.calls == ["put"] and store.put[1] == b"1,a\n2,\\N\n"
    key = store.put[0]
    assert key.startswith("t-") and "/" not in key
    empty = FakeStore()
    analyticdb.land_csv("s3://bucket/p", "t", iter(()), {}, client=empty)
    assert empty.calls == ["put"] and empty.put[1] == b""


def test_a_failed_part_abandons_the_upload_and_says_why():
    store = FakeStore(fail_on=2)
    with pytest.raises(OSError, match="store refused"):
        analyticdb.land_csv("s3://bucket/in", "t",
                            _wide_rows(130_000, [0]), {}, client=store)
    assert store.calls[-1] == "abort" and "complete" not in store.calls


def test_a_rows_failure_midway_abandons_the_upload_too():
    def broken():
        yield from _wide_rows(100_000, [0])
        raise RuntimeError("source read failed")
    store = FakeStore()
    with pytest.raises(RuntimeError, match="source read failed"):
        analyticdb.land_csv("s3://bucket/in", "t", broken(), {}, client=store)
    assert store.calls[-1] == "abort"


def test_the_load_reads_the_table_a_page_at_a_time_into_the_landing(
        eng, monkeypatch):
    """`move_table` hands the landing a generator over `neutral_read`'s
    pages: the whole table is never one list, every row is landed once,
    and the log says how many."""
    pages = []

    def read(side, db, table, columns, after=None, limit=None, where=None):
        assert limit is None            # the decision sizes the page
        lo = after[0] if after else 0
        rows = [(i, f"v{i}") for i in range(lo + 1, min(lo + 3, 10) + 1)]
        pages.append(len(rows))
        return rows, ((rows[-1][0],) if rows and rows[-1][0] < 10 else None)
    eng.neutral_read = read
    eng.neutral_columns = lambda side, db, t: [("id", "number"),
                                               ("v", "text")]
    eng.load_way = lambda db, t, log=None: types.SimpleNamespace(
        path="landing")
    eng._landing = lambda: "s3://bucket/in"
    ran, said = [], []
    eng._run_target = lambda db, statement: ran.append(statement)
    store = FakeStore()
    real = analyticdb.land_csv
    monkeypatch.setattr(analyticdb, "land_csv", lambda url, name, rows,
                        options, client=None, piece=None: real(
                            url, name, rows, options, client=store))
    eng.move_table("app", "", "t", 1, None, said.append)
    assert pages == [3, 3, 3, 1]
    assert store.put[1].decode().splitlines() == [
        f"{i},v{i}" for i in range(1, 11)]
    assert len(ran) == 3 and "10 rows loaded" in said[-1]

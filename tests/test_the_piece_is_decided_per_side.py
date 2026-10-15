"""The piece a large value is read and written by is one decision per side
(backlog P0-PARITY.lv.new): the sizes the side's own limits take - read off
TiDB and CockroachDB, DSQL's documented quota - then the fastest measured.

On fakes: no server is reached. What each server does at its limit is
still owed a server of each kind.
"""
import pytest

from migkit import pieces
from migkit.engines.cockroachdb import CockroachEngine
from migkit.engines.dbapi import MARK
from migkit.engines.dsql import DsqlEngine
from migkit.engines.tidb import TiDBEngine

PROVES = [("move.large-values", "tidb")]

MB = 2 ** 20


def test_sizes_are_read_as_servers_write_them():
    assert pieces.setting_bytes("64 MiB") == 64 * MB
    assert pieces.setting_bytes("16MB") == 16 * MB
    assert pieces.setting_bytes("67108864") == 64 * MB
    assert pieces.setting_bytes("0 B") == 0
    assert pieces.setting_bytes("off") is None
    assert pieces.setting_bytes(None) is None


def test_a_side_without_limits_keeps_the_one_size():
    sz = pieces.Sizing(None, "dst", "d", limits=())
    assert sz.inline == pieces.PIECE
    assert [r.name for r in sz.rungs] == [str(pieces.PIECE)]
    assert sz.pick() == pieces.PIECE
    assert sz.row is None and sz.too_wide(10 ** 12) is None


def test_a_small_packet_narrows_the_piece_and_the_inline_value():
    sz = pieces.Sizing(None, "dst", "d", limits=(
        pieces.Limit("max_allowed_packet", 4 * MB, "sent"),))
    # a piece sent escaped takes twice its bytes and the statement's room
    assert sz.inline == MB
    assert sz.pick() == MB
    seen = set()
    for _ in range(12):
        got = sz.pick()
        seen.add(got)
        sz.took(got, 10 * MB, 10 * MB / got)
    # a size past the packet is never handed out
    assert max(seen) <= MB and min(seen) >= MB >> pieces.Sizing.BELOW


def test_the_fastest_measured_size_carries_the_rest():
    sz = pieces.Sizing(None, "dst", "d", limits=(
        pieces.Limit("max_allowed_packet", 64 * MB, "sent"),))
    rate = {int(r.name): (2.0 if int(r.name) == 4 * MB else 1.0)
            for r in sz.rungs}
    for _ in range(len(sz.rungs)):
        got = sz.pick()
        sz.took(got, 100 * MB, 100 * MB / (rate[got] * 1e8))
    assert [sz.pick() for _ in range(3)] == [4 * MB] * 3


def test_a_refused_size_is_not_tried_again():
    sz = pieces.Sizing(None, "dst", "d", limits=(
        pieces.Limit("packet", 64 * MB, "sent"),))
    first = sz.pick()
    sz.failed(first, "packet too large")
    assert all(sz.pick() != first for _ in range(10))


def test_a_row_past_the_widest_kept_is_named():
    sz = pieces.Sizing(None, "dst", "d", limits=(
        pieces.Limit("the entry size limit", 6 * MB, "row"),))
    assert sz.too_wide(6 * MB) is None
    why = sz.too_wide(6 * MB + 1)
    assert "entry size limit" in why and "target" in why


class _Rows:
    def __init__(self, answers):
        self.answers, self.asked = answers, []

    def __call__(self, side, *rest):
        sql = next(a for a in rest if isinstance(a, str) and " " in a)
        self.asked.append(sql)
        for k, v in self.answers.items():
            if k in sql:
                if isinstance(v, Exception):
                    raise v
                return v
        return []


def test_tidb_reads_its_limits_off_the_server():
    eng = TiDBEngine.__new__(TiDBEngine)
    eng._q = _Rows({"max_allowed_packet": [(67108864,)],
                    "tidb_txn_entry_size_limit": [(0,)],
                    "show config": [("tidb", "a:4000", "x", "6291456"),
                                    ("tidb", "b:4000", "x", "8388608")]})
    got = {(x.name, x.bounds): x.most for x in eng.piece_limits("dst", "d")}
    assert got == {("max_allowed_packet", "sent"): 64 * MB,
                   ("max_allowed_packet", "returned"): 64 * MB,
                   ("the entry size limit", "row"): 6 * MB}


def test_tidb_without_a_readable_entry_limit_says_none():
    eng = TiDBEngine.__new__(TiDBEngine)
    eng._q = _Rows({"max_allowed_packet": [(1048576,)],
                    "tidb_txn_entry_size_limit": RuntimeError("unknown"),
                    "show config": RuntimeError("denied")})
    got = eng.piece_limits("dst", "d")
    assert [x.bounds for x in got] == ["sent", "returned"]


def test_tidb_runs_pieces_statements_with_its_driver_places():
    eng = TiDBEngine.__new__(TiDBEngine)

    class Cur:
        def execute(self, sql, args=None):
            self.sql, self.args = sql, args
    cur = Cur()
    eng._run(cur, f"update `t%x` set `b` = concat(`b`, {MARK}) where"
                  f" `id` = {MARK}", [b"x", 1])
    assert cur.sql == ("update `t%%x` set `b` = concat(`b`, %s) where"
                       " `id` = %s") and cur.args == (b"x", 1)
    assert pieces._quote(eng, "a`b") == "`a``b`"


def test_cockroachdb_reads_its_cluster_settings():
    eng = CockroachEngine.__new__(CockroachEngine)
    eng._rows = _Rows({"max_read_buffer_message_size": [("16 MiB",)],
                       "kv.raft.command.max_size": [("64 MiB",)],
                       "max_row_size_err": [("512 MiB",)]})
    got = {(x.name, x.bounds): x.most for x in eng.piece_limits("dst", "d")}
    assert got[("sql.conn.max_read_buffer_message_size", "sent")] == 16 * MB
    assert got[("kv.raft.command.max_size", "row")] == 64 * MB
    sz = pieces.Sizing(eng, "dst", "d")
    assert sz.inline * 2 + pieces.HEADROOM <= 16 * MB and sz.row == 64 * MB


def test_dsql_says_its_quota_is_not_read():
    eng = DsqlEngine.__new__(DsqlEngine)
    (lim,) = eng.piece_limits("dst", "d")
    assert lim.read is False and lim.bounds == "stored"
    assert "quota" in lim.said()


class _Writer:
    """An engine as `written_in_pieces` sees it, statements recorded."""
    CANON_ENGINE = "postgres"
    LARGE_VALUES = DsqlEngine.LARGE_VALUES

    def __init__(self, limits):
        self.limits, self.ran = limits, []

    def piece_limits(self, side, db):
        return self.limits

    def neutral_columns(self, side, db, table):
        return [("id", "integer"), ("b", "bytea")]

    def _q(self, name):
        return f'"{name}"'

    def _qualified(self, side, db, table):
        return f'"public"."{table}"'

    def _run(self, cur, sql, args):
        self.ran.append((sql, list(args)))


def test_a_wide_row_is_written_a_piece_a_unit_by_the_given_upsert():
    eng = _Writer([pieces.Limit("a write transaction", 10 * MB, "stored",
                                read=False)])
    units = []

    def each(work):
        units.append(1)
        work(None)
    value = bytes(range(256)) * (20 * MB // 256)
    rest = pieces.written_in_pieces(
        eng, None, "dst", "d", "t", [("id", "int"), ("b", "bytes")], ["id"],
        [[1, value], [2, b"small"]], insert=f"upsert {MARK} {MARK}",
        each=each)
    assert rest == [[2, b"small"]]
    first, *adds = eng.ran
    assert first[0] == f"upsert {MARK} {MARK}"
    assert len(first[1][1]) + pieces.HEADROOM <= 10 * MB
    assert adds and all('"b" = "b" ||' in sql for sql, _ in adds)
    assert b"".join([first[1][1]] + [a[0] for _, a in adds]) == value
    assert len(units) == len(eng.ran)


def test_a_row_past_the_targets_widest_is_never_written_in_part():
    eng = _Writer([pieces.Limit("entry", 2 * MB, "row")])
    with pytest.raises(SystemExit, match="not written cut"):
        pieces.written_in_pieces(
            eng, None, "dst", "d", "t", [("id", "int"), ("b", "bytes")],
            ["id"], [[1, b"x" * (3 * MB)]])
    assert eng.ran == []

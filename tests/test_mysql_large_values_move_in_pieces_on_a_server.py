"""A MySQL value wider than the server's packet moves in pieces sized from
that packet, and is the same value, hashed where it lies, on both sides
(R10.pieces).

On a MySQL 8.4 pair: a 64 MiB byte value and a 20 MiB text value are made
on the source while its `max_allowed_packet` is large, then the source's
packet is set below them, so no statement or row can carry either whole.
`pieces.Sizing` reads that packet off the server, the rows are read in
pieces and written in pieces (the target's packet takes the whole value,
which a text or byte value can only be appended to up to), and the two
sides' values are equal by the server's own `md5` and by the digest folded
here, which never holds the value (`pieces.DigestPlan`).
"""
import hashlib

from migkit import canon, pieces
from migkit.config import Hop
from tests import servers
from tests.conftest import needs_docker

pytestmark = needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("move.large-values", "mysql")]

pair = servers.fixture("mysql")

MIB = 1 << 20
BLOB, TEXT = 64 * MIB, 20 * MIB
SRC_PACKET, DST_PACKET = 16 * MIB, 128 * MIB
DEFAULT_PACKET = 64 * MIB


def _engine(pair, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="pieces", engine="mysql", source=pair.src.endpoint(),
              target=pair.dst.endpoint(), databases=[pair.name])
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


def test_a_value_wider_than_the_packet_moves_in_pieces_and_is_equal(
        pair, tmp_path):
    db = pair.name
    table = (f"create table `{db}`.big (id int primary key,"
             " b longblob, t longtext)")
    try:
        for s in (pair.src, pair.dst):
            s.sql(f"set global max_allowed_packet = {DST_PACKET}")
            s.sql(f"drop table if exists `{db}`.big")
            s.sql(table)
        # built under a large packet: a string function cannot make more
        # than the packet, so the value could not be made under the small
        pair.src.sql(
            f"insert into `{db}`.big values (1,"
            f" repeat(unhex(md5('seed')), {BLOB // 16}),"
            f" repeat('0123456789abcdef', {TEXT // 16})),"
            f" (2, unhex('00ff'), 'small')")
        assert pair.src.sql(f"select length(b), length(t) from `{db}`.big"
                            " where id = 1")[0] == (BLOB, TEXT)
        pair.src.sql(f"set global max_allowed_packet = {SRC_PACKET}")
        eng = _engine(pair, tmp_path)

        # the packet the sizing reads is the server's, and it is below both
        got = pieces.sizing(eng, "src", db)
        assert [x.name for x in got.limits] == ["max_allowed_packet"] * 2
        assert {x.most for x in got.limits} == {SRC_PACKET}
        assert all(x.read for x in got.limits)
        assert BLOB > SRC_PACKET and TEXT > SRC_PACKET
        assert got.inline * 2 + pieces.HEADROOM <= SRC_PACKET
        assert got.pick() <= got.inline < pieces.PIECE
        assert pieces.sizing(eng, "dst", db).inline == pieces.PIECE

        columns = [(n, canon.fold_class("mysql", d, n))
                   for n, d in eng.neutral_columns("src", db, "big")]
        assert [c for _, c in columns] == ["integer", "bytes", "text"] or \
            columns[1][1] == "bytes"
        # the byte value is left out of its row and read by pieces of the
        # size the sizing gave; a text value is read whole (a piece is
        # counted in characters, a length in bytes), and both are written in
        # pieces on the target
        read = eng._read_plan("src", db, "big", columns)
        assert [n for _, n in read.split] == ["b"]
        assert read.limit == got.inline
        seen, spy = [], pieces.written_in_pieces

        def watch(*a, **k):
            seen.append(sum(1 for r in a[7] for v in r
                            if isinstance(v, (bytes, str))
                            and len(v) > pieces.sizing(
                                eng, "dst", db).inline))
            return spy(*a, **k)
        pieces.written_in_pieces = watch
        try:
            moved = 0
            for batch in eng.neutral_batches("src", db, "big", columns):
                assert all(isinstance(v, (bytes, str, int, type(None)))
                           for r in batch for v in r)
                moved += eng.neutral_write("dst", db, "big", columns, batch)
        finally:
            pieces.written_in_pieces = spy
        assert moved == 2 and seen == [2]

        # equal where the values lie, by the server's own hash
        ask = (f"select id, length(b), md5(b), length(t), md5(t) from"
               f" `{db}`.big order by id")
        assert pair.src.sql(ask) == pair.dst.sql(ask)
        row = pair.dst.sql(ask)[0]
        assert row[1] == BLOB and row[3] == TEXT
        assert row[2] == hashlib.md5(
            hashlib.md5(b"seed").digest() * (BLOB // 16)).hexdigest()
        assert row[4] == hashlib.md5(
            b"0123456789abcdef" * (TEXT // 16)).hexdigest()

        # and by the digest folded here, in which a wide value is only its
        # length and hash (`Hashed`): equal on both sides, and not a digest
        # of an empty value
        def folded(side):
            with pieces.hashing(eng):
                return canon.fold_batches(columns, eng.neutral_batches(
                    side, db, "big", columns))
        assert folded("src") == folded("dst")
        assert folded("src")[0] == 2
        plan = None
        with pieces.hashing(eng):
            plan = eng._read_plan("src", db, "big", columns)
        assert isinstance(plan, pieces.DigestPlan)
        assert "md5(" in plan.select
    finally:
        for s in (pair.src, pair.dst):
            s.sql(f"set global max_allowed_packet = {DEFAULT_PACKET}")
            s.sql(f"drop table if exists `{db}`.big")

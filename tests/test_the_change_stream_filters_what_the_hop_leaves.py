"""The document store's change stream is filtered by the server to what
the hop takes (DTS's change-stream filter).

Every event of the database crossed the wire and was dropped here, one
by one, for a hop that takes three collections of three hundred. The
stream's own `$match` now leaves out what the hop does not take, read
from the hop's patterns so a collection made later under a taken pattern
is in it; an event renaming one into the scope passes by its new name;
one about the whole database always passes. Each event is still asked
here - the server's filter only saves the reading.

Pure: the filter is held to `Hop.excluded` on names, with Python's
regular expressions, which read these the way the server's do.
"""
import re

from migkit.config import Endpoint, Hop
from migkit.engines.mongodb import MongoEngine


def _eng(**kw):
    return MongoEngine(Hop(name="h", engine="mongodb", source=Endpoint(),
                           target=Endpoint(), **kw))


def _rx(match):
    return match[0]["$match"]["$or"][0]["ns.coll"]["$regex"]


def test_a_hop_that_takes_everything_reads_the_stream_whole():
    assert _eng().stream_match("shop") == []


def test_the_filter_takes_what_the_hop_takes():
    cases = [dict(include=["shop.orders", "shop.lines_*"]),
             dict(exclude=["shop.audit*", "sessions"]),
             dict(include=["shop.*"], exclude=["shop.tmp_*"])]
    names = ["orders", "lines_2024", "audit", "audit_log", "sessions",
             "tmp_x", "carts", "fs.chunks"]
    for kw in cases:
        eng = _eng(**kw)
        rx = re.compile(_rx(eng.stream_match("shop")))
        for n in names:
            assert bool(rx.match(n)) == (not eng.hop.excluded("shop", n)), \
                (kw, n)


def test_a_rename_into_the_scope_and_a_database_event_pass():
    got = _eng(include=["shop.orders"]).stream_match("shop")[0]["$match"]
    keys = [list(x)[0] for x in got["$or"]]
    assert keys == ["ns.coll", "to.coll", "ns.coll"]
    assert got["$or"][2] == {"ns.coll": {"$exists": False}}

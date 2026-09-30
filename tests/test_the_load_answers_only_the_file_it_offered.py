"""The MySQL target's bulk load answers a server's request for a file only
with the rows migkit prepared for that statement, once, and refuses every
other name - the client setting the load needs otherwise lets a server
read any file the client can, which is why the load had been set aside.

Proved here against the connection's own handler, a server's packets
faked: no server can be trusted to ask for the wrong file on cue.
"""
import pytest
from pymysql.constants import ER
from pymysql.protocol import MysqlPacket

from migkit.config import Endpoint, Hop
from migkit.engines import mysql

#: a server's OK after a load: 5 rows, no warnings
OK = b"\x00\x05\x00\x02\x00\x00\x00"


def _asked(name):
    return MysqlPacket(b"\xfb" + name, "utf8")


def _connection(feed, answers):
    """The pinned connection class, not connected: what it would write to
    the server is kept, and the server's packets are `answers`."""
    cls = mysql._loading()
    assert cls is not None
    conn = cls.__new__(cls)
    conn.feed = feed
    conn.max_allowed_packet = 16 * 2 ** 20
    conn.server_status = 0
    conn._result = None
    sent, queue = [], list(answers)
    conn.write_packet = sent.append
    conn._read_packet = lambda *a, **k: MysqlPacket(queue.pop(0), "utf8")
    return conn, sent


def test_the_offered_rows_are_sent_once():
    rows = b"1\tone\n2\t\\N\n"
    conn, sent = _connection((b"rows-abc", rows),
                             [b"\xfbrows-abc", OK])
    assert conn._read_query_result() == 5
    # the rows, then the empty packet that ends a file
    assert sent == [rows, b""]
    assert conn.feed is None
    # the same name asked again is not answered again
    conn._read_packet = iter([_asked(b"rows-abc"),
                              MysqlPacket(OK, "utf8")]).__next__
    sent.clear()
    with pytest.raises(Exception) as e:
        conn._read_query_result()
    assert e.value.args[0] == ER.FILE_NOT_FOUND
    assert sent == [b""]


@pytest.mark.parametrize("feed", [None, (b"rows-abc", b"1\tone\n")],
                         ids=["nothing offered", "other rows offered"])
@pytest.mark.parametrize("name", [b"/etc/passwd", b"rows-abc.", b"ROWS-ABC",
                                  b"../rows-abc", b""])
def test_any_other_name_is_sent_nothing(feed, name):
    if feed is None and name == b"rows-abc":
        pytest.skip("the offered name, with nothing offered")
    conn, sent = _connection(feed, [b"\xfb" + name, OK])
    with pytest.raises(Exception, match="did not offer; it was sent"
                                        " nothing") as e:
        conn._read_query_result()
    assert e.value.args[0] == ER.FILE_NOT_FOUND
    # an empty file, which the server takes as no rows: nothing of the
    # client's, and the connection still in step with the server
    assert sent == [b""]
    # and what was offered is not left to be served to a later request
    assert conn.feed is None


def test_rows_larger_than_a_packet_go_in_pieces():
    rows = bytes(range(256)) * (3 * 4096 + 7)
    conn, sent = _connection((b"rows-big", rows), [b"\xfbrows-big", OK])
    conn._read_query_result()
    assert sent[-1] == b"" and len(sent) > 3
    assert all(len(p) < 2 ** 20 for p in sent)
    assert b"".join(sent) == rows


def test_only_the_target_is_offered_loads(monkeypatch):
    """The source's connection is the driver's own, which refuses every
    request for a file; only the target's is the pinned one."""
    made = []

    def opener(**kw):
        made.append(kw)
        raise ConnectionRefusedError("not connecting in this test")

    class Pinned:
        def __new__(cls, **kw):
            return opener(pinned=True, **kw)
    import pymysql
    monkeypatch.setattr(pymysql, "connect", lambda **kw: opener(**kw))
    monkeypatch.setattr(mysql, "_loading", lambda: Pinned)
    ep = Endpoint(host="127.0.0.1", port=1, user="u", password="p")
    eng = mysql.MySQLEngine(Hop(name="x", engine="mysql", source=ep,
                                target=ep, databases=["d"]))
    for side in ("src", "dst"):
        with pytest.raises(ConnectionRefusedError):
            eng._conn(side, retry=False)
    src, dst = made
    assert src["local_infile"] is False and "pinned" not in src
    assert dst["local_infile"] is True and dst["pinned"] is True

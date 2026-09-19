"""The row encoding has to survive a charset difference between the sides.

A migration that lands `utf8mb4` data in a `latin1` target is not theoretical
- it is the shape of a real leg, later corrected. Two questions follow, and
they pull in opposite directions:

1. Data both charsets can hold must encode **identically**, or every row of
   every table reads as a difference nobody can act on.
2. Data the target charset cannot hold must encode **differently**, because
   that is real corruption and the whole point of the check.

Both are measured here against two servers started with different server
charsets, and both hold.

The reason they hold is worth stating, because it is not obvious and it is
what a refactor could remove: `cast(v as char)` converts into the
*connection's* character set, and migkit pins that to utf8mb4 on both sides.
Measured, `café` in a latin1 column and in a utf8mb4 column both come back as
4 characters and 5 bytes - the column's own storage never reaches the hash.
Drop the pinned charset and each side casts into its own server default, and
every row on a mixed-charset pair becomes a difference.
"""
import socket
import subprocess
import time

import pytest

L1, U8 = "migkit-test-cs-latin1", "migkit-test-cs-utf8"
L1_PORT, U8_PORT = 13455, 13456


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(),
                                 reason="docker not available")]


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


@pytest.fixture(scope="module")
def pair():
    for n, p, args in ((L1, L1_PORT, ["--character-set-server=latin1",
                                      "--collation-server=latin1_swedish_ci"]),
                       (U8, U8_PORT, ["--character-set-server=utf8mb4"])):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{p}:3306",
                        "mysql:8"] + args, check=True, capture_output=True)
    for p in (L1_PORT, U8_PORT):
        assert _wait(p)
    for n in (L1, U8):
        for _ in range(90):
            r = subprocess.run(["docker", "exec", n, "mysql", "-uroot",
                                "-ptest", "-e", "select 1"],
                               capture_output=True)
            if r.returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail(f"{n} never accepted a connection")
    yield
    for n in (L1, U8):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def _conn(port):
    import pymysql
    # the charset migkit itself pins on both sides, which is what makes
    # `cast(... as char)` land in one encoding regardless of the column's
    return pymysql.connect(host="127.0.0.1", port=port, user="root",
                           password="test", charset="utf8mb4",
                           autocommit=True)


def _load(port, column_charset, value, relax=False):
    c = _conn(port)
    cur = c.cursor()
    cur.execute("drop database if exists cs")
    cur.execute("create database cs")
    cur.execute(f"create table cs.t (id int primary key,"
                f" v varchar(40) character set {column_charset})")
    if relax:
        # what a mover does when the target cannot hold the source's bytes
        cur.execute("set session sql_mode=''")
    cur.execute("insert into cs.t values (1, %s)", (value,))
    c.close()


def _encoded(port):
    from migkit import rowtext
    expr = rowtext.mysql_row(["id", "v"])
    c = _conn(port)
    cur = c.cursor()
    cur.execute(f"select {expr}, v, char_length(cast(`v` as char)),"
                f" length(cast(`v` as char)) from cs.t")
    row = cur.fetchone()
    c.close()
    assert row, "nothing was loaded, so nothing below is being tested"
    return row


def test_the_same_text_encodes_the_same_in_either_charset(pair):
    """`café` fits in both, so the two sides must agree - otherwise every
    row of every table on a mixed-charset pair is a false difference."""
    _load(U8_PORT, "utf8mb4", "café")
    _load(L1_PORT, "latin1", "café")
    a, b = _encoded(U8_PORT), _encoded(L1_PORT)
    assert a[1] == b[1] == "café", (a[1], b[1])
    assert a[0] == b[0], (a[0], b[0])


def test_the_connection_charset_is_what_makes_the_two_sides_agree(pair):
    """The real mechanism, and the real trap.

    `cast(v as char)` converts into the *connection's* character set, not the
    column's - so with the connection pinned to utf8mb4 on both sides, a
    latin1 column and a utf8mb4 column holding `café` are measured after the
    same conversion. Measured: 4 characters and 5 bytes on both, even though
    the latin1 column stores 4 bytes on disk.

    The trap is therefore not `char_length` versus `length` - both agree here
    - it is dropping the pinned connection charset. Then each side casts into
    its own server default and the two stop agreeing, on every row.
    """
    _load(U8_PORT, "utf8mb4", "café")
    _load(L1_PORT, "latin1", "café")
    u8, l1 = _encoded(U8_PORT), _encoded(L1_PORT)
    assert u8[2] == l1[2] == 4, ("character counts drifted", u8[2], l1[2])
    assert u8[3] == l1[3] == 5, (
        "the cast is no longer normalising into one charset", u8[3], l1[3])
    assert u8[0].startswith("1:1|4:"), u8[0]


def test_migkit_pins_the_connection_charset(pair, tmp_path):
    """Read from the engine rather than the test's own connection: the
    property above only holds because migkit itself pins it."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="c", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=L1_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=U8_PORT, user="root",
                              password="test"),
              db_map={"cs": "cs"})
    hop.report_dir = lambda db=None: tmp_path
    eng = MySQLEngine(hop)
    got = eng._q("src", "select @@session.character_set_client,"
                        " @@session.character_set_results")[0]
    assert all(str(v).startswith("utf8") for v in got), got


def test_text_the_target_charset_cannot_hold_encodes_differently(pair):
    """Real corruption, and the reason the check exists. A latin1 column
    cannot hold Thai; forced through, it becomes question marks."""
    thai = "สวัสดี"
    _load(U8_PORT, "utf8mb4", thai)
    _load(L1_PORT, "latin1", thai, relax=True)
    u8, l1 = _encoded(U8_PORT), _encoded(L1_PORT)
    assert u8[1] == thai
    assert l1[1] == "?" * len(thai), l1[1]
    assert u8[0] != l1[0], "charset corruption hashed the same"


def test_a_strict_target_refuses_the_insert_outright(pair):
    """Before a mover relaxes sql_mode, MySQL rejects it - which is the
    loudest and best outcome, and worth knowing is the default."""
    import pymysql
    with pytest.raises(pymysql.err.DataError):
        _load(L1_PORT, "latin1", "สวัสดี")

"""A digest of rows holding wide values, without writing the values out
(backlog R22's finding).

Every server-side digest rendered a byte value as its hex and wrote it into
the row's text, a UTF-8 copy of that, and the md5 over all of it - a value
held three and four times over, once more a parallel worker. A PostgreSQL
source digesting rows of 160 MB values was killed by the kernel on a 4 GB
machine; on a small production source the check could take the database
down. A text or byte value wider than `canon.WIDE` now goes into the row's
text as its length and the md5 of its bytes, hashed where it lies - the
same text on every engine and in this process, so two sides still meet -
and a table of wide rows is digested by fewer parallel workers.
"""
import hashlib
import socket
import subprocess
import time

import pytest

from migkit import canon, render, rowtext
from migkit.config import Endpoint, Hop


def test_a_wide_value_is_its_length_and_md5():
    big = bytes(range(256)) * (canon.WIDE // 256 + 1)
    assert canon.digest_value("bytes", big) == (
        f"{canon.WIDE_MARK}{len(big)}:{hashlib.md5(big).hexdigest()}")
    assert canon.digest_value("bytes", b"\x00\xff") == "00FF"
    text = "é" * (canon.WIDE // 2 + 1)     # wide as UTF-8, not as chars
    data = text.encode()
    assert canon.digest_value("text", text) == (
        f"{canon.WIDE_MARK}{len(data)}:{hashlib.md5(data).hexdigest()}")
    assert canon.digest_value("text", "é" * 10) == "é" * 10
    # a key, a record, a column compared by value keep the value whole
    assert canon.render_value("bytes", big) == big.hex().upper()


def test_the_renderer_a_fold_uses_writes_what_digest_value_writes(
        monkeypatch):
    monkeypatch.setattr(canon, "WIDE", 8)
    for cls, values in (("bytes", [b"", b"\x01" * 8, b"\x02" * 9,
                                   bytearray(b"\x03" * 20), None]),
                        ("text", ["", "a" * 8, "a" * 9, "é" * 4, "é" * 5,
                                  None]),
                        ("integer", [1, 10 ** 30])):
        one = render.digest_renderer(cls)
        for v in values:
            assert one(v) == (None if v is None
                              else canon.digest_value(cls, v)), (cls, v)
    rows = [[b"\x09" * 30, "x" * 30], [b"\x01", "y"]]
    want = 0
    for r in rows:
        want = canon.digest_step(want, rowtext.encode(
            [canon.digest_value("bytes", r[0]),
             canon.digest_value("text", r[1])]))
    assert canon.fold_rows(["bytes", "text"], rows) == (2, want)


def test_every_server_rendering_hashes_the_value_where_it_lies():
    pg_b = canon.expr("postgres", "c", "bytes")
    assert 'md5("c")' in pg_b and f"> {canon.WIDE}" in pg_b, pg_b
    pg_t = canon.expr("postgres", "c", "text")
    assert "md5(convert_to(\"c\"::text, 'UTF8'))" in pg_t, pg_t
    my_b = canon.expr("mysql", "c", "bytes")
    assert "md5(`c`)" in my_b and "hex(`c`)" in my_b, my_b
    my_t = canon.expr("mysql", "c", "text")
    assert "md5(convert(`c` using utf8mb4))" in my_t, my_t
    own = rowtext.mysql_row(["a", "b"])
    assert own.count("md5(`") == 2 and own.count("char_length") == 2, own


def test_the_same_engine_row_hash_of_postgresql_does_too(monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="w", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["d"])
    eng = PostgresEngine(hop)
    monkeypatch.setattr(eng, "_row_cols", lambda *a: [("b", "wide"),
                                                      ("id", None)])
    h = eng._row_hash_expr("src", "d", "public.t")
    assert 'md5(t."b")' in h and 't."b"::text' in h, h


def test_a_table_of_wide_rows_is_digested_by_fewer_workers(monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="w", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["d"])
    eng = PostgresEngine(hop)
    sizes = {"public.wide": 400 * 2 ** 20, "public.narrow": 2 ** 20}
    monkeypatch.setattr(eng, "row_sizes", lambda side, db, tables: {
        tables[0]: {"largest": sizes[tables[0]]}})
    assert eng._digest_workers("d", "public.wide", 8) == 0
    assert eng._digest_workers("d", "public.narrow", 8) == 8


# --- on servers ---------------------------------------------------------

PG_SRC, PG_DST, MY = (("migkit-test-vd-wide-src", 16276),
                      ("migkit-test-vd-wide-dst", 16277),
                      ("migkit-test-vd-wide-my", 16278))


def _docker():
    from tests.conftest import _have_docker
    return _have_docker()


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


def _pg(name, sql):
    return subprocess.run(
        ["docker", "exec", name, "psql", "-U", "postgres", "-d", "postgres",
         "-At", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)


def _sh(name, cmd):
    return subprocess.run(["docker", "exec", name, "sh", "-c", cmd],
                          capture_output=True, text=True)


@pytest.fixture(scope="module")
def servers():
    if not _docker():
        pytest.skip("docker not available")
    for name, _ in (PG_SRC, PG_DST, MY):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    try:
        # the source capped as a small production server is: 1 GB, and no
        # swap to hide in
        subprocess.run(["docker", "run", "-d", "--name", PG_SRC[0],
                        "--memory", "1g", "--memory-swap", "1g", "-e",
                        "POSTGRES_PASSWORD=test", "-p",
                        f"127.0.0.1:{PG_SRC[1]}:5432", "postgres:16"],
                       check=True, capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", PG_DST[0], "-e",
                        "POSTGRES_PASSWORD=test", "-p",
                        f"127.0.0.1:{PG_DST[1]}:5432", "postgres:16"],
                       check=True, capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", MY[0], "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p",
                        f"127.0.0.1:{MY[1]}:3306", "mysql:8.4",
                        "--max-allowed-packet=256M"],
                       check=True, capture_output=True)
        for name, port in (PG_SRC, PG_DST):
            assert _wait(port)
            for _ in range(60):
                if _pg(name, "select 1").returncode == 0:
                    break
                time.sleep(1)
            else:
                pytest.fail(f"{name} never answered")
        assert _wait(MY[1])
        for _ in range(150):
            if subprocess.run(["docker", "exec", MY[0], "mysql", "-uroot",
                               "-ptest", "-e", "select 1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail("mysql never answered")
        yield
    finally:
        for name, _ in (PG_SRC, PG_DST, MY):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _seed_wide():
    """Three rows each side: 150 MB of bytes and 100 MB of text in two of
    them, read from files written in the container, the same on both."""
    for name, _ in (PG_SRC, PG_DST):
        got = _sh(name, "yes 0123456789abcdef | head -c 157286400"
                        " > /tmp/b && yes 'wide text, é' | head -c"
                        " 104857600 > /tmp/t")
        assert got.returncode == 0, got.stderr
        # a statement a row: loading all three at once would itself hold
        # 400 MB on the capped source
        for sql in ("drop table if exists w; create table w (id int"
                    " primary key, b bytea, t text)",
                    "insert into w values (1, pg_read_binary_file('/tmp/b'),"
                    " convert_from(pg_read_binary_file('/tmp/t'), 'UTF8'))",
                    "insert into w values (2, 'x', 'small')",
                    "insert into w values (3, pg_read_binary_file('/tmp/b'),"
                    " null)"):
            got = _pg(name, sql)
            assert got.returncode == 0, got.stderr


def _alive():
    got = subprocess.run(["docker", "inspect", "-f",
                          "{{.State.Running}} {{.State.OOMKilled}}",
                          PG_SRC[0]], capture_output=True, text=True)
    return got.stdout.split()


def _pg_engine(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="w", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=PG_SRC[1],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=PG_DST[1],
                              user="postgres", password="test"),
              databases=["postgres"], workers=1)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


@pytest.mark.docker
def test_a_capped_source_digests_rows_of_150_mb_and_lives(servers,
                                                          tmp_path):
    from migkit import leaves
    _seed_wide()
    eng = _pg_engine(tmp_path)
    got = [r for r in eng.check_data("postgres") if "w" in r.scope
           or r.scope == "postgres"]
    assert got and all(r.status == "ok" for r in got), \
        [(r.scope, r.status, r.detail) for r in got]
    assert _alive() == ["true", "false"], _alive()
    # the canonical digest, and the leaves of the tree, both sides
    cols = [("b", "bytes"), ("id", "integer"), ("t", "text")]
    a = eng.neutral_digest("src", "postgres", "public.w", cols)
    b = eng.neutral_digest("dst", "postgres", "public.w", cols)
    assert a == b and int(a[0]) == 3, (a, b)
    spec = leaves.plan(3, "hash", salt="s")
    sa = leaves.Side(eng, "src", "postgres", "public.w", cols, ["id"])
    sb = leaves.Side(eng, "dst", "postgres", "public.w", cols, ["id"])
    assert leaves.scan(sa, spec).leaves == leaves.scan(sb, spec).leaves
    assert _alive() == ["true", "false"], _alive()


@pytest.mark.docker
def test_one_byte_changed_in_a_wide_value_is_a_difference(servers,
                                                          tmp_path):
    _seed_wide()
    got = _pg(PG_DST[0], "update w set b = set_byte(b, 100000000, 7)"
                         " where id = 1")
    assert got.returncode == 0, got.stderr
    eng = _pg_engine(tmp_path)
    cols = [("b", "bytes"), ("id", "integer"), ("t", "text")]
    a = eng.neutral_digest("src", "postgres", "public.w", cols)
    b = eng.neutral_digest("dst", "postgres", "public.w", cols)
    assert a[0] == b[0] and a[1] != b[1], (a, b)
    got = [r for r in eng.check_data("postgres") if r.status == "diff"]
    assert got, "a changed byte in a wide value went unseen"
    assert _alive() == ["true", "false"], _alive()


@pytest.mark.docker
def test_two_engines_and_this_process_write_a_wide_value_alike(
        servers, monkeypatch):
    """Across engines the rendering has to meet: PostgreSQL, MySQL and the
    fold here give one digest of the same wide values (made wide here by a
    small `WIDE`)."""
    import pymysql
    monkeypatch.setattr(canon, "WIDE", 64)
    got = _pg(PG_SRC[0], "drop table if exists x; create table x (id int"
                         " primary key, b bytea, t text); insert into x"
                         " values (1, decode(repeat('ab', 100), 'hex'),"
                         " repeat('é', 50)), (2, '\\x01', 'short')")
    assert got.returncode == 0, got.stderr
    con = pymysql.connect(host="127.0.0.1", port=MY[1], user="root",
                          password="test", charset="utf8mb4",
                          autocommit=True)
    with con.cursor() as cur:
        cur.execute("create database if not exists w")
        cur.execute("drop table if exists w.x")
        cur.execute("create table w.x (id int primary key, b longblob,"
                    " t longtext) character set utf8mb4")
        cur.execute("insert into w.x values (1, %s, %s), (2, %s, 'short')",
                    (bytes([0xab]) * 100, "é" * 50, b"\x01"))
    con.close()
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    ep_pg = Endpoint(host="127.0.0.1", port=PG_SRC[1], user="postgres",
                     password="test")
    ep_my = Endpoint(host="127.0.0.1", port=MY[1], user="root",
                     password="test")
    pg = PostgresEngine(Hop(name="p", engine="postgres", source=ep_pg,
                            target=ep_pg, databases=["postgres"]))
    my = MySQLEngine(Hop(name="m", engine="mysql", source=ep_my,
                         target=ep_my, databases=["w"]))
    cols = [("b", "bytes"), ("id", "integer"), ("t", "text")]
    a = pg.neutral_digest("src", "postgres", "public.x", cols)
    b = my.neutral_digest("src", "w", "x", cols)
    rows = [[bytes([0xab]) * 100, 1, "é" * 50], [b"\x01", 2, "short"]]
    here = canon.fold_rows([c for _, c in cols], rows)
    try:
        assert (int(a[0]), str(a[1])) == (int(b[0]), str(b[1])) \
            == (here[0], str(here[1])), (a, b, here)
    finally:
        _pg(PG_SRC[0], "drop table if exists x")

"""Large values, and the truncation that no row count will show you.

Every mover with a LOB mode has a size limit, and the fast mode is the one
that truncates past it. AWS DMS's limited LOB mode pre-allocates to
`LobMaxSize` and cuts anything longer, with a warning in a task log nobody
reads; the rows all arrive, the counts agree, and a column is quietly
shorter than it was.

Simulated here the way it happens - the values on the target cut at 32 KB
while the source's run to 61 KB - migkit reports:

    deep postgres lobs: DIFF 1 columns hold smaller values on the target
    than on the source: public.bench_lobs.blob 61,437 -> 32,768 bytes

There is no clever pre-filter, because the clever one was measured to have
a hole: a 1,000,000-byte value compressed to 11,452 bytes on the way in, so
anything reading stored sizes would have skipped the value most likely to
be truncated. It is also unnecessary - `max(octet_length(col))` over two
columns of a 2,000,000-row, 531 MB table answered in 0.25 s.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

BIG = 40000      # bytes per value on the source


def _engine(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="lob", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=2)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _seed(pg_pair, target_bytes=BIG):
    """A wide column on both sides; the target's values can be cut."""
    for port, size in ((pg_pair["src"], BIG), (pg_pair["dst"],
                                               target_bytes)):
        got = psql(port, "drop table if exists public.papers;"
                         " drop table if exists public.plain;"
                         " create table public.papers (id bigint primary key,"
                         " body bytea);"
                         " create table public.plain (id bigint primary key,"
                         " v text);"
                         " insert into public.plain select g, 'v'||g from"
                         " generate_series(1,2000) g;"
                         " insert into public.papers select g,"
                         # two hex characters make one byte, so `size`
                         # repetitions of 'ab' is `size` bytes
                         f" decode(repeat('ab', {size}), 'hex')"
                         " from generate_series(1,50) g;")
        assert got.returncode == 0, got.stderr


def _lobs(results):
    got = [r for r in results if r.scope.endswith("lobs")]
    assert got, [r.scope for r in results]
    return got[0]


def test_a_column_cut_on_the_target_is_named_with_both_sizes(pg_pair,
                                                              tmp_path):
    _seed(pg_pair, target_bytes=8000)
    got = _lobs(_engine(pg_pair, tmp_path).check_deep("postgres"))
    assert got.status == "diff", got.detail
    assert "public.papers.body" in got.detail, got.detail
    assert "40,000" in got.detail and "8,000" in got.detail, got.detail
    assert "row counts will not show it" in got.detail


def test_equal_sides_report_the_biggest_value_for_sizing_the_mover(pg_pair,
                                                                    tmp_path):
    """The number an operator needs before the move, not after it."""
    _seed(pg_pair)
    got = _lobs(_engine(pg_pair, tmp_path).check_deep("postgres"))
    assert got.status == "ok", got.detail
    assert "public.papers.body" in got.detail, got.detail
    assert "40,000 bytes" in got.detail, got.detail


def test_a_value_that_compressed_away_is_still_found(pg_pair, tmp_path):
    """The obvious shortcut - only look where PostgreSQL stored something
    out of line - has a hole in it, and this is the hole. A million bytes
    of repeated text compresses to about eleven thousand, so a check
    reading stored sizes would pass over exactly the value a limited LOB
    mode truncates."""
    _seed(pg_pair)
    psql(pg_pair["src"], "insert into public.papers values (999,"
                         " decode(repeat('61', 500000), 'hex'));")
    eng = _engine(pg_pair, tmp_path)
    stored, real = psql(
        pg_pair["src"],
        "select pg_column_size(body)||' '||octet_length(body) from"
        " public.papers where id = 999;").stdout.split()
    assert int(stored) < int(real) / 10, (stored, real)

    sizes = {(t, c): src for t, c, src, _, _ in eng._lob_sizes("postgres")}
    assert sizes[("public.papers", "body")] == 500000, sizes


def test_the_verdict_needs_no_server(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="l", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    gb = PostgresEngine.PG_FIELD_LIMIT

    cut = eng._lob_result("x", [("t", "body", 5000, 1000, gb)], "limit", "h")
    assert cut.status == "diff" and "5,000" in cut.detail

    # a target that is *bigger* is not this failure - it is something else,
    # and the checksum is what has an opinion about it
    bigger = eng._lob_result("x", [("t", "body", 1000, 5000, gb)], "limit",
                             "h")
    assert bigger.status == "ok", bigger.detail

    near = eng._lob_result("x", [("t", "body", int(gb * 0.9), int(gb * 0.9),
                                  gb)], "the limit", "h")
    assert near.status == "warn" and "the limit" in near.detail

    none = eng._lob_result("x", [], "limit", "h")
    assert none.status == "ok" and "out of line" in none.detail

    # a target table that does not exist yet is not a truncation
    absent = eng._lob_result("x", [("t", "body", 5000, None, gb)], "l", "h")
    assert absent.status == "ok", absent.detail


# ---- mysql, so this is not a postgres-only capability ------------------

MY = "migkit-test-lob-my"
MY_PORT = 13402


@pytest.fixture(scope="module")
def mysql_one():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8"], check=True, capture_output=True)
    end = time.time() + 180
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", MY_PORT)) == 0:
                break
        time.sleep(1)
    for _ in range(60):
        if subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                           "-h127.0.0.1", "--protocol=tcp",
                           "-e", "select 1"],
                          capture_output=True).returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("mysql never answered")
    yield MY_PORT
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _my(sql):
    return subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                           "-N", "-B", "-e", sql], capture_output=True,
                          text=True)


def test_mysql_sees_the_same_truncation(mysql_one, tmp_path):
    """Two databases on the one server stand in for the two sides."""
    from migkit.engines.mysql import MySQLEngine
    for name, size in (("lsrc", 300000), ("ldst", 1000)):
        got = _my(f"drop database if exists {name}; create database {name};"
                  f" create table {name}.docs (id int primary key,"
                  " body longblob, note text);"
                  f" insert into {name}.docs values (1, repeat('x',{size}),"
                  " repeat('n',500));")
        assert got.returncode == 0, got.stderr

    hop = Hop(name="lob", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=mysql_one, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=mysql_one, user="root",
                              password="test"),
              databases=["lsrc"], db_map={"lsrc": "ldst"})
    hop.report_dir = lambda db=None: tmp_path
    got = MySQLEngine(hop)._lob_check("lsrc")
    assert got.status == "diff", got.detail
    assert "docs.body" in got.detail, got.detail
    assert "300,000" in got.detail and "1,000" in got.detail, got.detail

    # and with both sides equal it reports the size instead of an alarm
    _my("update ldst.docs set body = repeat('x',300000);")
    again = MySQLEngine(hop)._lob_check("lsrc")
    assert again.status == "ok", again.detail
    assert "300,000 bytes" in again.detail, again.detail

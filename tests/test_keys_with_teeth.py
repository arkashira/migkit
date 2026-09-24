"""Primary keys holding the characters the drilldown files are made of.

The same five keys are pushed through every engine that keeps a drilldown: a
backslash, a tab, a newline, a double quote, and one ordinary key to prove
the test is not just failing everything. They are not exotic - a backslash
arrives with any Windows path or regex, and a newline with any key built
from something a human pasted.

What the sweep found, each measured against a real server:

    mysql      `apply` returned with no error and did nothing **at all**,
               for every key, ordinary ones included. The check wrote
               `rowtext`'s `2:11` and the repair read it back with
               `split("\\t")`, so the `where` compared the key against the
               whole encoded string and matched no row. The next check
               reported the same differences it had just been asked to fix.
    mysql      with that fixed, a key holding a newline broke the
               `pt-table-sync --print` path: its statements are read back a
               line at a time, so half a statement ran - `SQL syntax ...
               near ''line'`. A backslash silently matched nothing there
               too, MySQL treating it as an escape inside the literal.
    redis      keys are binary-safe, the drilldown was written raw, and a
               key with a newline was written as one line and read as two.
    sqlite     already right: the keys go out as JSON.
    mongodb    already right, for the same reason.

The postgres half of this lives in `test_pg_exotic_keys.py`, which is where
the sweep started.
"""
import json
import socket
import sqlite3
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import psql

KEYS = ["plain", "back\\slash", "has\ttab", "line\nbreak", '"quoted"']


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
        time.sleep(1)
    return False


def _start(name, port, image, inner, *extra):
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                    f"{port}:{inner}", *extra, image], check=True,
                   capture_output=True)


# ---- rowtext, which needs no server ----------------------------------

def test_the_encoding_reads_back_what_it_wrote():
    from migkit import rowtext
    assert rowtext.parse("2:11") == ["11"]
    assert rowtext.parse("1:3|2:99") == ["3", "99"]
    assert rowtext.parse("7:has\ttab") == ["has\ttab"]
    assert rowtext.parse("10:line\nbreak") == ["line\nbreak"]
    for bad in ("2:11|", "9:short", "nope", ""):
        with pytest.raises(ValueError):
            rowtext.parse(bad)


def test_a_file_of_keys_is_read_by_length_not_by_line():
    """A key may contain the separator the file uses between keys, so the
    lengths are what say where one ends."""
    from migkit import rowtext
    body = "10:back\\slash\n10:line\nbreak\n6:quo\"te\n7:has\ttab\n"
    assert rowtext.parse_all(body) == [["back\\slash"], ["line\nbreak"],
                                       ["quo\"te"], ["has\ttab"]]
    assert len(body.splitlines()) == 5, "the line count is the wrong answer"
    assert rowtext.parse_all("") == []
    with pytest.raises(ValueError):
        rowtext.parse_all("10:line\nbreak\nnot an encoded row\n")


# ---- mysql -----------------------------------------------------------

MY1, MY2 = "migkit-test-teeth-my1", "migkit-test-teeth-my2"
MY1_PORT, MY2_PORT = 13395, 13396


def _mysql(container, sql):
    return subprocess.run(
        ["docker", "exec", container, "mysql", "-uroot", "-ptest", "-N", "-B",
         "-h127.0.0.1", "--protocol=tcp", "-e", sql],
        capture_output=True, text=True)


@pytest.fixture(scope="module")
def mysql_pair():
    for name, port in ((MY1, MY1_PORT), (MY2, MY2_PORT)):
        _start(name, port, "mysql:8", 3306, "-e", "MYSQL_ROOT_PASSWORD=test")
    for port in (MY1_PORT, MY2_PORT):
        assert _wait(port)
    for name in (MY1, MY2):
        for _ in range(60):
            if _mysql(name, "select 1").returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail(f"{name} never answered")
        _mysql(name, "create database app;")
    yield {"src": MY1_PORT, "dst": MY2_PORT}
    for name in (MY1, MY2):
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)


def _mysql_engine(mysql_pair, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="teeth", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=mysql_pair["src"],
                              user="root", password="test"),
              target=Endpoint(host="127.0.0.1", port=mysql_pair["dst"],
                              user="root", password="test"),
              databases=["app"])
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


def _mysql_keys(container):
    got = _mysql(container, "select hex(k) from app.t order by k")
    assert got.returncode == 0, got.stderr
    return sorted(got.stdout.split())


def _seed_mysql(mysql_pair, keys):
    rows = ", ".join("(0x{}, 'v')".format(k.encode().hex()) for k in keys)
    for name in (MY1, MY2):
        got = _mysql(name, "drop table if exists app.t;"
                           " create table app.t (k varbinary(60) primary key,"
                           " v varchar(60));"
                           f" insert into app.t values {rows};")
        assert got.returncode == 0, got.stderr


def test_a_mysql_repair_with_ordinary_keys_really_writes(mysql_pair,
                                                          tmp_path):
    """The first thing the sweep found, and the reason this file exists:
    nothing about it needed a strange key."""
    _seed_mysql(mysql_pair, ["a", "b", "c", "d"])
    _mysql(MY2, "update app.t set v='CHANGED' where k='a';"
                " delete from app.t where k='b';"
                " insert into app.t values ('zz','extra');")
    eng = _mysql_engine(mysql_pair, tmp_path)
    got = [r for r in eng.check_data("app") if r.scope.endswith(".t")]
    assert got[0].status == "diff", got[0].detail
    assert "missing=1 extra=1 changed=1" in got[0].detail, got[0].detail

    actions = eng.repair_plan("app", "rows")
    assert actions, "nothing to repair"
    eng.apply("app", actions[0])

    assert _mysql_keys(MY1) == _mysql_keys(MY2)
    assert _mysql(MY2, "select v from app.t where k='a'"
                  ).stdout.strip() == "v"
    after = [r for r in eng.check_data("app") if r.scope.endswith(".t")]
    assert after[0].status == "ok", after[0].detail


def test_mysql_repairs_keys_that_carry_the_file_separators(mysql_pair,
                                                            tmp_path):
    _seed_mysql(mysql_pair, KEYS)
    hexes = ", ".join("0x{}".format(k.encode().hex()) for k in KEYS[1:])
    _mysql(MY2, f"delete from app.t where k in ({hexes});"
                " insert into app.t values ('stray','z');")

    eng = _mysql_engine(mysql_pair, tmp_path)
    got = [r for r in eng.check_data("app") if r.scope.endswith(".t")]
    assert got[0].status == "diff", got[0].detail
    assert "missing=4 extra=1" in got[0].detail, got[0].detail

    actions = eng.repair_plan("app", "rows")
    # counted as keys, not lines: the newline key would have made it five
    assert "missing=4" in actions[0].statements[0], actions[0].statements
    eng.apply("app", actions[0])

    assert _mysql_keys(MY1) == _mysql_keys(MY2), "the keys did not come back"
    after = [r for r in eng.check_data("app") if r.scope.endswith(".t")]
    assert after[0].status == "ok", after[0].detail


def test_the_printed_sql_path_stands_aside_for_keys_it_cannot_quote():
    """pt-table-sync's statements are read back a line at a time and its
    `--where` is built by quoting, so keys with a newline or a backslash go
    down the parameterised path instead."""
    from migkit.engines.mysql import MySQLEngine
    for key in ("line\nbreak", "back\\slash", "carriage\rreturn"):
        assert any(c in key for c in MySQLEngine.PT_UNSAFE), key
    assert not any(c in "ordinary-key" for c in MySQLEngine.PT_UNSAFE)


# ---- redis -----------------------------------------------------------

RD1, RD2 = "migkit-test-teeth-rd1", "migkit-test-teeth-rd2"
RD1_PORT, RD2_PORT = 16445, 16446


@pytest.fixture(scope="module")
def redis_pair():
    for name, port in ((RD1, RD1_PORT), (RD2, RD2_PORT)):
        _start(name, port, "redis:7", 6379)
    for port in (RD1_PORT, RD2_PORT):
        assert _wait(port)
    yield {"src": RD1_PORT, "dst": RD2_PORT}
    for name in (RD1, RD2):
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)


def test_redis_repairs_keys_that_carry_a_newline(redis_pair, tmp_path):
    """Redis keys are binary-safe, so the drilldown cannot be a list of
    lines of raw keys."""
    import redis as redis_driver
    from migkit.engines.redis import RedisEngine
    src = redis_driver.Redis(host="127.0.0.1", port=redis_pair["src"],
                             decode_responses=True)
    dst = redis_driver.Redis(host="127.0.0.1", port=redis_pair["dst"],
                             decode_responses=True)
    src.flushall()
    dst.flushall()
    for key in KEYS:
        src.set(key, "v")
    dst.set("plain", "v")
    dst.set("stray", "z")

    hop = Hop(name="teeth", engine="redis",
              source=Endpoint(host="127.0.0.1", port=redis_pair["src"],
                              user="", password=""),
              target=Endpoint(host="127.0.0.1", port=redis_pair["dst"],
                              user="", password=""),
              db_map={"0": "0"})
    hop.report_dir = lambda db=None: tmp_path
    eng = RedisEngine(hop)
    assert any(r.status == "diff" for r in eng.check_data("0"))
    written = (tmp_path / "data-db0.missing").read_text().splitlines()
    assert len(written) == 4, written
    assert [json.loads(l) for l in written] == sorted(KEYS[1:]), written

    for action in eng.repair_plan("0", "rows"):
        eng.apply("0", action)
    assert sorted(dst.keys("*")) == sorted(KEYS)
    assert [r.status for r in eng.check_data("0")] == ["ok"]


# ---- sqlite, which needs nothing at all ------------------------------

def test_sqlite_repairs_them_too(tmp_path):
    from migkit.engines.sqlite import SQLiteEngine
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    for path, keys in ((src, KEYS), (dst, KEYS[:1])):
        con = sqlite3.connect(path)
        con.execute("create table t (k text primary key, v text)")
        con.executemany("insert into t values (?,?)", [(k, "v") for k in keys])
        if path == dst:
            con.execute("insert into t values ('stray','z')")
        con.commit()
        con.close()
    report = tmp_path / "report"
    report.mkdir()
    hop = Hop(name="teeth", engine="sqlite",
              source=Endpoint(host=str(src), port=0, user="", password=""),
              target=Endpoint(host=str(dst), port=0, user="", password=""),
              db_map={"main": "main"})
    hop.report_dir = lambda db=None: report
    eng = SQLiteEngine(hop)
    assert eng.check_data("main")[0].status == "diff"
    for action in eng.repair_plan("main", "rows"):
        eng.apply("main", action)
    con = sqlite3.connect(dst)
    got = sorted(r[0] for r in con.execute("select k from t"))
    con.close()
    assert got == sorted(KEYS), got
    assert eng.check_data("main")[0].status == "ok"


# ---- the two engines that do not own their driver --------------------

def test_hetero_carries_such_keys_between_two_engines(pg_pair, mysql_pair,
                                                       tmp_path):
    """A cross-engine hop writes the key on one side and matches it on the
    other, so an encoding that loses a character loses the row."""
    from migkit.engines.hetero import HeteroEngine
    rows = ", ".join("($tag${}$tag$, 'v')".format(k) for k in KEYS)
    got = psql(pg_pair["src"], "drop table if exists hk;"
                               " create table hk (k text primary key,"
                               " v text);"
                               f" insert into hk values {rows};")
    assert got.returncode == 0, got.stderr
    assert _mysql(MY2, "drop table if exists app.hk;"
                       " create table app.hk (k varchar(60) primary key,"
                       " v varchar(60));"
                       " insert into app.hk values ('plain','v'),"
                       " ('stray','z');").returncode == 0

    hop = Hop(name="teeth", engine="hetero",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test",
                              options={"database": "postgres"}),
              target=Endpoint(host="127.0.0.1", port=mysql_pair["dst"],
                              user="root", password="test",
                              options={"database": "app"}),
              options={"source_engine": "postgres",
                       "target_engine": "mysql"},
              db_map={"postgres": "app"})
    hop.report_dir = lambda db=None: tmp_path
    eng = HeteroEngine(hop)
    found = [r for r in eng.check_data("postgres") if r.scope.endswith("hk")]
    assert found[0].status == "diff", found[0].detail

    # as a set: the order in the file is the source's collation, which is
    # the server's business rather than something to pin here
    written = (tmp_path / "data-hk.missing").read_text().splitlines()
    assert {json.loads(l)[0] for l in written} == set(KEYS[1:]), written

    for action in eng.repair_plan("postgres", "rows"):
        if action.scope.endswith("hk"):
            eng.apply("postgres", action)

    src_rows = psql(pg_pair["src"], "select md5(k) from hk order by k;"
                    ).stdout.split()
    dst_rows = _mysql(MY2, "select md5(k) from app.hk order by k").stdout.split()
    assert sorted(src_rows) == sorted(dst_rows), (src_rows, dst_rows)
    after = [r for r in eng.check_data("postgres") if r.scope.endswith("hk")]
    assert after[0].status == "ok", after[0].detail


@pytest.mark.skipif(not __import__("migkit.util", fromlist=["which"]
                                   ).which("reladiff"),
                    reason="reladiff is not where migkit would look for it")
def test_generic_writes_values_holding_them_back_unchanged(pg_pair, tmp_path):
    """reladiff will not key on a string at all (the test below pins that),
    so what carries these characters here is the values - and those go back
    through the borrowed writer as SQL literals."""
    from migkit.engines.generic import GenericEngine
    values = ", ".join("({}, $tag${}$tag$)".format(i, v)
                       for i, v in enumerate(KEYS + ["%s and _ and $x$"], 1))
    for port in (pg_pair["src"], pg_pair["dst"]):
        got = psql(port, "drop table if exists nv;"
                         " create table nv (id bigint primary key, v text);"
                         f" insert into nv values {values};")
        assert got.returncode == 0, got.stderr
    psql(pg_pair["dst"], "update nv set v='WRONG' where id in (2,4);"
                         " delete from nv where id=3;"
                         " insert into nv values (99,'extra');")

    def url(port):
        return f"postgresql://postgres:test@127.0.0.1:{port}/postgres"
    hop = Hop(name="teeth", engine="generic",
              source=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["src"])}),
              target=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["dst"])}),
              options={"tables": ["nv"], "key": "id"})
    hop.report_dir = lambda db=None: tmp_path
    eng = GenericEngine(hop)
    assert eng.check_data("-")[0].status == "diff"
    for action in eng.repair_plan("-", "rows"):
        eng.apply("-", action)

    def digests(port):
        return psql(port, "select md5(v) from nv order by id;").stdout.split()
    assert digests(pg_pair["src"]) == digests(pg_pair["dst"])
    assert eng.check_data("-")[0].status == "ok"


@pytest.mark.skipif(not __import__("migkit.util", fromlist=["which"]
                                   ).which("reladiff"),
                    reason="reladiff is not where migkit would look for it")
def test_generic_says_so_when_one_key_makes_reladiff_refuse_the_table(
        pg_pair, tmp_path):
    """Measured on reladiff 0.6.0, and it is the values rather than the
    declared type that decide it: the same `varchar(60)` key column compares
    fine while its values are ordinary, and the moment one row's key holds a
    tab the whole table comes back `Cannot use a column of type Text() as a
    key`. reladiff samples the column and will not bisect on one it reads as
    free text.

    That is a real limit of the tool this engine borrows, and what matters
    here is that the check calls it an error rather than a table that
    matched, and leaves no drilldown for `sync` to act on.
    """
    from migkit.engines.generic import GenericEngine

    def url(port):
        return f"postgresql://postgres:test@127.0.0.1:{port}/postgres"
    hop = Hop(name="teeth", engine="generic",
              source=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["src"])}),
              target=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["dst"])}),
              options={"tables": ["sk"], "key": "k"})
    hop.report_dir = lambda db=None: tmp_path
    eng = GenericEngine(hop)

    for port in (pg_pair["src"], pg_pair["dst"]):
        got = psql(port, "drop table if exists sk;"
                         " create table sk (k varchar(60) primary key,"
                         " v text);"
                         " insert into sk values ('plain','1'),"
                         " ('also-plain','2');")
        assert got.returncode == 0, got.stderr
    ordinary = eng.check_data("-")
    assert [r.status for r in ordinary] == ["ok"], [r.detail
                                                    for r in ordinary]

    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "insert into sk values (E'has\\ttab','3');")
    refused = eng.check_data("-")
    assert [r.status for r in refused] == ["error"], [r.detail
                                                      for r in refused]
    assert "as a key" in refused[0].detail, refused[0].detail
    assert not list(tmp_path.glob("data-sk.*")), list(tmp_path.iterdir())


def test_no_engine_hides_the_base_row_repair_behind_its_own(tmp_path):
    """`_apply_rows` on the base carries rows between two engines. Two
    engines had a method of the same name taking different arguments, which
    is a trap for whoever calls the wrong one: the mysql one is
    `_apply_rows_native`, the generic one `_apply_rows_borrowed`."""
    import inspect

    from migkit.engines import NAMES, engine_named
    from migkit.engines.base import Engine
    expected = inspect.signature(Engine._apply_rows)
    for name in NAMES:
        cls = type(engine_named(name, _stub_hop(tmp_path)))
        own = cls.__dict__.get("_apply_rows")
        if own is None:
            continue
        assert inspect.signature(own) == expected, (
            f"{name} defines its own _apply_rows with a different shape")


def _stub_hop(tmp_path):
    hop = Hop(name="sig", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              options={"source_engine": "postgres",
                       "target_engine": "mysql"},
              databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    return hop

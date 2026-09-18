"""The same logical row, rendered by two different engines, coming out equal.

This is the measurement the module is built on rather than a restatement of
it: the values are inserted into a real MySQL and a real PostgreSQL, each side
renders them with its own generated expression, and the two texts are compared
byte for byte.

The last two tests are the ones that matter most. They pin the traps that a
rendering chosen for tidiness rather than measured would have walked into -
four different doubles collapsing to one string on MySQL, and PostgreSQL's NaN
sailing past the standard NaN test.
"""
import socket
import subprocess
import time

import pytest

from migkit import canon as c


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

MY, PG = "migkit-test-canon-my", "migkit-test-canon-pg"
MY_PORT, PG_PORT = 13387, 15487

# One row per column below, the same logical value on both sides.
COLS = [
    # name,        mysql type,       postgres type,  mysql literal, pg literal
    ("c_int",      "bigint",         "bigint",
     "9223372036854775807", "9223372036854775807"),
    ("c_dec",      "decimal(12,4)",  "numeric(12,4)", "-0.05", "-0.05"),
    ("c_float",    "double",         "double precision", "1e20", "1e20"),
    ("c_small",    "double",         "double precision", "0.000001", "0.000001"),
    ("c_txt",      "varchar(50)",    "varchar(50)",   "'café'", "'café'"),
    ("c_empty",    "varchar(50)",    "varchar(50)",   "''", "''"),
    ("c_bool",     "tinyint(1)",     "boolean",       "1", "true"),
    ("c_date",     "date",           "date",          "'2026-09-18'",
     "'2026-09-18'"),
    ("c_ts",       "datetime(6)",    "timestamp(6)",
     "'2026-01-01 00:00:00'", "'2026-01-01 00:00:00'"),
    ("c_time",     "time(6)",        "time(6)",       "'00:00:00'",
     "'00:00:00'"),
    ("c_bin",      "varbinary(20)",  "bytea",         "x'00FF41'",
     "'\\x00FF41'"),
    ("c_json",     "json",           "jsonb",         "'{\"b\":1,\"a\":2}'",
     "'{\"b\":1,\"a\":2}'"),
]


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _my(sql, db="cx"):
    r = subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                        "-N", "-B", "-D", db, "-e", sql],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.rstrip("\n")


def _pg(sql):
    r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", PG, "psql",
                        "-U", "postgres", "-d", "cx", "-At", "-F", "\t",
                        "-v", "ON_ERROR_STOP=1", "-c", sql],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.rstrip("\n")


@pytest.fixture(scope="module")
def pair():
    for n in (MY, PG):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8"], check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PG_PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    assert _wait(MY_PORT) and _wait(PG_PORT)
    for _ in range(60):
        r = subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                            "-e", "create database if not exists cx"],
                           capture_output=True)
        if r.returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("mysql never accepted a connection")
    for _ in range(40):
        r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", PG,
                            "psql", "-U", "postgres", "-c",
                            "create database cx"], capture_output=True)
        if r.returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("postgres never accepted a connection")

    mycols = ", ".join(f"`{n}` {t}" for n, t, _, _, _ in COLS)
    pgcols = ", ".join(f'"{n}" {t}' for n, _, t, _, _ in COLS)
    _my(f"create table v (id int, {mycols})")
    _pg(f'create table v (id int, {pgcols})')
    _my("insert into v values (1, "
        + ", ".join(lit for _, _, _, lit, _ in COLS) + ")")
    _pg("insert into v values (1, "
        + ", ".join(lit for _, _, _, _, lit in COLS) + ")")
    # an empty or half-written seed would let every comparison below pass
    assert _my("select count(*) from v") == "1"
    assert _pg("select count(*) from v") == "1"
    yield
    for n in (MY, PG):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def _declared():
    """(name -> declared type) as each server reports it, not as we wrote it."""
    my = dict(l.split("\t") for l in _my(
        "select column_name, column_type from information_schema.columns"
        " where table_schema='cx' and table_name='v'").splitlines())
    pg = dict(l.split("\t") for l in _pg(
        "select column_name, data_type from information_schema.columns"
        " where table_name='v'").splitlines())
    return my, pg


def _rendered(pair_unused=None):
    my_t, pg_t = _declared()
    out = {}
    for name, _, _, _, _ in COLS:
        mcls, mwhy = c.comparable("mysql", my_t[name])
        pcls, pwhy = c.comparable("postgres", pg_t[name])
        assert mcls and pcls, (name, my_t[name], pg_t[name], mwhy, pwhy)
        a = _my(f"select {c.expr('mysql', name, mcls)} from v")
        b = _pg(f"select {c.expr('postgres', name, pcls)} from v")
        out[name] = (a, b, mcls, pcls)
    return out


def test_every_column_renders_to_the_same_text_on_both_engines(pair):
    bad = {n: v for n, v in _rendered().items() if v[0] != v[1]}
    assert not bad, bad


def test_the_types_the_two_engines_declare_are_not_the_same_names(pair):
    """The agreement above is not because the columns are identical: MySQL
    reports `tinyint(1)` where PostgreSQL reports `boolean`, and `datetime`
    where PostgreSQL reports `timestamp without time zone`. The rendering is
    what makes them meet, so if this stops being true the test above has
    stopped proving anything."""
    my_t, pg_t = _declared()
    assert my_t["c_bool"].startswith("tinyint")
    assert pg_t["c_bool"] == "boolean"
    assert my_t["c_ts"].startswith("datetime")
    assert pg_t["c_ts"] == "timestamp without time zone"


def test_a_real_difference_still_reads_as_a_difference(pair):
    """Without this the agreement above could be produced by a rendering that
    throws the value away."""
    _pg("update v set c_txt = 'cafe'")
    try:
        got = _rendered()
        assert got["c_txt"][0] != got["c_txt"][1], got["c_txt"]
        assert got["c_int"][0] == got["c_int"][1]
    finally:
        _pg("update v set c_txt = 'café'")


def test_two_different_large_doubles_do_not_collapse_to_one_string(pair):
    """The trap the banded rule exists for. `cast(d as decimal(65,20))`
    saturates on MySQL: 1e45, 1e46, 1e300 and 1.797e308 all come back as the
    same maximum, with no error. A rendering built on that cast would make
    every double above 1e45 equal to every other one."""
    _my("create table big (d double)")
    try:
        _my("insert into big values (1e45),(1e46),(1e300),"
            "(1.7976931348623157e308)")
        naive = _my("select distinct cast(d as decimal(65,20)) from big")
        assert len(naive.splitlines()) == 1, naive

        rendered = _my(f"select distinct {c.expr('mysql', 'd', 'float')}"
                       " from big")
        assert len(rendered.splitlines()) == 4, rendered
    finally:
        _my("drop table big")


def test_the_values_mysql_cannot_hold_are_marked_not_rendered(pair):
    """PostgreSQL takes Infinity and NaN in a double and MySQL refuses them,
    so there is no text that makes the two comparable. Both come back as the
    marker - including NaN, which slips past the usual `x <> x` test because
    PostgreSQL defines NaN as equal to itself."""
    _pg("create table ext (d double precision)")
    try:
        _pg("insert into ext values ('Infinity'), ('-Infinity'), ('NaN'),"
            " (1e300)")
        got = _pg(f"select {c.expr('postgres', 'd', 'float')} from ext"
                  " order by 1").splitlines()
        assert got.count(c.UNCOMPARABLE) == 3, got
        assert "1e300" in got, got
        assert "NaN" not in got, got

        # the other half of "cannot hold": MySQL refuses the value outright
        # rather than storing something near it, so there is nothing on that
        # side for these rows to be compared against
        _my("create table ext (d double)")
        try:
            for lit, code in (("'NaN'", "1265"), ("1e400", "1367")):
                r = subprocess.run(
                    ["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                     "-D", "cx", "-e", f"insert into ext values ({lit})"],
                    capture_output=True, text=True)
                assert r.returncode != 0, (lit, r.stdout)
                assert code in r.stderr, (lit, r.stderr)
        finally:
            _my("drop table ext")
    finally:
        _pg("drop table ext")

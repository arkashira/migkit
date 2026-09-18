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
    # `--default-character-set` is not decoration. Without it the client
    # negotiates latin1, and `café` sent as UTF-8 bytes is stored as the five
    # characters `cafÃ©`. Reading it back through the same connection undoes
    # the damage, so a column-by-column comparison still passes while the two
    # sides hold different data - which is how the row-length prefix caught a
    # seed that every earlier assertion had been happy with.
    r = subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                        "--default-character-set=utf8mb4",
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


def _classes():
    """[(name, class)] per engine, from the types each server declares."""
    my_t, pg_t = _declared()
    my, pg = [], []
    for name, _, _, _, _ in COLS:
        mcls, mwhy = c.comparable("mysql", my_t[name])
        pcls, pwhy = c.comparable("postgres", pg_t[name])
        assert mcls and pcls, (name, mwhy, pwhy)
        my.append((name, mcls))
        pg.append((name, pcls))
    return my, pg


def test_the_whole_row_encodes_to_the_same_text_on_both_engines(pair):
    """One string per row, length-prefixed so a separator inside a value
    cannot shift a field boundary, built from the canonical rendering of each
    column rather than the column itself."""
    my_cols, pg_cols = _classes()
    a = _my(f"select {c.row_expr('mysql', my_cols)} from v")
    b = _pg(f"select {c.row_expr('postgres', pg_cols)} from v")
    assert a == b, f"\nmysql={a!r}\npg   ={b!r}"
    assert a.startswith("19:9223372036854775807|"), a


def test_the_digest_agrees_across_engines(pair):
    """The number that actually crosses the network. Neither side sends a
    row: each folds its own table down to one value, and the two values are
    compared."""
    my_cols, pg_cols = _classes()
    a = _my(f"select count(*), {c.digest_expr('mysql', c.row_expr('mysql', my_cols))} from v")
    b = _pg(f"select count(*), {c.digest_expr('postgres', c.row_expr('postgres', pg_cols))} from v")
    assert a == b, f"\nmysql={a!r}\npg   ={b!r}"
    assert a.split("\t")[1] != "0", a


def test_a_one_character_change_moves_the_digest(pair):
    """Without this the agreement above could come from a digest that is the
    same number for every table."""
    my_cols, pg_cols = _classes()
    def digest_pg():
        return _pg(f"select {c.digest_expr('postgres', c.row_expr('postgres', pg_cols))} from v")
    before = digest_pg()
    _pg("update v set c_txt = 'cafe'")
    try:
        assert digest_pg() != before
    finally:
        _pg("update v set c_txt = 'café'")
    assert digest_pg() == before


def test_the_digest_does_not_degrade_to_floating_point_on_mysql(pair):
    """MySQL's `conv()` returns a string and summing it coerces to DOUBLE.
    Measured, the same three rows came back as `7.50945936868949e17` from
    MySQL against `750945936868948924` from PostgreSQL - a difference the
    aggregate invented. The cast to decimal is what stops it, so a digest
    that has grown an exponent is this bug coming back."""
    my_cols, _ = _classes()
    got = _my(f"select {c.digest_expr('mysql', c.row_expr('mysql', my_cols))} from v")
    assert "e" not in got.lower() and "." not in got, got

    naive = _my("select sum(conv(substr(md5(c_txt),1,15),16,10)) from v")
    exact = _my("select sum(cast(conv(substr(md5(c_txt),1,15),16,10)"
                " as decimal(65,0))) from v")
    assert naive != exact, (naive, exact)


LONG_EXPANSION = {4: "1.0/3", 7: "1.0/7", 3: "2.0/3", 11: "1.0/11"}


def test_a_float_with_a_long_decimal_expansion_still_agrees(pair):
    """The values that caught `float8::numeric`.

    PostgreSQL's cast from float8 to numeric keeps about fifteen significant
    digits; its own `::text` keeps seventeen, and MySQL's
    `cast(d as decimal)` goes through the shortest round-trip decimal. So the
    canonical rendering had to route through the text form, and every float
    with a short decimal expansion - 0.1, 1e20, 123456.789 - agreed either
    way, which is why the first set of measurements missed it.

    The literals are written as float division on both sides on purpose:
    MySQL evaluates `1.0/7` with DECIMAL arithmetic and stores 0.142857142,
    so a seed written the obvious way puts different numbers on the two sides
    and the test fails for a reason that has nothing to do with rendering.
    """
    assert _pg("create table longf (id int, d double precision)"
               ) is not None
    _my("create table longf (id int, d double)")
    try:
        for id_, expr in LONG_EXPANSION.items():
            _pg(f"insert into longf values ({id_}, {expr})")
            num, _, den = expr.partition("/")
            _my(f"insert into longf values ({id_},"
                f" cast({num} as double)/{den})")
        assert _pg("select count(*) from longf") == str(len(LONG_EXPANSION))
        assert _my("select count(*) from longf") == str(len(LONG_EXPANSION))

        # the raw values must already be equal, or this measures the seed
        raw_pg = _pg("select d::text from longf order by id")
        raw_my = _my("select cast(d as char) from longf order by id")
        assert raw_pg == raw_my, (raw_pg, raw_my)
        assert any(len(l) > 17 for l in raw_pg.splitlines()), raw_pg

        cols = [("d", "float")]
        a = _my("select " + c.digest_expr("mysql", c.row_expr("mysql", cols))
                + " from longf")
        b = _pg("select "
                + c.digest_expr("postgres", c.row_expr("postgres", cols))
                + " from longf")
        assert a == b, f"\nmysql={a!r}\npg   ={b!r}"
    finally:
        _pg("drop table longf")
        _my("drop table longf")

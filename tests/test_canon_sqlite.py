"""A third engine joining the comparison, with neither of the two primitives.

SQLite has no `md5`, and no number wide enough to fold one into: `sum()` over
60-bit values raises `integer overflow` past 2**63, and `total()` answers with
a real that has already dropped the low digits. Both are supplied by migkit on
the connection, which is only honest because SQLite runs inside this process -
there is no server to have pushed the work into.

What has to hold is that the text and the total come out identical to what
MySQL and PostgreSQL produce for the same rows. That is measured here against
all three at once.
"""
import socket
import sqlite3
import subprocess
import time

import pytest

from migkit import canon

PG, MY = "migkit-test-sq-pg", "migkit-test-sq-my"
PG_PORT, MY_PORT = 15479, 13379

# Only the classes SQLite maps: its declared type is a hint, so `decimal`,
# the date types and `json` are deliberately not claimed yet.
VALUES = [
    (1, "café", 1e20, b"\x00\xffA"),
    (2, "", 0.000001, b""),
    (3, None, None, None),
    (4, "x|y", -0.05, b"\x7f"),
    (5, "N:5", 0.0, b"\x00"),
]


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


def _hop(engine, host, port, user, password, db):
    from migkit.config import Endpoint, Hop
    ep = Endpoint(host=host, port=port, user=user, password=password)
    return Hop(name="s", engine=engine, source=ep, target=ep,
               db_map={db: db})


@pytest.fixture(scope="module")
def sqlite_engine(tmp_path_factory):
    path = tmp_path_factory.mktemp("sq") / "a.db"
    conn = sqlite3.connect(path)
    conn.execute("create table v (id integer primary key, name text,"
                 " ratio real, blob_col blob)")
    conn.executemany("insert into v values (?,?,?,?)", VALUES)
    conn.commit()
    assert conn.execute("select count(*) from v").fetchone()[0] == len(VALUES)
    conn.close()
    from migkit.engines.sqlite import SQLiteEngine
    return SQLiteEngine(_hop("sqlite", str(path), 0, "", "", "main"))


@pytest.fixture(scope="module")
def servers():
    for n in (PG, MY):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PG_PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8"], check=True, capture_output=True)
    assert _wait(PG_PORT) and _wait(MY_PORT)

    def pg(sql, db="cx"):
        return subprocess.run(
            ["docker", "exec", "-e", "PGPASSWORD=test", PG, "psql", "-U",
             "postgres", "-d", db, "-At", "-v", "ON_ERROR_STOP=1", "-c", sql],
            capture_output=True, text=True)

    def my(sql, db=None):
        cmd = ["docker", "exec", MY, "mysql", "-uroot", "-ptest",
               "--default-character-set=utf8mb4", "-N", "-B"]
        if db:
            cmd += ["-D", db]
        return subprocess.run(cmd + ["-e", sql], capture_output=True,
                              text=True)
    for _ in range(40):
        if pg("select 1", "postgres").returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("postgres never answered")
    for _ in range(60):
        if my("select 1").returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("mysql never answered")
    assert pg("create database cx", "postgres").returncode == 0
    assert my("create database cx").returncode == 0
    assert pg("create table v (id bigint primary key, name text,"
              " ratio double precision, blob_col bytea)").returncode == 0
    assert my("create table v (id bigint primary key, name text,"
              " ratio double, blob_col blob)", "cx").returncode == 0

    def lit_pg(v):
        if v is None:
            return "null"
        if isinstance(v, bytes):
            return "'\\x" + v.hex() + "'"
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"
        return repr(v)

    def lit_my(v):
        if v is None:
            return "null"
        if isinstance(v, bytes):
            return "x'" + v.hex() + "'" if v else "x''"
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"
        return repr(v)
    for row in VALUES:
        assert pg("insert into v values ("
                  + ",".join(lit_pg(v) for v in row) + ")").returncode == 0
        assert my("insert into v values ("
                  + ",".join(lit_my(v) for v in row) + ")",
                  "cx").returncode == 0
    assert pg("select count(*) from v").stdout.strip() == str(len(VALUES))
    assert my("select count(*) from v", "cx").stdout.strip() == str(len(VALUES))

    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    pg_eng = PostgresEngine(_hop("postgres", "127.0.0.1", PG_PORT,
                                 "postgres", "test", "cx"))
    my_eng = MySQLEngine(_hop("mysql", "127.0.0.1", MY_PORT,
                              "root", "test", "cx"))
    yield pg_eng, my_eng, pg, my
    for n in (PG, MY):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def _cols(eng, side, db, table):
    out = []
    for name, declared in eng.neutral_columns(side, db, table):
        cls, why = canon.comparable(eng.CANON_ENGINE, declared)
        assert cls, (eng.CANON_ENGINE, name, declared, why)
        out.append((name, cls))
    return sorted(out)


def test_sqlite_maps_only_the_types_it_can_answer_for(sqlite_engine):
    """A SQLite column declared DECIMAL(12,4) stores a float, and one
    declared INTEGER holds text if something writes text to it. Claiming a
    rendering for those would be claiming to know what is in them."""
    assert canon.type_class("sqlite", "integer") == "integer"
    assert canon.type_class("sqlite", "real") == "float"
    assert canon.type_class("sqlite", "blob") == "bytes"
    for unmapped in ("decimal(12,4)", "json", "numeric"):
        cls, why = canon.comparable("sqlite", unmapped)
        assert cls is None, (unmapped, cls)
        assert "no canonical rendering" in why


def test_the_date_names_are_compared_as_the_text_they_hold(sqlite_engine,
                                                           tmp_path):
    """`datetime` is mapped now, and to `text` rather than to a date class,
    because that is what SQLite stores. The affinity rules are what make it
    work, and they are measured here rather than taken from the manual: these
    names carry NUMERIC affinity, so a timestamp stays text (it cannot be
    turned into a number) while something that looks like a number does not.
    """
    import sqlite3
    for declared in ("date", "datetime", "timestamp", "time"):
        assert canon.type_class("sqlite", declared) == "text", declared

    conn = sqlite3.connect(tmp_path / "affinity.db")
    conn.execute("create table t (a datetime, b numeric)")
    conn.execute("insert into t values (?, ?)",
                 ("2024-01-02 03:04:05.000006", "1.50"))
    conn.execute("insert into t values (?, ?)", ("1704164645", "2"))
    got = conn.execute("select typeof(a), a, typeof(b), b from t").fetchall()
    conn.close()
    assert got[0][:2] == ("text", "2024-01-02 03:04:05.000006"), got
    assert got[1][:2] == ("integer", 1704164645), got
    # and the other half: why `numeric` is still not mapped
    assert got[0][2:] == ("real", 1.5), got


def test_the_three_engines_agree_on_the_same_rows(sqlite_engine, servers):
    pg_eng, my_eng, _, _ = servers
    sq = sqlite_engine.neutral_digest(
        "src", "main", "v", _cols(sqlite_engine, "src", "main", "v"))
    pg = pg_eng.neutral_digest("src", "cx", "public.v",
                               _cols(pg_eng, "src", "cx", "public.v"))
    my = my_eng.neutral_digest("src", "cx", "v",
                               _cols(my_eng, "src", "cx", "v"))
    assert sq == pg == my, {"sqlite": sq, "postgres": pg, "mysql": my}
    assert sq[0] == len(VALUES)


def test_the_row_text_is_byte_identical_across_the_three(sqlite_engine,
                                                         servers):
    """Stronger than the digest agreeing, and far easier to read when it
    stops. The values include `x|y` and `N:5`, which are the separator and
    the NULL marker - the length prefix is what stops them being read as
    structure."""
    pg_eng, my_eng, pg, my = servers
    sq_cols = _cols(sqlite_engine, "src", "main", "v")
    got_sq = sqlite_engine._q(
        "src", f"select {canon.row_expr('sqlite', sq_cols)} from v"
               " order by id")
    got_pg = pg("select " + canon.row_expr(
        "postgres", _cols(pg_eng, "src", "cx", "public.v"))
        + " from v order by id").stdout.splitlines()
    got_my = my("select " + canon.row_expr(
        "mysql", _cols(my_eng, "src", "cx", "v"))
        + " from v order by id", "cx").stdout.splitlines()
    lines_sq = [r[0] for r in got_sq]
    assert len(lines_sq) == len(VALUES)
    assert lines_sq == got_pg == got_my, {
        "sqlite": lines_sq, "postgres": got_pg, "mysql": got_my}
    assert any("|3:x|y|" in line for line in lines_sq), lines_sq


def test_a_change_in_sqlite_alone_moves_only_its_digest(sqlite_engine,
                                                        servers):
    """Otherwise the agreement above could come from three digests that all
    ignore the data."""
    pg_eng, _, _, _ = servers
    cols = _cols(sqlite_engine, "src", "main", "v")
    before = sqlite_engine.neutral_digest("src", "main", "v", cols)
    conn = sqlite3.connect(sqlite_engine._path("src"))
    conn.execute("update v set name = 'cafe' where id = 1")
    conn.commit()
    conn.close()
    try:
        after = sqlite_engine.neutral_digest("src", "main", "v", cols)
        assert after != before
        assert after[0] == before[0]
        pg = pg_eng.neutral_digest("src", "cx", "public.v",
                                   _cols(pg_eng, "src", "cx", "public.v"))
        assert pg == before and pg != after
    finally:
        conn = sqlite3.connect(sqlite_engine._path("src"))
        conn.execute("update v set name = 'café' where id = 1")
        conn.commit()
        conn.close()


def test_neither_sqlite_aggregate_could_have_carried_the_digest(
        sqlite_engine):
    """Both of SQLite's own ways to add these numbers up, against the thing
    migkit does instead. `sum()` refuses; `total()` answers, and the answer
    is already wrong in the low digits - which is the one that would have
    shipped quietly."""
    big = (1 << 60) - 1
    conn = sqlite3.connect(sqlite_engine._path("src"))
    conn.execute("create table wide (a integer)")
    conn.executemany("insert into wide values (?)", [(big,)] * 20)
    conn.commit()
    try:
        with pytest.raises(sqlite3.OperationalError) as e:
            conn.execute("select sum(a) from wide").fetchone()
        assert "overflow" in str(e.value)

        approx, kind = conn.execute(
            "select total(a), typeof(total(a)) from wide").fetchone()
        assert kind == "real"
        assert int(approx) != big * 20, (approx, big * 20)
        conn.close()

        n, digest = sqlite_engine.neutral_digest("src", "main", "wide",
                                                 [("a", "integer")])
        assert n == 20
        assert digest.isdigit(), digest
        assert int(digest) > 2 ** 63, digest
    finally:
        conn = sqlite3.connect(sqlite_engine._path("src"))
        conn.execute("drop table wide")
        conn.commit()
        conn.close()

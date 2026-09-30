"""The table copier's two ways on the MySQL table ladder, timed on the
shapes the ladder's other rungs were measured on (`MY_BENCH`), to seed the
one it had no number for (`PASS_BENCH`).

    python tools/with_docker_lock.py .venv/bin/python bench/table_ladder.py

Two MySQL 8.4 servers in docker (`local_infile` on), 127.0.0.1:16320 and
16321; `appdb.narrow` 500,000 rows of int key, int, varchar(40), datetime
and decimal with two secondary indexes, `appdb.wide` 500,000 rows of
bigint key, a 320-byte varbinary and a text with a prefix index,
`appdb.tiny` one row - `MY_BENCH`'s tables. Each copied into an emptied
target table by `move_table` with the ladder held to one way - the rows
passed server to server, and the table copier's rows - four workers, three
runs each, alternated; the median is printed with the table's bytes, and
each copy is held equal (a sum of row hashes on both sides). Containers
`migkit-test-ladder-*`, removed after.
"""
import contextlib
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
os.environ.setdefault("MIGKIT_REPORTS", tempfile.mkdtemp(prefix="bench-"))

from migkit import ranges  # noqa: E402
from migkit.cli import _Checkpoint  # noqa: E402
from migkit.config import Endpoint, Hop  # noqa: E402

SRC, DST = 16320, 16321
NAMES = {SRC: "migkit-test-ladder-src", DST: "migkit-test-ladder-dst"}
RUNS = int(os.environ.get("RUNS", "3"))
ROWS = int(os.environ.get("ROWS", "500000"))

DDL = {
    "narrow": "create table narrow (id int primary key, a int, b varchar(40),"
              " d datetime, c decimal(12,2), key ka (a), key kb (b))",
    "wide": "create table wide (id bigint primary key, raw varbinary(320),"
            " t text, key kt (t(16)))",
    "tiny": "create table tiny (id int primary key, v int)",
}
FILL = {
    "narrow": "insert into narrow with recursive g(n) as (select 1 union all"
              " select n + 1 from g where n < {rows}) select n, n % 1000,"
              " concat('row-', n, repeat('x', 24)), timestamp('2026-01-01')"
              " + interval n second, n / 7 from g",
    "wide": "insert into wide with recursive g(n) as (select 1 union all"
            " select n + 1 from g where n < {rows}) select n,"
            " unhex(repeat(md5(n), 20)), concat('text-', n, repeat('t', 48))"
            " from g",
    "tiny": "insert into tiny values (1, 1)",
}
SAME = {
    "narrow": "concat_ws('|', id, a, b, d, c)",
    "wide": "concat_ws('|', id, hex(raw), t)",
    "tiny": "concat_ws('|', id, v)",
}


def say(*a):
    print(" ".join(str(x) for x in a), flush=True)


def my(port, sql, db="appdb", before=()):
    import pymysql
    c = pymysql.connect(host="127.0.0.1", port=port, user="root",
                        password="test", database=db, autocommit=True,
                        read_timeout=7200)
    try:
        with c.cursor() as cur:
            for s in before:
                cur.execute(s)
            cur.execute(sql)
            return cur.fetchall()
    finally:
        c.close()


def up():
    for p, n in NAMES.items():
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
        got = subprocess.run(
            ["docker", "run", "-d", "--name", n, "-e",
             "MYSQL_ROOT_PASSWORD=test", "-e", "MYSQL_DATABASE=appdb", "-p",
             f"127.0.0.1:{p}:3306", "mysql:8.4", "--local-infile=1",
             "--innodb-buffer-pool-size=512M", "--max-allowed-packet=256M"],
            capture_output=True, text=True)
        assert got.returncode == 0, got.stderr
    for p in NAMES:
        for _ in range(180):
            try:
                my(p, "select 1")
                break
            except Exception:  # noqa: BLE001 - not up yet
                time.sleep(1)


def seed():
    sizes = {}
    for t, ddl in DDL.items():
        my(SRC, ddl)
        my(SRC, FILL[t].format(rows=ROWS),
           before=("set session cte_max_recursion_depth = 10000000",))
        my(SRC, f"analyze table {t}")
        my(DST, ddl)
        sizes[t] = int(my(SRC, "select data_length from"
                               " information_schema.tables where"
                               " table_schema = 'appdb' and table_name ="
                               f" '{t}'")[0][0])
        say(f"seed {t}: {sizes[t] / 2**20:,.1f} MB")
    return sizes


def same(t):
    q = (f"select count(*), sum(cast(conv(substring(md5({SAME[t]}), 1, 15),"
         f" 16, 10) as unsigned)) from {t}")
    return my(SRC, q) == my(DST, q)


def engine():
    from migkit.engines.mysql import MySQLEngine

    def ep(p):
        return Endpoint(host="127.0.0.1", port=p, user="root",
                        password="test")
    return MySQLEngine(Hop(name="bench-ladder", engine="mysql",
                           source=ep(SRC), target=ep(DST),
                           databases=["appdb"], workers=4))


@contextlib.contextmanager
def only(name):
    from migkit.engines.mysql import MySQLEngine
    before = MySQLEngine.TABLE_WAYS
    MySQLEngine.TABLE_WAYS = tuple(w for w in before if w.name == name)
    try:
        yield
    finally:
        MySQLEngine.TABLE_WAYS = before


def once(way, t):
    my(DST, f"truncate table {t}")
    tmp = Path(tempfile.mkdtemp(prefix="bench-"))
    said = []
    before = ranges.active
    ranges.active = ranges.Slots(4)
    try:
        with only(way):
            t0 = time.monotonic()
            engine().move_table("appdb", "", t, 500_000,
                                _Checkpoint(tmp / "move.json"), said.append)
            took = time.monotonic() - t0
    finally:
        ranges.active = before
        shutil.rmtree(tmp, ignore_errors=True)
    assert same(t), (way, t, said[-5:])
    return took, [m for m in said if "as rows" in m or "could not" in m]


def main():
    up()
    try:
        sizes = seed()
        times = {}
        for n in range(RUNS):
            for t in DDL:
                for way in ("pass-through", "rows"):
                    took, odd = once(way, t)
                    times.setdefault((way, t), []).append(took)
                    say(f"  run {n + 1} {way:<13} {t:<7} {took:7.2f}s"
                        + (f"  {odd[:2]}" if odd else ""))
        say("median seconds, and bytes a second over the time less the"
            " one-row table's:")
        for way in ("pass-through", "rows"):
            med = {t: statistics.median(times[(way, t)]) for t in DDL}
            rate = {t: sizes[t] / (med[t] - med["tiny"])
                    for t in ("narrow", "wide")}
            say(f"  {way:<13} narrow {med['narrow']:6.2f}s"
                f" wide {med['wide']:6.2f}s tiny {med['tiny']:5.2f}s -"
                f" narrow {rate['narrow'] / 1e6:6.1f} MB/s,"
                f" wide {rate['wide'] / 1e6:6.1f} MB/s")
    finally:
        for n in NAMES.values():
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


if __name__ == "__main__":
    main()

"""How fast a cross-engine table goes each way (`migkit.crossmove`,
backlog R20.13 and R20.14), on the same data, through migkit's own move:

    python tools/with_docker_lock.py .venv/bin/python bench/cross_rates.py 1000000
    python tools/with_docker_lock.py .venv/bin/python bench/cross_rates.py 5000000 json
    python tools/with_docker_lock.py .venv/bin/python bench/cross_rates.py 5000000 json compiled

A MySQL 8.4 and a PostgreSQL 16 in containers, a table shaped as
`bench/seed.py`'s rows (a key, five texts - accents, quotes, a newline, a
tab and a backslash in one of them, a tenth of them NULL or empty - a
decimal, two integers, a flag, a datetime, a JSON document and 16 bytes),
the target built by migkit. Each way named with `MIGKIT_TABLE_COPY`, four
workers, the table's ranges side by side as a move runs them, each range
proved on both servers before it counts: MySQL into PostgreSQL by the
table copier, the compiled mover and the source rendering the target's
load format; PostgreSQL into MySQL by the table copier and the source
rendering MySQL's load; and MySQL into PostgreSQL by the one-pass load
program the move drives for a whole database. Every result is then
checked (`check`); a server-to-server way's time is also split into its
copy and its proof, the whole table as one stream.

The words after the rows name the ways to time (`WAYS`) and the shapes,
every one of each where none is named. Two shapes: `json`, the table as
it is, and `plain`, the same without its JSON
document - a JSON column is hashed in this process for every proof of a
range (`canon.FOLDED_HERE`), every other class by the servers, so the two
show what the proof costs beside the copy.

Containers `migkit-test-xe-*` on 127.0.0.1:16190-16191, removed after.
Numbers print as JSON; nothing is written anywhere else.
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

DB = "cx"
MY, MY_PORT = "migkit-test-xe-my", 16190
PG, PG_PORT = "migkit-test-xe-pg", 16191
TABLE = """
create table bench (
  id bigint primary key, name varchar(100) not null,
  email varchar(100) not null, city varchar(60), note text,
  tag varchar(30), amount decimal(14,4), qty int, ok tinyint,
  created_at datetime(6), payload json, raw varbinary(64)
) default charset utf8mb4
"""


def _sh(argv, **kw):
    return subprocess.run(argv, capture_output=True, text=True, **kw)


def _my(sql, db=DB):
    got = _sh(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
               "--default-character-set=utf8mb4", "-N", "-B"]
              + (["-D", db] if db else []) + ["-e", sql])
    if got.returncode:
        raise SystemExit(got.stderr)
    return got.stdout


def _pg(sql, db=DB):
    got = _sh(["docker", "exec", "-e", "PGPASSWORD=test", PG, "psql", "-U",
               "postgres", "-d", db, "-At", "-v", "ON_ERROR_STOP=1", "-c",
               sql])
    if got.returncode:
        raise SystemExit(got.stderr)
    return got.stdout


def _wait(port, ask, timeout=240):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                try:
                    ask()
                    return
                except SystemExit:
                    pass
        time.sleep(2)
    raise SystemExit(f"nothing answered on {port}")


def _down():
    for n in (MY, PG):
        _sh(["docker", "rm", "-f", "-v", n])


def _up():
    _down()
    _sh(["docker", "run", "-d", "--name", PG, "-e", "POSTGRES_PASSWORD=test",
         "-p", f"127.0.0.1:{PG_PORT}:5432", "postgres:16", "-c",
         "shared_buffers=256MB", "-c", "max_wal_size=4GB"])
    # the one-pass load program signs in with MySQL's older password
    # plugin only; the load from the client, which a server of the
    # operator's may have off, is on here
    _sh(["docker", "run", "-d", "--name", MY, "-e",
         "MYSQL_ROOT_PASSWORD=test", "-p", f"127.0.0.1:{MY_PORT}:3306",
         "mysql:8.4", "--innodb-buffer-pool-size=768M",
         "--mysql-native-password=ON", "--local-infile=1"])
    _wait(PG_PORT, lambda: _pg("select 1", "postgres"))
    _wait(MY_PORT, lambda: _my("select 1", None))
    _pg(f"create database {DB}", "postgres")
    _my(f"create database {DB}", None)
    _my("create user 'loader'@'%' identified with mysql_native_password"
        " by 'test'; grant all on *.* to 'loader'@'%'", None)


def _fill(rows, shape):
    _my("drop table if exists bench")
    _my(TABLE)
    first = min(rows, 1_000_000)
    _my("set session cte_max_recursion_depth = 100000000; insert into bench"
        " with recursive s(n) as (select 1 union all select n + 1 from s"
        f" where n < {first}) select n,"
        " concat('Näme ', substr(md5(n), 1, 12), ' Ó''Brien'),"
        " concat(substr(md5(n * 3), 1, 10), '@example.com'),"
        " if(n % 10 = 0, null, concat('City ', n % 977)),"
        " if(n % 10 = 0, '', concat('line one ', md5(n),"
        " '\\nline\\ttwo \\\\ ', repeat('x', n % 50))),"
        " concat('tag', n % 200), (n % 10000000) / 100, n % 1000,"
        " n % 3 = 0, timestamp('2020-09-13 12:26:40') + interval (n * 7)"
        " second + interval (n % 1000000) microsecond,"
        " json_object('src', 'bench', 'n', n % 997, 't',"
        " concat('tag', n % 200)), unhex(md5(n)) from s")
    have = first
    while have < rows:
        step = min(have, rows - have)
        _my(f"insert into bench select id + {have}, name, email, city, note,"
            " tag, amount, qty, ok, created_at, payload, raw from bench"
            f" where id <= {step}")
        have += step
    if shape == "plain":
        _my("alter table bench drop column payload")
    _my("analyze table bench")


def _hop(src, dst, where, user="root"):
    from migkit.config import Endpoint, Hop
    ends = {"mysql": Endpoint(host="127.0.0.1", port=MY_PORT, user=user,
                              password="test"),
            "postgres": Endpoint(host="127.0.0.1", port=PG_PORT,
                                 user="postgres", password="test")}
    hop = Hop(name="xe", engine="hetero", source=ends[src], target=ends[dst],
              db_map={DB: DB}, workers=4,
              options={"source_engine": src, "target_engine": dst})
    hop.report_dir = lambda db=None, _p=where: _p
    return hop


class _Checkpoint(dict):
    def save(self):
        pass


def _checked(eng):
    got = [r for r in eng.check_data(DB) if r.scope.endswith("bench")]
    return [r.status for r in got]


def _move(src, dst, way, rows, where, shape):
    from migkit import ranges
    from migkit.engines.hetero import HeteroEngine
    os.environ["MIGKIT_TABLE_COPY"] = way
    eng = HeteroEngine(_hop(src, dst, where))
    eng.create_missing(DB, lambda m: None)
    ranges.active = ranges.Slots(4)
    said = []
    began = time.monotonic()
    eng.move_table(DB, "", "bench", 500_000, _Checkpoint(), said.append)
    took = time.monotonic() - began
    return {"from": src, "to": dst, "way": way, "shape": shape, "rows": rows,
            "seconds": round(took, 2), "rows_a_second": round(rows / took),
            "fell": [s for s in said if "did not prove" in s][:1],
            "check": _checked(eng)}


def _split_up(way, rows, where, shape):
    """Where a server-to-server way's time goes: the whole table as one
    unit, one stream, its copy and then its proof on both servers."""
    from migkit import crossmove
    from migkit.engines.hetero import HeteroEngine
    eng = HeteroEngine(_hop("mysql", "postgres", where))
    began = time.monotonic()
    crossmove.copy(eng, DB, "bench", "public.bench", way, None, None)
    copied = time.monotonic()
    why = crossmove.proved(eng, DB, "bench", "public.bench", None, None)
    done = time.monotonic()
    return {"from": "mysql", "to": "postgres", "way": way, "shape": shape,
            "rows": rows, "one stream": True,
            "copy_seconds": round(copied - began, 2),
            "proof_seconds": round(done - copied, 2), "proved": why is None}


def _one_pass(rows, where, shape):
    from migkit import movers
    from migkit.engines.hetero import HeteroEngine
    _pg("truncate public.bench")
    from migkit.util import tool_env
    hop = _hop("mysql", "postgres", where, user="loader")
    said = []
    real = movers._sh

    def kept(cmd, env=None, log=None, progress=None):
        # the program's own summary, which the move keeps off the screen
        if str(cmd[0]).endswith("pgloader"):
            got = subprocess.run([str(cmd[0]), "--verbose"]
                                 + [str(c) for c in cmd[1:]],
                                 env=tool_env(env), capture_output=True,
                                 text=True)
            said.append((got.stdout + got.stderr)[-3000:])
            return got
        return real(cmd, env, log, progress)
    movers._sh = kept
    began = time.monotonic()
    try:
        movers.pgloader_move(hop, DB, 4, True, said.append)
    finally:
        movers._sh = real
    took = time.monotonic() - began
    return {"from": "mysql", "to": "postgres", "way": "one-pass load",
            "shape": shape, "rows": rows, "seconds": round(took, 2),
            "rows_a_second": round(rows / took),
            "landed": int(_pg("select count(*) from public.bench")),
            "said": [str(m)[-3000:] for m in said][-3:],
            "check": _checked(HeteroEngine(_hop("mysql", "postgres",
                                                where)))}


#: the ways a run times, each by the name a later word may give it
WAYS = ("copier", "compiled", "rendered", "one-pass")


def main():
    rows = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    words = sys.argv[2:]
    shapes = [w for w in words if w in ("json", "plain")] or ["json",
                                                                "plain"]
    ways = [w for w in words if w in WAYS] or list(WAYS)
    out = []

    def said(got):
        out.append(got)
        print(json.dumps(got), flush=True)
    _up()
    try:
        with tempfile.TemporaryDirectory() as where:
            where = Path(where)
            for shape in shapes:
                _fill(rows, shape)
                for way in ("copier", "compiled", "rendered"):
                    if way in ways:
                        _pg("drop table if exists public.bench")
                        said(_move("mysql", "postgres", way, rows, where,
                                   shape))
                for way in ("compiled", "rendered"):
                    if way in ways:
                        said(_split_up(way, rows, where, shape))
                # the table the last way landed, back the other way, its
                # statistics taken as a loaded table's would be
                _pg("analyze public.bench")
                for way in ("copier", "rendered"):
                    if way in ways:
                        _my("drop table if exists bench")
                        said(_move("postgres", "mysql", way, rows, where,
                                   shape))
                if "one-pass" in ways:
                    _fill(rows, shape)
                    said(_one_pass(rows, where, shape))
    finally:
        os.environ.pop("MIGKIT_TABLE_COPY", None)
        _down()
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()

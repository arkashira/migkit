"""A benchmark anyone can re-run (backlog 28).

No speed claim holds until the numbers behind it can be produced again, on
the reader's own machine, from nothing but this repository. This harness:

* starts a disposable source and target (PostgreSQL 16 or MySQL 8.4) in
  containers, sized and named so they cannot touch anything else
* builds the table shapes that decide how a move behaves, inside the source
  server so the generator is not what is being measured:

      keyed         bigint key, text, numeric, timestamp, json
      keyless       the same rows with no key at all
      wide          forty columns
      lob           a binary payload of about 20 KB per row, 1 row in 100
      skewed        a composite key where nine rows in ten share a tenant
      partitioned   the keyed shape, split by range into four partitions

* times three things:
  * the bulk copy, through each path migkit can take
  * the verification, `migkit check`
  * with `--cdc-rate`, how far behind migkit's change tail is when the
    writer stops, at a fixed number of transactions a second
* times the same copy through the open programs migkit could have run
  directly, so its own overhead is on the record beside its numbers
* writes everything - the numbers, the hardware, the versions and every
  setting - to `reports/bench/<time>.json`, and prints it as a table

    .venv/bin/python bench/run.py --engine postgres --rows 100000
    .venv/bin/python bench/run.py --engine mysql --rows 100000 --cdc-rate 200

The defaults are small on purpose: a laptop VM shares two cores between
both servers. Results are only comparable on the same hardware, which is
why the hardware is in the result.
"""
import argparse
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
SHAPES = ("keyed", "keyless", "wide", "lob", "skewed", "partitioned")
IMAGES = {"postgres": "postgres:16", "mysql": "mysql:8.4"}


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _sh(argv, **kw):
    return subprocess.run(argv, capture_output=True, text=True, **kw)


class Server:
    """One disposable database server in a container."""

    def __init__(self, engine, role, tag):
        self.engine, self.role = engine, role
        self.name = f"migkit-bench-{tag}-{role}"
        self.port = _free_port()

    def start(self):
        _sh(["docker", "rm", "-f", "-v", self.name])
        args = ["docker", "run", "-d", "--name", self.name, "-p",
                f"{self.port}:{5432 if self.engine == 'postgres' else 3306}"]
        if self.engine == "postgres":
            args += ["-e", "POSTGRES_PASSWORD=bench", IMAGES["postgres"],
                     "-c", "wal_level=logical", "-c", "shared_buffers=256MB"]
        else:
            args += ["-e", "MYSQL_ROOT_PASSWORD=bench", IMAGES["mysql"],
                     "--binlog-row-metadata=FULL",
                     "--server-id=" + ("1" if self.role == "src" else "2")]
        got = _sh(args)
        if got.returncode:
            raise SystemExit(got.stderr)
        end = time.time() + 240
        while time.time() < end:
            if self.sql("select 1", check=False,
                        db="postgres" if self.engine == "postgres"
                        else None).returncode == 0:
                return self
            time.sleep(2)
        raise SystemExit(f"{self.name} never answered")

    def stop(self):
        _sh(["docker", "rm", "-f", "-v", self.name])

    def sql(self, text, check=True, db=None):
        if self.engine == "postgres":
            argv = ["docker", "exec", "-i", "-e", "PGPASSWORD=bench",
                    self.name, "psql", "-U", "postgres", "-h", "127.0.0.1",
                    "-d", db or "bench", "-v", "ON_ERROR_STOP=1", "-At"]
        else:
            argv = ["docker", "exec", "-i", self.name, "mysql", "-uroot",
                    "-pbench", "-h127.0.0.1", "--protocol=tcp", "-N", "-B"]
            if db:
                argv += ["-D", db]
        got = subprocess.run(argv, input=text, capture_output=True,
                             text=True)
        if check and got.returncode:
            raise SystemExit(f"{self.name}: {got.stderr.strip()[-400:]}")
        return got

    def connect(self):
        """A driver connection to the bench database, from the host."""
        if self.engine == "postgres":
            import psycopg2
            return psycopg2.connect(host="127.0.0.1", port=self.port,
                                    user="postgres", password="bench",
                                    dbname="bench")
        import pymysql
        return pymysql.connect(host="127.0.0.1", port=self.port,
                               user="root", password="bench",
                               database="bench")

    def version(self):
        q = ("show server_version" if self.engine == "postgres"
             else "select version()")
        return self.sql(q, db="postgres" if self.engine == "postgres"
                        else None).stdout.strip()


# ---- the shapes, generated inside the server ------------------------------

PG_SHAPES = {
    "keyed": """
        create table keyed (id bigint primary key, name text, amount
          numeric(14,4), created_at timestamptz, payload jsonb);
        insert into keyed select g, 'name ' || g, g * 1.25,
          timestamptz '2024-01-01' + g * interval '1 minute',
          jsonb_build_object('k', g, 'tag', 't' || (g % 50))
          from generate_series(1, {rows}) g""",
    "keyless": """
        create table keyless (id bigint, name text, amount numeric(14,4));
        insert into keyless select g, 'name ' || g, g * 1.25
          from generate_series(1, {rows}) g""",
    "wide": """
        create table wide (id bigint primary key, {wide_cols});
        insert into wide select g, {wide_vals}
          from generate_series(1, {rows}) g""",
    "lob": """
        create table lob (id bigint primary key, label text, body bytea);
        insert into lob select g, 'lob ' || g,
          decode(string_agg(md5(g::text || x::text), ''), 'hex')
          from generate_series(1, greatest({rows} / 100, 1)) g,
               generate_series(1, 640) x group by g""",
    "skewed": """
        create table skewed (tenant int, id bigint, v text,
          primary key (tenant, id));
        insert into skewed select case when g % 10 = 0 then g % 97 else 1
          end, g, 'v' || g from generate_series(1, {rows}) g""",
    "partitioned": """
        create table parted (id bigint, created_at date, v text,
          primary key (id, created_at)) partition by range (created_at);
        create table parted_1 partition of parted
          for values from ('2020-01-01') to ('2021-01-01');
        create table parted_2 partition of parted
          for values from ('2021-01-01') to ('2022-01-01');
        create table parted_3 partition of parted
          for values from ('2022-01-01') to ('2023-01-01');
        create table parted_4 partition of parted
          for values from ('2023-01-01') to ('2024-01-01');
        insert into parted select g,
          date '2020-01-01' + (g % 1460), 'v' || g
          from generate_series(1, {rows}) g""",
}

MY_SEQ = """
    with recursive d as (select 0 n union all select n + 1 from d
                         where n < 9)
    select a.n + 10 * b.n + 100 * c.n + 1000 * e.n + 10000 * f.n
           + 100000 * h.n + 1 as g
      from d a, d b, d c, d e, d f, d h"""

MY_SHAPES = {
    "keyed": """
        create table keyed (id bigint primary key, name varchar(64), amount
          decimal(14,4), created_at datetime, payload json);
        insert into keyed select g, concat('name ', g), g * 1.25,
          '2024-01-01' + interval g minute,
          json_object('k', g, 'tag', concat('t', g % 50))
          from ({seq}) s where g <= {rows}""",
    "keyless": """
        create table keyless (id bigint, name varchar(64),
          amount decimal(14,4));
        insert into keyless select g, concat('name ', g), g * 1.25
          from ({seq}) s where g <= {rows}""",
    "wide": """
        create table wide (id bigint primary key, {wide_cols});
        insert into wide select g, {wide_vals} from ({seq}) s
          where g <= {rows}""",
    "lob": """
        create table lob (id bigint primary key, label varchar(64),
          body longblob);
        insert into lob select g, concat('lob ', g), random_bytes(1024)
          from ({seq}) s where g <= greatest({rows} div 100, 1);
        update lob set body = concat(body, body, body, body, body, body,
          body, body, body, body, body, body, body, body, body, body, body,
          body, body, body)""",
    "skewed": """
        create table skewed (tenant int, id bigint, v varchar(32),
          primary key (tenant, id));
        insert into skewed select case when g % 10 = 0 then g % 97 else 1
          end, g, concat('v', g) from ({seq}) s where g <= {rows}""",
    "partitioned": """
        create table parted (id bigint, created_at date, v varchar(32),
          primary key (id, created_at))
          partition by range (year(created_at)) (
            partition p2020 values less than (2021),
            partition p2021 values less than (2022),
            partition p2022 values less than (2023),
            partition p2023 values less than (2024));
        insert into parted select g, '2020-01-01' + interval (g % 1460) day,
          concat('v', g) from ({seq}) s where g <= {rows}""",
}


def seed(src, rows, shapes):
    """Build the shapes on the source; returns seconds per shape."""
    if src.engine == "postgres":
        src.sql("create database bench", db="postgres")
        wide_cols = ", ".join(f"c{i} text" for i in range(1, 40))
        wide_vals = ", ".join(f"'c{i}-' || g" for i in range(1, 40))
        table = PG_SHAPES
    else:
        src.sql("create database bench")
        wide_cols = ", ".join(f"c{i} varchar(32)" for i in range(1, 40))
        wide_vals = ", ".join(f"concat('c{i}-', g)" for i in range(1, 40))
        table = MY_SHAPES
        # the generator yields a million rows at most; say so rather than
        # quietly stop at it
        if rows > 1_000_000:
            raise SystemExit("--rows above 1,000,000 is PostgreSQL only here")
    out = {}
    for shape in shapes:
        began = time.monotonic()
        src.sql(table[shape].format(rows=rows, wide_cols=wide_cols,
                                    wide_vals=wide_vals, seq=MY_SEQ),
                db="bench")
        out[shape] = round(time.monotonic() - began, 2)
    return out


# ---- migkit, as the operator runs it ---------------------------------------

def _conf(path, engine, src, dst, hetero=False):
    body = (f"hops:\n  bench:\n"
            f"    engine: {'hetero' if hetero else engine}\n"
            f"    source: {{host: 127.0.0.1, port: {src.port},"
            f" user: {'postgres' if engine == 'postgres' else 'root'},"
            " password: bench}\n"
            f"    target: {{host: 127.0.0.1, port: {dst.port},"
            f" user: {'postgres' if engine == 'postgres' else 'root'},"
            " password: bench}\n"
            "    databases: [bench]\n")
    if hetero:
        body += (f"    options: {{source_engine: {engine},"
                 f" target_engine: {engine}}}\n")
    path.write_text(body)
    return path


def migkit(conf, reports, *argv, env=None, background=False):
    full = dict(os.environ, MIGKIT_CONF=str(conf),
                MIGKIT_REPORTS=str(reports), **(env or {}))
    cmd = [sys.executable, "-c", "from migkit.cli import main; main()",
           *argv]
    if background:
        return subprocess.Popen(cmd, env=full, cwd=ROOT,
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.STDOUT)
    began = time.monotonic()
    got = subprocess.run(cmd, env=full, cwd=ROOT, capture_output=True,
                         text=True)
    return got, round(time.monotonic() - began, 2)


def empty_target(dst):
    if dst.engine == "postgres":
        dst.sql("drop database if exists bench with (force)", db="postgres")
        dst.sql("create database bench", db="postgres")
    else:
        dst.sql("drop database if exists bench; create database bench")


def movers(engine):
    """The paths `migkit move` can take here, by the name it reads."""
    from migkit import movers as m
    got = ["builtin"]
    if engine == "postgres" and m.which("pg_dump") and m.which("pg_restore"):
        got.append("pgdump")
    if engine == "mysql" and m.which("mydumper") and m.which("myloader"):
        got.append("mydumper")
    return got


def direct(engine, src, dst, workdir):
    """The same copy through the open programs, run directly: what migkit
    could have done without itself, as the baseline for its overhead."""
    from migkit.util import tool_env
    empty_target(dst)
    out = Path(workdir) / "direct"
    shutil.rmtree(out, ignore_errors=True)
    began = time.monotonic()
    if engine == "postgres":
        env = tool_env({"PGPASSWORD": "bench"})
        a = _sh(["pg_dump", "-h", "127.0.0.1", "-p", str(src.port), "-U",
                 "postgres", "-d", "bench", "-Fd", "-j", "2", "-f",
                 str(out)], env=env)
        b = _sh(["pg_restore", "-h", "127.0.0.1", "-p", str(dst.port), "-U",
                 "postgres", "-d", "bench", "-j", "2", str(out)], env=env)
        ok = a.returncode == 0
        what = "pg_dump -Fd -j2 | pg_restore -j2"
    else:
        env = tool_env({"MYSQL_PWD": "bench"})
        a = _sh(["mydumper", "-h", "127.0.0.1", "-P", str(src.port), "-u",
                 "root", "-B", "bench", "-o", str(out), "-t", "2"], env=env)
        b = _sh(["myloader", "-h", "127.0.0.1", "-P", str(dst.port), "-u",
                 "root", "-B", "bench", "-d", str(out), "-t", "2"],
                env=env)
        ok = a.returncode == 0
        what = "mydumper -t2 | myloader -t2"
    took = round(time.monotonic() - began, 2)
    # judged by what arrived rather than by the exit code: a restore
    # returns non-zero for a setting the older server does not know and
    # still loads every row, which is the case migkit reads its way through
    same = ok and _counts(src) == _counts(dst)
    return {"path": what, "seconds": took, "ok": same,
            "exit": [a.returncode, b.returncode],
            "said": (a.stderr + b.stderr).strip()[-300:]}


def _counts(server):
    """{table: rows} for the shapes, as the server counts them."""
    names = ("keyed", "keyless", "wide", "lob", "skewed", "parted")
    out = {}
    for n in names:
        got = server.sql(f"select count(*) from {n}", db="bench",
                         check=False)
        out[n] = got.stdout.strip() if got.returncode == 0 else None
    return out


def cdc_lag(engine, src, dst, conf, reports, rate, seconds):
    """How far behind migkit's change tail is when a writer at `rate`
    transactions a second stops: the seconds until the target holds every
    row the writer wrote."""
    # made before the tail starts: a table made on the source while it
    # runs is a schema change, and the tail stops at one
    src.sql("create table if not exists writes (id bigint primary key,"
            " v int)", db="bench")
    dst.sql("create table if not exists writes (id bigint primary key,"
            " v int)", db="bench")
    tail = migkit(conf, reports, "move", "bench", "--db", "bench", "--mode",
                  "cdc", "--go", background=True)
    time.sleep(8)
    stop, wrote = time.monotonic() + seconds, [0]

    def writer():
        # one transaction per row, over one driver connection, ten ticks a
        # second: a client process per statement could not keep up with the
        # rates worth measuring
        conn = src.connect()
        per_tick = max(rate // 10, 1)
        n = 0
        try:
            cur = conn.cursor()
            while time.monotonic() < stop:
                tick = time.monotonic()
                for _ in range(per_tick):
                    cur.execute("insert into writes values (%s, %s)", (n, n))
                    conn.commit()
                    n += 1
                wait = 0.1 - (time.monotonic() - tick)
                if wait > 0:
                    time.sleep(wait)
        finally:
            conn.close()
        wrote[0] = n
    t = threading.Thread(target=writer)
    t.start()
    t.join()
    ended, lag = time.monotonic(), None
    while time.monotonic() - ended < 300:
        got = dst.sql("select count(*) from writes", db="bench",
                      check=False).stdout.strip()
        if got.isdigit() and int(got) >= wrote[0]:
            lag = round(time.monotonic() - ended, 2)
            break
        time.sleep(0.5)
    tail.terminate()
    tail.wait(timeout=60)
    return {"rate": rate, "seconds": seconds, "rows": wrote[0],
            "achieved_rate": round(wrote[0] / seconds),
            "lag_seconds": lag}


def machine():
    info = _sh(["docker", "info", "--format",
                "{{.NCPU}} {{.MemTotal}} {{.ServerVersion}}"]).stdout.split()
    import migkit
    return {"host": platform.platform(), "host_cpus": os.cpu_count(),
            "python": platform.python_version(),
            "docker_cpus": info[0] if info else "?",
            "docker_memory_mb": (int(info[1]) // 2 ** 20) if len(info) > 1
            else "?",
            "migkit": migkit.__version__}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--engine", choices=("postgres", "mysql"),
                    default="postgres")
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--shapes", default=",".join(SHAPES))
    ap.add_argument("--cdc-rate", type=int, default=0,
                    help="transactions a second for the change-tail lag")
    ap.add_argument("--cdc-seconds", type=int, default=30)
    ap.add_argument("--keep", action="store_true",
                    help="leave the containers running")
    args = ap.parse_args()
    shapes = [s for s in args.shapes.split(",") if s]
    tag = time.strftime("%H%M%S")
    src = Server(args.engine, "src", tag)
    dst = Server(args.engine, "dst", tag)
    work = Path(tempfile.mkdtemp(prefix="migkit-bench-"))
    reports = work / "reports"
    result = {"when": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
              "engine": args.engine, "rows": args.rows, "shapes": shapes,
              "machine": machine(), "moves": [], "checks": []}
    try:
        src.start()
        dst.start()
        result["versions"] = {"source": src.version(),
                              "target": dst.version()}
        result["seed_seconds"] = seed(src, args.rows, shapes)
        conf = _conf(work / "hops.yaml", args.engine, src, dst)
        for via in movers(args.engine):
            empty_target(dst)
            got, took = migkit(conf, reports, "move", "bench", "--go",
                               env={"MIGKIT_MOVER": via})
            result["moves"].append({"path": via, "seconds": took,
                                    "ok": got.returncode == 0,
                                    "tail": got.stdout[-300:]
                                    if got.returncode else ""})
            got, took = migkit(conf, reports, "check", "bench")
            verdict = {}
            try:
                verdict = json.loads(
                    (reports / "bench" / "verdict.json").read_text())
            except (OSError, ValueError):
                pass
            result["checks"].append({
                "after": via, "seconds": took,
                "verdict": verdict.get("status"),
                # what made it so: a key-less table is one of the shapes on
                # purpose, and the check is right to name it
                "findings": [f"{f.get('status')} {f.get('check')}"
                             f" {f.get('scope')}"
                             for f in verdict.get("findings", [])]})
        result["direct"] = direct(args.engine, src, dst, work)
        if args.cdc_rate:
            empty_target(dst)
            hconf = _conf(work / "tail.yaml", args.engine, src, dst,
                          hetero=True)
            migkit(hconf, reports, "move", "bench", "--go")
            result["cdc"] = cdc_lag(args.engine, src, dst, hconf, reports,
                                    args.cdc_rate, args.cdc_seconds)
    finally:
        if not args.keep:
            src.stop()
            dst.stop()
    out = ROOT / "reports" / "bench"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{time.strftime('%Y%m%d-%H%M%S')}-{args.engine}.json"
    path.write_text(json.dumps(result, indent=1))
    m = result["machine"]
    print(f"\n{args.engine}, {args.rows:,} rows per shape, on {m['host']}"
          f" ({m['docker_cpus']} vCPU, {m['docker_memory_mb']} MB for the"
          f" containers), migkit {m['migkit']}\n")
    print("| step | path | seconds | result |")
    print("|---|---|---|---|")
    for shape, took in result.get("seed_seconds", {}).items():
        print(f"| seed {shape} | in the server | {took} | |")
    for mv, ck in zip(result["moves"], result["checks"]):
        print(f"| move | {mv['path']} | {mv['seconds']} |"
              f" {'ok' if mv['ok'] else 'FAILED'} |")
        print(f"| check | after {ck['after']} | {ck['seconds']} |"
              f" {ck['verdict']}: {'; '.join(ck['findings'][:3])} |")
    d = result.get("direct")
    if d:
        print(f"| copy, no migkit | {d['path']} | {d['seconds']} |"
              f" {'ok' if d['ok'] else 'FAILED'} |")
    c = result.get("cdc")
    if c:
        print(f"| change tail | {c['achieved_rate']} rows/s for"
              f" {c['seconds']}s | lag {c['lag_seconds']} | {c['rows']:,}"
              " rows |")
    print(f"\nwritten to {path}")
    return result


if __name__ == "__main__":
    main()

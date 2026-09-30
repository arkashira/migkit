"""The physical seed against the logical copy, on one pair (R19 lever 1).

Starts a disposable source and an empty target (PostgreSQL 16 or MySQL
8.4) on one docker network, fills the source with about `--gb` of tables
and indexes built inside the server, and times, each onto a fresh empty
target and each with a writer inserting on the source throughout:

* the physical rung migkit takes (`physical`): a copy of the pages
  (PostgreSQL) or the target cloning itself (MySQL), from the target's
  stop to a target that answers, recovered
* PostgreSQL only: the copy of the pages alone, by the same script, sent
  as they are and compressed on the source - what compressing buys or
  costs on this link
* the logical copy `migkit move --mode full` takes without the physical
  option (the bulk program it picks, or its own copier)
* PostgreSQL only: a subscription that copies the rows (the native
  full+cdc path) onto the schema made first, until every table is ready

and prints one JSON line with the numbers and the machine.

    .venv/bin/python tools/with_docker_lock.py \
        .venv/bin/python bench/physical_seed.py --engine postgres --gb 1.4
"""
import argparse
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
from pathlib import Path

NET = "migkit-test-phys-bnet"
SRC, DST = "migkit-test-phys-bsrc", "migkit-test-phys-bdst"
VOL = "migkit-test-phys-bvol"
PORTS = {"postgres": (16124, 16125), "mysql": (16126, 16127)}
PG_DATA = "/var/lib/postgresql/data"


def sh(*argv, check=True, input=None):
    got = subprocess.run(list(argv), capture_output=True, text=True,
                         input=input)
    if check and got.returncode != 0:
        raise SystemExit(f"{' '.join(argv)}: {got.stderr.strip()}")
    return got.stdout.strip()


def pg(name, sql, db="postgres"):
    return sh("docker", "exec", "-e", "PGPASSWORD=test", name, "psql", "-h",
              "127.0.0.1", "-U", "postgres", "-d", db, "-At", "-v",
              "ON_ERROR_STOP=1", "-c", sql)


def my(name, sql):
    return sh("docker", "exec", "-i", name, "mysql", "-uroot", "-ptest",
              "-N", "-B", input=sql)


def wait(ask, most=300):
    end = time.time() + most
    while time.time() < end:
        try:
            ask()
            return
        except SystemExit:
            time.sleep(1)
    raise SystemExit("never answered")


def clean():
    sh("docker", "rm", "-f", "-v", SRC, DST, check=False)
    sh("docker", "volume", "rm", VOL, check=False)


def start(engine, name, port, volume=False):
    sh("docker", "rm", "-f", "-v", name, check=False)
    if volume:
        sh("docker", "volume", "rm", VOL, check=False)
    if engine == "postgres":
        sh("docker", "run", "-d", "--name", name, "--network", NET,
           "--shm-size=256m", "-e", "POSTGRES_PASSWORD=test",
           *(["-v", f"{VOL}:{PG_DATA}"] if volume else []),
           "-p", f"127.0.0.1:{port}:5432", "postgres:16", "-c",
           "wal_level=logical", "-c", "max_wal_size=4GB")
        wait(lambda: pg(name, "select 1"))
    else:
        sid = "61" if name == SRC else "62"
        sh("docker", "run", "-d", "--name", name, "--network", NET, "-e",
           "MYSQL_ROOT_PASSWORD=test", "-p", f"127.0.0.1:{port}:3306",
           "mysql:8.4", f"--server-id={sid}", "--gtid-mode=ON",
           "--enforce-gtid-consistency=ON", "--binlog-row-metadata=FULL",
           "--plugin-load-add=mysql_clone.so",
           "--innodb-buffer-pool-size=512M")
        wait(lambda: my(name, "select 1"))


def fill_pg(gb):
    rows = int(gb * 7_000_000)
    pg(SRC, "create database app")
    sh("docker", "exec", SRC, "sh", "-c",
       f"echo 'host replication all all scram-sha-256' >> {PG_DATA}/"
       "pg_hba.conf")
    pg(SRC, "select pg_reload_conf()")
    for name, n in (("a", rows // 2), ("b", rows // 3), ("c", rows // 6)):
        pg(SRC, f"create table {name} (id bigint primary key, v text,"
                " n int, at timestamptz, w text);"
                f" insert into {name} select g, md5(g::text), g % 1000,"
                " now() - g * interval '1 second', repeat(md5((g * 7)::text),"
                f" 2) from generate_series(1, {n}) g;"
                f" create index on {name} (n)", "app")
        # not inside the transaction psql makes of one -c
        pg(SRC, f"vacuum analyze {name}", "app")
    return {"rows": rows,
            "bytes": int(pg(SRC, "select pg_database_size('app')"))}


def fill_my(gb):
    rows = int(gb * 5_500_000)
    # ten thousand numbers, crossed with themselves for the rows
    my(SRC, "create database app; create table app.nums (i int primary"
            " key); set session cte_max_recursion_depth = 20000;"
            " insert into app.nums with recursive g(i) as (select 0 union"
            " all select i + 1 from g where i < 9999) select i from g")
    for name, n in (("a", rows // 2), ("b", rows // 3), ("c", rows // 6)):
        my(SRC, f"create table app.{name} (id bigint primary key, v"
                " varchar(64), n int, at datetime, w varchar(128), key(n));"
                f" insert into app.{name} select g, md5(g), g % 1000,"
                " now() - interval g second, concat(md5(g * 7), md5(g * 7))"
                " from (select x.i * 10000 + y.i + 1 as g from app.nums x,"
                f" app.nums y where x.i <= {n // 10000}) s where g <= {n}")
    my(SRC, "drop table app.nums")
    return {"rows": rows, "bytes": int(my(
        SRC, "select sum(data_length + index_length) from"
             " information_schema.tables where table_schema = 'app'"))}


def hops(engine, tmp):
    s, d = PORTS[engine]
    user = "postgres" if engine == "postgres" else "root"
    phys = (f"        data: {PG_DATA}\n"
            f"        run: docker run --rm -i --network {NET} -u postgres"
            f" -v {VOL}:{PG_DATA} postgres:16 sh -c\n"
            f"        stop: docker stop {DST}\n"
            f"        start: docker start {DST}\n"
            f"        source: {SRC}:5432\n" if engine == "postgres" else
            f"        source: {SRC}:3306\n"
            f"        restart: docker restart {DST}\n")
    base = (f"    engine: {engine}\n"
            f"    source: {{host: 127.0.0.1, port: {s}, user: {user},"
            " password: test}\n"
            f"    target: {{host: 127.0.0.1, port: {d}, user: {user},"
            " password: test}\n"
            "    databases: [app]\n")
    conf = tmp / "hops.yaml"
    conf.write_text("hops:\n  bphys:\n" + base
                    + "    options:\n      physical:\n" + phys
                    + "  blog:\n" + base)
    return conf


class Writer:
    """Inserts into `app.w` on the source, one row a transaction, for as
    long as a timing runs: every way is timed under the same writes."""

    def __init__(self, engine, port):
        import threading
        self.engine, self.port = engine, port
        self.stop, self.n = threading.Event(), 0
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        if self.engine == "postgres":
            import psycopg2
            conn = psycopg2.connect(host="127.0.0.1", port=self.port,
                                    user="postgres", password="test",
                                    dbname="app")
            conn.autocommit = True
            sql = "insert into w (at) values (now())"
        else:
            import pymysql
            conn = pymysql.connect(host="127.0.0.1", port=self.port,
                                   user="root", password="test",
                                   database="app", autocommit=True)
            sql = "insert into w (at) values (now())"
        with conn.cursor() as cur:
            while not self.stop.is_set():
                cur.execute(sql)
                self.n += 1
                time.sleep(0.01)
        conn.close()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        self.thread.join(10)


def timed(fn, engine, port, out, name):
    with Writer(engine, port) as w:
        began = time.time()
        fn()
        out[f"{name}_seconds"] = round(time.time() - began, 1)
    out[f"{name}_writes"] = w.n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", choices=("postgres", "mysql"),
                    default="postgres")
    ap.add_argument("--gb", type=float, default=1.4)
    args = ap.parse_args()
    engine = args.engine
    s, d = PORTS[engine]
    tmp = Path(tempfile.mkdtemp(prefix="migkit-bench-phys-"))
    os.environ["MIGKIT_CONF"] = str(hops(engine, tmp))
    os.environ["MIGKIT_REPORTS"] = str(tmp / "reports")
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from migkit import physical
    from migkit.config import get_hop
    from migkit.engines import get_engine
    clean()
    sh("docker", "network", "create", NET, check=False)
    out = {"engine": engine, "machine": platform.platform(),
           "cpus": os.cpu_count()}
    try:
        start(engine, SRC, s)
        began = time.time()
        out.update(fill_pg(args.gb) if engine == "postgres" else
                   fill_my(args.gb))
        out["fill_seconds"] = round(time.time() - began, 1)
        if engine == "postgres":
            pg(SRC, "create table w (id bigserial primary key, at"
                    " timestamptz)", "app")
        else:
            my(SRC, "create table app.w (id bigint auto_increment primary"
                    " key, at datetime)")
        # the physical rung, end to end, onto a fresh empty target
        start(engine, DST, d, volume=True)
        hop = get_hop("bphys")
        eng = get_engine(hop)
        said = []
        timed(lambda: physical.carry(hop, eng, "", "full", True,
                                     said.append),
              engine, s, out, "physical")
        out["physical_said"] = said
        out["physical_record"] = physical.seeded(hop)
        if engine == "postgres":
            # the copy alone, the same script, each way the pages travel
            for way, compressed in (("pages_plain", False),
                                    ("pages_zstd", True)):
                start(engine, DST, d, volume=True)
                sh("docker", "stop", DST)
                script = physical._pg_script(hop, physical.settings(hop),
                                             compressed)
                timed(lambda: sh("docker", "run", "--rm", "-i", "--network",
                                 NET, "-u", "postgres", "-v",
                                 f"{VOL}:{PG_DATA}", "postgres:16", "sh",
                                 "-c", script, input="test\n"),
                      engine, s, out, way)
        # the logical copy migkit picks, onto a fresh empty target
        start(engine, DST, d)
        if engine == "postgres":
            pg(DST, "create database app")
        from click.testing import CliRunner

        from migkit import cli
        got = {}

        def logical():
            got["r"] = CliRunner().invoke(cli.main, ["move", "blog", "--go"])
        timed(logical, engine, s, out, "logical")
        out["logical_exit"] = got["r"].exit_code
        out["logical_said"] = got["r"].output[-1500:]
        if engine == "postgres":
            start(engine, DST, d)
            pg(SRC, "create publication bp for all tables", "app")
            pg(DST, "create database app")
            sh("docker", "exec", "-e", "PGPASSWORD=test", SRC, "sh", "-c",
               "pg_dump -h 127.0.0.1 -U postgres -s app | psql -h"
               f" {DST} -U postgres -q app", check=False)

            def subscribed():
                pg(DST, "create subscription bs connection 'host=" + SRC
                        + " dbname=app user=postgres password=test'"
                        " publication bp", "app")
                while pg(DST, "select count(*) from pg_subscription_rel"
                              " where srsubstate <> 'r'", "app") != "0":
                    time.sleep(1)
            timed(subscribed, engine, s, out, "subscription")
    finally:
        clean()
        sh("docker", "network", "rm", NET, check=False)
    print(json.dumps(out, default=str))


if __name__ == "__main__":
    main()

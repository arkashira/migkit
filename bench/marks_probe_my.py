"""What a MySQL or MariaDB server shows migkit's binlog reader of the marks
a two-way tail can leave (backlog R3, the mark rungs).

    .venv/bin/python bench/marks_probe_my.py mysql     # mysql:8.4
    .venv/bin/python bench/marks_probe_my.py mariadb   # mariadb:11

MySQL 8.4, started with GTIDs on and each statement's text logged with
its rows: the raw bytes of the tagged GTID event (type 42) the reader
does not know, what the server does with a transaction whose GTID it
already executed, `gtid_executed` with a tag in it, whether a statement's
leading comment survives into its rows-query event, and which grants a
plain user needs. MariaDB 11: whether the reader sees `skip_replication`
in the row events' header flags, and which privilege sets it.
"""
import pathlib
import subprocess
import sys
import time
import uuid

import pymysql

# this checkout's migkit, not whichever the environment installed
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

KIND = sys.argv[1] if len(sys.argv) > 1 else "mysql"
NAME, PORT = "migkit-test-rung-my", 15961
IMAGE = {"mysql": "mysql:8.4", "mariadb": "mariadb:11"}[KIND]
ARGS = {"mysql": ["--gtid-mode=ON", "--enforce-gtid-consistency=ON",
                  "--binlog-rows-query-log-events=ON",
                  "--binlog-row-metadata=FULL"],
        "mariadb": ["--log-bin", "--binlog-format=ROW",
                    "--binlog-row-metadata=FULL", "--server-id=1"]}[KIND]
UUID = str(uuid.uuid5(uuid.NAMESPACE_URL, "migkit marks probe"))


def sh(*a):
    return subprocess.run(a, capture_output=True, text=True)


def conn(user="root", pw="test", db=None):
    return pymysql.connect(host="127.0.0.1", port=PORT, user=user,
                           password=pw, database=db, autocommit=False)


def first_line(e):
    return str(e).strip().splitlines()[0]


def main():
    sh("docker", "rm", "-f", "-v", NAME)
    env = ["-e", "MYSQL_ROOT_PASSWORD=test"] if KIND == "mysql" else \
        ["-e", "MARIADB_ROOT_PASSWORD=test"]
    subprocess.run(["docker", "run", "-d", "--name", NAME, *env, "-p",
                    f"{PORT}:3306", IMAGE, *ARGS], check=True,
                   capture_output=True)
    for _ in range(90):
        try:
            conn().close()
            break
        except Exception:
            time.sleep(2)
    try:
        probe()
    finally:
        sh("docker", "rm", "-f", "-v", NAME)


def events(start, only=None):
    from pymysqlreplication import BinLogStreamReader
    from pymysqlreplication.event import (GtidEvent, MariadbGtidEvent,
                                          NotImplementedEvent, QueryEvent,
                                          RowsQueryLogEvent, XidEvent)
    from pymysqlreplication.row_event import (TableMapEvent,
                                              UpdateRowsEvent,
                                              WriteRowsEvent)
    # the reader passes on only the events it is asked for, by class
    only = [GtidEvent, MariadbGtidEvent, NotImplementedEvent, QueryEvent,
            RowsQueryLogEvent, XidEvent, TableMapEvent, WriteRowsEvent,
            UpdateRowsEvent, *(only or ())]
    stream = BinLogStreamReader(
        connection_settings={"host": "127.0.0.1", "port": PORT,
                             "user": "root", "passwd": "test"},
        server_id=91001, blocking=False, resume_stream=True,
        log_file=start[0], log_pos=start[1], only_events=only,
        filter_non_implemented_events=False)
    out = list(stream)
    stream.close()
    return out, NotImplementedEvent


def position(c):
    with c.cursor() as cur:
        try:
            cur.execute("show binary log status")
        except pymysql.err.ProgrammingError:
            cur.execute("show master status")
        got = cur.fetchone()
    return got[0], int(got[1])


def probe():
    a = conn()
    with a.cursor() as cur:
        cur.execute("select version()")
        print("VERSION", cur.fetchone()[0])
        cur.execute("create database app")
        cur.execute("create table app.t (id int primary key, v text)")
        cur.execute("show grants")
        print("GRANTS root", [g[0][:160] for g in cur.fetchall()])
    a.commit()
    if KIND == "mysql":
        mysql(a)
    else:
        mariadb(a)
    a.close()


def mysql(a):
    from pymysqlreplication.packet import BinLogPacketWrapper
    raw = {}

    from pymysqlreplication.event import BinLogEvent

    class Raw(BinLogEvent):
        def __init__(self, from_packet, event_size, table_map, ctl, **kw):
            super().__init__(from_packet, event_size, table_map, ctl, **kw)
            self.body = self.packet.read(event_size)
            raw.setdefault("bodies", []).append(self.body)
    BinLogPacketWrapper._BinLogPacketWrapper__event_map[42] = Raw
    start = position(a)
    with a.cursor() as cur:
        for gno in (5, 300, 2 ** 40 + 7):
            cur.execute(f"set gtid_next = '{UUID}:migkit:{gno}'")
            cur.execute("begin")
            cur.execute("insert into app.t values (%s, 'tagged')",
                        (gno % 1000,))
            cur.execute("commit")
        cur.execute("set gtid_next = 'AUTOMATIC'")
        cur.execute("select @@global.gtid_executed")
        print("GTID_EXECUTED", cur.fetchone()[0].replace("\n", ""))
        # the same GTID again
        cur.execute(f"set gtid_next = '{UUID}:migkit:5'")
        cur.execute("begin")
        try:
            cur.execute("insert into app.t values (900, 'again')")
            cur.execute("commit")
            print("EXECUTED GTID AGAIN: statement ok")
        except Exception as e:
            print("EXECUTED GTID AGAIN:", first_line(e))
            a.rollback()
        cur.execute("set gtid_next = 'AUTOMATIC'")
        cur.execute("select count(*) from app.t where id = 900")
        print("  row 900 written:", cur.fetchone()[0])
        a.commit()
        # an empty transaction under a tag
        cur.execute(f"set gtid_next = '{UUID}:migkit:77'")
        cur.execute("begin")
        cur.execute("commit")
        cur.execute("set gtid_next = 'AUTOMATIC'")
        cur.execute("select gtid_subset(%s, @@global.gtid_executed)",
                    (f"{UUID}:migkit:77",))
        print("EMPTY tagged transaction executed:", cur.fetchone()[0])
        a.commit()
        # a leading comment and the rows-query event
        cur.execute("/* migkit:probe */ insert into app.t values (7, 'c')")
    a.commit()
    got, nie = events(start, [Raw])
    from migkit.binlog_marks import tagged_gtid
    for ev in got:
        name = type(ev).__name__
        if name == "Raw":
            print("EVENT 42 flags", hex(ev.packet.flags), "body",
                  ev.body.hex())
            try:
                print("  decoded", tagged_gtid(ev.body))
            except Exception as e:
                print("  decoded ->", type(e).__name__, e)
        elif name == "RowsQueryLogEvent":
            print("ROWS_QUERY", repr(ev.query))
        elif isinstance(ev, nie):
            print("NOT IMPLEMENTED type", ev.event_type)
        else:
            print("EVENT", name)
    proofs()
    # grants a plain user needs
    with a.cursor() as cur:
        cur.execute("create user u1 identified by 'u1'")
        cur.execute("grant all on app.* to u1")
    a.commit()
    for extra in ("", "SESSION_VARIABLES_ADMIN", "TRANSACTION_GTID_TAG"):
        if extra:
            with a.cursor() as cur:
                cur.execute(f"grant {extra} on *.* to u1")
            a.commit()
        u = conn("u1", "u1")
        with u.cursor() as cur:
            try:
                cur.execute(f"set gtid_next = '{UUID}:migkit:{1000 + len(extra)}'")
                cur.execute("begin")
                cur.execute("commit")
                cur.execute("set gtid_next = 'AUTOMATIC'")
                print("PLAIN + [", extra, "] tagged gtid: ok")
            except Exception as e:
                print("PLAIN + [", extra, "] tagged gtid:", first_line(e))
            cur.execute("show grants")
            print("  grants", [g[0][:200] for g in cur.fetchall()])
        u.close()


def proofs():
    """migkit's own proof of each rung the server allows, and the facts
    it climbs from."""
    from migkit import marks
    from migkit.binlog_marks import register
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    from pymysqlreplication.packet import BinLogPacketWrapper
    # the reader's own class for the tagged GTID, not this probe's
    BinLogPacketWrapper._BinLogPacketWrapper__event_map.pop(42, None)
    import migkit.binlog_marks as bm
    bm._REGISTERED.clear()
    register()
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    eng = MySQLEngine(Hop(name="probe", engine="mysql", source=ep,
                          target=ep, databases=["app"],
                          options={"two_way": {"on_conflict": "error"},
                                   "server_id": 91002}))
    facts = eng.mark_facts("dst", "app")
    print("FACTS", {k: v for k, v in facts.items() if k != "grants"})
    ranked, dropped = marks.choose_rung(eng, facts)
    print("ALLOWED", [r.name for r in ranked],
          "DROPPED", [(r.name, w) for r, w in dropped])
    for r in ranked:
        print("PROOF", r.name, eng.mark_prove("dst", "app", r.name)
              or "proved")


def mariadb(a):
    start = position(a)
    with a.cursor() as cur:
        cur.execute("set skip_replication = 1")
        cur.execute("insert into app.t values (1, 'skip')")
    a.commit()
    with a.cursor() as cur:
        cur.execute("set skip_replication = 0")
        cur.execute("insert into app.t values (2, 'plain')")
    a.commit()
    got, nie = events(start)
    for ev in got:
        print("EVENT", type(ev).__name__, "flags", hex(ev.packet.flags),
              getattr(ev, "rows", None) and ev.rows[0].get("values"))
    proofs()
    with a.cursor() as cur:
        cur.execute("create user u1 identified by 'u1'")
        cur.execute("grant all on app.* to u1")
    a.commit()
    for extra in ("", "BINLOG ADMIN", "SUPER"):
        if extra:
            with a.cursor() as cur:
                try:
                    cur.execute(f"grant {extra} on *.* to u1")
                except Exception as e:
                    print("  grant", extra, first_line(e))
                    continue
            a.commit()
        u = conn("u1", "u1")
        with u.cursor() as cur:
            try:
                cur.execute("set skip_replication = 1")
                print("PLAIN + [", extra, "] skip_replication: ok")
            except Exception as e:
                print("PLAIN + [", extra, "] skip_replication:",
                      first_line(e))
            cur.execute("show grants")
            print("  grants", [g[0][:200] for g in cur.fetchall()])
        u.close()


if __name__ == "__main__":
    main()

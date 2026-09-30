"""The numbers behind `MSSQLEngine.WRITE_COSTS`, produced again.

Starts a disposable SQL Server (SQL Edge by default; the full 2022 server
with `MIGKIT_MSSQL_IMAGE=mcr.microsoft.com/mssql/server:2022-latest`, which
an arm64 machine runs under Rosetta through `tools/with_docker_lock.py --vm
migkit`), fills a table of seven columns (bigint key, nvarchar, decimal,
datetime2(7), varchar, varbinary, uniqueidentifier), and times the ways
rows go into another database of it:

* statements, in batches of 50,000 (what every engine without a load of
  its own writes by)
* the bulk load, staged and promoted, in the same batches
* the whole table read as Arrow and loaded as read (`native_bulk`)
* one batch of 1, 100 and 1,000 rows each way, twenty times, for the part
  each way costs every time it is used

    .venv/bin/python tools/with_docker_lock.py .venv/bin/python \\
        bench/sql_server_write_ways.py --rows 500000
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

NAME, PORT, PW = "migkit-bench-mssql", 16139, "CHANGE_ME-Bench-9x!"
BATCH = 50_000
DDL = ("(id bigint primary key, name nvarchar(40), amount decimal(18,2),"
       " at datetime2(7), note varchar(60), raw varbinary(64),"
       " u uniqueidentifier)")
COLS = [("amount", "decimal"), ("at", "timestamp"), ("id", "integer"),
        ("name", "text"), ("note", "text"), ("raw", "bytes"), ("u", "text")]


def sql(db, *statements):
    import pymssql
    conn = pymssql.connect(server="127.0.0.1", port=str(PORT), user="sa",
                           password=PW, database=db, autocommit=True,
                           tds_version="7.4", login_timeout=10)
    try:
        cur = conn.cursor()
        out = None
        for s in statements:
            cur.execute(s)
            out = cur.fetchall() if cur.description else None
        return out
    finally:
        conn.close()


def start():
    from tests.conftest import mssql_run
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(mssql_run(NAME, PORT, PW, memory_mb=2048), check=True,
                   capture_output=True)
    end = time.time() + 900
    while time.time() < end:
        try:
            sql("master", "select 1")
            return
        except Exception:
            time.sleep(2)
    raise SystemExit("SQL Server never answered")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=500_000)
    n = ap.parse_args().rows
    from migkit.config import Endpoint, Hop
    from migkit.engines.dbapi import DbapiRows
    from migkit.engines.mssql import MSSQLEngine
    start()
    try:
        for db in ("m", "mt"):
            sql("master", f"create database {db}")
            sql(db, f"create table dbo.big {DDL}")
        sql("m", f"with n as (select top {n} row_number() over (order by"
                 " (select null)) i from sys.all_objects a cross join"
                 " sys.all_objects b cross join sys.all_objects c)"
                 " insert into dbo.big select i, concat(N'name-', i),"
                 " i * 1.25, dateadd(second, i, '2024-01-01'),"
                 " replicate('x', i % 60), cast(i as varbinary(8)), newid()"
                 " from n")
        ep = Endpoint(host="127.0.0.1", port=PORT, user="sa", password=PW)
        eng = MSSQLEngine(Hop(name="bench", engine="mssql", source=ep,
                              target=ep, databases=["m"],
                              db_map={"m": "mt"}))
        batches = list(eng.neutral_batches("src", "m", "dbo.big", COLS,
                                           BATCH))
        # the engine's own write, held to the bulk load for every batch:
        # statements measured dearer than anything it could cost here
        eng.WRITE_COSTS = {"statements": (1.0, 1.0),
                           "bulk load": (0.0, 0.0)}

        def bulk(rows):
            eng.neutral_write("dst", "m", "dbo.big", COLS, rows)
            assert eng.write_way("dst", "m", "dbo.big", COLS,
                                 len(rows))[0] == "bulk load"

        def statements(rows):
            DbapiRows.neutral_write(eng, "dst", "m", "dbo.big", COLS, rows)

        out = {"image": os.environ.get("MIGKIT_MSSQL_IMAGE", "SQL Edge"),
               "rows": n}
        for label, go in (
                ("bulk load", lambda: [bulk(b) for b in batches]),
                ("read as Arrow and loaded as read",
                 lambda: eng._native_table("m", "dbo.big", None)),
                ("statements", lambda: [statements(b) for b in batches])):
            sql("mt", "truncate table dbo.big")
            began = time.monotonic()
            go()
            took = time.monotonic() - began
            assert sql("mt", "select count(*) from dbo.big")[0][0] == n
            out[label] = {"seconds": round(took, 2),
                          "rows a second": round(n / took)}
            print(label, out[label], flush=True)
        for label, way in (("statements", statements), ("bulk load", bulk)):
            for k in (1, 100, 1000):
                sql("mt", "truncate table dbo.big")
                began = time.monotonic()
                for _ in range(20):
                    way(batches[0][:k])
                out[f"{label}, a batch of {k}"] = round(
                    (time.monotonic() - began) / 20, 4)
        print(json.dumps(out, indent=1))
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", NAME],
                       capture_output=True)


if __name__ == "__main__":
    main()

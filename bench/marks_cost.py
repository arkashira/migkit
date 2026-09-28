"""What each two-way mark costs, measured through migkit's own applier and
reader (backlog R3: the rungs are ranked by this, not by taste).

    .venv/bin/python bench/marks_cost.py postgres   # postgres:16
    .venv/bin/python bench/marks_cost.py mysql      # mysql:8.4, GTIDs on
    .venv/bin/python bench/marks_cost.py mariadb    # mariadb:11

A disposable server on 127.0.0.1:15964. Each round applies N one-row
transactions through `neutral_apply` under each rung and under none, the
rungs in turn so a slow minute of the machine falls on all of them; the
rung's cost a transaction is its time less none's in the same round, the
median of the rounds, and the spread is the largest difference none shows
between rounds. The reader's cost: `neutral_changes` over what each rung
wrote, a transaction. Exact rungs are applied as a counter hop applies
them, each batch numbered.
"""
import json
import pathlib
import statistics
import subprocess
import sys
import time

# this checkout's migkit, not whichever the environment installed
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

KIND = sys.argv[1] if len(sys.argv) > 1 else "postgres"
N = int(sys.argv[2]) if len(sys.argv) > 2 else 300
ROUNDS = int(sys.argv[3]) if len(sys.argv) > 3 else 5
NAME, PORT = "migkit-test-rung-cost", 15964
RUNGS = {"postgres": ["none", "origin", "message", "table"],
         "mysql": ["none", "gtid_tag", "comment", "table"],
         "mariadb": ["none", "skip_flag", "table"]}[KIND]
EXACT = {"origin", "table", "gtid_tag"}


def sh(*a):
    return subprocess.run(a, capture_output=True, text=True)


def start():
    sh("docker", "rm", "-f", "-v", NAME)
    if KIND == "postgres":
        run = ["-e", "POSTGRES_PASSWORD=test", "-p", f"{PORT}:5432",
               "postgres:16", "-c", "wal_level=logical"]
    elif KIND == "mysql":
        run = ["-e", "MYSQL_ROOT_PASSWORD=test", "-p", f"{PORT}:3306",
               "mysql:8.4", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
               "--binlog-rows-query-log-events=ON",
               "--binlog-row-metadata=FULL"]
    else:
        run = ["-e", "MARIADB_ROOT_PASSWORD=test", "-p", f"{PORT}:3306",
               "mariadb:11", "--log-bin", "--binlog-format=ROW",
               "--binlog-row-metadata=FULL", "--binlog-row-image=FULL",
               "--server-id=1"]
    subprocess.run(["docker", "run", "-d", "--name", NAME, *run], check=True,
                   capture_output=True)


def engines():
    from migkit.config import Endpoint, Hop
    ep = Endpoint(host="127.0.0.1", port=PORT,
                  user="postgres" if KIND == "postgres" else "root",
                  password="test")

    def hop():
        return Hop(name="cost", engine="postgres" if KIND == "postgres"
                   else "mysql", source=ep, target=ep, databases=["app"],
                   options={"two_way": dict(TWO_WAY), "server_id": 7701})
    if KIND == "postgres":
        from migkit.engines.postgres import PostgresEngine as E
    else:
        from migkit.engines.mysql import MySQLEngine as E
    return E(hop()), E(hop())


TWO_WAY = {"on_conflict": "error"}


def ready(applier):
    for _ in range(90):
        try:
            if KIND == "postgres":
                applier._psql("dst", "postgres", "select 1")
            else:
                applier._q("dst", "select 1")
            return
        except Exception:
            time.sleep(2)


def setup(applier):
    if KIND == "postgres":
        applier._psql("dst", "postgres", "create database app")
        applier._psql("dst", "app", "create table t (id int primary key,"
                                     " v text, n int)")
    else:
        applier._q("dst", "create database app")
        applier._q("dst", "create table app.t (id int primary key, v text,"
                          " n int)")


def apply(applier, rung, base, batch0):
    from migkit import canon
    applier._mark_rung = None if rung == "none" else rung
    applier.hop.options["two_way"] = None if rung == "none" else TWO_WAY
    began = time.perf_counter()
    for i in range(N):
        if rung in EXACT:
            applier._batch_seen = json.dumps({"token": "x",
                                              "batch": batch0 + i})
        else:
            applier.__dict__.pop("_batch_seen", None)
        applier.neutral_apply("dst", "app", [canon.change(
            "insert", "t", {"id": base + i}, {"id": base + i, "v": "x",
                                              "n": i})])
    applier.hop.options["two_way"] = TWO_WAY
    return (time.perf_counter() - began) / N * 1e6


def main():
    start()
    applier, reader = engines()
    try:
        ready(applier)
        time.sleep(3)
        ready(applier)
        setup(applier)
        if KIND == "postgres":
            reader.change_point("src", "app")
        for r in RUNGS:
            if r != "none":
                why = applier.mark_prove("dst", "app", r)
                print("PROOF", r, why or "proved")
        token = reader.change_point("src", "app")
        took = {r: [] for r in RUNGS}
        read = {r: [] for r in RUNGS}
        base, batch0 = 0, 10 ** 6
        for _ in range(ROUNDS):
            for r in RUNGS:
                took[r].append(apply(applier, r, base, batch0))
                base += N
                batch0 += N
                began = time.perf_counter()
                got, token = reader.neutral_changes("src", "app", token,
                                                    limit=10 * N)
                read[r].append((time.perf_counter() - began) / N * 1e6)
                if r != "none" and got:
                    print("  LEAK", r, len(got), "changes came back")
        none = statistics.median(took["none"])
        spread = max(took["none"]) - min(took["none"])
        print(f"{KIND}: N={N} a round, {ROUNDS} rounds, none"
              f" {none:.0f} us/txn, spread {spread:.0f} us")
        for r in RUNGS:
            med = statistics.median(took[r])
            # against none in the same round: the machine's slow minutes
            # fall on both
            paired = [a - b for a, b in zip(took[r], took["none"])]
            print(f"  {r:9s} apply {med:7.0f} us/txn, paired"
                  f" +{statistics.median(paired):6.0f}"
                  f"  rounds {[round(x) for x in paired]}"
                  f"  reader {statistics.median(read[r]):6.1f} us/txn")
    finally:
        sh("docker", "rm", "-f", "-v", NAME)


if __name__ == "__main__":
    main()

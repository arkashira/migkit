"""The plan says what the target has free, where the target says
(backlog 7).

Measured on PostgreSQL: a target whose disk filled mid-load did not only
refuse the rows, it stopped altogether (`PANIC: could not write to file
"pg_wal/..."`). PostgreSQL and MySQL do not report their disk's free
space, and the plan does not guess it. MongoDB does (`dbStats`), so a
MongoDB target's plan says it, and says when it is not enough. A MongoDB
move's plan had no size at all before: the collections' own statistics
now give it one.
"""
import subprocess
import time


from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

MG, MG_PORT = "migkit-test-room-free", 15796


def test_the_line_says_free_space_and_when_it_is_short():
    from migkit import planner
    decisions = [planner.Decision("t", planner.BULK, "x")]
    facts = {"t": {"bytes": 3 * 2 ** 30, "index_bytes": 2 ** 30}}
    roomy = planner.size_line(decisions, facts, "mongodb", free=10 * 2 ** 30)
    assert "and has 10.0 GB free" in roomy, roomy
    assert "NOT ENOUGH" not in roomy
    short = planner.size_line(decisions, facts, "mongodb", free=2 ** 30)
    assert "has 1.0 GB free - NOT ENOUGH" in short, short
    silent = planner.size_line(decisions, facts, "postgres")
    assert "free" not in silent, silent


@needs_docker
def test_a_mongodb_target_reports_its_free_space():
    from migkit.engines.mongodb import MongoEngine
    subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", MG, "-p",
                        f"{MG_PORT}:27017", "mongo:7"], check=True,
                       capture_output=True)
        for _ in range(60):
            if subprocess.run(["docker", "exec", MG, "mongosh", "--quiet",
                               "--eval", "db.runCommand({ping:1}).ok"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(1)
        subprocess.run(["docker", "exec", MG, "mongosh", "--quiet", "app",
                        "--eval", "db.c.insertMany([{a: 1}, {a: 2}])"],
                       check=True, capture_output=True)
        ep = Endpoint(host="127.0.0.1", port=MG_PORT, user="", password="")
        eng = MongoEngine(Hop(name="f", engine="mongodb", source=ep,
                              target=ep, databases=["app"]))
        free = eng.free_bytes("dst", "app")
        df = subprocess.run(["docker", "exec", MG, "df", "-B1",
                             "/data/db"], capture_output=True,
                            text=True).stdout.splitlines()[-1].split()
        # the server's figure and the filesystem's own, a few MB apart
        assert free and abs(free - int(df[3])) < 64 * 2 ** 20, (free, df)
        facts = eng.table_facts("src", "app")
        assert facts["c"]["rows"] == 2 and facts["c"]["bytes"] > 0, facts
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)


def test_postgres_and_mysql_say_nothing_rather_than_guess():
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="127.0.0.1", port=1, user="u", password="p")
    for cls, engine in ((PostgresEngine, "postgres"), (MySQLEngine, "mysql")):
        assert cls(Hop(name="n", engine=engine, source=ep, target=ep)
                   ).free_bytes("dst", "x") is None

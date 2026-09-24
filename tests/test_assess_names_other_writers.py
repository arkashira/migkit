"""`assess` names replication that already writes into the target.

DTS fails a task whose objects another task writes to: two writers on one
table race, and whichever lands last survives. migkit named such a writer
only when a repair was about to run beside it. `assess` now names it
before the move, and says nothing - rather than "none found" - on an
engine whose target it cannot ask.
"""
import sqlite3
import subprocess

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql


def _pg(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="ow", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


ITEM = "no other replication writes into the target"


@needs_docker
def test_a_subscription_on_the_target_fails_it(pg_pair, tmp_path):
    src_ip = subprocess.run(
        ["docker", "inspect", "-f",
         "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
         "migkit-test-pg-src"], capture_output=True, text=True).stdout.strip()
    psql(pg_pair["src"], "create table public.t (id int primary key);"
                         " create publication ow_pub for table public.t")
    psql(pg_pair["dst"], "create table public.t (id int primary key)")
    eng = _pg(pg_pair, tmp_path)
    before = [i for i in eng._writer_items("postgres") if i["item"] == ITEM]
    assert [i["level"] for i in before] == ["pass"], before
    got = psql(pg_pair["dst"],
               "create subscription ow_sub connection"
               f" 'host={src_ip} port=5432 dbname=postgres user=postgres"
               " password=test' publication ow_pub with (copy_data = false)")
    assert got.returncode == 0, got.stderr
    try:
        items = [i for i in eng.assess() if i["item"] == ITEM]
    finally:
        psql(pg_pair["dst"], "drop subscription if exists ow_sub")
        psql(pg_pair["src"], "drop publication if exists ow_pub")
    assert [i["level"] for i in items] == ["fail"], items
    assert "subscription ow_sub already writes into it" in \
        items[0]["detail"], items


def test_an_engine_that_cannot_ask_says_nothing(tmp_path):
    from migkit.engines.sqlite import SQLiteEngine
    for name in ("a.db", "b.db"):
        con = sqlite3.connect(tmp_path / name)
        con.execute("create table t (id integer primary key)")
        con.commit()
        con.close()
    hop = Hop(name="lite", engine="sqlite",
              source=Endpoint(host=str(tmp_path / "a.db"), port=0, user="",
                              password=""),
              target=Endpoint(host=str(tmp_path / "b.db"), port=0, user="",
                              password=""), databases=["main"])
    hop.report_dir = lambda db=None: tmp_path
    assert not [i for i in SQLiteEngine(hop).assess() if i["item"] == ITEM]

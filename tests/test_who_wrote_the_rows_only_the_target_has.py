"""Rows only the target has: when they were written, against when the
move began.

The deep boundary check could flag a target ahead of its source but not
say why - a target that was not emptied, a stream applying twice, or
something writing to the target. PostgreSQL can tell the first from the
rest: a move now records the oldest transaction still running on the
target as it begins, and a row whose own transaction is older was already
there. Where the target keeps commit timestamps, the check gives the times
as well.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _eng(src, dst, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="ww", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=src, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=dst, user="postgres",
                              password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def test_rows_there_before_the_move_are_told_from_later_ones(pg_pair,
                                                             tmp_path):
    from migkit import cli
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.t (id int primary key, v text);"
                   " insert into public.t values (1, 'a'), (2, 'b')")
    psql(pg_pair["dst"], "insert into public.t values (100, 'left over')")
    eng = _eng(pg_pair["src"], pg_pair["dst"], tmp_path)
    cli._mark_move(eng.hop, eng, "postgres")
    psql(pg_pair["dst"], "insert into public.t values (101, 'written during')")
    got = [r for r in eng.check_data("postgres") if r.check == "data"]
    assert [r.status for r in got] == ["diff"], [r.__dict__ for r in got]
    said = got[0].detail
    assert "public.t: 2 rows only on the target" in said, said
    assert "1 written before the move of" in said, said
    assert "the target was not emptied of them" in said, said
    assert "1 written after it began" in said, said


def test_without_a_mark_it_says_it_cannot_tell(pg_pair, tmp_path):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.t (id int primary key)")
    psql(pg_pair["dst"], "insert into public.t values (7)")
    got = [r for r in _eng(pg_pair["src"], pg_pair["dst"], tmp_path)
           .check_data("postgres") if r.check == "data"]
    assert "when they were written is not known here" in got[0].detail, \
        got[0].detail


PORT, NAME = 15725, "migkit-test-committs"


@pytest.fixture
def stamped():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PORT}:5432",
                    "postgres:16", "-c", "track_commit_timestamp=on"],
                   check=True, capture_output=True)
    try:
        for _ in range(60):
            with socket.socket() as s:
                s.settimeout(2)
                ok = s.connect_ex(("127.0.0.1", PORT)) == 0
            if ok and subprocess.run(["docker", "exec", NAME, "pg_isready",
                                      "-U", "postgres"],
                                     capture_output=True).returncode == 0:
                break
            time.sleep(1)
        time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", NAME],
                       capture_output=True)


def _sql(sql):
    return subprocess.run(["docker", "exec", NAME, "psql", "-U", "postgres",
                           "-At", "-c", sql], capture_output=True, text=True)


def test_a_target_keeping_commit_times_gives_them(stamped, pg_pair,
                                                  tmp_path):
    psql(pg_pair["src"], "create table public.t (id int primary key)")
    assert _sql("create table public.t (id int primary key);"
                " insert into public.t values (5)").returncode == 0
    eng = _eng(pg_pair["src"], PORT, tmp_path)
    got = [r for r in eng.check_data("postgres") if r.check == "data"]
    today = time.strftime("%Y-%m-%d", time.gmtime())
    assert "committed between " + today in got[0].detail, got[0].detail


MG_SRC, MG_DST = "migkit-test-ww-mg-src", "migkit-test-ww-mg-dst"
MG_SRC_PORT, MG_DST_PORT = 15726, 15727


@pytest.fixture
def mongo_pair():
    import pymongo
    for n, p in ((MG_SRC, MG_SRC_PORT), (MG_DST, MG_DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n, "-p",
                        f"{p}:27017", "mongo:7"], check=True,
                       capture_output=True)
    try:
        for p in (MG_SRC_PORT, MG_DST_PORT):
            for _ in range(60):
                try:
                    pymongo.MongoClient(f"mongodb://127.0.0.1:{p}/",
                                        serverSelectionTimeoutMS=1000
                                        ).admin.command("ping")
                    break
                except Exception:
                    time.sleep(1)
        yield
    finally:
        for n in (MG_SRC, MG_DST):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def test_documents_only_the_target_has_are_dated_by_their_ids(mongo_pair,
                                                              tmp_path):
    """An ObjectId carries the second it was made."""
    import datetime

    import pymongo
    from bson import ObjectId

    from migkit import cli
    from migkit.engines.mongodb import MongoEngine
    hop = Hop(name="wm", engine="mongodb",
              source=Endpoint(host="127.0.0.1", port=MG_SRC_PORT),
              target=Endpoint(host="127.0.0.1", port=MG_DST_PORT),
              databases=["shop"])
    hop.report_dir = lambda db=None: tmp_path
    eng = MongoEngine(hop)
    src = pymongo.MongoClient(f"mongodb://127.0.0.1:{MG_SRC_PORT}/")["shop"]
    dst = pymongo.MongoClient(f"mongodb://127.0.0.1:{MG_DST_PORT}/")["shop"]
    both = [{"_id": ObjectId(), "v": 1}, {"_id": ObjectId(), "v": 2}]
    src.orders.insert_many(both)
    dst.orders.insert_many(both)
    cli._mark_move(hop, eng, "shop")
    now = datetime.datetime.now(datetime.timezone.utc)
    dst.orders.insert_many([
        {"_id": ObjectId.from_datetime(now - datetime.timedelta(days=3)),
         "v": "left over"},
        {"_id": ObjectId.from_datetime(now + datetime.timedelta(minutes=1)),
         "v": "written during"},
        {"_id": "order-77", "v": "its own id"}])
    got = [r for r in eng.check_data("shop") if r.check == "data"
           and r.status == "diff"]
    said = " ".join(r.detail for r in got)
    assert "3 documents only on the target" in said, said
    assert "1 with ids made before the move of" in said, said
    assert "1 with ids made after it began" in said, said
    assert "1 with ids that carry no time" in said, said

"""Asking each engine for named rows, so a cross-engine drilldown can exist.

`check` on a cross-engine pair compares one digest per table. When the two
disagree it can say the table differs and nothing more, because finding out
*which* rows differ means lining the two sides up - and the obvious way to do
that, walking both in key order and merging, is wrong here. Two engines do not
agree on the order of a text key; one collation difference is enough to make a
merge report rows missing on one side and extra on the other with neither being
true.

Asking both sides for the same keys asks a question with one answer. This file
pins that question on all four engines that carry rows, including the part that
makes the answers comparable: the map is keyed by the canonical text of the
key, so the same logical row lines up whatever Python object each driver hands
back.
"""
import socket
import sqlite3
import subprocess
import time

import pytest

from migkit import canon
from migkit.config import Endpoint, Hop

MY, MG = "migkit-test-bk-my", "migkit-test-bk-mg"
MY_PORT, MG_PORT = 13381, 27088

# a quote, a colon and brackets in the key, because the key travels into a
# query and into the canonical text, and both have been broken by punctuation
AWKWARD = 'a:b["x"]'


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(),
                                 reason="docker not available")]


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def my_sql(sql, db=None):
    cmd = ["docker", "exec", MY, "mysql", "-uroot", "-ptest",
           "--default-character-set=utf8mb4", "-N", "-B"]
    if db:
        cmd += ["-D", db]
    return subprocess.run(cmd + ["-e", sql], capture_output=True, text=True)


@pytest.fixture(scope="module")
def mysql_server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8"], check=True, capture_output=True)
    assert _wait(MY_PORT)
    for _ in range(90):
        # over TCP: the image's first server answers on its socket only,
        # then stops for the real one - a `select 1` through the socket
        # passed in that window and the table made next was never made
        if subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                           "-h127.0.0.1", "--protocol=tcp", "-e",
                           "select 1"], capture_output=True).returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("mysql never accepted a connection")
    yield
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


@pytest.fixture(scope="module")
def mongo_server():
    subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MG, "-p",
                    f"{MG_PORT}:27017", "mongo:7"], check=True,
                   capture_output=True)
    assert _wait(MG_PORT)
    for _ in range(40):
        r = subprocess.run(["docker", "exec", MG, "mongosh", "--quiet",
                            "--eval", "db.runCommand({ping:1}).ok"],
                           capture_output=True, text=True)
        if r.returncode == 0 and "1" in r.stdout:
            break
        time.sleep(1)
    else:
        pytest.fail("mongo never answered")
    yield
    subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)


def _cols(engine, side, db, table):
    """The (name, class) pairs the comparison uses, sorted by name."""
    out = []
    for name, declared in engine.neutral_columns(side, db, table):
        cls, _ = canon.comparable(engine.CANON_ENGINE, declared)
        if cls:
            out.append((name, cls))
    return sorted(out)


# ---------------------------------------------------------------- sqlite

def _sqlite(tmp_path):
    from migkit.engines.sqlite import SQLiteEngine
    path = tmp_path / "bk.db"
    conn = sqlite3.connect(path)
    conn.execute("create table t (id integer primary key, label text,"
                 " v text)")
    conn.executemany("insert into t values (?,?,?)",
                     [(1, AWKWARD, "one"), (2, "plain", None),
                      (3, "third", "three")])
    conn.commit()
    conn.close()

    def ep(p):
        return Endpoint(host=str(p), port=0, user="", password="")
    return SQLiteEngine(Hop(name="b", engine="sqlite", source=ep(path),
                            target=ep(path)))


def test_sqlite_answers_for_exactly_the_keys_it_was_given(tmp_path):
    eng = _sqlite(tmp_path)
    cols = _cols(eng, "src", "main", "t")
    got = eng.neutral_rows_by_key("src", "main", "t", cols, ["id"],
                                  [(1,), (3,), (99,)])
    assert sorted(got) == [("1",), ("3",)], sorted(got)
    at = {n: i for i, (n, _) in enumerate(cols)}
    assert got[("1",)][at["label"]] == AWKWARD
    assert got[("3",)][at["v"]] == "three"
    # a key that is not there is absent, not a row of nulls
    assert ("99",) not in got


def test_nothing_asked_is_nothing_returned(tmp_path):
    eng = _sqlite(tmp_path)
    cols = _cols(eng, "src", "main", "t")
    assert eng.neutral_rows_by_key("src", "main", "t", cols, ["id"], []) == {}
    assert eng.neutral_rows_by_key("src", "main", "t", cols, [],
                                   [(1,)]) == {}


def test_a_composite_key_is_matched_on_both_parts(tmp_path):
    from migkit.engines.sqlite import SQLiteEngine
    path = tmp_path / "c.db"
    conn = sqlite3.connect(path)
    conn.execute("create table c (a text, b integer, v text,"
                 " primary key (a, b))")
    conn.executemany("insert into c values (?,?,?)",
                     [(AWKWARD, 1, "first"), (AWKWARD, 2, "second"),
                      ("other", 1, "third")])
    conn.commit()
    conn.close()

    def ep(p):
        return Endpoint(host=str(p), port=0, user="", password="")
    eng = SQLiteEngine(Hop(name="b", engine="sqlite", source=ep(path),
                           target=ep(path)))
    cols = _cols(eng, "src", "main", "c")
    got = eng.neutral_rows_by_key("src", "main", "c", cols, ["a", "b"],
                                  [(AWKWARD, 2), ("other", 1)])
    assert sorted(got) == [(AWKWARD, "2"), ("other", "1")], sorted(got)
    at = {n: i for i, (n, _) in enumerate(cols)}
    assert got[(AWKWARD, "2")][at["v"]] == "second"
    # the other row sharing the first key part is not dragged in with it
    assert (AWKWARD, "1") not in got


# ------------------------------------------------------------- postgres

def _pg(ports, tmp_path):
    import migkit.config as cfg
    from migkit.engines.postgres import PostgresEngine
    cfg.REPORTS = tmp_path / "reports"
    return PostgresEngine(Hop(
        name="b", engine="postgres",
        source=Endpoint(host="127.0.0.1", port=ports["src"], user="postgres",
                        password="test"),
        target=Endpoint(host="127.0.0.1", port=ports["dst"], user="postgres",
                        password="test"),
        databases=["postgres"]))


def test_postgres_answers_for_exactly_the_keys_it_was_given(pg_pair,
                                                            tmp_path):
    from tests.conftest import psql
    psql(pg_pair["src"],
         "create table t (id bigint primary key, label text, v text);"
         f" insert into t values (1, $${AWKWARD}$$, 'one'),"
         " (2, 'plain', null), (3, 'third', 'three');")
    eng = _pg(pg_pair, tmp_path)
    cols = _cols(eng, "src", "postgres", "public.t")
    got = eng.neutral_rows_by_key("src", "postgres", "public.t", cols, ["id"],
                                  [(1,), (3,), (99,)])
    assert sorted(got) == [("1",), ("3",)], sorted(got)
    at = {n: i for i, (n, _) in enumerate(cols)}
    assert got[("1",)][at["label"]] == AWKWARD


def test_the_canonical_key_is_the_same_on_two_different_engines(pg_pair,
                                                                tmp_path):
    """The whole reason the map is keyed by text: the two sides hand back
    the same logical key as different Python objects, and a drilldown that
    matched on those would find nothing in common."""
    from tests.conftest import psql
    psql(pg_pair["src"],
         "create table k (id numeric primary key, v text);"
         " insert into k values (7, 'seven');")
    eng = _pg(pg_pair, tmp_path)
    cols = _cols(eng, "src", "postgres", "public.k")
    pg_got = eng.neutral_rows_by_key("src", "postgres", "public.k", cols,
                                     ["id"], [(7,)])

    lite = _sqlite(tmp_path)
    lite_cols = _cols(lite, "src", "main", "t")
    lite_got = lite.neutral_rows_by_key("src", "main", "t", lite_cols, ["id"],
                                        [(3,)])
    assert list(pg_got)[0] == ("7",), list(pg_got)
    assert list(lite_got)[0] == ("3",), list(lite_got)
    # postgres handed back a Decimal and sqlite an int for the id column
    pg_at = {n: i for i, (n, _) in enumerate(cols)}
    lite_at = {n: i for i, (n, _) in enumerate(lite_cols)}
    assert type(pg_got[("7",)][pg_at["id"]]) is not type(
        lite_got[("3",)][lite_at["id"]])


# ---------------------------------------------------------------- mysql

def test_mysql_answers_for_exactly_the_keys_it_was_given(mysql_server,
                                                         tmp_path):
    from migkit.engines.mysql import MySQLEngine
    for sql, db in (("create database if not exists bk", None),
                    ("drop table if exists t;"
                     " create table t (id bigint primary key,"
                     " label varchar(64), v varchar(64));", "bk"),
                    (f"insert into t values (1, '{AWKWARD}', 'one'),"
                     " (2, 'plain', null), (3, 'third', 'three');", "bk")):
        got = my_sql(sql, db=db)
        assert got.returncode == 0, got.stderr
    ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                  password="test")
    eng = MySQLEngine(Hop(name="b", engine="mysql", source=ep, target=ep,
                          db_map={"bk": "bk"}))
    cols = _cols(eng, "src", "bk", "t")
    got = eng.neutral_rows_by_key("src", "bk", "t", cols, ["id"],
                                  [(1,), (3,), (99,)])
    assert sorted(got) == [("1",), ("3",)], sorted(got)
    at = {n: i for i, (n, _) in enumerate(cols)}
    assert got[("1",)][at["label"]] == AWKWARD


# -------------------------------------------------------------- mongodb

def test_mongodb_answers_for_exactly_the_keys_it_was_given(mongo_server):
    from pymongo import MongoClient
    from migkit.engines.mongodb import MongoEngine
    client = MongoClient(f"mongodb://127.0.0.1:{MG_PORT}/"
                         "?directConnection=true")
    client.drop_database("bk")
    client["bk"]["t"].insert_many([
        {"_id": 1, "label": AWKWARD, "v": "one"},
        {"_id": 2, "label": "plain", "v": None},
        {"_id": 3, "label": "third"},          # v is absent, not null
    ])
    ep = Endpoint(host="127.0.0.1", port=MG_PORT, user="", password="",
                  options={"uri_options": "directConnection=true"})
    eng = MongoEngine(Hop(name="b", engine="mongodb", source=ep, target=ep,
                          db_map={"bk": "bk"}))
    cols = _cols(eng, "src", "bk", "t")
    got = eng.neutral_rows_by_key("src", "bk", "t", cols, ["_id"],
                                  [(1,), (3,), (99,)])
    assert sorted(got) == [("1",), ("3",)], sorted(got)
    at = {n: i for i, (n, _) in enumerate(cols)}
    assert got[("1",)][at["label"]] == AWKWARD
    # the field that is not there stays told apart from one holding null
    assert got[("3",)][at["v"]] is canon.ABSENT
    assert got[("1",)][at["v"]] == "one"
    client.close()


def test_every_engine_that_can_read_rows_can_read_them_by_key():
    """A contract with three of four engines behind it is not a contract -
    the cross-engine drilldown would work on some pairs and not others."""
    from migkit.engines.base import Engine
    from migkit.engines.mongodb import MongoEngine
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    from migkit.engines.sqlite import SQLiteEngine
    for cls in (PostgresEngine, MySQLEngine, SQLiteEngine, MongoEngine):
        assert cls.neutral_read is not Engine.neutral_read, cls
        assert cls.neutral_rows_by_key is not Engine.neutral_rows_by_key, cls

"""A MongoDB document moved into PostgreSQL and back keeps every BSON type
it had, and a check of either hop compares type as well as value
(type-fidelity G5, docs/research/type-fidelity-2026-09-28.md).

Before: an ObjectId, a `Timestamp`, a regular expression, MinKey and
MaxKey, a document and an array had no class, so a table holding one
could not be built and the move stopped; a UUID (binary subtype 4)
landed as plain bytes and came back subtype 0; a `long` came back an
`int`. And a table made by hand - `_id` as the ObjectId's 24 hex digits,
a 32-bit field in a `bigint` - compared equal to the collection, the
ObjectId rendered as its hex and 5 as 5, so the round trip lost the types
and every check on the way said nothing.
"""
import datetime
import socket
import subprocess
import time
import uuid

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker
from tests.typepair import Checkpoint

pytestmark = needs_docker

MG, PG = "migkit-test-ty2-mg", "migkit-test-ty2-mpg"
MG_PORT, PG_PORT = 16212, 16213
U = uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")


def _up(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def pg(sql, db="cx"):
    got = subprocess.run(["docker", "exec", "-i", PG, "psql", "-U",
                          "postgres", "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _mongo():
    from pymongo import MongoClient
    return MongoClient(f"mongodb://127.0.0.1:{MG_PORT}/"
                       "?directConnection=true")


def _docs():
    from bson import Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
    from bson.binary import Binary
    from bson.decimal128 import Decimal128
    one = {"_id": ObjectId("65a1b2c3d4e5f6a7b8c9d0e1"), "i": 5,
           "l": Int64(5), "d": 5.0, "dec": Decimal128("1.50"),
           "u": Binary.from_uuid(U), "legacy": Binary(U.bytes, 3),
           "raw": b"\x00\xff", "at": datetime.datetime(2024, 1, 1, 0, 0, 0,
                                                        123000),
           "ts": Timestamp(1704067200, 7), "re": Regex("a.*", "i"),
           "lo": MinKey(), "hi": MaxKey(),
           "doc": {"z": 1, "a": Int64(2), "m": [1.5, "x"]},
           "arr": [1, "x", Int64(3)], "mixed": 1}
    two = dict(one, _id=ObjectId("65a1b2c3d4e5f6a7b8c9d0e2"), i=-7,
               l=Int64(2 ** 40), dec=Decimal128("-0.50"), mixed="one",
               doc={"b": None}, arr=[])
    return [one, two]


@pytest.fixture(scope="module")
def servers():
    for n in (MG, PG):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", MG, "-p",
                        f"127.0.0.1:{MG_PORT}:27017", "mongo:7"],
                       check=True, capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                        "POSTGRES_PASSWORD=test", "-p",
                        f"127.0.0.1:{PG_PORT}:5432", "postgres:16"],
                       check=True, capture_output=True)
        assert _up(MG_PORT) and _up(PG_PORT)
        for _ in range(60):
            try:
                _mongo().admin.command("ping")
                break
            except Exception:  # noqa: BLE001 - not up yet
                time.sleep(2)
        for _ in range(60):
            if subprocess.run(["docker", "exec", PG, "pg_isready", "-U",
                               "postgres"], capture_output=True
                              ).returncode == 0:
                break
            time.sleep(2)
        time.sleep(2)
        yield
    finally:
        for n in (MG, PG):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


@pytest.fixture
def fresh(servers):
    client = _mongo()
    for name in ("cx", "back"):
        client.drop_database(name)
    client["cx"]["src"].insert_many(_docs())
    pg("drop database if exists cx with (force)", db="postgres")
    pg("create database cx", db="postgres")
    yield client


def _hop(tmp_path, source, target, **options):
    mongo = Endpoint(host="127.0.0.1", port=MG_PORT, user="", password="",
                     options={"uri_options": "directConnection=true"})
    post = Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                    password="test")
    hop = Hop(name="g5", engine="hetero",
              source=mongo if source == "mongodb" else post,
              target=mongo if target == "mongodb" else post,
              databases=["cx"],
              db_map={"cx": "back"} if target == "mongodb" else {},
              options={"source_engine": source, "target_engine": target,
                       **options})
    hop.report_dir = lambda db=None: tmp_path
    from migkit.engines.hetero import HeteroEngine
    return HeteroEngine(hop)


def _verdict(eng, table):
    got = [r for r in eng.check_data("cx")
           if r.check == "data" and r.scope.endswith(f".{table}")]
    assert len(got) == 1, [(r.scope, r.status, r.detail) for r in got]
    return got[0]


def _typed(doc):
    """Each field's canonical Extended JSON: the type and the value."""
    from bson import json_util
    return {k: json_util.dumps(v, json_options=json_util
                               .CANONICAL_JSON_OPTIONS)
            for k, v in doc.items()}


def test_a_document_comes_back_from_postgresql_with_every_type(fresh,
                                                              tmp_path):
    there = _hop(tmp_path / "a", "mongodb", "postgres")
    (tmp_path / "a").mkdir()
    there.move_table("cx", "", "src", 100, Checkpoint(), [].append)
    # what the table holds: a column that records each type
    cols = dict(line.split("|") for line in pg(
        "select attname || '|' || format_type(atttypid, atttypmod) from"
        " pg_attribute where attrelid = 'src'::regclass and attnum > 0"
        " and not attisdropped").splitlines())
    assert cols["i"] == "integer" and cols["l"] == "bigint", cols
    assert cols["u"] == "uuid" and cols["_id"] == "jsonb", cols
    assert cols["doc"] == "json" and cols["ts"] == "jsonb", cols
    assert pg("select _id::text from src order by _id::text limit 1") \
        == '{"$oid": "65a1b2c3d4e5f6a7b8c9d0e1"}'
    got = _verdict(there, "src")
    assert got.status == "ok", got.detail

    back = _hop(tmp_path / "b", "postgres", "mongodb")
    (tmp_path / "b").mkdir()
    back.move_table("cx", "public", "src", 100, Checkpoint(), [].append)
    got = _verdict(back, "src")
    assert got.status == "ok", got.detail

    was = {d["_id"]: _typed(d) for d in fresh["cx"]["src"].find()}
    now = {d["_id"]: _typed(d) for d in fresh["back"]["src"].find()}
    assert set(was) == set(now) and len(was) == 2
    for key in was:
        assert was[key] == now[key], (key, {
            k: (was[key][k], now[key].get(k)) for k in was[key]
            if was[key][k] != now[key].get(k)})


def test_a_type_changed_on_either_hop_is_found(fresh, tmp_path):
    from bson import Int64
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    there = _hop(tmp_path / "a", "mongodb", "postgres")
    there.move_table("cx", "", "src", 100, Checkpoint(), [].append)
    back = _hop(tmp_path / "b", "postgres", "mongodb")
    back.move_table("cx", "public", "src", 100, Checkpoint(), [].append)
    assert _verdict(there, "src").status == "ok"
    assert _verdict(back, "src").status == "ok"
    # the same 5, as a 64-bit integer where the column holds 32 bits
    fresh["cx"]["src"].update_one({"i": 5}, {"$set": {"i": Int64(5)}})
    got = _verdict(there, "src")
    assert got.status == "diff", got.detail
    # and back in MongoDB, a 64-bit 5 now a 32-bit one
    fresh["back"]["src"].update_one({"i": 5}, {"$set": {"l": 5}})
    got = _verdict(back, "src")
    assert got.status == "diff", got.detail


def test_a_table_made_by_hand_is_held_to_the_types(fresh, tmp_path):
    """The recipe of G5: `_id` kept as the ObjectId's hex, a 32-bit and a
    64-bit field in `bigint`s - what a hand-written migration makes."""
    from bson import Int64, ObjectId
    fresh["cx"]["h"].insert_many([
        {"_id": ObjectId("65a1b2c3d4e5f6a7b8c9d0e1"), "i": 5,
         "l": Int64(5)},
        {"_id": ObjectId("65a1b2c3d4e5f6a7b8c9d0e2"), "i": 6,
         "l": Int64(6)}])
    pg("create table h (_id text primary key, i bigint, l bigint);"
       " insert into h values ('65a1b2c3d4e5f6a7b8c9d0e1', 5, 5),"
       " ('65a1b2c3d4e5f6a7b8c9d0e2', 6, 6)")
    eng = _hop(tmp_path, "mongodb", "postgres")
    got = _verdict(eng, "h")
    assert got.status == "diff", got.detail
    with pytest.raises(SystemExit) as refused:
        eng.move_table("cx", "", "h", 100, Checkpoint(), [].append)
    said = str(refused.value)
    assert "h._id: 2 rows hold a value of another BSON type than the" \
           " string" in said, said
    assert "h.i: 2 rows hold" in said and "h.l" not in said, said
    assert "nothing was copied" in said

    # what the hop says may change: compared, and moved, by value
    let = _hop(tmp_path, "mongodb", "postgres",
               accept_changes=["h._id", "h.i"])
    got = _verdict(let, "h")
    assert got.status == "ok", got.detail
    pg("delete from h")
    let.move_table("cx", "", "h", 100, Checkpoint(), [].append)
    assert pg("select _id || ':' || i || ':' || l from h order by _id") \
        == ("65a1b2c3d4e5f6a7b8c9d0e1:5:5\n"
            "65a1b2c3d4e5f6a7b8c9d0e2:6:6")
    assert _verdict(let, "h").status == "ok"

"""A saved change position resumed only on the source it was taken on
(backlog 44).

A position belongs to one server's log. The source is rebuilt under the
same address here, which is what a failover to a server with a log of its
own looks like from the tail: MySQL gets a new server id, PostgreSQL a new
system identifier, MongoDB a new replica set id under the same set name.
Each time, the tail stops before applying anything and says why.

MongoDB's oplog is also asked whether it still reaches back to the
position, and whether the position is later than the set's own clock.
Both are asked before the stream opens.
"""
import json
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = [pytest.mark.docker]

MG, MG_PORT = "migkit-test-srcid-mongo", 15829
MY, MY_PORT = "migkit-test-srcid-mysql", 15830
PG, PG_PORT = "migkit-test-srcid-pg", 15831


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _sh(*a):
    return subprocess.run(list(a), capture_output=True, text=True)


def _port_open(port, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


def _mongo_up():
    import pymongo
    _sh("docker", "rm", "-f", "-v", MG)
    _sh("docker", "run", "-d", "--name", MG, "-p", f"{MG_PORT}:27017",
        "mongo:7", "--replSet", "rs0", "--bind_ip_all")
    assert _port_open(MG_PORT)
    c = pymongo.MongoClient(port=MG_PORT, directConnection=True,
                            serverSelectionTimeoutMS=60000)
    for _ in range(60):
        try:
            c.admin.command("replSetInitiate", {
                "_id": "rs0", "members": [{"_id": 0,
                                           "host": "127.0.0.1:27017"}]})
            break
        except Exception as e:
            if "already initialized" in str(e):
                break
            time.sleep(1)
    for _ in range(60):
        if c.admin.command("hello").get("isWritablePrimary"):
            break
        time.sleep(1)
    c.app.t.insert_one({"_id": 1})
    return c


def _mysql_up():
    _sh("docker", "rm", "-f", "-v", MY)
    _sh("docker", "run", "-d", "--name", MY, "-e", "MYSQL_ROOT_PASSWORD=test",
        "-p", f"{MY_PORT}:3306", "mysql:8.4")
    for _ in range(90):
        if _sh("docker", "exec", MY, "mysql", "-uroot", "-ptest",
               "-h127.0.0.1", "--protocol=tcp", "-e",
               "create database if not exists app").returncode == 0:
            break
        time.sleep(2)
    assert _port_open(MY_PORT)


def _pg_up():
    _sh("docker", "rm", "-f", "-v", PG)
    _sh("docker", "run", "-d", "--name", PG, "-e", "POSTGRES_PASSWORD=test",
        "-p", f"{PG_PORT}:5432", "postgres:16")
    for _ in range(60):
        if _sh("docker", "exec", PG, "psql", "-U", "postgres", "-c",
               "select 1").returncode == 0:
            break
        time.sleep(1)
    assert _port_open(PG_PORT)
    time.sleep(2)


@pytest.fixture(scope="module")
def docker_ok():
    if not _docker():
        pytest.skip("docker not available")
    yield
    for n in (MG, MY, PG):
        _sh("docker", "rm", "-f", "-v", n)


def _mongo_engine():
    from migkit.engines.mongodb import MongoEngine
    ep = Endpoint(host="127.0.0.1", port=MG_PORT, user="", password="",
                  options={"uri_options": "directConnection=true"})
    return MongoEngine(Hop(name="sid", engine="mongodb", source=ep,
                           target=ep, databases=["app"]))


def _token_at(real, seconds):
    """`real` with its cluster time moved to `seconds`."""
    raw = bytearray(bytes.fromhex(real))
    raw[1:5] = int(seconds).to_bytes(4, "big")
    return raw.hex().upper()


def test_mongodb_position_lost_ahead_and_from_another_set(docker_ok,
                                                          tmp_path):
    from migkit import tailctl
    client = _mongo_up()
    eng = _mongo_engine()
    token = eng.change_point("src", "app")
    path = tmp_path / "tail-token.json"
    path.write_text(json.dumps({"_data": token}))
    tailctl.same_source(eng, "app", path, None)
    # the same set, the position still held: nothing to say
    tailctl.same_source(eng, "app", path, {"_data": token})
    oldest = next(client.local["oplog.rs"].find().sort("$natural", 1)
                  .limit(1))["ts"].time
    with pytest.raises(SystemExit) as e:
        tailctl.same_source(eng, "app", path,
                            {"_data": _token_at(token, oldest - 100)})
    assert "the changes between them are gone from the source" in \
        str(e.value) and "Nothing was applied" in str(e.value)
    with pytest.raises(SystemExit) as e:
        tailctl.same_source(eng, "app", path,
                            {"_data": _token_at(token, time.time() + 3600)})
    assert "it was taken on another replica set" in str(e.value)
    # the same address, the same set name, a set built again
    client.close()
    _mongo_up().close()
    with pytest.raises(SystemExit) as e:
        tailctl.same_source(eng, "app", path, {"_data": token})
    said = str(e.value)
    assert "belongs to another source" in said and "rs0" in said, said


def test_the_mongodb_tail_stops_before_it_opens_the_stream(docker_ok,
                                                           tmp_path):
    eng = _mongo_engine()
    token = eng.change_point("src", "app")
    path = tmp_path / "tail-token.json"
    path.write_text(json.dumps({"_data": _token_at(token, 1000)}))
    said = []
    with pytest.raises(SystemExit) as e:
        eng.tail_apply("app", True, path, said.append)
    assert "gone from the source" in str(e.value), str(e.value)
    assert said == [], said


def test_mysql_and_postgresql_rebuilt_under_the_same_address(docker_ok,
                                                            tmp_path):
    from migkit import tailctl
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    _mysql_up()
    _pg_up()
    my_ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                     password="test")
    pg_ep = Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                     password="test")
    engines = {
        "mysql": (MySQLEngine(Hop(name="m", engine="mysql", source=my_ep,
                                  target=my_ep, databases=["app"])),
                  "app", _mysql_up, "server"),
        "postgres": (PostgresEngine(Hop(name="p", engine="postgres",
                                        source=pg_ep, target=pg_ep,
                                        databases=["postgres"])),
                     "postgres", _pg_up, "cluster"),
    }
    for name, (eng, db, rebuild, word) in engines.items():
        path = tmp_path / name / "tail-token.json"
        path.parent.mkdir()
        token = eng.change_point("src", db) if name == "mysql" else "0/0"
        path.write_text(json.dumps({"token": token}))
        tailctl.same_source(eng, db, path, None)
        tailctl.same_source(eng, db, path, token)
        rebuild()
        with pytest.raises(SystemExit) as e:
            tailctl.same_source(eng, db, path, token)
        said = str(e.value)
        assert "belongs to another source" in said and word in said, said

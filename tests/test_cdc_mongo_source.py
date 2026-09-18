"""MongoDB as the change source, PostgreSQL as the target.

The third source, and the one whose log is least like the other two: a change
stream is a live cursor over the oplog rather than a file position or an LSN,
so it only exists on a replica set and it only remembers where it was through
a token the caller keeps.

Two of its behaviours are load-bearing and neither is obvious, so both are
measured here rather than assumed: an `update` event carries only the delta
unless the stream was opened with `updateLookup`, and `$unset` is reported
separately from the document rather than by the field simply not being there.
"""
import socket
import subprocess
import time

import pytest

MG, PG = "migkit-test-mgcdc", "migkit-test-mgcdc-pg"
MG_PORT, PG_PORT = 27069, 15455


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


def mongosh(script, db="cx", container=MG):
    return subprocess.run(
        ["docker", "exec", container, "mongosh", "--quiet", db,
         "--eval", script], capture_output=True, text=True)


def pg_sql(sql, db="cx"):
    return subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=test", PG, "psql", "-U",
         "postgres", "-d", db, "-At", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)


def _hop(engine, port, user, password):
    from migkit.config import Endpoint, Hop
    options = {}
    if engine == "mongodb":
        # the replica set names its member by the address it sees from
        # inside the container, so a client that discovers the topology
        # follows that name to a port nothing is listening on out here.
        # `directConnection` is how migkit reaches a member through a
        # forward or a bastion, which is the same situation.
        options = {"uri_options": "directConnection=true"}
    ep = Endpoint(host="127.0.0.1", port=port, user=user, password=password,
                  options=options)
    return Hop(name="mgcdc", engine=engine, source=ep, target=ep,
               db_map={"cx": "cx"})


@pytest.fixture(scope="module")
def pair():
    for n in (MG, PG):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MG, "-p",
                    f"{MG_PORT}:27017", "mongo:7", "--replSet", "rs0",
                    "--bind_ip_all"], check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PG_PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    assert _wait(MG_PORT) and _wait(PG_PORT)
    for _ in range(40):
        if mongosh("db.runCommand({ping:1}).ok", "admin").returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("mongo never answered")
    mongosh('rs.initiate({_id:"rs0",members:'
            '[{_id:0,host:"127.0.0.1:27017"}]})', "admin")
    for _ in range(30):
        if "PRIMARY" in mongosh("rs.status().myState === 1 ? 'PRIMARY' : 'no'",
                                "admin").stdout:
            break
        time.sleep(2)
    else:
        pytest.fail("the replica set never became primary")
    for _ in range(40):
        if pg_sql("select 1", "postgres").returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("postgres never answered")
    assert pg_sql("create database cx", "postgres").returncode == 0
    assert pg_sql('create table t ("_id" bigint primary key, name text,'
                  " n bigint, keep text)").returncode == 0

    from migkit.engines.mongodb import MongoEngine
    from migkit.engines.postgres import PostgresEngine
    yield (MongoEngine(_hop("mongodb", MG_PORT, "", "")),
           PostgresEngine(_hop("postgres", PG_PORT, "postgres", "test")))
    for n in (MG, PG):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def test_a_standalone_is_refused_with_the_way_out(pair):
    """A change stream reads the oplog, and a standalone has none. The
    message is the two commands that fix it, because nothing client-side
    can."""
    other = "migkit-test-mgcdc-solo"
    subprocess.run(["docker", "rm", "-f", other], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", other, "-p",
                    "27068:27017", "mongo:7"], check=True,
                   capture_output=True)
    try:
        assert _wait(27068)
        for _ in range(40):
            if mongosh("db.runCommand({ping:1}).ok", "admin",
                       other).returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail("the standalone never answered")
        from migkit.engines.mongodb import MongoEngine
        eng = MongoEngine(_hop("mongodb", 27068, "", ""))
        with pytest.raises(SystemExit) as e:
            eng.neutral_changes("src", "cx")
        assert "standalone" in str(e.value)
        assert "--replSet rs0" in str(e.value)
        assert "rs.initiate()" in str(e.value)
    finally:
        subprocess.run(["docker", "rm", "-f", other], capture_output=True)


def test_an_update_without_updatelookup_carries_only_the_delta(pair):
    """The measurement behind opening the stream with `updateLookup`. Left at
    the default, an update event has no `fullDocument` at all - applying it to
    a row that is missing on the target would insert the key and one field."""
    got = mongosh(
        'const cs = db.probe.watch();'
        ' db.probe.insertOne({_id:1, a:1, b:"keep"});'
        ' db.probe.updateOne({_id:1},{$set:{a:2}});'
        ' let ev, out = [];'
        ' while ((ev = cs.tryNext())) {'
        '   out.push(ev.operationType + "=" +'
        '            (ev.fullDocument ? "full" : "none")) }'
        ' print(out.join(","))').stdout.strip()
    assert got.endswith("insert=full,update=none"), got
    mongosh("db.probe.drop()")


def test_the_three_row_operations_arrive_as_neutral_records(pair):
    mg, _ = pair
    _, token = mg.neutral_changes("src", "cx")
    assert token, "the first call must hand back a token to resume from"
    mongosh('db.t.insertOne({_id:1, name:"ca\'fé : [x]", n:1, keep:"k"})')
    mongosh('db.t.updateOne({_id:1},{$set:{n:2}})')
    changes, token = mg.neutral_changes("src", "cx", token)
    assert [c["op"] for c in changes] == ["insert", "update"], changes
    assert changes[0]["table"] == "t"
    assert changes[0]["key"] == {"_id": 1}
    assert changes[0]["values"]["name"] == "ca'fé : [x]"
    # the update carries the whole document, not just `n`
    assert changes[1]["values"]["keep"] == "k", changes[1]
    assert changes[1]["values"]["n"] == 2

    mongosh("db.t.deleteOne({_id:1})")
    changes, token = mg.neutral_changes("src", "cx", token)
    assert [c["op"] for c in changes] == ["delete"], changes
    assert changes[0]["values"] == {}


def test_an_update_whose_document_is_already_gone_yields_no_row(pair):
    """`updateLookup` reads the document as it is *now*, so an update to a
    row that was deleted before the tail caught up finds nothing there.

    Carrying a key with no values would upsert an empty row onto the target
    and leave it there until the delete arrived. Whatever deleted the
    document produced its own event, and that event is behind this one in the
    same stream - so the truth about that key is already on its way, and the
    update has nothing left to add."""
    mg, _ = pair
    _, token = mg.neutral_changes("src", "cx")
    mongosh('db.t.insertOne({_id:77, name:"brief", n:1, keep:"k"})')
    mongosh('db.t.updateOne({_id:77},{$set:{n:2}})')
    mongosh("db.t.deleteOne({_id:77})")
    changes, token = mg.neutral_changes("src", "cx", token)
    assert [c["op"] for c in changes] == ["insert", "delete"], changes
    assert changes[-1]["key"] == {"_id": 77}


def test_an_unset_field_arrives_as_absent_not_as_missing(pair):
    """`$unset` is the one thing a full document cannot express - the field
    is simply not in it, which is indistinguishable from never having been
    there. The stream reports it separately, so migkit marks it."""
    from migkit import canon
    mg, _ = pair
    mongosh('db.t.insertOne({_id:2, name:"x", n:1, keep:"gone soon"})')
    _, token = mg.neutral_changes("src", "cx")
    mongosh('db.t.updateOne({_id:2},{$unset:{keep:""}})')
    changes, token = mg.neutral_changes("src", "cx", token)
    assert len(changes) == 1, changes
    assert changes[0]["values"]["keep"] is canon.ABSENT, changes[0]
    mongosh("db.t.deleteOne({_id:2})")
    mg.neutral_changes("src", "cx", token)


def test_the_changes_apply_onto_postgres(pair):
    mg, pg = pair
    assert pg_sql("delete from t").returncode == 0
    mongosh("db.t.drop()")
    _, token = mg.neutral_changes("src", "cx")

    mongosh('db.t.insertMany([{_id:10, name:"ten", n:10, keep:"a"},'
            ' {_id:11, name:"eleven", n:11, keep:"b"}])')
    mongosh('db.t.updateOne({_id:10},{$set:{n:99}})')
    mongosh("db.t.deleteOne({_id:11})")
    changes, token = mg.neutral_changes("src", "cx", token)
    assert pg.neutral_apply("dst", "cx", changes) == len(changes)

    assert pg_sql("select count(*) from t").stdout.strip() == "1"
    assert pg_sql('select "_id", name, n, keep from t').stdout.strip() \
        == "10|ten|99|a"


def test_a_token_resumes_rather_than_starting_over(pair):
    mg, _ = pair
    _, token = mg.neutral_changes("src", "cx")
    mongosh('db.t.insertOne({_id:40, name:"a", n:1, keep:"k"})')
    first, token = mg.neutral_changes("src", "cx", token)
    assert len(first) == 1, first
    again, token = mg.neutral_changes("src", "cx", token)
    assert again == [], again
    mongosh('db.t.insertOne({_id:41, name:"b", n:1, keep:"k"})')
    third, _ = mg.neutral_changes("src", "cx", token)
    assert len(third) == 1 and third[0]["key"] == {"_id": 41}, third


def test_a_dropped_collection_stops_the_tail_rather_than_passing_as_a_row(
        pair):
    """A drop is not an insert, an update or a delete, and nothing a
    row-shaped applier does would carry it. Skipping it would let the tail
    keep running against a collection that is not there any more."""
    mg, _ = pair
    _, token = mg.neutral_changes("src", "cx")
    mongosh('db.doomed.insertOne({_id:1})')
    mongosh("db.doomed.drop()")
    with pytest.raises(SystemExit) as e:
        mg.neutral_changes("src", "cx", token)
    assert "'drop'" in str(e.value)
    assert "not a row change" in str(e.value)
    assert "Re-run the full load" in str(e.value)

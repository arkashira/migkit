"""Documents into a table, and a table back into documents.

The two directions are not symmetric, and the asymmetry is the point. MongoDB
can store "this field is not here" and a SQL column cannot, so one direction
loses something the other does not - and the number of values it lost is
printed rather than left for someone to discover later.
"""
import socket
import subprocess
import time

import pytest

MONGO, PG = "migkit-test-mmv", "migkit-test-mmv-pg"
MONGO_PORT, PG_PORT = 27073, 15469

# document 3 holds nulls; document 4 simply has no `name`, `ratio` or `flag`
SEED = """
db.getSiblingDB("cx").src.insertMany([
 {_id:1, name:"café", ratio:1e20, flag:true},
 {_id:2, name:"", ratio:0.000001, flag:false},
 {_id:3, name:null, ratio:null, flag:null},
 {_id:4}
]);
print(db.getSiblingDB("cx").src.countDocuments({}));
"""


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


def mongosh(script, db="cx"):
    return subprocess.run(
        ["docker", "exec", MONGO, "mongosh", "--quiet", db, "--eval", script],
        capture_output=True, text=True)


def pg_sql(sql, db="cx"):
    return subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=test", PG, "psql", "-U",
         "postgres", "-d", db, "-At", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)


class _Checkpoint(dict):
    def save(self):
        pass


def _hop(src, dst):
    from migkit.config import Endpoint, Hop
    mongo = Endpoint(host="127.0.0.1", port=MONGO_PORT, user="", password="")
    pg = Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                  password="test")
    return Hop(name="mm", engine="hetero",
               source=mongo if src == "mongodb" else pg,
               target=pg if dst == "postgres" else mongo,
               db_map={"cx": "cx"},
               options={"source_engine": src, "target_engine": dst})


@pytest.fixture(scope="module")
def servers():
    for n in (MONGO, PG):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MONGO, "-p",
                    f"{MONGO_PORT}:27017", "mongo:7"], check=True,
                   capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PG_PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    assert _wait(MONGO_PORT) and _wait(PG_PORT)
    for _ in range(40):
        if mongosh("db.runCommand({ping:1}).ok", "admin").returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("mongo never answered")
    for _ in range(40):
        if pg_sql("select 1", "postgres").returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("postgres never answered")
    assert mongosh(SEED, "admin").stdout.strip().endswith("4")
    assert pg_sql("create database cx", "postgres").returncode == 0
    assert pg_sql('create table src ("_id" bigint primary key, name text,'
                  " ratio double precision, flag boolean)").returncode == 0
    assert pg_sql("select count(*) from src").stdout.strip() == "0"
    yield
    for n in (MONGO, PG):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def test_documents_land_in_a_table_and_the_digest_agrees(servers):
    from migkit.engines.hetero import HeteroEngine
    eng = HeteroEngine(_hop("mongodb", "postgres"))
    lines = []
    eng.move_table("cx", "", "src", 2, _Checkpoint(), lines.append)
    assert pg_sql("select count(*) from src").stdout.strip() == "4"
    got = [r for r in eng.check_data("cx") if r.scope.endswith(".src")]
    assert len(got) == 1, [(r.scope, r.status, r.detail) for r in got]
    assert got[0].status == "ok", got[0].detail


def test_the_values_that_were_not_there_are_counted_out_loud(servers):
    """Document 4 has no `name`, `ratio` or `flag`. They arrive as NULL
    because a PostgreSQL column has nowhere else to put them, and the line
    says how many - three - rather than letting the migration look lossless.
    """
    from migkit.engines.hetero import HeteroEngine
    eng = HeteroEngine(_hop("mongodb", "postgres"))
    lines = []
    eng.move_table("cx", "", "src", 10, _Checkpoint(), lines.append)
    said = [m for m in lines if "were not there on the source" in m]
    assert said, lines
    assert "3 values" in said[0], said
    assert "postgres has no way to store" in said[0], said

    # and the target really does hold nulls there
    assert pg_sql('select count(*) from src where "_id" = 4'
                  " and name is null and ratio is null"
                  " and flag is null").stdout.strip() == "1"


def test_a_table_moved_into_mongo_keeps_its_nulls_as_nulls(servers):
    """The other direction. A SQL NULL is a value the column holds, so it is
    written as null rather than left out - inventing an absent field would be
    claiming the source said something it did not."""
    from migkit.engines.hetero import HeteroEngine
    assert pg_sql("drop table if exists back;"
                  ' create table back ("_id" bigint primary key, name text);'
                  " insert into back values (1,'x'),(2,null)").returncode == 0
    assert mongosh("db.back.drop()").returncode == 0
    eng = HeteroEngine(_hop("postgres", "mongodb"))
    eng.move_table("cx", "public", "back", 10, _Checkpoint(),
                   lambda m: None)
    got = mongosh('printjson(db.back.find({}, {_id:1, name:1})'
                  ".sort({_id:1}).toArray())")
    assert "'x'" in got.stdout or '"x"' in got.stdout, got.stdout
    # the row whose name is NULL has the field, holding null
    present = mongosh('print(db.back.countDocuments({_id:2, name:null}))')
    assert present.stdout.strip().endswith("1"), present.stdout
    exists = mongosh('print(db.back.countDocuments('
                     '{_id:2, name:{$exists:true}}))')
    assert exists.stdout.strip().endswith("1"), exists.stdout


def test_a_mongo_to_mongo_copy_keeps_a_missing_field_missing(servers):
    """Nothing is flattened when the target can hold the distinction. This is
    what the counting in the other direction is protecting."""
    from migkit.engines.hetero import HeteroEngine
    assert mongosh("db.copy.drop()").returncode == 0
    eng = HeteroEngine(_hop("mongodb", "mongodb"))
    eng.dst_engine = eng.src_engine
    lines = []
    # move src -> copy by hand: the pair helper matches names, so make one
    cols = [(n, c) for n, c in
            [("_id", "integer"), ("name", "text"), ("ratio", "float"),
             ("flag", "boolean")]]
    rows, _ = eng.src_engine.neutral_read("src", "cx", "src", cols, None, 100)
    rows, flattened = eng._flatten_absent(rows)
    assert flattened == 0, "a mongo target must keep absence"
    eng.src_engine.neutral_write("src", "cx", "copy", cols, rows)
    assert lines == []

    missing = mongosh('print(db.copy.countDocuments('
                      '{_id:4, name:{$exists:false}}))')
    assert missing.stdout.strip().endswith("1"), missing.stdout
    nulled = mongosh('print(db.copy.countDocuments('
                     '{_id:3, name:null, name:{$exists:true}}))')
    assert nulled.stdout.strip().endswith("1"), nulled.stdout

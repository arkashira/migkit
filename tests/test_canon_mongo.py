"""A collection of documents compared against a table of rows.

MongoDB brings a problem neither SQL engine had: there are no columns to ask
about, and there is no hashing operator to fold with. Measured on MongoDB 7.0,
`$md5`, `$sha1`, `$sha256` and `$hash` are each "Unknown expression" - the only
two ways to a hash are `$toHashedIndexKey`, which is internal and produces a
number no other engine can reproduce, and `$function`, which runs server-side
JavaScript that would have to carry its own MD5.

So the documents are folded here instead, with the same rendering and the same
arithmetic. The number comes out identical to PostgreSQL's; what differs is
that the collection crossed the network to produce it, and `check` says so.
"""
import socket
import subprocess
import time

import pytest

from migkit import canon

MONGO, PG = "migkit-test-mg", "migkit-test-mg-pg"
MONGO_PORT, PG_PORT = 27075, 15473

SEED = """
db.getSiblingDB("cx").v.insertMany([
 {_id:1, name:"café", ratio:1e20, flag:true},
 {_id:2, name:"", ratio:0.000001, flag:false},
 {_id:3, name:null, ratio:null, flag:null},
 {_id:4}
]);
print(db.getSiblingDB("cx").v.countDocuments({}));
"""
PG_SEED = """
create table v (_id bigint primary key, name text,
                ratio double precision, flag boolean);
insert into v values (1,'café',1e20,true),(2,'',0.000001,false),
                     (3,null,null,null),(4,null,null,null);
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


def _hop(engine, host, port, user, password):
    from migkit.config import Endpoint, Hop
    ep = Endpoint(host=host, port=port, user=user, password=password)
    return Hop(name="m", engine=engine, source=ep, target=ep,
               db_map={"cx": "cx"})


@pytest.fixture(scope="module")
def pair():
    for n in (MONGO, PG):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
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
    assert pg_sql(PG_SEED).returncode == 0
    assert pg_sql("select count(*) from v").stdout.strip() == "4"

    from migkit.engines.mongodb import MongoEngine
    from migkit.engines.postgres import PostgresEngine
    mo = MongoEngine(_hop("mongodb", "127.0.0.1", MONGO_PORT, "", ""))
    pg = PostgresEngine(_hop("postgres", "127.0.0.1", PG_PORT,
                             "postgres", "test"))
    yield mo, pg
    for n in (MONGO, PG):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def _cols(eng, side, db, table, drop=()):
    out = []
    for name, declared in eng.neutral_columns(side, db, table):
        if name in drop:
            continue
        cls, why = canon.comparable(eng.CANON_ENGINE, declared)
        assert cls, (name, declared, why)
        out.append((name, cls))
    return sorted(out)


def test_mongo_has_no_hashing_operator_at_all(pair):
    """The measurement the client-side fold exists because of. If a future
    MongoDB grows one of these, this test says so and the fold can move into
    the server."""
    for op in ("$md5", "$sha1", "$sha256", "$hash"):
        r = mongosh("try { db.v.aggregate([{$limit:1},{$project:{x:{\"%s\":"
                    "\"abc\"}}}]).toArray(); print('AVAILABLE') }"
                    " catch (e) { print('missing') }" % op)
        assert "missing" in r.stdout, (op, r.stdout, r.stderr)


def test_the_field_list_comes_from_every_document_not_a_sample(pair):
    """`extra` would exist in one document in a million just as easily. A
    sampled field list reports a collection nobody fully looked at as fully
    described."""
    mo, _ = pair
    assert mongosh("db.v.insertOne({_id:99, rare:'seen once'}).acknowledged"
                   ).returncode == 0
    try:
        got = mo.field_types("src", "cx", "v")
        assert "rare" in got, sorted(got)
        assert got["rare"][1] == 1, got["rare"]
    finally:
        mongosh("db.v.deleteOne({_id:99})")


def test_absent_and_null_are_counted_apart(pair):
    """MongoDB keeps them apart and a SQL target cannot. Document 3 has
    `flag: null`; document 4 has no `flag` at all. Both become NULL on the
    target, so the conversion is one-way - which is worth a line in the
    report rather than a silence."""
    mo, _ = pair
    types, present, nulls = mo.field_types("src", "cx", "v")["flag"]
    assert present == 3, present          # document 4 does not have it
    assert nulls == 1, nulls              # document 3 has it, holding null
    assert "null" in types and "bool" in types


def test_a_field_holding_two_real_types_is_refused(pair):
    """`null` alongside a type says nothing; `string` alongside `int` is a
    field migkit cannot render one way, and picking the more common one would
    be inventing the answer."""
    mo, _ = pair
    assert canon.type_class("mongodb", "bool|null") == "boolean"
    assert canon.type_class("mongodb", "string|int") is None
    assert mongosh("db.v.insertOne({_id:98, ratio:'not a number'})"
                   ".acknowledged").returncode == 0
    try:
        declared = dict(mo.neutral_columns("src", "cx", "v"))["ratio"]
        assert "string" in declared and "double" in declared, declared
        cls, why = canon.comparable("mongodb", declared)
        assert cls is None
        assert "no canonical rendering" in why
    finally:
        mongosh("db.v.deleteOne({_id:98})")


def test_a_collection_and_a_table_fold_to_the_same_number(pair):
    """The documents include one with three fields simply absent, against a
    row holding three NULLs. They are equal after the migration, and the
    digest says so."""
    mo, pg = pair
    mc = _cols(mo, "src", "cx", "v")
    pc = _cols(pg, "src", "cx", "public.v")
    assert [n for n, _ in mc] == [n for n, _ in pc], (mc, pc)
    a = mo.neutral_digest("src", "cx", "v", mc)
    b = pg.neutral_digest("src", "cx", "public.v", pc)
    assert a == b, (a, b)
    assert a[0] == 4


def test_a_change_in_one_document_moves_only_that_sides_digest(pair):
    mo, pg = pair
    mc = _cols(mo, "src", "cx", "v")
    pc = _cols(pg, "src", "cx", "public.v")
    before = mo.neutral_digest("src", "cx", "v", mc)
    assert mongosh("db.v.updateOne({_id:1},{$set:{name:'cafe'}})"
                   ".modifiedCount").stdout.strip().endswith("1")
    try:
        after = mo.neutral_digest("src", "cx", "v", mc)
        assert after != before
        assert after[0] == before[0] == 4
        assert pg.neutral_digest("src", "cx", "public.v", pc) == before
    finally:
        mongosh("db.v.updateOne({_id:1},{$set:{name:'café'}})")
    assert mo.neutral_digest("src", "cx", "v", mc) == before


def test_the_report_says_which_side_folded_locally(pair):
    """The number is the same either way; where the data went is not, and an
    operator sizing the run needs the second fact."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="h", engine="hetero",
              source=Endpoint(host="127.0.0.1", port=MONGO_PORT, user="",
                              password=""),
              target=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              db_map={"cx": "cx"},
              options={"source_engine": "mongodb",
                       "target_engine": "postgres"})
    got = [r for r in HeteroEngine(hop).check_data("cx")
           if r.scope.endswith(".v")]
    assert len(got) == 1, [(r.scope, r.status, r.detail) for r in got]
    assert got[0].status == "ok", got[0].detail
    assert "mongodb has no hashing operator of its own" in got[0].detail
    assert "crossed the network to be folded here" in got[0].detail

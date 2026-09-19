"""The documents a table only points at.

A PostgreSQL large object does not live in the table. The table holds an
`oid`; the bytes live in `pg_largeobject`, and nothing enforces a
relationship between the two. Copy the table and you have copied the
integer.

`pg_dump` makes it easy to lose them: it skips large objects whenever `-s`,
`-n` or `-t` is used - a schema- or table-restricted dump produces an
archive whose oid columns are intact and whose blobs are absent. There is a
`-b` to put them back and no inverse.

Measured before this check existed, on a pair whose table contents were
**byte-identical**:

    table on both sides   1contract16391,2invoice16391
    source                lo_get(16391) -> 'the actual contents...'
    target                ERROR:  large object 16391 does not exist

    counts   postgres: OK 1 tables, rows 2==2
    data     postgres: OK 1 tables, 2 rows, checksums equal both sides
    verdict: same

Every document gone, and migkit certified the migration - because the
column really did hold the same integer on both sides.

**The design risk is the opposite mistake.** Plenty of `oid` columns hold
something that is not a large object at all: a `regclass`, a type oid.
Those resolve on neither side and an anti-join would call them all broken.
So a column is only treated as a large object reference when the **source**
resolves it - measured, a column holding `'refs'::regclass::oid` resolved 0
rows where a real document column resolved 1.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = 15527, 15528
NAMES = {SRC: "migkit-test-lo-src", DST: "migkit-test-lo-dst"}


def q(port, sql):
    return subprocess.run(
        ["docker", "exec", NAMES[port], "psql", "-U", "postgres", "-d",
         "postgres", "-At", "-q", "-c", sql], capture_output=True, text=True)


@pytest.fixture(scope="module")
def lo_pair():
    for port, name in NAMES.items():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "POSTGRES_PASSWORD=test", "-p", f"{port}:5432",
                        "postgres:16"], check=True, capture_output=True)
    for port in NAMES:
        end = time.time() + 120
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    break
            time.sleep(1)
        for _ in range(60):
            if subprocess.run(["docker", "exec", NAMES[port], "pg_isready",
                               "-U", "postgres"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail(f"postgres on {port} never answered")
    yield {"src": SRC, "dst": DST}
    for name in NAMES.values():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


@pytest.fixture
def documents(lo_pair):
    """A real large object on the source, the same rows on the target, and
    nothing behind the oid there - plus an oid column that is not a large
    object reference at all."""
    for port in NAMES:
        q(port, "drop table if exists docs; drop table if exists refs;")
    q(lo_pair["src"], "select lo_unlink(oid) from pg_largeobject_metadata")
    oid = q(lo_pair["src"],
            "select lo_from_bytea(0, 'the actual contents'::bytea)"
            ).stdout.strip()
    assert oid.isdigit(), oid
    for port in NAMES:
        got = q(port, "create table docs (id int primary key, name text,"
                      " body oid);"
                      f" insert into docs values (1,'contract',{oid}),"
                      f" (2,'invoice',{oid});"
                      " create table refs (id int primary key, tbl oid);"
                      " insert into refs values (1,'refs'::regclass::oid);")
        assert got.returncode == 0, got.stderr
    return {**lo_pair, "oid": oid}


def _engine(pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="lo", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=1)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def test_the_tables_are_identical_and_the_documents_are_not(documents):
    """The control, and the reason this is worth a check: nothing about the
    rows is different."""
    rows = [q(p, "select string_agg(id||name||body, ',' order by id)"
                 " from docs").stdout.strip() for p in NAMES]
    assert rows[0] == rows[1], rows
    assert q(documents["src"],
             f"select length(lo_get({documents['oid']}))").stdout.strip() \
        == "19", q(documents["src"], "select 1").stdout
    broken = q(documents["dst"], f"select lo_get({documents['oid']})")
    assert broken.returncode != 0
    assert "does not exist" in broken.stderr, broken.stderr


def test_the_dangling_reference_is_reported(documents, tmp_path):
    got = _engine(documents, tmp_path)._large_objects("postgres")
    assert got.status == "diff", got.detail
    assert "public.docs.body 2 of 2 rows" in got.detail, got.detail
    assert "what they refer to did not" in got.detail, got.detail
    assert "-b" in got.fix_hint, got.fix_hint


def test_an_oid_column_that_is_not_a_document_is_left_alone(documents,
                                                             tmp_path):
    """`refs.tbl` holds a regclass oid on both sides. An anti-join would
    call it broken everywhere; requiring the source to resolve it first is
    what keeps this quiet."""
    assert q(documents["src"], "select count(*) from refs where tbl is not"
                               " null").stdout.strip() == "1"
    got = _engine(documents, tmp_path)._large_objects("postgres")
    assert "refs" not in got.detail, got.detail


def test_the_full_deep_report_carries_it(documents, tmp_path):
    got = [r for r in _engine(documents, tmp_path).check_deep("postgres")
           if r.scope.endswith("large objects")]
    assert len(got) == 1, [r.scope for r in got]
    assert got[0].status == "diff", got[0].detail


def test_a_database_with_no_large_objects_says_so_quietly(lo_pair, tmp_path):
    """Crying wolf on every database without a single blob would make this
    line worth skipping."""
    for port in NAMES:
        q(port, "drop table if exists docs; drop table if exists refs;")
        q(port, "select lo_unlink(oid) from pg_largeobject_metadata")
    got = _engine(lo_pair, tmp_path)._large_objects("postgres")
    assert got.status == "ok", got.detail
    assert "no large objects on either side" in got.detail, got.detail


def test_matching_sides_are_ok_and_admit_what_they_cannot_see(lo_pair,
                                                               tmp_path):
    for port in NAMES:
        q(port, "drop table if exists docs;")
        q(port, "select lo_unlink(oid) from pg_largeobject_metadata")
    oids = {}
    for port in NAMES:
        oids[port] = q(port, "select lo_from_bytea(0, 'same'::bytea)"
                       ).stdout.strip()
        q(port, "create table docs (id int primary key, body oid);"
                f" insert into docs values (1,{oids[port]});")
    got = _engine(lo_pair, tmp_path)._large_objects("postgres")
    assert got.status == "ok", got.detail
    assert "1 large objects on both sides" in got.detail, got.detail
    # the blind spot it cannot cover is stated rather than left implied
    assert "plain integer column are not found" in got.detail, got.detail


def test_the_verdict_needs_no_server(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="l", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)

    hit = eng._large_object_result("x", 5, 5, [("t.body", 3, 5)], 1, "h")
    assert hit.status == "diff" and "t.body 3 of 5 rows" in hit.detail

    gone = eng._large_object_result("x", 5, 0, [], 0, "h")
    assert gone.status == "diff", gone.detail
    assert "the target holds none" in gone.detail, gone.detail
    assert "restricted by schema or table" in gone.detail, gone.detail

    fewer = eng._large_object_result("x", 5, 3, [], 1, "h")
    assert fewer.status == "diff" and "5 large objects on the source" \
        in fewer.detail

    quiet = eng._large_object_result("x", 0, 0, [], 0, "h")
    assert quiet.status == "ok"
    assert "no large objects on either side" in quiet.detail

    fine = eng._large_object_result("x", 5, 5, [], 2, "h")
    assert fine.status == "ok" and "2 checked" in fine.detail

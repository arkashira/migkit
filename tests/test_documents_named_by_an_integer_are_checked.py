"""The documents a plain integer column names, and the documents' bytes
(problems D11).

The large-object check found a reference by its type - `oid`, what
`vacuumlo` looks at too - and said the blind spot out loud: a reference
parked in a `bigint` was invisible to it, and the objects it named could
be gone from the target with the check green. And it asked only whether
each object was there, never whether it held the source's bytes.

Now an integer column is taken for references where at least half of what
it holds names an object on the source - an id column counting past the
first object's oid names a few by chance and stays quiet - and every
object both sides hold is compared a piece at a time by its digest.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = 16270, 16271
NAMES = {SRC: "migkit-test-vd-lo-src", DST: "migkit-test-vd-lo-dst"}


def q(port, sql):
    return subprocess.run(
        ["docker", "exec", NAMES[port], "psql", "-U", "postgres", "-d",
         "postgres", "-At", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)


@pytest.fixture(scope="module")
def lo_pair():
    from tests.conftest import _have_docker
    if not _have_docker():
        pytest.skip("docker not available")
    for port, name in NAMES.items():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "POSTGRES_PASSWORD=test", "-p",
                        f"127.0.0.1:{port}:5432", "postgres:16"],
                       check=True, capture_output=True)
    try:
        for port in NAMES:
            end = time.time() + 120
            while time.time() < end:
                with socket.socket() as s:
                    s.settimeout(2)
                    if s.connect_ex(("127.0.0.1", port)) == 0:
                        break
                time.sleep(1)
            for _ in range(60):
                if subprocess.run(["docker", "exec", NAMES[port],
                                   "pg_isready", "-U", "postgres"],
                                  capture_output=True).returncode == 0:
                    break
                time.sleep(1)
            else:
                pytest.fail(f"postgres on {port} never answered")
        yield {"src": SRC, "dst": DST}
    finally:
        for name in NAMES.values():
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _engine(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="lo", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=SRC, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST, user="postgres",
                              password="test"),
              databases=["postgres"], workers=1)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _fresh():
    for port in NAMES:
        got = q(port, "drop table if exists files; drop table if exists"
                      " counters; select lo_unlink(oid) from"
                      " pg_largeobject_metadata")
        assert got.returncode == 0, got.stderr


def _object(port, oid, text):
    got = q(port, f"select lo_from_bytea({oid}, '{text}'::bytea)")
    assert got.returncode == 0, got.stderr


def test_an_integer_column_of_references_is_found_and_checked(lo_pair,
                                                              tmp_path):
    """The same rows both sides, the objects only on the source: the
    bigint column that names them is found by its values and reported."""
    _fresh()
    for oid in (70001, 70002, 70003):
        _object(SRC, oid, f"document {oid}")
    for port in NAMES:
        got = q(port, "create table files (id int primary key, doc bigint);"
                      " insert into files values (1, 70001), (2, 70002),"
                      " (3, 70003), (4, null)")
        assert got.returncode == 0, got.stderr
    got = _engine(tmp_path)._large_objects("postgres")
    assert got.status == "diff", got.detail
    assert "public.files.doc (an integer column) 3 of 3 rows" in got.detail, \
        got.detail


def test_an_id_that_names_objects_by_chance_is_left_alone(lo_pair,
                                                           tmp_path):
    """An integer column counting through the oids the objects happen to
    have names a few of them among many: not a column of references."""
    _fresh()
    for oid in (70010, 70011):
        _object(SRC, oid, "x")
        _object(DST, oid, "x")
    for port in NAMES:
        got = q(port, "create table counters (n bigint primary key);"
                      " insert into counters select g from"
                      " generate_series(69990, 70100) g")
        assert got.returncode == 0, got.stderr
    q(DST, "select lo_unlink(70011)")
    got = _engine(tmp_path)._large_objects("postgres")
    assert "counters" not in got.detail, got.detail
    # the count still says the target is one short
    assert got.status == "diff" and "2 large objects on the source and 1" \
        in got.detail, got.detail


def test_an_object_with_other_bytes_is_a_difference(lo_pair, tmp_path):
    """Present on both sides under the same oid - every reference resolves
    - and the target's document is not the source's."""
    _fresh()
    _object(SRC, 70020, "the signed contract")
    _object(DST, 70020, "the unsigned draft!")
    for port in NAMES:
        got = q(port, "create table files (id int primary key, doc oid);"
                      " insert into files values (1, 70020)")
        assert got.returncode == 0, got.stderr
    got = _engine(tmp_path)._large_objects("postgres")
    assert got.status == "diff", got.detail
    assert "1 of 1 large objects both sides hold are other bytes" \
        in got.detail and "70020" in got.detail, got.detail


def test_a_long_object_is_compared_past_its_first_piece(lo_pair, tmp_path,
                                                        monkeypatch):
    """A difference past the first piece is found: pieces of 16 bytes
    here, the change in the fourth."""
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.setattr(PostgresEngine, "LO_PIECE", 16)
    _fresh()
    body = "0123456789abcdef" * 3
    _object(SRC, 70030, body + "tail")
    _object(DST, 70030, body + "TAIL")
    got = _engine(tmp_path)._large_objects("postgres")
    assert got.status == "diff" and "70030" in got.detail, got.detail
    _fresh()
    _object(SRC, 70031, body + "tail")
    _object(DST, 70031, body + "tail")
    got = _engine(tmp_path)._large_objects("postgres")
    assert got.status == "ok", got.detail
    assert "every one the same bytes on both (1 compared" in got.detail, \
        got.detail


def test_the_verdicts_need_no_server(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="l", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    other = eng._large_object_result("x", 3, 3, [], 1, "h",
                                     contents=(3, [70020]), by_value=1)
    assert other.status == "diff" and "1 of 3 large objects" in other.detail
    same = eng._large_object_result("x", 3, 3, [], 2, "h",
                                    contents=(3, []), by_value=1)
    assert same.status == "ok", same.detail
    assert "(1 found that way)" in same.detail, same.detail
    assert "3 compared a piece at a time" in same.detail, same.detail


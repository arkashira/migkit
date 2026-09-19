"""The parser against a slot, rather than against a transcription of one.

`test_pgslot.py` covers the shapes with strings pasted into the file. Pasted
strings test the parser against what someone typed; this tests it against what
the server emits, which is the only version that matters when PostgreSQL
changes how it prints something.
"""
import socket
import subprocess
import time

import pytest

from migkit import pgslot

PG = "migkit-test-slot-pg"
PG_PORT = 15459
SLOT = "migkit_test_slot"


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


def q(sql, db="cx"):
    r = subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=test", PG, "psql", "-U",
         "postgres", "-d", db, "-At", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.rstrip("\n")


@pytest.fixture(scope="module")
def slot():
    subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PG_PORT}:5432",
                    "postgres:16", "-c", "wal_level=logical"],
                   check=True, capture_output=True)
    assert _wait(PG_PORT)
    for _ in range(40):
        r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", PG,
                            "psql", "-U", "postgres", "-c", "select 1"],
                           capture_output=True)
        if r.returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("postgres never answered")
    assert q("show wal_level", "postgres") == "logical"
    q("create database cx", "postgres")
    q("create table t (id bigint primary key, name varchar(50),"
      " amount numeric(12,4), b bytea, doc jsonb, made timestamp(6),"
      " flag boolean)")
    # the slot exists before anything is written, or the changes below are
    # not in it - the same ordering a migration needs for the same reason
    q(f"select pg_create_logical_replication_slot('{SLOT}','test_decoding')")
    yield
    subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)


def _drain():
    out = q(f"select data from pg_logical_slot_get_changes('{SLOT}',"
            " null, null)")
    return [l for l in out.splitlines() if l]


def test_the_plugins_a_stock_server_actually_has(slot):
    """The measurement behind choosing `test_decoding` at all: `wal2json`
    emits JSON and would need no parsing, and it is not here."""
    out = subprocess.run(
        ["docker", "exec", PG, "sh", "-c",
         'ls $(pg_config --pkglibdir) | grep -iE "decod|wal2|pgoutput"'],
        capture_output=True, text=True).stdout.split()
    assert "test_decoding.so" in out, out
    assert "pgoutput.so" in out, out
    assert not any("wal2json" in n for n in out), out


def test_a_round_trip_through_the_parser_keeps_every_value(slot):
    _drain()
    q("insert into t values (1,'ca''fé : [x]',-0.05,'\\x00FF41',"
      "'{\"b\":1}','2026-01-01 00:00:00.123456',true)")
    lines = _drain()
    recs = [pgslot.change(p, ["id"])
            for p in (pgslot.parse_line(l) for l in lines) if p]
    assert len(recs) == 1, lines
    got = recs[0]
    assert got["op"] == "insert" and got["key"] == {"id": 1}
    v = got["values"]
    assert v["name"] == "ca'fé : [x]", v["name"]
    assert v["b"] == b"\x00\xffA", v["b"]
    assert str(v["amount"]) == "-0.0500"
    assert v["flag"] is True
    assert v["id"] == 1


def test_a_null_column_arrives_as_none_not_as_the_word(slot):
    _drain()
    q("insert into t values (5,null,null,null,null,null,null)")
    recs = [pgslot.change(p, ["id"])
            for p in (pgslot.parse_line(l) for l in _drain()) if p]
    assert len(recs) == 1
    v = recs[0]["values"]
    assert v["name"] is None and v["b"] is None and v["flag"] is None


def test_a_text_column_holding_the_word_null_survives(slot):
    """The other half of the same distinction, and the one a parser that
    compares text gets wrong."""
    _drain()
    q("insert into t values (6,'null',1,null,null,null,false)")
    recs = [pgslot.change(p, ["id"])
            for p in (pgslot.parse_line(l) for l in _drain()) if p]
    assert recs[0]["values"]["name"] == "null"
    assert recs[0]["values"]["b"] is None


def test_an_update_that_moves_the_key_gives_the_old_one_as_the_key(slot):
    _drain()
    q("insert into t values (10,'before',1,null,null,null,true)")
    _drain()
    q("update t set id = 11, name = 'after' where id = 10")
    recs = [pgslot.change(p, ["id"])
            for p in (pgslot.parse_line(l) for l in _drain()) if p]
    assert len(recs) == 1
    assert recs[0]["op"] == "update"
    assert recs[0]["key"] == {"id": 10}
    assert recs[0]["values"]["id"] == 11


def test_a_delete_under_full_identity_still_yields_a_key(slot):
    """With REPLICA IDENTITY FULL the old tuple omits the NULL columns, so
    the key has to come from the catalogue rather than from the line."""
    _drain()
    q("alter table t replica identity full")
    try:
        q("insert into t values (20,'x',1,null,null,null,false)")
        _drain()
        q("delete from t where id = 20")
        lines = _drain()
        parsed = [p for p in (pgslot.parse_line(l) for l in lines) if p]
        assert len(parsed) == 1
        names = [n for n, _, _, _ in parsed[0]["old"]]
        assert "b" not in names, names          # it was NULL, so it is gone
        rec = pgslot.change(parsed[0], ["id"])
        assert rec == {"op": "delete", "table": "public.t",
                       "key": {"id": 20}, "values": {}}
    finally:
        q("alter table t replica identity default")


def test_every_line_the_server_emits_is_either_parsed_or_refused(slot):
    """No line is skipped silently. A shape migkit has not seen raises, which
    is how a change of format is found here rather than as a row that never
    arrived."""
    _drain()
    q("insert into t values (30,'a',1,null,null,null,true)")
    q("update t set name='b' where id=30")
    q("delete from t where id=30")
    lines = _drain()
    assert any(l.startswith("BEGIN") for l in lines), lines
    assert any(l.startswith("COMMIT") for l in lines), lines
    ops = []
    for line in lines:
        parsed = pgslot.parse_line(line)     # raises on anything unexpected
        if parsed:
            ops.append(parsed["op"])
    assert ops == ["insert", "update", "delete"], ops

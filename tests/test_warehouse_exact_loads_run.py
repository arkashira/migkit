"""Warehouse writes and digests run where they can run here.

**Redshift, on PostgreSQL where the two share their SQL.** A PostgreSQL 16
database stands in for the cluster: the MERGE, the temporary stage, the
ledger's transaction and every text function of the digest are
PostgreSQL's too. Redshift's own `STRTOL`, `SHA2`, `TO_HEX` of a VARBYTE,
`TO_VARBYTE`, `JSON_PARSE` and `GETDATE` are given to it as functions doing
what Redshift documents them to do for the arguments migkit passes. What
this does not prove, and a real cluster would: COPY from S3, the slices,
the load history, VARBYTE and SUPER themselves, and Redshift's MERGE
accepting what PostgreSQL's does.

**A tail into a lake**, PostgreSQL's slot to Delta and Iceberg tables on
this disk, each table's batch committed once.
"""
import json
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import _wait_tcp, needs_docker

pytestmark = needs_docker

PG, PG_PORT = "migkit-test-wh-pg", 16152

SHIMS = """
create function strtol(t text, base int) returns bigint language sql
  immutable as $$ select ('x' || lpad(t, 16, '0'))::bit(64)::bigint $$;
create function sha2(t text, bits int) returns text language sql immutable
  as $$ select encode(sha256(convert_to(t, 'UTF8')), 'hex') $$;
create function to_hex(b bytea) returns text language sql immutable
  as $$ select encode(b, 'hex') $$;
create function to_varbyte(t text, f text) returns bytea language sql
  immutable as $$ select decode(t, 'hex') $$;
create function json_parse(t text) returns jsonb language sql immutable
  as $$ select t::jsonb $$;
create function getdate() returns timestamp language sql stable
  as $$ select now()::timestamp $$;
"""


def pg(sql, db="postgres"):
    got = subprocess.run(["docker", "exec", "-i", PG, "psql", "-U",
                          "postgres", "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def postgres():
    subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p",
                    f"127.0.0.1:{PG_PORT}:5432", "postgres:16", "-c",
                    "wal_level=logical"], check=True, capture_output=True)
    try:
        assert _wait_tcp("127.0.0.1", PG_PORT)
        for _ in range(60):
            if subprocess.run(["docker", "exec", PG, "pg_isready", "-U",
                               "postgres"], capture_output=True
                              ).returncode == 0:
                break
            time.sleep(1)
        time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)


@pytest.fixture
def dbs(postgres):
    pg("select pg_drop_replication_slot(slot_name) from pg_replication_slots")
    for name in ("src", "rs"):
        pg(f"drop database if exists {name} with (force)")
        pg(f"create database {name}")
    pg(SHIMS, db="rs")
    yield


def _redshift_hop(tmp_path, **kw):
    hop = Hop(name="rs", engine="redshift", source=Endpoint(),
              target=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test",
                              options={"database": "rs"}),
              databases=["public"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _pair(tmp_path, target, target_engine, db_map, name="p"):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name=name, engine="hetero",
              source=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              target=target, databases=["src"], db_map=db_map,
              options={"source_engine": "postgres",
                       "target_engine": target_engine})
    (tmp_path / name).mkdir(exist_ok=True)
    hop.report_dir = lambda db=None: tmp_path / name
    return HeteroEngine(hop)


def _rs_target():
    return Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                    password="test", options={"database": "rs"})


ITEMS = """
    create table public.items (id bigint primary key, name varchar(60),
        price numeric(12, 2), active boolean, day date, stamp timestamp,
        zoned timestamptz, clock time);
    insert into public.items select g, 'item-' || g || ' |é:',
        g / 7.0, g % 2 = 0, date '2024-01-01' + g,
        timestamp '2024-01-01 00:00:00.123456' + g * interval '1 minute',
        timestamptz '2024-01-01 00:00:00+07' + g * interval '1 hour',
        time '01:02:03.5' + g * interval '1 second'
      from generate_series(1, 3000) g;
    insert into public.items (id, name) values (0, '');"""


def _data(eng, db):
    return [(r.status, r.detail) for r in eng.check_data(db)
            if r.check == "data"]


def test_redshifts_digest_is_the_fold_in_its_own_sql(dbs, tmp_path,
                                                     monkeypatch):
    """Every class Redshift renders, from rows with the separators, NULLs
    and empty text a row encoding has to survive: the server's digest is
    the fold of the rows read back, SHA-256 in its SQL as well when canon
    folds by it, and one value changed moves it."""
    import hashlib

    from migkit import canon
    from migkit.engines import engine_named
    from migkit.engines import warehouse_sql as W
    pg(ITEMS, db="rs")
    pg("create table public.blobs (id bigint primary key, raw bytea);"
       " insert into public.blobs values (1, '\\x00ff'), (2, ''),"
       " (3, null)", db="rs")
    eng = engine_named("redshift", _redshift_hop(tmp_path))
    cols = [("id", "integer"), ("name", "text"), ("price", "decimal"),
            ("active", "boolean"), ("day", "date"), ("stamp", "timestamp"),
            ("zoned", "timestamp"), ("clock", "time")]

    def fold():
        rows = [r for b in eng.neutral_batches("dst", "public", "items",
                                               cols, 5000) for r in b]
        n, total = canon.fold_rows([c for _, c in cols], rows)
        return n, str(total)
    got = eng.neutral_digest("dst", "public", "items", cols)
    assert eng.folded_last is False
    assert got == fold() and got[0] == 3001
    # bytes, as a VARBYTE renders: the shim is TO_HEX's documented result
    sel = W.digest_select("redshift", [("id", "integer", "bigint"),
                                       ("raw", "bytes", "varbyte")],
                          lambda n: f'"{n}"')
    got_b = pg(f"select {sel} from public.blobs", db="rs").split("|")
    n, total = canon.fold_rows(["integer", "bytes"],
                               [[1, b"\x00\xff"], [2, b""], [3, None]])
    assert (int(got_b[0]), got_b[1]) == (n, str(total))

    def sha_step(total, text):
        h = hashlib.sha256(("" if text is None else str(text)).encode())
        return total + int(h.hexdigest()[:canon.DIGEST_HEX], 16)
    monkeypatch.setattr(canon, "digest_step", sha_step)
    assert "SHA2(" in W.digest_select(
        "redshift", [(n, c, "bigint") for n, c in cols[:1]], str)
    assert eng.neutral_digest("dst", "public", "items", cols) == fold()
    monkeypatch.undo()

    before = eng.neutral_digest("dst", "public", "items", cols)
    pg("update public.items set clock = clock + interval '1 microsecond'"
       " where id = 17", db="rs")
    assert eng.neutral_digest("dst", "public", "items", cols) != before


def test_a_copy_into_redshift_is_merged_whole_and_checked_there(dbs,
                                                                tmp_path):
    """PostgreSQL into Redshift: the table made by migkit, each batch
    staged and merged in one transaction and read back by the server's
    digest of its key range; the pair's check answered by both servers'
    digests, and "different" after one value is changed; a copy stopped
    after a batch was written, run again, ends equal."""
    from migkit.cli import _Checkpoint
    pg(ITEMS, db="src")
    eng = _pair(tmp_path, _rs_target(), "redshift", {"src": "public"})
    eng.hop.options["verify_batches"] = True
    import os
    os.environ["MIGKIT_FAILPOINT"] = "batch.written:2:raise"
    from migkit import failpoint
    failpoint._counts.clear()
    try:
        with pytest.raises(RuntimeError, match="failpoint batch.written"):
            eng.move_table("src", "public", "items", 1000,
                           _Checkpoint(tmp_path / "ck.json"), [].append)
    finally:
        del os.environ["MIGKIT_FAILPOINT"]
    log = []
    eng.move_table("src", "public", "items", 1000,
                   _Checkpoint(tmp_path / "ck.json"), log.append)
    assert pg("select count(*), count(distinct id), sum(price) from"
              " public.items", db="rs") == pg(
        "select count(*), count(distinct id), sum(price) from public.items",
        db="src")
    got = _data(eng, "src")
    assert [s for s, _ in got] == ["ok"], got
    assert "crossed the network" not in got[0][1], got
    pg("update public.items set name = 'changed' where id = 5", db="rs")
    got = _data(eng, "src")
    assert [s for s, _ in got] == ["diff"], got
    # the staging tables were the transaction's own, and are gone
    assert pg("select count(*) from pg_tables where tablename like"
              " '\\_migkit\\_%'", db="rs") == "0"


def _tail(eng, token):
    said, ended = [], {}

    def run():
        try:
            eng.tail_apply("src", True, token, said.append)
        except BaseException as e:
            ended["e"] = e
    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t, said, ended


def _stop(t):
    import ctypes
    if t.is_alive():
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(t.ident), ctypes.py_object(KeyboardInterrupt))
    t.join(timeout=30)


def _until(check, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        got = check()
        if got:
            return got
        time.sleep(1)
    return check()


ACCT = """
    create table public.acct (id int primary key, balance int,
        note varchar(20));
    alter table public.acct replica identity full;
    insert into public.acct select g, 100, 'n' || g
      from generate_series(1, 5) g;"""


def test_a_tail_into_redshift_commits_each_batch_with_its_ledger_line(
        dbs, tmp_path, monkeypatch):
    """Changes from PostgreSQL's slot, merged into the target in one
    transaction with the ledger's line for the hop; a batch committed
    there before its position was saved here - a stop between the two -
    is known by the ledger, and the tail goes on after it instead of
    applying it again."""
    pg(ACCT, db="src")
    pg(ACCT.replace("alter table public.acct replica identity full;", "")
       .split("insert")[0], db="rs")
    pg("insert into public.acct select g, 100, 'n' || g from"
       " generate_series(1, 5) g", db="rs")
    eng = _pair(tmp_path, _rs_target(), "redshift", {"src": "public"})
    token = tmp_path / "p" / "tail-token.json"
    eng.tail_start("src", token)
    pg("update public.acct set balance = 110, note = 'x' where id = 1;"
       " delete from public.acct where id = 2;"
       " insert into public.acct values (9, 900, 'new')", db="src")
    monkeypatch.setenv("MIGKIT_FAILPOINT", "tail.applied:1:raise")
    from migkit import failpoint
    failpoint._counts.clear()
    said = []
    with pytest.raises(RuntimeError, match="failpoint tail.applied"):
        eng.tail_apply("src", True, token, said.append)
    monkeypatch.delenv("MIGKIT_FAILPOINT")
    want = pg("select id, balance, note from public.acct order by id",
              db="src")
    # committed on the target, with the ledger's line saying which batch
    assert pg("select id, balance, note from public.acct order by id",
              db="rs") == want
    saved = json.loads(token.read_text())
    ledger = pg("select batch, token from public._migkit_ledger where"
                " hop = 'p'", db="rs").split("|")
    assert int(ledger[0]) == saved["batch"] + 1, (ledger, saved)
    seen = eng.dst_engine.origin_seen("dst", "src")
    assert seen["batch"] == saved["batch"] + 1
    t, said, ended = _tail(eng, token)
    try:
        assert _until(lambda: any("had committed batch" in m for m in said),
                      30), said
        pg("update public.acct set balance = 120 where id = 3", db="src")
        assert _until(lambda: pg("select balance from public.acct where"
                                 " id = 3", db="rs") == "120"), said
    finally:
        _stop(t)
    assert not ended or isinstance(ended["e"], KeyboardInterrupt), ended
    got = json.loads(token.read_text())
    assert got["batch"] > saved["batch"] + 1
    assert pg("select batch from public._migkit_ledger where hop = 'p'",
              db="rs") == str(got["batch"])
    assert pg("select id, balance, note from public.acct order by id",
              db="rs") == pg("select id, balance, note from public.acct"
                             " order by id", db="src")


def test_the_cluster_is_asked_what_it_is_and_what_it_allows(dbs, tmp_path):
    """The interlock's reading of a Redshift target - the database and the
    schema it writes, what the server says of itself - and the privileges
    a copy needs of it, asked in the functions Redshift keeps: a user who
    may not create in the schema is handed the grant."""
    from migkit import dba, interlock
    pg("create table public.items (id bigint primary key, name text)",
       db="src")
    eng = _pair(tmp_path, _rs_target(), "redshift", {"src": "public"})
    got = interlock.identity(eng.hop, eng, "dst", "src")
    assert got["engine"] == "redshift" and got["database"] == "rs.public"
    assert got["version"].startswith("PostgreSQL") and got["absent"] is False
    found = dba.needs(eng.hop, eng, goal="copy")
    write = [n for n in found if n.side == "target"
             and n.what == "create and write tables in public"]
    assert write and write[0].ok is True
    pg("create role clerk login password 'clerk';"
       " revoke create on schema public from public;"
       " grant usage on schema public to clerk", db="rs")
    eng = _pair(tmp_path, Endpoint(host="127.0.0.1", port=PG_PORT,
                                   user="clerk", password="clerk",
                                   options={"database": "rs"}),
                "redshift", {"src": "public"}, name="q")
    found = dba.needs(eng.hop, eng, goal="copy")
    write = [n for n in found if n.side == "target"
             and n.what == "create and write tables in public"][0]
    assert write.ok is False
    assert "grant usage, create on schema public to clerk;" in write.fix
    script = dba.script(eng.hop, eng, "target", found)
    assert "grant usage, create on schema public to clerk;" in script


@pytest.mark.parametrize("kind", ["delta", "iceberg"])
def test_a_tail_into_a_lake_commits_each_tables_batch_once(dbs, tmp_path,
                                                           kind):
    pg(ACCT, db="src")
    target = (Endpoint(options={"url": str(tmp_path / "lake")})
              if kind == "delta" else
              Endpoint(options={"catalog": f"sqlite:///{tmp_path}/c.db",
                                "warehouse": str(tmp_path / "wh")}))
    eng = _pair(tmp_path, target, kind, {"src": "sales"}, name=kind)
    from migkit.cli import _Checkpoint
    token = tmp_path / kind / "tail-token.json"
    eng.tail_start("src", token)
    eng.move_table("src", "public", "acct", 1000,
                   _Checkpoint(tmp_path / f"{kind}.json"), [].append)
    pg("update public.acct set balance = 110 where id = 1;"
       " delete from public.acct where id = 2;"
       " insert into public.acct values (9, 900, 'new')", db="src")
    t, said, ended = _tail(eng, token)
    cols = [("id", "integer"), ("balance", "integer"), ("note", "text")]

    def rows():
        got, _ = eng.dst_engine.neutral_read("dst", "src", "acct", cols,
                                             None, 0)
        return sorted(tuple(r) for r in got)
    want = [(1, 110, "n1"), (3, 100, "n3"), (4, 100, "n4"), (5, 100, "n5"),
            (9, 900, "new")]
    try:
        got = _until(lambda: rows() == want and rows())
    finally:
        _stop(t)
    assert got == want, (rows(), said[-3:], ended)
    assert [s for s, _ in _data(eng, "src")] == ["ok"]
    saved = json.loads(token.read_text())
    # the table's last commit names the tail's batch and its position
    if kind == "delta":
        last = eng.dst_engine._last("dst", "src", "acct")
    else:
        last = eng.dst_engine._table("dst", "src", "acct").current_snapshot(
        ).summary.additional_properties
    assert last["migkit.batch"] == str(saved["batch"])
    assert json.loads(last["migkit.token"]) == saved["token"]

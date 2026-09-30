"""A seed shipped on media between two PostgreSQL servers: the export
takes the stream's slot before a row is read, writes a table with an
integer key as ranges in the binary copy format - each read under the
table's exported snapshot and digested by the source in the same
transaction - and the rest as rows; the load writes each part through the
target's own copy, proved by the target's digest, and the stream starts
from the slot's position and carries what the source changed while the
carrier travelled. Where the slot did not keep its WAL that long, the load
makes the slot again and copies only what changed, found leaf by leaf.

Two servers in containers of this test's own (127.0.0.1:16410, 16411),
removed after.
"""
import socket
import subprocess
import time

import pytest
from click.testing import CliRunner

from migkit import carriers, ship
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = "migkit-ship-pg-src", "migkit-ship-pg-dst"
PORT = {SRC: 16410, DST: 16411}


def _psql(name, sql, db="app"):
    got = subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=test", name, "psql", "-h",
         "127.0.0.1", "-U", "postgres", "-d", db, "-At", "-v",
         "ON_ERROR_STOP=1", "-c", sql], capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _ready(name, most=120):
    end = time.time() + most
    while time.time() < end:
        ok = subprocess.run(["docker", "exec", name, "pg_isready", "-h",
                             "127.0.0.1", "-U", "postgres"],
                            capture_output=True).returncode == 0
        with socket.socket() as s:
            s.settimeout(2)
            if ok and s.connect_ex(("127.0.0.1", PORT[name])) == 0:
                return
        time.sleep(1)
    pytest.fail(f"{name} never answered")


SEED = (
    "create table orders (id bigint primary key, item text not null,"
    " qty int, at timestamptz, note bytea, amount numeric(12,2));"
    " insert into orders select g, 'item-' || g, g % 7,"
    " timestamptz '2026-10-01 00:00+00' + g * interval '1 minute',"
    " decode(md5(g::text), 'hex'), g / 3.0 from generate_series(1, 20000) g;"
    " create table tags (code text primary key, label text);"
    " insert into tags select 't' || lpad(g::text, 5, '0'), 'label é ' || g"
    " from generate_series(1, 3000) g;"
    " create table log (at timestamptz, what text);"
    " insert into log select timestamptz '2026-10-01 00:00+00', 'event ' || g"
    " from generate_series(1, 4000) g;"
    " insert into log values (timestamptz '2026-10-01 00:00+00', 'event 1')")


@pytest.fixture
def pair():
    subprocess.run(["docker", "rm", "-f", "-v", SRC, DST],
                   capture_output=True)
    try:
        for name in (SRC, DST):
            subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                            "POSTGRES_PASSWORD=test", "-p",
                            f"127.0.0.1:{PORT[name]}:5432", "postgres:16",
                            "-c", "wal_level=logical"],
                           check=True, capture_output=True)
        for name in (SRC, DST):
            _ready(name)
            _psql(name, "create database app", "postgres")
        _psql(SRC, SEED)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", SRC, DST],
                       capture_output=True)


@pytest.fixture
def run(pair, tmp_path, monkeypatch):
    """`migkit` on a hop of this test's own, the carrier a directory."""
    import pyrage

    import migkit.config as cfg
    from migkit import cli
    who = pyrage.x25519.Identity.generate()
    (tmp_path / "me.age").write_text(str(who) + "\n")
    monkeypatch.setenv("MIGKIT_IDENTITY", str(tmp_path / "me.age"))
    # ranges of a few hundred rows: the table is many parts
    monkeypatch.setattr(ship, "PART_BYTES", 64 * 1024)
    monkeypatch.setattr(ship, "ROOM_SAMPLE", 1)
    conf = tmp_path / "hops.yaml"
    conf.write_text("hops:\n" + "".join(
        f"  {name}:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {PORT[SRC]}, user: postgres,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {PORT[DST]}, user: postgres,"
        " password: test}\n"
        "    databases: [app]\n    options:\n"
        f"      at_rest: {{recipients: ['{who.to_public()}']}}\n"
        "      follow_until: caught_up\n"
        f"      ship: {{carrier: disk, path: {tmp_path / 'carrier'},"
        " shipping: 1d}\n" for name in ("seed", "lost")))
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")

    def go(*argv):
        got = CliRunner().invoke(cli.main, list(argv))
        return got, " ".join((got.output + str(got.exception or "")
                              ).split())
    go.carrier = tmp_path / "carrier"
    return go


def _same(table):
    order = {"orders": "id", "tags": "code"}.get(table, "at, what")
    sql = (f"select count(*) || ':' || coalesce(md5(string_agg(x::text, '|'"
           f" order by {order}, x::text)), '') from {table} x")
    return _psql(SRC, sql) == _psql(DST, sql)


def _manifest(name, carrier):
    from migkit.config import get_hop
    from migkit.engines import engine_named
    hop = get_hop(name)
    store = carriers.LocalStore(carrier)
    w = ship._Work(hop, engine_named("postgres", hop), "app", "full+cdc",
                   lambda m: None)
    return ship.read_manifest(w, store, ship.seal_at(store, hop, "app"))


def test_the_seed_goes_in_binary_parts_and_the_stream_from_its_slot(run):
    got, said = run("move", "seed", "--mode", "full+cdc", "--db", "app",
                    "--go")
    assert got.exit_code == 0, said
    assert "ship the carrier" in said, said
    # the stream's slot was made before a row was read, and nothing is on
    # the target: the rows travel on the carrier
    assert _psql(SRC, "select count(*) from pg_replication_slots where"
                      " slot_name = 'migkit_seed'") == "1"
    assert _psql(DST, "select count(*) from pg_tables where schemaname ="
                      " 'public'") == "0"
    tables = {e["table"].split(".")[-1]: e for e in _manifest(
        "seed", run.carrier)["tables"]}
    assert (tables["orders"]["cut"], tables["orders"]["format"]) == \
        ("ranges", "binary copy")
    assert len(tables["orders"]["parts"]) > 2
    assert tables["tags"]["cut"] == "key order"
    assert tables["log"]["format"] == "rows"
    # what the source does while the carrier travels
    _psql(SRC, "update orders set qty = qty + 100 where id <= 10;"
               " delete from orders where id = 20000;"
               " insert into orders values (20001, 'late', 1, now(),"
               " '\\x00', 1.5);"
               " update tags set label = 'changed' where code = 't00007';"
               " insert into log values (now(), 'late event')")
    got, said = run("move", "seed", "--mode", "full+cdc", "--db", "app",
                    "--go")
    assert got.exit_code == 0, said
    assert "loaded and proved" in said, said
    assert "starting from where the source's log was" in said, said
    for table in ("orders", "tags", "log"):
        assert _same(table), table
    got, said = run("check", "seed")
    assert got.exit_code == 0, said
    # and the check says different once the target is changed behind it
    _psql(DST, "update orders set item = 'tampered' where id = 5")
    got, said = run("check", "seed")
    assert got.exit_code != 0, said


def test_a_slot_that_did_not_keep_its_wal_is_made_again_and_only_changes_go(
        run):
    _psql(SRC, "alter system set max_slot_wal_keep_size = '1MB'", "postgres")
    _psql(SRC, "select pg_reload_conf()", "postgres")
    got, said = run("move", "lost", "--mode", "full+cdc", "--db", "app",
                    "--go")
    assert got.exit_code == 0, said
    # the room the source keeps is read as the slot is made, and what
    # happens where it runs out is said before anything ships
    assert "can still keep" in said, said
    assert "the seed is not shipped again" in said, said
    _psql(SRC, "update orders set qty = -1 where id % 1000 = 0;"
               " delete from tags where code = 't00003'")
    # WAL written in another database of the cluster: the slot's is the
    # cluster's
    _psql(SRC, "create table filler (x text)", "postgres")
    for _ in range(20):
        _psql(SRC, "insert into filler select repeat(md5(g::text), 30) from"
                   " generate_series(1, 20000) g; select pg_switch_wal();"
                   " checkpoint", "postgres")
        status = _psql(SRC, "select wal_status from pg_replication_slots"
                            " where slot_name = 'migkit_lost'")
        if status in ("lost", "unreserved"):
            break
    assert status in ("lost", "unreserved"), status
    got, said = run("move", "lost", "--mode", "full+cdc", "--db", "app",
                    "--go")
    assert got.exit_code == 0, said
    assert "copies only those - the seed is not shipped again" in said, said
    assert "copying what differs, leaf by leaf" in said, said
    # a new slot, holding its WAL
    assert _psql(SRC, "select wal_status from pg_replication_slots where"
                      " slot_name = 'migkit_lost'") in ("reserved",
                                                        "extended")
    for table in ("orders", "tags", "log"):
        assert _same(table), table

"""A PostgreSQL target seeded from a copy of the source's pages, then
carried on by a subscription from the exact LSN the restored target's
recovery ended at - with a writer on the source throughout: before the
slot, during the copy, while the target recovers, after it goes on.

Nothing is lost and nothing is applied twice, and each is shown on a
table that would show it: `log` has no key and is only inserted into, so
a transaction applied twice is a second row and one missed is a missing
id; `t` has a key and is updated in place. Then migkit's own check reads
both sides and agrees - and says "different" once a row is changed on the
target behind the subscription's back.

Measured on the first run of this test (colima, 2 CPUs): the WAL the copy
streamed ended at the segment after the backup's own end (the backup ends
with a switch of segment), so the target's recovery ended past the
backup's end LSN - which is why the position is read from the target and
the backup's end is only held under it.
"""
import socket
import subprocess
import threading
import time

import pytest
from click.testing import CliRunner

NET = "migkit-test-phys-net"
SRC, DST = "migkit-test-phys-src", "migkit-test-phys-dst"
VOL = "migkit-test-phys-dstdata"
SRC_PORT, DST_PORT = 16120, 16121
DATA = "/var/lib/postgresql/data"


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _psql(name, sql, db="postgres"):
    got = subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=test", name, "psql", "-h",
         "127.0.0.1", "-U", "postgres", "-d", db, "-At", "-v",
         "ON_ERROR_STOP=1", "-c", sql], capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _ready(name, port, most=120):
    end = time.time() + most
    while time.time() < end:
        ok = subprocess.run(["docker", "exec", name, "pg_isready", "-h",
                             "127.0.0.1", "-U", "postgres"],
                            capture_output=True).returncode == 0
        with socket.socket() as s:
            s.settimeout(2)
            if ok and s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(1)
    pytest.fail(f"{name} never answered")


def _rm():
    subprocess.run(["docker", "rm", "-f", "-v", SRC, DST],
                   capture_output=True)
    subprocess.run(["docker", "volume", "rm", VOL], capture_output=True)


@pytest.fixture(scope="module")
def pair():
    if not _docker():
        pytest.skip("docker not available")
    _rm()
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", SRC, "--network", NET,
                    "-e", "POSTGRES_PASSWORD=test", "-p",
                    f"127.0.0.1:{SRC_PORT}:5432", "postgres:16", "-c",
                    "wal_level=logical"], check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", DST, "--network", NET,
                    "-e", "POSTGRES_PASSWORD=test", "-v", f"{VOL}:{DATA}",
                    "-p", f"127.0.0.1:{DST_PORT}:5432", "postgres:16", "-c",
                    "wal_level=logical"], check=True, capture_output=True)
    try:
        _ready(SRC, SRC_PORT)
        _ready(DST, DST_PORT)
        # the DBA's part: a replication connection from the target's host
        subprocess.run(["docker", "exec", SRC, "sh", "-c",
                        "echo 'host replication all all scram-sha-256' >>"
                        f" {DATA}/pg_hba.conf"], check=True)
        _psql(SRC, "select pg_reload_conf()")
        _psql(SRC, "create database app")
        _psql(SRC, "create table t (id bigint primary key, v text,"
                   " n int default 0);"
                   " create table log (id bigint, at timestamptz default"
                   " now());"
                   " create index on t (v);"
                   " insert into t select g, md5(g::text) from"
                   " generate_series(1, 200000) g;"
                   " insert into log select g from generate_series(1, 1000) g",
              "app")
        yield
    finally:
        _rm()
        subprocess.run(["docker", "network", "rm", NET], capture_output=True)


@pytest.fixture(scope="module")
def hop(pair, tmp_path_factory):
    """One hop for the module: the second move reads what the first
    recorded."""
    import migkit.config as cfg
    tmp_path = tmp_path_factory.mktemp("seed")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  seed:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {SRC_PORT}, user: postgres,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {DST_PORT}, user: postgres,"
        " password: test}\n"
        "    options:\n      physical:\n"
        f"        data: {DATA}\n"
        f"        run: docker run --rm -i --network {NET} -u postgres -v"
        f" {VOL}:{DATA} postgres:16 sh -c\n"
        f"        stop: docker stop {DST}\n"
        f"        start: docker start {DST}\n"
        f"        source: {SRC}:5432\n")
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(cfg, "CONF", str(conf))
        mp.setattr(cfg, "REPORTS", tmp_path / "reports")
        from migkit.config import get_hop
        yield get_hop("seed")


class _Writer(threading.Thread):
    """Inserts into `log` and updates `t` on the source, one transaction
    after another, until told to stop; the ids it wrote, kept."""

    def __init__(self):
        super().__init__(daemon=True)
        self.stop, self.ids, self.error = threading.Event(), [], None

    def run(self):
        import psycopg2
        try:
            conn = psycopg2.connect(host="127.0.0.1", port=SRC_PORT,
                                    user="postgres", password="test",
                                    dbname="app")
            conn.autocommit = True
            i = 100000
            with conn.cursor() as cur:
                while not self.stop.is_set():
                    i += 1
                    cur.execute("begin; insert into log values (%s);"
                                " update t set n = n + 1, v = v || 'u'"
                                " where id = %s; insert into t (id, v)"
                                " values (%s, 'new'); commit",
                                (i, i % 5000 + 1, 1_000_000 + i))
                    self.ids.append(i)
            conn.close()
        except Exception as e:  # noqa: BLE001 - asserted by the test
            self.error = e


def _caught_up(most=120):
    lsn = _psql(SRC, "select pg_current_wal_lsn()", "app")
    end = time.time() + most
    while time.time() < end:
        if _psql(SRC, "select coalesce(bool_and(confirmed_flush_lsn >="
                      f" '{lsn}'), false) from pg_replication_slots where"
                      " slot_type = 'logical'", "app") == "t":
            return
        time.sleep(1)
    pytest.fail("the subscription never caught up with the source")


def test_a_target_that_is_not_empty_is_refused_with_the_reason(hop):
    from migkit import physical
    from migkit.engines import get_engine
    _psql(DST, "create table stray (id int)")
    try:
        rung, why = physical.choose(hop, get_engine(hop), "full+cdc")
        assert rung is None
        assert "the target holds 1 tables (postgres.public.stray)" in why, \
            why
    finally:
        _psql(DST, "drop table stray")


def test_the_dry_run_stops_nothing(hop):
    from migkit import cli
    got = CliRunner().invoke(cli.main, ["move", "seed", "--mode",
                                        "full+cdc"])
    assert got.exit_code == 0, got.output
    out = " ".join(got.output.split())
    assert "a copy of the source's pages" in out, out
    assert "dry-run: nothing stopped, copied or made" in out, out
    assert "pg_basebackup" not in out
    running = subprocess.run(["docker", "inspect", "-f",
                              "{{.State.Running}}", DST],
                             capture_output=True, text=True).stdout.strip()
    assert running == "true"
    assert _psql(SRC, "select count(*) from pg_replication_slots") == "0"


def test_seeded_by_its_pages_and_carried_on_exactly(hop):
    from migkit import cli, physical
    from migkit.engines import get_engine
    writer = _Writer()
    writer.start()
    time.sleep(2)
    got = CliRunner().invoke(cli.main, ["move", "seed", "--mode",
                                        "full+cdc", "--go"])
    out = " ".join(got.output.split())
    assert got.exit_code == 0, out + str(got.exception or "")
    assert "the copy is proved against its manifest" in out, out
    # going on while the subscription applies, then quiet
    time.sleep(4)
    writer.stop.set()
    writer.join(30)
    assert writer.error is None, writer.error
    assert len(writer.ids) > 20, len(writer.ids)
    _caught_up()
    rec = physical.seeded(hop)
    # the copy streamed past the backup's own end, and the target says where
    assert physical.lsn_value(rec["lsn"]) >= physical.lsn_value(
        rec["copied_to"]), rec
    assert rec["slots"] == {"app": physical.seed_slot(get_engine(hop),
                                                      "app")}
    # nothing lost, nothing applied twice: every id once, on both sides
    want = 1000 + len(writer.ids)
    for side in (SRC, DST):
        count, distinct = _psql(side, "select count(*) || ' ' ||"
                                      " count(distinct id) from log",
                                "app").split()
        assert (int(count), int(distinct)) == (want, want), (side, count,
                                                             distinct)
    rows = "select md5(string_agg(id || ':' || v || ':' || n, ',' order by" \
           " id)) from t"
    assert _psql(SRC, rows, "app") == _psql(DST, rows, "app")
    ids = "select md5(string_agg(id::text, ',' order by id, at)) from log"
    assert _psql(SRC, ids, "app") == _psql(DST, ids, "app")
    # migkit's own check agrees, every table read on both sides
    eng = get_engine(hop)
    data = [r for r in eng.check_data("app") if r.check == "data"]
    assert [r.status for r in data] == ["ok"], [(r.status, r.detail)
                                               for r in data]
    assert "2 tables" in data[0].detail, data[0].detail
    # and says different, once a row differs behind the stream's back
    _psql(DST, "update t set v = 'changed on the target' where id = 7",
          "app")
    data = [r for r in eng.check_data("app") if r.check == "data"]
    assert [r.status for r in data] == ["diff"], [(r.status, r.detail)
                                                 for r in data]
    assert "public.t" in data[0].detail, data[0].detail
    _psql(DST, "update t set v = md5('7') where id = 7", "app")


def test_a_second_move_goes_on_from_the_same_copy(hop):
    """The copy is the target now: a run again neither copies again nor
    sets the subscription's origin back."""
    from migkit import cli, physical
    before = physical.seeded(hop)
    _psql(SRC, "insert into log values (-1)", "app")
    got = CliRunner().invoke(cli.main, ["move", "seed", "--mode",
                                        "full+cdc", "--go"])
    out = " ".join(got.output.split())
    assert got.exit_code == 0, out + str(got.exception or "")
    assert "going on from it" in out and "already going on from the copy" \
        in out, out
    assert physical.seeded(hop) == before
    _caught_up()
    assert _psql(DST, "select count(*) from log where id = -1", "app") == "1"

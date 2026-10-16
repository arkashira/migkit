"""The export side never runs under the replica role, and the apply side
asks for it only where the target has a trigger or a key to keep quiet
(P0-DEPTH.triggers): three range workers load a trigger-free table with
the session left as it is, a trigger on the source fires for a write made
while the exports are open (counted), and the bulk paths' rung
(`decide.quiet_way`) reads the target: `none` for a table with nothing to
keep quiet, the restore's own switch or the session role for one with a
trigger.

On the shared PostgreSQL 16 pair, tables named for this module alone."""
import os
import threading

import pytest

from migkit import decide, movers
from migkit.config import Endpoint, Hop
from migkit.engines import pg_pass
from migkit.engines.postgres import PostgresEngine
from tests.conftest import needs_docker, psql

PROVES = [("verify.structure.triggers", "postgres")]

pytestmark = needs_docker

PLAIN, TRIG, LOG = "w18k_plain", "w18k_trig", "w18k_log"
WORKERS, EACH, WAIT = 3, 50, 60
FUNC = (f"create or replace function w18k_count() returns trigger"
        f" language plpgsql as $$ begin insert into {LOG} values"
        " (new.id); return new; end $$")


def _hop(pg_pair):
    return Hop(name="w18k", engine="postgres",
               source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                               user="postgres", password="test"),
               target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                               user="postgres", password="test"),
               databases=["postgres"])


@pytest.fixture(autouse=True)
def client_programs(monkeypatch):
    for bin_ in ("/opt/homebrew/opt/libpq/bin", "/usr/local/opt/libpq/bin"):
        if os.path.isdir(bin_):
            monkeypatch.setenv("PATH", bin_ + os.pathsep + os.environ["PATH"])


@pytest.fixture
def pair(pg_pair):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, f"drop table if exists {PLAIN}, {TRIG}, {LOG} cascade")
    # the source keeps a trigger of its own: it must fire for its writes
    assert psql(pg_pair["src"],
                f"create table {TRIG} (id int primary key, v text);"
                f" create table {LOG} (n bigint); {FUNC};"
                f" create trigger w18k_count after insert on {TRIG}"
                " for each row execute function w18k_count()"
                ).returncode == 0
    assert psql(pg_pair["dst"],
                f"create table {PLAIN} (id int primary key, v text);"
                f" create table {TRIG} (id int primary key, v text);"
                f" create table {LOG} (n bigint); {FUNC};"
                f" create trigger w18k_count after insert on {TRIG}"
                " for each row execute function w18k_count()"
                ).returncode == 0
    yield pg_pair
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, f"drop table if exists {PLAIN}, {TRIG}, {LOG} cascade")


def _count(port, table):
    return int(psql(port, f"select count(*) from {table}").stdout.strip())


def test_three_workers_load_a_table_with_nothing_to_keep_quiet_as_origin(
        pair):
    eng = PostgresEngine(_hop(pair))
    wrote = threading.Barrier(WORKERS + 1, timeout=WAIT)
    leave = threading.Barrier(WORKERS + 1, timeout=WAIT)
    roles, errors = [], []

    def work(n):
        try:
            with eng.table_hold("postgres", f"public.{PLAIN}"):
                with eng._connect("dst", "postgres", "write") as conn:
                    roles.append(conn.execute(
                        "show session_replication_role").fetchone()[0])
                    for i in range(n * EACH, (n + 1) * EACH):
                        conn.execute(
                            f"insert into {PLAIN} values (%s, 'x')", (i,))
                    conn.commit()
                wrote.wait()
                leave.wait()
        except BaseException as e:  # noqa: BLE001 - reported to the test
            errors.append(e)
            wrote.abort()
            leave.abort()
    threads = [threading.Thread(target=work, args=(n,), daemon=True)
               for n in range(WORKERS)]
    for t in threads:
        t.start()
    fired = None
    try:
        wrote.wait()
        # while the workers hold: the export connection is origin, and a
        # write on the source fires the source's trigger
        export = pg_pass._Export(eng, "postgres", "pg_current_snapshot")
        try:
            assert export.conn.execute(
                "show session_replication_role").fetchone()[0] == "origin"
            assert psql(pair["src"], f"insert into {TRIG} values (1, 's')"
                        ).returncode == 0
            fired = _count(pair["src"], LOG)
            assert eng.__dict__.get("_as_replica", set()) == set()
        finally:
            export.close()
    finally:
        try:
            leave.wait()
        except threading.BrokenBarrierError:
            pass
        for t in threads:
            t.join(WAIT)
    assert not errors, errors
    assert fired == 1
    assert roles == ["origin"] * WORKERS
    assert _count(pair["dst"], PLAIN) == WORKERS * EACH


def test_the_bulk_path_asks_the_target_and_only_the_target(pair):
    hop = _hop(pair)
    # nothing to keep quiet on the loaded table: the session is left alone
    assert movers._pg_quiet_triggers(hop, "postgres", [PLAIN]) == "none"
    # a trigger on it: the restore's own switch for a superuser
    assert movers._pg_quiet_triggers(hop, "postgres", [TRIG]) == "flag"
    # the source's address is never given the role
    assert "session_replication_role" not in movers._pg_uri(
        hop.source, "postgres")
    assert "session_replication_role" in movers._pg_uri(
        hop.target, "postgres", quiet_triggers=True)
    # the rung records why
    got = decide.quiet_way({"triggers": [TRIG], "superuser": False,
                            "settable": True})
    assert got.path == "session"
    assert decide.quiet_way({"triggers": [TRIG]}).path is None
    assert decide.quiet_way({}).path == "untouched"

"""A CDC leg that does not need the target to dial the source.

`CREATE SUBSCRIPTION` runs on the target and connects back to the source.
Plenty of migrations cannot do that at all - the route only goes one way -
and where it can, the target ends up storing the source's password in
`pg_subscription` (G2). `pgcopydb follow` runs where migkit runs and opens
both connections itself.

Wrapping it plainly would ship three measured traps. On pgcopydb 0.18:

* **Apply is off by default.** Without `stream sentinel set apply` nothing
  is ever written to the target. The log says only "Waiting until the
  pgcopydb sentinel apply is enabled", and the run looks healthy.
* **It reports progress the target does not have.** With apply held back,
  `replay_lsn` came back equal to `write_lsn` - fully replayed - while the
  target held 0 rows of 200, and the source's slot had advanced to the head
  with **56 bytes** of WAL retained. The source stops being the safety net
  the moment pgcopydb writes a change to its own directory, which is why
  that directory belongs under the hop's reports and not in `/tmp`.
* **`--endpos` is the stop nothing else here offers** - and the target's
  origin does not reach it. Measured: endpos 0/156D578, final origin
  0/1567D28, on a run that carried every row correctly. So the run ends
  when the process exits, not when an LSN comparison comes true. That is
  the same trap as D12 and this is the second place it had to be learned.

The source keeps its guarantee: checked before and after a run, it gained
no schema and no table, only a publication - the same object the native
path already creates.
"""
import os
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

SRC, DST = 15587, 15588
NS, ND = "migkit-test-follow-src", "migkit-test-follow-dst"


def _hop(tmp_path, name="fl", dbs=("postgres",)):
    hop = Hop(name=name, engine="postgres",
              source=Endpoint(host="127.0.0.1", port=SRC, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST, user="postgres",
                              password="test"),
              databases=list(dbs))
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return hop


def test_it_is_off_unless_the_environment_asks(monkeypatch):
    """Every hop in use takes the native path, and none of them may change
    because this exists."""
    from migkit import movers
    monkeypatch.delenv("MIGKIT_CDC", raising=False)
    assert movers.follow_selected() == ""
    for value in ("follow", "FOLLOW", " follow "):
        monkeypatch.setenv("MIGKIT_CDC", value)
        assert movers.follow_selected() == "follow"
    monkeypatch.setenv("MIGKIT_CDC", "native")
    assert movers.follow_selected() == "native"


def test_a_name_it_does_not_know_is_refused(monkeypatch):
    """Not silently ignored: "MIGKIT_CDC=folow" would otherwise run the
    path the operator was trying to avoid."""
    from migkit import movers
    monkeypatch.setenv("MIGKIT_CDC", "carrier-pigeon")
    with pytest.raises(SystemExit) as e:
        movers.follow_selected()
    assert "native" in str(e.value) and "follow" in str(e.value)


def test_it_is_an_environment_variable_and_not_a_flag():
    """Which path is possible is a fact about the network between the two
    servers, not an operator preference, so it stays off the command
    surface the way MIGKIT_MOVER does."""
    import pathlib
    cli = (pathlib.Path(__file__).resolve().parents[1] / "migkit" /
           "cli.py").read_text()
    assert "--mode" in cli
    for invented in ("--follow", "--cdc-via", "--via-follow"):
        assert invented not in cli, invented


def test_the_slot_name_is_one_postgres_will_accept(tmp_path):
    """Measured rather than assumed - `pg_create_logical_replication_slot`
    on 16 answers `replication slot name "migkit_Prod_EU_db" contains an
    invalid character`, with the hint that only lower case letters, numbers
    and underscores are allowed. The origin namespace is not so strict, so
    the two names are deliberately not the same string."""
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop(tmp_path, name="Prod-EU"))
    slot = eng.follow_slot("MyDB")
    assert slot == slot.lower(), slot
    assert all(c.islower() or c.isdigit() or c == "_" for c in slot), slot
    assert len(slot) <= 63
    assert eng.follow_origin("MyDB").lower() == slot


def test_the_state_directory_is_not_a_temp_dir(tmp_path):
    """Because the source releases WAL as soon as pgcopydb has written a
    change there, that directory is the only copy until the target has it.
    A `/tmp` sweep between the write and the apply is data loss."""
    from migkit import movers
    got = movers.follow_dir(_hop(tmp_path), "postgres")
    assert str(got).startswith(str(tmp_path)), got
    assert got.name == "follow"


def test_the_plan_names_the_two_things_a_bare_wrap_would_miss(tmp_path):
    from migkit import movers
    steps = movers.pgcopydb_follow(_hop(tmp_path), "postgres", go=False)
    joined = "\n".join(steps)
    assert "sentinel set apply" in joined, joined
    assert "nothing is ever applied" in joined, joined
    assert "sentinel set endpos --current" in joined, joined
    assert "--origin migkit_fl_postgres" in joined, joined
    assert "--slot-name migkit_fl_postgres" in joined, joined


def test_the_plan_says_what_it_does_not_carry(tmp_path):
    """The slot is created when the leg starts, so anything written before
    that is not in it. Measured end to end: 22 rows on the source, 4 on the
    target, and those 4 exactly right - which is correct, and a surprise if
    nobody said so."""
    from migkit import movers
    joined = "\n".join(movers.pgcopydb_follow(_hop(tmp_path), "postgres",
                                              go=False))
    assert "before the slot" in joined and "are not carried" in joined, joined


def test_a_dry_run_starts_nothing(tmp_path):
    from migkit import movers
    movers.pgcopydb_follow(_hop(tmp_path), "postgres", go=False)
    assert not (tmp_path / "follow").exists()


def _sql(name, sql, db="postgres"):
    p = subprocess.run(["docker", "exec", name, "psql", "-U", "postgres",
                        "-d", db, "-q", "-v", "ON_ERROR_STOP=1", "-c", sql],
                       capture_output=True, text=True)
    assert p.returncode == 0, p.stderr
    return p.stdout


def _sql1(name, sql, db="postgres"):
    return subprocess.run(["docker", "exec", name, "psql", "-U", "postgres",
                           "-d", db, "-tAc", sql],
                          capture_output=True, text=True).stdout.strip()


def _catalogue(name):
    return (_sql1(name, "select coalesce(string_agg(nspname,','order by"
                        " nspname),'') from pg_namespace where nspname not"
                        " like 'pg\\_%' and nspname <> 'information_schema'"),
            _sql1(name, "select coalesce(string_agg(schemaname||'.'||"
                        "tablename,','order by 1),'') from pg_tables where"
                        " schemaname not in ('pg_catalog',"
                        "'information_schema')"))


@pytest.fixture(scope="module")
def pair():
    for n in (NS, ND):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    for n, p in ((NS, SRC), (ND, DST)):
        subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                        "POSTGRES_PASSWORD=test", "-p", f"{p}:5432",
                        "postgres:16", "-c", "wal_level=logical",
                        "-c", "max_replication_slots=8",
                        "-c", "max_wal_senders=8"],
                       check=True, capture_output=True)
    try:
        for n, p in ((NS, SRC), (ND, DST)):
            end = time.time() + 180
            while time.time() < end:
                with socket.socket() as s:
                    s.settimeout(2)
                    if s.connect_ex(("127.0.0.1", p)) == 0:
                        break
                time.sleep(1)
            for _ in range(90):
                if subprocess.run(["docker", "exec", n, "pg_isready", "-U",
                                   "postgres"],
                                  capture_output=True).returncode == 0:
                    break
                time.sleep(1)
            else:
                pytest.fail(f"{n} never answered")
        for n in (NS, ND):
            _sql(n, "create table t (id int primary key, v text)")
        yield
    finally:
        subprocess.run(["pkill", "-f", "pgcopydb follow"],
                       capture_output=True)
        for n in (NS, ND):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


needs_pgcopydb = pytest.mark.skipif(
    subprocess.run(["which", "pgcopydb"], capture_output=True).returncode != 0,
    reason="pgcopydb binary not installed")


@needs_docker
@needs_pgcopydb
def test_it_carries_changes_without_the_target_dialling_the_source(pair,
                                                                   tmp_path):
    """The whole point, end to end. Nothing on the target is told where the
    source is: the only connection out of the target is the one pgcopydb
    opens from here."""
    from migkit import movers
    hop = _hop(tmp_path)
    before = _catalogue(NS)
    stop = []

    def writer():
        i = 0
        while not stop:
            i += 1
            subprocess.run(["docker", "exec", NS, "psql", "-U", "postgres",
                            "-q", "-c", f"insert into t values ({i},'w{i}')"],
                           capture_output=True)
            time.sleep(0.3)

    import threading
    th = threading.Thread(target=writer, daemon=True)
    th.start()
    try:
        time.sleep(2)
        steps = movers.pgcopydb_follow(hop, "postgres", go=True, timeout=240)
    finally:
        stop.append(True)
        th.join(timeout=20)
    assert any("target applied up to" in s for s in steps), steps

    dst = _sql1(ND, "select coalesce(string_agg(id||'='||v,','order by id),'')"
                    " from t")
    assert dst, "the leg applied nothing"
    ids = [int(p.split("=")[0]) for p in dst.split(",")]
    # a contiguous window: the leg carries what happened between the slot
    # being created and the end position, and nothing else
    assert ids == list(range(ids[0], ids[-1] + 1)), ids
    src = _sql1(NS, "select coalesce(string_agg(id||'='||v,','order by id),'')"
                    f" from t where id between {ids[0]} and {ids[-1]}")
    assert dst == src, (dst, src)

    # and the promise about the source held
    assert _catalogue(NS)[0] == before[0], "a schema appeared on the source"
    assert _catalogue(NS)[1] == before[1], "a table appeared on the source"
    assert _sql1(NS, "select count(*) from pg_publication") == "1"

    # the position it reported is the target's own, not the mover's opinion
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(hop)
    assert eng.applied_lsn("postgres") == _sql1(
        ND, "select remote_lsn::text from pg_replication_origin_status"
            f" where external_id = '{eng.follow_origin('postgres')}'")


@needs_docker
@needs_pgcopydb
def test_tearing_it_down_leaves_nothing_pinning_wal(pair, tmp_path):
    """An abandoned logical slot pins WAL until the source runs out of
    disk - a source outage caused by the migration tooling (E5). All three
    objects are named by migkit, so all three can be taken back."""
    from migkit import movers
    hop = _hop(tmp_path)
    assert _sql1(NS, "select count(*) from pg_replication_slots") == "1", \
        "the previous test should have left one"
    steps = movers.follow_teardown(hop, "postgres", go=True)
    assert _sql1(NS, "select count(*) from pg_replication_slots") == "0"
    assert _sql1(NS, "select count(*) from pg_publication") == "0"
    assert _sql1(ND, "select count(*) from pg_replication_origin") == "0"
    assert any("state directory left in place" in s for s in steps), steps


@needs_docker
@needs_pgcopydb
def test_tearing_down_a_leg_that_never_ran_is_not_an_error(pair, tmp_path):
    """Running `--drop` twice, or on a hop that never started one, has to
    be quiet - otherwise the safe habit of tearing down becomes one people
    stop doing."""
    from migkit import movers
    movers.follow_teardown(_hop(tmp_path, name="never-ran"), "postgres",
                           go=True)
    assert _sql1(NS, "select count(*) from pg_replication_slots") == "0"

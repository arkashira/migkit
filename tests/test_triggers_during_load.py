"""A trigger on the target, rewriting the rows as they land.

The source says a row was last touched in 2001. A `BEFORE INSERT` trigger on
the target says `updated_at := now()`. The load runs, reports success, and
the column now holds the date of the migration for every row in the table.

This was measured through a real `migkit move --mode full --go`, not
reasoned about:

    source rows          2001-01-01+00        2002-02-02+00
    landed on target     2026-09-19 07:22+00  2026-09-19 07:22+00
    migkit said          postgres: bulk copy complete

The verification is not fooled - the values differ, so `migkit check`
reports it afterwards. But afterwards is the whole problem.

The fix is a **connection** option, `session_replication_role = replica`,
carried on the target URI. Not `ALTER TABLE ... DISABLE TRIGGER`: disabling
triggers is a change to the target that outlives a crash, and a target left
with disabled triggers is the exact failure `check --deep` already reports.
The repair path has always set this; the mover was the inconsistent one.

The `pg_dump`/`pg_restore` path was already covered, because migkit passes
`--disable-triggers` there. Only the pgcopydb path was exposed, and a test
below runs both.
"""
import subprocess

import pytest

from migkit import movers
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

SOURCE = """
drop table if exists public.notes;
create table public.notes (id int primary key, body text,
                           updated_at timestamptz);
insert into public.notes values (1,'a','2001-01-01'),(2,'b','2002-02-02');
"""

TARGET = """
drop table if exists public.notes;
create table public.notes (id int primary key, body text,
                           updated_at timestamptz);
create or replace function stamp() returns trigger as $$
begin new.updated_at := now(); return new; end $$ language plpgsql;
drop trigger if exists notes_stamp on public.notes;
create trigger notes_stamp before insert on public.notes
  for each row execute function stamp();
"""


@pytest.fixture
def stamped(pg_pair):
    for port, sql in ((pg_pair["src"], SOURCE), (pg_pair["dst"], TARGET)):
        got = psql(port, sql)
        assert got.returncode == 0, got.stderr
    return pg_pair


def _landed(pg_pair):
    return psql(pg_pair["dst"], "select id||' '||updated_at::text from"
                                " public.notes order by id").stdout.split()


def _move(pg_pair, tmp_path, monkeypatch, mover):
    from click.testing import CliRunner

    from migkit import cli
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  trig:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n    workers: 1\n")
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", mover)
    got = CliRunner().invoke(cli.main, ["move", "trig", "--mode", "full",
                                        "--go"])
    assert got.exit_code == 0, got.output
    return got.output


def test_the_trigger_really_does_rewrite_an_unguarded_load(stamped):
    """The control. Without this, a passing test below would prove only
    that the trigger was never firing."""
    got = subprocess.run(
        ["docker", "exec", "-i", "-e", "PGPASSWORD=test",
         "migkit-test-pg-dst", "psql", "-U", "postgres", "-c",
         "copy public.notes (id, body, updated_at) from stdin"],
        input="9\tc\t2003-03-03 00:00:00+00\n", capture_output=True,
        text=True)
    assert got.returncode == 0, got.stderr
    stored = psql(stamped["dst"], "select updated_at::text from public.notes"
                                  " where id = 9").stdout.strip()
    assert not stored.startswith("2003"), stored


def test_a_move_keeps_the_values_the_source_sent(stamped, tmp_path,
                                                  monkeypatch):
    if movers.pgcopydb_runner() == "":
        pytest.skip("pgcopydb not available")
    out = _move(stamped, tmp_path, monkeypatch, "pgcopydb")
    assert "bulk copy complete" in out, out
    assert _landed(stamped) == ["1", "2001-01-01", "00:00:00+00",
                                "2", "2002-02-02", "00:00:00+00"], \
        _landed(stamped)


def test_the_dump_path_keeps_them_too(stamped, tmp_path, monkeypatch):
    """It always did - `pg_restore --disable-triggers` - and this pins that
    so the two movers cannot drift apart on something this quiet."""
    out = _move(stamped, tmp_path, monkeypatch, "pgdump")
    assert "bulk copy complete" in out, out
    assert _landed(stamped) == ["1", "2001-01-01", "00:00:00+00",
                                "2", "2002-02-02", "00:00:00+00"], \
        _landed(stamped)


def test_the_trigger_is_still_enabled_afterwards(stamped, tmp_path,
                                                  monkeypatch):
    """The reason this is a connection option and not `DISABLE TRIGGER`. A
    move that quiets a trigger permanently has traded one silent corruption
    for another, and a crash mid-move must not be able to leave it that
    way."""
    _move(stamped, tmp_path, monkeypatch, "pgdump")

    state = psql(stamped["dst"], "select tgenabled::text from pg_trigger"
                                 " where tgname = 'notes_stamp'").stdout.strip()
    assert state == "O", state

    # and it fires, which is what `tgenabled` only claims
    psql(stamped["dst"], "insert into public.notes values (9,'c',"
                         "'2003-03-03')")
    after = psql(stamped["dst"], "select updated_at::text from public.notes"
                                 " where id = 9").stdout.strip()
    assert not after.startswith("2003"), after


def test_the_target_uri_carries_the_option_and_the_source_does_not():
    """The source is only read; asking it to behave like a replica would be
    a change to somebody's session for no reason."""
    assert movers.QUIET_TRIGGERS.startswith("?options=")
    assert "session_replication_role" in movers.QUIET_TRIGGERS
    assert "replica" in movers.QUIET_TRIGGERS

    hop = Hop(name="t", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=2, user="u",
                              password="p"), databases=["x"], workers=1)
    steps = movers.pgcopydb_move(hop, "x", 1, False, None)
    # the connection strings are in the command the step runs; the plan
    # itself says what happens and carries no command line
    line = " ".join(s.command for s in steps if getattr(s, "argv", None))
    assert line.count(movers.QUIET_TRIGGERS) == 1, line
    assert f":2/x{movers.QUIET_TRIGGERS}" in line, line
    assert f":1/x{movers.QUIET_TRIGGERS}" not in line, line

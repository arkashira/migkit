"""Which triggers the load is about to silence.

`migkit move` runs with `session_replication_role = replica`, so the
target's triggers do not fire for the migrated rows. That is deliberate and
measured - a `BEFORE INSERT` trigger setting `updated_at := now()` rewrote
every migrated row to the date of the migration - but work that does not
happen is still worth naming. An audit trigger records nothing for the
migrated rows; a denormalised counter is not maintained.

The check used to look one way only:

    deep postgres triggers: OK no disabled triggers on target

while two user triggers sat on a table the load was about to write. It now
says both halves.

Two kinds of noise are deliberately excluded, because a line that lists
things nobody can act on is a line people learn to skip:

* **internal triggers** - a foreign key is implemented as a pair of
  constraint triggers, and the target here really does carry
  `RI_ConstraintTrigger_c_*` rows for its FK;
* **tables the load will not write** - one that exists on the target alone
  is never touched by a move.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = 15527, 15528
NAMES = {SRC: "migkit-test-trg-src", DST: "migkit-test-trg-dst"}

BOTH = """
create table notes (id int primary key, body text, updated_at timestamptz);
create table parent (id int primary key);
create table child (id int primary key, pid int references parent(id));
"""

TARGET_ONLY = """
create table onlyhere (id int primary key);
create function stamp() returns trigger as $$
begin new.updated_at := now(); return new; end $$ language plpgsql;
create trigger notes_stamp before insert on notes
  for each row execute function stamp();
create trigger notes_audit after update on notes
  for each row execute function stamp();
create trigger onlyhere_t before insert on onlyhere
  for each row execute function stamp();
"""


def q(port, sql):
    return subprocess.run(
        ["docker", "exec", NAMES[port], "psql", "-U", "postgres", "-d",
         "postgres", "-At", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)


@pytest.fixture(scope="module")
def trg_pair():
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
        assert q(port, BOTH).returncode == 0
    assert q(DST, TARGET_ONLY).returncode == 0
    yield {"src": SRC, "dst": DST}
    for name in NAMES.values():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


def _engine(trg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="tg", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=trg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=trg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=1)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _triggers(trg_pair, tmp_path):
    got = [r for r in _engine(trg_pair, tmp_path).check_deep("postgres")
           if r.scope.endswith("triggers")]
    assert len(got) == 1, [r.scope for r in got]
    return got[0]


def test_the_target_really_carries_all_three_kinds(trg_pair):
    """The control. If the FK were not there, or the target-only table had
    no trigger, the exclusions below would pass for the wrong reason."""
    rows = q(trg_pair["dst"],
             "select c.relname||'/'||t.tgname||'/'||t.tgisinternal::int::text"
             " from pg_trigger t join pg_class c on c.oid = t.tgrelid"
             " where c.relname in ('notes','child','onlyhere') order by 1"
             ).stdout.split()
    internal = [r for r in rows if r.endswith("/1")]
    assert internal, rows          # the FK's constraint triggers exist
    assert any("onlyhere" in r for r in rows), rows
    assert sum("notes/" in r for r in rows) == 2, rows


def test_the_enabled_triggers_a_load_writes_over_are_named(trg_pair,
                                                            tmp_path):
    got = _triggers(trg_pair, tmp_path)
    assert got.status == "ok", got.detail
    assert "2 enabled on tables a load writes" in got.detail, got.detail
    assert "public.notes.notes_stamp" in got.detail, got.detail
    assert "public.notes.notes_audit" in got.detail, got.detail
    assert "session_replication_role = replica" in got.detail, got.detail


def test_the_noise_is_left_out(trg_pair, tmp_path):
    """A foreign key's constraint triggers, and a table the move will never
    write, are both present on this target and neither belongs in the
    line."""
    got = _triggers(trg_pair, tmp_path)
    assert "RI_Constraint" not in got.detail, got.detail
    assert "onlyhere" not in got.detail, got.detail


def test_a_disabled_trigger_is_still_the_fault(trg_pair, tmp_path):
    """The verdict this check has always had must survive the addition -
    a target left with a trigger switched off is a difference, not a note
    at the end of an ok line."""
    assert q(trg_pair["dst"], "alter table notes disable trigger notes_stamp"
             ).returncode == 0
    try:
        got = _triggers(trg_pair, tmp_path)
        assert got.status == "diff", got.detail
        assert "disabled on target" in got.detail, got.detail
        assert "notes_stamp" in got.detail, got.detail
        # and it does not also lecture about the quieted ones
        assert "session_replication_role" not in got.detail, got.detail
    finally:
        q(trg_pair["dst"], "alter table notes enable trigger notes_stamp")


def test_a_target_with_no_triggers_says_so_plainly(trg_pair, tmp_path):
    for name in ("notes_stamp", "notes_audit"):
        assert q(trg_pair["dst"], f"drop trigger {name} on notes"
                 ).returncode == 0
    try:
        got = _triggers(trg_pair, tmp_path)
        assert got.status == "ok", got.detail
        assert "none enabled on the tables a load would write" in got.detail
    finally:
        q(trg_pair["dst"], TARGET_ONLY.split("create trigger", 1)[0]
          .replace("create table onlyhere (id int primary key);", "")
          + "create trigger notes_stamp before insert on notes"
            " for each row execute function stamp();"
            " create trigger notes_audit after update on notes"
            " for each row execute function stamp();")


def test_the_verdict_needs_no_server(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="t", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)

    off = eng._trigger_result("x", ["public.t.t_a"], ["public.t.t_b"], "h")
    assert off.status == "diff", off.detail
    # the fault wins the line rather than sharing it
    assert "t_b" not in off.detail, off.detail

    quiet = eng._trigger_result("x", [], ["public.t.t_b"], "h")
    assert quiet.status == "ok" and "1 enabled" in quiet.detail

    none = eng._trigger_result("x", [], [], "h")
    assert none.status == "ok"
    assert "none enabled on the tables a load would write" in none.detail

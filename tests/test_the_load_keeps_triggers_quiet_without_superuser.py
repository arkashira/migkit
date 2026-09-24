"""A PostgreSQL load into a target whose user is not a superuser.

The dump path asked the restore to disable every trigger, which only a
superuser may do: a managed service's admin user is not one. Measured as a
plain table owner on PostgreSQL 16, with a foreign key between two tables:
* the restore's attempt was refused on the foreign key's system triggers,
  `permission denied: "RI_ConstraintTrigger_c_16399" is a system trigger`
* the child table's rows were then refused on the foreign key, 100 of 100
* the load reported it as complete (before `_RestoreLog`)

Now the target's user decides the way:
* a superuser keeps the restore's own switch
* a user allowed `session_replication_role` loads as a replica, which
  changes no table
* a user with neither is refused before anything is emptied, naming the
  grant - unless nothing it loads has a trigger to keep quiet
"""
import pytest
from click.testing import CliRunner

from migkit import movers
from tests.conftest import needs_docker, psql

pytestmark = [needs_docker, pytest.mark.skipif(
    not (movers.which("pg_dump") and movers.which("pg_restore")),
    reason="the PostgreSQL dump programs are not installed")]


@pytest.fixture
def owner(pg_pair, tmp_path, monkeypatch):
    """The target's tables belong to `app`, which is no superuser."""
    import migkit.config as cfg
    psql(pg_pair["dst"], "drop owned by app; drop role if exists app")
    psql(pg_pair["dst"], "create role app login password 'test'")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  tq:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: app, password: test}\n"
        "    databases: [postgres]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "pgdump")
    yield
    psql(pg_pair["dst"], "revoke set on parameter session_replication_role"
                         " from app")
    psql(pg_pair["dst"], "drop owned by app; drop role if exists app")


def _tables(pg_pair, with_keys=True):
    ref = " references public.parent (id)" if with_keys else ""
    ddl = ("create table public.parent (id int primary key, at date);"
           f" create table public.child (id int primary key, p int{ref})")
    psql(pg_pair["src"], ddl + "; insert into public.parent select g,"
                               " '2001-01-01' from generate_series(1, 100) g;"
                               " insert into public.child select g, g from"
                               " generate_series(1, 100) g")
    psql(pg_pair["dst"], ddl + "; alter table public.parent owner to app;"
                               " alter table public.child owner to app;"
                               " insert into public.parent values (7, null)")


def _move():
    from migkit import cli
    got = CliRunner().invoke(cli.main, ["move", "tq", "--go"])
    return got, " ".join((got.output + str(got.exception or "")).split())


def _count(pg_pair, table):
    return psql(pg_pair["dst"], f"select count(*) from public.{table}"
                ).stdout.strip()


def test_a_plain_owner_is_stopped_before_anything_is_emptied(owner,
                                                             pg_pair):
    _tables(pg_pair)
    got, said = _move()
    assert got.exit_code != 0, said
    assert "can neither turn them off for the load nor load as a replica" \
        in said, said
    assert "GRANT SET ON PARAMETER session_replication_role TO app" in said
    # the row the target had is still there
    assert _count(pg_pair, "parent") == "1"


def test_an_owner_allowed_the_replica_role_loads_everything(owner, pg_pair):
    _tables(pg_pair)
    psql(pg_pair["dst"], "grant set on parameter session_replication_role"
                         " to app")
    # a trigger that would rewrite what arrives: quiet for the load
    psql(pg_pair["dst"], "create function public.stamp() returns trigger"
                         " language plpgsql as $$ begin new.at := now();"
                         " return new; end $$;"
                         " create trigger stamp before insert on"
                         " public.parent for each row execute function"
                         " public.stamp()")
    try:
        got, said = _move()
    finally:
        psql(pg_pair["dst"], "drop function if exists public.stamp()"
                             " cascade")
    assert got.exit_code == 0, said
    assert _count(pg_pair, "parent") == "100"
    assert _count(pg_pair, "child") == "100"
    assert psql(pg_pair["dst"], "select count(*) from public.parent"
                                " where at = '2001-01-01'").stdout.strip() \
        == "100"


def test_a_plain_owner_with_nothing_to_quiet_still_loads(owner, pg_pair):
    _tables(pg_pair, with_keys=False)
    got, said = _move()
    assert got.exit_code == 0, said
    assert _count(pg_pair, "child") == "100"

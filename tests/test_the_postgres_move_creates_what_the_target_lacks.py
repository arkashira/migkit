"""The PostgreSQL bulk paths create what the target lacks, and the
streaming path carries what it used to leave behind.

Measured before, on PostgreSQL 16:
* onto a target database without the tables, the dump path said `bulk copy
  complete` with nothing loaded (`test_the_restore_says_what_it_refused.py`
  is why it now stops instead)
* the streaming copy empties each table itself, and on a target with a
  foreign key it stopped on `cannot truncate a table referenced in a
  foreign key constraint`
* after a streaming copy of 52 rows the target's sequence stayed at 1, and
  the first insert failed on `duplicate key ... (id)=(1)`

Now the tables are created from the source's definition before the load,
and their keys, indexes and constraints are added after it: every object
of the source's on a target with no tables, only the missing tables
otherwise. A target whose foreign keys the streaming copy cannot empty
around goes through a local copy, and the streaming copy sets the target's
sequences afterwards.
"""
import json

import pytest
from click.testing import CliRunner

from migkit import movers
from tests.conftest import needs_docker, psql

pytestmark = [needs_docker, pytest.mark.skipif(
    not (movers.which("pg_dump") and movers.which("pg_restore")),
    reason="the PostgreSQL dump programs are not installed")]

SOURCE = ("create type mood as enum ('ok', 'sad');"
          " create table public.parent (id serial primary key, m mood);"
          " create table public.child (id int primary key,"
          "  p int references public.parent (id));"
          " create index child_p on public.child (p);"
          " create table public.audit (id int primary key);"
          " create view public.v_parent as select id from public.parent;"
          " insert into public.parent (m)"
          "  select 'ok' from generate_series(1, 52);"
          " insert into public.child values (1, 1), (2, 52);"
          " insert into public.audit values (1)")


@pytest.fixture
def hop(pg_pair, tmp_path, monkeypatch):
    import migkit.config as cfg
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  pc:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n    exclude: [public.audit]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    psql(pg_pair["src"], SOURCE)
    yield tmp_path
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "drop view if exists public.v_parent;"
                   " drop table if exists public.child, public.parent;"
                   " drop type if exists mood")


def _move(monkeypatch, via):
    from migkit import cli
    monkeypatch.setenv("MIGKIT_MOVER", via)
    got = CliRunner().invoke(cli.main, ["move", "pc", "--go"])
    return got, " ".join((got.output + str(got.exception or "")).split())


def _dst(pg_pair, sql):
    got = psql(pg_pair["dst"], sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _landed(pg_pair):
    assert _dst(pg_pair, "select count(*) from public.parent") == "52"
    assert _dst(pg_pair, "select count(*) from public.child") == "2"
    # the keys and the index came after the rows
    assert _dst(pg_pair, "select string_agg(conname, ',' order by conname)"
                         " from pg_constraint where conrelid ="
                         " 'public.child'::regclass") == \
        "child_p_fkey,child_pkey"
    assert _dst(pg_pair, "select count(*) from pg_indexes"
                         " where indexname = 'child_p'") == "1"
    # the next id is past every row
    assert _dst(pg_pair, "select nextval('public.parent_id_seq')") == "53"


def test_a_target_with_no_tables_gets_the_whole_schema(hop, pg_pair,
                                                       monkeypatch):
    got, said = _move(monkeypatch, "pgdump")
    assert got.exit_code == 0, said
    _landed(pg_pair)
    # the type and the view came with it; what the hop excludes did not
    assert _dst(pg_pair, "select count(*) from public.v_parent") == "52"
    assert _dst(pg_pair, "select to_regclass('public.audit') is null") == "t"
    assert not (hop / "reports" / "pc" / "postgres" /
                "created-tables.json").exists()


def test_only_the_missing_table_is_created(hop, pg_pair, monkeypatch):
    psql(pg_pair["dst"], "create type mood as enum ('ok', 'sad');"
                         " create table public.parent (id serial primary"
                         " key, m mood);"
                         " comment on table public.parent is 'the target''s'")
    got, said = _move(monkeypatch, "pgdump")
    assert got.exit_code == 0, said
    assert "create the tables the target does not have yet, from the" \
        " source's definition: 1" in said, said
    _landed(pg_pair)
    # the table it had is loaded and left as the target made it
    assert _dst(pg_pair, "select obj_description('public.parent'::regclass)"
                ) == "the target's"


def test_keys_are_added_on_the_next_run_after_a_failed_load(
        hop, pg_pair, monkeypatch):
    real = movers._pgdump_restore

    def fails(*a, **k):
        raise RuntimeError("the load stopped")

    monkeypatch.setattr(movers, "_pgdump_restore", fails)
    got, said = _move(monkeypatch, "pgdump")
    assert got.exit_code != 0, said
    record = hop / "reports" / "pc" / "postgres" / "created-tables.json"
    assert json.loads(record.read_text())["whole"] is True
    monkeypatch.setattr(movers, "_pgdump_restore", real)
    got, said = _move(monkeypatch, "pgdump")
    assert got.exit_code == 0, said
    _landed(pg_pair)
    assert not record.exists()


needs_the_stream = pytest.mark.skipif(
    not movers.which("pgcopydb"), reason="the streaming copy is not"
                                          " installed")


@needs_the_stream
def test_the_streaming_copy_onto_a_target_with_no_tables(hop, pg_pair,
                                                         monkeypatch):
    got, said = _move(monkeypatch, "pgcopydb")
    assert got.exit_code == 0, said
    _landed(pg_pair)


@needs_the_stream
def test_foreign_keys_on_the_target_take_the_local_copy(hop, pg_pair,
                                                        monkeypatch):
    got, said = _move(monkeypatch, "pgdump")
    assert got.exit_code == 0, said
    psql(pg_pair["dst"], "truncate public.child, public.parent")
    got, said = _move(monkeypatch, "pgcopydb")
    assert got.exit_code == 0, said
    _landed(pg_pair)


@needs_the_stream
def test_the_streaming_copy_sets_the_sequences(hop, pg_pair, monkeypatch):
    """No foreign key, so the streaming copy itself runs; the sequence the
    target already had stays at 1 unless something sets it."""
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "drop view if exists public.v_parent;"
                   " drop table if exists public.child")
    psql(pg_pair["dst"], "create type mood as enum ('ok', 'sad');"
                         " create table public.parent (id serial primary"
                         " key, m mood)")
    got, said = _move(monkeypatch, "pgcopydb")
    assert got.exit_code == 0, said
    assert _dst(pg_pair, "select count(*) from public.parent") == "52"
    assert _dst(pg_pair, "select nextval('public.parent_id_seq')") == "53"


def test_what_the_target_made_ready_is_left_as_it_is(hop, pg_pair,
                                                     monkeypatch):
    """No tables yet, but the type they need made ready by hand: the whole
    schema goes on around it rather than stopping on `already exists`."""
    psql(pg_pair["dst"], "create type mood as enum ('ok', 'sad')")
    got, said = _move(monkeypatch, "pgdump")
    assert got.exit_code == 0, said
    _landed(pg_pair)

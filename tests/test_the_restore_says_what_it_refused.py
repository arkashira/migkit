"""A PostgreSQL restore that the target refused is not a finished move.

A newer dump writes session settings an older server does not have, and
the restore exits 1 over them with every row in place, so that one failure
is tolerated. The tolerance used to be for any restore ending in `errors
ignored on restore`, which is how every failed restore ends. Measured onto
a target without the tables: every `COPY` failed on `relation
"public.parent" does not exist`, and the move said `bulk copy complete`
with nothing loaded. The same held for a row the target's column could not
take.
"""
import pytest
from click.testing import CliRunner

from migkit import movers
from tests.conftest import needs_docker, psql


def _feed(lines):
    got = movers._RestoreLog()
    for line in lines:
        got(line)
    return got


SETTING = ['pg_restore: error: could not execute query: ERROR:  unrecognized'
           ' configuration parameter "transaction_timeout"',
           "Command was: SET transaction_timeout = 0;"]


def test_only_a_setting_the_server_lacks_is_tolerated():
    assert _feed(SETTING * 3 + ["pg_restore: warning: errors ignored on"
                                " restore: 3"]).real() == []
    got = _feed(SETTING + [
        'pg_restore: error: could not execute query: ERROR:  relation'
        ' "public.parent" does not exist',
        "Command was: COPY public.parent (id, m) FROM stdin;",
        "pg_restore: warning: errors ignored on restore: 2"]).real()
    assert got == ['ERROR: relation "public.parent" does not exist'], got


def test_context_lines_between_refusals_are_not_refusals():
    """Measured in a parallel restore: `from TOC entry ...` arrived as an
    error line of its own, between a refusal and its statement. It was
    read as a refusal - `the target refused 1 statements of the load:
    from TOC entry 3421; ...` - and a restore that had met only settings
    was stopped."""
    got = _feed([SETTING[0], "pg_restore: while PROCESSING TOC:",
                 "pg_restore: error: from TOC entry 3421; 1262 16384"
                 " DATABASE app postgres", SETTING[0], SETTING[1],
                 "pg_restore: warning: errors ignored on restore: 2"])
    assert got.real() == [], got.refused


def test_a_refused_copy_is_read_as_one():
    got = _feed(['pg_restore: error: COPY failed for table "t": ERROR:'
                 '  value "70000" is out of range for type smallint'])
    assert got.real() == ['ERROR: value "70000" is out of range for type'
                          ' smallint'], got.refused


def test_a_refusal_it_could_not_read_is_not_tolerated():
    """The restore's own count is the floor: one it says it ignored and
    this did not read is real, never a setting."""
    got = _feed(SETTING + ["pg_restore: warning: errors ignored on"
                           " restore: 2"]).real()
    assert got == ["1 more the restore counted and did not say"], got


def test_a_table_name_is_still_read_as_progress():
    got = movers._RestoreLog()
    assert got('pg_restore: processing data for table "public.t"') == \
        "public.t: loaded (1 tables)"


def _hop(pg_pair, tmp_path, monkeypatch):
    import migkit.config as cfg
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  rs:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "pgdump")


def _move():
    from migkit import cli
    got = CliRunner().invoke(cli.main, ["move", "rs", "--go"])
    return got, " ".join((got.output + str(got.exception or "")).split())


needs_the_dump = pytest.mark.skipif(
    not (movers.which("pg_dump") and movers.which("pg_restore")),
    reason="the PostgreSQL dump programs are not installed")


@needs_docker
@needs_the_dump
def test_a_target_without_the_tables_is_not_a_finished_move(
        pg_pair, tmp_path, monkeypatch):
    """The move now creates the tables a target lacks
    (`test_the_postgres_move_creates_what_the_target_lacks.py`); without
    that step, this is the load it measured."""
    monkeypatch.setattr(movers, "_pg_create_missing", lambda *a, **k: None)
    _hop(pg_pair, tmp_path, monkeypatch)
    psql(pg_pair["src"], "create table public.parent (id int primary key);"
                         " insert into public.parent values (1), (2)")
    got, said = _move()
    assert got.exit_code != 0, said
    assert "bulk copy complete" not in said, said
    assert 'relation "public.parent" does not exist' in said, said


@needs_docker
@needs_the_dump
def test_a_row_the_target_cannot_take_is_not_a_finished_move(
        pg_pair, tmp_path, monkeypatch):
    _hop(pg_pair, tmp_path, monkeypatch)
    psql(pg_pair["src"], "create table public.t (id int primary key, n int);"
                         " insert into public.t values (1, 1), (2, 70000)")
    psql(pg_pair["dst"], "create table public.t (id int primary key,"
                         " n smallint)")
    got, said = _move()
    assert got.exit_code != 0, said
    assert "bulk copy complete" not in said, said
    assert "out of range for type smallint" in said, said


@needs_docker
@needs_the_dump
def test_a_restore_that_landed_every_row_still_completes(
        pg_pair, tmp_path, monkeypatch):
    _hop(pg_pair, tmp_path, monkeypatch)
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.t (id int primary key, n int)")
    psql(pg_pair["src"], "insert into public.t values (1, 1), (2, 2)")
    got, said = _move()
    assert got.exit_code == 0, said
    assert "bulk copy complete" in said, said
    assert psql(pg_pair["dst"], "select count(*) from public.t"
                ).stdout.strip() == "2"


def test_an_object_already_there_is_tolerated_only_where_asked():
    """Creating what the target lacks meets what it has: a schema or type
    made ready for the move is left as it is. Loading rows is never one of
    those."""
    got = _feed(['pg_restore: error: could not execute query: ERROR:  type'
                 ' "mood" already exists',
                 "Command was: CREATE TYPE public.mood AS ENUM ('ok');"])
    assert got.real(existing_ok=True) == []
    assert got.real() == ['ERROR: type "mood" already exists']

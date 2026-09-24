"""A mover that exits 0 and moves nothing must not be called complete.

This is not hypothetical. pgcopydb built against a newer PostgreSQL emits
`SET transaction_timeout = 0`; an older server rejects it, and a
whole-database `clone` was measured reporting each rejection separately
while moving no rows at all. Any external mover can fail this way - the
shape is "the tool exited 0, the log looked busy, the target is empty".

Re-measured for the `copy table-data` path migkit actually uses: the server
does reject the parameter (it appears in the PostgreSQL 16 log) and the rows
arrive anyway - 50,000 and then 2,000,000 of them. That is what makes it
reasonable to run the local binary rather than insisting on the
version-matched container image, and this guard is what makes it safe: the
outcome is checked, not assumed.

The speed that buys, on this hardware: a 555 MB table in 4.1 s against the
dump path's 11.7 s, and 2,093 MB of large objects in 18.2 s against 93.6 s.
"""
import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _engine(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="moved", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=2)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _both(pg_pair, sql):
    for port in (pg_pair["src"], pg_pair["dst"]):
        got = psql(port, sql)
        assert got.returncode == 0, got.stderr


def test_an_empty_target_table_is_named(pg_pair, tmp_path):
    _both(pg_pair, "drop table if exists public.moved;"
                   " create table public.moved (id bigint primary key,"
                   " v text);")
    psql(pg_pair["src"], "insert into public.moved select g, 'v'||g from"
                         " generate_series(1,500) g;")
    eng = _engine(pg_pair, tmp_path)
    assert eng.moved_nothing("postgres") == ["public.moved"]


def test_a_target_that_received_the_rows_is_clean(pg_pair, tmp_path):
    _both(pg_pair, "drop table if exists public.moved;"
                   " create table public.moved (id bigint primary key,"
                   " v text);"
                   " insert into public.moved select g, 'v'||g from"
                   " generate_series(1,500) g;")
    eng = _engine(pg_pair, tmp_path)
    assert eng.moved_nothing("postgres") == []


def test_one_row_is_enough_to_pass_the_guard(pg_pair, tmp_path):
    """The guard answers "did anything arrive", not "is it correct" - that
    is `migkit check`'s job, and conflating them would make this expensive
    for no extra safety."""
    _both(pg_pair, "drop table if exists public.moved;"
                   " create table public.moved (id bigint primary key,"
                   " v text);")
    psql(pg_pair["src"], "insert into public.moved select g, 'v'||g from"
                         " generate_series(1,500) g;")
    psql(pg_pair["dst"], "insert into public.moved values (1, 'v1');")
    eng = _engine(pg_pair, tmp_path)
    assert eng.moved_nothing("postgres") == []


def test_a_table_empty_on_both_sides_is_not_a_failure(pg_pair, tmp_path):
    _both(pg_pair, "drop table if exists public.moved;"
                   " create table public.moved (id bigint primary key,"
                   " v text);")
    eng = _engine(pg_pair, tmp_path)
    assert eng.moved_nothing("postgres") == []


def test_the_move_command_refuses_to_report_success(pg_pair, tmp_path,
                                                     monkeypatch):
    """The guard where it matters: the command an operator runs."""
    from click.testing import CliRunner

    from migkit import cli, movers
    _both(pg_pair, "drop table if exists public.moved;"
                   " create table public.moved (id bigint primary key,"
                   " v text);")
    psql(pg_pair["src"], "insert into public.moved select g, 'v'||g from"
                         " generate_series(1,500) g;")

    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  moved:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n    workers: 1\n")
    # the config path is resolved when `migkit.config` is imported, so the
    # environment variable is too late here - and a test that fell back to
    # the machine's real hops.yaml would be reading somebody's production
    # endpoints
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")

    # a mover that does exactly what the failure looks like: says its piece,
    # exits without error, moves nothing
    monkeypatch.setattr(movers, "chosen", lambda engine, table="": "pgdump")
    monkeypatch.setattr(movers, "run_via",
                        lambda *a, **k: ["# pretended to copy everything"])

    got = CliRunner().invoke(cli.main, ["move", "moved", "--mode", "full",
                                        "--go"])
    assert got.exit_code != 0, got.output
    assert "still empty on the target" in got.output, got.output
    assert "public.moved" in got.output, got.output
    assert "bulk copy complete" not in got.output, got.output


def test_it_says_so_when_the_engine_cannot_answer(tmp_path):
    """An engine with no way to look must not read as a clean bill: the
    base returns None, which the caller has to tell apart from []."""
    from migkit.engines.base import Engine
    hop = Hop(name="x", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    assert Engine(hop).moved_nothing("x") is None


# ---- what the hop told the copy to skip ----

def _conf(tmp_path, pg_pair, monkeypatch, extra=""):
    import migkit.config as cfg
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  moved:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n    workers: 1\n" + extra)
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def test_a_table_the_hop_excludes_is_not_a_failed_copy(pg_pair, tmp_path,
                                                       monkeypatch):
    """Measured before the fix, on a real move: the carried table arrived,
    the excluded one was left alone as the hop asked, and the command said

        pgdump reported success and appdb is still empty on the target:
        public.audit_log. Nothing has been marked as moved

    - a correct move reported as failed, blamed on the copy for skipping a
    table it had been told to skip, and never recorded as done. A table the
    hop excludes is one the target owns; a new one is empty by nature."""
    from click.testing import CliRunner

    from migkit import cli, movers
    _both(pg_pair, "drop table if exists public.moved;"
                   " drop table if exists public.audit_log;"
                   " create table public.moved (id bigint primary key);"
                   " create table public.audit_log (id bigint primary key);")
    psql(pg_pair["src"], "insert into public.audit_log values (1);")
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "insert into public.moved values (1);")
    _conf(tmp_path, pg_pair, monkeypatch, "    exclude: [audit_log]\n")
    monkeypatch.setattr(movers, "chosen", lambda engine, table="": "pgdump")
    monkeypatch.setattr(movers, "run_via",
                        lambda *a, **k: ["# copied what it was told to"])

    got = CliRunner().invoke(cli.main, ["move", "moved", "--mode", "full",
                                        "--go"])
    assert got.exit_code == 0, got.output
    assert "still empty" not in got.output, got.output
    assert "bulk copy complete" in got.output, got.output


def test_an_excluded_table_does_not_hide_a_real_failure(pg_pair, tmp_path,
                                                        monkeypatch):
    """The filter must take out only what the hop excludes. A carried table
    left empty beside it is still the failure this guard exists for."""
    from click.testing import CliRunner

    from migkit import cli, movers
    _both(pg_pair, "drop table if exists public.moved;"
                   " drop table if exists public.audit_log;"
                   " create table public.moved (id bigint primary key);"
                   " create table public.audit_log (id bigint primary key);")
    psql(pg_pair["src"], "insert into public.audit_log values (1);"
                         " insert into public.moved values (1);")
    _conf(tmp_path, pg_pair, monkeypatch, "    exclude: [audit_log]\n")
    monkeypatch.setattr(movers, "chosen", lambda engine, table="": "pgdump")
    monkeypatch.setattr(movers, "run_via",
                        lambda *a, **k: ["# pretended to copy everything"])

    got = CliRunner().invoke(cli.main, ["move", "moved", "--mode", "full",
                                        "--go"])
    assert got.exit_code != 0, got.output
    # the failure names what stayed empty; the plan above it names the
    # excluded table too, as left alone, which is the other half of the
    # same truth
    said = " ".join(got.output.split())
    empty = said.split("still empty on the target:", 1)[1].split(". ")[0]
    assert "public.moved" in empty, said
    assert "public.audit_log" not in empty, said
    assert "public.audit_log: left alone" in said, said


def test_the_refusal_does_not_name_the_program_that_ran(pg_pair, tmp_path,
                                                         monkeypatch):
    """The name came from a variable, so the scan over string constants in
    `test_the_report_does_not_name_its_tools.py` could not see it - and the
    operator read `pgdump reported success`, the name of something they did
    not install and cannot run."""
    from click.testing import CliRunner

    from migkit import cli, movers
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    _both(pg_pair, "drop table if exists public.moved;"
                   " create table public.moved (id bigint primary key);")
    psql(pg_pair["src"], "insert into public.moved values (1);")
    _conf(tmp_path, pg_pair, monkeypatch)
    for via in ("pgdump", "pgcopydb"):
        monkeypatch.setattr(movers, "chosen",
                            lambda engine, table="", _v=via: _v)
        monkeypatch.setattr(movers, "run_via", lambda *a, **k: [])
        got = CliRunner().invoke(cli.main, ["move", "moved", "--mode",
                                            "full", "--go"])
        assert got.exit_code != 0, got.output
        said = got.output.lower()
        assert "still empty on the target" in said, said
        # the label itself, not only the shared list: `pgdump` is not in
        # TOOLS, so checking the list alone would pass for that one without
        # having looked
        assert via not in said, said
        assert not [t for t in TOOLS if t in said], said


def test_a_table_the_target_does_not_have_is_named(pg_pair, tmp_path):
    """Asking only about the tables both sides have passed a target with
    none of them: nothing had arrived, and nothing was said."""
    _both(pg_pair, "drop table if exists public.moved;")
    psql(pg_pair["src"], "create table public.moved (id bigint primary key);"
                         " insert into public.moved values (1)")
    eng = _engine(pg_pair, tmp_path)
    assert eng.moved_nothing("postgres") == ["public.moved"]
    psql(pg_pair["src"], "drop table public.moved")

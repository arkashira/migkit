"""The check compares what the hop takes and nothing else.

A job told to leave a log table behind moves the rest; the target's copy
of that table, if it has one, is not the source's. Measured on the pair:
with `include` naming the tables the move took, a log table that differs
does not make the check differ, and a taken table that differs does.
"""
import json

import pytest
from click.testing import CliRunner

from tests.conftest import needs_docker, psql

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.listings", "postgres")]

pytestmark = needs_docker


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


@pytest.fixture
def taken(pg_pair, tmp_path, monkeypatch):
    import migkit.config as cfg
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  inc:\n    engine: postgres\n    goal: verify\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n"
        "    include: [postgres.public.orders, postgres.public.lines]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.orders (id int primary key, v text);"
                   " create table public.lines (id int primary key, n int);"
                   " create table public.event_log (id int primary key,"
                   " at text);"
                   " insert into public.orders select g, 'o' || g from"
                   " generate_series(1, 50) g;"
                   " insert into public.lines select g, g from"
                   " generate_series(1, 20) g")
    psql(pg_pair["src"], "insert into public.event_log select g, 'e' || g"
                         " from generate_series(1, 30) g")
    yield tmp_path
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "drop table if exists public.orders, public.lines,"
                   " public.event_log")


def test_a_table_the_hop_does_not_take_is_not_compared(taken, pg_pair):
    got, said = _run("check", "inc", "--only", "data")
    # narrowed to the data check, and nothing found in what it took
    assert got.exit_code == 3 and "the run was narrowed to" in said, said
    assert "event_log" not in said, said
    env = json.loads((taken / "reports" / "inc" / "verdict.json")
                     .read_text())
    assert env["scope"]["include"] == ["postgres.public.orders",
                                       "postgres.public.lines"]
    assert not env["has_differences"]
    assert "event_log" not in json.dumps(env["findings"])


def test_a_table_the_hop_takes_that_differs_is_a_difference(taken,
                                                            pg_pair):
    psql(pg_pair["dst"], "delete from public.orders where id > 40")
    got, said = _run("check", "inc", "--only", "data")
    assert got.exit_code == 1 and "orders" in said, said
    assert "event_log" not in said, said


def test_the_schema_check_leaves_what_the_hop_does_not_take(taken, pg_pair):
    psql(pg_pair["dst"], "alter table public.event_log add column x int")
    got, said = _run("check", "inc", "--only", "schema")
    assert got.exit_code == 3 and "event_log" not in said, said
    psql(pg_pair["dst"], "alter table public.lines add column x int")
    got, said = _run("check", "inc", "--only", "schema")
    assert got.exit_code == 1 and "lines" in said, said

"""A clean count of a database nothing could be read from.

Counts ride along with the checksum pass rather than scanning the tables
twice. That is the right trade until the pass cannot read a table at all,
at which point there is nothing left to add up - and this used to report
the absence as agreement:

    counts   postgres: OK 0 tables, rows 0==0
    data     postgres: ERROR  errors: public.sales

Measured with a role holding a column-level grant, which is a natural way
to keep a migration tool away from columns it has no business reading:
`GRANT SELECT (id) ON sales` makes `select *` fail, and PostgreSQL's error
names the **table** rather than the column, which is its own trap.

Nobody was badly misled in a default run - `data` errored beside it and the
verdict was `error`. But "OK, 0 tables" is the same shape as a mover
reporting success over an empty target, which `moved_nothing` already
refuses, so it is refused here too.

The two cases that must stay apart:

    no tables at all            -> ok, there is genuinely nothing to count
    tables that could not be    -> error, naming them
      read

Told apart by whether any table exists on both sides, not by the count
being zero.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = 15527, 15528
NAMES = {SRC: "migkit-test-cnt-src", DST: "migkit-test-cnt-dst"}

SEED = """
create table sales (id int primary key, amt numeric);
insert into sales select g, g*10 from generate_series(1,100) g;
create role partial login password 'test';
grant usage on schema public to partial;
grant select (id) on sales to partial;
"""


def q(port, sql, user="postgres"):
    got = subprocess.run(
        ["docker", "exec", NAMES[port], "psql", "-U", user, "-d", "postgres",
         "-At", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)
    return got


@pytest.fixture(scope="module")
def cnt_pair():
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
        got = q(port, SEED)
        assert got.returncode == 0, got.stderr
    yield {"src": SRC, "dst": DST}
    for name in NAMES.values():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


def _check(cnt_pair, tmp_path, monkeypatch, user, only="counts,data"):
    from click.testing import CliRunner

    from migkit import cli
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  p:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {cnt_pair['src']},"
        f" user: {user}, password: test}}\n"
        f"    target: {{host: 127.0.0.1, port: {cnt_pair['dst']},"
        f" user: {user}, password: test}}\n"
        "    databases: [postgres]\n    workers: 1\n")
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return CliRunner().invoke(cli.main, ["check", "p", "--only", only]).output


def _line(out, name):
    for line in out.splitlines():
        if line.startswith(name):
            return line
    raise AssertionError(f"no {name} line in:\n{out}")


def test_the_role_really_cannot_read_the_table(cnt_pair):
    """The control. Without it, everything below could pass because the
    grant was never restrictive."""
    denied = q(cnt_pair["src"], "select * from sales limit 1", "partial")
    assert denied.returncode != 0
    assert "permission denied for table" in denied.stderr, denied.stderr
    # and the misleading part: it says table when the grant is per column
    assert "column" not in denied.stderr.split("\n")[0], denied.stderr
    # counting one readable column is still allowed, which is why counts
    # run on its own is honest here
    ok = q(cnt_pair["src"], "select count(*) from sales", "partial")
    assert ok.returncode == 0 and ok.stdout.strip() == "100", ok.stdout


def test_counts_refuses_to_report_a_clean_zero(cnt_pair, tmp_path,
                                                monkeypatch):
    out = _check(cnt_pair, tmp_path, monkeypatch, "partial")
    counts = _line(out, "counts")
    assert "ERROR" in counts, counts
    assert "could not be read" in out, out
    assert "public.sales" in out, out
    assert ": OK" not in counts, counts
    # data still reports it too, which is where the error itself belongs
    assert "ERROR" in _line(out, "data"), out


def test_a_role_that_can_read_everything_still_gets_a_count(cnt_pair,
                                                             tmp_path,
                                                             monkeypatch):
    """The other half: this must not have turned into a check that errors
    whenever it feels uncertain."""
    out = _check(cnt_pair, tmp_path, monkeypatch, "postgres")
    counts = _line(out, "counts")
    assert "OK" in counts, counts
    assert "rows 100==100" in counts, counts


def test_counts_on_its_own_is_unchanged(cnt_pair, tmp_path, monkeypatch):
    """`--only counts` does not go through the checksum pass at all, and
    was already honest: `count(*)` needs one readable column, and the
    hundred it reports is real."""
    out = _check(cnt_pair, tmp_path, monkeypatch, "partial", only="counts")
    counts = _line(out, "counts")
    assert "OK" in counts, counts
    assert "100 rows both sides" in counts, counts


def test_an_empty_database_is_not_an_error(cnt_pair, tmp_path, monkeypatch):
    """Zero tables counted is only alarming when there were tables to
    count. Crying wolf on an empty database would make the new message
    worth ignoring."""
    for port in NAMES:
        assert q(port, "drop table if exists sales").returncode == 0
    try:
        out = _check(cnt_pair, tmp_path, monkeypatch, "postgres")
        counts = _line(out, "counts")
        assert "OK" in counts, counts
        assert "no tables on both sides to count" in counts, counts
    finally:
        for port in NAMES:
            q(port, "create table sales (id int primary key, amt numeric);"
                    " insert into sales select g, g*10 from"
                    " generate_series(1,100) g;"
                    " grant select (id) on sales to partial;")


def test_the_failed_table_list_is_read_in_one_place(tmp_path):
    """`check_data` and the counts derived from the same output both need
    to know which tables failed, and only one of them used to look."""
    from migkit.engines.postgres import PostgresEngine
    out = ("public.a: OK rows=5\n"
           "public.b: ERROR ERROR:  permission denied for table b\n"
           "public.c: DIFF src=1|x dst=2|y\n")
    assert PostgresEngine._failed_tables(out) == ["public.b"]
    assert PostgresEngine._failed_tables("public.a: OK rows=5\n") == []

    n, rs, rd, bad = PostgresEngine._parse_fast(out)
    # the parser still ignores the error line, which is why the caller has
    # to ask separately rather than trusting n
    assert n == 2, n

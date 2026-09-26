"""Whether the target answers the source's own reads as well as the source
does (backlog 24, problems file G1) - PostgreSQL.

The source's busiest reads come from its statement statistics, and each
is planned on both sides; one with no parameters left in it is also run on
both, read-only, warmed and alternated. A table the source reaches through
an index and the target reads whole is a finding. A finding is `warn`,
never `diff` - the data is the same - and it stops `check` only where the
hop says `performance: gate`.
"""
import subprocess
import time

import pytest
from click.testing import CliRunner

from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = ("migkit-test-perf-src", 15803), ("migkit-test-perf-dst", 15804)
SEED = ("create table public.t (id int primary key, k int, pad text);"
        " insert into public.t select g, g % 50000, repeat('x', 60)"
        " from generate_series(1, 300000) g;"
        " create index t_k on public.t (k); analyze public.t;")


def _sql(name, sql, user="postgres"):
    got = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                          "psql", "-U", user, "-d", "postgres", "-At", "-c",
                          sql], capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    try:
        for (name, port), extra in ((SRC, ["-c", "shared_preload_libraries"
                                                 "=pg_stat_statements"]),
                                    (DST, [])):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                            "POSTGRES_PASSWORD=test", "-p", f"{port}:5432",
                            "postgres:16", *extra], check=True,
                           capture_output=True)
        for name, _ in (SRC, DST):
            for _ in range(60):
                if subprocess.run(["docker", "exec", name, "pg_isready",
                                   "-U", "postgres"],
                                  capture_output=True).returncode == 0:
                    break
                time.sleep(1)
            time.sleep(2)
            _sql(name, SEED)
        _sql(SRC[0], "create extension pg_stat_statements;"
                     " create role app login password 'test';"
                     " grant select on public.t to app")
        # the application's reads, as the application: the hop's own
        # user's statements are migkit's, and are left out
        for _ in range(30):
            _sql(SRC[0], "select count(*) from public.t where k = 777",
                 user="app")
            _sql(SRC[0], "select max(k) from public.t", user="app")
        yield
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _engine(tmp_path, **options):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine

    def ep(port):
        return Endpoint(host="127.0.0.1", port=port, user="postgres",
                        password="test")
    hop = Hop(name="perf", engine="postgres", source=ep(SRC[1]),
              target=ep(DST[1]), databases=["postgres"], options=options)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def test_the_same_indexes_answer_as_well(pair, tmp_path):
    from migkit import workload
    got = workload.compare(_engine(tmp_path), "postgres")
    assert got.status == "ok", got.detail
    assert "2 of the source's busiest reads planned on both sides, 1 timed" \
        in got.detail, got.detail


def test_an_index_that_did_not_come_over_is_found(pair, tmp_path):
    from migkit import workload
    _sql(DST[0], "drop index public.t_k")
    try:
        got = workload.compare(_engine(tmp_path), "postgres")
        assert got.status == "warn", got.detail
        assert "reads t whole on the target where the source uses index" \
               " t_k" in got.detail, got.detail
        # the statement is shown normalised, never with its literal
        assert "777" not in got.detail, got.detail
    finally:
        _sql(DST[0], "create index t_k on public.t (k); analyze public.t")


def test_no_statement_statistics_is_said_not_passed(pair, tmp_path):
    from migkit import workload
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="127.0.0.1", port=DST[1], user="postgres",
                  password="test")
    got = workload.compare(PostgresEngine(Hop(
        name="p", engine="postgres", source=ep, target=ep,
        databases=["postgres"])), "postgres")
    assert got.status == "skip" and "pg_stat_statements" in got.detail


def test_a_slow_target_stops_check_only_when_gated(pair, tmp_path,
                                                   monkeypatch):
    import migkit.config as cfg
    from migkit import cli
    conf = tmp_path / "hops.yaml"

    def hop(options):
        conf.write_text(
            "hops:\n  perf:\n    engine: postgres\n"
            f"    source: {{host: 127.0.0.1, port: {SRC[1]}, user: postgres,"
            " password: test}\n"
            f"    target: {{host: 127.0.0.1, port: {DST[1]}, user: postgres,"
            " password: test}\n    databases: [postgres]\n"
            f"    options: {options}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    _sql(DST[0], "drop index public.t_k")
    try:
        import re

        def problems(output):
            m = re.search(r"(\d+) problems", output)
            return int(m.group(1)) if m else 0
        hop("{}")
        got = CliRunner().invoke(cli.main, ["check", "perf", "--only",
                                            "deep"])
        said = " ".join(got.output.split())
        assert "answer the source's reads more slowly on the target" in said
        assert "postgres performance: WARN" in said, said
        hop("{performance: gate}")
        gated = CliRunner().invoke(cli.main, ["check", "perf", "--only",
                                              "deep"])
        assert gated.exit_code != 0, gated.output
        assert "answer the source's reads more slowly" not in gated.output
        # the one difference the gate makes is the slow reads: the pair
        # differs in other ways on purpose (the source's statistics module
        # and its application role), and those count either way
        assert problems(gated.output) == problems(got.output) + 1, (
            got.output, gated.output)
    finally:
        _sql(DST[0], "create index t_k on public.t (k); analyze public.t")

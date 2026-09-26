"""A stream back at cutover, or both ways, where the hop asks (backlogs 36
and 37).

Both write to the source, so both are off unless the hop's options say:

    reverse: at_cutover     tearing the stream down starts one back, from
                            the new primary to the old source
    topology: two_way       both ways, all the time

At cutover the stream forward goes first and the one back starts from
that moment: what the application writes on the new primary from then on
reaches the old source, so abandoning the cutover loses nothing, and
nothing goes round. Both ways at once needs each side to tell its own
changes from the other's - a subscription with `origin = none`, which is
PostgreSQL 16 on both sides - and is refused without it.
"""
import subprocess
import time

import pytest
from click.testing import CliRunner

from tests.conftest import needs_docker

NET = "migkit-test-rev-net"
A, B = ("migkit-test-rev-a", 15807), ("migkit-test-rev-b", 15808)


def _sql(name, sql):
    got = subprocess.run(["docker", "exec", name, "psql", "-U", "postgres",
                          "-At", "-v", "ON_ERROR_STOP=1", "-c", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _ip(name):
    return subprocess.run(["docker", "inspect", "-f",
                           "{{range .NetworkSettings.Networks}}"
                           "{{.IPAddress}}{{end}}", name],
                          capture_output=True, text=True).stdout.strip()


@pytest.fixture(scope="module")
def pair():
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    try:
        for name, port in (A, B):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", name,
                            "--network", NET, "-e", "POSTGRES_PASSWORD=test",
                            "-p", f"{port}:5432", "postgres:16", "-c",
                            "wal_level=logical"], check=True,
                           capture_output=True)
        for name, _ in (A, B):
            for _ in range(60):
                if subprocess.run(["docker", "exec", name, "pg_isready",
                                   "-U", "postgres"],
                                  capture_output=True).returncode == 0:
                    break
                time.sleep(1)
            time.sleep(2)
        yield {A[1]: _ip(A[0]), B[1]: _ip(B[0])}
    finally:
        for name, _ in (A, B):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
        subprocess.run(["docker", "network", "rm", NET], capture_output=True)


@pytest.fixture
def hop_file(pair, tmp_path, monkeypatch):
    """A hop between the two, and the addresses its statements name made
    the ones each container reaches the other by."""
    import migkit.config as cfg
    from migkit.engines.postgres import PostgresEngine
    real = PostgresEngine.apply_replication_stmt

    def reachable(self, side, db, stmt):
        for port, ip in pair.items():
            stmt = stmt.replace(f"host=127.0.0.1 port={port}",
                                f"host={ip} port=5432")
        return real(self, side, db, stmt)
    monkeypatch.setattr(PostgresEngine, "apply_replication_stmt", reachable)
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    for name, _ in (A, B):
        _sql(name, "drop table if exists public.o;"
                   " create table public.o (id int primary key, v text)")

    def write(options):
        (tmp_path / "hops.yaml").write_text(
            "hops:\n  rev:\n    engine: postgres\n"
            f"    source: {{host: 127.0.0.1, port: {A[1]}, user: postgres,"
            " password: test}\n"
            f"    target: {{host: 127.0.0.1, port: {B[1]}, user: postgres,"
            " password: test}\n    databases: [postgres]\n"
            f"    options: {options}\n")
        monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    yield write
    for name, _ in (A, B):
        for row in _sql(name, "select subname from pg_subscription"
                              ).splitlines():
            _sql(name, f"drop subscription if exists {row}")
        for row in _sql(name, "select pubname from pg_publication"
                              ).splitlines():
            _sql(name, f"drop publication if exists {row}")
        _sql(name, "drop table if exists public.o")


def _move(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, ["move", "rev", *argv])
    return got, " ".join((got.output + str(got.exception or "")).split())


def _arrives(name, sql, want, limit=60):
    for _ in range(limit * 2):
        if _sql(name, sql) == want:
            return True
        time.sleep(0.5)
    return False


@needs_docker
def test_the_stream_back_starts_as_the_one_forward_stops(hop_file):
    hop_file("{reverse: at_cutover}")
    got, said = _move("--mode", "full+cdc", "--go")
    assert got.exit_code == 0, said
    _sql(A[0], "insert into public.o values (1, 'before cutover')")
    assert _arrives(B[0], "select count(*) from public.o", "1"), said
    # cutover: forward torn down, the stream back set up
    got, said = _move("--mode", "cdc", "--drop", "--go")
    assert got.exit_code == 0, said
    assert "the other way - from the new primary back to the old source" \
        in said, said
    _sql(B[0], "insert into public.o values (2, 'written on the new"
               " primary')")
    assert _arrives(A[0], "select v from public.o where id = 2",
                    "written on the new primary"), said
    # and nothing goes forward any more
    _sql(A[0], "insert into public.o values (3, 'old source')")
    time.sleep(3)
    assert _sql(B[0], "select count(*) from public.o where id = 3") == "0"


@needs_docker
def test_without_the_option_the_source_is_not_written(hop_file):
    hop_file("{}")
    got, said = _move("--mode", "full+cdc", "--go")
    assert got.exit_code == 0, said
    got, said = _move("--mode", "cdc", "--drop", "--go")
    assert got.exit_code == 0 and "the other way" not in said, said
    assert _sql(A[0], "select count(*) from pg_subscription") == "0"


@needs_docker
def test_both_ways_carry_each_sides_writes_and_nothing_goes_round(hop_file):
    hop_file("{topology: two_way}")
    got, said = _move("--mode", "full+cdc", "--go")
    assert got.exit_code == 0, said
    assert "two-way, as the hop says" in said, said
    for name in (A[0], B[0]):
        assert _sql(name, "select count(*) from pg_subscription where"
                          " suborigin = 'none'") == "1", said
    _sql(A[0], "insert into public.o values (10, 'from a')")
    _sql(B[0], "insert into public.o values (20, 'from b')")
    assert _arrives(B[0], "select count(*) from public.o", "2"), said
    assert _arrives(A[0], "select count(*) from public.o", "2"), said
    time.sleep(3)
    # an echo would be a second insert of the same key, and an apply error
    for name in (A[0], B[0]):
        assert _sql(name, "select coalesce(sum(apply_error_count), 0) from"
                          " pg_stat_subscription_stats") == "0", name
        assert _sql(name, "select count(*) from public.o") == "2"
    got, said = _move("--mode", "cdc", "--drop", "--go")
    assert got.exit_code == 0, said
    for name in (A[0], B[0]):
        assert _sql(name, "select count(*) from pg_subscription") == "0"


def test_both_ways_is_refused_before_postgresql_16(monkeypatch, tmp_path):
    import migkit.config as cfg
    from migkit import cli
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.setattr(PostgresEngine, "_server_version",
                        lambda self, side, db: 150000)
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  rev:\n    engine: postgres\n"
        "    source: {host: 10.0.0.1, port: 5432, user: u,"
        " password: CHANGE_ME}\n"
        "    target: {host: 10.0.0.2, port: 5432, user: u,"
        " password: CHANGE_ME}\n    databases: [app]\n"
        "    options: {topology: two_way}\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    got = CliRunner().invoke(cli.main, ["move", "rev", "--mode", "full+cdc"])
    said = " ".join((got.output + str(got.exception or "")).split())
    assert got.exit_code != 0, said
    assert "PostgreSQL 16" in said and "Nothing was set up" in said, said


def test_an_option_it_does_not_know_is_refused(tmp_path):
    from migkit.cli import _topology
    from migkit.config import Endpoint, Hop
    ep = Endpoint(host="h", port=1, user="u", password="p")
    with pytest.raises(SystemExit, match="at_cutover"):
        _topology(Hop(name="x", engine="postgres", source=ep, target=ep,
                      options={"reverse": "always"}))
    assert _topology(Hop(name="x", engine="postgres", source=ep, target=ep)
                     ) == (False, False)

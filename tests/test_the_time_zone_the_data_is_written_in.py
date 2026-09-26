"""The time zone the data is written in, against the server's own (backlog
23). A zone-less column's latest value in the future of the server's UTC
clock proves the application writes in a zone ahead of it; a latest value
in the past proves nothing and is not read as a zone."""
import datetime


from tests.conftest import needs_docker, psql


def _fake(offset, heads):
    class Eng:
        def zone_evidence(self, side, db):
            return offset, datetime.datetime(2026, 9, 25, 3, 0), heads
    return Eng()


def test_times_ahead_of_utc_on_a_utc_server_are_named():
    from migkit import zones
    ahead = datetime.datetime(2026, 9, 25, 9, 55)     # 6h55m ahead
    got = zones.infer(_fake(0, [("orders.created", ahead)]), "app")
    assert got.status == "warn", got.detail
    assert "at least UTC+6:30" in got.detail, got.detail
    assert "server's own (UTC+0)" in got.detail and "6h30m off" in got.detail


def test_the_servers_own_zone_is_fine_and_the_past_proves_nothing():
    from migkit import zones
    local = datetime.datetime(2026, 9, 25, 9, 55)
    got = zones.infer(_fake(420, [("orders.created", local)]), "app")
    assert got.status == "ok", got.detail
    old = datetime.datetime(2020, 1, 1)
    got = zones.infer(_fake(0, [("orders.created", old)]), "app")
    assert got.status == "skip", got.detail


@needs_docker
def test_postgres_reads_it_by_the_index(pg_pair):
    from migkit import zones
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    got = psql(pg_pair["src"], "drop table if exists public.z;"
                               " create table public.z (id int primary key,"
                               " at timestamp, noidx timestamp);"
                               " create index z_at on public.z (at);"
                               " insert into public.z values (1, (now() at"
                               " time zone 'utc') + interval '7 hours',"
                               " (now() at time zone 'utc') + interval"
                               " '12 hours')")
    assert got.returncode == 0, got.stderr
    try:
        ep = Endpoint(host="127.0.0.1", port=pg_pair["src"], user="postgres",
                      password="test")
        eng = PostgresEngine(Hop(name="z", engine="postgres", source=ep,
                                 target=ep, databases=["postgres"]))
        got = zones.infer(eng, "postgres")
        assert got.status == "warn", got.detail
        assert "public.z.at" in got.detail and "UTC+6:30" in got.detail
        # the column no index leads is not read: one probe each, no scans
        assert "noidx" not in got.detail, got.detail
    finally:
        psql(pg_pair["src"], "drop table if exists public.z")


@needs_docker
def test_mysql_reads_it_by_the_index():
    import subprocess
    import time

    from migkit import zones
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    name, port = "migkit-test-zone-my", 15811
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{port}:3306",
                        "mysql:8.4"], check=True, capture_output=True)
        for _ in range(90):
            if subprocess.run(["docker", "exec", name, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "select 1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        got = subprocess.run(
            ["docker", "exec", "-i", name, "mysql", "-uroot", "-ptest"],
            input="create database app; create table app.z (id int primary"
                  " key, at datetime, key z_at (at));"
                  " insert into app.z values (1, utc_timestamp() + interval"
                  " 7 hour)", capture_output=True, text=True)
        assert got.returncode == 0, got.stderr
        ep = Endpoint(host="127.0.0.1", port=port, user="root",
                      password="test")
        got = zones.infer(MySQLEngine(Hop(name="z", engine="mysql",
                                          source=ep, target=ep,
                                          databases=["app"])), "app")
        assert got.status == "warn", got.detail
        assert "z.at" in got.detail and "UTC+6:30" in got.detail, got.detail
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)

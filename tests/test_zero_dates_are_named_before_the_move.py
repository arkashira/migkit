"""MySQL's zero dates are named before a move to an engine that has none
(B6).

Measured, MySQL into PostgreSQL: `'0000-00-00'` comes back from the driver
as that string, and PostgreSQL answers `date/time field value out of
range`. The copy stops at the first such row, with everything before it
already on the target. `assess` and `check` now name the columns and how
many rows hold one - a zero year, month or day - before anything is read.
"""
import subprocess
import time

import pytest
from click.testing import CliRunner

from tests.conftest import needs_docker, psql

pytestmark = needs_docker

MY, MY_PORT = "migkit-test-zerodate-my", 15766


def my(sql):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def mysql_server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                        "mysql:8.4"], check=True, capture_output=True)
        for _ in range(90):
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "select 1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail("mysql never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


def test_zero_dates_are_named(mysql_server, pg_pair, tmp_path, monkeypatch):
    import migkit.config as cfg
    # a legacy server's sql_mode, the way such rows got in
    my("drop database if exists cx; create database cx;"
       " create table cx.posts (id int primary key, published date,"
       "  touched datetime);"
       " set session sql_mode = '';"
       " insert into cx.posts values (1, '2024-01-02', '2024-01-02 03:04:05'),"
       " (2, '0000-00-00', '2024-01-02 03:04:05'),"
       " (3, '2020-00-15', '0000-00-00 00:00:00')")
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  zd:\n    engine: hetero\n"
        f"    source: {{host: 127.0.0.1, port: {MY_PORT}, user: root,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [cx]\n    db_map: {cx: postgres}\n"
        "    options: {source_engine: mysql, target_engine: postgres}\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    got, said = _run("assess", "zd")
    assert "fail pair cx zero dates 3 rows hold a date with a zero year," \
        " month or day" in said, said
    assert "posts.published 2" in said and "posts.touched 1" in said, said
    got, said = _run("check", "zd", "--only", "deep")
    assert got.exit_code != 0 and "zero dates" in said, said
    # the premise: the target really refuses one
    refused = psql(pg_pair["dst"], "select '0000-00-00'::date")
    assert "out of range" in refused.stderr, refused.stderr
    # and a source without them is clean
    my("set session sql_mode = ''; update cx.posts set published ="
       " '2020-01-15', touched = now() where id in (2, 3)")
    got, said = _run("check", "zd", "--only", "deep")
    assert "no zero dates" in said, said

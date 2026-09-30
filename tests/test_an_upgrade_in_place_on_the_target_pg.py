"""An upgrade by a copy of the pages, upgraded in place on the target
(backlog R23, 7): the target seeded at the source's version 16 from a copy
of its pages, with a slot made first; the new version's own upgrade run
where the target's data is - checked first, the way its files are carried
climbed from a link down to a copy - the new version's server started on
it, and the changes the source took since the copy applied from the
position the copy holds. The source is never stopped or upgraded.

The two data directories are two docker volumes: a hard link between two
mounts is refused even on one file system, so the ladder is shown to fall
from the link to the next way that its own check passes.
"""
import socket
import subprocess
import time

import pytest
from click.testing import CliRunner

NET = "migkit-test-upg-net"
SRC, OLD, NEW = ("migkit-test-upg-src", "migkit-test-upg-dst16",
                 "migkit-test-upg-dst17")
V16, V17 = "migkit-test-upg-v16", "migkit-test-upg-v17"
SRC_PORT, DST_PORT = 16330, 16331
IMAGE = "tianon/postgres-upgrade:16-to-17"


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(),
                                 reason="docker not available")]


def _psql(port, sql, db="app"):
    import psycopg2
    conn = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                            password="test", dbname=db, connect_timeout=5)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall() if cur.description else None
    finally:
        conn.close()


def _ready(port, most=180):
    end = time.time() + most
    while time.time() < end:
        try:
            _psql(port, "select 1", "postgres")
            return
        except Exception:
            time.sleep(1)
    pytest.fail(f"nothing answered on {port}")


def _rm():
    subprocess.run(["docker", "rm", "-f", "-v", SRC, OLD, NEW],
                   capture_output=True)
    subprocess.run(["docker", "volume", "rm", V16, V17], capture_output=True)


@pytest.fixture(scope="module")
def servers():
    _rm()
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    subprocess.run(["docker", "pull", IMAGE], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", SRC, "--network", NET,
                    "-e", "POSTGRES_PASSWORD=test", "-p",
                    f"127.0.0.1:{SRC_PORT}:5432", "postgres:16", "-c",
                    "wal_level=logical"], check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", OLD, "--network", NET,
                    "-e", "POSTGRES_PASSWORD=test", "-v",
                    f"{V16}:/var/lib/postgresql/data", "-p",
                    f"127.0.0.1:{DST_PORT}:5432", "postgres:16"],
                   check=True, capture_output=True)
    try:
        _ready(SRC_PORT)
        _ready(DST_PORT)
        # the DBA's part: a replication connection from the target's host
        subprocess.run(["docker", "exec", SRC, "sh", "-c",
                        "echo 'host replication all all scram-sha-256' >>"
                        " /var/lib/postgresql/data/pg_hba.conf"], check=True)
        _psql(SRC_PORT, "select pg_reload_conf()", "postgres")
        _psql(SRC_PORT, "create database app", "postgres")
        _psql(SRC_PORT, "create table t (id bigint primary key, v text);"
                        " create index on t (v);"
                        " insert into t select g, md5(g::text) from"
                        " generate_series(1, 50000) g")
        yield
    finally:
        _rm()
        subprocess.run(["docker", "network", "rm", NET], capture_output=True)


@pytest.fixture(scope="module")
def hop(servers, tmp_path_factory):
    import migkit.config as cfg
    tmp = tmp_path_factory.mktemp("upgrade")
    # the copy is taken by the source's own version's programs; the new
    # version's are named by the hop's upgrade settings
    run = (f"docker run --rm -i --network {NET} -u postgres --entrypoint sh"
           " -e PATH=/usr/lib/postgresql/16/bin:/usr/local/sbin:"
           "/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
           f" -v {V16}:/var/lib/postgresql/16/data"
           f" -v {V17}:/var/lib/postgresql/17/data {IMAGE} -c")
    start = (f"docker run -d --name {NEW} --network {NET} -v"
             f" {V17}:/var/lib/postgresql/data -p"
             f" 127.0.0.1:{DST_PORT}:5432 postgres:17")
    conf = tmp / "hops.yaml"
    conf.write_text(
        "hops:\n  up:\n    engine: postgres\n    goal: upgrade\n"
        f"    source: {{host: 127.0.0.1, port: {SRC_PORT}, user: postgres,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {DST_PORT}, user: postgres,"
        " password: test}\n"
        "    databases: [app]\n"
        "    options:\n      physical:\n"
        "        data: /var/lib/postgresql/16/data\n"
        f"        run: {run}\n"
        f"        stop: docker stop {OLD}\n"
        f"        start: docker start {OLD}\n"
        f"        source: {SRC}:5432\n"
        "        upgrade:\n"
        "          version: '17'\n"
        "          bin: /usr/lib/postgresql/17/bin\n"
        "          old_bin: /usr/lib/postgresql/16/bin\n"
        "          data: /var/lib/postgresql/17/data\n"
        f"          start: {start}\n")
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(cfg, "CONF", str(conf))
        mp.setattr(cfg, "REPORTS", tmp / "reports")
        from migkit.config import get_hop
        yield get_hop("up")


def test_the_plan_takes_the_pages_and_says_the_upgrade(hop):
    from migkit import cli
    got = CliRunner().invoke(cli.main, ["move", "up", "--mode", "full+cdc"])
    assert got.exit_code == 0, got.output
    out = " ".join(got.output.split())
    assert "upgrade: a copy of the pages upgraded on the target" in out, out
    assert "then the target is upgraded in place by the new version's own" \
        " upgrade" in out, out
    assert "pg_upgrade" not in got.output and "initdb" not in got.output


def test_the_copy_is_upgraded_on_the_target_and_goes_on(hop):
    from migkit import cli, physical
    got = CliRunner().invoke(cli.main, ["move", "up", "--mode", "full+cdc",
                                        "--go"])
    assert got.exit_code == 0, got.output
    out = " ".join(got.output.split())
    assert "the new version's own check passed" in out, out
    # two volumes: the files cannot be linked across them, and the ladder
    # says why it fell past the link
    assert "the files linked into the new version's directory did not" \
        " prove itself" in out, out
    rec = physical.seeded(hop)
    assert rec["upgraded"]["version"].startswith("17"), rec
    assert rec["upgraded"]["files"] in ("clone", "copy"), rec
    version = _psql(DST_PORT, "show server_version")[0][0]
    assert version.startswith("17"), version
    # the source is where it was: its version, and still writable
    assert _psql(SRC_PORT, "show server_version")[0][0].startswith("16")
    _psql(SRC_PORT, "insert into t values (1000001, 'after the upgrade')")
    end = time.time() + 120
    while time.time() < end:
        got = _psql(DST_PORT, "select v from t where id = 1000001")
        if got:
            break
        time.sleep(1)
    assert got == [("after the upgrade",)], got
    assert _psql(DST_PORT, "select count(*) from t")[0][0] == 50001
    # the step the source would have been stopped for, measured and said
    from migkit import upgrade
    assert "would stop it for" in upgrade.in_place_line(hop)


def test_the_upgraded_target_checks_equal(hop):
    from migkit import cli
    for _ in range(30):
        got = CliRunner().invoke(cli.main, ["check", "up", "--only",
                                            "counts,data"])
        if got.exit_code == 3 and "the run was narrowed to" in got.output:
            break
        time.sleep(2)
    assert got.exit_code == 3 and "the run was narrowed to" in \
        got.output, got.output
    # and says otherwise when it is not
    _psql(DST_PORT, "alter subscription "
          + _psql(DST_PORT, "select subname from pg_subscription")[0][0]
          + " disable")
    _psql(DST_PORT, "update t set v = 'changed on the target' where id = 7")
    got = CliRunner().invoke(cli.main, ["check", "up", "--only",
                                        "counts,data"])
    assert got.exit_code != 0, got.output

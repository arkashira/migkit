"""The cutover as one timed path, on MySQL (research: cutover, section
14): `move --mode cdc --drop --go` on a hop with `cutover:`, the target a
replica of the source by GTID.

The same proofs as on PostgreSQL, from an application writing all the
way through: the outage it saw, every acknowledged row where it now
writes and once, and after a failure at any step - raised, or migkit
killed - a source that is writable and as it was. MySQL adds what only it
has here: the whole server made read-only (`super_read_only`) where the
hop's databases are all of it, grants revoked where they are not, and
the source's events switched off before its freeze and on on the target
at the flip.
"""
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pymysql
import pytest
import yaml
from click.testing import CliRunner

from tests import cutover_harness as harness

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("cutover.jobs", "mysql")]

pytestmark = pytest.mark.docker

NET = "migkit-test-cut-net"
SRC, DST = ("migkit-test-cut-mysrc", 16072), ("migkit-test-cut-mydst", 16073)
HOP = "myc"


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


if not _docker():
    pytest.skip("docker not available", allow_module_level=True)


def _my(node, sql, user="root", password="test"):
    got = subprocess.run(["docker", "exec", "-i", node[0], "mysql",
                          f"-u{user}", f"-p{password}", "-N", "-B"],
                         input=sql, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _up(node, server_id):
    subprocess.run(["docker", "rm", "-f", "-v", node[0]], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", node[0], "--network",
                    NET, "-e", "MYSQL_ROOT_PASSWORD=test", "-p",
                    f"127.0.0.1:{node[1]}:3306", "mysql:8.4",
                    f"--server-id={server_id}", "--gtid-mode=ON",
                    "--enforce-gtid-consistency=ON"],
                   check=True, capture_output=True)


def _ready(node):
    end = time.time() + 180
    while time.time() < end:
        ok = subprocess.run(["docker", "exec", node[0], "mysql", "-uroot",
                             "-ptest", "-h127.0.0.1", "--protocol=tcp", "-e",
                             "select 1"], capture_output=True).returncode == 0
        with socket.socket() as s:
            s.settimeout(2)
            if ok and s.connect_ex(("127.0.0.1", node[1])) == 0:
                return
        time.sleep(2)
    pytest.fail(f"{node[0]} never answered")


@pytest.fixture(scope="module")
def servers():
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    try:
        _up(SRC, 1)
        _up(DST, 2)
        _ready(SRC)
        _ready(DST)
        yield
    finally:
        for node in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", node[0]],
                           capture_output=True)
        subprocess.run(["docker", "network", "rm", NET], capture_output=True)


SCHEMA = """
set session sql_log_bin = 0;
create database app;
create table app.orders (id bigint auto_increment primary key,
                         tag varchar(64) not null);
create user app identified by 'CHANGE_ME';
grant select, insert on app.* to app;
insert into app.orders (tag) with recursive g(n) as (select 1 union all
  select n + 1 from g where n < 200) select concat('seed', n) from g;
create event app.tick on schedule every 1 day starts current_timestamp
  + interval 1 day do insert into app.orders (tag) values ('tick');
"""


def _reset(node):
    _my(node, "stop replica; reset replica all;"
              " set global super_read_only = 0; set global read_only = 0;"
              " reset persist if exists super_read_only;"
              " reset persist if exists read_only;"
              " set session sql_log_bin = 0;"
              " drop database if exists app; drop database if exists other;"
              " drop user if exists app, migkit_repl;"
              " reset binary logs and gtids;")


@pytest.fixture
def cut(servers, tmp_path, monkeypatch):
    """Both servers holding the same copy, the event carried switched off
    on the target as a copy carries it, then a replica of the source on
    the target, the target held from the application."""
    import migkit.config as cfg
    routes = {f"127.0.0.1:{SRC[1]}": f"{SRC[0]}:3306",
              f"127.0.0.1:{DST[1]}": f"{DST[0]}:3306"}
    harness.install(routes, monkeypatch)
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    for node in (SRC, DST):
        _reset(node)
        _my(node, SCHEMA)
    _my(DST, "set session sql_log_bin = 0;"
             " alter event app.tick disable on replica")
    route = tmp_path / "route"
    route.write_text("src")

    class Cut:
        pass
    c = Cut()
    c.tmp, c.route, c.routes = tmp_path, route, routes
    c.reports = tmp_path / "reports" / HOP

    def hop(cutover, **more):
        options = {"protect_target": True, "reverse": "at_cutover",
                   "cutover": cutover,
                   "state": {"mirror": str(tmp_path / "mirror")}, **more}
        conf = tmp_path / "hops.yaml"
        conf.write_text(yaml.safe_dump({"hops": {HOP: {
            "engine": "mysql",
            "source": {"host": "127.0.0.1", "port": SRC[1], "user": "root",
                       "password": "test"},
            "target": {"host": "127.0.0.1", "port": DST[1], "user": "root",
                       "password": "test"},
            "databases": ["app"], "options": options}}}))
        monkeypatch.setattr(cfg, "CONF", str(conf))
        c.conf = conf
    c.hop = hop

    def connect(where):
        return pymysql.connect(host="127.0.0.1",
                               port=SRC[1] if where == "src" else DST[1],
                               user="app", password="CHANGE_ME",
                               database="app", connect_timeout=3,
                               read_timeout=10, write_timeout=10)
    c.connect = connect
    yield c
    for node in (SRC, DST):
        _reset(node)


def _cli(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


def _tags(node):
    got = _my(node, "select tag from app.orders")
    return got.split()


def _follow(c):
    got, said = _cli("move", HOP, "--mode", "cdc", "--go")
    assert got.exit_code == 0, said
    running = ("select count(*) from performance_schema"
               ".replication_connection_status c join performance_schema"
               ".replication_applier_status a using (channel_name) where"
               " c.service_state = 'ON' and a.service_state = 'ON'")
    assert harness.wait_for(lambda: _my(DST, running) == "1", 60), said


def _writing(c):
    w = harness.Writer(c.connect, c.route, "app.orders")
    w.start()
    assert harness.wait_for(lambda: len(w.acked()) >= 30, 30), w.errors()[-3:]
    return w


def _arrived(c, tags, timeout=60):
    return harness.wait_for(lambda: set(tags) <= set(_tags(DST)), timeout)


def _nothing_lost_or_twice(w, node):
    have = _tags(node)
    assert len(have) == len(set(have)), "a row written twice"
    assert set(w.acked()) <= set(have), \
        f"{len(set(w.acked()) - set(have))} acknowledged rows missing"
    seeds = {f"seed{i}" for i in range(1, 201)}
    assert set(have) <= w.attempted() | seeds, "rows nobody wrote"


def _state(node):
    """What a cutover may change on the source and an undo must put
    back."""
    return {
        "grants": _my(node, "show grants for app"),
        "read_only": _my(node, "select @@global.read_only,"
                               " @@global.super_read_only"),
        "persisted": _my(node, "select group_concat(variable_name order by"
                               " 1) from performance_schema"
                               ".persisted_variables where variable_name"
                               " like '%read_only'"),
        "event": _my(node, "select status from information_schema.events"
                           " where event_name = 'tick'"),
        "replica": _my(node, "select count(*) from performance_schema"
                             ".replication_connection_configuration"),
    }


def test_the_cutover_moves_the_application_and_its_jobs(cut):
    c = cut
    c.hop({"budget": 20, "flip": f"printf dst > {c.route}"})
    _follow(c)
    w = _writing(c)
    try:
        got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
        assert got.exit_code == 0, said
        assert "super_read_only" in said, said
        flipped = time.time()
        assert harness.wait_for(lambda: len(w.oks_after(flipped)) >= 30,
                                30), w.errors()[-3:]
    finally:
        w.stop()
    outage = w.outage()
    print(f"\nmeasured from the application: {outage:.2f}s without an"
          f" acknowledged write; {len(w.errors())} writes refused")
    assert 0 < outage < 20
    after = [e for e in w.log if e[2] == "dst"]
    assert after and all(e[3] == "ok" for e in after), \
        [e for e in after if e[3] != "ok"][:3]
    _nothing_lost_or_twice(w, DST)
    # the stream back carried what the new primary took to the old source
    assert harness.wait_for(lambda: sorted(_tags(SRC)) == sorted(_tags(DST))
                            ), "the old source did not follow"
    # the job runs in one place: the target
    assert _my(DST, "select status from information_schema.events") \
        == "ENABLED"
    assert _my(SRC, "select status from information_schema.events") \
        != "ENABLED"
    # the old source refuses the application
    with pytest.raises(pymysql.Error):
        conn = c.connect("src")
        conn.cursor().execute("insert into app.orders (tag) values"
                              " ('straggler')")
        conn.commit()
    run = [json.loads(x) for x in
           (c.reports / "cutover-runs.jsonl").read_text().splitlines()][-1]
    assert run["state"] == "flipped" and run["outage"] < 20, run
    assert run["took"].get("jobs_on") is not None, run["took"]


def test_going_back_raises_the_old_sources_counter(cut):
    """The new primary handed out ids and deleted the top ones; the old
    source's counter, followed from the rows alone, stayed below them."""
    c = cut
    c.hop({"budget": 20, "flip": f"printf dst > {c.route}"})
    _follow(c)
    got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
    assert got.exit_code == 0, said
    _my(DST, "set session sql_log_bin = 0;"
             " alter table app.orders auto_increment = 5000")
    got, said = _cli("rollback", HOP, "--db", "app", "--apply")
    assert got.exit_code == 0, said
    assert "counters raised past the new primary's" in said, said
    conn = c.connect("src")
    cur = conn.cursor()
    cur.execute("insert into app.orders (tag) values ('back')")
    conn.commit()
    cur.execute("select last_insert_id()")
    assert cur.fetchone()[0] >= 5000
    conn.close()


#: "prepare" first: the hop streams back, and the stream back is made
#: ahead of the clock where it can wait idle
PHASES = ("prepare", "jobs_off", "freeze", "drain", "verify", "counters",
          "stream", "reverse", "jobs_on", "flip")


def test_a_failure_at_any_step_undoes_every_step(cut, monkeypatch):
    from migkit import failpoint
    c = cut
    c.hop({"budget": 20, "flip": f"printf dst > {c.route}"})
    _follow(c)
    before = _state(SRC)
    w = _writing(c)
    try:
        for phase in PHASES:
            failpoint._counts.clear()
            monkeypatch.setenv("MIGKIT_FAILPOINT", f"cutover.{phase}:1:raise")
            got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
            assert got.exit_code != 0, (phase, said)
            assert "the cutover was undone" in said, (phase, said)
            back = time.time()
            assert harness.wait_for(lambda: len(w.oks_after(back)) >= 10,
                                    30), (phase, w.errors()[-3:])
            assert _state(SRC) == before, phase
            assert not (c.reports / "cutover.json").exists(), phase
            assert _arrived(c, w.acked()), phase
    finally:
        monkeypatch.delenv("MIGKIT_FAILPOINT", raising=False)
        w.stop()
    _nothing_lost_or_twice(w, SRC)
    assert _arrived(c, _tags(SRC))


def test_a_cutover_killed_half_way_is_undone_by_the_next_run(cut):
    c = cut
    c.hop({"budget": 20, "flip": f"printf dst > {c.route}"})
    _follow(c)
    before = _state(SRC)
    w = _writing(c)
    root = Path(__file__).resolve().parents[1]
    try:
        for phase in ("drain", "reverse"):
            env = {**os.environ, "MIGKIT_CONF": str(c.conf),
                   "MIGKIT_REPORTS": str(c.tmp / "reports"),
                   "MIGKIT_FAILPOINT": f"cutover.{phase}:1:exit",
                   "MIGKIT_TEST_ROUTES": ",".join(
                       f"{a}={b}" for a, b in c.routes.items()),
                   "PYTHONPATH": str(root)}
            got = subprocess.run([sys.executable, harness.__file__, "move",
                                  HOP, "--mode", "cdc", "--drop", "--go"],
                                 env=env, capture_output=True, text=True,
                                 timeout=300)
            assert got.returncode == 137, (phase, got.stdout[-500:],
                                           got.stderr[-500:])
            # killed with the source frozen: the application is refused
            assert _my(SRC, "select @@global.super_read_only") == "1"
            got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
            assert "the interrupted cutover was undone" in said, said
            back = time.time()
            assert harness.wait_for(lambda: len(w.oks_after(back)) >= 10,
                                    30), (phase, w.errors()[-3:])
            assert _state(SRC) == before, phase
            assert _arrived(c, w.acked()), phase
    finally:
        w.stop()
    _nothing_lost_or_twice(w, SRC)


def test_a_server_the_hop_shares_is_frozen_per_database(cut):
    """Another database on the source that the hop does not move: the
    server stays writable for it, and the application's account loses
    its grants on the hop's database only, for the outage."""
    c = cut
    _my(SRC, "set session sql_log_bin = 0; create database other;"
             " create table other.t (v int)")
    c.hop({"budget": 20, "flip": f"printf dst > {c.route}"})
    _follow(c)
    w = _writing(c)
    try:
        got, said = _cli("move", HOP, "--mode", "cdc", "--drop")
        assert "per database" in said and "other" in said, said
        got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
        assert got.exit_code == 0, said
        assert "write grants revoked" in said, said
        flipped = time.time()
        assert harness.wait_for(lambda: len(w.oks_after(flipped)) >= 20, 30)
    finally:
        w.stop()
    assert _my(SRC, "select @@global.super_read_only") == "0"
    _my(SRC, "insert into other.t values (1)")
    _nothing_lost_or_twice(w, DST)
    print(f"\nper database: {w.outage():.2f}s without an acknowledged write")

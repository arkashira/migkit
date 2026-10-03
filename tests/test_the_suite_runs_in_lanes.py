"""The test infrastructure the one final run stands on (audit T2-T13): the
lanes' ports and names, the shared servers put back as they were found,
the marks a test earns by what it reaches, strict skips, the docker lock's
lanes, and the final run's layout and record."""
import json
import os
import re
import sys
import threading
import time
import types
from pathlib import Path

import pytest

from tests import conftest, servers
from tests.conftest import needs_docker
from tests.waits import NeverCame, until

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))
import final_suite  # noqa: E402
import with_docker_lock as lock  # noqa: E402

#: the program's name, not written where this module's own code would
#: read as running it (`conftest.reached`)
PROGRAM = "dock" + "er"
#: lane 0's pair as the modules before the lanes name it, spelled so
#: this file does not read as one of them (`servers.scan`)
PAIR_SRC, PAIR_AT = "migkit-test-pg-" + "src", 55_432


def _lane(monkeypatch, lane, worker=None):
    monkeypatch.setenv("MIGKIT_TEST_LANE", str(lane))
    if worker is None:
        monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    else:
        monkeypatch.setenv("PYTEST_XDIST_WORKER", f"gw{worker}")


# ---- ports and names --------------------------------------------------------

def test_each_lane_and_worker_has_a_block_of_its_own(monkeypatch):
    blocks = []
    for lane in range(servers.LANES):
        for worker in range(servers.WORKERS):
            _lane(monkeypatch, lane, worker)
            blocks.append({servers.port(o) for o in range(servers.BLOCK)})
    assert len(blocks) == 100
    everything = set().union(*blocks)
    assert len(everything) == 100 * servers.BLOCK
    assert min(everything) == servers.BASE
    _lane(monkeypatch, 3, 2)
    assert servers.lane() == 32
    assert servers.port(5) == servers.BASE + 32 * servers.BLOCK + 5
    assert servers.name("migkit-test-x") == "migkit-test-x-l32"
    assert servers.private_port(0) == \
        servers.BASE + 32 * servers.BLOCK + servers.PRIVATE
    with pytest.raises(ValueError):
        servers.private_port(servers.BLOCK - servers.PRIVATE)
    _lane(monkeypatch, 0)
    assert servers.name("migkit-test-x") == "migkit-test-x"
    _lane(monkeypatch, 10)
    with pytest.raises(ValueError):
        servers.lane()


def test_the_profiles_fit_their_block_and_share_no_port():
    taken = {}
    for p in servers.PROFILES.values():
        for i in range(len(p.roles)):
            offset = p.offset + i
            assert offset < servers.PRIVATE, p.key
            assert offset not in taken, (p.key, taken.get(offset))
            taken[offset] = p.key


def test_only_lane_0_keeps_the_pairs_fixed_names_and_ports(monkeypatch):
    pg = servers.PROFILES["pg"]
    _lane(monkeypatch, 0)
    assert servers._placed(pg, "src", 0) == (PAIR_SRC, PAIR_AT)
    assert servers._placed(pg, "dst", 1)[1] == PAIR_AT + 1
    _lane(monkeypatch, 2)
    name, port = servers._placed(pg, "src", 0)
    assert (name, port) == (PAIR_SRC + "-l20",
                            servers.BASE + 20 * servers.BLOCK + pg.offset)


def test_a_file_naming_lane_0s_pair_fails_loudly_on_another_lane():
    """On lane 3 the pair's fixed port is lane 0's server, perhaps another
    run's: such a file stops at its first server test, before starting
    or touching anything."""
    import subprocess
    pinned = "tests/test_the_fence_holds_the_hops_own_stream.py"
    env = final_suite.child_env(MIGKIT_TEST_LANE="3", MIGKIT_TEST_STRICT="")
    got = subprocess.run([sys.executable, "-m", "pytest", "-q", "-x", "-p",
                          "no:cacheprovider", pinned], cwd=ROOT, env=env,
                         capture_output=True, text=True, timeout=300)
    assert got.returncode == 1, got.stdout[-2000:]
    assert "runs on lane 0 only" in got.stdout, got.stdout[-2000:]


def test_no_test_file_fixes_a_port_the_lanes_hand_out():
    """The lanes' blocks are free for them only while no file fixes a
    port inside them."""
    top = servers.BASE + servers.BLOCK * servers.LANES * servers.WORKERS
    inside = {f.name: sorted(p for p in servers.scan(f).ports
                             if servers.BASE <= p < top)
              for f in (ROOT / "tests").glob("*.py")}
    assert not {f: p for f, p in inside.items() if p}


_MAPPING = re.compile(r"""["']-p["'],\s*f?["']([^"']*)["']""")


def _unbound(text):
    return [m for m in _MAPPING.findall(text)
            if not m.startswith(("127.0.0.1:", "{HOST}:"))]


def test_the_shared_servers_map_127_0_0_1_only():
    assert _unbound('["-p", f"{port}:5432"]') == ["{port}:5432"]
    assert _unbound('["-p", f"127.0.0.1:{port}:5432"]') == []
    for helper in ("servers.py", "conftest.py", "mysql_pair.py"):
        assert _unbound((ROOT / "tests" / helper).read_text()) == [], helper
    assert servers.HOST == "127.0.0.1"


def test_a_modules_space_name_fits_every_engine():
    names = {}
    for f in (ROOT / "tests").glob("test_*.py"):
        n = servers.space_name(f"tests.{f.stem}")
        assert re.fullmatch(r"m_[a-z0-9_]{1,20}_[0-9a-f]{6}", n), n
        assert len(n) <= 30, n
        assert n not in names, (f.name, names.get(n))
        names[n] = f.name


# ---- what a file holds ------------------------------------------------------

def _write(where, name, text):
    p = where / name
    p.write_text(text)
    return p


def test_the_scan_finds_what_two_files_would_fight_over(tmp_path):
    a = _write(tmp_path, "test_a.py",
               'PORT = 16001\nNAME = "migkit-test-aa"\n'
               'argv = ["-p", f"127.0.0.1:{PORT}:5432", "postgres:16"]\n')
    b = _write(tmp_path, "test_b.py",
               'def f(port=16001):\n    return connect(port=16001)\n')
    c = _write(tmp_path, "test_c.py",
               'X = {"port": 16002}\nname = "migkit-test-cc"\n'
               'where = "127.0.0.1:5432"\n')
    ka, kb, kc = (servers.scan(p).keys() for p in (a, b, c))
    assert ka & kb == {"port:16001"}
    assert not ka & kc
    assert "name:migkit-test-aa" in ka and "name:migkit-test-cc" in kc
    # a server's own port inside its container is never a host port
    assert "port:5432" not in kc
    assert servers.scan(a).images == {"postgres:16"}
    assert not servers.scan(a).pinned


def test_a_file_naming_lane_0s_pair_is_pinned_and_so_is_its_helpers_user(
        tmp_path):
    (tmp_path / "helperx.py").write_text(
        f'SRC = "{PAIR_SRC}"\nOTHER = 16005\nOTHER_PORT = 16005\n')
    user = _write(tmp_path, "test_user.py", "from tests import helperx\n")
    plain = _write(tmp_path, "test_plain.py", "import os\n")
    assert servers.scan(user).pinned
    assert "port:16005" in servers.scan(user).keys()
    assert not servers.scan(plain).pinned
    assert servers.scan(ROOT / "tests" / "test_the_fence_holds_the_hops"
                                         "_own_stream.py").pinned


# ---- the marks a test earns -------------------------------------------------

def _fn(source, **globs):
    """A function of a `tests.` module, as the analysis sees ours."""
    ns = {"__name__": "tests.fake", **globs}
    exec(source, ns)
    return ns["f"]


def test_a_test_is_known_by_what_it_reaches():
    conftest._REACHED.clear()
    starts = _fn(f"def f():\n    run(['{PROGRAM}', 'run', 'x'])\n",
                 run=print)
    names = _fn(f"def f():\n    return ['mydumper', '{PROGRAM}']\n")
    through = _fn("def f():\n    return helper()\n", helper=starts)
    image = _fn("def f():\n    return helper(IMAGE)\n", helper=starts,
                IMAGE="gvenzl/oracle-free:slim-faststart")
    plain = _fn("def f():\n    return 1\n")
    assert conftest._signs_of(starts) == {PROGRAM}
    assert conftest._signs_of(names) == set()
    assert conftest._signs_of(through) == {PROGRAM}
    assert conftest._signs_of(image) == {PROGRAM, "vm"}
    assert conftest._signs_of(plain) == set()
    lend = _fn("def f(request):\n    yield 1\n")
    lend.migkit_profile = "oracle"
    assert conftest._signs_of(lend) == {PROGRAM, "vm"}
    lend2 = _fn("def f(request):\n    yield 2\n")
    lend2.migkit_profile = "mysql"
    assert conftest._signs_of(lend2) == {PROGRAM}


def test_the_pure_tests_here_stay_pure(request):
    """The analysis keeps what only names the program apart from what
    runs it: this module's own pure tests are not moved to the lanes."""
    assert not request.node.get_closest_marker("docker")
    assert request.node.get_closest_marker("timeout") is None


# ---- strict skips -----------------------------------------------------------

def _skip(reason):
    return types.SimpleNamespace(skipped=True, outcome="skipped",
                                 longrepr=("f.py", 1, f"Skipped: {reason}"))


def test_a_missing_server_fails_under_strict_and_a_chosen_skip_does_not(
        monkeypatch):
    monkeypatch.setattr(conftest, "STRICT", conftest._strict("1"))
    gone = _skip("kafka did not come up in this sandbox")
    conftest._judge(gone)
    assert gone.outcome == "failed"
    assert "kafka did not come up" in gone.longrepr
    chosen = _skip("root reads everything")
    conftest._judge(chosen)
    assert chosen.outcome == "skipped"
    monkeypatch.setattr(conftest, "STRICT", conftest._strict("server"))
    program = _skip("pgcopydb not installed here")
    conftest._judge(program)
    assert program.outcome == "skipped"
    server = _skip("docker not available")
    conftest._judge(server)
    assert server.outcome == "failed"
    monkeypatch.setattr(conftest, "STRICT", conftest._strict(""))
    off = _skip("docker not available")
    conftest._judge(off)
    assert off.outcome == "skipped"


def test_strict_takes_only_the_kinds_it_knows():
    assert conftest._strict("server,program") == {"server", "program"}
    assert conftest._strict("yes") == {"server", "program", "module"}
    with pytest.raises(pytest.UsageError):
        conftest._strict("servers")
    assert conftest.infrastructure("could not import 'moto'") == "module"
    assert conftest.infrastructure("mydumper not installed here") == \
        "program"
    assert conftest.infrastructure("MariaDB's parallel appliers always"
                                   " commit in order") is None


def test_a_vm_test_runs_only_where_the_vm_is(monkeypatch):
    monkeypatch.delenv("MIGKIT_TEST_VM", raising=False)
    monkeypatch.setenv("DOCKER_CONTEXT", "colima")
    monkeypatch.setitem(servers._DAEMON, "arch", "aarch64")
    assert not conftest._vm_here()
    monkeypatch.setitem(servers._DAEMON, "arch", "x86_64")
    assert conftest._vm_here()
    monkeypatch.setitem(servers._DAEMON, "arch", "aarch64")
    monkeypatch.setenv("DOCKER_CONTEXT", "colima-migkit")
    assert conftest._vm_here()


# ---- the docker lock --------------------------------------------------------

@pytest.fixture
def locks(tmp_path, monkeypatch):
    monkeypatch.setattr(lock, "LOCK", str(tmp_path / "all.lock"))
    monkeypatch.setattr(lock, "INTENT", str(tmp_path / "intent.lock"))
    monkeypatch.setattr(lock, "LOCKS", tmp_path / "d")
    return tmp_path


def _hold(cm, ready, release, got):
    with cm as value:
        got.append(value)
        ready.set()
        release.wait(20)


def test_two_commands_get_two_lanes_and_wait_only_for_a_shared_port(locks):
    ready, release, first = threading.Event(), threading.Event(), []
    a = threading.Thread(target=_hold, args=(lock.shared(
        [1, 2], {"port:16001"}), ready, release, first))
    a.start()
    assert ready.wait(10)
    began = time.monotonic()
    with lock.shared([1, 2], {"port:16002"}) as second:
        assert second == 2 and first == [1]
    assert time.monotonic() - began < 5
    ready2, release2, third = threading.Event(), threading.Event(), []
    b = threading.Thread(target=_hold, args=(lock.shared(
        [1, 2], {"port:16001"}), ready2, release2, third))
    b.start()
    assert not ready2.wait(2), "took a port another command holds"
    release.set()
    assert ready2.wait(10)
    release2.set()
    a.join(10)
    b.join(10)


def test_the_whole_of_docker_waits_for_every_lane(locks):
    ready, release, got = threading.Event(), threading.Event(), []
    a = threading.Thread(target=_hold, args=(lock.shared([1], set()), ready,
                                             release, got))
    a.start()
    assert ready.wait(10)
    ready2, release2, got2 = threading.Event(), threading.Event(), []
    b = threading.Thread(target=_hold, args=(lock.exclusive(), ready2,
                                             release2, got2))
    b.start()
    assert not ready2.wait(2), "took all of docker while a lane ran"
    release.set()
    assert ready2.wait(10)
    release2.set()
    a.join(10)
    b.join(10)


def test_the_lock_reads_the_files_a_command_runs(tmp_path):
    (tmp_path / "tests").mkdir()
    f = tmp_path / "tests" / "test_x.py"
    f.write_text("PORT = 16009\n")
    argv = ["python", "-m", "pytest", "-q", "tests/test_x.py::test_y"]
    assert lock.test_files(argv, tmp_path) == [f]
    assert lock.test_files(["python", "-m", "pytest", "tests"],
                           tmp_path) is None
    assert lock.test_files(["python", "script.py"], tmp_path) is None
    assert lock.holds([f]) == {"port:16009"}
    pinned = ROOT / "tests" / "test_the_fence_holds_the_hops_own_stream.py"
    assert "lane0" in lock.holds([pinned])


# ---- the final run's layout and record --------------------------------------

def test_the_final_run_lays_its_lanes_out(tmp_path, monkeypatch):
    monkeypatch.setattr(final_suite, "TIMES", tmp_path / "none.json")
    monkeypatch.setattr(lock, "home_context", lambda: "colima")
    monkeypatch.setattr(lock, "_has_colima", lambda: True)
    run = f'"{PROGRAM}", "run"'
    files = {
        "pinned": f'NAME = "{PAIR_SRC}"\n',
        "left": f'PORT = 16101\nargv = [{run}]\n',
        "right": 'def f(port=16101):\n    pass\n',
        "oracle": f'argv = [{run}, "gvenzl/oracle-free:slim-faststart"]\n',
        "heavy": f'argv = [{run}, "cassandra:4.1"]\n',
        "light1": f'PORT = 16102\nargv = [{run}]\n',
        "light2": f'PORT = 16103\nargv = [{run}]\n',
    }
    items = []
    for stem, text in files.items():
        p = _write(tmp_path, f"test_{stem}.py", text)
        marks = [PROGRAM] + (["vm"] if stem == "oracle" else [])
        items += [{"id": f"{p}::t{i}", "file": str(p), "marks": marks,
                   "timeout": 900} for i in range(3)]
    items.append({"id": "pure", "file": str(tmp_path / "test_pure.py"),
                  "marks": [], "timeout": 900})
    lanes, missed = final_suite.plan(items, {"docker", "vm"},
                                     {"home": (4000, 2, "aarch64")},
                                     "colima=2,migkit=1")
    assert not missed
    where = {Path(f).stem[5:]: ln for ln in lanes for f in ln.files}
    assert set(where) == set(files)
    assert where["pinned"].number == 0
    assert where["left"] is where["right"]
    assert where["oracle"].kind == "vm"
    assert where["heavy"].kind in ("any", "vm")
    assert len({ln.number for ln in lanes}) == len(lanes)
    only_home, _ = final_suite.plan(items, {"docker"},
                                    {"home": (4000, 2, "aarch64")},
                                    "colima=1")
    assert "oracle" not in {Path(f).stem[5:] for ln in only_home
                            for f in ln.files}


@pytest.fixture
def under_root(tmp_path):
    """A directory inside the checkout (its `reports/`, which git
    ignores), where a test's id names its file as the lanes' ids do."""
    import shutil
    where = ROOT / "reports" / f"lane-check-{os.getpid()}"
    where.mkdir(parents=True, exist_ok=True)
    yield where
    shutil.rmtree(where, ignore_errors=True)


def test_a_lane_that_stops_answering_is_ended_and_its_other_files_run(
        under_root, tmp_path, monkeypatch):
    """A test hung where the per-test timeout cannot reach it (its alarm
    blocked, as a C call holds it): the lane is ended past the timeout and
    the files after it still run."""
    lane = "import pytest\npytestmark = pytest.mark.docker\n\n\n"
    hangs = _write(under_root, "test_hangs.py", lane +
                   "def test_hangs():\n    import signal, time\n"
                   "    signal.pthread_sigmask(signal.SIG_BLOCK,"
                   " [signal.SIGALRM])\n    time.sleep(120)\n")
    after = _write(under_root, "test_after.py", lane + "def test_after():\n"
                                                       "    pass\n")
    monkeypatch.setattr(final_suite, "TIMEOUT", 2)
    monkeypatch.setattr(final_suite, "GRACE", 3)
    # the conftest's record of each test, for files outside `tests/`
    monkeypatch.setattr(final_suite, "EXTRA", ["-p", "tests.conftest"])
    monkeypatch.setattr(final_suite, "sweep", lambda *a, **k: [])
    ln = final_suite.Lane("home", None, "any")
    ln.number, ln.files = 9, [str(hangs), str(after)]
    began = time.monotonic()
    final_suite.run_lane(ln, tmp_path, "", "check", threading.Event())
    assert time.monotonic() - began < 90
    assert [h[0].rsplit("::", 1)[-1] for h in ln.hung] == ["test_hangs"]
    rows, _ = final_suite.lane_rows(tmp_path / "lane-9.jsonl")
    assert {r["id"].rsplit("::", 1)[-1]: r["outcome"] for r in rows
            if r["id"].endswith("test_after")} == {"test_after": "passed"}
    assert not ln.missed


def test_a_lanes_record_reads_back(tmp_path):
    prog = tmp_path / "lane.jsonl"
    rows = [
        {"ev": "start", "id": "tests/test_a.py::t1", "t": 100.0},
        {"ev": "report", "id": "tests/test_a.py::t1", "when": "setup",
         "outcome": "passed", "duration": 2.0, "t": 102.0},
        {"ev": "report", "id": "tests/test_a.py::t1", "when": "call",
         "outcome": "failed", "duration": 1.0, "message": "boom", "t": 103.0},
        {"ev": "end", "id": "tests/test_a.py::t1", "last": False, "t": 103.0},
        {"ev": "start", "id": "tests/test_a.py::t2", "t": 103.0},
        {"ev": "report", "id": "tests/test_a.py::t2", "when": "setup",
         "outcome": "skipped", "duration": 0.1, "reason": "why", "t": 103.1},
        {"ev": "end", "id": "tests/test_a.py::t2", "last": True, "t": 110.5},
    ]
    prog.write_text("\n".join(json.dumps(r) for r in rows) + "\nnot json\n")
    tests, spans = final_suite.lane_rows(prog)
    by = {t["id"]: t for t in tests}
    assert by["tests/test_a.py::t1"]["outcome"] == "failed"
    assert by["tests/test_a.py::t1"]["message"] == "boom"
    assert by["tests/test_a.py::t1"]["seconds"] == 3.0
    assert by["tests/test_a.py::t2"]["outcome"] == "skipped"
    assert spans == {"tests/test_a.py": 10.5}
    junit = tmp_path / "unit.xml"
    junit.write_text(
        '<testsuites><testsuite>'
        '<testcase classname="tests.test_b" name="t1" time="1.5"/>'
        '<testcase classname="tests.test_b" name="t2" time="0.5">'
        '<failure message="no">x</failure></testcase>'
        '</testsuite></testsuites>')
    got = {r["id"]: r for r in final_suite.junit_rows(junit)}
    assert got["tests/test_b.py::t1"]["outcome"] == "passed"
    assert got["tests/test_b.py::t2"]["outcome"] == "failed"
    assert got["tests/test_b.py::t2"]["file"] == "tests/test_b.py"


# ---- waits ------------------------------------------------------------------

def test_until_returns_the_sign_and_names_what_never_came():
    calls = []

    def later():
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("refused")
        return "up"
    assert until(later, 5, "the server", every=0.01) == "up"
    with pytest.raises(NeverCame) as e:
        until(lambda: 1 / 0, 0.2, "a sign", every=0.01)
    assert "a sign" in str(e.value) and "ZeroDivisionError" in str(e.value)
    began = time.monotonic()
    with pytest.raises(NeverCame):
        until(lambda: False, 0.3, "nothing", every=0.05)
    assert time.monotonic() - began < 2


# ---- what installs and runs the suite ---------------------------------------

def test_the_dev_extra_brings_the_plugins_and_the_timeout():
    import tomllib
    got = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert {"pytest-xdist", "pytest-timeout"} <= set(
        got["project"]["optional-dependencies"]["dev"])
    assert got["project"]["requires-python"] == ">=3.10.12"
    ini = got["tool"]["pytest"]["ini_options"]
    assert ini["timeout"] == 900
    assert any(m.startswith("vm:") for m in ini["markers"])
    assert any(m.startswith("long:") for m in ini["markers"])


def test_ci_and_release_run_the_final_suite_as_the_owner_does():
    flows = {f.name: f.read_text()
             for f in (ROOT / ".github" / "workflows").glob("*.yml")}
    for name in ("ci.yml", "release.yml", "x86-engines.yml"):
        assert "tools/final_suite.py" in flows[name], name
        assert '".[dev]"' in flows[name], name
        assert "faker pytest" not in flows[name], name
    release = flows["release.yml"]
    assert "['project']['version']" not in release
    assert "migkit/__init__.py" in release


# ---- the shared servers, put back as they were found ------------------------

@pytest.fixture
def lane7(monkeypatch):
    """Lane 7, away from every lane a session here runs on, and a registry
    of its own: what the test starts is all it stops."""
    _lane(monkeypatch, 7)
    for attr in ("_RUNNING", "_LAST", "_TAKEN", "_TLS"):
        monkeypatch.setattr(servers, attr, {})
    monkeypatch.setattr(servers, "_BROKEN", set())
    monkeypatch.setattr(servers, "_NETWORKS", set())
    yield
    servers.stop_all()


def _pg_leave(s):
    s.sql("create database extra")
    s.sql("create role r1 login")
    s.sql("create schema side; create table side.t (id int);"
          " alter table side.t owner to r1; create publication p1 for all"
          " tables; select lo_create(0)")
    s.sql("select pg_create_logical_replication_slot('s1', 'pgoutput')")
    s.sql("alter system set work_mem = '7MB'")
    s.sql("select pg_reload_conf()")


def _pg_left(s):
    q = lambda sql: s.sql(sql)[0][0]  # noqa: E731
    return [what for what, n in (
        ("databases", q("select count(*) from pg_database where datname ="
                        " 'extra'")),
        ("roles", q("select count(*) from pg_roles where rolname = 'r1'")),
        ("schemas", q("select count(*) from pg_namespace where nspname ="
                      " 'side'")),
        ("publications", q("select count(*) from pg_publication")),
        ("large objects", q("select count(*) from"
                            " pg_largeobject_metadata")),
        ("slots", q("select count(*) from pg_replication_slots")),
        ("settings", q("select count(*) from pg_file_settings where name ="
                       " 'work_mem'"))) if n]


def _my_leave(s):
    s.sql("create database extra; create user u1 identified by 'x';"
          " set global max_connections = 77;"
          " create table extra.t (id int); insert into extra.t values (1)")


def _my_left(s):
    return [what for what, bad in (
        ("databases", s.sql("show databases like 'extra'")),
        ("users", s.sql("select user from mysql.user where user = 'u1'")),
        ("globals", s.sql("select @@global.max_connections = 77")[0][0]),
        # a fresh log holds only the events that describe the log itself
        ("binary log", [e for e in s.sql("show binlog events")
                        if e[2] not in _LOG_OWN])) if bad]


_LOG_OWN = {"Format_desc", "Previous_gtids", "Gtid_list", "Binlog_checkpoint",
            "Rotate"}


def _redis_leave(s):
    c = s.client()
    c.set("k", "v")
    c.acl_setuser("u1", enabled=True)
    c.config_set("maxmemory-policy", "allkeys-lru")


def _redis_left(s):
    c = s.client()
    return [what for what, bad in (
        ("keys", c.dbsize()), ("users", "u1" in c.acl_users()),
        ("config", c.config_get("maxmemory-policy")["maxmemory-policy"]
         == "allkeys-lru")) if bad]


def _mongo_leave(s):
    c = s.client()
    c["extra"]["t"].insert_one({"a": 1})
    c.admin.command("createUser", "u1", pwd="x", roles=[])


def _mongo_left(s):
    c = s.client()
    return [what for what, bad in (
        ("databases", "extra" in c.list_database_names()),
        ("users", any(u["user"] == "u1" for u in c.admin.command(
            "usersInfo")["users"]))) if bad]


def _kafka_leave(s):
    from kafka.admin import NewTopic
    a = s.client()
    try:
        a.create_topics([NewTopic("extra", 1, 1)])
    finally:
        a.close()


def _kafka_left(s):
    a = s.client()
    try:
        return ["topics"] if [t for t in a.list_topics()
                              if not t.startswith("_")] else []
    finally:
        a.close()


def _ch_leave(s):
    s.sql("create database extra")


def _ch_left(s):
    return ["databases"] if s.sql("select count() from system.databases"
                                  " where name = 'extra'")[0][0] else []


def _dynamo_leave(s):
    s.client().create_table(
        TableName="extra", BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id",
                               "AttributeType": "S"}])


def _dynamo_left(s):
    return ["tables"] if s.client().list_tables()["TableNames"] else []


def _minio_leave(s):
    s.client().put_object(Bucket=servers.space_name(
        "tests.test_put_back_minio").replace("_", "-"), Key="k", Body=b"v")


def _minio_left(s):
    return ["buckets"] if s.client().list_buckets()["Buckets"] else []


def _cassandra_leave(s):
    s.sql("create keyspace extra with replication = {'class':"
          " 'SimpleStrategy', 'replication_factor': 1}")


def _cassandra_left(s):
    return ["keyspaces"] if s.sql(
        "select keyspace_name from system_schema.keyspaces where"
        " keyspace_name = 'extra'") else []


def _os_leave(s):
    servers._OpenSearch.http(s, "PUT", "/extra")


def _os_left(s):
    return ["indices"] if [i for i in servers._OpenSearch.http(
        s, "GET", "/_cat/indices?format=json")
        if not i["index"].startswith(".")] else []


LEAVE = {"pg": (_pg_leave, _pg_left), "mysql": (_my_leave, _my_left),
         "mariadb": (_my_leave, _my_left),
         "redis": (_redis_leave, _redis_left),
         "mongo": (_mongo_leave, _mongo_left),
         "kafka": (_kafka_leave, _kafka_left),
         "redpanda": (_kafka_leave, _kafka_left),
         "clickhouse": (_ch_leave, _ch_left),
         "dynamodb": (_dynamo_leave, _dynamo_left),
         "minio": (_minio_leave, _minio_left),
         "cassandra": (_cassandra_leave, _cassandra_left),
         "opensearch": (_os_leave, _os_left)}


def _ids(servers_):
    return {s.container: servers.docker("inspect", "-f", "{{.Id}}",
                                        s.container).stdout
            for s in servers_}


@needs_docker
@pytest.mark.parametrize("key", sorted(LEAVE))
def test_a_profile_is_put_back_as_it_was_found(key, lane7):
    leave, left = LEAVE[key]
    with servers.space(key, f"tests.test_put_back_{key}") as sp:
        ids = _ids(sp.servers)
        first = sp.servers[0]
        assert first.port >= servers.BASE + 70 * servers.BLOCK
        leave(first)
        # the probe can say "left": what it names is there now
        assert left(first), key
    assert key not in servers._BROKEN
    again = servers.servers_of(key)
    assert _ids(again) == ids, "started again, not reset"
    # read before the next module's space is made in them; a broker
    # finishes deleting a topic after the call returns
    until(lambda: left(again[0]) == [], 30, f"{key} to be as it was")


@needs_docker
def test_a_reset_that_cannot_finish_gives_the_next_module_fresh_servers(
        lane7, monkeypatch):
    with servers.space("pg", "tests.test_broken_one") as sp:
        first = sp.src.container
        before = _ids(sp.servers)
        with monkeypatch.context() as m:
            m.setattr(servers._Postgres, "reset", lambda self, s: 1 / 0)
            sp.__exit__(None, None, None)
            assert "pg" in servers._BROKEN
    with servers.space("pg", "tests.test_broken_two") as sp:
        assert sp.src.container == first
        assert _ids(sp.servers) != before
        assert sp.src.sql("select 1") == [(1,)]


@needs_docker
def test_a_standby_follows_its_primary_and_forgets_with_it(lane7):
    seen = "select count(*) from pg_database where datname = 'extra'"
    with servers.space("pg-replica", "tests.test_replica_one") as sp:
        sp.primary.sql("create database extra")
        until(lambda: sp.standby.sql(seen) == [(1,)], 30,
              "the standby to see the new database")
    with servers.space("pg-replica", "tests.test_replica_two") as sp:
        until(lambda: sp.standby.sql(seen) == [(0,)], 30,
              "the standby to forget it")
        assert sp.standby.sql("select pg_is_in_recovery()") == [(True,)]


@needs_docker
def test_a_tls_server_verifies_against_the_authority(lane7):
    import psycopg2
    with servers.space("pg-tls", "tests.test_tls") as sp:
        c = psycopg2.connect(host="127.0.0.1", port=sp.one.port,
                             user="postgres", password="test",
                             dbname="postgres", sslmode="verify-full",
                             sslrootcert=sp.one.ca)
        try:
            with c.cursor() as cur:
                cur.execute("select ssl from pg_stat_ssl where pid ="
                            " pg_backend_pid()")
                assert cur.fetchall() == [(True,)]
        finally:
            c.close()


@needs_docker
def test_an_image_is_built_once_while_its_recipe_holds(lane7):
    recipe = "FROM alpine:3.20\nCOPY note /note\n"
    tag = servers.image("lanes-check", recipe, {"note": "one"})
    try:
        began = time.monotonic()
        assert servers.image("lanes-check", recipe, {"note": "one"}) == tag
        assert time.monotonic() - began < 5
        other = servers.image("lanes-check", recipe, {"note": "two"})
        assert other != tag
        assert servers.docker("image", "inspect", tag).returncode != 0, \
            "the stale build was kept"
    finally:
        got = servers.docker("images", "migkit-test-lanes-check", "--format",
                             "{{.Repository}}:{{.Tag}}")
        if got.stdout.split():
            servers.docker("rmi", *got.stdout.split())


@needs_docker
@pytest.mark.parametrize("key", [
    pytest.param(k, marks=[pytest.mark.vm] if p.vm else [])
    for k, p in sorted(servers.PROFILES.items())])
def test_every_profile_starts_lends_and_is_put_back(key, lane7):
    """One at a time: each profile starts, lends a module its space and
    is reset without breaking."""
    with servers.space(key, f"tests.test_profile_{key}") as sp:
        assert sp.servers and all(s.port for s in sp.servers)
    assert key not in servers._BROKEN
    servers.stop(key)

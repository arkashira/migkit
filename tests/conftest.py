import collections
import json
import os
import re
import signal
import socket
import subprocess
import time
import types

import pytest

from tests import servers as _servers

# what the decision engine learns across hops (`costmodel.Knowledge`) is
# kept apart for a test run, never added to this machine's own
if not os.environ.get("MIGKIT_KNOWLEDGE"):
    import tempfile
    os.environ["MIGKIT_KNOWLEDGE"] = os.path.join(
        tempfile.mkdtemp(prefix="migkit-knowledge-"), "knowledge.json")

# the user's state directory (`config.state_home`: the masking secret, restore
# points) is kept apart for a test run, never this machine's own
if not os.environ.get("XDG_STATE_HOME"):
    import tempfile
    os.environ["XDG_STATE_HOME"] = tempfile.mkdtemp(prefix="migkit-state-")


def _free(host, port):
    with socket.socket() as s:
        s.settimeout(1)
        return s.connect_ex((host, port)) != 0


def _wait_tcp(host, port, timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex((host, port)) == 0:
                return True
        time.sleep(2)
    return False


def _have_docker():
    return _servers.docker_up()


#: only the mark. It used to carry a `skipif` as its argument, which pytest
#: never applies (a mark given a mark stores it), while asking `docker info`
#: at every import; each server fixture asks once, when it is needed
needs_docker = pytest.mark.docker

#: the engines whose servers run on x86-64 only, by the words their test
#: files are named with: a file named for one is marked for it, and the x86
#: workflow runs `pytest -m <engine>` - every such test and nothing else
ENGINE_MARKS = {"mssql": ("sql_server", "mssql"), "db2": ("db2",),
                "ase": ("sap_ase",), "teradata": ("teradata_against",)}

#: the SQL Server the tests start. SQL Edge where nothing else runs: SQL
#: Server's engine in the one build made for arm64 (retired, still
#: runnable). The x86 workflow sets the full 2022 server here.
MSSQL_IMAGE = _servers.MSSQL_IMAGE

#: per-test timeouts past `pyproject.toml`'s 900 s: a test in the migkit VM
#: (a server under Rosetta took up to 7 minutes to answer), and a test
#: marked `long`
VM_TIMEOUT, LONG_TIMEOUT = 2700, 3600
VM_REASON = ("needs the migkit VM (Oracle, SQL Server, Db2, ASE or an x86"
             " image): run it under tools/with_docker_lock.py --vm migkit")

#: `MIGKIT_TEST_STRICT=1` (the final run sets it) makes a skip for a
#: missing server, program or module a failure: a run where Kafka never
#: started must not read green (audit T8). A list of those kinds
#: (`server`) fails only them: CI, where the programs whose terms need
#: the operator's acceptance are not installed, fails on servers only
def _strict(value):
    value = (value or "").strip().lower()
    if value in ("1", "true", "yes", "on", "all"):
        return {"server", "program", "module"}
    if value in ("", "0", "false", "no", "off"):
        return set()
    kinds = {k.strip() for k in value.split(",")}
    unknown = kinds - {"server", "program", "module"}
    if unknown:
        raise pytest.UsageError(
            f"MIGKIT_TEST_STRICT={value}: 1, or some of server, program,"
            f" module - not {', '.join(sorted(unknown))}")
    return kinds


STRICT = _strict(os.environ.get("MIGKIT_TEST_STRICT"))
INFRASTRUCTURE = (
    ("server", re.compile(
        r"docker not available|did not come up|never answered|did not answer"
        r"|never started|cannot run on this machine|could not be set up"
        r"|could not be built|does not publish|needs the migkit VM"
        r"|emulation too slow|not reachable", re.I)),
    ("program", re.compile(
        r"not installed|not available on this machine|cannot run here"
        r"|not available$|is not available|driver is not installed", re.I)),
    ("module", re.compile(r"could not import|no module named", re.I)),
)


def mssql_run(name, port, password, memory_mb=1024):
    """`docker run` for a SQL Server container of `MSSQL_IMAGE`: an x86
    image is asked for as x86, which an arm64 machine runs under Rosetta
    (`tools/with_docker_lock.py --vm migkit`)."""
    x86 = [] if "azure-sql-edge" in MSSQL_IMAGE else ["--platform",
                                                      "linux/amd64"]
    return ["docker", "run", "-d", "--name", name, *x86,
            "-p", f"127.0.0.1:{port}:1433", "-e", "ACCEPT_EULA=Y",
            "-e", f"MSSQL_SA_PASSWORD={password}",
            "-e", f"MSSQL_MEMORY_LIMIT_MB={memory_mb}",
            "-e", "MSSQL_TELEMETRY_ENABLED=false", MSSQL_IMAGE]


# ---- what a test reaches: docker, the VM (audit T6, T13) --------------------

_REACHED = {}
_BUSY = set()


#: what follows `docker` in a command: the program named as a value (a
#: list of programs to install, an argv a fake records) is not a server
_DOCKER_VERBS = {"run", "exec", "rm", "inspect", "info", "create", "start",
                 "stop", "cp", "logs", "network", "pull", "build", "compose",
                 "pause", "unpause", "kill", "restart", "volume", "image",
                 "ps", "commit", "port", "images", "rmi", "stats", "wait"}


#: what code calls when it runs a program: `docker` beside one of these is
#: run, whatever command its caller hands it (`_docker("run", ...)`)
_RUNS = {"subprocess", "run", "Popen", "check_output", "check_call", "call",
         "execvp", "spawn"}


def _signs_of_text(strings, names=()):
    """Signs in the strings one piece of code names together, with the
    names it calls."""
    got = set()
    strings = set(strings)
    if any(s.split()[:1] == ["docker"] and s.split()[1:2] and
           s.split()[1] in _DOCKER_VERBS for s in strings) or (
            "docker" in strings and (strings & _DOCKER_VERBS
                                     or _RUNS & set(names))):
        got.add("docker")
    if any(v in s for s in strings for v in _servers.VM_SIGNS):
        got.add("vm")
    return got


def _strings(values):
    out = []
    for v in values:
        if isinstance(v, str):
            out.append(v)
        elif isinstance(v, (tuple, frozenset)):
            out += _strings(v)
    return out


def _ours(obj):
    return (getattr(obj, "__module__", "") or "").startswith("tests.") \
        or (getattr(obj, "__name__", "") or "").startswith("tests.")


def _signs_of(obj, names=()):
    """What running `obj` reaches: `docker` where it runs the program, `vm`
    where it names an image only the migkit VM runs - through the functions
    of the tests' own modules it calls and the strings it names, so a test
    whose fixture starts a server is known by the fixture, however it is
    marked (11 files asked for servers with no mark)."""
    if isinstance(obj, str):
        return _signs_of_text([obj])
    if isinstance(obj, (list, tuple, frozenset, set)):
        return _signs_of_text(_strings(obj))
    if isinstance(obj, types.ModuleType):
        got = set()
        if _ours(obj):
            for n in names:
                o = getattr(obj, n, None)
                if isinstance(o, (types.FunctionType, str, list, tuple)):
                    got |= _signs_of(o)
        return got
    fn = getattr(obj, "__wrapped__", obj)
    if not isinstance(fn, types.FunctionType) or not _ours(fn):
        return set()
    k = id(fn)
    if k in _REACHED:
        return _REACHED[k]
    if k in _BUSY:          # a cycle: its other members add the rest
        return set()
    _BUSY.add(k)
    try:
        got = set()
        profile = getattr(fn, "migkit_profile", None)
        if profile:
            got.add("docker")
            if _servers.PROFILES[profile].vm:
                got.add("vm")
        got |= _signs_of_code(fn.__code__, fn.__globals__)
    finally:
        _BUSY.discard(k)
    _REACHED[k] = got
    return got


def _signs_of_code(code, glb):
    got = _signs_of_text(_strings(code.co_consts), code.co_names)
    for c in code.co_consts:
        if isinstance(c, types.CodeType):
            got |= _signs_of_code(c, glb)
    for n in code.co_names:
        if n in glb:
            got |= _signs_of(glb[n], code.co_names)
    return got


#: this file's autouse fixtures, in every test's closure, touch a server
#: only for a test that asked for one
_NEUTRAL = {"clean_pg", "leases_let_go"}


def reached(item):
    got = _signs_of(getattr(item, "obj", None))
    info = getattr(item, "_fixtureinfo", None)
    for name in getattr(item, "fixturenames", ()):
        if name in _NEUTRAL:
            continue
        defs = info.name2fixturedefs.get(name) if info else None
        if defs:
            got |= _signs_of(defs[-1].func)
    return got


def _timeout(item):
    mark = item.get_closest_marker("timeout")
    if mark and mark.args:
        return float(mark.args[0])
    got = item.config.getoption("timeout", None)
    if got is None:
        try:
            got = item.config.getini("timeout")
        except ValueError:
            got = None
    return float(got) if got not in (None, "") else None


def pytest_collection_modifyitems(config, items):
    for item in items:
        name = item.path.stem if hasattr(item, "path") else ""
        for mark, words in ENGINE_MARKS.items():
            if any(re.search(rf"(^|_){w}(_|$)", name) for w in words):
                item.add_marker(getattr(pytest.mark, mark))
        signs = reached(item)
        if "docker" in signs and not item.get_closest_marker("docker"):
            item.add_marker(pytest.mark.docker)
        if "vm" in signs and item.get_closest_marker("docker"):
            item.add_marker(pytest.mark.vm)
        if not item.get_closest_marker("timeout"):
            if item.get_closest_marker("long"):
                item.add_marker(pytest.mark.timeout(LONG_TIMEOUT))
            elif item.get_closest_marker("vm"):
                item.add_marker(pytest.mark.timeout(VM_TIMEOUT))


def pytest_collection_finish(session):
    """`MIGKIT_TEST_COLLECT_OUT=<file>`: every item with its marks, for
    `tools/final_suite.py` to lay the lanes out by."""
    out = os.environ.get("MIGKIT_TEST_COLLECT_OUT")
    if not out:
        return
    rows = [{"id": i.nodeid, "file": str(i.path),
             "marks": sorted({m.name for m in i.iter_markers()}),
             "timeout": _timeout(i)} for i in session.items]
    with open(out, "w") as fh:
        json.dump(rows, fh)


def _vm_here():
    if os.environ.get("DOCKER_CONTEXT") == "colima-migkit" or \
            os.environ.get("MIGKIT_TEST_VM", "").lower() in ("1", "true",
                                                             "yes", "on"):
        return True
    up, arch = _servers.daemon()
    return up and arch in ("x86_64", "amd64")


_PINNED_CHECKED = {}


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    if item.get_closest_marker("vm") and not _vm_here():
        pytest.skip(VM_REASON)
    if item.get_closest_marker("docker") and _servers.lane() != 0:
        path = str(item.path)
        if path not in _PINNED_CHECKED:
            _PINNED_CHECKED[path] = _servers.scan(path).pinned
        if _PINNED_CHECKED[path]:
            pytest.fail(f"{item.path.name} names lane 0's PostgreSQL pair by"
                        f" its fixed port or container name, and this is lane"
                        f" {_servers.lane()}: it runs on lane 0 only"
                        f" (tools/final_suite.py puts it there), or reads"
                        f" `pg_pair`'s ports", pytrace=False)


# ---- strict skips and the count of skips by reason (audit T8) ---------------

def _skip_reason(rep):
    lr = rep.longrepr
    reason = lr[2] if isinstance(lr, tuple) and len(lr) == 3 else str(lr)
    return reason[len("Skipped: "):] if reason.startswith("Skipped: ") \
        else reason


def infrastructure(reason):
    """What a skip's reason says is missing - `server`, `program`,
    `module` - or None for a skip the test chose."""
    return next((kind for kind, pat in INFRASTRUCTURE if pat.search(reason)),
                None)


def _judge(rep):
    if not rep.skipped or hasattr(rep, "wasxfail"):
        return
    reason = _skip_reason(rep)
    kind = infrastructure(reason)
    if kind in STRICT:
        rep.outcome = "failed"
        rep.longrepr = (f"MIGKIT_TEST_STRICT: a missing {kind} fails the"
                        f" test here instead of skipping it: {reason}")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    _judge(outcome.get_result())


@pytest.hookimpl(hookwrapper=True)
def pytest_make_collect_report(collector):
    outcome = yield
    _judge(outcome.get_result())


def pytest_terminal_summary(terminalreporter):
    stats = terminalreporter.stats
    counted = collections.Counter()
    for rep in stats.get("skipped", []):
        if hasattr(rep, "wasxfail"):
            continue
        reason = _skip_reason(rep)
        counted[(infrastructure(reason) or "chosen", reason)] += 1
    for rep in stats.get("failed", []) + stats.get("error", []):
        text = str(rep.longrepr or "")
        if text.startswith("MIGKIT_TEST_STRICT:"):
            m = re.match(r"MIGKIT_TEST_STRICT: a missing (\w+)", text)
            counted[(m.group(1) + ", failed", text.split(": ", 2)[-1])] += 1
    if not counted:
        return
    terminalreporter.write_sep("=", "skips by reason")
    for (kind, reason), n in counted.most_common():
        terminalreporter.write_line(f"{n:5d}  [{kind}] {reason[:160]}")


# ---- the final run's record of each test (tools/final_suite.py) -------------

_PROGRESS = os.environ.get("MIGKIT_TEST_PROGRESS")


def _note(**row):
    if not _PROGRESS or os.environ.get("PYTEST_XDIST_WORKER"):
        return
    row["t"] = round(time.time(), 3)
    with open(_PROGRESS, "a") as fh:
        fh.write(json.dumps(row, default=str)[:4000] + "\n")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item, nextitem):
    _note(ev="start", id=item.nodeid, timeout=_timeout(item))
    yield
    _note(ev="end", id=item.nodeid, last=nextitem is None
          or nextitem.path != item.path)


def pytest_runtest_logreport(report):
    if not _PROGRESS:
        return
    row = {"ev": "report", "id": report.nodeid, "when": report.when,
           "outcome": report.outcome, "duration": round(report.duration, 3)}
    if report.skipped:
        row["reason"] = _skip_reason(report)
    elif report.failed:
        row["message"] = (report.longreprtext or "")[-1500:]
    _note(**row)


def pytest_collectreport(report):
    if report.failed:
        _note(ev="collect-error", id=report.nodeid,
              message=(report.longreprtext or "")[-1500:])


def pytest_configure(config):
    """A SIGTERM (the final run's watchdog, a CI cancel) ends the session
    as Ctrl-C does, so every fixture's teardown and the servers' removal
    still run."""
    if signal.getsignal(signal.SIGTERM) is signal.SIG_DFL:
        def stop(*_):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, stop)
    _note(ev="session-start", lane=_servers.lane())


def pytest_sessionfinish(session, exitstatus):
    _servers.stop_all()
    _note(ev="session-end", status=int(exitstatus))


# ---- the PostgreSQL pair every module before the lanes shares ---------------

@pytest.fixture(scope="module")
def pg_pair(request):
    """The lane's `pg` pair (`tests/servers.py`), lent to the module and
    reset when it is done: other databases, slots, publications,
    subscriptions, roles, schemas and settings a module left no longer
    reach the next one (audit T2: another module's large objects and
    slots did)."""
    with _servers.space("pg", request.module.__name__, own=False) as got:
        yield {"src": got.src.port, "dst": got.dst.port}


def psql(port, sql, db="postgres"):
    return subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=test", _pg_container(port),
         "psql", "-U", "postgres", "-d", db, "-At", "-c", sql],
        capture_output=True, text=True)


def _pg_container(port):
    for s in _servers.running().get("pg", ()):
        if s.port == port:
            return s.container
    role = "src" if port == 55432 else "dst"
    return _servers.PINNED[("pg", role)][0] if _servers.lane() == 0 \
        else _servers.name(f"{_servers.PREFIX}-pg-{role}")


_WIPE = """
do $$ declare r record; begin
  for r in select tablename from pg_tables where schemaname = 'public'
  loop execute 'drop table if exists public.'||quote_ident(r.tablename)||' cascade'; end loop;
  for r in select sequencename from pg_sequences where schemaname = 'public'
  loop execute 'drop sequence if exists public.'||quote_ident(r.sequencename)||' cascade'; end loop;
  for r in select extname from pg_extension where extname <> 'plpgsql'
  loop execute 'drop extension if exists '||quote_ident(r.extname)||' cascade'; end loop;
  for r in select p.oid::regprocedure as f from pg_proc p
           join pg_namespace n on n.oid = p.pronamespace
           where n.nspname = 'public' and p.prokind in ('f', 'p')
  loop execute 'drop routine if exists '||r.f||' cascade'; end loop;
  -- large objects too: a whole-database move carries them now
  perform lo_unlink(oid) from pg_largeobject_metadata;
end $$;
"""


@pytest.fixture(autouse=True)
def leases_let_go():
    """A lease a test took and left - a holder it played dead - is not
    asked by the next test's write loops (`lease.check_held`)."""
    yield
    from migkit import lease
    with lease._HOLDING_LOCK:
        lease._HOLDING.clear()


@pytest.fixture(autouse=True)
def clean_pg(request):
    """Drop all user tables, sequences and routines on both DBs before
    each pg test, so the shared session containers never leak state
    between tests: a trigger function one test left behind read as an
    extra object in another's schema check. Through the driver: two
    `docker exec psql` a test were a process and an exec each."""
    if "pg_pair" not in request.fixturenames:
        yield
        return
    request.getfixturevalue("pg_pair")
    for s in _servers.servers_of("pg"):
        s.sql(_WIPE)
    yield


def migkit_cmd():
    """How to invoke the CLI under test.

    Not `<repo>/.venv/bin/migkit`: that only exists in a checkout that happens
    to have a venv at that path, which is the assumption migkit is trying to
    stop making. Use the console script belonging to the interpreter running
    the tests, and fall back to running the module.
    """
    import sys
    import sysconfig
    from pathlib import Path
    for d in (sysconfig.get_path("scripts"), str(Path(sys.executable).parent)):
        if d:
            exe = Path(d) / "migkit"
            if exe.exists():
                return [str(exe)]
    return [sys.executable, "-m", "migkit.cli"]


def verdict(results, scope):
    """The one result carrying exactly this scope.

    Selecting with `r.scope.endswith(...)` picks the wrong record the moment
    a second check's name ends with the same word. `f"{db} duplicate keys"`
    was added beside `f"{db} keys"`, and `endswith("keys")` started answering
    with whichever the engine happened to append first - the deep battery
    returns them in the order it runs them, not an order any test chose.

    That one failed loudly, because the two verdicts disagreed. A pair where
    both read `ok` would have passed on the wrong record, which is the shape
    this helper exists to make impossible: it takes the scope in full and
    refuses anything other than exactly one match.
    """
    got = [r for r in results if r.scope == scope]
    if len(got) != 1:
        tail = scope.rsplit(" ", 1)[-1]
        near = [r.scope for r in results if r.scope.endswith(tail)]
        raise AssertionError(
            f"{len(got)} verdicts with scope {scope!r}"
            f" - scopes ending in {tail!r}: {near}")
    return got[0]

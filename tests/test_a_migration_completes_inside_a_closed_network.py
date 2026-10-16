"""W8: a migkit that reaches nothing but its two databases installs, sets up
its hop, moves the data and follows the changes made after the copy, checks
the target and writes its report - and anything that tries to call out is
named.

One docker network with no way off it (`servers.closed`, `--internal`): a
MySQL source, a PostgreSQL target and a bare Python container. migkit is
installed in that container from the offline bundle's wheels with
`--no-index` (the sums checked first), then `init`, `move --mode full+cdc`
(rows written, changed and deleted while it follows), `check` and `report`
run there against both. Every name looked up and every connection opened
from Python is written down by an audit hook as it is made, with the
modules that made it: one to anywhere but the two databases fails the test,
named. What a library does below Python (a client library's own resolver)
meets the network's own lack of a route, which the canary proves: a lookup
of an outside name and a connection to an outside address both fail, and
the hook names the module that tried.
"""
import json
import time
from pathlib import Path

import pytest

from tests import servers

pytestmark = [pytest.mark.docker, pytest.mark.long]

if not servers.docker_up():
    pytestmark.append(pytest.mark.skip(reason="docker not available"))

PY_IMAGE = "python:3.12-slim"
ROWS = 200
#: what the container may reach: the two databases by alias, and itself
ALLOWED = {"source", "target", "localhost", "127.0.0.1", "::1"}

#: written into the migkit container: every lookup and connection a Python
#: module makes, as one JSON line, with the modules on the stack
AUDIT = r'''
import json, os, sys

def _who():
    out, f = [], sys._getframe(2)
    while f is not None and len(out) < 8:
        path = f.f_code.co_filename
        if "site-packages/" in path:
            top = path.split("site-packages/", 1)[1].split("/", 1)[0]
        else:
            top = path.rsplit("/python3.12/", 1)[-1].split("/", 1)[0]
        top = top.removesuffix(".py")
        if top not in ("socket", "ssl", "selectors") and top not in out:
            out.append(top)
        f = f.f_back
    return out

def _hook(event, args):
    where = os.environ.get("MIGKIT_TEST_EGRESS_LOG")
    if not where:
        return
    if event == "socket.connect":
        address = args[1]
        if not isinstance(address, tuple):
            return
        host, port = address[0], address[1]
    elif event == "socket.getaddrinfo":
        host, port = args[0], args[1]
    elif event == "socket.gethostbyname":
        host, port = args[0], None
    else:
        return
    if isinstance(host, bytes):
        host = host.decode()
    with open(where, "a") as f:
        f.write(json.dumps({"event": event, "host": host, "port": port,
                            "who": _who()}) + "\n")

sys.addaudithook(_hook)
'''

DRIVE = r'''
import pathlib, subprocess, sys
log = open("/work/move.log", "w")
p = subprocess.Popen(sys.argv[1:], stdout=log, stderr=subprocess.STDOUT,
                     cwd="/work")
pathlib.Path("/work/move.pid").write_text(str(p.pid))
pathlib.Path("/work/move.exit").write_text(str(p.wait()))
'''

CANARY = r'''
import socket, urllib.request
failed = []
try:
    urllib.request.urlopen("http://example.com/", timeout=5)
except Exception as e:
    failed.append("lookup")
try:
    socket.create_connection(("192.0.2.1", 443), timeout=5).close()
except OSError:
    failed.append("route")
print(" ".join(failed))
'''


def _sh(container, *argv, env=None, timeout=600):
    flags = [a for k, v in (env or {}).items() for a in ("-e", f"{k}={v}")]
    return servers.docker("exec", *flags, container, *argv, timeout=timeout)


def _wait(what, probe, seconds):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if probe():
            return
        time.sleep(2)
    pytest.fail(f"{what} did not come within {seconds}s")


def _my(c, sql):
    got = _sh(c, "mysql", "-uroot", f"-p{servers.PASSWORD}", "-N", "-B",
              "-e", sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _pg(c, sql, db="app"):
    got = _sh(c, "psql", "-U", "postgres", "-d", db, "-At", "-c", sql)
    return got.stdout.strip() if got.returncode == 0 else None


def _bundle(tmp_path):
    """The bundle's wheels for the docker machine's Linux, summed and
    signed, built here where the network is."""
    import subprocess

    from migkit import tools
    arch = {"arm64": "aarch64", "amd64": "x86_64"}.get(
        servers.daemon()[1], servers.daemon()[1])
    out = tmp_path / "bundle"
    out.mkdir()
    key = tmp_path / "signer"
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f",
                    str(key)], check=True)
    said = []
    tools._bundle_wheels(out, tools._platform(f"linux-{arch}"), said.append)
    mp = pytest.MonkeyPatch()
    mp.setenv("MIGKIT_BUNDLE_KEY", str(key))
    try:
        tools._sign_bundle(out, said.append)
    finally:
        mp.undo()
    assert (out / "SHA256SUMS.sig").exists(), said
    # migkit's own wheels whole; the second reading's are not installed here
    assert not any("dependencies' wheels" in s or "own wheel" in s
                   for s in said), said
    return out


def _entries(c, path):
    got = _sh(c, "cat", path)
    return [json.loads(x) for x in got.stdout.splitlines() if x.strip()]


def test_a_migration_completes_inside_a_closed_network(tmp_path):
    bundle = _bundle(tmp_path)
    work = tmp_path / "put"
    (work / "audit").mkdir(parents=True)
    (work / "audit" / "sitecustomize.py").write_text(AUDIT)
    (work / "drive.py").write_text(DRIVE)
    (work / "canary.py").write_text(CANARY)
    module = __name__
    mysql = servers.PROFILES["mysql"]
    with servers.closed(module) as net, \
            servers.private(module, "src", mysql.image,
                            "--binlog-row-metadata=FULL",
                            env={"MYSQL_ROOT_PASSWORD": servers.PASSWORD},
                            options=("--network", net, "--network-alias",
                                     "source")) as (src, _), \
            servers.private(module, "dst", servers.PROFILES["pg"].image,
                            "-c", "wal_level=logical",
                            env={"POSTGRES_PASSWORD": servers.PASSWORD},
                            options=("--network", net, "--network-alias",
                                     "target")) as (dst, _), \
            servers.private(module, "migkit", PY_IMAGE, "sleep", "infinity",
                            options=("--network", net)) as (mk, _):
        for what, rel in ((bundle, "/bundle"), (work / "audit", "/audit")):
            assert servers.docker("cp", str(what), f"{mk}:{rel}").returncode \
                == 0
        _sh(mk, "mkdir", "-p", "/work")
        for f in ("drive.py", "canary.py"):
            servers.docker("cp", str(work / f), f"{mk}:/work/{f}")
        # install: the sums hold, then the wheels with no index at all
        got = _sh(mk, "sh", "-c", "cd /bundle && sha256sum -c --quiet"
                  " SHA256SUMS")
        assert got.returncode == 0, got.stdout + got.stderr
        got = _sh(mk, "python", "-m", "pip", "install", "-q", "--no-index",
                  "--find-links", "/bundle/wheels", "migkit", timeout=600)
        assert got.returncode == 0, got.stdout[-2000:] + got.stderr[-2000:]

        _wait("the source", lambda: _sh(
            src, "mysql", "-uroot", f"-p{servers.PASSWORD}", "-e",
            "select 1").returncode == 0, mysql.start_s)
        _wait("the target", lambda: _pg(dst, "select 1", "postgres") == "1",
              180)
        _my(src, "create database app; create table app.t (id int primary"
            " key, v varchar(40), b blob); insert into app.t"
            " with recursive n(i) as (select 1 union all select i + 1 from"
            f" n where i < {ROWS}) select i, concat('v', i), repeat('x', i)"
            " from n")
        assert _pg(dst, "create database app", "postgres") is not None

        env = {"MIGKIT_SOURCE": "mysql://root@source:3306/app",
               "MIGKIT_TARGET": "postgres://postgres@target:5432/app",
               "MIGKIT_SOURCE_PASSWORD": servers.PASSWORD,
               "MIGKIT_TARGET_PASSWORD": servers.PASSWORD,
               "MIGKIT_HOP": "closed", "MIGKIT_CONF": "/work/conf/hops.yaml",
               "MIGKIT_REPORTS": "/work/reports", "HOME": "/work",
               "XDG_CONFIG_HOME": "/work/config",
               "XDG_STATE_HOME": "/work/state", "NO_COLOR": "1",
               "MIGKIT_EGRESS": "none", "PYTHONPATH": "/audit",
               "MIGKIT_TEST_EGRESS_LOG": "/work/egress.jsonl"}

        def migkit(*argv, timeout=300):
            got = _sh(mk, "sh", "-c", "cd /work && exec migkit \"$@\"", "-",
                      *argv, env=env, timeout=timeout)
            return got.returncode, got.stdout + got.stderr

        code, said = migkit("init", "--here")
        assert code == 0, said[-3000:]

        # the move with its tail, in the background of the container
        assert servers.docker(
            "exec", "-d", *[a for k, v in env.items()
                            for a in ("-e", f"{k}={v}")],
            mk, "python", "/work/drive.py", "migkit", "move", "closed",
            "--mode", "full+cdc", "--go").returncode == 0
        where = {}

        def copied():
            if not where:
                schema = _pg(dst, "select table_schema from"
                             " information_schema.tables where"
                             " table_name = 't'")
                if not schema:
                    return False
                where["t"] = f'"{schema.splitlines()[0]}".t'
            return _pg(dst, f"select count(*) from {where['t']}") == str(ROWS)

        def moved():
            return _sh(mk, "cat", "/work/move.log").stdout

        _wait("the copy", copied, 300)
        # written while it follows: new rows, a change, a delete
        _my(src, "insert into app.t values (1001, 'after', repeat('y',"
            " 70000)), (1002, 'after', null); update app.t set v = 'changed'"
            " where id = 1; delete from app.t where id = 2")

        def followed():
            return _pg(dst, f"select count(*) || ':' || coalesce((select v"
                       f" from {where['t']} where id = 1), '') || ':' ||"
                       f" (select count(*) from {where['t']} where id = 2)"
                       f" || ':' || coalesce((select length(b) from"
                       f" {where['t']} where id = 1001), 0)"
                       f" from {where['t']}") == f"{ROWS + 1}:changed:0:70000"

        _wait("the changes made while it follows", followed, 180)
        pid = _sh(mk, "cat", "/work/move.pid").stdout.strip()
        _sh(mk, "kill", "-INT", pid)
        _wait("the end of the follow", lambda: _sh(
            mk, "test", "-s", "/work/move.exit").returncode == 0, 120)
        assert _sh(mk, "cat", "/work/move.exit").stdout.strip() == "0", \
            moved()[-3000:]
        assert "stopped after" in moved(), moved()[-3000:]

        code, said = migkit("check", "closed")
        assert code == 0, said[-3000:]
        found = _sh(mk, "sh", "-c", "find /work/reports -name verdict.json"
                    " | head -1").stdout.strip()
        verdict = json.loads(_sh(mk, "cat", found).stdout)
        assert verdict["status"] == "same", verdict

        code, said = migkit("report", "closed")
        assert code == 0, said[-3000:]
        written = _sh(mk, "sh", "-c", "find /work/reports -type f"
                      " -newer " + found).stdout.split()
        assert written, said[-2000:]

        # nothing the migration did looked up or opened anything but the two
        ips = {servers.docker("inspect", "-f", "{{range .NetworkSettings"
                              ".Networks}}{{.IPAddress}} {{end}}",
                              c).stdout.split()[0] for c in (src, dst)}
        seen = _entries(mk, "/work/egress.jsonl")
        assert any(e["host"] in ("source", "target") for e in seen), seen[:5]
        out = [e for e in seen if e["host"] not in ALLOWED | ips]
        assert not out, "called out: " + "; ".join(
            f"{e['host']}:{e['port']} by {' < '.join(e['who'])}"
            for e in out[:10])

        # the canary: an outside call fails, and the hook names who made it
        got = _sh(mk, "python", "/work/canary.py", env={
            "PYTHONPATH": "/audit",
            "MIGKIT_TEST_EGRESS_LOG": "/work/canary.jsonl"}, timeout=60)
        assert got.stdout.split() == ["lookup", "route"], got.stdout
        tried = _entries(mk, "/work/canary.jsonl")
        named = [e for e in tried if e["host"] == "example.com"]
        assert named and "urllib" in named[0]["who"], tried
        assert any(e["host"] == "192.0.2.1" for e in tried), tried

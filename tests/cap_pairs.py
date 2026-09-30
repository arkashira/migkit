"""Container pairs of the resource model's tests (`migkit.model`): a
MySQL 8.4 pair and a PostgreSQL 16 pair, made with the server options a
test asks for and removed after, and a runner for migkit itself in a
container whose memory is capped. Names `migkit-test-cap-*`, ports
16220-16229."""
import json
import os
import socket
import subprocess
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
MY_SRC, MY_DST = "migkit-test-cap-my-src", "migkit-test-cap-my-dst"
MY_PORTS = (16220, 16221)
PG_SRC, PG_DST = "migkit-test-cap-pg-src", "migkit-test-cap-pg-dst"
PG_PORTS = (16224, 16225)
RUNNER = "migkit-test-cap-runner"
RUNNER_IMAGE = "migkit-test-cap-runner:1"


def docker_up():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:  # noqa: BLE001
        return False


def remove(*names):
    for n in names:
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def _answers(port, timeout):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


def my_pair(*server_args, run_args=()):
    """A MySQL 8.4 source and target; yields their host ports."""
    if not docker_up():
        pytest.skip("docker not available")
    remove(MY_SRC, MY_DST)
    try:
        for n, p in ((MY_SRC, MY_PORTS[0]), (MY_DST, MY_PORTS[1])):
            subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                            "MYSQL_ROOT_PASSWORD=test", "-p",
                            f"127.0.0.1:{p}:3306", *run_args, "mysql:8.4",
                            *server_args], check=True, capture_output=True)
        for n, p in ((MY_SRC, MY_PORTS[0]), (MY_DST, MY_PORTS[1])):
            end = time.time() + 300
            while time.time() < end:
                if _answers(p, 5) and subprocess.run(
                        ["docker", "exec", n, "mysql", "-uroot", "-ptest",
                         "-h127.0.0.1", "--protocol=tcp", "-e", "select 1"],
                        capture_output=True).returncode == 0:
                    break
                time.sleep(2)
            else:
                pytest.fail(f"{n} never answered")
        yield {"src": MY_PORTS[0], "dst": MY_PORTS[1]}
    finally:
        remove(MY_SRC, MY_DST)


def my(name, sql):
    got = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def pg_pair(dst_args=()):
    """A PostgreSQL 16 source and target, the target made with `dst_args`
    (a device's write rate, say); yields their host ports."""
    if not docker_up():
        pytest.skip("docker not available")
    remove(PG_SRC, PG_DST)
    try:
        for n, p, extra in ((PG_SRC, PG_PORTS[0], ()),
                            (PG_DST, PG_PORTS[1], dst_args)):
            subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                            "POSTGRES_PASSWORD=test", "-p",
                            f"127.0.0.1:{p}:5432", *extra, "postgres:16",
                            "-c", "wal_level=logical"], check=True,
                           capture_output=True)
        for n, p in ((PG_SRC, PG_PORTS[0]), (PG_DST, PG_PORTS[1])):
            end = time.time() + 120
            while time.time() < end:
                if _answers(p, 5) and subprocess.run(
                        ["docker", "exec", n, "pg_isready", "-U",
                         "postgres"], capture_output=True).returncode == 0:
                    break
                time.sleep(1)
            else:
                pytest.fail(f"{n} never answered")
        time.sleep(2)
        yield {"src": PG_PORTS[0], "dst": PG_PORTS[1]}
    finally:
        remove(PG_SRC, PG_DST)


def pg(name, sql, db="postgres"):
    got = subprocess.run(["docker", "exec", "-i", name, "psql", "-U",
                          "postgres", "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def ip(name):
    return subprocess.run(["docker", "inspect", "-f",
                           "{{range .NetworkSettings.Networks}}"
                           "{{.IPAddress}}{{end}}", name],
                          capture_output=True, text=True).stdout.strip()


def runner_image():
    """An image that runs migkit from this checkout: Python, the drivers a
    MySQL and a PostgreSQL move import, and the PostgreSQL client."""
    got = subprocess.run(["docker", "image", "inspect", RUNNER_IMAGE],
                         capture_output=True)
    if got.returncode == 0:
        return RUNNER_IMAGE
    work = Path(os.path.expanduser("~/.cache/migkit-cap-runner"))
    work.mkdir(parents=True, exist_ok=True)
    (work / "Dockerfile").write_text(
        "FROM python:3.12-slim\n"
        "RUN apt-get update && apt-get install -y --no-install-recommends"
        " postgresql-client && rm -rf /var/lib/apt/lists/*\n"
        "RUN pip install --no-cache-dir click rich pyyaml pymysql psutil"
        " psycopg2-binary\n")
    got = subprocess.run(["docker", "build", "-t", RUNNER_IMAGE, str(work)],
                         capture_output=True, text=True, timeout=900)
    assert got.returncode == 0, got.stdout[-2000:] + got.stderr[-2000:]
    return RUNNER_IMAGE


def watched(argv, env, progress, timeout=1200):
    """`argv` run to its end, its `progress.json` read twice a second
    meanwhile: (exit code, what it printed, [(seconds since the start,
    progress)], seconds it took)."""
    import tempfile
    began = time.monotonic()
    out = tempfile.TemporaryFile()
    proc = subprocess.Popen(argv, env=env, cwd=ROOT, stdout=out,
                            stderr=subprocess.STDOUT)
    samples = []
    while proc.poll() is None:
        if time.monotonic() - began > timeout:
            proc.kill()
            break
        try:
            samples.append((time.monotonic() - began,
                            json.loads(Path(progress).read_text())))
        except (OSError, ValueError):
            pass
        time.sleep(0.5)
    took = time.monotonic() - began
    out.seek(0)
    return proc.returncode, out.read().decode(errors="replace"), samples, took


def live_eta(samples):
    """How far the time left the move said at about half its rows was
    from the time it then took: (said, took, share off), or None where
    no sample fell in the middle of the copy."""
    running = [(t, p) for t, p in samples if not p.get("done")
               and p.get("eta_copy") is not None and p.get("rows_total")]
    ends = [t for t, p in samples if p.get("done")]
    if not running or not ends:
        return None
    t, p = min(running, key=lambda s: abs(
        s[1]["rows_done"] / s[1]["rows_total"] - 0.5))
    # the copy ended where the move wrote its last progress
    end = ends[0] - 0.5
    took = end - t
    if took <= 0:
        return None
    return p["eta_copy"], took, abs(p["eta_copy"] - took) / took


def run_capped(work, memory, *args, env=None, image=None, network=None):
    """`migkit <args>` from this checkout in a container held to `memory`
    (no swap), with `work` as its configuration and reports: (exit code,
    what it printed, killed for memory, the most its cgroup held). `image`
    one with the drivers another engine's move imports, `network` a docker
    network its servers are reached on by name."""
    remove(RUNNER)
    envs = []
    for k, v in {"PYTHONPATH": "/src", "MIGKIT_CONF": "/work/hops.yaml",
                 "MIGKIT_REPORTS": "/work/reports", "MIGKIT_MOVER": "builtin",
                 "COLUMNS": "200", **(env or {})}.items():
        envs += ["-e", f"{k}={v}"]
    cmd = ("python -m migkit.cli " + " ".join(args)
           + "; code=$?; cat /sys/fs/cgroup/memory.peak > /work/peak"
           " 2>/dev/null; cat /sys/fs/cgroup/memory.events > /work/events"
           " 2>/dev/null; exit $code")
    got = subprocess.run(["docker", "run", "--name", RUNNER, "--memory",
                          memory, "--memory-swap", memory, "-v",
                          f"{ROOT}:/src:ro", "-v", f"{work}:/work", *envs,
                          *(["--network", network] if network else []),
                          image or runner_image(), "sh", "-c", cmd],
                         capture_output=True, text=True, timeout=1800)
    state = json.loads(subprocess.run(
        ["docker", "inspect", "-f", "{{json .State}}", RUNNER],
        capture_output=True, text=True).stdout or "{}")
    remove(RUNNER)
    try:
        peak = int((Path(work) / "peak").read_text())
    except (OSError, ValueError):
        peak = None
    try:
        events = dict(line.split() for line in (
            Path(work) / "events").read_text().splitlines())
    except (OSError, ValueError):
        events = {}
    killed = bool(state.get("OOMKilled")) or int(
        events.get("oom_kill", 0)) > 0 or got.returncode == 137
    return got.returncode, got.stdout + got.stderr, killed, peak

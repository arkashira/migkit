"""Throwaway Redis servers for the tests that need a real one, started from
the `redis-server` program on this machine (no container): a source and a
target on free local ports, nothing persisted, their directory removed when
the module is done. A test module using it skips cleanly where there is no
`redis-server` or no `redis` package."""
import pathlib
import shutil
import socket
import subprocess
import tempfile
import time

import pytest

BINARY = shutil.which("redis-server")


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait(port, timeout=20):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(1)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.1)
    return False


class Local:
    """One server: `port`, a client (`client()`), and `stop()`."""

    def __init__(self, *args):
        self.port = _free_port()
        self.dir = pathlib.Path(tempfile.mkdtemp(
            prefix="migkit-redis-", dir="/var/tmp"
            if pathlib.Path("/var/tmp").is_dir() else None))
        self.proc = subprocess.Popen(
            [BINARY, "--port", str(self.port), "--bind", "127.0.0.1",
             "--save", "", "--appendonly", "no", "--dir", str(self.dir),
             *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if not _wait(self.port):
            self.stop()
            raise RuntimeError("redis-server did not start")

    def client(self, **kw):
        import redis
        return redis.Redis(host="127.0.0.1", port=self.port,
                           decode_responses=True, **kw)

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        shutil.rmtree(self.dir, ignore_errors=True)


def available():
    """Why the servers cannot be started here, or ""."""
    if not BINARY:
        return "no redis-server on this machine"
    try:
        import redis  # noqa: F401
    except ImportError:
        return "the redis package is not installed"
    return ""


needs_server = pytest.mark.skipif(bool(available()), reason=available())


@pytest.fixture(scope="module")
def pair():
    """A source and a target, both empty, stopped and removed at the end."""
    src, dst = Local(), Local()
    try:
        yield src, dst
    finally:
        src.stop()
        dst.stop()


@pytest.fixture
def clean(pair):
    for server in pair:
        server.client().flushall()
        c = server.client()
        for user in c.acl_users():
            if user != "default":
                c.acl_deluser(user)
        c.config_set("maxmemory", 0)
        c.function_flush()
    return pair


def engine(pair, tmp_path, **hop):
    """A Redis engine over the pair, reports under `tmp_path`."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.redis import RedisEngine
    src, dst = pair
    hop.setdefault("databases", ["0"])
    h = Hop(name="rl", engine="redis",
            source=Endpoint(host="127.0.0.1", port=src.port),
            target=Endpoint(host="127.0.0.1", port=dst.port), **hop)
    h.report_dir = lambda db=None, _p=tmp_path: _p
    return RedisEngine(h)

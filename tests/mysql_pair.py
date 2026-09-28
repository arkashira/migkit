"""A MySQL 8.4 source and target for the checks that compare the two, one
pair per module that asks for it and removed when the module is done."""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-chk-my-src", "migkit-test-chk-my-dst"
SRC_PORT, DST_PORT = 16030, 16031


def docker_up():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _ready(name, port, timeout=300):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            up = s.connect_ex(("127.0.0.1", port)) == 0
        if up and subprocess.run(
                ["docker", "exec", name, "mysql", "-uroot", "-ptest",
                 "-h127.0.0.1", "--protocol=tcp", "-e", "select 1"],
                capture_output=True).returncode == 0:
            return True
        time.sleep(2)
    return False


def _remove():
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


@pytest.fixture(scope="module")
def my_pair():
    if not docker_up():
        pytest.skip("docker not available")
    _remove()
    try:
        for n, p in ((SRC, SRC_PORT), (DST, DST_PORT)):
            subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                            "MYSQL_ROOT_PASSWORD=test", "-p", f"{p}:3306",
                            "mysql:8.4"], check=True, capture_output=True)
        for n, p in ((SRC, SRC_PORT), (DST, DST_PORT)):
            if not _ready(n, p):
                logs = subprocess.run(["docker", "logs", "--tail", "15", n],
                                      capture_output=True, text=True)
                pytest.fail(f"{n} never answered:"
                            f" {logs.stdout[-1500:]} {logs.stderr[-1500:]}")
        yield {"src": SRC_PORT, "dst": DST_PORT}
    finally:
        _remove()


def my(side, sql):
    """Run statements on one side; stops the test on an error."""
    got = subprocess.run(["docker", "exec", "-i", SRC if side == "src"
                          else DST, "mysql", "-uroot", "-ptest", "-N", "-B"],
                         input=sql, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def engine(db, options=None):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="chk", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              databases=[db], options=options or {})
    return MySQLEngine(hop)

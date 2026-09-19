"""MongoDB reports its own load, and an idle deployment is not throttled.

The probe is what makes the throttle engine-independent, so it is tested
against a real mongod rather than a stub: `serverStatus` field names are the
contract, and they are exactly the kind of thing that moves between versions.
"""
import socket
import subprocess
import time

import pytest

NAME = "migkit-test-mongohealth"
PORT = 15495


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


def _wait(port, timeout=90):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


@pytest.fixture(scope="module")
def mongod():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-p",
                    f"{PORT}:27017", "mongo:7"], check=True,
                   capture_output=True)
    assert _wait(PORT)
    time.sleep(3)
    yield
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)


def _engine():
    from migkit.config import Endpoint, Hop
    from migkit.engines.mongodb import MongoEngine
    hop = Hop(name="g", engine="mongodb",
              source=Endpoint(host="127.0.0.1", port=PORT),
              target=Endpoint(host="127.0.0.1", port=PORT))
    return MongoEngine(hop)


def test_probe_reads_real_numbers_from_serverstatus(mongod):
    h = _engine()._health("src")
    assert h is not None, "serverStatus field names may have moved"
    assert h.busy_ratio is not None
    assert 0.0 <= h.busy_ratio <= 1.0, h.busy_ratio


def test_an_idle_deployment_is_not_throttled(mongod):
    h = _engine()._health("src")
    assert h.stressed() == "", h.stressed()


def test_a_standalone_reports_unknown_lag_not_zero(mongod):
    """A deployment with no replica set must not look like one that is
    perfectly caught up - unknown and fine are different answers."""
    h = _engine()._health("src")
    assert h.lag_seconds is None


def test_an_unreachable_deployment_yields_no_health_rather_than_crashing():
    from migkit.config import Endpoint, Hop
    from migkit.engines.mongodb import MongoEngine
    hop = Hop(name="g", engine="mongodb",
              source=Endpoint(host="127.0.0.1", port=1),
              target=Endpoint(host="127.0.0.1", port=1))
    assert MongoEngine(hop)._health("src") is None

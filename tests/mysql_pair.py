"""A MySQL 8.4 source and target for the checks that compare the two: the
lane's shared `mysql` pair (`tests/servers.py`), lent to each module that
asks for it and put back as it was found when the module is done - one
MySQL start a lane instead of one a module."""
import pytest

from tests import servers

PROFILE = "mysql"


def docker_up():
    return servers.docker_up()


def _side(side):
    return next(s for s in servers.servers_of(PROFILE) if s.role == side)


@pytest.fixture(scope="module")
def my_pair(request):
    with servers.space(PROFILE, request.module.__name__, own=False) as got:
        yield {"src": got.src.port, "dst": got.dst.port}


def my(side, sql):
    """Run statements on one side; stops the test on an error."""
    got = _side(side).exec("mysql", "-uroot", "-ptest", "-N", "-B",
                           input=sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def engine(db, options=None):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="chk", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=_side("src").port,
                              user="root", password="test"),
              target=Endpoint(host="127.0.0.1", port=_side("dst").port,
                              user="root", password="test"),
              databases=[db], options=options or {})
    return MySQLEngine(hop)

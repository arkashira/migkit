"""TDSQL (distributed) followed set by set (backlog P0-CLOUDS): its proxy
keeps no binlog, and each set - a MySQL server holding its share of the
rows - keeps its own. Two MySQL 8.4 servers stand in for two sets here;
the proxy that routes a statement to one is TDSQL's, and is not.

Each set's changes are read from its own binlog, all of them handed on,
and the follow's position is reached only once every set's is: one set
read to its end and another not is not caught up.
"""
import subprocess

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker
from tests.mysql_pair import _ready

pytestmark = needs_docker

SETS = (("migkit-test-tdsql-a", 16456), ("migkit-test-tdsql-b", 16457))


def _sql(name, sql):
    got = subprocess.run(["docker", "exec", name, "mysql", "-uroot", "-ptest",
                          "-h127.0.0.1", "--protocol=tcp", "-e", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout


@pytest.fixture(scope="module")
def sets():
    for name, _ in SETS:
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    try:
        for name, port in SETS:
            subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                            "MYSQL_ROOT_PASSWORD=test", "-p",
                            f"127.0.0.1:{port}:3306", "mysql:8.4",
                            "--binlog-row-metadata=FULL",
                            f"--server-id={port}"], check=True,
                           capture_output=True)
        for name, port in SETS:
            assert _ready(name, port), name
        for name, _ in SETS:
            _sql(name, "create database shop; create table shop.t"
                       " (id int primary key, n varchar(20))")
        yield
    finally:
        for name, _ in SETS:
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _engine(tmp_path):
    from migkit.engines.distmysql import TdsqlEngine
    first = SETS[0][1]
    src = Endpoint(host="127.0.0.1", port=first, user="root",
                   password="test",
                   options={"sets": [f"127.0.0.1:{p}" for _, p in SETS]})
    hop = Hop(name="tdsql", engine="dcdb", source=src,
              target=Endpoint(host="127.0.0.1", port=first, user="root",
                              password="test"),
              databases=["shop"])
    hop.report_dir = lambda db=None: tmp_path
    return TdsqlEngine(hop)


def _all(eng, token):
    out = []
    for _ in range(20):
        got, token = eng.neutral_changes("src", "shop", token, 1000)
        out += got
        if not got:
            break
    return out, token


def test_every_sets_changes_are_handed_on_from_its_own_binlog(sets,
                                                             tmp_path):
    eng = _engine(tmp_path)
    token = eng.change_point("src", "shop")
    assert set(token["sets"]) == {f"127.0.0.1:{p}" for _, p in SETS}
    a, b = SETS[0][0], SETS[1][0]
    _sql(a, "insert into shop.t values (1, 'on a')")
    _sql(b, "insert into shop.t values (2, 'on b')")
    _sql(b, "update shop.t set n = 'changed on b' where id = 2")
    _sql(a, "delete from shop.t where id = 1")
    want = eng.log_position("src", "shop")
    assert eng.position_reached(token, want) is False
    out, token = _all(eng, token)
    eng.release_changes()
    by = {}
    for c in out:
        by.setdefault(c["key"]["id"], []).append(
            (c["op"], (c.get("values") or {}).get("n")))
    assert by[1][0] == ("insert", "on a") and by[1][-1][0] == "delete"
    assert by[2][-1] == ("update", "changed on b")
    assert eng.position_reached(token, want) is True


def test_one_set_read_and_another_not_is_not_caught_up(sets, tmp_path):
    eng = _engine(tmp_path)
    token = eng.change_point("src", "shop")
    _sql(SETS[1][0], "insert into shop.t values (3, 'b again')")
    want = eng.log_position("src", "shop")
    # the first set's position moved to its end, the second's left behind
    behind = {"sets": dict(token["sets"])}
    behind["sets"][f"127.0.0.1:{SETS[0][1]}"] = \
        want["sets"][f"127.0.0.1:{SETS[0][1]}"]
    assert eng.position_reached(behind, want) is False
    _, token = _all(eng, token)
    eng.release_changes()
    assert eng.position_reached(token, want) is True


def test_a_mysql_server_named_tdsql_is_moved_as_mysql(sets, tmp_path):
    """A hop that names its engine tdsql asks the server: a MySQL server -
    a set alone, or TDSQL-C - reads the proxy's hint as a comment and
    names no set, so the hop is moved as MySQL; the distributed engine
    refuses it by the same answer."""
    from migkit import engines
    from migkit.engines.distmysql import TdsqlEngine, proxy_status, spread
    from migkit.engines.mysql import MySQLEngine
    port = SETS[0][1]

    def ep():
        return Endpoint(host="127.0.0.1", port=port, user="root",
                        password="test")
    hop = Hop(name="named-tdsql", engine="tdsql", source=ep(), target=ep(),
              databases=["shop"])
    hop.report_dir = lambda db=None: tmp_path
    status = proxy_status(MySQLEngine(hop), "src")
    assert "Uptime" in status and not spread(status)
    assert type(engines.get_engine(hop)) is MySQLEngine
    assert hop.engine == "mysql"
    with pytest.raises(SystemExit, match="is not TDSQL"):
        _engine(tmp_path).server_identity("src", "shop")

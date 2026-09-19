"""The MySQL half of moving indexes out of a bulk load's way.

Measured worth on MySQL 8, 200,000 rows and three secondary indexes: 1.150s
with them in place against 0.303s + 0.511s as a bare load plus one ALTER.

The protection that has no PostgreSQL equivalent is the foreign key. InnoDB
requires an index behind one and refuses to drop it, so those are left alone
rather than attempted and logged as a failure.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-mixw-src", "migkit-test-mixw-dst"
SRC_PORT, DST_PORT = 13421, 13422


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


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _my(name, sql, db="d"):
    r = subprocess.run(["docker", "exec", name, "mysql", "-uroot", "-ptest",
                        "-N", db, "-e", sql], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    for n, p in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{p}:3306",
                        "mysql:8"], check=True, capture_output=True)
    for p in (SRC_PORT, DST_PORT):
        assert _wait(p)
    for n in (SRC, DST):
        for _ in range(90):
            r = subprocess.run(["docker", "exec", n, "mysql", "-uroot",
                                "-ptest", "-e", "select 1"],
                               capture_output=True)
            if r.returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail(f"{n} never accepted a connection")
        _my(n, "create database d", db="mysql")
        _my(n, "create table parent (id int primary key, tag varchar(20));"
               " create table t (id int primary key, a varchar(40),"
               "   b varchar(40) unique, pid int,"
               "   index ix_a (a), index ix_apid (a, pid),"
               "   constraint fk_p foreign key (pid) references parent(id))")
    _my(SRC, "insert into parent values (1,'p')")
    _my(SRC, "insert into t values (1,'aa','bb',1),(2,'cc','dd',1)")
    assert _my(SRC, "select count(*) from t") == "2"
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="w", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              db_map={"d": "d"})
    hop.report_dir = lambda db=None: tmp_path
    return hop, MySQLEngine(hop)


def _idx(name, table="t"):
    return sorted(set(_my(name, "select index_name from"
                               " information_schema.statistics"
                               f" where table_schema='d'"
                               f" and table_name='{table}'").splitlines()))


def test_the_foreign_key_index_is_never_touched(pair, tmp_path):
    """InnoDB refuses to drop it, so attempting it would only produce a
    logged failure. It is excluded before anything is tried."""
    from migkit import movers
    hop, eng = _engine(tmp_path)
    before = _idx(DST)
    assert "fk_p" in before, before
    with movers._MyIndexWindow(eng, hop, "d", 2, None) as w:
        inside = _idx(DST)
        assert "fk_p" in inside, inside        # still there
        assert "PRIMARY" in inside and "b" in inside, inside
        assert "ix_a" not in inside, inside    # plain ones went
    assert not any("fk_p" in k for k in w.dropped), w.dropped
    assert _idx(DST) == before


def test_an_index_that_only_contains_an_fk_column_can_still_go(pair,
                                                               tmp_path):
    """`ix_apid` is (a, pid). InnoDB needs the referencing column *leftmost*,
    which this is not - `fk_p` is the index actually holding the key up. So
    this one is free to move out of the way, and excluding every index that
    merely mentions an FK column left that win on the table."""
    from migkit import movers
    hop, eng = _engine(tmp_path)
    before = _idx(DST)
    with movers._MyIndexWindow(eng, hop, "d", 2, None) as w:
        assert sorted(k.split(".", 1)[1] for k in w.dropped) == \
            ["ix_a", "ix_apid"], w.dropped
        inside = _idx(DST)
        assert "fk_p" in inside and "PRIMARY" in inside, inside
    assert _idx(DST) == before


def test_the_definitions_reach_disk_first(pair, tmp_path):
    from migkit import indexes as ix
    from migkit import movers
    hop, eng = _engine(tmp_path)
    where = tmp_path / "dropped-indexes.json"
    with movers._MyIndexWindow(eng, hop, "d", 2, None):
        assert where.exists(), "nothing was written before the drop"
        saved = ix.restore_from(where)
        assert all("ADD INDEX" in v for v in saved.values()), saved


def test_the_rebuild_happens_even_when_the_load_raises(pair, tmp_path):
    from migkit import movers
    hop, eng = _engine(tmp_path)
    before = _idx(DST)
    with pytest.raises(RuntimeError, match="load died"):
        with movers._MyIndexWindow(eng, hop, "d", 2, None):
            raise RuntimeError("load died")
    assert _idx(DST) == before


def test_nothing_is_dropped_when_the_file_cannot_be_written(pair, tmp_path):
    from migkit import movers
    hop, eng = _engine(tmp_path)
    blocked = tmp_path / "blocked"
    blocked.write_text("not a directory")
    hop.report_dir = lambda db=None: blocked / "nested"
    before = _idx(DST)
    said = []
    with movers._MyIndexWindow(eng, hop, "d", 2, said.append):
        assert _idx(DST) == before, "dropped without a saved file"
    assert any("could not save" in s for s in said), said

"""OceanBase as a side of a pair (backlog P0-CLOUDS), MySQL mode, against
the community edition's single-node image (`oceanbase/oceanbase-ce`,
`MODE=mini`, its tenant `test`), which takes about six gigabytes: run
through `tools/with_docker_lock.py --vm migkit`.

A database copied to another in the tenant and compared, its digest
worked out by OceanBase's SQL once proved on the table's first rows, a
changed row a difference; its follow asked for and refused with where
OceanBase's binlog is - the image runs no binlog service. The Oracle mode
is the enterprise edition's, which no image here has: it is proved only
on a real tenant.
"""
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

PROVES = [
    ("move.table-copy", "oceanbase"),
    ("schema.create", "oceanbase"),
]

pytestmark = [needs_docker, pytest.mark.vm]

NAME, PORT = "migkit-test-oceanbase", 16455
IMAGE = "oceanbase/oceanbase-ce:latest"


def _conn(db=None):
    import pymysql
    return pymysql.connect(host="127.0.0.1", port=PORT, user="root@test",
                           password="", database=db, autocommit=True,
                           connect_timeout=10)


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-e", "MODE=mini",
                    "-p", f"127.0.0.1:{PORT}:2881", IMAGE], check=True,
                   capture_output=True)
    try:
        end = time.time() + 900
        ready = False
        while time.time() < end and not ready:
            try:
                _conn().close()
                ready = True
            except Exception:
                time.sleep(5)
        assert ready
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", NAME],
                       capture_output=True)


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


@pytest.fixture
def shop(server):
    c = _conn()
    with c.cursor() as cur:
        for db in ("shop", "shop_copy"):
            cur.execute(f"drop database if exists {db}")
            cur.execute(f"create database {db}")
        cur.execute("create table shop.items (id int primary key, name"
                    " varchar(40), price decimal(10,2), at datetime(6),"
                    " raw varbinary(16))")
        cur.executemany("insert into shop.items values (%s, %s, %s, %s, %s)",
                        [(i, f"n{i}", f"{i}.25",
                          f"2026-01-0{i % 9 + 1} 10:00:00.{i:06d}",
                          bytes([i, 255])) for i in range(1, 31)])
    c.close()


def _engine(tmp_path):
    from migkit.engines.oceanbase import OceanBaseEngine

    def ep():
        return Endpoint(host="127.0.0.1", port=PORT, user="root@test",
                        password="")
    hop = Hop(name="ob", engine="oceanbase", source=ep(), target=ep(),
              databases=["shop"], db_map={"shop": "shop_copy"})
    hop.report_dir = lambda db=None: tmp_path
    return OceanBaseEngine(hop)


def _data(eng):
    return {r.scope: r.status for r in eng.check_data("shop")
            if r.check == "data"}


def test_it_says_it_is_oceanbase_and_a_database_is_copied_and_compared(
        shop, tmp_path):
    from migkit.cli import _Checkpoint
    eng = _engine(tmp_path)
    assert "oceanbase" in (eng.server_identity("src", "shop")["version"]
                           + str(eng._q("src", "select @@version_comment")
                                 )).lower()
    ck = _Checkpoint(tmp_path / "move.json")
    eng._as_pair().create_missing("shop", lambda m: None)
    for sch, t in eng.list_move_tables("shop"):
        eng.move_table("shop", sch, t, 10, ck, lambda m: None)
    got = _data(eng)
    assert got and set(got.values()) == {"ok"}, got
    c = _conn()
    with c.cursor() as cur:
        cur.execute("update shop_copy.items set at = '2026-01-01"
                    " 10:00:00.000001' where id = 9")
    c.close()
    got = _data(_engine(tmp_path))
    assert "diff" in got.values(), got


def test_its_digest_is_proved_on_its_own_rows_then_worked_out_there(
        shop, tmp_path):
    from migkit import canon
    eng = _engine(tmp_path)
    cols = [("id", "integer"), ("name", "text"), ("price", "decimal"),
            ("at", "timestamp"), ("raw", "bytes")]
    server = eng.neutral_digest("src", "shop", "items", cols)
    assert eng.folded_last is False, "OceanBase's rendering was not proved"
    here = canon.fold_batches(cols, eng.neutral_batches(
        "src", "shop", "items", cols, 1000))
    assert server == here and server[0] == 30


def test_its_follow_says_where_its_binlog_is(shop, tmp_path):
    with pytest.raises(SystemExit, match="binlog service"):
        _engine(tmp_path).change_point("src", "shop")

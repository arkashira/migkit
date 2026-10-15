"""On a MySQL source and its native replica: equal tables proved by both
readings, a row changed on the replica alone found by both (DEC.ptcs).
Needs the shared test servers and the program; written, not yet run."""
import pytest

from migkit.util import which
from tests import servers

pytestmark = pytest.mark.skipif(not which("pt-table-checksum"),
                                reason="the replica checksum is not here")

pair = servers.fixture("mysql-gtid")


def _replicate(pair):
    src, dst = pair.src, pair.dst
    src.sql(f"create table `{pair.name}`.t (id int primary key, v text)")
    src.sql(f"insert into `{pair.name}`.t select n, concat('v', n) from"
            " (with recursive s(n) as (select 1 union all select n + 1"
            " from s where n < 5000) select n from s) x")
    dst.sql("stop replica")
    dst.sql("reset replica all")
    dst.sql("change replication source to source_host ="
            f" '{servers.ip(src.container)}', source_port = 3306,"
            " source_user = 'root',"
            f" source_password = '{servers.PASSWORD}',"
            " source_auto_position = 1, get_source_public_key = 1")
    dst.sql("start replica")


def test_both_readings_prove_a_replica_and_find_a_change(pair):
    from migkit.config import Hop
    from migkit.engines.mysql import MySQLEngine
    _replicate(pair)
    hop = Hop(name="r", engine="mysql", source=pair.src.endpoint(),
              target=pair.dst.endpoint(), databases=[pair.name])
    eng = MySQLEngine(hop)
    got = eng._replica_checksum(pair.name)
    assert got is not None and got.status == "ok", got
    pair.dst.sql("set sql_log_bin = 0")
    pair.dst.sql(f"update `{pair.name}`.t set v = 'x' where id = 42")
    got = eng._replica_checksum(pair.name)
    assert got.status == "diff" and "both readings differ" in got.detail

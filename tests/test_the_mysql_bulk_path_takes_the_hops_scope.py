"""The MySQL bulk path takes exactly what the hop takes (backlog
P0-SCOPE.6d), against MySQL.

The step that empties the target, and the windows that take its indexes and
triggers off for the load, reached "every target table the hop does not
exclude": a table only the target has - never named in `exclude`, since the
source has none to leave out - was emptied, and nothing refilled it. A
table under a row filter was emptied whole, though the dump carries only
the rows the filter selects. A renamed table was made, emptied and looked
for under the source's name. And a table made for the load kept its
foreign key into a table the target is not given.
"""
import shutil
import socket
import subprocess
import time

import pytest

from migkit import movers
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.names", "mysql")]

pytestmark = needs_docker

SRC, DST = "migkit-test-myscope-src", "migkit-test-myscope-dst"
SRC_PORT, DST_PORT = 13493, 13494

have_tools = pytest.mark.skipif(
    not (shutil.which("mydumper", path=movers.tool_env()["PATH"])
         and shutil.which("myloader", path=movers.tool_env()["PATH"])),
    reason="the MySQL bulk programs are not installed")


def _sql(name, sql):
    got = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    names = (SRC, DST)
    try:
        for n, p in zip(names, (SRC_PORT, DST_PORT)):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                            "MYSQL_ROOT_PASSWORD=test", "-p", f"{p}:3306",
                            "mysql:8"], check=True, capture_output=True)
        for n, p in zip(names, (SRC_PORT, DST_PORT)):
            end = time.time() + 180
            while time.time() < end:
                ok = subprocess.run(["docker", "exec", n, "mysql", "-uroot",
                                     "-ptest", "-h127.0.0.1",
                                     "--protocol=tcp", "-e", "select 1"],
                                    capture_output=True).returncode == 0
                with socket.socket() as s:
                    s.settimeout(2)
                    if ok and s.connect_ex(("127.0.0.1", p)) == 0:
                        break
                time.sleep(2)
            else:
                pytest.fail(f"{n} never answered")
        yield
    finally:
        for n in names:
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def _hop(tmp_path, **kw):
    hop = Hop(name="mys", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              databases=["appdb"], workers=2, **kw)
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return hop


def _fresh(src="", dst=""):
    for name, more in ((SRC, src), (DST, dst)):
        _sql(name, "drop database if exists appdb; create database appdb;"
                   f" use appdb; {more}")


def _ids(table):
    return _sql(DST, f"select group_concat(id order by id) from"
                     f" appdb.{table}")


ORDERS = "create table orders (id int primary key, v varchar(20));"


def test_a_table_only_the_target_has_is_not_emptied(pair, tmp_path):
    _fresh(ORDERS + " insert into orders values (1, 's');",
           ORDERS + " insert into orders values (9, 'stale');"
           " create table notes (id int primary key);"
           " insert into notes values (1), (2);")
    lines = []
    assert movers._my_truncate_target(_hop(tmp_path), "appdb",
                                      lines.append) == ["orders"]
    assert _ids("notes") == "1,2", "the target's own table was emptied"
    assert _ids("orders") == "NULL", "the carried table was not emptied"
    assert any("tables only the target has are not emptied" in str(ln)
               for ln in lines), lines


def test_a_filtered_table_keeps_the_rows_outside_its_filter(pair,
                                                            tmp_path):
    _fresh(ORDERS + " insert into orders values (1, 's'), (2, 's'),"
                    " (3, 's');",
           ORDERS + " insert into orders values (1, 'own'), (2, 'old'),"
                    " (99, 'old');")
    hop = _hop(tmp_path, mapping={"where": {"orders": "id > 1"}})
    movers._my_truncate_target(hop, "appdb")
    assert _ids("orders") == "1", "rows outside the filter were emptied"


@have_tools
def test_a_filtered_move_lands_beside_the_targets_own_rows(pair, tmp_path):
    _fresh(ORDERS + " insert into orders values (1, 's'), (2, 's'),"
                    " (3, 's');",
           ORDERS + " insert into orders values (1, 'own'), (2, 'old');")
    hop = _hop(tmp_path, mapping={"where": {"orders": "id > 1"}})
    movers.mydumper_move(hop, "appdb", 2, True, None)
    assert _sql(DST, "select group_concat(concat(id, v) order by id) from"
                     " appdb.orders") == "1own,2s,3s"


def test_a_table_kept_by_its_contract_is_not_emptied_over_its_rows(
        pair, tmp_path):
    """`mapping.ops` without delete: the target's rows of the table are the
    ones the source deleted, kept; nothing is emptied over them."""
    _fresh(ORDERS + " insert into orders values (1, 's');"
                    " create table people (id int primary key);",
           ORDERS + " insert into orders values (9, 'kept');"
                    " create table people (id int primary key);"
                    " insert into people values (1);")
    hop = _hop(tmp_path, mapping={"ops": {"orders": "insert, update"}})
    with pytest.raises(SystemExit) as e:
        movers._my_truncate_target(hop, "appdb")
    assert "orders" in str(e.value) and "mapping.ops" in str(e.value)
    assert _ids("orders") == "9" and _ids("people") == "1"
    _sql(DST, "delete from appdb.orders")
    assert movers._my_truncate_target(hop, "appdb") == ["orders", "people"]


def test_the_windows_leave_a_table_only_the_target_has(pair, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    _fresh(ORDERS + " create index orders_v on orders (v);",
           ORDERS + " create index orders_v on orders (v);"
           " create table notes (id int primary key, n varchar(20),"
           " index notes_n (n));"
           " create trigger notes_t before insert on notes for each row"
           " set new.n = upper(new.n);")
    hop = _hop(tmp_path)
    lines = []
    with movers._MyIndexWindow(MySQLEngine(hop), hop, "appdb", 2,
                               lines.append) as w, \
            movers._MyTriggerWindow(hop, "appdb", lines.append) as t:
        assert _sql(DST, "select count(*) from information_schema.statistics"
                         " where table_schema = 'appdb'"
                         " and index_name = 'notes_n'") == "1"
        assert _sql(DST, "select count(*) from information_schema.triggers"
                         " where trigger_schema = 'appdb'") == "1"
    assert w.dropped == ["orders.orders_v"], w.dropped
    assert t.dropped == [], t.dropped
    assert any("tables the move does not fill were left in place" in str(ln)
               for ln in lines), lines


def test_a_key_into_a_table_the_target_is_not_given_is_not_made(pair,
                                                                tmp_path):
    _fresh("create table customers (id int primary key);"
           " create table orders (id int primary key, cid int,"
           " constraint orders_c foreign key (cid) references"
           " customers (id));")
    lines = []
    made = movers._my_create_missing(_hop(tmp_path, exclude=["customers"]),
                                     "appdb", lines.append)
    assert made == ["orders"], made
    assert _sql(DST, "select count(*) from information_schema"
                     ".referential_constraints where constraint_schema ="
                     " 'appdb'") == "0"
    said = "\n".join(str(ln) for ln in lines)
    assert "orders_c on orders -> customers" in said, said


def test_a_renamed_table_is_made_and_emptied_under_its_new_name(pair,
                                                                tmp_path):
    """Made by the pair's copier, which renames it; emptied before the load
    under the name the copier fills; the table under the old name is the
    target's own."""
    _fresh(ORDERS + " insert into orders values (1, 's');")
    hop = _hop(tmp_path, mapping={"tables": {"orders": "orders_v2"}})
    assert movers._my_missing_tables(_hop(tmp_path), "appdb")[0] == \
        ["orders"]
    assert movers._my_missing_tables(hop, "appdb")[0] == [], \
        "made here, it stood under the source's name"
    _sql(DST, "use appdb; " + ORDERS + " insert into orders values (9,"
              " 'own'); create table orders_v2 (id int primary key,"
              " v varchar(20)); insert into orders_v2 values (5, 'stale');")
    assert movers._my_truncate_target(hop, "appdb") == ["orders_v2"]
    assert _ids("orders") == "9"
    assert _ids("orders_v2") == "NULL"


def test_a_renamed_table_is_looked_for_under_its_new_name(pair, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    _fresh(ORDERS + " insert into orders values (1, 's');",
           "create table orders_v2 (id int primary key, v varchar(20));"
           " insert into orders_v2 values (1, 's');")
    eng = MySQLEngine(_hop(tmp_path,
                           mapping={"tables": {"orders": "orders_v2"}}))
    assert eng.moved_nothing("appdb") == []
    assert MySQLEngine(_hop(tmp_path)).moved_nothing("appdb") == ["orders"]

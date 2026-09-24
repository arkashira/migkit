"""The table copier leaves the target equal to the source, strays included.

The keyed copiers replace one key range per chunk, from the source's
lowest key to its highest. A target row whose key lies outside that range
was in no chunk, so it stayed: a target still carrying an earlier attempt
kept its strays, and an empty source table left the target's rows alone
altogether. Rows the hop's row filter excludes are not this copy's to
remove, and stay.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


class _Checkpoint(dict):
    def save(self):
        pass


def _pg_engine(pg_pair, tmp_path, where=None):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="c", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"],
              mapping={"where": {"orders": where}} if where else {})
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return PostgresEngine(hop)


def _pg(port, sql):
    got = psql(port, sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _pg_ids(port):
    return _pg(port, "select coalesce(string_agg(id::text, ','"
                     " order by id), '') from public.orders")


def _pg_seed(pg_pair, source_rows):
    for port in (pg_pair["src"], pg_pair["dst"]):
        _pg(port, "drop table if exists public.orders;"
                   " create table public.orders (id int primary key,"
                   " region text)")
    if source_rows:
        _pg(pg_pair["src"], f"insert into public.orders values {source_rows}")
    _pg(pg_pair["dst"], "insert into public.orders values (0, 'emea'),"
                         " (2, 'apac'), (90, 'apac'), (91, 'emea')")


def test_pg_strays_beyond_the_source_range_are_removed(pg_pair, tmp_path):
    _pg_seed(pg_pair, "(1, 'apac'), (2, 'apac'), (3, 'emea')")
    lines = []
    _pg_engine(pg_pair, tmp_path).move_table(
        "postgres", "public", "orders", 1000, _Checkpoint(), lines.append)
    assert _pg_ids(pg_pair["dst"]) == "1,2,3", lines
    assert any("removed 3 target rows" in ln for ln in lines), lines


def test_pg_an_empty_source_table_empties_the_target_table(pg_pair,
                                                           tmp_path):
    _pg_seed(pg_pair, "")
    _pg_engine(pg_pair, tmp_path).move_table(
        "postgres", "public", "orders", 1000, _Checkpoint(), lambda m: None)
    assert _pg_ids(pg_pair["dst"]) == ""


def test_pg_rows_outside_the_row_filter_stay(pg_pair, tmp_path):
    """`region = 'apac'`: the copy owns only apac rows on the target, so
    the emea strays are left for the check to report on their own line."""
    _pg_seed(pg_pair, "(1, 'apac'), (2, 'apac'), (3, 'emea')")
    _pg_engine(pg_pair, tmp_path, "region = 'apac'").move_table(
        "postgres", "public", "orders", 1000, _Checkpoint(), lambda m: None)
    assert _pg_ids(pg_pair["dst"]) == "0,1,2,91"


SRC, DST = "migkit-test-strays-src", "migkit-test-strays-dst"
SRC_PORT, DST_PORT = 15657, 15658


def _my(name, sql):
    got = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def my_pair():
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


def _my_seed(source_rows):
    schema = ("drop database if exists appdb; create database appdb;"
              " use appdb; create table orders (id int primary key,"
              " region varchar(10));")
    _my(SRC, schema + (f" insert into orders values {source_rows};"
                       if source_rows else ""))
    _my(DST, schema + " insert into orders values (0, 'emea'), (2, 'apac'),"
                      " (90, 'apac'), (91, 'emea');")


def _my_ids():
    return _my(DST, "select coalesce(group_concat(id order by id), '')"
                    " from appdb.orders")


def _my_engine(tmp_path, where=None):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="c", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              databases=["appdb"],
              mapping={"where": {"orders": where}} if where else {})
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return MySQLEngine(hop)


def test_mysql_strays_beyond_the_source_range_are_removed(my_pair, tmp_path):
    _my_seed("(1, 'apac'), (2, 'apac'), (3, 'emea')")
    lines = []
    _my_engine(tmp_path).move_table("appdb", "", "orders", 1000,
                                    _Checkpoint(), lines.append)
    assert _my_ids() == "1,2,3", lines
    assert any("removed 3 target rows" in ln for ln in lines), lines


def test_mysql_an_empty_source_table_empties_the_target_table(my_pair,
                                                              tmp_path):
    _my_seed("")
    _my_engine(tmp_path).move_table("appdb", "", "orders", 1000,
                                    _Checkpoint(), lambda m: None)
    assert _my_ids() == ""


def test_mysql_rows_outside_the_row_filter_stay(my_pair, tmp_path):
    _my_seed("(1, 'apac'), (2, 'apac'), (3, 'emea')")
    _my_engine(tmp_path, "region = 'apac'").move_table(
        "appdb", "", "orders", 1000, _Checkpoint(), lambda m: None)
    assert _my_ids() == "0,1,2,91"

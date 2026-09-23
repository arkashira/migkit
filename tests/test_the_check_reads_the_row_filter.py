"""A row filter the move applies has to be the filter the check applies.

`mapping.where` narrows what moves. `Hop.row_filter()` says it is "pushed
into the mover's own flag and into the checksum's WHERE" - and it had no
caller anywhere in the package. The MySQL mover applied it; no check did.

So a filtered move landed exactly the rows it was asked to, and the check
then compared that against the whole source table. It reported the rows the
filter left behind as missing, for as long as anyone ran it: the move looked
right and the verification never went green.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

APAC = "region = 'apac'"


# ---- PostgreSQL, on the shared pair ----

def _pg_hop(pg_pair, where=None):
    return Hop(name="f", engine="postgres",
               source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                               user="postgres", password="test"),
               target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                               user="postgres", password="test"),
               databases=["postgres"],
               mapping={"where": {"orders": where}} if where else {})


def _pg_seed(pg_pair):
    for port in (pg_pair["src"], pg_pair["dst"]):
        assert psql(port, "create table orders (id int primary key,"
                    " region text, v text)").returncode == 0
    psql(pg_pair["src"], "insert into orders values (1,'apac','a'),"
                         "(2,'emea','b'),(3,'apac','c'),(4,'emea','d')")
    # what a filtered move leaves: only the rows the filter selects
    psql(pg_pair["dst"], "insert into orders values (1,'apac','a'),"
                         "(3,'apac','c')")


def _pg_engine(hop, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return PostgresEngine(hop)


def test_pg_the_premise_without_a_filter_the_target_is_short(pg_pair,
                                                              tmp_path):
    """Without the mapping the same pair really is two rows short - so a
    green verdict below comes from reading the filter, not from a check
    that cannot see a difference."""
    _pg_seed(pg_pair)
    eng = _pg_engine(_pg_hop(pg_pair), tmp_path)
    got = [r for r in eng.check_counts("postgres") if r.status == "diff"]
    assert got, "the pair was supposed to differ without the filter"


def test_pg_counts_read_the_filter(pg_pair, tmp_path):
    _pg_seed(pg_pair)
    eng = _pg_engine(_pg_hop(pg_pair, APAC), tmp_path)
    # the counts verdict is one per database, and an OK one does not name
    # its tables - so every result is held to it, not only those that say
    # "orders"
    got = eng.check_counts("postgres")
    assert got and all(r.status == "ok" for r in got), \
        [(r.scope, r.status, r.detail) for r in got]


def test_pg_data_reads_the_filter(pg_pair, tmp_path):
    _pg_seed(pg_pair)
    eng = _pg_engine(_pg_hop(pg_pair, APAC), tmp_path)
    # an OK data verdict here is one per database ("1 tables, 2 rows,
    # checksums equal") and names no table, so every result is held to it
    got = eng.check_data("postgres")
    assert got and all(r.status == "ok" for r in got), \
        [(r.scope, r.status, r.detail) for r in got]
    assert "2 rows" in got[0].detail, got[0].detail


def test_pg_a_row_inside_the_filter_that_is_missing_is_still_caught(
        pg_pair, tmp_path):
    """The filter narrows what is compared; it must not hide a real loss
    inside the part that is compared."""
    _pg_seed(pg_pair)
    psql(pg_pair["dst"], "delete from orders where id = 3")
    eng = _pg_engine(_pg_hop(pg_pair, APAC), tmp_path)
    got = eng.check_data("postgres")
    bad = [r for r in got if r.status == "diff"]
    assert bad, [(r.scope, r.status, r.detail) for r in got]
    assert any("orders" in f"{r.scope} {r.detail}" for r in bad), \
        [(r.scope, r.detail) for r in bad]


def test_pg_rows_on_the_target_outside_the_filter_are_said(pg_pair,
                                                           tmp_path):
    """Narrowing the comparison to the filter must not make the rest of the
    target invisible. The move puts no row outside the filter there, so one
    that is there - left by an earlier load, or written since - is said."""
    _pg_seed(pg_pair)
    psql(pg_pair["dst"], "insert into orders values (9,'emea','stray')")
    got = _pg_engine(_pg_hop(pg_pair, APAC), tmp_path).check_counts(
        "postgres")
    bad = [r for r in got if r.status == "diff"]
    assert bad, [(r.scope, r.status, r.detail) for r in got]
    assert "1 rows the hop's row filter excludes" in bad[0].detail, \
        bad[0].detail


# ---- MySQL, on a pair of its own ----

SRC, DST = "migkit-test-rowfilter-src", "migkit-test-rowfilter-dst"
SRC_PORT, DST_PORT = 13491, 13492


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
                                     "-ptest", "-e", "select 1"],
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


def _my_seed():
    schema = ("drop database if exists appdb; create database appdb;"
              " use appdb; create table orders (id int primary key,"
              " region varchar(10), v varchar(10));")
    _my(SRC, schema + " insert into orders values (1,'apac','a'),"
                      "(2,'emea','b'),(3,'apac','c'),(4,'emea','d');")
    _my(DST, schema + " insert into orders values (1,'apac','a'),"
                      "(3,'apac','c');")


def _my_engine(tmp_path, where=None):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="f", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              databases=["appdb"],
              mapping={"where": {"orders": where}} if where else {})
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return MySQLEngine(hop)


def test_mysql_the_premise_without_a_filter_the_target_is_short(my_pair,
                                                                 tmp_path):
    _my_seed()
    got = [r for r in _my_engine(tmp_path).check_counts("appdb")
           if r.status == "diff"]
    assert got, "the pair was supposed to differ without the filter"


def test_mysql_counts_read_the_filter(my_pair, tmp_path):
    _my_seed()
    got = _my_engine(tmp_path, APAC).check_counts("appdb")
    assert got and all(r.status == "ok" for r in got), \
        [(r.scope, r.status, r.detail) for r in got]


def test_mysql_data_reads_the_filter(my_pair, tmp_path):
    _my_seed()
    got = [r for r in _my_engine(tmp_path, APAC).check_data("appdb")
           if "orders" in r.scope]
    assert got and all(r.status == "ok" for r in got), \
        [(r.scope, r.status, r.detail) for r in got]


def test_mysql_a_row_inside_the_filter_that_is_missing_is_still_caught(
        my_pair, tmp_path):
    _my_seed()
    _my(DST, "delete from appdb.orders where id = 3;")
    got = [r for r in _my_engine(tmp_path, APAC).check_data("appdb")
           if "orders" in r.scope]
    assert got and any(r.status == "diff" for r in got), \
        [(r.scope, r.status, r.detail) for r in got]


def test_mysql_rows_on_the_target_outside_the_filter_are_said(my_pair,
                                                              tmp_path):
    _my_seed()
    _my(DST, "insert into appdb.orders values (9,'emea','stray');")
    got = _my_engine(tmp_path, APAC).check_counts("appdb")
    bad = [r for r in got if r.status == "diff"]
    assert bad, [(r.scope, r.status, r.detail) for r in got]
    assert "1 rows the hop's row filter excludes" in " ".join(
        r.detail for r in bad), [r.detail for r in bad]


# ---- the move routes a filtered table instead of refusing the database ----

def test_pg_the_bulk_move_routes_the_filtered_table(pg_pair, tmp_path,
                                                    monkeypatch):
    """The whole path, through the command an operator runs. The bulk copy
    cannot filter rows, so `orders` is left out of it and carried by the
    table copier with the filter applied; `people`, which no rule names,
    goes the fast way. This used to refuse the whole database."""
    from click.testing import CliRunner

    from migkit import cli
    import migkit.config as cfg
    for port in (pg_pair["src"], pg_pair["dst"]):
        assert psql(port, "create table orders (id int primary key,"
                    " region text, v text); create table people"
                    " (id int primary key, n text)").returncode == 0
    psql(pg_pair["src"], "insert into orders values (1,'apac','a'),"
                         "(2,'emea','b'),(3,'apac','c'),(4,'emea','d');"
                         " insert into people values (1,'x'),(2,'y')")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  f:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n    workers: 1\n"
        "    mapping: {where: {orders: \"region = 'apac'\"}}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "pgdump")

    plan = CliRunner().invoke(cli.main, ["move", "f", "--mode", "full"])
    assert plan.exit_code == 0, plan.output
    assert "copied table by table" in plan.output, plan.output
    got = CliRunner().invoke(cli.main, ["move", "f", "--mode", "full",
                                        "--go"])
    assert got.exit_code == 0, got.output
    rows = psql(pg_pair["dst"], "select string_agg(id::text, ',' order by id)"
                " from orders").stdout.strip()
    assert rows == "1,3", rows
    assert psql(pg_pair["dst"], "select count(*) from people"
                ).stdout.strip() == "2"
    # and the check agrees with the move about what should be there
    eng = _pg_engine(_pg_hop(pg_pair, APAC), tmp_path)
    assert all(r.status == "ok" for r in eng.check_counts("postgres"))


def test_mysql_the_table_copier_applies_the_filter(my_pair, tmp_path):
    """The copier every routed table goes through - including a filter with
    `%` in it, which the parameterised statements would otherwise read as a
    placeholder."""
    from migkit.cli import _Checkpoint
    _my_seed()
    _my(DST, "delete from appdb.orders; insert into appdb.orders values"
             " (7,'emea','kept');")
    eng = _my_engine(tmp_path, "region like 'ap%'")
    eng.move_table("appdb", "", "orders", 1, _Checkpoint(tmp_path / "ck"),
                   lambda m: None)
    got = _my(DST, "select group_concat(id order by id) from appdb.orders;")
    assert got == "1,3,7", got

"""A hop between engines, and one copied through the pair, move and compare
only the rows the hop's row filter selects, and its change tail follows the
filter too (backlog 0d).

The pair refused a row filter outright: its copier read whole tables, and
its tail applied every change. Now:
* the copier reads through the filter on the source, and on the target
  replaces only what the filter selects there
* the comparison reads both sides through it. Target rows outside it are
  counted and named, so narrowing the comparison hides nothing
* the tail asks the source which changed rows the filter selects now. An
  insert or update of a row outside it becomes a delete, so a row updated
  out of the filter leaves the target

The filter is SQL written for the source. On the target it is translated
into that engine's SQL, under the column names the hop's mapping gives.
"""
import sqlite3
import subprocess
import threading
import time

import pytest
from click.testing import CliRunner

from tests.conftest import needs_docker, psql


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


@pytest.fixture
def lite(tmp_path, monkeypatch):
    import migkit.config as cfg
    con = sqlite3.connect(tmp_path / "a.db")
    con.executescript(
        "create table orders (id integer primary key, region text, v text);"
        " insert into orders values (1, 'apac', 'x'), (2, 'emea', 'y'),"
        " (3, 'apac', 'z');")
    con.commit()
    con.close()
    con = sqlite3.connect(tmp_path / "b.db")
    con.executescript(
        "create table orders (id integer primary key, region text, v text);"
        # the target's own row, outside the filter: not the move's
        " insert into orders values (9, 'emea', 'theirs');")
    con.commit()
    con.close()
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 'a.db'}, user: x, password: x}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}, user: x, password: x}}\n"
        "    databases: [main]\n"
        "    mapping:\n      where:\n        orders: \"region like 'ap%'\"\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return tmp_path


def _rows(path):
    con = sqlite3.connect(path)
    try:
        return con.execute("select id, region, v from orders order by id"
                           ).fetchall()
    finally:
        con.close()


def test_the_copy_and_the_check_read_through_the_filter(lite):
    got, said = _run("move", "lite", "--go")
    assert got.exit_code == 0, said
    assert _rows(lite / "b.db") == [(1, "apac", "x"), (3, "apac", "z"),
                                    (9, "emea", "theirs")]
    got, said = _run("check", "lite", "--only", "data")
    assert got.exit_code == 0, said
    assert "1 rows on the target lie outside the hop's row filter" in said, \
        said
    # a difference inside the filter is still one
    con = sqlite3.connect(lite / "b.db")
    con.execute("update orders set v = 'changed' where id = 3")
    con.commit()
    con.close()
    got, said = _run("check", "lite", "--only", "data")
    assert got.exit_code != 0 and "orders" in said, said


MY, MY_PORT = "migkit-test-pairfilter-my", 15761


def my(sql):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def mysql_server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                        "mysql:8.4", "--binlog-row-metadata=FULL"],
                       check=True, capture_output=True)
        for _ in range(90):
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "select 1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail("mysql never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _dst(pg_pair, sql):
    got = psql(pg_pair["dst"], sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@needs_docker
@pytest.mark.parametrize("renamed", [True, False])
def test_mysql_to_postgres_copies_checks_and_tails_through_it(
        mysql_server, pg_pair, tmp_path, monkeypatch, renamed):
    """Renamed, the table goes through the copier that reads the mapping;
    not, through the one written for MySQL to PostgreSQL. Both read the
    filter."""
    import migkit.config as cfg
    col = "area" if renamed else "region"
    my("drop database if exists cx; create database cx;"
       " create table cx.orders (id int primary key, region varchar(10),"
       "  v text);"
       " insert into cx.orders values (1, 'apac', 'x'), (2, 'emea', 'y'),"
       " (3, 'apac', 'z')")
    psql(pg_pair["dst"], "drop table if exists public.orders;"
                         " create table public.orders (id int primary key,"
                         f" {col} varchar(10), v text);"
                         " insert into public.orders values (9, 'emea',"
                         " 'theirs')")
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  mp:\n    engine: hetero\n"
        f"    source: {{host: 127.0.0.1, port: {MY_PORT}, user: root,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [cx]\n    db_map: {cx: postgres}\n"
        "    options: {source_engine: mysql, target_engine: postgres}\n"
        "    mapping:\n      where:\n        orders: \"region like 'ap%'\"\n"
        + ("      columns:\n        orders: {rename: {region: area}}\n"
           if renamed else ""))
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")
    try:
        got, said = _run("move", "mp", "--go")
        assert got.exit_code == 0, said
        assert _dst(pg_pair, f"select string_agg(id || '=' || {col}, ','"
                             " order by id) from public.orders") == \
            "1=apac,3=apac,9=emea", said
        got, said = _run("check", "mp", "--only", "data")
        assert got.exit_code == 0, said
        # the tail: what the filter selects arrives, what it leaves out
        # does not, and a row updated out of it leaves
        from migkit.config import get_hop
        from migkit.engines import get_engine
        eng = get_engine(get_hop("mp"))
        lines, done = [], threading.Event()

        def run():
            try:
                eng.tail_apply("cx", True, tmp_path / "tok.json",
                               lines.append)
            except BaseException as e:
                lines.append(repr(e))
            finally:
                done.set()
        import ctypes
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        time.sleep(4)
        my("insert into cx.orders values (4, 'apac', 'new'),"
           " (5, 'emea', 'left out');"
           " update cx.orders set region = 'emea' where id = 1")
        time.sleep(6)
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(thread.ident),
            ctypes.py_object(KeyboardInterrupt))
        assert done.wait(timeout=30)
        assert _dst(pg_pair, f"select string_agg(id || '=' || {col}, ','"
                             " order by id) from public.orders") == \
            "3=apac,4=apac,9=emea", lines
    finally:
        psql(pg_pair["dst"], "drop table if exists public.orders")

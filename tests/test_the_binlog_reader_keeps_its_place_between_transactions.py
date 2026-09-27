"""The MySQL change reader keeps its place only where a transaction ended.

Measured before: one insert of 20,000 rows, read 1,000 at a time. The
reader kept its position after whichever rows event the limit fell on;
read from there, the rest of that transaction's rows came before the table
map that describes them, and the reader dropped them and went on to the
end of the log. 1,281 changes came back and the position was past all of
them - 18,719 rows gone from the tail, and nothing said. A change-only
verify (`delta_verify`) stopped at its limit the same way, and the rows of
the rest of the transaction were never compared.

The position is now where a transaction ended, with the number of its
rows already handed back (as Debezium keeps its own), and the stream is
held open while the caller comes back with the position it was given.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = pytest.mark.docker

MY, PORT = "migkit-test-binlog-place", 15877


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


if not _docker():
    pytest.skip("docker not available", allow_module_level=True)


def my(sql):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{PORT}:3306",
                    "mysql:8.4", "--server-id=7",
                    "--binlog-row-metadata=FULL"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            ok = subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                                 "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                 "-e", "select 1"],
                                capture_output=True).returncode == 0
            with socket.socket() as s:
                s.settimeout(2)
                if ok and s.connect_ex(("127.0.0.1", PORT)) == 0:
                    break
            time.sleep(2)
        else:
            pytest.fail("MySQL never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


@pytest.fixture
def tables(server):
    my("drop database if exists appdb; create database appdb;"
       " create table appdb.a (id int primary key, v varchar(20));"
       " create table appdb.b (id int primary key, v varchar(20));"
       " create table appdb.own (id int primary key, v varchar(20))")


def _eng(**kw):
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    return MySQLEngine(Hop(name="bp", engine="mysql", source=ep, target=ep,
                           databases=["appdb"], **kw))


def _read_all(token, limit, fresh):
    """Every change after `token`, `limit` at a time - on one engine, or a
    new one each time, as a tail started again would be."""
    eng, got, calls = _eng(exclude=["appdb.own"]), [], 0
    while True:
        if fresh:
            eng = _eng(exclude=["appdb.own"])
        batch, after = eng.neutral_changes("src", "appdb", token,
                                           limit=limit)
        calls += 1
        got += [(c["table"], c["op"], c["key"]["id"]) for c in batch]
        if not batch and after == token:
            return got, calls
        token = after
        assert calls < 500, "the reader never reached the end"


@pytest.mark.parametrize("fresh", [False, True],
                         ids=["held open", "started again each time"])
def test_a_transaction_larger_than_a_read_arrives_whole(tables, fresh):
    token = _eng().change_point("src", "appdb")
    my("set session cte_max_recursion_depth = 100000; insert into appdb.a"
       " with recursive g(n) as (select 1 union all select n + 1 from g"
       " where n < 20000) select n, concat('v', n) from g")
    got, calls = _read_all(token, 1000, fresh)
    assert [i for _, _, i in got] == list(range(1, 20_001)), (
        len(got), calls)
    assert calls > 10, calls


@pytest.mark.parametrize("fresh", [False, True],
                         ids=["held open", "started again each time"])
def test_tables_interleaved_in_transactions_arrive_once_in_order(
        tables, fresh):
    token = _eng().change_point("src", "appdb")
    body = []
    for t in range(30):
        stmts = [f"insert into appdb.{'ab'[i % 2]} values"
                 + ", ".join(f"({t * 1000 + i * 50 + k}, 'x')"
                             for k in range(50)) for i in range(6)]
        stmts.append(f"insert into appdb.own values ({t}, 'target''s')")
        stmts.append(f"update appdb.a set v = 'u' where id < {t * 1000 + 25}"
                     f" and id >= {t * 1000}")
        body.append("start transaction; " + "; ".join(stmts) + "; commit;")
    my(" ".join(body))
    whole, _ = _read_all(token, 10 ** 9, False)
    got, calls = _read_all(token, 37, fresh)
    assert got == whole and calls > 30, (len(got), len(whole), calls)
    assert not any(t == "own" for t, _, _ in got)
    assert len(whole) == 30 * (300 + 25)


def test_a_compressed_transaction_larger_than_a_read_arrives_whole(tables):
    token = _eng().change_point("src", "appdb")
    my("set session binlog_transaction_compression = ON; set session"
       " cte_max_recursion_depth = 100000; insert into appdb.b with"
       " recursive g(n) as (select 1 union all select n + 1 from g where"
       " n < 5000) select n, 'packed' from g")
    got, _ = _read_all(token, 700, True)
    assert [i for _, _, i in got] == list(range(1, 5001)), len(got)


def test_a_change_only_verify_reads_a_transaction_to_its_end(tables,
                                                             tmp_path):
    eng = _eng()
    eng.hop.report_dir = lambda db=None: tmp_path
    eng.hop.db_map = {"appdb": "appdb_copy"}
    my("create database appdb_copy; create table appdb_copy.a (id int"
       " primary key, v varchar(20))")
    eng.delta_verify("appdb")
    my("set session cte_max_recursion_depth = 100000; insert into appdb.a"
       " with recursive g(n) as (select 1 union all select n + 1 from g"
       " where n < 3000) select n, 'v' from g; insert into appdb_copy.a"
       " select * from appdb.a where id <> 2999")
    got = {r.scope: r for r in eng.delta_verify("appdb", limit=500)}
    # the row the target lacks is near the end of the one transaction
    assert got["appdb.a"].status == "diff", [
        (r.scope, r.status, r.detail) for r in got.values()]
    assert "of 3000 touched rows: missing=1" in got["appdb.a"].detail

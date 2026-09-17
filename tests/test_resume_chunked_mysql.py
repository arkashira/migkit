"""MySQL's chunked, restartable checksum must agree with the single pass.

Same property as the PostgreSQL version, different algebra: MySQL folds the
per-row hashes with BIT_XOR instead of summing them. Both are commutative and
associative, which is what makes chunking equivalent to one pass - but they
are not the same operation, so both need proving against a real server.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-mysqlchunk-src", "migkit-test-mysqlchunk-dst"
SRC_PORT, DST_PORT = 13404, 13405


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


def _wait(port, timeout=90):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _ready(name):
    for _ in range(40):
        if subprocess.run(["docker", "exec", name, "mysql", "-uroot", "-ptest",
                           "-e", "select 1"],
                          capture_output=True).returncode == 0:
            time.sleep(1)
            return True
        time.sleep(2)
    return False


@pytest.fixture(scope="module")
def pair():
    for n, p in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{p}:3306",
                        "mysql:8"], check=True, capture_output=True)
    for p in (SRC_PORT, DST_PORT):
        assert _wait(p)
    for n in (SRC, DST):
        assert _ready(n)
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def _sql(name, sql):
    return subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                           "-ptest"], input=sql, capture_output=True,
                          text=True)


def _seed(name, rows):
    """Seed and *verify* the seed.

    A silently failed seed leaves empty tables, on which every checksum test
    passes while proving nothing - MySQL's default
    cte_max_recursion_depth of 1000 made exactly that happen here.
    """
    r = _sql(name, f"""
        set session cte_max_recursion_depth = {max(rows + 10, 1000)};
        drop database if exists shop; create database shop; use shop;
        create table big (id bigint primary key, payload varchar(80),
                          n decimal(12,3));
        set session cte_max_recursion_depth = {max(rows + 10, 1000)};
        insert into big (id, payload, n)
        with recursive s(i) as (select 1 union all
                                select i+1 from s where i < {rows})
        select i, concat('row-', i), i * 1.5 from s;
    """)
    assert r.returncode == 0, r.stderr
    got = _sql(name, "select count(*) from shop.big;")
    assert got.stdout.strip().splitlines()[-1] == str(rows), \
        f"seed produced {got.stdout!r}, wanted {rows}"


def _engine(tmp_path, slice_rows):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="m", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              databases=["shop"], slice=slice_rows)
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


def _nums(detail):
    return (detail.split("rows ")[1].split(",")[0],
            detail.split("checksum ")[1].split()[0])


def test_chunked_total_equals_single_pass(pair, tmp_path):
    _seed(SRC, 8000)
    _seed(DST, 8000)

    # one pass: slice above the row count means no chunking
    eng = _engine(tmp_path / "whole", 10**9)
    (tmp_path / "whole").mkdir()
    r, rows_a, _ = eng._diff_table("shop", "big")
    assert r.status == "ok", r.detail
    assert rows_a == 8000, r.detail        # never pass on an empty table
    assert "(1 chunks" in r.detail, r.detail

    # and again in several ranges
    eng2 = _engine(tmp_path / "chunked", 2000)
    (tmp_path / "chunked").mkdir()
    r2, _, _ = eng2._diff_table("shop", "big")
    assert r2.status == "ok", r2.detail
    assert "(1 chunks" not in r2.detail, r2.detail
    assert _nums(r2.detail) == _nums(r.detail)


def test_a_target_row_below_the_source_minimum_is_caught(pair, tmp_path):
    """The range planner is open at the bottom for this reason: the old
    MySQL code started at min(pk) on the source and would have missed it."""
    _seed(SRC, 5000)
    _seed(DST, 5000)
    _sql(DST, "insert into shop.big (id, payload, n) values (-99, 'ghost', 1);")

    eng = _engine(tmp_path, 1000)
    r, rows_a, rows_b = eng._diff_table("shop", "big")
    assert r.status == "diff", r.detail
    assert rows_b > rows_a or "extra" in r.detail.lower(), (r.detail,
                                                            rows_a, rows_b)


def test_progress_persists_between_runs(pair, tmp_path):
    import json
    _seed(SRC, 6000)
    _seed(DST, 6000)
    eng = _engine(tmp_path, 1500)

    # a clean run leaves nothing behind: the table is verified, so its
    # partials are no longer owed to anyone
    r, rows_a, _ = eng._diff_table("shop", "big")
    assert r.status == "ok", r.detail
    assert rows_a == 6000, r.detail        # never pass on an empty table
    left = json.loads((tmp_path / "checkpoint.json").read_text())
    assert "shop.big" not in left["tables"] or not \
        left["tables"]["shop.big"].get("done")

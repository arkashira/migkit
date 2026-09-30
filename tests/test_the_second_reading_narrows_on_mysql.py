"""The same narrowing, row naming and second row reading on a MySQL pair
(backlog 0f) - one logic, the base engine's, over MySQL's own digest, row
walk and index statistics.

A text key, so the table is not checked in integer key chunks and its
difference would otherwise be walked whole: 3,000 rows in ten groups of
an indexed `g`, and a hop slice of 500.
"""
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from migkit import second_reader as sr
from migkit.config import Endpoint, Hop
from tests.conftest import verdict
from tests.test_the_report_does_not_name_its_tools import TOOLS

READER = os.environ.get("MIGKIT_SECOND_READER_PYTHON") or str(
    Path(sys.prefix).parent / ".venv-dvt" / "bin" / "python")
NAMES = {"src": ("migkit-test-dvt-my-src", 16040),
         "dst": ("migkit-test-dvt-my-dst", 16041)}
LEAKS = TOOLS + ("dvt", "data-validation", "data_validation")


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(), reason="docker not available"),
              pytest.mark.skipif(not Path(READER).exists(),
                                 reason="the second reader is not installed")]


def _my(side, sql):
    got = subprocess.run(["docker", "exec", "-i", NAMES[side][0], "mysql",
                          "-uroot", "-ptest", "-N"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def my_pair():
    for name, port in NAMES.values():
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p",
                        f"127.0.0.1:{port}:3306", "mysql:8"], check=True,
                       capture_output=True)
    try:
        for side, (name, port) in NAMES.items():
            for _ in range(120):
                ok = subprocess.run(["docker", "exec", name, "mysql",
                                     "-uroot", "-ptest", "-h127.0.0.1",
                                     "--protocol=tcp", "-e", "select 1"],
                                    capture_output=True).returncode == 0
                if ok:
                    break
                time.sleep(2)
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        yield
    finally:
        for name, _ in NAMES.values():
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


@pytest.fixture
def reader(monkeypatch):
    monkeypatch.setenv("MIGKIT_SECOND_READER_PYTHON", READER)


def _seed(*changes):
    for side in ("src", "dst"):
        _my(side, "drop database if exists app; create database app;"
                  " create table app.t (code varchar(20) primary key, g int,"
                  " n bigint, s varchar(40), index (g));"
                  " set cte_max_recursion_depth = 10000;"
                  " insert into app.t with recursive r(i) as (select 1"
                  " union all select i + 1 from r where i < 3000)"
                  " select concat('c', i), i % 10, i * 7, concat('v', i)"
                  " from r;")
    # the source's index statistics, the test's own doing
    _my("src", "analyze table app.t;")
    for sql in changes:
        _my("dst", sql)


def _engine(tmp_path, **options):
    from migkit.engines.mysql import MySQLEngine
    ep = {side: Endpoint(host="127.0.0.1", port=port, user="root",
                         password="test")
          for side, (_, port) in NAMES.items()}
    hop = Hop(name="sr0fmy", engine="mysql", source=ep["src"],
              target=ep["dst"], databases=["app"], slice=500,
              options=options)
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


def _named(tmp_path, kind):
    from migkit import rowtext
    f = tmp_path / f"data-t.{kind}"
    return sorted(rowtext.parse(k)[0] for k in f.read_text().splitlines()
                  if k) if f.exists() else []


@pytest.mark.usefixtures("my_pair", "reader")
def test_the_group_is_found_and_the_rest_proved_equal(tmp_path):
    _seed("update app.t set n = n + 1 where code = 'c13';")
    got = _engine(tmp_path).check_data("app")
    data = verdict(got, "app.t")
    assert data.status == "diff", data.detail
    assert _named(tmp_path, "changed") == ["c13"], data.detail
    assert "narrowed by g to 1 of about 10 groups" in data.detail, data.detail
    assert "the rest proved equal" in data.detail, data.detail
    rows = verdict(got, "app.app.t rows")
    assert rows.status == "diff" and "the same 1 rows differ" in rows.detail
    kept = (tmp_path / sr.BATCH).read_text()
    assert "coalesce(`g` in (3), false)" in kept, kept
    said = " ".join(f"{r.detail} {r.fix_hint}" for r in got).lower()
    assert not [t for t in LEAKS if t in said], said


@pytest.mark.usefixtures("my_pair", "reader")
def test_a_difference_no_group_measure_sees_is_still_named(tmp_path):
    _seed("update app.t set n = n + 1 where code = 'c13';",
          "update app.t set s = 'x25' where code = 'c25';")
    got = _engine(tmp_path).check_data("app")
    data = verdict(got, "app.t")
    assert _named(tmp_path, "changed") == ["c13", "c25"], data.detail
    assert "walked as well" in data.detail, data.detail
    rows = verdict(got, "app.app.t rows")
    assert rows.status == "diff" and "the same 2 rows differ" in rows.detail
    # two rows: few enough to be explained column by column
    assert "t column by column" in data.detail, data.detail
    said = (tmp_path / "data-t.explained").read_text()
    assert "n: 1 rows differ - code=c13: source 91 target 92" in said, said
    assert "s: 1 rows differ - code=c25: source 'v25' target 'x25'" in said


@pytest.mark.usefixtures("my_pair", "reader")
def test_two_readings_of_every_table_where_the_hop_asks(tmp_path):
    _seed()
    eng = _engine(tmp_path, two_readings=True)
    equal = eng.check_second("app")
    assert verdict(equal, "app.app.t").status == "ok"
    assert verdict(equal, "app.app.t rows").status == "ok"
    _my("dst", "update app.t set n = n + 1 where code = 'c13';")
    differ = eng.check_second("app")
    assert verdict(differ, "app.app.t").status == "diff"
    rows = verdict(differ, "app.app.t rows")
    assert rows.status == "diff" and "(by key: c13)" in rows.detail

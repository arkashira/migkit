"""A column the server computes and refuses to be told.

`GENERATED ALWAYS AS (expr) STORED` belongs to the server. A repair carries
a whole row, generated column included, and before this it argued with the
server and lost:

    cannot insert a non-DEFAULT value into column "total"

which meant `migkit apply` could not repair a single row in any table that
had one. MySQL says the same thing in its own words, measured on 8:
`ERROR 3105 (HY000): The value specified for generated column 'total' in
table 't' is not allowed.`

**The fix is not the one the identity column needed**, and that is worth
writing down because the two look alike. `OVERRIDING SYSTEM VALUE` was
tried here and answered the same error. A generated column has to be left
out of the statement entirely, after which the server computes it -
measured, an insert omitting `total` stored `total=20` from `price * qty`.

**The movers were never affected**, which is what kept the change to the
repair path. Both were run for real against a table with a generated
column and both landed all five rows with the totals computed.

Comparing the column is untouched: `neutral_columns` still reports it, so a
target whose expression differs from the source's shows up as a value
difference. Only the writing gives way.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

SHAPE = """
drop table if exists public.gen;
create table public.gen (id int primary key, price numeric, qty int,
                         total numeric generated always as (price * qty)
                         stored);
"""


def _engine(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="gen", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=1)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


@pytest.fixture
def shaped(pg_pair):
    for port in (pg_pair["src"], pg_pair["dst"]):
        got = psql(port, SHAPE)
        assert got.returncode == 0, got.stderr
    psql(pg_pair["src"], "insert into public.gen (id, price, qty)"
                         " select g, g*10, 2 from generate_series(1,5) g;")
    return pg_pair


def _row(pg_pair, key):
    return psql(pg_pair["dst"], "select price||'/'||qty||'/'||total from"
                                f" public.gen where id = {key}").stdout.strip()


def test_a_repair_lands_and_the_server_computes_the_value(shaped, tmp_path):
    """The failure this exists for - and the value migkit was carrying is
    deliberately wrong, so the row proves who computed it."""
    eng = _engine(shaped, tmp_path)
    eng._apply_upsert("dst", "postgres", "public.gen", {"id": 9},
                      {"price": 7, "qty": 3, "total": 999})
    assert _row(shaped, 9) == "7/3/21", _row(shaped, 9)

    # and an update through the same path recomputes rather than sticking
    eng._apply_upsert("dst", "postgres", "public.gen", {"id": 9},
                      {"price": 7, "qty": 4, "total": 999})
    assert _row(shaped, 9) == "7/4/28", _row(shaped, 9)


def test_the_server_still_refuses_the_column(shaped):
    """The control. migkit works around the restriction; it does not remove
    it, and if this stops failing the sandbox changed."""
    refused = psql(shaped["dst"], "insert into public.gen (id, price, qty,"
                                  " total) values (50, 1, 1, 999)")
    assert refused.returncode != 0
    assert "generated column" in refused.stderr, refused.stderr
    # and OVERRIDING SYSTEM VALUE, which fixed the identity case, does not
    # fix this one
    still = psql(shaped["dst"], "insert into public.gen (id, price, qty,"
                                " total) overriding system value"
                                " values (51, 1, 1, 999)")
    assert still.returncode != 0, still.stdout
    assert "generated column" in still.stderr, still.stderr


def test_the_column_is_still_compared(shaped, tmp_path):
    """Dropping it from the write must not drop it from the comparison - a
    target whose expression differs is a real difference."""
    eng = _engine(shaped, tmp_path)
    names = [n for n, _ in eng.neutral_columns("src", "postgres",
                                               "public.gen")]
    assert "total" in names, names
    assert eng._unwritable_columns("dst", "postgres", "public.gen") == {"total"}


def test_an_ordinary_table_is_left_alone(shaped, tmp_path):
    """The control for the fix itself: nothing is dropped from a table that
    has no generated column."""
    psql(shaped["dst"], "drop table if exists public.plain;"
                        " create table public.plain (id int primary key,"
                        " v text);")
    eng = _engine(shaped, tmp_path)
    assert eng._unwritable_columns("dst", "postgres", "public.plain") == set()
    eng._apply_upsert("dst", "postgres", "public.plain", {"id": 1},
                      {"v": "ok"})
    assert psql(shaped["dst"], "select v from public.plain where id=1"
                ).stdout.strip() == "ok"


def test_the_cache_does_not_shadow_the_method(shaped, tmp_path):
    """A regression guard for a bug made writing this: the per-table cache
    lives in `self.__dict__`, and naming it after the method that reads it
    shadowed that method - every call answered `'dict' object is not
    callable`."""
    eng = _engine(shaped, tmp_path)
    eng._unwritable_columns("dst", "postgres", "public.gen")
    assert callable(eng._write_rules), type(eng._write_rules)
    assert callable(eng._unwritable_columns)
    # called twice, it still works and still answers the same thing
    assert eng._unwritable_columns("dst", "postgres", "public.gen") == {"total"}


def test_a_real_move_was_never_affected(shaped, tmp_path, monkeypatch):
    """Why the change is confined to the repair path. Measured with a real
    move rather than assumed from the COPY documentation."""
    from click.testing import CliRunner

    from migkit import cli
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  gen:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {shaped['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {shaped['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n    workers: 1\n")
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "pgdump")

    got = CliRunner().invoke(cli.main, ["move", "gen", "--mode", "full",
                                        "--go"])
    assert got.exit_code == 0, got.output
    landed = psql(shaped["dst"], "select count(*)||' '||sum(total) from"
                                 " public.gen").stdout.strip()
    assert landed == "5 300", landed   # 20+40+60+80+100


def test_engines_without_the_concept_claim_nothing(tmp_path):
    from migkit.engines.base import Engine
    hop = Hop(name="g", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    assert Engine(hop)._unwritable_columns("dst", "x", "t") == set()


# ---- mysql, which refuses the same thing in its own words ---------------

MY = "migkit-test-gen-my"
MY_PORT = 13403


@pytest.fixture(scope="module")
def mysql_one():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8"], check=True, capture_output=True)
    end = time.time() + 180
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", MY_PORT)) == 0:
                break
        time.sleep(1)
    for _ in range(60):
        if subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                           "-e", "select 1"],
                          capture_output=True).returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("mysql never answered")
    yield MY_PORT
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def test_mysql_repairs_around_both_kinds(mysql_one, tmp_path):
    """A VIRTUAL column is not stored at all and is refused just as flatly
    as a STORED one, so both have to be left out."""
    from migkit.engines.mysql import MySQLEngine
    sql = ("drop database if exists g; create database g;"
           " create table g.t (id int primary key, price decimal(10,2),"
           " qty int, total decimal(12,2) as (price*qty) stored,"
           " virt decimal(12,2) as (price*qty));")
    got = subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                          "-e", sql], capture_output=True, text=True)
    assert got.returncode == 0, got.stderr

    refused = subprocess.run(
        ["docker", "exec", MY, "mysql", "-uroot", "-ptest", "-e",
         "insert into g.t (id, price, qty, total) values (1,10,2,999)"],
        capture_output=True, text=True)
    assert refused.returncode != 0
    assert "generated column" in refused.stderr, refused.stderr

    hop = Hop(name="g", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=mysql_one, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=mysql_one, user="root",
                              password="test"),
              databases=["g"])
    hop.report_dir = lambda db=None: tmp_path
    eng = MySQLEngine(hop)
    assert eng._unwritable_columns("dst", "g", "t") == {"total", "virt"}
    eng._apply_upsert("dst", "g", "t", {"id": 5},
                      {"price": 3, "qty": 4, "total": 999, "virt": 999})

    stored = subprocess.run(
        ["docker", "exec", MY, "mysql", "-uroot", "-ptest", "-N", "-B", "-e",
         "select concat(total,'/',virt) from g.t where id=5"],
        capture_output=True, text=True).stdout.strip()
    assert stored == "12.00/12.00", stored

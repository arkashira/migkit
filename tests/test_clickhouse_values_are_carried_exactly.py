"""ClickHouse's values carried and compared as they are (type-fidelity G3,
G6, G20).

Measured before, ClickHouse 24.8 and PostgreSQL 16:

* a `DateTime64(9)` read through the driver arrived at six digits, on both
  sides of a check alike: a ClickHouse to ClickHouse move wrote
  `...00.123456000` for `...00.123456789` and compared equal;
* a NULL written into a column that is not Nullable was stored as 0 -
  `input_format_null_as_default` - and only the check after the move saw
  it;
* a `LowCardinality(Nullable(String))` column had no class, so it was
  named as not compared.
"""
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import psql
from tests.typepair import Checkpoint

pytestmark = [pytest.mark.docker]

CH, CH_PORT = "migkit-test-f0t-ch", 16062


@pytest.fixture(scope="module")
def clickhouse():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
    except Exception:
        pytest.skip("docker not available")
    subprocess.run(["docker", "rm", "-f", "-v", CH], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", CH, "-e",
                    "CLICKHOUSE_PASSWORD=test", "-p",
                    f"127.0.0.1:{CH_PORT}:8123", "--ulimit",
                    "nofile=262144:262144",
                    "clickhouse/clickhouse-server:24.8"], check=True,
                   capture_output=True)
    try:
        import clickhouse_connect
        end, client = time.time() + 120, None
        while time.time() < end:
            try:
                client = clickhouse_connect.get_client(
                    host="127.0.0.1", port=CH_PORT, username="default",
                    password="test")
                client.command("select 1")
                break
            except Exception:
                client = None
                time.sleep(1)
        assert client is not None
        yield client
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", CH], capture_output=True)


def _ch():
    return Endpoint(host="127.0.0.1", port=CH_PORT, user="default",
                    password="test")


def _pg(port):
    return Endpoint(host="127.0.0.1", port=port, user="postgres",
                    password="test")


def _pair(tmp_path, src, dst, src_ep, dst_ep, db="cx", db_map=None):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="f0tch", engine="hetero",
              options={"source_engine": src, "target_engine": dst},
              source=src_ep, target=dst_ep, databases=[db],
              db_map=db_map or {})
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def _move(eng, table, schema="", db="cx"):
    made = eng.dst_engine.prepare_target(db)
    eng.move_table(db, schema, table, 1000, Checkpoint(), lambda m: None)
    return made


def _verdict(eng, table, db="cx"):
    got = [r for r in eng.check_data(db)
           if r.check == "data" and r.scope == f"{db}.{table}"]
    assert len(got) == 1, [(r.scope, r.status, r.detail) for r in got]
    return got[0]


NS = ("2024-01-01 00:00:00.123456789", "2024-01-01 00:00:00.000000001",
      "2024-01-01 00:00:00.5")


@pytest.fixture
def nanos(clickhouse):
    for d in ("cx", "cy"):
        clickhouse.command(f"drop database if exists {d}")
        clickhouse.command(f"create database {d}")
    clickhouse.command("create table cx.t (id Int64, ts DateTime64(9))"
                       " engine = MergeTree order by id")
    clickhouse.command("insert into cx.t values " + ", ".join(
        f"({i}, '{v}')" for i, v in enumerate(NS)))
    return clickhouse


def test_nine_digits_move_between_clickhouse_databases(nanos, tmp_path):
    eng = _pair(tmp_path, "clickhouse", "clickhouse", _ch(), _ch(),
                db_map={"cx": "cy"})
    _move(eng, "t")
    got = nanos.query("select toString(ts) from cy.t order by id"
                      ).result_rows
    assert [r[0] for r in got] == ["2024-01-01 00:00:00.123456789",
                                   "2024-01-01 00:00:00.000000001",
                                   "2024-01-01 00:00:00.500000000"], got
    assert _verdict(eng, "t").status == "ok"
    # the digits past the sixth are compared, not cut on both sides
    nanos.command("alter table cy.t update ts = toDateTime64("
                  "'2024-01-01 00:00:00.123456000', 9) where id = 0"
                  " settings mutations_sync = 2")
    assert _verdict(eng, "t").status == "diff"


def test_nine_digits_into_postgres_are_refused(nanos, pg_pair, tmp_path):
    psql(pg_pair["dst"], "drop database if exists cx")
    assert psql(pg_pair["dst"], "create database cx").returncode == 0
    eng = _pair(tmp_path, "clickhouse", "postgres", _ch(),
                _pg(pg_pair["dst"]))
    with pytest.raises(SystemExit) as got:
        _move(eng, "t")
    said = " ".join(str(got.value).split())
    assert "ts: 2 rows hold more than 6 digits of a second" in said, said
    assert "id 0, 1" in said, said


def test_a_null_into_a_column_that_takes_none_is_refused(clickhouse,
                                                         pg_pair,
                                                         tmp_path):
    port = pg_pair["src"]
    psql(port, "drop database if exists cx")
    assert psql(port, "create database cx").returncode == 0
    assert psql(port, "create table t (id bigint primary key, v bigint,"
                      " s text); insert into t values (1, 5, 'a'),"
                      " (2, null, null), (3, null, 'c')",
                db="cx").returncode == 0
    clickhouse.command("drop database if exists cx")
    clickhouse.command("create database cx")
    clickhouse.command("create table cx.t (id Int64, v Int64,"
                       " s Nullable(String)) engine = MergeTree order by id")
    eng = _pair(tmp_path, "postgres", "clickhouse", _pg(port), _ch())
    with pytest.raises(SystemExit) as got:
        _move(eng, "t", "public")
    said = " ".join(str(got.value).split())
    assert "v: 2 rows hold NULL, which the clickhouse column is not" \
        " Nullable for" in said and "id 2, 3" in said, said
    assert "s:" not in said, said
    assert clickhouse.query("select count() from cx.t").result_rows \
        == [(0,)]
    # a column that takes NULL takes them
    clickhouse.command("drop table cx.t")
    clickhouse.command("create table cx.t (id Int64, v Nullable(Int64),"
                       " s Nullable(String)) engine = MergeTree order by id")
    _move(_pair(tmp_path, "postgres", "clickhouse", _pg(port), _ch()), "t",
          "public")
    assert _verdict(_pair(tmp_path, "postgres", "clickhouse", _pg(port),
                          _ch()), "t").status == "ok"


def test_a_low_cardinality_nullable_column_is_compared(clickhouse, pg_pair,
                                                       tmp_path):
    port = pg_pair["dst"]
    psql(port, "drop database if exists cx")
    assert psql(port, "create database cx").returncode == 0
    assert psql(port, "create table t (id bigint primary key, k text);"
                      " insert into t values (1, 'a'), (2, null)",
                db="cx").returncode == 0
    clickhouse.command("drop database if exists cx")
    clickhouse.command("create database cx")
    clickhouse.command("create table cx.t (id Int64,"
                       " k LowCardinality(Nullable(String)))"
                       " engine = MergeTree order by id")
    clickhouse.command("insert into cx.t values (1, 'a'), (2, null)")
    eng = _pair(tmp_path, "clickhouse", "postgres", _ch(), _pg(port))
    got = _verdict(eng, "t")
    assert got.status == "ok", got.detail
    assert "not compared" not in got.detail, got.detail
    clickhouse.command("alter table cx.t update k = 'b' where id = 1"
                       " settings mutations_sync = 2")
    assert _verdict(eng, "t").status == "diff"


def test_each_question_has_one_answer_in_sql_and_here(clickhouse, tmp_path):
    """ClickHouse answers `canon.unfit_sql` on the server, so a source of
    billions of rows is not read here to be asked - and the answer is the
    one `canon.unfit_value` gives over the same rows read here."""
    from migkit import canon
    clickhouse.command("drop database if exists cq")
    clickhouse.command("create database cq")
    clickhouse.command(
        "create table cq.q (id Int64, ts DateTime64(9), n Nullable(Int64),"
        " s String, f Float64, d Decimal(12, 4)) engine = MergeTree"
        " order by id")
    clickhouse.command(
        "insert into cq.q values"
        " (1, '2024-01-01 00:00:00.123456789', 5, 'abcd', 1, 1.2345),"
        " (2, '2024-01-01 00:00:00.5', null, 'a😀', nan, 1.5),"
        " (3, '2024-01-01 00:00:00.000001', 101, 'x', inf, 2.25),"
        " (4, '2024-01-01 00:00:01', -6, 'ab\\0c', 2, 3)")
    asked = [("ts", "fraction", 6), ("ts", "fraction", 3),
             ("n", "null", None), ("n", "int-range", (-5, 100)),
             ("s", "chars", 3), ("s", "bytes", 4), ("s", "nul", None),
             ("s", "supplementary", "x"), ("f", "float-nonfinite", None),
             ("d", "scale", 1)]
    columns = [("id", "integer"), ("ts", "timestamp"), ("n", "integer"),
               ("s", "text"), ("f", "float"), ("d", "decimal")]
    eng = _pair(tmp_path, "clickhouse", "postgres", _ch(), _ch(),
                db="cq").src_engine
    assert all(canon.unfit_sql("clickhouse", k, "c", a) for _, k, a in asked)
    sql = [n for n, _ in eng.count_unfit("src", "cq", "q", columns, asked,
                                         ["id"])]
    names = [n for n, _ in columns]
    here = [0] * len(asked)
    for rows in eng._every_row("src", "cq", "q", columns):
        for r in rows:
            for i, (col, kind, arg) in enumerate(asked):
                here[i] += canon.unfit_value(kind, r[names.index(col)], arg)
    assert sql == here, list(zip(asked, sql, here))
    assert all(0 < n < 4 for n in sql), list(zip(asked, sql))

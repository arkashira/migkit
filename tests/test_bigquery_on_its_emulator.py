"""BigQuery's side of a pair, run on BigQuery's emulator
(goccy/bigquery-emulator, MIT, v0.8.1, the arm64 image): its SQL through
the REST API, its Storage Write API through gRPC.

The emulator is a Go program over SQLite, not BigQuery, and what passes
here is proved against it. Asked on 2026-09-28, it does not implement -
so these are proved only on a real project:
* a multi-statement transaction's atomicity: a script whose second
  statement failed kept its first (the ledger's line and a batch's merge
  commit together on BigQuery; here they only run in order);
* `ROUND(x, n, 'ROUND_HALF_EVEN')` - the float rendering's middle band -
  and `REGEXP_REPLACE`'s back references (`\\1` came back as `${2`), so a
  table with a FLOAT64 column is folded in this process here;
* a FLOAT64's shortest text: 5e-324 came back as 4.94065645841247e-324;
* a MERGE whose source is a subquery (a table only - which is why the
  tail's merges read a table of the script's own);
* `INFORMATION_SCHEMA.JOBS_BY_PROJECT` and `TABLE_CONSTRAINTS`: the load
  jobs of the day go uncounted (and are said so), and a key is read from
  the table's own metadata;
* Arrow rows in the Storage Write API: it reads proto rows only.
"""
import datetime
import json
import subprocess
import time
from decimal import Decimal

import pytest

from migkit import canon
from migkit.config import Endpoint, Hop
from tests.conftest import _wait_tcp, needs_docker

pytestmark = needs_docker

BQ, HTTP, GRPC = "migkit-test-wh-bq", 16150, 16151
PROJECT, DS = "proj", "ds"


@pytest.fixture(scope="module")
def emulator():
    subprocess.run(["docker", "rm", "-f", "-v", BQ], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", BQ, "-p",
                    f"127.0.0.1:{HTTP}:9050", "-p", f"127.0.0.1:{GRPC}:9060",
                    "ghcr.io/goccy/bigquery-emulator:latest",
                    f"--project={PROJECT}"], check=True, capture_output=True)
    try:
        assert _wait_tcp("127.0.0.1", HTTP) and _wait_tcp("127.0.0.1", GRPC)
        time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", BQ], capture_output=True)


@pytest.fixture
def eng(emulator, tmp_path, monkeypatch):
    from google.cloud import bigquery

    from migkit.engines import engine_named
    # the emulator answers a query it cannot run with a 500, which the
    # client takes for a passing error and retries for ten minutes -
    # measured, a view it lacks held one read for longer than that; here
    # it is said at once, as BigQuery says a query it cannot run
    real = bigquery.Client.query
    real_result = bigquery.QueryJob.result

    def query(self, sql, *a, **kw):
        kw.setdefault("job_retry", None)
        kw.setdefault("retry", None)
        return real(self, sql, *a, **kw)

    def result(self, *a, **kw):
        kw.setdefault("retry", None)
        kw.setdefault("job_retry", None)
        return real_result(self, *a, **kw)
    monkeypatch.setattr(bigquery.Client, "query", query)
    monkeypatch.setattr(bigquery.QueryJob, "result", result)
    hop = Hop(name="bq", engine="bigquery", source=Endpoint(),
              target=Endpoint(options={
                  "database": PROJECT, "anonymous": True,
                  "api_endpoint": f"http://127.0.0.1:{HTTP}",
                  "write_endpoint": f"127.0.0.1:{GRPC}"}),
              databases=[DS])
    hop.report_dir = lambda db=None: tmp_path
    e = engine_named("bigquery", hop)
    client = e._client("dst")
    client.delete_dataset(DS, delete_contents=True, not_found_ok=True)
    client.create_dataset(DS)
    return e


def _q(e, sql):
    return [list(r.values()) for r in e._client("dst").query(sql).result()]


def _fold(e, table, cols):
    back = [r for b in e.neutral_batches("dst", DS, table, cols, 1000)
            for r in b]
    n, total = canon.fold_rows([c for _, c in cols], back)
    return n, str(total)


ITEMS = [("id", "integer"), ("name", "text"), ("price", "decimal"),
         ("ok", "boolean"), ("raw", "bytes"), ("day", "date"),
         ("stamp", "timestamp"), ("zoned", "timestamp"), ("clock", "time")]


def _items(e, n=300):
    _q(e, f"create table {DS}.items (id INT64, name STRING, price"
          " NUMERIC(12, 2), ok BOOL, raw BYTES, day DATE, stamp DATETIME,"
          " zoned TIMESTAMP, clock TIME, ratio FLOAT64)")
    names = ["x|y", "é", "", "N:"]
    rows = [f"({i}, {names[i % 4]!r}, NUMERIC '{Decimal(i).scaleb(-2)}',"
            f" {'true' if i % 2 else 'false'}, b'\\x00\\xff',"
            " DATE '2024-01-02',"
            f" DATETIME '2024-01-02 03:04:05.{i:06d}',"
            " TIMESTAMP '2024-01-02 03:04:05.123456+07',"
            f" TIME '03:04:05.5', {i / 7!r})" for i in range(n)]
    rows.append("(9999, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,"
                " NULL)")
    _q(e, f"insert into {DS}.items values " + ", ".join(rows))


def test_bigquerys_digest_is_the_fold_in_its_own_sql(eng):
    """Every class BigQuery renders, from rows whose text a row encoding
    has to survive: the digest its SQL answers is the fold of the same
    rows read back, and one value changed moves it."""
    _items(eng)
    got = eng.neutral_digest("dst", DS, "items", ITEMS)
    assert eng.folded_last is False
    assert got == _fold(eng, "items", ITEMS) and got[0] == 301
    _q(eng, f"update {DS}.items set price = NUMERIC '9.99' where id = 17")
    assert eng.neutral_digest("dst", DS, "items", ITEMS) != got
    # its rows sized by listing them, every one of the 301
    sized = eng.row_sizes("dst", DS, ["items"])["items"]
    assert sized["rows"] == 301 and sized["exact"] is True, sized


def test_a_float_the_emulator_cannot_render_is_folded_here(eng):
    """The float rendering's rounding is BigQuery's own, which the emulator
    refuses: the table is folded here, to the same number."""
    _items(eng, 40)
    cols = ITEMS + [("ratio", "float")]
    got = eng.neutral_digest("dst", DS, "items", cols)
    assert eng.folded_last is True
    assert got == _fold(eng, "items", cols)


ORDERS = [("id", "integer", (), {"null": False}), ("name", "text", ()),
          ("price", "decimal", (12, 2)), ("at", "timestamp", ())]


def test_a_batch_is_staged_by_a_load_job_and_merged_by_its_key(eng):
    """A copy's batch into BigQuery: the table made through the API with
    its key, the batch loaded from Parquet into a stage of its own and
    merged in by that key, the stage dropped; written again, as a copy
    stopped before its checkpoint writes it, it replaces itself."""
    eng.neutral_create("dst", DS, "orders", ORDERS, key=("id",))
    assert eng.neutral_key("dst", DS, "orders") == ["id"]
    names = [(c[0], c[1]) for c in ORDERS]
    at = datetime.datetime(2024, 2, 29, 7, 5, tzinfo=datetime.timezone(
        datetime.timedelta(hours=7)))
    batch = [[1, "a", Decimal("1.50"), at], [2, None, None, None]]
    assert eng.neutral_write("dst", DS, "orders", names, batch) == 2
    assert eng.neutral_write("dst", DS, "orders", names, batch) == 2
    eng.neutral_write("dst", DS, "orders", names,
                      [[2, "b", Decimal("2.00"), None]])
    got = _q(eng, f"select id, name, cast(price as string),"
                  f" cast(at as string) from {DS}.orders order by id")
    assert got == [[1, "a", "1.5", "2024-02-29 00:05:00"],
                   [2, "b", "2", None]], got
    assert not [t for t in eng._table_names("dst", DS) if eng._own(t)]
    assert eng.neutral_tables("dst", DS) == ["orders"]
    # read back through the driver at the declared scale, as the other
    # engines print it
    rows, _ = eng.neutral_read("dst", DS, "orders", names, None, 10)
    assert [str(r[2]) for r in rows] == ["1.50", "2.00"]
    assert dict(eng.neutral_columns("dst", DS, "orders"))["price"] == \
        "BIGNUMERIC(12, 2)"


def test_the_write_stream_refuses_rows_at_an_offset_it_holds(eng):
    """A committed stream's append sent again at the offset it already
    filled is refused there, not written twice."""
    from google.cloud.bigquery import SchemaField
    _q(eng, f"create table {DS}.j (id INT64, name STRING)")
    writer = eng._write_client("dst")
    stream = eng._new_stream(writer, "dst", DS, "j")
    fields = [SchemaField("id", "INT64"), SchemaField("name", "STRING")]
    rows = [[1, "a"], [2, "b"]]
    assert eng._append(writer, stream, fields, ["id", "name"], rows, 0) == 0
    held = eng._append(writer, stream, fields, ["id", "name"], rows, 0)
    assert _q(eng, f"select count(*) from {DS}.j") == [[2]]
    assert held == 2


def _changes(*items):
    return [canon.change(op, "acct", {"id": k}, v) for op, k, v in items]


def test_a_tail_batch_is_merged_with_its_ledger_line(eng):
    """The batch's changes land in the table's journal through the write
    stream and are merged, with the ledger's line; the ledger answers the
    tail's question after a stop, and the batch applied again changes
    nothing."""
    cols = [("id", "integer", (), {"null": False}),
            ("balance", "integer", ()), ("note", "text", ())]
    eng.neutral_create("dst", DS, "acct", cols, key=("id",))
    _q(eng, f"insert into {DS}.acct values (1, 100, 'n1'), (2, 100, 'n2')")
    eng._batch_seen = json.dumps({"token": {"lsn": 5}, "batch": 77})
    batch = _changes(("update", 1, {"id": 1, "balance": 110}),
                     ("delete", 2, {}),
                     ("insert", 3, {"id": 3, "balance": 300, "note": "c"}))
    eng.neutral_apply("dst", DS, batch)
    want = [[1, 110, "n1"], [3, 300, "c"]]
    assert _q(eng, f"select id, balance, note from {DS}.acct order by id"
               ) == want
    assert eng.origin_seen("dst", DS) == {"batch": 77, "token": {"lsn": 5}}
    eng.neutral_apply("dst", DS, batch)
    assert _q(eng, f"select id, balance, note from {DS}.acct order by id"
               ) == want
    # the interlock's reading of the target
    said = eng.server_identity("dst", DS)
    assert said["id"] == PROJECT and said["absent"] is False
    assert eng.side_database("dst", DS) == f"{PROJECT}.{DS}"

"""What migkit says to each warehouse to land a batch once, and how it
reads what the warehouse answers - pinned against the requests and the
answers each documents, since no Snowflake runs anywhere but on an
account, and Redshift's COPY and load history run only on Redshift.

Proved only on a real account, and so not here: that Snowflake's COPY
skips a file its load history holds (64 days), that a channel's
committed offset token survives a reopen, that Redshift's COPY reads the
Parquet a slice at a time and `STL_LOAD_COMMITS` lists the files of a
committed COPY, and every SQL text below as each server parses it.
BigQuery's and Redshift's SQL that an emulator or PostgreSQL can run is
run in `test_warehouse_exact_loads_run.py`.
"""
import io
import json
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop


def _engine(name, **target):
    from migkit.engines import engine_named
    return engine_named(name, Hop(
        name="w", engine=name, source=Endpoint(),
        target=Endpoint(user="u", options={"database": "analytics",
                                           "account": "acme", **target}),
        databases=["sales"]))


class _Cursor:
    """A DB-API cursor answering each statement from `answers`, a list of
    (prefix, description, rows), and recording what it was asked."""

    def __init__(self, log, answers, files=None):
        self.log, self.answers, self.files = log, answers, files
        self.description, self._rows, self.rowcount = None, [], 0

    def execute(self, sql, args=None):
        self.log.append((" ".join(sql.split()), args))
        low = sql.strip().lower()
        if low.startswith("put ") and self.files is not None:
            import pyarrow.parquet as pq
            path = sql.split("'")[1][len("file://"):]
            self.files.append(pq.read_table(path))
        self.description, self._rows = None, []
        for prefix, desc, rows in self.answers:
            if low.startswith(prefix):
                self.description = [(d,) for d in desc] if desc else None
                self._rows = list(rows)
                break

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Conn:
    def __init__(self, cur):
        self.cur, self.closed = cur, False

    def cursor(self):
        return self.cur

    def commit(self):
        self.cur.log.append(("<commit>", None))

    def rollback(self):
        pass

    def close(self):
        self.closed = True


COPY_COLS = ["file", "status", "rows_parsed", "rows_loaded", "error_limit",
             "errors_seen", "first_error", "first_error_line",
             "first_error_character", "first_error_column_name"]


def _snowflake(answers, files=None, **target):
    eng = _engine("snowflake", **target)
    log = []
    cur = _Cursor(log, answers, files)
    eng._connect = lambda side, db: _Conn(cur)
    eng.neutral_columns = lambda side, db, table: [
        ("id", "INTEGER"), ("name", "TEXT(40)"), ("price", "NUMBER(12,2)"),
        ("raw", "BINARY(8388608)"), ("doc", "VARIANT")]
    eng.neutral_key = lambda side, db, table: ["id"]
    return eng, log


COLS = [("id", "integer"), ("name", "text"), ("price", "decimal"),
        ("raw", "bytes"), ("doc", "json")]
ROWS = [[1, "a'b", Decimal("1.50"), b"\x00\xff", {"k": [1, 2]}],
        [2, None, None, None, None]]


def test_snowflake_stages_a_batch_as_text_and_merges_it_by_key():
    files = []
    eng, log = _snowflake([("copy into", COPY_COLS, [
        ("migkit-x.parquet", "LOADED", 2, 2, 2, 0, None, None, None,
         None)])], files)
    assert eng.neutral_write("dst", "sales", "orders", COLS, ROWS) == 2
    said = [s for s, _ in log]
    fp = eng._fingerprint("orders", [n for n, _ in COLS], ROWS)
    stage = f'"_MIGKIT_STAGE_ORDERS_{fp.upper()}"'
    assert said[0].startswith("put 'file://") and said[0].endswith(
        f"migkit-{fp}.parquet' @\"SALES\".%\"ORDERS\" auto_compress = false"
        " overwrite = true"), said[0]
    assert said[1] == (f'create temporary table {stage} like'
                       ' "SALES"."ORDERS"')
    assert said[2] == (
        f'copy into {stage} ("ID", "NAME", "PRICE", "RAW", "DOC") from'
        ' (select $1:"ID"::STRING::INTEGER, $1:"NAME"::STRING::TEXT(40),'
        ' $1:"PRICE"::STRING::NUMBER(12,2),'
        ' TO_BINARY($1:"RAW"::STRING, \'HEX\'),'
        ' PARSE_JSON($1:"DOC"::STRING) from @"SALES".%"ORDERS")'
        f" files = ('migkit-{fp}.parquet') file_format = (type = parquet)"
        " purge = true")
    assert said[3:5] == ["begin", (
        f'merge into "SALES"."ORDERS" t using {stage} s on t."ID" = s."ID"'
        ' when matched then update set t."NAME" = s."NAME",'
        ' t."PRICE" = s."PRICE", t."RAW" = s."RAW", t."DOC" = s."DOC"'
        ' when not matched then insert ("ID", "NAME", "PRICE", "RAW",'
        ' "DOC") values (s."ID", s."NAME", s."PRICE", s."RAW", s."DOC")')]
    assert said[5:] == ["commit", f"drop table if exists {stage}"]
    # every value staged as text Snowflake casts back exactly
    staged = files[0].to_pylist()
    assert staged == [{"ID": "1", "NAME": "a'b", "PRICE": "1.50",
                       "RAW": "00FF", "DOC": '{"k": [1, 2]}'},
                      {"ID": "2", "NAME": None, "PRICE": None, "RAW": None,
                       "DOC": None}]


def test_snowflake_stops_on_a_file_that_did_not_load_whole():
    eng, _ = _snowflake([("copy into", COPY_COLS, [
        ("migkit-x.parquet", "PARTIALLY_LOADED", 2, 1, 2, 1, "bad", 1, 1,
         "PRICE")])])
    with pytest.raises(SystemExit, match="did not load"):
        eng.neutral_write("dst", "sales", "orders", COLS, ROWS)
    eng, _ = _snowflake([("copy into", COPY_COLS, [
        ("migkit-x.parquet", "LOADED", 2, 1, 2, 0, None, None, None, None)])])
    with pytest.raises(SystemExit, match="1 of 2 staged rows"):
        eng.neutral_write("dst", "sales", "orders", COLS, ROWS)


def test_snowflake_copies_a_keyless_batch_straight_in_once():
    """A table with no key: the file goes straight into it, and a file its
    load history holds is skipped - the answer Snowflake gives then is one
    row saying no file was new, which is not an error."""
    eng, log = _snowflake([("copy into", ["status"], [
        ("Copy executed with 0 files processed.",)])])
    eng.neutral_key = lambda side, db, table: []
    assert eng.neutral_write("dst", "sales", "events", COLS, ROWS) == 2
    said = [s for s, _ in log]
    assert said[1].startswith('copy into "SALES"."EVENTS" ("ID",')
    assert not any(s.startswith(("merge", "begin", "create temporary"))
                   for s in said)


class _Status:
    def __init__(self, token):
        self.latest_committed_offset_token = token


class _Channel:
    def __init__(self, calls, committed):
        self.calls, self.committed = calls, committed

    def append_rows(self, rows, start_offset_token=None,
                    end_offset_token=None):
        self.calls.append(("append", rows, start_offset_token,
                           end_offset_token))
        self.committed[0] = end_offset_token

    def wait_for_commit(self, check, timeout_seconds=None):
        assert check(self.committed[0])
        self.calls.append(("waited", self.committed[0]))

    def close(self):
        self.calls.append(("closed",))


class _Streaming:
    def __init__(self, calls, committed):
        self.calls, self.committed = calls, committed

    def open_channel(self, name):
        self.calls.append(("open", name))
        return (_Channel(self.calls, self.committed),
                _Status(self.committed[0]))


def _changes():
    from migkit import canon
    return [canon.change("update", "orders", {"id": 1},
                         {"id": 1, "name": "A"}),
            canon.change("delete", "orders", {"id": 2}),
            canon.change("insert", "orders", {"id": 3},
                         {"id": 3, "name": "c", "price": Decimal("3.00")})]


def test_snowflake_lands_a_tail_batch_through_its_channel_once(tmp_path):
    key = tmp_path / "key.p8"
    key.write_text("-----BEGIN PRIVATE KEY-----\nx\n-----END PRIVATE KEY-----")
    eng, log = _snowflake([], private_key_file=str(key))
    calls, committed = [], [None]
    made = []

    def streaming(cls, side, db, journal, props):
        made.append((journal, props["account"], props["user"]))
        return _Streaming(calls, committed)
    eng._streaming = streaming
    eng._batch_seen = json.dumps({"token": {"lsn": 7}, "batch": 12})
    eng.neutral_apply("dst", "sales", _changes())
    appended = [c for c in calls if c[0] == "append"]
    assert len(appended) == 1
    rows, start, end = appended[0][1:]
    assert start == end and end.startswith("12:")
    # the journal's rows: the carried columns, the operation and which
    # columns each carried, the batch's number and fingerprint
    assert [(r["ID"], r["_MIGKIT_OP"], r["_MIGKIT_COLS"], r["_MIGKIT_BATCH"])
            for r in rows] == [(1, "upsert", "id,name", 12),
                               (2, "delete", "id", 12),
                               (3, "upsert", "id,name,price", 12)]
    said = [s for s, _ in log]
    at = said.index("begin")
    merges = said[at + 1:at + 4]
    assert all(m.startswith('merge into "SALES"."ORDERS" t using (select *'
                            ' from "SALES"."_MIGKIT_JOURNAL_ORDERS_') for m in
               merges), merges
    assert all("when matched and s.\"_MIGKIT_OP\" = 'delete' then delete"
               in m and "qualify row_number() over (partition by"
               ' "_MIGKIT_SEQ") = 1' in m for m in merges), merges
    assert [a for s, a in log[at + 1:at + 4]] == [("id,name",), ("id",),
                                                  ("id,name,price",)]
    assert said[at + 4:] == [
        'delete from "SALES"."_MIGKIT_LEDGER" where hop = %s and db = %s',
        'insert into "SALES"."_MIGKIT_LEDGER" (hop, db, batch, token, at)'
        ' select %s, %s, %s, %s, sysdate()', "commit"]
    assert log[at + 5][1] == ("w", "sales", 12, '{"lsn": 7}')
    # the same batch again, as the tail replays it after a stop: the
    # channel's committed token is this attempt's, and nothing is appended
    calls.clear()
    eng.neutral_apply("dst", "sales", _changes())
    assert not [c for c in calls if c[0] == "append"], calls
    assert made[0][1:] == ("acme", "u")


def test_snowflake_lands_through_a_staged_file_without_a_key_pair():
    eng, log = _snowflake([("copy into", COPY_COLS, [
        ("f", "LOADED", 3, 3, 3, 0, None, None, None, None)])])
    eng._batch_seen = json.dumps({"token": {"lsn": 7}, "batch": 12})
    eng.neutral_apply("dst", "sales", _changes())
    said = [s for s, _ in log]
    put = [s for s in said if s.startswith("put ")]
    assert len(put) == 1 and "migkit-orders-12-" in put[0]
    assert '@"SALES".%"_MIGKIT_JOURNAL_ORDERS_' in put[0]


# ---- Redshift ---------------------------------------------------------------

def _redshift(answers, **target):
    eng = _engine("redshift", **target)
    log = []
    cur = _Cursor(log, answers)
    eng._connect = lambda side, db: _Conn(cur)
    eng.neutral_columns = lambda side, db, table: [
        ("id", "bigint"), ("name", "character varying(40)"),
        ("price", "numeric(12,2)"), ("raw", "binary varying(64)")]
    eng.neutral_key = lambda side, db, table: ["id"]
    return eng, log


RS_COLS = [("id", "integer"), ("name", "text"), ("price", "decimal"),
           ("raw", "bytes")]
RS_ROWS = [[1, "a", Decimal("1.50"), b"\x00\xff"], [2, "b", None, None],
           [3, "c", Decimal("3.00"), b""], [4, "d", None, None],
           [5, "e", None, None]]


def test_redshift_copies_a_batch_from_s3_a_file_a_slice(monkeypatch):
    import boto3
    from moto import mock_aws
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="stage")
        eng, log = _redshift([("select count(*) from stv_slices", None,
                               [(4,)])],
                             stage="s3://stage/migkit",
                             iam_role="arn:aws:iam::1:role/rs",
                             stage_region="us-east-1")
        seen = []
        real = eng._drop_files

        def drop(side, url):
            got = s3.list_objects_v2(Bucket="stage")
            seen.extend(o["Key"] for o in got.get("Contents", []))
            real(side, url)
        eng._drop_files = drop
        eng.neutral_write("dst", "sales", "orders", RS_COLS, RS_ROWS)
        fp = eng._fingerprint("orders", [n for n, _ in RS_COLS], RS_ROWS)
        # four slices, four files, all the table's columns in its order
        assert seen == [f"migkit/w/orders/{fp}/part-{i:04d}.parquet"
                        for i in range(4)]
        assert "Contents" not in s3.list_objects_v2(Bucket="stage")
        said = [s for s, _ in log]
        stage = f'"_migkit_stage_orders_{fp}"'
        assert said[:4] == [
            f'create temp table {stage} as select * from "sales"."orders"'
            ' where 1 = 0',
            "select count(*) from stv_slices",
            f"copy {stage} from 's3://stage/migkit/w/orders/{fp}/' iam_role"
            " 'arn:aws:iam::1:role/rs' format as parquet",
            f'merge into "sales"."orders" using {stage} on "sales"."orders".'
            f'"id" = {stage}."id" when matched then update set "name" ='
            f' {stage}."name", "price" = {stage}."price", "raw" ='
            f' {stage}."raw" when not matched then insert ("id", "name",'
            f' "price", "raw") values ({stage}."id", {stage}."name",'
            f' {stage}."price", {stage}."raw")']
        # one transaction: the stage dropped, then committed
        assert said[4:] == [f"drop table {stage}", "<commit>"]


def test_redshift_asks_its_load_history_when_a_commit_goes_unanswered():
    """A keyless batch whose commit's answer was lost: the cluster's load
    history says whether the files were loaded by a COPY that committed,
    and a batch it says was is not inserted again."""
    eng, log = _redshift([])
    eng.neutral_key = lambda side, db, table: []
    asked = []

    def rows(side, db, sql, args=None):
        asked.append((" ".join(sql.split()), args))
        if "stl_load_commits" in sql:
            raise RuntimeError("relation stl_load_commits does not exist")
        return [[3]]
    eng._rows = rows
    assert eng._loaded("dst", "sales", "s3://stage/x/")
    assert asked[0][0].startswith("select count(*) from stl_load_commits c"
                                  " join stl_query q on q.query = c.query")
    assert "stl_commit_stats" in asked[0][0]
    assert asked[1][0].startswith("select count(*) from sys_load_detail d")
    assert asked[1][1] == ("s3://stage/x/%",)


def test_redshift_without_a_stage_inserts_bytes_and_json_as_it_reads_them():
    eng, log = _redshift([])
    eng.neutral_columns = lambda side, db, table: [
        ("id", "bigint"), ("raw", "binary varying(64)"), ("doc", "super")]
    captured = {}
    from psycopg2 import extras

    def values(cur, sql, rows, template=None, page_size=100):
        captured.update(sql=sql, rows=rows, template=template)
    import unittest.mock
    with unittest.mock.patch.object(extras, "execute_values", values):
        eng.neutral_write("dst", "sales", "docs",
                          [("id", "integer"), ("raw", "bytes"),
                           ("doc", "json")],
                          [[1, b"\x0a", {"a": 1}]])
    assert captured["template"] == ("(%s, to_varbyte(%s, 'hex'),"
                                    " json_parse(%s))")
    assert captured["rows"] == [(1, "0a", '{"a": 1}')]


# ---- BigQuery ----------------------------------------------------------------

class _Job:
    def __init__(self, rows=None, affected=0):
        self._rows, self.num_dml_affected_rows = rows or [], affected

    def result(self):
        return self._rows


class _Row(dict):
    def values(self):
        return list(super().values())


class _BQ:
    """The BigQuery client's surface the writer uses, recorded."""

    def __init__(self, loads_today):
        self.calls, self.loads_today = [], loads_today

    def get_table(self, ref):
        self.calls.append(("get_table", ref))
        from google.cloud.bigquery import SchemaField
        return type("T", (), {"schema": [SchemaField("id", "INTEGER"),
                                         SchemaField("name", "STRING"),
                                         SchemaField("price", "NUMERIC")]})

    def create_table(self, table, exists_ok=False):
        self.calls.append(("create_table", table.reference.table_id,
                           table.expires is not None))

    def query(self, sql, job_config=None):
        self.calls.append(("query", " ".join(sql.split())))
        if "JOBS_BY_PROJECT" in sql:
            if self.loads_today is None:
                raise RuntimeError("Access Denied: jobs.listAll")
            return _Job([_Row(n=self.loads_today)])
        return _Job()

    def load_table_from_file(self, body, ref, job_config=None):
        import pyarrow.parquet as pq
        self.calls.append(("load", ref, job_config.source_format,
                           job_config.write_disposition,
                           pq.read_table(io.BytesIO(body.read())).to_pylist()))
        return _Job()

    def delete_table(self, ref, not_found_ok=False):
        self.calls.append(("delete_table", ref))


def _bigquery(loads_today):
    eng = _engine("bigquery")
    client = _BQ(loads_today)
    eng._client = lambda side: client
    eng.neutral_key = lambda side, db, table: ["id"]
    return eng, client


BQ_COLS = [("id", "integer"), ("name", "text"), ("price", "decimal")]
BQ_ROWS = [[1, "a", Decimal("1.5")], [2, None, None]]


def test_bigquery_loads_a_batch_into_a_stage_of_its_own_and_merges_it():
    eng, client = _bigquery(loads_today=12)
    eng.neutral_write("dst", "sales", "orders", BQ_COLS, BQ_ROWS)
    fp = eng._fingerprint("orders", [n for n, _ in BQ_COLS], BQ_ROWS)
    stage = f"_migkit_stage_orders_{fp}"
    calls = client.calls
    assert calls[1] == ("create_table", stage, True)
    load = next(c for c in calls if c[0] == "load")
    assert load[1:4] == (f"analytics.sales.{stage}", "PARQUET",
                         "WRITE_TRUNCATE")
    assert load[4] == [{"id": 1, "name": "a",
                        "price": Decimal("1.500000000")},
                       {"id": 2, "name": None, "price": None}]
    merge = [c[1] for c in calls if c[0] == "query"
             and c[1].startswith("merge")]
    assert merge == [f"merge `sales`.`orders` T using `sales`.`{stage}` S"
                     " on T.`id` = S.`id` when matched then update set"
                     " `name` = S.`name`, `price` = S.`price` when not"
                     " matched then insert (`id`, `name`, `price`) values"
                     " (S.`id`, S.`name`, S.`price`)"]
    assert calls[-1] == ("delete_table", f"analytics.sales.{stage}")
    # the target itself is never loaded into: only the stage is
    assert all(c[1] != "analytics.sales.orders" for c in calls
               if c[0] == "load")


def test_bigquery_writes_through_the_stream_once_the_load_jobs_run_out():
    from migkit.engines.warehouse import BigQueryEngine
    eng, client = _bigquery(
        loads_today=BigQueryEngine.LOAD_JOBS_A_DAY)
    streamed = []
    eng._write_stage = lambda side, db, stage, fields, names, rows: \
        streamed.append((stage, names, rows))
    eng.neutral_write("dst", "sales", "orders", BQ_COLS, BQ_ROWS)
    assert streamed and not [c for c in client.calls if c[0] == "load"]
    # a project whose job history cannot be read loads, and says so
    eng, client = _bigquery(loads_today=None)
    eng.neutral_write("dst", "sales", "orders", BQ_COLS, BQ_ROWS)
    assert [c for c in client.calls if c[0] == "load"]
    said = eng._assess_extra()
    assert "could not be read" in said[0]["detail"]


def test_bigquery_sizes_its_rows_by_listing_them_not_by_a_billed_query():
    """A query is billed by the bytes of the columns it reads whatever its
    LIMIT: the rows' sizes the resource model asks for come from listing a
    table's first rows, which is not billed, and no query runs for them."""
    eng, client = _bigquery(loads_today=0)

    class _Listed(list):
        total_rows = 5000

    def list_rows(ref, max_results=None):
        client.calls.append(("list_rows", ref, max_results))
        return _Listed([_Row(id=1, name="abc", doc={"a": [1, 2]}),
                        _Row(id=2, name=None, doc=None)])
    client.list_rows = list_rows
    got = eng.row_sizes("dst", "sales", ["orders"])
    assert client.calls == [("list_rows", "analytics.sales.orders",
                             eng.SIZE_SAMPLE)]
    # 8 for the number, 3 for the text, 13 for the JSON as its text, 1 for
    # the row; a NULL is 1
    assert got == {"orders": {"largest": 25, "p99": 25, "mean": 18.0,
                              "rows": 5000, "exact": False}}
    # every row listed is every row of the table
    _Listed.total_rows = 2
    assert eng.row_sizes("dst", "sales", ["orders"])["orders"]["exact"]


class _Resp(dict):
    """An AppendRowsResponse as proto-plus shows one: `"error" in resp`
    where the server set an error."""

    def __init__(self, code=0):
        super().__init__({"error": 1} if code else {})
        self.error = type("E", (), {"code": code, "message": "m"})


class _Writer:
    def __init__(self, answers):
        self.answers, self.offsets, self.sent = list(answers), [], []

    def append_rows(self, requests):
        for req in requests:
            rows = list(req.proto_rows.rows.serialized_rows)
            self.offsets.append((req.offset, len(rows)))
            self.sent.append((req.proto_rows.writer_schema.proto_descriptor,
                              rows))
            yield _Resp(self.answers.pop(0))


def test_the_write_stream_counts_rows_it_held_at_an_offset_as_written():
    from google.cloud.bigquery import SchemaField
    eng, _ = _bigquery(loads_today=0)
    fields = [SchemaField("id", "INT64")]
    rows = [[i] for i in range(10)]
    # two bytes a row: five rows a request
    eng.APPEND_BYTES = 10
    # the second request's rows were there already: a retry after an
    # answer that was lost, refused at its offset rather than written twice
    w = _Writer([0, 6])
    assert eng._append(w, "s", fields, ["id"], rows, 100) == 5
    assert w.offsets == [(100, 5), (105, 5)]
    w = _Writer([0, 0])
    assert eng._append(w, "s", fields, ["id"], rows, 0) == 0
    from google.api_core import exceptions
    with pytest.raises(exceptions.OutOfRange):
        eng._append(_Writer([11]), "s", fields, ["id"], rows, 7)


def test_the_write_stream_is_given_each_value_as_its_column_takes_it():
    """The rows as the Storage Write API takes them, in a message type made
    from the table's schema (proto2, as it asks): a NUMERIC as its text, a
    DATE as days since 1970, a TIMESTAMP as microseconds since 1970 - an
    instant with a zone, or one read as UTC - a DATETIME as UTC reads the
    instant, a TIME as its text, JSON as its text."""
    import datetime

    from google.cloud.bigquery import SchemaField
    from google.protobuf import descriptor_pool
    from google.protobuf.message_factory import GetMessageClass
    eng, _ = _bigquery(loads_today=0)
    fields = [SchemaField("id", "INT64"), SchemaField("price", "NUMERIC"),
              SchemaField("day", "DATE"), SchemaField("seen", "TIMESTAMP"),
              SchemaField("at", "DATETIME"), SchemaField("clock", "TIME"),
              SchemaField("doc", "JSON"), SchemaField("raw", "BYTES"),
              SchemaField("ok", "BOOL"), SchemaField("ratio", "FLOAT64")]
    names = [f.name for f in fields]
    zone = datetime.timezone(datetime.timedelta(hours=7))
    at = datetime.datetime(2024, 2, 29, 7, 5, 0, 12, tzinfo=zone)
    rows = [[1, Decimal("1.50"), datetime.date(1970, 1, 3), at, at,
             datetime.time(1, 2, 3), {"a": [1, 2]}, b"\x00\xff", True,
             0.1],
            [2, None, None, None, None, None, None, None, None, None]]
    w = _Writer([0])
    assert eng._append(w, "s", fields, names, rows, 0) == 0
    desc, sent = w.sent[0]
    assert desc.name == "Row" and [f.name for f in desc.field] == names
    from google.protobuf import descriptor_pb2
    fdp = descriptor_pb2.FileDescriptorProto(name="t.proto",
                                             package="t", syntax="proto2")
    fdp.message_type.add().CopyFrom(desc)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(fdp)
    cls = GetMessageClass(pool.FindMessageTypeByName("t.Row"))
    first, second = (cls.FromString(b) for b in sent)
    assert (first.id, first.price, first.day, first.at, first.clock,
            first.doc, first.raw, first.ok, first.ratio) == (
        1, "1.50", 2, "2024-02-29 00:05:00.000012", "01:02:03.000000",
        '{"a": [1, 2]}', b"\x00\xff", True, 0.1)
    assert first.seen == 1709165100000012
    # a NULL is a field not set, not a zero
    assert not second.HasField("price") and not second.HasField("seen")


def test_migkits_own_tables_are_left_out_of_the_listing_and_named_by_assess():
    eng = _engine("redshift")
    eng._rows = lambda side, db, sql, args=None: [
        ["orders"], ["_migkit_ledger"], ["_migkit_journal_orders_1a2b3c4d"]]
    assert eng.neutral_tables("dst", "sales") == ["orders"]
    said = eng._assess_extra()
    assert said[0]["item"] == "sales: migkit's own tables"
    assert said[0]["detail"].startswith(
        "2: _migkit_ledger, _migkit_journal_orders_1a2b3c4d")
    # the same said of the warehouse as a pair's target, which a pair asks
    # each side for; a source keeps none of them
    assert eng.reach_items("dst") == said
    assert eng.reach_items("src") == []
    eng._rows = lambda side, db, sql, args=None: [["orders"]]
    assert eng._assess_extra() == []


def test_a_billed_source_is_not_probed_table_by_table():
    """The resource model times the source's reads per table; on BigQuery
    each such read is a query billed by every byte of its columns. The
    pair's probe and the engine's own both answer None there, and the
    reads of an engine that does not bill are still timed."""
    from migkit.engines.base import Engine
    from migkit.engines.warehouse import BigQueryEngine
    assert BigQueryEngine.READS_BILLED is True
    touched = []

    class Billed(BigQueryEngine):
        def __init__(self):
            pass

        def neutral_columns(self, *a, **k):
            touched.append("read")
            return []

    assert Engine.probe_read(Billed(), "src", "d", "t") is None
    assert touched == []
    assert Engine.READS_BILLED is False

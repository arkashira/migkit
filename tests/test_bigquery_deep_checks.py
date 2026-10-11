"""What a BigQuery dataset keeps beside its rows is held to the source's:
views, routines and materialized views, declared keys and foreign keys,
the columns' rules, collation, row and column policies, indexes, options,
who may read, and - within what a query may read - the values a copy can
leave wrong (`engines/bigquery_checks.py`).

BigQuery refuses no repeated key, no key that points at nothing and no
column left without its policy, and bills a query by the bytes it reads
whatever its LIMIT: so a check that reads rows asks the dry run first and
names the table it did not read.

The client is a stand-in that answers as the documentation says
(`tests/bigquery_fake.py`); no project has answered these calls, so none
of these cells is in a `PROVES` (`capabilities.UNPROVED`).
"""
from types import SimpleNamespace as NS

import pytest

pytest.importorskip("google.cloud.bigquery")

from tests import bigquery_fake as fake  # noqa: E402
from tests.bigquery_fake import Field, Table  # noqa: E402

GIB = 1024 ** 3


def _eng(monkeypatch, tmp_path, src=(), dst=(), **hop):
    eng = fake.engine(monkeypatch, report=tmp_path, **hop)
    for side, tables in (("src", src), ("dst", dst)):
        for t in tables:
            eng.clients[side].dataset("sales").add(t)
    return eng


def _orders(*extra, **kw):
    kw.setdefault("constraints", fake.constraints(primary=["id"]))
    return Table("orders", [Field("id", "INTEGER", "REQUIRED"),
                            Field("name", "STRING"),
                            *extra], **kw)


def _routine(name, body="x + 1", kind="SCALAR_FUNCTION", dataset="sales"):
    return NS(routine_id=name, type_=kind, language="SQL", body=body,
              arguments=[NS(name="x", data_type=NS(type_kind="INT64"),
                            mode=None)],
              return_type=NS(type_kind="INT64"))


# ---- views, routines, materialized views ---------------------------------

def test_a_view_defined_alike_in_another_project_is_the_same(monkeypatch,
                                                            tmp_path):
    src = Table("v", table_type="VIEW",
                view_query="select * from `proj-a.sales.orders`")
    dst = Table("v", table_type="VIEW",
                view_query="select *   from `proj-b.sales.orders`")
    eng = _eng(monkeypatch, tmp_path, [src], [dst])
    res = eng._deep_objects("sales")
    assert (res.status, res.scope) == ("ok", "sales objects")
    assert "1 views, routines and other objects" in res.detail


def test_a_view_a_routine_the_target_lacks_or_defines_otherwise_is_found(
        monkeypatch, tmp_path):
    srcv = Table("v", table_type="VIEW", view_query="select 1")
    dstv = Table("v", table_type="VIEW", view_query="select 2")
    eng = _eng(monkeypatch, tmp_path, [srcv], [dstv])
    eng.clients["src"].dataset("sales").routines["f"] = _routine("f")
    res = eng._deep_objects("sales")
    assert res.status == "diff"
    assert "1 on the source and not the target: routine f" in res.detail
    assert "1 defined differently: view v" in res.detail


def test_objects_the_hop_does_not_carry_are_not_held(monkeypatch, tmp_path):
    srcv = Table("v", table_type="VIEW", view_query="select 1")
    eng = _eng(monkeypatch, tmp_path, [srcv], [], objects={"views": False})
    eng.clients["src"].dataset("sales").routines["f"] = _routine(
        "f", kind="PROCEDURE")
    res = eng._deep_objects("sales")
    assert res.status == "diff" and "procedure" not in res.detail
    assert "routine f" in res.detail and "view v" not in res.detail
    eng = _eng(monkeypatch, tmp_path, [srcv], [], objects={
        "views": False, "routines": False})
    eng.clients["src"].dataset("sales").routines["f"] = _routine("f")
    assert eng._deep_objects("sales").status == "ok"


def test_a_materialized_view_never_refreshed_on_the_target_is_found(
        monkeypatch, tmp_path):
    src = Table("mv", table_type="MATERIALIZED_VIEW", mview_query="select 1")
    dst = Table("mv", table_type="MATERIALIZED_VIEW", mview_query="select 1",
                mview_last_refresh_time=None)
    eng = _eng(monkeypatch, tmp_path, [src], [dst])
    res = eng._deep_matviews("sales")
    assert (res.status, res.scope) == ("diff", "sales matviews")
    assert "mv: never refreshed on the target" in res.detail
    ok = _eng(monkeypatch, tmp_path, [src], [Table(
        "mv", table_type="MATERIALIZED_VIEW", mview_query="select 1")])
    assert ok._deep_matviews("sales").status == "ok"


# ---- keys, foreign keys --------------------------------------------------

def test_a_key_held_twice_is_found_because_bigquery_refuses_nothing(
        monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, [_orders()], [_orders()])
    eng.clients["dst"].on("having", [[3]], 1 * GIB)
    res = eng._deep_keys("sales")
    assert (res.status, res.scope) == ("diff", "sales keys")
    assert "orders: 3 key values held by more than one row on the target" \
        in res.detail
    # the dry run came first, and only the key's column is read
    asked = eng.clients["dst"].asked[0]
    assert "group by `id`" in asked and "select 1 from" in asked


def test_a_key_that_differs_or_is_missing_is_found(monkeypatch, tmp_path):
    other = _orders(constraints=fake.constraints(primary=["name"]))
    eng = _eng(monkeypatch, tmp_path, [_orders()], [other])
    eng.clients["dst"].on("having", [[0]], 1 * GIB)
    res = eng._deep_keys("sales")
    assert res.status == "diff"
    assert "orders: key ['id'] on the source, ['name'] on the target" \
        in res.detail
    keyless = _orders(constraints=None)
    eng = _eng(monkeypatch, tmp_path, [keyless], [_orders(constraints=None)])
    eng.clients["dst"].on("having", [[0]], 1 * GIB)
    res = eng._deep_keys("sales")
    assert res.status == "warn"
    assert "no key on 1 tables, rows cannot be matched" in res.detail


def test_a_table_too_large_to_read_is_named_never_called_clean(
        monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, [_orders()], [_orders()])
    eng.clients["dst"].on("having", [[0]], 900 * GIB)
    res = eng._deep_keys("sales")
    assert res.status == "warn"
    assert "orders (900.0 GiB)" in res.detail
    # and nothing was run once the dry run said so
    assert eng.clients["dst"].asked == []


def test_a_foreign_key_the_target_lacks_or_breaks_is_found(monkeypatch,
                                                           tmp_path):
    fk = fake.constraints(primary=["id"], foreign=[
        ("customers", [("customer_id", "id")])])
    lines = Table("lines", [Field("id", "INTEGER"),
                            Field("customer_id", "INTEGER")],
                  constraints=fk)
    bare = Table("lines", lines.schema,
                 constraints=fake.constraints(primary=["id"]))
    eng = _eng(monkeypatch, tmp_path, [lines], [bare])
    assert eng.foreign_keys("src", "sales") == [
        ("lines", ["customer_id"], "customers", ["id"])]
    res = eng._deep_fk("sales")
    assert (res.status, res.scope) == ("diff", "sales fk")
    assert "lines(customer_id) -> customers(id): missing on target" \
        in res.detail
    both = _eng(monkeypatch, tmp_path, [lines], [lines])
    both.clients["dst"].on("not exists", [[4]])
    res = both._deep_fk("sales")
    assert res.status == "diff"
    assert "lines(customer_id): 4 rows with no customers row" in res.detail


# ---- columns, narrowing, collation ---------------------------------------

def test_a_column_rule_left_off_the_target_is_found(monkeypatch, tmp_path):
    src = _orders(Field("status", "STRING", "REQUIRED", default="'new'"),
                  Field("ssn", "STRING", tags=["projects/p/tags/pii"]))
    dst = _orders(Field("status", "STRING"), Field("ssn", "STRING"))
    eng = _eng(monkeypatch, tmp_path, [src], [dst])
    res = eng._deep_columns("sales")
    assert (res.status, res.scope) == ("diff", "sales columns")
    assert "orders.status: REQUIRED on the source, NULLABLE on the target" \
        in res.detail
    assert "orders.status: default \"'new'\" on the source, None on the" \
        " target" in res.detail
    assert "orders.ssn: a policy tag restricts it on the source and" \
        " nothing does on the target" in res.detail
    same = _eng(monkeypatch, tmp_path, [src], [src])
    assert same._deep_columns("sales").status == "ok"


def test_the_rules_of_a_table_are_read_as_a_pair_takes_them(monkeypatch,
                                                            tmp_path):
    src = _orders(Field("status", "STRING", "REQUIRED", default="'new'"))
    eng = _eng(monkeypatch, tmp_path, [src], [])
    assert eng.neutral_column_rules("src", "sales", "orders") == {
        "id": {"null": False, "default": None, "identity": False},
        "name": {"null": True, "default": None, "identity": False},
        "status": {"null": False, "default": "'new'", "identity": False}}


def test_a_narrower_column_on_the_target_is_found(monkeypatch, tmp_path):
    src = _orders(Field("code", "STRING", max_length=40),
                  Field("price", "NUMERIC", precision=12, scale=2))
    dst = _orders(Field("code", "STRING", max_length=20),
                  Field("price", "NUMERIC", precision=12, scale=2))
    eng = _eng(monkeypatch, tmp_path, [src], [dst])
    res = eng._deep_narrowing("sales")
    assert (res.status, res.scope) == ("diff", "sales narrowing")
    assert "orders.code: STRING(40) -> STRING(20) (longer values are cut)" \
        in res.detail
    assert "price" not in res.detail


def test_a_collation_that_differs_is_found(monkeypatch, tmp_path):
    ci = {"defaultCollation": "und:ci"}
    src = _orders(api=ci)
    dst = _orders(Field("c", "STRING", collation="und:ci"))
    eng = _eng(monkeypatch, tmp_path, [src], [dst])
    eng.clients["dst"].dataset("sales").is_case_insensitive = True
    res = eng._deep_collation("sales")
    assert (res.status, res.scope) == ("diff", "sales collation")
    assert "table names are case sensitive on the source, insensitive on" \
        " the target" in res.detail
    assert "orders: table collation 'und:ci' on the source, None on the" \
        " target" in res.detail


# ---- policies, indexes, options, grants ----------------------------------

def test_a_row_access_policy_the_target_lacks_is_found(monkeypatch,
                                                       tmp_path):
    eng = _eng(monkeypatch, tmp_path, [_orders()], [_orders()])
    eng.clients["src"].dataset("sales").rowpolicies["orders"] = {
        "th_only": ("region = 'TH'", ["group:th@example.com"])}
    res = eng._deep_rls("sales")
    assert (res.status, res.scope) == ("diff", "sales rls")
    assert "orders: policy th_only missing on target" in res.detail
    eng.clients["dst"].dataset("sales").rowpolicies["orders"] = {
        "th_only": ("region = 'TH'", ["group:th@example.com"])}
    assert eng._deep_rls("sales").status == "ok"
    eng.clients["dst"].dataset("sales").rowpolicies["orders"] = {
        "th_only": ("region = 'US'", ["group:th@example.com"])}
    assert "filters differently" in eng._deep_rls("sales").detail


def test_a_search_index_the_target_lacks_or_has_not_built_is_found(
        monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, [_orders()], [_orders()])
    for side in ("src", "dst"):
        eng.clients[side].on("search_index_columns",
                             [["orders", "ix_name", "name"]])
    eng.clients["src"].on("search_indexes",
                          [["orders", "ix_name", "ACTIVE"]])
    res = eng._deep_indexes("sales")
    assert (res.status, res.scope) == ("diff", "sales indexes")
    assert "search index ix_name on orders: missing on target" in res.detail
    eng.clients["dst"].on("search_indexes", [["orders", "ix_name", "RUNNING"]])
    assert "RUNNING on the target" in eng._deep_indexes("sales").detail
    eng.clients["dst"].answers.insert(0, ("search_indexes",
                                          [["orders", "ix_name", "ACTIVE"]],
                                          0))
    assert eng._deep_indexes("sales").status == "ok"


def test_options_that_delete_the_copy_are_found(monkeypatch, tmp_path):
    import datetime
    soon = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    eng = _eng(monkeypatch, tmp_path, [_orders()],
               [_orders(expires=soon, labels={"team": "x"})])
    eng.clients["dst"].dataset("sales").default_table_expiration_ms = 3600000
    res = eng._deep_options("sales")
    assert (res.status, res.category) == (
        "diff", "structure.collection-options")
    assert "dataset default table expiry (ms): None on the source," \
        " 3600000 on the target" in res.detail
    assert "orders expires: None on the source, '2026-01-01T00:00:00+00:00'" \
        " on the target" in res.detail


def test_access_the_target_gives_everyone_is_a_finding_other_is_a_warning(
        monkeypatch, tmp_path):
    def entry(role, who):
        return NS(role=role, entity_type="userByEmail", entity_id=who)
    eng = _eng(monkeypatch, tmp_path, [_orders()], [_orders()])
    eng.clients["src"].dataset("sales").access_entries = [
        entry("READER", "a@example.com")]
    eng.clients["dst"].dataset("sales").access_entries = [
        entry("READER", "b@example.com")]
    res = eng._deep_grants("sales")
    assert (res.status, res.scope) == ("warn", "sales grants")
    assert "a@example.com has READER on the source only" in res.detail
    eng.clients["dst"].dataset("sales").access_entries.append(
        entry("READER", "allAuthenticatedUsers"))
    res = eng._deep_grants("sales")
    assert res.status == "diff"
    assert "given to everyone on the target" in res.detail
    # a table's own IAM policy is held the same way
    eng = _eng(monkeypatch, tmp_path, [_orders()], [_orders()])
    eng.clients["src"].dataset("sales").iam["orders"] = [
        {"role": "roles/bigquery.dataViewer", "members": ["user:a@x.test"]}]
    res = eng._deep_grants("sales")
    assert res.status == "warn"
    assert "orders: user:a@x.test has roles/bigquery.dataViewer on the" \
        " source only" in res.detail


def test_a_whole_definition_that_differs_is_found(monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, [_orders()], [_orders()])
    eng.clients["src"].on("information_schema.tables", [
        ["orders", "CREATE TABLE `proj-a.sales.orders`(id INT64)"
                   " OPTIONS(description=\"x\")"]])
    eng.clients["dst"].on("information_schema.tables", [
        ["orders", "CREATE TABLE `proj-b.sales.orders`(id INT64)"]])
    res = eng._deep_schema_diff("sales")
    assert (res.check, res.scope, res.status) == (
        "schema", "sales (structural)", "diff")
    assert "1 of 1 tables are defined differently" in res.detail
    eng.clients["dst"].answers[:] = [(
        "information_schema.tables", [
            ["orders", "CREATE TABLE `proj-b.sales.orders`(id INT64)"
                       " OPTIONS(description=\"x\")"]], 0)]
    assert eng._deep_schema_diff("sales").status == "ok"


# ---- the rows, within what a query may read --------------------------------

def test_a_target_past_the_sources_largest_key_is_found(monkeypatch,
                                                        tmp_path):
    eng = _eng(monkeypatch, tmp_path, [_orders()], [_orders()])
    eng.clients["src"].on("max(`id`)", [[10]], 1000)
    eng.clients["dst"].on("max(`id`)", [[12]], 1000)
    res = eng._deep_boundary("sales")
    assert (res.status, res.category) == ("diff", "movement.target-ahead")
    assert "orders.id src_max=10 dst_max=12" in res.detail
    eng.clients["dst"].answers[:] = [("max(`id`)", [[7]], 1000)]
    res = eng._deep_boundary("sales")
    assert res.status == "ok" and "1 behind" in res.detail


def test_null_and_the_empty_string_are_counted_apart(monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, [_orders()], [_orders()])
    eng.clients["src"].on("countif(", [[2, 5]], 1000)
    eng.clients["dst"].on("countif(", [[7, 0]], 1000)
    res = eng._deep_nullempty("sales")
    assert (res.status, res.scope) == ("diff", "sales nullempty")
    assert "orders.name: src null/empty=2|5 dst=7|0" in res.detail


def test_floats_are_held_within_a_tolerance_not_a_checksum(monkeypatch,
                                                           tmp_path):
    t = Table("m", [Field("id", "INTEGER"), Field("v", "FLOAT")])
    eng = _eng(monkeypatch, tmp_path, [t], [t])
    eng.clients["src"].on("coalesce(sum(", [[10, 100.0, 9.0]], 1000)
    eng.clients["dst"].on("coalesce(sum(", [[10, 100.0000000001, 9.0]], 1000)
    assert eng._deep_float("sales").status == "ok"
    eng.clients["dst"].answers[:] = [("coalesce(sum(",
                                      [[10, 101.5, 9.0]], 1000)]
    res = eng._deep_float("sales")
    assert res.status == "diff" and "m.v: sum src=100.0 dst=101.5" \
        in res.detail
    assert res.category == "value.precision"


def test_replacement_characters_the_source_does_not_have_are_found(
        monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, [_orders()], [_orders()])
    eng.clients["src"].on("ufffd", [[1]], 1000)
    eng.clients["dst"].on("ufffd", [[4]], 1000)
    res = eng._deep_charset("sales")
    assert (res.status, res.scope) == ("diff", "sales charset")
    assert "orders.name: 3 extra U+FFFD replacement characters" in res.detail


def test_a_timestamp_shifted_as_a_whole_is_found(monkeypatch, tmp_path):
    t = Table("ev", [Field("id", "INTEGER", "REQUIRED"),
                     Field("at", "TIMESTAMP")],
              constraints=fake.constraints(primary=["id"]))
    eng = _eng(monkeypatch, tmp_path, [t], [t])
    base = 1_700_000_000_000_000
    eng.clients["src"].on("unix_micros", [[i, base + i] for i in range(5)],
                          1000)
    eng.clients["dst"].on("unix_micros",
                          [[i, base + i + 7 * 3600 * 10 ** 6]
                           for i in range(5)], 1000)
    res = eng._deep_timeshift("sales")
    assert res.status == "diff" and res.scope == "sales timeshift"
    assert "ev.at: every row shifted 25200s" in res.detail


def test_an_exotic_column_that_renders_differently_is_found(monkeypatch,
                                                            tmp_path):
    t = Table("geo", [Field("id", "INTEGER", "REQUIRED"),
                      Field("shape", "GEOGRAPHY")],
              constraints=fake.constraints(primary=["id"]))
    eng = _eng(monkeypatch, tmp_path, [t], [t])
    eng.clients["src"].on("to_json_string", [["1", '{"a":1}']], 1000)
    eng.clients["dst"].on("to_json_string", [["1", '{"a":2}']], 1000)
    res = eng._deep_render("sales")
    assert (res.status, res.scope) == ("diff", "sales render")
    assert "geo.shape: renders differently" in res.detail


def test_every_value_check_names_the_table_it_was_kept_from_reading(
        monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, [_orders()], [_orders()])
    eng.clients["src"].on("countif(", [[0, 0]], 400 * GIB)
    res = eng._deep_nullempty("sales")
    assert res.status == "warn"
    assert "orders (400.0 GiB)" in res.detail
    eng = _eng(monkeypatch, tmp_path, [_orders()], [_orders()])
    eng.SCAN_TABLES_MOST = 0
    res = eng._deep_charset("sales")
    assert res.status == "warn" and "past the 0 tables" in res.detail


# ---- all of it ---------------------------------------------------------------

def test_the_deep_check_runs_every_check_and_one_that_fails_does_not_stop(
        monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, [_orders()], [_orders()])

    def _deep_rls(self, db):
        raise RuntimeError("Access Denied: rowAccessPolicies.list")
    monkeypatch.setattr(type(eng), "_deep_rls", _deep_rls)
    got = eng.check_deep("sales")
    scopes = [r.scope for r in got]
    for word in ("partitions", "objects", "matviews", "keys", "fk",
                 "columns", "narrowing", "collation", "rls", "indexes",
                 "grants", "boundary", "nullempty", "float", "timeshift",
                 "charset", "render", "options", "(structural)"):
        assert f"sales {word}" in scopes, word
    rls = next(r for r in got if r.scope == "sales rls")
    assert rls.status == "error" and "Access Denied" in rls.detail


def test_what_a_check_read_is_not_kept_for_the_next(monkeypatch, tmp_path):
    """A table is made or changed between two checks: the listing and the
    metadata one check read are let go when it ends."""
    eng = _eng(monkeypatch, tmp_path, [_orders()], [_orders()])
    first = {r.scope: r.status for r in eng.check_deep("sales")}
    assert first["sales options"] == "ok" and first["sales columns"] == "ok"
    assert "_reads" not in eng.__dict__
    eng.clients["dst"].dataset("sales").tables["orders"].labels = {"a": "b"}
    eng.clients["dst"].dataset("sales").tables["orders"].schema[1] = Field(
        "name", "STRING", "REQUIRED")
    again = {r.scope: r.status for r in eng.check_deep("sales")}
    assert again["sales options"] == "diff"
    assert again["sales columns"] == "diff"


def test_the_listing_is_asked_once_in_a_check(monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, [_orders()], [_orders()])
    asked = []
    real = eng.clients["src"].list_tables
    eng.clients["src"].list_tables = lambda ref: (asked.append(ref),
                                                  real(ref))[1]
    eng.check_deep("sales")
    assert len(asked) == 1
    asked.clear()
    eng._deep_columns("sales")
    eng._deep_columns("sales")
    assert len(asked) == 2

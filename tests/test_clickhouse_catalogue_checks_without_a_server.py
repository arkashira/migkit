"""The deep checks that hold two ClickHouse servers' catalogues to each
other, and the assessment, read from a catalogue answered in this process.

What these hold is migkit's reasoning over what the servers say: which
differences are findings, which are only worth saying, which tables and
objects the hop takes, and what is never claimed. The statements they send
to a server (`system.tables`, `system.columns`, `system.data_skipping_indices`,
`system.row_policies`, `system.grants`, `system.detached_parts`, ...) have
not been run against one here, so none of these is named in `PROVES`: each
cell they build waits in `capabilities.UNPROVED` for a test against a
server (P0-PARITY.verify, P0-PARITY.all).
"""
import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.clickhouse import ClickHouseEngine

TABLE_COLS = ("name", "engine", "partition_key", "sorting_key",
              "primary_key", "engine_full", "storage_policy",
              "create_table_query", "as_select")


def _table(name, engine="MergeTree", part="", order="id", full=None,
           policy="default", create="", query=""):
    return (name, engine, part, order, order, full or f"{engine} ORDER BY "
            f"{order or 'tuple()'}", policy, create, query)


def _col(table, name, typ, kind="", expr="", codec=""):
    return (table, name, typ, kind, expr, codec)


def c_q(column):
    """A column as the statements quote it, inside a call."""
    return f"`{column}`"


class Catalogue:
    """One side of a ClickHouse server as the catalogue says it."""

    def __init__(self, **kw):
        self.tables, self.columns, self.indexes = [], [], []
        self.parts, self.policies, self.grants = [], [], []
        self.users, self.roles, self.functions = {"app"}, set(), []
        self.clusters, self.mutations, self.detached = {"main"}, [], []
        self.nulls, self.tops, self.utf8, self.times = {}, {}, {}, {}
        self.running, self.queues, self.version = [], [], "24.8.1.1"
        self.replicas_bad = 0
        self.missing = False
        self.__dict__.update(kw)

    def answer(self, sql, params):
        params = params or {}
        if "from system.tables where database = {d:String} and not" \
                " is_temporary and not startsWith" in sql:
            return self.tables
        if "from system.columns" in sql and "order by table" in sql:
            return self.columns
        if "from system.data_skipping_indices" in sql:
            return self.indexes
        if "from system.parts" in sql and "partition_id" in sql:
            return self.parts
        if "from system.row_policies" in sql and "select_filter" in sql:
            return self.policies
        if "from system.grants" in sql:
            return self.grants
        if "from system.databases" in sql:
            return [(int(not self.missing),)]
        if "from system.users" in sql:
            return [(u,) for u in sorted(self.users)]
        if "from system.roles" in sql:
            return [(r,) for r in sorted(self.roles)]
        if "from system.functions" in sql:
            return self.functions
        if "from system.clusters" in sql:
            return [(int(params["c"] in self.clusters),)]
        if "from system.detached_parts" in sql:
            return self.detached
        if "from system.mutations" in sql:
            return self.mutations
        if "countIf(isNull(" in sql:
            for (t, c), n in self.nulls.items():
                if f"`{t}`" in sql and f"isNull(`{c}`)" in sql:
                    return [(n,)]
            return [(0,)]
        if "coalesce(max(" in sql:
            for (t, c), n in self.tops.items():
                if f"`{t}`" in sql and f"max(`{c}`)" in sql:
                    return [(n,)]
            return [(0,)]
        if "isValidUTF8" in sql:
            for (t, c), n in self.utf8.items():
                if f"`{t}`" in sql and f"`{c}`" in sql:
                    return [n]
            return [(0, 0)]
        if "toString(" in sql:
            for (t, c), rows in getattr(self, "strings", {}).items():
                if f"`{t}`" in sql and f"toString({c_q(c)})" in sql:
                    return rows
            return []
        if "toUnixTimestamp" in sql:
            self.time_sql = getattr(self, "time_sql", []) + [sql]
            for (t, c), rows in self.times.items():
                if f"`{t}`" in sql and f"({c_q(c)})" in sql:
                    return rows
            return []
        if "select version()" in sql:
            return [(self.version,)]
        if "system.build_options" in sql:
            return [("ClickHouse " + self.version,)]
        if "from system.replicas" in sql:
            return [(self.replicas_bad,)]
        if "from system.processes" in sql:
            return self.running
        if "dependencies_table" in sql:
            return self.queues
        return []


@pytest.fixture
def pair(monkeypatch, tmp_path):
    """(source, target) catalogues, and an engine over them."""
    src, dst = Catalogue(), Catalogue()

    def rows(eng, side, db, sql, params=None, settings=None):
        return (src if side == "src" else dst).answer(sql, params)
    monkeypatch.setattr(ClickHouseEngine, "_rows", rows)

    def engine(**kw):
        hop = Hop(name="ch", engine="clickhouse",
                  source=Endpoint(host="10.0.0.1", port=8123, user="u",
                                  password="CHANGE_ME"),
                  target=Endpoint(host="10.0.0.2", port=8123, user="u",
                                  password="CHANGE_ME"),
                  databases=["app"], **kw)
        hop.report_dir = lambda db=None: tmp_path
        return ClickHouseEngine(hop)
    return src, dst, engine


def _found(eng, word, **kw):
    """The one finding whose scope ends in `word`."""
    got = [r for r in eng.check_deep("app") if r.scope == f"app {word}"]
    assert len(got) == 1, [r.scope for r in eng.check_deep("app")]
    return got[0]


def _same(src, dst, *tables):
    for side in (src, dst):
        side.tables += list(tables)


def _both(src, dst, *columns):
    for side in (src, dst):
        side.columns += list(columns)


# ---- columns ----------------------------------------------------------------

def test_a_column_that_drifted_is_named_with_both_declarations(pair):
    src, dst, engine = pair
    _same(src, dst, _table("orders"))
    src.columns += [_col("orders", "id", "UInt64"),
                    _col("orders", "note", "String", codec="CODEC(ZSTD(3))"),
                    _col("orders", "qty", "Int32"),
                    _col("orders", "gone", "String")]
    dst.columns += [_col("orders", "id", "UInt64"),
                    _col("orders", "note", "String", codec="CODEC(LZ4)"),
                    _col("orders", "qty", "Nullable(Int16)"),
                    _col("orders", "extra", "String")]
    got = _found(engine(), "columns")
    assert got.status == "diff" and got.category == "structure.columns"
    for part in ("orders.gone: on the source only",
                 "orders.extra: on the target only",
                 "orders.qty: Int32 on the source, Nullable(Int16) on the"
                 " target"):
        assert part in got.detail or part in open(got.report).read(), part
    assert "orders.note" in open(got.report).read()


def test_nullable_and_low_cardinality_are_not_a_column_drift(pair):
    src, dst, engine = pair
    _same(src, dst, _table("orders"))
    src.columns += [_col("orders", "kind", "String")]
    dst.columns += [_col("orders", "kind", "LowCardinality(Nullable(String))")]
    assert _found(engine(), "columns").status == "ok"


# ---- computed columns, NULLs, narrowing, precision ---------------------------

def test_a_computed_column_that_is_stored_on_the_target_is_a_finding(pair):
    src, dst, engine = pair
    _same(src, dst, _table("orders"))
    src.columns += [_col("orders", "id", "UInt64"),
                    _col("orders", "tax", "Float64", "MATERIALIZED",
                         "price * 0.2")]
    dst.columns += [_col("orders", "id", "UInt64"),
                    _col("orders", "tax", "Float64")]
    got = _found(engine(), "generated")
    assert got.status == "diff" and got.category == "value.generated"
    assert "orders.tax: MATERIALIZED price * 0.2 on the source, stored on" \
        " the target" in open(got.report).read()
    src.columns, dst.columns = src.columns[:1], dst.columns[:1]
    assert _found(engine(), "generated").status == "ok"


def test_a_null_the_target_column_cannot_hold_is_counted_on_the_source(pair):
    src, dst, engine = pair
    _same(src, dst, _table("orders"))
    src.columns += [_col("orders", "note", "Nullable(String)"),
                    _col("orders", "memo", "Nullable(String)")]
    dst.columns += [_col("orders", "note", "String"),
                    _col("orders", "memo", "String")]
    src.nulls = {("orders", "note"): 12}
    got = _found(engine(), "nullempty")
    assert got.status == "diff" and got.category == "value.null-empty"
    assert "orders.note: 12 rows hold NULL" in got.detail
    assert "memo" not in got.detail
    src.nulls = {}
    got = _found(engine(), "nullempty")
    assert got.status == "warn" and "none holds one now" in got.detail


def test_a_narrower_target_column_is_found_by_the_shared_reasoning(pair):
    src, dst, engine = pair
    _same(src, dst, _table("orders"))
    src.columns += [_col("orders", "qty", "Int64"),
                    _col("orders", "id", "UInt32")]
    dst.columns += [_col("orders", "qty", "Int32"),
                    _col("orders", "id", "UInt64")]
    got = _found(engine(), "narrowing")
    assert got.status == "diff" and got.category == "value.narrowing"
    assert "orders.qty" in got.detail and "orders.id" not in got.detail


def test_fewer_digits_on_the_target_are_found(pair):
    src, dst, engine = pair
    _same(src, dst, _table("orders"))
    src.columns += [_col("orders", "score", "Float64"),
                    _col("orders", "at", "DateTime64(9)"),
                    _col("orders", "price", "Decimal(18, 4)"),
                    _col("orders", "seen", "DateTime64(3)")]
    dst.columns += [_col("orders", "score", "Float32"),
                    _col("orders", "at", "DateTime64(3)"),
                    _col("orders", "price", "Decimal(18, 2)"),
                    _col("orders", "seen", "DateTime")]
    got = _found(engine(), "float")
    assert got.status == "diff" and got.category == "value.precision"
    every = open(got.report).read()
    for line in ("orders.score: Float64 -> Float32",
                 "orders.at: DateTime64(9) -> DateTime64(3)",
                 "orders.price: Decimal(18, 4) -> Decimal(18, 2) (scale 4"
                 " -> 2: rounds)",
                 "orders.seen: DateTime64(3) -> DateTime (the fraction of a"
                 " second is dropped)"):
        assert line in every, line


# ---- keys, partitions, indexes, table options ---------------------------------

def test_a_table_ordered_another_way_is_a_finding_and_a_keyless_one_is_said(
        pair):
    src, dst, engine = pair
    src.tables += [_table("orders", order="id"), _table("log", order="")]
    dst.tables += [_table("orders", order="at, id"), _table("log", order="")]
    got = _found(engine(), "keys")
    assert got.status == "diff" and got.category == "structure.keys"
    assert "orders: ordered by (id) on the source, (at, id)" in got.detail
    dst.tables[0] = _table("orders", order="id")
    got = _found(engine(), "keys")
    assert got.status == "warn" and "log" in got.detail


def test_partitions_are_held_to_each_other_by_the_shared_verdict(pair):
    src, dst, engine = pair
    src.tables += [_table("events", part="toYYYYMM(at)")]
    dst.tables += [_table("events", part="toYYYYMM(at)")]
    src.parts = [("events", "202401", 10), ("events", "202402", 20)]
    dst.parts = [("events", "202401", 10), ("events", "202402", 19)]
    got = _found(engine(), "partitions")
    assert got.status == "diff" and got.category == "structure.partitions"
    assert "events partition 202402: src=20 dst=19 rows" in got.detail
    dst.parts = list(src.parts)
    assert _found(engine(), "partitions").status == "ok"
    # another key on the target is said, and its partitions not compared
    dst.tables = [_table("events", part="toYYYYMMDD(at)")]
    got = _found(engine(), "partitions")
    assert got.status == "diff" and "partitioned by toYYYYMM(at)" in got.detail
    # nothing partitioned
    src.tables = dst.tables = [_table("events")]
    assert "no partitioned tables" in _found(engine(), "partitions").detail


def test_a_data_skipping_index_missing_or_changed_on_the_target(pair):
    src, dst, engine = pair
    _same(src, dst, _table("orders"))
    src.indexes = [("orders", "by_region", "bloom_filter", "region", 4),
                   ("orders", "by_qty", "minmax", "qty", 2)]
    dst.indexes = [("orders", "by_qty", "minmax", "qty", 8)]
    got = _found(engine(), "indexes")
    assert got.status == "diff" and got.category == "structure.indexes"
    assert "orders.by_region: not on the target" in got.detail
    assert "orders.by_qty" in got.detail
    dst.indexes = list(src.indexes) + [("orders", "more", "minmax", "x", 1)]
    got = _found(engine(), "indexes")
    assert got.status == "ok" and "1 more on the target" in got.detail


def test_a_ttl_that_differs_changes_what_the_target_deletes(pair):
    src, dst, engine = pair
    src.tables += [_table("events", full="MergeTree ORDER BY id TTL at + "
                                         "toIntervalDay(30) SETTINGS "
                                         "index_granularity = 8192")]
    dst.tables += [_table("events", full="MergeTree ORDER BY id SETTINGS "
                                         "index_granularity = 8192")]
    got = _found(engine(), "table options")
    assert got.status == "diff"
    assert got.category == "structure.collection-options"
    assert "events: TTL at + toIntervalDay(30) on the source, none on the" \
        " target" in got.detail
    # a setting alone is said, not failed
    dst.tables = [_table("events", full="MergeTree ORDER BY id TTL at + "
                                        "toIntervalDay(30) SETTINGS "
                                        "index_granularity = 4096")]
    assert _found(engine(), "table options").status == "warn"
    dst.tables = list(src.tables)
    assert _found(engine(), "table options").status == "ok"


# ---- views, materialized views, definers, row policies, sharding --------------

def test_views_dictionaries_and_functions_are_the_hops_to_carry(pair):
    src, dst, engine = pair
    src.tables += [_table("v_open", "View", order="", query="SELECT 1"),
                   _table("v_audit", "View", order="", query="SELECT 2"),
                   _table("dict_geo", "Dictionary", order="")]
    dst.tables += [_table("v_open", "View", order="", query="SELECT 9")]
    src.functions = [("f_tax", "CREATE FUNCTION f_tax AS x -> x * 2")]
    got = _found(engine(), "objects")
    assert got.status == "diff" and got.category == "structure.objects"
    for line in ("view v_audit: not on the target",
                 "view v_open: defined by another query on the target",
                 "dictionary dict_geo: not on the target",
                 "function f_tax: not on the target"):
        assert line in open(got.report).read(), line
    # what the hop leaves out is nobody's finding
    eng = engine(exclude=["app.v_audit", "app.dict_geo", "app.v_open"],
                 objects={"functions": False})
    assert _found(eng, "objects").status == "ok"


def test_a_materialized_view_missing_or_writing_elsewhere_is_a_finding(pair):
    src, dst, engine = pair
    src.tables += [
        _table("mv_a", "MaterializedView", order="",
               create="CREATE MATERIALIZED VIEW app.mv_a TO app.totals AS"
                      " SELECT 1", query="SELECT 1"),
        _table("mv_b", "MaterializedView", order="",
               create="CREATE MATERIALIZED VIEW app.mv_b TO app.t2 AS"
                      " SELECT 1", query="SELECT 1")]
    dst.tables += [_table(
        "mv_a", "MaterializedView", order="",
        create="CREATE MATERIALIZED VIEW app.mv_a TO app.other AS SELECT 1",
        query="SELECT 1")]
    got = _found(engine(), "matviews")
    assert got.status == "diff"
    assert got.category == "structure.materialized-views"
    assert "mv_b: not on the target" in got.detail
    assert "mv_a: writes into totals on the source and other on the target" \
        in open(got.report).read()


def test_a_view_that_runs_with_another_account_is_a_finding(pair):
    src, dst, engine = pair
    src.tables += [_table("v", "View", order="", create=(
        "CREATE VIEW app.v DEFINER = app SQL SECURITY DEFINER AS SELECT 1"))]
    dst.tables += [_table("v", "View", order="", create=(
        "CREATE VIEW app.v DEFINER = migkit SQL SECURITY DEFINER AS "
        "SELECT 1"))]
    got = _found(engine(), "ownership")
    assert got.status == "diff"
    assert got.category == "access.object-ownership"
    assert "app/DEFINER -> migkit/DEFINER" in got.detail
    dst.tables = list(src.tables)
    assert _found(engine(), "ownership").status == "ok"


def test_a_row_policy_missing_on_the_target_leaves_its_rows_open(pair):
    src, dst, engine = pair
    _same(src, dst, _table("orders"), _table("audit_log"))
    src.policies = [("orders", "mine", "tenant = 7", 0, 0, ["app"], []),
                    ("audit_log", "all", "1", 0, 1, [], [])]
    dst.policies = [("orders", "mine", "tenant = 8", 0, 0, ["app"], [])]
    got = _found(engine(exclude=["app.audit_log"]), "rls")
    assert got.status == "diff" and got.category == "structure.row-security"
    assert "orders: policy mine filters another way" in got.detail
    assert "audit_log" not in got.detail
    dst.policies = list(src.policies)
    assert _found(engine(), "rls").status == "ok"


def test_a_distributed_table_over_a_cluster_the_target_lacks(pair):
    src, dst, engine = pair
    src.tables += [_table("d_orders", "Distributed", order="", full=(
        "Distributed('main', 'app', 'orders', cityHash64(id))"))]
    dst.tables += [_table("d_orders", "Distributed", order="", full=(
        "Distributed('edge', 'app', 'orders', rand())"))]
    got = _found(engine(), "sharding")
    assert got.status == "diff" and got.category == "structure.sharding"
    assert "cluster main sharded by cityHash64(id) on the source, cluster" \
        " edge sharded by rand() on the target" in open(got.report).read()
    assert "cluster edge is not defined on the target" \
        in open(got.report).read()
    dst.tables = list(src.tables)
    assert _found(engine(), "sharding").status == "ok"


# ---- the values themselves ----------------------------------------------------

def test_a_target_ahead_of_the_source_is_found(pair):
    src, dst, engine = pair
    _same(src, dst, _table("orders"))
    _both(src, dst, _col("orders", "id", "UInt64"))
    src.tops, dst.tops = {("orders", "id"): 3}, {("orders", "id"): 100}
    got = _found(engine(), "boundary")
    assert got.status == "diff" and got.category == "movement.target-ahead"
    assert "orders src_max=3 dst_max=100" in got.detail
    dst.tops = {("orders", "id"): 2}
    assert _found(engine(), "boundary").status == "ok"


def test_text_that_stopped_being_utf8_or_turned_to_replacement_marks(pair):
    src, dst, engine = pair
    _same(src, dst, _table("notes"))
    _both(src, dst, _col("notes", "body", "String"),
          _col("notes", "blob", "FixedString(4)"))
    src.utf8 = {("notes", "body"): (2, 0)}
    dst.utf8 = {("notes", "body"): (2, 5)}
    got = _found(engine(), "charset")
    assert got.status == "diff" and got.category == "value.charset"
    assert "notes.body: 5 more U+FFFD replacement characters" in got.detail
    assert "1 text columns read" in got.detail
    dst.utf8 = {("notes", "body"): (2, 0)}
    assert _found(engine(), "charset").status == "ok"


def test_a_value_the_two_servers_write_as_other_text_is_named_by_column(pair):
    src, dst, engine = pair
    _same(src, dst, _table("readings"))
    _both(src, dst, _col("readings", "id", "UInt64"),
          _col("readings", "ratio", "Float64"),
          _col("readings", "tags", "Map(String, UInt8)"),
          _col("readings", "name", "String"))
    src.strings = {("readings", "ratio"): [(1, "0.1"), (2, "0.25")],
                   ("readings", "tags"): [(1, "{'a':1}")]}
    dst.strings = {("readings", "ratio"): [(1, "0.10000000000000001"),
                                           (2, "0.25")],
                   ("readings", "tags"): [(1, "{'a':1}")]}
    got = _found(engine(), "render")
    assert got.status == "diff" and got.category == "value.rendering"
    assert "readings.ratio: renders differently (source '0.1', target" \
        " '0.10000000000000001')" in got.detail
    assert "tags" not in got.detail and "name" not in got.detail
    dst.strings = dict(src.strings)
    got = _found(engine(), "render")
    assert got.status == "ok" and "2 exotic-typed columns sampled" \
        in got.detail


def test_a_time_column_shifted_as_a_whole_is_a_zone_not_row_damage(pair):
    src, dst, engine = pair
    _same(src, dst, _table("events"))
    _both(src, dst, _col("events", "id", "UInt64"),
          _col("events", "at", "DateTime"))
    src.times = {("events", "at"): [(1, 1000), (2, 2000), (3, 3000)]}
    dst.times = {("events", "at"): [(1, 1000 + 25200), (2, 2000 + 25200),
                                    (3, 3000 + 25200)]}
    got = _found(engine(), "timeshift")
    assert got.status == "diff" and got.category == "value.timezone"
    assert "events.at: every row shifted 25200s" in got.detail
    dst.times = dict(src.times)
    assert _found(engine(), "timeshift").status == "ok"
    # a time of finer ticks is read as whole seconds, by the server
    _both(src, dst, _col("events", "at64", "Nullable(DateTime64(3))"))
    src.times[("events", "at64")] = [(1, 5), (2, 6), (3, 7)]
    dst.times[("events", "at64")] = [(1, 5 + 3600), (2, 6 + 3600),
                                     (3, 7 + 3600)]
    got = _found(engine(), "timeshift")
    assert "events.at64: every row shifted 3600s" in got.detail
    assert any("intDiv(toUnixTimestamp64Milli(`at64`), 1000)" in q
               for q in src.time_sql), src.time_sql


# ---- grants -----------------------------------------------------------------

def test_a_grant_the_target_lacks_or_adds_is_a_finding(pair):
    src, dst, engine = pair
    _same(src, dst, _table("orders"), _table("audit_log"))
    src.users, dst.users = {"app", "etl", "ghost"}, {"app", "etl"}
    src.grants = [("app", None, "SELECT", "app", "orders", None, 0, 0),
                  ("app", None, "INSERT", "app", "orders", None, 0, 0),
                  ("etl", None, "SELECT", "app", "audit_log", None, 0, 0),
                  ("ghost", None, "SELECT", "app", "orders", None, 0, 0),
                  ("etl", None, "SELECT", None, None, None, 0, 0),
                  ("default", None, "ALL", None, None, None, 0, 1)]
    dst.grants = [("app", None, "SELECT", "app", "orders", None, 0, 0),
                  ("app", None, "ALTER", "app", "orders", None, 0, 0),
                  ("etl", None, "SELECT", "app", "audit_log", None, 0, 0)]
    got = [r for r in engine(exclude=["app.audit_log"]).check_deep("app")
           if r.scope == "app grants"]
    assert len(got) == 1 and got[0].status == "diff"
    assert got[0].category == "access.table-grants"
    assert "app: INSERT on orders" in got[0].detail
    assert "app: ALTER on orders" in got[0].detail
    assert "ghost" in got[0].detail
    # a grant on the whole server reaches these tables too
    assert "etl: SELECT on *.*" in got[0].detail
    dst.grants += [("app", None, "INSERT", "app", "orders", None, 0, 0),
                   ("etl", None, "SELECT", None, None, None, 0, 0)]
    dst.grants.remove(("app", None, "ALTER", "app", "orders", None, 0, 0))
    dst.users.add("ghost")
    dst.grants += [("ghost", None, "SELECT", "app", "orders", None, 0, 0)]
    got = [r for r in engine(exclude=["app.audit_log"]).check_deep("app")
           if r.scope == "app grants"]
    assert got[0].status == "ok"


def test_a_hop_that_carries_no_accounts_does_not_compare_their_grants(pair):
    src, dst, engine = pair
    got = [r for r in engine(options={"accounts": False}).check_deep("app")
           if r.scope == "app grants"]
    assert got[0].status == "skip"


# ---- a check that cannot read says so ------------------------------------------

def test_a_check_whose_catalogue_cannot_be_read_is_an_error_not_a_pass(
        monkeypatch, tmp_path):
    def rows(eng, side, db, sql, params=None, settings=None):
        if "system.row_policies" in sql:
            raise RuntimeError("not enough privileges")
        return []
    monkeypatch.setattr(ClickHouseEngine, "_rows", rows)
    hop = Hop(name="ch", engine="clickhouse", source=Endpoint(),
              target=Endpoint(), databases=["app"])
    hop.report_dir = lambda db=None: tmp_path
    got = {r.scope: r for r in ClickHouseEngine(hop).check_deep("app")}
    assert got["app rls"].status == "error"
    assert "not enough privileges" in got["app rls"].detail
    assert got["app columns"].status == "ok"


# ---- the assessment ---------------------------------------------------------

def test_detached_parts_hold_rows_no_copy_carries(pair):
    src, dst, engine = pair
    _same(src, dst, _table("orders"))
    src.detached = [("orders", 2), ("audit_log", 9)]
    got = engine(exclude=["app.audit_log"])._source_unsettled("app")
    assert got.status == "diff" and "2 parts of 1 tables" in got.detail
    src.detached = []
    src.mutations = [("orders", "DELETE WHERE 1", "")]
    assert engine()._source_unsettled("app").status == "warn"
    src.mutations = []
    assert engine()._source_unsettled("app").status == "ok"


def test_the_versions_and_the_brand_come_from_the_servers(pair):
    src, dst, engine = pair
    src.version, dst.version = "24.3.5.48", "24.8.4.13"
    eng = engine()
    assert eng._server_versions() == ("24.3.5.48", "24.8.4.13")
    source, target = eng._brands()
    assert (source.name, target.name) == ("clickhouse", "clickhouse")
    assert target.version == "24.8.4.13"
    src.version = "22.8.15.25.altinitystable"
    assert engine()._brands()[0].name == "altinity"


def test_the_assessment_names_what_the_move_leaves_for_hands(pair):
    src, dst, engine = pair
    src.version = dst.version = "24.8.4.13"

    def answer(self, sql, params=None, real=Catalogue.answer):
        if "endsWith(engine, 'MergeTree')" in sql:
            return [("log",), ("audit_log",)]
        if "engine in ('View', 'MaterializedView'" in sql:
            return [("v_orders", "View"), ("v_audit", "View")]
        if "engine = 'Dictionary'" in sql:
            return [("dict_geo",)]
        if "from system.row_policies" in sql:
            return [("orders", "mine")]
        if "engine = 'Distributed'" in sql:
            return [("d_orders",)]
        if "engine in ('Kafka'" in sql:
            return [("q_events",)]
        return real(self, sql, params)
    src.answer = answer.__get__(src)
    src.functions = [("f_tax", "CREATE FUNCTION f_tax AS x -> x")]
    eng = engine(exclude=["app.audit_log", "app.v_audit"])
    inv = eng._handwork()
    text = " ".join(r["detail"] for r in inv.rows())
    assert "log" in text and "audit_log" not in text
    assert "v_orders" in text and "v_audit" not in text
    for word in ("dict_geo", "orders.mine", "d_orders", "q_events", "f_tax"):
        assert word in text, word
    items = {i["item"]: i for i in eng._assess_extra()}
    assert items["target deletes by key"]["level"] == "pass"
    dst.version = "22.8.1.1"
    items = {i["item"]: i for i in engine()._assess_extra()}
    assert items["target deletes by key"]["level"] == "warn"
    dst.replicas_bad = 2
    items = {i["item"]: i for i in engine()._assess_extra()}
    assert items["target replicas writable"]["level"] == "warn"
    assert items["source replicas writable"]["level"] == "pass"


def test_a_reading_that_failed_is_unknown_never_none(monkeypatch, tmp_path):
    def rows(eng, side, db, sql, params=None, settings=None):
        if "from system.tables" in sql and "'Dictionary'" in sql:
            raise RuntimeError("no access to system.tables")
        if "show databases" in sql:
            return [("app",)]
        return []
    monkeypatch.setattr(ClickHouseEngine, "_rows", rows)
    hop = Hop(name="ch", engine="clickhouse", source=Endpoint(),
              target=Endpoint(), databases=["app"])
    hop.report_dir = lambda db=None: tmp_path
    out = ClickHouseEngine(hop)._handwork().rows()
    assert any("UNKNOWN" in r["detail"] and "dictionaries" in r["detail"]
               for r in out), out


def test_a_target_not_made_yet_has_no_other_writer(pair):
    src, dst, engine = pair
    dst.missing, dst.running = True, [("etl", 2)]
    assert engine().stream_writers("app") == []


def test_another_writer_on_the_target_is_named_and_migkits_own_is_not(pair):
    src, dst, engine = pair
    dst.queues = [("q_events", "Kafka"), ("q_audit", "Kafka")]
    dst.running = [("etl", 2)]
    got = engine(exclude=["app.q_audit"]).stream_writers("app")
    assert got == [("Kafka table q_events reading a stream into the tables"
                    " it feeds", False),
                   ("2 insert(s) running now from etl", False)]


# ---- the schema other engines are given ----------------------------------------

def test_a_columns_nullability_and_default_are_what_the_catalogue_says(pair):
    src, dst, engine = pair
    src.answer_columns = None

    def answer(self, sql, params=None, real=Catalogue.answer):
        if "default_kind," in sql and "default_expression from" in sql:
            return [("id", "UInt64", "", ""),
                    ("note", "Nullable(String)", "", ""),
                    ("status", "String", "DEFAULT", "'new'"),
                    ("tax", "Float64", "MATERIALIZED", "price * 0.2")]
        return real(self, sql, params)
    src.answer = answer.__get__(src)
    got = engine().neutral_column_rules("src", "app", "orders")
    assert got == {
        "id": {"null": False, "default": None, "identity": False},
        "note": {"null": True, "default": None, "identity": False},
        "status": {"null": False, "default": "'new'", "identity": False},
        "tax": {"null": False, "default": None, "identity": False}}


def test_a_table_is_made_with_the_sources_not_null_and_default(pair):
    _, _, engine = pair
    from migkit import canon
    sql = engine().neutral_create_sql(
        "dst", "app", "orders",
        [("id", "integer", (), {"null": False}),
         ("note", "text", (), {"null": True}),
         ("status", "text", (), {"null": False, "default": "'new'"}),
         ("seq", "integer", (), {"null": True, "identity": True}),
         ("plain", "text", ())], key=("id",))
    assert sql == (
        "create table `app`.`orders` (`id` " + canon.ddl_type(
            "clickhouse", "integer", ()) + ", `note` Nullable("
        + canon.ddl_type("clickhouse", "text", ()) + "), `status` "
        + canon.ddl_type("clickhouse", "text", ()) + " default 'new',"
        " `seq` Nullable(" + canon.ddl_type("clickhouse", "integer", ())
        + "), `plain` Nullable(" + canon.ddl_type("clickhouse", "text", ())
        + ")) engine = MergeTree order by (`id`)")


def test_an_index_is_a_skipping_index_and_a_unique_one_is_made_as_one_that_is_not(
        pair):
    src, dst, engine = pair

    def answer(self, sql, params=None, real=Catalogue.answer):
        if "from system.columns" in sql and "table = {t:String}" in sql \
                and "order by position" in sql:
            return [("id", "UInt64"), ("region", "String"),
                    ("at", "DateTime")]
        if "from system.data_skipping_indices" in sql:
            return [("by_region", "region"), ("by_day", "toDate(at)"),
                    ("by_pair", "(id, region)")]
        return real(self, sql, params)
    dst.answer = src.answer = answer.__get__(src)
    eng = engine()
    assert eng.neutral_indexes("src", "app", "orders") == [
        ("by_region", False, ["region"], True),
        ("by_day", False, [], False),
        ("by_pair", False, ["id", "region"], True)]
    assert eng.neutral_create_index_sql(
        "dst", "app", "orders", "u", True, ["id", "at"]) == (
        "alter table `app`.`orders` add index `u` (`id`, `at`) type minmax"
        " granularity 4")
    assert eng.neutral_create_index_sql(
        "dst", "app", "orders", "r", False, ["region"]).endswith(
        "type bloom_filter(0.025) granularity 4")


def test_an_index_added_is_built_for_the_rows_already_there(monkeypatch,
                                                           tmp_path):
    sent = []

    class Client:
        def command(self, sql, parameters=None, settings=None):
            sent.append((sql, settings))

        def close(self):
            pass
    monkeypatch.setattr(ClickHouseEngine, "_client",
                        lambda self, side, db=None, wait=None: Client())
    hop = Hop(name="ch", engine="clickhouse", source=Endpoint(),
              target=Endpoint(), databases=["app"])
    eng = ClickHouseEngine(hop)
    eng.execute_ddl("dst", "app", "alter table `app`.`orders` add index"
                    " `by_region` (`region`) type minmax granularity 4")
    assert sent[1] == ("alter table `app`.`orders` materialize index"
                       " `by_region`", {"mutations_sync": 2})
    eng.execute_ddl("dst", "app", "alter table `app`.`orders` drop index x")
    assert len(sent) == 3
    with pytest.raises(SystemExit, match="on a source"):
        eng.execute_ddl("src", "app", "alter table t add index i (c)")


def test_views_are_the_hops_and_a_materialized_one_is_not_a_view(pair):
    src, dst, engine = pair

    def answer(self, sql, params=None, real=Catalogue.answer):
        if "select name, as_select from system.tables" in sql:
            assert "engine = 'View'" in sql
            return [("v_open", "SELECT 1"), ("v_audit", "SELECT 2")]
        return real(self, sql, params)
    src.answer = answer.__get__(src)
    assert engine(exclude=["app.v_audit"]).neutral_views("src", "app") == [
        ("v_open", "SELECT 1")]
    assert engine(objects={"views": False}).neutral_views("src", "app") == []


def test_a_default_works_where_the_server_evaluates_it(monkeypatch, tmp_path):
    seen = []

    def rows(eng, side, db, sql, params=None, settings=None):
        seen.append((sql, settings))
        if "order_id" in sql:
            raise RuntimeError("unknown identifier order_id")
        return [(1,)]
    monkeypatch.setattr(ClickHouseEngine, "_rows", rows)
    eng = ClickHouseEngine(Hop(name="ch", engine="clickhouse",
                               source=Endpoint(), target=Endpoint()))
    assert eng.default_works("dst", "app", "now()") is True
    assert eng.default_works("dst", "app", "order_id + 1") is False
    assert seen[0] == ("select (now())", {"readonly": 1})


# ---- scheduled jobs: views that refresh themselves --------------------------

def _jobs_pair(monkeypatch, tmp_path, **kw):
    """A hop over a source holding two refreshing views and a target holding
    none, and the statements the servers were sent."""
    sent = []
    state = {"src": {"nightly": "Scheduled", "audit_roll": "Scheduled"},
             "dst": {}}

    def rows(eng, side, db, sql, params=None, settings=None):
        if "from system.view_refreshes" in sql:
            return sorted(state[side].items())
        if "engine = 'MaterializedView'" in sql:
            return [("nightly", "SELECT sum(x) FROM orders"),
                    ("audit_roll", "SELECT * FROM audit_log")]
        if "select name from system.tables where database" in sql:
            return [("orders",), ("audit_log",)]
        return []

    class Client:
        def __init__(self, side):
            self.side = side

        def command(self, sql, parameters=None, settings=None):
            sent.append((self.side, sql))
            _, _, view = sql.rpartition(".")
            view = view.strip("`")
            state[self.side][view] = "Disabled" if sql.startswith(
                "system stop") else "Scheduled"

        def close(self):
            pass
    monkeypatch.setattr(ClickHouseEngine, "_rows", rows)
    monkeypatch.setattr(ClickHouseEngine, "_client",
                        lambda self, side, db=None, wait=None: Client(side))
    hop = Hop(name="ch", engine="clickhouse",
              source=Endpoint(host="10.0.0.1", port=8123, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=8123, user="u",
                              password="CHANGE_ME"), databases=["app"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return hop, ClickHouseEngine(hop), sent, state


def test_a_refreshing_view_runs_on_one_side_at_a_time(monkeypatch, tmp_path):
    from migkit import jobs
    hop, eng, sent, state = _jobs_pair(monkeypatch, tmp_path)
    assert jobs.inventory(hop, eng, "app", "src") == [("audit_roll", True),
                                                      ("nightly", True)]
    assert jobs.switch(hop, eng, "app", "src", ["nightly"], False,
                       lambda m: None) == 1
    assert sent == [("src", "system stop view `app`.`nightly`")]
    assert [e["name"] for e in jobs.recorded(hop)] == ["nightly"]
    # on the target at the flip, from the record
    state["dst"]["nightly"] = "Disabled"
    assert jobs.switch(hop, eng, "app", "dst", ["nightly"], True,
                       lambda m: None) == 1
    assert sent[-1] == ("dst", "system start view `app`.`nightly`")
    # a switch already made is not made again
    assert jobs.switch(hop, eng, "app", "src", ["nightly"], False,
                       lambda m: None) == 0
    # and every switch is put back, the last first
    jobs.undo(hop, eng, lambda m: None)
    assert sent[-2:] == [("dst", "system stop view `app`.`nightly`"),
                         ("src", "system start view `app`.`nightly`")]
    assert jobs.recorded(hop) == []


def test_a_refreshing_view_that_writes_only_what_the_hop_leaves_out_stays(
        monkeypatch, tmp_path):
    from migkit import jobs
    hop, eng, sent, _ = _jobs_pair(monkeypatch, tmp_path,
                                   exclude=["app.audit_log"])
    assert jobs.inventory(hop, eng, "app", "src") == [("nightly", True)]
    assert jobs.left_alone(hop, eng, "app", "src") == ["audit_roll"]
    assert sent == []

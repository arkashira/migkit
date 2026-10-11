"""TiDB's deep check finds what the MySQL wire's deep check finds
(P0-PARITY.verify), without a server: the engine's own `check_deep` and
`check_objects` over the answers a TiDB catalogue gives.

TiDB answers MySQL's `information_schema`, so the MySQL engine's deep check
reads it as it is: a table with no key, an orphan row, a column that
drifted or narrowed, a time shifted by a zone, a charset downgraded, a
partition that arrived empty, a generated column, a collation that
collapses a unique key, a target ahead, NULL turned into empty, a float
off its sum, a CHECK that is not enforced, a grant and a definer that did
not cross. TiDB runs it behind its own `ADMIN CHECK TABLE`, and without the
parts of it that read a server TiDB is not (a GTID replica, the
statistics, the compiled collations, `max_allowed_packet`).

Each case answers only the catalogue statement it is about, from a source
and a target that differ in exactly that.
"""
import re

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines import mysql
from migkit.engines.tidb import TiDBEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("scope.objects", "tidb"),
    ("verify.structure.objects", "tidb"),
    ("verify.structure.keys", "tidb"),
    ("verify.structure.foreign-keys", "tidb"),
    ("verify.structure.columns", "tidb"),
    ("verify.structure.partitions", "tidb"),
    ("verify.structure.unvalidated-constraints", "tidb"),
    ("verify.value.narrowing", "tidb"),
    ("verify.value.timezone", "tidb"),
    ("verify.value.charset", "tidb"),
    ("verify.value.generated", "tidb"),
    ("verify.value.collation", "tidb"),
    ("verify.value.null-empty", "tidb"),
    ("verify.value.precision", "tidb"),
    ("verify.movement.target-ahead", "tidb"),
    ("verify.access.table-grants", "tidb"),
    ("verify.access.object-ownership", "tidb"),
    ("move.partitioned", "tidb"),
]

EP = Endpoint(host="x", port=1, user="x", password="x")


def _norm(sql):
    return " ".join(sql.lower().split())


class Catalogue:
    """The answers of a TiDB catalogue, by what a statement says: the
    first rule whose tokens all appear in the statement (and whose side
    is the one asked) answers; a statement no rule knows is answered with
    no rows."""

    def __init__(self):
        self.rules = []
        self.asked = []

    def on(self, *tokens, src=None, dst=None, rows=None):
        """`src`/`dst` are the rows each side answers; `rows` a function
        of (side, sql, args) where the answer is built."""
        self.rules.append((tuple(t.lower() for t in tokens), src, dst, rows))
        return self

    def ask(self, side, sql, args=None):
        text = _norm(sql)
        self.asked.append((side, text, tuple(args or ())))
        for tokens, src, dst, rows in self.rules:
            if all(t in text for t in tokens):
                if rows is not None:
                    return rows(side, sql, args)
                got = src if side == "src" else dst
                if got is not None:
                    return got
        return []

    def said(self, token):
        return [a for a in self.asked if token.lower() in a[1]]


@pytest.fixture
def cat(monkeypatch):
    c = Catalogue()
    monkeypatch.setattr(
        mysql.MySQLEngine, "_q",
        lambda self, side, sql, args=None, fresh=False: c.ask(side, sql,
                                                              args))
    monkeypatch.setattr(
        mysql.MySQLEngine, "_rows_utc",
        lambda self, side, sql: c.ask(side, sql))
    monkeypatch.setattr(
        mysql.MySQLEngine, "_all_tables",
        lambda self, side, db: ["orders", "child", "parent", "events",
                                "sales", "logs"])
    c.ddl = {}
    monkeypatch.setattr(
        mysql.MySQLEngine, "table_ddl",
        lambda self, side, db, table: c.ddl.get(
            (side, table), "CREATE TABLE `t` (`id` bigint) ENGINE=InnoDB"))
    return c


def _engine(tmp_path, **kw):
    hop = Hop(name="t", engine="tidb", source=EP, target=EP,
              databases=["d"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return TiDBEngine(hop)


def _deep(tmp_path, **kw):
    """{scope: finding} of the deep check."""
    return {r.scope: r for r in _engine(tmp_path, **kw).check_deep("d")}


def test_a_clean_catalogue_is_clean_and_asks_nothing_of_another_server(
        tmp_path, cat):
    got = _deep(tmp_path)
    assert got["d target indexes"].status == "ok"
    for scope in ("d keys", "d fk", "d columns", "d narrowing",
                  "d timeshift", "d charset", "d partitions", "d generated",
                  "d collation", "d boundary", "d nullempty", "d float",
                  "d checks", "d grants", "d ownership", "d storage"):
        assert got[scope].status == "ok", (scope, got[scope].detail)
    # what MySQL's deep check reads of a MySQL server, and TiDB is not
    for token in ("gtid", "show replica status", "mysql.time_zone_name",
                  "max_allowed_packet", "innodb_table_stats",
                  "information_schema.triggers", "information_schema.events",
                  "information_schema.routines"):
        assert cat.said(token) == [], token


def test_a_table_with_no_key_is_found(tmp_path, cat):
    cat.on("from information_schema.tables t",
           "not exists (select 1 from information_schema.table_constraints",
           src=[("logs",)])
    got = _deep(tmp_path)["d keys"]
    assert got.status == "diff"
    assert "logs" in got.detail and "no pk/unique" in got.detail
    assert got.category == "structure.keys"


def test_rows_pointing_at_nothing_are_found(tmp_path, cat):
    cat.on("referenced_column_name from information_schema.key_column_usage",
           dst=[("fk_parent", "child", "pid", "parent", "id")])
    cat.on("select count(*) from", "c where", "not exists", dst=[(2,)])
    got = _deep(tmp_path)["d fk"]
    assert got.status == "diff" and "child.fk_parent: 2 orphan rows" \
        in got.detail
    assert got.category == "structure.foreign-keys"


def test_a_column_that_drifted_is_found(tmp_path, cat):
    cat.on("coalesce(column_default, '')",
           src=[("orders", "orders.note", "varchar(40)", "YES", "",
                 "utf8mb4", "utf8mb4_bin", "")],
           dst=[("orders", "orders.note", "varchar(40)", "NO", "",
                 "utf8mb4", "utf8mb4_bin", "")])
    got = _deep(tmp_path)["d columns"]
    assert got.status == "diff" and "orders.note" in got.detail
    assert (tmp_path / "deep-columns.diff").exists()


def test_a_column_the_target_made_narrower_is_found(tmp_path, cat):
    cat.on("concat(table_name,'.',column_name), column_type from"
           " information_schema.columns",
           src=[("orders", "orders.note", "varchar(200)"),
                ("orders", "orders.qty", "bigint(20)")],
           dst=[("orders", "orders.note", "varchar(20)"),
                ("orders", "orders.qty", "bigint(20)")])
    got = _deep(tmp_path)["d narrowing"]
    assert got.status == "diff" and "orders.note" in got.detail, got.detail
    assert "orders.qty" not in got.detail


def test_a_time_shifted_by_a_zone_is_found(tmp_path, cat):
    cat.on("data_type in ('datetime','timestamp')", src=[("events", "at")])
    cat.on("k.constraint_name='primary' order by k.ordinal_position",
           src=[("id",)])
    cat.on("unix_timestamp(`at`)", src=[(i, 1_700_000_000 + i)
                                        for i in range(1, 6)],
           dst=[(i, 1_700_000_000 + i + 7 * 3600) for i in range(1, 6)])
    got = _deep(tmp_path)["d timeshift"]
    assert got.status == "diff" and "events.at" in got.detail, got.detail


def test_a_charset_downgrade_and_a_replacement_character_are_found(
        tmp_path, cat):
    cat.on("character_set_name from information_schema.columns",
           "character_set_name is not null",
           src=[("orders", "note", "utf8mb4")],
           dst=[("orders", "note", "latin1")])
    got = _deep(tmp_path)["d charset"]
    assert got.status == "diff"
    assert "orders.note: charset utf8mb4 -> latin1 (DOWNGRADE" in got.detail
    # and the same charset with a replacement character the source lacks
    cat2 = Catalogue()
    cat2.on("select distinct table_name, column_name", src=[("orders",
                                                           "note")])
    cat2.on("0xefbfbd", src=[(0,)], dst=[(3,)])
    cat.rules[:] = cat2.rules
    got = _deep(tmp_path)["d charset"]
    assert got.status == "diff" and "3 extra U+FFFD" in got.detail


def test_a_partition_that_arrived_empty_is_found(tmp_path, cat):
    parts = {"src": [("sales", "RANGE", "sale_date", "p1", "100"),
                     ("sales", "RANGE", "sale_date", "p2", "MAXVALUE")],
             "dst": [("sales", "RANGE", "sale_date", "p1", "100"),
                     ("sales", "RANGE", "sale_date", "p2", "MAXVALUE")]}
    rows = {"src": {"p1": 5, "p2": 7}, "dst": {"p1": 5, "p2": 0}}
    cat.on("from information_schema.partitions", src=parts["src"],
           dst=parts["dst"])
    cat.on("select column_name from information_schema.columns",
           "order by ordinal_position", src=[("id",)])

    def counted(side, sql, args):
        names = re.findall(r"select (\d+), count\(\*\).*?partition "
                           r"\(`(\w+)`\)", sql)
        return [(int(i), rows[side][name], "0") for i, name in names]
    cat.on("partition (", rows=counted)
    got = _deep(tmp_path)["d partitions"]
    assert got.status == "diff", got.detail
    assert "partition p2: empty on target, the source's holds 7 rows" \
        in got.detail
    assert got.category == "structure.partitions"
    # the partition is read by TiDB's partition selection, as MySQL's is
    assert cat.said("from `d`.`sales` partition (`p2`)")


def test_a_generated_column_that_is_not_one_on_the_target_is_found(
        tmp_path, cat):
    cat.on("generation_expression", "extra,",
           src=[("orders", "total", "VIRTUAL GENERATED", "`qty` * `price`")])
    got = _deep(tmp_path)["d generated"]
    assert got.status == "diff"
    assert "orders.total: generated on source, plain/missing on target" \
        in got.detail


def test_a_collation_that_collapses_a_unique_key_is_found(tmp_path, cat):
    cat.on("c.collation_name", "tc.constraint_type",
           src=[("orders", "code", "utf8mb4_bin")],
           dst=[("orders", "code", "utf8mb4_general_ci")])
    cat.on("group by `code` collate utf8mb4_general_ci", src=[(2,)])
    got = _deep(tmp_path)["d collation"]
    assert got.status == "diff"
    assert "2 source groups COLLAPSE to duplicates" in got.detail


def test_a_target_ahead_of_the_source_is_found(tmp_path, cat):
    cat.on("k.constraint_name='primary'", "c.data_type in ('tinyint'",
           src=[("orders", "id")], dst=[("orders", "id")])
    cat.on("coalesce(max(`id`), 0)", src=[("orders", 100)],
           dst=[("orders", 150)])
    got = _deep(tmp_path)["d boundary"]
    assert got.status == "diff" and "orders src_max=100 dst_max=150" \
        in got.detail
    assert got.category == "movement.target-ahead"


def test_null_turned_into_empty_is_found(tmp_path, cat):
    cat.on("data_type in (%s,%s,%s", "order by 1,2", src=[("orders", "note")])
    cat.on("sum(`note` is null)", src=[(2, 0)], dst=[(0, 2)])
    got = _deep(tmp_path)["d nullempty"]
    assert got.status == "diff" and "orders.note" in got.detail, got.detail


def test_a_float_off_its_sum_is_found(tmp_path, cat):
    cat.on("data_type in (%s, %s)", src=[("orders", "price")])
    cat.on("count(`price`)", src=[(3, 10.0, 5.0)], dst=[(3, 10.5, 5.0)])
    got = _deep(tmp_path)["d float"]
    assert got.status == "diff" and "orders.price: sum" in got.detail
    assert got.category == "value.precision"


def test_a_check_constraint_that_is_not_enforced_is_found(tmp_path, cat):
    cat.on("check_clause", src=[("orders", "qty_positive", "qty > 0",
                                 "YES")],
           dst=[("orders", "qty_positive", "qty > 0", "NO")])
    got = _deep(tmp_path)["d checks"]
    assert got.status == "diff" and "NOT ENFORCED on target" in got.detail


def test_a_grant_that_did_not_cross_is_found(tmp_path, cat):
    cat.on("from mysql.user", src=[("app", "%")], dst=[("app", "%")])
    cat.on("show grants for", src=[("GRANT SELECT ON `d`.* TO `app`@`%`",)],
           dst=[])
    got = _deep(tmp_path)["d grants"]
    assert got.status == "diff" and "grants missing on target" in got.detail
    assert got.category == "access.table-grants"


def test_a_view_whose_definer_changed_is_found(tmp_path, cat):
    cat.on("definer, security_type from information_schema.views",
           src=[("v_orders", "v_orders", "app@%", "DEFINER")],
           dst=[("v_orders", "v_orders", "mover@%", "INVOKER")])
    got = _deep(tmp_path)["d ownership"]
    assert got.status == "diff", got.detail
    assert "INVOKER" in got.detail and "run with the caller" in got.detail
    # TiDB keeps no routine, trigger or event to carry a definer on
    assert cat.said("information_schema.routines") == []


# ---- the objects beside the tables, held to the hop's ----------------------

HELD = {
    "table": [("orders", "orders"), ("audit", "audit")],
    "view": [("v_orders", "v_orders"), ("v_audit", "v_audit")],
    "index": [("orders.PRIMARY", "orders"), ("audit.ix_owner", "audit")],
}


def _objects(tmp_path, cat, only_on_target=(), **kw):
    queries = {q: k for k, q in TiDBEngine.OBJECT_QUERIES.items()}
    cat.on("select ", rows=lambda side, sql, args: [
        r for r in HELD[queries[sql]] if side == "dst" or not any(
            o in (r[0], r[1]) for o in only_on_target)]
        if sql in queries else [])
    cat.on("from information_schema.tables where table_schema=%s and"
           " table_type in", rows=lambda side, sql, args: [
               (t,) for t, _ in HELD["table"]
               if side == "dst" or t not in only_on_target])
    return _engine(tmp_path, **kw)


def test_tidb_asks_only_for_the_objects_it_keeps(tmp_path):
    assert sorted(TiDBEngine.OBJECT_QUERIES) == ["index", "table", "view"]


def test_an_object_the_hop_excludes_on_the_target_alone_is_not_extra(
        tmp_path, cat):
    eng = _objects(tmp_path, cat, ("audit", "v_audit"),
                   exclude=["audit", "v_audit"])
    got = eng.check_objects("d")
    assert got.status == "ok", got.detail
    said = (tmp_path / "objects.json").read_text()
    for name in ("audit", "v_audit", "ix_owner"):
        assert name not in said, (name, said)


def test_the_same_target_without_exclude_is_different(tmp_path, cat):
    got = _objects(tmp_path, cat, ("audit", "v_audit")).check_objects("d")
    assert got.status == "diff"
    for name in ("audit", "v_audit"):
        assert name in got.detail, (name, got.detail)
    assert got.category == "structure.objects"


def test_the_schema_check_carries_the_objects_beside_the_pairs(tmp_path,
                                                               cat):
    """The pair compares every table's columns; the objects by name come
    with it."""
    eng = _objects(tmp_path, cat, ("v_audit",))

    class Pair:
        def check_schema(self, db):
            return []
    eng._as_pair = lambda within=None: Pair()
    got = eng.check_schema("d")
    assert [(r.scope, r.status) for r in got] == [("d objects", "diff")]


# ---- the options only TiDB keeps on a table ---------------------------------
#
# Not named in `PROVES`: the comment forms below are TiDB's documented
# `SHOW CREATE TABLE` output, written here from its documentation and never
# read off a server (`capabilities.UNPROVED`, verify.structure.collection-
# options).

SRC_DDL = (
    "CREATE TABLE `orders` (\n  `id` bigint(20) NOT NULL,\n"
    "  `at` datetime NOT NULL,\n"
    "  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n"
    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin "
    "/*T! SHARD_ROW_ID_BITS=4 */ /*T![placement] PLACEMENT POLICY=`hot` */"
    " /*T![ttl] TTL=`at` + INTERVAL 3 MONTH */ "
    "/*T![auto_rand_base] AUTO_RANDOM_BASE=30001 */")


def test_the_options_of_a_table_are_read_apart_from_its_counters():
    assert dict(TiDBEngine.table_options(SRC_DDL)) == {
        ("clustered_index", "CLUSTERED"): 1,
        ("placement", "PLACEMENT POLICY=`hot`"): 1,
        ("ttl", "TTL=`at` + INTERVAL 3 MONTH"): 1,
        ("", "SHARD_ROW_ID_BITS=4"): 1}


def test_a_table_that_lost_its_policy_and_ttl_is_found(tmp_path, cat):
    cat.ddl[("src", "orders")] = SRC_DDL
    cat.ddl[("dst", "orders")] = (
        "CREATE TABLE `orders` (\n  `id` bigint(20) NOT NULL,\n"
        "  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n"
        ") ENGINE=InnoDB /*T! SHARD_ROW_ID_BITS=4 */")
    got = _deep(tmp_path)["d storage"]
    assert got.status == "diff", got.detail
    assert got.category == "structure.collection-options"
    assert "orders: target lacks" in got.detail
    assert "PLACEMENT POLICY=`hot`" in got.detail and "TTL=" in got.detail
    assert "SHARD_ROW_ID_BITS" not in got.detail


def test_the_same_options_in_another_order_with_other_counters_agree(
        tmp_path, cat):
    cat.ddl[("src", "orders")] = SRC_DDL
    cat.ddl[("dst", "orders")] = (
        "CREATE TABLE `orders` (\n  `id` bigint(20) NOT NULL,\n"
        "  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n) "
        "/*T![ttl] TTL=`at` + INTERVAL 3 MONTH */ "
        "/*T![placement] PLACEMENT POLICY=`hot`*/ "
        "/*T! SHARD_ROW_ID_BITS=4 */ "
        "/*T![auto_rand_base] AUTO_RANDOM_BASE=9 */")
    assert _deep(tmp_path)["d storage"].status == "ok"


def test_a_ttl_only_the_target_has_is_found(tmp_path, cat):
    cat.ddl[("src", "orders")] = "CREATE TABLE `orders` (`id` bigint)"
    cat.ddl[("dst", "orders")] = ("CREATE TABLE `orders` (`id` bigint) "
                                  "/*T![ttl] TTL=`at` + INTERVAL 1 DAY */")
    got = _deep(tmp_path)["d storage"]
    assert got.status == "diff" and "target adds" in got.detail


def test_options_that_cannot_be_read_do_not_pass(tmp_path, cat, monkeypatch):
    def boom(self, side, db, table):
        raise RuntimeError("Table 'd.orders' doesn't exist")
    monkeypatch.setattr(mysql.MySQLEngine, "table_ddl", boom)
    got = _deep(tmp_path)["d storage"]
    assert got.status == "error" and "could not read" in got.detail

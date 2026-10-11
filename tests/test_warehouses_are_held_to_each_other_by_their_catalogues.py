"""A warehouse enforces none of the keys it declares and keeps no index to
refuse a wrong row, so a move is proved by what the catalogue of each side
says: Redshift and Snowflake hold the two catalogues to each other for the
tables and objects the hop takes (`check_deep`), say what the move will not
do (`assess`), and read the rest of what a pair is given of an engine - its
foreign keys, columns' rules and views.

No warehouse runs here: a side is a table of answers keyed by what the
statement reads, and what is pinned is what migkit does with them - which
tables are compared, what is called different, what a catalogue that
would not answer is called (an error, never a pass), and what a table the
hop leaves out costs. That any server accepts any statement below as
written is owed a real account (`capabilities.UNPROVED`).
"""
import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.warehouse import RedshiftEngine, SnowflakeEngine

ENGINES = {"redshift": RedshiftEngine, "snowflake": SnowflakeEngine}


class World:
    """Two sides of a warehouse: for each, (needle, answer) pairs - the
    first needle a statement holds answers it; an answer is rows, a
    function of the statement's arguments, or an exception to raise."""

    def __init__(self, src, dst):
        self.sides = {"src": list(src), "dst": list(dst)}
        self.asked = []

    def ask(self, side, sql, args):
        said = " ".join(sql.split())
        low = said.lower()
        self.asked.append((side, said, args))
        for needle, answer in self.sides[side]:
            if needle in low:
                if isinstance(answer, Exception):
                    raise answer
                return answer(args) if callable(answer) else answer
        raise AssertionError(f"unanswered on the {side}: {low}")


def _engine(name, world, tmp_path, **kw):
    ep = Endpoint(user="u", password="CHANGE_ME", options={
        "database": "ANALYTICS", "account": "acme"})
    hop = Hop(name="w", engine=name, source=ep, target=ep,
              databases=["sales"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    eng = ENGINES[name](hop)
    eng._rows = lambda side, db, sql, args=None: [
        list(r) for r in world.ask(side, sql, args)]

    def listing(side, db, sql, args=None):
        return [dict(r) for r in world.ask(side, sql, args)]
    eng._listing = listing
    return eng


def _tables(*names):
    return ("information_schema.tables where", [[n] for n in names])


# ---- the columns ---------------------------------------------------------------

COLS = ("from information_schema.columns where",)


def _columns(rows):
    return (COLS[0], rows)


@pytest.mark.parametrize("name, orders", [
    ("redshift", "orders"), ("snowflake", "ORDERS")])
def test_the_columns_of_both_sides_are_held_to_each_other(name, orders,
                                                          tmp_path):
    same = [[orders, "id", "bigint", "NO", None],
            [orders, "amount", "numeric(12,2)", "YES", None]]
    other = [[orders, "id", "bigint", "NO", None],
             [orders, "amount", "numeric(14,2)", "NO", None]]
    world = World([_tables(orders), _columns(same)],
                  [_tables(orders), _columns(other)])
    eng = _engine(name, world, tmp_path)
    got = eng._columns_result("sales")
    assert (got.status, got.category) == ("diff", "structure.columns")
    assert "orders (amount)" in got.detail and got.tables == {"orders": "diff"}
    assert got.fix_hint
    world.sides["dst"][1] = _columns(same)
    ok = eng._columns_result("sales")
    assert (ok.status, ok.tables) == ("ok", {"orders": "ok"})
    assert not ok.fix_hint


def test_a_table_only_one_side_has_is_the_copys_to_say_not_the_columns(
        tmp_path):
    cols = [["orders", "id", "bigint", "NO", None]]
    world = World([_tables("orders", "parcels"), _columns(cols)],
                  [_tables("orders"), _columns(cols)])
    got = _engine("redshift", world, tmp_path)._columns_result("sales")
    assert got.status == "ok" and list(got.tables) == ["orders"]


def test_a_table_the_hop_leaves_out_is_not_compared(tmp_path):
    src = [["orders", "id", "bigint", "NO", None],
           ["event_log", "at", "timestamp", "NO", None]]
    dst = [["orders", "id", "bigint", "NO", None],
           ["event_log", "at", "date", "NO", None]]
    world = World([_tables("orders", "event_log"), _columns(src)],
                  [_tables("orders", "event_log"), _columns(dst)])
    eng = _engine("redshift", world, tmp_path, exclude=["event_log"])
    got = eng._columns_result("sales")
    assert got.status == "ok" and got.tables == {"orders": "ok"}


def test_an_identity_and_a_sequence_are_said_without_what_is_the_clusters_own(
        tmp_path):
    """Redshift names an identity's table by its object id, and Snowflake
    a sequence's default by its database and schema: a copy on another
    cluster, or in another schema, is the same column."""
    rs = _engine("redshift", World([], []), tmp_path)
    assert rs._default_text("src", "sales", '"identity"(104562, 0, \'1,1\'::text)') \
        == rs._default_text("dst", "sales", '"identity"(209911, 0, \'1,1\'::text)') \
        == "identity"
    assert rs._default_text("src", "sales", None) is None
    sf = _engine("snowflake", World([], []), tmp_path)
    assert sf._default_text("src", "sales", "ANALYTICS.SALES.IDS.NEXTVAL") \
        == "<database>.<schema>.IDS.NEXTVAL"


# ---- the keys ------------------------------------------------------------------

def test_a_declared_key_is_held_and_a_key_more_than_one_row_holds_is_found(
        tmp_path):
    keys = ("information_schema.table_constraints", [["orders", "id"]])
    world = World(
        [_tables("orders"), keys],
        [_tables("orders"), keys,
         ("group by", lambda args: [[3]])])
    eng = _engine("redshift", world, tmp_path)
    got = eng._keys_result("sales")
    assert (got.status, got.category) == ("diff", "structure.keys")
    assert "orders: 3" in got.detail and got.tables == {"orders": "diff"}
    sql = [q for s, q, _ in world.asked if "group by" in q][0]
    assert sql == ('select count(*) from (select 1 from "sales"."orders" '
                   'group by "id" having count(*) > 1) d')
    # nothing more than one row holds: the key is the same on both sides
    world.sides["dst"][2] = ("group by", [[0]])
    assert eng._keys_result("sales").status == "ok"
    # a key declared on one side only differs
    world.sides["dst"][1] = ("information_schema.table_constraints", [])
    got = eng._keys_result("sales")
    assert got.status == "diff" and "orders" in got.detail


def test_snowflake_reads_its_keys_from_show_and_quotes_them_as_it_stores_them(
        tmp_path):
    show = ("show primary keys", [
        {"table_name": "ORDERS", "column_name": "TENANT", "key_sequence": 1},
        {"table_name": "ORDERS", "column_name": "ID", "key_sequence": 2},
        {"table_name": "EVENT_LOG", "column_name": "ID", "key_sequence": 1}])
    world = World([_tables("ORDERS", "EVENT_LOG"), show],
                  [_tables("ORDERS", "EVENT_LOG"), show,
                   ("group by", [[0]])])
    eng = _engine("snowflake", world, tmp_path, exclude=["event_log"])
    assert eng._keys_of("src", "sales") == {"orders": ["tenant", "id"]}
    assert eng._keys_result("sales").status == "ok"
    sql = [q for s, q, _ in world.asked if "group by" in q][0]
    assert 'from "SALES"."ORDERS" group by "TENANT", "ID"' in sql


def test_a_catalogue_that_would_not_answer_is_an_error_not_a_pass(tmp_path):
    world = World([_tables("orders"), COLS and _columns(
        RuntimeError("insufficient privileges"))],
        [_tables("orders"), _columns([])])
    got = _engine("redshift", world, tmp_path)._columns_result("sales")
    assert got.status == "error" and "insufficient privileges" in got.detail
    # and the keys it could not read are not the keys of a clean target
    world = World([_tables("orders"), ("information_schema.table_constraints",
                                       RuntimeError("denied"))],
                  [_tables("orders")])
    got = _engine("redshift", world, tmp_path)._keys_result("sales")
    assert got.status == "error" and "denied" in got.detail


def test_the_source_is_looked_at_for_repeated_keys_before_the_move(tmp_path):
    keys = ("information_schema.table_constraints", [["orders", "id"]])
    world = World([_tables("orders"), keys, ("group by", [[2]])], [])
    eng = _engine("redshift", world, tmp_path)
    assert eng.PREFLIGHT == ("_duplicate_source_keys",)
    got = eng._duplicate_source_keys("sales")
    assert (got.status, got.category) == ("diff", "structure.keys")
    assert "orders: 2" in got.detail
    items = eng._preflight_items()
    fails = [i for i in items if i["scope"] == "before the move"]
    assert fails and fails[0]["level"] == "fail"
    world.sides["src"][2] = ("group by", [[0]])
    assert eng._duplicate_source_keys("sales").status == "ok"


# ---- the foreign keys ----------------------------------------------------------

def test_redshift_reads_a_foreign_key_from_the_constraints_own_text(tmp_path):
    world = World([("pg_get_constraintdef", [
        ["lines", "FOREIGN KEY (order_id, tenant) REFERENCES"
                  " sales.orders(id, tenant)"],
        ["notes", 'FOREIGN KEY ("note id") REFERENCES "notes"("id")'],
        ["event_log", "FOREIGN KEY (a) REFERENCES orders(id)"]])], [])
    eng = _engine("redshift", world, tmp_path, exclude=["event_log"])
    assert eng.foreign_keys("src", "sales") == [
        ("lines", ["order_id", "tenant"], "orders", ["id", "tenant"]),
        ("notes", ["note id"], "notes", ["id"])]


def test_snowflake_reads_a_foreign_key_from_show_in_the_keys_order(tmp_path):
    def key(seq, child, parent, name="fk_1"):
        return {"fk_table_name": child, "fk_column_name": f"C{seq}",
                "pk_table_name": parent, "pk_column_name": f"P{seq}",
                "key_sequence": seq, "fk_name": name}
    world = World([("show imported keys", [
        key(2, "LINES", "ORDERS"), key(1, "LINES", "ORDERS"),
        key(1, "EVENT_LOG", "ORDERS", "fk_2")])], [])
    eng = _engine("snowflake", world, tmp_path, exclude=["event_log"])
    assert eng.foreign_keys("src", "sales") == [
        ("lines", ["c1", "c2"], "orders", ["p1", "p2"])]
    both = World([("show imported keys", [key(1, "LINES", "ORDERS")]),
                  _tables("LINES")],
                 [("show imported keys", []), _tables("LINES")])
    got = _engine("snowflake", both, tmp_path)._foreign_keys_result("sales")
    assert (got.status, got.category) == ("diff", "structure.foreign-keys")
    assert "lines" in got.detail


# ---- views, materialized views, options, policies ------------------------------

def test_a_view_is_its_select_and_a_view_one_side_lacks_differs(tmp_path):
    world = World(
        [("from information_schema.views", [
            ["V_OPEN", "create or replace view v_open as select id from"
                       " orders where open"],
            ["V_LOG", "select 1"]])],
        [("from information_schema.views", [])])
    eng = _engine("snowflake", world, tmp_path, exclude=["v_log"])
    assert eng.neutral_views("src", "sales") == [
        ("v_open", "SELECT id FROM orders WHERE open")]
    got = eng._objects_result("sales")
    assert (got.status, got.category) == ("diff", "structure.objects")
    assert "v_open (only on the source" in got.detail
    # a hop that carries no views compares none
    none = _engine("snowflake", world, tmp_path, objects={"views": False})
    assert none._objects_result("sales").status == "ok"


def test_redshift_reads_its_views_from_pg_views(tmp_path):
    world = World([("from pg_views", [["v_open", "select id from orders"]])],
                  [("from pg_views", [["v_open", "SELECT id FROM orders"]])])
    eng = _engine("redshift", world, tmp_path)
    assert eng._objects_result("sales").status == "ok"


def test_a_materialized_view_that_is_stale_or_invalid_differs(tmp_path):
    world = World([("from stv_mv_info", [["mv_daily", "f"]])],
                  [("from stv_mv_info", [["mv_daily", "t"]])])
    got = _engine("redshift", world, tmp_path)._matviews_result("sales")
    assert (got.status, got.category) == (
        "diff", "structure.materialized-views")
    world = World([("show materialized views", [
        {"name": "MV_DAILY", "invalid": "false"}])],
        [("show materialized views", [
            {"name": "MV_DAILY", "invalid": "true"}])])
    got = _engine("snowflake", world, tmp_path)._matviews_result("sales")
    assert got.status == "diff" and "mv_daily" in got.detail


def test_the_options_a_table_was_made_with_are_held(tmp_path):
    world = World([_tables("orders"), ("from svv_table_info", [
        ["orders", "created_at", 1, "Y"]])],
        [_tables("orders"), ("from svv_table_info", [
            ["orders", "id", 1, "Y"]])])
    got = _engine("redshift", world, tmp_path)._options_result("sales")
    assert (got.status, got.category) == (
        "diff", "structure.collection-options")
    world = World([_tables("ORDERS"), ("show tables", [
        {"name": "ORDERS", "kind": "TABLE", "cluster_by": "LINEAR(ID)",
         "retention_time": "7", "change_tracking": "OFF"}])],
        [_tables("ORDERS"), ("show tables", [
            {"name": "ORDERS", "kind": "TRANSIENT",
             "cluster_by": "LINEAR(ID)", "retention_time": "1",
             "change_tracking": "OFF"}])])
    got = _engine("snowflake", world, tmp_path)._options_result("sales")
    assert got.status == "diff" and "orders" in got.detail


def test_row_policies_grants_and_owners_are_held_table_by_table(tmp_path):
    grants = ("from information_schema.table_privileges", [
        ["orders", "analyst", "SELECT"], ["orders", "etl", "INSERT"]])
    fewer = ("from information_schema.table_privileges", [
        ["orders", "analyst", "SELECT"]])
    world = World([_tables("orders"), grants], [_tables("orders"), fewer])
    eng = _engine("redshift", world, tmp_path)
    got = eng._grants_result("sales")
    assert (got.status, got.category) == ("diff", "access.table-grants")
    assert got.tables == {"orders": "diff"}
    owners = ("from pg_tables", [["orders", "etl"]])
    world = World([_tables("orders"), owners],
                  [_tables("orders"), ("from pg_tables", [
                      ["orders", "admin"]])])
    got = _engine("redshift", world, tmp_path)._ownership_result("sales")
    assert (got.status, got.category) == ("diff", "access.object-ownership")
    owner = ("table_owner", [["ORDERS", "OWNER_ROLE"]])
    world = World([owner, _tables("ORDERS")], [owner, _tables("ORDERS")])
    assert _engine("snowflake", world, tmp_path)._ownership_result(
        "sales").status == "ok"


def test_redshift_finds_a_policy_by_the_columns_the_view_names(tmp_path):
    world = World(
        [("svv_rls_attached_policy", [{"relname": "orders",
                                       "policyname": "tenant_only",
                                       "relschema": "sales"}]),
         ("svv_attached_masking_policy", RuntimeError("no masking here"))],
        [("svv_rls_attached_policy", []),
         ("svv_attached_masking_policy", RuntimeError("no masking here"))])
    eng = _engine("redshift", world, tmp_path)
    assert eng._policies("src", "sales") == {
        "orders": {("svv_rls_attached_policy", "tenant_only")}}
    world.sides["src"][0] = ("svv_rls_attached_policy", [{"x": 1}])
    with pytest.raises(ValueError):
        eng._policies("src", "sales")
    # neither view answering is the error of the first
    both = World([("svv_rls", RuntimeError("a")),
                  ("svv_attached", RuntimeError("b"))], [])
    with pytest.raises(RuntimeError, match="a"):
        _engine("redshift", both, tmp_path)._policies("src", "sales")


def test_snowflake_asks_each_table_what_policies_it_is_under(tmp_path):
    seen = []

    class Cur:
        def execute(self, sql, args=None):
            seen.append((" ".join(sql.split()), args))

        def fetchall(self):
            return [("ROW_ACCESS_POLICY", "TENANT_ONLY")] \
                if "ORDERS" in seen[-1][1][0] else []

    class Conn:
        def cursor(self):
            return Cur()

        def close(self):
            pass
    world = World([_tables("ORDERS", "LINES")], [])
    eng = _engine("snowflake", world, tmp_path)
    eng._connect = lambda side, db: Conn()
    assert eng._policies("src", "sales") == {
        "orders": {("ROW_ACCESS_POLICY", "TENANT_ONLY")}}
    assert seen[0][1] == ('"ANALYTICS"."SALES"."ORDERS"',)


# ---- how the cluster spreads, and what a sequence is -----------------------------

def test_how_redshift_spreads_a_table_is_held_and_auto_is_left_to_the_cluster(
        tmp_path):
    world = World(
        [_tables("orders", "lines"), ("from svv_table_info", [
            ["orders", "KEY(id)"], ["lines", "AUTO(EVEN)"]])],
        [_tables("orders", "lines"), ("from svv_table_info", [
            ["orders", "EVEN"], ["lines", "AUTO(KEY(id))"]])])
    got = _engine("redshift", world, tmp_path)._deep_extra("sales")[0]
    assert (got.status, got.category) == ("diff", "structure.sharding")
    assert got.tables == {"orders": "diff", "lines": "ok"}


def test_a_sequence_that_would_hand_out_what_the_source_has_is_found(
        tmp_path):
    seqs = lambda **n: ("show sequences", [  # noqa: E731
        {"name": k, "next_value": v, "interval": 1} for k, v in n.items()])
    world = World([seqs(IDS=500, OLD=10)], [seqs(IDS=120)])
    eng = _engine("snowflake", world, tmp_path, objects={})
    got = [r for r in eng._deep_extra("sales") if r.scope.endswith("parity")]
    assert got[0].status == "diff" and got[0].category == \
        "identity.sequence-parity"
    assert "ids (next 120, the source's 500)" in got[0].detail
    assert "old (not on the target)" in got[0].detail
    world = World([seqs(IDS=500)], [seqs(IDS=900)])
    ok = [r for r in _engine("snowflake", world, tmp_path)._deep_extra(
        "sales") if r.scope.endswith("parity")]
    assert ok[0].status == "ok"
    # a hop that carries no sequences holds none
    none = _engine("snowflake", World([seqs(IDS=500)], [seqs(IDS=1)]),
                   tmp_path, objects={"sequences": False})
    assert [r for r in none._deep_extra("sales")
            if r.scope.endswith("parity")][0].status == "ok"


# ---- what a pair is given -----------------------------------------------------

def test_a_columns_rules_are_its_nullability_default_and_identity(tmp_path):
    world = World([("from information_schema.columns where", [
        ["id", "NO", None, True], ["status", "NO", "'new'::character varying",
                                   False], ["note", "YES", None, False]])],
                  [])
    eng = _engine("redshift", world, tmp_path)
    assert eng.neutral_column_rules("src", "sales", "orders") == {
        "id": {"null": False, "default": None, "identity": True},
        "status": {"null": False, "default": "'new'::character varying",
                   "identity": False},
        "note": {"null": True, "default": None, "identity": False}}
    sf = _engine("snowflake", World([("from information_schema.columns where",
                                      [["ID", "NO", "IDS.NEXTVAL", "YES"]])],
                                    []), tmp_path)
    assert sf.neutral_column_rules("src", "sales", "orders") == {
        "id": {"null": False, "default": None, "identity": True}}


# ---- what the move will not do --------------------------------------------------

def test_a_redshift_source_names_what_no_copy_of_its_tables_carries(tmp_path):
    keys = ("information_schema.table_constraints", [["orders", "id"]])
    world = World([
        _tables("orders", "lines"), keys,
        ("p.prokind", lambda args: [["f_total"]] if args[1] == "f" else []),
        ("from stv_mv_info", [["mv_daily", "f"]]),
        ("from svv_datashares", [["sales_share"]]),
        ("from svv_external_schemas", RuntimeError("no spectrum"))], [])
    eng = _engine("redshift", world, tmp_path)
    inv = eng._handwork("src")
    rows = {r["item"]: r for r in inv.rows()}
    assert "lines" in rows["tables with no primary key or unique index"][
        "detail"]
    assert "f_total" in rows["routines, triggers and views a data mover"
                             " does not carry"]["detail"]
    assert "sales_share" in rows["objects whose contents no data mover"
                                 " copies"]["detail"]
    # a probe that could not run is unknown, never none
    assert "UNKNOWN" in rows["things that must exist on the target before"
                             " the load"]["detail"]
    assert "no spectrum" in rows["things that must exist on the target"
                                 " before the load"]["detail"]
    said = eng.reach_items("src")
    assert said[-1]["scope"] == "manual work"


def test_a_snowflake_source_lists_what_lives_beside_its_tables(tmp_path):
    world = World([_tables("ORDERS"),
                   ("show primary keys", [
                       {"table_name": "ORDERS", "column_name": "ID",
                        "key_sequence": 1}]),
                   ("show user functions", [{"name": "F_TOTAL"}]),
                   ("show user procedures", RuntimeError("no rights")),
                   ("show tasks", [{"name": "NIGHTLY"}]),
                   ("show streams", [{"name": "ORDERS_CHANGES"}]),
                   ("show pipes", []), ("show stages", [{"name": "RAW"}]),
                   ("show sequences", [{"name": "IDS"}]),
                   ("show dynamic tables", []),
                   ("show materialized views", []),
                   ("show masking policies", []),
                   ("show row access policies", [{"name": "TENANT_ONLY"}])],
                  [])
    inv = _engine("snowflake", world, tmp_path)._handwork("src")
    text = " ".join(r["detail"] for r in inv.rows())
    # the routines and tasks are one kind of work, one probe of which could
    # not run: the row says so and counts the rest, never naming them as a
    # whole
    for word in ("orders_changes", "raw", "ids", "tenant_only", "UNKNOWN",
                 "no rights", "2 found by the probes that did run"):
        assert word in text, (word, text)
    # a hop that carries no sequences leaves them out
    none = _engine("snowflake", world, tmp_path, objects={"sequences": False})
    assert "ids" not in " ".join(r["detail"] for r in
                                 none._handwork("src").rows())


# ---- the room ------------------------------------------------------------------

def test_a_provisioned_clusters_free_disk_is_read_in_megabyte_blocks(
        tmp_path):
    eng = _engine("redshift", World([], [("stv_partitions", [[2048]])]),
                  tmp_path)
    assert eng.free_bytes("dst", "sales") == 2048 * 1048576
    # Serverless keeps no such view: nothing is said, and nothing is guessed
    eng = _engine("redshift", World([], [("stv_partitions",
                                          RuntimeError("no such view"))]),
                  tmp_path)
    assert eng.free_bytes("dst", "sales") is None


def test_snowflake_says_how_many_queries_run_on_the_warehouse_it_uses(
        tmp_path):
    world = World([], [("select current_warehouse()", [["LOAD_WH"]]),
                       ("show warehouses like", [{"running": 3,
                                                  "queued": 1}])])
    eng = _engine("snowflake", world, tmp_path)
    assert eng.capacity("dst", "sales") == {"running": 3}
    nothing = _engine("snowflake", World([], [
        ("select current_warehouse()", [[None]])]), tmp_path)
    assert nothing.capacity("dst", "sales") == {}


# ---- a target that is ahead -------------------------------------------------------

def test_a_target_whose_largest_key_is_past_the_sources_is_found(tmp_path):
    keys = ("information_schema.table_constraints",
            [["orders", "id"], ["notes", "slug"], ["lines", "id"]])
    columns = ("from information_schema.columns where", lambda args: [
        ["slug", "character varying(40)"]] if args[1] == "notes" else [
        ["id", "bigint"]])

    def side(orders, lines):
        return [_tables("orders", "notes", "lines"), keys, columns,
                ('select max("id") from "sales"."orders"', [[orders]]),
                ('select max("id") from "sales"."lines"', [[lines]])]
    world = World(side(100, None), side(120, None))
    eng = _engine("redshift", world, tmp_path)
    got = eng._boundary_result("sales")
    assert (got.status, got.category) == ("diff", "movement.target-ahead")
    assert "orders source 100, target 120" in got.detail
    # a text key has no largest value to compare; an empty table has none
    assert got.tables == {"orders": "diff", "lines": "ok"}
    # behind is the copy still running, not a writer
    world = World(side(100, None), side(90, None))
    assert _engine("redshift", world, tmp_path)._boundary_result(
        "sales").status == "ok"
    # a table the source holds no row of, that the target holds one of
    world = World(side(100, None), side(100, 7))
    got = _engine("redshift", world, tmp_path)._boundary_result("sales")
    assert got.status == "diff" and "lines source None, target 7" in got.detail

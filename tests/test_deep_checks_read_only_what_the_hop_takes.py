"""The deep checks read only the tables the hop takes.

Every deep check built its own list of tables from the catalogue, and none
of them asked the hop's scope: a DTS job imported as a hop that took three
tables was held to every keyless table, every drifted column, every NOT
VALID constraint and every unvalidated index of the whole database, and
one keyless table the job never took made `deep keys` say different.
Each listing now goes through the engine's one scope answer
(`Engine.in_scope`), so a deep check reads what the move took and nothing
else (backlog P0-SCOPE.6b).

Pure: each engine's catalogue reads are answered in-process.
"""
from migkit.config import Endpoint, Hop
from migkit.engines.base import Result

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("scope.listings", "mssql"),
    ("scope.deep", ("postgres", "mysql", "mssql", "clickhouse", "oracle",
                    "db2", "teradata", "cockroachdb")),
    ("scope.counters", "oracle"),
    ("scope.objects", "oracle"),
    ("verify.structure.columns", "mssql"),
    ("verify.structure.foreign-keys", "mssql"),
    ("verify.structure.objects", ("oracle", "cockroachdb")),
    ("verify.structure.triggers", "mssql"),
    ("verify.rows-apart", "mssql"),
    ("verify.tables", "mssql"),
]


def _hop(engine, tmp_path, **kw):
    hop = Hop(name="h", engine=engine, source=Endpoint(), target=Endpoint(),
              databases=["appdb"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _pg(tmp_path, answers, **kw):
    """A PostgreSQL engine whose catalogue reads are `answers`: (test of
    the side and the statement, the output - or a function of the side)."""
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop("postgres", tmp_path, **kw))

    def psql(side, db, sql, **_):
        for match, out in answers:
            if match(side, sql):
                return out(side) if callable(out) else out
        return ""
    eng._psql = psql
    return eng


TAKES = ["appdb.public.orders", "appdb.public.lines"]


# ---- PostgreSQL ------------------------------------------------------------

def test_a_table_named_without_its_schema_is_read_in_public(tmp_path):
    """`regclass` names a table on the search path without its schema:
    `orders` asked as itself missed an `include` of `appdb.public.orders`
    and every constraint on it read as left out."""
    eng = _pg(tmp_path, [], include=TAKES)
    assert eng.in_scope("appdb", "orders")
    assert eng.in_scope("appdb", "public.orders")
    assert not eng.in_scope("appdb", "public.event_log")
    assert not eng.in_scope("appdb", "event_log")


def test_a_sequence_is_read_one_way_by_the_counters_and_the_inventory(
        tmp_path):
    """The counters and the inventory read a sequence's scope one way
    (`_seq_gone`): by the table owning it, else the tables drawing from
    it, else its own name - and only where the hop carries sequences. The
    inventory once read an unowned sequence by its name alone, and the
    counters read `objects: {sequences: false}` as nothing."""
    owned = "public.orders_id_seq|public|orders|id"
    used = "public.shared_seq|public|lines"
    answers = [(lambda side, sql: "pg_attrdef" in sql, used),
               (lambda side, sql: "deptype" in sql or "attnum" in sql,
                owned)]
    eng = _pg(tmp_path, answers, include=TAKES)
    eng._seq_owned = lambda side, db: {"public.orders_id_seq": (
        "public", "orders", "id")}
    names = ["public.orders_id_seq", "public.shared_seq", "public.loose_seq"]
    gone = eng._seq_left_out("src", "appdb", names)
    assert gone == {"public.loose_seq"}
    out = eng._object_left_out("src", "appdb")
    assert {n for n in names if out("sequence", n)} == gone
    none = _pg(tmp_path, answers, include=TAKES,
               objects={"sequences": False})
    none._seq_owned = eng._seq_owned
    assert none._seq_left_out("src", "appdb", names) == set(names)


def test_a_listing_keeps_only_the_lines_of_tables_the_hop_takes(tmp_path):
    eng = _pg(tmp_path, [], include=TAKES)
    got = eng._scoped("appdb", "public.orders\npublic.event_log|a\n\n"
                               "public.lines|b")
    assert got == ["public.orders", "public.lines|b"]
    assert eng._scoped("appdb", ["public.event_log\tx"], "\t") == []


def test_a_hop_that_takes_everything_reads_every_table_as_before(tmp_path):
    eng = _pg(tmp_path, [])
    lines = "public.orders\npublic.event_log"
    assert eng._scoped("appdb", lines) == ["public.orders",
                                            "public.event_log"]


def test_the_large_columns_sized_are_the_hops(tmp_path):
    eng = _pg(tmp_path, [(lambda s, q: "typname = any" in q,
                          "public.orders\tnote\ttext\n"
                          "public.event_log\tbody\ttext")], include=TAKES)
    assert eng._lob_columns("appdb") == [("public.orders", "note", "text")]


def test_a_not_valid_constraint_on_a_table_left_out_is_not_a_finding(
        tmp_path):
    eng = _pg(tmp_path, [(lambda s, q: "convalidated" in q and s == "dst",
                          "public.orders\x1forders\x1fck_a\x1f"
                          "CHECK (a > 0) NOT VALID\n"
                          "public.event_log\x1fevent_log\x1fck_b\x1f"
                          "CHECK (b > 0) NOT VALID")], include=TAKES)
    assert eng._unvalidated_checks("appdb") == [
        ("orders", "ck_a", "CHECK (a > 0) NOT VALID")]


def test_the_text_columns_sampled_and_repaired_are_the_hops(tmp_path):
    """One reading for the mojibake check and its repair: the repair read
    every text column of the target, and with MIGKIT_REPAIR_TEXT updated
    tables the hop leaves out."""
    from migkit.engines.postgres import PostgresEngine
    eng = _pg(tmp_path, [(lambda s, q: q == PostgresEngine.TEXT_COLUMNS,
                          "public.orders\tnote\npublic.event_log\tbody\n"
                          "public.orders\tname")], exclude=["event_log"])
    assert eng._text_columns("dst", "appdb") == {
        "public.orders": ["note", "name"]}


def test_an_invalid_index_on_a_table_left_out_is_not_a_finding(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    eng = _pg(tmp_path, [(lambda s, q: q == PostgresEngine.INDEX_FLAGS,
                          "public.orders\torders_pkey\ti\t1\t1\n"
                          "public.event_log\tevent_log_ix\ti\t0\t1")],
              include=TAKES)
    got = eng._invalid_indexes("appdb")
    assert got.status == "ok", got.detail


def test_a_unique_index_left_out_is_not_hunted_for_duplicates(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    asked = []

    def hunt(side, sql):
        asked.append(sql)
        return False
    eng = _pg(tmp_path, [
        (lambda s, q: q == PostgresEngine.UNIQUE_TEXT_INDEXES,
         "public.orders\torders_code\tcode\t0\n"
         "public.event_log\tevent_log_code\tcode\t0"),
        (hunt, "")], include=TAKES)
    eng._duplicate_keys("appdb", "the sort order changed")
    groups = [q for q in asked if "group by" in q]
    assert groups and all("event_log" not in q for q in groups), groups
    assert any("orders" in q for q in groups), groups


def test_a_collation_only_a_table_left_out_uses_is_not_reported(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    eng = _pg(tmp_path, [
        (lambda s, q: q == PostgresEngine.COLLATION_DRIFT,
         "en_US\t1.0\t2.0\nde_DE\t1.0\t2.0"),
        (lambda s, q: q == PostgresEngine.COLLATION_USERS,
         "en_US\tpublic.orders\nde_DE\tpublic.event_log"),
        (lambda s, q: "server_version_num" in q, "140000")], include=TAKES)
    got = eng._collation_versions("appdb")
    assert "en_US" in got.detail and "de_DE" not in got.detail, got.detail


def test_a_hop_that_takes_everything_counts_its_collations_as_before(
        tmp_path):
    from migkit.engines.postgres import PostgresEngine
    eng = _pg(tmp_path, [
        (lambda s, q: q == PostgresEngine.COLLATION_DRIFT, ""),
        (lambda s, q: q == PostgresEngine.COLLATION_COUNT, "3"),
        (lambda s, q: q == PostgresEngine.COLLATION_USERS,
         "en_US\tpublic.orders"),
        (lambda s, q: "server_version_num" in q, "140000")])
    assert eng._collations_taken("src", "appdb") is None
    got = eng._collation_versions("appdb")
    assert got.status == "ok" and "6" in got.detail, got.detail


def test_the_statistics_read_are_of_the_tables_taken(tmp_path):
    eng = _pg(tmp_path, [
        (lambda s, q: "pg_stat_user_tables" in q,
         "public.orders\t10\t1\t0\t1\npublic.event_log\t10\t0\t10\t0"),
        (lambda s, q: "name = 'autovacuum'" in q, "on")], include=TAKES)
    got = eng._planner_stats("appdb")
    assert "event_log" not in got.detail, got.detail


def test_the_row_level_security_read_is_of_the_tables_taken(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    eng = _pg(tmp_path, [(lambda s, q: q == PostgresEngine.RLS_FILTERED,
                          "public.event_log")], include=TAKES)
    assert eng._filtered_tables("src", "appdb") == []


def test_an_extensions_table_is_compared_only_where_the_hop_takes_it(
        tmp_path):
    from migkit.engines.postgres import PostgresEngine
    eng = _pg(tmp_path, [(lambda s, q: q == PostgresEngine.EXTENSION_DATA,
                          "postgis\tpublic.spatial_ref_sys")],
              include=TAKES)
    got = eng._extension_data("appdb")
    assert "spatial_ref_sys" not in got.detail, got.detail


def test_the_whole_database_second_schema_reading_is_not_run_for_a_hop_that_takes_some(  # noqa: E501
        tmp_path, monkeypatch):
    monkeypatch.setenv("MIGKIT_CROSSCHECK", "1")
    eng = _pg(tmp_path, [], include=TAKES)
    got = eng._crosscheck_schema("appdb")
    assert got.status == "skip" and "takes only some" in got.detail


def test_the_owners_compared_are_of_what_the_hop_takes(tmp_path):
    """A table, a view or a routine by its own name, a sequence by the
    table it feeds; a schema's owner is the schema of what is taken."""
    from migkit.engines.postgres import PostgresEngine
    lines = {"src": ("table public.orders|app\ntable public.event_log|app\n"
                     "table public.event_log_id_seq|app\n"
                     "routine public.f_log|app\nschema public|app"),
             "dst": ("table public.orders|app\ntable public.event_log|dts\n"
                     "table public.event_log_id_seq|dts\n"
                     "routine public.f_log|dts\nschema public|app")}
    eng = _pg(tmp_path, [
        (lambda s, q: q == PostgresEngine.OWNER_SQL, lambda side: lines[side]),
        (lambda s, q: q == PostgresEngine.SEQ_OWNED_Q,
         "public.event_log_id_seq|public|event_log|id")], include=TAKES)
    got = eng._deep_ownership("appdb")
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]
    assert "2 objects" in got[0].detail, got[0].detail


# ---- MySQL -----------------------------------------------------------------

def _my(tmp_path, answer, **kw):
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_hop("mysql", tmp_path, **kw))
    eng._q = lambda side, sql, args=None, fresh=False: answer(side, sql)
    return eng


def test_mysql_rows_of_a_listing_are_kept_by_their_table(tmp_path):
    eng = _my(tmp_path, lambda s, q: [], include=["appdb.orders"])
    rows = [("orders", "id"), ("event_log", "id"), ("orders", "note")]
    assert eng._scoped_rows("appdb", rows) == [("orders", "id"),
                                               ("orders", "note")]
    assert eng._scoped_rows("appdb", [("x", "orders")], 1) == [
        ("x", "orders")]


def test_mysql_text_and_float_columns_are_the_hops(tmp_path):
    def answer(side, sql):
        if "information_schema.columns" in sql:
            return [("event_log", "body"), ("orders", "note")]
        return [(0, 0, 0)]
    eng = _my(tmp_path, answer, exclude=["event_log"])
    assert eng._text_columns("appdb") == [("orders", "note")]
    got = eng._deep_float("appdb", "appdb")
    assert got[0].status == "ok" and "1 float" in got[0].detail, got


def test_mysql_a_check_not_enforced_on_a_table_left_out_is_not_a_finding(
        tmp_path):
    def answer(side, sql):
        if "check_constraints" in sql:
            if side == "src":
                return [("orders", "ck_a", "a > 0", "YES"),
                        ("event_log", "ck_b", "b > 0", "YES")]
            return [("orders", "ck_a", "a > 0", "YES"),
                    ("event_log", "ck_b", "b > 0", "NO")]
        return []
    eng = _my(tmp_path, answer, include=["appdb.orders"])
    got = eng._deep_unenforced_checks("appdb", "appdb")
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]


def test_mysql_the_definers_compared_are_of_what_the_hop_takes(tmp_path):
    """A view, routine or event by its own name; a trigger by its table."""
    def answer(side, sql):
        who = "app@%" if side == "src" else "dts@%"
        if "information_schema.views" in sql:
            return [("v_orders", "v_orders", "app@%", "DEFINER"),
                    ("v_audit", "v_audit", who, "DEFINER")]
        if "information_schema.triggers" in sql:
            return [("orders", "trg_orders", "app@%"),
                    ("audit_log", "trg_audit", who)]
        return []
    eng = _my(tmp_path, answer, include=["appdb.orders", "appdb.v_orders"])
    got = eng._deep_ownership("appdb", "appdb")
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]
    assert "2 objects" in got[0].detail, got[0].detail


# ---- SQL Server ------------------------------------------------------------

def _ms(tmp_path, answer, **kw):
    from migkit.engines.mssql import MSSQLEngine
    eng = MSSQLEngine(_hop("mssql", tmp_path, **kw))
    eng._cmd = lambda side, db, sql: answer(side, sql)
    return eng


def test_sql_server_data_check_compares_the_tables_taken(tmp_path):
    """The row-hash pass listed every table of both sides and compared
    them all; it reads the hop's tables, says one the target lacks, and
    says how each came out (`Result.tables`) and the rows it counted
    (`Result.rows_apart`) - what a hop keeping some changes off the
    target, and a service's own check, are held to."""
    def answer(side, sql):
        if "sys.tables" in sql:
            got = [["dbo.orders"], ["dbo.lines"], ["dbo.event_log"]]
            return got if side == "src" else got[:1] + got[2:]
        if "count_big" in sql:
            return [["3", "10"]] if side == "src" else [["4", "11"]]
        return []
    eng = _ms(tmp_path, answer,
              include=["appdb.dbo.orders", "appdb.dbo.lines"])
    eng._row_json = lambda db, t: ("x", [])
    eng._health = lambda side, db=None: None
    asked = []
    eng._drilldown = lambda db, t: asked.append(t) or (0, 1, 0)
    eng._resolve_inflight = lambda db, bad, stream=None: (bad, [], [])
    got = [r for r in eng.check_data("appdb") if r.check == "data"]
    assert asked == ["dbo.orders"]
    assert got[0].tables == {"dbo.orders": "diff", "dbo.lines": "diff"}
    assert got[0].rows_apart == {"dbo.orders": {
        "missing": 0, "extra": 1, "changed": 0, "exact": True}}
    assert "dbo.lines missing on target" in got[0].detail
    assert "event_log" not in got[0].detail


def test_sql_server_deep_listings_are_the_hops(tmp_path, monkeypatch):
    def answer(side, sql):
        if "not exists (select 1 from sys.indexes" in sql:
            return [["dbo.event_log"]]
        if "sys.foreign_keys" in sql:
            return [["dbo.event_log.fk_x", "1", "0", "dbo.event_log"]]
        if "sys.triggers" in sql:
            return [["dbo.event_log.trg", "dbo.event_log"]]
        if "information_schema.columns" in sql:
            text = ("dbo.orders.id|int|NO||||" if side == "src"
                    else "dbo.orders.id|int|NO||||")
            other = "dbo.event_log.v|int|YES||||" + (
                "" if side == "src" else "x")
            return [[text, "dbo.orders"], [other, "dbo.event_log"]]
        return []
    eng = _ms(tmp_path, answer, include=["appdb.dbo.orders"])
    import migkit.workload as workload
    monkeypatch.setattr(workload, "compare",
                        lambda e, db: Result("deep", f"{db} performance",
                                             "skip"))
    monkeypatch.setattr(type(eng), "_mojibake",
                        lambda self, db: Result("deep", f"{db} mojibake",
                                                "ok"))
    got = {r.scope: r for r in eng.check_deep("appdb")}
    for scope in ("appdb keys", "appdb fk", "appdb triggers",
                  "appdb columns"):
        assert got[scope].status == "ok", (scope, got[scope].detail)


def test_sql_server_counts_are_of_the_tables_taken(tmp_path):
    def answer(side, sql):
        return [["dbo.orders", "3"]] + (
            [["dbo.audit_log", "9"]] if side == "dst" else [])
    eng = _ms(tmp_path, answer, exclude=["audit_log"])
    got = eng.check_counts("appdb")
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]


# ---- Oracle, CockroachDB, ClickHouse ---------------------------------------

def test_oracle_invalid_objects_and_loose_constraints_are_the_hops(
        tmp_path, monkeypatch):
    from migkit.engines.oracle import OracleEngine
    eng = OracleEngine(_hop("oracle", tmp_path, include=["appdb.orders"]))

    def rows(side, db, sql, args=None):
        if "status = 'INVALID'" in sql:
            return [("TRIGGER TRG_AUDIT", "AUDIT_LOG"),
                    ("VIEW V_AUDIT", "V_AUDIT")]
        if "all_constraints" in sql:
            return [("CK_B on AUDIT_LOG (DISABLED, NOT VALIDATED)",
                     "AUDIT_LOG"),
                    ("CK_A on ORDERS (ENABLED, NOT VALIDATED)", "ORDERS")]
        return []
    eng._rows = rows
    monkeypatch.setattr(type(eng), "_mojibake",
                        lambda self, db: Result("deep", f"{db} mojibake",
                                                "ok"))
    got = {r.scope: r for r in eng.check_deep("appdb")}
    assert got["appdb target objects"].status == "ok", \
        got["appdb target objects"].detail
    loose = got["appdb target constraints"]
    assert loose.status == "diff" and "CK_A" in loose.detail \
        and "CK_B" not in loose.detail, loose.detail


def test_oracle_counters_are_the_ones_the_hop_takes(tmp_path):
    """The parity check and its repair read every sequence of the schema,
    and set the counters of tables the hop left out - ones the target
    writes itself. An identity column's goes with its table, a sequence a
    default draws from with the tables drawing from it, any other by its
    own name."""
    from migkit.engines.oracle import OracleEngine
    eng = OracleEngine(_hop("oracle", tmp_path, include=["appdb.orders"]))

    def rows(side, db, sql, args=None):
        if "all_tab_identity_cols" in sql and "join" not in sql:
            return [("ORDERS", "ID", "ISEQ$$_1"),
                    ("AUDIT_LOG", "ID", "ISEQ$$_2")]
        if "data_default" in sql:
            return [("ORDERS", '"APP"."ORDER_NO_SEQ"."NEXTVAL"'),
                    ("AUDIT_LOG", "AUDIT_SEQ.nextval")]
        if "all_sequences" in sql:
            return [("ISEQ$$_1", 10), ("ISEQ$$_2", 20),
                    ("ORDER_NO_SEQ", 30), ("AUDIT_SEQ", 40),
                    ("LOOSE_SEQ", 50)]
        return []
    eng._rows = rows
    eng._owner = lambda side, db: "APP"
    got = eng._sequences("src", "appdb")
    assert set(got) == {"orders.id", "order_no_seq"}, got
    every = OracleEngine(_hop("oracle", tmp_path))
    every._rows, every._owner = rows, eng._owner
    assert len(every._sequences("src", "appdb")) == 5
    none = OracleEngine(_hop("oracle", tmp_path,
                             objects={"sequences": False}))
    none._rows, none._owner = rows, eng._owner
    assert none._sequences("src", "appdb") == {}


def test_cockroach_a_broken_object_left_out_is_not_a_finding(tmp_path):
    from migkit.engines.cockroachdb import CockroachEngine
    eng = CockroachEngine(_hop("cockroachdb", tmp_path,
                               include=["appdb.public.orders"]))
    eng._rows = lambda side, db, sql, args=None: [
        ("audit_log", "descriptor broken", "public")]
    eng.rangefeeds = lambda side, db: True
    got = {r.scope: r for r in eng.check_deep("appdb")}
    assert got["appdb target objects"].status == "ok", \
        got["appdb target objects"].detail


def test_clickhouse_a_mutation_on_a_table_left_out_is_not_a_finding(
        tmp_path):
    from migkit.engines.clickhouse import ClickHouseEngine
    eng = ClickHouseEngine(_hop("clickhouse", tmp_path,
                                exclude=["appdb.audit_log"]))
    eng._rows = lambda side, db, sql, params=None, settings=None: [
        ("audit_log", "DELETE WHERE 1", "")]
    got = eng.check_deep("appdb")
    assert [r.status for r in got] == ["ok", "ok"], [r.detail for r in got]


def test_db2_a_table_left_out_left_pending_is_not_a_finding(tmp_path,
                                                            monkeypatch):
    from migkit.engines.db2 import Db2Engine
    eng = Db2Engine(_hop("db2", tmp_path, include=["appdb.orders"]))
    eng._rows = lambda side, db, sql, args=None: [
        ("AUDIT_LOG   ", "C", "F")]
    monkeypatch.setattr(type(eng), "_mojibake",
                        lambda self, db: Result("deep", f"{db} mojibake",
                                                "ok"))
    got = {r.scope: r for r in eng.check_deep("appdb")}
    assert got["appdb target tables"].status == "ok", \
        got["appdb target tables"].detail
    eng._rows = lambda side, db, sql, args=None: [("ORDERS", "C", "F")]
    got = {r.scope: r for r in eng.check_deep("appdb")}
    assert got["appdb target tables"].status == "diff"


def test_teradata_a_table_left_out_left_mid_load_is_not_read(tmp_path):
    from migkit.engines.teradata import TeradataEngine
    eng = TeradataEngine(_hop("teradata", tmp_path,
                              exclude=["appdb.audit_log"]))
    read = []

    def rows(side, db, sql, args=None):
        if "DBC.TablesV" in sql:
            return [("orders",), ("audit_log",)]
        read.append(sql)
        return [(1,)]
    eng._rows = rows
    got = eng._left_by_loads("appdb")
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]
    assert read and all("audit_log" not in q for q in read), read

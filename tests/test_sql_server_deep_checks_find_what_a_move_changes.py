"""SQL Server's deep checks find what PostgreSQL's and MySQL's do - values
that arrive wrong while looking present, structure and constraints the
schema comparison cannot see, and the access that does not move with the
data (P0-PARITY.verify).

Without a server: a scripted one (`tests/mssql_stub.py`) answers the
catalogue reads with rows the test gives, and what is held is what each
check makes of them - which finding it is, what it names, what it leaves
alone. Every statement here is new and has not run on a server, so none of
these items is in `PROVES`: each waits in `capabilities.UNPROVED` for the
test that runs it on one.
"""

from migkit.engines.base import Result
from tests.mssql_stub import Server, engine


def _both(server, pattern, src, dst):
    """The rows a statement answers on each side."""
    return server.on(pattern, src, "src").on(pattern, dst, "dst")


def _eng(monkeypatch, tmp_path, server, **kw):
    eng = engine(monkeypatch, tmp_path, server, **kw)
    eng._pk_cols = lambda db, t: ["id"]
    eng._row_json = lambda db, t: ("x", [])
    return eng


def test_a_narrower_target_column_is_named_with_why(monkeypatch, tmp_path):
    s = _both(Server(), "character_maximum_length",
              [("dbo.orders", "note", "varchar", 50, None, None, "c"),
               ("dbo.orders", "price", "decimal", None, 10, 2, None),
               ("dbo.orders", "qty", "int", None, 10, 0, None),
               ("dbo.audit", "msg", "varchar", 50, None, None, "c")],
              [("dbo.orders", "note", "varchar", 20, None, None, "c"),
               ("dbo.orders", "price", "decimal", None, 8, 2, None),
               ("dbo.orders", "qty", "smallint", None, 5, 0, None),
               ("dbo.audit", "msg", "varchar", 10, None, None, "c")])
    eng = _eng(monkeypatch, tmp_path, s, include=["appdb.dbo.orders"])
    got = eng._deep_narrowing("appdb")
    assert got.status == "diff" and got.category == "value.narrowing"
    assert "varchar(50) -> varchar(20) (longer values are cut)" in got.detail
    assert "decimal(10,2) -> decimal(8,2)" in got.detail
    assert "int -> smallint (overflow)" in got.detail
    # a table the hop leaves out is the target's own
    assert "audit" not in got.detail


def test_a_column_as_wide_as_the_source_is_not_narrower(monkeypatch,
                                                         tmp_path):
    row = [("dbo.t", "a", "nvarchar", -1, None, None, "c"),
           ("dbo.t", "b", "varchar", 30, None, None, "c")]
    wider = [("dbo.t", "a", "nvarchar", -1, None, None, "c"),
             ("dbo.t", "b", "varchar", 60, None, None, "c")]
    eng = _eng(monkeypatch, tmp_path, _both(Server(), "character_maximum",
                                            row, wider))
    assert eng._deep_narrowing("appdb").status == "ok"


def test_a_timestamp_column_shifted_as_a_whole_is_a_zone_converted(
        monkeypatch, tmp_path):
    s = Server()
    _both(s, "datediff_big", [("1", 1000), ("2", 2000), ("3", 3000)],
          [("1", 1000 + 25200), ("2", 2000 + 25200), ("3", 3000 + 25200)])
    s.on("information_schema.columns c where c.data_type in",
         [("dbo.orders", "placed", "datetime2")])
    eng = _eng(monkeypatch, tmp_path, s)
    got = eng._deep_timeshift("appdb")
    assert got.status == "diff" and got.category == "value.timezone"
    assert "dbo.orders.placed: every row shifted 25200s" in got.detail
    # the instant is asked at UTC, a zone-aware column switched to it
    asked = s.sent("datediff_big")
    assert asked and all("cast('1970-01-01' as datetime2)" in q
                         for q in asked)


def test_timestamps_that_agree_are_no_shift(monkeypatch, tmp_path):
    s = Server()
    same = [("1", 1000), ("2", 2000), ("3", 3000)]
    _both(s, "datediff_big", same, same)
    s.on("information_schema.columns c where c.data_type in",
         [("dbo.orders", "placed", "datetimeoffset")])
    eng = _eng(monkeypatch, tmp_path, s)
    assert eng._deep_timeshift("appdb").status == "ok"
    assert any("switchoffset(t.[placed], '+00:00')" in q
               for q in s.sent("datediff_big"))


def test_a_text_column_downgraded_loses_what_it_cannot_hold(monkeypatch,
                                                             tmp_path):
    s = Server()
    _both(s, "collationproperty",
          [("dbo.people", "name", "nvarchar", 0),
           ("dbo.people", "city", "varchar", 65001),
           ("dbo.people", "note", "varchar", 1252)],
          [("dbo.people", "name", "varchar", 1252),
           ("dbo.people", "city", "varchar", 1252),
           ("dbo.people", "note", "varchar", 1252)])
    eng = _eng(monkeypatch, tmp_path, s)
    got = eng._deep_charset("appdb")
    assert got.status == "diff" and got.category == "value.charset"
    assert "dbo.people.name: nvarchar/0 -> varchar/1252 (DOWNGRADE" \
        in got.detail
    assert "dbo.people.city: varchar/65001 -> varchar/1252 (DOWNGRADE" \
        in got.detail
    assert "note" not in got.detail


def test_replacement_characters_that_appeared_on_the_target_are_counted(
        monkeypatch, tmp_path):
    s = Server()
    _both(s, "collationproperty", [("dbo.people", "name", "nvarchar", 0)],
          [("dbo.people", "name", "nvarchar", 0)])
    s.on("charindex", lambda side, sql: [(0,)] if side == "src" else [(3,)])
    eng = _eng(monkeypatch, tmp_path, s)
    got = eng._deep_charset("appdb")
    assert got.status == "diff"
    assert "3 more rows hold U+FFFD on the target" in got.detail
    assert any("charindex(nchar(65533), [name])" in q
               for q in s.sent("charindex"))
    # the same count on both sides is no finding
    s.rules[-1] = (None, s.rules[-1][1], [(3,)])
    assert eng._deep_charset("appdb").status == "ok"


def test_a_unique_key_whose_collation_collapses_rows_says_so(monkeypatch,
                                                              tmp_path):
    s = Server()
    s.on("is_primary_key = 1 or i.is_unique = 1",
         lambda side, sql: [("dbo.users", "email",
                             "Latin1_General_CS_AS" if side == "src"
                             else "Latin1_General_CI_AS", 1)])
    s.on("collate Latin1_General_CI_AS having", [(2,)])
    eng = _eng(monkeypatch, tmp_path, s)
    got = eng._deep_collation("appdb")
    assert got.status == "diff" and got.category == "value.collation"
    assert "2 source groups COLLAPSE" in got.detail
    # the collation named in the statement is the target's, from its
    # catalogue and nothing else
    assert s.sent("collate Latin1_General_CI_AS having", "src")


def test_the_same_collation_is_no_finding(monkeypatch, tmp_path):
    row = [("dbo.users", "email", "Latin1_General_CS_AS", 1)]
    eng = _eng(monkeypatch, tmp_path,
               _both(Server(), "is_primary_key = 1", row, row))
    assert eng._deep_collation("appdb").status == "ok"


def test_a_computed_column_the_target_holds_plain_is_found(monkeypatch,
                                                            tmp_path):
    s = Server()
    s.on("sys.computed_columns",
         lambda side, sql: [("dbo.lines", "total", "([a]+[b])", 1),
                            ("dbo.lines", "tax", "([a]*(0.2))", 0)]
         if side == "src" else
         [("dbo.lines", "tax", "([a]*(0.25))", 0),
          ("dbo.lines", "extra", "([a])", 0)])
    eng = _eng(monkeypatch, tmp_path, s)
    got = eng._deep_generated("appdb")
    assert got.status == "diff" and got.category == "value.generated"
    assert "dbo.lines.total: generated on the source, plain or missing" \
        in got.detail
    assert "dbo.lines.tax: generation expression differs" in got.detail
    assert "dbo.lines.extra: generated on the target but plain" in got.detail


def test_a_computed_column_that_reads_alike_is_no_finding(monkeypatch,
                                                           tmp_path):
    eng = _eng(monkeypatch, tmp_path, Server().on(
        "sys.computed_columns",
        lambda side, sql: [("dbo.lines", "total",
                            "([a]+[b])" if side == "src" else "a + b", 1)]))
    assert eng._deep_generated("appdb").status == "ok"


def test_null_and_empty_swapped_between_the_sides_is_found(monkeypatch,
                                                            tmp_path):
    s = Server()
    s.on("select sum\\(case when", lambda side, sql: [(1, 5)]
         if side == "src" else [(5, 1)])
    s.on("information_schema.columns c where c.data_type in",
         [("dbo.people", "nick")])
    eng = _eng(monkeypatch, tmp_path, s)
    got = eng._deep_nullempty("appdb")
    assert got.status == "diff" and got.category == "value.null-empty"
    assert "dbo.people.nick: src null/empty=1|5 dst=5|1" in got.detail


def test_floats_that_sum_apart_are_found_and_those_that_do_not_are_not(
        monkeypatch, tmp_path):
    s = Server()
    s.on("information_schema.columns c where c.data_type in",
         [("dbo.m", "x"), ("dbo.m", "y")])

    def agg(side, sql):
        # x drifts; y is the same to the last digit
        if "[x]" in sql:
            return [(3, 10.0 if side == "src" else 10.5, 4.0)]
        return [(3, 7.0, 4.0)]
    s.on("isnull\\(sum\\(cast", agg)
    eng = _eng(monkeypatch, tmp_path, s)
    got = eng._deep_float("appdb")
    assert got.status == "diff" and got.category == "value.precision"
    assert "dbo.m.x: sum" in got.detail and "dbo.m.y" not in got.detail


def test_text_that_renders_otherwise_is_found(monkeypatch, tmp_path):
    s = Server()
    s.on("ty.name in", [("dbo.shapes", "area", "geography")])
    s.on("top \\(5\\)", [("1", "POINT (1 2)")])
    s.on("in \\('1'\\)", [("1", "POINT (1.0 2.0)")], "dst")
    eng = _eng(monkeypatch, tmp_path, s)
    got = eng._deep_render("appdb")
    assert got.status == "diff" and got.category == "value.rendering"
    assert "dbo.shapes.area: renders differently" in got.detail
    assert any("t.[area].ToString()" in q for q in s.sent("top"))


def test_a_check_constraint_loaded_without_checking_is_found(monkeypatch,
                                                              tmp_path):
    s = Server()
    s.on("sys.check_constraints", lambda side, sql: [
        ("dbo.t", "ck_a", "([n]>(0))", 0, 0),
        ("dbo.t", "ck_b", "([m]>(0))", 0, 0),
        ("dbo.t", "ck_c", "([k]>(0))", 0, 0)] if side == "src" else [
        ("dbo.t", "ck_a", "([n]>(0))", 0, 1),
        ("dbo.t", "ck_b", "([m]>(0))", 1, 0)])
    eng = _eng(monkeypatch, tmp_path, s)
    got = eng._deep_unenforced_checks("appdb")
    assert got.status == "diff"
    assert got.category == "structure.unvalidated-constraints"
    assert "dbo.t.ck_a not trusted on target" in got.detail
    assert "dbo.t.ck_b DISABLED on target" in got.detail
    assert "dbo.t.ck_c missing on target" in got.detail


def test_checks_that_match_are_no_finding(monkeypatch, tmp_path):
    row = [("dbo.t", "ck_a", "([n]>(0))", 0, 0)]
    eng = _eng(monkeypatch, tmp_path,
               _both(Server(), "sys.check_constraints", row, row))
    assert eng._deep_unenforced_checks("appdb").status == "ok"


def _partitioned(s, src_rows, dst_rows, dst_scheme=True):
    scheme = [("dbo.events", "pf_month", 0, 3, "at")]
    s.on("sys.partition_schemes", scheme, "src")
    if dst_scheme:
        s.on("sys.partition_schemes", scheme, "dst")
    bounds = [("pf_month", 1, "2026-01-01"), ("pf_month", 2, "2026-02-01")]
    s.on("sys.partition_range_values", bounds)
    s.on("PARTITION", lambda side, sql: src_rows if side == "src"
         else dst_rows)
    s.on("neutral|sys.tables", [("dbo.events",)])
    return s


def test_a_partition_that_arrived_empty_is_found(monkeypatch, tmp_path):
    s = _partitioned(Server(), [(1, 5, "11"), (2, 3, "22"), (3, 1, "33")],
                     [(1, 5, "11"), (3, 1, "33")])
    eng = _eng(monkeypatch, tmp_path, s)
    got = eng._deep_partitions("appdb")
    assert got.status == "diff" and got.category == "structure.partitions"
    assert "dbo.events partition p2: empty on target" in got.detail
    # every partition is read in one statement by the server's own function
    assert any("$PARTITION.[pf_month](t.[at])" in q
               for q in s.sent("PARTITION", "src"))


def test_partitions_that_hold_the_same_rows_are_no_finding(monkeypatch,
                                                            tmp_path):
    rows = [(1, 5, "11"), (2, 3, "22"), (3, 1, "33")]
    eng = _eng(monkeypatch, tmp_path, _partitioned(Server(), rows, rows))
    got = eng._deep_partitions("appdb")
    assert got.status == "ok", got.detail
    assert "3 partitions" in got.detail


def test_a_table_partitioned_on_one_side_only_is_found(monkeypatch,
                                                       tmp_path):
    s = _partitioned(Server(), [(1, 5, "11")], [], dst_scheme=False)
    eng = _eng(monkeypatch, tmp_path, s)
    got = eng._deep_partitions("appdb")
    assert got.status == "diff"
    assert "dbo.events: partitioned on source, not on target" in got.detail \
        or "missing on target" in got.detail


def test_the_bounds_of_a_partition_follow_the_function_side():
    from migkit.engines.mssql import MSSQLEngine as E
    left = {"m": "range left", "bounds": ["10", "20"]}
    right = {"m": "range right", "bounds": ["10", "20"]}
    assert [E._partition_bound(left, n) for n in (1, 2, 3)] == [
        "<= 10", "<= 20", "> 20"]
    assert [E._partition_bound(right, n) for n in (1, 2, 3)] == [
        "< 10", ">= 10", ">= 20"]


def test_an_index_a_load_switched_off_is_found(monkeypatch, tmp_path):
    s = Server()
    s.on("sys.indexes i join sys.tables t", lambda side, sql: [
        ("dbo.orders", "ix_a", 0, 0), ("dbo.orders", "ix_b", 0, 0)]
        if side == "src" else [
        ("dbo.orders", "ix_a", 0, 0), ("dbo.orders", "ix_b", 1, 0)])
    eng = _eng(monkeypatch, tmp_path, s)
    got = eng._deep_indexes("appdb")
    assert got.status == "diff" and got.category == "structure.indexes"
    assert "target dbo.orders.ix_b (disabled" in got.detail
    eng2 = _eng(monkeypatch, tmp_path, Server().on(
        "sys.indexes i join", [("dbo.orders", "ix_a", 0, 0)]))
    assert eng2._deep_indexes("appdb").status == "ok"


def test_a_security_policy_the_data_arrived_without_is_found(monkeypatch,
                                                              tmp_path):
    s = Server()
    s.on("serverproperty", [(16,)])
    s.on("sys.security_policies", lambda side, sql: [
        ("sec.by_region", 1, 1, "FILTER_PREDICATE", "NULL",
         "([sec].[fn]([region]))", "dbo.orders")] if side == "src" else [])
    eng = _eng(monkeypatch, tmp_path, s)
    got = eng._deep_row_security("appdb")
    assert got.status == "diff" and got.category == "structure.row-security"
    assert "policy sec.by_region: missing on target" in got.detail


def test_a_server_before_row_security_has_no_policy_to_lose(monkeypatch,
                                                             tmp_path):
    s = Server().on("serverproperty", [(12,)])
    eng = _eng(monkeypatch, tmp_path, s)
    got = eng._deep_row_security("appdb")
    assert got.status == "ok" and "no row-level security" in got.detail
    assert not s.sent("sys.security_policies")


def test_a_routine_that_runs_as_another_account_is_found(monkeypatch,
                                                          tmp_path):
    s = Server()
    s.on("execute_as_principal_id", lambda side, sql: [
        ("dbo.p_close", "SQL_STORED_PROCEDURE",
         "app_owner" if side == "src" else "CALLER", "dbo.p_close"),
        ("dbo.v_all", "VIEW", "CALLER", "dbo.v_all")])
    eng = _eng(monkeypatch, tmp_path, s)
    got = eng._deep_ownership("appdb")
    assert got.status == "diff" and got.category == "access.object-ownership"
    assert "EXECUTE AS app_owner -> EXECUTE AS CALLER" in got.detail
    assert "now run with the caller's privileges" in got.detail
    # a hop that carries no procedures leaves theirs out
    eng = _eng(monkeypatch, tmp_path, s, objects={"procedures": False})
    assert eng._deep_ownership("appdb").status == "ok"


def _grants(s, src, dst, principals=("app", "report")):
    def perms(side, sql):
        return src if side == "src" else dst
    s.on("from sys.database_permissions p join sys.database_principals g",
         perms)
    s.on("o.type in \\('U', 'V', 'SO'\\)",
         [("dbo.orders", "U"), ("dbo.seq_a", "SO")])
    s.on("from sys.database_principals where principal_id > 4",
         [(p,) for p in principals])
    return s


def _perm(who, perm, obj, state="GRANT"):
    return (who, state, perm, 1, "dbo", obj, None, None, "OBJECT_OR_COLUMN",
            1, 0)


def test_a_table_grant_and_a_sequence_grant_that_did_not_move_are_found(
        monkeypatch, tmp_path):
    s = _grants(Server(),
                [_perm("app", "SELECT", "orders"),
                 _perm("app", "INSERT", "orders"),
                 _perm("app", "UPDATE", "seq_a"),
                 _perm("sa", "SELECT", "orders")],
                [_perm("app", "SELECT", "orders")])
    eng = _eng(monkeypatch, tmp_path, s)
    tables, sequences = eng._deep_grants("appdb")
    assert tables.scope == "appdb grants" and tables.status == "diff"
    assert tables.category == "access.table-grants"
    assert "1 table and view grants missing on target" in tables.detail
    assert "INSERT ON OBJECT::[dbo].[orders]" in tables.detail
    assert sequences.scope == "appdb seq-grants" and \
        sequences.status == "diff"
    assert sequences.category == "access.sequence-grants"
    assert "UPDATE ON OBJECT::[dbo].[seq_a]" in sequences.detail


def test_grants_that_match_are_no_finding_and_a_hop_without_users_skips(
        monkeypatch, tmp_path):
    same = [_perm("app", "SELECT", "orders")]
    s = _grants(Server(), same, same)
    tables, sequences = _eng(monkeypatch, tmp_path, s)._deep_grants("appdb")
    assert (tables.status, sequences.status) == ("ok", "ok")
    eng = _eng(monkeypatch, tmp_path, s, objects={"roles": False})
    assert [r.status for r in eng._deep_grants("appdb")] == ["skip", "skip"]


def test_an_account_the_target_lacks_is_said_where_its_grants_are(
        monkeypatch, tmp_path):
    s = _grants(Server(), [_perm("report", "SELECT", "orders")], [],
                principals=("app",))
    tables, _ = _eng(monkeypatch, tmp_path, s)._deep_grants("appdb")
    assert tables.status == "diff"
    assert "1 users hold table and view grants on the source but do not" \
        " exist on the target: report" in tables.detail


def test_a_check_that_cannot_read_is_a_finding_not_a_pass(monkeypatch,
                                                           tmp_path):
    def refuse(side, sql):
        raise RuntimeError("the source refused a statement: denied")
    s = Server().on("sys.check_constraints", refuse)
    eng = _eng(monkeypatch, tmp_path, s)
    got = eng._guarded("appdb", "checks", eng._deep_unenforced_checks)
    assert [(r.scope, r.status) for r in got] == [("appdb checks", "error")]
    assert "unknown, not clean" in got[0].detail


def test_the_deep_check_runs_every_one_of_them(monkeypatch, tmp_path):
    import migkit.workload as workload
    s = Server()
    eng = _eng(monkeypatch, tmp_path, s)
    monkeypatch.setattr(workload, "compare",
                        lambda e, db: Result("deep", f"{db} performance",
                                             "skip"))
    monkeypatch.setattr(type(eng), "_mojibake",
                        lambda self, db: Result("deep", f"{db} mojibake",
                                                "ok"))
    got = {r.scope: r for r in eng.check_deep("appdb")}
    for word in ("narrowing", "timeshift", "charset", "collation",
                 "generated", "nullempty", "float", "render", "checks",
                 "partitions", "indexes", "rls", "ownership", "grants",
                 "seq-grants"):
        assert f"appdb {word}" in got, (word, sorted(got))
        assert got[f"appdb {word}"].status != "error", \
            got[f"appdb {word}"].detail

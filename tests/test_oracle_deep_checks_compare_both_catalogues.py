"""Oracle's deep checks of a finished move (P0-PARITY.verify), without a
server: both sides' catalogue answered in-process, each check held to what
it finds and to the hop's scope.

None of the new statements these checks send has run against an Oracle
server, so almost no cell of the depth matrix is proved by this module
(`capabilities.UNPROVED` says each is owed a sandbox run). What it holds is
what is migkit's own - the comparisons, the scope, the words, and that a
check that cannot read says so as an error and never as a pass. The three
cells named in `PROVES` rest on statements the engine already sent.
"""
import tempfile
from pathlib import Path

from migkit.config import Endpoint, Hop
from migkit.engines.base import CATEGORIES, Result
from migkit.engines.oracle import OracleEngine
from tests.oracle_catalogue import cat  # noqa: F401

EP = Endpoint(host="10.0.0.5", port=1521, user="app", password="CHANGE_ME")

#: what this module proves, item by engine (`capabilities.proofs`): the three
#: whose every statement is one the engine already sent
PROVES = [
    ("verify.structure.unvalidated-constraints", "oracle"),
    ("verify.value.charset", "oracle"),
    ("move.create-missing", "oracle"),
]


def _eng(**kw):
    hop = Hop(name="t", engine="oracle", source=EP, target=EP,
              databases=["app"], **kw)
    where = Path(tempfile.mkdtemp())
    hop.report_dir = lambda db=None: where
    return OracleEngine(hop)


# ---- keys, foreign keys, deferrable -----------------------------------------

def test_a_table_no_key_names_a_row_of_is_found_on_the_source(cat):
    cat.on("not exists (select 1 from all_constraints", [("EVENTS",),
                                                       ("AUDIT_LOG",)])
    got = _eng(exclude=["audit_log"])._deep_keys("app")
    assert got.status == "diff" and "1 tables have no pk/unique" in got.detail
    assert "events" in got.detail and "audit_log" not in got.detail
    cat.answers.clear()
    cat.on("not exists (select 1 from all_constraints", [])
    assert _eng()._deep_keys("app").status == "ok"


def test_a_row_pointing_at_nothing_is_an_orphan_and_a_parent_left_out_is_said(
        cat):
    eng = _eng(exclude=["archive", "archive_rows"])
    eng.foreign_keys = lambda side, db: [
        ("lines", ["order_id"], "orders", ["id"]),
        ("notes", ["lead_id"], "archive", ["id"]),
        ("archive_rows", ["x"], "orders", ["id"])]
    counts = {"LINES": 4, "NOTES": 2}

    def count(side, sql):
        return [(next((n for t, n in counts.items() if f'"{t}"' in sql
                       and "not exists" in sql), 0),)]
    cat.on("select count(*) from", count)
    got = eng._deep_fk("app")
    assert got.status == "diff"
    assert "lines(order_id): 4 orphan rows" in got.detail
    assert "notes(lead_id): 2 rows point at archive" in got.detail
    # the child of a table the hop leaves out is not read at all
    assert not [s for _, s in cat.sent if "ARCHIVE_ROWS" in s]
    counts["LINES"] = 0
    assert eng._deep_fk("app").status == "warn"
    counts["NOTES"] = 0
    assert eng._deep_fk("app").status == "ok"


def test_a_constraint_checked_at_another_time_is_a_difference(cat):
    cat.on("c.deferrable", lambda side, sql: [
        ("ORDERS", "R", "DEFERRABLE" if side == "src" else "NOT DEFERRABLE",
         "DEFERRED" if side == "src" else "IMMEDIATE", "CUSTOMER_ID"),
        ("LINES", "P", "NOT DEFERRABLE", "IMMEDIATE", "ID")])
    got = _eng()._deep_deferrable("app")
    assert got.status == "diff"
    assert "orders foreign key (CUSTOMER_ID): deferrable, deferred on the" \
        " source, checked at each statement on the target" in got.detail
    cat.answers.clear()
    cat.on("c.deferrable", [("LINES", "P", "NOT DEFERRABLE", "IMMEDIATE",
                             "ID")])
    assert _eng()._deep_deferrable("app").status == "ok"


# ---- columns -----------------------------------------------------------------

def _facts(side, narrow=False, drift=False, gen=False):
    return [
        ("ORDERS", "ID", "NUMBER(10,0)", "N", None, "NO"),
        ("ORDERS", "NOTE", "VARCHAR2(10)" if side == "dst" and narrow
         else "VARCHAR2(40)", "Y", "'new'" if not (side == "dst" and drift)
         else None, "NO"),
        ("ORDERS", "TOTAL", "NUMBER(12,2)", "Y",
         "price * qty" if side == "src" else
         ("price * qty" if not gen else None), "YES" if side == "src" or not
         gen else "NO"),
        ("AUDIT_LOG", "BODY", "VARCHAR2(10)" if side == "dst"
         else "VARCHAR2(99)", "Y", None, "NO")]


def test_columns_that_drifted_narrowed_or_stopped_being_computed_are_found(
        cat):
    cat.on("from all_tab_cols c", lambda side, sql: _facts(
        side, narrow=True, drift=True, gen=True))
    eng = _eng(exclude=["audit_log"])
    cols, narrow, gen = eng._deep_columns("app")
    assert cols.status == "diff" and "orders.note" in cols.detail
    assert Path(cols.report).read_text().count("orders.note") == 1
    assert narrow.status == "diff" and "orders.note" in narrow.detail
    assert "VARCHAR2(40) -> VARCHAR2(10)" not in narrow.detail  # as varchar
    assert "longer values are cut" in narrow.detail
    assert "audit_log" not in narrow.detail
    assert gen.status == "diff" and "orders.total: computed on the source," \
        " plain on the target" in gen.detail
    assert [r.scope.split()[-1] for r in (cols, narrow, gen)] == [
        "columns", "narrowing", "generated"]


def test_columns_that_match_are_ok(cat):
    cat.on("from all_tab_cols c", lambda side, sql: _facts(side)[:3])
    cols, narrow, gen = _eng()._deep_columns("app")
    assert [r.status for r in (cols, narrow, gen)] == ["ok", "ok", "ok"]
    assert "3 columns compared" in cols.detail


def test_a_collation_that_differs_is_found_and_a_key_column_is_said(cat):
    cat.on("collation from all_tab_cols", lambda side, sql: [
        ("USERS", "EMAIL", "BINARY" if side == "src" else "BINARY_CI"),
        ("USERS", "NOTE", "USING_NLS_COMP")])
    cat.on("c.constraint_type in ('P', 'U')", [("USERS", "EMAIL")])
    got = _eng()._deep_collation("app")
    assert got.status == "diff"
    assert "users.email: BINARY on the source, BINARY_CI on the target" \
        in got.detail and "in a key" in got.detail
    cat.answers.clear()
    cat.on("collation from all_tab_cols",
           RuntimeError("ORA-00904: \"COLLATION\": invalid identifier"))
    got = _eng()._deep_collation("app")
    assert got.status == "skip" and "12.2" in got.detail


# ---- values ------------------------------------------------------------------

def test_a_character_set_the_target_cannot_widen_back_is_a_downgrade(
        monkeypatch):
    eng = _eng()
    settings = {"src": {"nls_characterset": "AL32UTF8",
                        "nls_nchar_characterset": "AL16UTF16"},
                "dst": {"nls_characterset": "WE8MSWIN1252",
                        "nls_nchar_characterset": "AL16UTF16"}}
    eng._settings = lambda side, db: settings[side]
    got = eng._deep_charset("app")
    assert got.status == "diff" and "DOWNGRADE" in got.detail
    assert "database character set AL32UTF8 on the source, WE8MSWIN1252" \
        in got.detail
    settings["dst"] = dict(settings["src"])
    assert eng._deep_charset("app").status == "ok"


def test_a_text_column_that_lost_its_values_to_null_is_found(cat):
    cat.on("c.data_type from all_tab_columns c", [
        ("ORDERS", "NOTE", "VARCHAR2"), ("ORDERS", "CITY", "VARCHAR2")])

    def counts(side, sql):
        if '"NOTE"' in sql:
            return [(10, 10 if side == "src" else 7)]
        return [(10, 10)]
    cat.on("select count(*), count(", counts)
    got = _eng()._deep_nullempty("app")
    assert got.status == "diff"
    assert "orders.note: 10 values on the source, 7 on the target" \
        in got.detail and "city" not in got.detail
    # tables of other row counts are the counts check's to say
    cat.answers[-1] = ("select count(*), count(", None,
                       lambda side, sql: [(10 if side == "src" else 9, 10)])
    assert _eng()._deep_nullempty("app").status == "ok"


def test_a_check_that_read_part_of_the_columns_says_so(cat):
    cat.on("c.data_type from all_tab_columns c",
           [("T", f"C{i}", "VARCHAR2") for i in range(35)])
    cat.on("select count(*), count(", [(1, 1)])
    got = _eng()._deep_nullempty("app")
    assert got.status == "warn" and "read 30 of 35" in got.detail


def test_approximate_numbers_are_held_to_a_tolerance(cat):
    cat.on("c.data_type from all_tab_columns c", [("M", "RATIO",
                                                   "BINARY_DOUBLE")])
    cat.on("nvl(sum(", lambda side, sql: [
        (5, 10.5 if side == "src" else 10.5 + 1e-12, 4.0)])
    assert _eng()._deep_float("app").status == "ok"
    cat.answers[-1] = ("nvl(sum(", None, lambda side, sql: [
        (5, 10.5 if side == "src" else 11.0, 4.0)])
    got = _eng()._deep_float("app")
    assert got.status == "diff" and "m.ratio: sum src=10.5 dst=11.0" \
        in got.detail


def test_a_column_every_row_of_which_is_an_hour_away_is_a_shift(cat):
    cat.on("constraint_type = 'P'", [("EVENTS", "ID")])
    cat.on("c.data_type from all_tab_columns c", [
        ("EVENTS", "AT", "TIMESTAMP(6)")])
    cat.on("86400", lambda side, sql: [
        (i, 1_700_000_000 + i + (3600 if side == "dst" else 0))
        for i in range(1, 6)])
    got = _eng()._deep_timeshift("app")
    assert got.status == "diff" and "events.at: every row shifted 3600s" \
        in got.detail
    cat.answers[-1] = ("86400", None, lambda side, sql: [
        (i, 1_700_000_000 + i) for i in range(1, 6)])
    assert _eng()._deep_timeshift("app").status == "ok"


def test_a_zone_region_lost_is_a_rendering_difference(cat):
    cat.on("constraint_type = 'P'", [("EVENTS", "ID")])
    cat.on("c.data_type from all_tab_columns c", [
        ("EVENTS", "AT", "TIMESTAMP(6) WITH TIME ZONE")])
    cat.on("to_char(", lambda side, sql: [
        ("1", "2026-01-01 10:00:00.000000000 "
              + ("EUROPE/ROME" if side == "src" else "+01:00"))])
    got = _eng()._deep_render("app")
    assert got.status == "diff" and "events.at: renders differently" \
        in got.detail
    cat.answers[-1] = ("to_char(", None, [("1", "x")])
    assert _eng()._deep_render("app").status == "ok"


def test_a_target_ahead_of_the_source_is_found(cat):
    cat.on("constraint_type = 'P'", [("ORDERS", "ID"), ("LINES", "ID")])
    cat.on('"ORDERS"', lambda side, sql: [(100 if side == "src" else 140,)])
    cat.on('"LINES"', lambda side, sql: [(50 if side == "src" else 20,)])
    got = _eng()._deep_boundary("app")
    assert got.status == "diff" and "orders src_max=100 dst_max=140" \
        in got.detail
    cat.answers[1] = ('"ORDERS"', None, [(100,)])
    got = _eng()._deep_boundary("app")
    assert got.status == "ok" and "1 behind" in got.detail


# ---- what hangs off a table --------------------------------------------------

def test_triggers_missing_or_left_disabled_on_the_target_are_found(cat):
    cat.on("from all_triggers", lambda side, sql: [
        ("TRG_A", "ORDERS", "ENABLED"), ("TRG_B", "ORDERS", "ENABLED")]
        if side == "src" else [("TRG_B", "ORDERS", "DISABLED")])
    got = _eng()._deep_triggers("app")
    assert got.status == "diff"
    assert "1 missing on the target: trg_a" in got.detail
    assert "1 disabled on the target: trg_b" in got.detail
    cat.answers.clear()
    cat.on("from all_triggers", [("TRG_B", "ORDERS", "ENABLED")])
    assert _eng()._deep_triggers("app").status == "ok"
    # a trigger on a table the hop leaves out is not asked about
    cat.answers.clear()
    cat.on("from all_triggers", lambda side, sql: [
        ("TRG_X", "AUDIT_LOG", "ENABLED")] if side == "src" else [])
    assert _eng(exclude=["audit_log"])._deep_triggers("app").status == "ok"


def _indexes(side):
    rows = [("ORDERS", "IX_CUST", "NONUNIQUE", "VALID", 1, "CUSTOMER_ID"),
            ("ORDERS", "SYS_C1", "UNIQUE", "VALID", 1, "REF"),
            ("ORDERS", "IX_F", "NONUNIQUE", "VALID", 1, "SYS_NC0001$")]
    if side == "dst":
        # the same by what they cover, under other names; one unusable
        rows = [("ORDERS", "IX_CUST2", "NONUNIQUE", "UNUSABLE", 1,
                 "CUSTOMER_ID"),
                ("ORDERS", "SYS_C9", "UNIQUE", "VALID", 1, "REF")]
    return rows


def test_an_index_missing_or_unusable_on_the_target_is_found(cat):
    cat.on("from all_indexes i join all_ind_columns", lambda side, sql:
           _indexes(side))
    cat.on("from all_ind_expressions", [("IX_F", 1, "UPPER(\"NAME\")")],
           side="src")
    cat.on("from all_ind_expressions", [], side="dst")
    got = _eng()._deep_indexes("app")
    assert got.status == "diff"
    assert "1 missing on the target: orders (UPPER(\"NAME\"))" in got.detail
    assert "1 unusable on the target: orders.ix_cust2" in got.detail
    cat.answers.clear()
    cat.on("from all_indexes i join all_ind_columns", [
        ("ORDERS", "A", "UNIQUE", "VALID", 1, "REF")])
    cat.on("from all_ind_expressions", [])
    assert _eng()._deep_indexes("app").status == "ok"


def test_a_materialized_view_missing_or_stale_is_found(cat):
    cat.on("from all_mviews", lambda side, sql: [
        ("SALES_MV", "FRESH", "VALID", "2026-01-01"),
        ("COUNTS_MV", "FRESH", "VALID", "2026-01-01")] if side == "src"
        else [("SALES_MV", "STALE", "VALID", "2026-01-01")])
    got = _eng()._deep_matviews("app")
    assert got.status == "diff"
    assert "1 missing on the target: counts_mv" in got.detail
    assert "sales_mv (stale)" in got.detail
    cat.answers.clear()
    cat.on("from all_mviews", [])
    assert "no materialized views" in _eng()._deep_matviews("app").detail


def test_a_policy_the_target_lacks_leaves_its_rows_open(cat):
    cat.on("from all_policies", lambda side, sql: [
        ("ORDERS", "BY_REGION", "YES")] if side == "src" else [])
    got = _eng()._deep_rls("app")
    assert got.status == "diff" and "visible to every user" in got.detail
    cat.answers.clear()
    cat.on("from all_policies", lambda side, sql: [
        ("ORDERS", "BY_REGION", "YES" if side == "src" else "NO")])
    assert "disabled on the target" in _eng()._deep_rls("app").detail


def test_grants_missing_on_the_target_are_found_and_not_carried_is_said(cat):
    cat.on("from dba_tab_privs", lambda side, sql: [
        ("APPUSER", "ORDERS", "SELECT", "NO"),
        ("APPUSER", "LINES", "UPDATE", "NO")] if side == "src" else
        [("APPUSER", "ORDERS", "SELECT", "NO")])
    got = _eng()._deep_grants("app")
    assert got.status == "diff"
    assert "APPUSER: UPDATE on lines" in got.detail
    assert "1 grants missing on the target" in got.detail
    # the dictionary is not readable: the schema's own view is
    cat.answers.clear()
    cat.on("from dba_tab_privs", RuntimeError("ORA-00942"))
    cat.on("from all_tab_privs", [("APPUSER", "ORDERS", "SELECT", "NO")])
    assert _eng()._deep_grants("app").status == "ok"
    # a hop that carries no accounts compares none
    assert _eng(options={"accounts": False})._deep_grants("app").status \
        == "skip"


def test_sequence_grants_are_read_apart_from_table_grants(cat):
    cat.on("type in ('SEQUENCE')", lambda side, sql: [
        ("APPUSER", "ORDERS_SEQ", "SELECT", "NO")] if side == "src" else [])
    got = _eng()._deep_seq_grants("app")
    assert got.status == "diff" and got.scope == "app seq-grants"


def test_a_routine_that_runs_with_the_callers_rights_instead_is_found(cat):
    cat.on("from all_procedures", lambda side, sql: [
        ("PAY", "PROCEDURE", "DEFINER" if side == "src" else "CURRENT_USER"),
        ("REPORT", "FUNCTION", "DEFINER")])
    got = _eng()._deep_ownership("app")
    assert got.status == "diff"
    assert "DEFINER -> CURRENT_USER on 1: procedure pay" in got.detail
    assert "caller's privileges" in got.detail
    cat.answers[-1] = ("from all_procedures", None, [("PAY", "PROCEDURE",
                                                      "DEFINER")])
    assert _eng()._deep_ownership("app").status == "ok"


def test_table_options_that_differ_are_found_and_a_read_only_target_is_said(
        cat):
    cat.on("read_only, iot_type from all_tables", lambda side, sql: [
        ("ORDERS", "YES", "DISABLED", "ENABLED", "NO", None),
        ("LINES", "YES", "DISABLED", "DISABLED", "NO", None)] if side == "src"
        else [("ORDERS", "NO", "DISABLED", "ENABLED", "YES", None),
              ("LINES", "YES", "DISABLED", "DISABLED", "NO", None)])
    got = _eng()._deep_options("app")
    assert got.status == "diff"
    assert got.category == "structure.collection-options"
    assert "orders: logging YES on the source, NO on the target" in got.detail
    assert "orders: read-only NO on the source, YES on the target" \
        in got.detail
    assert "lines" not in got.detail
    # a server before 12.2 keeps no read-only flag: the rest are compared
    cat.answers.clear()
    cat.on("null, iot_type from all_tables",
           [("ORDERS", "YES", "DISABLED", "ENABLED", None, None)])
    cat.on("read_only, iot_type from all_tables",
           RuntimeError("ORA-00904: \"READ_ONLY\": invalid identifier"))
    assert _eng()._deep_options("app").status == "ok"


# ---- all of them together ----------------------------------------------------

def test_a_check_that_cannot_read_is_an_error_and_the_others_still_run(
        cat, monkeypatch):
    eng = _eng()
    for name in ("_deep_partitions", "_mojibake"):
        monkeypatch.setattr(OracleEngine, name, lambda self, db, n=name:
                            Result("deep", f"{db} {n.strip('_')}", "ok"))
    cat.on("status = 'INVALID'", [])
    cat.on("validated = 'NOT VALIDATED'", [("CK on ORDERS (DISABLED, "
                                            "NOT VALIDATED)", "ORDERS")])
    cat.on("from all_tab_cols c", RuntimeError("ORA-00942: no such view"))
    cat.answers.append(("", None, []))      # every other statement: nothing
    got = {r.scope: r for r in eng.check_deep("app")}
    for word in ("columns", "narrowing", "generated"):
        assert got[f"app {word}"].status == "error", word
        assert "ORA-00942" in got[f"app {word}"].detail
    assert got["app keys"].status == "ok"
    loose = got["app target constraints"]
    assert loose.status == "diff" \
        and loose.category == "structure.unvalidated-constraints"


def test_every_word_a_finding_is_named_by_is_a_category():
    words = {"keys", "fk", "deferrable", "columns", "narrowing",
             "generated", "collation", "charset", "nullempty", "float",
             "timeshift", "render", "boundary", "triggers", "indexes",
             "matviews", "rls", "grants", "seq-grants", "ownership"}
    assert words <= set(CATEGORIES)


# ---- what the matrix lets this module prove ------------------------------------

def test_a_constraint_a_load_left_not_enforced_is_a_structure_finding(cat):
    cat.on("status = 'INVALID'", [])
    cat.on("validated = 'NOT VALIDATED'", lambda side, sql: [
        ("CK_A on ORDERS (ENABLED, NOT VALIDATED)", "ORDERS"),
        ("CK_B on AUDIT_LOG (DISABLED, NOT VALIDATED)", "AUDIT_LOG")])
    eng = _eng(exclude=["audit_log"])
    for name in ("_deep_partitions", "_mojibake"):
        setattr(eng, name, lambda db, n=name: Result(
            "deep", f"{db} {n.strip('_')}", "ok"))
    eng._deep_more = lambda db: []
    got = {r.scope: r for r in eng.check_deep("app")}
    loose = got["app target constraints"]
    assert loose.status == "diff" and "CK_A" in loose.detail \
        and "CK_B" not in loose.detail
    assert loose.category == "structure.unvalidated-constraints"


def test_a_table_the_target_lacks_is_made_from_the_sources_columns(
        monkeypatch, cat):
    made = []
    monkeypatch.setattr(OracleEngine, "neutral_columns",
                        lambda self, side, db, t: [
                            ("id", "NUMBER(10,0)"),
                            ("note", "VARCHAR2(40 CHAR)")])
    monkeypatch.setattr(OracleEngine, "neutral_key",
                        lambda self, side, db, t: ["id"])
    monkeypatch.setattr(OracleEngine, "canonical_type",
                        lambda self, side, db, declared: "decimal"
                        if declared.startswith("NUMBER") else "text")
    monkeypatch.setattr(OracleEngine, "neutral_create_code",
                        lambda self, side, db, ddl: made.append(ddl))
    monkeypatch.setattr(OracleEngine, "neutral_column_rules",
                        lambda self, side, db, t: (_ for _ in ()).throw(
                            RuntimeError("rules not readable")))
    cat.on("from all_tables", lambda side, sql: [("ORDERS",)]
           if side == "src" else [])
    said = []
    _eng().create_missing("app", said.append)
    assert len(made) == 1 and made[0].lower().startswith("create table")
    assert '"ORDERS"' in made[0] and '"ID"' in made[0]
    assert any("not on the target, created it" in m for m in said)
    # a table already there is left as it is
    made.clear()
    cat.answers.clear()
    cat.on("from all_tables", [("ORDERS",)])
    _eng().create_missing("app", said.append)
    assert made == []

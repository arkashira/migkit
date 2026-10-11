"""What Oracle says of itself before a move and of its schema (P0-PARITY),
without a server: the version and product each side reports, the rules of
its columns, its indexes and how one is made, the room it has, the writers
beside migkit, and the work the move leaves to hands.

No cell of the depth matrix is proved here: each statement has yet to run
against an Oracle server (`capabilities.UNPROVED`). What is held is what is
migkit's own - how the answers are read, what is left out when the account
may not ask, and that nothing unreadable is reported as none.
"""
import tempfile
from pathlib import Path

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.oracle import OracleEngine
from tests.oracle_catalogue import Catalogue, cat  # noqa: F401

EP = Endpoint(host="10.0.0.5", port=1521, user="app", password="CHANGE_ME")


def _eng(**kw):
    hop = Hop(name="t", engine="oracle", source=EP, target=EP,
              databases=["app"], **kw)
    where = Path(tempfile.mkdtemp())
    hop.report_dir = lambda db=None: where
    return OracleEngine(hop)


def _product(version, product="Oracle Database 23ai Free"):
    return lambda side, sql: [(product, version(side))]


# ---- versions and brands -----------------------------------------------------

def test_each_sides_release_and_product_are_read(cat):
    cat.on("from product_component_version", _product(
        lambda side: "23.26.0.0.0" if side == "src" else "19.22.0.0.0"))
    eng = _eng()
    assert eng._server_versions() == ("23.26.0.0.0", "19.22.0.0.0")
    src, dst = eng._brands()
    assert (src.name, dst.name) == ("oracle", "oracle")
    assert src.version == "23.26.0.0.0"


def test_a_side_that_will_not_say_is_unknown_and_not_the_others(cat):
    cat.on("from product_component_version", lambda side, sql: (
        [("Oracle Database 19c", "19.22.0.0.0")] if side == "src"
        else (_ for _ in ()).throw(RuntimeError("ORA-01017"))))
    assert _eng()._server_versions() == ("19.22.0.0.0", None)
    assert _eng()._brand_probes()[1] == {}


def test_a_release_before_18c_is_read_from_its_four_part_number(cat):
    def answer(side, sql):
        if "version_full" in sql:
            raise RuntimeError("ORA-00904: \"VERSION_FULL\"")
        return [("Oracle Database 12c Enterprise Edition", "12.2.0.1.0")]
    cat.on("from product_component_version", answer)
    assert _eng()._server_versions() == ("12.2.0.1.0", "12.2.0.1.0")


def test_assess_says_both_versions_and_names_the_brand_pairing(cat, monkeypatch):
    cat.on("from product_component_version", _product(
        lambda side: "23.26.0.0.0" if side == "src" else "19.22.0.0.0"))
    monkeypatch.setattr(OracleEngine, "_assess_extra", lambda self: [])
    rows = _eng().assess()
    version = [r for r in rows if r["item"] == "server version match"]
    assert version and version[0]["level"] == "warn" \
        and "23.26.0.0.0" in version[0]["detail"]
    brands = [r for r in rows if r["scope"] == "brand"]
    assert {r["item"] for r in brands} == {"source brand", "target brand"}


# ---- the rules of columns, indexes -------------------------------------------

def test_a_columns_null_default_and_identity_are_read_and_a_computed_one_is_not(
        cat):
    cat.on("from all_tab_cols", [
        ("ID", "N", "\"APP\".\"ISEQ$$_1\".nextval", "NO", "YES"),
        ("STATUS", "N", "'new'  ", "NO", "NO"),
        ("NOTE", "Y", "NULL", "NO", "NO"),
        ("TOTAL", "Y", "price * qty", "YES", "NO")])
    got = _eng().neutral_column_rules("src", "app", "orders")
    assert got == {"id": {"null": False, "default": None, "identity": True},
                   "status": {"null": False, "default": "'new'",
                              "identity": False},
                   "note": {"null": True, "default": None,
                            "identity": False}}


def test_a_tables_indexes_are_listed_but_its_key_and_its_large_values(cat):
    cat.on("from all_indexes i", [
        ("IX_CUST", "NONUNIQUE", "NORMAL", "CUSTOMER_ID", "ASC"),
        ("IX_CUST", "NONUNIQUE", "NORMAL", "CREATED", "ASC"),
        ("UQ_REF", "UNIQUE", "NORMAL", "REF", "ASC"),
        ("IX_BIG", "NONUNIQUE", "BITMAP", "FLAG", "ASC"),
        ("IX_LOWER", "NONUNIQUE", "FUNCTION-BASED NORMAL", "SYS_NC0001$",
         "ASC"),
        ("IX_DESC", "NONUNIQUE", "NORMAL", "AMOUNT", "DESC")])
    got = _eng().neutral_indexes("src", "app", "orders")
    assert got == [("ix_big", False, ["flag"], False),
                   ("ix_cust", False, ["customer_id", "created"], True),
                   ("ix_desc", False, ["amount"], False),
                   ("ix_lower", False, ["sys_nc0001$"], False),
                   ("uq_ref", True, ["ref"], True)]
    sql = cat.sent[0][1]
    assert "i.index_type not in ('LOB', 'IOT - TOP')" in sql
    assert "k.constraint_type = 'P'" in sql


def test_an_index_is_made_in_the_targets_schema_under_its_name():
    eng = _eng()
    assert eng.neutral_create_index_sql(
        "dst", "app", "orders", "ix_cust", False, ["customer_id", "created"]
    ) == 'create index "APP"."IX_CUST" on "APP"."ORDERS" ("CUSTOMER_ID",' \
         ' "CREATED")'
    assert eng.neutral_create_index_sql(
        "dst", "app", "orders", "uq_ref", True, ["ref"]).startswith(
            'create unique index "APP"."UQ_REF"')
    with pytest.raises(SystemExit):
        eng.execute_ddl("src", "app", "create index x on y (z)")


# ---- the room a side has -----------------------------------------------------

def test_capacity_leaves_out_what_the_account_may_not_read(cat):
    cat.on("NUM_CPUS", [(8,)])
    cat.on("name = 'sessions'", [(500,)])
    cat.on("count(*) from v$session where type = 'USER' and status",
           [(7,)])
    cat.on("count(*) from v$session where type = 'USER'", [(120,)])
    cat.on("database_role", RuntimeError("ORA-00942"))
    assert _eng().capacity("dst", "app") == {
        "cpus": 8, "free_connections": 380, "running": 7}
    cat.answers[-1] = ("database_role", None, [("PHYSICAL STANDBY",)])
    assert _eng().capacity("dst", "app")["replica"] is True


def test_the_room_is_what_the_files_can_hold_held_to_the_quota(cat):
    cat.on("from dba_users u", [(5_000_000_000,)])
    cat.on("from dba_ts_quotas", [(2_000_000_000,)])
    assert _eng().free_bytes("dst", "app") == 2_000_000_000
    cat.answers[-1] = ("from dba_ts_quotas", None, [(None,)])
    assert _eng().free_bytes("dst", "app") == 5_000_000_000
    # nothing readable: no number, never a guess
    cat.answers.clear()
    cat.on("from dba_users u", RuntimeError("ORA-00942"))
    cat.on("from dba_ts_quotas", RuntimeError("ORA-00942"))
    assert _eng().free_bytes("dst", "app") is None


# ---- other writers -----------------------------------------------------------

def test_other_writers_on_the_target_are_named_and_cannot_be_paused(
        cat, monkeypatch):
    monkeypatch.setattr("migkit.tailctl.alive", lambda *a, **k: True)
    cat.on("from dba_apply", [("OGG_APPLY",)])
    cat.on("from dba_scheduler_jobs", [("NIGHTLY_LOAD",)])
    cat.on("from v$session", [("sqlplus@host",)])
    got = _eng().stream_writers("app")
    assert ("migkit's change tail", True) in got
    assert ("an apply process, OGG_APPLY", False) in got
    assert ("a scheduler job, NIGHTLY_LOAD", False) in got
    assert ("a session of the schema's owner, sqlplus@host", False) in got
    # a question the account may not ask is not an answer of none
    cat.answers.clear()
    cat.on("from dba_apply", RuntimeError("ORA-00942"))
    cat.on("from dba_scheduler_jobs", [])
    cat.on("from v$session", RuntimeError("ORA-00942"))
    assert _eng().stream_writers("app") == [("migkit's change tail", True)]


# ---- what the move leaves to hands ---------------------------------------------

def _leavings(cat, external):
    cat.on("not exists (select 1 from all_constraints", [("EVENTS",),
                                                       ("AUDIT_LOG",)])
    cat.on("from all_db_links", [("REMOTE_ERP",)])
    cat.on("from all_synonyms", [])
    cat.on("from all_external_tables", external)
    cat.on("temporary = 'Y'", [("SESSION_TMP",)])
    cat.on("from all_queues", [])
    cat.on("from all_scheduler_jobs", [("NIGHTLY",)])
    inv = _eng(exclude=["audit_log"])._handwork()
    return {r["item"]: r for r in inv.rows()}


def test_the_work_a_move_leaves_is_counted_and_named(cat):
    rows = _leavings(cat, [])
    keys = rows["tables with no primary key or unique index"]
    assert "events" in keys["detail"] and "audit_log" not in keys["detail"]
    gone = rows["objects whose contents no data mover copies"]
    assert "database links: 1 (remote_erp)" in gone["detail"]
    assert "temporary tables (their contents): 1 (session_tmp)" \
        in gone["detail"]
    assert "scheduler jobs to switch on at cutover: 1 (nightly)" \
        in rows["settings that need a decision rather than a copy"]["detail"]


def test_a_probe_the_account_cannot_run_is_unknown_and_never_none(cat):
    rows = _leavings(cat, RuntimeError("ORA-00942: no such view"))
    gone = rows["objects whose contents no data mover copies"]
    assert "UNKNOWN" in gone["detail"] and "external tables" in gone["detail"]
    assert "Not zero" in gone["detail"]


def test_assess_carries_the_inventory_and_the_pre_flight(cat, monkeypatch):
    cat.on("from product_component_version", _product(
        lambda side: "23.26.0.0.0"))
    cat.on("not exists (select 1 from all_constraints", [("EVENTS",)])
    for word in ("from all_db_links", "from all_synonyms",
                 "from all_external_tables", "temporary = 'Y'",
                 "from all_queues", "from all_scheduler_jobs"):
        cat.on(word, [])
    from migkit.engines import oracle_logminer
    monkeypatch.setattr(oracle_logminer, "assess", lambda *a: [])
    monkeypatch.setattr(OracleEngine, "_mojibake",
                        lambda self, db: __import__(
                            "migkit.engines.base", fromlist=["Result"]
                        ).Result("deep", f"{db} mojibake", "ok", "clean"))
    monkeypatch.setattr(OracleEngine, "_mapping_items", lambda self: [])
    rows = _eng().assess()
    before = [r for r in rows if r["scope"] == "before the move"]
    assert [r["item"] for r in before] == ["app keys", "app mojibake"]
    assert before[0]["level"] == "fail" and "events" in before[0]["detail"]
    assert any(r["scope"] == "manual work" for r in rows)


# ---- rows sized where they lie ---------------------------------------------

def test_a_rows_widest_value_is_measured_by_the_server_not_sampled(cat):
    eng = _eng()
    eng.neutral_columns = lambda side, db, table: [
        ("id", "NUMBER(10,0)"), ("body", "CLOB"), ("blob", "BLOB"),
        ("note", "VARCHAR2(40)")]
    eng.table_facts = lambda side, db: {"orders": {"rows": 1000,
                                                   "bytes": 5_000_000}}
    eng.neutral_tables = lambda side, db: ["orders"]
    cat.on("select max(s), avg(s), count(*) from",
           [(40_000_000, 120_000.5, 1000)])
    cat.on("fetch first", [(100,), (900,), (40_000_000,)])
    got = eng.row_sizes("src", "app")
    assert got["orders"]["largest"] == 40_000_000
    assert got["orders"]["rows"] == 1000
    sent = " ".join(sql for _, sql in cat.sent)
    # a text large object is counted at the most a character can take, a
    # binary one by its length, an ordinary column by its stored bytes, the
    # rest at eight
    assert 'coalesce(dbms_lob.getlength("BODY") * 4, 0)' in sent
    assert 'coalesce(dbms_lob.getlength("BLOB"), 0)' in sent
    assert 'coalesce(lengthb("NOTE"), 0)' in sent


def test_a_table_with_no_column_that_can_be_large_is_sized_from_its_catalogue(
        cat):
    eng = _eng()
    eng.neutral_columns = lambda side, db, table: [
        ("id", "NUMBER(10,0)"), ("at", "DATE")]
    eng.table_facts = lambda side, db: {"orders": {"rows": 10,
                                                   "bytes": 1_000}}
    got = eng.row_sizes("src", "app", ["orders"])
    assert got["orders"]["largest"] == 8 * 2 + 1
    assert cat.sent == []

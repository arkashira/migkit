"""Teradata held to the depth PostgreSQL and MySQL have (backlog
P0-PARITY): the scope read one way in every path, the two systems' tables
held to each other by the deep check, what a repair acts on, what a load
sets aside, what the assessment reads, and the source held at the cutover.

No Teradata runs in a container, so the driver is a stand-in
(`tests/teradata_systems.py`) that answers each statement from what a test
puts in the two systems. What is proved here is what migkit does with the
answers - which table it reads, what it makes of a difference, what it
leaves undone - over statements the engine already sent to a system
(`DBC.ColumnsV`, `DBC.IndicesV`, the tables themselves). The cells whose
statements are new, and have never met a real system, are not named in
`PROVES` but in `capabilities.UNPROVED`; `test_what_the_deep_check_says_of_*`
below still holds the logic they stand on.
"""
import json

import pytest

from migkit import freeze
from migkit.engines.base import LEFT_OUT
from tests import teradata_systems as td
from tests.teradata_systems import SRC, DST, Systems, column, system

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("scope.listings", "teradata"), ("scope.copy", "teradata"),
    ("scope.row-filter", "teradata"), ("scope.names", "teradata"),
    ("verify.table", "teradata"),
    ("verify.structure.columns", "teradata"),
    ("verify.structure.keys", "teradata"),
    ("verify.structure.indexes", "teradata"),
    ("verify.value.charset", "teradata"),
    ("verify.value.collation", "teradata"),
    ("verify.value.generated", "teradata"),
    ("verify.value.narrowing", "teradata"),
    ("verify.value.timezone", "teradata"),
    ("schema.indexes", "teradata"), ("schema.defaults", "teradata"),
    ("move.create-missing", "teradata"),
    ("scope.repair", "teradata"), ("verify.repair", "teradata"),
    ("verify.repair-undo", "teradata"), ("assess.versions", "teradata"),
    ("cutover.freeze", "teradata"),
]

ID = column("id", "I", 4, nullable="N")
CODE = column("code", "CV", 20, chartype=1, upper="N")
NAME = column("name", "CV", 200, chartype=2, upper="C")
AMOUNT = column("amount", "D", 8, 12, 2)
BORN = column("born", "TS", 26, None, 6)


def orders(**changes):
    cols = {"id": ID, "code": CODE, "name": NAME, "amount": AMOUNT,
            "born": BORN}
    cols.update(changes)
    return [c for c in cols.values() if c is not None]


def pair(monkeypatch, tmp_path, src=None, dst=None, hook=None, **given):
    """Two systems each holding `shop.orders` unless a test says otherwise,
    and the engine over them."""
    base = dict(tables=["orders"], columns={"orders": orders()},
                keys={"orders": ["id"]})
    systems = Systems(system(**{**base, **(src or {})}),
                      system(**{**base, **(dst or {})}))
    systems.hook = hook
    systems.install(monkeypatch)
    return systems, td.engine(tmp_path, **given)


def results(eng, db="shop"):
    # the columns of a table are asked once; a test changes them between
    eng.__dict__.pop("_catalogs", None)
    return {r.scope.split(" ", 1)[1]: r for r in eng.check_deep(db)}


# ---- the scope, asked one way ------------------------------------------------

def test_a_table_the_hop_leaves_out_is_neither_listed_nor_copied(
        monkeypatch, tmp_path):
    systems, eng = pair(
        monkeypatch, tmp_path,
        src={"tables": ["orders", "audit_log"],
             "columns": {"orders": orders(), "audit_log": orders()},
             "keys": {"orders": ["id"], "audit_log": ["id"]}},
        dst={"tables": ["orders", "audit_log"],
             "columns": {"orders": orders(), "audit_log": orders()},
             "keys": {"orders": ["id"], "audit_log": ["id"]}},
        exclude=["audit_log"])
    assert eng.neutral_tables("src", "shop") == ["orders"]
    assert eng.neutral_tables("dst", "shop") == ["orders"]
    # what the copy lists, through the pair, is the hop's as well
    assert eng.list_move_tables("shop") == [("", "orders")]
    # and a table migkit keeps in a database is none of the application's
    systems.hosts[SRC]["tables"].append("migkit_origin")
    assert "migkit_origin" not in eng.neutral_tables("src", "shop")


def test_the_hops_row_filter_is_read_by_the_copy_and_the_check_alike(
        monkeypatch, tmp_path):
    systems, eng = pair(monkeypatch, tmp_path,
                        mapping={"where": {"orders": "id > 5"}})
    pairing = eng._as_pair()
    src_where, dst_where = pairing._row_scope("shop", "orders")
    assert src_where and dst_where and "id" in src_where
    cols = [("id", "integer")]
    list(eng.neutral_batches("src", "shop", "orders", cols,
                             where=src_where))
    reads = [s for s in systems.sql(SRC) if s.endswith("where (id > 5)")
             or "(id > 5)" in s]
    assert reads, systems.sql(SRC)


def test_a_renamed_table_is_found_under_its_new_name_by_the_check(
        monkeypatch, tmp_path):
    systems, eng = pair(
        monkeypatch, tmp_path,
        dst={"tables": ["orders_new"],
             "columns": {"orders_new": orders()},
             "keys": {"orders_new": ["id"]}},
        mapping={"tables": {"orders": "orders_new"}})
    pairing = eng._as_pair()
    pairs, src_only, dst_only, _ = pairing.match_tables(
        eng.neutral_tables("src", "shop"), eng.neutral_tables("dst", "shop"),
        pairing._renamer("shop"))
    assert pairs == [("orders", "orders_new")] and not src_only


def test_a_finding_names_its_table(monkeypatch, tmp_path):
    from migkit.engines.hetero import HeteroEngine

    def hook(host, low, args):
        if "hashbucket(hashrow(" in low:
            return [(3, 11, 12, 13, 14 if host == SRC else 15)]
        return None
    systems, eng = pair(monkeypatch, tmp_path, hook=hook)
    monkeypatch.setattr(HeteroEngine, "_drill", lambda self, *a, **k: "")
    (found,) = eng.check_data("shop")
    assert found.status == "diff" and found.scope == "shop.orders"
    assert found.table == "orders"


# ---- the schema ------------------------------------------------------------------

def test_secondary_indexes_are_read_and_made(monkeypatch, tmp_path):
    systems, eng = pair(
        monkeypatch, tmp_path,
        src={"indexes": {"orders": [(2, "by_code", "N", "code"),
                                    (3, "by_name", "Y", "name"),
                                    (3, "by_name", "Y", "code")]}})
    got = eng.neutral_indexes("src", "shop", "orders")
    assert got == [("by_code", False, ["code"], True),
                   ("by_name", True, ["name", "code"], True)]
    assert eng.neutral_create_index_sql(
        "dst", "shop", "orders", "by_name", True, ["name", "code"]) == \
        'create unique index "by_name" ("name", "code") on "shop"."orders"'


def test_a_columns_nullability_default_and_identity_are_read(
        monkeypatch, tmp_path):
    systems, eng = pair(
        monkeypatch, tmp_path,
        src={"columns": {"orders": orders(
            id=column("id", "I", 4, nullable="N", identity="GD"),
            code=column("code", "CV", 20, chartype=1, default="'x'"))}})
    rules = eng.neutral_column_rules("src", "shop", "orders")
    assert rules["id"] == {"null": False, "default": None, "identity": True}
    assert rules["code"]["default"] == "'x'" and rules["code"]["null"]
    # the pair carries them into the table it makes
    made = eng.neutral_create_sql(
        "dst", "shop", "t", [("id", "integer", (), {"null": False}),
                             ("code", "text", (20,), {"default": "'x'",
                                                      "null": False})],
        key=("id",))
    assert '"id" BIGINT not null' in made
    assert "default 'x'" in made.lower() and made.lower().count(
        "not null") == 2


def test_tables_the_target_lacks_are_made_before_any_is_filled(
        monkeypatch, tmp_path):
    systems, eng = pair(monkeypatch, tmp_path, dst={"tables": [],
                                                    "columns": {}})
    said = []
    eng.create_missing("shop", said.append)
    made = [s for s in systems.sql(DST) if s.startswith("create multiset")]
    assert made and '"shop"."orders"' in made[0], systems.sql(DST)
    assert 'primary key ("id")' in made[0]


# ---- what the deep check finds -------------------------------------------------

def test_a_column_that_drifted_is_found_and_written_down(
        monkeypatch, tmp_path):
    systems, eng = pair(
        monkeypatch, tmp_path,
        dst={"columns": {"orders": orders(
            amount=column("amount", "D", 8, 10, 2),
            code=None)}})
    got = results(eng)
    r = got["columns"]
    assert r.status == "diff" and "orders.code: not on the target" in r.detail
    assert "orders.amount: type DECIMAL(12,2) -> DECIMAL(10,2)" in r.detail
    assert (tmp_path / "reports" / "shop" / "deep-columns.diff").exists()
    # narrower, and said as the value that would not fit
    assert got["narrowing"].status == "diff"
    assert "DECIMAL(12,2) -> DECIMAL(10,2)" in got["narrowing"].detail
    systems.hosts[DST]["columns"]["orders"] = orders()
    got = results(eng)
    assert got["columns"].status == "ok" and got["narrowing"].status == "ok"


def test_a_table_with_no_key_is_named_with_what_it_costs(
        monkeypatch, tmp_path):
    systems, eng = pair(monkeypatch, tmp_path, src={"keys": {}})
    r = results(eng)["keys"]
    assert r.status == "warn" and "orders" in r.detail
    assert "repaired row by row" in r.detail
    systems.hosts[SRC]["keys"] = {"orders": ["id"]}
    assert results(eng)["keys"].status == "ok"


def test_a_secondary_index_the_target_lacks_is_found(monkeypatch, tmp_path):
    src = {"indexes": {"orders": [(2, "by_code", "N", "code")]}}
    systems, eng = pair(monkeypatch, tmp_path, src=src)
    r = results(eng)["indexes"]
    assert r.status == "diff" and "orders.by_code (code)" in r.detail
    systems.hosts[DST]["indexes"] = src["indexes"]
    assert results(eng)["indexes"].status == "ok"


def test_a_narrower_character_set_and_another_case_rule_are_found(
        monkeypatch, tmp_path):
    systems, eng = pair(
        monkeypatch, tmp_path,
        dst={"columns": {"orders": orders(
            name=column("name", "CV", 200, chartype=1, upper="N"))}})
    got = results(eng)
    assert got["charset"].status == "diff"
    assert "orders.name: UNICODE -> LATIN" in got["charset"].detail
    assert got["collation"].status == "diff"
    assert "CASESPECIFIC -> NOT CASESPECIFIC" in got["collation"].detail
    # widening into UNICODE loses nothing
    systems.hosts[SRC]["columns"]["orders"] = orders(
        name=column("name", "CV", 200, chartype=1, upper="C"))
    systems.hosts[DST]["columns"]["orders"] = orders(
        name=column("name", "CV", 200, chartype=2, upper="C"))
    got = results(eng)
    assert got["charset"].status == "ok" and got["collation"].status == "ok"


def test_an_identity_column_the_target_always_generates_is_found(
        monkeypatch, tmp_path):
    systems, eng = pair(
        monkeypatch, tmp_path,
        src={"columns": {"orders": orders(
            id=column("id", "I", 4, nullable="N", identity="GD"))}},
        dst={"columns": {"orders": orders(
            id=column("id", "I", 4, nullable="N", identity="GA"))}})
    r = results(eng)["generated"]
    assert r.status == "diff" and "orders.id" in r.detail
    assert "refuses the value the source holds" in r.detail
    systems.hosts[DST]["columns"]["orders"] = orders()
    assert results(eng)["generated"].status == "warn"
    systems.hosts[DST]["columns"]["orders"] = \
        systems.hosts[SRC]["columns"]["orders"]
    assert results(eng)["generated"].status == "ok"


def test_a_timestamp_column_shifted_as_a_whole_is_found(
        monkeypatch, tmp_path):
    import datetime
    rows = {SRC: [(i, datetime.datetime(2024, 1, 1, 12, 0, i))
                  for i in range(1, 6)]}
    rows[DST] = [(i, t + datetime.timedelta(hours=7)) for i, t in rows[SRC]]

    def hook(host, low, args):
        if low.startswith("select top 200") and '"born"' in low:
            return rows[host]
        if low.startswith("select") and '"born"' in low \
                and 'where ("id" = ?' in low:
            return rows[host]
        return None
    systems, eng = pair(monkeypatch, tmp_path, hook=hook)
    r = results(eng)["timeshift"]
    assert r.status == "diff" and "orders.born: every row shifted" in r.detail
    rows[DST] = list(rows[SRC])
    assert results(eng)["timeshift"].status == "ok"


# ---- what a repair acts on --------------------------------------------------------

def test_a_repair_leaves_alone_a_table_the_hop_no_longer_takes(
        monkeypatch, tmp_path):
    systems, eng = pair(
        monkeypatch, tmp_path,
        src={"tables": ["orders", "audit_log"],
             "columns": {"orders": orders(), "audit_log": orders()},
             "keys": {"orders": ["id"], "audit_log": ["id"]}},
        dst={"tables": ["orders", "audit_log"],
             "columns": {"orders": orders(), "audit_log": orders()},
             "keys": {"orders": ["id"], "audit_log": ["id"]}},
        exclude=["audit_log"])
    reports = eng.hop.report_dir("shop")
    # the last check ran before the hop left audit_log out
    (reports / "data-audit_log.missing").write_text(json.dumps([7]) + "\n")
    plan = eng.repair_plan("shop", "rows")
    left = [a for a in plan if a.kind == LEFT_OUT]
    assert left and "audit_log" in left[0].note
    assert not [a for a in plan if a.kind == "rows"
                and "audit_log" in a.scope]
    # an action of another kind is the pair's to carry out
    assert eng.repair_plan("shop", "sequences") == []


# ---- the source held at the cutover ------------------------------------------------

def test_a_source_is_held_by_its_rights_and_given_back_as_it_was(
        monkeypatch, tmp_path):
    rights = [("APP", "orders", "I", "Y"), ("APP", "All", "U", "N"),
              ("MIGKIT", "All", "I", "Y"), ("DBC", "All", "D", "Y")]
    log = []
    sessions = {"open": 2}

    def hook(host, low, args):
        if "from DBC.AllRightsV" in low and "GrantAuthority" in low:
            return [(u, t, r, g) for u, t, r, g in rights]
        if low.startswith("select user"):
            return [("MIGKIT",)]
        if low.startswith("revoke ") or low.startswith("grant "):
            log.append(low)
            who = low.split('"')[-2]
            if low.startswith("revoke "):
                rights[:] = [x for x in rights if not (
                    x[0] == who and ("on " + ('"shop"' if x[1] == "All"
                                              else f'"shop"."{x[1]}"'))
                    in low)]
            return 0
        if "SYSLIB.AbortSessions" in low:
            sessions["open"] = 0
            log.append("aborted")
            return [(2,)]
        if "from DBC.FunctionsV" in low:
            return [(1,)]
        if "from DBC.SessionInfoV" in low and "count(*)" in low:
            return [(sessions["open"],)]
        if "from DBC.SessionInfoV" in low:
            return [(1,)]
        if "extract(hour" in low:
            return [("2026-10-11", 120000.0)]
        if "from DBC.DatabasesV" in low:
            return [("DBC",)]
        if "from DBC.AllRoleRightsV" in low:
            return []
        return None
    systems, eng = pair(monkeypatch, tmp_path, hook=hook)
    h = freeze.TeradataHold(td.hop(tmp_path), eng, "src")
    said = []
    h.hold(["shop"], "td_grants", said.append)
    assert [r[0] for r in rights] == ["MIGKIT", "DBC"], rights
    assert "aborted" in log
    record = freeze.state(h.hop, "shop", "src")
    assert record["at"] and sorted(record["roles"]) == ["APP"]
    assert any("sessions ended" in s or "ended" in s for s in said), said
    # read back: nothing of APP's writes, and no session from before
    got = h.held("shop", "td_grants")
    assert all(ok for ok, _ in got), got
    # a session the freeze did not end is read back as not held
    sessions["open"] = 1
    assert not all(ok for ok, _ in h.held("shop", "td_grants"))
    sessions["open"] = 0
    h.release("shop", record, said.append)
    # each right given back as it was, with the option it had
    assert 'grant insert on "shop"."orders" to "APP" with grant option' in log
    assert 'grant update on "shop" to "APP"' in log
    assert not [x for x in log if x.startswith("grant") and "MIGKIT" in x]


# ---- the checks over statements no system has answered yet ------------------
#
# Each of these reads a DBC view, or runs an aggregate, that migkit has not
# sent to a real Teradata; the logic is held here, the cells wait in
# `capabilities.UNPROVED` for a system to answer.

def test_what_the_deep_check_says_of_null_and_empty_and_floats(
        monkeypatch, tmp_path):
    seen = {SRC: (2, 3), DST: (5, 0)}
    floats = {SRC: (10, 1.5, 9.0), DST: (10, 1.5000000001, 9.0)}

    def hook(host, low, args):
        if "is null then 1" in low:
            return [seen[host]]
        if "coalesce(sum(cast(" in low:
            return [floats[host]]
        return None
    cols = orders(ratio=column("ratio", "F", 8))
    systems, eng = pair(monkeypatch, tmp_path, hook=hook,
                        src={"columns": {"orders": cols}},
                        dst={"columns": {"orders": cols}})
    got = results(eng)
    assert got["nullempty"].status == "diff"
    assert "source NULL|empty=2|3, target 5|0" in got["nullempty"].detail
    assert got["float"].status == "ok"
    floats[DST] = (10, 2.5, 9.0)
    seen[DST] = (2, 3)
    got = results(eng)
    assert got["nullempty"].status == "ok"
    assert got["float"].status == "diff" and "orders.ratio: sum" in \
        got["float"].detail


def test_objects_beside_the_tables_are_held_to_the_hops_objects(
        monkeypatch, tmp_path):
    objects = [("v_orders", "V"), ("m_clean", "M"), ("trg_audit", "G"),
               ("ji_by_code", "I"), ("migkit_origin", "V")]
    trig = [("shop", "trg_audit", "orders", "Y")]
    systems, eng = pair(monkeypatch, tmp_path,
                        src={"objects": objects, "triggers": trig},
                        dst={"objects": [("v_orders", "V")],
                             "triggers": []})
    r = results(eng)["objects"]
    assert r.status == "diff"
    assert "macro m_clean" in r.detail and "trigger trg_audit" in r.detail
    assert "join index ji_by_code" in r.detail
    assert "v_orders" not in r.detail and "migkit_origin" not in r.detail
    # a kind the hop does not carry is out of its scope
    systems, eng = pair(monkeypatch, tmp_path,
                        src={"objects": objects, "triggers": trig},
                        dst={"objects": [("v_orders", "V")]},
                        objects={"procedures": False, "triggers": False})
    r = results(eng)["objects"]
    assert "macro" not in r.detail and "trigger" not in r.detail
    assert "join index ji_by_code" in r.detail


def test_a_trigger_left_disabled_and_a_reference_left_out_are_found(
        monkeypatch, tmp_path):
    ref = (1, "fk_cust", "orders", "id", "shop", "customers", "id", "N")
    systems, eng = pair(
        monkeypatch, tmp_path,
        src={"refs": [ref]},
        dst={"refs": [], "triggers": [("shop", "t1", "orders", "N"),
                                       ("shop", "t2", "orders", "Y")]})
    got = results(eng)
    assert got["triggers"].status == "diff" and "t1 on orders" in \
        got["triggers"].detail
    assert got["fk"].status == "diff" and "orders -> customers" in \
        got["fk"].detail
    systems.hosts[DST]["refs"] = [ref[:7] + ("Y",)]
    systems.hosts[DST]["triggers"] = []
    got = results(eng)
    assert got["fk"].status == "diff" and "flagged inconsistent" in \
        got["fk"].detail
    systems.hosts[DST]["refs"] = [ref]
    got = results(eng)
    assert got["fk"].status == "ok" and got["triggers"].status == "ok"
    assert eng.foreign_keys("src", "shop") == [
        ("orders", ["id"], "customers", ["id"])]


def test_partitions_and_the_primary_index_are_held_to_the_source(
        monkeypatch, tmp_path):
    expr = "RANGE_N(born BETWEEN DATE '2020-01-01' AND DATE '2030-12-31'"\
           " EACH INTERVAL '1' MONTH)"
    systems, eng = pair(
        monkeypatch, tmp_path,
        src={"ppi": {"orders": expr}, "parts": {"orders": [(1, 10), (2, 5)]},
             "pi": {"orders": ["customer"]}},
        dst={"parts": {}, "pi": {"orders": ["id"]}})
    got = results(eng)
    assert got["partitions"].status == "diff"
    assert "orders: partitioned on the source" in got["partitions"].detail
    assert got["sharding"].status == "warn"
    assert "(customer) -> (id)" in got["sharding"].detail
    systems.hosts[DST]["ppi"] = {"orders": expr}
    systems.hosts[DST]["parts"] = {"orders": [(1, 10), (2, 4)]}
    systems.hosts[DST]["pi"] = {"orders": ["customer"]}
    got = results(eng)
    assert got["partitions"].status == "diff"
    systems.hosts[DST]["parts"] = {"orders": [(1, 10), (2, 5)]}
    got = results(eng)
    assert got["partitions"].status == "ok" and got["sharding"].status == "ok"


def test_rights_owners_and_the_boundary_are_held_to_the_source(
        monkeypatch, tmp_path):
    maxes = {SRC: 100, DST: 250}

    def hook(host, low, args):
        if low.startswith("select coalesce(max("):
            return [(maxes[host],)]
        return None
    systems, eng = pair(
        monkeypatch, tmp_path, hook=hook,
        src={"rights": [("APP", "orders", "R"), ("APP", "All", "I"),
                        ("DBC", "All", "R")], "owner": "OWNER_A",
             "creators": {"v1": ("APP", "V")}},
        dst={"rights": [("APP", "orders", "R")], "owner": "OWNER_B",
             "creators": {"v1": ("MIGKIT", "V")}})
    got = results(eng)
    assert got["grants"].status == "diff"
    assert "app I on all" in got["grants"].detail
    assert "dbc" not in got["grants"].detail
    assert got["ownership"].status == "diff"
    assert "OWNER_A on the source and OWNER_B" in got["ownership"].detail
    assert "v1: made by APP -> MIGKIT" in got["ownership"].detail
    assert got["boundary"].status == "diff"
    assert "orders source max=100 target max=250" in got["boundary"].detail
    maxes[DST] = 90
    assert results(eng)["boundary"].status == "ok"


def test_a_check_that_cannot_run_says_so_and_never_passes(
        monkeypatch, tmp_path):
    systems, eng = pair(monkeypatch, tmp_path)
    systems.fail["DBC.AllRightsV"] = td.DriverError("3523 no access")
    got = results(eng)
    assert got["grants"].status == "error"
    assert "could not run" in got["grants"].detail


# ---- what a load sets aside ----------------------------------------------------

def test_a_loads_triggers_are_disabled_and_enabled_again_even_if_it_fails(
        monkeypatch, tmp_path):
    trig = [("shop", "t_audit", "orders", "Y"), ("shop", "t_off", "orders",
                                                 "N"),
            ("shop", "t_other", "lines", "Y")]
    systems, eng = pair(monkeypatch, tmp_path, dst={"triggers": list(trig)})
    said = []
    with pytest.raises(RuntimeError):
        with eng.load_window("shop", said.append, {"orders"}):
            ran = systems.sql(DST)
            assert 'alter trigger "shop"."t_audit" disabled' in ran
            # one already off is not ours to enable, one on another table
            # is not the load's to touch
            assert not [s for s in ran if s.startswith("alter")
                        and ("t_off" in s or "t_other" in s)]
            # the way back is on disk before anything was disabled
            files = list(eng.hop.report_dir("shop").glob(
                "disabled-triggers.*.json"))
            assert len(files) == 1
            assert "enabled" in files[0].read_text()
            raise RuntimeError("the load stopped")
    assert 'alter trigger "shop"."t_audit" enabled' in systems.sql(DST)
    assert not list(eng.hop.report_dir("shop").glob(
        "disabled-triggers.*.json"))


def test_triggers_a_dead_load_left_disabled_are_named_and_enabled_by_the_next(
        monkeypatch, tmp_path):
    import socket
    systems, eng = pair(monkeypatch, tmp_path,
                        dst={"triggers": [("shop", "t_audit", "orders", "N")]})
    path = eng.hop.report_dir("shop") / (
        f"disabled-triggers.{socket.gethostname()}.999999999.1.json")
    path.write_text(json.dumps({
        "shop.t_audit": 'alter trigger "shop"."t_audit" enabled'}))
    r = eng.set_aside("shop")
    assert r.status == "diff" and "t_audit" in r.detail
    with eng.load_window("shop", None, {"orders"}):
        pass
    assert 'alter trigger "shop"."t_audit" enabled' in systems.sql(DST)
    assert not path.exists()


def test_a_load_does_not_begin_where_the_triggers_cannot_be_read(
        monkeypatch, tmp_path):
    systems, eng = pair(monkeypatch, tmp_path)
    systems.fail["from DBC.TriggersV"] = td.DriverError("3523 no access")
    with pytest.raises(SystemExit, match="Nothing has been loaded"):
        with eng.load_window("shop", None, {"orders"}):
            pytest.fail("the load began")


# ---- what the assessment reads ---------------------------------------------------

def test_both_systems_versions_are_read(monkeypatch, tmp_path):
    systems, eng = pair(monkeypatch, tmp_path,
                        dst={"version": "20.00.01.02"})
    assert eng._server_versions() == ("17.20.03.19", "20.00.01.02")
    # a side that would not say is unknown, not clean
    systems.fail["teradata_database_version"] = td.DriverError("no")
    assert eng._server_versions() == (None, None)


def test_the_brand_each_system_names_is_read(monkeypatch, tmp_path):
    systems, eng = pair(monkeypatch, tmp_path,
                        dst={"version": "20.00.01.02"})
    src, dst = eng._brands()
    assert src.name == "teradata" and dst.name == "teradata"
    assert src.version == "17.20.03.19"


def test_room_on_the_target_is_the_headroom_of_its_fullest_amp(
        monkeypatch, tmp_path):
    systems, eng = pair(monkeypatch, tmp_path,
                        dst={"space": [(1000, 100), (1000, 900),
                                       (1000, 500)]})
    # three AMPs, the fullest with 100 bytes left
    assert eng.free_bytes("dst", "shop") == 300
    systems.hosts[DST]["space"] = [(0, 0), (0, 0)]
    assert eng.free_bytes("dst", "shop") is None
    systems.hosts[DST]["space"] = []
    assert eng.free_bytes("dst", "shop") is None


def test_the_accounts_that_may_write_the_target_are_named(
        monkeypatch, tmp_path):
    systems, eng = pair(
        monkeypatch, tmp_path,
        dst={"rights": [("APP", "orders", "I"), ("REPORT", "All", "R"),
                        ("MIGKIT", "All", "I"), ("DBC", "All", "D")]})
    names = [w for w, _ in eng.stream_writers("shop")]
    assert len(names) == 1 and names[0].startswith("APP may write shop")


def test_the_work_the_move_will_not_do_is_counted_and_named(
        monkeypatch, tmp_path):
    systems, eng = pair(
        monkeypatch, tmp_path,
        src={"keys": {}, "objects": [("v1", "V"), ("p1", "P"), ("q1", "Q"),
                                     ("ji", "I")],
             "ppi": {"orders": "RANGE_N(...)"}})
    rows = eng._handwork().rows()
    said = {r["item"]: r["detail"] for r in rows}
    assert "orders" in said["tables with no primary key or unique index"]
    code = said["routines, triggers and views a data mover does not carry"]
    assert "views: 1" in code and "stored procedures: 1" in code
    other = said["objects whose contents no data mover copies"]
    assert "queue tables" in other and "join and hash indexes" in other
    assert "settings that need a decision rather than a copy" in said


def test_views_are_read_as_the_select_they_are_defined_as(
        monkeypatch, tmp_path):
    systems, eng = pair(
        monkeypatch, tmp_path,
        src={"views": [("v_orders", "CREATE VIEW shop.v_orders (a, b) AS"
                                    " LOCKING ROW FOR ACCESS SELECT id, code"
                                    " FROM shop.orders;"),
                       ("v_long", "x" * 12500)]})
    assert eng.neutral_views("src", "shop") == [(
        "v_orders", "LOCKING ROW FOR ACCESS SELECT id, code FROM"
                    " shop.orders")]
    # the one the catalogue may have cut is named, not handed on cut
    assert eng._view_rows("src", "shop")[1] == ["v_long"]


def test_large_values_are_sized_where_they_lie(monkeypatch, tmp_path):
    from migkit import pieces
    cols = orders(doc=column("doc", "BO", 0), note=column("note", "CO", 0))
    seen = []

    def hook(host, low, args):
        if low.startswith("select max(s), avg(s), count(*)"):
            seen.append(low)
            return [(5000, 300.0, 40)]
        if low.startswith("select top"):
            return [(120,), (5000,)]
        return None
    systems, eng = pair(monkeypatch, tmp_path, hook=hook,
                        src={"columns": {"orders": cols}})
    got = eng.row_sizes("src", "shop", ["orders"])
    assert got["orders"]["largest"] >= 5000
    assert 'coalesce(bytes("doc"), 0)' in seen[0]
    assert 'coalesce(octet_length("note"), 0)' in seen[0]
    assert isinstance(eng.LARGE_VALUES, pieces.Sql)
    assert not eng.LARGE_VALUES.piece and not eng.LARGE_VALUES.append


# ---- a repair, end to end, on two systems that keep rows ---------------------

class Rows:
    """The rows of `shop.orders` of two systems, kept in memory and changed
    by the statements a repair sends: a read by key, a delete by key and an
    insert."""

    def __init__(self, src, dst):
        self.rows = {SRC: dict(src), DST: dict(dst)}

    def __call__(self, host, low, args):
        import re
        if low.startswith("select") and "from DBC" not in low and \
                '"shop"."orders"' in low and "where" in low and "?" in low:
            names = re.findall(r'"(\w+)"', low.split(" from ")[0])
            got = [self.rows[host][k] for k in args if k in self.rows[host]]
            at = {"id": 0, "name": 1}
            return [tuple(r[at[n]] for n in names) for r in got]
        if low.startswith('delete from "shop"."orders"'):
            gone = 0
            for k in args:
                gone += self.rows[host].pop(k, None) is not None
            return gone
        if low.startswith('MANY insert into "shop"."orders"'):
            return None
        return None


def test_rows_that_differ_are_carried_across_and_every_one_is_undone(
        monkeypatch, tmp_path):
    cols = [ID, NAME]
    held = Rows({1: (1, "one"), 2: (2, "TWO"), 4: (4, "four")},
                {2: (2, "two"), 3: (3, "three"), 4: (4, "four")})
    systems, eng = pair(
        monkeypatch, tmp_path, hook=held,
        src={"columns": {"orders": cols}},
        dst={"columns": {"orders": cols}})
    # what a driver does with a batch of inserts: the rows land
    real = type(systems.connect(host=DST).cursor()).executemany

    def executemany(self, sql, seq):
        real(self, sql, seq)
        if sql.startswith('insert into "shop"."orders"'):
            for r in seq:
                held.rows[self.conn.host][r[0]] = tuple(r)
    monkeypatch.setattr(type(systems.connect(host=DST).cursor()),
                        "executemany", executemany)
    reports = eng.hop.report_dir("shop")
    (reports / "data-orders.missing").write_text(json.dumps(["1"]) + "\n")
    (reports / "data-orders.changed").write_text(json.dumps(["2"]) + "\n")
    (reports / "data-orders.extra").write_text(json.dumps(["3"]) + "\n")
    plan = eng.repair_plan("shop", "rows")
    (action,) = [a for a in plan if a.kind == "rows"]
    assert "1 missing, 1 changed, 1 extra" in action.note
    eng.apply("shop", action)
    assert held.rows[DST] == {1: (1, "one"), 2: (2, "TWO"), 4: (4, "four")}
    # what the target held of the rows it overwrote or removed is kept
    undo = (reports / "undo" / "orders.rows.jsonl").read_text().splitlines()
    kept = [json.loads(l) for l in undo]
    assert {(r["key"][0], r["row"]["name"]) for r in kept} == {
        ("2", "two"), ("3", "three")}
    # and the statement-level record was written whole before anything landed
    assert list((reports / "undo").glob("*")), list(reports.glob("**/*"))
    # every statement of the repair undone exactly, from the record
    from migkit import repairlog
    (record,) = repairlog.records(reports / "undo")
    done, same, left = repairlog.reverse(
        record, repairlog.io_for(eng, "shop", record))
    assert (done, left) == (3, [])
    assert held.rows[DST] == {2: (2, "two"), 3: (3, "three"),
                              4: (4, "four")}
    # a plan for nothing else: the rows are all this engine repairs
    assert eng.repair_plan("shop", "sequences") == []

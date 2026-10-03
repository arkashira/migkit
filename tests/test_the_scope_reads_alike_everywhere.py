"""The scope's shared pieces: one way of writing it, one way of matching
it, one digest of it, and the way back reading it in the target's names.

A hop's scope is asked about by the copy, the tail, the check, the
repair, the cutover and the way back. Each reading here is the one they
all use, so a scope cannot mean one thing to the copy and another to the
check, on one machine and not another, or before a cutover and not after.
"""
import pytest

from migkit import hopkeys
from migkit.config import (Endpoint, Hop, build_hop, name_matches,
                           parse_ops, scope_patterns)

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.objects", "mysql")]


def _hop(engine="postgres", **kw):
    return Hop(name="h", engine=engine, source=Endpoint(), target=Endpoint(),
               **kw)


def test_the_nested_form_says_the_same_as_the_dotted_list():
    assert scope_patterns({
        "billing": "all",
        "logistics": ["orders", "parcels"],
        "shop": {"public": ["orders", "lines_*"], "audit": "all"},
        "skipped": False}) == [
        "billing.*", "logistics.orders", "logistics.parcels",
        "shop.public.orders", "shop.public.lines_*", "shop.audit.*"]
    assert scope_patterns(["a.b"]) == ["a.b"]
    assert scope_patterns(None) == []
    with pytest.raises(SystemExit):
        scope_patterns({"shop": 3})


def test_the_nested_form_reaches_the_hop_and_is_checked():
    raw = {"engine": "mysql", "source": {"host": "10.0.0.1", "user": "a"},
           "target": {"host": "10.0.0.2", "user": "a"},
           "include": {"shop": ["orders"]}, "exclude": {"crm": "all"}}
    assert not [p for p in hopkeys.check({"hops": {"h": raw}})
                if p.level == "error"]
    hop = build_hop("h", raw)
    assert hop.include == ["shop.orders"] and hop.exclude == ["crm.*"]
    bad = dict(raw, exclude={"crm": 7})
    assert [p.where for p in hopkeys.check({"hops": {"h": bad}})
            if p.level == "error"] == ["hops.h.exclude"]


def test_a_pattern_reads_the_same_on_every_machine():
    """`fnmatch` folds case on some systems; the scope is matched with
    case kept, and without it only where the engine's names have none."""
    assert name_matches("public.Orders", "public.Orders")
    assert not name_matches("public.Orders", "public.orders")
    assert name_matches("HR.EMPLOYEES", "hr.employees", fold=True)
    ora = _hop("oracle", include=["hr.employees"])
    assert not ora.excluded("ORCL", "HR", "EMPLOYEES")
    assert ora.excluded("ORCL", "HR", "JOBS")
    pg = _hop(include=["hr.employees"])
    assert pg.excluded("db", "HR", "EMPLOYEES")


def test_the_changes_a_table_carries():
    h = _hop(mapping={"ops": {"*": "insert,update",
                              "shop.public.orders": ["insert"]}})
    assert h.carries("shop", "public", "orders") == {"insert"}
    assert h.carries("shop", "public", "lines") == {"insert", "update"}
    assert _hop().carries("x", "y") == {"insert", "update", "delete"}
    assert parse_ops("i, u ,d") == {"insert", "update", "delete"}
    with pytest.raises(SystemExit):
        parse_ops("upsert")


def test_a_contract_said_wrong_is_refused():
    raw = {"engine": "postgres", "source": {"host": "10.0.0.1", "user": "a"},
           "target": {"host": "10.0.0.2", "user": "a"},
           "mapping": {"ops": {"orders": "merge", "lines": []}}}
    errs = sorted(p.where for p in hopkeys.check({"hops": {"h": raw}})
                  if p.level == "error")
    assert errs == ["hops.h.mapping.ops.lines", "hops.h.mapping.ops.orders"]


def test_the_digest_of_the_scope_moves_with_every_part_of_it():
    base = _hop(databases=["appdb"])
    fp = base.scope_fingerprint()
    assert fp == _hop(databases=["appdb"]).scope_fingerprint()
    for change in ({"include": ["appdb.public.orders"]},
                   {"exclude": ["appdb.public.log"]},
                   {"db_map": {"appdb": "app"}},
                   {"mapping": {"where": {"orders": "id > 1"}}},
                   {"mapping": {"ops": {"*": ["insert"]}}}):
        assert _hop(databases=["appdb"], **change).scope_fingerprint() != fp
    assert not base.scope_changed({"scope_fingerprint": fp})
    assert base.scope_changed("0" * 16)
    # a record made before digests existed is taken as before
    assert not base.scope_changed({})


def test_the_way_back_takes_what_the_way_there_took():
    there = _hop(databases=["appdb"], db_map={"appdb": "app"},
                 include=["appdb.public.*"],
                 exclude=["appdb.public.audit_log"],
                 mapping={"tables": {"appdb.public.orders":
                                     "public.orders_v2"},
                          "columns": {"appdb.public.orders": {
                              "keep": ["id", "st"],
                              "rename": {"st": "status"}}},
                          "where": {"appdb.public.orders": "region = 'TH'"}})
    back = there.reversed()
    assert back.databases == ["app"] and back.db_map == {"app": "appdb"}
    assert back.excluded("app", "public", "audit_log")
    assert not back.excluded("app", "public", "orders_v2")
    assert back.target_table_in("app", "public", "orders_v2") == \
        "public.orders"
    rules = back.column_rules("app", "public", "orders_v2")
    assert rules["keep"] == ["id", "status"]
    assert rules["rename"] == {"status": "st"}
    assert back.row_filter("app", "public", "orders_v2") == "region = 'TH'"
    # every table the way there took, the way back takes, by its new name
    for table in ("orders", "lines", "audit_log"):
        new = there.target_table_in("appdb", "public", table).split(".")[-1]
        assert there.excluded("appdb", "public", table) == \
            back.excluded("app", "public", new)


def test_the_objects_beside_the_tables_a_hop_carries():
    h = _hop("mysql", objects={"views": ["shop.v_orders", "crm.*"],
                               "routines": False, "triggers": "all"})
    assert h.takes_object("views", "shop", "v_orders")
    assert not h.takes_object("views", "shop", "v_other")
    assert h.takes_object("views", "crm", "anything")
    assert not h.takes_object("functions", "shop", "f")
    assert not h.takes_object("procedures", "shop", "p")
    assert h.takes_object("triggers", "shop", "t")
    # a kind not said is all of it
    assert h.takes_object("events", "shop", "e")
    assert _hop().takes_object("roles", "x")


def test_objects_said_wrong_are_refused_and_said_right_reach_the_hop():
    raw = {"engine": "mysql", "source": {"host": "10.0.0.1", "user": "a"},
           "target": {"host": "10.0.0.2", "user": "a"},
           "objects": {"views": ["shop.v1"], "users": False}}
    assert not [p for p in hopkeys.check({"hops": {"h": raw}})
                if p.level == "error"]
    hop = build_hop("h", raw)
    assert not hop.takes_object("roles", "x")
    bad = dict(raw, objects={"widgets": False})
    assert [p.where for p in hopkeys.check({"hops": {"h": bad}})
            if p.level == "error"] == ["hops.h.objects"]


def test_the_way_back_names_the_objects_in_the_targets_database():
    there = _hop("mysql", databases=["shop"], db_map={"shop": "shop2"},
                 objects={"views": ["shop.v1"]})
    back = there.reversed()
    assert back.takes_object("views", "shop2", "v1")
    assert not back.takes_object("views", "shop2", "v2")


def test_a_name_rule_writes_every_name_of_a_level_one_way():
    h = _hop(mapping={"names": {"tables": {"case": "lower", "prefix": "x_"},
                                "schemas": {"case": "lower"}},
                      "tables": {"appdb.public.Keep": "public.kept"}})
    assert h.target_table_in("appdb", "Sales", "Orders") == "sales.x_orders"
    # a rename the hop names itself is taken as written
    assert h.target_table_in("appdb", "public", "Keep") == "public.kept"
    m = _hop("mysql", db_map={"crm": "crm2"},
             mapping={"names": {"tables": {"strip_prefix": "tbl_",
                                           "case": "upper"},
                                "databases": {"suffix": "_new"}}})
    assert m.target_table_in("shop", "tbl_orders") == "ORDERS"
    assert m.target_db("shop") == "shop_new" and m.target_db("crm") == "crm2"
    # a document store's database is never taken for a schema
    g = _hop("mongodb", mapping={"names": {"tables": {"case": "lower"}}})
    assert g.target_table_in("Db", "Coll") == "coll"
    assert _hop().target_table_in("a", "public", "T") == "public.T"


def test_a_name_rule_said_wrong_is_refused():
    raw = {"engine": "postgres", "source": {"host": "10.0.0.1", "user": "a"},
           "target": {"host": "10.0.0.2", "user": "a"},
           "mapping": {"names": {"tables": {"case": "title",
                                            "prefix": "a.b"},
                                 "rows": {}}}}
    assert sorted(p.where for p in hopkeys.check({"hops": {"h": raw}})
                  if p.level == "error") == [
        "hops.h.mapping.names.rows", "hops.h.mapping.names.tables.case",
        "hops.h.mapping.names.tables.prefix"]


def test_an_inventory_asks_one_question_of_every_object():
    """A view, a routine, a trigger: its table in scope, and its kind
    carried - one answer for the inventory, the schema comparers and the
    schema fix (`Engine.takes`)."""
    from migkit.config import scoped
    from migkit.engines.mysql import MySQLEngine
    h = _hop("mysql", objects={"views": ["shop.v1"], "triggers": False})
    eng = MySQLEngine(h)
    assert eng.takes("shop", "view", "v1")
    assert not eng.takes("shop", "view", "v2")
    assert not eng.takes("shop", "trigger", "t1", "orders")
    assert eng.takes("shop", "index", "orders.i", "orders")
    # a hop that narrows its objects is one that leaves something out
    assert scoped(h) and not scoped(_hop("mysql"))
    out = MySQLEngine(_hop("mysql", exclude=["shop.orders"]))
    assert not out.takes("shop", "trigger", "t1", "orders")


def test_how_a_hop_narrows_is_read_in_one_place():
    """Seven readers once asked "does the hop take less than everything"
    each its own way, and a copy of the pages served a hop that kept
    deletes off the target or left its views out. One reading
    (`narrowed_by`) names every way; each reader takes the parts it
    cannot serve from it."""
    from migkit import verdict
    from migkit.config import narrowed_by, narrowed_said, scoped
    whole = _hop()
    assert narrowed_by(whole) == {} and not scoped(whole)
    cases = {
        "include": dict(include=["shop.orders"]),
        "exclude": dict(exclude=["shop.audit"]),
        "objects_not_carried": dict(objects={"views": False}),
        "objects_named": dict(objects={"functions": ["shop.f_*"]}),
        "row_filters": dict(mapping={"where": {"orders": "id > 1"}}),
        "column_rules": dict(mapping={"columns": {"orders": {"keep":
                                                             ["id"]}}}),
        "values_masked": dict(options={"mask_on_write":
                                       {"orders.email": "null"}}),
        "renamed": dict(mapping={"tables": {"orders": "orders_v2"}}),
        "names_ruled": dict(mapping={"names": {"tables":
                                               {"prefix": "x_"}}}),
        "databases_renamed": dict(db_map={"shop": "shop2"}),
        "changes_carried": dict(mapping={"ops": {"orders": "insert"}}),
    }
    for part, kw in cases.items():
        h = _hop(**kw)
        got = narrowed_by(h)
        assert list(got) == [part], (part, got)
        assert narrowed_said(h)
        assert verdict.hop_scope(h), part
        assert scoped(h) == (part in ("include", "exclude",
                                      "objects_not_carried",
                                      "objects_named")), part
    # a database mapped to its own name, or a rule carrying every change,
    # narrows nothing
    assert narrowed_by(_hop(db_map={"shop": "shop"})) == {}
    assert narrowed_by(_hop(mapping={"ops": {"*": "i,u,d"}})) == {}
    # a backup seed serves database names and the changes the tail
    # carries; nothing else
    from migkit import backupseed
    assert narrowed_said(_hop(db_map={"shop": "shop2"}),
                         backupseed._SEED_SERVES) == ""
    assert narrowed_said(_hop(exclude=["shop.audit"]),
                         backupseed._SEED_SERVES)


def test_the_way_back_keeps_the_schema_of_a_table_renamed_alone():
    """`appdb.public.orders: orders_v2` renames the table in its schema;
    the way back read it as `app.orders_v2`, which names no table, and
    wrote the changes back under the new name."""
    there = _hop(databases=["appdb"], db_map={"appdb": "app"},
                 include=["appdb.public.orders", "appdb.public.lines"],
                 mapping={"tables": {"appdb.public.orders": "orders_v2"},
                          "ops": {"appdb.public.orders": "insert, update"}})
    back = there.reversed()
    assert back.mapping["tables"] == {"app.public.orders_v2": "orders"}
    assert back.target_table_in("app", "public", "orders_v2") == "orders"
    # what the way there took, the way back takes under the new name
    assert back.include == ["app.public.orders_v2", "app.public.lines"]
    assert not back.excluded("app", "public", "orders_v2")
    assert back.excluded("app", "public", "orders")
    assert back.carries("app", "public", "orders_v2") == {"insert",
                                                          "update"}


def test_the_way_back_turns_the_name_rules_round():
    there = _hop(databases=["shop"],
                 mapping={"names": {"tables": {"prefix": "x_"},
                                    "databases": {"suffix": "_new"}}})
    assert there.target_db("shop") == "shop_new"
    assert there.target_table_in("shop", "public", "orders") == \
        "public.x_orders"
    back = there.reversed()
    assert back.databases == ["shop_new"]
    assert back.mapping["names"] == {"tables": {"strip_prefix": "x_"},
                                     "databases": {"strip_suffix": "_new"}}
    assert back.target_db("shop_new") == "shop"
    assert back.target_table_in("shop_new", "public", "x_orders") == \
        "public.orders"
    # a scope written in the source's names reaches the target's
    scoped_there = _hop(mapping={"names": {"tables": {"prefix": "x_"}}},
                        include=["shop.public.ord*"])
    assert scoped_there.reversed().include == ["shop.public.x_ord*"]


def test_a_name_rule_that_cannot_be_turned_round_is_refused():
    """Lower case, or a stripped prefix, does not say what each name was:
    the way back would write to a table the old source does not have."""
    for rule in ({"case": "lower"}, {"strip_prefix": "t_"}):
        there = _hop(mapping={"names": {"tables": rule}})
        with pytest.raises(SystemExit, match="cannot be turned round"):
            there.reversed()


#: the hop's rules asked with a table's parts: a listed name is split
#: for them one way (`Hop.parts_of`)
ASKED_BY_PARTS = ("excluded", "carries", "row_filter", "row_filter_zone",
                  "column_rules", "target_table_in", "takes_object",
                  "tolerance")


def test_a_listed_table_is_asked_of_the_scope_one_way():
    """A table's listed name is split once, the way names are
    (`Hop.parts_of`, `Hop.takes_table`, `Engine.in_scope`): split by hand
    beside them, `public."v1.orders"` broke at the wrong dot and a bare
    PostgreSQL name was asked outside its schema - by the scope, the row
    filter, the changes carried and every other rule."""
    import ast
    import pathlib
    root = pathlib.Path(__file__).resolve().parents[1] / "migkit"
    bad = []
    for f in sorted(root.rglob("*.py")):
        for node in ast.walk(ast.parse(f.read_text())):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in ASKED_BY_PARTS):
                continue
            for arg in node.args:
                inner = arg.value if isinstance(arg, ast.Starred) else None
                if inner is None:
                    continue
                for sub in ast.walk(inner):
                    by_dot = (isinstance(sub, ast.Call)
                              and isinstance(sub.func, ast.Attribute)
                              and sub.func.attr == "split"
                              and (not sub.args or (
                                  isinstance(sub.args[0], ast.Constant)
                                  and sub.args[0].value == ".")))
                    if by_dot or (isinstance(sub, ast.Attribute)
                                  and sub.attr == "_split"):
                        bad.append(f"{f.relative_to(root)}:{node.lineno}")
    # a pattern checked against itself, and the generic engine's own
    # names, each said where it is
    allowed = {"hopkeys.py", "engines/generic.py"}
    assert [b for b in bad if b.split(":")[0] not in allowed] == []


def test_a_quoted_or_bare_name_is_taken_as_the_engine_names_it():
    h = _hop(include=["appdb.public.orders", 'appdb.public.v1.orders'])
    assert h.takes_table("appdb", "orders")              # public, bare
    assert h.takes_table("appdb", "public.orders")
    assert h.takes_table("appdb", 'public."v1.orders"')
    assert not h.takes_table("appdb", "crm.orders")
    my = _hop("mysql", include=["shop.orders"])
    assert my.takes_table("shop", "orders")
    assert not my.takes_table("crm", "orders")

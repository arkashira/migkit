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

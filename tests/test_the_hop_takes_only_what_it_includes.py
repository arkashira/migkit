"""A hop's `include`: what it takes, read through the one scope answer.

A migration tool told to leave a log table behind moves the rest; the
check that follows has to compare exactly the rest. `include` says what a
hop takes the way `exclude` says what it leaves - the same names and
shell patterns, matched the same right-anchored way - and both are read
through `Hop.excluded`, so the copy, the check, the repair and the tail
cannot disagree about a table.
"""
import json

import pytest

from migkit import hopkeys, rehearsal, report, verdict
from migkit.config import Endpoint, Hop, build_hop


def _hop(engine="postgres", include=(), exclude=(), **kw):
    return Hop(name="h", engine=engine, source=Endpoint(), target=Endpoint(),
               include=list(include), exclude=list(exclude), **kw)


def test_a_hop_without_include_takes_everything_as_before():
    h = _hop()
    assert not h.excluded("appdb", "public", "orders")
    assert not h.excluded("appdb")
    assert h.included("anything", "at", "all")


@pytest.mark.parametrize("entry", ["orders", "public.orders",
                                   "appdb.public.orders", "appdb.public.*",
                                   "appdb.*", "appdb"])
def test_every_way_of_naming_a_table_takes_it(entry):
    h = _hop(include=[entry])
    assert not h.excluded("appdb", "public", "orders")


def test_what_include_does_not_name_is_left_out():
    h = _hop(include=["appdb.public.orders", "appdb.billing.*"])
    assert h.excluded("appdb", "public", "event_log")
    assert h.excluded("appdb", "audit", "orders")
    assert not h.excluded("appdb", "billing", "invoices")


def test_exclude_wins_where_both_name_a_table():
    h = _hop(include=["appdb.billing.*"], exclude=["appdb.billing.audit_log"])
    assert not h.excluded("appdb", "billing", "invoices")
    assert h.excluded("appdb", "billing", "audit_log")


def test_a_table_passed_as_one_part_is_taken_all_the_same():
    """Some callers pass `schema.table` as one part; a bare entry must
    take it either way, or a foreign key's check reads it as left out."""
    h = _hop(include=["orders"])
    assert not h.excluded("appdb", "public.orders")
    assert h.excluded("appdb", "public.order_log")


def test_a_database_is_skipped_only_when_every_entry_names_another():
    pg = _hop(include=["crm.public.orders"])
    assert pg.excluded("shop") and not pg.excluded("crm")
    # a shorter entry may be in any database: every one is opened
    assert not _hop(include=["public.orders"]).excluded("shop")
    assert not _hop(include=["orders"]).excluded("shop")
    my = _hop("mysql", include=["crm.orders"])
    assert my.excluded("shop") and not my.excluded("crm")
    # where a name may hold a dot of its own, no entry is sure of it
    assert not _hop("mongodb", include=["crm.orders"]).excluded("shop")


def test_a_collection_or_topic_keeps_its_dots():
    mongo = _hop("mongodb", include=["fs.chunks"])
    assert not mongo.excluded("app", "fs.chunks")
    assert mongo.excluded("app", "chunks")
    kafka = _hop("kafka", include=["orders.*"])
    assert not kafka.excluded("cluster", "orders.created")
    assert kafka.excluded("cluster", "payments.created")


def test_a_pair_reads_names_as_its_source_names_them():
    h = _hop("hetero", include=["shop.orders"],
             options={"source_engine": "mysql", "target_engine": "postgres"})
    assert not h.excluded("shop", "orders")
    assert h.excluded("shop", "sessions") and h.excluded("crm")


def test_an_entry_that_takes_no_table_is_named():
    h = _hop(include=["appdb.public.orders", "appdb.public.gone", "nope.*"])
    tables = [("appdb", "public.orders"), ("appdb", "public.event_log")]
    assert h.unused_include(tables) == ["appdb.public.gone", "nope.*"]
    assert _hop("mysql", include=["shop.*"]).unused_include(
        [("shop", "orders")]) == []


def test_migkits_own_and_other_tools_bookkeeping_stay_out_whatever_is_named():
    h = _hop(include=["appdb.*"])
    assert h.excluded("appdb", "public", "migkit_origin")


def test_a_rename_that_names_its_database_is_read_with_it():
    """`appdb.public.orders: orders_v2` is the only way to say which of two
    databases' `orders` is renamed; read without the database it was
    passed over, and the table compared against a name it no longer has."""
    h = _hop(mapping={"tables": {"appdb.public.orders": "public.orders_v2"}})
    assert h.target_table_in("appdb", "public", "orders") == \
        "public.orders_v2"
    assert h.target_table_in("crm", "public", "orders") == "public.orders"
    from migkit.engines.hetero import HeteroEngine
    pair = HeteroEngine.__new__(HeteroEngine)
    pair.hop = h
    assert pair._rename("public.orders", "appdb") == "public.orders_v2"
    assert pair._rename("public.orders", "crm") == "public.orders"
    assert pair._renamer("appdb")("public.orders") == "public.orders_v2"


def _problems(raw):
    return [(p.level, p.where, p.text)
            for p in hopkeys.check({"hops": {"h": raw}})]


BASE = {"engine": "postgres",
        "source": {"host": "10.0.0.1", "user": "app"},
        "target": {"host": "10.0.0.2", "user": "app"},
        "databases": ["appdb"]}


def test_include_is_a_hop_key_and_reaches_the_hop():
    raw = {**BASE, "include": ["appdb.public.orders"]}
    assert not [p for p in _problems(raw) if p[0] == "error"]
    assert build_hop("h", raw).include == ["appdb.public.orders"]


def test_include_said_wrong_or_against_the_rest_of_the_hop_is_refused():
    errs = [p for p in _problems({**BASE, "include": "appdb"})
            if p[0] == "error"]
    assert [e[1] for e in errs] == ["hops.h.include"], errs
    errs = [p for p in _problems({**BASE, "include": ["appdb..orders"]})
            if p[0] == "error"]
    assert errs and "not a dotted name" in errs[0][2]
    errs = [p for p in _problems({**BASE, "include": ["appdb.public.log"],
                                  "exclude": ["appdb.public.*"]})
            if p[0] == "error"]
    assert errs and "takes nothing" in errs[0][2]
    errs = [p for p in _problems({
        **BASE, "include": ["appdb.public.orders"],
        "mapping": {"where": {"appdb.public.lines": "id > 1",
                              "orders": "id > 1"}}}) if p[0] == "error"]
    # the rule naming its database and a table include leaves out; a
    # shorter rule may be any database's and is not guessed at
    assert [e[1] for e in errs] == ["hops.h.mapping.where.appdb.public.lines"]


def test_the_verdict_says_what_it_covers():
    h = _hop(include=["appdb.public.orders"], exclude=["appdb.public.tmp*"],
             mapping={"where": {"appdb.public.orders": "id > 1"}})
    assert verdict.hop_scope(h) == {
        "include": ["appdb.public.orders"], "exclude": ["appdb.public.tmp*"],
        "row_filters": ["appdb.public.orders"]}
    assert verdict.hop_scope(_hop()) == {}
    html = report._scope_section(h)
    assert "takes only" in html and "appdb.public.orders" in html
    assert report._scope_section(_hop()) == ""


def test_a_same_within_the_hops_scope_is_a_same():
    """The hop's own scope is its definition, not a narrowed run."""
    h = _hop(include=["appdb.public.orders"])
    got = verdict.decide([{"status": "ok", "scope": "appdb.public.orders"}],
                         h)
    assert got.status == "same"


def test_a_rehearsal_whose_include_differs_says_so():
    a = rehearsal.path_of(_hop(include=["appdb.public.orders"]))
    b = rehearsal.path_of(_hop())
    assert "what is taken (include)" in rehearsal.differences(a, b)
    # one recorded before include existed reads as the same scope
    old = {k: v for k, v in b.items() if k != "include"}
    assert "what is taken (include)" not in rehearsal.differences(old, b)


def test_a_whole_copy_is_refused_where_the_hop_takes_only_some():
    from migkit.config import scoped
    assert scoped(_hop(include=["appdb.public.orders"]))
    assert scoped(_hop(exclude=["x"]))
    assert not scoped(_hop())
    from migkit import redisshake
    regexes, why = redisshake.key_filters(
        _hop("redis", include=["0.session:*"]), "0")
    assert regexes is None and "only some keys" in why


def test_the_hop_file_written_back_keeps_include():
    raw = {**BASE, "include": ["appdb.public.orders"]}
    hop = build_hop("h", raw)
    assert hopkeys.decisions(hop)["include"] == ["appdb.public.orders"]
    assert json.dumps(hopkeys.decisions(hop))


class _Listed:
    """An engine that lists what a source holds and nothing else, the
    scope of the hop applied the way every engine applies it."""

    def __new__(cls, hop, held):
        from migkit.engines.base import Engine

        class One(Engine):
            def databases(self):
                return list(held)

            def neutral_tables(self, side, db):
                return [t for t in held[db]
                        if not self.hop.excluded(db, *t.split("."))]
        return One(hop)


def test_before_the_move_an_entry_that_takes_nothing_is_said():
    held = {"appdb": ["public.orders", "public.event_log"],
            "logs": ["public.lines"]}
    h = _hop(include=["appdb.public.orders", "appdb.public.gone"])
    items = _Listed(h, held)._scope_items()
    said = " | ".join(i["detail"] for i in items)
    assert {i["level"] for i in items} == {"warn"}, items
    assert "'appdb.public.gone'" in said and "database logs" in said
    ok = _Listed(_hop(include=["appdb.public.orders"]),
                 {"appdb": held["appdb"]})._scope_items()
    assert [i["level"] for i in ok] == ["pass"]
    assert "takes 1 tables" in ok[0]["detail"]
    # and nothing is said for a hop that takes everything
    assert _Listed(_hop(), held)._scope_items() == []

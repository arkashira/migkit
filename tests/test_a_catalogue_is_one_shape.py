"""A side's catalogue is one shape every reader and comparer meets on, and a
table's fingerprint changes with anything the server says about it (audit
2026-10-01, SC3, SC15).

Drift was (column name, type string): `varchar(50)` to `varchar(100)`, a
column made NOT NULL, a default, an index, a foreign key or a collation
changed on the source read as no change.
"""
import pytest

from migkit.catalog import Catalog, Column, ForeignKey, Index, Shape


def _orders(**change):
    cols = {"id": Column("id", "bigint", "int", False),
            "code": Column("code", "varchar(50)", "text", True, None,
                           "en_US"),
            "state": Column("state", "text", "text", True, "'new'::text")}
    cols.update(change.pop("columns", {}))
    got = dict(tables=["public.orders"],
               columns={"public.orders": list(cols.values())},
               keys={"public.orders": ("id",)},
               indexes={"public.orders": [Index("orders_code", ("code",))]},
               fks={"public.orders": [ForeignKey("orders_cust", ("cust",),
                                                 "public.customers",
                                                 ("id",))]})
    got.update(change)
    return Catalog(**got)


CHANGES = {
    "widened": dict(columns={"code": Column("code", "varchar(100)", "text",
                                            True, None, "en_US")}),
    "not null": dict(columns={"code": Column("code", "varchar(50)", "text",
                                             False, None, "en_US")}),
    "default": dict(columns={"state": Column("state", "text", "text", True,
                                             "'open'::text")}),
    "collation": dict(columns={"code": Column("code", "varchar(50)", "text",
                                              True, None, "C")}),
    "index": dict(indexes={"public.orders": [
        Index("orders_code", ("code",), unique=True)]}),
    "foreign key": dict(fks={}),
    "key": dict(keys={"public.orders": ("id", "code")}),
}


@pytest.mark.parametrize("what", sorted(CHANGES))
def test_every_change_the_server_can_say_moves_the_fingerprint(what):
    before, after = _orders(), _orders(**CHANGES[what])
    assert before.shape() != after.shape()
    assert before.shape().changes(after.shape()) == {
        "created": [], "dropped": [], "altered": ["public.orders"]}
    assert before.explain(after, "public.orders"), what


def test_what_does_not_change_the_table_does_not_move_it():
    """migkit's own reading of a column (`cls`), an index's name and the
    case of a declared type are not the table's shape: a newer migkit
    must not see drift in a table nobody touched."""
    a = _orders()
    b = _orders(columns={"id": Column("id", "BIGINT", "decimal", False)},
                indexes={"public.orders": [Index("renamed", ("code",))]})
    assert a.shape() == b.shape()
    assert a.explain(b, "public.orders") == []
    assert a.shape().changed(b.shape()) == set()


def test_it_says_what_changed_in_words():
    a = _orders()
    b = _orders(columns={"code": Column("code", "varchar(100)", "text",
                                        False, None, "en_US"),
                         "note": Column("note", "text")},
                fks={})
    said = a.explain(b, "public.orders")
    assert "column code varchar(50) -> varchar(100)" in said, said
    assert "column code now NOT NULL" in said, said
    assert "column note added" in said, said
    assert "foreign key orders_cust dropped" in said, said
    assert a.explain(Catalog(), "public.orders") == ["dropped"]
    assert Catalog().explain(a, "public.orders") == ["created"]


def test_created_and_dropped_tables():
    a = Shape({"t1": "x", "t2": "y"})
    b = Shape({"t2": "y", "t3": "z"})
    assert a.changes(b) == {"created": ["t3"], "dropped": ["t1"],
                            "altered": []}
    assert a.changed(b) == {"t1", "t3"}


def test_it_is_kept_and_read_back_whole():
    a = _orders()
    back = Catalog.from_dict(a.as_dict())
    assert back.shape() == a.shape()
    assert back.explain(a, "public.orders") == []
    # an engine's column catalogue as it answers today reads into it
    got = Catalog.from_columns({"public.t": [("id", "integer"),
                                             ("v", "text")]})
    assert got.tables == ["public.t"]
    assert [c.name for c in got.columns["public.t"]] == ["id", "v"]

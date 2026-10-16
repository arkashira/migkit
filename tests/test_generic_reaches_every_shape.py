"""The generic engine on the shapes left after the scope: columns written
transformed, the changes the hop carries, keys, foreign keys and indexes
read from each side's catalogue in its own spelling, NULLs told from empty
texts and the views the hop's `objects` take. Fakes only: no server."""
import pytest

from migkit.config import Endpoint, Hop

PROVES = [("scope.transforms", "generic"), ("scope.ops", "generic"),
          ("scope.objects", "generic"),
          ("verify.structure.keys", "generic"),
          ("verify.structure.foreign-keys", "generic"),
          ("verify.structure.indexes", "generic"),
          ("verify.structure.objects", "generic"),
          ("verify.value.null-empty", "generic")]


class _Dialect:
    @staticmethod
    def quote(name):
        return f'"{name}"'


class _Conn:
    """A driver connection answering by the first word of `answers` its
    statement holds; a statement none matches is refused, as a catalogue
    the server does not keep is."""

    dialect = _Dialect()
    default_schema = "public"

    def __init__(self, answers=()):
        self.answers, self.asked = list(answers), []

    @staticmethod
    def parse_table_name(table):
        return tuple(table.split("."))

    def query(self, sql, kind):
        self.asked.append(sql)
        for word, rows in self.answers:
            if word in sql:
                return list(rows)
        raise RuntimeError(f"relation does not exist: {sql[:40]}")


def _engine(options=None, mapping=None, objects=None):
    from migkit.engines.generic import GenericEngine
    hop = Hop(name="g", engine="generic",
              source=Endpoint(host="", port=0, user="", password="",
                              options={"url": "postgresql://u@10.0.0.1/a"}),
              target=Endpoint(host="", port=0, user="", password="",
                              options={"url": "postgresql://w@10.0.0.2/a"}),
              mapping=mapping or {},
              options={"tables": ["orders"], **(options or {})},
              **({"objects": objects} if objects is not None else {}))
    return GenericEngine(hop)


def _wire(eng, **conns):
    eng._connect = lambda side: conns[side]
    eng._from = lambda conn, table: table
    return eng


def test_a_masked_column_is_written_and_read_back_transformed():
    eng = _engine({"mask_on_write": {"orders.email": "truncate 3"}})
    rows = eng._masked("orders", ["id", "email"], [(1, "alice@x"),
                                                   (2, None)])
    assert rows == [[1, "ali"], [2, None]]
    assert eng.masked_on_the_way("-", ["orders", "items"]) == {"orders"}
    # the row check reads the source as the target holds it, the target
    # as stored
    _wire(eng, src=_Conn([("SELECT", [(1, "alice@x")])]),
          dst=_Conn([("SELECT", [(1, "ali")])]))
    assert eng._side_rows("src", "orders", ["id", "email"]) == [[1, "ali"]]
    assert eng._side_rows("dst", "orders", ["id", "email"]) == [(1, "ali")]


def test_a_transformed_key_is_never_resumed_by_and_unmasked_one_is():
    eng = _engine({"mask_on_write": {"orders.id": "hash"},
                   "mask_key": "CHANGE_ME"})
    eng._unique = lambda table: True
    choice = eng.copy_way("orders", ["id", "email"])
    assert choice.rung.name == "whole-table"
    plain = _engine()
    plain._unique = lambda table: True
    assert plain.copy_way("orders", ["id", "email"]).rung.name == "key-order"


def test_only_the_changes_the_hop_carries_are_followed():
    eng = _engine(mapping={"ops": {"orders": "insert"}})
    eng._rows_by_keys = lambda side, table, columns, keys: {("1",): (1, "a")}
    rows = [(1, "a2"), (2, "b")]
    # row 1 is on the target: its change is an update the hop leaves
    assert eng._carried_ops("orders", ["id", "v"], rows) == [(2, "b")]
    upd = _engine(mapping={"ops": {"orders": "update, delete"}})
    upd._rows_by_keys = eng._rows_by_keys
    assert upd._carried_ops("orders", ["id", "v"], rows) == [(1, "a2")]
    both = _engine()
    assert both._carried_ops("orders", ["id", "v"], rows) == rows
    none = _engine(mapping={"ops": {"orders": "delete"}})
    assert none._carried_ops("orders", ["id", "v"], rows) == []


_KEYS = ("PRIMARY KEY", "c1", "id", 1)


def test_keys_and_foreign_keys_compare_by_shape_not_name():
    src = _Conn([("table_constraints tc", [
        _KEYS, ("UNIQUE", "u_mail", "email", 1),
        ("FOREIGN KEY", "fk_c", "customer_id", 1)]),
        ("statistics", [("ix_a", 0, "created", 1)]),
        ("constraint_type = 'CHECK'", [("ck1",)])])
    # the target is another dialect: no standard key catalogue, its own
    dst = _Conn([("all_constraints c", [
        ("P", "SYS_C1", "ID", 1)]),
        ("all_ind_columns", [("SYS_C1", 1, "ID", 1)]),
        ("constraint_type = 'C'", [("ck9",)])])
    eng = _wire(_engine(), src=src, dst=dst)
    got = {r.scope.split()[-1]: r for r in
           eng._structure_results("orders", "orders")}
    assert got["keys"].status == "diff" and "(email)" in got["keys"].detail
    assert got["fk"].status == "diff" and "customer_id" in got["fk"].detail
    # the key's own index is the key, read once; the source's other is lost
    assert got["indexes"].status == "warn" and "created" in \
        got["indexes"].detail
    assert got["check-constraints"].status == "ok"
    # the spelling that answered is asked first for the next table
    before = len(dst.asked)
    eng._structure("dst", "items")
    assert all("information_schema" not in q for q in dst.asked[before:])


def test_alike_keys_pass_and_an_unanswered_catalogue_is_said_not_passed():
    same = [("table_constraints tc", [_KEYS]), ("statistics", [])]
    eng = _wire(_engine(), src=_Conn(same), dst=_Conn(same))
    got = {r.scope.split()[-1]: r.status
           for r in eng._structure_results("orders", "orders")}
    assert got == {"keys": "ok", "fk": "ok", "indexes": "ok"}
    eng = _wire(_engine(), src=_Conn(same), dst=_Conn([]))
    got = {r.scope.split()[-1]: r.status
           for r in eng._structure_results("orders", "orders")}
    assert got == {"keys": "skip", "indexes": "skip"}


def _null_conn(counts):
    return _Conn([("SUM(CASE", [counts])])


def test_an_empty_text_turned_null_is_found_column_by_column():
    eng = _wire(_engine(), src=_null_conn((0, 0, 0, 3)),
                dst=_null_conn((0, 0, 3, 0)))
    eng._declared_types = lambda side, t: {"id": "integer",
                                           "note": "varchar(20)"}
    r = eng._null_empty_result("orders", "orders")
    assert r.status == "diff"
    assert "note: 0 NULL/3 empty" in r.detail
    assert "turned NULL" in r.detail
    eng = _wire(_engine(), src=_null_conn((0, 0, 1, 2)),
                dst=_null_conn((0, 0, 1, 2)))
    eng._declared_types = lambda side, t: {"id": "integer",
                                           "note": "varchar(20)"}
    assert eng._null_empty_result("orders", "orders").status == "ok"
    # only a text column is asked for empty texts
    sql = eng._connect("src").asked[-1]
    assert "\"id\" = ''" not in sql and "\"note\" = ''" in sql


def test_a_view_the_hop_takes_and_the_target_lacks_is_found():
    views = [("public", "v_orders"), ("public", "v_tmp"),
             ("information_schema", "tables")]
    eng = _wire(_engine(objects={"view": ["v_orders"]}),
                src=_Conn([("views", views)]),
                dst=_Conn([("views", [])]))
    r = eng._objects_result("-")
    assert r.status == "diff" and "public.v_orders" in r.detail
    assert "v_tmp" not in r.detail.split(";")[0]
    assert "1 left out by the hop's objects" in r.detail
    eng = _wire(_engine(), src=_Conn([("views", views[:1])]),
                dst=_Conn([("all_views", [("PUBLIC", "V_ORDERS")])]))
    assert eng._objects_result("-").status == "ok"


def test_a_side_without_a_view_catalogue_is_skipped_never_passed():
    eng = _wire(_engine(), src=_Conn([]), dst=_Conn([]))
    assert eng._objects_result("-").status == "skip"

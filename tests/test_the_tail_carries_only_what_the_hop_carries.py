"""What the change tail puts on the target, held to the hop's mapping
(backlog P0-SCOPE.6e), without a server:

* `mapping.ops`: a change of an operation the hop does not carry is left
  out and counted as such - decided on the source's own operation, before
  the row filter turns an insert or an update into a delete - and where it
  keeps deletes, a row that leaves the filter is kept too;
* a delete the row filter makes says so, and a two-way tail holds it to
  no conflict: a row the target never had was a `delete_missing`, which
  under `error` stopped the tail;
* a column the hop keeps out or drops is not one the target lacks: an ADD
  COLUMN on a table with `keep` stopped the tail, or parked the table for
  good;
* two schemas' tables of one name go each to the target's table of its
  own whole name, never both into one;
* a large value the log left out as unchanged is counted.
"""
from collections import Counter

import pytest

from migkit import canon, tailtables, twoway
from migkit.config import Endpoint, Hop
from migkit.engines.hetero import HeteroEngine, _Accounts
from tests.twoway_sides import Side, pair

EP = Endpoint(host="x", port=1, user="x", password="x")


class _Source:
    """A source that answers which rows of a table its row filter selects
    now: those whose key is in `selected`."""
    CANON_ENGINE = "postgres"

    def __init__(self, selected=()):
        self.selected = set(selected)

    def neutral_columns(self, side, db, table):
        return [("id", "integer"), ("region", "text"), ("v", "text")]

    def neutral_rows_by_key(self, side, db, table, cols, key, keys,
                            where=None):
        return {HeteroEngine._key_of(cols, key, list(k)): k for k in keys
                if k[0] in self.selected}


class _Target:
    CANON_ENGINE = "postgres"
    EXPRESSES_ABSENT = False
    CREATES_ON_WRITE = False

    def __init__(self, columns=()):
        self.columns = list(columns)

    def neutral_columns(self, side, db, table):
        return list(self.columns)


def _pair(src=None, dst=None, **kw):
    hop = Hop(name="c", engine="hetero", source=EP, target=EP,
              databases=["d"], options={"source_engine": "postgres",
                                        "target_engine": "postgres"}, **kw)
    got = HeteroEngine.__new__(HeteroEngine)
    got.hop, got.src_name, got.dst_name = hop, "postgres", "postgres"
    got.src_engine, got.dst_engine = src or _Source(), dst or _Target()
    got._row_scope = lambda db, t: ((hop.row_filter(db, *t.split(".")),) * 2)
    return got


def _ins(i, table="public.orders"):
    return canon.change("insert", table, {"id": i}, {"id": i, "v": "x"})


def _del(i, table="public.orders"):
    return canon.change("delete", table, {"id": i})


def _upd(i, table="public.orders"):
    return canon.change("update", table, {"id": i}, {"id": i, "v": "y"})


def test_a_change_the_hop_does_not_carry_is_left_out_and_counted(tmp_path):
    p = _pair(mapping={"ops": {"public.orders": "insert, update"}})
    acc = _Accounts(tmp_path)
    batch = [_ins(1), _del(2), _upd(3), _del(4, "public.other")]
    acc.read(batch)
    out, _ = p._to_target("d", batch, {}, acc)
    assert [(c["op"], c["key"]["id"]) for c in out] == [
        ("insert", 1), ("update", 3), ("delete", 4)]
    acc.check("d", out, "0/1")
    assert acc.rows["public.orders"][tailtables.NOT_CARRIED] == 1
    assert acc.rows["public.orders"]["applied"] == 2


def test_a_hop_that_carries_everything_is_as_it_was(tmp_path):
    p = _pair()
    batch = [_ins(1), _del(2), _upd(3)]
    out, _ = p._to_target("d", batch, {"public.orders": "public.orders"})
    assert out == batch


def test_a_row_that_leaves_the_filter_is_kept_where_deletes_are_not_carried(
        tmp_path):
    """Archive semantics: the delete the filter made of it went through, and
    the archive lost the row."""
    where = {"public.orders": "region = 'TH'"}
    p = _pair(_Source(selected={1}), mapping={
        "where": where, "ops": {"public.orders": "insert, update"}})
    acc = _Accounts(tmp_path)
    batch = [_upd(1), _upd(2)]
    acc.read(batch)
    out, _ = p._to_target("d", batch, {}, acc)
    assert [(c["op"], c["key"]["id"]) for c in out] == [("update", 1)]
    acc.check("d", out, "0/1")
    assert acc.rows["public.orders"][tailtables.NOT_CARRIED] == 1
    # deletes carried: the row goes from the target, said as the filter's
    p = _pair(_Source(selected={1}), mapping={"where": where})
    out, _ = p._to_target("d", batch, {})
    assert out[1]["op"] == "delete" and out[1][canon.FILTERED]
    assert canon.FILTERED not in out[0]


def test_a_delete_the_filter_makes_is_no_conflict(tmp_path):
    """Two ways with a row filter: a row that left it on one side was a
    `delete_missing` on the other, logged, and under `error` the tail
    stopped."""
    side = Side({1: {"v": "a"}})
    p = pair("ab", {"on_conflict": "error", "delete_missing": "error"},
             side, tmp_path / "ab")
    made = {"op": "delete", "table": "t", "key": {"id": 9},
            canon.FILTERED: True}
    got = twoway.resolve(p, "d", [dict(made)], [].append)
    assert [(c["op"], c["key"]) for c in got] == [("delete", {"id": 9})]
    assert not (tmp_path / "ab" / "d" / twoway.RECORD).exists()
    # the source's own delete of a row the target has not got is still one
    with pytest.raises(SystemExit) as e:
        twoway.resolve(p, "d", [canon.change("delete", "t", {"id": 9})],
                       [].append)
    assert "delete_missing" in str(e.value)


def test_a_column_the_hop_keeps_out_is_not_one_the_target_lacks():
    """With `keep`, an ADD COLUMN on the source named its new column as
    missing on the target: the tail stopped, or under park the table
    waited for good for a column the hop never carries."""
    p = _pair(dst=_Target([("id", "integer"), ("v", "text")]),
              mapping={"columns": {"public.orders": {"keep": ["id", "v"]}}})
    p._tail_targets = lambda db: {"public.orders": "public.orders"}
    after = {"public.orders": [("id", "integer"), ("v", "text"),
                               ("added", "text")]}
    assert p._lacking("d", {}, after) == {}
    # by the target's names: a renamed column is there under its new one
    p = _pair(dst=_Target([("id", "integer"), ("label", "text")]),
              mapping={"columns": {"public.orders": {
                  "rename": {"v": "label"}, "drop": ["secret"]}}})
    p._tail_targets = lambda db: {"public.orders": "public.orders"}
    after = {"public.orders": [("id", "integer"), ("v", "text"),
                               ("secret", "text"), ("w", "int")]}
    assert p._lacking("d", {}, after) == {"public.orders": (
        "public.orders", ["w"])}


def test_two_schemas_tables_of_one_name_go_each_to_its_own(monkeypatch):
    class Lists:
        CANON_ENGINE = "postgres"

        def __init__(self, names):
            self.names = names

        def neutral_tables(self, side, db):
            return list(self.names)

        def target_missing(self, db):
            return False
    p = _pair(Lists(["sales.orders", "crm.orders", "public.items"]),
              Lists(["sales.orders", "crm.orders", "public.items"]))
    got = p._tail_targets("d")
    assert p._tail_target(got, "sales.orders", "d") == "sales.orders"
    assert p._tail_target(got, "crm.orders", "d") == "crm.orders"
    assert p._tail_target(got, "public.items", "d") == "public.items"
    assert p._tail_target(got, "items", "d") == "public.items"
    # the target has only one of them: the other stops the batch, never
    # lands in its namesake
    p = _pair(Lists(["sales.orders", "crm.orders"]), Lists(["orders"]))
    got = p._tail_targets("d")
    with pytest.raises(SystemExit) as e:
        p._tail_target(got, "crm.orders", "d")
    assert "another table of the source has its name" in str(e.value)


def test_a_large_value_kept_as_the_target_holds_it_is_counted(tmp_path):
    acc = _Accounts(tmp_path)
    kept = dict(_upd(1), **{canon.UNCHANGED: ["doc"]})
    acc.read([kept, _upd(2)])
    acc.ddl_left("public.orders", "truncate left out by the hop's ddl")
    acc.check("d", [kept, _upd(2)], "0/1")
    row = acc.rows["public.orders"]
    assert row[_Accounts.UNCHANGED] == 1
    assert row["ddl truncate left out by the hop's ddl"] == 1
    assert row["read"] == row["applied"] == 2
    acc.save()
    acc.read([kept])
    acc.check("d", [kept], "0/2")
    acc.save()
    assert acc.total["public.orders"][_Accounts.UNCHANGED] == 2


def test_a_truncate_of_a_table_whose_deletes_are_kept_empties_nothing():
    p = _pair(mapping={"ops": {"*": ["insert", "update"]}})
    said = []
    p.dst_engine.neutral_empty = lambda *a, **k: pytest.fail("emptied")
    p._carry_ddl("d", ("truncate", "public.orders"), {}, said.append)
    assert "does not carry its deletes" in said[0]


def test_the_accounts_say_what_the_hop_does_not_carry_among_the_reasons():
    assert tailtables.NOT_CARRIED in _Accounts.WHY
    assert Counter(_Accounts.WHY)[tailtables.NOT_CARRIED] == 1

"""The second reading reads a renamed table against its new name
(backlog P0-SCOPE.6a).

Within one engine the second reading paired each table with the table of
the same name on the target. A table the hop renames (`mapping.tables`,
the rule read with its database, as the pair engine reads it) has no
table of that name there, so it was never read twice - and nothing said
so. It is paired now with the name it has on the target, the way the
pair engine pairs it (`HeteroEngine.match_tables`); every other table is
paired by its own full name as before, two schemas' tables of one name
included.
"""
from migkit.config import Endpoint, Hop
from migkit.engines.base import Engine


class _Sides(Engine):
    CANON_ENGINE = "postgres"

    def __init__(self, hop, src, dst):
        super().__init__(hop)
        self.tables = {"src": list(src), "dst": list(dst)}

    def neutral_tables(self, side, db):
        return self.tables[side]


def _hop(mapping=None):
    return Hop(name="sr", engine="postgres", source=Endpoint(),
               target=Endpoint(), databases=["appdb", "other"],
               mapping=mapping or {})


def test_a_renamed_table_is_read_against_its_new_name():
    eng = _Sides(_hop({"tables": {"appdb.public.orders": "orders_v2"}}),
                 ["public.orders", "public.lines", "audit.lines"],
                 ["public.orders_v2", "public.lines", "audit.lines"])
    assert eng._second_pairs("appdb") == [
        ("audit.lines", "audit.lines"), ("public.lines", "public.lines"),
        ("public.orders", "public.orders_v2")]


def test_a_rule_for_another_database_renames_nothing_here():
    eng = _Sides(_hop({"tables": {"other.public.orders": "orders_v2"}}),
                 ["public.orders"], ["public.orders", "public.orders_v2"])
    assert eng._second_pairs("appdb") == [("public.orders",
                                           "public.orders")]


def test_a_rename_the_target_does_not_have_is_not_read_as_its_old_name():
    eng = _Sides(_hop({"tables": {"orders": "orders_v2"}}),
                 ["public.orders"], ["public.orders"])
    assert eng._second_pairs("appdb") == []


def test_without_renames_every_table_both_sides_have_as_before():
    eng = _Sides(_hop(), ["public.orders", "public.lines", "audit.lines",
                          "public.only_here"],
                 ["public.orders", "public.lines", "audit.lines",
                  "public.only_there"])
    assert eng._second_pairs("appdb") == [
        ("audit.lines", "audit.lines"), ("public.lines", "public.lines"),
        ("public.orders", "public.orders")]

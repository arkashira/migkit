"""TiDB's tables, indexes, foreign keys, defaults and views are made and
read as the catalogue gives them (P0-PARITY.schema), without a server: the
engine's own methods over what `information_schema` answers.

* a table is made by TiDB's own definition, its key a primary key, quoted
  by backticks under the target's database;
* an index is read with its columns in order, and one that cannot be made
  from its columns alone - a prefix, an expression - is said so, not made
  as a different index;
* a foreign key of several columns is one key, its columns in order;
* a default is read as the catalogue keeps it - a literal quoted, a number
  or an expression as written - with its nullability and identity;
* a view the hop leaves out is not read.
"""
from migkit.config import Endpoint, Hop
from migkit.engines.tidb import TiDBEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("schema.create", "tidb"), ("schema.indexes", "tidb"),
          ("schema.foreign-keys", "tidb"), ("schema.defaults", "tidb"),
          ("schema.views", "tidb")]

EP = Endpoint(host="x", port=1, user="x", password="x")


def _engine(catalogue=None):
    hop = Hop(name="t", engine="tidb", source=EP, target=EP,
              databases=["d"], exclude=["skipme"])
    eng = TiDBEngine(hop)
    asked = []

    def q(side, sql, args=None, fresh=False):
        asked.append((side, " ".join(sql.split()), tuple(args or ())))
        for table, rows in (catalogue or {}).items():
            if f"information_schema.{table}" in sql:
                return rows
        return []
    eng._q = q
    eng.asked = asked
    return eng


def test_a_table_is_made_by_tidbs_definition_under_the_targets_database():
    eng = _engine()
    cols = [("id", "integer", (64,)), ("name", "text", (40,)),
            ("amount", "decimal", (12, 2))]
    got = eng.neutral_create_sql("dst", "d", "t", cols, key=("id",))
    assert got == ("create table `d`.`t` (`id` bigint, `name` varchar(40),"
                   " `amount` decimal(12,2), primary key (`id`))")
    keyless = eng.neutral_create_sql("dst", "d", "t", cols)
    assert "primary key" not in keyless and keyless.endswith(")")


def test_an_index_is_made_from_its_columns_in_order():
    eng = _engine()
    assert eng.neutral_create_index_sql(
        "dst", "d", "t", "ix_name", True, ["name", "id"]) == (
        "create unique index `ix_name` on `d`.`t` (`name`, `id`)")
    assert eng.neutral_create_index_sql(
        "dst", "d", "t", "ix_n", False, ["n"]) == (
        "create index `ix_n` on `d`.`t` (`n`)")


def test_an_index_is_read_with_its_columns_and_whether_they_say_all_of_it():
    eng = _engine({"statistics": [
        ("ix_a", 0, "a", None, None), ("ix_a", 0, "b", None, None),
        ("ix_f", 1, "n", 10, None), ("ix_e", 1, None, None, "(lower(n))")]})
    got = eng.neutral_indexes("src", "d", "t")
    # (name, unique, columns, made from its columns alone)
    assert got == [("ix_a", True, ["a", "b"], True),
                   ("ix_e", False, [], False),
                   ("ix_f", False, ["n"], False)]
    sent = [s for s in eng.asked if "statistics" in s[1]][0]
    assert sent[2] == ("d", "t") and "index_name <> 'PRIMARY'" in sent[1]


def test_a_foreign_key_of_several_columns_is_one_key():
    eng = _engine({"key_column_usage": [
        ("fk1", "child", "a", "parent", "id"),
        ("fk1", "child", "b", "parent", "id2"),
        ("fk2", "child", "c", "other", "id")]})
    assert sorted(eng.foreign_keys("src", "d")) == [
        ("child", ["a", "b"], "parent", ["id", "id2"]),
        ("child", ["c"], "other", ["id"])]


def test_a_default_is_read_as_the_catalogue_keeps_it():
    eng = _engine({"columns": [
        ("id", "NO", None, "auto_increment", "bigint"),
        ("n", "YES", "it's", "", "varchar"),
        ("c", "NO", "1", "", "int"),
        ("t", "YES", "CURRENT_TIMESTAMP", "DEFAULT_GENERATED",
         "timestamp")]})
    assert eng.neutral_column_rules("src", "d", "t") == {
        "id": {"null": False, "default": None, "identity": True},
        "n": {"null": True, "default": "'it''s'", "identity": False},
        "c": {"null": False, "default": "1", "identity": False},
        "t": {"null": True, "default": "CURRENT_TIMESTAMP",
              "identity": False}}


def test_a_view_the_hop_leaves_out_is_not_read():
    eng = _engine({"views": [("v1", "select 1"), ("skipme", "select 2")]})
    assert eng.neutral_views("src", "d") == [("v1", "select 1")]

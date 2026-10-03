"""A table's shape is compared by everything its catalogue says - each
column's whole definition and the table's key, indexes, foreign keys,
checks and triggers - not by (name, type string).

Measured before: `varchar(50)` -> `varchar(100)` read as `character
varying` on both sides, `numeric(10,2)` -> `numeric(12,4)` as `numeric`, and
a NOT NULL, a default, a collation or a unique key added was no change at
all - so a resumed `check` said "was ok" after `ADD UNIQUE`. Held without a
server: a shape is a `catalog.Catalog` kept as (column, text) pairs
(`drift.pairs`), read back whole (`drift.catalog_of`), and compared and
explained by the catalogue's one comparer, a narrowing with why.
"""
import json
import time

from migkit import drift
from migkit.catalog import Catalog, Column, ForeignKey, Index


def _shape(columns, keys=(), indexes=(), fks=(), others=()):
    t = "s.t"
    return drift.pairs(Catalog(columns={t: list(columns)},
                               keys={t: tuple(keys)} if keys else {},
                               indexes={t: list(indexes)} if indexes else {},
                               fks={t: list(fks)} if fks else {},
                               others={t: list(others)} if others else {}))


BEFORE = _shape([Column("id", "int", "", False),
                 Column("code", "varchar(255)", "", True, None, "C"),
                 Column("n", "numeric(12,4)", "", True, "0")], keys=("id",))


def test_every_part_of_a_column_is_a_change():
    after = _shape([Column("id", "int", "", False),
                    Column("code", "varchar(10)", "", False, None, "POSIX"),
                    Column("n", "numeric(12,2)", "", True, "1")],
                   keys=("id",), indexes=[Index("uq_code", ("code",), True)])
    (said,) = drift.changes(BEFORE, after, "postgres")
    for part in ("column code varchar(255) -> varchar(10) (longer values are"
                 " cut)", "column code now NOT NULL",
                 "column code collation C -> POSIX",
                 "column n numeric(12,4) -> numeric(12,2) (scale 4 -> 2:"
                 " rounds)", "column n default 0 -> 1",
                 "unique index uq_code (code) added"):
        assert part in said, (part, said)
    assert drift.changed_tables(BEFORE, after) == {"s.t"}


def test_the_table_s_own_parts_alone_are_a_change():
    """`ALTER TABLE ... ADD UNIQUE` or a trigger touch no column's type:
    they moved the table all the same, and a resumed check reuses nothing
    of it."""
    cols = [Column("id", "int", "", False)]
    a = _shape(cols, keys=("id",))
    for b, words in (
            (_shape(cols, keys=("id",),
                    others=["trigger trg after 0a1b2c3d"]),
             "trigger trg after 0a1b2c3d added"),
            (_shape(cols, keys=("id",),
                    fks=[ForeignKey("fk", ("id",), "s.o", ("id",))]),
             "foreign key fk (id) -> s.o (id) added")):
        assert drift.changed_tables(a, b) == {"s.t"}
        assert drift.changes(a, b) == [f"s.t: {words}"]
    assert drift.changed_tables(a, dict(a)) == set()
    assert drift.changes(a, dict(a)) == []


def test_a_shape_kept_in_json_reads_back_the_same():
    back = {t: [tuple(c) for c in cols]
            for t, cols in json.loads(json.dumps(BEFORE)).items()}
    assert drift.changes(BEFORE, back) == []
    assert drift.catalog_of(back).shape() == drift.catalog_of(BEFORE).shape()
    # what every reader of a shape does with it still works: a column's
    # name and its text, a pair each
    assert [n for n, _ in back["s.t"]] == ["id", "code", "n"]
    assert dict(back["s.t"])["n"].startswith("numeric(12,4)")


def test_a_shape_of_names_and_types_is_read_as_it_was():
    before = {"t": [("a", "integer"), ("b", "text")]}
    after = {"t": [("a", "bigint"), ("c", "text")]}
    assert drift.changes(before, after) == [
        "t: column c added, column b dropped, column a integer -> bigint"]
    assert drift.changed_tables(before, {"t": [("a", "integer"),
                                               ("b", "text")]}) == set()


def test_the_schema_check_s_words_are_its_own():
    a = _shape([Column("id", "int")], others=["check ck ((id > 0))"])
    b = _shape([Column("id", "int"), Column("x", "int")])
    assert drift.table_changes(a["s.t"], b["s.t"],
                               added="only on the target",
                               dropped="missing on the target") == [
        "column x only on the target",
        "check ck ((id > 0)) missing on the target"]


def test_working_tables_are_found_once_for_the_whole_side():
    tables = [f"s.t{i}" for i in range(5000)] + ["s._t1_gho", "s._t2_new",
                                                 "s._x_old"]
    began = time.perf_counter()
    got = drift.transient_among(tables)
    took = time.perf_counter() - began
    assert got == {"s._t1_gho", "s._t2_new"}
    # 509 ms when each table gathered the side's names again, 7 ms once
    # (measured 2026-10-01)
    assert took < 0.25, took
    # another tool's own tables are one answer everywhere
    assert drift.transient("app._orders_ghk") and \
        drift.transient("_dlt_loads")
    assert not drift.transient("orders")

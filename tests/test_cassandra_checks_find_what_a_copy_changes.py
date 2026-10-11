"""The deep checks of a Cassandra hop find what a copy changes without a
word (P0-PARITY.verify): a column the target lacks or holds as another
type, a target column narrower than the source's, a null turned into an
empty text or the other way, a float that drifted, replacement characters
the source does not have, every timestamp of a column shifted by one
offset, a cell newer on the target than the copy could have made it, and a
permission on a taken table that never crossed.

Each reads rows or schema rows this engine already reads on a server (the
table's columns, a table read whole, a row read by its key, a cell's write
time, the cluster's auth tables), so the logic is proved here with
`tests/cassandra_fake.py`; the schema tables read for what a keyspace holds
beside its rows (indexes, triggers, views, types, functions) are asked in
`test_cassandra_what_a_keyspace_holds_is_compared.py`.
"""
import datetime

from migkit.config import Endpoint, Hop
from migkit.engines.cassandra import CassandraEngine
from tests import cassandra_fake as fake

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("verify.structure.columns", "cassandra"),
    ("verify.value.narrowing", "cassandra"),
    ("verify.value.null-empty", "cassandra"),
    ("verify.value.precision", "cassandra"),
    ("verify.value.charset", "cassandra"),
    ("verify.value.timezone", "cassandra"),
    ("verify.movement.target-ahead", "cassandra"),
    ("verify.access.table-grants", "cassandra"),
    ("verify.tables", "cassandra"),
]

COLS = {"id": "int", "name": "text", "score": "double", "at": "timestamp",
        "n": "bigint"}
T0 = datetime.datetime(2024, 3, 1, 12, 0, 0)


def _row(i, **kw):
    return {"id": i, "name": f"n{i}", "score": float(i), "at":
            T0 + datetime.timedelta(minutes=i), "n": i, **kw}


def _engine(tmp_path, monkeypatch, src_rows, dst_rows, dst_cols=None,
            src_at=1_000_000, dst_at=1_000_000, **kw):
    src, dst = fake.Cluster(), fake.Cluster()
    src.table("shop", "orders", COLS, ["id"], rows=src_rows, at=src_at)
    dst.table("shop", "orders", dst_cols or COLS, ["id"], rows=dst_rows,
              at=dst_at)
    fake.install(monkeypatch, src, dst)
    hop = Hop(name="cs", engine="cassandra", source=Endpoint(host="10.0.0.1"),
              target=Endpoint(host="10.0.0.2"), databases=["shop"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return CassandraEngine(hop), src, dst


def _found(eng, word):
    got = [r for r in eng.check_deep("shop") if r.scope == f"shop {word}"]
    assert len(got) == 1, [r.scope for r in eng.check_deep("shop")]
    return got[0]


def _same(n=4):
    return [_row(i) for i in range(1, n + 1)]


# ---- structure ------------------------------------------------------------

def test_columns_the_same_on_both_sides_are_found_equal(tmp_path,
                                                        monkeypatch):
    eng, *_ = _engine(tmp_path, monkeypatch, _same(), _same())
    assert _found(eng, "columns").status == "ok"


def test_a_column_the_target_lacks_is_named_with_the_statement_to_add_it(
        tmp_path, monkeypatch):
    cols = {k: v for k, v in COLS.items() if k != "score"}
    eng, *_ = _engine(tmp_path, monkeypatch, _same(),
                      [{k: v for k, v in r.items() if k != "score"}
                       for r in _same()], dst_cols=cols)
    got = _found(eng, "columns")
    assert got.status == "diff" and "orders.score: not on the target" \
        in got.detail
    fix = (tmp_path / "structural-fix.cql").read_text()
    assert 'alter table "shop"."orders" add "score" double' in fix
    assert (tmp_path / "structural-fix.revert.cql").read_text().strip() == \
        'alter table "shop"."orders" drop "score";'


def test_a_column_of_another_type_is_named_and_needs_a_copy_again(
        tmp_path, monkeypatch):
    eng, *_ = _engine(tmp_path, monkeypatch, _same(), _same(),
                      dst_cols={**COLS, "name": "ascii"})
    got = _found(eng, "columns")
    assert got.status == "diff"
    assert "text on the source, ascii on the target" in got.detail
    assert "copied again" in got.fix_hint


def test_a_key_that_is_not_the_same_key_is_named(tmp_path, monkeypatch):
    eng, src, dst = _engine(tmp_path, monkeypatch, _same(), _same())
    dst.table("shop", "orders", COLS, ["id", "n"], rows=_same())
    got = _found(eng, "columns")
    assert got.status == "diff" and "partition_key" in got.detail


def test_a_renamed_table_is_held_to_the_columns_of_its_new_name(
        tmp_path, monkeypatch):
    eng, src, dst = _engine(tmp_path, monkeypatch, _same(), _same(),
                            mapping={"tables": {"orders": "orders_v2"}})
    dst.keyspaces["shop"]["orders_v2"] = dst.keyspaces["shop"].pop("orders")
    dst.keyspaces["shop"]["orders_v2"].name = "orders_v2"
    assert _found(eng, "columns").status == "ok"


def test_a_target_column_narrower_than_the_sources_is_found(tmp_path,
                                                            monkeypatch):
    eng, *_ = _engine(tmp_path, monkeypatch, _same(), _same(),
                      dst_cols={**COLS, "n": "int", "score": "float"})
    got = _found(eng, "narrowing")
    assert got.status == "diff"
    assert "orders.n: bigint -> int (overflow)" in got.detail
    assert "orders.score: double -> float" in got.detail


def test_a_wider_target_column_is_not_a_narrowing(tmp_path, monkeypatch):
    eng, *_ = _engine(tmp_path, monkeypatch, _same(), _same(),
                      dst_cols={**COLS, "n": "varint"})
    assert _found(eng, "narrowing").status == "ok"


# ---- values -----------------------------------------------------------------

def test_a_null_turned_into_an_empty_text_is_found(tmp_path, monkeypatch):
    src = [_row(1, name=None), _row(2, name=""), _row(3), _row(4)]
    dst = [_row(1, name=""), _row(2, name=None), _row(3), _row(4)]
    eng, *_ = _engine(tmp_path, monkeypatch, src, dst)
    got = _found(eng, "nullempty")
    assert got.status == "diff"
    assert "orders.name: 2 rows null on one side and empty on the other" \
        in got.detail
    assert "(every row)" in got.detail


def test_nulls_and_empties_that_match_are_not_a_finding(tmp_path,
                                                        monkeypatch):
    rows = [_row(1, name=None), _row(2, name="")]
    eng, *_ = _engine(tmp_path, monkeypatch, rows, rows)
    assert _found(eng, "nullempty").status == "ok"


def test_a_float_that_drifted_is_found_and_the_tolerance_accepts_it(
        tmp_path, monkeypatch):
    src = [_row(1, score=1.0000001), _row(2)]
    dst = [_row(1, score=1.0), _row(2)]
    eng, *_ = _engine(tmp_path, monkeypatch, src, dst)
    got = _found(eng, "float")
    assert got.status == "diff" and "orders.score: 1 values drift" \
        in got.detail
    eng, *_ = _engine(tmp_path, monkeypatch, src, dst,
                      options={"float_tolerance": 1e-3})
    assert _found(eng, "float").status == "ok"


def test_replacement_characters_only_the_target_has_are_found(
        tmp_path, monkeypatch):
    src = [_row(1, name="café"), _row(2, name="a � kept")]
    dst = [_row(1, name="caf�"), _row(2, name="a � kept")]
    eng, *_ = _engine(tmp_path, monkeypatch, src, dst)
    got = _found(eng, "charset")
    assert got.status == "diff"
    assert "orders.name: 1 rows hold more U+FFFD" in got.detail


def test_timestamps_all_shifted_by_one_offset_are_found(tmp_path,
                                                        monkeypatch):
    shifted = [_row(i, at=r["at"] + datetime.timedelta(hours=3))
               for i, r in enumerate(_same(), 1)]
    eng, *_ = _engine(tmp_path, monkeypatch, _same(), shifted)
    got = _found(eng, "timeshift")
    assert got.status == "diff" and "orders.at: every row shifted 10800s" \
        in got.detail


def test_timestamps_that_agree_are_not_shifted(tmp_path, monkeypatch):
    eng, *_ = _engine(tmp_path, monkeypatch, _same(), _same())
    assert _found(eng, "timeshift").status == "ok"


def test_a_table_larger_than_the_sample_says_it_read_a_sample(
        tmp_path, monkeypatch):
    eng, *_ = _engine(tmp_path, monkeypatch, _same(10), _same(10))
    eng.VALUES_SAMPLE = 3
    got = _found(eng, "nullempty")
    assert "a sample - the first 3 in token order - of orders" in got.detail


def test_a_row_the_target_lacks_is_the_data_checks_to_name(tmp_path,
                                                           monkeypatch):
    eng, *_ = _engine(tmp_path, monkeypatch, _same(), _same(2))
    for word in ("nullempty", "float", "charset", "timeshift"):
        assert _found(eng, word).status == "ok"


# ---- the boundary -----------------------------------------------------------

def test_cells_newer_on_the_target_than_on_the_source_are_found(
        tmp_path, monkeypatch):
    eng, *_ = _engine(tmp_path, monkeypatch, _same(), _same(),
                      src_at=1_000_000, dst_at=90_000_000)
    got = [r for r in eng.check_deep("shop")
           if r.scope == "shop.orders boundary"][0]
    assert got.status == "diff" and got.table == "orders"
    assert "cells are newer on the target than on the source" in got.detail


def test_cells_as_old_as_the_sources_are_not_ahead(tmp_path, monkeypatch):
    eng, *_ = _engine(tmp_path, monkeypatch, _same(), _same())
    got = [r for r in eng.check_deep("shop")
           if r.scope == "shop.orders boundary"][0]
    assert got.status == "ok"


def test_cells_older_on_the_target_are_not_ahead(tmp_path, monkeypatch):
    eng, *_ = _engine(tmp_path, monkeypatch, _same(), _same(),
                      src_at=90_000_000, dst_at=1_000_000)
    got = [r for r in eng.check_deep("shop")
           if r.scope == "shop.orders boundary"][0]
    assert got.status == "ok"
    times = [r for r in eng.check_deep("shop")
             if r.scope == "shop.orders cell times"][0]
    assert times.status == "diff"


# ---- who may use the tables ----------------------------------------------------

def test_a_permission_that_did_not_cross_is_found(tmp_path, monkeypatch):
    eng, src, dst = _engine(tmp_path, monkeypatch, _same(), _same())
    src.add_role("app", [("SELECT", "data/shop/orders"),
                         ("MODIFY", "data/shop/orders"),
                         ("SELECT", "data/shop/scratch")])
    dst.add_role("app", [("SELECT", "data/shop/orders")])
    got = _found(eng, "grants")
    assert got.status == "diff"
    assert "app: MODIFY on data/shop/orders" in got.detail
    assert "scratch" not in got.detail
    assert "2 of 2" not in got.detail and "1 of 2" in got.detail


def test_permissions_that_crossed_are_found_equal(tmp_path, monkeypatch):
    eng, src, dst = _engine(tmp_path, monkeypatch, _same(), _same())
    for c in (src, dst):
        c.add_role("app", [("SELECT", "data/shop/orders"),
                           ("SELECT", "data/shop")])
    assert _found(eng, "grants").status == "ok"


def test_a_permission_is_read_under_the_names_the_target_gives(
        tmp_path, monkeypatch):
    eng, src, dst = _engine(tmp_path, monkeypatch, _same(), _same(),
                            mapping={"tables": {"orders": "orders_v2"}})
    dst.keyspaces["shop"]["orders_v2"] = dst.keyspaces["shop"].pop("orders")
    src.add_role("app", [("SELECT", "data/shop/orders")])
    dst.add_role("app", [("SELECT", "data/shop/orders_v2")])
    assert _found(eng, "grants").status == "ok"


def test_a_role_the_target_lacks_is_said(tmp_path, monkeypatch):
    eng, src, dst = _engine(tmp_path, monkeypatch, _same(), _same())
    src.add_role("app", [("SELECT", "data/shop/orders")])
    got = _found(eng, "grants")
    assert got.status == "diff" and "no such role on the target" \
        in got.detail


def test_a_hop_that_leaves_roles_out_is_not_held_to_their_permissions(
        tmp_path, monkeypatch):
    eng, src, dst = _engine(tmp_path, monkeypatch, _same(), _same(),
                            objects={"roles": False})
    src.add_role("app", [("SELECT", "data/shop/orders")])
    assert _found(eng, "grants").status == "skip"


# ---- how each table came out ----------------------------------------------------

def test_a_finding_says_how_each_table_it_compared_came_out(tmp_path,
                                                            monkeypatch):
    """A database's one answer about its values carries each table's own:
    the one that differs, the one that is equal, and the one that was not
    compared (the target lacks it)."""
    eng, src, dst = _engine(tmp_path, monkeypatch,
                            [_row(1, name=None), _row(2)],
                            [_row(1, name=""), _row(2)])
    for cl in (src, dst):
        cl.table("shop", "plain", COLS, ["id"], rows=_same(2))
    src.table("shop", "lonely", COLS, ["id"], rows=_same(2))
    got = _found(eng, "nullempty")
    assert got.tables == {"orders": "diff", "plain": "ok",
                          "lonely": "skip"}
    assert _found(eng, "float").tables == {"orders": "ok", "plain": "ok",
                                           "lonely": "skip"}
    assert _found(eng, "timeshift").tables["orders"] == "ok"
    assert _found(eng, "charset").tables["lonely"] == "skip"


def test_the_structure_findings_say_how_each_table_came_out(tmp_path,
                                                            monkeypatch):
    eng, src, dst = _engine(tmp_path, monkeypatch, _same(), _same(),
                            dst_cols={**COLS, "n": "int"})
    for cl in (src, dst):
        cl.table("shop", "plain", COLS, ["id"], rows=_same(2))
    src.table("shop", "lonely", COLS, ["id"], rows=_same(2))
    assert _found(eng, "columns").tables == {"orders": "diff", "plain": "ok",
                                             "lonely": "skip"}
    narrow = _found(eng, "narrowing")
    assert narrow.status == "diff"
    assert narrow.tables == {"orders": "diff", "plain": "ok",
                             "lonely": "skip"}
    assert _found(eng, "table options").tables["orders"] == "ok"


def test_a_table_that_differs_in_its_columns_is_named_in_the_finding(
        tmp_path, monkeypatch):
    eng, *_ = _engine(tmp_path, monkeypatch, _same(), _same(),
                      dst_cols={**COLS, "name": "ascii"})
    assert _found(eng, "columns").tables == {"orders": "diff"}


def test_a_finding_that_could_not_read_a_kind_says_no_table_is_equal(
        tmp_path, monkeypatch):
    eng, src, dst = _engine(tmp_path, monkeypatch, _same(), _same())
    dst.broken.add("system_schema.indexes")
    got = _found(eng, "indexes")
    assert got.status == "error" and got.tables == {}

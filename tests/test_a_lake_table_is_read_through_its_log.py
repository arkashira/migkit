"""A Delta or an Iceberg table as a Parquet source's table (`lake`): read
through its own log - the files of the current snapshot and no others -
described by its own schema, counted from its log, and never written.
Without a server: the files are on this disk.
"""
import datetime
import decimal

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.parquet import ParquetEngine

ROWS = [
    (1, "é😀", b"\x00\xff", decimal.Decimal("1.50"),
     datetime.datetime(2024, 1, 2, 3, 4, 5, 6), 0.1, True),
    (2, "", b"", None, None, None, False),
    (3, None, None, decimal.Decimal("-0.01"), datetime.datetime(1970, 1, 1),
     1e308, None),
]


def _table():
    import pyarrow as pa
    cols = list(zip(*ROWS))
    return pa.table({
        "id": pa.array(cols[0], pa.int32()),
        "v": pa.array(cols[1], pa.string()),
        "b": pa.array(cols[2], pa.binary()),
        "d": pa.array(cols[3], pa.decimal128(10, 2)),
        "t": pa.array(cols[4], pa.timestamp("us")),
        "f": pa.array(cols[5], pa.float64()),
        "ok": pa.array(cols[6], pa.bool_())})


@pytest.fixture
def lake(tmp_path):
    from deltalake import write_deltalake
    from pyiceberg.catalog.sql import SqlCatalog
    root = tmp_path / "lake"
    t = _table()
    # two writes and a delete: the directory holds a file the log removed
    write_deltalake(str(root / "app" / "dt"), t.slice(0, 2))
    write_deltalake(str(root / "app" / "dt"), t.slice(2), mode="append")
    from deltalake import DeltaTable
    DeltaTable(str(root / "app" / "dt")).delete("id = 2")
    cat = SqlCatalog("local", uri=f"sqlite:///{tmp_path}/cat.db",
                     warehouse=f"file://{tmp_path}/wh")
    cat.create_namespace("app")
    it = cat.create_table("app.it", schema=t.schema)
    it.append(t.slice(0, 2))
    it.append(t.slice(2))
    it.delete("id = 2")
    # an Iceberg table's directory, under the source's database
    import shutil
    shutil.copytree(tmp_path / "wh" / "app" / "it", root / "app" / "it")
    return root


def _eng(root, side="src"):
    ep = Endpoint(options={"path": str(root)})
    hop = Hop(name="lake", engine="parquet", source=ep, target=ep,
              databases=["app"])
    return ParquetEngine(hop)


def test_both_lakes_are_tables_described_by_their_own_schema(lake):
    eng = _eng(lake)
    assert eng.neutral_tables("src", "app") == ["dt", "it"]
    for t in ("dt", "it"):
        assert eng.neutral_columns("src", "app", t) == [
            ("id", "int64"), ("v", "string"), ("b", "binary"),
            ("d", "decimal128(10, 2)"), ("t", "timestamp"), ("f", "double"),
            ("ok", "bool")], t
        assert eng.neutral_key("src", "app", t) == []
        assert eng.lake("src", "app", t)[0] == {"dt": "delta",
                                                 "it": "iceberg"}[t]


def test_only_the_current_snapshot_is_read_and_counted(lake):
    eng = _eng(lake)
    cols = eng.neutral_columns("src", "app", "dt")
    from migkit import canon
    classes = [(n, canon.type_class("parquet", t)) for n, t in cols]
    want = [list(r) for r in ROWS if r[0] != 2]
    for t in ("dt", "it"):
        got = sorted((r for b in eng.neutral_batches("src", "app", t,
                                                     classes, 10)
                      for r in b), key=lambda r: r[0])
        assert got == want, (t, got)
        assert eng.table_facts("src", "app")[t]["rows"] == 2, t
        # the digest over the same rows as the rows themselves fold to
        assert eng.neutral_digest("src", "app", t, classes) == \
            (2, str(canon.fold_rows([c for _, c in classes], want)[1])), t
    # the removed file is still in the directory: reading the directory
    # as part files would have found the deleted row
    import pyarrow.parquet as pq
    loose = sum(pq.read_table(p).num_rows for p in
                (lake / "app" / "dt").glob("*.parquet"))
    assert loose > 2


def test_a_lake_is_never_written(lake, tmp_path):
    eng = _eng(lake)
    # on the target side a lake is no table of this engine's
    assert eng.lake("dst", "app", "dt") is None
    with pytest.raises(SystemExit, match="Delta or Iceberg"):
        eng.neutral_create("dst", "app", "dt", [("id", "integer", ())])
    with pytest.raises(SystemExit, match="read through its log"):
        eng.native_bulk("app", ["dt"], True, None)

"""Iceberg and Delta tables as sides of a pair, on this disk: an Iceberg
catalog in SQLite with its files beside it, Delta tables in a directory.

A DuckDB table goes into Iceberg, from Iceberg into Delta and from Delta
back into DuckDB, each hop checked by a digest DuckDB computes over the
table's rows - and each check is shown able to say "different" when one
value in the lake is changed. A batch is committed once: written again, the
table's last commit names it and nothing is written; a tail's batch applied
again after a stop is passed over by the table that has it, and applied by
the one that has not.
"""
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop

TABLE = """
    create table main.items (id bigint primary key, name varchar,
        price decimal(12, 2), ratio double, active boolean, raw blob,
        day date, stamp timestamp, zoned timestamptz, clock time);
    insert into main.items select g, 'item-' || g || ' |café:',
        g / 7, g / 3, g % 2 = 0, md5(g::varchar)::blob,
        date '2024-01-01' + g::int, timestamp '2024-01-01 00:00:00.123456'
        + g * interval '1 minute', timestamptz '2024-01-01 00:00:00+07'
        + g * interval '1 hour', time '01:02:03.5' + g * interval '1 second'
      from range(1, 2001) t(g);
    insert into main.items (id) values (0);"""


def _hop(tmp_path, src, dst, source_engine, target_engine, dbs, db_map,
         mapping=None):
    hop = Hop(name="lk", engine="hetero", source=src, target=dst,
              databases=dbs, db_map=db_map, mapping=mapping or {},
              options={"source_engine": source_engine,
                       "target_engine": target_engine})
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _iceberg(tmp_path):
    return Endpoint(options={"catalog": f"sqlite:///{tmp_path}/cat.db",
                             "warehouse": str(tmp_path / "wh")})


def _set_active(ice, key, value):
    import pyarrow as pa
    t = ice._table("dst", "main", "items")
    row = t.scan(row_filter=f"id = {key}").to_arrow()
    at = row.schema.get_field_index("active")
    t.overwrite(row.set_column(at, row.schema.field(at),
                               pa.array([value], pa.bool_())),
                overwrite_filter=f"id = {key}")


def _data(eng, db):
    return [(r.status, r.detail) for r in eng.check_data(db)
            if r.check == "data"]


def _copy(eng, db, table, tmp_path, name):
    from migkit.cli import _Checkpoint
    log = []
    eng.move_table(db, "", table, 500_000, _Checkpoint(tmp_path / name),
                   log.append)
    return log


def test_duckdb_into_iceberg_into_delta_and_back(tmp_path):
    import duckdb

    from migkit.engines.hetero import HeteroEngine
    src = tmp_path / "app.duckdb"
    con = duckdb.connect(str(src))
    con.execute(TABLE)
    con.close()

    into = HeteroEngine(_hop(tmp_path, Endpoint(host=str(src)),
                             _iceberg(tmp_path), "duckdb", "iceberg",
                             ["main"], {"main": "sales"}))
    _copy(into, "main", "items", tmp_path, "a.json")
    got = _data(into, "main")
    assert [s for s, _ in got] == ["ok"], got
    assert "2,001" in got[0][1], got

    # a value changed in the Iceberg table is found, and put back
    ice = into.dst_engine
    _set_active(ice, 7, True)
    got = _data(into, "main")
    assert [s for s, _ in got] == ["diff"], got
    _set_active(ice, 7, False)
    assert [s for s, _ in _data(into, "main")] == ["ok"]

    # Iceberg's key is its identifier fields; the digest was DuckDB's
    assert ice.neutral_key("dst", "main", "items") == ["id"]
    assert ice.folded_last is False

    # Delta has no time of day: the table is refused, by name, until the
    # hop leaves the column out
    lake = Endpoint(options={"url": str(tmp_path / "lake")})
    on = HeteroEngine(_hop(tmp_path, _iceberg(tmp_path), lake, "iceberg",
                           "delta", ["sales"], {"sales": "sales"}))
    with pytest.raises(SystemExit, match=r"delta has no column that holds"
                                         r" these as they are - clock"
                                         r" \(time\)"):
        _copy(on, "sales", "items", tmp_path, "b.json")
    on = HeteroEngine(_hop(tmp_path, _iceberg(tmp_path), lake, "iceberg",
                           "delta", ["sales"], {"sales": "sales"},
                           {"columns": {"items": {"drop": ["clock"]}}}))
    _copy(on, "sales", "items", tmp_path, "b.json")
    got = _data(on, "sales")
    assert [s for s, _ in got] == ["ok"], got
    delta = on.dst_engine
    assert delta.neutral_key("dst", "sales", "items") == ["id"]
    # what each lake's clock means, as a pair's deep check reads it off the
    # type it made: Iceberg's a wall clock, Delta's an instant
    from migkit import canon
    assert canon.time_meaning("iceberg", dict(ice.neutral_columns(
        "dst", "main", "items"))["stamp"]) == canon.WALL
    assert canon.time_meaning("delta", dict(delta.neutral_columns(
        "dst", "sales", "items"))["stamp"]) == canon.INSTANT
    dt = delta._table("dst", "sales", "items")
    dt.update(updates={"name": "'changed'"}, predicate="id = 11")
    got = _data(on, "sales")
    assert [s for s, _ in got] == ["diff"], got

    back = HeteroEngine(_hop(tmp_path,
                             Endpoint(options={"url": str(tmp_path / "lake")}),
                             Endpoint(host=str(tmp_path / "back.duckdb")),
                             "delta", "duckdb", ["sales"], {"sales": "main"}))
    _copy(back, "sales", "items", tmp_path, "c.json")
    got = _data(back, "sales")
    assert [s for s, _ in got] == ["ok"], got
    con = duckdb.connect(str(tmp_path / "back.duckdb"))
    assert con.execute("select name from main.items where id = 11"
                       ).fetchone() == ("changed",)
    q = ("select count(*), sum(price)::varchar, sum(ratio), count(raw),"
         " epoch(max(zoned)), min(stamp) from main.items")
    got = con.execute(q).fetchone()
    con.close()
    con = duckdb.connect(str(src), read_only=True)
    assert got == con.execute(q).fetchone()
    con.close()


COLUMNS = [("id", "integer"), ("name", "text"), ("price", "decimal")]


def _lake(kind, tmp_path):
    from migkit.engines import engine_named
    dst = (_iceberg(tmp_path) if kind == "iceberg"
           else Endpoint(options={"url": str(tmp_path / "lake")}))
    hop = Hop(name="lk", engine=kind, source=dst, target=dst,
              databases=["sales"])
    hop.report_dir = lambda db=None: tmp_path
    eng = engine_named(kind, hop)
    eng.neutral_create("dst", "sales", "orders",
                       [("id", "integer", (), {"null": False}),
                        ("name", "text", ()), ("price", "decimal", (12, 2))],
                       key=("id",))
    return eng


def _versions(eng):
    if eng.CANON_ENGINE == "iceberg":
        return len(eng._table("dst", "sales", "orders").snapshots())
    return eng._table("dst", "sales", "orders").version()


def _all(eng):
    rows, _ = eng.neutral_read("dst", "sales", "orders", COLUMNS, None, 0)
    return sorted(tuple(r) for r in rows)


@pytest.mark.parametrize("kind", ["iceberg", "delta"])
def test_a_batch_written_again_is_not_written_twice(kind, tmp_path):
    eng = _lake(kind, tmp_path)
    rows = [[1, "a", Decimal("1.50")], [2, "b", None]]
    eng.neutral_write("dst", "sales", "orders", COLUMNS, rows)
    after = _versions(eng)
    # the same batch again, as a copy stopped before its checkpoint writes
    # it: the table's last commit names it, and nothing is committed
    eng.neutral_write("dst", "sales", "orders", COLUMNS, rows)
    assert _versions(eng) == after
    # a batch that differs is merged in by the key
    eng.neutral_write("dst", "sales", "orders", COLUMNS,
                      [[2, "B", Decimal("2.00")], [3, "c", None]])
    assert _all(eng) == [(1, "a", Decimal("1.50")), (2, "B", Decimal("2.00")),
                         (3, "c", None)]


def _changes(*items):
    from migkit import canon
    out = []
    for op, key, values in items:
        out.append(canon.change(op, "orders", {"id": key}, values))
    return out


@pytest.mark.parametrize("kind", ["iceberg", "delta"])
def test_a_tail_batch_is_applied_once_per_table(kind, tmp_path):
    import json
    eng = _lake(kind, tmp_path)
    eng.neutral_write("dst", "sales", "orders", COLUMNS,
                      [[1, "a", Decimal("1.50")], [2, "b", None]])
    batch = _changes(("update", 1, {"id": 1, "name": "A"}),
                     ("delete", 2, {}),
                     ("insert", 5, {"id": 5, "name": "e",
                                    "price": Decimal("5.00")}))
    eng._batch_seen = json.dumps({"token": {"lsn": 10}, "batch": 41})
    eng.neutral_apply("dst", "sales", batch)
    # the update carried one column: the other kept what the table held
    want = [(1, "A", Decimal("1.50")), (5, "e", Decimal("5.00"))]
    assert _all(eng) == want
    held = _versions(eng)
    # the same batch at the same position, as the tail replays it after a
    # stop: this table has it, and commits nothing
    eng.neutral_apply("dst", "sales", batch)
    assert _versions(eng) == held
    assert _all(eng) == want
    # the same number at another position is another attempt, and applies
    eng._batch_seen = json.dumps({"token": {"lsn": 12}, "batch": 41})
    eng.neutral_apply("dst", "sales", _changes(
        ("update", 5, {"id": 5, "name": "E"})))
    assert _versions(eng) != held
    assert _all(eng) == [(1, "A", Decimal("1.50")),
                         (5, "E", Decimal("5.00"))]
    if kind == "delta":
        # the batch's number is the protocol's own transaction version
        dt = eng._table("dst", "sales", "orders")
        assert dt.transaction_version("migkit:lk:sales:orders") == 41


def test_the_digest_says_different_for_every_changed_class(tmp_path):
    """Each class DuckDB renders here, changed in one row, moves the
    digest: a rendering that dropped a class's difference would not."""
    eng = _lake("delta", tmp_path)
    cols = [("id", "integer"), ("name", "text"), ("price", "decimal")]
    base = [[1, "a", Decimal("1.50")], [2, "b", Decimal("2.25")]]
    eng.neutral_write("dst", "sales", "orders", cols, base)
    first = eng.neutral_digest("dst", "sales", "orders", cols)
    assert eng.folded_last is False
    from migkit import canon
    assert first == (2, str(canon.fold_rows(["integer", "text", "decimal"],
                                            base)[1]))
    for changed in ([[2, "c", Decimal("2.25")]], [[2, "b", Decimal("2.26")]],
                    [[2, "b", None]]):
        eng.neutral_write("dst", "sales", "orders", cols, changed)
        assert eng.neutral_digest("dst", "sales", "orders", cols) != first
    eng.neutral_write("dst", "sales", "orders", cols, [base[1]])
    assert eng.neutral_digest("dst", "sales", "orders", cols) == first


def test_a_time_has_no_delta_column_and_says_so():
    from migkit import canon
    with pytest.raises(ValueError):
        canon.ddl_type("delta", "time")
    assert canon.ddl_type("iceberg", "time") == "time"
    assert canon.type_class("iceberg", "decimal(12, 2)") == "decimal"
    assert canon.type_class("delta", "timestamp_ntz") == "timestamp"

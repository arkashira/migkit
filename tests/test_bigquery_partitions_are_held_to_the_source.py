"""A copied BigQuery table is held to the source's layout, partition by
partition: its method and column, its clusters, the life of its partitions
and - from `INFORMATION_SCHEMA.PARTITIONS` - the rows each partition holds
(`Engine._partitions_verdict`, as SQL Server's, Oracle's and MySQL's are).

A table's total can look right while its partitions are not: a month that
arrived empty, rows stranded in `__UNPARTITIONED__` where no range took
them, a partition the target lacks. The counts are BigQuery's storage
metadata, so nothing is billed and no digest is compared; a table under a
row filter is named, not compared by partition.

The client is a stand-in that answers as the documentation says
(`tests/bigquery_fake.py`); no project has answered these calls, so the
cell is in `capabilities.UNPROVED` (backlog P0-PARITY.all).
"""
import pytest

pytest.importorskip("google.cloud.bigquery")

from tests import bigquery_fake as fake  # noqa: E402
from tests.bigquery_fake import Field, Table  # noqa: E402

COLS = [Field("id", "INTEGER", "REQUIRED"), Field("day", "DATE")]


def _eng(monkeypatch, tmp_path, src, dst, **hop):
    eng = fake.engine(monkeypatch, report=tmp_path, **hop)
    for side, tables in (("src", src), ("dst", dst)):
        for t in tables:
            eng.clients[side].dataset("sales").add(t)
    return eng


def _events(**kw):
    kw.setdefault("time_partitioning", fake.time_partitioning("day"))
    kw.setdefault("clustering_fields", ["id"])
    return Table("events", COLS, **kw)


def _counts(eng, side, rows):
    eng.clients[side].on("information_schema.partitions", rows)


def test_partitions_that_match_are_held_equal(monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, [_events()], [_events()])
    rows = [["events", "20240101", 10], ["events", "20240102", 20],
            ["events", "__NULL__", 2]]
    _counts(eng, "src", rows)
    _counts(eng, "dst", rows)
    res = eng._deep_partitions("sales")
    assert (res.status, res.scope) == ("ok", "sales partitions")
    assert "1 partitioned tables, 3 partitions" in res.detail
    # compared by count only: BigQuery's metadata holds no digest
    assert "compared by count only" in res.detail


def test_a_partition_that_arrived_empty_is_found(monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, [_events()], [_events()])
    _counts(eng, "src", [["events", "20240101", 10],
                         ["events", "20240102", 20]])
    _counts(eng, "dst", [["events", "20240101", 10],
                         ["events", "20240102", 0]])
    res = eng._deep_partitions("sales")
    assert res.status == "diff"
    assert "events partition 20240102: empty on target" in res.detail
    assert (tmp_path / "deep-partitions.diff").exists()


def test_rows_stranded_where_no_range_takes_them_are_found(monkeypatch,
                                                           tmp_path):
    ranged = dict(time_partitioning=None,
                  range_partitioning=fake.range_partitioning("id"))
    eng = _eng(monkeypatch, tmp_path, [_events(**ranged)],
               [_events(**ranged)])
    _counts(eng, "src", [["events", "0", 5], ["events", "10", 5]])
    _counts(eng, "dst", [["events", "0", 5], ["events", "10", 5],
                         ["events", "__UNPARTITIONED__", 3]])
    res = eng._deep_partitions("sales")
    assert res.status == "diff"
    assert "partition __UNPARTITIONED__: extra on target, holding 3" \
        in res.detail
    assert "__UNPARTITIONED__" in res.fix_hint


def test_a_partition_the_target_lacks_is_found(monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, [_events()], [_events()])
    _counts(eng, "src", [["events", "20240101", 10],
                         ["events", "20240102", 20]])
    _counts(eng, "dst", [["events", "20240101", 10]])
    res = eng._deep_partitions("sales")
    assert res.status == "diff"
    assert "events partition 20240102: missing on target, the source's" \
        " holds 20 rows" in res.detail


def test_a_layout_that_differs_is_found(monkeypatch, tmp_path):
    src = _events()
    dst = _events(time_partitioning=fake.time_partitioning("day", "MONTH"),
                  clustering_fields=["day"])
    eng = _eng(monkeypatch, tmp_path, [src], [dst])
    _counts(eng, "src", [["events", "20240101", 10]])
    _counts(eng, "dst", [["events", "20240101", 10]])
    res = eng._deep_partitions("sales")
    assert res.status == "diff"
    assert "partitioned by time (day) day on the source, time (month) day" \
        " on the target" in res.detail
    assert "clustered by id on the source, day on the target" in res.detail


def test_a_partition_expiry_the_source_lacks_is_a_finding(monkeypatch,
                                                          tmp_path):
    """Rows older than the target's expiry are deleted as they are copied:
    the copy looks complete until BigQuery removes it."""
    dst = _events(time_partitioning=fake.time_partitioning(
        "day", expiration_ms=86_400_000))
    eng = _eng(monkeypatch, tmp_path, [_events()], [dst])
    _counts(eng, "src", [["events", "20240101", 10]])
    _counts(eng, "dst", [["events", "20240101", 10]])
    res = eng._deep_partitions("sales")
    assert res.status == "diff"
    assert "partitions expire after None ms on the source, 86400000 ms on" \
        " the target - rows older than that are deleted as they are copied" \
        in res.detail


def test_a_table_partitioned_on_one_side_only_is_found(monkeypatch,
                                                       tmp_path):
    plain = Table("events", COLS)
    eng = _eng(monkeypatch, tmp_path, [_events()], [plain])
    _counts(eng, "src", [["events", "20240101", 10]])
    _counts(eng, "dst", [["events", None, 10]])
    res = eng._deep_partitions("sales")
    assert res.status == "diff"
    assert "events: partitioned by time (day) day on the source, nothing on" \
        " the target" in res.detail


def test_a_table_the_target_lacks_is_the_schema_checks_but_named(
        monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, [_events()], [])
    res = eng._deep_partitions("sales")
    assert res.status == "diff"
    assert "events: missing on target (partitioned on the source)" \
        in res.detail


def test_a_table_under_a_row_filter_is_named_not_compared(monkeypatch,
                                                          tmp_path):
    eng = _eng(monkeypatch, tmp_path, [_events()], [_events()],
               mapping={"where": {"events": "id > 5"}})
    _counts(eng, "src", [["events", "20240101", 10]])
    _counts(eng, "dst", [["events", "20240101", 3]])
    res = eng._deep_partitions("sales")
    # nothing else was compared, so it is not "no partitioned tables"
    assert res.status == "warn"
    assert "not compared by partition: events (under a row filter)" \
        in res.detail


def test_no_partitioned_table_is_said(monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, [Table("t", COLS)], [Table("t", COLS)])
    assert eng._deep_partitions("sales").detail == "no partitioned tables"


def test_a_table_the_hop_leaves_out_is_not_read(monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, [_events()], [_events()],
               exclude=["events"])
    res = eng._deep_partitions("sales")
    assert res.detail == "no partitioned tables"
    assert not eng.clients["src"].asked

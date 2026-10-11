"""A Cassandra hop reads the hop's scope in every path it lists tables for
(P0-PARITY.scope): the counts, the data and the schema check, the deep
checks, the copy, a repair's plan and the changes its tail applies. Each
used to ask `Hop.excluded` by the name of a table in its own listing; they
now ask the one scope answer (`Engine.in_scope`), so a table the hop leaves
out is neither counted, compared, deep-checked nor copied, a rename in the
hop's mapping is read by the copy and the check alike, a drilldown of a
table the hop no longer takes is never acted on, and a change of an
operation the hop's `mapping.ops` does not carry is never applied.

No server: the cluster is `tests/cassandra_fake.py`, which answers only
statements the docker tests of this engine have run.
"""
from migkit.config import Endpoint, Hop
from migkit.engines.cassandra import CassandraEngine
from tests import cassandra_fake as fake

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("scope.listings", "cassandra"),
    ("scope.deep", "cassandra"),
    ("scope.copy", "cassandra"),
    ("scope.names", "cassandra"),
    ("scope.ops", "cassandra"),
    ("scope.repair", "cassandra"),
    ("verify.table", "cassandra"),
]

COLS = {"id": "int", "name": "text", "note": "text"}


def _clusters(src_rows=None, dst_rows=None, extra=()):
    src, dst = fake.Cluster(), fake.Cluster()
    for cl, rows in ((src, src_rows or []), (dst, dst_rows or [])):
        for name in ("orders", "audit_log", "events") + tuple(extra):
            cl.table("shop", name, COLS, ["id"],
                     rows=rows if name == "orders" else [])
    return src, dst


def _engine(tmp_path, monkeypatch, src, dst, **kw):
    fake.install(monkeypatch, src, dst)
    hop = Hop(name="cs", engine="cassandra", source=Endpoint(host="10.0.0.1"),
              target=Endpoint(host="10.0.0.2"), databases=["shop"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return CassandraEngine(hop)


def test_a_listing_leaves_out_what_the_hop_excludes(tmp_path, monkeypatch):
    src, dst = _clusters()
    eng = _engine(tmp_path, monkeypatch, src, dst, exclude=["audit_log"])
    for side in ("src", "dst"):
        assert eng.neutral_tables(side, "shop") == ["events", "orders"]


def test_a_listing_keeps_only_what_the_hop_includes(tmp_path, monkeypatch):
    src, dst = _clusters()
    eng = _engine(tmp_path, monkeypatch, src, dst, include=["orders"])
    assert eng.neutral_tables("src", "shop") == ["orders"]


def test_a_scylla_change_log_table_is_not_a_table_of_the_hop(
        tmp_path, monkeypatch):
    src, dst = _clusters(extra=("orders_scylla_cdc_log",))
    eng = _engine(tmp_path, monkeypatch, src, dst)
    assert "orders_scylla_cdc_log" not in eng.neutral_tables("src", "shop")


def test_the_copy_lists_only_the_tables_the_hop_takes(tmp_path, monkeypatch):
    src, dst = _clusters()
    eng = _engine(tmp_path, monkeypatch, src, dst, exclude=["audit_log"])
    assert [t for _, t in eng.list_move_tables("shop")] == ["events",
                                                            "orders"]


def test_the_counts_the_data_and_the_schema_read_only_the_taken_tables(
        tmp_path, monkeypatch):
    rows = [{"id": 1, "name": "a", "note": "x"},
            {"id": 2, "name": "b", "note": "y"}]
    src, dst = _clusters(rows, rows[:1])
    # a table the hop leaves out differs on purpose, and is on one side only
    src.table("shop", "scratch", COLS, ["id"], rows=rows)
    eng = _engine(tmp_path, monkeypatch, src, dst,
                  exclude=["audit_log", "scratch"])
    for check in (eng.check_counts, eng.check_data, eng.check_schema):
        scopes = {r.scope for r in check("shop")}
        assert not any("audit_log" in s or "scratch" in s for s in scopes), \
            scopes
    data = {r.scope: r for r in eng.check_data("shop")}
    assert data["shop.orders"].status == "diff"
    assert data["shop.events"].status == "ok"


def test_the_deep_checks_read_only_the_taken_tables(tmp_path, monkeypatch):
    src, dst = _clusters([{"id": 1, "name": "a", "note": "x"}],
                         [{"id": 1, "name": "a", "note": "x"}])
    eng = _engine(tmp_path, monkeypatch, src, dst, exclude=["audit_log"])
    scopes = [r.scope for r in eng.check_deep("shop")]
    assert any("orders" in s for s in scopes)
    assert not any("audit_log" in s for s in scopes), scopes


def test_a_finding_names_its_table_as_the_source_names_it(
        tmp_path, monkeypatch):
    src, dst = _clusters([{"id": 1, "name": "a", "note": "x"}], [])
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = {r.scope: r for r in eng.check_data("shop")}["shop.orders"]
    assert got.status == "diff" and got.table == "orders"


def test_a_renamed_table_is_copied_and_compared_under_its_new_name(
        tmp_path, monkeypatch):
    """The hop's `mapping.tables` names the target's table: the copy writes
    it there and the check lines the two up, and neither lists the old
    name as a table the target lacks."""
    rows = [{"id": 1, "name": "a", "note": "x"}]
    src, dst = fake.Cluster(), fake.Cluster()
    src.table("shop", "orders", COLS, ["id"], rows=rows)
    dst.table("shop", "orders_v2", COLS, ["id"], rows=rows)
    eng = _engine(tmp_path, monkeypatch, src, dst,
                  mapping={"tables": {"orders": "orders_v2"}})
    scopes = {r.scope: r.status for r in eng.check_data("shop")}
    assert scopes == {"shop.orders": "ok"}, scopes


def test_a_hop_that_leaves_nothing_out_reads_every_table(tmp_path,
                                                         monkeypatch):
    src, dst = _clusters()
    eng = _engine(tmp_path, monkeypatch, src, dst)
    assert eng.neutral_tables("src", "shop") == ["audit_log", "events",
                                                  "orders"]


# ---- the changes the tail applies ---------------------------------------------

def _log_row(op, key, at=0, **extra):
    """A row of a table's change log as the engine reads one."""
    import types
    return {"cdc$operation": op, "cdc$stream_id": b"s",
            "cdc$time": types.SimpleNamespace(
                time=0x01B21DD213814000 + 10 * (1_700_000_000_000_000 + at)),
            "cdc$batch_seq_no": 0, "id": key, **extra}


def _followed(tmp_path, monkeypatch, ops):
    """The target after the tail has applied an insert of 1, an update of 2
    and a delete of 3 from the source's log, under a hop that carries `ops`
    of the table."""
    src, dst = fake.Cluster(), fake.Cluster()
    mine = [{"id": 1, "name": "new", "note": "n"},
            {"id": 2, "name": "changed", "note": "n"}]
    src.table("shop", "orders", COLS, ["id"], rows=mine, at=5_000_000)
    dst.table("shop", "orders", COLS, ["id"], rows=[
        {"id": 2, "name": "old", "note": "n"},
        {"id": 3, "name": "kept", "note": "n"}], at=1_000_000)
    kw = {"mapping": {"ops": {"orders": ops}}} if ops else {}
    eng = _engine(tmp_path, monkeypatch, src, dst, **kw)
    carried = eng._follow_table("shop", "orders", [
        _log_row(CassandraEngine.INSERT, 1, 1),
        _log_row(CassandraEngine.UPDATE, 2, 2),
        _log_row(CassandraEngine.ROW_DELETE, 3, 3)])
    return carried, {r["id"]: r["name"] for r in
                     fake.rows_of(dst.keyspaces["shop"]["orders"])}


def test_a_hop_that_carries_everything_applies_every_change(
        tmp_path, monkeypatch):
    carried, got = _followed(tmp_path, monkeypatch, None)
    assert carried == 3
    assert got == {1: "new", 2: "changed"}


def test_a_hop_that_carries_inserts_alone_applies_only_those(
        tmp_path, monkeypatch):
    """The update of 2 is not carried and nor is the delete of 3: the
    target keeps its row 2 as it was and its row 3."""
    carried, got = _followed(tmp_path, monkeypatch, "insert")
    assert carried == 1
    assert got == {1: "new", 2: "old", 3: "kept"}


def test_a_hop_that_keeps_deletes_applies_the_rest(tmp_path, monkeypatch):
    carried, got = _followed(tmp_path, monkeypatch, "insert,update")
    assert carried == 2
    assert got == {1: "new", 2: "changed", 3: "kept"}


def test_a_range_or_a_partition_deleted_is_a_delete(tmp_path, monkeypatch):
    src, dst = _clusters()
    eng = _engine(tmp_path, monkeypatch, src, dst,
                  mapping={"ops": {"orders": "insert,update"}})
    rows = [_log_row(op, 1) for op in (
        CassandraEngine.PARTITION_DELETE, 5, 6, 7, 8,
        CassandraEngine.ROW_DELETE)]
    assert eng._carried("shop", "orders", rows) == []
    both = _engine(tmp_path, monkeypatch, src, dst)
    assert both._carried("shop", "orders", rows) == rows


# ---- a repair's plan -----------------------------------------------------------

def test_a_drilldown_of_a_table_the_hop_leaves_out_is_said_not_acted_on(
        tmp_path, monkeypatch):
    """A check run before `exclude: [audit_log]` was added left
    `data-audit_log.extra`; the repair must not delete the target's own
    rows there."""
    from migkit.engines.base import LEFT_OUT
    (tmp_path / "data-audit_log.extra").write_text('["7"]\n')
    (tmp_path / "data-orders.missing").write_text('["1"]\n')
    src, dst = _clusters()
    eng = _engine(tmp_path, monkeypatch, src, dst, exclude=["audit_log"])
    plan = eng.repair_plan("shop", "rows")
    assert [a.table for a in plan if a.kind == "rows"] == ["orders"]
    said = [a for a in plan if a.kind == LEFT_OUT]
    assert said and "audit_log" in said[0].note
    assert all("audit_log" not in line for a in plan if a.kind == "rows"
               for line in a.statements)

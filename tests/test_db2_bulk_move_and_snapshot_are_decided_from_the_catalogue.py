"""Db2's own load, its rollback point and the rows changed since a clean
pass (backlog R13-DB2), each chosen by `decide` from the catalogue's
rows and proved on fakes - no Db2 server exists here ("built; not run on
a real Db2 server", each module's `UNPROVED_ON`).
"""
import pytest

from migkit import decide
from tests.test_db2_takes_sequences_params_users_and_statistics_on_fakes import (
    _reports, engine)

_ = _reports


def way(**f):
    base = {"platform": "luw", "rows": 5000, "width": 80, "empty": True,
            "mapped": False, "archived": False, "same_place": True,
            "copier_rate": None, "load_start": None}
    return decide.db2_bulk_way(dict(base, **f))


def test_the_load_is_taken_only_where_it_is_safe_and_pays():
    assert way().path == "load"
    assert way(archived=True).path == "copier"
    assert "waiting for a backup" in way(archived=None).reason
    assert way(same_place=False).path == "copier"
    assert way(empty=False).path == "copier"
    assert way(rows=None).path == "copier"
    assert way(mapped=True).path == "copier"
    assert way(copier_rate=10000.0, load_start=4.0).path == "copier"
    assert way(copier_rate=100.0, load_start=4.0).path == "load"
    z = dict(platform="zos", alone=True, always=False)
    assert way(**z).path == "cross-load"
    assert way(**dict(z, alone=False)).path == "copier"
    assert way(**dict(z, always=True)).path == "copier"
    assert way(platform="i").path == "copy-file"


LUW = [("select tabname, card, avgrowsize", [("ORDERS", 5000, 80),
                                              ("NEW", -1, None)])]


def _dst(status="N", rejected=0):
    return [("logarchmeth1", None),
            ("sysibmadm.dbcfg", [("logarchmeth1", "OFF")]),
            ("generated = 'a'", []),
            ("fetch first 1 rows only", []),
            ("admin_cmd", {"cols": ["ROWS_READ", "ROWS_SKIPPED",
                                    "ROWS_LOADED", "ROWS_REJECTED"],
                           "rows": [(5000, 0, 5000 - rejected, rejected)]}),
            ("select status from syscat.tables", [(status,)])]


def _luw(**kw):
    eng = engine(src=LUW, dst=_dst(**kw))
    eng.through_pair = lambda db, t: False
    eng.neutral_columns = lambda side, db, t: [("id", "INTEGER"),
                                               ("note", "VARCHAR(10)")]
    return eng


def test_each_table_is_routed_by_its_catalogue_size():
    steps = _luw().native_bulk("app", ["orders", "new"], False, None)
    assert steps[0].startswith("orders: the server's own load")
    assert steps[1].startswith("new: the table copier")


def test_a_load_is_checked_by_its_own_counts_and_its_pending_state():
    eng = _luw(status="C")
    eng._load("app", "orders", "load", {"modified": []}, None)
    ran = [(s, a) for s, a in eng.conns["dst"].ran]
    cmd = [a[0] for s, a in ran if "admin_cmd" in s][0]
    assert cmd == ('load from (select "ID", "NOTE" from "APP"."ORDERS") of'
                   ' cursor messages on server insert into "APP"."ORDERS"'
                   ' ("ID", "NOTE")')
    assert any("set integrity" in s for s, _ in ran)
    with pytest.raises(SystemExit, match="1 skipped or rejected"):
        _luw(rejected=1)._load("app", "orders", "load", {}, None)


def test_z_os_cross_loader_counts_both_sides_after_the_utility():
    eng = engine("db2zos", src=[("count_big", [(10,)])],
                 dst=[("count_big", [(9,)])])
    eng.neutral_columns = lambda side, db, t: [("id", "INTEGER")]
    with pytest.raises(SystemExit, match="loaded 9"):
        eng._load("app", "t1", "cross-load", {}, None)
    stmt = eng.conns["dst"].calls[0][1][2]
    assert "incursor(mkc) log yes resume yes" in stmt


def test_the_rollback_point_keeps_the_counters_and_the_log_mark(tmp_path):
    eng = engine(dst=[("syscat.sequences", [("ORDER_SEQ", 41)]),
                      ("syscat.colidentattributes", [("ORDERS", "ID", 7)]),
                      ("mon_get_transaction_log", [("0000000000ABCDEF",)])])
    eng.snapshot_state("app", tmp_path)
    assert (tmp_path / "dst-db2-counters.txt").read_text() == (
        "ORDERS.ID|7\nORDER_SEQ|41\n")
    mark = (tmp_path / "dst-db2-mark.txt").read_text()
    assert mark.startswith(f"log mark|{0xABCDEF}|")
    assert eng.restore_sequences("app", tmp_path) == [
        'alter table "APP"."ORDERS" alter column "ID" restart with 7;',
        'alter sequence "APP"."ORDER_SEQ" restart with 41;']
    z = engine("db2zos", dst=[("syssequences", [])])
    z.snapshot_state("app", tmp_path)
    assert (tmp_path / "dst-db2-mark.txt").read_text().startswith(
        "together||")


def test_a_log_position_is_compared_as_a_number():
    from migkit.engines.db2 import Db2Engine
    assert Db2Engine.position_reached({"lsn": 10}, 9) is True
    assert Db2Engine.position_reached(8, 9) is False
    assert Db2Engine.position_reached(None, 9) is None


def test_the_rows_changed_are_found_by_the_way_the_source_allows():
    def path(**f):
        return decide.db2_delta_way(dict({"log": False, "stamped": 0,
                                          "tables": 2, "same": True},
                                         **f)).path
    assert path(log=True) == "log"
    assert path(stamped=1) == "stamped"
    assert path(stamped=1, same=False) == "none"
    assert path() == "none"
    assert engine().change_log() is None


def test_stamped_rows_are_compared_by_key_and_the_mark_moves_when_clean():
    rows = {"src": [(1, "a"), (2, "b")], "dst": {1: (1, "a"), 2: (2, "x")}}
    src = [("rowchangetimestamp = 'y'", [("ORDERS", "CHANGED")]),
           ("current timestamp", [("2026-10-16-10.00.00",)]),
           ("> ?", lambda s, a: rows["src"])]
    dst = [("where \"id\" = ?",
            lambda s, a: [rows["dst"][a[0]]] if a[0] in rows["dst"] else [])]
    eng = engine(src=src, dst=dst)
    eng.neutral_tables = lambda side, db: ["orders"]
    eng.neutral_key = lambda side, db, t: ["id"]
    eng.neutral_columns = lambda side, db, t: [("id", "INTEGER"),
                                               ("v", "VARCHAR(5)")]
    assert eng.delta_verify("app")[0].detail.startswith("baseline")
    got = eng.delta_verify("app")[0]
    assert got.status == "diff" and "1 different" in got.detail
    rows["dst"][2] = (2, "b")
    assert eng.delta_verify("app")[0].status == "ok"


def test_z_os_has_no_fence_and_no_change_to_verify_by():
    from migkit import capabilities
    z = engine("db2zos")
    assert z.fence_wait("app", 5) is None and z.src_lsn("app") is None
    row = capabilities.matrix()["db2zos"]
    assert [row[c] for c in ("fence", "confirm", "delta")] == [
        "not applicable"] * 3

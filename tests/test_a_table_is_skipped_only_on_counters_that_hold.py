"""A table is skipped when its modification counters are newer than the last
proof and nothing explains them away: the server has not restarted, they
were not reset, nothing was written, and the source is not being held for a
cutover (R20.11). Never because of what the server is called.

Without a server: a scripted one (`tests/mssql_stub.py`) answers the
counters and the hashes, and what is held is which tables the check reads
again and what it says about them. The two counter statements have not run
on a SQL Server; CockroachDB and MySQL have no counters read (`UNPROVED`).
"""
import json

from migkit import unchanged
from tests.mssql_stub import Server, engine

SRC_UP = "2026-10-01T08:00:00.000"


class World:
    """Two sides' counters and hashes, changed by the test between runs."""

    def __init__(self):
        self.started = SRC_UP
        self.auto_close = 0
        self.read_only = 0
        self.tables = ["dbo.orders", "dbo.lines"]
        self.counters = {
            side: {t: self.fresh() for t in self.tables}
            for side in ("src", "dst")}
        self.digest = {"src": (100, 4242), "dst": (100, 4242)}
        self.read = []

    @staticmethod
    def fresh():
        return {"updates": 5, "last": "2026-10-01T09:00:00.000", "rows": 100,
                "pages": 10, "columns": 3, "shape": 777,
                "altered": "2026-09-01T00:00:00.000", "mods": 5,
                "rebuilt": "2026-10-01T09:30:00.000"}

    def facts(self, side, sql):
        return [(self.started, self.auto_close, 0,
                 self.read_only if side == "src" else 0,
                 "2026-09-01T00:00:00.000")]

    def usage(self, side, sql):
        return [(t, c["updates"], c["last"], c["rows"], c["pages"],
                 c["columns"], c["shape"], c["altered"], c["mods"],
                 c["rebuilt"]) for t, c in self.counters[side].items()]

    def hashed(self, side, sql):
        self.read.append(side)
        return [self.digest[side]]

    def run(self, monkeypatch, tmp_path, with_counts=False, table=None,
            **kw):
        server = (Server()
                  .on("sqlserver_start_time", self.facts)
                  .on("dm_db_index_usage_stats", lambda s, q: self.usage(s, q))
                  .on("rows_hashed", self.hashed)
                  .on("order by 1", [(t,) for t in self.tables]))
        eng = engine(monkeypatch, tmp_path, server, **kw)
        # where the sums differ the rows differ by three, and stay so
        eng._drilldown = lambda db, t: (0, 0, 3)
        eng._resolve_inflight = lambda db, bad, stream=None: (bad, [], [])
        eng._row_json = lambda db, t: ("x", [])
        eng._health = lambda side, db=None: None
        lines, before = [], len(self.read)
        got = eng.check_data("appdb", table=table, stream=lines.append,
                             with_counts=with_counts)
        data = [r for r in got if r.check == "data"][0]
        return data, lines, len(self.read) - before


def _proved(world, monkeypatch, tmp_path):
    first = world.run(monkeypatch, tmp_path)
    assert first[0].status == "ok"
    return first


def test_a_quiet_table_is_read_once_and_then_skipped_with_its_evidence(
        monkeypatch, tmp_path):
    world = World()
    data, lines, reads = world.run(monkeypatch, tmp_path)
    assert data.status == "ok" and reads == 4
    data, lines, reads = world.run(monkeypatch, tmp_path)
    assert reads == 0, "nothing moved: no row may be read"
    assert data.status == "ok"
    assert data.tables == {"dbo.orders": "ok", "dbo.lines": "ok"}
    skipped = [ln for ln in lines if ln.startswith("dbo.orders:")]
    assert len(skipped) == 1
    assert "unchanged since it was last proved equal" in skipped[0]
    assert f"server up since {SRC_UP}" in skipped[0]
    assert "5 writes counted" in skipped[0]
    assert "2 of 2 tables unchanged" in data.detail


def test_a_restart_since_the_proof_reads_every_table_again(
        monkeypatch, tmp_path):
    world = World()
    _proved(world, monkeypatch, tmp_path)
    world.started = "2026-10-02T03:00:00.000"
    data, lines, reads = world.run(monkeypatch, tmp_path)
    assert reads == 4
    assert any("the server restarted" in ln for ln in lines), lines
    assert "unchanged" not in data.detail


def test_a_write_after_the_proof_reads_that_table_and_only_it(
        monkeypatch, tmp_path):
    world = World()
    _proved(world, monkeypatch, tmp_path)
    c = world.counters["src"]["dbo.orders"]
    c["updates"] += 1
    c["last"] = "2026-10-02T10:00:00.000"
    data, lines, reads = world.run(monkeypatch, tmp_path)
    assert reads == 2, "orders is read on both sides, lines is skipped"
    assert any(ln.startswith("# dbo.orders read again")
               and "the table was written to" in ln for ln in lines), lines
    assert any(ln.startswith("dbo.lines: unchanged") for ln in lines)
    assert "1 of 2 tables unchanged" in data.detail


def test_a_write_on_the_target_is_not_hidden_by_a_quiet_source(
        monkeypatch, tmp_path):
    world = World()
    _proved(world, monkeypatch, tmp_path)
    world.counters["dst"]["dbo.lines"]["updates"] += 3
    world.counters["dst"]["dbo.lines"]["last"] = "2026-10-02T10:00:00.000"
    _, lines, reads = world.run(monkeypatch, tmp_path)
    assert reads == 2
    assert any("the target moved since the proof" in ln for ln in lines)


def test_an_emptied_table_is_read_though_no_write_was_counted(
        monkeypatch, tmp_path):
    """TRUNCATE is not counted as an update; the rows and pages are in the
    marker for it."""
    world = World()
    _proved(world, monkeypatch, tmp_path)
    c = world.counters["dst"]["dbo.orders"]
    c["rows"], c["pages"] = 0, 2
    world.digest["dst"] = (0, 0)
    data, lines, reads = world.run(monkeypatch, tmp_path)
    assert data.status == "diff", "the emptied table is found, not skipped"
    assert any("rows were removed" in ln for ln in lines)


def test_counters_that_went_down_are_a_reset_not_quiet(
        monkeypatch, tmp_path):
    world = World()
    _proved(world, monkeypatch, tmp_path)
    world.counters["src"]["dbo.orders"]["updates"] = 0
    _, lines, reads = world.run(monkeypatch, tmp_path)
    assert reads == 2
    assert any("the usage counters were cleared" in ln for ln in lines)


def test_a_table_with_no_usage_row_has_no_marker_and_is_never_skipped(
        monkeypatch, tmp_path):
    """No counter is not zero writes."""
    world = World()
    world.counters["src"]["dbo.orders"]["updates"] = None
    for _ in range(2):
        data, lines, reads = world.run(monkeypatch, tmp_path)
        assert data.status == "ok"
        assert reads >= 2
        assert not any(ln.startswith("dbo.orders: unchanged")
                       for ln in lines)
    kept = json.loads((tmp_path / "proof.json").read_text())
    assert "dbo.orders" not in kept and "dbo.lines" in kept


def test_a_source_held_read_only_is_read_whole(monkeypatch, tmp_path):
    """The proof that goes into a cutover never skips."""
    world = World()
    _proved(world, monkeypatch, tmp_path)
    world.read_only = 1
    data, lines, reads = world.run(monkeypatch, tmp_path)
    assert reads == 4
    assert any("held read-only" in ln for ln in lines), lines
    assert "unchanged" not in data.detail


def test_a_database_that_closes_when_idle_is_not_believed(
        monkeypatch, tmp_path):
    world = World()
    _proved(world, monkeypatch, tmp_path)
    world.auto_close = 1
    _, lines, reads = world.run(monkeypatch, tmp_path)
    assert reads == 4
    assert any("closes when idle" in ln for ln in lines)


def test_a_server_that_will_not_say_its_counters_is_read_whole(
        monkeypatch, tmp_path):
    world = World()
    _proved(world, monkeypatch, tmp_path)

    def refused(side, sql):
        raise RuntimeError("no VIEW SERVER STATE")
    world.usage = refused
    data, lines, reads = world.run(monkeypatch, tmp_path)
    assert reads == 4 and data.status == "ok"
    assert any("did not give its counters" in ln for ln in lines)


def test_counted_rows_and_a_named_table_are_never_skipped(
        monkeypatch, tmp_path):
    world = World()
    _proved(world, monkeypatch, tmp_path)
    _, _, reads = world.run(monkeypatch, tmp_path, with_counts=True)
    assert reads == 4
    _, _, reads = world.run(monkeypatch, tmp_path, table="dbo.orders")
    assert reads == 2


def test_a_table_that_came_out_different_keeps_no_proof(
        monkeypatch, tmp_path):
    world = World()
    _proved(world, monkeypatch, tmp_path)
    world.digest["dst"] = (100, 9999)
    world.counters["src"]["dbo.orders"]["updates"] += 1
    data, _, _ = world.run(monkeypatch, tmp_path)
    assert data.status == "diff"
    kept = json.loads((tmp_path / "proof.json").read_text())
    assert "dbo.orders" not in kept
    # the counters stay put: a proof of the wrong thing would have skipped it
    _, lines, reads = world.run(monkeypatch, tmp_path)
    assert not any(ln.startswith("dbo.orders: unchanged") for ln in lines)
    assert reads >= 2


def test_a_changed_scope_is_not_covered_by_an_old_proof(
        monkeypatch, tmp_path):
    world = World()
    _proved(world, monkeypatch, tmp_path)
    world.run(monkeypatch, tmp_path)
    _, lines, reads = world.run(monkeypatch, tmp_path,
                                exclude=["appdb.dbo.nothing"])
    assert reads == 4
    assert any("the hop's scope changed" in ln for ln in lines)


# --- the registry ---------------------------------------------------------

def test_no_brand_list_decides_what_is_trusted():
    assert not hasattr(unchanged, "TRUSTED_BRANDS")
    assert set(unchanged.COUNTERS) >= {"postgres", "mssql"}


def test_kinds_without_counters_read_are_declared_unproved_and_never_skip():
    assert {"mysql", "cockroachdb"} <= set(unchanged.UNPROVED)
    for kind in ("mysql", "cockroachdb", "citus", "no-such-kind"):
        got = unchanged.judge(kind, {"src": "A", "dst": "B"}, "A", "B")
        assert not got.skip and got.why, kind
    assert "empty" in unchanged.UNPROVED["cockroachdb"]


def test_the_verdict_says_why_for_every_refusal():
    pg = unchanged.COUNTERS["postgres"]
    mark = "1|2|3|16384|8192|3:abc"
    stored = {"src": mark, "dst": mark}
    assert unchanged.judge("postgres", stored, mark, mark).skip
    assert "no earlier proof" in unchanged.judge(
        "postgres", None, mark, mark).why
    assert "cannot be read" in unchanged.judge(
        "postgres", stored, None, mark).why
    moved = unchanged.judge("postgres", stored, "1|9|3|16384|8192|3:abc",
                            mark)
    assert not moved.skip and "rows were updated (2 -> 9)" in moved.why
    reset = unchanged.judge("postgres", stored, "0|0|0|16384|8192|3:abc",
                            mark)
    assert "the statistics were reset" in reset.why
    assert pg.refusal({"server_version": "13.0.0"})
    assert not pg.refusal({"server_version": "16.4"})


def test_a_server_that_keeps_no_file_identity_never_skips():
    """relfilenode 0 on every table is the same string forever."""
    dead = "-1|-1|-1|0|0|3:abc"
    got = unchanged.judge("postgres", {"src": dead, "dst": dead}, dead, dead)
    assert not got.skip and "no file identity" in got.why


def test_sql_server_marker_pieces_are_named_and_explained():
    ms = unchanged.COUNTERS["mssql"]
    assert len(ms.pieces) == 12
    base = ["S1", "C1", 5, "T1", 100, 10, 3, 777, "A1", 5, "R1", "scope"]
    mark = unchanged.marker(base)
    assert ms.parts(mark)["started"] == "S1"
    moved = unchanged.marker(["S2"] + base[1:])
    assert ms.explain(mark, moved) == [
        "the server restarted, so its counters began again (S1 -> S2)"]
    assert unchanged.marker(base[:2] + [None] + base[3:]) is None
    for facts, why in (
            ({"started": "S1", "auto_close": 1, "state": 0,
              "read_only": 0}, "closes when idle"),
            ({"started": "S1", "auto_close": 0, "state": 6,
              "read_only": 0}, "not online"),
            ({"started": "NULL", "auto_close": 0, "state": 0,
              "read_only": 0}, "does not say when it started")):
        assert why in ms.refusal(facts)
    ok = {"started": "S1", "auto_close": 0, "state": 0, "read_only": 1}
    assert "read-only" in ms.refusal(ok, "src")
    assert ms.refusal(ok, "dst") == ""


def test_the_brand_route_is_the_registry():
    from migkit import variants as v
    pg = v.identify("postgres", {"version": "PostgreSQL 16.15 on x86_64",
                                 "server_version": "16.15"})
    crdb = v.identify("postgres", {"version": "CockroachDB CCL v23.2.5",
                                   "server_version": "13.0.0"})
    ms = v.identify("mssql", {"banner": "Microsoft SQL Server 2022"})
    assert unchanged.usable_brand(pg)
    assert unchanged.usable_brand(ms)
    assert not unchanged.usable_brand(crdb)
    assert not unchanged.usable_brand(v.identify("postgres", {}))
    assert not unchanged.usable_brand(None)


def _held(world, monkeypatch, tmp_path, **eng_attrs):
    """A run whose engine is set as the cutover sets it, all else as the
    proved run left it."""
    from migkit.engines.mssql import MSSQLEngine
    for k, v in eng_attrs.items():
        monkeypatch.setattr(MSSQLEngine, k, v)
    return world.run(monkeypatch, tmp_path)


def test_the_cutovers_own_proof_reads_every_table(monkeypatch, tmp_path):
    """Nothing moved, so the counters would pass both tables over; the
    proof that goes into the flip never does."""
    from migkit.engines.base import Engine
    assert Engine.cutover_proof is False
    world = World()
    _proved(world, monkeypatch, tmp_path)
    data, lines, reads = _held(world, monkeypatch, tmp_path,
                               cutover_proof=True)
    assert reads == 4
    assert "unchanged" not in data.detail
    assert any(ln.startswith("# dbo.orders read again")
               and "goes into a cutover" in ln for ln in lines), lines


def test_a_source_held_for_a_cutover_reads_every_table(
        monkeypatch, tmp_path):
    from migkit import freeze
    world = World()
    _proved(world, monkeypatch, tmp_path)
    monkeypatch.setattr(freeze, "source_frozen", lambda hop, dbs: True)
    data, lines, reads = world.run(monkeypatch, tmp_path)
    assert reads == 4
    assert "unchanged" not in data.detail
    assert any("held for a cutover" in ln for ln in lines), lines


def test_a_freeze_record_that_cannot_be_read_reads_every_table(
        monkeypatch, tmp_path):
    """A doubt reads."""
    from migkit import freeze
    world = World()
    _proved(world, monkeypatch, tmp_path)

    def broken(hop, dbs):
        raise OSError("state file truncated")
    monkeypatch.setattr(freeze, "source_frozen", broken)
    data, lines, reads = world.run(monkeypatch, tmp_path)
    assert reads == 4
    assert "unchanged" not in data.detail
    assert any("could not be read (state file truncated)" in ln
               for ln in lines), lines
    # the doubt gone, the quiet tables are passed over again
    monkeypatch.setattr(freeze, "source_frozen", lambda hop, dbs: False)
    _, _, reads = world.run(monkeypatch, tmp_path)
    assert reads == 0

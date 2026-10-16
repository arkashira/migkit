"""Every probe of a run is kept with when it was taken and how fresh it is,
two schemas are compared by one comparer over one neutral shape, and the
cutover starts only on a lag, a verify verdict and probes that all hold
(backlog AUD-H)."""
import json
from types import SimpleNamespace


from migkit import cutover, diagnostics, schemaformats, verifiers
from migkit.catalog import Catalog, Column, ForeignKey, Index
from migkit.util import Probes


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def _acc():
    c = Clock()
    return diagnostics.ProbeAccumulator(c), c


def test_a_fresh_probe_is_not_measured_again_and_a_stale_one_is():
    acc, clock = _acc()
    calls = []

    def measure():
        calls.append(1)
        return len(calls)
    assert acc.read("grants", "app", measure, max_age=10) == 1
    clock.now += 5
    assert acc.read("grants", "app", measure, max_age=10) == 1
    assert len(calls) == 1
    clock.now += 6
    assert acc.read("grants", "app", measure, max_age=10) == 2
    assert len(calls) == 2
    assert acc.get("grants", "app").reused == 1


def test_a_failed_measure_is_never_answered_by_the_older_value():
    acc, clock = _acc()
    acc.record("version", "src", "16.1")
    clock.now += 100

    def broken():
        raise TimeoutError("server did not answer")
    assert acc.read("version", "src", broken, max_age=10) is None
    got = acc.get("version", "src")
    assert got.ok is False and "did not answer" in got.why
    why = acc.stale("version", "src", max_age=10_000)
    assert why.startswith("could not be read")
    assert acc.refusals([("version", "src")], 10_000) == [
        "version of src: could not be read (server did not answer)"]


def test_a_witness_that_moved_ends_a_reading_whatever_its_age():
    acc, _ = _acc()
    acc.record("shape", "db", {"t": 1}, witness="abc")
    assert acc.stale("shape", "db", max_age=1e9, witness="abc") == ""
    assert "changed" in acc.stale("shape", "db", max_age=1e9, witness="abd")


def test_a_probe_seen_to_change_is_held_to_half_its_pace():
    acc, clock = _acc()
    acc.record("rate", "src", 10, paced=True)
    clock.now += 8
    acc.record("rate", "src", 20, paced=True)
    got = acc.get("rate", "src")
    assert got.moves == [8.0]
    assert acc.allowed_age(got, max_age=60) == 4.0
    clock.now += 5
    assert "5.0s old" in acc.stale("rate", "src", max_age=60)
    # a consumer who can live with less is not given more
    assert acc.allowed_age(got, max_age=2) == 2


def test_a_never_measured_probe_is_a_refusal_naming_it():
    acc, _ = _acc()
    assert acc.refusals([("lag", "main")], 30) == [
        "lag of main: never measured"]
    assert acc.refusals([], 30) == []


def test_the_rows_say_what_was_measured_and_never_what_it_held():
    acc, clock = _acc()
    acc.record("grants", "app", ["select on secret_table"], took=0.25)
    clock.now += 3
    rows = acc.rows(max_age=10)
    assert rows == [{"kind": "grants", "scope": "app", "ok": True, "why": "",
                     "taken": 1000.0, "age": 3.0, "took": 0.25, "reads": 1,
                     "reused": 0, "fresh": True}]
    assert "secret_table" not in json.dumps(rows)
    clock.now += 20
    assert "stale" in acc.said(max_age=10)[0]


def test_a_deep_checks_unread_part_becomes_an_unusable_reading():
    acc, _ = _acc()
    seen = Probes("columns", ["a.x", "a.y"])
    seen.done("a.x")
    seen.failed("a.y", TimeoutError("timeout"))
    got = acc.absorb("columns", "a", seen)
    assert got.ok is False and "a.y" in got.why
    assert acc.stale("columns", "a", 10).startswith("could not be read")
    whole = Probes("columns", ["a.x"])
    whole.done("a.x")
    assert acc.absorb("columns", "b", whole).ok is True


def test_the_accumulator_is_one_per_hop_and_saved_beside_the_reports(tmp_path):
    hop = SimpleNamespace(name="w12g-a")
    try:
        assert diagnostics.accumulator(hop) is diagnostics.accumulator(hop)
        diagnostics.accumulator(hop).record("lag", "main", (1, True, 0, ""))
        path = tmp_path / diagnostics.PROBES_FILE
        diagnostics.accumulator(hop).save(path, 30)
        hop.report_dir = lambda: tmp_path
        kept = diagnostics.kept(hop)
        assert [(r["kind"], r["scope"], r["ok"]) for r in kept] == [
            ("lag", "main", True)]
    finally:
        diagnostics.forget(hop)
    assert diagnostics.accumulator(hop).get("lag", "main") is None
    diagnostics.forget(hop)


# --- the neutral comparer ---------------------------------------------------

def _cat(*tables, keys=None, indexes=None, fks=None):
    cat = Catalog()
    for name, cols in tables:
        cat.tables.append(name)
        cat.columns[name] = cols
    cat.keys.update(keys or {})
    cat.indexes.update(indexes or {})
    cat.fks.update(fks or {})
    return cat


def _kinds(got):
    return sorted((d.kind, d.level) for d in got.differences)


ORDERS_PG = [Column("id", "integer", nullable=False),
             Column("name", "varchar(50)", nullable=True),
             Column("price", "numeric(10,2)", default="0")]
ORDERS_MY = [Column("ID", "int", nullable=False),
             Column("NAME", "varchar(50)", nullable=True),
             Column("PRICE", "decimal(10,2)", default="0")]


def test_two_engines_spelling_the_same_shape_differently_are_the_same():
    src = schemaformats.neutral("postgres", _cat(
        ("orders", ORDERS_PG), keys={"orders": ("id",)},
        indexes={"orders": [Index("o_name", ("name",), False)]}))
    dst = schemaformats.neutral("mysql", _cat(
        ("ORDERS", ORDERS_MY), keys={"ORDERS": ("ID",)},
        indexes={"ORDERS": [Index("x", ("NAME",), False)]}))
    got = schemaformats.compare(src, dst, same_engine=False)
    assert got.compared == 1
    assert got.diffs() == []
    # what the two cannot be held to is said, not passed
    assert {(a) for _, a, _ in got.unread} == {"collation",
                                               "checks and triggers"}


def test_a_hetero_pair_reports_what_the_target_would_break():
    narrow = [Column("id", "int", nullable=True),
              Column("name", "varchar(20)", nullable=True),
              Column("price", "decimal(10,2)")]
    src = schemaformats.neutral("postgres", _cat(
        ("orders", ORDERS_PG), keys={"orders": ("id",)}))
    dst = schemaformats.neutral("mysql", _cat(("orders", narrow)))
    got = schemaformats.compare(src, dst, same_engine=False)
    kinds = _kinds(got)
    assert ("capacity", "diff") in kinds     # varchar(50) -> varchar(20)
    assert ("null", "diff") in kinds         # NOT NULL id takes NULL
    assert ("default", "diff") in kinds      # price default 0 lost
    assert ("key", "diff") in kinds          # the primary key is gone
    text = " ".join(d.detail for d in got.diffs())
    assert "name is varchar(50) on the source and the narrower" in text


def test_a_wider_target_is_fine_and_a_class_change_is_not():
    src = schemaformats.neutral("mysql", _cat(("t", [
        Column("a", "varchar(10)"), Column("b", "int")])))
    wider = schemaformats.neutral("postgres", _cat(("t", [
        Column("a", "varchar(30)"), Column("b", "text")])))
    got = schemaformats.compare(src, wider, same_engine=False)
    assert [d.kind for d in got.diffs()] == ["type"]
    assert "b is int (integer)" in got.diffs()[0].detail


def test_a_type_with_no_neutral_class_is_unread_not_equal():
    src = schemaformats.neutral("postgres", _cat(("t", [
        Column("a", "hstore_of_something")])))
    dst = schemaformats.neutral("mysql", _cat(("t", [
        Column("a", "varchar(10)")])))
    got = schemaformats.compare(src, dst, same_engine=False)
    assert got.diffs() == []
    assert any("t.a type" == a for _, a, _ in got.unread)
    res = schemaformats.results(got, "main")
    assert [r.status for r in res] == ["warn"]
    assert "t.a type" in res[0].detail


def test_one_engines_two_sides_are_compared_as_declared():
    a = _cat(("t", [Column("n", "varchar(50)", collation="utf8_bin",
                           extra="auto_increment")]),
             indexes={"t": [Index("i", ("n",), True, "where n > 1")]},
             fks={"t": [ForeignKey("f", ("n",), "u", ("id",))]})
    b = _cat(("t", [Column("n", "varchar(100)", collation="utf8_general",
                           extra="")]),
             indexes={"t": [Index("i", ("n",), True, "")]})
    got = schemaformats.compare(schemaformats.neutral("mysql", a),
                                schemaformats.neutral("mysql", b), True)
    kinds = {d.kind for d in got.diffs()}
    assert kinds == {"type", "collation", "extra", "index", "foreign key"}
    assert got.unread == []


def test_tables_on_one_side_only_and_a_renamed_column_are_named():
    src = schemaformats.neutral("postgres", _cat(
        ("a", [Column("x", "integer")]), ("only_src", [Column("c", "text")])))
    dst = schemaformats.neutral("mysql", _cat(
        ("a", [Column("y", "int")]), ("only_dst", [Column("c", "text")])))
    got = schemaformats.compare(src, dst, False, columns={"a": {"x": "y"}})
    assert got.diffs() and {d.table for d in got.diffs()} == {"only_src"}
    assert [(d.table, d.level) for d in got.differences
            if d.kind == "table"] == [("only_src", "diff"),
                                      ("only_dst", "note")]
    plain = schemaformats.compare(src, dst, False)
    assert {d.kind for d in plain.diffs()} == {"table", "column"}


def test_results_name_each_differing_table_and_say_ok_only_over_all():
    src = schemaformats.neutral("mysql", _cat(("t", [Column("a", "int")])))
    same = schemaformats.compare(src, src, True)
    ok = schemaformats.results(same, "main")
    assert [(r.status, r.scope) for r in ok] == [("ok", "main")]
    bad = schemaformats.neutral("mysql", _cat(("t", [Column("a", "text")])))
    res = schemaformats.results(schemaformats.compare(src, bad, True), "main")
    assert [(r.status, r.scope) for r in res] == [("diff", "main.t")]


def test_the_engines_with_their_own_compare_are_listed():
    own = verifiers.engines_with_own_compare()
    assert own == sorted(own) and "mysql" in own


# --- the cutover gate -------------------------------------------------------

class Hop:
    def __init__(self, tmp, name):
        self.name, self.dir = name, tmp

    def report_dir(self, db=""):
        return self.dir


def _verdict(hop, **kw):
    (hop.dir / "verdict.json").write_text(json.dumps(
        {"status": "same", "why": "", **kw}))


def test_the_gate_names_the_verify_verdict_that_stops_it(tmp_path):
    hop = Hop(tmp_path, "w12g-verdict")
    try:
        rows = cutover._verify_rows(hop, None, "fail", 30)
        assert rows[0][0] == "fail" and "none could be read" in rows[0][1]
        _verdict(hop, status="different", why="3 tables differ")
        rows = cutover._verify_rows(hop, None, "fail", 30)
        assert rows == [("fail", "verify verdict: different - 3 tables"
                                 " differ")]
        _verdict(hop, stale=True)
        assert "marked stale" in cutover._verify_rows(
            hop, None, "fail", 30)[0][1]
        _verdict(hop)
        assert cutover._verify_rows(hop, None, "fail", 30) == [
            ("ok", "verify verdict: the two are the same")]
    finally:
        diagnostics.forget(hop)


def test_the_cutover_does_not_start_on_a_lag_a_stale_verdict_or_a_stale_probe(
        tmp_path, monkeypatch):
    from migkit import freeze
    hop = Hop(tmp_path, "w12g-gate")
    leg = SimpleNamespace(fence=object())
    conf = dict(cutover.DEFAULTS)
    reached = {"v": True}
    monkeypatch.setattr(cutover, "_probes", lambda h, l, dbs, c: {
        d: ("0/16", reached["v"], 0.1, "") for d in dbs})
    monkeypatch.setattr(freeze, "open_work", lambda *a, **k: [])
    try:
        _verdict(hop)
        assert cutover.recheck(hop, None, ["main"], leg, conf) == []
        # the stream falls behind: the exact fact is named
        diagnostics.accumulator(hop).record("lag", "main", ("0/16", False,
                                                            1.0, ""))
        stopped = cutover.recheck(hop, None, ["main"], leg, conf)
        assert stopped == ["main: the stream is more than 1s behind the"
                           " source (start_below)"]
        # a verdict that is not the same stops it as well
        _verdict(hop, status="incomplete", why="a check was skipped")
        reached["v"] = True
        diagnostics.accumulator(hop).record("lag", "main", ("0/16", True,
                                                            0.1, ""))
        stopped = cutover.recheck(hop, None, ["main"], leg, conf)
        assert stopped == ["verify verdict: incomplete - a check was"
                           " skipped"]
        # a prepared transaction, read afresh after the window
        monkeypatch.setattr(freeze, "open_work",
                            lambda *a, **k: ["prepared transaction t1"])
        _verdict(hop)
        acc = diagnostics.accumulator(hop)
        acc.record("open-work", "main:src", [])
        acc.get("open-work", "main:src").taken -= 31
        stopped = cutover.recheck(hop, None, ["main"], leg, conf)
        assert any("prepared transaction t1" in w for w in stopped)
        # a probe that cannot be read is a stop, never an empty answer
        def broken(*a, **k):
            raise OSError("connection refused")
        monkeypatch.setattr(freeze, "open_work", broken)
        acc.get("open-work", "main:dst").taken -= 31
        acc.get("open-work", "main:src").taken -= 31
        stopped = cutover.recheck(hop, None, ["main"], leg, conf)
        assert any("open transactions could not be read" in w
                   and "connection refused" in w for w in stopped)
        assert any(w.startswith("probe open-work of main:src: could not be"
                                " read") for w in stopped)
    finally:
        diagnostics.forget(hop)


def test_a_marked_drain_is_not_held_to_a_position(tmp_path, monkeypatch):
    from migkit import freeze
    hop = Hop(tmp_path, "w12g-mark")
    monkeypatch.setattr(freeze, "open_work", lambda *a, **k: [])
    _verdict(hop)
    try:
        assert cutover.recheck(hop, None, ["main"], SimpleNamespace(),
                               dict(cutover.DEFAULTS), marked=True) == []
    finally:
        diagnostics.forget(hop)


def test_the_window_a_probe_may_rest_on_is_the_outage_the_cutover_may_take():
    assert cutover.fresh_window({"budget": 12}) == 12.0
    assert cutover.fresh_window(None) == float(cutover.DEFAULTS["budget"])

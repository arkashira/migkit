"""Every answer, position and checkpoint is good only for the scope it was
taken under (backlog P0-SCOPE.6c).

A hop's scope - what it takes, the rows it filters, the columns it
chooses, the names it gives - can change between two runs. Measured at
ee1032d before this: a delta position the PostgreSQL slot had advanced
past an excluded table's changes was gone on from after `include` took
the table in, and the cutover's last check compared only the delta; a
`check --only` merged the last run's answers taken under another scope; a
straight copy's stopped dump was loaded again under another row filter;
a seed's load wrote every table of its manifest, the ones the hop had
since left out too; the report and the evidence said today's scope over
yesterday's answers; `watch` summed every table of the database and a
plan timed every table. Each keeps the scope it was taken under now
(`Hop.scope_fingerprint`), and one taken under another is never reused as
if it were this one's. Pure: no server is asked anything.
"""
import json
from types import SimpleNamespace

import pytest

import migkit.config as cfg
from migkit import cli, cutover, digests, export, planner, report, ship, \
    verdict
from migkit.config import Endpoint, Hop
from migkit.engines.base import Engine, Result


@pytest.fixture(autouse=True)
def reports(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return tmp_path / "reports"


def _hop(engine="postgres", **kw):
    return Hop(name="h", engine=engine, source=Endpoint(host="a", port=1,
                                                        user="u"),
               target=Endpoint(host="b", port=2, user="u"), **kw)


class _Delta(Engine):
    """An engine whose delta is a record of what was asked of it."""
    CANON_ENGINE = "postgres"

    def __init__(self, hop):
        super().__init__(hop)
        self.asked = []

    def delta_verify(self, db, limit=20000, log=None):
        self.asked.append("verify")
        return [Result("delta", db, "ok", "0 changed rows")]

    def delta_teardown(self, db):
        self.asked.append("teardown")


# ---- the fingerprint and the scope it stands for ---------------------------

def test_the_fingerprint_is_what_it_was_and_its_record_says_the_scope():
    """The digest every kept fingerprint was taken with stays the same,
    and the scope it digests is kept whole where a reader must know what
    that scope took, not only that it was another."""
    h = _hop(databases=["appdb"], include=["appdb.public.orders"],
             mapping={"where": {"appdb.public.orders": "region = 'TH'"}})
    assert h.scope_fingerprint() == "9d2d7e39f4331586"
    then = h.under({"scope": {"databases": ["appdb"], "db_map": {},
                              "include": [], "mapping": {},
                              "exclude": ["appdb.public.audit"]}})
    assert then.excluded("appdb", "public", "audit")
    assert not then.excluded("appdb", "public", "lines")
    assert h.excluded("appdb", "public", "lines")
    assert then.scope_fingerprint() != h.scope_fingerprint()
    assert h.under(h.scope_record()).scope_fingerprint() == \
        h.scope_fingerprint()
    # a record that kept no scope says nothing of one
    assert h.under({"at": "2026-10-03"}) is None


# ---- the delta's position ---------------------------------------------------

def test_a_delta_position_taken_under_another_scope_is_taken_again(reports):
    """The position is dropped and taken afresh, said as an answer to read
    - the delta of a new position sees only what changes from it."""
    h = _hop(include=["appdb.public.orders"])
    eng = _Delta(h)
    assert [r.status for r in eng.delta_in_scope("appdb")] == ["ok"]
    kept = json.loads((h.report_dir("appdb") / Engine.DELTA_SCOPE)
                      .read_text())
    assert kept["scope_fingerprint"] == h.scope_fingerprint()
    assert eng.delta_covers("appdb")
    (h.report_dir("appdb") / "delta-pos.json").write_text("{}")
    h.include = ["appdb.public.orders", "appdb.public.lines"]
    assert not eng.delta_covers("appdb")
    got = eng.delta_in_scope("appdb")
    assert eng.asked == ["verify", "teardown", "verify"]
    assert not (h.report_dir("appdb") / "delta-pos.json").exists()
    assert [r.status for r in got] == ["ok", "warn"], got
    assert "scope changed" in got[-1].detail
    assert verdict.decide([r.__dict__ for r in got], h).status != "same"


def test_after_a_scope_change_the_cutover_compares_every_table(reports):
    """Until a whole check under the new scope finds the two the same, a
    delta from the new position is not the cutover's last check: a table
    the scope took in was never compared whole."""
    h = _hop(include=["appdb.public.orders"])
    eng = _Delta(h)
    eng.delta_in_scope("appdb")
    (h.report_dir("appdb") / "pair-delta-token.json").write_text("{}")
    assert cutover._has_delta(h, eng, "appdb")
    h.include = ["appdb.public.*"]
    # a position of the scope before
    assert not cutover._has_delta(h, eng, "appdb")
    eng.delta_in_scope("appdb")
    (h.report_dir("appdb") / "pair-delta-token.json").write_text("{}")
    # taken again, and no whole check since
    assert not cutover._has_delta(h, eng, "appdb")
    got = eng.delta_in_scope("appdb")
    assert [r.status for r in got] == ["ok", "warn"], got
    # a whole check under another scope does not stand for this one
    (h.report_dir() / "verdict.json").write_text(json.dumps(
        {"status": "same", "scope_fingerprint": "another"}))
    assert not cutover._has_delta(h, eng, "appdb")
    (h.report_dir() / "verdict.json").write_text(json.dumps(
        {"status": "same", "scope_fingerprint": h.scope_fingerprint()}))
    assert cutover._has_delta(h, eng, "appdb")
    assert [r.status for r in eng.delta_in_scope("appdb")] == ["ok"]


def test_a_delta_position_from_before_scopes_were_kept_goes_on(reports):
    """A position with nothing beside it predates the fingerprints and is
    gone on from as it always was - and from now on carries one."""
    h = _hop()
    eng = _Delta(h)
    (h.report_dir("appdb") / "delta-pos.json").write_text("{}")
    assert eng.delta_covers("appdb")
    assert [r.status for r in eng.delta_in_scope("appdb")] == ["ok"]
    assert eng.asked == ["verify"]
    assert (h.report_dir("appdb") / "delta-pos.json").exists()
    assert not h.scope_changed(json.loads(
        (h.report_dir("appdb") / Engine.DELTA_SCOPE).read_text()))


def test_teardown_removes_the_scope_with_the_position(reports, monkeypatch):
    h = _hop()
    eng = _Delta(h)
    eng.delta_in_scope("appdb")
    monkeypatch.setattr(cli, "get_hop", lambda name: h)
    monkeypatch.setattr(cli, "get_engine", lambda hop: eng)
    monkeypatch.setattr(cli, "_require_configured", lambda hop: None)
    cli._delta_loop("h", "appdb", 1, 1, True)
    assert not (h.report_dir("appdb") / Engine.DELTA_SCOPE).exists()


# ---- watch ------------------------------------------------------------------

def test_watch_sums_only_what_the_hop_takes():
    """A busy table the hop leaves out is neither in the sums nor a sign
    the source is still written; the target's tables are the ones the
    taken tables land in, under their new names; a filtered table is
    named, its source counted whole."""
    h = _hop(include=["appdb.public.orders", "appdb.public.lines"],
             mapping={"where": {"appdb.public.orders": "region = 'TH'"},
                      "tables": {"appdb.public.lines": "lines_v2"}})
    src, dst, narrowed = _Delta(h).watched(
        "appdb", {"public.orders": 100, "public.lines": 10,
                  "public.audit": 10 ** 9},
        {"public.orders": 5, "public.lines_v2": 10, "public.audit": 7,
         "public.lines": 99})
    assert src == {"public.orders": 100, "public.lines": 10}
    assert dst == {"public.orders": 5, "public.lines_v2": 10}
    assert narrowed == ["public.orders"]
    # the left-out table written between two samples says nothing
    busier = _Delta(h).watched("appdb", {"public.orders": 100,
                                         "public.lines": 10,
                                         "public.audit": 2 * 10 ** 9},
                               {})[0]
    v = cli._cutover_verdict({"src_rows": 110, "dst_rows": 110,
                              "src_tables": src},
                             {"src_rows": 110, "dst_rows": 110,
                              "src_tables": busier}, 1)
    assert v is None or "HOT" not in v[2], v


def test_a_hop_that_narrows_nothing_is_watched_and_sized_as_before():
    h = _hop()
    src = {"public.a": 1, "migkit_changelog": 2}
    assert _Delta(h).watched("appdb", src, {"public.b": 3}) == (
        src, {"public.b": 3}, [])

    class Facts(_Delta):
        def table_facts(self, side, db):
            return {"public.a": {"rows": 1}, "public.b": {"rows": 2}}
    assert Facts(h).carried("appdb") == (
        {"public.a": {"rows": 1}, "public.b": {"rows": 2}}, [])


def test_a_watch_reads_the_hop_again_each_cycle(monkeypatch):
    """The scope changed while the loop ran: a new engine - nothing the
    old one cached - the databases listed again, and said."""
    before, after = _hop(), _hop(exclude=["appdb.public.audit"])
    said = []
    monkeypatch.setattr(cli.console, "print",
                        lambda m="", **k: said.append(str(m)))
    monkeypatch.setattr(cli, "_require_configured", lambda hop: None)
    made = []

    class Listed(_Delta):
        def databases(self):
            return ["appdb"]

    def engine(hop):
        made.append(hop)
        return Listed(hop)
    monkeypatch.setattr(cli, "get_engine", engine)
    monkeypatch.setattr(cli, "get_hop", lambda name: before)
    old = Listed(before)
    assert cli._hop_again("h", before, old, "", ["x"]) == (
        before, old, ["x"], False)
    assert not made
    monkeypatch.setattr(cli, "get_hop", lambda name: after)
    hop, eng, dbs, moved = cli._hop_again("h", before, old, "", ["x"])
    assert moved and hop is after and eng is not old and dbs == ["appdb"]
    assert made == [after]
    assert any("scope changed" in s for s in said), said


# ---- sizing -----------------------------------------------------------------

class _Sized(_Delta):
    def table_facts(self, side, db):
        return {"public.orders": {"rows": 1000, "bytes": 8000},
                "public.audit": {"rows": 10 ** 9, "bytes": 10 ** 12}}


def test_the_move_is_sized_by_what_it_carries():
    """A table the hop leaves out is no part of the rows, the time or the
    size; a table a row filter narrows is counted whole and said as the
    most."""
    h = _hop(include=["appdb.public.orders"],
             mapping={"where": {"appdb.public.orders": "region = 'TH'"}})
    taken, narrowed = _Sized(h).carried("appdb")
    assert taken == {"public.orders": {"rows": 1000, "bytes": 8000}}
    assert narrowed == ["public.orders"]
    assert cli._source_rows(_Sized(h), "appdb") == (1000, ["public.orders"])
    said = planner.estimate(h, "builtin", 1000, narrowed)
    assert said.startswith("at most") and "1,000 rows" in said, said
    assert planner.estimate(h, "builtin", 1000).startswith("about")
    decisions = [SimpleNamespace(table="public.orders", path="bulk")]
    line = planner.size_line(decisions, _Sized(h).table_facts("src", "x"),
                             "postgres", narrowed=narrowed)
    assert line.startswith("at most") and "row filter" in line, line


def test_no_rate_is_kept_from_rows_counted_whole(monkeypatch, reports):
    """A copy whose rows are only the most it carried keeps no rate: the
    rate every later plan is read from - other hops' too - was its rows
    over its seconds."""
    from migkit import machine, rehearsal, sizing
    kept = []
    monkeypatch.setattr(planner, "record_rate",
                        lambda hop, via, rows, *a, **k: kept.append(rows))
    monkeypatch.setattr(rehearsal, "held_to", lambda *a: [])
    monkeypatch.setattr(machine, "size_of", lambda *a: 0)
    monkeypatch.setattr(sizing, "host", lambda: {})
    monkeypatch.setattr(cli, "chat", lambda m: None)
    h = _hop()
    cli._record_run(h, _Sized(h), "appdb", "builtin", 1000, 0.0,
                    machine.Spent(), ["public.orders"])
    cli._record_run(h, _Sized(h), "appdb", "builtin", 1000, 0.0,
                    machine.Spent())
    assert kept == [None, 1000]


def test_the_model_says_a_filtered_table_is_weighed_whole():
    from migkit import model
    h = _hop(include=["appdb.public.orders"],
             mapping={"where": {"appdb.public.orders": "region = 'TH'"}})
    eng = _Sized(h)
    carried, narrowed = model._carried(h, eng, "appdb",
                                       eng.table_facts("src", "appdb"), None)
    assert carried == ["public.orders"] and narrowed == ["public.orders"]


# ---- answers ----------------------------------------------------------------

def test_the_same_findings_under_another_scope_are_not_identical(reports):
    """`identical to the previous run` and the fingerprint automation
    compares are about the scope the findings were taken under."""
    rows = [{"category": "data", "scope": "appdb", "status": "ok"}]
    h = _hop(include=["appdb.public.orders"])
    verdict.write(h, rows)
    assert verdict.unchanged_since(h, rows)
    h.include = ["appdb.public.*"]
    assert not verdict.unchanged_since(h, rows)
    assert verdict.fingerprint(rows, "a") != verdict.fingerprint(rows, "b")
    # a hop that takes everything whole keeps the fingerprint it had
    assert verdict.summarize(_hop(), rows)["fingerprint"] == \
        verdict.fingerprint(rows)


def test_the_digests_listed_are_of_what_the_hop_takes_now(reports):
    """A table the hop has since left out is not listed with its old
    answer; the evidence names it as left out. A renamed table's target
    side stays."""
    h = _hop(include=["appdb.public.orders"],
             mapping={"tables": {"appdb.public.orders": "orders_v2"}})
    lines = [{"session": "s", "db": "appdb", "table": t, "side": side,
              "rows": 1, "digest": "9", "method": "own",
              "engine": "postgres"}
             for t, side in (("public.orders", "src"),
                             ("public.orders_v2", "dst"),
                             ("public.audit", "src"),
                             ("public.audit", "dst"))]
    (h.report_dir() / digests.FILE).write_text(
        "".join(json.dumps(x) + "\n" for x in lines))
    left = []
    got = digests.read(h, None, left)
    assert sorted(t for _, t, _ in got) == ["public.orders",
                                            "public.orders_v2"]
    assert left == [("appdb", "public.audit")]
    # read without the hop, every line is there as before
    assert len(digests.read_in(h.report_dir())) == 3


def test_the_report_says_the_scope_its_answers_were_taken_under(reports):
    """Rendered again from the last check, the report says the scope that
    check recorded, and that the hop's is another now - it said today's
    scope over yesterday's answers."""
    h = _hop(include=["appdb.public.orders"])
    (h.report_dir() / "summary.json").write_text(json.dumps(
        [{"check": "data", "scope": "appdb", "status": "ok"}]))
    (h.report_dir() / "verdict.json").write_text(json.dumps(
        {"status": "same", "scope_fingerprint": "an-older-one",
         "scope": {"exclude": ["appdb.public.audit"]}}))
    path, _ = report.from_summary(h)
    html = path.read_text()
    assert "leaves out" in html and "appdb.public.audit" in html
    assert "the hop now" in html and "takes only" in html
    # the scope it was taken under, and the hop's the same: nothing more
    (h.report_dir() / "verdict.json").write_text(json.dumps(
        {"status": "same", "scope_fingerprint": h.scope_fingerprint(),
         "scope": verdict.hop_scope(h)}))
    html = report.from_summary(h)[0].read_text()
    assert "takes only" in html and "the hop now" not in html
    # a verdict file from before scopes were kept: the hop's, as before
    (h.report_dir() / "verdict.json").write_text(json.dumps(
        {"status": "same"}))
    html = report.from_summary(h)[0].read_text()
    assert "takes only" in html and "the hop now" not in html
    assert report._scope_section(_hop(), None) == ""


def test_the_evidence_says_its_verdicts_scope_and_the_hops_now(reports):
    h = _hop(include=["appdb.public.orders"],
             db_map={"appdb": "app"},
             mapping={"where": {"appdb.public.orders": "id > 1"}})
    (h.report_dir() / "verdict.json").write_text(json.dumps(
        {"status": "same", "scope_fingerprint": "an-older-one",
         "scope": {"exclude": ["appdb.public.audit"]}}))
    doc = export.gather(h)
    assert doc["verdict"]["scope"] == {"exclude": ["appdb.public.audit"]}
    assert doc["verdict"]["scope_changed_since"] is True
    assert doc["route"]["scope"]["row_filters"] == ["appdb.public.orders"]
    assert doc["route"]["databases_renamed"] == {"appdb": "app"}
    rows = {what: names for title, _, got in export._sections(doc)
            if title == "the scope the verdict covers"
            for what, names in got}
    assert rows["leaves out"] == "appdb.public.audit"
    assert "the hop now" in rows and rows["now takes only"] == \
        "appdb.public.orders"
    assert rows["databases on the target"] == "appdb -> app"
    assert "the scope the verdict covers" in export.markdown(doc)


# ---- resume records ---------------------------------------------------------

def test_a_stopped_straight_copy_under_another_scope_starts_over(
        reports, tmp_path, monkeypatch):
    """Its dump holds what the scope of then took - loaded again under
    another row filter, it put those rows in tables this scope takes
    otherwise."""
    from migkit import movers
    h = _hop("mysql")
    folder = tmp_path / "straight-copy"
    folder.mkdir()
    (folder / "@.done.json").write_text("{}")
    monkeypatch.setattr(movers, "_my_source_at",
                        lambda hop: {"log_file": "b.1", "log_pos": 4})
    rec = h.report_dir("shop") / "bulk-resume.json"

    def kept(**more):
        rec.write_text(json.dumps({"tables": ["a"], "position": {
            "log_file": "b.1", "log_pos": 4}, **more}))
        return movers._shell_resumable(h, "shop", ["a"], folder)
    assert kept(scope_fingerprint=h.scope_fingerprint())
    assert kept() is not None
    assert kept(scope_fingerprint="another") is None
    # nor one whose values were masked otherwise as they were written
    assert kept(scope_fingerprint=h.scope_fingerprint(), masked={})
    assert kept(scope_fingerprint=h.scope_fingerprint(),
                masked={"a": {"email": ["null", ""]}}) is None


def test_the_copy_keeps_the_scope_it_copied(reports):
    h = _hop(include=["public.orders"])
    cli._record_copy(h, "appdb", {"lsn": "0/1"})
    rec = cli._copy_record(h, "appdb")
    assert rec["scope_fingerprint"] == h.scope_fingerprint()
    assert h.under(rec).scope_fingerprint() == h.scope_fingerprint()


def test_tables_the_scope_took_in_after_the_copy_are_copied_first(reports):
    """A stream started from a copy's position carries only what changes:
    a table the scope took in since the copy - or whose rows it filters
    otherwise now - would hold only the rows changed from there."""
    then = _hop(include=["public.orders"])
    now = _hop(include=["public.orders", "public.lines"],
               mapping={"where": {"public.orders": "id > 5"}})

    class Listed:
        def list_move_tables(self, d):
            return [("public", "orders"), ("public", "lines"),
                    ("public", "audit")]
    rec = {"scope_fingerprint": then.scope_fingerprint(),
           "scope": then.scope_record()}
    assert cli._entered(now, Listed(), "appdb", rec) == [
        ("public", "orders"), ("public", "lines")]
    assert cli._entered(then, Listed(), "appdb", rec) == []
    # a record from before scopes were kept is gone on from as before
    assert cli._entered(now, Listed(), "appdb", {"at": "x"}) == []
    # every table where the database lands elsewhere now
    moved = _hop(include=["public.orders"], db_map={"appdb": "other"})
    assert cli._entered(moved, Listed(), "appdb", rec) == [
        ("public", "orders")]


# ---- a seed on media --------------------------------------------------------

class _Pair:
    def list_move_tables(self, db):
        return [("public", "orders"), ("public", "lines")]


def _work(hop, said):
    return SimpleNamespace(hop=hop, db="appdb", pair=_Pair(),
                           log=said.append)


ENTRIES = [{"table": "public.orders", "where": None},
           {"table": "public.lines", "where": None}]


def test_a_seed_load_holds_every_entry_to_the_scope_now():
    """A table the hop has left out since the export is not loaded - the
    load empties each table it writes, and wrote over the target's own -
    and said."""
    said = []
    w = _work(_hop(exclude=["public.lines"]), said)
    got = ship._in_scope_now(w, {"scope_fingerprint": "export"}, ENTRIES)
    assert [e["table"] for e in got] == ["public.orders"]
    assert "public.lines" in said[0], said
    # a manifest from before scopes were kept, or of this scope, as before
    assert ship._in_scope_now(w, {}, ENTRIES) == ENTRIES
    assert ship._in_scope_now(w, {"scope_fingerprint":
                                  w.hop.scope_fingerprint()},
                              ENTRIES) == ENTRIES


def test_a_seed_load_refuses_what_cannot_be_filtered_at_the_load():
    said = []
    filtered = _work(_hop(mapping={"where": {"public.orders": "id > 1"}}),
                     said)
    with pytest.raises(SystemExit, match="row filter changed"):
        ship._in_scope_now(filtered, {"scope_fingerprint": "export"},
                           ENTRIES)

    class More(_Pair):
        def list_move_tables(self, db):
            return super().list_move_tables(db) + [("public", "new")]
    w = _work(_hop(), said)
    w.pair = More()
    with pytest.raises(SystemExit, match="public.new .not in the seed"):
        ship._in_scope_now(w, {"scope_fingerprint": "export"}, ENTRIES)


def test_the_seeds_target_table_is_found_with_its_database():
    """A rename that names its database - the only way to tell two
    databases' `orders` apart - was passed over by the load, which asked
    without the database, and the rows landed in the old name."""
    from migkit.engines.hetero import HeteroEngine
    h = _hop(mapping={"tables": {"appdb.public.orders": "orders_v2"}})

    class Pair:
        hop = h
        _leaf = staticmethod(HeteroEngine._leaf)
        match_tables = staticmethod(HeteroEngine.match_tables)
        _rename = HeteroEngine._rename
        _renamer = HeteroEngine._renamer

    side = SimpleNamespace(target_missing=lambda db: False,
                           neutral_tables=lambda s, db: (
                               ["public.orders"] if s == "src" else
                               ["public.orders", "public.orders_v2"]),
                           CREATES_ON_WRITE=True)
    w = SimpleNamespace(pair=Pair(), db="appdb", src=side, dst=side)
    assert ship._target_table(w, "public.orders", print) == \
        "public.orders_v2"


# ---- the delta reads both sides as the check does ---------------------------

def test_the_delta_reads_the_target_under_its_name_through_the_filter():
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    my = MySQLEngine(_hop("mysql", db_map={"shop": "shop2"}, mapping={
        "where": {"orders": "region = 'TH'"},
        "tables": {"shop.orders": "orders_v2"}}))
    assert my._scope("dst", "shop", "orders", renamed=True) == \
        "(select * from `shop2`.`orders_v2` where region = 'TH') t"
    # the whole-database passes read it as they always did
    assert my._scope("dst", "shop", "orders") == \
        "(select * from `shop2`.`orders` where region = 'TH') t"
    pg = PostgresEngine(_hop(mapping={
        "where": {"public.orders": "region = 'TH'"},
        "tables": {"appdb.public.orders": "orders_v2"}}))
    assert pg._scope("appdb", "public.orders", "dst") == \
        '(select * from "public"."orders_v2" where region = \'TH\') t'
    assert pg._scope("appdb", "public.orders") == \
        '(select * from "public"."orders" where region = \'TH\') t'
    assert pg._scope("appdb", "public.lines", "dst") == '"public"."lines" t'


def test_a_target_name_is_read_back_to_the_table_it_was(reports):
    """A target's name is asked as the source's table it was
    (`Hop.source_table_in`): a rename in another database is not this
    one's, and a name the hop's rules wrote is its table's - the leaf of
    any rename once kept another database's table, and a prefixed name
    was dropped."""
    h = _hop(include=["appdb.public.orders"],
             mapping={"tables": {"crm.public.lines": "orders_v2"},
                      "names": {"tables": {"prefix": "x_"}}})
    assert h.source_table_in("appdb", "public", "x_orders") == (
        "public", "orders")
    assert digests.taken(h, "appdb", "public.x_orders", "postgres", "dst")
    # `orders_v2` is crm's `lines` renamed, not a table of appdb's
    assert not digests.taken(h, "appdb", "public.orders_v2", "postgres",
                             "dst")
    assert h.source_table_in("crm", "public", "orders_v2") == (
        "public", "lines")
    # a rule that cannot be turned round says nothing it cannot know
    lossy = _hop(mapping={"names": {"tables": {"case": "lower"}}})
    assert lossy.source_table_in("appdb", "public", "orders") is None

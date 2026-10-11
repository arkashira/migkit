"""`doctor` names the way the whole database moves, the way the changes are
followed, what each leaves, and how deeply each table's changes can be read
with the statement that raises it (P0-DE.2, LT.R1). Stubs only: nothing
connects to a server."""
import pytest

from migkit import legs, movers, waysaid
from migkit.config import Endpoint, Hop


def _hop(engine="postgres"):
    ep = Endpoint(host="x", port=1, user="x", password="x")
    return Hop(name="waysaid", engine=engine, source=ep, target=ep)


@pytest.fixture(autouse=True)
def _no_history(monkeypatch):
    monkeypatch.delenv("MIGKIT_CDC", raising=False)
    monkeypatch.setattr("migkit.decide.HopCosts.get",
                        lambda self, name, default=None: default)


def test_every_way_says_what_it_leaves():
    rungs = list(movers._ladder()) + list(movers.stream_ways()) \
        + list(legs.rungs())
    silent = sorted({r.name for r in rungs if not r.leaves})
    assert silent == [], f"ways that do not say what they leave: {silent}"
    # the number only breaks a tie: a way that has one says it in words
    assert all(r.leaves for r in rungs if r.footprint)


def test_the_words_name_no_program_migkit_drives():
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    rungs = list(movers._ladder()) + list(movers.stream_ways()) \
        + list(legs.rungs())
    said = " ".join(r.leaves.lower() for r in rungs)
    assert not [t for t in TOOLS if t in said], said


def _stub_move(monkeypatch, via, why=""):
    monkeypatch.setattr(movers, "for_hop", lambda *a, **k: (via, ""))
    monkeypatch.setattr(movers, "fitted", lambda *a, **k: (via, why))


def test_the_move_is_named_with_what_it_leaves(monkeypatch):
    _stub_move(monkeypatch, "pgdump")
    got = waysaid.move_line(_hop())
    assert got.startswith("whole-database move: the dump and restore in"
                          " parallel"), got
    assert "it leaves a directory of dump files" in got, got
    assert "chosen again when the move runs" in got, got


def test_a_way_that_acts_on_a_server_is_proved_when_the_move_goes(
        monkeypatch):
    _stub_move(monkeypatch, "pgsubscribe")
    got = waysaid.move_line(_hop())
    assert "a temporary subscription on the target" in got, got
    assert "publication and a slot on the source" in got, got
    assert "proved on the servers only when the move goes" in got, got
    assert "offered when the move goes" in got, got


def test_the_table_copier_is_chosen_per_table_at_run_time(monkeypatch):
    _stub_move(monkeypatch, "builtin", "the table copier carries the hop")
    got = waysaid.move_line(_hop())
    assert "the table copier" in got and "checkpoint" in got, got
    assert "each table's way is chosen when it is copied" in got, got


def test_a_move_that_cannot_be_chosen_now_is_chosen_at_run_time(monkeypatch):
    def refuse(*a, **k):
        raise SystemExit("no way here")
    monkeypatch.setattr(movers, "for_hop", refuse)
    got = waysaid.move_line(_hop())
    assert "no way chosen now (no way here)" in got, got
    assert "chosen when the move runs" in got, got


def test_the_move_line_is_read_from_the_ladder_the_move_uses():
    # nothing stubbed: the line is the one `movers.for_hop` gives here
    via = movers.fitted(_hop("mysql"), "mysql",
                        movers.for_hop(_hop("mysql"), "mysql")[0])[0]
    got = waysaid.move_line(_hop("mysql"))
    assert waysaid.rung_named(via).said in got, got


def _climb(engine, repl, tail, ready, risk=None):
    def climb(hop, eng, db, drop):
        facts = movers.stream_facts(hop, engine, repl, tail, ready, drop,
                                    risk)
        return facts, movers.stream_ways(), None, "following waysaid"
    return climb


class _Eng:
    def __init__(self, maps=False, depths=None, boom=None):
        self._maps, self._depths, self._boom = maps, depths, boom

    def maps_columns(self):
        return self._maps

    def change_depths(self):
        if self._boom:
            raise self._boom
        return self._depths


def test_the_changes_are_named_with_what_they_leave():
    got = waysaid.follow_lines(_hop(), _Eng(),
                               _climb("postgres", True, True, True))
    assert len(got) == 1
    line = got[0]
    assert line.startswith("following changes: the servers' own"
                           " replication"), line
    assert "a slot and a publication on the source" in line, line
    assert "a subscription on the target" in line, line
    assert "chosen again when the move runs" in line, line


def test_a_target_that_cannot_dial_the_source_follows_from_here(monkeypatch):
    monkeypatch.setenv("MIGKIT_CDC", "follow")
    line = waysaid.follow_lines(_hop(), _Eng(),
                                _climb("postgres", True, True, True))[0]
    assert "the change leg run from here" in line, line
    assert "an origin on the target" in line, line


def test_a_publication_at_risk_is_read_and_the_tail_is_named():
    line = waysaid.follow_lines(
        _hop(), _Eng(), _climb("postgres", True, True, True,
                               risk=lambda: "an older minor release"))[0]
    assert "migkit's own change tail" in line.split(";")[0], line
    assert "a position file" in line, line


def test_the_facts_the_changes_were_chosen_from_are_said():
    line = waysaid.follow_lines(_hop(), _Eng(),
                                _climb("postgres", True, True, True))[0]
    assert "read now:" in line, line


def test_a_hop_that_maps_columns_follows_by_the_tail():
    line = waysaid.follow_lines(_hop(), _Eng(maps=True), None)[0]
    assert "migkit's own change tail" in line and "maps columns" in line


def test_changes_that_cannot_be_chosen_now_are_chosen_at_run_time():
    def boom(*a):
        raise RuntimeError("source unreachable")
    line = waysaid.follow_lines(_hop(), _Eng(), boom)[0]
    assert "not chosen now (source unreachable)" in line, line
    assert "chosen when the move runs" in line, line
    assert "the source's version and settings" in line, line


def test_no_way_set_up_says_why():
    line = waysaid.follow_lines(_hop("mysql"), _Eng(),
                                _climb("mysql", False, True, False))[0]
    assert "no way is set up on this machine" in line, line


def test_an_engine_with_no_way_to_follow_says_so(monkeypatch):
    monkeypatch.setattr("migkit.capabilities.unavailable",
                        lambda e, c: "migkit cannot follow this engine")
    line = waysaid.follow_lines(_hop(), _Eng(), None)[0]
    assert line.startswith("following changes: no way on postgres"), line


def test_each_table_below_the_best_depth_names_what_raises_it():
    got = waysaid.depth_lines(_Eng(depths={
        "appdb.a": ("D4", ""), "appdb.b": ("D3",
                                           "set global binlog_row_image ="
                                           " 'FULL'"),
        "appdb.c": ("D1", "give the table a primary key")}))
    text = "\n".join(got)
    assert "change reading D4" in text and "appdb.a" in text
    assert "appdb.b - to raise it: set global binlog_row_image = 'FULL'" \
        in text, text
    assert "appdb.c - to raise it: give the table a primary key" in text
    # a table at the best depth has nothing to raise
    assert "appdb.a - to raise" not in text


def test_an_engine_whose_log_is_not_read_says_it_is_not_worked_out():
    got = waysaid.depth_lines(_Eng(depths=None))
    assert len(got) == 1 and "not worked out on this engine" in got[0], got
    assert "no statement that raises one" in got[0], got


def test_a_depth_that_cannot_be_read_now_is_said_and_never_fails():
    got = waysaid.depth_lines(_Eng(boom=RuntimeError("no catalogue")))
    assert "not worked out now (no catalogue)" in got[0], got


def test_no_table_in_scope_is_said():
    assert "no table in scope" in waysaid.depth_lines(_Eng(depths={}))[0]


def test_lines_ask_only_what_was_reached(monkeypatch):
    _stub_move(monkeypatch, "pgdump")
    eng = _Eng(depths={"d.t": ("D4", "")})
    only_source = waysaid.lines(_hop(), eng, None, True, False)
    assert len(only_source) == 1 and "change reading D4" in only_source[0]
    assert waysaid.lines(_hop(), eng, None, False, False) == []
    both = waysaid.lines(_hop(), eng,
                         _climb("postgres", True, True, True), True, True)
    assert [l.split(":")[0] for l in both] == [
        "whole-database move", "following changes", "change reading D4 "
        "(every change with its before and after image)"], both


def test_the_stream_climb_is_one_reading_for_the_move_and_doctor():
    """`cli._stream_climb` is what `_cdc` climbs and what doctor says."""
    import inspect

    from migkit import cli
    assert "_stream_climb(" in inspect.getsource(cli._cdc)
    assert "_stream_climb" in inspect.getsource(cli._doctor_hop)

    class Eng:
        def replicate_sql(self):
            pass

        def tail_apply(self):
            pass
        tail_requires = ""

        def databases(self):
            raise AssertionError("nothing is asked until a rung needs it")
    facts, rungs, where, work = cli._stream_climb(_hop(), Eng(), None,
                                                  False)
    assert [r.name for r in rungs] == [r.name for r in movers.stream_ways()]
    assert all(r.leaves for r in rungs)
    assert work == "following waysaid's source"
    assert str(where).endswith("stream.json")


def test_mysql_and_postgres_depths_come_from_the_engines():
    from migkit.engines.base import Engine
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    assert MySQLEngine.change_depths is not Engine.change_depths
    assert PostgresEngine.change_depths is not Engine.change_depths
    assert Engine.change_depths(object()) is None


def test_the_postgres_depths_are_read_from_wal_level_and_identity():
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop())
    eng.databases = lambda: ["appdb"]
    eng.in_scope = lambda db, name: not name.endswith("skip")

    def psql(side, db, sql):
        if sql == "show wal_level":
            return "logical"
        return "\n".join(["public.full|f|true", "public.keyed|d|true",
                          "public.nokey|d|false", "public.skip|d|true"])
    eng._psql = psql
    got = eng.change_depths()
    assert got["appdb.public.full"] == ("D4", "")
    assert got["appdb.public.keyed"][0] == "D3"
    assert "replica identity full" in got["appdb.public.keyed"][1]
    assert got["appdb.public.nokey"][0] == "D1"
    assert "appdb.public.skip" not in got


def test_the_mysql_depths_are_read_from_the_log_settings_and_keys():
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_hop("mysql"))
    eng.databases = lambda: ["appdb"]
    eng._d = lambda side, db: db
    eng._tables = lambda side, db: ["k", "nokey"]
    settings = {"log_bin": "ON", "binlog_format": "ROW",
                "binlog_row_image": "MINIMAL",
                "binlog_row_metadata": "FULL"}

    def q(side, sql, args=None):
        if sql.startswith("show variables like"):
            name = sql.split("'")[1]
            return [(name, settings[name])]
        return [("nokey",)]
    eng._q = q
    got = eng.change_depths()
    assert got["appdb.k"][0] == "D3"
    assert "binlog_row_image = 'FULL'" in got["appdb.k"][1]
    assert got["appdb.nokey"][0] == "D1"


def test_the_next_way_is_named_with_what_it_leaves():
    from migkit.decide import Facts, Rung

    def climb(hop, eng, db, drop):
        rungs = (Rung("a", "the first way", leaves="a file here"),
                 Rung("b", "the second way", leaves="a slot on the source"))
        return Facts({"costs": {}}), rungs, None, "w"
    line = waysaid.follow_lines(_hop(), _Eng(), climb)[0]
    assert line.startswith("following changes: the first way"), line
    assert "next: the second way (a slot on the source)" in line, line


def test_doctor_says_the_ways_after_both_sides_answered(monkeypatch, tmp_path):
    import migkit.config as cfg
    from migkit import cli
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    hop = _hop("mysql")
    eng = _Eng(depths={"appdb.t": ("D3", "set global binlog_row_image ="
                                         " 'FULL'")})
    eng.databases = lambda: ["appdb"]
    monkeypatch.setattr(cli, "get_engine", lambda h: eng)
    monkeypatch.setattr(cli, "_doctor_side", lambda name, h, e, side, ep: (
        [f"{name} {side}: ok"], 1.0, True, False))
    monkeypatch.setattr(cli, "_doctor_setup", lambda *a, **k: None)
    monkeypatch.setattr(cli, "_placement_said", lambda *a, **k: "")
    monkeypatch.setattr(cli, "_decisions_said", lambda h: [])
    monkeypatch.setattr(cli, "_stream_climb",
                        _climb("mysql", True, True, True))
    _stub_move(monkeypatch, "mydumper")
    out, failed = cli._doctor_hop("waysaid", hop, {})
    said = "\n".join(out)
    assert "whole-database move: the parallel dump and load" in said, said
    assert "following changes:" in said, said
    assert "appdb.t - to raise it: set global binlog_row_image" in said, said
    assert not failed
    # one side answering is enough for the depth, not for the ways
    monkeypatch.setattr(cli, "_doctor_side", lambda name, h, e, side, ep: (
        [f"{name} {side}: ok"], 1.0, side == "src", False))
    out, _ = cli._doctor_hop("waysaid", hop, {})
    said = "\n".join(out)
    assert "whole-database move" not in said and "change reading" in said

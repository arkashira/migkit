"""The streaming copy splits a table across streams only where the facts
and the measurements say it pays (backlog 0f).

One table copied by one stream is the copy's floor, and the split is the
feature its author credits its speed to. It was not measured faster
everywhere: on this machine, both servers sharing two CPUs, whole tables
went faster. So the threshold comes from what the copy carries, the parts
from how the build cuts a table (measured: `ceil(size / threshold) + 1`),
and the way from the rates each way reached on this hop or its rehearsal.
"""
import json
import sqlite3

import pytest

from migkit import movers, planner
from migkit.config import Endpoint, Hop

HAVE = frozenset({"--split-tables-larger-than", "--split-max-parts"})
MB = 1 << 20


@pytest.fixture
def hop(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    h = Hop(name="sp", engine="postgres",
            source=Endpoint(host="10.0.0.1", port=1, user="u",
                            password="CHANGE_ME"),
            target=Endpoint(host="10.0.0.2", port=2, user="u",
                            password="CHANGE_ME"),
            databases=["appdb"])
    return h


def test_tables_within_a_share_go_whole(hop):
    got = movers._copy_split(hop, 4, {"a": 10 * MB, "b": 10 * MB,
                                      "c": 10 * MB, "d": 10 * MB}, HAVE)
    assert got.way == "whole" and got.argv() == [], got.why
    assert "even share" in got.why


def test_one_stream_never_splits(hop):
    got = movers._copy_split(hop, 1, {"a": 900 * MB, "b": MB}, HAVE)
    assert got.way == "whole" and got.argv() == []


def test_a_build_without_the_split_copies_whole(hop):
    got = movers._copy_split(hop, 4, {"a": 900 * MB, "b": MB}, frozenset())
    assert got.way == "whole" and got.argv() == []
    assert "cannot split" in got.why


def test_the_table_that_outlasts_the_rest_is_cut_to_fit_a_share(hop):
    """Half a share as the threshold: with the build's own cut, the last
    part twice the others, the largest part fits one share."""
    sizes = {"public.big": 800 * MB, "public.small": 200 * MB}
    got = movers._copy_split(hop, 4, sizes, HAVE)
    share = 1000 * MB / 4
    assert got.way == "split", got.why
    assert got.tables == ("public.big",)
    assert got.threshold == share / 2
    # 800 / 125 = 6.4 -> 7, and the part the build keeps for NULL keys
    assert got.parts == 8
    largest = 2 * 800 * MB / got.parts
    assert largest <= share
    args = got.argv()
    assert args == ["--split-tables-larger-than", str(int(share / 2)),
                    "--split-max-parts", "8"]


def test_measured_whole_faster_goes_whole(hop):
    planner.record_variant(hop, "pgcopydb", "whole", 1000 * MB, 10, 20 * MB)
    planner.record_variant(hop, "pgcopydb", "split", 1000 * MB, 14, 12 * MB)
    got = movers._copy_split(hop, 4, {"a": 800 * MB, "b": 200 * MB}, HAVE)
    assert got.way == "whole", got.why
    assert "measured faster here" in got.why


def test_measured_split_faster_splits(hop):
    planner.record_variant(hop, "pgcopydb", "whole", 1000 * MB, 14, 20 * MB)
    planner.record_variant(hop, "pgcopydb", "split", 1000 * MB, 10, 12 * MB)
    got = movers._copy_split(hop, 4, {"a": 800 * MB, "b": 200 * MB}, HAVE)
    assert got.way == "split", got.why
    assert "measured faster here" in got.why


def test_a_tie_goes_whole_which_asks_the_source_less(hop):
    """Within 5% is a tie (`decide.TIE`), and the footprint decides: a
    split asks the source a query a part."""
    planner.record_variant(hop, "pgcopydb", "whole", 1000 * MB, 10.3,
                           20 * MB)
    planner.record_variant(hop, "pgcopydb", "split", 1000 * MB, 10.0,
                           12 * MB)
    got = movers._copy_split(hop, 4, {"a": 800 * MB, "b": 200 * MB}, HAVE)
    assert got.way == "whole", got.why
    assert "too close to call" in got.why
    # past the tie, the split wins
    planner.record_variant(hop, "pgcopydb", "whole", 1000 * MB, 11,
                           20 * MB)
    got = movers._copy_split(hop, 4, {"a": 800 * MB, "b": 200 * MB}, HAVE)
    assert got.way == "split", got.why


def test_one_way_measured_tries_the_other(hop):
    planner.record_variant(hop, "pgcopydb", "split", 1000 * MB, 10, 5 * MB)
    assert movers._copy_split(hop, 4, {"a": 800 * MB, "b": 200 * MB},
                              HAVE).way == "whole"
    planner.record_variant(hop, "pgcopydb", "whole", 1000 * MB, 9, 5 * MB)
    # both now: whole was faster
    assert movers._copy_split(hop, 4, {"a": 800 * MB, "b": 200 * MB},
                              HAVE).way == "whole"


def test_a_part_shorter_than_its_query_is_not_made(hop):
    """What one stream carried here, times ten seconds, is the least a part
    is worth; a table that would only make shorter parts goes whole."""
    planner.record_variant(hop, "pgcopydb", "whole", 1000 * MB, 10,
                           100 * MB)
    got = movers._copy_split(hop, 4, {"a": 800 * MB, "b": 200 * MB}, HAVE)
    assert got.way == "whole", got.why
    assert "seconds" in got.why or "s of one stream" in got.why


def test_a_rehearsals_rates_decide_a_hop_that_has_none(hop, monkeypatch):
    other = Hop(name="rehearsal", engine="postgres", source=hop.source,
                target=hop.target, databases=["appdb"])
    planner.record_variant(other, "pgcopydb", "whole", 1000 * MB, 10,
                           20 * MB)
    planner.record_variant(other, "pgcopydb", "split", 1000 * MB, 20,
                           10 * MB)
    got = movers._copy_split(hop, 4, {"a": 800 * MB, "b": 200 * MB}, HAVE)
    assert got.way == "whole", got.why
    assert "rehearsal" in got.why


def _catalogue(tmp_path, rows, parts=()):
    d = tmp_path / "work" / "schema"
    d.mkdir(parents=True)
    c = sqlite3.connect(d / "source.db")
    c.executescript(
        "create table s_table(oid integer primary key, qname text);"
        "create table s_table_part(oid integer, partnum integer,"
        " partcount integer, min integer, max integer, count integer);"
        "create table summary(pid integer, tableoid integer, partnum"
        " integer, indexoid integer, conoid integer, extoid integer,"
        " start_time_epoch integer, done_time_epoch integer, duration"
        " integer, bytes integer, command text);")
    c.executemany("insert into s_table values (?, ?)",
                  [(1, "public.t1"), (2, '"public"."Big"')])
    c.executemany("insert into s_table_part values (?, ?, ?, 0, 0, 0)",
                  parts)
    c.executemany("insert into summary values (0, ?, ?, null, null, null,"
                  " 0, ?, ?, ?, 'COPY')", rows)
    c.commit()
    c.close()
    return tmp_path / "work"


def test_a_stopped_copy_goes_on_cut_the_way_it_began(tmp_path):
    """The facts can move between a stop and the run after it - the
    source's sizes, a rate recorded meanwhile - and the copy it goes on
    from was cut one way: that way is kept beside the record that it
    began, and read back, never chosen again."""
    begun = tmp_path / "streaming-work.begun"
    begun.write_text("2026-09-29T10:00:00")
    movers._Split("split", "why", 125 * MB, 8, ("public.big",)).keep(begun)
    got = movers._Split.resumed(begun)
    assert got.argv() == ["--split-tables-larger-than", str(125 * MB),
                          "--split-max-parts", "8"]
    assert got.why == "tables go in parts, as the stopped copy began"
    movers._Split("whole", "why").keep(begun)
    assert movers._Split.resumed(begun).argv() == []
    # a record from before the way was kept: that copy went whole
    from migkit.decide import forget
    forget(begun)
    assert movers._Split.resumed(begun).argv() == []
    # nothing begun, nothing to go on from
    begun.unlink()
    assert movers._Split.resumed(begun) is None


def _stopped(hop, tmp_path, monkeypatch, way):
    """The streaming copy run with `--go` over a copy stopped part way
    that was cut `way`; (the copy's command line, what it logged, whether
    the target was emptied first)."""
    hop.report_dir = lambda db=None: tmp_path
    monkeypatch.setattr(movers, "pgcopydb_runner", lambda: "local")
    monkeypatch.setattr(movers, "_pgcopydb_options", lambda *sub: HAVE)
    monkeypatch.setattr(movers, "_SIZES", {"public.big": 800 * MB,
                                           "public.small": 200 * MB})
    emptied = []
    for name in ("_pg_create_missing", "_pg_finish_created",
                 "_pg_carry_sequences", "_record_copy"):
        monkeypatch.setattr(movers, name, lambda *a, **k: None)
    monkeypatch.setattr(movers, "_pg_truncate_target",
                        lambda *a, **k: emptied.append(True))
    monkeypatch.setattr(movers, "_pg_references_into",
                        lambda h, db, routed=(): [])
    monkeypatch.setattr(movers, "_pg_missing_tables",
                        lambda h, db: ([], False))

    class Nothing:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False
    monkeypatch.setattr(movers, "_IndexWindow", Nothing)
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.setattr(PostgresEngine, "carry_database_objects",
                        lambda self, db, log=None: None)
    ran = []

    def sh(cmd, env=None, log=None, progress=None):
        ran.append(cmd)
    monkeypatch.setattr(movers, "_sh", sh)
    work, begun = movers._streaming_work(hop, "appdb")
    work.mkdir(parents=True, exist_ok=True)
    begun.write_text("2026-09-30T10:00:00")
    movers._Split(way, "why", 125 * MB, 8, ("public.big",)).keep(begun)
    said = []
    movers.pgcopydb_move(hop, "appdb", 4, True, said.append)
    copy = [c for c in ran if "table-data" in c][0]
    return copy, said, bool(emptied)


def test_a_copy_stopped_while_it_cut_a_table_starts_over(hop, tmp_path,
                                                         monkeypatch):
    """Going on from it lost half a table's rows and exited 0 (measured,
    `_Split.keep`): it starts over, the target emptied first, and says
    why."""
    copy, said, emptied = _stopped(hop, tmp_path, monkeypatch, "split")
    assert "--resume" not in copy, copy
    assert emptied
    assert any("cannot be gone on from without losing rows" in str(s)
               for s in said), said


def test_a_copy_stopped_while_its_tables_went_whole_goes_on(hop, tmp_path,
                                                            monkeypatch):
    """Whole, it goes on from the tables it finished, cut as it began -
    not split now, whatever the facts say today."""
    copy, said, emptied = _stopped(hop, tmp_path, monkeypatch, "whole")
    assert "--resume" in copy and "--not-consistent" in copy, copy
    assert "--split-tables-larger-than" not in copy, copy
    assert not emptied
    assert any("going on from the tables it finished" in str(s)
               for s in said), said


def test_one_tables_run_is_cut_by_the_same_rule(hop, tmp_path, monkeypatch):
    """The table copier's streaming rung (`pgcopydb_table`) and the whole
    database's copy cut a table by one rule and build their command line
    in one place: it split a table at a share of its size, a cut the whole
    database's copy would not have made, and never by what was measured."""
    hop.report_dir = lambda db=None: tmp_path
    monkeypatch.setattr(movers, "pgcopydb_runner", lambda: "local")
    monkeypatch.setattr(movers, "_pgcopydb_options", lambda *sub: HAVE)
    ran = []
    monkeypatch.setattr(movers, "_sh", lambda cmd, env=None, log=None,
                        progress=None: ran.append(cmd))
    said = []
    movers.pgcopydb_table(hop, "appdb", "public.big", "00000003-0000004A-1",
                          4, 800 * MB, said.append)
    cmd = ran[0]
    want = movers._copy_split(hop, 4, {"public.big": 800 * MB}, HAVE)
    assert want.way == "split"
    at = cmd.index("--split-tables-larger-than")
    assert cmd[at:at + 4] == want.argv(), cmd
    assert cmd[cmd.index("--snapshot") + 1] == "00000003-0000004A-1"
    assert said == [f"public.big: {want.why}"], said
    # measured whole faster here: the table goes whole too
    planner.record_variant(hop, "pgcopydb", "whole", 1000 * MB, 10, 20 * MB)
    planner.record_variant(hop, "pgcopydb", "split", 1000 * MB, 14, 12 * MB)
    ran.clear()
    movers.pgcopydb_table(hop, "appdb", "public.big", "s", 4, 800 * MB)
    assert "--split-tables-larger-than" not in ran[0], ran[0]


def test_the_copys_own_catalogue_is_read_for_what_it_did(tmp_path):
    """Its own reader is broken in this build, so its catalogue is read:
    per table the bytes and the time, done only once every part is."""
    work = _catalogue(tmp_path, [
        (1, 0, 5, 2000, 40 * MB),
        (2, 1, 5, 10, 0),
        (2, 2, 6, 1000, 20 * MB),
        (2, 3, None, None, None),
    ], parts=[(2, 1, 3), (2, 2, 3), (2, 3, 3)])
    got = movers._copy_catalogue(work, {"public.t1": 54 * MB})
    t1, big = got["tables"]["public.t1"], got["tables"]["public.Big"]
    assert t1 == {"bytes": 40 * MB, "seconds": 2.0, "done": True,
                  "parts": 1}, t1
    assert big["done"] is False and big["parts"] == 3, big
    # a stream's rate in bytes on disk: 40 MB sent for 54 MB on disk
    assert got["streams"][-1] == pytest.approx(54 * MB / 2.0)
    assert got["streams"][0] == pytest.approx(20 * MB / 1.0)


def test_the_finished_copy_leaves_the_next_run_its_rates(tmp_path, hop):
    work = _catalogue(tmp_path, [(1, 0, 5, 2000, 40 * MB)])
    movers._record_copy(hop, work, "split", 4.0, {"public.t1": 54 * MB})
    got = planner.variants(hop, "pgcopydb")
    assert got["split"]["rate"] == pytest.approx(54 * MB / 4.0)
    assert got["split"]["stream"] == pytest.approx(54 * MB / 2.0, rel=1e-3)
    kept = json.loads((hop.report_dir() / "throughput.json").read_text())
    assert kept["pgcopydb:split"][0]["tables"]["public.t1"][0] == 40 * MB


def test_the_progress_says_each_table_as_it_is_done(tmp_path, monkeypatch):
    work = _catalogue(tmp_path, [(1, 0, 5, 2000, 40 * MB),
                                 (2, 1, 5, 1000, 20 * MB)],
                      parts=[(2, 1, 2), (2, 2, 2)])
    monkeypatch.setattr(movers, "_SIZES", {"public.t1": 54 * MB,
                                           "public.Big": 30 * MB})
    said = []
    with movers._CopyProgress(work, said.append):
        pass
    # the one table whose parts are all done, and only it
    assert len(said) == 1 and said[0].startswith("public.t1: copied (1 of"
                                                 " 2 tables"), said


def _plan(hop, tmp_path, monkeypatch, sizes, objects=0):
    hop.report_dir = lambda db=None: tmp_path
    monkeypatch.setattr(movers, "pgcopydb_runner", lambda: "local")
    monkeypatch.setattr(movers, "_source_sizes", lambda h, db: sizes)
    monkeypatch.setattr(movers, "_pg_large_objects", lambda h, db: objects)
    monkeypatch.setattr(movers, "_pg_references_into",
                        lambda h, db, routed=(): [])
    monkeypatch.setattr(movers, "_pg_missing_tables",
                        lambda h, db: ([], False))
    return movers.pgcopydb_move(hop, "appdb", 4, False, None)


@pytest.mark.skipif(not movers.which("pgcopydb"),
                    reason="the streaming copy is not installed here")
def test_the_plan_says_the_split_and_why(hop, tmp_path, monkeypatch):
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    steps = _plan(hop, tmp_path, monkeypatch,
                  {"public.big": 800 * MB, "public.small": 200 * MB},
                  objects=3)
    joined = "\n".join(steps)
    assert "1 large tables copied in parts" in joined, joined
    assert "go in up to 8 parts" in joined, joined
    assert "carry the source's 3 large objects" in joined, joined
    assert not [t for t in TOOLS if t in joined.lower()], joined
    run = [s for s in steps if getattr(s, "argv", None)][0].argv
    assert "--split-tables-larger-than" in run and "--split-max-parts" in run
    # and the build here takes every option the plan passes it
    flags = {a for a in run if a.startswith("--")}
    assert flags <= movers._long_options("pgcopydb", "copy", "table-data")


@pytest.mark.skipif(not movers.which("pgcopydb"),
                    reason="the streaming copy is not installed here")
def test_a_plan_with_even_tables_passes_no_split(hop, tmp_path, monkeypatch):
    # as many tables as streams, none more than a share
    steps = _plan(hop, tmp_path, monkeypatch,
                  {f"public.t{i}": 100 * MB for i in range(4)})
    run = [s for s in steps if getattr(s, "argv", None)][0].argv
    assert "--split-tables-larger-than" not in run
    assert any("go whole" in s for s in steps), steps


def test_a_copy_out_of_message_queues_says_so(hop, tmp_path, monkeypatch):
    """Copies stopped by force leave their message queues behind, and the
    next one said only `No space left on device` - measured, after 38
    were left on this machine."""
    hop.report_dir = lambda db=None: tmp_path
    monkeypatch.setattr(movers, "pgcopydb_runner", lambda: "local")
    monkeypatch.setattr(movers, "_source_sizes", lambda h, db: {})
    for name in ("_pg_create_missing", "_pg_truncate_target",
                 "_pg_finish_created", "_pg_carry_sequences"):
        monkeypatch.setattr(movers, name, lambda *a, **k: None)
    monkeypatch.setattr(movers, "_pg_references_into",
                        lambda h, db, routed=(): [])
    monkeypatch.setattr(movers, "_pg_missing_tables",
                        lambda h, db: ([], False))

    class NoWindow:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False
    monkeypatch.setattr(movers, "_IndexWindow", NoWindow)

    def sh(cmd, env=None, log=None, progress=None):
        if "ping" in cmd:
            return None
        raise RuntimeError("FATAL queue_utils.c:34 Failed to create message"
                           " queue: No space left on device")
    monkeypatch.setattr(movers, "_sh", sh)
    with pytest.raises(RuntimeError) as e:
        movers.pgcopydb_move(hop, "appdb", 2, True, None)
    assert str(e.value) == movers.QUEUES_GONE
    assert "ipcs -q" in str(e.value)

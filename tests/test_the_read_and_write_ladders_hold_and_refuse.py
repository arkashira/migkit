"""How a tail reads its log, how a table's range edges are cut, how a tail
batch and a batch of columns are written, whether a schedule is admitted,
whether a table is held to a sample first and what level a change-reading
depth earns - each rung held and refused on the facts that decide it
(backlog AUD-Z.rungs-owed): a streamed log only with a reader and the
right, statistics only while they are fine and fresh, typed columns only
where every value went in exactly on one lane, a sample only where it is
asked, measured and smaller than the table. Fakes only."""
from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("CHANGE_READS", "stream", "holds"), ("CHANGE_READS", "stream", "refuses"),
    ("CHANGE_READS", "poll", "holds"),
    ("CHUNK_EDGES", "statistics", "holds"),
    ("CHUNK_EDGES", "statistics", "refuses"),
    ("CHUNK_EDGES", "sample", "holds"), ("CHUNK_EDGES", "sample", "refuses"),
    ("CHUNK_EDGES", "key order", "holds"),
    ("TAIL_APPLIES", "columns", "holds"),
    ("TAIL_APPLIES", "columns", "refuses"),
    ("TAIL_APPLIES", "records", "holds"),
    ("COLUMN_WRITES", "copy", "holds"),
    ("COLUMN_WRITES", "statements", "holds"),
    ("admit", "as scheduled", "holds"), ("admit", "as scheduled", "refuses"),
    ("check_sample", "sample", "holds"), ("check_sample", "sample", "refuses"),
    ("check_sample", "full", "holds"),
    ("change_depth_level", "whole", "holds"),
    ("change_depth_level", "whole", "refuses"),
    ("change_depth_level", "unread", "holds"),
    ("change_depth_level", "unread", "refuses"),
    ("change_depth_level", "partial", "holds"),
    ("change_depth_level", "partial", "refuses"),
    ("change_depth_level", "none", "holds"),
]


def test_a_log_is_streamed_only_with_a_reader_and_the_right_to_stream():
    with watched() as seen:
        got = decide.change_read({"stream client": True,
                                  "stream granted": True})
    assert got.path == "stream"
    assert branch(seen, "stream") == [HOLDS]
    assert branch(seen, "poll") == [HOLDS]
    with watched() as seen:
        got = decide.change_read({"stream client": True,
                                  "stream granted": False,
                                  "stream why": "the user lacks REPLICATION"})
    assert got.path == "poll"
    assert branch(seen, "stream") == [REFUSES]
    assert why(seen, "stream") == "the user lacks REPLICATION"
    with watched() as seen:
        got = decide.change_read({"stream client": False,
                                  "stream granted": True})
    assert got.path == "poll"
    assert "cannot read a streamed log" in why(seen, "stream")


def test_range_edges_come_from_statistics_only_while_they_hold():
    with watched() as seen:
        got = decide.chunk_edges("t", {"statistics": True,
                                       "statistics fresh": True,
                                       "sample": True})
    assert got.path == "statistics"
    assert branch(seen, "statistics") == [HOLDS]
    assert branch(seen, "sample") == [HOLDS]
    with watched() as seen:
        got = decide.chunk_edges("t", {
            "statistics": True, "statistics fresh": False,
            "statistics stale": "the table moved 60% of a range since",
            "sample": True})
    assert got.path == "sample"
    assert branch(seen, "statistics") == [REFUSES]
    assert why(seen, "statistics") == "the table moved 60% of a range since"


def test_an_engine_with_no_statistics_and_no_sample_reads_the_key_in_order():
    with watched() as seen:
        got = decide.chunk_edges("t", {"statistics": False, "sample": False})
    assert got.path == "key order"
    assert "no statistics fine enough" in why(seen, "statistics")
    assert branch(seen, "sample") == [REFUSES]
    assert "no statement for a sample" in why(seen, "sample")
    assert branch(seen, "key order") == [HOLDS]


def _tail(facts):
    return decide.climb("t", decide.tail_apply(facts), decide.Facts(facts),
                        prove=False)


def test_a_tail_batch_goes_as_columns_only_typed_exactly_on_one_lane():
    whole = {"writer": True, "typed": True, "one lane": True}
    with watched() as seen:
        got = _tail(whole)
    assert got.path == "columns"
    assert branch(seen, "columns") == [HOLDS]
    assert branch(seen, "records") == [HOLDS]
    for off, said in (
            ({"writer": False}, "no writer of typed columns"),
            ({"typed": False, "typed why": "a decimal wider than its type"},
             "a decimal wider than its type"),
            ({"one lane": False}, "lanes side by side")):
        with watched() as seen:
            got = _tail({**whole, **off})
        assert got.path == "records", off
        assert branch(seen, "columns") == [REFUSES]
        assert said in why(seen, "columns")


def test_a_batch_of_columns_is_written_by_either_way_and_costs_decide():
    with watched() as seen:
        got = decide.column_write({})
    assert branch(seen, "copy") == [HOLDS]
    assert branch(seen, "statements") == [HOLDS]
    assert got.path in ("copy", "statements")
    with watched() as seen:
        got = decide.column_write({}, costs={"copy": 2.0, "statements": 0.5})
    assert got.path == "statements"
    assert branch(seen, "copy") == [HOLDS]


def test_a_schedule_is_admitted_only_where_it_breaks_nothing():
    with watched() as seen:
        ok, said = decide.admit([], {}, facts=decide.Facts(
            {decide.SCHEDULE_BREAKS: []}))
    assert (ok, said) == (True, "")
    assert branch(seen, "as scheduled") == [HOLDS]
    with watched() as seen:
        ok, said = decide.admit([], {}, facts=decide.Facts(
            {decide.SCHEDULE_BREAKS: ["orders would lose its commit order"]}))
    assert not ok
    assert said == "orders would lose its commit order"
    assert branch(seen, "as scheduled") == [REFUSES]
    assert why(seen, "as scheduled") == said


def test_a_sample_first_is_taken_only_asked_measured_and_smaller():
    asked = {"asked": True, "rows": 1_000_000, "rate": 1000, "seconds": 10}
    with watched() as seen:
        got = decide.check_sample(asked)
    assert got.path == "sample"
    assert "10,000 rows" in got.rung.said
    assert branch(seen, "sample") == [HOLDS]
    assert branch(seen, "full") == [HOLDS]
    for off, said in (({"asked": False}, "no quick signal was asked"),
                      ({"rate": 0}, "rate on this pair is not measured"),
                      ({"rows": 10_000}, "the sample would be the whole")):
        with watched() as seen:
            got = decide.check_sample({**asked, **off})
        assert got.path == "full", off
        assert branch(seen, "sample") == [REFUSES]
        assert said in why(seen, "sample")


def test_a_change_depth_earns_its_level_from_the_depth_and_the_goal():
    with watched() as seen:
        got = decide.change_depth_level({"depth": "D4", "goal": "follow",
                                         "follows": True})
    assert got.path == "whole"
    assert branch(seen, "whole") == [HOLDS]
    assert branch(seen, "unread") == [REFUSES]
    assert "the goal follows the source's changes" in why(seen, "unread")
    with watched() as seen:
        got = decide.change_depth_level({"depth": "D2", "goal": "follow",
                                         "follows": True})
    assert got.path == "partial"
    assert branch(seen, "whole") == [REFUSES]
    assert "not every change whole (D4)" in why(seen, "whole")
    assert branch(seen, "partial") == [HOLDS]
    with watched() as seen:
        got = decide.change_depth_level({"depth": "D0", "goal": "copy",
                                         "follows": False})
    assert got.path == "unread"
    assert branch(seen, "unread") == [HOLDS]
    with watched() as seen:
        got = decide.change_depth_level({"depth": "D0", "goal": "follow"})
    assert got.path == "none"
    assert "was not said" in why(seen, "unread")
    assert branch(seen, "partial") == [REFUSES]
    assert "reads no changes at all" in why(seen, "partial")
    assert branch(seen, "none") == [HOLDS]

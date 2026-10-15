"""A batch of changes is collapsed change by change, or - where those rows
applied slower than the source changes - with its keys collapsed as columns
and each table's rows written as one run of each kind (`applyway`, W9).
Either way the statements, their order and the batch's one transaction are
the same."""
import random

import pytest

from migkit import applyway, canon
from migkit.config import Endpoint, Hop
from migkit.engines.base import Engine


class Rec(Engine):
    def __init__(self, hop, ordered=None):
        super().__init__(hop)
        self.ordered, self.calls = ordered, []

    def _ordered_tables(self, side, db):
        return self.ordered

    def _apply_upserts(self, side, db, table, rows):
        self.calls.append(("upsert", table, rows))

    def _apply_deletes(self, side, db, table, keys):
        self.calls.append(("delete", table, keys))


class Conn:
    def __init__(self, calls):
        self.calls = calls
        calls.append("begin")

    def commit(self):
        self.calls.append("commit")

    def rollback(self):
        self.calls.append("rollback")

    def close(self):
        pass


class Held(Rec):
    def _open_writer(self, side, db):
        return Conn(self.calls)


def _hop():
    ep = Endpoint(host="x", port=0, user="", password="")
    hop = Hop(name="r", engine="sqlite", source=ep, target=ep,
              databases=["main"])
    hop.workers = 1
    return hop


def _batch(n, seed, at=1000.0, tables=("a", "b", "c")):
    r = random.Random(seed)
    out = []
    for i in range(n):
        t, k = r.choice(tables), r.randrange(max(n // 3, 2))
        op = r.choice(["insert", "update", "update", "delete"])
        if op == "delete":
            out.append({"op": op, "table": t, "key": {"id": k}})
        else:
            new = k + 7 if r.random() < 0.01 else k
            out.append({"op": op, "table": t, "key": {"id": k},
                        "values": {"id": new, "v": f"x{i}", "n": i}})
        if i % 10 == 9:
            out[-1]["txn_end"] = True
            out[-1]["txn_ts"] = at + i * 1e-7
    return out


def _behind(eng):
    """The rows way timed slower than the source: the next batch may take
    the columns."""
    pace = applyway.state(eng)
    pace.arrived([{"txn_ts": 1.0}, {"txn_ts": 1.001}])
    pace.applied("rows", 1, 10.0)
    assert pace.behind()[0]


@pytest.mark.parametrize("ordered", [None, {}, {"a": "a"},
                                     {"a": "g", "b": "g"}])
def test_the_columns_leave_what_the_rows_leave(ordered):
    for seed in range(60):
        changes = _batch(random.Random(seed).randrange(1, 400), seed)
        eng = Rec(_hop(), ordered)
        plan = applyway.Plan.of(eng, "dst", "main", changes)
        assert isinstance(plan, applyway.Plan), plan
        assert repr(plan.net_rows()) == repr(
            eng._net_rows("dst", "main", changes))
        rows, cols = Rec(_hop(), ordered), Rec(_hop(), ordered)
        rows._apply_net("dst", "main", rows._net_rows("dst", "main",
                                                      changes))
        plan.write(cols, "dst", "main")
        assert repr(cols.calls) == repr(rows.calls), seed


def test_a_hundred_thousand_changes_climb_to_the_columns_byte_equal():
    eng, plain, said = Rec(_hop(), {}), Rec(_hop(), {}), []
    eng.lend_log(said.append)
    for b in range(4):
        changes = _batch(100_000, b, at=1000.0 + b * 0.01)
        eng.neutral_apply("dst", "main", changes)
        # the same changes with no commit time: never measured behind
        plain.neutral_apply("dst", "main",
                            [{k: v for k, v in c.items() if k != "txn_ts"}
                             for c in changes])
    assert applyway.state(eng).on == "columns"
    assert applyway.state(plain).on == "rows"
    assert repr(eng.calls) == repr(plain.calls)
    way, = [w for w in eng.measured_ways() if w["work"] == applyway.WORK
            and w["rows_from"] == 100_000]
    assert way["chosen"] == "columns"
    assert way["seconds_a_row"]["columns"] < way["seconds_a_row"]["rows"]
    switched = [s for s in said if "collapsed as columns" in s
                and "where the batch collapsed change by change" in s]
    assert len(switched) == 1 and "changes a second" in switched[0]


def test_one_batch_is_one_transaction_either_way():
    eng = Held(_hop(), {"a": "g", "b": "g"})
    batch = [{"op": "delete", "table": "b", "key": {"id": 1}},
             {"op": "delete", "table": "a", "key": {"id": 1}},
             {"op": "insert", "table": "c", "key": {"id": 2},
              "values": {"id": 2, "v": "x"}},
             {"op": "delete", "table": "c", "key": {"id": 3}}]
    eng.neutral_apply("dst", "main", list(batch))
    assert applyway.state(eng).on == "rows"
    by_rows = list(eng.calls)
    _behind(eng)
    eng.calls.clear()
    eng.neutral_apply("dst", "main", list(batch))
    assert applyway.state(eng).on == "columns"
    assert eng.calls == by_rows
    assert eng.calls[0] == "begin" and eng.calls[-1] == "commit"
    assert eng.calls.count("begin") == 1 and eng.calls.count("commit") == 1
    # the bound tables first, change by change in the source's order: the
    # child's delete before its parent's, then the loose deletes, then
    # the upserts
    assert [c[:2] for c in eng.calls[1:-1]] == [
        ("delete", "b"), ("delete", "a"), ("delete", "c"), ("upsert", "c")]


def test_a_write_refused_in_the_columns_way_fails_the_whole_batch():
    class Refusing(Held):
        def _apply_upserts(self, side, db, table, rows):
            raise RuntimeError("refused by the target")
    eng = Refusing(_hop(), {})
    eng.neutral_apply("dst", "main", [{"op": "delete", "table": "a",
                                       "key": {"id": 1}}])
    _behind(eng)
    eng.calls.clear()
    with pytest.raises(RuntimeError, match="refused"):
        eng.neutral_apply("dst", "main", [
            {"op": "delete", "table": "a", "key": {"id": 2}},
            {"op": "insert", "table": "a", "key": {"id": 3},
             "values": {"id": 3}}])
    assert "rollback" in eng.calls and "commit" not in eng.calls


@pytest.mark.parametrize("changes, said", [
    ([{"op": "update", "table": "a", "key": {"id": 1},
       "values": {"id": 1, "n": canon.Added(1, 2)}}], "adds to a value"),
    ([{"op": "insert", "table": "a", "key": {"id": 1},
       "values": {"id": 1, "v": "x"}},
      {"op": "update", "table": "a", "key": {"id": 1},
       "values": {"v": "y"}}], "carry different columns"),
    # one number carried as two types: two rows to the rows way
    ([{"op": "insert", "table": "a", "key": {"id": 1},
       "values": {"id": 1, "v": "p"}},
      {"op": "insert", "table": "a", "key": {"id": "1"},
       "values": {"id": "1", "v": "q"}}], "one type"),
    ([{"op": "insert", "table": "a", "key": {"id": 1.5},
       "values": {"id": 1.5}}], "one type"),
])
def test_a_batch_the_columns_cannot_collapse_alike_goes_by_rows(changes,
                                                                 said):
    eng = Rec(_hop(), {})
    plan = applyway.Plan.of(eng, "dst", "main", changes)
    assert isinstance(plan, str) and said in plan
    _behind(eng)
    choice = applyway.apply(eng, "dst", "main", changes)
    assert choice.path == "rows" and said in choice.reason


def test_an_unknown_change_is_refused_as_the_rows_way_refuses_it():
    eng = Rec(_hop(), {})
    _behind(eng)
    with pytest.raises(ValueError, match="unknown change op"):
        eng.neutral_apply("dst", "main", [{"op": "merge", "table": "a",
                                           "key": {"id": 1}}])


def test_keeping_up_or_unmeasured_stays_on_the_rows():
    eng = Rec(_hop(), {})
    changes = [{"op": "insert", "table": "a", "key": {"id": i},
                "values": {"id": i}} for i in range(50)]
    assert applyway.apply(eng, "dst", "main", changes).path == "rows"
    assert "no commit time" in applyway.state(eng).behind()[1]
    pace = applyway.state(eng)
    pace.arrived([{"txn_ts": 1.0}, {"txn_ts": 101.0}])
    pace.applied("rows", 1_000_000, 1.0)
    keeping, why = pace.behind()
    assert not keeping and "changes a second" in why
    choice = applyway.apply(eng, "dst", "main", changes)
    assert choice.path == "rows" and "keep up" in choice.reason


def test_the_source_rate_is_read_from_its_commit_times():
    pace = applyway.Pace()
    pace.arrived([{"op": "x"}, {"txn_ts": 10.0}] + [{}] * 9
                 + [{"txn_ts": 11.0}])
    assert pace.source_rate() == pytest.approx(10.0)
    # a batch read again after a restart, older than what was read: not
    # counted again
    pace.arrived([{"txn_ts": 10.5}])
    assert pace.source_rate() == pytest.approx(10.0)
    pace.arrived([{}] * 9 + [{"txn_ts": 12.0}])
    assert pace.source_rate() == pytest.approx(10.0)

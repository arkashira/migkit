"""How a move is run, without a server: tables side by side under a gate,
one writer at a time where the target takes one, the checkpoint written
whole, and a copy that finishes with nothing on the target - or with a
bulk result that differs from the source - not called complete.
"""
import json
import threading
import time
import types

import pytest

from migkit.config import Endpoint, Hop


def _hop(tmp_path, **kw):
    hop = Hop(name="side", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=5432, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=5432, user="u",
                              password="CHANGE_ME"),
              databases=["app"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return hop


class _Copier:
    """An engine whose tables take a moment each, counting how many are in
    flight at once."""

    def __init__(self, parallel=True, fail=None):
        self.WRITES_IN_PARALLEL = parallel
        self.now = self.most = 0
        self.lock = threading.Lock()
        self.fail = fail
        self.copied, self.started = [], []

    def through_pair(self, d, t):
        return False

    def table_facts(self, side, d):
        return {"t1": {"rows": 10}, "t2": {"rows": 1000}, "t3": {"rows": 5},
                "t4": {"rows": 1}}

    def move_table(self, d, sch, t, chunk, ck, log):
        with self.lock:
            self.started.append(t)
            self.now += 1
            self.most = max(self.most, self.now)
        time.sleep(0.2)
        with self.lock:
            self.now -= 1
            self.copied.append(t)
        if t == self.fail:
            raise RuntimeError(f"{t} broke")


TABLES = [("", "t1"), ("", "t2"), ("", "t3"), ("", "t4")]


def test_tables_go_side_by_side_largest_first(tmp_path, monkeypatch):
    from migkit import cli
    monkeypatch.setattr(cli, "_changelog", lambda *a, **k: None)
    eng = _Copier()
    cli._copy_tables(_hop(tmp_path, workers=3), eng, "app", TABLES, 100, {})
    assert sorted(eng.copied) == ["t1", "t2", "t3", "t4"]
    assert eng.most == 3, eng.most
    # the largest started first, so it is not left running alone at the end
    assert eng.started[0] == "t2" and eng.started[-1] == "t4", eng.started


def test_a_target_that_takes_one_writer_is_written_one_table_at_a_time(
        tmp_path, monkeypatch):
    from migkit import cli
    monkeypatch.setattr(cli, "_changelog", lambda *a, **k: None)
    eng = _Copier(parallel=False)
    cli._copy_tables(_hop(tmp_path, workers=4), eng, "app", TABLES, 100, {})
    assert eng.most == 1 and len(eng.copied) == 4


def test_a_table_that_fails_lets_the_others_finish_and_is_said(
        tmp_path, monkeypatch):
    from migkit import cli
    monkeypatch.setattr(cli, "_changelog", lambda *a, **k: None)
    eng = _Copier(fail="t2")
    with pytest.raises(RuntimeError, match="t2 broke"):
        cli._copy_tables(_hop(tmp_path, workers=2), eng, "app", TABLES, 100,
                         {})
    # what was in flight finished; nothing was left half-started
    assert eng.now == 0


def test_the_checkpoint_is_written_whole(tmp_path, monkeypatch):
    from migkit import cli
    ck = cli._Checkpoint(tmp_path / "move.json")
    ck["a"] = {"last": 1}
    real = json.dumps
    failed = []

    def once_changed(obj, **kw):
        if not failed:
            failed.append(1)
            raise RuntimeError("dictionary changed size during iteration")
        return real(obj, **kw)
    monkeypatch.setattr(cli.json, "dumps", once_changed)
    ck.save()
    assert failed, "the encoder was asked again after the entry changed"
    assert json.loads((tmp_path / "move.json").read_text()) == {
        "a": {"last": 1}}
    assert not (tmp_path / "move.json.tmp").exists()


def test_a_table_copy_that_left_the_target_empty_is_not_complete(
        tmp_path, monkeypatch):
    from migkit import cli
    eng = types.SimpleNamespace(moved_nothing=lambda d: ["public.orders",
                                                         "public.audit"])
    hop = _hop(tmp_path, exclude=["public.audit"])
    with pytest.raises(SystemExit) as e:
        cli._refuse_if_nothing_arrived(hop, eng, "app", "the table copy",
                                       print)
    assert str(e.value).startswith("the table copy reported success and app"
                                   " is still empty on the target:"
                                   " public.orders."), e.value
    said = []
    cli._refuse_if_nothing_arrived(
        hop, types.SimpleNamespace(moved_nothing=lambda d: None), "app",
        "the table copy", said.append)
    assert said and "cannot confirm the rows landed" in said[0]


def test_a_bulk_result_that_differs_from_the_source_is_not_complete(
        tmp_path):
    from migkit import cli
    from migkit.engines.base import Result
    eng = types.SimpleNamespace(check_data=lambda d: [
        Result("data", "app.orders", "diff", "3 rows differ"),
        Result("data", "app.lines", "ok", "equal")])
    with pytest.raises(SystemExit) as e:
        cli._held_to_the_source(_hop(tmp_path), eng, "app", print)
    assert str(e.value).startswith("the bulk copy finished and 1 of app's"
                                   " tables do not hold what the source"
                                   " does: app.orders: 3 rows differ"), e.value
    # the hop can turn the comparison off
    asked = []
    cli._held_to_the_source(
        _hop(tmp_path, options={"verify_batches": False}),
        types.SimpleNamespace(check_data=lambda d: asked.append(d)), "app",
        print)
    assert asked == []


def test_the_one_pass_load_empties_the_target_first(tmp_path):
    from migkit import movers
    hop = Hop(name="ml", engine="hetero",
              source=Endpoint(host="10.0.0.1", port=3306, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=5432, user="u",
                              password="CHANGE_ME"),
              databases=["app"],
              options={"source_engine": "mysql",
                       "target_engine": "postgres"})
    hop.report_dir = lambda db=None: tmp_path
    steps = movers.pgloader_move(hop, "app", 2, False, None)
    assert steps[0].startswith("# empty all the target's app user tables"), \
        steps


def test_ranges_are_planned_once_and_grow_with_the_source():
    from migkit import ranges
    st, saved = {}, []
    todo = ranges.plan(st, 1, 100, lambda: [25, 50, 75],
                       lambda: saved.append(1))
    assert todo == [(0, 25), (25, 50), (50, 75), (75, 100)] and saved
    ranges.finished(st, 0, lambda: None)
    ranges.finished(st, 50, lambda: None)
    assert st["last"] == 25            # done from the start as far as 25
    # asked again after the source grew past its end: a range for the rest
    todo = ranges.plan(st, 1, 130, lambda: pytest.fail("planned again"),
                       lambda: None)
    assert todo == [(25, 50), (75, 100), (100, 130)]
    # a checkpoint from before ranges counts what it had done
    old = {"last": 50}
    assert ranges.plan(old, 1, 100, lambda: [25, 50, 75],
                       lambda: None) == [(50, 75), (75, 100)]
    assert ranges.step(1_000_000, 500_000, 4) == 125_000
    assert ranges.step(60_000, 500_000, 4) == ranges.LEAST

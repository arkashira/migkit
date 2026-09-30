"""Every resource a move uses, in one model: the binding one named, the
workers sized to it by Little's law, the widest rows' memory, and each
phase's time with its reason (backlog R22).

Synthetic stages here - the rates are chosen, so the right answer is known;
the docker tests in `test_the_model_names_what_binds.py` hold the same
model to measured servers.
"""
import hashlib
import json
import math
import random
import time

import pytest

from migkit import model as M
from migkit import planner, sizing
from migkit.model import MEASURED, Model, Stage

MB = 2 ** 20


def _model(source=(50, 200), target=(20, 40), link=(100, 1000), check=None,
           **kw):
    stages = [Stage("source", "the source's reads", source[0] * MB,
                    source[1] * MB, MEASURED, side="src"),
              Stage("target", "the target's writes", target[0] * MB,
                    target[1] * MB, MEASURED, side="dst",
                    helps="a larger target instance"),
              Stage("link:dst", "the link to the target", link[0] * MB,
                    link[1] * MB, MEASURED, side="dst",
                    steps=("copy", "check") if check else ("copy",))]
    if check:
        stages.append(Stage("target-check", "the target reading back",
                            check * MB, None, MEASURED, side="dst",
                            steps=("check",)))
    return Model(stages, measured=True, **kw)


def test_the_least_stage_binds_and_littles_law_sizes_the_workers():
    m = _model()
    assert m.binding().name == "target"
    # one stream: its slowest stage, the target's 20 MB/s; the target
    # takes 40 MB/s in all - two streams carry it, a third adds nothing
    assert m.little == 2
    assert m.rate(1) == pytest.approx(20 * MB)
    assert m.rate(2) == pytest.approx(40 * MB)
    assert m.rate(8) == pytest.approx(40 * MB)
    said = " ".join(m.lines())
    assert "binds: the target's writes at ~40.0 MB/s" in said, said
    assert "2 at a time carry them - more would not help" in said
    assert "a larger target instance would" in said
    # and it can say another: a source that takes 10 MB/s in all binds
    m = _model(source=(5, 10))
    assert m.binding().name == "source"
    assert m.little == 2 and m.rate() == pytest.approx(10 * MB)


def test_a_far_link_takes_more_workers_than_any_cpu_guess():
    # 2 MB/s a stream over a long round trip, 400 MB/s in all: the target's
    # 40 MB/s takes twenty streams to fill, each mostly waiting on the link
    m = _model(link=(2, 400))
    assert m.binding().name == "target"
    assert m.little == 20
    plan = sizing.estimate(_Hop(), None, "app", here={"cpus": 2,
                                                      "memory": 32 * 2 ** 30},
                           sides={"src": {"cpus": 2, "free_connections": 400},
                                  "dst": {"cpus": 2,
                                          "free_connections": 400}},
                           model=m)
    # the CPUs' guess would have started it at 2; the model starts it where
    # the target fills, within what the connections allow
    assert plan.start == 20, plan.factors
    assert any("Little's law" in why for why in plan.factors)
    # nothing measured: the guesses stand as before
    plan = sizing.estimate(_Hop(), None, "app", here={"cpus": 2,
                                                      "memory": 32 * 2 ** 30},
                           sides={"src": {"cpus": 2, "free_connections": 400},
                                  "dst": {"cpus": 2,
                                          "free_connections": 400}},
                           model=Model())
    assert plan.start == 2


def test_the_check_after_each_range_is_on_the_path():
    # the target reads each range back at 40 MB/s a stream, and the link
    # carries every byte twice
    m = _model(check=40)
    s = m.seconds_per_byte() * MB
    assert s == pytest.approx(1 / 20 + 1 / 40)
    assert m.stages["link:dst"].bound() == pytest.approx(500 * MB)
    assert m.little == 3


def test_reads_and_writes_are_sized_apart():
    m = _model(source=(50, 200), target=(10, 40))
    # a dump fills the source's 200 MB/s with four streams of 50; a load
    # fills the target's 40 with four of 10 - and the coupled copy with 4
    assert m.read_workers == 4
    assert m.write_workers == 4
    m = _model(source=(25, 200), target=(20, 40))
    assert (m.read_workers, m.write_workers) == (8, 2)


def test_a_resource_that_scaled_all_the_way_is_a_floor_and_said_so():
    got = M.scaled(10, 12, 4)
    assert got[:2] == (12, False)
    total, floor, how = M.scaled(10, 38, 4, cpus=16)
    assert (round(total), floor) == (152, False) and "16 CPUs" in how
    assert M.scaled(10, 38, 4)[:2] == (38, True)
    m = Model([Stage("source", "the source's reads", 10, 38, MEASURED,
                     at_least=True, side="src")], measured=True)
    assert "nothing found to bind" in " ".join(m.lines())


def test_what_a_worker_holds_follows_the_widest_row():
    class Eng:
        COPY_MEMORY = {"own": 96 * MB, "per_row": 9.0, "batch": 64 * MB}
    sizes = {"blobs": {"largest": 250 * MB, "exact": True},
             "narrow": {"largest": 200, "exact": False}}
    held, widest = M.worker_memory(Eng(), sizes)
    assert widest == ("blobs", 250 * MB, True)
    assert held == 96 * MB + 9 * 250 * MB
    held, widest = M.worker_memory(Eng(), sizes, ["narrow"])
    assert widest[0] == "narrow" and held == 96 * MB + 9 * 64 * MB
    m = Model(worker_memory=96 * MB + 9 * 250 * MB,
              widest=("blobs", 250 * MB, True))
    assert m.too_wide(8 * 2 ** 30) is None
    said = m.too_wide(2 * 2 ** 30)
    assert said and "blobs" in said and "nothing is cut short" in said
    # the sizing holds the move to what fits: 6 GB free, 2.3 GB a worker
    plan = sizing.estimate(_Hop(), None, "app",
                           here={"cpus": 16, "memory": 6 * 2 ** 30},
                           sides={"src": {"cpus": 16}, "dst": {"cpus": 16}},
                           model=m)
    assert plan.most == 2, plan.factors
    assert any("rows of up to 250.0 MB (blobs)" in why
               for why in plan.factors)


def test_the_models_costs_and_facts_reach_the_decision_layer():
    from migkit import decide
    costs, facts = decide.from_model(_model())
    assert costs["target"] == pytest.approx(1 / 20)
    assert costs["source"] == pytest.approx(1 / 50)
    assert facts["binding"] == "target" and facts["workers"] == 2
    # a rung whose need asks the model is climbed past when it does not hold
    rungs = (decide.Rung("wide", "the way that needs the link to bind",
                         needs=(decide.Need("the link binding",
                                            lambda f: f["binding"]
                                            == "link:dst"),)),
             decide.Rung("plain", "the plain way"))
    got = decide.climb("t", rungs, facts)
    assert got.path == "plain"


def test_the_model_is_kept_and_read_back(tmp_path):
    m = _model(check=40, arrival=(120.0, MEASURED, "x"),
               apply=(5000.0, MEASURED, "y"))
    m.planned = planner.phases(None, m, {"t": {"rows": 10 ** 6,
                                                "bytes": 400 * MB,
                                                "key": True}}, ["t"])
    m.save(tmp_path)
    got = M.load(tmp_path)
    assert got.binding().name == "target" and got.little == m.little
    assert [p.name for p in got.planned] == [p.name for p in m.planned]
    assert got.arrival[0] == 120.0


class _Probed:
    """An engine whose probes answer set rates: the source 50 MB/s a stream
    and 50 in all (saturated), the target 20 a stream and 20 in all, links
    100 a stream and scaling, rows of 1,000 bytes."""
    COPY_MEMORY = {"own": 64 * MB, "per_row": 2.0, "batch": 64 * MB}
    COPIES_IN_PROCESSES = True

    def __init__(self, wide=1000):
        self.wide, self.written = wide, []

    def table_facts(self, side, db):
        return {"t": {"rows": 1_000_000, "bytes": 1000 * MB, "key": True},
                "small": {"rows": 10, "bytes": 10_000, "key": True}}

    def row_sizes(self, side, db, tables=None):
        return {"t": {"largest": self.wide, "p99": 1000, "mean": 1000,
                      "exact": False},
                "small": {"largest": 900, "exact": True}}

    def pressure(self, side, db):
        return {"cpus": 4, "free_connections": 200}

    def changes_made(self, side, db):
        return (360_000, 3600.0)

    def probe_read(self, side, db, table, part=0, parts=1, budget=0,
                   seconds=0, keep=0):
        assert table == "t"
        each = 50 * MB if part == 0 else 50 * MB / (parts - 1)
        return {"rows": 1000, "bytes": MB, "seconds": MB / each,
                "sample": (b"x" * 999 + b"\n") * 1024 if keep else b""}

    def probe_link(self, side, db, nbytes):
        return nbytes / (100 * MB)

    def probe_write(self, db, table, sample, seconds=0, stream=0,
                    apply=True):
        self.written.append((stream, apply))
        each = 20 * MB if stream == 0 else 5 * MB
        return {"rows": 1000, "bytes": MB, "seconds": MB / each,
                "read_seconds": MB / (100 * MB), "applied": 1000,
                "apply_seconds": 0.1, "how": "a table rolled back"}


def _probe_hop(**options):
    from migkit.config import Endpoint, Hop
    return Hop(name="p", engine="postgres",
               source=Endpoint(host="127.0.0.1", port=1),
               target=Endpoint(host="127.0.0.1", port=1),
               options=options)


def test_the_probes_make_the_stages_and_name_what_binds():
    eng = _Probed()
    m = M.build(_probe_hop(), eng, "app", probe=True)
    assert m.measured and m.binding().name == "target", m.lines()
    src, dst = m.stages["source"], m.stages["target"]
    assert src.per_stream == pytest.approx(50 * MB)
    assert src.total == pytest.approx(50 * MB) and not src.at_least
    # the target writes 20 MB/s in all, and reads each range back at 100:
    # 20 x (1 / (1/20 + 1/100)) / 20 MB/s of the move
    assert dst.bound() == pytest.approx(20 * MB * (1 / (1 / 20 + 1 / 100))
                                        / 20)
    assert m.stages["link:dst"].steps == ("copy", "check")
    assert m.stages["link:src"].at_least
    # one stream, four side by side, and the upserts on a stream of their
    # own after them
    assert eng.written == [(0, False), (1, False), (2, False), (3, False),
                           (4, False), (5, True)]
    assert m.arrival[0] == pytest.approx(100.0)
    assert m.apply[0] == pytest.approx(10_000.0)
    assert m.per_row == pytest.approx(MB / 1000)
    # without the check after each range the target only writes
    m = M.build(_probe_hop(verify_batches=False), _Probed(), "app",
                probe=True)
    assert m.stages["target"].bound() == pytest.approx(20 * MB)
    assert "target-check" not in m.stages
    assert m.stages["link:dst"].steps == ("copy",)
    # too small to probe, and nothing measured before: no stage at all
    m = M.build(_probe_hop(), _Probed(), "app", probe=False)
    assert not m.measured and m.binding() is None and m.little is None


def test_a_move_too_small_to_probe_goes_by_an_earlier_runs_stages(
        tmp_path, monkeypatch):
    import dataclasses

    from migkit import config
    monkeypatch.setattr(config, "REPORTS", tmp_path)
    hop = _probe_hop()
    M.build(hop, _Probed(), "app", probe=True).save(hop.report_dir("app"))
    m = M.build(hop, _Probed(), "app", probe=False)
    assert m.measured and m.binding().name == "target"
    assert m.binding().truth == M.EARLIER
    assert m.per_row == pytest.approx(MB / 1000)
    # a hop of the same engines that never ran goes by the one that did -
    # a rehearsal's numbers for the migration it rehearses
    planner.record_rate(hop, "builtin", 1000, 1.0)
    prod = dataclasses.replace(hop, name="prod")
    assert M.build(prod, _Probed(), "app", probe=False).binding().truth \
        == M.EARLIER
    # and not a hop of other engines
    other = dataclasses.replace(hop, name="other", engine="mysql")
    assert not M.build(other, _Probed(), "app", probe=False).measured


def test_rows_too_wide_to_hold_are_not_probed():
    m = M.build(_probe_hop(), _Probed(wide=64 * MB), "app", probe=True)
    assert not m.stages and any("narrower than" in n for n in m.notes)
    assert m.widest == ("t", 64 * MB, False)
    assert m.worker_memory == 64 * MB + 2 * 64 * MB


# --- the copier's own way, and what it measured of itself --------------------

class _Passing(_Probed):
    """The same servers, the copier's ranges passing server to server and
    checked beside the next range's load."""
    PASS_MEMORY = {"own": 48 * MB, "per_row": 1.0, "batch": 64 * 1024}
    CHECKS_BESIDE = True

    def passes_through(self, db, table):
        return True


def test_ranges_passed_server_to_server_are_modelled_as_they_go():
    m = M.build(_probe_hop(), _Passing(), "app", probe=True)
    assert m.overlapped and "server to server" in m.path
    # the source reads each byte twice - the copy and its digest - so its
    # 50 MB/s in all carries 25 of the move
    assert m.stages["source"].steps == ("copy", "check")
    assert m.stages["source"].bound() == pytest.approx(25 * MB)
    # only the digests' two numbers cross back: the link carries a byte once
    assert m.stages["link:dst"].steps == ("copy",)
    # a stream's check runs beside the next range's load: the longer of
    # the two, not their sum
    assert m.seconds_per_byte() == pytest.approx(
        max(1 / (20 * MB), 1 / (50 * MB), 1 / (100 * MB)))
    # this machine only passes bytes on: nothing tallied here, so its CPU
    # is not what binds
    assert m.binding().name != "cpu"
    # through migkit, the same servers: read back and tallied here
    m = M.build(_probe_hop(), _Probed(), "app", probe=True)
    assert not m.overlapped and "through migkit" in m.path
    assert m.seconds_per_byte() == pytest.approx(
        1 / (20 * MB) + 1 / (100 * MB))


def test_a_server_read_through_a_narrow_link_is_the_links_speed():
    # the source read at 7.1 MB/s one stream and four, and its link
    # carried 8 in all: what held the reads was the link
    m = Model([Stage("source", "the source's reads", 7.1 * MB, 7.1 * MB,
                     MEASURED, side="src", steps=("copy", "check")),
               Stage("link:src", "the link to the source", 8 * MB, 8 * MB,
                     MEASURED, side="src"),
               Stage("target", "the target's writes", 30 * MB, 90 * MB,
                     MEASURED, side="dst")], measured=True)
    M._held_by_links(m)
    assert m.stages["source"].through_link and m.stages["source"].at_least
    assert m.binding().name == "link:src"
    assert m.per_stream_binding().name == "link:src"
    # and a slow server behind a wide link is the server's own
    m = Model([Stage("source", "the source's reads", 5 * MB, 5 * MB,
                     MEASURED, side="src"),
               Stage("link:src", "the link to the source", 100 * MB,
                     400 * MB, MEASURED, side="src")], measured=True)
    M._held_by_links(m)
    assert not m.stages["source"].through_link
    assert m.binding().name == "source"


def test_streams_held_by_this_processs_one_core_are_a_floor():
    class Processes:
        COPIES_IN_PROCESSES = True

    class Threads:
        COPIES_IN_PROCESSES = False

    def stage():
        return Stage("source", "the source's reads", 5 * MB, 6 * MB,
                     MEASURED, side="src")
    got = stage()
    M._one_core(Processes(), got, 0.95)
    assert got.at_least and "one core" in got.detail
    # the copier's own workers share this process too: what was measured
    # is what the move gets
    got = stage()
    M._one_core(Threads(), got, 0.95)
    assert not got.at_least
    # a process mostly waiting on the server was not held by its core
    got = stage()
    M._one_core(Processes(), got, 0.3)
    assert not got.at_least
    # and the share is this process's CPU over the time the streams ran
    M._side_by_side(lambda i: sum(range(300_000)), 2)
    assert M._side_by_side.busy > 0.5
    M._side_by_side(lambda i: time.sleep(0.2), 2)
    assert M._side_by_side.busy < 0.2


def test_a_worker_holds_what_its_way_holds():
    sizes = {"t": {"largest": 300 * MB, "exact": True}}
    held, _ = M.worker_memory(_Passing(), sizes, db="app")
    assert held == 48 * MB + 300 * MB
    held, _ = M.worker_memory(_Probed(), sizes, db="app")
    assert held == 64 * MB + 2 * 300 * MB
    # the way not asked (no database): as through migkit
    held, _ = M.worker_memory(_Passing(), sizes)
    assert held == 64 * MB + 2 * 300 * MB


def test_what_a_move_measured_of_itself_sizes_the_next(tmp_path,
                                                       monkeypatch):
    from migkit import config
    monkeypatch.setattr(config, "REPORTS", tmp_path)
    hop = _probe_hop()
    m = M.build(hop, _Probed(), "app", probe=True)
    ways = [{"work": "range copy", "table": "t", "rows_from": None,
             "seconds_a_row": {"pass-through": 4e-6, "pipe": 9e-6},
             "chosen": "pass-through"},
            {"work": "range copy", "table": "u", "rows_from": None,
             "seconds_a_row": {"pipe": 6e-6}, "chosen": None},
            {"work": "table copy, a byte", "table": None, "rows_from": None,
             "seconds_a_row": {"ranges": 1e-9}, "chosen": "ranges"}]
    m.observe(ways, 1_000_000, 10.0, 6)
    assert m.observed["seconds_a_row"] == pytest.approx(6e-6)
    assert m.observed["rows_a_second"] == pytest.approx(100_000)
    m.save(hop.report_dir("app"))
    again = M.build(hop, _Probed(), "app", probe=False)
    # one stream's time is the ranges' own now, not the probes' sum
    assert again.seconds_per_byte() == pytest.approx(6e-6 / again.per_row)
    assert again.little == math.ceil(again.ceiling() * 6e-6 / again.per_row)


def test_packets_sent_again_while_a_link_is_probed_are_said():
    assert M._loss((10_000, 10), (15_000, 210)) == pytest.approx(0.04)
    # too few sent to say, or no reading
    assert M._loss((0, 0), (500, 100)) is None
    assert M._loss(None, (15_000, 210)) is None


def test_a_pace_counts_the_work_to_its_last_unit():
    clock = _Clock()
    pace = sizing.Pace(2, 4, clock=clock)
    clock.t = 5
    pace.done(1000, 1)
    clock.t = 9
    assert pace.took() == 5


class _Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def test_the_copy_from_the_table_copys_own_rate_before(tmp_path,
                                                        monkeypatch):
    import dataclasses

    from migkit import config
    from migkit.config import Endpoint, Hop
    monkeypatch.setattr(config, "REPORTS", tmp_path)
    hop = Hop(name="h", engine="postgres", source=Endpoint(host="x"),
              target=Endpoint(host="y"))
    # the engines keep the table copy's cost a byte by way
    planner.record_rate(hop, "table way/ranges", 100 * MB, 4.0)
    planner.record_rate(hop, "table way/streaming", 100 * MB, 2.0)
    got = planner.phases(hop, Model(), _facts())
    assert got[0].p50 == pytest.approx(500 / 50)
    assert got[0].p90 == pytest.approx(500 / 25)
    assert "earlier run" in got[0].truth
    # a hop of the same engines that has not run goes by it
    other = dataclasses.replace(hop, name="prod")
    got = planner.phases(other, Model(), _facts())
    assert got[0].p50 == pytest.approx(10.0) and "of h" in got[0].truth
    # and one of other engines does not
    mysql = dataclasses.replace(hop, name="m", engine="mysql")
    assert planner.phases(mysql, Model(), _facts())[0].p50 is None


def test_index_builds_beside_the_copy_leave_only_the_rest_after_it():
    m = _model()
    facts = {"a": {"rows": 4_000_000, "bytes": 400 * MB, "key": True,
                   "index_bytes": 160 * MB},
             "b": {"rows": 2_000_000, "bytes": 200 * MB, "key": True,
                   "index_bytes": 40 * MB}}
    got = {p.name: p for p in planner.phases(None, m, facts, n=2)}
    # 200 MB of indexes at 40 MB/s is 5s of builds; the copy is 15s, the
    # first table done at 10s: all but the last table's 1s is built beside
    assert got["copy"].p50 == pytest.approx(15.0)
    assert got["index builds"].p50 == pytest.approx(1.0)
    assert "beside the next tables' copy" in got["index builds"].detail
    # a bulk path loads first and builds after: all of it
    got = {p.name: p for p in planner.phases(None, m, facts, n=2,
                                              via="pgdump")}
    assert got["index builds"].p50 == pytest.approx(5.0)


def test_the_cutover_is_what_this_hops_last_one_took_or_its_budget(
        tmp_path):
    import json as _json

    class Hop:
        name = "h"
        engine = "postgres"
        options = {"cutover": {"budget": 12}}

        def report_dir(self, db=""):
            return tmp_path
    m = _model(arrival=(100.0, MEASURED, ""), apply=(20_000.0, MEASURED, ""))
    got = {p.name: p for p in planner.phases(Hop(), m, _facts(), n=2)}
    assert got["cutover"].p90 == 12.0 and "12s budget" in got["cutover"].detail
    (tmp_path / "cutover-runs.jsonl").write_text(_json.dumps(
        {"state": "flipped", "outage": 3.25}) + "\n")
    got = {p.name: p for p in planner.phases(Hop(), m, _facts(), n=2)}
    assert got["cutover"].p50 == 3.25
    assert got["cutover"].truth == "measured on an earlier run"


def test_init_says_the_time_from_the_model(tmp_path, monkeypatch):
    from migkit import config, guide
    monkeypatch.setattr(config, "REPORTS", tmp_path)

    class Eng:
        def table_facts(self, side, db):
            return _facts()
    monkeypatch.setattr(M, "build", lambda *a, **k: _model())
    lines = guide._eta(_probe_hop(), {"eng": Eng(), "dbs": ["app"]})
    said = "\n".join(lines)
    assert "binds: the target's writes" in said
    assert "time: copy ~" in said and "time: check" in said
    # nothing measured: nothing said, and the older lines stand
    monkeypatch.setattr(M, "build", lambda *a, **k: Model())
    assert guide._eta(_probe_hop(), {"eng": Eng(), "dbs": ["app"]}) == []


# --- the phases and their time ---------------------------------------------

def _facts():
    return {"big": {"rows": 4_000_000, "bytes": 400 * MB, "key": True,
                    "index_bytes": 80 * MB},
            "keyless": {"rows": 1_000_000, "bytes": 100 * MB, "key": False,
                        "index_bytes": 0}}


def test_the_copy_is_the_work_over_the_rate_or_its_longest_table():
    m = _model()
    got = {p.name: p for p in planner.phases(None, m, _facts(), n=2)}
    # 500 MB at 40 MB/s, the keyless table one stream of 20 MB/s: 5s
    assert got["copy"].p50 == pytest.approx(500 / 40)
    assert got["copy"].binds == "the target's writes"
    assert got["copy"].p90 >= got["copy"].p50 * 1.2
    # the indexes at the target's rate: 80 MB at 40 MB/s
    assert got["index builds"].p50 == pytest.approx(2.0)
    assert "assumed" in got["index builds"].truth
    # a keyless table larger than the rest goes alone, one stream at a time
    facts = _facts()
    facts["keyless"]["bytes"] = 2000 * MB
    got = {p.name: p for p in planner.phases(None, _model(), facts, n=2)}
    assert got["copy"].p50 == pytest.approx(2000 / 20)
    assert got["copy"].binds == "keyless, one range at a time"


def test_a_catch_up_slower_than_the_changes_never_ends_and_says_why():
    got = planner.catch_up_sum(10_000, 500, 400, MEASURED)
    assert got[0].never and got[1].never
    assert "never catches up" in got[0].line()
    assert "a larger target instance" in got[0].line()
    got = planner.catch_up_sum(10_000, 500, 2500, MEASURED)
    assert not got[0].never
    assert got[0].p50 == pytest.approx(10_000 / 2000)
    # at half the rate: 10,000 / (1250 - 500)
    assert got[0].p90 == pytest.approx(10_000 / 750)
    got = planner.catch_up_sum(10_000, 500, 900, MEASURED)
    assert got[0].p90 is None and "make it sure" in got[0].detail


def test_the_catch_up_from_the_model():
    m = _model(arrival=(100.0, MEASURED, ""), apply=(20_000.0, MEASURED, ""))
    got = {p.name: p for p in planner.phases(None, m, _facts(), n=2)}
    behind = got["copy"].p50 + got["index builds"].p50
    assert got["catch-up"].p50 == pytest.approx(100 * behind / 19_900)
    m = _model(arrival=(100.0, MEASURED, ""), apply=(80.0, MEASURED, ""))
    got = {p.name: p for p in planner.phases(None, m, _facts(), n=2)}
    assert got["catch-up"].never


def test_without_a_measurement_no_time_is_made_up(tmp_path, monkeypatch):
    from migkit import config
    from migkit.config import Endpoint, Hop
    monkeypatch.setattr(config, "REPORTS", tmp_path)
    hop = Hop(name="h", engine="postgres", source=Endpoint(host="x"),
              target=Endpoint(host="y"))
    got = planner.phases(hop, Model(), _facts())
    assert got[0].name == "copy" and got[0].p50 is None
    assert "nothing on this path measured" in got[0].line()
    planner.record_rate(hop, "builtin", 1_000_000, 10.0)
    got = planner.phases(hop, Model(), _facts())
    assert got[0].p50 == pytest.approx(5_000_000 / 100_000)
    assert "earlier run" in got[0].truth


def test_the_live_catch_up_is_the_sum_of_what_arrives_and_what_is_applied():
    clock = [0.0]
    arrived, applied, held = [0], [0], [10 * MB]

    def run(arrive, apply, shrink, steps=5, behind=120):
        cu = planner.CatchUp(lambda: arrived[0], lambda: applied[0],
                             lambda: held[0], lambda: behind,
                             clock=lambda: clock[0])
        for _ in range(steps):
            cu.sample()
            clock[0] += 10
            arrived[0] += arrive * 10
            applied[0] += apply * 10
            held[0] = max(held[0] - shrink * 10, 0)
        cu.sample()
        return cu.phases()
    got = run(100, 50, -2000)
    assert got[0].never and "never catches up" in got[0].line()
    held[0] = 10 * MB
    got = run(100, 400, 64 * 1024)
    assert not got[0].never
    assert got[0].p50 == pytest.approx(held[0] / (64 * 1024), rel=0.01)
    got = run(100, 100, 0, behind=2)
    assert got[0].p50 == 0 and "keeping up" in got[0].detail


# --- the memory this machine has --------------------------------------------

def test_a_containers_memory_limit_is_what_it_may_use(tmp_path):
    (tmp_path / "memory.max").write_text("2147483648\n")
    (tmp_path / "memory.current").write_text("1073741824\n")
    (tmp_path / "memory.stat").write_text("anon 900\ninactive_file"
                                          " 104857600\n")
    assert sizing._cgroup_memory(str(tmp_path)) == (2 * 2 ** 30,
                                                    2 ** 30 - 100 * MB)
    # psutil reads the host's inside a container: the least of the two
    assert sizing._memory(str(tmp_path)) <= 1024 * MB + 100 * MB
    (tmp_path / "memory.max").write_text("max\n")
    assert sizing._cgroup_memory(str(tmp_path)) == (None, None)
    for name in ("memory.max", "memory.current", "memory.stat"):
        (tmp_path / name).unlink()
    (tmp_path / "memory").mkdir()
    (tmp_path / "memory" / "memory.limit_in_bytes").write_text("3221225472")
    (tmp_path / "memory" / "memory.usage_in_bytes").write_text("1073741824")
    assert sizing._cgroup_memory(str(tmp_path)) == (3 * 2 ** 30, 2 ** 30)
    assert sizing.process_memory() > 10 * MB


def test_the_memory_watchdog_holds_workers_back_before_the_kernel_acts():
    free, ours = [4 * 2 ** 30], [200 * MB]
    mem = sizing.Memory(1024 * MB, read=lambda: free[0],
                        ours=lambda: ours[0], clock=lambda: time.monotonic())
    mem.FRESH = 0
    # (4 GB + 200 MB - 512 MB) / 1 GB: three fit
    assert mem.fits(1) == 3
    assert mem.room(2) and not mem.room(3)
    assert mem.signal() is None
    # the workers grew past the estimate - 2.4 GB held by two - and what
    # each was seen to hold counts from then on: (1 + 2.4 - 0.5) / 1.2
    ours[0], free[0] = int(2.4 * 2 ** 30), 2 ** 30
    assert mem.fits(2) == 2 and not mem.room(2)
    # memory running out under them: at most one
    free[0] = 300 * MB
    s = mem.signal()
    assert s is not None and s.at_most == 1 and "300.0 MB free" in s.why
    pace = sizing.Pace(4, 8, memory=mem)
    assert pace.admit(0) and not pace.admit(1)


# --- the copier's tally over a row of any length -------------------------------

def test_a_row_the_chunks_split_is_tallied_the_same_and_in_one_pass():
    from migkit.tally import Tally
    rng = random.Random(5)
    for _ in range(500):
        data = bytes(rng.choice(b"ab\n") for _ in range(rng.randint(0, 80)))
        whole = Tally()
        for line in data.split(b"\n")[:-1] + (
                [data.split(b"\n")[-1]] if data.split(b"\n")[-1] else []):
            whole.row(line)
        cut = Tally()
        at = 0
        while at < len(data):
            step = rng.randint(1, 9)
            cut.feed(data[at:at + step])
            at += step
        assert cut.end().value() == whole.value(), data
    # a 64 MB row in 64 KB pieces: hashed as it comes, 0.11s measured here.
    # Joined to every next piece before, it took 16.5s and held the row
    # twice over
    row = b"x" * (64 * MB)
    t = Tally()
    began = time.monotonic()
    for at in range(0, len(row), 1 << 16):
        t.feed(row[at:at + (1 << 16)])
    t.feed(b"\n")
    took = time.monotonic() - began
    assert t.end().value() == (1, int(hashlib.md5(row).hexdigest()[:16], 16))
    assert took < 2.0, took


def test_a_long_value_is_tallied_as_its_rendering_in_pieces(monkeypatch):
    from migkit import tally
    monkeypatch.setattr(tally, "PIECE", 5)
    rng = random.Random(1)
    some_b = [b"'", b'"', b"\\", b"a", b"\n", b"\x00", b"\xff", b"\t"]
    some_s = ["'", '"', "\\", "a", "\n", "\x00", "é", "\U0001f600",
              "\x7f", "\t"]
    for _ in range(20000):
        row = []
        for _ in range(rng.randint(1, 3)):
            k, n = rng.random(), rng.randint(0, 20)
            if k < 0.4:
                row.append(b"".join(rng.choice(some_b) for _ in range(n)))
            elif k < 0.7:
                row.append("".join(rng.choice(some_s) for _ in range(n)))
            elif k < 0.8:
                row.append(bytearray(b"".join(rng.choice(some_b)
                                              for _ in range(n))))
            else:
                row.append(rng.choice([None, 1, 2.5]))
        t = tally.Tally()
        t.row(row)
        want = int(hashlib.md5(repr(tuple(row)).encode()).hexdigest()[:16],
                   16)
        assert t.total == want, row
    # and it can say different: one byte changed in a long value
    a, b = tally.Tally(), tally.Tally()
    a.row((1, b"x" * 50))
    b.row((1, b"x" * 49 + b"y"))
    assert a.value() != b.value()


def test_watch_says_the_time_left_what_holds_it_and_what_comes_after(
        tmp_path):
    from migkit import cli

    class Hop:
        def report_dir(self, db=""):
            return tmp_path
    m = _model(arrival=(100.0, MEASURED, ""), apply=(20_000.0, MEASURED, ""))
    m.planned = planner.phases(None, m, _facts(), n=2)
    m.save(tmp_path)
    pace = sizing.Pace(2, 4, resources={
        "the target's writes": lambda: "3 of 4 sessions waiting on storage"})
    pace.recent, pace.total = 2000.0, 1_000_000
    for lim in pace.limits.values():
        lim.update(4, pace.CUT)
    planner.progress(tmp_path, pace, 5_000_000, m)
    said = cli._move_said(Hop(), "app")
    assert said[0].startswith("move: 2 at a time, ~33m20s of the copy left"
                              " at the recent 2,000 rows/s, the target's"
                              " writes binding; held down by the target's"
                              " writes: 3 of 4 sessions waiting on storage")
    assert any(s.startswith("then index builds ~") for s in said)
    assert any(s.startswith("then catch-up under a second") for s in said)


# --- the metrics -----------------------------------------------------------

def test_the_metrics_say_what_binds_and_how_long_is_left(tmp_path):
    from migkit import ui
    where = tmp_path / "hop" / "app"
    where.mkdir(parents=True)
    m = _model(arrival=(100.0, MEASURED, ""), apply=(80.0, MEASURED, ""),
               worker_memory=300 * MB)
    m.planned = planner.phases(None, m, _facts(), n=2)
    m.save(where)
    pace = sizing.Pace(2, 4)
    pace.recent, pace.total = 1000.0, 500_000
    planner.progress(where, pace, 5_000_000, m)
    got = "\n".join(ui._move_metrics(tmp_path / "hop", 'hop="hop"'))
    assert ('migkit_resource_binding{hop="hop",db="app",'
            'resource="the target\'s writes"} 1') in got
    assert 'migkit_move_phase_never{hop="hop",db="app",phase="catch-up"} 1' \
        in got
    assert ('migkit_move_eta_seconds{hop="hop",db="app",phase="copy",'
            'quantile="live"} 4500') in got
    assert 'migkit_move_workers{hop="hop",db="app"} 2' in got
    assert 'migkit_worker_memory_bytes{hop="hop",db="app"} 314572800' in got
    assert json.loads((where / "progress.json").read_text())["rate"] == 1000


class _Hop:
    workers, workers_set = 4, False

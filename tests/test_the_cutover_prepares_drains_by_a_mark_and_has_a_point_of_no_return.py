"""The cutover's steps added after its first version, without a server:
the stream back made ahead and only started at the flip, the drain by a
mark where the stream keeps no position, a stream's own positions waited
for after it, what a stream leaves taken away once the flip is done, and
the point of no return as a step of its own - each in its place in the
order, and each put back where the path goes back.

The servers are in `test_the_cutover_on_postgresql.py` (the stream back
made ahead, measured against one made at the flip, and the point of no
return), `test_a_cutover_without_a_position_drains_by_a_mark.py` (a MySQL
replica without GTIDs) and the engines' own hold tests.
"""
import json
import time

import pytest

from migkit import cutover, decide
from migkit.config import Endpoint, Hop


def _hop(tmp_path, dbs=("app",), **options):
    options.setdefault("state", {"mirror": str(tmp_path / "mirror")})
    hop = Hop(name="cx", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=5432, user="migkit",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=5432, user="migkit",
                              password="CHANGE_ME"),
              databases=list(dbs), options=options)
    root = tmp_path / "reports"

    def report_dir(db=""):
        d = root / db if db else root
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    return hop


# --- which drain ---------------------------------------------------------------

def test_the_drain_falls_back_to_a_mark_only_where_migkit_may_write_one(
        tmp_path, monkeypatch):
    from migkit import drainmark
    hop = _hop(tmp_path, cutover={"budget": 30})
    conf = cutover.settings(hop)
    none = {"app": (None, None, 0.0, "the stream keeps no position migkit"
                                     " can hold it to")}
    have = {"app": ("0/10", True, 0.1, "")}
    leg = cutover.Leg("pipeline")
    monkeypatch.setattr(drainmark, "able", lambda hop, eng: "")
    assert cutover.drain_way(hop, None, leg, conf, have).rung.name == \
        "position"
    assert cutover.drain_way(hop, None, leg, conf, none).rung.name == "mark"
    # a stream behind is not a stream without a position: no mark
    behind = {"app": ("0/10", False, 1.0, "")}
    assert cutover.drain_way(hop, None, leg, conf, behind).rung.name == \
        "position"
    # without cutover: migkit writes nothing to the source
    got = cutover.drain_way(hop, None, leg, None, none)
    assert got.rung is None and "without cutover:" in got.reason, got
    # nor under the operator's own stop
    op = _hop(tmp_path, cutover={"freeze": "operator"})
    got = cutover.drain_way(op, None, leg, cutover.settings(op), none)
    assert got.rung is None and "freeze: operator" in got.reason, got
    monkeypatch.setattr(drainmark, "able", lambda hop, eng: "no mark of"
                        " migkit's is written on or read back from a redis"
                        " source")
    got = cutover.drain_way(hop, None, leg, conf, none)
    assert got.rung is None and "redis source" in got.reason, got


def test_a_mark_can_be_written_only_where_both_sides_keep_one(tmp_path):
    from migkit import drainmark

    class Side:
        def __init__(self, kind):
            self.CANON_ENGINE = kind

    class Pair:
        def __init__(self, a, b):
            self.src_engine, self.dst_engine = Side(a), Side(b)
    hop = _hop(tmp_path)
    assert drainmark.able(hop, Pair("mysql", "postgres")) == ""
    assert "redis source" in drainmark.able(hop, Pair("redis", "mysql"))
    assert "kafka target" in drainmark.able(hop, Pair("mysql", "kafka"))
    with pytest.raises(ValueError):
        drainmark._safe("x'; drop table t; --")


# --- the path, with a stand-in for every server ------------------------------

def _path(monkeypatch, tmp_path, *, proof="", fail_at=None, dbs=("app",),
          fence=False):
    import migkit.config as cfg
    from migkit import drainmark, freeze, jobs
    from migkit.engines.base import RepairAction, Result
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    log = []
    hop = _hop(tmp_path, dbs, cutover={"budget": 60}, reverse="at_cutover",
               fence_timeout=5)

    class Eng:
        def maps_columns(self):
            return False

        def src_lsn(self, d):
            return "0/10"

        def fence_wait(self, d, pos, timeout=0):
            return True

        def check_data(self, d):
            return [Result("data", d, "ok", "")]

        def repair_plan(self, d, kind):
            return [RepairAction(d, "sequences", ["select 1"], ["select 0"],
                                 "1 set")]

        def apply(self, d, action):
            log.append(f"counters {action.statements[0]}")

        def snapshot_state(self, d, where, kind):
            (where / "dst-sequences.txt").write_text("")

        def apply_replication_stmt(self, side, d, stmt):
            log.append(f"left {stmt}")
    eng = Eng()

    def choose(hop, eng, dbs, how="auto", need=(), side="src"):
        name = "pg_database" if freeze.OURS in need else "pg_server"
        return decide.Choice("the freeze", decide.Rung(name, name), "why")
    monkeypatch.setattr(freeze, "choose", choose)
    monkeypatch.setattr(freeze, "freeze_source",
                        lambda hop, eng, dbs, say, how: log.append(
                            f"freeze {how}"))
    monkeypatch.setattr(freeze, "proof", lambda *a: [(True, "held")])
    monkeypatch.setattr(freeze, "thaw_source", lambda *a: log.append("thaw"))
    monkeypatch.setattr(freeze, "freeze", lambda *a: log.append("hold"))
    monkeypatch.setattr(freeze, "thaw", lambda *a: log.append("release"))
    monkeypatch.setattr(jobs, "inventory", lambda *a: [])
    monkeypatch.setattr(jobs, "switch", lambda *a: 0)
    monkeypatch.setattr(jobs, "undo", lambda *a: None)
    monkeypatch.setattr(cutover, "_has_delta", lambda *a: False)
    monkeypatch.setattr(drainmark, "make",
                        lambda hop, eng, d: log.append(f"make {d}"))
    monkeypatch.setattr(drainmark, "put",
                        lambda hop, eng, d, t: log.append(f"put {t[-1]}")
                        or t)
    monkeypatch.setattr(drainmark, "drop",
                        lambda hop, eng, d: log.append(f"mark drop {d}"))
    monkeypatch.setattr(drainmark, "able", lambda hop, eng: "")

    def step(what, value=None):
        def fn(*a, **k):
            log.append(what if not a else f"{what} {a[0]}")
            if what == "back arm" and fail_at == "arm":
                raise RuntimeError("the old source refused it")
            return value
        return fn
    leg = cutover.Leg(
        "native" if fence else "pipeline", fence=eng if fence else None,
        down=step("down", []), up=step("up"),
        seen=lambda d, t: log.append(f"seen {t[-1]}") or True,
        settle=lambda d, t: log.append(f"settle {d}") or True,
        after_flip=step("after flip", ["select 'left for later'"]),
        back_up=step("back up"), back_down=step("back down"),
        back_fence=eng, back_arm=step("back arm"),
        back_fire=step("back fire"),
        back_proof=lambda d: log.append(f"proof {d}") or proof)
    real = cutover.failpoint.hit

    def hit(name):
        if name == f"cutover.{fail_at}":
            raise RuntimeError(f"broken at {fail_at}")
        return real(name)
    monkeypatch.setattr(cutover.failpoint, "hit", hit)
    return hop, eng, leg, log


def _runs(hop):
    return [json.loads(x) for x in (hop.report_dir() / "cutover-runs.jsonl")
            .read_text().splitlines()]


def test_the_stream_back_is_made_ahead_and_only_started_at_the_flip(
        monkeypatch, tmp_path):
    hop, eng, leg, log = _path(monkeypatch, tmp_path, fence=True)
    rec = cutover._phased(hop, eng, ["app"], leg, cutover.settings(hop),
                          lambda m: None, [])
    assert rec["back"] == "armed" and rec["drain"] == "position", rec
    # made before the clock: before the freeze, and proven there
    assert log.index("back arm app") < log.index("proof app") \
        < log.index("freeze pg_server"), log
    # at the flip only started, never made
    assert "back fire app" in log and "back up app" not in log, log
    assert log.index("down app") < log.index("back fire app"), log
    run = _runs(hop)[-1]
    assert run["state"] == "flipped" and "prepare" in run["took"], run
    # what the stream leaves, taken away once the clock stopped, and what
    # it leaves for the old source kept for when it is written again
    assert log.index("after flip app") > log.index("back fire app")
    assert run["left_on_source"] == {"app": ["select 'left for later'"]}


def test_a_stream_back_made_ahead_that_is_not_ready_is_made_at_the_flip(
        monkeypatch, tmp_path):
    hop, eng, leg, log = _path(monkeypatch, tmp_path, fence=True,
                               proof="its subscription knows 0 of the 3"
                                     " tables")
    said = []
    rec = cutover._phased(hop, eng, ["app"], leg, cutover.settings(hop),
                          said.append, [])
    assert rec["back"] == "at_flip", rec
    # the one made ahead taken away before anything was frozen
    assert log.index("back down app") < log.index("freeze pg_server"), log
    assert "back up app" in log and "back fire app" not in log, log
    assert any("not ready" in s for s in said), said


def test_one_made_ahead_is_dropped_when_the_path_goes_back(monkeypatch,
                                                           tmp_path):
    hop, eng, leg, log = _path(monkeypatch, tmp_path, fence=True,
                               fail_at="drain")
    with pytest.raises(SystemExit, match="the cutover was undone"):
        cutover._phased(hop, eng, ["app"], leg, cutover.settings(hop),
                        lambda m: None, [])
    assert "back down app" in log and "back up app" not in log, log
    assert log.index("back down app") < log.index("thaw"), log
    assert _runs(hop)[-1]["state"] == "undone"


def test_the_drain_by_a_mark_is_written_after_the_freeze(monkeypatch,
                                                         tmp_path):
    hop, eng, leg, log = _path(monkeypatch, tmp_path)
    rec = cutover._phased(hop, eng, ["app"], leg, cutover.settings(hop),
                          lambda m: None, [])
    assert rec["drain"] == "mark", rec
    # a way migkit's own account writes through was asked for
    assert "freeze pg_database" in log, log
    # the table and a first mark before the clock, found where the stream
    # keeps it, and the stream's own offsets settled behind it
    assert log.index("make app") < log.index("put 0") < log.index(
        "seen 0") < log.index("settle app") < log.index("freeze pg_database")
    # the drain's mark written after the freeze, then the offsets again
    at = log.index("freeze pg_database")
    assert at < log.index("put 1") < log.index("seen 1"), log
    assert log.index("seen 1") < len(log) - log[::-1].index("settle app")
    assert rec["phases"]["drain"]["positions"]["app"].startswith("mark")


def test_the_marks_table_is_taken_away_last_when_the_path_goes_back(
        monkeypatch, tmp_path):
    hop, eng, leg, log = _path(monkeypatch, tmp_path, fail_at="verify")
    with pytest.raises(SystemExit, match="the cutover was undone"):
        cutover._phased(hop, eng, ["app"], leg, cutover.settings(hop),
                        lambda m: None, [])
    assert log.index("thaw") < log.index("mark drop app"), log
    assert _runs(hop)[-1]["state"] == "undone"


def test_a_mark_that_never_arrives_stops_before_anything_is_frozen(
        monkeypatch, tmp_path):
    from migkit import drainmark
    hop, eng, leg, log = _path(monkeypatch, tmp_path)
    leg.seen = lambda d, t: False
    monkeypatch.setattr(drainmark, "wait",
                        lambda hop, eng, d, t, timeout, seen_by=None: False)
    with pytest.raises(SystemExit, match="did not reach the target"):
        cutover._phased(hop, eng, ["app"], leg, cutover.settings(hop),
                        lambda m: None, [])
    assert not [x for x in log if x.startswith("freeze")], log
    assert "mark drop app" in log


# --- the point of no return ---------------------------------------------------

def _flipped(hop, dbs=("app",)):
    run = {"id": "f1", "state": "flipped", "began": time.time() - 60,
           "outage": 1.25, "freeze": "database", "freeze_way": "pg_database",
           "dbs": list(dbs), "mark": {d: {"made": True} for d in dbs},
           "left_on_source": {"app": ["drop user if exists 'migkit_repl'"]}}
    with (hop.report_dir() / "cutover-runs.jsonl").open("a") as f:
        f.write(json.dumps(run) + "\n")
    return run


def _after(monkeypatch, tmp_path, dbs=("app",), broken=()):
    import migkit.config as cfg
    from migkit import drainmark, freeze
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    hop = _hop(tmp_path, dbs, cutover={"budget": 30}, reverse="at_cutover",
               origin_table="drop")
    log, asked = [], []

    class Eng:
        def apply_replication_stmt(self, side, d, stmt):
            log.append(f"left {d}")
    frozen = [True]
    monkeypatch.setattr(freeze, "source_frozen", lambda *a: frozen[0])

    def thaw(*a):
        frozen[0] = False
        log.append("thaw")
    monkeypatch.setattr(freeze, "thaw_source", thaw)
    monkeypatch.setattr(freeze, "freeze_source",
                        lambda hop, eng, dbs, say, how: log.append(
                            f"freeze again {how}"))
    monkeypatch.setattr(drainmark, "drop",
                        lambda hop, eng, d: log.append(f"mark drop {d}"))

    def back_down(d):
        if d in broken:
            raise RuntimeError(f"{d}: the old source did not answer")
        log.append(f"back down {d}")
    leg = cutover.Leg("native", fence=Eng(), back_down=back_down,
                      back_up=lambda d: None, end=lambda d: log.append(
                          f"end {d}"),
                      origin=lambda d: log.append(f"origin {d}"))

    def approve(detail, step="cutover"):
        asked.append((step, detail.get("step")))
        return ["ann"]
    return hop, Eng(), leg, log, asked, approve


def test_the_point_of_no_return_is_a_step_of_its_own(monkeypatch,
                                                     tmp_path):
    hop, eng, leg, log, asked, approve = _after(monkeypatch, tmp_path)
    _flipped(hop)
    said = []
    # shown, and nothing done or asked without --go
    assert cutover.run(hop, eng, ["app"], False, leg, said.append,
                       approve) is None
    assert log == [] and asked == []
    text = "\n".join(said)
    assert "point of no return" in text and "the stream back: dropped" in \
        text and "migkit's own batch table taken away" in text, text
    cutover.run(hop, eng, ["app"], True, leg, said.append, approve)
    assert asked == [(cutover.NO_RETURN_STEP, cutover.NO_RETURN_STEP)]
    assert log == ["end app", "thaw", "back down app", "left app",
                   "mark drop app", "origin app"], log
    assert cutover.past_flip(hop) is None
    ended = cutover.last_run(hop, after=True)
    assert ended["state"] == "no-return" and ended["run"] == "f1" and \
        ended["approved_by"] == ["ann"], ended
    # the flipped run is still the last cutover a next one is judged by
    assert cutover.last_run(hop)["state"] == "flipped"
    # going back is refused from here, and nothing is left to cut over
    with pytest.raises(SystemExit, match="past the point of no return"):
        cutover.back_plan(hop, eng, "app", said.append)
    log.clear()
    assert cutover.run(hop, eng, ["app"], True, leg, said.append,
                       approve) is None
    assert log == [] and "nothing left to cut over" in said[-1]


def test_a_point_of_no_return_that_fails_before_any_drop_is_undone(
        monkeypatch, tmp_path):
    hop, eng, leg, log, asked, approve = _after(monkeypatch, tmp_path,
                                                broken=("app",))
    _flipped(hop)
    with pytest.raises(SystemExit, match="point of no return was undone"):
        cutover.run(hop, eng, ["app"], True, leg, lambda m: None, approve)
    # the old source frozen again, the way the cutover had frozen it
    assert log == ["end app", "thaw", "freeze again pg_database"], log
    assert cutover.past_flip(hop)["id"] == "f1"
    assert cutover.record(hop) is None


def test_a_drop_that_failed_half_way_goes_on_at_the_next_run(monkeypatch,
                                                            tmp_path):
    hop, eng, leg, log, asked, approve = _after(
        monkeypatch, tmp_path, dbs=("app", "crm"), broken=("crm",))
    _flipped(hop, ("app", "crm"))
    with pytest.raises(SystemExit, match="goes on from there"):
        cutover.run(hop, eng, ["app", "crm"], True, leg, lambda m: None,
                    approve)
    assert "back down app" in log and log[-1] == "freeze again pg_database"
    rec = cutover.record(hop)
    assert rec["state"] == "no-return" and rec["back_off"] == ["app"]
    assert any("point of no return stopped" in s
               for s in cutover.said_by_doctor(hop))
    # going back is refused while it is half way
    with pytest.raises(SystemExit, match="past the point of no return"):
        cutover.back_plan(hop, eng, "app", lambda m: None)
    log.clear()
    leg.back_down = lambda d: log.append(f"back down {d}")
    cutover.run(hop, eng, ["app", "crm"], True, leg, lambda m: None, approve)
    # asked once, at the start; the database dropped already is not again
    assert [a for a in asked] == [(cutover.NO_RETURN_STEP,
                                   cutover.NO_RETURN_STEP)]
    assert "back down app" not in log and "back down crm" in log, log
    assert cutover.last_run(hop, after=True)["state"] == "no-return"


def test_at_the_flip_both_are_signed_before_the_clock_starts(monkeypatch,
                                                             tmp_path):
    hop = _hop(tmp_path, cutover={"budget": 30, "no_return": "at_flip"})
    order = []

    def preflight(hop, eng, dbs, leg, conf, chose=None):
        chose.update(drain="position", freeze="pg_server")
        return []

    def phased(hop, eng, dbs, leg, conf, say, approvers, drain=None,
               way=None):
        order.append(("phased", approvers, drain, way))
        return {"state": "flipped"}

    def no_return(hop, eng, dbs, go, leg, say, approve, rec=None,
                  approvers=None):
        order.append(("no return", approvers))
    monkeypatch.setattr(cutover, "preflight", preflight)
    monkeypatch.setattr(cutover, "_phased", phased)
    monkeypatch.setattr(cutover, "no_return", no_return)

    def approve(detail, step="cutover"):
        order.append(("asked", step, detail.get("no_return")))
        return [f"{step} signer"]
    cutover.run(hop, None, ["app"], True, cutover.Leg("native"),
                lambda m: None, approve)
    assert order == [("asked", "cutover", "at_flip"),
                     ("asked", cutover.NO_RETURN_STEP, "at_flip"),
                     ("phased", ["cutover signer"], "position", "pg_server"),
                     ("no return", [f"{cutover.NO_RETURN_STEP} signer"])], \
        order
    with pytest.raises(SystemExit, match="no_return is later"):
        cutover.settings(_hop(tmp_path, cutover={"no_return": "soon"}))


def test_going_back_ends_the_flip_so_the_next_run_is_a_cutover(monkeypatch,
                                                                tmp_path):
    from migkit import drainmark, freeze
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    hop = _hop(tmp_path, cutover={"budget": 30}, reverse="at_cutover")
    _flipped(hop)
    done = []

    class Eng:
        def apply_replication_stmt(self, side, d, stmt):
            done.append(stmt)

    class Rev:
        def apply(self, d, a):
            pass
    monkeypatch.setattr(freeze, "source_frozen", lambda *a: False)
    monkeypatch.setattr(drainmark, "drop",
                        lambda hop, eng, d: done.append(f"mark drop {d}"))
    cutover.back_apply(hop, Eng(), "app", {"rev": Rev(), "tdb": "app",
                                           "plan": []}, True, lambda m: None)
    assert done == ["drop user if exists 'migkit_repl'", "mark drop app"]
    assert cutover.past_flip(hop) is None
    assert cutover.last_run(hop, after=True)["state"] == "rolled back"


def test_a_follow_ends_between_looks_when_asked(tmp_path):
    """What the point of no return ends a tail by (`tailctl.stop`): a
    follow that looks again and again ends after the look it was asked
    during, that look's position saved - it used to go on for ever."""
    from migkit import tailctl
    token = tmp_path / "tail-token.json"
    token.write_text(json.dumps({"token": {"at": 0}}))
    looks = []

    class Eng:
        hop = Hop(name="t", engine="opensearch", source=Endpoint(),
                  target=Endpoint())

        def stream_identity(self, side, db):
            return None

        def position_lost(self, side, db, token):
            return None

    def look(at):
        looks.append(at)
        if len(looks) == 2:
            tailctl.stop(tmp_path)
        return {"at": len(looks)}, 1, True
    tailctl.follow(Eng(), "app", True, token, lambda m: None, "documents",
                   look, lambda at: "", every=0)
    assert looks == [{"at": 0}, {"at": 1}], looks
    assert json.loads(token.read_text())["token"] == {"at": 2}
    assert not (tmp_path / tailctl.STOP).exists()
    assert not tailctl.alive(tmp_path)


# --- the pipeline's own offsets, and migkit's batch table -------------------

DESCRIBED = """GROUP        connect-migkit-cx-sink
COORDINATOR  0
STATE        Stable
TOTAL-LAG    {total}

TOPIC                                   PARTITION  CURRENT-OFFSET  LOG-START-OFFSET  LOG-END-OFFSET  LAG  MEMBER-ID  CLIENT-ID  HOST
migkit-cx.public.orders                 0          {orders}        0                 40              {lag}  m-1  c-1  /10.0.0.3
{mark}
"""


def test_the_pipeline_is_drained_only_when_every_offset_is_applied():
    from migkit import cli
    mark = ("migkit-cx.public.migkit_cutover_mark  0  2  0  2  0  m-1  c-1"
            "  /10.0.0.3")
    topic = "migkit-cx.public.migkit_cutover_mark"
    assert cli._pipeline_drained(DESCRIBED.format(
        total=0, orders=40, lag=0, mark=mark), topic)
    assert not cli._pipeline_drained(DESCRIBED.format(
        total=3, orders=37, lag=3, mark=mark), topic)
    # the sink has not taken the mark's topic in yet
    assert not cli._pipeline_drained(DESCRIBED.format(
        total=0, orders=40, lag=0, mark=""), topic)
    # nothing committed on a partition that holds messages
    assert not cli._pipeline_drained(DESCRIBED.format(
        total=0, orders="-", lag="-", mark=mark), topic)
    assert not cli._pipeline_drained("", topic)


def test_migkits_batch_table_goes_only_where_asked_and_no_tail_needs_it(
        tmp_path, monkeypatch):
    from migkit import cli, tailctl
    torn = []

    class Pair:
        def two_way_teardown(self, d, token, go):
            torn.append((d, go))
            return ["drop table if exists public.migkit_origin"]
    hop = _hop(tmp_path, ("app", "crm"), origin_table="drop")
    monkeypatch.setattr(cli, "_tail_pair", lambda eng: Pair())
    monkeypatch.setattr(tailctl, "alive",
                        lambda where: where.name == "crm")
    said = []
    got = cli._origin_away(hop, object(), ["app", "crm"], said.append)
    assert torn == [("app", True)], torn
    assert got == ["drop table if exists public.migkit_origin"]
    assert any("crm" in s and "still runs" in s for s in said), said
    # both ways: the other way reads it, so it stays
    torn.clear()
    two = _hop(tmp_path, ("app",), origin_table="drop", topology="two_way")
    cli._origin_away(two, object(), ["app"], said.append)
    assert torn == [] and "streams both ways" in said[-1]
    from migkit import hopkeys
    assert hopkeys.origin_dropped(hop) and not hopkeys.origin_dropped(
        _hop(tmp_path))
    with pytest.raises(SystemExit, match="keep, or drop"):
        hopkeys.origin_dropped(_hop(tmp_path, origin_table="maybe"))

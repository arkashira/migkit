"""`migkit migrate`'s flow (`autonomous.run`) on fake steps: a goal runs
its steps per database, a failed unit climbs the one decision engine's
recovery ladder, a unit that cannot complete is held apart while the other
databases go on, a run again keeps what was done and re-establishes the
catch-up before the proof, and every question is one of `QUESTIONS`."""
import pytest

from migkit import autonomous, decide


class Fake:
    def __init__(self, dbs=("a", "b"), fail=None, verdict=None, stop=None,
                 assess=(), pieces=True, ready=()):
        self._dbs = list(dbs)
        #: (step, db) -> how many times it fails before it runs
        self.fail = dict(fail or {})
        self.verdict = dict(verdict or {})
        self.stop = dict(stop or {})
        self._assess = list(assess)
        self.pieces = pieces
        self._ready = dict(ready)
        self.calls = []

    def dbs(self):
        return self._dbs

    def facts(self, db):
        return {"chunk": 1000, "resumes": True, "pieces": self.pieces}

    def assess(self):
        self.calls.append(("assess", ""))
        return self._assess

    def _hit(self, step, db, *extra):
        self.calls.append((step, db) + extra)
        if (step, db) in self.stop:
            raise SystemExit(self.stop[(step, db)])
        if self.fail.get((step, db), 0) > 0:
            self.fail[(step, db)] -= 1
            raise ConnectionError(f"{step} of {db}: connection reset")

    def move(self, db, chunk):
        self._hit("move", db, chunk)

    def follow(self, db):
        self._hit("follow", db)
        return True

    def verify(self, db):
        self._hit("verify", db)
        return self.verdict.get(db, "equal")

    def ready(self, db):
        self._hit("ready", db)
        return list(self._ready.get(db, ()))

    def forecast(self, kind, db):
        return f"{kind} of {db}: about 1 minute left"

    def progress(self, db):
        return []


def _run(goal, ops, tmp_path=None, waits=None):
    said = []
    got = autonomous.run(goal, ops, said.append,
                         tmp_path / "workflow.json" if tmp_path else None,
                         wait=(waits.append if waits is not None
                               else lambda s: None))
    return got, said


def test_each_goal_runs_its_own_steps_per_database():
    for goal, steps in (("copy", ["move", "verify"]),
                        ("sync", ["move", "follow", "verify"]),
                        ("cutover", ["move", "follow", "verify", "ready"])):
        ops = Fake(dbs=("a",))
        got, said = _run(goal, ops)
        assert got.complete and got.exit == 0
        assert [c[0] for c in ops.calls] == ["assess"] + steps
        assert got.databases["a"][0] == "done"
        assert any("left" in s for s in said)


def test_an_unknown_goal_is_refused_before_anything_runs():
    ops = Fake()
    with pytest.raises(SystemExit, match="one of copy, sync, cutover"):
        _run("two-way", ops)
    assert ops.calls == []


def test_a_lost_connection_is_recovered_from_its_position_after_a_backoff(
        tmp_path):
    ops = Fake(dbs=("a",), fail={("move", "a"): 1})
    waits = []
    got, said = _run("copy", ops, tmp_path, waits)
    assert got.complete
    assert [c[0] for c in ops.calls].count("move") == 2
    assert len(waits) == 1 and autonomous.BACKOFF_MIN <= waits[0] \
        <= autonomous.BACKOFF_MAX
    kinds = [d["what"] for d in autonomous.evidence(
        tmp_path / "workflow.json")]
    assert "recover" in kinds
    assert any("again from its last position" in s for s in said)


def test_a_failure_twice_resizes_the_copy_into_smaller_pieces(tmp_path):
    ops = Fake(dbs=("a",), fail={("move", "a"): 2})
    got, _ = _run("copy", ops, tmp_path, [])
    assert got.complete
    moves = [c for c in ops.calls if c[0] == "move"]
    assert [m[2] for m in moves] == [1000, 1000, 250]
    assert got.workflow.by_key()["move:a"].way == "smaller"


def test_without_pieces_the_ladder_ends_and_the_unit_is_held_apart(
        tmp_path):
    ops = Fake(fail={("move", "a"): 9}, pieces=False)
    got, said = _run("sync", ops, tmp_path, [])
    assert not got.complete and got.exit == 1
    keys = got.workflow.by_key()
    assert keys["move:a"].state == "failed"
    assert "no way of" in keys["move:a"].why
    # its later steps wait; the other database went on to the end
    assert keys["follow:a"].state == "held"
    assert keys["verify:a"].state == "held"
    assert [keys[f"{s}:b"].state for s in ("move", "follow", "verify")] \
        == ["done"] * 3
    assert got.databases["b"][0] == "done"
    assert got.databases["a"][0] == "failed"
    assert any("the rest goes on without it" in s for s in said)
    # bounded: one try a rung, never a storm
    assert len([c for c in ops.calls if c[:2] == ("move", "a")]) == 2


def test_a_stop_of_migkit_s_own_is_never_retried():
    ops = Fake(dbs=("a",), stop={("move", "a"): "the source has no key"})
    got, _ = _run("copy", ops)
    assert [c for c in ops.calls if c[0] == "move"] == [("move", "a", 1000)]
    assert got.databases["a"] == (
        "failed", "copy stopped: the source has no key")


def test_a_difference_after_the_repair_is_a_verdict_not_retried():
    ops = Fake(dbs=("a",), verdict={"a": "different"})
    got, _ = _run("copy", ops)
    assert [c[0] for c in ops.calls].count("verify") == 1
    state, words = got.databases["a"]
    assert state == "failed" and "different" in words
    assert "kept: copy" in words


def test_a_stop_needing_a_business_choice_is_asked_as_its_question():
    ops = Fake(dbs=("a",), stop={("move", "a"): "orders already holds rows;"
                                 " target_rows: refuse"})
    got, _ = _run("copy", ops)
    u = got.workflow.by_key()["move:a"]
    assert u.state == "asked"
    assert autonomous.BY_KEY["target_rows"].asked in u.why
    assert autonomous.BUSINESS in u.why


def test_an_assessment_failing_for_one_database_holds_that_one_only():
    ops = Fake(assess=[("a", "logical replication is off")])
    got, _ = _run("copy", ops)
    keys = got.workflow.by_key()
    assert keys["move:a"].state == "failed"
    assert "logical replication is off" in keys["move:a"].why
    assert ("move", "a", 1000) not in ops.calls
    assert keys["verify:b"].state == "done"


def test_an_assessment_failing_for_the_hop_starts_nothing():
    ops = Fake(assess=[("", "the target cannot be reached")])
    got, _ = _run("copy", ops)
    assert [c[0] for c in ops.calls] == ["assess"]
    assert all(s == "held" for s, _ in got.databases.values())
    assert "the target cannot be reached" in got.databases["a"][1]


def test_a_run_again_keeps_done_work_and_catches_up_before_the_proof(
        tmp_path):
    ops = Fake(dbs=("a",), verdict={"a": "different"})
    first, _ = _run("sync", ops, tmp_path)
    assert first.workflow.by_key()["verify:a"].state == "failed"
    again = Fake(dbs=("a",))
    second, _ = _run("sync", again, tmp_path)
    assert second.complete
    # the copy is kept; the catch-up is proved again before the proof
    assert [c[0] for c in again.calls] == ["follow", "verify"]


def test_a_run_of_another_goal_starts_afresh(tmp_path):
    _run("copy", Fake(dbs=("a",)), tmp_path)
    ops = Fake(dbs=("a",))
    _run("cutover", ops, tmp_path)
    assert [c[0] for c in ops.calls] == ["assess", "move", "follow",
                                         "verify", "ready"]


def test_cutover_readiness_failing_is_said_and_the_rest_is_kept():
    ops = Fake(dbs=("a",), ready={"a": ["the stream back is not ready"]})
    got, _ = _run("cutover", ops)
    state, words = got.databases["a"]
    assert state == "failed" and "the stream back is not ready" in words
    assert "kept: copy, catch-up, proof" in words


def test_a_fault_in_a_step_is_held_never_passed_as_success():
    class Broken(Fake):
        def verify(self, db):
            raise KeyError("columns")
    got, _ = _run("copy", Broken(dbs=("a",)))
    u = got.workflow.by_key()["verify:a"]
    assert u.state == "failed" and "stopped unexpectedly" in u.why
    assert not got.complete


def test_every_question_is_typed_and_said_with_its_reason():
    kinds = {autonomous.INTENT, autonomous.ACCESS, autonomous.BUSINESS,
             autonomous.AUTHORIZE}
    assert len(autonomous.BY_KEY) == len(autonomous.QUESTIONS)
    for q in autonomous.QUESTIONS:
        assert q.kind in kinds and q.asked and q.reason
    with pytest.raises(autonomous.Asked, match="authorization"):
        autonomous.ask(None, "write")
    assert autonomous.ask(lambda q, d: q.key, "goal") == "goal"
    with pytest.raises(KeyError):
        autonomous.ask(lambda q, d: "x", "which-copier")
    assert autonomous.question_for("permission denied for table t").key \
        == "grant"
    assert autonomous.question_for("the source has no key") is None


def test_the_backoff_follows_the_failed_attempt_and_is_bounded():
    assert autonomous.backoff(0, 1) == autonomous.BACKOFF_MIN
    assert autonomous.backoff(10, 1) == 10
    assert autonomous.backoff(10, 2) == 20
    assert autonomous.backoff(10_000, 1) == autonomous.BACKOFF_MAX
    assert autonomous.backoff(200, 3) == autonomous.BACKOFF_MAX


def test_the_ladder_is_the_decision_engine_s_rungs():
    rungs = autonomous.ladder("move", decide.Facts({"pieces": True}))
    assert [r.name for r in rungs] == ["as-planned", "resume", "smaller"]
    got = decide.climb("copy", rungs, decide.Facts({"pieces": False}),
                       need=("complete",))
    assert got.rung.name == "as-planned"
    assert [r.name for r in autonomous.ladder("ready", decide.Facts())] \
        == ["as-planned"]

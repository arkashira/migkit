"""`migkit migrate`'s flow (`autonomous.run`) on fake steps: a goal runs
its steps per database, a failed unit climbs the one decision engine's
recovery as `decide.recovery` chooses from the fault catalogue (a
sizing cause retuned, not retried), a unit that cannot complete is held apart while the other
databases go on, a run again keeps what was done and re-establishes the
catch-up before the proof, the units left are re-planned after an
isolate or a throttle, a cutover's switch is made only on the operator's
yes, and every question is one of `QUESTIONS`."""
import pytest

from migkit import autonomous, decide, faults


class Fake:
    def __init__(self, dbs=("a", "b"), fail=None, verdict=None, stop=None,
                 assess=(), pieces=True, ready=(), errors=None,
                 etas=None):
        self._dbs = list(dbs)
        #: (step, db) -> how many times it fails before it runs
        self.fail = dict(fail or {})
        self.verdict = dict(verdict or {})
        self.stop = dict(stop or {})
        self._assess = list(assess)
        self.pieces = pieces
        self._ready = dict(ready)
        #: (step, db) -> [errors] raised, one a run, before it runs
        self.errors = {k: list(v) for k, v in (errors or {}).items()}
        self.etas = dict(etas or {})
        self.calls = []
        #: (step, db) -> {key: record} a run before left open
        self.owned = {}
        #: key -> what the target says it holds of it
        self.landed = {}
        self.workers = []

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
        if self.errors.get((step, db)):
            raise self.errors[(step, db)].pop(0)
        if self.fail.get((step, db), 0) > 0:
            self.fail[(step, db)] -= 1
            raise ConnectionError(f"{step} of {db}: connection reset")

    def move(self, db, chunk, workers=None):
        self.workers.append((db, workers))
        self._hit("move", db, chunk)

    def uncertain(self, step, db):
        return dict(self.owned.get((step, db)) or {})

    def settle(self, step, db, key, rec):
        self.calls.append(("settle", db, key))
        return self.landed.get(key)

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

    def eta(self, kind, db):
        return self.etas.get(db)


class Switching(Fake):
    def __init__(self, *a, switch_fails=None, **kw):
        super().__init__(*a, **kw)
        self.switched, self.switch_fails = [], switch_fails

    def switch(self, db):
        self.calls.append(("switch", db))
        if self.switch_fails:
            raise SystemExit(self.switch_fails)
        self.switched.append(db)


class Reading(Fake):
    def __init__(self, *a, read=None, **kw):
        super().__init__(*a, **kw)
        self.read = read

    def reconcile(self, db):
        self.calls.append(("reconcile", db))
        return self.read


def _run(goal, ops, tmp_path=None, waits=None, answer=None, go=False):
    said = []
    got = autonomous.run(goal, ops, said.append,
                         tmp_path / "workflow.json" if tmp_path else None,
                         wait=(waits.append if waits is not None
                               else lambda s: None),
                         answer=answer, go=go)
    return got, said


def _recovered(tmp_path):
    return [d for d in autonomous.evidence(tmp_path / "workflow.json", 500)
            if d["what"] == "recover"]


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
    rec = _recovered(tmp_path)
    # a copy's write lost mid-way has an effect not known: it is read
    # (from the copy's kept position) before anything runs it again
    assert [r["way"] for r in rec] == ["reconcile"]
    assert rec[0]["fault"] == "uncertain_commit"
    assert rec[0]["reconciled"] == "nothing" and rec[0]["held"] is True
    assert rec[0]["post"] == faults.CATALOGUE["uncertain_commit"].retry_after
    assert got.workflow.by_key()["move:a"].way == "resume"
    assert any("again from its last position" in s for s in said)


def test_a_write_read_as_committed_proceeds_without_running_it_again(
        tmp_path):
    ops = Reading(dbs=("a",), fail={("move", "a"): 1}, read="committed")
    got, _ = _run("copy", ops, tmp_path, [])
    assert got.complete
    assert [c[0] for c in ops.calls if c[0] in ("move", "reconcile")] \
        == ["move", "reconcile"]
    assert got.workflow.by_key()["move:a"].way == "reconciled"


def test_a_write_whose_effect_cannot_be_read_is_not_run_again(tmp_path):
    ops = Reading(dbs=("a",), fail={("move", "a"): 1}, read=None)
    got, _ = _run("copy", ops, tmp_path, [])
    u = got.workflow.by_key()["move:a"]
    assert u.state == "failed" and "cannot be read" in u.why
    assert [c[0] for c in ops.calls].count("move") == 1
    assert got.workflow.by_key()["verify:a"].state == "held"


def test_a_sizing_cause_is_retuned_never_the_same_work_again(tmp_path):
    big = RuntimeError("Got a packet bigger than 'max_allowed_packet' bytes")
    ops = Fake(dbs=("a",), errors={("move", "a"): [big]})
    got, _ = _run("copy", ops, tmp_path, [])
    assert got.complete
    moves = [c[2] for c in ops.calls if c[0] == "move"]
    assert len(moves) == 2 and moves[1] < moves[0] == 1000
    assert got.workflow.by_key()["move:a"].way == "retuned"
    rec = _recovered(tmp_path)
    assert [r["way"] for r in rec] == ["retune"] and rec[0]["held"] is True


def test_a_refused_account_stops_at_once_with_the_catalogue_s_reason(
        tmp_path):
    bad = RuntimeError("password authentication failed for user x")
    ops = Fake(dbs=("a", "b"), errors={("verify", "a"): [bad] * 5})
    got, _ = _run("copy", ops, tmp_path, [])
    u = got.workflow.by_key()["verify:a"]
    assert u.state == "failed"
    assert faults.CATALOGUE["auth"].retry_after in u.why
    assert [c for c in ops.calls if c[0] == "verify"].count(
        ("verify", "a")) == 1
    assert [r["way"] for r in _recovered(tmp_path)] == ["stop"]
    assert got.databases["b"][0] == "done"


def test_a_throttle_replans_the_units_left_at_a_smaller_size(tmp_path):
    ops = Fake(dbs=("a", "b"),
               errors={("verify", "a"): [faults.Injected("quota")]})
    waits = []
    got, said = _run("copy", ops, tmp_path, waits)
    assert got.complete
    assert [r["way"] for r in _recovered(tmp_path)] == ["throttle"]
    # the throttle waits longer than a retry of the same attempt would
    assert waits and waits[0] >= 2 * autonomous.BACKOFF_MIN
    # the copy of b, planned after it, runs at what the throttle left
    assert ("move", "b", 500) in ops.calls
    plan = [d for d in autonomous.evidence(tmp_path / "workflow.json", 500)
            if d["what"] == "replan"]
    assert plan and plan[0]["scale"] == 0.5
    assert any(s.startswith("replanned after proof of a was throttled")
               and "2 steps left over b" in s for s in said)


def test_after_an_isolate_the_shortest_forecast_database_goes_first(
        tmp_path):
    bad = RuntimeError("password authentication failed")
    ops = Fake(dbs=("a", "b", "c"), etas={"b": 600.0, "c": 5.0},
               errors={("move", "a"): [bad]})
    got, said = _run("copy", ops, tmp_path, [])
    moves = [c[1] for c in ops.calls if c[0] == "move"]
    assert moves == ["a", "c", "b"]
    assert got.databases["c"][0] == got.databases["b"][0] == "done"
    assert any("over c, b" in s and "about 605s of measured work left" in s
               for s in said)


def test_a_fault_that_does_not_clear_is_bounded_and_held_apart(tmp_path):
    ops = Fake(fail={("move", "a"): 9}, pieces=False)
    got, said = _run("sync", ops, tmp_path, [])
    assert not got.complete and got.exit == 1
    keys = got.workflow.by_key()
    assert keys["move:a"].state == "failed"
    assert f"every try spent ({autonomous.unmeasured_tries()})" in keys["move:a"].why
    # its later steps wait; the other database went on to the end
    assert keys["follow:a"].state == "held"
    assert keys["verify:a"].state == "held"
    assert [keys[f"{s}:b"].state for s in ("move", "follow", "verify")] \
        == ["done"] * 3
    assert got.databases["b"][0] == "done"
    assert got.databases["a"][0] == "failed"
    assert any("the rest goes on without it" in s for s in said)
    # bounded: never more runs than the tries, never a storm
    assert len([c for c in ops.calls if c[:2] == ("move", "a")]) \
        == autonomous.unmeasured_tries()
    held = [r["held"] for r in _recovered(tmp_path)]
    assert held and not any(held)


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


def test_a_cutover_proved_ready_asks_the_switch_and_never_makes_it_alone(
        tmp_path):
    ops = Switching(dbs=("a", "b"))
    got, said = _run("cutover", ops, tmp_path)
    assert got.complete and got.exit == 0 and got.switch == "asked"
    assert ops.switched == []
    assert any(autonomous.BY_KEY["switch"].asked in s for s in said)
    asked = [d for d in autonomous.evidence(tmp_path / "workflow.json")
             if d["unit"] == "switch"]
    assert asked[0]["what"] == "asked" and "a: copy" in asked[0]["evidence"]
    assert "waits for the operator" in got.databases["a"][1]
    # the next run proves the catch-up and readiness again, then asks
    again = Switching(dbs=("a", "b"))
    second, _ = _run("cutover", again, tmp_path, answer=lambda q, d: "no")
    assert second.switch == "declined" and again.switched == []
    assert [c[0] for c in again.calls] == ["follow", "ready"] * 2


def test_a_go_given_up_front_is_the_yes_to_the_switch(tmp_path):
    ops = Switching(dbs=("a", "b"))
    got, _ = _run("cutover", ops, tmp_path, go=True)
    assert got.switch == "switched" and ops.switched == ["a", "b"]
    assert "switched to the target" in got.databases["b"][1]
    asked = []
    ops = Switching(dbs=("a",))
    got, _ = _run("cutover", ops, answer=lambda q, d: asked.append(q.key)
                  or "yes")
    assert asked == ["switch"] and ops.switched == ["a"]
    # a switch done is not proposed again
    third = Switching(dbs=("a", "b"))
    got, _ = _run("cutover", third, tmp_path, go=True)
    assert third.calls == [] and third.switched == []


def test_the_switch_is_not_proposed_while_a_database_is_not_ready():
    ops = Switching(dbs=("a", "b"), ready={"b": ["the stream back is off"]})
    got, said = _run("cutover", ops, go=True)
    assert got.switch == "not ready" and ops.switched == []
    assert any("not proposed: b not ready" in s for s in said)
    failing = Switching(dbs=("a",), switch_fails="the drain timed out")
    got, _ = _run("cutover", failing, go=True)
    assert got.switch == "failed" and got.exit == 1


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


def test_a_class_failing_again_cools_the_side_before_one_more_try():
    told = faults.classify(ConnectionError("connection reset"))
    first = autonomous.recovery_facts(told, 1, None, False, 3)
    again = autonomous.recovery_facts(told, 2, None, False, 2)
    assert first["breaker"] == "closed" and again["breaker"] == "open"
    assert decide.recovery(first).path == "retry"
    assert decide.recovery(again).path == "pause"
    assert decide.recovery({**again, "tries_left": 0}).path == "stop"


class Measured(Fake):
    def __init__(self, *a, budget=None, **kw):
        super().__init__(*a, **kw)
        self.budget = dict(budget or {})

    def facts(self, db):
        return {**super().facts(db), **self.budget}


class NoLedger(Fake):
    uncertain = None


def test_the_flow_hands_each_copy_its_share_of_the_measured_budget(
        tmp_path):
    ops = Measured(dbs=("a", "b"), budget={"start": 8, "most": 6},
                   errors={("verify", "a"): [faults.Injected("quota")]})
    got, _ = _run("copy", ops, tmp_path, [])
    assert got.complete
    # the measured start, never past the most both servers take
    assert ops.workers[0] == ("a", 6)
    # the throttle on a cut the budget too: b runs at half
    assert ops.workers[1] == ("b", 3)


def test_unmeasured_the_flow_hands_no_budget_down_and_the_copy_sizes_itself():
    ops = Fake(dbs=("a",))
    got, _ = _run("copy", ops)
    assert got.complete and ops.workers == [("a", None)]


def test_a_plain_move_asks_the_same_sizing_rung():
    n, c = decide.worker_budget({"start": 6, "most": 4})
    assert (n, c.path) == (4, "the measured start")
    n, c = decide.worker_budget({"budget": 3, "start": 6, "scale": 0.5})
    assert (n, c.path) == (3, "the flow's budget")
    n, c = decide.worker_budget({"little": 5, "scale": 0.5})
    assert (n, c.path) == (2, "the measured carry")
    n, c = decide.worker_budget({})
    assert (n, c.path) == (1, "one at a time")
    n, _ = decide.worker_budget({"start": 1, "scale": 0.1})
    assert n == 1


def test_model_budget_says_what_the_rung_reads():
    from migkit import model
    class Carried(model.Model):
        little = 3
    m = Carried.__new__(Carried)
    m.observed = {"workers": 4, "rows_a_second": 900.0}
    b = m.budget(most=7)
    assert b == {"little": 3, "observed": 4, "observed_rate": 900.0,
                 "most": 7}


def test_a_step_reconciles_what_a_run_before_left_open_before_it_runs(
        tmp_path):
    ops = Fake(dbs=("a",))
    ops.owned[("move", "a")] = {"op:1": {"state": "began", "position": "9"}}
    ops.landed["op:1"] = "committed"
    got, said = _run("copy", ops, tmp_path, [])
    assert got.complete
    calls = [c[0] for c in ops.calls]
    assert calls.index("settle") < calls.index("move")
    rec = [d for d in autonomous.evidence(tmp_path / "workflow.json", 500)
           if d["what"] == "reconcile"]
    assert rec and rec[0]["held"] is True
    assert rec[0]["states"] == {"op:1": "committed"}
    assert any("read on the target before it runs again" in s for s in said)


def test_an_operation_that_cannot_be_read_blocks_with_the_next_step(
        tmp_path):
    ops = Fake(dbs=("a", "b"))
    ops.owned[("move", "a")] = {"op:1": {"state": "unknown"},
                                "op:2": {"state": "began"}}
    ops.landed["op:1"] = "nothing"
    got, _ = _run("copy", ops, tmp_path, [])
    u = got.workflow.by_key()["move:a"]
    assert u.state == "failed"
    assert "1 operations a run before began cannot be read" in u.why
    assert "check the target for them, then run again" in u.why
    # never replayed blind; the other database goes on
    assert ("move", "a", 1000) not in ops.calls
    assert got.databases["b"][0] == "done"


def test_an_operation_still_running_waits_for_it_rather_than_repeat_it(
        tmp_path):
    ops = Fake(dbs=("a",))
    ops.owned[("verify", "a")] = {"op:3": {"state": "running"}}
    ops.landed["op:3"] = "running"
    got, _ = _run("copy", ops, tmp_path, [])
    u = got.workflow.by_key()["verify:a"]
    assert u.state == "failed" and "wait for them to end" in u.why
    assert ("verify", "a") not in ops.calls


def test_a_kept_position_alone_is_not_proof_nothing_landed(tmp_path):
    ops = NoLedger(dbs=("a",), fail={("move", "a"): 1})
    got, _ = _run("copy", ops, tmp_path, [])
    u = got.workflow.by_key()["move:a"]
    assert u.state == "failed"
    assert len([c for c in ops.calls if c[:2] == ("move", "a")]) == 1


def test_the_tries_are_what_the_forecast_pays_for():
    ops = Fake(dbs=("a",), etas={"a": 50.0})
    u = autonomous.Unit("move", "a")
    assert autonomous.tries_for(ops, u, {"attempt_seconds": 10.0}) == 6
    assert autonomous.tries_for(ops, u, {}) == autonomous.unmeasured_tries()
    assert autonomous.tries_for(Fake(dbs=("a",)), u,
                                {"attempt_seconds": 10.0}) \
        == autonomous.unmeasured_tries()
    import inspect
    assert autonomous.unmeasured_tries() == inspect.signature(
        faults.retried).parameters["tries"].default


def test_the_scale_floor_is_one_worker_of_the_budget():
    assert autonomous.scale_floor({"most": 4, "chunk": 1000}) == 0.25
    assert autonomous.scale_floor({"chunk": 1000}) == 0.001
    assert autonomous.scale_floor({}) == 0.0


def test_the_write_consent_is_not_the_switch_s():
    from migkit import cli
    names = {p.name for p in cli.migrate.params}
    assert {"go", "switch"} <= names
    ops = Switching(dbs=("a",))
    got, said = _run("cutover", ops)
    assert got.switch == "asked" and ops.switched == []


def test_the_comparison_is_filled_only_from_runs():
    import json
    table = ("| Scenario | Engine | Inputs | Interventions | Completed |"
             " Recovered | Retry waste | Seconds | Managed service |\n"
             "| healthy | MySQL | 6 | to be measured | to be measured |"
             " to be measured | to be measured | to be measured |"
             " to be measured |\n"
             "| healthy | PostgreSQL | 6 | to be measured | to be measured |"
             " to be measured | to be measured | to be measured |"
             " to be measured |\n")
    lines = [json.dumps({"scenario": "healthy", "engine": "mysql",
                         "inputs": 6, "interventions": 0, "completed": True,
                         "recovered": None, "retry_waste": 0,
                         "seconds": 12.5})]
    got = autonomous.comparison(lines, table).splitlines()
    assert got[1] == ("| healthy | MySQL | 6 | 0 | yes | none needed | 0 |"
                      " 12.5 | to be measured |")
    # a row no run measured keeps its cells
    assert got[2] == table.splitlines()[2]


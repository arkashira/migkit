"""Every phase `migkit migrate` composes declares its contract
(P0-WORKFLOW-DEPTH.model): its outcome, scope, prerequisites with the
evidence each must have left, the evidence it leaves, and its entry and
exit criteria as predicates. On fake steps: a failed prerequisite blocks
what waits on it with the evidence missing while the independent work goes
on, a step a crash left running is failed-unknown and its ledger's open
units are reconciled (`attempts.reconcile_units`) before it runs again, the
pair's gaps (`autonomous.gaps`) are said before the first costly step, and
the order of the phases is the one decision engine's choice from the
facts (`decide.phase_order`)."""
import json

import pytest

from migkit import attempts, autonomous, decide, durable, planner

PAIRS = (("postgres", "postgres"), ("mysql", "mysql"), ("mysql", "postgres"),
         ("postgres", "mysql"), ("mongodb", "mongodb"))


class Fake:
    """The steps of a hop of `pair`, each call kept in `log` (the same
    list as what is said, so the order of both is read)."""

    def __init__(self, pair=("postgres", "postgres"), dbs=("a", "b"),
                 log=None, cycle=None, refuse=(), crash=(), eng=None):
        self._pair, self._dbs = pair, list(dbs)
        self.log = log if log is not None else []
        #: db -> whether its foreign keys close a cycle
        self.cycle = dict(cycle or {})
        self.refuse, self.crash = set(refuse), set(crash)
        if eng is not None:
            self.eng = eng

    def pair(self):
        return self._pair

    def order(self, db):
        return {"fk cycle": self.cycle.get(db, False),
                "target keys": self._pair[1] != "mongodb"}

    def dbs(self):
        return self._dbs

    def facts(self, db):
        return {"chunk": 1000, "resumes": True, "pieces": False}

    def _hit(self, step, db):
        self.log.append((step, db))
        if (step, db) in self.crash:
            self.crash.discard((step, db))
            raise KeyboardInterrupt
        if (step, db) in self.refuse:
            raise SystemExit(f"the target refused the {step} of {db}")

    def assess(self):
        self.log.append(("assess", ""))
        return []

    def schema(self, db):
        self._hit("schema", db)

    def constraints(self, db):
        self._hit("constraints", db)

    def move(self, db, chunk, workers=None):
        self._hit("move", db)

    def follow(self, db):
        self._hit("follow", db)
        return True

    def verify(self, db):
        self._hit("verify", db)
        return "equal"

    def ready(self, db):
        self._hit("ready", db)
        return []

    def forecast(self, kind, db):
        return f"{kind} of {db}: about 1 minute left"

    def progress(self, db):
        return []


def _run(goal, ops, path=None):
    return autonomous.run(goal, ops, ops.log.append, path,
                          wait=lambda s: None)


@pytest.mark.parametrize("pair", PAIRS, ids="->".join)
@pytest.mark.parametrize("goal", autonomous.GOALS)
def test_every_phase_the_flow_composes_declares_its_contract(pair, goal,
                                                             tmp_path):
    got = _run(goal, Fake(pair, cycle={"a": True}), tmp_path / "wf.json")
    phases = got.workflow.phases
    assert set(phases) == {u.key for u in got.workflow.units}
    assert {p.step for p in phases.values()} >= {"assess", "schema", "move",
                                                "verify"}
    for p in phases.values():
        assert p.hollow() == [], (p.key, p.hollow())
        assert p.state == "done"
        assert got.workflow.evidence[p.key] == list(p.evidence)
    # the contract is durable beside the units
    kept = durable.read_state(tmp_path / "wf.json", "state")
    assert kept["phases"]["move:a"]["evidence"] == list(
        phases["move:a"].evidence)
    assert kept["phases"]["move:a"]["prerequisites"] == {
        "schema:a": ["the target's schema applied"]}


def test_a_hollow_contract_is_told_part_by_part():
    copy = planner.Phase("copy", step="move", db="a", outcome="copied",
                         scope=("a",), entry=(("x", lambda f: True),),
                         exit=(("y", lambda f: True),),
                         prerequisites=(("schema:a", ()),))
    assert copy.hollow() == ["evidence", "evidence of schema:a"]
    assert "prerequisites" in planner.Phase("copy", step="move").hollow()
    bad = planner.Phase("copy", entry=(("x", "not a predicate"),))
    assert "entry 'x' is no predicate" in bad.hollow()


def test_a_refused_schema_blocks_the_copy_with_the_evidence_missing(
        tmp_path):
    ops = Fake(refuse={("schema", "a")})
    got = _run("copy", ops, tmp_path / "wf.json")
    keys = got.workflow.by_key()
    assert keys["schema:a"].state == "failed"
    move = keys["move:a"]
    assert move.state == "held" and move.why.startswith("blocked: the schema"
                                                         " of a, which failed")
    assert "the target's schema applied is missing" in move.why
    assert ("move", "a") not in ops.log
    # the independent work runs to its end: the assessment and every
    # phase of the other database
    assert ("assess", "") in ops.log
    assert all(keys[k].state == "done" for k in
               ("assess", "schema:b", "move:b", "verify:b"))
    kept = durable.read_state(tmp_path / "wf.json", "state")
    assert kept["phases"]["move:a"]["state"] == "blocked"
    assert kept["phases"]["verify:a"]["state"] == "blocked"
    assert kept["phases"]["schema:a"]["state"] == "failed"
    assert "every phase of a" in got.workflow.phases["move:b"].independent
    held = [d for d in got.workflow.decisions
            if d["unit"] == "move:a" and d["what"] == "held"]
    assert held and held[0]["way"] == "blocked"


def test_an_exit_criterion_that_fails_is_no_done_phase(tmp_path):
    class Refusing(Fake):
        def schema(self, db):
            self._hit("schema", db)
            return False                # refused in its answer, not raised
    got = _run("copy", Refusing(dbs=("a",)), tmp_path / "wf.json")
    u = got.workflow.by_key()["schema:a"]
    assert u.state == "failed" and "the schema step did not refuse" in u.why
    assert "schema:a" not in got.workflow.evidence
    move = got.workflow.by_key()["move:a"]
    assert move.state == "held" and "is missing" in move.why
    assert not got.complete and got.exit == 1


def _ledger_with_an_open_unit(path):
    led = attempts.Ledger(path)
    led.unit_opened("orders", kind="move")
    led.flush()


class _Eng:
    def __init__(self, path, log):
        self.path, self.log = path, log

    def attempts(self):
        self.log.append(("reconcile", ""))
        return type("A", (), {"ledger": attempts.Ledger(self.path)})()


def test_a_step_a_crash_left_running_is_reconciled_before_it_runs_again(
        tmp_path):
    wf_path, led_path = tmp_path / "wf.json", tmp_path / "ledger.json"
    _ledger_with_an_open_unit(led_path)
    log = []
    ops = Fake(dbs=("a",), log=log, crash={("move", "a")},
               eng=_Eng(led_path, log))
    with pytest.raises(KeyboardInterrupt):
        _run("copy", ops, wf_path)
    kept = durable.read_state(wf_path, "state")
    assert kept["units"]["move:a"]["state"] == "running"
    assert kept["phases"]["move:a"]["state"] == "running"
    _, phases = autonomous.compose("copy", ["a"], autonomous.OWN_STEPS,
                                   {"a": ops.order("a")})
    loaded = autonomous.Workflow.load(wf_path, "copy", ["a"], phases)
    assert loaded.by_key()["move:a"].state == "failed-unknown"
    assert loaded.by_key()["schema:a"].state == "done"

    log.clear()
    got = _run("copy", ops, wf_path)
    assert got.complete
    assert log.index(("reconcile", "")) < log.index(("move", "a"))
    assert ("schema", "a") not in log           # done work is kept
    rec = [d for d in got.workflow.decisions if d["what"] == "reconciled"]
    assert rec and rec[0]["unit"] == "move:a" and rec[0]["cut"] == ["orders"]
    assert attempts.Ledger(led_path).records["orders"]["state"] == "failed"
    # a run again finds nothing left running, and reconciles nothing
    log.clear()
    _run("copy", ops, wf_path)
    assert ("reconcile", "") not in log


def test_the_pair_s_gaps_name_the_engine_that_lacks_each():
    rows = autonomous.gaps("mysql", "cassandra")
    want = {("mysql", "source", k, n) for k, n, _, _ in
            decide.still_lacking("mysql")["mysql"]} | {
        ("cassandra", "target", k, n) for k, n, _, _ in
        decide.still_lacking("cassandra")["cassandra"]}
    assert want and {r[:4] for r in rows} == want
    assert any(r[0] == "cassandra" for r in rows)
    assert all(r[4] in decide._OWED for r in rows)
    same = autonomous.gaps("postgres", "postgres")
    assert {r[1] for r in same} <= {"source and target"}


def test_the_gaps_are_said_before_the_first_costly_step():
    ops = Fake(("mysql", "cassandra"), dbs=("a",))
    got = _run("copy", ops)
    lines = autonomous.gap_lines("mysql", "cassandra")
    assert lines and all(line in ops.log for line in lines)
    last = max(ops.log.index(line) for line in lines)
    first = min(i for i, x in enumerate(ops.log) if isinstance(x, tuple))
    assert last < first
    assert any(d["what"] == "gaps" for d in got.workflow.decisions)


def test_the_order_of_the_phases_is_chosen_from_the_dependencies():
    assert decide.phase_order({"fk cycle": True}).path == "constraints-after"
    assert decide.phase_order({"fk cycle": False}).path == "schema-whole"
    # a cycle not read is not taken as none
    assert decide.phase_order({}).path == "constraints-after"
    # a target without foreign keys has no cycle to load around
    assert decide.phase_order({"fk cycle": True,
                               "target keys": False}).path == "schema-whole"
    got = _run("copy", Fake(cycle={"a": True, "b": False}))
    keys = got.workflow.by_key()
    assert "constraints:a" in keys and "constraints:b" not in keys
    assert keys["verify:a"].after == ("constraints:a",)
    assert keys["verify:b"].after == ("move:b",)
    order = {d["why"].split(":")[0]: d["way"] for d in got.workflow.decisions
             if d["what"] == "order"}
    assert order == {"a": "constraints-after", "b": "schema-whole"}
    assert [x for x in got.workflow.units if x.db == "a"][-1].state == "done"


def test_each_transition_is_the_decision_engine_s_choice():
    assert decide.phase_step({}).path == "enter"
    assert decide.phase_step({"prerequisites owed": "the copy of a"}
                             ).path == "wait"
    assert decide.phase_step({"prerequisites owed": "the copy of a",
                              "prerequisite failed": "the copy of a"}
                             ).path == "blocked"
    # entry criteria that fail with nothing left to wait for block it
    assert decide.phase_step({"entry unmet": "no schema"}).path == "blocked"


def test_the_flow_s_record_is_json_with_every_phase(tmp_path):
    _run("cutover", Fake(dbs=("a",)), tmp_path / "wf.json")
    kept = json.loads((tmp_path / "wf.json").read_text())
    assert set(kept["phases"]) == set(kept["units"])
    assert set(kept["phases"]["ready:a"]) >= {"state", "outcome", "scope",
                                              "evidence", "prerequisites",
                                              "independent"}

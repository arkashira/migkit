"""`migkit migrate`: two addresses, their access and a goal in; the whole
workflow run to a verified outcome (backlog P0-AUTONOMOUS-MIGRATION).

The goal says the steps (`FLOWS`): a copy is moved and proved, a sync is
moved, followed and proved, a cutover is all of that and its readiness.
Each database is a chain of units, one a step, each waiting for the one
before it; the hop's assessment comes first. Every way a unit may run is
A failed unit is told by its class (`faults.classify`); a cause a
size answers retunes the copy (`tuning.Retune`), and every other is
recovered as the one decision engine chooses from the fault catalogue
(`decide.recovery`): reconcile an effect not known, proceed, pause,
throttle, retry from the last durable position after a backoff the
failed attempt's own time measures, replan, probe, isolate or stop -
under the one recovery ladder (`faults.retried`), its tries the ones the
unit's forecast pays for (`tries_for`), each choice and whether its
postcondition held kept in `workflow.json`. Before any step runs again,
what a run before it left begun is reconciled on the target (`settle`,
`attempts.Ledger.uncertain`, `faults.landed`); one that cannot be read
is a blocker with its next step, never replayed blind. The flow owns the
copy's workers: one share of the measured budget a database, cut by a
throttle (`Plan.workers`, `decide.worker_budget`). migkit's own stop is a blocker said in its words, never
retried; a fault is held, never passed as success. A unit that cannot
complete is isolated: its database's later steps are held, every other
database goes on, and after an isolate or a throttle the units left are
re-planned (sizes, order) with their forecast and scope said again
(`replan`). Done units are kept and a run again starts from them,
re-establishing a stream's catch-up before the proof that depends on it
(and, for a cutover, readiness before the switch). The time left is the
engine's forecast (`Engine.forecast`) after every unit. A cutover proved
ready proposes the switch as a question, with its evidence, and makes it
only on the operator's yes (`propose_switch`).

It asks only what migkit cannot find out: each question it may ask is a
`Question` in `QUESTIONS`, with its kind and the reason it is the
operator's. A step that needs one stops on it; nothing is guessed, no
scope is widened, nothing is written without the authorization to write.
"""
import time
from dataclasses import dataclass, field
from pathlib import Path

from . import decide, durable

#: what the operator says the move is for -> the hop's goal and its steps
FLOWS = {
    "copy": ("copy", ("move", "verify")),
    "sync": ("keep-in-sync", ("move", "follow", "verify")),
    "cutover": ("cutover", ("move", "follow", "verify", "ready")),
}
GOALS = tuple(FLOWS)
#: what each step does, in an operator's words
STEP_SAID = {"assess": "assessment", "schema": "schema", "move": "copy",
             "constraints": "keys and constraints", "follow": "catch-up",
             "verify": "proof", "ready": "cutover readiness"}
#: a move step for the goal: a sync and a cutover keep the stream running
MOVE_MODE = {"copy": "full", "sync": "full+cdc", "cutover": "full+cdc"}

INTENT, ACCESS, BUSINESS, AUTHORIZE = ("intent", "access",
                                       "business choice", "authorization")

#: the comparison's results that prove the target equal: "same" is the
#: verdict's own green word (`verdict.VERDICTS`), the others older words
PROVED = ("same", "equal", "pass")


# --- each phase's contract (P0-WORKFLOW-DEPTH.model) -------------------------
# A phase of the flow is a `planner.Phase` with its contract: what it leaves
# true, the evidence a done one leaves in `workflow.json`, the evidence each
# phase it waits on must have left (its entry criteria), and what its own
# result must show to count as done (its exit criteria).

#: step -> (outcome, evidence a done one leaves, exit criteria on
#: {"result": what the step returned})
CONTRACTS = {
    "assess": ("every check of the hop and of each database read, and what"
               " fails said with the database it holds apart",
               ("the hop's checks read", "each database's checks read"),
               (("the checks answered for each database",
                 lambda f: isinstance(f.get("result"), list)),)),
    "schema": ("the target's tables made as the order chosen says",
               ("the target's schema applied",),
               (("the schema step did not refuse",
                 lambda f: f.get("result") is not False),)),
    "move": ("every table in scope copied, each unit closed in the ledger",
             ("every table's rows landed", "each unit closed in the ledger"),
             (("the copy did not refuse",
               lambda f: f.get("result") is not False),)),
    "constraints": ("the keys and constraints made on the rows, checked"
                    " whole", ("the keys and constraints in place",),
                    (("the constraints step did not refuse",
                      lambda f: f.get("result") is not False),)),
    "follow": ("the target caught up with the source's changes",
               ("the stream caught up",),
               (("the stream says it caught up",
                 lambda f: f.get("result") is True),)),
    "verify": ("the target proved equal to the source after its one repair",
               ("the verdict of the comparison",),
               (("the verdict is equal",
                 lambda f: f.get("result") in PROVED),)),
    "ready": ("the cutover's readiness read, every check passing",
              ("the cutover's readiness read",),
              (("every readiness check passes",
                lambda f: f.get("result") is True),)),
}
#: the steps the flow composes only where the steps offered do them
OWN_STEPS = ("schema", "constraints")


def _said(key):
    step, _, db = key.partition(":")
    return STEP_SAID[step] + (f" of {db}" if db else "")


def _entry(key, ev):
    """The entry criteria of waiting on `key`: each evidence it left."""
    return tuple((f"{e} by the {_said(key)}",
                  lambda f, k=key, e=e: e in (f.get("evidence", {}).get(k)
                                              or ()))
                 for e in ev)


def compose(goal, dbs, can=(), facts=None):
    """(the order chosen for each database, [planner.Phase]) of `goal`'s
    flow over `dbs`, each phase with its contract. The order is the one
    decision engine's (`decide.phase_order`) from what each database's
    `facts` say of its dependencies and the engines; a schema and a
    constraints phase are composed where the steps offered (`can`) do
    them, and the copy carries them where not. A phase waits on the one
    before it and names the evidence it needs of it; another database's
    phases are independent of it and go on while it fails."""
    from .planner import Phase
    if goal not in FLOWS:
        raise SystemExit(f"goal {goal}: one of {', '.join(GOALS)}")
    choices = {}
    root = Phase(STEP_SAID["assess"], step="assess",
                 outcome=CONTRACTS["assess"][0], scope=tuple(dbs) or ("",),
                 root=True, evidence=CONTRACTS["assess"][1],
                 entry=(("the hop's databases are listed",
                         lambda f: f.get("dbs") is not None),),
                 exit=CONTRACTS["assess"][2],
                 independent=tuple(f"each check of {d}" for d in dbs))
    out = [root]
    for d in dbs:
        f = (facts or {}).get(d) or {}
        c = decide.phase_order(f)
        choices[d] = c
        seq = ["schema"] if "schema" in can else []
        seq.append("move")
        # the keys come after the rows where the order says so, or where
        # the engine's own schema way leaves them for after (a fact of the
        # engine, handed in with the dependencies)
        if "constraints" in can and (
                c.path == "constraints-after" or "schema" in can
                and f.get("schema carries keys") is False):
            seq.append("constraints")
        seq += [s for s in FLOWS[goal][1] if s != "move"]
        before, others = root, [o for o in dbs if o != d]
        for step in seq:
            outcome_, ev, exit_ = CONTRACTS[step]
            if step == "move" and "schema" not in can:
                outcome_ += ("; the schema made by the copy, its keys and"
                             " constraints " + (
                                 "after the rows" if c.path ==
                                 "constraints-after" else
                                 "with each table before its rows"))
            ph = Phase(STEP_SAID[step], step=step, db=d, outcome=outcome_,
                       scope=(d,), evidence=ev,
                       prerequisites=((before.key, before.evidence),),
                       entry=_entry(before.key, before.evidence),
                       exit=exit_,
                       independent=tuple(f"every phase of {o}"
                                         for o in others))
            out.append(ph)
            before = ph
    return choices, out


def gaps(src, dst):
    """[(engine, side, kind, name, state, why)]: what the pair `src` ->
    `dst` lacks, read off the code (`decide.still_lacking`) - each shape,
    capability and depth item one of the two engines has no way for, or
    no proof of, with the engine that lacks it and the backlog row it
    waits on (P0-DECISION-ALL.engines). The flow says these before any
    costly step; nothing the pair lacks is skipped silently."""
    sides = ((src, "source and target"),) if src == dst else (
        (src, "source"), (dst, "target"))
    return [(eng, side, kind, name, state, why)
            for eng, side in sides
            for kind, name, state, why in decide.still_lacking(eng)[eng]]


def gap_lines(src, dst):
    """Each of `gaps`' rows in an operator's words."""
    return [f"{eng} ({side}) lacks the {kind} {name}: {state}"
            + (f" - {why}" if why else "")
            for eng, side, kind, name, state, why in gaps(src, dst)]


@dataclass(frozen=True)
class Question:
    """One thing only the operator can say."""
    key: str
    kind: str
    asked: str
    #: why migkit does not decide it itself
    reason: str
    #: words in a step's stop that mean this is what it needs
    marks: tuple = ()


QUESTIONS = (
    Question("source", ACCESS, "Where is the data now?",
             "where the data lives cannot be discovered"),
    Question("target", ACCESS, "Where should it go?",
             "where the data goes is the operator's to name"),
    Question("password", ACCESS, "Password for the user",
             "a sign-in the address does not carry cannot be found"),
    Question("goal", INTENT, "Copy once (copy), keep in sync (sync), or"
             " move and prepare the switch (cutover)?",
             "what the move is for changes what is written and when; it is"
             " never guessed"),
    Question("write", AUTHORIZE, "Run it now, writing to the target?",
             "nothing is written to a database without the operator's go"),
    Question("target_rows", BUSINESS, "The target's tables already hold"
             " rows: refuse, replace (what they hold is removed) or keep"
             " (left out)? Say it as target_rows on the hop",
             "replacing rows removes data the operator owns",
             marks=("target_rows",)),
    Question("grant", ACCESS, "The users lack what the steps need: run the"
             " script the assessment wrote, or grant it, then run again",
             "a grant is the database owner's to give",
             marks=("grant", "privilege", "permission denied",
                    "not allowed")),
    Question("switch", AUTHORIZE, "Every database is proved ready: switch"
             " the applications to the target now?",
             "the switch moves where the applications write; when is the"
             " operator's to say"),
)
BY_KEY = {q.key: q for q in QUESTIONS}


class Asked(SystemExit):
    """A step needs what only the operator can say."""

    def __init__(self, question, detail=""):
        self.question = question
        super().__init__(f"{question.asked} ({question.kind}: "
                         f"{question.reason})"
                         + (f" - {detail}" if detail else ""))


def ask(answer, key, default=""):
    """The answer to `QUESTIONS`' `key`, from `answer(question, default)`;
    no `answer` (no terminal) stops on it as `Asked`. A question not in
    `QUESTIONS` is never asked."""
    q = BY_KEY[key]
    from . import telemetry
    telemetry.intervened("asked", key)
    if answer is None:
        raise Asked(q)
    got = answer(q, default)
    telemetry.intervened("answered", key)
    return got


def question_for(text):
    """The question a step's stop asks of the operator, or None."""
    low = str(text).lower()
    for q in QUESTIONS:
        if any(m in low for m in q.marks):
            return q
    return None


@dataclass
class Unit:
    """One step over one database ("" is the hop's own)."""
    step: str
    db: str = ""
    after: tuple = ()
    state: str = "pending"          #: pending, done, failed, held, asked
    why: str = ""
    way: str = ""
    took: float = 0.0
    at: float = 0.0

    @property
    def key(self):
        return f"{self.step}:{self.db}" if self.db else self.step

    def row(self):
        return {"state": self.state, "why": self.why, "way": self.way,
                "took": round(self.took, 3), "at": self.at}


def units(goal, dbs, phases=None):
    """The units a goal's flow is made of, each with what it waits for:
    one a phase of `phases` (`compose`) where given."""
    if phases is not None:
        return [Unit(p.step, p.db, tuple(k for k, _ in p.prerequisites))
                for p in phases]
    out = [Unit("assess")]
    for d in dbs:
        before = ("assess",)
        for step in FLOWS[goal][1]:
            u = Unit(step, d, before)
            out.append(u)
            before = (u.key,)
    return out


@dataclass
class Workflow:
    """The units of one hop's flow and what became of them, kept on disk."""
    goal: str
    units: list
    path: Path = None
    decisions: list = field(default_factory=list)
    #: key -> its `planner.Phase` (the contract), where composed
    phases: dict = field(default_factory=dict)
    #: key -> the evidence its phase left when it was done
    evidence: dict = field(default_factory=dict)

    def by_key(self):
        return {u.key: u for u in self.units}

    def save(self):
        if self.path is None:
            return
        for u in self.units:
            if u.key in self.phases:
                self.phases[u.key].state = PHASE_OF[u.state]
        durable.write_json(self.path, {
            "goal": self.goal,
            "units": {u.key: u.row() for u in self.units},
            "phases": {k: p.row() for k, p in self.phases.items()},
            "evidence": self.evidence,
            "decisions": self.decisions[-500:]}, indent=1)

    @classmethod
    def load(cls, path, goal, dbs, phases=None):
        """The flow, with what an earlier run of the same goal finished; a
        unit it left running is failed-unknown - what it did is reconciled
        before it runs again (`run`)."""
        wf = cls(goal, units(goal, dbs, phases), Path(path) if path else None,
                 phases={p.key: p for p in phases or ()})
        kept = durable.read_state(path, "state") if path else None
        if not kept or kept.get("goal") != goal:
            return wf
        rows = kept.get("units") or {}
        ev = kept.get("evidence") or {}
        for u in wf.units:
            r = rows.get(u.key)
            if r and r.get("state") == "done":
                u.state, u.way = "done", r.get("way", "")
                u.took, u.at = float(r.get("took") or 0), r.get("at") or 0
                if u.key in ev:
                    wf.evidence[u.key] = list(ev[u.key])
            elif r and r.get("state") in ("running", "failed-unknown"):
                u.state = "failed-unknown"
                u.why = "a run before stopped while it ran"
        wf.decisions = list(kept.get("decisions") or [])
        return wf

    def again(self):
        """The units this run does: every one not done, and a done
        catch-up before a proof still owed - the stream is caught up again
        at the moment the proof reads it."""
        keys = self.by_key()
        owed = {u.key for u in self.units if u.state != "done"}
        for u in self.units:
            if u.step == "follow" and u.state == "done" and any(
                    k in owed and u.key in keys[k].after for k in keys):
                owed.add(u.key)
        if self.goal == "cutover" and not self.switched():
            # readiness is read at the moment the switch is proposed
            for u in self.units:
                if u.step in ("follow", "ready"):
                    owed.add(u.key)
        return owed

    def switched(self):
        return any(d.get("what") == "switched" for d in self.decisions)

    def decided(self, unit, what, why="", way="", **more):
        """Keep one decision; returned so its postcondition can be added
        once the next run of the unit says it."""
        got = {"unit": unit.key, "what": what, "why": why, "way": way,
               "at": time.time(), **more}
        self.decisions.append(got)
        return got


#: a unit's state -> its phase's durable state (`planner.PHASE_STATES`): a
#: held unit waits on a phase that did not finish, so it is blocked
PHASE_OF = {"pending": "not-started", "running": "running", "done": "done",
            "failed": "failed", "held": "blocked", "asked": "blocked",
            "failed-unknown": "failed-unknown"}


#: the bounds of the wait before a unit is tried again from its position
BACKOFF_MIN, BACKOFF_MAX = 1.0, 300.0
def unmeasured_tries():
    """The runs a unit may make where nothing measures what a try costs:
    the recovery ladder's own bound (`faults.retried`), kept there once."""
    import inspect
    from . import faults
    return inspect.signature(faults.retried).parameters["tries"].default


def tries_for(ops, u, facts):
    """The runs of a step one unit may make, every recovery counted: as
    many attempts as the unit's forecast work pays for, each costed at
    what one attempt of it was measured to take (`attempt_seconds`, else
    its kept run) - recovery never spends more time than the work is
    worth; the work itself and one recovery of it always fit. Where
    neither is measured, `unmeasured_tries`."""
    import math
    eta = _seconds(ops, u.step, u.db)
    took = facts.get("attempt_seconds") or u.took or None
    if not eta or not took:
        return unmeasured_tries()
    return max(2, 1 + math.ceil(float(eta) / max(float(took), BACKOFF_MIN)))


def scale_floor(facts):
    """The least share of the plan throttles may leave a unit: one worker
    of the measured budget and one row of the planned piece - below that
    a cut changes nothing but the time."""
    most = facts.get("most") or facts.get("start") or facts.get("little")
    chunk = facts.get("chunk")
    return max(1 / int(most) if most else 0.0,
               1 / int(chunk) if chunk else 0.0)


def backoff(took, tries):
    """Seconds to wait before trying a unit again: the time its failed
    attempt took (a server that refused at once is retried soon, one that
    failed after a long wait is given as long), doubled each try,
    bounded."""
    base = min(BACKOFF_MAX, max(BACKOFF_MIN, float(took or 0)))
    return min(BACKOFF_MAX, base * (2 ** max(0, tries - 1)))


@dataclass
class Plan:
    """What the units left run at, re-planned after an isolate or a
    throttle: the share of each unit's planned size a throttle left, and
    why the plan last changed (cleared once the units left are re-planned)."""
    scale: float = 1.0
    changed: str = ""
    #: the least `scale` the measured budget leaves (`scale_floor`)
    floor: float = 0.0
    #: one cooling-down record of the sides this flow's units fail on
    breaker: object = None

    def chunk(self, facts):
        return max(1, int(int(facts.get("chunk") or 500000) * self.scale))

    def workers(self, facts):
        """(workers, `decide.Choice`) the copy of one database runs: its
        share of the measured budget (`model.Model.budget`), cut by what
        the throttles left; (None, choice) where nothing was measured, so
        the copy asks the same rung with what it measures itself."""
        n, choice = decide.worker_budget({**facts, "scale": self.scale})
        return (n if choice.path != "one at a time" else None), choice

    def cut(self):
        self.scale = max(self.floor, self.scale / 2)


def recovery_facts(told, repeats, reconciled, can_check, tries_left):
    """The facts `decide.recovery` reads for one failure of a unit: the
    class and the actions the catalogue admits for it (`faults.CATALOGUE`),
    whether its effect is known, and the side cooling down where the same
    class failed this unit again (a wait, not the same retry, comes next)."""
    f = told.fault
    return {"class": f.name, "admits": f.admits, "transient": f.transient,
            "uncertain": told.uncertain, "can_check": can_check,
            "renewable": False, "reconciled": reconciled,
            "tries_left": tries_left,
            "breaker": "open" if repeats > 1 else "closed",
            "phase": told.phase}


STATES = ("committed", "running", "nothing")


def settle(u, ops):
    """{operation: state} of what a run before left begun under the step,
    reconciled on the target before anything repeats it: the ledger's
    uncertain operations (`ops.uncertain(step, db)`,
    `attempts.Ledger.uncertain`), each read for its actual effect
    (`ops.settle(step, db, key, record)`: a count, a position or a marker
    through `faults.landed`) - "committed", "running", "nothing", or None,
    not known. {} where the ops keep no ledger; a ledger that cannot be
    read is itself not known."""
    read = getattr(ops, "uncertain", None)
    if read is None:
        return {}
    try:
        left = dict(read(u.step, u.db) or {})
    except Exception:  # noqa: BLE001 - an unread ledger is not known
        return {"the ledger": None}
    check = getattr(ops, "settle", None)
    out = {}
    for key, rec in left.items():
        try:
            got = check(u.step, u.db, key, rec) if check else None
        except Exception:  # noqa: BLE001 - an unanswered read is not known
            got = None
        out[key] = got if got in STATES else None
    return out


def reconcile(u, ops, facts):
    """Whether a unit whose failed write has an unknown effect took
    effect: the step's own reading (`ops.reconcile(db)`) where it has
    one; else the ledger's operations read on the target (`settle`):
    "running" while one still runs, "nothing" once every one is read -
    what landed is kept and the step goes on past it; None, not known,
    where one cannot be read or nothing reads them. A kept position alone
    is never taken as proof that nothing landed after it."""
    read = getattr(ops, "reconcile", None)
    if read is not None:
        try:
            got = read(u.db)
        except Exception:  # noqa: BLE001 - an unanswered read is not known
            return None
        return got if got in STATES else None
    if getattr(ops, "uncertain", None) is None:
        return None
    states = settle(u, ops)
    if any(v is None for v in states.values()):
        return None
    return "running" if "running" in states.values() else "nothing"


def can_reconcile(ops, facts):
    return (getattr(ops, "reconcile", None) is not None
            or getattr(ops, "uncertain", None) is not None)


def _settled(u, wf, ops, say, work):
    """The reconcile contract every step keeps before it runs: what a run
    before it left begun is read on the target (`settle`); "" where every
    one was read, else the blocker - the stop `decide.recovery` chooses
    for an effect nothing can read - and the operator's next step."""
    states = settle(u, ops)
    if not states:
        return ""
    bad = sorted(k for k, v in states.items() if v is None)
    busy = sorted(k for k, v in states.items() if v == "running")
    rec = wf.decided(u, "reconcile", f"{len(states)} operations a run"
                     " before left open, read on the target", "reconcile",
                     states=states)
    rec["held"] = not bad and not busy
    if rec["held"]:
        say(f"{work}: {len(states)} operations a run before left open were"
            " read on the target before it runs again")
        return ""
    choice = decide.recovery({
        "class": "unknown_outcome", "admits": (), "transient": False,
        "uncertain": True, "can_check": False, "renewable": False,
        "reconciled": None, "tries_left": 0, "breaker": "closed"})
    if bad:
        return (f"{len(bad)} operations a run before began cannot be read"
                f" on the target, so they are not run again; {choice.said}:"
                " check the target for them, then run again")
    return (f"{len(busy)} operations a run before began still run on the"
            f" target; {choice.said}: wait for them to end, then run again")


@dataclass
class Outcome:
    workflow: Workflow
    #: database -> (state, words)
    databases: dict
    #: the switch: "switched", "asked", "declined", "failed", "not ready"
    #: or "" where the goal has none
    switch: str = ""

    @property
    def complete(self):
        return all(u.state == "done" for u in self.workflow.units)

    @property
    def exit(self):
        return 0 if self.complete and self.switch != "failed" else 1


def run(goal, ops, say, path=None, wait=time.sleep, now=time.time,
        answer=None, go=False):
    """The flow of `goal` over `ops` to its outcome.

    `ops` does the steps: `dbs()`, `facts(db)` (`resumes`, `pieces`,
    `chunk`), `assess()` -> [(db or "", why)] that fail, `move(db, chunk)`,
    `follow(db)` -> False where not caught up, `verify(db)` -> the
    verdict after its one repair, `ready(db)` -> [why] that fail,
    `forecast(kind, db)` -> words, `progress(db)` -> [words]; where it
    has them, `eta(kind, db)` -> seconds left, `reconcile(db)` and
    `switch(db)`. A cutover proved ready proposes the switch (`switch`):
    `answer(question, default)` says yes or no, `go` is the yes given up
    front; with neither it is asked, never done.

    Where `ops` has them: `pair()` -> (source engine, target engine), whose
    gaps (`gaps`) are said before any step; `order(db)` -> the facts the
    order of its phases is chosen from (`decide.phase_order`); `schema(db)`
    and `constraints(db)`, phases of their own (`compose`); `eng`, whose
    ledger's units a run before left open are closed as cut off
    (`attempts.reconcile_units`) before a unit it left running is run
    again. Each phase enters, waits or is blocked as `decide.phase_step`
    chooses from its prerequisites and entry criteria, and counts as done
    only where its exit criteria hold; the evidence it leaves is kept."""
    if goal not in FLOWS:
        raise SystemExit(f"goal {goal}: one of {', '.join(GOALS)}")
    dbs = list(ops.dbs())
    pair = ops.pair() if callable(getattr(ops, "pair", None)) else None
    told = gap_lines(*pair) if pair else []
    for line in told:
        say(line)
    order = getattr(ops, "order", None)
    handed = {d: order(d) for d in dbs} if callable(order) else None
    choices, phases = compose(
        goal, dbs, [s for s in OWN_STEPS if callable(getattr(ops, s, None))],
        handed)
    wf = Workflow.load(path, goal, dbs, phases)
    hop = Unit("assess")
    if told:
        wf.decided(hop, "gaps", f"{len(told)} things the pair lacks said"
                   " before any step", gaps=told)
    for d, c in choices.items():
        # the dependency facts the order stood on, kept with it
        f = (handed or {}).get(d) or {}
        wf.decided(hop, "order", f"{d}: {c.said} - {c.reason}", c.path,
                   facts={k: f.get(k) for k in c.asked},
                   keys=f.get("foreign keys"),
                   first=f.get("parents first"))
    unknown = [u for u in wf.units if u.state == "failed-unknown"]
    if unknown:
        from . import attempts
        cut = attempts.reconcile_units(getattr(ops, "eng", None))
        for u in unknown:
            wf.decided(u, "reconciled", "a run before stopped while it ran;"
                       " its open units closed as cut off before it runs"
                       " again", cut=list(cut or ()))
        say(f"{len(unknown)} step(s) a run before left running: reconciled"
            f" ({len(cut or ())} open unit(s) closed) before they run again")
    owed = wf.again()
    keys = wf.by_key()
    total = len(wf.units)
    from . import faults
    apart, seen = {}, set()
    plan = Plan(breaker=faults.Breaker())
    while True:
        u = next((x for x in wf.units
                  if x.key in owed and x.key not in seen), None)
        if u is None:
            break
        seen.add(u.key)
        now_ = _transition(u, wf, keys, dbs)
        if now_.path != "enter":
            u.state = "held"
            u.why = now_.reason
            wf.decided(u, "held", u.why, now_.path)
            continue
        if u.db in apart and u.step == "move":
            u.state, u.why = "failed", f"the assessment: {apart[u.db]}"
            wf.decided(u, "failed", u.why)
            say(f"{u.db}: {u.why}; the rest goes on without it")
        else:
            u.state = "running"
            wf.save()
            got = _unit(u, wf, ops, say, wait, now, plan)
            _exited(u, wf, got, say)
            if u.step == "assess" and u.state == "done":
                # what fails for one database isolates that one only
                apart.update((d, why) for d, why in got or () if d)
        if plan.changed:
            replan(wf, ops, plan, say)
        wf.save()
        say(_progress(wf, ops, total))
    switch = propose_switch(wf, ops, dbs, say, answer, go)
    wf.save()
    return Outcome(wf, outcome(wf, dbs), switch)


def _transition(u, wf, keys, dbs):
    """`decide.phase_step`'s choice for `u` now, from what its phase waits
    on: a prerequisite that failed blocks it with the evidence missing, one
    still owed holds it, entry criteria that fail stop it."""
    ph = wf.phases.get(u.key)
    owed = [keys[k] for k in u.after if keys[k].state != "done"]
    bad = [x for x in owed if x.state != "pending"]
    facts = {"prerequisites owed": ", ".join(_said(x.key) for x in owed)}
    if bad:
        x = bad[0]
        lack = dict(ph.missing(wf.evidence)).get(x.key) if ph else None
        facts["prerequisite failed"] = (
            f"the {_said(x.key)}, which "
            + ("failed" if x.state in ("failed", "failed-unknown") else
               "did not finish") + (f" ({x.why})" if x.why else "")
            + (f", so {', '.join(lack)} is missing" if lack else ""))
    if ph and not owed:
        unmet = ph.unmet({"evidence": wf.evidence, "dbs": dbs})
        facts["entry unmet"] = "; ".join(unmet)
    from dataclasses import replace
    got = decide.phase_step(facts)
    if got.path == "blocked" and facts.get("prerequisite failed"):
        return replace(got, reason=f"blocked: {facts['prerequisite failed']};"
                       " it runs once that is put right and the flow is run"
                       " again")
    if got.path == "wait" and owed:
        return replace(got, reason=f"waits for the {_said(owed[0].key)}")
    return got


def _exited(u, wf, got, say):
    """A done unit counts as done only where its phase's exit criteria hold
    on what it returned; then the evidence it leaves is kept."""
    ph = wf.phases.get(u.key)
    if u.state != "done" or ph is None:
        return
    unmet = ph.unmet({"result": got}, "exit")
    if unmet:
        u.state, u.way = "failed", ""
        u.why = "its exit criteria do not hold: " + "; ".join(unmet)
        wf.decided(u, "failed", u.why)
        say(f"{_said(u.key)}: {u.why}; the rest goes on without it")
        return
    wf.evidence[u.key] = list(ph.evidence)


def _progress(wf, ops, total):
    done = sum(1 for x in wf.units if x.state == "done")
    left = [x for x in wf.units if x.state == "pending"]
    line = f"{done}/{total} done"
    bad = [x for x in wf.units if x.state in ("failed", "asked")]
    if bad:
        line += f", {len(bad)} held back"
    nxt = next((x for x in left if x.db), None)
    if nxt is not None:
        try:
            line += f"; {ops.forecast(nxt.step, nxt.db)}"
        except Exception:  # noqa: BLE001 - the progress says the rest
            pass
        for extra in ops.progress(nxt.db) or ():
            line += f"; {extra}"
    return line


def _seconds(ops, kind, db):
    eta = getattr(ops, "eta", None)
    if eta is None:
        return None
    try:
        got = eta(kind, db)
    except Exception:  # noqa: BLE001 - not measured: keeps its place
        return None
    return float(got) if isinstance(got, (int, float)) else None


def replan(wf, ops, plan, say):
    """The units left, re-planned from what is known now, after an isolate
    or a throttle (`plan.changed`): each database's facts read again, the
    sizes at what the throttle left (`Plan.scale`), and the databases
    whose remaining work the engine forecasts shortest run first - while
    a server is pressed or a unit is set apart, the most databases reach
    their proof; one not measured keeps its place after them. A database's
    own steps keep their order. The scope left and its forecast are said
    and kept with the decisions."""
    why, plan.changed = plan.changed, ""
    apart = {u.db for u in wf.units if u.state in ("failed", "asked")}
    left = [u for u in wf.units
            if u.state == "pending" and u.db and u.db not in apart]
    if not left:
        return
    dbs = list(dict.fromkeys(u.db for u in left))
    secs = {d: _seconds(ops, next(u.step for u in left if u.db == d), d)
            for d in dbs}
    ranked = sorted(dbs, key=lambda d: (secs[d] is None, secs[d] or 0.0,
                                        dbs.index(d)))
    rank = {d: i for i, d in enumerate(ranked)}
    wf.units = ([u for u in wf.units if u.db not in rank]
                + sorted((u for u in wf.units if u.db in rank),
                         key=lambda u: rank[u.db]))
    known = [s for s in secs.values() if s is not None]
    scope = (f"{len(left)} steps left over {', '.join(ranked)}"
             if ranked else "nothing left to run")
    said = (f"replanned after {why}: {scope}; sizes at"
            f" {plan.scale:.0%} of the plan")
    if known:
        said += f"; about {sum(known):.0f}s of measured work left"
    if len(known) < len(dbs):
        said += f", {len(dbs) - len(known)} not measured yet"
    wf.decisions.append({"unit": "plan", "what": "replan", "why": said,
                         "way": ",".join(ranked), "scale": plan.scale,
                         "at": time.time()})
    say(said)


def _unit(u, wf, ops, say, wait, now, plan):
    """One unit run to done, asked or failed. Before it runs, what a run
    before it left begun is reconciled (`_settled`). The step then runs
    under the one recovery ladder (`faults.retried`: `decide.recovery`
    choosing from the catalogue, `reconcile` its check), a cause a size
    answers retuned inside it (`tuning.sized`), never the same work
    again. Only the workflow's own paths are taken here, from the choice
    the ladder made: a throttle cuts what the units left run at
    (`Plan.cut`) and re-plans them, a replan or a probe reads the facts
    again once each, an isolate sets the unit apart. Each choice and
    whether its postcondition held kept in the decisions."""
    from . import faults, tuning
    began = now()
    facts = dict(ops.facts(u.db)) if u.db else {}
    work = STEP_SAID[u.step] + (f" of {u.db}" if u.db else "")
    run = {"chunk": plan.chunk(facts), "way": "as-planned", "last": None,
           "err": None, "took": 0.0, "reconciled": None, "workers": None,
           "runs": 0}
    got = None
    blocked = _settled(u, wf, ops, say, work)
    if blocked:
        u.state, u.why = "failed", blocked
        return _ended(u, wf, say, plan, began, now, run, got)
    plan.floor = max(plan.floor, scale_floor(facts))
    tries = tries_for(ops, u, facts)

    def sized():
        if u.step == "move":
            run["workers"], _ = plan.workers(facts)
    sized()
    retune = (tuning.Retune(f"the copy of {u.db}", rows=run["chunk"])
              if u.step == "move" and facts.get("pieces") else None)

    def taken():
        # the ladder's choice for the failure before this try, as the
        # workflow's own path
        e, run["err"] = run["err"], None
        if e is None:
            return
        _held(run["last"], False)
        path = getattr(e, "recovery", "") or ""
        f = faults.CATALOGUE.get(getattr(e, "fault", ""))
        said = f.said if f else _words(e)
        if path == "throttle":
            plan.cut()
            run["chunk"] = max(1, run["chunk"] // 2)
            sized()
            plan.changed = f"{work} was throttled"
        run["way"] = "resume" if path in ("retry", "reconcile") else path
        run["last"] = wf.decided(
            u, "recover", f"{said}: {path}", path,
            fault=getattr(e, "fault", ""), post=f.retry_after if f else "",
            reconciled=run["reconciled"])
        say(f"{work}: {said}; {path}, again from its last position")

    def apply(settings):
        rows = max(1, int(settings.get("rows") or run["chunk"]))
        if run["err"] is not None and rows != run["chunk"]:
            # a size answered the failure: retuned, not recovered
            e, run["err"] = run["err"], None
            _held(run["last"], False)
            run["way"] = "retuned"
            run["last"] = wf.decided(
                u, "recover", f"{_words(e)}: {rows} rows a piece", "retune",
                post="the unit completes at the new size")
            say(f"{work}: {rows} rows a piece after {_words(e)}")
        run["chunk"] = rows

    def once():
        taken()
        run["runs"] += 1
        t0 = now()
        try:
            return _step(u, ops, run["chunk"], run["workers"])
        except decide.Verdict as e:
            raise _Final(e) from None
        except Exception as e:
            run["took"], run["err"] = now() - t0, e
            q = question_for(e)
            if q is not None:
                raise _Final(Asked(q, _words(e))) from None
            raise

    def check():
        run["reconciled"] = reconcile(u, ops, facts)
        return run["reconciled"]

    def step():
        if retune is None:
            return once()
        return tuning.sized(retune, u.key, once, apply)

    spent = set()
    while True:
        try:
            got = faults.retried(
                step, tries=tries - run["runs"], side=u.db or "hop",
                label=work,
                writes=u.step == "move",
                check=check if can_reconcile(ops, facts) else None,
                log=say, wait=wait, breaker=plan.breaker,
                backoff=lambda i: backoff(run["took"], i + 1))
        except _Final as e:
            err = e.error
            if isinstance(err, Asked):
                u.state, u.why = "asked", str(err)
            else:
                u.state, u.why = "failed", _words(err)
        except SystemExit as e:
            # migkit's own stop: said, never retried here
            q = question_for(e)
            u.state = "asked" if q else "failed"
            u.why = str(Asked(q, _words(e))) if q else _words(e)
        except Exception as e:  # noqa: BLE001 - classified, never success
            path = getattr(e, "recovery", "") or "stop"
            f = faults.CATALOGUE.get(getattr(e, "fault", ""))
            said = f.said if f else _words(e)
            run["err"] = None
            _held(run["last"], False)
            run["last"] = wf.decided(
                u, "recover", f"{said}: {path}", path,
                fault=getattr(e, "fault", ""),
                post=f.retry_after if f else "",
                reconciled=run["reconciled"])
            if run["reconciled"] in STATES[1:] and run["runs"] < tries:
                # read as nothing landed (or still running): a new attempt
                # from its position, its runs counted against the same tries
                run["reconciled"], run["way"] = None, "resume"
                continue
            if path in ("probe", "replan") and path not in spent:
                # read again what the step reads; one try of each
                spent.add(path)
                facts = dict(ops.facts(u.db)) if u.db else {}
                run["chunk"] = plan.chunk(facts)
                sized()
                run["way"] = path
                say(f"{work}: {said}; {path}")
                continue
            u.state = "failed"
            if path == "reconcile" and run["reconciled"] not in STATES[1:]:
                u.why = (f"{said}; what it did cannot be read, so it is not"
                         " run again: check the target for it, then run"
                         " again")
            elif path in ("retry", "pause", "throttle", "reconcile",
                          "probe", "replan") \
                    or run["reconciled"] in STATES[1:]:
                u.why = f"{said}; every try spent ({tries})"
            elif path == "isolate":
                u.why = f"{said}; set apart, the rest goes on"
                plan.changed = f"{work} was set apart"
            else:
                u.why = f"{said}; stopped: " + (f.retry_after if f
                                                 else "not recoverable")
        else:
            u.state, u.why = "done", ""
            if run["reconciled"] == "committed":
                run["way"] = "reconciled"
            _held(run["last"], True)
        break
    return _ended(u, wf, say, plan, began, now, run, got)


def _ended(u, wf, say, plan, began, now, run, got):
    work = STEP_SAID[u.step] + (f" of {u.db}" if u.db else "")
    if u.state == "done":
        u.way = run["way"]
    else:
        _held(run["last"], False)
    u.took, u.at = now() - began, now()
    wf.decided(u, u.state, u.why, u.way)
    if u.state != "done":
        say(f"{work}: {u.why}; the rest goes on without it")
        if u.state == "failed" and u.db and not plan.changed:
            plan.changed = f"{work} was set apart"
    return got


def _held(record, ok):
    """Whether a recovery's postcondition held: the next run of the unit
    completed, or failed again."""
    if record is not None and "held" not in record:
        record["held"] = ok


def _words(e):
    from .movers import DRIVEN
    from .wording import without_programs
    text = str(e).strip() or type(e).__name__
    return without_programs(text.splitlines()[-1][:300], DRIVEN)


class _Final(BaseException):
    """A step's answer no recovery may retry - a verdict, a question -
    carried through the ladder unchanged."""

    def __init__(self, error):
        super().__init__(str(error))
        self.error = error


def _step(u, ops, chunk, workers=None):
    if u.step == "assess":
        bad = list(ops.assess())
        hop_wide = [why for d, why in bad if not d]
        if hop_wide:
            raise SystemExit("; ".join(hop_wide))
        return bad
    if u.step == "move":
        return ops.move(u.db, chunk, workers)
    if u.step in OWN_STEPS:
        return getattr(ops, u.step)(u.db)
    if u.step == "follow":
        if ops.follow(u.db) is False:
            raise TimeoutError(f"the stream of {u.db} has not caught up")
        return True
    if u.step == "verify":
        got = ops.verify(u.db)
        if got not in PROVED:
            raise decide.Verdict(f"{u.db} is {got} after its repair")
        return got
    if u.step == "ready":
        bad = list(ops.ready(u.db))
        if bad:
            raise SystemExit("; ".join(bad))
        return True
    raise SystemExit(f"no step {u.step}")


def evidence_of(wf, db):
    """What proves `db` ready for the switch, in an operator's words: each
    step done and the way it went."""
    mine = [u for u in wf.units if u.db == db]
    return ", ".join(STEP_SAID[u.step]
                     + (f" ({u.way})" if u.way not in ("", "as-planned")
                        else "") for u in mine if u.state == "done")


def propose_switch(wf, ops, dbs, say, answer, go):
    """The cutover's switch, proposed only when every database of the hop
    is proved ready in this run (the applications move as one), with the
    evidence; performed only on the operator's yes - `go` is that yes,
    given up front by the switch's own consent (`migrate --switch`), never
    by the consent to write - and kept in the decisions, never done on migkit's own."""
    if wf.goal != "cutover":
        return ""
    if wf.switched():
        return "switched"
    mine = [u for u in wf.units if u.db]
    if not dbs or any(u.state != "done" for u in mine):
        held = sorted({u.db for u in mine if u.state != "done"})
        if held:
            say(f"the switch is not proposed: {', '.join(held)} not ready")
        return "not ready"
    switch = getattr(ops, "switch", None)
    if switch is None:
        return ""
    proof = "; ".join(f"{d}: {evidence_of(wf, d)}" for d in dbs)
    say(f"ready for the switch - {proof}")
    q = BY_KEY["switch"]
    if go:
        yes, how = True, "the switch's yes given up front"
    elif answer is None:
        wf.decisions.append({"unit": "switch", "what": "asked", "why":
                             f"{q.asked} ({q.kind}: {q.reason})",
                             "way": "", "evidence": proof,
                             "at": time.time()})
        say(f"{q.asked} ({q.kind}: {q.reason})")
        return "asked"
    else:
        yes = str(answer(q, "n")).strip().lower() in ("y", "yes")
        how = "the operator's answer"
    if not yes:
        wf.decisions.append({"unit": "switch", "what": "declined",
                             "why": how, "way": "", "evidence": proof,
                             "at": time.time()})
        return "declined"
    state = "switched"
    for d in dbs:
        try:
            switch(d)
        except (Exception, SystemExit) as e:  # noqa: BLE001 - said, kept
            state = "failed"
            wf.decisions.append({"unit": f"switch:{d}", "what": "failed",
                                 "why": _words(e), "way": how,
                                 "evidence": proof, "at": time.time()})
            say(f"the switch of {d} stopped: {_words(e)}")
            break
        wf.decisions.append({"unit": f"switch:{d}", "what": "switched",
                             "why": how, "way": "", "evidence":
                             evidence_of(wf, d), "at": time.time()})
    return state


def outcome(wf, dbs):
    """Each database's outcome in one line: what it reached and, where it
    stopped, why and what was kept."""
    out = {}
    assess = wf.by_key()["assess"]
    switched = {d["unit"].split(":", 1)[1] for d in wf.decisions
                if d.get("what") == "switched"
                and str(d.get("unit", "")).startswith("switch:")}
    for d in dbs:
        mine = [u for u in wf.units if u.db == d]
        done = [STEP_SAID[u.step] for u in mine if u.state == "done"]
        bad = next((u for u in mine if u.state in ("failed", "asked")), None)
        if assess.state != "done":
            out[d] = ("held", f"not started: {assess.why}")
        elif bad is None and len(done) == len(mine):
            out[d] = ("done", {"copy": "copied and proved equal",
                               "sync": "copied, followed and proved equal",
                               "cutover": "copied, followed, proved equal"
                               + (" and switched to the target"
                                  if d in switched else
                                  " and ready for the switch, which waits"
                                  " for the operator")}[wf.goal])
        elif bad is not None:
            out[d] = (bad.state, f"{STEP_SAID[bad.step]} stopped: {bad.why}"
                      + (f"; kept: {', '.join(done)}" if done else ""))
        else:
            out[d] = ("held", "waits for an earlier step")
    return out


def evidence(path, last=50):
    """The decisions of the last runs, for `migrate --why`."""
    kept = durable.read_state(path, "state") if Path(path).exists() else None
    return list((kept or {}).get("decisions") or [])[-last:]


#: the scenario harness's names -> the comparison's rows
#: (docs/research/autonomous-comparison.md, tests/test_autonomous_scenarios.py)
SCENARIOS = {"healthy": "healthy",
             "recoverable in the run": "fault mid-copy, recovered in the run",
             "recoverable by a run again": "crash mid-copy, run again",
             "unrecoverable": "refused target (unrecoverable)",
             "baseline, workers pinned to 1": "baseline, workers pinned to 1",
             "interrupted three times": "interrupted three times, run again",
             "a value above the packet size":
                 "a value above the target's packet size",
             "injected defects": "injected defects, then the check",
             "asymmetric capacity": "target capped below its source"}
ENGINES = {"mysql": "MySQL", "postgres": "PostgreSQL"}
#: the measured columns, in the table's order after scenario and engine
MEASURED = ("inputs", "interventions", "completed", "recovered",
            "retry_waste", "seconds", "forecast_error", "workers",
            "questions")


def _cell(key, v):
    if v is None:
        return "none needed" if key == "recovered" else "not measured"
    if key == "completed":
        return "yes" if v else "no"
    if key == "recovered":
        return f"{float(v):.0%}"
    return f"{v:g}" if isinstance(v, float) else str(v)


def comparison(lines, table):
    """`table` (the comparison's markdown) with each scenario row's
    measured cells filled from the harness's JSON lines (one a run,
    `MIGKIT_MEASURE_OUT`): the last run of a scenario on an engine counts;
    a row no run measured is left as it was - a number is never written
    that no run produced. The managed-service column is the providers'
    documented band for each scenario (`scenarios.managed_rows`), each
    cell said as documented, not measured."""
    import json
    got = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        m = json.loads(line)
        row = (SCENARIOS.get(m.get("scenario")), ENGINES.get(m.get("engine")))
        if all(row):
            got[row] = m
    from .scenarios import managed_rows
    managed, at = managed_rows(), 2 + len(MEASURED)
    out = []
    for line in table.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        row = line.startswith("| ") and len(cells) > at \
            and cells[0] in SCENARIOS.values()
        if not row:
            out.append(line)
            continue
        m = got.get(tuple(cells[:2])) or {}
        for i, key in enumerate(MEASURED):
            if key in m:
                cells[2 + i] = _cell(key, m[key])
        if cells[0] in managed:
            cells[at] = managed[cells[0]]
        out.append("| " + " | ".join(cells) + " |")
    return "\n".join(out) + ("\n" if table.endswith("\n") else "")

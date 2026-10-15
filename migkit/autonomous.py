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
bounded, each choice and whether its postcondition held kept in
`workflow.json`. migkit's own stop is a blocker said in its words, never
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
STEP_SAID = {"assess": "assessment", "move": "copy", "follow": "catch-up",
             "verify": "proof", "ready": "cutover readiness"}
#: a move step for the goal: a sync and a cutover keep the stream running
MOVE_MODE = {"copy": "full", "sync": "full+cdc", "cutover": "full+cdc"}

INTENT, ACCESS, BUSINESS, AUTHORIZE = ("intent", "access",
                                       "business choice", "authorization")


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


def units(goal, dbs):
    """The units a goal's flow is made of, each with what it waits for."""
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

    def by_key(self):
        return {u.key: u for u in self.units}

    def save(self):
        if self.path is None:
            return
        durable.write_json(self.path, {
            "goal": self.goal,
            "units": {u.key: u.row() for u in self.units},
            "decisions": self.decisions[-500:]}, indent=1)

    @classmethod
    def load(cls, path, goal, dbs):
        """The flow, with what an earlier run of the same goal finished."""
        wf = cls(goal, units(goal, dbs), Path(path) if path else None)
        kept = durable.read_state(path, "state") if path else None
        if not kept or kept.get("goal") != goal:
            return wf
        rows = kept.get("units") or {}
        for u in wf.units:
            r = rows.get(u.key)
            if r and r.get("state") == "done":
                u.state, u.way = "done", r.get("way", "")
                u.took, u.at = float(r.get("took") or 0), r.get("at") or 0
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


#: the bounds of the wait before a unit is tried again from its position
BACKOFF_MIN, BACKOFF_MAX = 1.0, 300.0
#: the runs of a step one unit may make, every recovery counted: no
#: failure makes more than this of anything
TRIES = 4
#: the least of the planned sizes throttles may leave a unit
MIN_SCALE = 1 / 16


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

    def chunk(self, facts):
        return max(1, int(int(facts.get("chunk") or 500000) * self.scale))


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


def reconcile(u, ops, facts):
    """Whether a unit whose failed write has an unknown effect took
    effect: the step's own reading (`ops.reconcile(db)`: "committed",
    "running" or "nothing") where it has one; else, for a copy that keeps
    its position on disk, "nothing" past that position - the copy goes on
    from it and counts nothing after it as done; else None, not known."""
    read = getattr(ops, "reconcile", None)
    if read is not None:
        try:
            got = read(u.db)
        except Exception:  # noqa: BLE001 - an unanswered read is not known
            return None
        return got if got in ("committed", "running", "nothing") else None
    return "nothing" if facts.get("resumes") else None


def can_reconcile(ops, facts):
    return hasattr(ops, "reconcile") or bool(facts.get("resumes"))


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
    front; with neither it is asked, never done."""
    if goal not in FLOWS:
        raise SystemExit(f"goal {goal}: one of {', '.join(GOALS)}")
    dbs = list(ops.dbs())
    wf = Workflow.load(path, goal, dbs)
    owed = wf.again()
    keys = wf.by_key()
    total = len(wf.units)
    apart, seen, plan = {}, set(), Plan()
    while True:
        u = next((x for x in wf.units
                  if x.key in owed and x.key not in seen), None)
        if u is None:
            break
        seen.add(u.key)
        stuck = [keys[k] for k in u.after if keys[k].state != "done"]
        if stuck:
            u.state = "held"
            u.why = (f"waits for the {STEP_SAID[stuck[0].step]}"
                     + (f" of {stuck[0].db}" if stuck[0].db else ""))
            wf.decided(u, "held", u.why)
            continue
        if u.db in apart and u.step == "move":
            u.state, u.why = "failed", f"the assessment: {apart[u.db]}"
            wf.decided(u, "failed", u.why)
            say(f"{u.db}: {u.why}; the rest goes on without it")
        else:
            got = _unit(u, wf, ops, say, wait, now, plan)
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
    """One unit run to done, asked or failed. A failure is told by its
    class (`faults.classify`); a cause a size answers retunes the copy
    (`tuning.Retune`), never the same work again; every other failure is
    recovered as `decide.recovery` chooses from the catalogue: reconcile
    an effect not known, proceed on one that committed, pause, throttle,
    retry, replan, probe, isolate or stop - each choice and whether its
    postcondition then held kept in the decisions."""
    from . import faults, tuning
    began = now()
    facts = dict(ops.facts(u.db)) if u.db else {}
    chunk = plan.chunk(facts)
    retune = (tuning.Retune(f"the copy of {u.db}", rows=chunk)
              if u.step == "move" and facts.get("pieces") else None)
    work = STEP_SAID[u.step] + (f" of {u.db}" if u.db else "")
    tries, reconciled, last, repeats, got = 0, None, None, {}, None
    way, spent = "as-planned", set()
    while True:
        t0 = now()
        try:
            got = _step(u, ops, chunk)
        except decide.Verdict as e:
            u.state, u.why = "failed", str(e)
        except Asked as e:
            u.state, u.why = "asked", str(e)
        except SystemExit as e:
            # migkit's own stop: said, never retried here
            q = question_for(e)
            u.state = "asked" if q else "failed"
            u.why = str(Asked(q, _words(e))) if q else _words(e)
        except Exception as e:  # noqa: BLE001 - classified, never success
            took = now() - t0
            tries += 1
            _held(last, False)
            q = question_for(e)
            if q is not None:
                u.state, u.why = "asked", str(Asked(q, _words(e)))
                break
            told = faults.classify(e, writes=u.step == "move")
            name = told.fault.name
            repeats[name] = repeats.get(name, 0) + 1
            if retune is not None and tries < TRIES:
                try:
                    adj = retune.failed(u.key, e, rows=chunk)
                except decide.Blocked as b:
                    u.state, u.why = "failed", _words(b)
                    break
                if adj is not None:
                    chunk = max(1, int(retune.settings()["rows"]))
                    way = "retuned"
                    last = wf.decided(u, "recover", f"{told.fault.said}:"
                                      f" {adj.said}", "retune", fault=name,
                                      post="the unit completes at the new"
                                      " size", tries=tries)
                    say(f"{work}: {told.fault.said}; {adj.said}")
                    continue
            choice = decide.recovery(recovery_facts(
                told, repeats[name], reconciled,
                can_reconcile(ops, facts), TRIES - tries))
            act = choice.path
            last = wf.decided(u, "recover", f"{told.fault.said}:"
                              f" {choice.said}", act, fault=name,
                              post=told.fault.retry_after, tries=tries,
                              because=choice.reason)
            if act == "reconcile":
                reconciled = reconcile(u, ops, facts)
                last["reconciled"] = reconciled
                say(f"{work}: {told.fault.said}; read whether it took"
                    f" effect: {reconciled or 'not known'}")
                if reconciled == "committed":
                    way = "reconciled"
                    got = None
                    _held(last, True)
                    u.state, u.why = "done", ""
                    break
                if reconciled in ("nothing", "running") and tries >= TRIES:
                    u.state = "failed"
                    u.why = f"{told.fault.said}; every try spent ({tries})"
                    break
                if reconciled in ("nothing", "running"):
                    secs = backoff(now() - t0, tries)
                    say(f"{work}: again from its last position in"
                        f" {secs:.0f}s")
                    wait(secs)
                    way, reconciled = "resume", None
                    continue
                u.state = "failed"
                u.why = (f"{told.fault.said}; what it did cannot be read,"
                         " so it is not run again: check the target for"
                         " it, then run again")
                break
            if act == "proceed":
                way = "reconciled"
                _held(last, True)
                u.state, u.why = "done", ""
                break
            if act in ("retry", "pause", "throttle") and tries < TRIES:
                secs = backoff(took, tries)
                if act != "retry":
                    secs = min(BACKOFF_MAX, secs * 2)
                if act == "throttle":
                    plan.scale = max(MIN_SCALE, plan.scale / 2)
                    chunk = max(1, chunk // 2)
                    plan.changed = f"{work} was throttled"
                say(f"{work}: {told.fault.said}; {choice.said}, again"
                    f" from its last position in {secs:.0f}s")
                wait(secs)
                way = "resume" if act == "retry" else act
                continue
            if act in ("probe", "replan") and act not in spent \
                    and tries < TRIES:
                # read again what the step reads; one try of each
                spent.add(act)
                facts = dict(ops.facts(u.db)) if u.db else {}
                if act == "replan":
                    chunk = plan.chunk(facts)
                say(f"{work}: {told.fault.said}; {choice.said}")
                way = act
                continue
            u.state = "failed"
            u.why = (f"{told.fault.said}; "
                     + ("set apart, the rest goes on" if act == "isolate"
                        else f"every try spent ({tries})" if
                        act in ("retry", "pause", "throttle", "probe",
                                "replan")
                        else "stopped: " + told.fault.retry_after))
            if act == "isolate":
                plan.changed = f"{work} was set apart"
        else:
            u.state, u.why = "done", ""
            _held(last, True)
        break
    if u.state == "done":
        u.way = way
    else:
        _held(last, False)
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


def _step(u, ops, chunk):
    if u.step == "assess":
        bad = list(ops.assess())
        hop_wide = [why for d, why in bad if not d]
        if hop_wide:
            raise SystemExit("; ".join(hop_wide))
        return bad
    if u.step == "move":
        return ops.move(u.db, chunk)
    if u.step == "follow":
        if ops.follow(u.db) is False:
            raise TimeoutError(f"the stream of {u.db} has not caught up")
        return True
    if u.step == "verify":
        got = ops.verify(u.db)
        if got not in ("equal", "pass"):
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
    evidence; performed only on the operator's yes - `go` given up front
    is that yes - and kept in the decisions, never done on migkit's own."""
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
        yes, how = True, "the go given up front"
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

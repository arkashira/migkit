"""`migkit migrate`: two addresses, their access and a goal in; the whole
workflow run to a verified outcome (backlog P0-AUTONOMOUS-MIGRATION).

The goal says the steps (`FLOWS`): a copy is moved and proved, a sync is
moved, followed and proved, a cutover is all of that and its readiness.
Each database is a chain of units, one a step, each waiting for the one
before it; the hop's assessment comes first. Every way a unit may run is
a rung of the one decision engine (`decide.attempt`): as planned, from
its last durable position after a backoff the failed attempt's own time
measures, in smaller pieces where the copy is cut into pieces. An
operational failure climbs that ladder, bounded, with what it gave up
kept beside the unit; migkit's own stop is a blocker said in its words,
never retried; a fault is held, never passed as success. A unit that
cannot complete is isolated: its database's later steps are held, every
other database goes on. Done units are kept (`workflow.json`) and a run
again starts from them, re-establishing a stream's catch-up before the
proof that depends on it. The time left is the engine's forecast
(`Engine.forecast`) after every unit.

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
    if answer is None:
        raise Asked(q)
    return answer(q, default)


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
        return owed

    def decided(self, unit, what, why="", way=""):
        self.decisions.append({"unit": unit.key, "what": what, "why": why,
                               "way": way, "at": time.time()})


#: the bounds of the wait before a unit is tried again from its position
BACKOFF_MIN, BACKOFF_MAX = 1.0, 300.0


def backoff(took, tries):
    """Seconds to wait before trying a unit again: the time its failed
    attempt took (a server that refused at once is retried soon, one that
    failed after a long wait is given as long), doubled each try,
    bounded."""
    base = min(BACKOFF_MAX, max(BACKOFF_MIN, float(took or 0)))
    return min(BACKOFF_MAX, base * (2 ** max(0, tries - 1)))


def ladder(step, facts):
    """The rungs a unit of `step` climbs: as planned; from its last
    durable position after a backoff; for a copy cut into pieces, smaller
    pieces. A proof read again is read-only and safe to repeat."""
    plan = decide.Rung("as-planned", f"the {STEP_SAID[step]} as planned",
                       gives=frozenset({"complete"}))
    resume = decide.Rung(
        "resume", f"the {STEP_SAID[step]} again from its last position",
        gives=frozenset({"complete"}),
        needs=(decide.Need("a durable position", lambda f: f.get(
            "resumes", True), "{rung} needs a position kept on disk"),))
    rungs = [plan, resume]
    if step == "move":
        rungs.append(decide.Rung(
            "smaller", "the copy in smaller pieces",
            gives=frozenset({"complete"}),
            needs=(decide.Need("a copy cut into pieces", lambda f: bool(
                f.get("pieces")), "{rung} needs a copy cut into pieces"),)))
    if step in ("assess", "ready"):
        rungs = [plan]
    return rungs


@dataclass
class Outcome:
    workflow: Workflow
    #: database -> (state, words)
    databases: dict

    @property
    def complete(self):
        return all(u.state == "done" for u in self.workflow.units)

    @property
    def exit(self):
        return 0 if self.complete else 1


def run(goal, ops, say, path=None, wait=time.sleep, now=time.time):
    """The flow of `goal` over `ops` to its outcome.

    `ops` does the steps: `dbs()`, `facts(db)` (`resumes`, `pieces`,
    `chunk`), `assess()` -> [(db or "", why)] that fail, `move(db, chunk)`,
    `follow(db)` -> False where not caught up, `verify(db)` -> the
    verdict after its one repair, `ready(db)` -> [why] that fail,
    `forecast(kind, db)` -> words, `progress(db)` -> [words]."""
    if goal not in FLOWS:
        raise SystemExit(f"goal {goal}: one of {', '.join(GOALS)}")
    dbs = list(ops.dbs())
    wf = Workflow.load(path, goal, dbs)
    owed = wf.again()
    keys = wf.by_key()
    total = len(wf.units)
    apart = {}
    for u in wf.units:
        if u.key not in owed:
            continue
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
            got = _unit(u, wf, ops, say, wait, now)
            if u.step == "assess" and u.state == "done":
                # what fails for one database isolates that one only
                apart.update((d, why) for d, why in got or () if d)
        wf.save()
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
        say(line)
    wf.save()
    return Outcome(wf, outcome(wf, dbs))


def _unit(u, wf, ops, say, wait, now):
    began = now()
    facts = decide.Facts(ops.facts(u.db) if u.db else {})
    tries = {"n": 0, "took": 0.0}

    def go(choice):
        name = choice.rung.name
        if name != "as-planned":
            tries["n"] += 1
            secs = backoff(tries["took"], tries["n"])
            wf.decided(u, "recover", f"waiting {secs:.0f}s, then"
                       f" {choice.rung.said}", name)
            wait(secs)
        t0 = now()
        try:
            return _step(u, ops, facts, name)
        finally:
            tries["took"] = now() - t0

    where = (wf.path.parent / f"workflow-{u.key.replace(':', '.')}.json"
             if wf.path is not None else None)
    got = None
    try:
        choice, got = decide.attempt(
            f"{STEP_SAID[u.step]}" + (f" of {u.db}" if u.db else ""),
            ladder(u.step, facts), go, facts, need=("complete",),
            where=where, log=say)
    except decide.Verdict as e:
        u.state, u.why = "failed", str(e)
    except Asked as e:
        u.state, u.why = "asked", str(e)
    except SystemExit as e:
        # migkit's own stop, or every way spent: said, never retried here
        q = question_for(e)
        u.state = "asked" if q else "failed"
        u.why = str(Asked(q, _words(e))) if q else _words(e)
    except Exception as e:  # noqa: BLE001 - held, never passed as success
        u.state = "failed"
        u.why = f"stopped unexpectedly: {_words(e)}"
    else:
        u.state, u.way = "done", choice.rung.name
        u.why = ""
    u.took, u.at = now() - began, now()
    wf.decided(u, u.state, u.why, u.way)
    if u.state != "done":
        say(f"{STEP_SAID[u.step]}" + (f" of {u.db}" if u.db else "")
            + f": {u.why}; the rest goes on without it")
    return got


def _words(e):
    from .movers import DRIVEN
    from .wording import without_programs
    text = str(e).strip() or type(e).__name__
    return without_programs(text.splitlines()[-1][:300], DRIVEN)


def _step(u, ops, facts, way):
    if u.step == "assess":
        bad = list(ops.assess())
        hop_wide = [why for d, why in bad if not d]
        if hop_wide:
            raise SystemExit("; ".join(hop_wide))
        return bad
    if u.step == "move":
        chunk = int(facts.get("chunk") or 500000)
        return ops.move(u.db, chunk // 4 if way == "smaller" else chunk)
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


def outcome(wf, dbs):
    """Each database's outcome in one line: what it reached and, where it
    stopped, why and what was kept."""
    out = {}
    assess = wf.by_key()["assess"]
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
                               " and ready for the switch, which waits for"
                               " the operator"}[wf.goal])
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

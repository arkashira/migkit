"""Right the first time, not a runbook to recover with (backlog R23, 6).

The owner: "not a document - make it succeed once, no rollback needed".
So everything a hop's goal needs is known before anything is written, in
one place, each with its fix - and a move does not start where something
known now would stop it half way:

* the servers: each side's version, connection and scope (the engine's own
  `assess`), and what the goal needs each to allow (`dba.needs`), with the
  exact statements a DBA runs;
* the target: the interlock's word on it (`interlock.refusals` - the
  source itself, production the hop does not name, rows it did not write),
  the room its disk has for the tables, and what one worker of the copy
  holds against this machine's memory (`model`);
* the path: a mapping that would land two tables on one, the goal's own
  checks - an upgrade's (`upgrade.checks`), a subset's rules
  (`subset.problems`) - and the cutover as it is set up;
* the rehearsal (`rehearsal`): whether this hop's path was rehearsed, how
  it ended, where it went another way, and the forecast it gives;
* what a person must still do, and when (`by_hand`): a DBA's grant before
  the move, the application pointed at the target at the flip - said by
  the plan at the moment it is needed, not in a separate document.

`assess` shows all of it. `move --go` and `sync --go` stop, before the
first write, on every item that would stop the step itself and is known
for sure now (`gate`): a grant the copy reads through, a stream the source
cannot give, a refusal of the interlock, a row too wide for this machine,
a disk too small for the tables alone, a mapping that collides, a goal's
own refusal, a rehearsal that ended with differences. Each is said with
its fix, all at once - one run for all of them, not one per failure.
`MIGKIT_PREFLIGHT=warn` says them and goes on. What is only likely - a
disk too small for the tables with their indexes, a catch-up the probes
say may never end, a path not rehearsed - is said, and does not stop it.
"""
import os
from dataclasses import dataclass

BEFORE = "before the move"
CUTOVER = "at the cutover"
AFTER = "after the move"


@dataclass
class Item:
    """One thing the goal needs, in `assess`'s shape: `gates` where it
    would stop the step itself and is known for sure now; `by_hand` where
    a person does it, `when`."""
    level: str
    when: str
    what: str
    detail: str = ""
    fix: str = ""
    by_hand: bool = False
    gates: bool = False

    def row(self):
        return {"level": self.level, "scope": self.when, "item": self.what,
                "detail": self.detail + (f"  [fix] {self.fix}"
                                         if self.fix and self.level != "pass"
                                         else "")}

    def __str__(self):
        return f"{self.what}: {self.detail}" + (f" - {self.fix}"
                                                 if self.fix else "")


def _last(e):
    return (str(e).strip().splitlines() or [type(e).__name__])[-1][:120]


def _goal(hop):
    return getattr(hop, "goal", "") or "copy"


def _streams(hop, mode=None):
    """Whether the step follows the source's changes: the mode where one is
    given, the goal where it is not."""
    from .dba import STREAMS
    if mode is not None:
        return mode in ("cdc", "full+cdc")
    return _goal(hop) in STREAMS


def _dbs(hop, eng, dbs):
    if dbs:
        return list(dbs)
    if hop.databases:
        return list(hop.databases)
    try:
        return list(eng.databases())
    except Exception:  # noqa: BLE001 - said by the servers' own rows
        return []


# --- what each part says -------------------------------------------------------

def _servers(hop, eng, mode):
    """What the goal needs each server to allow, as `dba.needs` asks it,
    and the DBA's scripts written for what is missing."""
    from . import dba
    goal = _goal(hop)
    if goal == "verify":
        return []
    try:
        found = dba.needs(hop, eng, goal, streaming=_streams(hop, mode))
    except Exception as e:  # noqa: BLE001 - unknown, said so
        return [Item("warn", BEFORE, "what the servers allow",
                     f"could not be asked: {_last(e)} - unknown, not clean")]
    try:
        written = dba.write(hop, eng, found)
    except Exception:  # noqa: BLE001 - the items say it all the same
        written = {}
    out = []
    for n in found:
        if n.ok is None:
            out.append(Item("warn", BEFORE, f"{n.side}: {n.what}",
                            f"could not be asked ({n.now}) - unknown, not"
                            " clean"))
            continue
        if n.ok:
            continue
        script = written.get(n.side)
        # sure to stop the step: a table the copy reads, a database the
        # target does not have and the user may not make, a stream the
        # source does not give; what the target may already hold in
        # another way (its schemas, its extensions) is said, not held to
        sure = n.follow or (n.side == "source") or \
            n.what.startswith("a database ")
        out.append(Item(
            "fail" if sure else "warn", BEFORE, f"{n.side}: {n.what}",
            n.now + (" - takes a restart" if n.restart else ""),
            "; ".join(line for line in n.fix if not line.startswith("--"))
            [:300] + (f" (the DBA's script: {script})" if script else ""),
            by_hand=True, gates=sure))
    if not out and found:
        out.append(Item("pass", BEFORE, "what the servers allow",
                        f"everything goal {goal} needs of both sides"))
    return out


def _target(hop, eng, dbs):
    """The interlock's word on the target, before anything is written."""
    from . import interlock
    if _goal(hop) == "verify":
        return []
    out = []
    for d in dbs:
        try:
            tables = eng.list_move_tables(d) if interlock.reached(hop) \
                else None
        except Exception:  # noqa: BLE001 - asked below without the tables
            tables = None
        try:
            said = interlock.refusals(hop, eng, d, tables)
        except Exception as e:  # noqa: BLE001 - unknown, said so
            out.append(Item("warn", BEFORE, f"{d}: the target",
                            f"could not be asked: {_last(e)}"))
            continue
        for why in said:
            out.append(Item("fail", BEFORE, f"{d}: the target", why,
                            "point the hop at the target meant, or say in"
                            " its options what may be written there",
                            gates=True))
    return out


def _resources(hop, eng, dbs):
    """Room on the target's disk for the tables, and this machine's memory
    for one worker of the copy - from the catalogue, no probe run."""
    from . import model as resource_model
    from . import sizing
    from .wording import human_bytes
    if _goal(hop) == "verify":
        return []
    out = []
    for d in dbs:
        try:
            facts = eng.table_facts("src", d)
        except Exception:  # noqa: BLE001 - the copy asks again
            continue
        carried = {t: f for t, f in (facts or {}).items()
                   if isinstance(f, dict)
                   and not hop.excluded(d, *str(t).split("."))}
        data = sum(int(f.get("bytes") or 0) for f in carried.values())
        index = sum(int(f.get("index_bytes") or 0) for f in carried.values())
        try:
            free = eng.free_bytes("dst", d)
        except Exception:  # noqa: BLE001 - not asked, not said
            free = None
        if free is not None and data:
            if free < data:
                out.append(Item(
                    "fail", BEFORE, f"{d}: room on the target",
                    f"{human_bytes(free)} free for {human_bytes(data)} of"
                    " tables alone: a load that fills a disk can stop the"
                    " target altogether", "a larger volume for the target,"
                    " or fewer tables in the hop", gates=True))
            elif free < data + index:
                out.append(Item(
                    "warn", BEFORE, f"{d}: room on the target",
                    f"{human_bytes(free)} free for {human_bytes(data)} of"
                    f" tables and {human_bytes(index)} of indexes",
                    "a larger volume for the target"))
            else:
                out.append(Item("pass", BEFORE, f"{d}: room on the target",
                                f"{human_bytes(free)} free for"
                                f" {human_bytes(data + index)}"))
        try:
            m = resource_model.build(hop, eng, d, facts, probe=False,
                                     write=False)
            short = m.too_wide(sizing._memory())
        except Exception:  # noqa: BLE001 - the copy measures it itself
            short = None
        if short:
            out.append(Item("fail", BEFORE, f"{d}: this machine's memory",
                            short, gates=True))
    return out


def _mapping(eng):
    """A mapping that would land two tables on one, or match nothing
    (`Engine._mapping_items`): the first stops, the second is said."""
    try:
        rows = eng._mapping_items()
    except Exception:  # noqa: BLE001 - said by the engine's own assess
        return []
    out = []
    for r in rows:
        detail, _, fix = str(r["detail"]).partition("  [fix] ")
        out.append(Item(r["level"], BEFORE, "mapping", detail, fix,
                        gates=r["level"] == "fail"))
    return out


def _rehearsed(hop, eng, dbs):
    """Whether this hop's path was rehearsed, how the rehearsal ended,
    where it went another way, and the forecast it gives."""
    from . import rehearsal
    goal = _goal(hop)
    own = rehearsal.settings(hop)
    if own is not None:
        return _rehearsing(hop, own)
    rec = rehearsal.of(hop)
    if rec is None:
        if goal in ("cutover", "upgrade"):
            return [Item("warn", BEFORE, "rehearsal",
                         "this path has not been rehearsed: its times are"
                         " not measured, and what would go wrong on it is"
                         " found in the real run",
                         f"a hop with goal: rehearse and rehearses:"
                         f" {hop.name}, onto a copy of the source, run to"
                         " the end first")]
        return []
    out = []
    verdict, state, outage = rehearsal.outcome(rec)
    when = str(rec.get("at", ""))[:10]
    if verdict in ("different", "error"):
        out.append(Item("fail", BEFORE, "rehearsal",
                        f"the rehearsal {rec['name']} ({when}) ended with"
                        " the two sides not equal: what went wrong there"
                        " goes wrong here", f"fix it on the rehearsal"
                        f" until migkit check {rec['name']} is equal",
                        gates=True))
    elif verdict == "same":
        out.append(Item("pass", BEFORE, "rehearsal",
                        f"the rehearsal {rec['name']} ({when}) ended equal"))
    else:
        out.append(Item("warn", BEFORE, "rehearsal",
                        f"the rehearsal {rec['name']} was "
                        + ("never checked" if verdict is None else
                           f"checked {verdict}"),
                        f"migkit check {rec['name']}"))
    if goal in ("cutover", "upgrade") and state != "flipped":
        out.append(Item("warn", CUTOVER, "rehearsal",
                        f"the rehearsal {rec['name']} has "
                        + ("not cut over" if state is None else
                           f"a cutover that was {state}")
                        + ": the outage is not measured",
                        f"migkit move {rec['name']} --mode cdc --drop --go"))
    diff = rehearsal.differences(rec.get("path") or {},
                                 rehearsal.path_of(hop))
    if diff:
        out.append(Item("warn", BEFORE, "rehearsal",
                        f"the rehearsal went another way in"
                        f" {', '.join(diff)}: what it proved and measured is"
                        " not all this hop's",
                        "set the rehearsal as this hop is set, and rehearse"
                        " again"))
    from . import cutover
    conf = cutover.settings(hop)
    if conf and state == "flipped" and outage is not None \
            and float(outage) > float(conf["budget"]):
        out.append(Item("fail", CUTOVER, "rehearsal",
                        f"the rehearsal's cutover took {float(outage):.2f}s"
                        f" of outage and this hop's budget is"
                        f" {float(conf['budget']):g}s: the cutover would be"
                        " undone", "a larger budget, or what the rehearsal's"
                        " cutover phases say took longest made faster",
                        gates=True))
    for d in dbs[:3]:
        for line in rehearsal.forecast_lines(hop, eng, d):
            out.append(Item("pass", BEFORE, f"{d}: forecast", line))
    return out


def _rehearsing(hop, own):
    """A rehearsal's own: the hop it rehearses is one the configuration
    has."""
    from . import config
    if not own["hop"]:
        return [Item("warn", BEFORE, "rehearsal",
                     "no hop named as the one this rehearses: what it"
                     " measures is taken by any hop of the same engines",
                     "options: rehearses: <the real run's hop>")]
    try:
        have = config.load_hops()
    except SystemExit:
        have = {}
    if own["hop"] not in have:
        return [Item("warn", BEFORE, "rehearsal",
                     f"it rehearses {own['hop']}, which the configuration"
                     " does not have", "name the real run's hop")]
    from . import rehearsal
    diff = rehearsal.differences(rehearsal.path_of(hop),
                                 rehearsal.path_of(have[own["hop"]]))
    if diff:
        return [Item("warn", BEFORE, "rehearsal",
                     f"{own['hop']} goes another way in {', '.join(diff)}:"
                     " what this rehearsal proves is not all that hop's",
                     "set both alike")]
    return [Item("pass", BEFORE, "rehearsal",
                 f"rehearses {own['hop']}, the same path")]


def _goal_checks(hop, eng, dbs, quick):
    """The goal's own: an upgrade's version checks and its way, a subset's
    rules."""
    from . import subset, upgrade
    out = []
    if upgrade.settings(hop) is not None:
        for d in dbs[:1] if quick else dbs:
            for r in upgrade.checks(hop, eng, d, quick=quick):
                detail, _, fix = str(r["detail"]).partition("  [fix] ")
                out.append(Item(r["level"], BEFORE, f"upgrade: {r['item']}",
                                detail, fix, gates=r["level"] == "fail"))
            if not quick:
                try:
                    lines = upgrade.plan_lines(hop, eng, d)
                except Exception as e:  # noqa: BLE001 - said
                    lines = [f"upgrade: the way could not be chosen:"
                             f" {_last(e)}"]
                out.append(Item("pass", BEFORE, "upgrade: the way",
                                " ".join(x.strip() for x in lines)))
    if subset.settings(hop) is not None:
        for d in dbs:
            for why in subset.problems(hop, eng, d):
                out.append(Item("fail", BEFORE, f"{d}: subset", why,
                                gates=True))
    return out


def by_hand(hop, eng=None):
    """[Item] of what a person does, and when - nothing else is left to a
    document: the cutover's own steps where the hop leaves them to the
    operator, the application pointed at the target."""
    from . import cutover
    goal = _goal(hop)
    if goal not in ("cutover", "upgrade", "rehearse", "keep-in-sync"):
        return []
    conf = cutover.settings(hop)
    out = []
    if not conf:
        out.append(Item(
            "warn", CUTOVER, "by hand",
            "no cutover: in the hop's options, so the operator stops the"
            " application's writes to the source and its jobs, and raises"
            " the target's counters - migkit proves around each and lists"
            f" them then (migkit move {hop.name} --mode cdc --drop)",
            "cutover: {budget: <seconds>} runs them, timed, and undoes"
            " them past the budget", by_hand=True))
    else:
        if str(conf.get("freeze")) == "operator":
            out.append(Item("warn", CUTOVER, "by hand",
                            "the operator stops the source's writes at the"
                            " cutover (freeze: operator); migkit proves"
                            " they stopped", by_hand=True))
        if not conf.get("flip"):
            out.append(Item("warn", CUTOVER, "by hand",
                            "the application is pointed at the target by"
                            " hand while the outage runs (no flip"
                            " command)", "flip: <command> runs it inside"
                            " the budget", by_hand=True))
    if goal == "keep-in-sync":
        return []
    return out


# --- in one place ------------------------------------------------------------

def items(hop, eng, dbs=None, mode=None, quick=False):
    """Every `Item` of the hop's goal: with `quick`, only what a step's
    gate needs (no engine assess, no probe, no statement compared)."""
    dbs = _dbs(hop, eng, dbs)
    out = []
    if not quick:
        try:
            for r in eng.assess():
                detail, _, fix = str(r.get("detail", "")).partition(
                    "  [fix] ")
                out.append(Item(r["level"], r.get("scope") or BEFORE,
                                r["item"], detail, fix))
        except Exception as e:  # noqa: BLE001 - said, the rest stands
            out.append(Item("warn", BEFORE, "the servers",
                            f"could not be assessed: {_last(e)}"))
        from . import cutover
        try:
            level, what = cutover.summary(hop, eng, dbs)
        except Exception as e:  # noqa: BLE001 - said, the rest stands
            level, what = "warn", f"could not be worked out ({_last(e)})"
        out.append(Item(level, CUTOVER, "cutover", what))
        if dbs:
            from . import machine
            for r in machine.assess_items(hop, eng, dbs[0]):
                out.append(Item(r["level"], r["scope"], r["item"],
                                r["detail"]))
    else:
        out += _mapping(eng)
    for part in (lambda: _servers(hop, eng, mode),
                 lambda: _target(hop, eng, dbs),
                 lambda: _resources(hop, eng, dbs),
                 lambda: _goal_checks(hop, eng, dbs, quick),
                 lambda: _rehearsed(hop, eng, dbs) if not quick else
                 [i for i in _rehearsed_quick(hop)],
                 lambda: by_hand(hop, eng) if not quick else []):
        try:
            out += part()
        except Exception as e:  # noqa: BLE001 - said, the rest stands
            out.append(Item("warn", BEFORE, "readiness",
                            f"a part could not be asked: {_last(e)}"))
    return out


def _rehearsed_quick(hop):
    """What of the rehearsal a gate holds to: how it ended, and its outage
    against the budget - no forecast built."""
    from . import cutover, rehearsal
    rec = rehearsal.of(hop)
    if rec is None or rehearsal.settings(hop) is not None:
        return []
    verdict, state, outage = rehearsal.outcome(rec)
    out = []
    if verdict in ("different", "error"):
        out.append(Item("fail", BEFORE, "rehearsal",
                        f"the rehearsal {rec['name']} ended with the two"
                        " sides not equal: what went wrong there goes wrong"
                        " here", f"fix it on the rehearsal until migkit"
                        f" check {rec['name']} is equal", gates=True))
    conf = cutover.settings(hop)
    if conf and state == "flipped" and outage is not None \
            and float(outage) > float(conf["budget"]):
        out.append(Item("fail", CUTOVER, "rehearsal",
                        f"the rehearsal's cutover took {float(outage):.2f}s"
                        f" of outage and the budget is"
                        f" {float(conf['budget']):g}s", gates=True))
    return out


def stops(hop, eng, dbs, mode, do_drop):
    """[Item] that would stop this step and are known for sure now. A
    cutover has its own preflight (`cutover.preflight`); a stream started
    alone reads no table, so only what the source must give it counts."""
    from . import interlock
    if do_drop and mode == "cdc" or not interlock.reached(hop):
        # a side nothing answers at is said by the step itself, soon, where
        # asking here would wait on each engine's own retries
        return []
    return [i for i in items(hop, eng, dbs, mode, quick=True)
            if i.gates and i.level == "fail"
            and (mode != "cdc" or i.what.startswith("source:"))]


def gate(hop, eng, dbs, mode, do_drop, log):
    """Before a step writes anything: every item that would stop it and is
    known for sure now (`stops`), each with its fix, all at once -
    SystemExit, or said and gone on from under `MIGKIT_PREFLIGHT=warn`."""
    got = stops(hop, eng, dbs, mode, do_drop)
    if not got:
        return []
    said = "; ".join(str(i) for i in got)
    if os.environ.get("MIGKIT_PREFLIGHT", "").lower() == "warn":
        for i in got:
            log(f"[yellow]known before the move, gone on from"
                f" (MIGKIT_PREFLIGHT=warn):[/yellow] {i}")
        return got
    raise SystemExit(
        f"{hop.name}: nothing was written - {len(got)} thing"
        f"{'s' if len(got) > 1 else ''} known now would stop this before its"
        f" end: {said}. `migkit assess {hop.name}` lists everything the"
        " goal needs; MIGKIT_PREFLIGHT=warn goes on regardless")

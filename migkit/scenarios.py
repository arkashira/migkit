"""Migration scenarios decided on observed facts (backlog P0-SCENARIOS):
a managed service change through the provider's own operations
(`cloudapi.OPERATIONS`), and a hardware / OS / architecture change.
A storage engine change is decided where it is planned, `tableengine`.
"""
from __future__ import annotations

import time
from types import SimpleNamespace
from dataclasses import dataclass

from . import decide
from .capabilities import NOT_YET


@dataclass(frozen=True)
class ScenarioDecision:
    case: str
    choice: decide.Choice
    details: tuple = ()

    def words(self):
        return "; ".join((f"{self.case}: {self.choice.said}",
                          self.choice.reason, *self.details))


#: the words a provider's API gives for an instance that takes work
RUNNING = frozenset({"available", "active", "running", "runnable", "ready",
                     "normal", "online"})
GIB = 1024 ** 3
#: where a provider operation's runs are kept with the hop's rates
RUN_KEY = "provider operation {}"


def _not_yet(what):
    return f"{NOT_YET}: the provider's API does not say {what}"


def _instances(f):
    return f.get("source"), f.get("target")


def _unknown(f, fields):
    """What of `fields` an instance of the pair does not carry."""
    return [f"{side}'s {name}" for side, inst in zip(("source", "target"),
                                                    _instances(f))
            for name in fields if not (inst and getattr(inst, name))]


def _status_holds(f):
    src, dst = _instances(f)
    return bool(src and dst and src.status and dst.status
                and src.status.lower() in RUNNING
                and dst.status.lower() in RUNNING)


def _status_why(f):
    unread = _unknown(f, ("status",))
    if unread:
        return _not_yet(", ".join(unread))
    return "; ".join(f"the {side} is {inst.status}, not available"
                     for side, inst in zip(("source", "target"), _instances(f))
                     if inst.status.lower() not in RUNNING)


def _room(f):
    dst, size = f.get("target"), f.get("size_bytes")
    return (size is not None and dst is not None and dst.storage_gb is not None
            and dst.storage_gb * GIB >= size)


def _room_why(f):
    dst, size = f.get("target"), f.get("size_bytes")
    if size is None:
        return _not_yet("the measured size of the data to place")
    if not dst or dst.storage_gb is None:
        return _not_yet("the target's storage")
    return (f"the target holds {dst.storage_gb} GiB and the data measures"
            f" {size / GIB:.1f} GiB")


def _check(check, f):
    """An operation's eligibility check on the pair: True, False, or
    None where an instance or the fact it reads was not given."""
    src, dst = _instances(f)
    if src is None or dst is None:
        return None
    return check(src, dst)


def _eligibility(offer):
    """One need for each of the operation's own eligibility checks: a
    check the facts cannot answer is not yet, never a yes."""
    def need(what, check, refused):
        return decide.Need(
            what, lambda f: _check(check, f) is True,
            lambda f: (_not_yet(f"the facts behind {what}")
                       if _check(check, f) is None else refused))
    return tuple(need(*c) for c in offer.get("checks", ()))


def provider_needs(offer):
    """What a provider operation needs, each with the reason it is refused:
    the API answering, the instances' classes and versions, the
    operation's own eligibility on the facts read (`cloudapi.OPERATIONS`),
    both instances available, room for the data, a maintenance window
    known, an adapter call that runs it and reads it back, and a duration
    from evidence (`duration`). A fact the API does not give is said as
    not yet, never assumed."""
    return (
        decide.Need("a reachable provider API",
                    lambda f: f.get("api_reachable") is True,
                    lambda f: "the provider's API was not reached: "
                              + (f.get("api_error") or "it was not asked")),
        decide.Need("known instance classes and engine versions",
                    lambda f: not _unknown(f, ("klass", "version")),
                    lambda f: _not_yet(", ".join(_unknown(
                        f, ("klass", "version"))))),
        *_eligibility(offer),
        decide.Need("both instances available", _status_holds, _status_why),
        decide.Need("room on the target for the measured data", _room,
                    _room_why),
        decide.Need("a known maintenance window on the target",
                    lambda f: bool(f.get("target") and f["target"].maintenance),
                    lambda f: _not_yet("the target's maintenance window")),
        decide.Need("a migkit execution owner",
                    lambda f: callable(offer.get("execute")),
                    f"{NOT_YET}: " + (offer.get("why_not") or
                                      f"no execution owner runs"
                                      f" {offer['said']}")),
        decide.Need("a duration from the documented startup and a measured"
                    " rate", lambda f: duration(offer, f.get("size_bytes"))
                    is not None,
                    lambda f: duration_why(offer, f.get("size_bytes"))))


def operation_runs(hop, name):
    """([{"bytes", "seconds"}], whose, truth) of a provider operation run
    before: this hop's own runs or its rehearsal's are measured, another
    hop's of the same engines only predicts; ([], "", "") where none ran."""
    from . import planner
    from .costmodel import MEASURED_TRUTH, PREDICTED
    key = RUN_KEY.format(name)
    try:
        mine = planner.runs(hop, key)
    except Exception:  # noqa: BLE001 - a hop with no report directory
        mine = []
    if mine:
        return mine, "measured on this hop", MEASURED_TRUTH
    try:
        got, names = planner._elsewhere(hop, key)
    except Exception:  # noqa: BLE001 - nothing kept to read
        got, names = [], []
    if not got:
        return [], "", ""
    if names and names[0].startswith("the rehearsal "):
        return got, f"measured on {names[0]}", MEASURED_TRUTH
    return got, "measured on " + ", ".join(names[:3]), PREDICTED


def record_operation(hop, name, size_bytes, seconds):
    """One finished provider operation kept with the hop's rates: the
    bytes it placed and the seconds it took, read by the next plan of this
    hop or of the hop it rehearses (`operation_runs`)."""
    from . import planner
    if not size_bytes or seconds <= 0:
        return
    planner._add_run(hop, RUN_KEY.format(name), {
        "bytes": int(size_bytes), "seconds": round(float(seconds), 3),
        "at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "kind": planner._kind(hop)})


def _usable(runs):
    return [(float(r["bytes"]), float(r["seconds"])) for r in runs or ()
            if float(r.get("bytes") or 0) > 0
            and float(r.get("seconds") or 0) > 0]


def duration(offer, size):
    """The operation's `costmodel.Estimate` for `size` bytes, or None.

    With a documented startup, each run's rate is its bytes over the
    seconds past the startup. With none, runs of two or more sizes are
    fitted to a startup and a rate; one size alone is scaled up to a
    larger move and never down - an upper bound, since its startup is
    not known apart from its rate. A run on less data than the move
    widens the spread (`rehearsal.sample_spread`)."""
    import statistics
    from . import rehearsal
    from .costmodel import Estimate
    runs = _usable(offer.get("runs"))
    if size is None or size < 0 or not runs:
        return None
    start = offer.get("startup_seconds")
    if start is not None:
        seen = [start + size * (t - start) / b for b, t in runs if t > start]
        how = f"{offer.get('startup_source') or 'documented startup'} and"
    elif len({b for b, _ in runs}) >= 2:
        mb, mt = (statistics.fmean(x) for x in zip(*runs))
        slope = (sum((b - mb) * (t - mt) for b, t in runs)
                 / sum((b - mb) ** 2 for b, _ in runs))
        if slope <= 0:
            return None
        lead = max(0.0, mt - slope * mb)
        seen = [lead + slope * size + (t - lead - slope * b)
                for b, t in runs]
        how = "a startup and rate fitted to runs of several sizes"
    else:
        seen = [t * max(1.0, size / b) for b, t in runs]
        seen = [max(seen)]
        how = "one run's size scaled up as a bound"
    if not seen:
        return None
    mean = max(0.0, statistics.fmean(seen))
    spread = rehearsal.sample_spread(max(b for b, _ in runs), size)
    sd = ((statistics.stdev(seen) if len(seen) > 1 else 0.0) ** 2
          + (mean * spread) ** 2) ** 0.5
    return Estimate(mean, sd, truth=offer.get("truth") or "predicted",
                    source=f"{how} {offer.get('duration_source')}",
                    n=len(runs))


def duration_why(offer, size):
    if size is None:
        return _not_yet("the measured size of the data to place")
    if not _usable(offer.get("runs")):
        return (f"{NOT_YET}: no rehearsal or other hop has run"
                f" {offer['said']} to measure its rate")
    return (f"the runs of {offer['said']} give no rate past its documented"
            " startup")


def provider_offers(hop, source_adapter, target_adapter=None):
    """The provider's own operations on the source's product
    (`cloudapi.Adapter.operations`) as rungs' offers: their eligibility
    checks, the adapter call that runs one (None: not yet), its
    documented startup and the runs it was measured by."""
    ops = getattr(source_adapter, "operations", None)
    if not callable(ops):
        return ()
    out = []
    for op in ops():
        runs, whose, truth = operation_runs(hop, op.name)
        out.append({"name": op.name, "said": op.said, "checks": op.checks,
                    "execute": source_adapter.runner(op),
                    "why_not": op.why_not, "startup_seconds": op.startup,
                    "startup_source": op.startup_source, "runs": runs,
                    "duration_source": whose, "truth": truth})
    return tuple(out)


def managed_facts(hop, eng, source_api, target_api, offers, size_bytes):
    """Read provider instances without changing either side: reachability,
    class, version, status, storage, replicas, upgrade targets and
    maintenance window as the provider's API states them. An API that
    does not answer is a fact (`api_reachable`), not an exception.

    Offers are the provider's operations on the source's product
    (`offers`), each held to its eligibility on these facts."""
    from . import clouds
    src = dst = None
    error = ""
    try:
        src, dst = source_api.instance(), target_api.instance()
    except (Exception, SystemExit) as e:  # noqa: BLE001 - said as the reason
        error = (str(e).strip().splitlines() or [type(e).__name__])[-1][:160]
    return decide.Facts({
        "source": src, "target": dst, "api_reachable": src is not None,
        "api_error": error,
        "locations": (clouds.of(eng, "src", hop.source),
                      clouds.of(eng, "dst", hop.target)),
        "offers": tuple(offers), "size_bytes": size_bytes,
    })


def managed_case(facts, verified=None):
    """Choose a provider operation; planning never claims verification.

    A duration is a documented startup plus a size-dependent rate observed
    on a rehearsal of this path (`duration`). Switchover downtime alone is
    not its cost. `verified`, when supplied after execution, is migkit's
    own full check.
    """
    rungs, costs = [], {}
    size = facts.get("size_bytes")
    for offer in facts.get("offers", ()):
        name = offer["name"]
        rungs.append(decide.Rung(
            name, offer["said"], frozenset({"managed path", "full verify"}),
            needs=provider_needs(offer),
            prove=(lambda f: None if verified() is True else
                   "migkit's full verify did not pass") if verified else None,
            because="the provider offers this path for both instances as"
                    " read; migkit must verify the result"))
        took = duration(offer, size)
        if took is not None:
            costs[name] = took
    choice = decide.climb("the managed service path", rungs, facts,
                          need=("managed path", "full verify"), costs=costs)
    details = ["provider completion is followed by migkit's full verify"]
    target = facts.get("target")
    if target is not None and target.maintenance:
        details.append(f"the target's maintenance window: {target.maintenance}")
    if not rungs:
        details.append("no eligible provider operation was read; consider the existing logical move")
    return ScenarioDecision("managed service change", choice, tuple(details))


def architecture_facts(eng, db, platform_reader):
    """The host facts of each server through a bounded reader, and how
    each compares and renders values as its engine reads them
    (`Engine.server_semantics`).

    The host reader returns architecture, OS and byte order of that server,
    never those of the machine running migkit. Unknown facts stay unknown;
    an engine that does not read its semantics leaves the reason why.
    """
    out = {}
    for side in ("src", "dst"):
        facts = dict(platform_reader(side))
        read = eng.server_semantics(side, db)
        if isinstance(read, str):
            facts["not_read"] = read
        else:
            facts.update(read)
        out[side] = facts
    return decide.Facts(out)


ARCH_FIELDS = ("architecture", "os", "byte_order", "collation",
               "case_sensitivity", "float_rendering")
#: what moves a value's meaning, carried only by the schema step
SEMANTIC = ("collation", "case_sensitivity", "float_rendering",
            "float_setting")


def _unread(value):
    return value is None or value == "" or (
        isinstance(value, tuple) and (not value or _unread(value[0])))


def _sides(f):
    return f.get("src") or {}, f.get("dst") or {}


def _arch_unread(f):
    a, b = _sides(f)
    return [k for k in ARCH_FIELDS if _unread(a.get(k)) or _unread(b.get(k))]


def _arch_read_why(f):
    said = sorted({side.get("not_read") for side in _sides(f)
                   if side.get("not_read")})
    return "; ".join(said) or "unread server facts: " + ", ".join(
        _arch_unread(f))


def _arch_differs(f):
    a, b = _sides(f)
    return [k for k in ARCH_FIELDS + ("float_setting",)
            if a.get(k) != b.get(k)]


def _arch_unhandled(f):
    carried = f.get("schema_carries") or ()
    return [k for k in _arch_differs(f) if k in SEMANTIC
            and k not in carried]


def _architecture_rungs():
    shared = (
        decide.Need("facts read from both servers",
                    lambda f: not _arch_unread(f), _arch_read_why),
        decide.Need("semantic differences carried by the schema step",
                    lambda f: not _arch_unhandled(f),
                    lambda f: "the schema step does not carry: "
                              + ", ".join(_arch_unhandled(f))))
    return decide.fenced((
        decide.Rung("architecture-pages", "a copy of the server's pages",
                    frozenset({"architecture change"}),
                    needs=(decide.Need(
                        "the same architecture, OS and byte order",
                        lambda f: not any(k in _arch_differs(f) for k in
                                          ("architecture", "os",
                                           "byte_order"))),)),
        decide.Rung("architecture-logical",
                    "a logical move across hardware or OS",
                    frozenset({"architecture change"}),
                    because="values are decoded and encoded for the"
                            " target")), *shared)


ARCHITECTURE = _architecture_rungs()


def architecture_case(facts, schema_carries=()):
    """Refuse unhandled semantic differences, route byte changes logically:
    the ladder (`ARCHITECTURE`) climbed on the facts read
    (`architecture_facts`) and what the schema step carries."""
    f = decide.Facts(dict(facts, schema_carries=tuple(schema_carries)))
    choice = decide.climb("hardware / OS / architecture", ARCHITECTURE, f,
                          need=("architecture change",))
    a, b = _sides(f)
    details = tuple(f"{k}: {a.get(k)!r} to {b.get(k)!r}"
                    for k in ARCH_FIELDS)
    return ScenarioDecision("hardware / OS / architecture change", choice,
                            details)


def run_managed_case(facts, verify, hop=None):
    """Execute the eligible operation and require migkit's own verification;
    on a hop, the run is kept as the evidence of the next plan's duration
    (`record_operation`)."""
    decision = managed_case(facts)
    if decision.choice.path is None:
        raise ValueError(decision.words())
    offer = next(o for o in facts["offers"] if o["name"] == decision.choice.path)
    began = time.monotonic()
    result = offer["execute"]()
    took = time.monotonic() - began
    checked = verify()
    selected = dict(facts, offers=(offer,))
    proved = managed_case(selected, verified=lambda: checked)
    if proved.choice.path != decision.choice.path:
        raise ValueError("managed service change: migkit's full verify did not pass")
    if hop is not None:
        record_operation(hop, offer["name"], facts.get("size_bytes"), took)
    return result, proved


def _read_words(side, inst):
    if inst is None:
        return f"{side}: not read"
    given = [f"{inst.klass or 'class not given'}",
             f"version {inst.version or 'not given'}",
             inst.status or "status not given",
             f"{inst.storage_gb} GiB" if inst.storage_gb is not None
             else "storage not given",
             f"window {inst.maintenance}" if inst.maintenance
             else "window not given"]
    return f"{side}: " + ", ".join(given)


def managed_lines(facts):
    """The plan's lines of the managed service change: the case decided on
    what the providers' APIs state, and those facts as read."""
    got = managed_case(facts)
    out = [f"  {got.words()}"]
    if facts.get("api_reachable") is True:
        out.append("  as the provider states it - "
                   + "; ".join(_read_words(s, facts.get(k)) for s, k in
                               (("source", "source"), ("target", "target"))))
    return out


def plan_lines(hop, eng, db, offers=None):
    """The plan's lines of the cases decided here, said before anything is
    written: the managed service change, where both sides are services
    whose provider's API migkit drives. `offers` are the provider
    operations for the pair, read from the source's adapter (`provider_offers`)
    where none are given; with none, the case says so and the logical
    move stands. A side with no API asks nothing."""
    from . import managed
    if any(managed.api_conf(hop, eng, w)[0] is None for w in ("src", "dst")):
        return []
    try:
        sized = eng.table_sizes(db, quick=True) if db else None
    except Exception:  # noqa: BLE001 - unmeasured, said as not yet
        sized = None
    size = (sum(float(t.get("bytes") or 0) for t in sized.values())
            if sized else None)
    adapters = {}

    def adapter(which):
        if which not in adapters:
            adapters[which] = managed.adapter(hop, eng, which)
        return adapters[which]

    if offers is None:
        try:
            offers = provider_offers(hop, adapter("src"))
        except (Exception, SystemExit):  # noqa: BLE001 - said by the read
            offers = ()

    def api(which):
        return SimpleNamespace(instance=lambda: adapter(which).instance())
    return managed_lines(managed_facts(hop, eng, api("src"), api("dst"),
                                       offers, size))

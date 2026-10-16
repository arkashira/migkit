"""Migration scenarios decided on observed facts (backlog P0-SCENARIOS):
a managed service change through the provider's own operations
(`cloudapi.OPERATIONS`), a hardware / OS / architecture change, and a
shard split or merge (`shard_case`, its copy and follow `reshard`'s), and
a many-node topology (`topology_case`, its math `topology`'s).
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
        decide.Need("a duration measured on runs or documented by the"
                    " provider", lambda f: duration(offer, f.get("size_bytes"))
                    is not None,
                    lambda f: duration_why(offer, f.get("size_bytes"))))


TIB = 1024 ** 4


def operation_rows():
    """{operation name: its catalogue row} - the documented preconditions
    and duration band of each provider operation (`data/managed.yaml`
    `operations`)."""
    from . import managed
    got = managed.catalogue().get("operations") or {}
    return got if isinstance(got, dict) else {}


#: what every cell of the managed column says first: no provider ran it
MANAGED_SOURCE = "documented, not measured"


def managed_rows():
    """{scenario row: its managed-service cell} for the comparison
    (`autonomous.comparison`, backlog C1.managed): the provider's own
    migration as its documentation states it for that case - an
    operation's duration band (`operation_rows`) or the mechanism the
    research notes cite - each said as documented, not measured. A row
    the catalogue holds nothing for is left out, never filled."""
    from . import managed
    from .machine import _took
    got = managed.catalogue().get("comparison") or {}
    ops, out = operation_rows(), {}
    for row, spec in (got.items() if isinstance(got, dict) else ()):
        spec = spec if isinstance(spec, dict) else {}
        op = ops.get(spec.get("operation") or "") or {}
        band = op.get("duration") or {}
        words = spec.get("said") or ""
        if not words and band.get("per_tib"):
            lo, hi = band["per_tib"]
            words = (f"{spec.get('label') or spec['operation']}:"
                     f" {_took(lo)} to {_took(hi)} a TiB, as its"
                     " documentation states")
        if words:
            out[str(row)] = f"{MANAGED_SOURCE}: {words}"
    return out


def documented(offer, size):
    """The operation's `costmodel.Estimate` for `size` bytes from the
    band its documentation states (`startup` and `per_tib` [low, high]),
    or None where it states no size-dependent part - a switchover's
    downtime alone is not the operation's duration."""
    from .costmodel import Estimate
    band = (offer.get("documented") or {}).get("duration") or {}
    rate = band.get("per_tib")
    if size is None or not rate:
        return None
    start = band.get("startup") or (0, 0)
    lo = float(start[0]) + float(rate[0]) * size / TIB
    hi = float(start[1]) + float(rate[1]) * size / TIB
    return Estimate((lo + hi) / 2, (hi - lo) / 4, truth="documented",
                    source="the provider's documentation: "
                           + str(band.get("said") or ""), n=0)


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
    if size is None or size < 0:
        return None
    if not runs:
        return documented(offer, size)
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
        said = ((offer.get("documented") or {}).get("duration") or {}).get(
            "said")
        return (f"{NOT_YET}: no rehearsal or other hop has run"
                f" {offer['said']} to measure its rate, and its"
                " documentation states none"
                + (f" ({said})" if said else ""))
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
    rows = operation_rows()
    for op in ops():
        runs, whose, truth = operation_runs(hop, op.name)
        out.append({"name": op.name, "said": op.said, "checks": op.checks,
                    "execute": source_adapter.runner(op),
                    "why_not": op.why_not, "startup_seconds": op.startup,
                    "new_instance": op.new_instance,
                    "startup_source": op.startup_source, "runs": runs,
                    "duration_source": whose, "truth": truth,
                    "documented": rows.get(op.name) or {}})
    return tuple(out)


def managed_facts(hop, eng, source_api, target_api, offers, size_bytes,
                  forecast=None):
    """Read provider instances without changing either side: reachability,
    class, version, status, storage, replicas, upgrade targets and
    maintenance window as the provider's API states them. An API that
    does not answer is a fact (`api_reachable`), not an exception.

    Offers are the provider's operations on the source's product
    (`offers`), each held to its eligibility on these facts; `forecast`
    migkit's own move's `costmodel.Estimate` for the data, which each
    operation is ranked against."""
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
        "forecast": forecast,
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
    own = facts.get("forecast")
    if own is not None:
        costs[decide.MANAGED_OWN_MOVE] = own
    choice = decide.climb("the managed service path", rungs, facts,
                          need=("managed path", "full verify"), costs=costs)
    own = facts.get("forecast")
    if own is not None:
        costs[decide.MANAGED_OWN_MOVE] = own
    against = decide.managed_against_own(choice, costs)
    details = ["provider completion is followed by migkit's full verify"]
    details += _ranked_words(facts, costs, choice)
    details.append(f"{against.said}: {against.reason}")
    target = facts.get("target")
    if target is not None and target.maintenance:
        details.append(f"the target's maintenance window: {target.maintenance}")
    if not rungs:
        details.append("no eligible provider operation was read; consider"
                       " the existing logical move")
    return ScenarioDecision("managed service change", choice, tuple(details))


def _ranked_words(facts, costs, choice):
    """The plan's words of each operation's duration beside migkit's own
    forecast - the band its documentation states or its measured runs -
    and the preconditions its documentation states that no API fact
    answers, for the operation chosen."""
    out = []
    own = costs.get(decide.MANAGED_OWN_MOVE)
    if own is None and facts.get("offers"):
        out.append(f"{NOT_YET}: migkit's own move has no forecast for the"
                   " measured data - an operation is ranked on its own"
                   " duration only")
    for offer in facts.get("offers", ()):
        took = costs.get(offer["name"])
        if took is not None:
            out.append(f"{offer['said']}: {took.said()} by {took.source}"
                       + (f", against migkit's own move {own.said()}"
                          if own is not None else ""))
        if choice.path == offer["name"]:
            out += [f"before it runs, as the provider documents: {p}"
                    for p in (offer.get("documented") or {}).get(
                        "preconditions") or ()]
    return out


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
            facts["not_read"] = "; ".join(
                w for w in (facts.get("not_read"), read) if w)
        else:
            facts.update(read)
        out[side] = facts
    return decide.Facts(out)


_HOST = ("architecture", "os", "byte_order")
_FIELDS = _HOST + ("collation", "case_sensitivity", "float_rendering")
_SEMANTIC = ("collation", "case_sensitivity", "float_rendering",
             "float_setting")
#: a build's machine word as each server writes it, the longer first
_ARCH = (("aarch64", "aarch64"), ("arm64", "aarch64"),
         ("x86_64", "x86_64"), ("amd64", "x86_64"), ("x64", "x86_64"),
         ("ppc64le", "ppc64le"), ("s390x", "s390x"), ("ppc64", "ppc64"),
         ("sparc", "sparc64"), ("i686", "x86"), ("i386", "x86"))
#: the byte order each architecture's builds store pages in
BYTE_ORDER = {"aarch64": "little", "x86_64": "little", "x86": "little",
              "ppc64le": "little", "s390x": "big", "ppc64": "big",
              "sparc64": "big"}
_OS = (("linux", "linux"), ("mingw", "windows"), ("windows", "windows"),
       ("visual c++", "windows"), ("win64", "windows"), ("darwin", "darwin"),
       ("osx", "darwin"), ("freebsd", "freebsd"), ("solaris", "solaris"))


def _word(text, table):
    t = str(text or "").lower()
    return next((v for k, v in table if k in t), None)


def _class_architecture(inst):
    """The architecture a provider's instance class names, or None: an
    AWS class whose family ends in `g` runs on its ARM processors."""
    import re
    if inst is None or getattr(inst, "provider", "") != "aws":
        return None
    klass = str(getattr(inst, "klass", "") or "")
    if re.match(r"^db\.[a-z]+\d+g[a-z]*\.", klass):
        return "aarch64"
    return "x86_64" if re.match(r"^db\.[a-z]+\d+[a-z]*\.", klass) else None


def server_platform(eng, db, side, instance=None):
    """The architecture, OS and byte order the server on `side` was built
    for, in the words its engine reads off the server
    (`Engine.server_platform`); the provider's instance class
    (`cloudapi.Instance`) where the server does not say. What neither
    says stays None, with why it was not read under "not_read"."""
    arch = os_ = None
    why = ""
    try:
        read = eng.server_platform(side, db)
    except (Exception, SystemExit) as e:  # noqa: BLE001 - said as not read
        read = ("the build was not read: "
                + ((str(e).strip().splitlines() or [type(e).__name__])[-1]
                   [:160]))
    if isinstance(read, dict):
        arch = _word(read.get("architecture"), _ARCH)
        os_ = _word(read.get("os"), _OS)
    else:
        why = read
    arch = arch or _class_architecture(instance)
    out = {"architecture": arch, "os": os_,
           "byte_order": BYTE_ORDER.get(arch)}
    return dict(out, not_read=why) if why else out


def reads_servers(eng):
    """Whether `eng` reads its servers' build or semantics at all
    (`Engine.server_platform`, `Engine.server_semantics`): an engine
    that reads neither has no architecture case to plan."""
    from .engines.base import Engine
    return any(getattr(type(eng), m, None) not in (None, getattr(Engine, m))
               for m in ("server_platform", "server_semantics"))


def _unread_value(value):
    return value is None or value == "" or (
        isinstance(value, tuple) and (not value or _unread_value(value[0])))


def _sides(f):
    return f.get("src") or {}, f.get("dst") or {}


def _unread(f):
    a, b = _sides(f)
    return [k for k in _FIELDS
            if _unread_value(a.get(k)) or _unread_value(b.get(k))]


def _differs(f, keys):
    a, b = _sides(f)
    return [k for k in keys if a.get(k) != b.get(k)]


def _topology(side):
    """One side's place among its provider's servers, in words, or None
    where no provider's API said it."""
    if side.get("product") is None and side.get("replicas") is None \
            and not side.get("replica_of"):
        return None
    if side.get("replica_of"):
        return f"a replica of {side['replica_of']}"
    n = side.get("replicas")
    alone = f"{side.get('product') or 'a server'}"
    return (f"{alone} with {n} replicas" if n else f"{alone}, alone"
            if n == 0 else f"{alone}, its replicas not said")


def architecture_case(facts, schema_carries=()):
    """The hardware / OS / architecture change decided on both servers'
    facts: a semantic difference the schema step does not carry refused,
    a target that follows another server refused, the server's pages
    copied only between the same architecture, OS and byte order, the
    provider's own operations (`cloudapi.OPERATIONS`, in `offers`) held
    to their eligibility on the instances read, and a logical move
    otherwise."""
    def unhandled(f):
        return [k for k in _differs(f, _SEMANTIC) if k not in schema_carries]

    shared = (
        decide.Need("facts read from both servers",
                    lambda f: not _unread(f),
                    lambda f: "unread server facts: " + ", ".join(_unread(f))
                    + "".join(f" ({side}: {s['not_read']})"
                              for side, s in zip(("source", "target"),
                                                 _sides(f))
                              if s.get("not_read"))),
        decide.Need("semantic differences carried by the schema step",
                    lambda f: not unhandled(f),
                    lambda f: "the schema step does not carry: "
                              + ", ".join(unhandled(f))),
        decide.Need("a target that follows no other server",
                    lambda f: not _sides(f)[1].get("replica_of"),
                    lambda f: "the target is a replica of"
                              f" {_sides(f)[1]['replica_of']}: it takes"
                              " writes only from what it follows"))
    provider = tuple(decide.Rung(
        o["name"], o["said"], frozenset({"architecture change"}),
        needs=(decide.Need(
            "an operation that places the data on another instance",
            lambda f, o=o: o.get("new_instance") is True,
            f"{o['said']} changes the instance in place: its hardware"
            " stays" if o.get("new_instance") is False else
            f"{NOT_YET}: whether {o['said']} places the data on an"
            " instance of another class is not read from the provider's"
            " operations"),) + provider_needs(o),
        because="the provider runs it on its own service, and migkit's"
                " full verify follows") for o in facts.get("offers", ()))
    physical = decide.Rung(
        "architecture-pages", "a copy of the server's pages",
        frozenset({"architecture change"}),
        needs=(decide.Need("the same architecture, OS and byte order",
                           lambda f: not _differs(f, _HOST)),))
    logical = decide.Rung(
        "architecture-logical", "a logical move across hardware or OS",
        frozenset({"architecture change"}),
        because="values are decoded and encoded for the target")
    choice = decide.climb("hardware / OS / architecture",
                          decide.fenced(provider + (physical, logical),
                                        *shared),
                          facts, need=("architecture change",))
    a, b = _sides(facts)
    details = tuple(f"{k}: {a.get(k)!r} to {b.get(k)!r}" for k in _FIELDS)
    if _topology(a) or _topology(b):
        details += (f"topology: {_topology(a) or 'not said'} to"
                    f" {_topology(b) or 'not said'}",)
    return ScenarioDecision("hardware / OS / architecture change", choice,
                            details)


def architecture_lines(hop, eng, db, instances=None, offers=(), size=None):
    """The plan's line of the hardware / OS / architecture change: each
    server's build and settings read (`architecture_facts`), the
    instances' topology as their provider states it, the decision kept in
    the run's record. Nothing where the two servers are alike in every
    fact and both were read."""
    if not reads_servers(eng):
        return []
    instances = instances or {}
    try:
        facts = architecture_facts(
            eng, db, lambda side: server_platform(eng, db, side,
                                                  instances.get(side)))
    except (Exception, SystemExit) as e:  # noqa: BLE001 - said, not guessed
        return ["  hardware / OS / architecture change: the servers'"
                f" settings were not read: {str(e).strip()[:160]}"]
    for side in ("src", "dst"):
        inst = instances.get(side)
        if inst is not None:
            facts[side].update(product=inst.product, replicas=inst.replicas,
                               replica_of=inst.replica_of)
    facts.update(source=instances.get("src"), target=instances.get("dst"),
                 api_reachable=None not in (instances.get("src"),
                                            instances.get("dst")),
                 offers=tuple(offers), size_bytes=size)
    a, b = _sides(facts)
    if not _unread(facts) and not _differs(facts, _FIELDS + _SEMANTIC) \
            and _topology(a) == _topology(b):
        return []
    got = architecture_case(facts)
    decide.record(hop, got.choice)
    return [f"  {got.words()}"]


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
    move stands. A side with no API asks nothing of a provider. The
    hardware / OS / architecture change follows (`architecture_lines`),
    on the instances the providers' APIs gave where both sides have one."""
    from . import managed
    if any(managed.api_conf(hop, eng, w)[0] is None for w in ("src", "dst")):
        return architecture_lines(hop, eng, db) + shard_lines(
            hop, eng, db) + copy_lines(hop, eng, db)
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

    offers_error = ""
    if offers is None:
        try:
            offers = provider_offers(hop, adapter("src"))
        except (Exception, SystemExit) as e:  # noqa: BLE001 - said below
            offers = ()
            offers_error = ((str(e).strip().splitlines()
                             or [type(e).__name__])[-1][:160])

    read = {}

    def instance(which):
        if which not in read:
            read[which] = adapter(which).instance()
        return read[which]

    def api(which):
        return SimpleNamespace(instance=lambda: instance(which))
    facts = managed_facts(hop, eng, api("src"), api("dst"), offers, size)
    said = ([f"  the provider's operations were not read: {offers_error} -"
             " check the API's access for the source's product, then plan"
             " again"] if offers_error else [])
    return managed_lines(facts) + said + architecture_lines(
        hop, eng, db, {"src": facts.get("source"), "dst": facts.get("target")},
        offers, size) + shard_lines(hop, eng, db) + copy_lines(hop, eng, db)


# --- shard split and merge ------------------------------------------------------

#: the shapes of a shard change: one source to many targets, many to one,
#: many to many under a new key
SHARD_SHAPES = {"split": "one source into many targets",
                "consolidate": "many sources into one target",
                "reshard": "many sources into many targets by a new key"}


def shard_facts(kind, sources, targets, tables):
    """The shard split / merge facts: the group's kind, how many sources
    and targets it holds, and each table's `{"key": [...], "columns":
    [...], "whole": bool}` - its shard key, its columns on the source, and
    whether every target takes it whole."""
    return decide.Facts({"kind": kind, "sources": int(sources),
                         "targets": int(targets), "tables": dict(tables)})


def shard_facts_of(hop, eng, db, siblings=None):
    """`shard_facts` read off the hop's group (`reshard.group`) and the
    source engine's catalogue: each table's key as the hop names it, else
    its own (`reshard.shard_key`), its columns, and whether the hop keeps
    it whole (`shared`). A group that does not hold together is said as the
    reason, never guessed past."""
    from . import reshard
    kind = reshard.kind_of(hop)
    if kind is None:
        return None
    try:
        g = reshard.group(hop, db, siblings)
    except SystemExit as e:
        return decide.Facts({"kind": kind, "sources": 0, "targets": 0,
                             "tables": {}, "not_read": str(e)[:300]})
    shared = g.conf.get("shared") or ()
    tables = {}
    for t in eng.neutral_tables("src", db):
        pk = list(eng.neutral_key("src", db, t) or [])
        tables[str(t)] = {
            "key": reshard.shard_key(g, t, pk),
            "columns": [n for n, _ in eng.neutral_columns("src", db, t)],
            "whole": reshard._named_in(shared, t)}
    return shard_facts(kind, len(g.sources), len(g.targets), tables)


def _keyless(f):
    """[(table, why)] of the tables a placement by key cannot place."""
    out = []
    for t, s in sorted((f.get("tables") or {}).items()):
        if s.get("whole"):
            continue
        key = list(s.get("key") or [])
        if not key:
            out.append((t, "no shard key: the hop names none and the table"
                           " has no key of its own"))
            continue
        absent = [k for k in key if k not in (s.get("columns") or ())]
        if absent:
            out.append((t, "its shard key " + ", ".join(absent)
                           + " is not among its columns"))
    return out


def _shape_holds(f):
    kind, n, m = f.get("kind"), f.get("sources", 0), f.get("targets", 0)
    return {"split": n == 1 and m > 1, "consolidate": n > 1 and m == 1,
            "reshard": n >= 1 and m >= 1}.get(kind, False)


def _shape_why(f):
    if f.get("not_read"):
        return "the group was not read: " + f["not_read"]
    return (f"a {f.get('kind')} is {SHARD_SHAPES.get(f.get('kind'), '?')};"
            f" the group holds {f.get('sources', 0)} sources and"
            f" {f.get('targets', 0)} targets")


def shard_case(facts):
    """The shard split / merge decided on the group's facts: a group whose
    sources and targets are not its kind's shape refused, a table with no
    shard key on its columns refused by name (a row with no key has no
    place), then the rows placed by a hash range of the key into the
    targets (`reshard.hash_place`), or - many into one - each source's
    rows written whole into the one target. The copy and the follow are
    `reshard`'s; the target is proved table by table after."""
    shared = (
        decide.Need("a group of its kind's shape", _shape_holds, _shape_why),
        decide.Need("a shard key on every table placed by key",
                    lambda f: not _keyless(f),
                    lambda f: "; ".join(f"{t}: {why}" for t, why
                                        in _keyless(f)[:6])
                    + " - name it under the hop's key, or list the table"
                      " in shared"))
    by_hash = decide.Rung(
        "shard-hash-range", "rows placed by a hash range of the shard key",
        frozenset({"shard change"}),
        needs=(decide.Need("more than one target",
                           lambda f: f.get("targets", 0) > 1),),
        because="each row's key hashes into exactly one target's range, so"
                " a row lands once")
    merged = decide.Rung(
        "shard-merge", "every source's rows written into the one target",
        frozenset({"shard change"}),
        needs=(decide.Need("one target",
                           lambda f: f.get("targets", 0) == 1),),
        because="many into one places every row in the one target; the key"
                " keeps two sources' rows apart")
    choice = decide.climb("shard split / merge",
                          decide.fenced((by_hash, merged), *shared),
                          facts, need=("shard change",))
    details = (f"{facts.get('kind')}: {facts.get('sources', 0)} sources to"
               f" {facts.get('targets', 0)} targets",)
    whole = sorted(t for t, s in (facts.get("tables") or {}).items()
                   if s.get("whole"))
    if whole:
        details += ("taken whole by every target: " + ", ".join(whole[:6]),)
    return ScenarioDecision("shard split / merge", choice, details)


def shard_lines(hop, eng, db, siblings=None):
    """The plan's line of a shard split or merge, the decision kept in the
    run's record; nothing for a hop that is no group's."""
    facts = shard_facts_of(hop, eng, db, siblings)
    if facts is None:
        return []
    got = shard_case(facts)
    decide.record(hop, got.choice)
    return [f"  {got.words()}"]


# --- many-node topologies -----------------------------------------------------

#: the shapes of a many-node topology (backlog R3.topo)
TOPOLOGIES = {"many-to-one": "many sources into one target",
              "one-to-many": "one source fanned out to many targets",
              "mesh": "every node both a source and a target"}

#: how a key both sides write may be settled, `twoway`'s policies but the
#: one that stops
SETTLES = ("apply_remote", "keep_local", "last_update_wins")


def topology_settings(hop):
    """What only the operator knows of a many-node topology, read from the
    hop's `nodes:` as `hopkeys` holds it: each source's key space of each
    table (`topology.KeySpace`), the conflict rule, a fan-out's routing and
    which nodes mark migkit's writes. Each is empty where the hop names
    none; a word that is not one it may be is refused."""
    from . import hopkeys, topology
    raw = hopkeys.opt(hop, "nodes") or {}
    if not isinstance(raw, dict):
        raise SystemExit("nodes: is a mapping - spaces, conflict, route,"
                         " marks")
    conflict = hopkeys.sub_choice("nodes", "conflict", raw.get("conflict"))
    if raw.get("conflict") not in (None, "") and conflict is None:
        raise SystemExit(f"nodes: conflict: {raw.get('conflict')} - one of"
                         f" {', '.join(SETTLES)}")
    route = dict(raw.get("route") or {})
    by = hopkeys.sub_choice("nodes", "route.by", route.get("by"))
    if route.get("by") not in (None, "") and by is None:
        raise SystemExit(f"nodes: route: by: {route.get('by')} - one of"
                         " table, range, hash")
    if by is not None:
        route["by"] = by
    spaces = {}
    for source, tables in (raw.get("spaces") or {}).items():
        for table, spec in (tables or {}).items():
            try:
                spaces.setdefault(str(source), {})[str(table)] = \
                    topology.KeySpace.of(spec)
            except (TypeError, ValueError) as e:
                raise SystemExit(f"nodes: spaces: {source}: {table}: the"
                                 f" key space is not read ({e})")
    return {"spaces": spaces, "conflict": conflict, "route": route,
            "marks": {str(k): bool(v) for k, v in
                      (raw.get("marks") or {}).items()}}


def topology_facts(kind, sources, targets, tables, spaces=None,
                   conflict=None, route=None, marks=None, links=None):
    """The facts of a many-node topology: its kind, the source and target
    names (a mesh's nodes are both), each table's `{"key": [...],
    "columns": [...]}`, each source's key space of a table (`{source:
    {table: spec}}`, `topology.KeySpace.of`), the conflict rule chosen
    (`twoway.POLICIES`), a fan-out's routing rule (`{"by": "table" |
    "range" | "hash", "tables": {...}, "bounds": [...]}`), whether each
    node tells migkit's own writes apart (`twoway`'s marks) and the links
    set up between nodes."""
    return decide.Facts({
        "kind": kind, "sources": tuple(sources), "targets": tuple(targets),
        "tables": dict(tables), "spaces": dict(spaces or {}),
        "conflict": conflict, "route": dict(route or {}),
        "marks": dict(marks or {}), "links": tuple(links or ())})


def _overlaps(f):
    from . import topology
    return topology.overlaps({s: (f.get("spaces") or {}).get(s)
                              for s in f.get("sources", ())},
                             f.get("tables") or {})


def _overlap_why(f):
    got = _overlaps(f)
    return ("; ".join(f"{t}: {a} and {b} both write key {k!r}"
                      for t, a, b, k in got[:4])
            + " - give each source its own range or stride of the key, or"
              " name a conflict rule (" + ", ".join(SETTLES) + ")")


def _settled(f):
    return f.get("conflict") in SETTLES


def _route(f):
    from . import topology
    r = f.get("route") or {}
    return topology.Route(r.get("by") or "hash", tuple(f.get("targets", ())),
                          dict(r.get("tables") or {}),
                          tuple(r.get("bounds") or ()))


def _ruled(by):
    return decide.Need(f"the routing rule is by {by}",
                       lambda f: (f.get("route") or {}).get("by") in (None, by),
                       lambda f: f"the hop routes by"
                                 f" {(f.get('route') or {}).get('by')}")


def _unrouted(f):
    r = f.get("route") or {}
    targets = set(f.get("targets", ()))
    named = dict(r.get("tables") or {})
    return [t for t in sorted(f.get("tables") or {})
            if named.get(t) not in targets]


def _keyed(f):
    return [t for t, s in sorted((f.get("tables") or {}).items())
            if not s.get("key") or any(k not in (s.get("columns") or ())
                                       for k in s.get("key") or ())]


def _unmarked(f):
    marks = f.get("marks") or {}
    return [n for n in f.get("sources", ()) if not marks.get(n)]


def _mesh_gaps(f):
    from . import topology
    return topology.missing_links(f.get("sources", ()), f.get("links", ()))


_PROVED = (("correct", "proved"), ("consistent", "per range"),
           ("idempotent", "converges"), ("resumable", "goes on"))


def _apart(name, said, because):
    return decide.Rung(
        name, said, frozenset({"topology"}),
        needs=(decide.Need("key spaces apart", lambda f: not _overlaps(f),
                           _overlap_why),),
        holds=_PROVED + (("exactly once", "exactly once"),),
        because=because)


def _settling(name, said):
    return decide.Rung(
        name, said, frozenset({"topology"}),
        needs=(decide.Need("a conflict rule", _settled,
                           lambda f: _overlap_why(f)),),
        holds=_PROVED + (("exactly once", "at least once"),),
        because="the key spaces meet, and the rule named settles a key two"
                " sources write: each write of it is applied, the rule"
                " keeping one")


def _many_to_one():
    shape = decide.Need(
        "many sources and one target",
        lambda f: len(f.get("sources", ())) > 1 and len(f.get("targets", ())) == 1,
        lambda f: f"many-to-one needs many sources and one target; the hop"
                  f" names {len(f.get('sources', ()))} and"
                  f" {len(f.get('targets', ()))}")
    return decide.fenced(
        (_apart("union-apart", "each source on its own tail into the one"
                " target, its keys apart from the others'",
                "the sources' key spaces were solved apart, so a row lands"
                " once and a delete only ever meets its own source's rows"),
         _settling("union-settled", "each source on its own tail into the"
                   " one target, keys both write settled by the rule")),
        shape)


def _one_to_many():
    shape = decide.Need(
        "one source and many targets",
        lambda f: len(f.get("sources", ())) == 1 and len(f.get("targets", ())) > 1,
        lambda f: f"one-to-many needs one source and many targets; the hop"
                  f" names {len(f.get('sources', ()))} and"
                  f" {len(f.get('targets', ()))}")
    from . import topology
    holds = _PROVED + (("exactly once", "exactly once"),)
    by_table = decide.Rung(
        "fanout-by-table", "each table to the target the rule names",
        frozenset({"topology"}),
        needs=(_ruled("table"),
               decide.Need("every table routed", lambda f: not _unrouted(f),
                           lambda f: "no target named for "
                                     + ", ".join(_unrouted(f)[:6]))),
        holds=holds, because="whole tables go, no row is placed one by one")
    by_range = decide.Rung(
        "fanout-by-range", "rows cut by ranges of the key",
        frozenset({"topology"}),
        needs=(_ruled("range"),
               decide.Need("ranges placing every key once",
                           lambda f: not topology.range_gaps(
                               (f.get("route") or {}).get("bounds") or (),
                               f.get("targets", ())),
                           lambda f: topology.range_gaps(
                               (f.get("route") or {}).get("bounds") or (),
                               f.get("targets", ()))),
               decide.Need("a key on every table", lambda f: not _keyed(f),
                           lambda f: "no key on its columns: "
                                     + ", ".join(_keyed(f)[:6]))),
        holds=holds, because="each key falls in exactly one range")
    by_hash = decide.Rung(
        "fanout-by-hash", "rows placed by a hash of the key",
        frozenset({"topology"}),
        needs=(_ruled("hash"),
               decide.Need("a key on every table", lambda f: not _keyed(f),
                           lambda f: "no key on its columns: "
                                     + ", ".join(_keyed(f)[:6]))),
        holds=holds, because="each key hashes into exactly one target, the"
                             " same in every run")
    return decide.fenced((by_table, by_range, by_hash), shape)


def _mesh():
    shared = (
        decide.Need("two nodes or more",
                    lambda f: len(f.get("sources", ())) > 1
                    and set(f.get("sources", ())) == set(f.get("targets", ())),
                    lambda f: "a mesh's nodes are each a source and a target"),
        decide.Need("every node tells migkit's writes apart",
                    lambda f: not _unmarked(f),
                    lambda f: "no mark of migkit's own writes on "
                              + ", ".join(_unmarked(f)[:6])
                              + ": a change would come back round"),
        decide.Need("a link between every two nodes",
                    lambda f: not _mesh_gaps(f),
                    lambda f: "links not set up: "
                              + ", ".join(f"{a} to {b}" for a, b
                                          in _mesh_gaps(f)[:6])
                              + " - a node would have to pass on another's"
                                " change, a forwarding ring, not a mesh"))
    return decide.fenced(
        (_apart("mesh-apart", "every node applies only the changes that"
                " began on its sender, keys apart",
                "each change carries its origin and goes straight from it"
                " to every other node, so none is passed on or comes back"),
         _settling("mesh-settled", "every node applies only the changes"
                   " that began on its sender, keys both write settled by"
                   " the rule")),
        *shared)


_LADDERS = {"many-to-one": _many_to_one, "one-to-many": _one_to_many,
            "mesh": _mesh}


def topology_case(facts):
    """A many-node topology decided on its facts: a kind migkit does not
    know refused; many-to-one with the sources' key spaces solved apart
    (`topology.overlaps`) or, where they meet, a conflict rule named, each
    source on its own tail so its order holds; one-to-many by the rule the
    hop names - whole tables, ranges of the key or a hash - the source's
    log held until the slowest target has applied it; a full mesh only
    with every node marking migkit's writes and every link set up. What
    the way keeps and gives up of the guarantees is said."""
    from . import guarantees as g
    kind = facts.get("kind")
    # the kind is decided before its ladder: held where migkit knows it,
    # refused with the kinds it knows where it does not
    known = decide.Rung(
        "topology-unknown", "a topology migkit knows", frozenset({"topology"}),
        needs=(decide.Need("a known topology",
                           lambda f: f.get("kind") in _LADDERS,
                           lambda f: f"{f.get('kind')!r} is not one of "
                                     + ", ".join(TOPOLOGIES)),))
    gate = decide.climb("many-node topology", (known,), facts,
                        need=("topology",))
    if gate.rung is None:
        return ScenarioDecision("many-node topology", gate)
    rungs = _LADDERS[kind]()
    choice = decide.climb(f"many-node topology ({kind})", rungs, facts,
                          need=("topology",))
    details = [f"{TOPOLOGIES[kind]}: {', '.join(facts.get('sources', ()))}"
               f" to {', '.join(facts.get('targets', ()))}"]
    if choice.rung is not None:
        got = g.of([choice.rung])
        best = g.strongest([g.of([r]) for r in rungs])
        details.append("keeps: " + g.said(got))
        lost = [f"{d}: {g.LEVELS[d][got[d]]} where another way here keeps"
                f" {g.LEVELS[d][best[i]]}" for i, d in enumerate(g.ORDER)
                if got[d] < best[i]]
        if lost:
            details.append("gives up: " + "; ".join(lost))
        if kind == "one-to-many":
            details.append("the source's log is held until the slowest"
                           " target has applied it")
        if kind == "many-to-one":
            details.append("deletes are scoped to the source they began on")
    return ScenarioDecision("many-node topology", choice, tuple(details))


def topology_hops(facts, decision):
    """The plan of a decided topology as hops the planner runs, one per
    source and target pair: what it carries, the origin it applies, the
    key space it writes and the rule it settles or routes by. Nothing
    where the case was refused."""
    if decision.choice.rung is None:
        return ()
    kind, way = facts.get("kind"), decision.choice.path
    tables = sorted(facts.get("tables") or {})
    spaces = facts.get("spaces") or {}
    settle = facts.get("conflict") if way.endswith("settled") else None
    if kind == "one-to-many":
        route, source = _route(facts), facts["sources"][0]
        out = []
        for t in facts["targets"]:
            carried = ([x for x in tables if route.place(x, ()) == t]
                       if route.by == "table" else tables)
            out.append({"source": source, "target": t, "tables": carried,
                        "route": route.by, "origin": source})
        return tuple(out)
    if kind == "many-to-one":
        target = facts["targets"][0]
        pairs = [(s, target) for s in facts["sources"]]
    else:
        from . import topology
        pairs = topology.mesh_links(facts["sources"])
    return tuple({"source": s, "target": t, "tables": tables, "origin": s,
                  "keys": dict(spaces.get(s) or {}), "settle": settle}
                 for s, t in pairs)


@dataclass(frozen=True)
class TopologyCheck:
    ok: bool
    lines: tuple = ()


def topology_verify(facts, rows):
    """The proof that the parts equal the whole for a decided topology.
    many-to-one: the target is the union of the sources (`prove_union`),
    a key two sources write allowed only under a conflict rule, and each
    source's changes applied in its order (`rows["applied"]`, [(source,
    position)]). one-to-many: the targets are the source cut once by the
    rule (`prove_partition`). mesh: every node holds the same rows
    (`prove_converged`) and no change came round (`rows["applied"]`,
    [(node, sender, origin, change)])."""
    from . import topology
    kind = facts.get("kind")
    lines = []
    if kind == "many-to-one":
        both = set()
        if _settled(facts):
            seen = {}
            for tables in rows["sources"].values():
                for t, got in tables.items():
                    for k in got:
                        seen[(t, k)] = seen.get((t, k), 0) + 1
            both = {x for x, n in seen.items() if n > 1}
        proof = topology.prove_union(rows["sources"], rows["target"], both)
        late = topology.out_of_order(rows.get("applied", ()))
        lines.append(f"union of {len(rows['sources'])} sources: {proof.words()}")
        if both:
            lines.append(f"{len(both)} keys written by two sources settled by"
                         f" {facts.get('conflict')}")
        if late:
            lines.append(f"{len(late)} changes applied out of their"
                         f" source's order: {late[:3]!r}")
        return TopologyCheck(proof.ok and not late, tuple(lines))
    if kind == "one-to-many":
        proof = topology.prove_partition(rows["source"], rows["targets"],
                                         _route(facts))
        lines.append(f"cut into {len(rows['targets'])} targets:"
                     f" {proof.words()}")
        return TopologyCheck(proof.ok, tuple(lines))
    if kind == "mesh":
        proof = topology.prove_converged(rows["nodes"])
        came = topology.echoes(rows.get("applied", ()))
        lines.append(f"{len(rows['nodes'])} nodes: "
                     + ("converged" if proof.ok else proof.words()))
        if came:
            lines.append(f"{len(came)} changes came round or were passed"
                         f" on: {came[:3]!r}")
        return TopologyCheck(proof.ok and not came, tuple(lines))
    return TopologyCheck(False, (f"{kind!r} is not a known topology",))


# --- how a table's storage decides the copy (access methods, table engines) ----

def storage_copy_facts(eng, db):
    """The facts of the copy way: each side's storage per table as the
    engine reads it (`tableengine.observed`) - a PostgreSQL access method
    (heap or an extension's columnar one) or a ClickHouse table engine -
    and the storage the physical copy can carry: pages of a plain heap
    (`Engine.table_storage` a string where the engine does not read it)."""
    from . import tableengine
    seen = tableengine.observed(eng, db)
    return decide.Facts({"source": seen["src"], "target": seen["dst"],
                         "not_read": seen.get("not_read")})


def _storage_read(f):
    return f.get("source") is not None and f.get("target") is not None


#: the storage whose pages or parts the physical copy carries as they are:
#: a PostgreSQL heap, an InnoDB table (the clone copies InnoDB's own
#: files; any other engine's table it leaves empty), a plain MergeTree
PLAIN_STORAGE = ("heap", "innodb", "mergetree")


def _plain_heap(tables):
    return all(m in PLAIN_STORAGE for m in (tables or {}).values())


def storage_copy_case(facts):
    """The way a database is copied, decided from its tables' storage: the
    physical way (pages, parts) only where every table of both sides is a
    plain heap, InnoDB table or MergeTree (`PLAIN_STORAGE`) - an extension's columnar access method, a
    table engine that merges rows or one that holds none of its own
    (memory, remote, null) is not a page of the same shape, so the rows are
    read and written (the logical way), and each table that is not plain
    is named. Storage that was not read is said, and the logical way kept:
    it carries every row of any of them."""
    physical = decide.Rung(
        "physical", "pages or parts copied as they are", frozenset({"copy"}),
        needs=(decide.Need("storage read on both sides", _storage_read,
                           lambda f: f.get("not_read")
                           or "the storage was not read"),
               decide.Need("every table a plain heap, InnoDB or MergeTree",
                           lambda f: _plain_heap(f.get("source"))
                           and _plain_heap(f.get("target")),
                           lambda f: "tables stored otherwise: " + ", ".join(
                               sorted(t for side in ("source", "target")
                                      for t, m in (f.get(side) or {}).items()
                                      if m not in PLAIN_STORAGE)[:5]))),
        because="every table is stored the plain way on both sides")
    logical = decide.Rung(
        "logical", "rows read and written", frozenset({"copy"}),
        footprint=1, because="it carries every row whatever the storage")
    choice = decide.climb("copy way by storage", (physical, logical), facts,
                          need=("copy",))
    odd = sorted({t: m for side in ("source", "target")
                  for t, m in (facts.get(side) or {}).items()
                  if m not in PLAIN_STORAGE}.items())
    return ScenarioDecision("copy way by storage", choice, tuple(
        f"{t}: stored as {m}, not copied as pages" for t, m in odd[:5]))


def storage_copy_way(hop, eng, db):
    """(the copy way the tables' storage allows, the decision) for a
    database - "physical" or "logical" - recorded in the run's record; None
    where the storage cannot be read of either side. The one answer the
    move's copy-way choice asks before it offers pages or parts
    (`physical.storage_held`, from `physical.choose` and `physical.carry`),
    so the plan's line is the way that runs."""
    try:
        facts = storage_copy_facts(eng, db)
    except Exception:  # noqa: BLE001 - unread storage keeps the logical way
        return None
    if facts.get("source") is None and facts.get("target") is None:
        return None
    got = storage_copy_case(facts)
    decide.record(hop, got.choice)
    return (got.choice.rung.name if got.choice.rung is not None else None,
            got)


def copy_lines(hop, eng, db):
    """The plan's lines of the copy way by storage, and for a consolidation
    the group as a many-to-one topology (`consolidate.topology_decision`,
    its conflict rule and key spaces from `nodes:`), each decision kept in
    the run's record. Nothing where nothing was read."""
    out = []
    if not db:
        return out
    got = storage_copy_way(hop, eng, db)
    if got is not None:
        out.append(f"  {got[1].words()}")
    if (getattr(hop, "goal", "") or "") == "consolidate":
        from . import consolidate
        try:
            topo = consolidate.topology_decision(hop, eng, db)
        except (Exception, SystemExit) as e:  # noqa: BLE001 - said, plan goes on
            out.append("  the consolidation's topology could not be decided:"
                       f" {(str(e).strip().splitlines() or [''])[-1][:160]}")
        else:
            decide.record(hop, topo.choice)
            out.append(f"  {topo.words()}")
    return out

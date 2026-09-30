"""The machine to run on, and the class the target needs (backlog R22,
item 6).

Two questions an operator asks before a move and used to answer by
guessing: what to run migkit (or its agents) on so the move meets its
deadline for the least money, and what size the target database must be
to carry the source's load.

**The machine for migkit.** A move goes as fast as its slowest stage
(a roofline): the source's reads, the target's writes, the link between
them - none of which a bigger machine for migkit changes - and this
machine's own CPUs, network and disk, which it does. So the advice takes
the stage rates the resource model measured (`from_model`; the hop's
earlier runs where no model was kept), the data's size and the hop's
`deadline:`, and walks the catalog of the cloud the servers run in
(`where`, from what `clouds` found): for each type, how long
the move takes on it, what binds there, and what it costs; the cheapest
that meets the deadline is said, with its numbers. Where a server binds,
no machine meets a deadline the server cannot - and it says so, and what
would (a larger source or target, fewer bytes), instead of selling a
bigger machine that would sit waiting.

Many types' network is a burst over a far lower baseline: an m6i.large
is "up to 12.5 Gbps" on a 0.78 Gbps baseline, its burst lasting from 5
to 60 minutes by AWS's own account, depending on the size; its EBS
bursts for 30 minutes once a day. A long move runs at the baseline, so
the time on a type is worked out in phases - the burst while it lasts
(the least the provider promises), the baseline after - and a move that
ends inside the burst gets it. Worked on the shipped catalog
(`test_the_machine_to_run_on.py`), an m6i.large whose network binds:
10 GB takes 6.4 seconds, all of it in the burst; 2 TB takes 4.4 hours -
five minutes at 12.5 Gbps and the rest at 0.78 - where reading "up to
12.5 Gbps" as the rate says 21 minutes.

**The catalog** (`data/instances.json`) is the providers' own published
numbers - vCPU, memory, network baseline and burst, disk throughput
baseline and burst, on-demand Linux prices for a few regions - with the
pages they came from in its header. Where the operator asks for it
(`MIGKIT_CATALOG_REFRESH=1`) and AWS credentials are present it is
refreshed from the provider's own API (`refresh`: the instance types'
description and the price list) into a copy of its own, kept a week.
Never unasked: a machine with credentials in `~/.aws` is not a machine
whose owner agreed to migkit calling AWS, and migkit makes no call out
it was not told to.

**The target's class** (the first piece of backlog W2): each `assess`
takes a snapshot of the source's load - its CPU as the sessions running
on it, its block reads and writes a second, its connections, its cache
and how often reads miss it (`observe`) - and keeps it beside the hop, so
the class is sized from the busiest the source was over every snapshot
kept, not from one moment.

No program is named here; what an operator reads is the machine's
numbers and why.
"""
import json
import math
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

CATALOG = Path(__file__).resolve().parent / "data" / "instances.json"
#: a refreshed catalog is taken over the shipped one while it is younger
#: than this
REFRESH_DAYS = 7
#: bytes a second in one gigabit a second, and in a published MB/s
GBPS, MBPS = 1e9 / 8, 1e6


def _refreshed_path():
    """Where a catalog refreshed from a provider's API is kept: beside
    the operator's other migkit files, never inside the installed
    package."""
    override = os.environ.get("MIGKIT_INSTANCES")
    if override:
        return Path(override).expanduser()
    base = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
    return base / "migkit" / "instances.json"


def catalog():
    """The catalog: the shipped one, with a refreshed copy's types taken
    over it where one exists and is recent."""
    got = json.loads(CATALOG.read_text())
    fresh = _refreshed_path()
    try:
        extra = json.loads(fresh.read_text())
    except (OSError, ValueError):
        return got
    if time.time() - float(extra.get("refreshed_at") or 0) \
            > REFRESH_DAYS * 86400:
        return got
    for cloud, types in (extra.get("types") or {}).items():
        mine = got["types"].setdefault(cloud, {})
        for name, spec in types.items():
            merged = dict(mine.get(name) or {})
            merged.update({k: v for k, v in spec.items() if k != "price_hour"})
            merged["price_hour"] = {**(merged.get("price_hour") or {}),
                                    **(spec.get("price_hour") or {})}
            mine[name] = merged
    got["refreshed"] = extra.get("refreshed")
    return got


@dataclass(frozen=True)
class Machine:
    """One instance type, in bytes a second and seconds."""
    cloud: str
    name: str
    vcpu: int
    memory_gib: float
    arch: str
    net: tuple                      #: (baseline, burst) bytes a second
    disk: tuple                     #: (baseline, burst) bytes a second
    net_burst_s: float = 0.0        #: how long the network's burst lasts
    disk_burst_s: float = 0.0       #: how long the disk's burst lasts
    iops: tuple = None              #: (baseline, burst), where published
    prices: dict = field(default_factory=dict)

    def price(self, region):
        return (self.prices or {}).get(region)

    def said(self):
        net = self.net[0] / GBPS
        burst = self.net[1] / GBPS
        way = (f"{_g(net)} Gbps sustained, {_g(burst)} in a burst"
               if burst > net * 1.01 else f"{_g(net)} Gbps")
        return (f"{self.name} ({self.vcpu} vCPU, {_g(self.memory_gib)} GiB,"
                f" {way})")


def _g(x):
    return f"{x:,.2f}".rstrip("0").rstrip(".")


def machines(cloud, region=None, cat=None):
    """The catalog's types for `cloud`, priced in `region` where one is
    given (a type without a price there is left out)."""
    cat = cat or catalog()
    minutes = (cat.get("burst_minutes") or {}).get(cloud) or {}
    out = []
    for name, s in ((cat.get("types") or {}).get(cloud) or {}).items():
        if region and region not in (s.get("price_hour") or {}):
            continue
        net, disk = s["network_gbps"], s["disk_mb_s"]
        iops = s.get("iops")
        out.append(Machine(
            cloud, name, int(s["vcpu"]), float(s["memory_gib"]),
            s.get("arch", "x86_64"),
            (net["baseline"] * GBPS, net["burst"] * GBPS),
            (disk["baseline"] * MBPS, disk["burst"] * MBPS),
            60.0 * float(minutes.get("network") or 0),
            60.0 * float(minutes.get("disk") or 0),
            (iops["baseline"], iops["burst"]) if iops else None,
            dict(s.get("price_hour") or {})))
    return out


# --- the stages the resource model measures ---------------------------------

#: how the servers and the link together are said
BEYOND = "the servers and the link together"


@dataclass
class Stages:
    """What each stage of a move carries, bytes a second - the shape the
    resource model (R22 item 1) answers in; None where a stage was not
    measured. The servers' and the link's rates are what no machine for
    migkit changes; `per_core` is migkit's own work on one core, `wire`
    the bytes crossing this machine's network each way for a byte moved
    (1 when the rows pass through it, less when they cross compressed, 0
    when they go server to server), `spill` the bytes its disk writes and
    reads back for one (2 for a local copy written and then loaded, 0
    for a stream)."""
    source: float = None
    target: float = None
    link: float = None
    #: the servers and the link together, where a run showed what they
    #: gave and not which of them held it
    beyond: float = None
    #: what the servers and the link gave at least, where a run that this
    #: machine held back showed only that much of them
    at_least: float = None
    per_core: float = None
    wire: float = 1.0
    spill: float = 0.0
    measured: str = ""

    def servers(self):
        """{what: rate} of the stages no machine for migkit changes."""
        return {w: r for w, r in (("the source's reads", self.source),
                                  ("the target's writes", self.target),
                                  ("the link", self.link),
                                  (BEYOND, self.beyond))
                if r}

    def host(self, m, net_burst=False, disk_burst=False):
        """{what: rate} of this machine's own stages on type `m`."""
        out = {}
        if self.per_core:
            out["its CPUs"] = self.per_core * m.vcpu
        if self.wire and self.wire > 0:
            out["its network"] = m.net[1 if net_burst else 0] / self.wire
        if self.spill and self.spill > 0:
            out["its disk"] = m.disk[1 if disk_burst else 0] / self.spill
        return out


def seconds_on(m, stages, size):
    """(seconds, what binds, rate at the end) for `size` bytes on type
    `m`: the least of the stages, phase by phase - each burst while it
    lasts, the baselines after it."""
    ends = sorted({t for t in (m.net_burst_s, m.disk_burst_s) if t > 0})
    at, left = 0.0, float(size)
    for end in ends + [math.inf]:
        rates = dict(stages.servers())
        rates.update(stages.host(m, at < m.net_burst_s,
                                 at < m.disk_burst_s))
        if not rates:
            return 0.0, "", math.inf
        binds = min(rates, key=rates.get)
        rate = rates[binds]
        if rate <= 0:
            return math.inf, binds, 0.0
        can = rate * (end - at)
        if can >= left:
            return at + left / rate, binds, rate
        left -= can
        at = end
    return math.inf, "", 0.0


@dataclass
class Advice:
    machine: Machine = None
    seconds: float = None
    binds: str = ""
    rate: float = None
    cost: float = None
    #: a server binds past the deadline: no machine for migkit meets it
    server_bound: bool = False
    words: str = ""


def advise(stages, size, deadline, types, region=None):
    """The cheapest type in `types` that moves `size` bytes within
    `deadline` seconds (None: the cheapest that keeps up with the
    servers), or why none can - an `Advice` whose `words` say it.

    Cost is the price a second times the time the move takes there, as
    the providers bill a running machine by the second: a faster machine
    that finishes sooner can cost less than a slower one."""
    from .wording import human_bytes
    servers = stages.servers()
    bound = min(servers.values()) if servers else None
    which = min(servers, key=servers.get) if servers else ""
    if deadline and bound and size / bound > deadline:
        return Advice(server_bound=True, binds=which, rate=bound,
                      seconds=size / bound, words=(
                          f"no machine for migkit meets the deadline of"
                          f" {_took(deadline)}: {_bind(which)} at"
                          f" ~{_mb(bound)} MB/s, which takes"
                          f" {_took(size / bound)} for"
                          f" {human_bytes(size)} whatever migkit runs on -"
                          f" a bigger machine would not help. "
                          + _what_would(which)))
    if not types:
        return Advice(words="no instance type in the catalog to weigh")
    if not stages.host(types[0]):
        m = min(types, key=lambda t: (t.price(region) or math.inf, t.vcpu))
        return Advice(m, bound and size / bound, which, bound, words=(
            f"the rows go from server to server and none cross migkit's"
            f" machine, so the smallest will do: {m.said()}"))
    rows = []
    for m in types:
        secs, binds, rate = seconds_on(m, stages, size)
        price = m.price(region)
        cost = price * secs / 3600 if price is not None \
            and secs != math.inf else None
        rows.append((m, secs, binds, rate, cost))
    if deadline:
        meet = [r for r in rows if r[1] <= deadline]
    else:
        # no deadline: the least that keeps up with the servers, or the
        # fastest there is where nothing does
        fast = min(r[1] for r in rows)
        meet = [r for r in rows if r[2] in servers] or \
            [r for r in rows if r[1] <= fast * 1.001]
    if not meet:
        m, secs, binds, rate, cost = min(rows, key=lambda r: r[1])
        return Advice(m, secs, binds, rate, cost, words=(
            f"no type here meets the deadline of {_took(deadline)}: the"
            f" fastest, {m.said()}, takes {_took(secs)} for"
            f" {human_bytes(size)}, with {binds} holding it to"
            f" ~{_mb(rate)} MB/s"))
    m, secs, binds, rate, cost = min(
        meet, key=lambda r: (r[4] if r[4] is not None else math.inf,
                             r[0].price(region) or math.inf, r[1]))
    price = m.price(region)
    words = (f"{m.said()}: about {_took(secs)} for {human_bytes(size)} at"
             f" ~{_mb(rate)} MB/s"
             + (f", about ${cost:,.2f} for the move at ${price:,.4g} an hour"
                f" in {region}" if cost is not None else "")
             + (f", inside the deadline of {_took(deadline)}" if deadline
                else ""))
    if binds in servers:
        words += (f"; {_bind(binds)} there, so a bigger machine would not"
                  " be faster")
    elif not servers and stages.at_least and rate > stages.at_least * 1.1:
        words += (f"; {_bind(binds)} there - if the servers and the link"
                  f" give it: they gave ~{_mb(stages.at_least)} MB/s to a"
                  " machine that held them back, and more is not measured"
                  " yet")
    else:
        burst = m.net[1] > m.net[0] * 1.01 and m.net_burst_s \
            and secs > m.net_burst_s
        words += f"; {_bind(binds)} there" + (
            f" - at its baseline: its burst lasts about"
            f" {_took(m.net_burst_s)} and the move longer"
            if burst and binds == "its network" else "")
    return Advice(m, secs, binds, rate, cost, words=words)


def _bind(what):
    """`what` with its verb: the reads bind, the network binds."""
    return f"{what} bind" if what.endswith(("reads", "writes", "CPUs",
                                            "together")) \
        else f"{what} binds"


def _what_would(which):
    return {"the source's reads": "A larger source instance, a replica to"
                                  " read from, or fewer bytes (a row filter,"
                                  " tables left out) would.",
            "the target's writes": "A larger target instance for the load,"
                                   " or indexes built after the rows (the"
                                   " move does that where it may), would.",
            "the link": "A wider link, or migkit's relay beside the source"
                        " sending it compressed, would.",
            BEYOND: "A larger source or target, or a wider link, would -"
                    " the run measured did not say which of them held it"
                    " back."}.get(which, "")


def _mb(rate):
    return f"{rate / 2 ** 20:,.0f}" if rate >= 2 ** 20 * 10 \
        else f"{rate / 2 ** 20:,.1f}"


def _took(secs):
    if secs is None or secs == math.inf:
        return "for ever"
    if secs >= 86400:
        return f"{secs / 86400:,.1f} days"
    if secs >= 3600:
        return f"{secs / 3600:,.1f} hours"
    if secs >= 60:
        return f"{secs / 60:,.0f} minutes"
    return f"{secs:,.0f} seconds"


# --- the hop's deadline, and which cloud it runs in -------------------------

_UNITS = {"s": 1, "m": 60, "min": 60, "h": 3600, "d": 86400}


def deadline(hop, now=None):
    """Seconds the hop's `deadline:` gives the move, or None: a duration
    (`6h`, `90m`, `2d`, a number of seconds) or a moment
    (`2026-10-01T02:00`, local time). A moment already past is 0."""
    raw = (getattr(hop, "options", None) or {}).get("deadline")
    if raw in (None, ""):
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    text = str(raw).strip()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(s|m|min|h|d)?", text)
    if m:
        return float(m.group(1)) * _UNITS[m.group(2) or "s"]
    import datetime
    try:
        when = datetime.datetime.fromisoformat(text)
    except ValueError:
        raise SystemExit(f"deadline: {text!r} is neither a duration (6h,"
                         " 90m, 2d) nor a moment (2026-10-01T02:00)")
    now = now if now is not None else time.time()
    return max(0.0, when.timestamp() - now)


def where(hop):
    """(cloud, region) the hop's servers run in, as far as it says: its
    `cloud:` and `region:` options first; then who runs each side as
    `clouds` found it - the target's before the source's, since the
    machine for the move goes beside the target - from what `doctor` or
    `init` recorded (`facts.json`) or else from the addresses; then an
    endpoint's `aws_region`. (None, None) where nothing says."""
    import json

    from . import clouds
    opts = getattr(hop, "options", None) or {}
    cloud, region = opts.get("cloud"), opts.get("region")
    try:
        seen = json.loads((hop.report_dir() / "facts.json").read_text())
    except (OSError, ValueError):
        seen = {}
    for side, ep in (("target", hop.target), ("source", hop.source)):
        found = (seen.get(side) or {}).get("where") or {}
        by_host = clouds.from_host(getattr(ep, "host", ""))
        side_opts = getattr(ep, "options", None) or {}
        its = (found.get("cloud") or by_host.cloud
               or ("aws" if side_opts.get("aws_region") else ""))
        cloud = cloud or its or None
        if its and its == cloud:
            # a region only from a side in the same cloud
            region = (region or found.get("region") or by_host.region
                      or side_opts.get("aws_region") or None)
    if cloud and not region:
        region = _DEFAULT_REGION.get(cloud)
    return (str(cloud).lower() if cloud else None,
            str(region) if region else None)


#: the region a cloud's prices are said in when nothing names one
_DEFAULT_REGION = {"aws": "us-east-1", "gcp": "us-central1",
                   "azure": "eastus"}


# --- stage rates until the resource model gives them -------------------------

def stages_for(hop, via, direct=False, db=None):
    """The `Stages` of a move by `via`: from the resource model kept
    beside the database's reports where it was built (`from_model`), else
    from the hop's own runs kept with its rates - the bytes a run carried
    in its seconds, and the CPU seconds this machine spent on it
    (`planner.record_rate`). A run whose CPUs were not near busy was held
    back by something else - the servers or the link - so its rate is
    what they gave; migkit's own work a core is its bytes over its CPU
    seconds. None where nothing was measured."""
    got = from_model(_kept_model(hop, db), direct)
    if got is not None:
        return got
    from . import planner
    runs = [r for r in planner.runs(hop, via)
            if r.get("bytes") and r.get("seconds")]
    if not runs:
        return None
    last = runs[-1]
    rate = last["bytes"] / last["seconds"]
    wire = 0.0 if direct else 1.0
    from . import movers
    spill = 2.0 if via in movers.LOCAL_COPY else 0.0
    cpu, cpus = last.get("cpu_seconds"), last.get("cpus")
    per_core = last["bytes"] / cpu if cpu else None
    busy = cpu / (last["seconds"] * cpus) if cpu and cpus else None
    held = busy is not None and busy < 0.8
    return Stages(beyond=rate if held else None,
                  at_least=None if held else rate, per_core=per_core,
                  wire=wire,
                  spill=spill, measured=(
                      f"the last run on this path ({last.get('at', '')[:10]}):"
                      f" {_mb(rate)} MB/s"
                      + (f", this machine's {cpus} CPUs {busy:.0%} busy"
                         if busy is not None else "")))


def _kept_model(hop, db):
    """The resource model the move's plan kept for `db`, or None - and
    None wherever that model is not part of this migkit."""
    if not db:
        return None
    try:
        from . import model
    except ImportError:
        return None
    load = getattr(model, "load", None)
    return load(hop.report_dir(db)) if load else None


def from_model(m, direct=False, cpus=None):
    """`Stages` from the resource model's own stages (R22 item 1): the
    source's reads, the target's writes and read-back, each link, and this
    machine's CPUs - each as the bytes of the move a second it allows
    (`Stage.bound`). A stage the probes never saw bind (`at_least`) is a
    floor, not a ceiling: it is kept as what the servers gave at least,
    never as the stage that binds. This machine's CPUs are turned into a
    core's worth, which is what scales with a type's vCPUs. None where
    there is no model."""
    if m is None or not getattr(m, "stages", None):
        return None
    stages = m.stages
    floors = []

    def least(*names):
        got = []
        for n in names:
            s = stages.get(n)
            b = s.bound() if s is not None else None
            if not b:
                continue
            (floors if getattr(s, "at_least", False) else got).append(b)
        return min(got) if got else None
    cpu = stages.get("cpu")
    here = cpus or (_host_cpus() or 1)
    per_core = (cpu.bound() / here) if cpu is not None and cpu.bound() \
        else None
    truths = sorted({getattr(s, "truth", "") for s in stages.values()
                     if getattr(s, "truth", "")})
    return Stages(source=least("source"),
                  target=least("target", "target-check"),
                  link=least("link:src", "link:dst"),
                  at_least=min(floors) if floors else None,
                  per_core=per_core, wire=0.0 if direct else 1.0,
                  measured="the resource model's stages ("
                           + ", ".join(truths) + ")")


def _host_cpus():
    from . import sizing
    return sizing.host().get("cpus")


def plan_line(hop, via, size, direct=False, db=None):
    """The plan's line about the machine to run on, or ""."""
    from .wording import human_bytes
    if not size:
        return ""
    cloud, region = where(hop)
    secs = deadline(hop)
    stages = stages_for(hop, via, direct, db)
    if stages is None:
        if direct:
            return ("the rows go from server to server, so migkit's"
                    " machine does not bind: the smallest will do")
        if secs:
            return (f"the deadline of {_took(secs)} needs ~"
                    f"{_mb(size / max(secs, 1))} MB/s end to end for"
                    f" {human_bytes(size)}; nothing is measured on this"
                    " path yet to weigh a machine against - the first"
                    " run's numbers are kept for the next plan")
        return ""
    types = machines(cloud, region) if cloud else []
    if not types:
        # no machine to name: where a server binds past the deadline that
        # is still worth saying, and nothing else is
        got = advise(stages, size, secs, [], region)
        if not got.server_bound:
            return ""
        return (f"{got.words} (going by {stages.measured})"
                + ("" if cloud else "; the hop does not say which cloud it"
                   " runs in (hop options cloud: and region:), so no type"
                   " is named"))
    got = advise(stages, size, secs, types, region)
    return f"{got.words} (going by {stages.measured})"


class Spent:
    """CPU seconds this process and the programs it waited for spent from
    its making: what a copy cost this machine, beside what it took."""

    def __init__(self):
        self.began = _cpu()

    def seconds(self):
        return max(0.0, _cpu() - self.began)


def _cpu():
    import resource
    own = resource.getrusage(resource.RUSAGE_SELF)
    kids = resource.getrusage(resource.RUSAGE_CHILDREN)
    return own.ru_utime + own.ru_stime + kids.ru_utime + kids.ru_stime


# --- refreshed from the provider's API --------------------------------------

def refresh(region="us-east-1", session=None, names=None):
    """The catalog's AWS types asked of AWS itself - their description and
    their on-demand Linux price in `region` - and kept over the shipped
    catalog for `REFRESH_DAYS`. Returns how many types were refreshed.

    The description gives the network's baseline and peak where AWS
    publishes them per network card, and the EBS baseline and maximum; the
    price list is asked for all the types at once."""
    if session is None:
        import boto3
        session = boto3.session.Session()
    names = list(names or (catalog()["types"].get("aws") or {}))
    ec2 = session.client("ec2", region_name=region)
    got = {}
    for i in range(0, len(names), 100):
        chunk = names[i:i + 100]
        try:
            pages = ec2.get_paginator("describe_instance_types").paginate(
                InstanceTypes=chunk)
            items = [t for p in pages for t in p.get("InstanceTypes", [])]
        except Exception:  # noqa: BLE001 - a type this region lacks
            items = []
            for n in chunk:
                try:
                    items += ec2.describe_instance_types(
                        InstanceTypes=[n]).get("InstanceTypes", [])
                except Exception:  # noqa: BLE001
                    continue
        for t in items:
            spec = _described(t)
            if spec:
                got[t["InstanceType"]] = spec
    prices = _prices(session, region, list(got) or names)
    for name, price in prices.items():
        got.setdefault(name, {})["price_hour"] = {region: price}
    path = _refreshed_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({
        "refreshed": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "refreshed_at": time.time(), "region": region,
        "types": {"aws": got}}, indent=1))
    os.replace(tmp, path)
    return len(got)


def _described(t):
    """A catalog entry from one `describe_instance_types` item: only what
    it says, so a field it leaves out keeps the shipped value."""
    out = {}
    v = (t.get("VCpuInfo") or {}).get("DefaultVCpus")
    if v:
        out["vcpu"] = int(v)
    mem = (t.get("MemoryInfo") or {}).get("SizeInMiB")
    if mem:
        out["memory_gib"] = round(mem / 1024, 2)
    archs = (t.get("ProcessorInfo") or {}).get("SupportedArchitectures") or []
    if archs:
        out["arch"] = "arm64" if "arm64" in archs else "x86_64"
    net = t.get("NetworkInfo") or {}
    cards = net.get("NetworkCards") or []
    base = sum(float(c.get("BaselineBandwidthInGbps") or 0) for c in cards)
    peak = sum(float(c.get("PeakBandwidthInGbps") or 0) for c in cards)
    if base and peak:
        out["network_gbps"] = {"baseline": base, "burst": max(base, peak)}
    ebs = (t.get("EbsInfo") or {}).get("EbsOptimizedInfo") or {}
    if ebs.get("BaselineThroughputInMBps") and \
            ebs.get("MaximumThroughputInMBps"):
        out["disk_mb_s"] = {"baseline": float(ebs["BaselineThroughputInMBps"]),
                            "burst": float(ebs["MaximumThroughputInMBps"])}
    if ebs.get("BaselineIops") and ebs.get("MaximumIops"):
        out["iops"] = {"baseline": int(ebs["BaselineIops"]),
                       "burst": int(ebs["MaximumIops"])}
    return out


def _prices(session, region, names):
    """{type: USD an hour} on demand, Linux, shared tenancy, in `region`,
    from the price list; {} where it cannot be asked."""
    try:
        client = session.client("pricing", region_name="us-east-1")
    except Exception:  # noqa: BLE001 - no price list here
        return {}
    out = {}
    for i in range(0, len(names), 50):
        chunk = names[i:i + 50]
        filters = [
            {"Type": "TERM_MATCH", "Field": "regionCode", "Value": region},
            {"Type": "TERM_MATCH", "Field": "operatingSystem",
             "Value": "Linux"},
            {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
            {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
            {"Type": "TERM_MATCH", "Field": "capacitystatus",
             "Value": "Used"},
            {"Type": "ANY_OF", "Field": "instanceType",
             "Value": ",".join(chunk)}]
        token = None
        while True:
            args = {"ServiceCode": "AmazonEC2", "Filters": filters,
                    "FormatVersion": "aws_v1", "MaxResults": 100}
            if token:
                args["NextToken"] = token
            try:
                page = client.get_products(**args)
            except Exception:  # noqa: BLE001 - the price list refused
                return out
            for item in page.get("PriceList") or []:
                got = json.loads(item) if isinstance(item, str) else item
                attrs = (got.get("product") or {}).get("attributes") or {}
                name = attrs.get("instanceType")
                for term in ((got.get("terms") or {}).get("OnDemand")
                             or {}).values():
                    for dim in (term.get("priceDimensions") or {}).values():
                        usd = (dim.get("pricePerUnit") or {}).get("USD")
                        if name and usd and float(usd) > 0:
                            out[name] = float(usd)
            token = page.get("NextToken")
            if not token:
                break
    return out


def maybe_refresh(cloud, region, log=None):
    """`refresh` for an AWS hop where the operator asked for it, this
    machine has AWS credentials and the kept copy is older than
    `REFRESH_DAYS`; said in a line, never a reason to stop."""
    if cloud != "aws" or os.environ.get("MIGKIT_CATALOG_REFRESH") != "1":
        return ""
    try:
        extra = json.loads(_refreshed_path().read_text())
        if time.time() - float(extra.get("refreshed_at") or 0) \
                < REFRESH_DAYS * 86400 and extra.get("region") == region:
            return ""
    except (OSError, ValueError):
        pass
    try:
        import boto3
        session = boto3.session.Session()
        if session.get_credentials() is None:
            return ""
        n = refresh(region, session)
    except Exception as e:  # noqa: BLE001 - the shipped catalog stands
        return (f"the instance catalog was not refreshed from AWS"
                f" ({type(e).__name__}); the shipped one is used")
    return f"the instance catalog was refreshed from AWS: {n} types in {region}"


# --- the source's load, and the target's class ------------------------------

#: how long one observation of the source's load lasts, and how often its
#: sessions are counted within it
OBSERVE_SECONDS, OBSERVE_EVERY = 3.0, 0.25
#: the share of the target's CPUs the source's busiest moment may take:
#: room for the peaks between observations and for the move's own work
CPU_SHARE = 0.6
#: connections kept above the busiest seen
CONNECTION_ROOM = 1.25
#: observations kept per hop
KEEP = 500


def observe(eng, db, seconds=None, clock=time.monotonic, sleep=time.sleep):
    """One observation of the source's load over `seconds`, or None where
    the engine does not say (`Engine.load_sample`): sessions on a CPU
    (mean and most), block reads and writes a second, transactions a
    second, connections, its cache and how often a read misses it."""
    fn = getattr(eng, "load_sample", None)
    if fn is None:
        return None
    seconds = OBSERVE_SECONDS if seconds is None else seconds
    first = fn("src", db)
    if not first:
        return None
    began = clock()
    running = [first.get("running") or 0]
    last = first
    while clock() - began < seconds:
        sleep(OBSERVE_EVERY)
        got = fn("src", db)
        if got:
            last = got
            running.append(got.get("running") or 0)
    took = max(clock() - began, 1e-6)

    def rate(key):
        a, b = first.get(key), last.get(key)
        if a is None or b is None:
            return None
        return max(0.0, (b - a) / took)
    reads, writes = rate("reads"), rate("writes")
    return {"at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "running": round(sum(running) / len(running), 2),
            "running_most": max(running),
            "iops": (round((reads or 0) + (writes or 0), 1)
                     if reads is not None or writes is not None else None),
            "tps": rate("xacts"),
            "connections": max(first.get("connections") or 0,
                               last.get("connections") or 0),
            "max_connections": last.get("max_connections"),
            "cache_bytes": last.get("cache_bytes"),
            "cache_share": last.get("cache_share"),
            "hit": last.get("hit"),
            "cpus": last.get("cpus")}


def _load_path(hop):
    return hop.report_dir() / "load.json"


def keep(hop, seen):
    """Add one observation to the hop's, and return them all."""
    path = _load_path(hop)
    try:
        got = json.loads(path.read_text())
    except (OSError, ValueError):
        got = []
    got.append(seen)
    del got[:-KEEP]
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(got, indent=1))
    os.replace(tmp, path)
    return got


def kept(hop):
    try:
        return json.loads(_load_path(hop).read_text())
    except (OSError, ValueError):
        return []


def _peak(values):
    """The busiest of the values: the most of a few, the 95th of many - a
    single spike among hundreds is not what a class is sized to."""
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    if len(vals) < 20:
        return vals[-1]
    return vals[min(len(vals) - 1, int(round(0.95 * (len(vals) - 1))))]


#: what a managed service's default connection limit is, from the class's
#: memory in bytes - RDS's published defaults: PostgreSQL
#: LEAST({DBInstanceClassMemory/9531392}, 5000), MySQL
#: {DBInstanceClassMemory/12582880} (less than the class's memory, which
#: the service keeps some of for itself - an 8 GiB class gets about 630 on
#: MySQL, not 683)
DEFAULT_CONNECTIONS = {
    ("aws", "postgres"): lambda b: min(int(b / 9531392), 5000),
    ("aws", "mysql"): lambda b: int(b / 12582880),
}
#: the families a managed database runs on: general purpose and memory
#: optimised, never compute optimised
DB_FAMILIES = {"aws": re.compile(r"^[mr]\d"),
               "gcp": re.compile(r"^n\d|^n2d|^e2"),
               "azure": re.compile(r"^Standard_[DE]")}


def target_class(snapshots, types, engine=None, cloud=None):
    """(what the target needs, the smallest type that has it, words) from
    the source's observations.

    CPUs: the busiest sessions-on-CPU seen, at `CPU_SHARE` of the class.
    Memory: the source's own cache at the share of memory a managed
    service gives it by default (a quarter for PostgreSQL's shared
    buffers, three quarters for MySQL's buffer pool) - the working set
    that serves its reads today - and twice that where more than one read
    in twenty misses it. IOPS: its busiest block reads and writes a
    second. Connections: its busiest, with room."""
    if not snapshots:
        return None, None, ("no observation of the source's load kept yet;"
                            " each assess takes one")
    running = _peak(s.get("running_most") for s in snapshots) or 0
    iops = _peak(s.get("iops") for s in snapshots)
    conns = _peak(s.get("connections") for s in snapshots) or 0
    cache = _peak(s.get("cache_bytes") for s in snapshots)
    shares = [s.get("cache_share") for s in snapshots if s.get("cache_share")]
    hits = [s.get("hit") for s in snapshots if s.get("hit") is not None]
    need = {"vcpu": max(2, math.ceil(running / CPU_SHARE)),
            "memory_gib": None, "iops": iops,
            "connections": math.ceil(conns * CONNECTION_ROOM)}
    missing = False
    if cache and shares:
        need["memory_gib"] = cache / shares[-1] / 2 ** 30
        if hits and min(hits) < 0.95:
            need["memory_gib"] *= 2
            missing = True
    window = (f"{len(snapshots)} observation"
              + ("s" if len(snapshots) != 1 else "")
              + f" from {snapshots[0].get('at', '')[:16]}"
              + (f" to {snapshots[-1].get('at', '')[:16]}"
                 if len(snapshots) > 1 else ""))
    said = (f"at the source's busiest over {window}: {running:g} sessions on"
            f" a CPU -> {need['vcpu']} vCPU")
    if need["memory_gib"]:
        said += (f", a {_g(cache / 2 ** 30)} GiB cache -> "
                 f"{_g(need['memory_gib'])} GiB of memory"
                 + (f" (reads miss it {1 - min(hits):.0%} of the time, so"
                    " twice that)" if missing else ""))
    if iops is not None:
        said += f", {iops:,.0f} block reads and writes a second"
    said += f", {need['connections']:,} connections"
    fam = DB_FAMILIES.get(cloud or "")
    fit = [m for m in types
           if (fam is None or fam.search(m.name))
           and m.vcpu >= need["vcpu"]
           and (need["memory_gib"] is None
                or m.memory_gib >= need["memory_gib"] * 0.98)
           and (iops is None or m.iops is None or m.iops[0] >= iops)]
    if not types:
        return need, None, said
    if not fit:
        return need, None, said + ("; no type in the catalog is that large"
                                   " - the load wants more than one server")
    best = min(fit, key=lambda m: (m.vcpu, m.memory_gib,
                                   min(m.prices.values()) if m.prices
                                   else 0))
    said += f"; the smallest with that: {best.name} ({best.vcpu} vCPU," \
            f" {_g(best.memory_gib)} GiB"
    if best.iops and iops is not None:
        said += f", {best.iops[0]:,} IOPS sustained"
    said += ")"
    if best.cloud == "aws":
        said += f", db.{best.name} where the engine is offered on it"
    limit = DEFAULT_CONNECTIONS.get((cloud, engine))
    if limit:
        default = limit(best.memory_gib * 2 ** 30)
        if default < need["connections"]:
            said += (f"; its default connection limit is about {default:,},"
                     f" under the {need['connections']:,} needed - raise"
                     " max_connections in its parameter group")
    return need, best, said


def assess_items(hop, eng, db):
    """`assess` rows: this observation of the source's load, the class
    the target needs from all of them, and the machine to run migkit on."""
    from . import movers
    from .engines import ALIASES
    items = []
    try:
        seen = observe(eng, db)
    except Exception as e:  # noqa: BLE001 - assess goes on
        seen = None
        items.append({"level": "warn", "scope": "sizing",
                      "item": "the source's load",
                      "detail": "could not be observed: "
                                + (str(e).splitlines() or [""])[-1][:100]})
    if seen is None and not items:
        items.append({"level": "pass", "scope": "sizing",
                      "item": "the target's class",
                      "detail": "the source's engine does not say its load,"
                                " so no class is sized from it"})
    if seen is not None:
        all_seen = keep(hop, seen)
        cloud, region = where(hop)
        note = maybe_refresh(cloud, region)
        if note:
            items.append({"level": "pass", "scope": "sizing",
                          "item": "instance catalog", "detail": note})
        engine = ALIASES.get(hop.engine, hop.engine)
        if engine == "hetero":
            engine = (hop.options or {}).get("target_engine", "postgres")
        types = machines(cloud) if cloud else []
        _, best, said = target_class(all_seen, types, engine, cloud)
        if not cloud:
            said += ("; the hop does not say which cloud the target runs in"
                     " (hop options cloud: and region:), so no class is"
                     " named")
        items.append({"level": "pass", "scope": "sizing",
                      "item": "the target's class", "detail": said})
    from . import legs
    via = movers.for_hop(hop, ALIASES.get(hop.engine, hop.engine))[0]
    line = plan_line(hop, via, size_of(eng, db, hop), legs.direct(via), db)
    if line:
        items.append({"level": "pass", "scope": "sizing",
                      "item": "the machine to run migkit on",
                      "detail": line})
    return items


def size_of(eng, db, hop):
    """Bytes of the source's tables the move carries (the hop's
    exclusions left out), 0 where the catalogue does not say."""
    try:
        facts = eng.table_facts("src", db)
    except Exception:  # noqa: BLE001 - no sizes, no weighing
        return 0
    return sum(int(f.get("bytes") or 0) for name, f in facts.items()
               if isinstance(f, dict)
               and not hop.excluded(db, *str(name).split(".")))

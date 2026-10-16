"""The estate, not the hop (backlog W2): every database a machine or an
account can reach, what moving each would take by migkit's own
measurements, and what that would cost on the machines it knows - one
fleet, from facts and history, never a table of rates.

`migkit estate 10.0.0.5:5432 10.0.0.0/28 --account aws:rds:us-east-1`:

- **Discovery.** Each address is knocked on as `init` knocks
  (`engineat.identify`: the server's own first words, the port only the
  order of asking); a network (CIDR) and a host without a port at every
  engine's own port; an account through its provider's own API
  (`cloudapi.Adapter.endpoints`, read only). Where the address names a
  user and `MIGKIT_SOURCE_PASSWORD` holds the password, it signs in to
  read the version and the bytes its databases hold - nothing else is
  read and nothing is written. Every knock is a probe kept with the time
  it took (`diagnostics.ProbeAccumulator`); each is bounded by the knock's
  own wait and the sign-in's, and a port that does not answer is said as
  unreachable, never left out. Probes run side by side up to what this
  process may still open (its file limit less what it holds), not a
  worker count picked here.
- **Sizing.** The time of each move is the size over the rates the
  ledger kept for that engine pair on every hop (`planner.record_rate`'s
  throughput files), the fastest and the slowest run its two bounds
  (`decide.estate_eta`); the source's engine to another target where the
  pair never ran, and "no history" - with the assessment's class
  (`machine.target_class`) - where nothing did.
- **Cost.** The measured time on each catalog type, priced in each
  region the catalog prices (`decide.estate_price`, `machine.advise`
  over `machine.from_history`): the cheapest at each end of the time.

No Terraform provider and no Kubernetes operator: the command line is
the estate's interface in 1.0 (lead decision, 2026-10-16).
"""
import ipaddress
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import click

from . import decide, engineat
from .cli import main
from .diagnostics import ProbeAccumulator

#: built and proved on stand-ins only: listing an account's instances
NOT_RUN = {"aws": "listing an account's RDS and Aurora instances: built,"
                  " not run against a real AWS account"}

#: what each family is asked once signed in: its version, the bytes its
#: databases hold
_ASKED = {
    "postgres": ("select current_setting('server_version'),"
                 " (select coalesce(sum(pg_database_size(datname)), 0)"
                 " from pg_database where datallowconn and not"
                 " datistemplate)"),
    "mysql": ("select @@version, (select coalesce(sum(data_length +"
              " index_length), 0) from information_schema.tables where"
              " table_schema not in ('mysql', 'information_schema',"
              " 'performance_schema', 'sys'))"),
}


@dataclass
class Found:
    """One server of the estate as it answered."""
    host: str
    port: int
    user: str = ""
    engine: str = ""
    version: str = ""
    size: int = None
    reached: bool = False
    note: str = ""
    took: float = 0.0
    eta: tuple = None
    sized_by: str = ""
    cost: tuple = None
    said: list = field(default_factory=list)

    @property
    def address(self):
        return f"{self.host}:{self.port}"


def _signed_in(family, host, port, user, password):
    """(version, bytes) the server says once signed in, or why not."""
    if family not in _ASKED:
        return f"no sign-in on the {family} wire here"
    try:
        if family == "mysql":
            import pymysql
            c = pymysql.connect(host=host, port=int(port), user=user,
                                password=password or "",
                                connect_timeout=int(engineat.WAIT * 4),
                                read_timeout=int(engineat.WAIT * 4))
        else:
            import psycopg2
            c = psycopg2.connect(host=host, port=int(port), user=user,
                                 password=password or "", dbname="postgres",
                                 connect_timeout=int(engineat.WAIT * 4))
        try:
            cur = c.cursor()
            cur.execute(_ASKED[family])
            version, size = cur.fetchone()
        finally:
            c.close()
    except Exception as e:  # noqa: BLE001 - said in the fleet
        return (str(e).strip().splitlines() or [type(e).__name__])[0][:160]
    return str(version or ""), int(size or 0)


def knock(host, port, user="", password=None, probes=None):
    """`Found` for `host:port`: what answers there, its version and size
    where a sign-in is given; kept in `probes` with the time it took."""
    probes = probes if probes is not None else ProbeAccumulator()
    got, start = Found(host, int(port), user), time.monotonic()
    choice, heard = engineat.identify(host, port, can_ask=False)
    got.reached = choice.path is not None and heard is not None
    if heard:
        got.engine, got.version = heard["engine"], heard.get("version", "")
        family = choice.path.partition("/")[0]
        if user:
            said = _signed_in(family, host, port, user, password)
            if isinstance(said, str):
                got.note = f"not signed in: {said}"
            else:
                got.version, got.size = said[0] or got.version, said[1]
        else:
            got.note = "no user given, so its size is not read"
    else:
        got.note = ("unreachable" if "nothing answers" in choice.reason
                    else f"answers as no engine migkit knows:"
                         f" {choice.reason}")
    got.took = time.monotonic() - start
    probes.record("estate.knock", got.address, value={
        "engine": got.engine, "version": got.version, "size": got.size},
        took=got.took, ok=got.reached, why=got.note, paced=True)
    return got


def _room():
    """How many probes may run at once: the files this process may still
    open, each probe holding one socket at a time."""
    try:
        import resource
        soft = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
        held = len(os.listdir("/dev/fd"))
    except (ImportError, OSError, ValueError):
        return 1
    return max(1, soft - held - 16)


def _ports():
    from .engines import PORTS
    mine = {e for k in engineat.KNOCKS for e in k.engines}
    return sorted({p for e, p in PORTS.items() if e in mine})


def targets(items, account_rows=(), user=""):
    """[(user, host, port or None)] of the addresses, networks and an
    account's endpoints - a host without a port at every engine's own."""
    out = []
    for item in items:
        login, _, rest = item.rpartition("@")
        try:
            net = ipaddress.ip_network(rest, strict=False)
        except ValueError:
            net = None
        if net is not None and "/" in rest:
            hosts = list(net.hosts()) or [net.network_address]
            out += [(login or user, str(h), None) for h in hosts]
            continue
        head, host, port, _ = engineat._split(item)
        out.append((head.rstrip("@") or user, host, port))
    out += [(user, r["host"], r.get("port")) for r in account_rows]
    return out


def discover(items, account_rows=(), user="", password=None, probes=None):
    """[`Found`] of every server the addresses reach, in their order."""
    probes = probes if probes is not None else ProbeAccumulator()
    asked = targets(items, account_rows, user)
    bare = [(u, h) for u, h, p in asked if p is None]
    every = _ports()
    pairs = [(u, h, p) for u, h, p in asked if p is not None]
    if bare:
        with ThreadPoolExecutor(min(len(bare) * len(every),
                                    _room())) as pool:
            up = pool.map(lambda x: (x, engineat._reachable(x[1], x[2])),
                          [(u, h, p) for u, h in bare for p in every])
            pairs += [x for x, ok in up if ok]
    if not pairs:
        return []
    with ThreadPoolExecutor(min(len(pairs), _room())) as pool:
        return list(pool.map(
            lambda x: knock(x[1], x[2], x[0], password, probes), pairs))


def _kind(engine, to):
    """The ledger's name of the pair (`planner._kind`)."""
    return engine if not to or to == engine else f"{engine}->{to}"


def history(kind, reports=None):
    """{"pair": [run], "engine": [run]} of every hop's kept copies: the
    runs of this engine pair, and of its source engine to another
    target."""
    from . import config
    root = reports or config.REPORTS
    source = kind.partition("->")[0]
    out = {"pair": [], "engine": []}
    try:
        dirs = sorted(p for p in root.iterdir() if p.is_dir())
    except OSError:
        return out
    for d in dirs:
        try:
            kept = json.loads((d / "throughput.json").read_text())
        except (OSError, ValueError):
            continue
        for runs in kept.values():
            for r in runs if isinstance(runs, list) else ():
                k = str(r.get("kind") or "")
                if k == kind:
                    out["pair"].append(r)
                elif k.partition("->")[0] == source:
                    out["engine"].append(r)
    return out


def _priced(cloud=None, region=None):
    """{(cloud, region): [Machine]} of the catalog's priced types."""
    from . import machine
    cat = machine.catalog()
    out = {}
    for c, types in sorted((cat.get("types") or {}).items()):
        if cloud and c != cloud:
            continue
        regions = sorted({r for s in types.values()
                          for r in (s.get("price_hour") or {})})
        for r in regions:
            if region and r != region:
                continue
            got = machine.machines(c, r, cat)
            if got:
                out[(c, r)] = got
    return out


def size(found, to=None, reports=None, priced=None):
    """`found` with its time and cost, from the ledger and the catalog."""
    from . import machine
    if not found.engine:
        return found
    kind = _kind(found.engine, to)
    kept = history(kind, reports)
    choice, bounds = decide.estate_eta({"size": found.size, **kept})
    found.eta, found.sized_by = bounds, choice.path
    if bounds is None:
        _, _, cls = machine.target_class([], [], found.engine)
        found.said.append(f"no history on {kind}: {choice.reason}; the"
                          f" target's class: {cls}")
        return found
    runs = kept["pair" if choice.path == "pair" else "engine"]
    stages = machine.from_history(runs)
    _, ends = decide.estate_price({
        "bounds": bounds, "size": found.size, "stages": stages,
        "types": priced if priced is not None else _priced()})
    found.cost = ends
    found.said.append(f"sized from {choice.rung.said}"
                      f" ({len(runs)} run{'s' if len(runs) != 1 else ''})")
    return found


def _cost_said(cost):
    if not cost or not all(cost):
        return "not priced"
    (low, lr), (high, hr) = cost
    if abs(high.cost - low.cost) < 0.005:
        return f"${low.cost:,.2f} ({low.machine.name}, {lr})"
    return (f"${low.cost:,.2f} ({low.machine.name}, {lr}) to"
            f" ${high.cost:,.2f} ({high.machine.name}, {hr})")


def fleet(found):
    """The fleet's table: one line a server, the unreachable kept."""
    from .machine import _took
    from .wording import human_bytes
    rows = [("address", "engine", "version", "size", "moved in", "cost",
             "note")]
    for f in found:
        took = ("no history" if f.engine and f.eta is None
                else f"{_took(f.eta[0])} to {_took(f.eta[1])}" if f.eta
                else "")
        rows.append((f.address, f.engine or "-", f.version or "-",
                     human_bytes(f.size) if f.size is not None else "-",
                     took or "-", _cost_said(f.cost) if f.eta else "-",
                     "; ".join(w for w in [f.note] + f.said if w)))
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]) - 1)]
    return "\n".join("  ".join(c.ljust(w) for c, w in zip(r, widths))
                     + "  " + r[-1] for r in rows)


def _account(spec):
    """The endpoints of `provider:product:region`, or why not."""
    from . import cloudapi
    provider, _, rest = spec.partition(":")
    product, _, region = rest.partition(":")
    kind = cloudapi.ADAPTERS.get(provider)
    if kind is None:
        return [], (f"no provider {provider!r}: one of"
                    f" {', '.join(sorted(cloudapi.ADAPTERS))}")
    try:
        rows = kind({"product": product, "region": region}).endpoints()
    except (Exception, SystemExit) as e:  # noqa: BLE001 - said in the fleet
        return [], (str(e).strip().splitlines() or [type(e).__name__])[0]
    return rows, NOT_RUN.get(provider, "")


@main.command("estate")
@click.argument("addresses", nargs=-1)
@click.option("--account", "accounts", multiple=True,
              help="a provider account's instances, provider:product:region"
                   " (aws:rds:us-east-1), read through its own API")
@click.option("--user", default="",
              help="who signs in where an address names nobody; the"
                   " password is MIGKIT_SOURCE_PASSWORD's")
@click.option("--to", default="",
              help="the target engine the moves are sized for (the same"
                   " engine where not given)")
@click.option("--cloud", default="", help="price on this cloud's types only")
@click.option("--region", default="", help="price in this region only")
def estate(addresses, accounts, user, to, cloud, region):
    """Every database these addresses, networks and accounts reach: its
    engine, version and size, the time its move takes by migkit's own
    measured runs, and its cost on the catalog's machines."""
    if not addresses and not accounts:
        raise SystemExit("name an address, a network (10.0.0.0/24) or an"
                         " account (--account aws:rds:us-east-1)")
    rows, notes = [], []
    for spec in accounts:
        got, why = _account(spec)
        rows += got
        if why:
            notes.append(f"{spec}: {why}")
    probes = ProbeAccumulator()
    found = discover(addresses, rows, user,
                     os.environ.get("MIGKIT_SOURCE_PASSWORD"), probes)
    priced = _priced(cloud or None, region or None)
    for f in found:
        size(f, to or None, priced=priced)
    click.echo(fleet(found) if found else "nothing answered")
    for n in notes:
        click.echo(n)

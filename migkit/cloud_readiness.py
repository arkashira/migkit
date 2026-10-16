"""The cloud side of readiness, audited before the move (backlog R25.2).

What a managed side's provider says of where it sits and who may reach
it (`cloudapi.network`), read through the provider's own API behind the
egress gate's `provider` kind, and each gap that would stop the move a
blocker with what to do next:

* the two sides' networks overlapping - they can never be peered or
  routed - and not peered, where a way that has one server read the
  other directly could not be taken
* the subnets a copy or replica of the side is made in with no free
  address left
* the side's security groups or firewall admitting the port from none of
  this machine's addresses
* maintenance the provider has pending that applies inside the move -
  the next maintenance window counts where the move's length is not yet
  measured
* the source's restore point, the way back, not taken or not ready

What the API does not say is said as not read, never passed. Proved on
stand-ins answering the providers' documented responses; no real account
has been asked (`UNPROVED`).
"""
import datetime
import ipaddress
import socket

#: built and proved on stand-ins only. A plain entry, with its reason in
#: words, until the declaration seam (W17A's reason carrier) takes it
UNPROVED = {
    f"managed.{part}.{who}": f"built; not run against a real {name} account"
    for who, name in (("aws", "AWS"), ("gcp", "Google Cloud"),
                      ("azure", "Azure"))
    for part in ("sign-in", "forwarder", "readiness")}


def _net(cidr):
    try:
        return ipaddress.ip_network(str(cidr), strict=False)
    except ValueError:
        return None


def _overlap(a, b):
    return [(x, y) for x in a for y in b
            if _net(x) and _net(y) and _net(x).overlaps(_net(y))]


def _window_start(window, now):
    """The next start of an `ddd:hh:mm-ddd:hh:mm` window after `now`, or
    None where the window is not in that form."""
    days = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
    try:
        day, hh, mm = window.split("-")[0].split(":")
        d = days.index(day.lower()[:3])
    except (ValueError, AttributeError):
        return None
    at = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
    at += datetime.timedelta(days=(d - now.weekday()) % 7)
    return at if at > now else at + datetime.timedelta(days=7)


def _when(text):
    try:
        got = datetime.datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except ValueError:
        return None
    return got if got.tzinfo else got.replace(tzinfo=datetime.timezone.utc)


def side_gaps(net, side, here=(), now=None, until=None, need=1):
    """[(level, what, detail, next step)] of one side's `Network`.
    `here`: this machine's addresses as the side sees them; `until`: the
    move's forecast end (None: not measured); `need`: the addresses a copy
    or replica of the side takes."""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    out = []
    if net.subnets is not None:
        free = sum(n for _, n in net.subnets)
        full = [s for s, n in net.subnets if n <= 0]
        if free < need:
            out.append(("fail", f"the {side}'s subnets",
                        f"{', '.join(s for s, _ in net.subnets)} have {free}"
                        f" free address(es); a copy or replica takes {need}",
                        "add a subnet with free addresses to the subnet"
                        " group, or free addresses in it"))
        elif full:
            out.append(("warn", f"the {side}'s subnets",
                        f"{', '.join(full)} is full: what is placed there"
                        " goes to the others", "free addresses in it"))
    if net.rules is not None and net.port:
        port = int(net.port)
        on = [r for r in net.rules if r[2] is not None
              and int(r[2]) <= port <= int(r[3])]
        ranges = [r for r in on if not str(r[1]).startswith("group:")]
        mine = [ipaddress.ip_address(a) for a in here]
        let = [r for r in ranges for a in mine
               if _net(r[1]) and a in _net(r[1])]
        if not mine:
            out.append(("warn", f"the {side}'s security groups",
                        f"this machine's address toward it is not known,"
                        f" so whether port {port} is admitted is not told",
                        "run where the side's address resolves"))
        elif not let:
            by = sorted({r[1][6:] for r in on if r not in ranges})
            out.append(("fail" if not by else "warn",
                        f"the {side}'s security groups",
                        f"nothing admits port {port} from"
                        f" {', '.join(map(str, mine))}"
                        + (f"; only members of {', '.join(by)} are, and this"
                           " machine's membership is not read" if by else ""),
                        f"add an inbound rule for port {port} from"
                        f" {mine[0]}/32"
                        + (f" to {sorted({r[0] for r in net.rules})[0]}"
                           if net.rules else "")))
    if net.pending is not None:
        start = _window_start(net.window, now)
        for what, when in net.pending:
            at = _when(when) if when else start
            inside = (at is None or until is None or now <= at <= until
                      or at < now)
            if inside:
                out.append(("fail", f"the {side}'s pending maintenance",
                            f"{what} applies "
                            + (f"at {at.isoformat()}" if at else
                               "at the next maintenance window")
                            + (", inside the move" if until else
                               ", and the move's length is not yet measured"),
                            "apply it before the move, or defer it past the"
                            " move's end"))
    for gap in net.unread:
        out.append(("warn", f"the {side}'s network", f"not read: {gap}",
                    "check it in the provider's console"))
    return out


def restore_gaps(net, side):
    """The way back: a restore point of the source ready before the
    move, where the product keeps them."""
    if net.restore_points is None or net.restore_kind == "point in time":
        return []
    ready = [p for p in net.restore_points if p.ready]
    if ready:
        return [("pass", f"the {side}'s restore point",
                 f"{max(ready, key=lambda p: p.at or '').id} is ready", "")]
    making = [p for p in net.restore_points if not p.ready]
    return [("fail", f"the {side}'s restore point",
             f"{making[0].id} is {making[0].status or 'not ready'}" if making
             else f"no {net.restore_kind} of it is kept",
             "take a restore point of it and wait until it is ready")]


def pair_gaps(src, dst):
    """The two networks: overlapping, or apart and not peered."""
    if not (src.cidrs and dst.cidrs) or src.network == dst.network:
        return []
    both = _overlap(src.cidrs, dst.cidrs)
    if both:
        return [("fail", "the two networks",
                 f"the source's {both[0][0]} and the target's {both[0][1]}"
                 " overlap: they can never be peered or routed",
                 "make the target in a network whose range overlaps none"
                 " of the source's")]
    if dst.network not in src.peers and src.network not in dst.peers:
        return [("warn", "the two networks",
                 "not peered: a way where one server reads the other"
                 " directly is not taken; the copy goes through this"
                 " machine", "peer the two networks to let that way be"
                 " taken")]
    return []


def audit(src, dst, here=None, now=None, until=None, need=1):
    """Every gap of both sides' `Network`s (either None where the side is
    no managed one): [(level, what, detail, next step)]."""
    here = here or {}
    out = []
    for side, net in (("source", src), ("target", dst)):
        if net is not None:
            out += side_gaps(net, side, here.get(side, ()), now, until, need)
    if src is not None:
        out += restore_gaps(src, "source")
    if src is not None and dst is not None:
        out += pair_gaps(src, dst)
    if not any(level != "pass" for level, *_ in out) and (src or dst):
        out.append(("pass", "the cloud side", "networks, subnets, security"
                    " groups, maintenance and restore point all hold", ""))
    return out


def _here(host, port):
    """This machine's address toward `host`: the one its route leaves by,
    found without a packet sent; () where the host does not resolve."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect((host, int(port or 1)))
            return (s.getsockname()[0],)
        finally:
            s.close()
    except (OSError, ValueError):
        return ()


def items(hop, eng):
    """The audit as `ready.Item`s, for `assess`: nothing where neither side
    is a managed one whose network is read here."""
    from . import cloudapi, egress, managed
    from .ready import BEFORE, Item
    nets, here = {}, {}
    for side, which in (("source", "src"), ("target", "dst")):
        try:
            if managed.api_conf(hop, eng, which)[0] is None:
                continue
        except (Exception, SystemExit):  # noqa: BLE001 - not a managed side
            continue
        ok, why = egress.decide(hop, "provider")
        if not ok:
            return [Item("warn", BEFORE, "the cloud side", f"not read: {why}",
                         "let calls to the cloud provider's own API out to"
                         " audit it")]
        try:
            nets[side] = cloudapi.network(managed.adapter(hop, eng, which))
        except (Exception, SystemExit) as e:  # noqa: BLE001 - said
            return [Item("warn", BEFORE, f"the {side}'s network",
                         f"not read: {str(e).splitlines()[0][:200]}", "")]
        ep = hop.source if side == "source" else hop.target
        here[side] = _here(ep.host, ep.port)
    if not nets:
        return []
    return [Item(level, BEFORE, what, detail, fix) for level, what, detail,
            fix in audit(nets.get("source"), nets.get("target"), here)]

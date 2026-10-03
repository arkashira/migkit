"""The whole-database ways of the keyed engines, ranked by what each has
measured on the hop (backlog F7.rshake, F7.mm2).

A Redis keyspace has three ways: migkit's own copier (every value as the
server serialises it, its expiry to the millisecond and how recently or
how often it was read, a cluster node by node, resumable by the scan's
cursor), the replica protocol's copy (`redisshake`: one snapshot of the
whole keyspace as of the moment the source forked, or a scan where the
source refuses a replica; every key written as plain commands into a
target that refuses the source's serialised form) and Redis's own
products' copy where its terms allow. A Kafka cluster has two: migkit's
copier and the cluster mirror Kafka ships (`mirror`). Which is faster
depends on the link, the sizes and the servers, none of which a list can
know - so they are one ladder, `movers._ladder`'s rungs for the engine,
climbed on the hop's own rates, kept after every copy under the name of
the way that made it (`planner.record_rate`): a way never timed is tried
first, in the list's order, and once each has its number the fastest is
taken (`decide`). A way that cannot carry what this hop asks of it is
passed over with the reason, whatever it measured; a copy the copier
stopped part way goes on by the copier, which is the one that resumes.
"""
import dataclasses
import os

#: the engines climbed here, each with its ways out of `movers._ladder`
WAYS = {"redis": ("riotx", "redisshake", "builtin"),
        "kafka": ("mirror", "builtin")}
MEASURED = tuple(WAYS)
#: what each way gives beyond `movers._ladder`'s capabilities for it
GIVES = {
    # migkit's copier: the history, a certificate checked, the position
    # header, and a stop gone on from
    "builtin": frozenset({"access history", "certificate checked",
                          "source position", "resume by chunk"}),
    # verifies the server's certificate (`RIOT_*_TLS_VERIFY`)
    "riotx": frozenset({"certificate checked"}),
    # writes every key as plain commands where the target is older
    "redisshake": frozenset({"into an older server"}),
    # the Java clients check a certificate against the hop's authority
    "mirror": frozenset({"certificate checked"}),
}


def measured(engine):
    """Whether `engine`'s whole-database way is climbed here: one of
    `MEASURED`, and not forced by `MIGKIT_MOVER` for debugging."""
    return engine in MEASURED and not os.environ.get("MIGKIT_MOVER",
                                                     "").strip()


def _unfinished(hop):
    """Whether a copy of this hop by the copier stopped part way: an entry
    in a database's checkpoint that is not done."""
    try:
        root = hop.report_dir()
        paths = list(root.glob("*/move.json"))
    except OSError:
        return False
    from . import checkpoint
    for path in paths:
        got = checkpoint.read_move(path, "cache")
        if got is None:
            continue
        if any(isinstance(v, dict) and v and not v.get("done")
               for v in got.values()):
            return True
    return False


def ladder(hop, engine):
    """The engine's rungs out of `movers._ladder`, each with every need
    it has of this hop and every capability it gives."""
    from . import mirror, movers, redisshake
    extra = {"riotx": movers._FITS.get("riotx", ()),
             "redisshake": redisshake.needs(), "mirror": mirror.needs()}
    out = []
    for r in movers._ladder():
        if r.name not in WAYS[engine]:
            continue
        out.append(dataclasses.replace(
            r, needs=tuple(r.needs) + tuple(extra.get(r.name, ())),
            gives=frozenset(r.gives) | GIVES.get(r.name, frozenset())))
    return tuple(out)


def need(hop, engine):
    """What this hop's whole-database copy must have, read from the
    servers only as far as it matters."""
    from . import redisshake
    out = []
    if _unfinished(hop):
        out.append("resume by chunk")
    if engine == "redis":
        if redisshake.checks_certificates(hop):
            out.append("certificate checked")
        if redisshake.evicts(hop):
            out.append("access history")
        if redisshake.older_target(hop):
            out.append("into an older server")
    if engine == "kafka":
        if any((ep.options or {}).get("ssl_cafile")
               for ep in (hop.source, hop.target)):
            out.append("certificate checked")
        if (hop.options or {}).get("offset_header"):
            out.append("source position")
    return tuple(out)


def facts(hop, engine):
    from . import mirror, movers, redisshake
    from .decide import Facts
    return Facts({"engine": engine, "hop": hop},
                 terms=lambda: movers._riotx_unfit(hop),
                 **redisshake.facts(hop), **mirror.facts(hop))


def choose(hop, engine):
    """The `Choice` of the whole-database way for this hop."""
    from .decide import HopCosts, climb
    return climb(hop.name, ladder(hop, engine), facts(hop, engine),
                 need(hop, engine), HopCosts(hop))


def fitted(hop, engine, via):
    """`movers.fitted` for the engines climbed here: (the way, why the
    copier took it) - the reason said only where the copier was left for
    a way that is here (its engine's and installed: `_ladder`'s first two
    needs), as the caller says it; a way this machine lacks is not news on
    every move. `via` is not read, the whole ladder is."""
    from .decide import _joined
    got = choose(hop, engine)
    if got.rung is None:
        return "builtin", got.reason
    if got.path != "builtin":
        return got.path, None
    facts_of = None
    here = []
    for r, why in got.passed:
        if facts_of is None:
            facts_of = facts(hop, engine)
        if all(n.holds(facts_of) for n in r.needs[:2]):
            here.append(why)
    return "builtin", _joined(here) or None

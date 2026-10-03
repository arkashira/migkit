"""The candidates for one routine's conversion, as data (backlog R11, P0).

Each names what it gives (which kinds of routine, for which pairs), what
it needs (a program built here, a model allowed to be sent code, the
operator's own file), how it is proved (the same differential proof for
every one - a person's version is not trusted more than a program's) and
how it is costed: the share of its versions that proved, per hop, per
pair and per kind of routine, kept beside the hop's reports (`Ledger`).
Each is one of the decision engine's rungs (`decide.Rung`): its needs are
checked and the candidates ranked there (`decide.prune`, `decide.rank`),
with the share of a candidate's versions that did not prove as its cost.

The order they are tried in: the operator's own file first (a person
wrote it for this routine) and a model last (the residue, in a capped
loop); between them a candidate never tried on this hop before one that
was, in the list's order, so each gets measured once, then the one whose
versions proved most often, a tie going to the one that leaves less
behind on the target.
"""
import json

from .. import decide

Need = decide.Need

#: how every candidate is proved: the same way, whoever wrote the version
PROVE = "run on both sides in the sandbox and compared"


class Candidate:
    """A rung, with what makes its version of a routine."""

    def __init__(self, name, said, gives=frozenset(), needs=(), prove=PROVE,
                 footprint=0, make=None, rounds=1):
        gives = frozenset(gives)
        converts = Need(
            "to convert this kind of routine for this pair",
            lambda f: f"{f['src']}->{f['dst']}:{f['kind']}" in gives,
            lambda f: f"{said} does not convert a {f['kind']} from"
                      f" {f['src']} to {f['dst']}")
        self.rung = decide.Rung(name=name, said=said, gives=gives,
                                needs=(converts,) + tuple(needs),
                                footprint=footprint)
        self.prove = prove
        self.make = make
        #: rounds it may be asked for (a model), each fed the last failure
        self.rounds = rounds

    name = property(lambda self: self.rung.name)
    said = property(lambda self: self.rung.said)
    gives = property(lambda self: self.rung.gives)
    needs = property(lambda self: self.rung.needs)
    footprint = property(lambda self: self.rung.footprint)

    def lacks(self, facts):
        """Why this candidate cannot convert the routine, or None."""
        _, out = decide.prune([self.rung], facts)
        return out[0][1] if out else None


KINDS = ("view", "function", "procedure", "trigger", "event")


def _all(*pairs, kinds=KINDS):
    return frozenset(f"{p}:{k}" for p in pairs for k in kinds)


def candidates(pair, routine, ctx):
    """[Candidate] for one routine of this pair, in their listed order."""
    from . import model, programs
    from .convert import EMULATORS, as_written, by_hand, emulate, \
        translator, rules
    src = pair.src_engine.SQL_DIALECT
    dst = pair.dst_engine.SQL_DIALECT
    pg_version = ctx.get("pg_version")
    return [
        Candidate("by-hand", "the operator's own file", _all(f"{src}->{dst}"),
                  (Need("a file of its own", lambda f: f.get("file")
                        is not None, "no file of its own is written for it"),),
                  make=lambda r: by_hand(ctx, r)),
        Candidate("translator", "the query translator",
                  _all(f"{src}->{dst}", kinds=("view", "function")),
                  (Need("one expression or one query",
                        lambda f: f.get("one") is not None,
                        "the query translator carries only a view or a"
                        " function of one expression"),),
                  make=lambda r: translator(pair, ctx, r)),
        Candidate("rules", "migkit's rules",
                  _all("mysql->postgres", "tsql->postgres") |
                  _all("postgres->mysql", "oracle->postgres",
                       kinds=("function", "procedure", "trigger")),
                  make=lambda r: rules(pair, ctx, r)),
        Candidate("emulate", "the target's emulation of Oracle's functions",
                  _all("oracle->postgres", kinds=("view", "function")),
                  (Need("an emulation the target runs",
                        lambda f: "orafce" in (f.get("emulations") or ()),
                        "the target runs no emulation of Oracle's"
                        " functions"),
                   Need("one expression or one query",
                        lambda f: f.get("one") is not None,
                        "an emulation carries only a view or a function of"
                        " one expression")),
                  footprint=1, make=lambda r: emulate(pair, ctx, r)),
        Candidate("emulator", "the target's own emulation of the source's"
                  " engine",
                  _all("oracle->mysql", "oracle->postgres", "tsql->postgres",
                       "mysql->postgres", kinds=("function", "procedure")),
                  (Need("a target that runs the source's own code",
                        lambda f: any(EMULATORS.get(m, (None,))[0] ==
                                      f["src"] for m in
                                      f.get("emulating") or ()),
                        "the target runs no emulation of the source's"
                        " engine (MariaDB's Oracle mode, IvorySQL,"
                        " openGauss, Babelfish, openHalo)"),),
                  footprint=1, make=lambda r: as_written(pair, ctx, r)),
        Candidate("plsql-converter", "the PL/SQL converter",
                  _all("oracle->postgres", "mysql->postgres",
                       "tsql->postgres",
                       kinds=("function", "procedure", "trigger", "view")),
                  (Need("the PL/SQL converter built here",
                        lambda f: programs.available("ora2pg"),
                        "the PL/SQL converter is not built here (migkit"
                        " doctor --install)"),),
                  make=lambda r: programs.ora2pg(r, src, dst, pg_version)),
        Candidate("dialect-converter", "the multi-dialect converter",
                  _all("oracle->postgres", "mysql->postgres",
                       "tsql->postgres", "postgres->mysql", "tsql->mysql",
                       "oracle->mysql",
                       kinds=("function", "procedure", "trigger", "view")),
                  (Need("the multi-dialect converter built here",
                        lambda f: programs.available("sqlines"),
                        "the multi-dialect converter is not built here"
                        " (migkit doctor --install)"),),
                  make=lambda r: programs.sqlines(r, src, dst)),
        Candidate("model", "a model", _all(f"{src}->{dst}"),
                  (Need("a model allowed to be sent code",
                        lambda f: model.configured(pair.hop),
                        "no model is allowed to be sent code (MIGKIT_AI and"
                        " MIGKIT_AI_SHARE=code)"),),
                  rounds=model.rounds(pair.hop)),
    ]


class Ledger:
    """How often each candidate's versions proved, per hop: the cost the
    ranking reads, kept beside the hop's reports."""

    def __init__(self, path):
        self.path = path
        try:
            self.data = json.loads(path.read_text()) if path and \
                path.exists() else {}
        except (OSError, ValueError):
            self.data = {}

    def key(self, facts):
        return f"{facts['src']}->{facts['dst']}:{facts['kind']}"

    def rate(self, name, facts):
        got = self.data.get(self.key(facts), {}).get(name)
        return (got["proved"], got["tried"]) if got else None

    def record(self, name, facts, proved):
        one = self.data.setdefault(self.key(facts), {}).setdefault(
            name, {"tried": 0, "proved": 0})
        one["tried"] += 1
        one["proved"] += 1 if proved else 0

    def save(self):
        if self.path:
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.data, indent=1, sort_keys=True))
            tmp.replace(self.path)


def ranked(cands, ledger, facts):
    """The candidates in the order they are tried (see the module's
    docstring), with the rest said: [(candidate, why it cannot)]."""
    by = {c.name: c for c in cands}
    kept, out = decide.prune([c.rung for c in cands], facts)
    costs = {}
    for r in kept:
        got = ledger.rate(r.name, facts)
        if got and got[1]:
            costs[r.name] = 1 - got[0] / got[1]
    ends = ("by-hand", "model")
    middle = decide.rank([r for r in kept if r.name not in ends], costs)
    order = [r for r in kept if r.name == "by-hand"] + list(middle) + \
        [r for r in kept if r.name == "model"]
    return [by[r.name] for r in order], [(by[r.name], why)
                                         for r, why in out]


def as_data(cands, ledger, facts):
    """The candidates as plain data, for a ranking made elsewhere (the
    decision engine's rungs): what each gives and needs, whether it lacks
    anything for these facts, how it is proved, and what it has cost on
    this hop - the share of its versions that proved."""
    out = []
    for c in cands:
        got = ledger.rate(c.name, facts)
        out.append({"name": c.name, "said": c.said,
                    "gives": sorted(c.gives),
                    "needs": [n.what for n in c.needs[1:]],
                    "lacks": c.lacks(facts), "prove": c.prove,
                    "cost": {"proved": got[0], "tried": got[1]} if got
                    else None,
                    "footprint": c.footprint, "rounds": c.rounds})
    return out

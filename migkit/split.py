"""One database into many (backlog P0-SCENARIOS): a service extracted, a
database split by region or by customer - every row placed by rule, each
exactly once, and proved.

    hops:
      shop-eu:
        goal: split
        source: postgres://app@10.0.0.1/shop
        target: postgres://app@10.0.0.2/shop
        exclude: [shipments]              # the tables this part leaves
        mapping:
          where: {customers: "region = 'eu'", orders: "region = 'eu'"}
        options:
          split:
            with: [shop-us]               # the other parts of the same source
            shared: [countries]           # tables each part takes whole
            rest: stays                   # rows no part takes stay behind
      shop-us:
        goal: split
        ...

**The rule is each part's own**: the tables a part carries are its hop's
(`exclude`), the rows of each are its row filter (`mapping.where`) - the
one place a row is placed by, the same the copy and the check apply - so
a split is its parts, each moved and checked as any hop is.

**Every row placed exactly once, before the move** (`placement`): for
each table of the source, every row counted against every part's rule in
one statement on the source - how many rows no part takes, how many more
than one - with a few of each by key; a table no part takes is all of it
placed nowhere. Placed nowhere is refused unless the hop says the rest
stays behind (`rest: stays`: a service extracted leaves the source the
rest); placed twice is refused unless the table is one every part takes
whole on purpose (`shared`).

**Proved after it** (`prove`): each part's target read by key, table by
table - no key in two parts' targets, and the targets' keys together the
source's keys each rule places; each part's rows as the source's are its
own check's.

**References across the split, said** (`crossing`): for each foreign
key of the source, the rows a part takes that name a row the same part
does not take - a reference the target cannot hold, counted by key and
part.
"""
from dataclasses import dataclass

GOAL = "split"
OPTION = "split"
EXAMPLES = 3


def _last(e):
    return (str(e).strip().splitlines() or [type(e).__name__])[-1][:160]


def settings(hop):
    """{"with", "shared", "rest"} of a hop whose goal is split; None for any
    other."""
    if (getattr(hop, "goal", "") or "") != GOAL:
        return None
    raw = (hop.options or {}).get("split") or {}
    if not isinstance(raw, dict):
        raise SystemExit("split: is a mapping - with, shared, rest")
    rest = str(raw.get("rest") or "refuse").lower()
    if rest not in ("refuse", "stays"):
        raise SystemExit(f"split: rest: {rest} - refuse, or stays")
    return {"with": [str(h) for h in raw.get("with") or []],
            "shared": [str(t) for t in raw.get("shared") or []],
            "rest": rest}


@dataclass(frozen=True)
class Part:
    """One part of the split: a hop."""
    hop: object

    @property
    def name(self):
        return self.hop.name

    def carries(self, db, table):
        return self.hop.takes_table(db, table)

    def rule(self, db, table):
        return self.hop.row_filter(db, *str(table).split("."))


def parts(hop, siblings=None):
    """[Part] of the split, this hop's first; SystemExit naming every
    reason the parts do not hold together."""
    from . import config
    got = settings(hop)
    out, wrong = [Part(hop)], []
    if got["with"]:
        try:
            have = siblings if siblings is not None else config.load_hops()
        except SystemExit as e:
            have = {}
            wrong.append(f"the hops it names could not be read: {e}")
        for name in got["with"]:
            other = have.get(name)
            if other is None:
                wrong.append(f"with: {name} is not a hop the configuration"
                             " has")
                continue
            if settings(other) is None:
                wrong.append(f"with: {name}'s goal is not split")
                continue
            if (other.source.host, other.source.port) != (
                    hop.source.host, hop.source.port) or not (
                    set(other.databases or []) & set(hop.databases or [])
                    or not (other.databases or hop.databases)):
                wrong.append(f"with: {name} reads another source")
                continue
            out.append(Part(other))
    if wrong:
        raise SystemExit(f"{hop.name}: the split does not hold together - "
                         + "; ".join(wrong))
    return out


def _pair(eng):
    from .engines.hetero import HeteroEngine
    return eng if isinstance(eng, HeteroEngine) else eng._as_pair()


def _engine(part, eng):
    if part.hop is getattr(eng, "hop", None):
        return eng
    from .engines import get_engine
    return get_engine(part.hop)


def _shared(hop, table):
    leaf = str(table).split(".")[-1]
    return any(s in (str(table), leaf) for s in settings(hop)["shared"])


def _taken(parts_, db, table):
    """The SQL counting how many parts take a row of `table`: 1 for a part
    taking it whole, its rule for one taking some of it, nothing for one
    that leaves it."""
    terms = []
    for p in parts_:
        if not p.carries(db, table):
            continue
        rule = p.rule(db, table)
        terms.append(f"(case when ({rule}) then 1 else 0 end)" if rule
                     else "1")
    return " + ".join(terms) if terms else "0"


@dataclass
class Placed:
    table: str
    rows: int
    nowhere: int
    twice: int
    shown_nowhere: list
    shown_twice: list
    shared: bool


def placement(hop, eng, db, parts_=None):
    """[Placed] of every table of the source: each row counted against
    every part's rule in one statement on the source (read only)."""
    from . import canon
    parts_ = parts_ or parts(hop)
    src = _pair(eng).src_engine
    stays = settings(hop)["rest"] == "stays"
    out = []
    for t in src.neutral_tables("src", db):
        if stays and not any(p.carries(db, t) for p in parts_):
            # left on the source by every part, and the rest stays there
            continue
        n = _taken(parts_, db, t)
        q = src._qualified("src", db, t)
        rows, nowhere, twice = src.run_rule(
            "src", db, f"select count(*), coalesce(sum(case when n = 0 then 1"
                       f" else 0 end), 0), coalesce(sum(case when n > 1 then"
                       f" 1 else 0 end), 0) from (select {n} as n from {q})"
                       " x")[0]
        key = list(src.neutral_key("src", db, t) or [])
        cls = {c: canon.type_class(src.CANON_ENGINE, typ)
               for c, typ in src.neutral_columns("src", db, t)}

        def shown(pred, n=n, t=t, key=key, cls=cls):
            if not key:
                return []
            got, _ = src.neutral_read("src", db, t,
                                      [(k, cls.get(k)) for k in key], None,
                                      EXAMPLES, where=f"({n}) {pred}")
            return [tuple(r) for r in got[:EXAMPLES]]
        out.append(Placed(str(t), int(rows or 0), int(nowhere or 0),
                          int(twice or 0),
                          shown("= 0") if nowhere else [],
                          shown("> 1") if twice else [],
                          _shared(hop, t)))
    return out


def crossing(hop, eng, db, parts_=None):
    """[(part, child, [cols], parent, rows)] of each foreign key of the
    source whose rows a part takes name a row the same part does not
    take."""
    parts_ = parts_ or parts(hop)
    src = _pair(eng).src_engine
    q = src._quote_ident
    out = []
    for child, cc, parent, pc in src.foreign_keys("src", db):
        for p in parts_:
            if not p.carries(db, child):
                continue
            cr = p.rule(db, child)
            filled = " and ".join(f"c.{q(a)} is not null" for a in cc)
            if p.carries(db, parent):
                pr = p.rule(db, parent)
                on = " and ".join(f"x.{q(b)} = c.{q(a)}"
                                  for a, b in zip(cc, pc))
                inner = (f"select 1 from {src._qualified('src', db, parent)}"
                         f" x where {on}" + (f" and ({pr})" if pr else ""))
                cond = f"{filled} and not exists ({inner})"
            else:
                cond = filled
            sql = (f"select count(*) from {src._qualified('src', db, child)}"
                   f" c where {cond}" + (f" and ({cr})" if cr else ""))
            n = int(src.run_rule("src", db, sql)[0][0] or 0)
            if n:
                out.append((p.name, child, list(cc), parent, n))
    return out


def problems(hop, eng, db):
    """[what stops this split before a row is written]: rows no part takes
    where the rest does not stay, rows more than one part takes of a
    table not shared."""
    try:
        parts_ = parts(hop)
        placed = placement(hop, eng, db, parts_)
    except SystemExit as e:
        return [str(e)]
    except Exception as e:  # noqa: BLE001 - asked of the source, said
        return [f"{db}: where each row goes could not be counted on the"
                f" source: {_last(e)}"]
    got = settings(hop)
    wrong = []
    for p in placed:
        if p.nowhere and got["rest"] != "stays":
            wrong.append(f"{p.table}: {p.nowhere:,} of its {p.rows:,} rows"
                         " no part takes"
                         + (f" (by key: {_keys(p.shown_nowhere)})"
                            if p.shown_nowhere else "")
                         + " - give them a part, or split: {rest: stays}")
        if p.twice and not p.shared:
            wrong.append(f"{p.table}: {p.twice:,} rows more than one part"
                         " takes" + (f" (by key: {_keys(p.shown_twice)})"
                                     if p.shown_twice else "")
                         + " - make the rules exclusive, or split: {shared:"
                           f" [{p.table}]}} where each part is to hold it"
                           " whole")
    return wrong


def _keys(ks):
    return ", ".join("(" + ", ".join(map(str, k)) + ")" for k in ks)


def rows(hop, eng, db):
    """assess rows of the split: where every row goes, and the references
    across it."""
    out = []
    try:
        parts_ = parts(hop)
        placed = placement(hop, eng, db, parts_)
    except Exception as e:  # noqa: BLE001 - the gate says it by problems
        return [{"level": "warn", "scope": "the split", "item": db,
                 "detail": f"could not be counted: {_last(e)}"}]
    total = sum(p.rows for p in placed)
    out.append({"level": "pass", "scope": "the split", "item": db,
                "detail": f"{len(parts_)} parts ({', '.join(p.name for p in parts_)}):"
                          f" {total:,} rows in {len(placed)} tables counted"
                          " against every part's rule"})
    try:
        for part, child, cc, parent, n in crossing(hop, eng, db, parts_):
            out.append({"level": "warn", "scope": "the split",
                        "item": f"{db}: {child} -> {parent} in {part}",
                        "detail": f"{n:,} rows of {child} {part} takes name"
                                  f" by ({', '.join(cc)}) a row of {parent}"
                                  f" {part} does not take: a key across the"
                                  " split, which the target cannot hold"
                                  "  [fix] take the rows they name into the"
                                  " same part, or drop the key there and"
                                  " hold the reference in the application"})
    except Exception as e:  # noqa: BLE001 - unknown, said so
        out.append({"level": "warn", "scope": "the split",
                    "item": f"{db}: references",
                    "detail": f"could not be counted: {_last(e)} - unknown,"
                              " not clean"})
    return out


def prove(hop, eng, db):
    """[Result] of the split, read on the parts' targets: no key of a table
    in two parts' targets (a table not shared), and the targets' keys
    together each source key a rule places - no more, no fewer."""
    from . import canon
    from .engines.base import Result
    try:
        parts_ = parts(hop)
    except SystemExit as e:
        return [Result("split", db, "error", str(e))]
    src = _pair(eng).src_engine
    out = []
    for t in src.neutral_tables("src", db):
        carriers = [p for p in parts_ if p.carries(db, t)]
        if not carriers:
            continue
        key = list(src.neutral_key("src", db, t) or [])
        if not key:
            out.append(Result("split", f"{db}.{t}", "skip",
                              "no key to place its rows by: each part's own"
                              " check holds its rows"))
            continue
        cls = {n: canon.type_class(src.CANON_ENGINE, typ)
               for n, typ in src.neutral_columns("src", db, t)}
        read = [(k, cls.get(k)) for k in key]
        n = _taken(parts_, db, t)
        want = set()
        for rs in src._every_row("src", db, t, read, f"({n}) > 0"):
            for r in rs:
                want.add(tuple(canon.render_value(cls.get(k), v)
                               for k, v in zip(key, r)))
        where_, twice, missing_part = {}, 0, []
        shared = _shared(hop, t)
        for p in carriers:
            pair = _pair(_engine(p, eng))
            dst = pair.dst_engine
            there = [] if dst.target_missing(db) else \
                dst.neutral_tables("dst", db)
            got, _, _, _ = pair.match_tables([t], there, pair._renamer(db))
            if not got:
                missing_part.append(p.name)
                continue
            dt = got[0][1]
            dcls = {n: canon.type_class(dst.CANON_ENGINE, typ)
                    for n, typ in dst.neutral_columns("dst", db, dt)}
            for rs in dst._every_row("dst", db, dt,
                                     [(k, dcls.get(k)) for k in key]):
                for r in rs:
                    k = tuple(canon.render_value(dcls.get(c), v)
                              for c, v in zip(key, r))
                    if k in where_ and not shared:
                        twice += 1
                    where_.setdefault(k, p.name)
        have = set(where_)
        lost = want - have
        more = have - want
        said = []
        if missing_part:
            said.append(f"not on the target of {', '.join(missing_part)}")
        if twice:
            said.append(f"{twice:,} keys in more than one part's target")
        if lost:
            said.append(f"{len(lost):,} rows a rule places are in no part's"
                        f" target ({_keys(sorted(lost)[:EXAMPLES])})")
        if more:
            said.append(f"{len(more):,} keys in a part's target no rule"
                        f" places ({_keys(sorted(more)[:EXAMPLES])})")
        if said:
            out.append(Result("split", f"{db}.{t}", "diff", "; ".join(said),
                              "", f"migkit move {hop.name} --mode full --go"))
        else:
            out.append(Result("split", f"{db}.{t}", "ok",
                              f"{len(want):,} rows, each in "
                              + ("every part's target (shared)" if shared
                                 else "exactly one part's target")
                              + f" of {len(carriers)}"))
    return out


def plan_lines(hop, eng, db):
    """The plan's line of a split, or []."""
    got = settings(hop)
    if got is None or not db:
        return []
    try:
        parts_ = parts(hop)
    except SystemExit as e:
        return [f"  a split that does not hold together: {e}"]
    return [f"  a split: {db} into {len(parts_)} parts"
            f" ({', '.join(p.name for p in parts_)}), each row by its"
            " part's rule (mapping.where), counted before the move and"
            " proved exactly once after it"
            + ("; the rows no part takes stay on the source"
               if got["rest"] == "stays" else "")]

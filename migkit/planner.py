"""Which way each table goes, decided from facts about it, and why.

A move used to be one decision per engine: the fastest program installed
took the whole database. The decisions that correctness forces were made
in three places - what the hop excludes, which filtered tables the bulk
copy cannot carry - and nothing said them per table. They are made here,
once, each with its reason, and the same decisions drive the dry run, the
copy and the table copier that follows it.

The facts behind them come from the source's own catalogue in one query
per database (`table_facts`): an estimate of the rows, and whether there
is a key. Rules that choose on speed wait for measurements to choose on
(backlog 28); the ones here are the ones a wrong answer would make
incorrect, not slow.
"""
from dataclasses import dataclass

BULK = "bulk"
COPIER = "copier"
LEFT = "left alone"

#: what each path is called where an operator reads it
WORDS = {BULK: "bulk copy", COPIER: "table by table", LEFT: "left alone"}


@dataclass(frozen=True)
class Decision:
    table: str
    path: str
    reason: str

    def __str__(self):
        return f"{self.table}: {WORDS[self.path]} - {self.reason}"


def _name(parts, qualifier):
    if len(parts) > 1 or not qualifier:
        return ".".join(parts)
    return f"{qualifier}.{parts[0]}"


def _about(fact):
    """The facts worth a clause, in words."""
    if not fact:
        return ""
    said = []
    rows = fact.get("rows")
    if rows is not None:
        said.append(f"about {int(rows):,} rows")
    if fact.get("key") is False:
        said.append("no key")
    return f" ({', '.join(said)})" if said else ""


def plan(hop, db, via, tables, qualifier="public", facts=None):
    """One `Decision` per table, in name order.

    `via` is the bulk path the database goes by; a table it cannot carry
    correctly goes table by table instead. `facts` is `table_facts`'
    answer, keyed like `tables`, or None where the engine gives none.
    """
    from . import movers
    facts = facts or {}
    filters_here = via not in movers.ROW_FILTER_MOVERS and bool(
        movers._filtered_here(hop, db))
    out = []
    for ident in sorted(tables):
        parts = [p for p in str(ident).split(".") if p]
        name = _name(parts, qualifier)
        about = _about(facts.get(ident) or facts.get(name))
        if hop.excluded(db, *parts):
            out.append(Decision(name, LEFT, "the hop excludes it"))
        elif filters_here and hop.row_filter(db, *parts):
            out.append(Decision(name, COPIER,
                                "its row filter is applied on both sides,"
                                " which the bulk copy cannot do" + about))
        else:
            out.append(Decision(name, BULK, "the fastest path here" + about))
    return out


def lines(decisions, limit=20):
    """The plan as an operator reads it: every table off the usual path,
    then as many of the rest as fit, then a count."""
    unusual = [d for d in decisions if d.path != BULK]
    usual = [d for d in decisions if d.path == BULK]
    shown = unusual + usual[:max(limit - len(unusual), 0)]
    out = [f"  {d}" for d in shown]
    hidden = len(decisions) - len(shown)
    if hidden:
        out.append(f"  ... and {hidden:,} more by bulk copy")
    return out

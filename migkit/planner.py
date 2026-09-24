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
        elif hop.column_rules(db, *parts):
            out.append(Decision(name, COPIER,
                                "its columns are mapped - kept, dropped or"
                                " renamed - which the bulk copy cannot do"
                                + about))
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


# --- how long it will take, from how long it took ---------------------------
# No throughput is assumed. Each finished copy records the rows it carried
# and the time it took, per path, for this hop; the next plan divides the
# rows it is about to carry by the latest rate measured on the same path.
# With no measurement yet, it says so rather than guessing.

def _rates_path(hop):
    return hop.report_dir() / "throughput.json"


def record_rate(hop, path, rows, seconds):
    """Remember one finished copy: `rows` in `seconds` by `path`."""
    import json
    import time
    if not rows or seconds <= 0:
        return
    f = _rates_path(hop)
    try:
        got = json.loads(f.read_text())
    except (OSError, ValueError):
        got = {}
    runs = got.setdefault(path, [])
    runs.append({"rows": int(rows), "seconds": round(seconds, 3),
                 "at": time.strftime("%Y-%m-%dT%H:%M:%S")})
    del runs[:-10]
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(json.dumps(got, indent=1))


def measured_rate(hop, path):
    """Rows per second the latest copy on `path` achieved, or None."""
    import json
    try:
        runs = json.loads(_rates_path(hop).read_text()).get(path) or []
    except (OSError, ValueError):
        return None
    last = runs[-1] if runs else None
    return last["rows"] / last["seconds"] if last else None


def estimate(hop, path, rows):
    """The plan's line about time, in words."""
    if not rows:
        return "no row count to estimate from"
    rate = measured_rate(hop, path)
    if not rate:
        return (f"about {rows:,} rows; no copy on this path measured yet,"
                " so no time is estimated")
    secs = rows / rate
    took = (f"{secs / 3600:,.1f}h" if secs >= 3600 else
            f"{secs / 60:,.0f}min" if secs >= 60 else f"{secs:,.0f}s")
    return (f"about {rows:,} rows, about {took} at the {rate:,.0f} rows/s"
            " measured on this path last time")


# --- how much it carries, and the room it needs -----------------------------
# What the target's log grows by while a load runs, per byte loaded, as
# measured: PostgreSQL wrote 79 MB of WAL loading 50 MB of table and 28 MB
# of index (indexes rebuilt after the rows); MySQL wrote a 42.8 MB binlog
# loading 52 MB of table data, where the target keeps a binlog.
LOG_PER_BYTE = {"postgres": ("its WAL", 1.0, "tables and indexes"),
                "mysql": ("its binlog (where it keeps one)", 0.8,
                          "table data")}


def _size(n):
    from .wording import human_bytes
    return human_bytes(n)


def size_line(decisions, facts, engine, price_per_gb=None):
    """The plan's line about size, from the source's catalogue: what the
    move carries and what the target needs for it. None where the
    catalogue gives no sizes.

    `price_per_gb` is what the hop says a gigabyte costs to carry across
    its network path (hop option `transfer_price_per_gb`). migkit cannot
    read a price from anywhere, so without one no cost is said. What
    crosses is the tables' rows; the indexes are built on the target."""
    carried = {d.table for d in decisions if d.path != LEFT}
    data = index = 0
    known = False
    for name, fact in (facts or {}).items():
        if not isinstance(fact, dict) or fact.get("bytes") is None:
            continue
        if name not in carried and not any(
                str(t).endswith("." + str(name)) or str(name).endswith(
                    "." + str(t)) for t in carried):
            continue
        known = True
        data += int(fact.get("bytes") or 0)
        index += int(fact.get("index_bytes") or 0)
    if not known:
        return None
    line = (f"about {_size(data + index)} to carry ({_size(data)} of"
            f" tables, {_size(index)} of indexes built on the target); the"
            " target needs that much room")
    log = LOG_PER_BYTE.get(engine)
    if log:
        what, per, base = log
        grows = (data + index if base == "tables and indexes" else data) * per
        line += (f", and {what} grows by about {_size(int(grows))} while"
                 " it loads")
    if price_per_gb is not None:
        try:
            price = float(price_per_gb)
        except (TypeError, ValueError):
            return line + (f"; transfer_price_per_gb={price_per_gb!r} is not"
                           " a number, so no cost is said")
        cost = data / 2 ** 30 * price
        line += (f"; carrying the tables' {_size(data)} across costs about"
                 f" {cost:,.2f} at the {price:g} per GB the hop gives")
    return line

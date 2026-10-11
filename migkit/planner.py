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
incorrect, not slow. Each table's choice is a climb of `ways` in
`decide`, the one way every choice migkit makes is decided.
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
    #: the table ladder's rung, where the reason is its (`tableway`)
    way: str = ""

    def __str__(self):
        return f"{self.table}: {WORDS[self.path]} - {self.reason}"


def _name(ident, parts, qualifier):
    if len(parts) > 1 or not qualifier:
        return str(ident)
    return f"{qualifier}.{ident}"


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


def ways(via):
    """The ways a table can go when its database goes by `via`, in the
    order a table takes them (`decide`): the bulk copy where it carries
    everything the table asks, the table copier where it does not, and
    nothing for a table the hop leaves out."""
    from .decide import Rung
    from .movers import (MASKING_MOVERS, NEEDS_KEYS, RENAMING_MOVERS,
                         ROW_FILTER_MOVERS)
    bulk = {"carried"} | ({"row filter"} if via in ROW_FILTER_MOVERS
                          else set()) | ({"keyless"} if via not in NEEDS_KEYS
                                         else set()) | (
        {"renamed"} if via in RENAMING_MOVERS else set()) | (
        {MASKED} if via in MASKING_MOVERS else set())
    # the table copier writes a masked table through the hop's transforms,
    # every one of them (`Engine.through_pair`); the dump only those it
    # says itself (`movers.DUMP_MASKS`)
    return (Rung(BULK, "the bulk copy", gives=frozenset(bulk),
                 because="the fastest path here"),
            Rung(COPIER, "the table copier",
                 gives=frozenset({"carried", "row filter",
                                  "column mapping", "keyless", "renamed",
                                  MASKED, HASHED})),
            Rung(LEFT, WORDS[LEFT], gives=frozenset({LEFT})))


#: what a masked table asks of its way: masked on the way, and - where a
#: transform is a hash or a cut - one that takes those too
MASKED, HASHED = "masked on the way", "hashed or cut on the way"


def masking_needs(hop, db, parts):
    """What the hop's transforms of this table (`Hop.masks`) ask of the way
    it goes."""
    from .movers import dump_masks
    got = hop.masks(db, *parts) if callable(getattr(hop, "masks", None)) \
        else {}
    if not got:
        return ()
    return (MASKED,) if dump_masks(got) else (MASKED, HASHED)


def plan(hop, db, via, tables, qualifier="public", facts=None, choice=None,
         roots=None):
    """One `Decision` per table, in name order.

    `via` is the bulk path the database goes by; a table it cannot carry
    correctly goes table by table instead. `facts` is `table_facts`'
    answer, keyed like `tables`, or None where the engine gives none.
    What a table asks - to be left alone, its row filter applied, its
    columns mapped, a name of its own on the target - is climbed against
    `ways(via)`. `roots` is {partition: its root} of a PostgreSQL source
    (`movers._pg_roots`): a partition asks what its root asks, the name
    the hop's rules are written in, where it used to ask with its own and
    match none of them.

    Where the engine's table ladder has the bulk path's programs among its
    rungs (`tableway`), a table the bulk copy carries goes the way that
    ladder gives it, from measured rates (`tableway.choose`, or `choice`
    where the caller has it): a table measured faster table by table goes
    that way. The decision says how long it is expected to take; which
    way and why is the decision record's (`decide.record`).
    """
    from . import movers, tableway
    from .decide import climb
    if choice is None:
        choice = tableway.for_plan(hop, db, via, tables)
    choice = choice or {}
    facts = facts or {}
    filtered = bool(movers._filtered_here(hop, db))
    rungs = ways(via)
    out = []
    for ident in sorted(tables):
        parts = movers._asked(hop, ident)
        # every question the hop answers is asked with the root's name
        asked = movers._asked(hop, ident, roots)
        name = _name(ident, parts, qualifier)
        fact = facts.get(ident) or facts.get(name) or {}
        about = _about(fact)
        if hop.excluded(db, *asked):
            need = (LEFT,)
        else:
            need = ("carried",) + (
                ("row filter",) if filtered and hop.row_filter(db, *asked)
                else ()) + (("column mapping",)
                            if hop.column_rules(db, *asked) else ()) + (
                ("keyless",) if fact.get("key") is False else ()) + (
                ("renamed",) if movers._renamed(hop, db, asked) else ()) + (
                masking_needs(hop, db, asked))
        got = climb(name, rungs, need=need)
        mine = (choice.get(name) or choice.get(parts[-1])
                if got.path == BULK else None)
        if mine is not None and mine.rung:
            # the time it takes on the console; its way and why are the
            # decision record's (`tableway.said`)
            out.append(Decision(name, BULK if tableway.by_the_move(
                hop, mine.rung) else COPIER, tableway.outcome(mine) + about,
                mine.rung))
            continue
        out.append(Decision(name, got.path,
                            got.reason + ("" if got.path == LEFT else about)))
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


def _kind(hop):
    """What makes one hop's runs a guide to another's: the engines on
    each side."""
    opts = hop.options or {}
    if hop.engine == "hetero":
        return f"{opts.get('source_engine')}->{opts.get('target_engine')}"
    return hop.engine


def record_rate(hop, path, rows, seconds, size=None, cpu_seconds=None,
                cpus=None):
    """Remember one finished copy: `rows` in `seconds` by `path` - and,
    where the caller knows them, the bytes it carried and the CPU seconds
    this machine spent on it out of its `cpus`, which is what tells the
    next plan whether this machine or something beyond it held the copy
    back (`machine.stages_for`)."""
    import time
    if not rows or seconds <= 0:
        return
    run = {"rows": int(rows), "seconds": round(seconds, 3),
           "at": time.strftime("%Y-%m-%dT%H:%M:%S"), "kind": _kind(hop)}
    if size:
        run["bytes"] = int(size)
    if cpu_seconds is not None and cpus:
        run["cpu_seconds"] = round(float(cpu_seconds), 3)
        run["cpus"] = int(cpus)
    _add_run(hop, path, run)


def _add_run(hop, key, run):
    """One run kept under `key` in the hop's throughput file, the last ten
    of each: read, added to and written back under the file's lock, since
    the databases of one move finish their copies side by side."""
    from . import durable

    def add(got):
        runs = got.setdefault(key, [])
        runs.append(run)
        del runs[:-10]
    durable.change_json(_rates_path(hop), add, indent=1)


def _runs(path_file, path):
    import json
    try:
        return json.loads(path_file.read_text()).get(path) or []
    except (OSError, ValueError):
        return []


def runs(hop, path):
    """The copies kept for `path` on this hop, oldest first."""
    return _runs(_rates_path(hop), path)


def measured_rate(hop, path):
    """Rows per second the latest copy on `path` achieved, or None."""
    runs = _runs(_rates_path(hop), path)
    last = runs[-1] if runs else None
    # a run kept to the millisecond can read 0.0: no rate, not a division
    return last["rows"] / last["seconds"] if last and last["seconds"] \
        else None


def _elsewhere(hop, path):
    """([run], [hop name]) from the other hops of the same kind on the same
    path: the runs of this hop's own rehearsal where it has one on the path
    (`rehearsal.of`), else every other hop's of the same engines."""
    from . import config, rehearsal
    rec = rehearsal.of(hop)
    if rec is not None:
        got = [r for r in _runs(rec["dir"] / "throughput.json", path)
               if r.get("kind") in (None, _kind(hop))]
        if got:
            return got, [f"the rehearsal {rec['name']}"]
    kind, runs, names = _kind(hop), [], []
    try:
        dirs = sorted(p for p in config.REPORTS.iterdir() if p.is_dir())
    except OSError:
        return [], []
    for d in dirs:
        if d.name == hop.name:
            continue
        got = [r for r in _runs(d / "throughput.json", path)
               if r.get("kind") == kind]
        if got:
            runs += got
            names.append(d.name)
    return runs, names


def record_variant(hop, path, way, carried, seconds, stream, tables=None):
    """Remember one finished copy on `path` done one `way` ("whole",
    "split"): `carried` bytes in `seconds`, and `stream`, the bytes a
    second one of its streams carried - with what each table took, as the
    copy's own catalogue says. What the next run chooses its way and sizes
    its parts from."""
    import time
    if not carried or not seconds or seconds <= 0:
        return
    _add_run(hop, f"{path}:{way}", {
        "bytes": int(carried), "seconds": round(seconds, 3),
        "stream": round(stream, 1) if stream else None,
        "tables": tables or {},
        "at": time.strftime("%Y-%m-%dT%H:%M:%S"), "kind": _kind(hop)})


def variants(hop, path):
    """{way: {"rate": bytes a second, "stream": bytes a second one stream
    carried, "whose": [hop] or None}} from the latest copy each way on
    `path` - this hop's own, else the latest of the other hops of the same
    engines (a rehearsal's), named."""
    out = {}
    for way in ("whole", "split"):
        key = f"{path}:{way}"
        runs, whose = _runs(_rates_path(hop), key), None
        if not runs:
            runs, whose = _elsewhere(hop, key)
        if not runs:
            continue
        last = runs[-1]
        out[way] = {"rate": last["bytes"] / last["seconds"],
                    "stream": last.get("stream"),
                    "whose": whose[-1:] if whose else None}
    return out


def _took(secs):
    return (f"{secs / 3600:,.1f}h" if secs >= 3600 else
            f"{secs / 60:,.0f}min" if secs >= 60 else f"{secs:,.0f}s")


def estimate(hop, path, rows, narrowed=()):
    """The plan's line about time, in words.

    A range from every run kept on the path, and which runs those were -
    one run is one number and is called that. A hop that has run nothing
    yet is given the runs of other hops of the same engines on the same
    path - a rehearsal's, typically - and told whose they are. Nothing
    measured, nothing estimated. `narrowed`: the tables of `rows` a row
    filter narrows, counted whole - the rows and the time are then the
    most, and said so."""
    if not rows:
        return "no row count to estimate from"
    said = _estimate(hop, path, rows)
    if not narrowed:
        return said
    return (f"at most - {len(narrowed)} tables a row filter narrows are"
            f" counted whole: {said}")


def _estimate(hop, path, rows):
    """`estimate`'s words for `rows`, from the runs kept on `path`."""
    runs, whose = _runs(_rates_path(hop), path), None
    if not runs:
        runs, others = _elsewhere(hop, path)
        whose = ", ".join(others[:3]) + (" ..." if len(others) > 3 else "")
    if not runs:
        return (f"about {rows:,} rows; no copy on this path measured yet,"
                " so no time is estimated")
    rates = sorted(r["rows"] / r["seconds"] for r in runs)
    when = sorted(r.get("at", "")[:10] for r in runs)
    where = f"this path by {whose}" if whose else "this path"
    if len(runs) == 1:
        return (f"about {rows:,} rows, about {_took(rows / rates[0])} at the"
                f" {rates[0]:,.0f} rows/s measured on {where}"
                + (" last time" if not whose else "")
                + f" ({when[0]}) - one run, so one number and not a range")
    return (f"about {rows:,} rows, between {_took(rows / rates[-1])} and"
            f" {_took(rows / rates[0])} going by the {len(runs)} runs on"
            f" {where} ({rates[0]:,.0f} to {rates[-1]:,.0f} rows/s,"
            f" {when[0]} to {when[-1]})")


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


def size_line(decisions, facts, engine, price_per_gb=None, free=None,
              kept=None, narrowed=()):
    """The plan's line about size, from the source's catalogue: what the
    move carries and what the target needs for it. None where the
    catalogue gives no sizes.

    `price_per_gb` is what the hop says a gigabyte costs to carry across
    its network path (hop option `transfer_price_per_gb`). migkit cannot
    read a price from anywhere, so without one no cost is said. What
    crosses is the tables' rows; the indexes are built on the target.

    `kept` is the target's own answer to how much of the log a load
    writes it holds at once: a callable from the bytes of log written to
    (bytes held, why), or None where the target cannot say. What it holds
    needs room beside the tables.

    `narrowed` are the carried tables a row filter narrows
    (`Engine.carried`): weighed whole, so the size said is the most."""
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
    # a table a row filter narrows is weighed whole: what it carries of it
    # is less, and the line says the most
    line = (f"{'at most' if narrowed else 'about'} {_size(data + index)}"
            f" to carry ({_size(data)} of tables, {_size(index)} of indexes"
            " built on the target"
            + (f"; {len(narrowed)} tables a row filter narrows are weighed"
               " whole" if narrowed else "")
            + "); the target needs that much room")
    log = LOG_PER_BYTE.get(engine)
    grows = held = 0
    if log:
        what, per, base = log
        grows = (data + index if base == "tables and indexes" else data) * per
        answer = kept(int(grows)) if kept else None
        if answer:
            held, why = answer
            line += (f", plus the {_size(int(held))} of {what} it holds at"
                     f" once while it loads ({why})")
    if free is not None:
        # measured on PostgreSQL: a target whose disk filled mid-load did
        # not just refuse the rows - it stopped altogether, on the log it
        # could not write
        line += (f" and has {_size(free)} free"
                 + (" - NOT ENOUGH: a load that fills a disk can stop the"
                    " target altogether, not only the move"
                    if free < data + index + held else ""))
    if log:
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


# --- how long each phase takes, and why (backlog R22) -----------------------
# From the resource model of the move (`model`): its rate at the workers it
# runs, each table's share of it, the target's writes for the indexes, both
# sides' reads for the check, and the source's changes against what the
# target applies for the catch-up. Where the stages were not measured, the
# rates of earlier runs on the path; where neither is, no time - nothing is
# assumed that could have been measured.

@dataclass
class Phase:
    name: str
    p50: float = None           #: seconds, or None where not estimated
    p90: float = None
    binds: str = ""             #: the resource that sets it
    truth: str = ""             #: where its rate came from
    detail: str = ""
    never: bool = False         #: a catch-up that cannot finish

    def line(self):
        if self.never:
            return f"{self.name}: NEVER - {self.detail}"
        if self.p50 is None:
            return f"{self.name}: not estimated - {self.detail}"
        return (f"{self.name} {_time_said(self.p50)}"
                + (f", p90 {_time_said(self.p90)}" if self.p90
                   and self.p90 > self.p50 * 1.05 else "")
                + (f", set by {self.binds}" if self.binds else "")
                + (f" ({self.truth})" if self.truth else "")
                + (f"; {self.detail}" if self.detail else ""))


def _time_said(secs):
    return "under a second" if secs < 1 else f"~{_took(secs)}"


#: the least a p90 is above its p50: probes of seconds do not see what a
#: server does over an hour
P90_LEAST = 0.2
#: seconds of changes the cutover waits for and applies after the source
#: stops taking writes, at the least
FENCE_SECONDS = 5.0


def _p90(p50, spread):
    return p50 * (1 + max(P90_LEAST, 2 * (spread or 0.0)))


def _ranges_of(fact, n):
    from . import ranges
    if not fact.get("key"):
        return 1
    return max(1, min(n, int(fact.get("rows") or 0) // ranges.LEAST))


def phases(hop, model, facts, tables=None, n=None, via="builtin"):
    """[Phase] of the move of one database: the copy by table, the index
    builds, the check, the catch-up and the cutover."""
    facts = facts or {}
    names = [t for t in (tables if tables is not None else facts)
             if t in facts]
    disk = sum(int((facts[t] or {}).get("bytes") or 0) for t in names)
    rows = sum(int((facts[t] or {}).get("rows") or 0) for t in names)
    measured = model is not None and model.measured
    rate = model.rate(n) if measured else None
    s = model.seconds_per_byte() if measured else None
    b = model.binding() if measured else None
    ratio = (model.carried / disk if measured and model.carried and disk
             else 1.0)
    out, copy, first = [], None, None
    if rate and disk and s:
        n = n or model.little or 1
        each = []
        for t in names:
            f = facts[t] or {}
            size = int(f.get("bytes") or 0) * ratio
            alone = min(_ranges_of(f, n) / s, rate)
            each.append((size / alone if alone else 0.0, t))
        whole = disk * ratio / rate
        longest = max(each) if each else (0.0, "")
        copy = max(whole, longest[0])
        first = longest[0]
        top = [(x, t) for x, t in sorted(each, reverse=True)[:3] if x > 0]
        k = _ranges_of(facts[longest[1]], n) if each else 1
        binds = ((f"{longest[1]}, {k} ranges at a time" if k > 1 else
                  f"{longest[1]}, one range at a time")
                 if longest[0] > whole else b.said if b else "")
        out.append(Phase(
            "copy", copy, _p90(copy, model.spread), binds,
            b.truth if b else "",
            f"{_size(disk * ratio)} at ~{_size(rate)}/s, {n} at a time"
            + (" - " + ", ".join(f"{t} ~{_took(x)}" for x, t in top)
               if len(top) > 1 else "")
            + ("" if "target" in model.stages else
               "; the target's writes not measured, so at least this")))
    else:
        copy = _copy_from_runs(hop, via, disk, rows, out)
        if copy is None:
            out.append(Phase("copy", detail="nothing on this path measured"
                             " yet" + ("" if measured else
                                       ", and too little to probe")))
    out += _index_builds(model if measured else None, facts, names, copy,
                         first, via)
    src = model.stages.get("source") if measured else None
    ran = _check_from_runs(hop, disk * ratio, rows) if disk else None
    if ran is not None:
        out.append(ran)
    elif src is not None and src.bound() and disk:
        back = model.stages.get("target-check")
        there = (back.per_stream * (n or 1) if back is not None
                 and back.per_stream else None)
        both = min(x for x in (src.bound(), there) if x)
        took = disk * ratio / both
        out.append(Phase("check", took, _p90(took, model.spread),
                         src.said if both == src.bound()
                         else "the target's reads", src.truth,
                         "every row read on both sides and digested there"))
    idx = sum(p.p50 or 0 for p in out if p.name == "index builds")
    return out + _with_cutover(hop, catch_up(model, (copy or 0) + idx))


def _copy_from_runs(hop, via, disk, rows, out):
    """The copy's time from earlier runs of this hop, or of another of the
    same engines: the table copy's own rate a byte where its ways were
    timed (`tableway.measured`, kept by the one table ladder), else the
    rows a second the whole path made. Appends the phase and returns its
    seconds, or None."""
    got = (_bytes_rates(hop, {"bytes": disk, "rows": rows})
           if disk and via == "builtin" else None)
    if got:
        rates, whose = got
        copy = disk / rates[-1]
        wide = _widened(hop, whose, rows)
        out.append(Phase(
            "copy", copy, max(disk / rates[0] if len(rates) > 1
                              else _p90(copy, 0), _p90(copy, wide)), "",
            "measured on an earlier run" + (f" of {whose}" if whose else ""),
            f"{_size(disk)} at the table copy's ~{_size(rates[-1])}/s"
            " measured before; which resource bound it was not measured"))
        return copy
    got = _rows_rates(hop, via) if rows else None
    if not got:
        return None
    rates, whose = got
    copy = rows / rates[len(rates) // 2]
    wide = _widened(hop, whose, rows)
    out.append(Phase(
        "copy", copy, max(rows / rates[0] if len(rates) > 1
                          else _p90(copy, 0), _p90(copy, wide)),
        "", "measured on an earlier run" + (f" of {whose}" if whose else ""),
        f"{rows:,} rows at the {len(rates)} rate"
        + ("s" if len(rates) > 1 else "") + " measured on this path; which"
        " resource bound it was not measured"))
    return copy


def _widened(hop, whose, rows):
    """The spread a forecast from this hop's rehearsal takes for a move of
    `rows` (`rehearsal.sample_spread`), from the most rows the rehearsal
    carried in one run; nothing for runs of any other hop."""
    if not whose or not str(whose).startswith("the rehearsal") or not rows:
        return 0.0
    from . import movers, rehearsal
    rec = rehearsal.of(hop)
    if rec is None:
        return 0.0
    had = [r.get("rows") for p in tuple(movers.VIAS) + (rehearsal.CHECK,)
           for r in _runs(rec["dir"] / "throughput.json", p)
           if r.get("rows")]
    return rehearsal.sample_spread(max(had), rows) if had else 0.0


def _check_from_runs(hop, size, rows):
    """[the check's phase] from whole checks measured - this hop's own
    (`rehearsal.CHECK`), else its rehearsal's: the bytes a second a check
    read both sides at. None where neither was measured: the check is then
    composed from the resource model's stages."""
    from . import rehearsal
    if hop is None or not size:
        return None
    runs, whose = _runs(_rates_path(hop), rehearsal.CHECK), None
    if not runs:
        rec = rehearsal.of(hop)
        if rec is not None:
            runs = _runs(rec["dir"] / "throughput.json", rehearsal.CHECK)
            whose = f"the rehearsal {rec['name']}"
    runs = [r for r in runs if r.get("bytes") and r.get("seconds")]
    if not runs:
        return None
    rates = sorted(r["bytes"] / r["seconds"] for r in runs)
    took = size / rates[len(rates) // 2]
    p90 = max(size / rates[0], _p90(took, _widened(hop, whose, rows)))
    return Phase("check", took, p90, "", "measured on an earlier run"
                 + (f" of {whose}" if whose else ""),
                 f"every row read on both sides and digested there, at the"
                 f" ~{_size(rates[-1])}/s whole checks were measured at")


def _bytes_rates(hop, unit):
    """([bytes a second of the table copy, least first], whose): each way
    it was measured on, fitted by the one table ladder's cost model and
    read for `unit` - the move's bytes and rows (`tableway.measured`) - of
    this hop, its rehearsal, or another hop of the same engines; None where
    there is none."""
    from . import config, rehearsal, tableway
    from .costmodel import HOP_FILE
    mine = tableway.measured(hop, unit)
    if mine:
        return sorted(mine), None
    rec = rehearsal.of(hop)
    if rec is not None:
        got = tableway.measured(hop, unit,
                                tableway._read(rec["dir"] / HOP_FILE) or {})
        if got:
            return sorted(got), f"the rehearsal {rec['name']}"
    whose, found = [], []
    try:
        dirs = sorted(p for p in config.REPORTS.iterdir() if p.is_dir())
    except OSError:
        return None
    for d in dirs:
        if d.name == hop.name:
            continue
        # a hop of other engines keeps its fits under its own pair
        got = tableway.measured(hop, unit,
                                tableway._read(d / HOP_FILE) or {})
        if got:
            found += got
            whose.append(d.name)
    return (sorted(found), ", ".join(whose[:3])) if found else None


def _index_builds(model, facts, names, copy, first, via):
    """[the index builds' phase]: each table's secondary indexes set aside
    for its load and built beside the next tables' copy (`movers.Beside`)
    where the table copier moves them, so what is left after the copy is
    what the builds had not caught up with, and at least the last table's
    own; after the whole load where a bulk path loads them. At the rate
    the target writes - assumed, no build timed."""
    index = sum(int((facts[t] or {}).get("index_bytes") or 0)
                for t in names)
    if not index:
        return []
    target = model.stages.get("target") if model is not None else None
    if target is None or not target.bound():
        return [Phase("index builds", detail=f"{_size(index)} of indexes,"
                      " the target's writes not measured")]
    rate = target.bound()
    took = index / rate
    if via != "builtin" or copy is None:
        return [Phase("index builds", took, 2 * took, target.said,
                      "assumed: at the rate the target writes",
                      f"{_size(index)} of indexes, built after the load")]
    # the tables go largest first: the last copied is the smallest with
    # indexes of its own, and nothing is built before the first finishes
    sized = sorted(((int((facts[t] or {}).get("bytes") or 0),
                     int((facts[t] or {}).get("index_bytes") or 0))
                    for t in names), reverse=True)
    last = next((i for _, i in reversed(sized) if i), 0)
    after = max(took - max(copy - (first or 0), 0), last / rate, 0.0)
    return [Phase("index builds", after, 2 * after, target.said,
                  "assumed: at the rate the target writes",
                  f"{_size(index)} of indexes, ~{_took(took)} of builds,"
                  " each table's beside the next tables' copy; this is"
                  " what is left once the copy ends")]


def _with_cutover(hop, got):
    """The catch-up's phases, with the cutover's from what this hop's own
    cutovers measured (`cutover.last_run`), or bounded by the outage its
    `cutover:` allows (the budget: past it, every step is undone)."""
    if hop is None or not got or got[-1].name != "cutover" or got[-1].never:
        return got
    try:
        from . import cutover, rehearsal
        last = cutover.last_run(hop)
        conf = cutover.settings(hop)
        rec = rehearsal.of(hop)
    except (Exception, SystemExit):  # noqa: BLE001 - as assumed, then
        return got
    if last and last.get("state") == "flipped" \
            and last.get("outage") is not None:
        took = float(last["outage"])
        return got[:-1] + [Phase(
            "cutover", took, 1.5 * took, "", "measured on an earlier run",
            "the write outage of this hop's last cutover")]
    theirs = rehearsal.last_cutover(rec)
    if theirs and theirs.get("state") == "flipped" \
            and theirs.get("outage") is not None:
        took = float(theirs["outage"])
        return got[:-1] + [Phase(
            "cutover", took, 1.5 * took, "",
            f"measured on the rehearsal {rec['name']}",
            "the write outage of the rehearsal's last cutover"
            + (f"; the budget here is {float(conf['budget']):g}s"
               if conf else ""))]
    if conf:
        budget = float(conf["budget"])
        est = got[-1]
        return got[:-1] + [Phase(
            "cutover", min(est.p50, budget), budget, "", est.truth,
            f"{est.detail}; never more than the {budget:g}s budget -"
            " past it, every step is undone")]
    return got


def catch_up(model, behind_for):
    """[Phase] of the catch-up and the cutover: the changes that arrive
    while the copy runs `behind_for` seconds, applied at the rate the
    target takes them while more keep arriving. Where that rate is no more
    than the arrival: never, and what would change it."""
    if model is None or not model.arrival:
        return [Phase("catch-up", detail="the source does not say how fast"
                      " it changes")]
    arrive, how = model.arrival[0], model.arrival[1]
    if not model.apply:
        return [Phase("catch-up", detail=f"changes arrive at ~{arrive:,.0f}"
                      f"/s ({how}); what the target applies not measured")]
    apply, truth = model.apply[0], model.apply[1]
    return catch_up_sum(arrive * behind_for, arrive, apply, truth)


#: what makes a target apply faster, or the changes fewer
HELPS = ("a larger target instance, fewer indexes on the target while it"
         " catches up, or the move at a quieter hour on the source")


def catch_up_sum(backlog, arrive, apply, truth="", slow=0.5):
    """[catch-up, cutover] from the changes behind, arriving a second and
    applied a second. The p90 takes the apply at `slow` of its rate: the
    tail reads the log and decodes it before the target writes a row."""
    if apply <= arrive:
        return [Phase("catch-up", never=True, binds="the target's writes",
                      truth=truth,
                      detail=f"changes arrive at ~{arrive:,.0f}/s and are"
                             f" applied at ~{apply:,.0f}/s, so it never"
                             f" catches up; {HELPS} would change that"),
                Phase("cutover", never=True,
                      detail="the catch-up never ends, so no cutover can"
                             " wait for it")]
    took = backlog / (apply - arrive)
    low = apply * slow
    p90 = backlog / (low - arrive) if low > arrive else None
    fence = arrive * FENCE_SECONDS / apply + FENCE_SECONDS
    return [Phase("catch-up", took, p90, "the target's writes", truth,
                  f"~{backlog:,.0f} changes behind, arriving at"
                  f" ~{arrive:,.0f}/s and applied at ~{apply:,.0f}/s"
                  + ("" if p90 else f"; at {slow:.0%} of that it would"
                     f" never catch up - {HELPS} would make it sure")),
            Phase("cutover", fence, 2 * fence, "", "assumed",
                  "the source stopped, the last changes applied and"
                  " checked")]


def _rows_rates(hop, path):
    """([rows a second, least first], whose) of the runs on `path`: this
    hop's, or another hop's of the same engines; None where there are
    none."""
    runs, whose = _runs(_rates_path(hop), path), None
    if not runs:
        runs, others = _elsewhere(hop, path)
        whose = ", ".join(others[:3]) if others else None
    if not runs:
        return None
    return sorted(r["rows"] / r["seconds"] for r in runs), whose


def phase_lines(ps):
    return [f"  time: {p.line()}" for p in ps]


class Ewma:
    """A rate as the recent samples say it: each new one weighs `alpha`,
    so a pause or a burst moves it without taking it over."""

    def __init__(self, alpha=1 / 3):
        self.alpha, self.value = alpha, None

    def add(self, x):
        self.value = x if self.value is None else (
            self.alpha * x + (1 - self.alpha) * self.value)
        return self.value


def progress(where, pace, total_rows, model=None, done=False, ways=()):
    """What the move has done and how long is left, kept as
    `progress.json` beside its reports for `watch` and the metrics: rows
    done, the recent rate (the pace's moving average), the time left of
    the copy at it, the number at a time and what holds it down - a
    resource whose own limit holds it now, measured while the move runs,
    before the one the model named from its probes. `done` once the copy
    has ended: its whole rate then."""
    import os
    import time
    rate, rows = pace.recent, pace.total
    took = pace.took() if done else pace._clock() - pace.started
    left = (0.0 if done else max(total_rows - rows, 0) / rate
            if rate and total_rows else None)
    held = pace.held_by()
    b = model.binding() if model is not None and model.measured else None
    got = {"at": time.time(), "rows_done": int(rows), "seconds": took,
           "rows_total": int(total_rows or 0),
           "rate": (rows / took if done and took else rate),
           "eta_copy": left, "workers": pace.limit,
           "started_at": pace.history[0][0], "done": bool(done),
           "held_by": held,
           "binding": (held.split(":")[0] if held else
                       b.said if b else None),
           "binding_truth": ("measured while it runs" if held else
                             b.truth if b else None),
           # each table's way and what a row of it cost, as its units
           # measured them (`Engine.measured_ways`)
           "ways": [{"table": w.get("table"), "work": w.get("work"),
                     "way": w.get("chosen"),
                     "seconds_a_row": w.get("seconds_a_row")}
                    for w in ways or ()][:50]}
    from . import durable
    durable.write_json(os.path.join(str(where), "progress.json"), got)


def read_progress(where):
    """`progress.json` of a report directory, or None."""
    import json
    import os
    try:
        with open(os.path.join(str(where), "progress.json")) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


#: seconds behind the end of the source's log that still count as keeping
#: up: the tail reads a batch, applies it and reads again
KEEPING_UP = 10.0


class CatchUp:
    """The catch-up's own sum while a tail runs, sample by sample: changes
    arriving on the source a second (`arrived`, the source's own count)
    against changes the tail applies a second (`applied`, its heartbeat's),
    and - where the source holds its log for the tail (`held`, bytes) - how
    fast what it holds shrinks, which is the time left itself. Each reader
    answers a running total or None."""

    def __init__(self, arrived, applied, held=None, behind=None,
                 clock=None):
        import time
        self._read = {"arrived": arrived, "applied": applied, "held": held,
                      "behind": behind}
        self._clock = clock or time.monotonic
        self._last = None
        self.rates = {"arrived": Ewma(), "applied": Ewma(), "held": Ewma()}
        self.now = {}

    def sample(self):
        now = {"t": self._clock()}
        for k, fn in self._read.items():
            try:
                now[k] = fn() if fn is not None else None
            except Exception:  # noqa: BLE001 - that one is not known now
                now[k] = None
        last, self._last, self.now = self._last, now, now
        if last is None or now["t"] <= last["t"]:
            return
        dt = now["t"] - last["t"]
        for k in ("arrived", "applied", "held"):
            a, b = last.get(k), now.get(k)
            # a count that went down was reset - a restarted tail, a
            # server's statistics - and is not a rate
            if a is not None and b is not None and (b >= a or k == "held"):
                self.rates[k].add((b - a) / dt)

    def phases(self):
        """[catch-up] as far as the samples say, or []."""
        arrive = self.rates["arrived"].value
        apply = self.rates["applied"].value
        behind = self.now.get("behind")
        held = self.now.get("held")
        if arrive is None or apply is None:
            return []
        if behind is not None and behind <= KEEPING_UP:
            return [Phase("catch-up", 0.0, 0.0, "", "measured",
                          f"keeping up: changes arrive at ~{arrive:,.1f}/s"
                          f" and are applied at ~{apply:,.1f}/s")]
        shrink = self.rates["held"].value
        if held is not None and shrink is not None:
            if shrink < 0:
                took = held / -shrink
                return [Phase("catch-up", took, 2 * took,
                              "the target's writes", "measured",
                              f"{_size(held)} of the source's log still to"
                              f" apply, shrinking ~{_size(-shrink)}/s;"
                              f" changes arrive at ~{arrive:,.1f}/s and are"
                              f" applied at ~{apply:,.1f}/s")]
            return catch_up_sum(held, arrive, min(apply, arrive),
                                "measured")[:1]
        if apply <= arrive:
            return catch_up_sum(0, arrive, apply, "measured")[:1]
        return [Phase("catch-up", detail=f"applied at ~{apply:,.1f}/s"
                      f" against ~{arrive:,.1f}/s arriving: catching up;"
                      " how far behind, the source does not say")]

    def line(self):
        self.sample()
        got = self.phases()
        return got[0].line() if got else ""

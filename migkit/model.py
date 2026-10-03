"""Every resource a move uses, in one model: which one binds, how many at
once carry it, what it takes of this machine's memory, and why (backlog
R22).

`sizing` started a move from the least of this machine's CPUs and memory
and each server's connections and CPUs - guesses about the resources, none
of them measured - and the pace found the rest while the move ran. Which
resource bound it nobody could say, so nobody could say what would help:
more workers, a larger target, a wider link, a bigger machine for migkit.

So before a move of any size (`PROBE_FROM`) each resource is measured by a
short probe, bounded in bytes and seconds and read-only on the source:

* the source's reads - the copier's own read statement, one stream and then
  `PROBE_STREAMS` side by side on different parts of the largest table;
* the target's writes - what was read, written the copier's way into a
  table nobody else sees and that is gone after (`Engine.probe_write`),
  read back as the copier checks it, and taken again as upserts, which is
  how the change tail writes;
* each link - a reply that costs the server nothing, one stream and then
  several, and the round trip;
* this machine - its CPUs for the work the copier does here per byte (the
  tally of every row it sends and reads back), its memory for the widest
  rows of the move (`Engine.row_sizes`, and what a worker of that
  engine's copier holds, `Engine.COPY_MEMORY`), its network where the
  interface says its speed;
* each server's free connections, load, replica lag and I/O wait
  (`Engine.pressure`), and how fast the source is changing
  (`Engine.changes_made`).

Each stage then has a rate one stream gets and a rate all streams together
get, and says where each came from (`MEASURED` now, `EARLIER` on a run of
this hop or of the same engines elsewhere, `CATALOGUED` from what a server
or the machine says of itself, `ASSUMED`). The move's rate is the least of
the stages' (a roofline); the stage that sets it binds; the workers are
what carry that stage and not one more - Little's law, concurrency = rate
x the time one unit takes - capped by each server's share of its
connections and by the memory the widest rows take; and the plan says it in
words ("the target's writes bind at ~18 MB/s; 3 at a time carry them - more
would not help; a larger target instance would").

What the others do: DMS and the DTS products size a task by instance class
and let the operator pick; pgcopydb and mydumper take a job count; TCP Vegas
and Netflix's concurrency-limits find a limit by latency - which the pace
does while the move runs (`sizing.Pace`), each resource with a limit of its
own. None of them names the resource that binds before the move.
"""
import json
import math
import os
import threading
import time
from dataclasses import asdict, dataclass

MEASURED = "measured"
EARLIER = "measured on an earlier run"
#: measured on the rehearsal of this hop (`rehearsal`), which it names
REHEARSED = "measured on its rehearsal"
CATALOGUED = "catalogued"
ASSUMED = "assumed"
#: from the strongest to the weakest
TRUTHS = (MEASURED, EARLIER, REHEARSED, CATALOGUED, ASSUMED)

#: seconds each probe may take; the probes of a move take about six of
#: these
PROBE_SECONDS = float(os.environ.get("MIGKIT_PROBE_SECONDS") or 1.5)
#: bytes one probe stream reads at most
PROBE_BYTES = 32 * 2 ** 20
#: a database carrying less than this is planned from the catalogue and
#: earlier runs, without the probes: its move takes about as long as they
#: would
PROBE_FROM = int(os.environ.get("MIGKIT_PROBE_FROM") or 256 * 2 ** 20)
#: streams the probes run side by side to see how a resource scales
PROBE_STREAMS = 4
#: streams side by side reaching this share of that many times one
#: stream's rate are taken to scale: the resource has not bound yet
SCALES = 0.8
#: what the read probe keeps for the write probe
SAMPLE_BYTES = 8 * 2 ** 20
#: the reply a link is timed with, and the least time that counts: a reply
#: faster than that on a local link says nothing about its width
LINK_BYTES = 8 * 2 ** 20
LINK_LEAST = 0.002
#: how far apart two arrival samples must be to count as a recent rate
ARRIVAL_LEAST = 2.0
#: a server's reads or writes measured within this factor of what its
#: link carried were measured at the link's speed: the probes
#: cross it, and a server read through a link of 8 MB/s reads at 8 MB/s
#: whatever it could do - measured, a PostgreSQL source behind a 64 Mbit/s
#: token bucket read at 7.1 MB/s one stream and four, its link carried
#: about as much, and the source was named as what bound the move
LINKED = 1.25

_CACHE = {}
_LOCK = threading.Lock()


@dataclass
class Stage:
    """One resource on the move's path."""
    name: str                 #: stable key
    said: str                 #: what an operator reads
    per_stream: float = None  #: bytes a second one stream gets
    total: float = None       #: bytes a second all streams together get
    truth: str = ASSUMED
    detail: str = ""
    #: the steps of a unit of work it takes part in: "copy" (the rows
    #: read, carried and written, all at once) and "check" (the range read
    #: back from the target and tallied)
    steps: tuple = ("copy",)
    #: the total is only as far as the probes went: it scaled all the way
    at_least: bool = False
    helps: str = ""
    #: which side of the move it is on: "src", "dst" or "" for this machine
    side: str = ""
    #: measured no faster than its side's link carries: what it was held
    #: to was the link's, and it gives way to the link where both are named
    through_link: bool = False

    def bound(self):
        """Bytes of the move a second this stage allows: its total, over
        the times each byte goes through it. None where nothing bounds it."""
        if not self.total:
            return None
        return self.total / max(len(self.steps), 1)

    def demand(self, step):
        """Seconds one stream spends on a byte in `step` here."""
        if step not in self.steps or not self.per_stream:
            return 0.0
        return 1.0 / self.per_stream


class Model:
    """The resources of one database's move and what follows from them."""

    def __init__(self, stages=(), rows=None, carried=None, per_row=None,
                 worker_memory=None, widest=None, sizes=None, arrival=None,
                 apply=None, spread=0.0, notes=(), measured=False,
                 sides=None, engine="", planned=(), overlapped=False,
                 path="", observed=None):
        self.stages = {s.name: s for s in stages}
        #: rows and bytes of the work, as carried
        self.rows, self.carried, self.per_row = rows, carried, per_row
        #: what one worker holds, and the widest row: (table, bytes, exact)
        self.worker_memory, self.widest = worker_memory, widest
        self.sizes = sizes or {}
        #: (changes a second, truth) arriving on the source, and applied
        self.arrival, self.apply = arrival, apply
        #: how far one stream's measured rates spread about their mean
        self.spread = spread
        self.notes = list(notes)
        self.measured = measured
        self.sides = sides or {}
        self.engine = engine
        #: the phases the plan worked out from it (`planner.phases`), kept
        #: for `watch` and the metrics
        self.planned = list(planned)
        #: each unit's check runs beside the next unit's copy, and the way
        #: the rows go, in words
        self.overlapped, self.path = overlapped, path
        #: what a move measured of itself (`observe`)
        self.observed = dict(observed or {})
        #: seconds the probes took, where they ran: what the probe floor
        #: is tuned by once the move has measured its own rate
        #: (`tuning`, `probe_floor`)
        self.probe_seconds = None

    # --- what follows -----------------------------------------------------

    def binding(self):
        """The stage that sets the move's rate: the least bound. A stage
        that scaled through every stream the probes ran (`at_least`) has
        shown a floor, not a ceiling, and binds only where no stage showed
        one. None where no stage has a bound."""
        return _least(self.stages.values())

    def per_stream_binding(self):
        """The stage that holds one stream back: where it spends longest on
        a byte. More streams are what get past it, up to the binding
        stage."""
        got = [s for s in self.stages.values() if s.per_stream]
        return max(got, key=lambda s: (1.0 / s.per_stream / (
            LINKED if s.through_link else 1.0), s.name.startswith("link")
            )) if got else None

    def ceiling(self):
        """Bytes a second the move goes at most (the binding stage's)."""
        b = self.binding()
        return b.bound() if b else None

    def seconds_per_byte(self, sides=("src", "dst", "")):
        """Seconds one stream takes over a byte: in each step its slowest
        stage, the steps one after the other - or, where each unit's check
        runs beside the next unit's copy (`overlapped`), the longer of the
        two."""
        steps = []
        for s in self.stages.values():
            for step in s.steps:
                if step not in steps:
                    steps.append(step)
        seen = (self.observed or {}).get("seconds_a_row")
        if seen and self.per_row and set(sides) >= {"src", "dst", ""}:
            # what one unit took on the way the copier settled on, measured
            # on the path itself by an earlier move of it
            return seen / self.per_row
        each = [max([s.demand(step) for s in self.stages.values()
                     if s.side in sides] or [0.0]) for step in steps]
        if not each:
            return None
        return (max(each) if self.overlapped else sum(each)) or None

    @property
    def little(self):
        """As many at once as carry the binding stage, and not one more:
        its rate times the time one unit takes (Little's law). None where
        the stages were not measured."""
        return self._little(("src", "dst", ""), ("src", "dst", ""))

    def _little(self, sides, bound_sides):
        if not self.measured:
            return None
        s = self.seconds_per_byte(sides)
        b = _least(x for x in self.stages.values() if x.side in bound_sides)
        if not s or b is None:
            return None
        return max(1, math.ceil(b.bound() * s - 1e-9))

    @property
    def read_workers(self):
        """Streams that carry the source's side on its own: a dump."""
        return self._little(("src", ""), ("src", ""))

    @property
    def write_workers(self):
        """Streams that carry the target's side on its own: a load."""
        return self._little(("dst", ""), ("dst", ""))

    def rate(self, n=None):
        """Bytes a second at `n` at once (the model's own number where
        None): n streams' worth, no more than the binding stage allows."""
        top = self.ceiling()
        s = self.seconds_per_byte()
        n = self.little if n is None else n
        if not s or not n:
            return top
        return min(n / s, top) if top else n / s

    def rows_rate(self, n=None):
        r = self.rate(n)
        return r / self.per_row if r and self.per_row else None

    # --- in words ---------------------------------------------------------

    def lines(self, n=None):
        """The plan's lines about the resources."""
        out = []
        b = self.binding()
        shown = sorted((s for s in self.stages.values() if s.bound()),
                       key=lambda s: s.bound())
        if shown:
            out.append("resources: " + "; ".join(
                f"{s.said} {'at least ' if s.at_least else ''}~"
                f"{_rate(s.bound())} ({s.truth}"
                + (f": {s.detail}" if s.detail else "") + ")"
                for s in shown[:6]))
        if b is not None:
            want = self.little
            n = want if n is None else n
            if b.at_least:
                out.append(
                    f"binds: nothing found to bind within the"
                    f" {PROBE_STREAMS} streams the probes ran - the least"
                    f" is {b.said} at ~{_rate(b.bound())} or more; the pace"
                    " goes on from there while the move runs")
            elif want and n is not None and n < want:
                out.append(
                    f"binds: {n} at a time is as many as allowed here, and"
                    f" {b.said} could take ~{want} (~{_rate(b.bound())});"
                    f" at {n} the move goes at ~{_rate(self.rate(n))}")
            else:
                out.append(
                    f"binds: {b.said} at ~{_rate(b.bound())}"
                    + (f"; {want} at a time carry {_them(b)} - more would"
                       " not help" if want else "")
                    + (f"; {b.helps} would" if b.helps else ""))
            one = self.per_stream_binding()
            if one is not None and one is not b and want and want > 1:
                out.append(f"each stream: held to ~{_rate(one.per_stream)}"
                           f" by {one.said}, so it takes {want} at once to"
                           f" fill {b.said}")
            below = [x for x in self.stages.values() if x.at_least
                     and x is not b and x.bound() and x.bound() < b.bound()]
            for x in below:
                out.append(f"unbound: {x.said} carried"
                           f" ~{_rate(x.bound())} at {PROBE_STREAMS} streams"
                           " without binding; the pace finds out whether it"
                           " does before the rest")
        if self.path:
            out.append(f"path: {self.path}")
        if self.widest:
            table, most, exact = self.widest
            out.append(
                f"memory: the widest row is ~{_size(most)} ({table},"
                f" {'every row measured' if exact else 'from a sample'});"
                f" a worker here holds about {_size(self.worker_memory)}")
        for note in self.notes:
            out.append(note)
        return out

    def observe(self, ways, rows, seconds, workers):
        """What the move measured of itself, kept for the next plan: its
        whole rate at the number it ran, and the seconds a row one unit
        took on the way each table's units settled on
        (`Engine.measured_ways`, the ranges' own timing) - the time one
        stream takes, for Little's law, from the path itself rather than
        composed from the probes."""
        # the work a copier times one unit - a range - of a table as
        from .engines.pg_pass import WORK
        costs = []
        for w in ways or ():
            got = w.get("seconds_a_row") or {}
            if w.get("work") != WORK or not got:
                continue
            way = w.get("chosen") or min(got, key=got.get)
            if got.get(way):
                costs.append(got[way])
        costs.sort()
        self.observed = {
            "rows_a_second": rows / seconds if rows and seconds else None,
            "workers": workers,
            "seconds_a_row": costs[len(costs) // 2] if costs else None,
            "at": time.time()}

    def too_wide(self, free):
        """Why not even one worker fits in `free` bytes of this machine's
        memory with the widest rows of the move, in words - or None. Said
        before the move starts: a copy that runs out of memory is killed
        halfway, and one that is told a smaller budget cuts values short
        (what a large-object size set too small does elsewhere) - neither
        is allowed here."""
        from .sizing import RESERVE_MEMORY
        if not free or not self.worker_memory or not self.widest:
            return None
        if self.worker_memory <= free - RESERVE_MEMORY:
            return None
        table, most, exact = self.widest
        return (f"the widest row of the move is ~{_size(most)} ({table},"
                f" {'every row measured' if exact else 'from a sample'}),"
                f" and one worker carrying it holds about"
                f" {_size(self.worker_memory)} here; this machine has"
                f" {_size(free)} free and keeps {_size(RESERVE_MEMORY)} for"
                f" everything else. Run migkit where it has at least"
                f" {_size(self.worker_memory + RESERVE_MEMORY)} free; nothing"
                " is cut short to fit")

    # --- for the decision layer -------------------------------------------

    def costs(self):
        """The stages as `decide.Costs`: seconds one stream spends on a
        megabyte at each measured stage, keyed by the stage's name, so a
        choice between ways that load different resources can be costed
        from what was measured here rather than tried first."""
        from .decide import Costs
        out = Costs()
        for s in self.stages.values():
            if s.per_stream and s.truth in (MEASURED, EARLIER, REHEARSED):
                out[s.name] = 2 ** 20 * len(s.steps) / s.per_stream
        return out

    def facts(self):
        """What a rung's needs may ask of the resources (`decide.Facts`)."""
        b = self.binding()
        return {"binding": b.name if b else None,
                "ceiling": self.ceiling(), "workers": self.little,
                "worker_memory": self.worker_memory,
                "widest": self.widest[1] if self.widest else None,
                **{f"rtt:{k}": v.get("rtt") for k, v in self.sides.items()}}

    # --- kept -------------------------------------------------------------

    def to_dict(self):
        return {"stages": [asdict(s) for s in self.stages.values()],
                "rows": self.rows, "carried": self.carried,
                "per_row": self.per_row, "worker_memory": self.worker_memory,
                "widest": list(self.widest) if self.widest else None,
                "arrival": list(self.arrival) if self.arrival else None,
                "apply": list(self.apply) if self.apply else None,
                "spread": self.spread, "notes": self.notes,
                "measured": self.measured, "engine": self.engine,
                "sides": self.sides, "at": time.time(),
                "planned": [asdict(p) for p in self.planned],
                "overlapped": self.overlapped, "path": self.path,
                "observed": self.observed}

    @classmethod
    def from_dict(cls, d):
        stages = [Stage(**{**s, "steps": tuple(s.get("steps") or ("copy",))})
                  for s in d.get("stages") or ()]
        return cls(stages, rows=d.get("rows"), carried=d.get("carried"),
                   per_row=d.get("per_row"),
                   worker_memory=d.get("worker_memory"),
                   widest=tuple(d["widest"]) if d.get("widest") else None,
                   arrival=tuple(d["arrival"]) if d.get("arrival") else None,
                   apply=tuple(d["apply"]) if d.get("apply") else None,
                   spread=d.get("spread") or 0.0, notes=d.get("notes") or (),
                   measured=bool(d.get("measured")), sides=d.get("sides"),
                   engine=d.get("engine") or "",
                   planned=[_phase(p) for p in d.get("planned") or ()],
                   overlapped=bool(d.get("overlapped")),
                   path=d.get("path") or "", observed=d.get("observed"))

    def save(self, where):
        """Kept beside the move's reports as `model.json`: `watch`, the
        metrics and the next run read it."""
        path = os.path.join(str(where), "model.json")
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.to_dict(), f, indent=1, default=str)
        os.replace(tmp, path)


def _least(stages):
    got = [s for s in stages if s.bound()]
    if not got:
        return None
    known = [s for s in got if not s.at_least]
    return min(known or got, key=lambda s: s.bound())


def _phase(d):
    from .planner import Phase
    return Phase(**d)


def _rate(bps):
    from .wording import human_bytes
    return f"{human_bytes(bps)}/s" if bps else "?"


def _size(n):
    from .wording import human_bytes
    return human_bytes(n or 0)


def _them(stage):
    return "them" if stage.said.endswith("s") else "it"


def load(where):
    """The model kept in a report directory, or None."""
    try:
        with open(os.path.join(str(where), "model.json")) as f:
            return Model.from_dict(json.load(f))
    except (OSError, ValueError, TypeError):
        return None


# --- what a worker holds ----------------------------------------------------

def worker_memory(eng, sizes, tables=None, db=None):
    """(bytes one worker of `eng`'s copier holds, (table, widest row, every
    row measured)) for the tables of the move: its own, and what it holds
    of rows - `per_row` times the larger of the widest row and the batch it
    reads at once - on the way the widest table's rows go: server to
    server where they pass so (`Engine.passes_through`, `PASS_MEMORY`),
    through migkit otherwise (`COPY_MEMORY`)."""
    widest = None
    for t, s in (sizes or {}).items():
        if tables is not None and t not in tables and not any(
                str(t).endswith("." + str(x)) or str(x).endswith(
                    "." + str(t)) for x in tables):
            continue
        if s and (widest is None or s["largest"] > widest[1]):
            widest = (t, int(s["largest"]), bool(s.get("exact")))
    from .sizing import WORKER_MEMORY
    held = getattr(eng, "COPY_MEMORY", None) or {}
    through = getattr(eng, "PASS_MEMORY", None)
    if through and widest and db is not None and _safe(
            lambda: eng.passes_through(db, widest[0])):
        held = through
    own = int(held.get("own") or 0)
    per = float(held.get("per_row") or 1.0)
    batch = int(held.get("batch") or 0)
    # a copier that reads a count of rows at a time, or so many bytes
    # (`COPY_BATCH`/`COPY_BYTES`, `READ_ROWS`/`READ_BYTES`), whichever is
    # fewer: narrow rows fill the count long before the bytes
    count = getattr(eng, "COPY_BATCH", None) or getattr(eng, "READ_ROWS",
                                                        None)
    most_bytes = getattr(eng, "COPY_BYTES", None) or getattr(
        eng, "READ_BYTES", None)
    if held is not through and count and most_bytes:
        means = [float(x.get("mean") or 0) for x in (sizes or {}).values()
                 if x]
        batch = min(int(most_bytes), int(count * max(means or [0])))
    most = widest[1] if widest else 0
    # a copier that carries a value past a piece a piece at a time holds a
    # piece of it, never the value (`CARRIES_IN_PIECES`, `pieces`)
    piece = getattr(eng, "CARRIES_IN_PIECES", None)
    if piece and most > piece:
        most = int(piece)
    # never under what a worker was measured to hold before rows were
    # sized: this only ever holds a move of wide rows back
    return max(WORKER_MEMORY, int(own + per * max(batch, most))), widest


# --- the probes -------------------------------------------------------------

def _safe(fn, default=None):
    try:
        return fn()
    except Exception:  # noqa: BLE001 - a probe never stops a move
        return default


def _cpu():
    import resource
    u = resource.getrusage(resource.RUSAGE_SELF)
    return u.ru_utime + u.ru_stime


def _side_by_side(fn, k):
    """fn(i) for i in 1..k on threads at once: ([answers], wall seconds).
    `_side_by_side.busy` is the share of that time this process spent on a
    CPU: near one, the threads were held by its one core (the
    interpreter's lock), not by what they measured."""
    got = [None] * k
    began = time.monotonic()
    cpu = _cpu()

    def one(i):
        got[i - 1] = _safe(lambda: fn(i))
    ts = [threading.Thread(target=one, args=(i,), daemon=True)
          for i in range(1, k + 1)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(PROBE_SECONDS * 20 + 60)
    wall = time.monotonic() - began
    _side_by_side.busy = (_cpu() - cpu) / wall if wall > 0 else 0.0
    return got, wall


_side_by_side.busy = 0.0

#: this process on a CPU this share of the time the probes ran side by side
#: held them to one core: the interpreter runs one thread at a time, and a
#: probe whose driver decodes every row in Python was measured at that
#: core's speed - where the copier's workers are processes of their own and
#: run on as many cores as this machine has
ONE_CORE = 0.8


def _one_core(eng, stage, busy):
    """A stage measured side by side while this process's one core was
    busy (`busy` of the time), where the copier runs its workers as
    processes: its total is a floor (`at_least`), said why."""
    if stage is None or stage.at_least or busy < ONE_CORE \
            or not getattr(eng, "COPIES_IN_PROCESSES", True):
        return
    stage.at_least = True
    stage.detail += ("; the streams side by side were held by this"
                     " process's one core, and the copier's workers are"
                     " processes of their own")


def scaled(one, many, k, cpus=None):
    """(total bytes a second, at least, how it was found) for a resource
    whose one stream got `one` and whose `k` streams side by side got
    `many` together. Short of `SCALES` of k times one, it has bound:
    `many` is its total. Past it, it has not: extrapolated to its CPUs
    where it says how many, and `many` as a floor where it does not."""
    if not one or not many:
        return None, False, ""
    eff = many / (k * one)
    if eff < SCALES:
        return many, False, (f"{k} streams got {eff * k:.1f} times one's"
                             f" ~{_rate(one)}")
    if cpus and cpus > k:
        return (one * eff * cpus, False,
                f"{k} streams scaled; taken on to its {cpus} CPUs")
    return many, True, f"{k} streams scaled"


#: the widest row a probe reads: a row arrives whole, and a probe of wide
#: rows side by side would hold several of them at once
PROBE_ROW = PROBE_BYTES // 8


#: the least a table holds to be probed: fewer bytes are read in less time
#: than a rate can be told from
PROBE_LEAST = PROBE_BYTES // 4


def _largest(facts, tables, sizes=None):
    """The table the probes read: the largest whose rows are no wider than
    `PROBE_ROW` and that holds at least `PROBE_LEAST`, or None."""
    best = None
    for t in tables:
        f = facts.get(t) or {}
        size = int(f.get("bytes") or 0)
        wide = int(((sizes or {}).get(t) or {}).get("largest") or 0)
        if int(f.get("rows") or 0) > 0 and size >= PROBE_LEAST \
                and wide <= PROBE_ROW and (best is None or size > best[1]):
            best = (t, size)
    return best[0] if best else None


def _carried(hop, eng, db, facts, tables):
    """(the tables the move carries, as the catalogue names them; those of
    them a row filter narrows - `Engine.carried` - whose rows and bytes
    are the whole table's)."""
    narrowed = []
    ask = getattr(eng, "carried", None)
    if callable(ask):
        try:
            narrowed = ask(db, facts)[1]
        except Exception:  # noqa: BLE001 - weighed whole, as before
            narrowed = []
    if tables is not None:
        got = [t for t in tables if t in facts] or list(tables)
    else:
        got = [t for t in facts
               if hop.takes_table(db, t)]
    return got, [t for t in narrowed if t in got]


def build(hop, eng, db, facts=None, tables=None, probe=None, write=True,
          log=None):
    """The `Model` of one database's move: the catalogue, the widest rows,
    what each server says of itself, and - where the move carries at least
    `PROBE_FROM` bytes, or `probe` says so - the probes. Without them the
    stages are an earlier run's (`EARLIER`), and a move nothing measured
    has none: no rate is assumed."""
    facts = facts if facts is not None else _safe(
        lambda: eng.table_facts("src", db), {}) or {}
    carried, narrowed = _carried(hop, eng, db, facts, tables)
    disk = sum(int((facts.get(t) or {}).get("bytes") or 0) for t in carried)
    rows = sum(int((facts.get(t) or {}).get("rows") or 0) for t in carried)
    sizes = _safe(lambda: eng.row_sizes("src", db, carried), {}) or {}
    held, widest = worker_memory(eng, sizes, carried, db)
    sides = {s: _safe(lambda s=s: eng.pressure(s, db), {}) or {}
             for s in ("src", "dst")}
    arrived = _safe(lambda: eng.changes_made("src", db))
    m = Model(rows=rows or None, carried=disk or None,
              per_row=(disk / rows) if disk and rows else None,
              worker_memory=held, widest=widest, sizes=sizes, sides=sides,
              engine=getattr(eng, "dst_name", None) or hop.engine)
    if arrived and arrived[1] > 0:
        m.arrival = (arrived[0] / arrived[1], CATALOGUED,
                     f"the source's own count over its last"
                     f" {arrived[1] / 3600:,.1f}h")
    from .pieces import PIECE
    way = getattr(eng, "LARGE_VALUES", None)
    if widest and widest[1] > PIECE and way is not None:
        # how the rows wider than a piece go, where there are any
        m.notes.append(f"large values: {way.said}")
    if narrowed:
        # a row filter's share of a table is not in any catalogue
        m.notes.append(f"{len(narrowed)} tables a row filter narrows are"
                       " weighed whole: the rows and bytes are the most the"
                       " move carries")
    if probe is None:
        probe = disk >= probe_floor(hop)
    if probe and carried:
        # both sides at once: a side that does not answer costs `REACH`
        rtts, _ = _side_by_side(
            lambda i: round_trip(hop.source if i == 1 else hop.target), 2)
        for side, rtt in zip(("src", "dst"), rtts):
            if rtt is not None:
                sides[side]["rtt"] = rtt
        began = time.monotonic()
        _probe(m, hop, eng, db, facts, carried, write, log)
        later = _safe(lambda: eng.changes_made("src", db))
        took = time.monotonic() - began
        m.probe_seconds = took
        if arrived and later and took >= ARRIVAL_LEAST \
                and later[0] >= arrived[0]:
            recent = (later[0] - arrived[0]) / took
            # the busier of the two: the recent sample is a few seconds,
            # the long one smooths a busy hour away
            if m.arrival is None or recent > m.arrival[0]:
                m.arrival = (recent, MEASURED,
                             f"over the {took:.0f}s the probes took")
        if not write:
            # what a plan may not measure without writing to the target,
            # from the rehearsal of this hop where one measured it
            _earlier(m, hop, db, only=("target", "target-check"))
    else:
        _earlier(m, hop, db)
    return m


def probe_floor(hop=None):
    """The least a database carries for its resources to be probed:
    `MIGKIT_PROBE_FROM` where the operator sets it, else `PROBE_FROM` as
    this machine's and this hop's moves have tuned it (`tuning`: the bytes
    a move carried in the time its probes took)."""
    if os.environ.get("MIGKIT_PROBE_FROM"):
        return PROBE_FROM
    from . import tuning
    try:
        return tuning.value("model.PROBE_FROM", hop)
    except Exception:  # noqa: BLE001 - the prior, then
        return PROBE_FROM


def tune_probe_floor(hop, m, rows, seconds):
    """What a move that was probed says the probe floor should have been:
    the bytes it carried in the time its probes took."""
    if not m.probe_seconds or not rows or not seconds or not m.per_row:
        return
    from . import tuning
    tuning.observe("model.PROBE_FROM",
                   m.probe_seconds * rows / seconds * m.per_row, hop)


#: seconds a server's port has to answer before its round trip is given up
#: on: a plan is not held up by a server it cannot reach yet
REACH = 1.0


def round_trip(ep):
    """Seconds a connection to an endpoint takes to open, the least of
    three; None where the first does not open within `REACH`."""
    from .tunnel import _round_trip
    if not getattr(ep, "host", None) or not getattr(ep, "port", None):
        return None
    first = _safe(lambda: _round_trip(ep.host, ep.port, 1, REACH))
    if first is None:
        return None
    return min(first, _safe(lambda: _round_trip(ep.host, ep.port, 2, REACH))
               or first)


def for_move(hop, eng, db, facts=None, tables=None, log=None, write=True,
             probe=None):
    """The model for this run's move of `db`: built once for the engine
    the run moves it with - the plan's and the copy's the same - and kept
    beside the reports."""
    key = (hop.name, db)
    with _LOCK:
        got = _CACHE.get(key)
    if got is not None and got[0] is eng:
        return got[1]
    m = build(hop, eng, db, facts, tables, probe=probe, write=write, log=log)
    with _LOCK:
        _CACHE[key] = (eng, m)
    _safe(lambda: m.save(hop.report_dir(db)))
    return m


def forget():
    """This run's models gone, for a run that starts over."""
    with _LOCK:
        _CACHE.clear()


def _earlier(m, hop, db, only=None):
    """The stages of the last run that measured them - this hop's, its
    rehearsal's (`rehearsal.of`, named), or another hop's of the same
    engines - as `EARLIER` (`REHEARSED` for the rehearsal's). `only` names
    the stages a plan could not measure now, taken from the rehearsal
    alone and only where the probes left them out. A rehearsal that
    carried fewer rows than this move widens the p90 by
    `rehearsal.sample_spread`."""
    from . import config, rehearsal
    from .planner import _kind
    rec = rehearsal.of(hop)
    found = whose = None
    if only is None:
        here = load(hop.report_dir(db))
        found = here if here is not None and here.measured else None
    if found is None and rec is not None:
        where = rehearsal.model_dir(rec, db)
        got = load(where) if where is not None else None
        if got is not None and got.measured:
            found, whose = got, rec
    if found is None and only is None:
        kind = _kind(hop)
        try:
            dirs = sorted(p for p in config.REPORTS.iterdir() if p.is_dir())
        except OSError:
            dirs = []
        for d in dirs:
            if d.name == hop.name:
                continue
            got = load(d / db) or next(
                (x for x in (load(s) for s in _subdirs(d)) if x), None)
            if got is not None and got.measured and _kind_of(d) == kind:
                found = got
                break
    if found is None:
        return
    said = rehearsal.line(hop, whose, found.rows, m.rows) if whose else ""
    if only is not None:
        took = []
        for name in only:
            s = found.stages.get(name)
            if s is None or name in m.stages:
                continue
            s.truth = REHEARSED
            m.stages[name] = s
            took.append(s.said)
        if took:
            m.spread = max(m.spread or 0.0,
                           rehearsal.sample_spread(found.rows, m.rows))
            m.notes.append(f"{said}; {', '.join(took)} from there, the"
                           " rest measured here now")
        if found.apply and m.apply is None:
            m.apply = (found.apply[0], REHEARSED) + tuple(found.apply[2:])
        return
    for s in found.stages.values():
        if s.truth == MEASURED:
            s.truth = REHEARSED if whose else EARLIER
        m.stages[s.name] = s
    m.measured = True
    m.spread = found.spread
    if whose is not None:
        m.spread = max(m.spread or 0.0,
                       rehearsal.sample_spread(found.rows, m.rows))
    m.overlapped, m.path = found.overlapped, found.path
    m.observed = found.observed
    if found.per_row:
        # the stages are bytes as carried: the rows are counted in them
        m.per_row = found.per_row
        m.carried = int(m.rows * found.per_row) if m.rows else m.carried
    if found.apply and m.apply is None:
        m.apply = (found.apply[0], REHEARSED if whose else EARLIER) \
            + tuple(found.apply[2:])
    if said:
        m.notes.append(said)


def _subdirs(d):
    try:
        return [p for p in d.iterdir() if p.is_dir()]
    except OSError:
        return []


def _kind_of(report_dir):
    """The engines a report directory's runs were of, from its rates."""
    try:
        got = json.loads((report_dir / "throughput.json").read_text())
    except (OSError, ValueError):
        return None
    for runs in got.values():
        for r in runs:
            if r.get("kind"):
                return r["kind"]
    return None


def _probe(m, hop, eng, db, facts, carried, write, log):
    """Every probe, each bounded; a probe that fails leaves its stage out
    and says why."""
    from . import sizing
    k = PROBE_STREAMS
    # each range read back from the target once written, unless the hop
    # says not
    checks = (hop.options or {}).get("verify_batches", True)
    table = _largest(facts, carried, m.sizes)
    if table is None:
        m.notes.append(f"no table of at least {_size(PROBE_LEAST)} with"
                       f" rows narrower than {_size(PROBE_ROW)} to probe;"
                       " the pace measures the move while it runs")
        return
    # the way the copier takes this table's ranges: server to server,
    # each server digesting every range beside the next one's load - or
    # through migkit, tallied here and read back from the target
    through = bool(_safe(lambda: eng.passes_through(db, table)))
    m.overlapped = through and checks and bool(
        getattr(eng, "CHECKS_BESIDE", False))
    m.path = ("its ranges pass server to server" + (
        ", each digested by both servers"
        + (" beside the next one's load" if m.overlapped else "")
        if checks else "") if through else
        "its rows pass through migkit" + (
            ", each range read back from the target and tallied here"
            if checks else ""))
    here = sizing.host()
    cpus_here = here.get("cpus") or 1
    src_cpus = m.sides.get("src", {}).get("cpus")
    dst_cpus = m.sides.get("dst", {}).get("cpus")
    say = log or (lambda _: None)
    say(f"measuring the resources on {table}: the source's reads, the"
        " target's writes, each link and this machine")

    # the source's reads, one stream and then k side by side
    cpu0 = _cpu()
    first = _safe(lambda: eng.probe_read("src", db, table, 0, k + 1,
                                         PROBE_BYTES, PROBE_SECONDS,
                                         SAMPLE_BYTES))
    client = _cpu() - cpu0
    if not first or not first.get("bytes") or not first.get("seconds"):
        m.notes.append("the source's reads could not be measured on"
                       f" {table}; the pace finds the number while it runs")
        return
    one = first["bytes"] / first["seconds"]
    per_row = first["bytes"] / max(first["rows"], 1)
    f = facts.get(table) or {}
    if f.get("bytes") and f.get("rows"):
        # what the catalogue's bytes are as carried, from the table read
        ratio = per_row / (int(f["bytes"]) / max(int(f["rows"]), 1))
        m.carried = int(m.carried * ratio) if m.carried else None
    m.per_row = per_row
    many, wall = _side_by_side(
        lambda i: eng.probe_read("src", db, table, i, k + 1, PROBE_BYTES,
                                 PROBE_SECONDS), k)
    got = [g for g in many if g and g.get("bytes") and g.get("seconds")]
    rates = [g["bytes"] / g["seconds"] for g in got]
    total = (sum(g["bytes"] for g in got) / max(g["seconds"] for g in got)
             if got else None)
    tot, floor, how = scaled(one, total, len(got) or k, src_cpus)
    busy = _side_by_side.busy
    src_primary = not m.sides.get("src", {}).get("replica")
    m.stages["source"] = Stage(
        "source", "the source's reads", one, tot, MEASURED,
        f"one stream ~{_rate(one)}" + (f", {how}" if how else "")
        + ("; each byte read again for its digest" if through and checks
           else ""),
        ("copy", "check") if through and checks else ("copy",), floor,
        ("reading from a replica, or a larger source instance"
         if src_primary else "a larger source instance"), "src")
    _one_core(eng, m.stages["source"], busy)
    if rates:
        mean = sum(rates) / len(rates)
        m.spread = (sum((r - mean) ** 2 for r in rates) / len(rates)
                    ) ** 0.5 / mean if mean else 0.0

    # the links, a reply that costs the server nothing
    for side, name in (("src", "the link to the source"),
                       ("dst", "the link to the target")):
        took = _safe(lambda side=side: eng.probe_link(side, db, LINK_BYTES))
        if not took:
            continue
        if took < LINK_LEAST:
            m.sides.setdefault(side, {})["link"] = "wider than measurable"
            continue
        lone = LINK_BYTES / took
        before = _safe(sizing.retransmits)
        both, wall = _side_by_side(
            lambda i, side=side: eng.probe_link(side, db, LINK_BYTES), k)
        after = _safe(sizing.retransmits)
        good = [t for t in both if t]
        ltot = (len(good) * LINK_BYTES / wall) if good and wall else None
        tot, floor, how = scaled(lone, ltot, len(good) or k)
        rtt = m.sides.get(side, {}).get("rtt")
        lost = _loss(before, after)
        if lost is not None:
            m.sides.setdefault(side, {})["loss"] = lost
        m.stages["link:" + side] = Stage(
            "link:" + side, name, lone, tot, MEASURED,
            f"one connection ~{_rate(lone)}"
            + (f", {rtt * 1000:.1f} ms round trip" if rtt else "")
            + (f", {lost:.1%} of this machine's packets sent again while"
               " it was measured" if lost else "")
            + (f", {how}" if how else ""),
            ("copy", "check") if side == "dst" and checks
            and not through else ("copy",),
            floor,
            "a wider link, or migkit run beside the "
            + ("source" if side == "src" else "target"), side)

    _held_by_links(m)

    # this machine: what the copier does here per byte. Server to server,
    # only the relay of the bytes; through migkit, the client's read where
    # migkit's own processes read the rows (`READS_HERE`) and the tally of
    # every row, each twice where every range is read back
    sample = first.get("sample")
    times = 2 if checks else 1
    if through:
        per_byte = _relayed(sample)
    else:
        per_byte = times * ((client / first["bytes"]
                             if getattr(eng, "READS_HERE", True) else 0.0)
                            + _tallied(sample))
    if per_byte > 0:
        cores = cpus_here if getattr(eng, "COPIES_IN_PROCESSES", True) else 1
        m.stages["cpu"] = Stage(
            "cpu", "this machine's CPUs" if cores > 1
            else "this machine's CPU for the copier's threads",
            1.0 / per_byte, cores / per_byte, MEASURED,
            f"{per_byte * 2 ** 20 * 1000:.1f} ms of CPU a MB here, {cores}"
            f" CPU{'s' if cores > 1 else ''} for it", ("copy",), False,
            "a machine for migkit with more CPUs" if cores > 1 else
            "more machines sharing the move", "")
    speed = _nic_speed()
    local = all(str(ep.host) in ("127.0.0.1", "localhost", "::1")
                for ep in (hop.source, hop.target))
    if speed and not local:
        m.stages["nic"] = Stage(
            "nic", "this machine's network", None, speed, CATALOGUED,
            f"its interface says {speed * 8 / 1e9:.0f} Gbit/s",
            ("copy", "check"), False, "a machine for migkit with a faster"
            " network", "")

    # the target's writes: what was read, written the copier's way
    if not write:
        m.notes.append("the target's writes are not measured here - nothing"
                       " is written to it yet; the move's own probes and"
                       " its pace measure them")
    if write and sample:
        w1 = _safe(lambda: eng.probe_write(db, table, sample,
                                           PROBE_SECONDS, 0, apply=False))
        if w1 and w1.get("seconds"):
            wone = w1["bytes"] / w1["seconds"]
            rone = (w1["bytes"] / w1["read_seconds"]
                    if w1.get("read_seconds") else None)
            ws, wall = _side_by_side(
                lambda i: eng.probe_write(db, table, sample, PROBE_SECONDS,
                                          i, apply=False), k)
            busy = _side_by_side.busy
            good = [w for w in ws if w and w.get("seconds")]
            wtot = (sum(w["bytes"] for w in good)
                    / max(w["seconds"] for w in good)) if good else None
            tot, floor, how = scaled(wone, wtot, len(good) or k, dst_cpus)
            # the check reads each range back: the target's time on a
            # byte is its write and its read, one after the other
            per = (1.0 / (1.0 / wone + 1.0 / rone) if checks and rone
                   else wone)
            m.stages["target"] = Stage(
                "target", "the target's writes", wone,
                tot * per / wone if tot else None, MEASURED,
                f"one stream ~{_rate(wone)}"
                + (f", read back ~{_rate(rone)}" if checks and rone else "")
                + (f", {how}" if how else "") + f"; {w1['how']}",
                ("copy",), floor,
                "a larger target instance - more CPUs, faster storage",
                "dst")
            _one_core(eng, m.stages["target"], busy)
            if checks and rone:
                m.stages["target"].steps = ("copy",)
                m.stages["target-check"] = Stage(
                    "target-check", "the target reading each range back",
                    rone, None, MEASURED, "", ("check",), False, "", "dst")
            # the upserts last, on a stream of their own: what they write
            # is not waited on by the writes measured before them
            up = _safe(lambda: eng.probe_write(db, table, sample,
                                               PROBE_SECONDS, k + 1))
            if up and up.get("applied") and up.get("apply_seconds"):
                m.apply = (up["applied"] / up["apply_seconds"], MEASURED,
                           "the target taking rows again as upserts")
        else:
            m.notes.append("the target's writes could not be measured here;"
                           " the pace finds them while the move runs")
    _held_by_links(m)
    m.measured = True


def _held_by_links(m):
    """A server stage measured at its link's speed (`LINKED`) is marked so
    (`through_link`): its total a floor where the link's total is no more
    than it, so the link, not the server, is named as what binds."""
    for stage in m.stages.values():
        link = m.stages.get(f"link:{stage.side}")
        if stage.name.startswith("link") or link is None \
                or stage.through_link or not stage.per_stream \
                or not link.per_stream:
            continue
        if stage.per_stream * LINKED < link.per_stream:
            # well under what the link carries: its own
            continue
        stage.through_link = True
        stage.detail += ("; measured no faster than its link carries -"
                         " the link's speed, or its own")
        if stage.total and link.total and not link.at_least \
                and link.total <= LINKED * stage.total:
            stage.at_least = True


def _loss(before, after):
    """The share of this machine's TCP segments sent again between two
    readings (`sizing.retransmits`), where enough were sent to say; None
    otherwise. Every connection of the machine is in it, not the probe's
    alone."""
    from .sizing import LOSS_SENT
    if not before or not after:
        return None
    sent, again = after[0] - before[0], after[1] - before[1]
    if sent < LOSS_SENT or again < 0:
        return None
    return again / sent


def _tallied(sample):
    """CPU seconds a byte the tally of the rows takes here (`tally`)."""
    from .tally import Tally
    began = _cpu()
    t = Tally()
    if isinstance(sample, (bytes, bytearray)):
        t.feed(bytes(sample))
        t.end()
        tallied = len(sample)
    else:
        from .engines.base import _row_bytes
        tallied = 0
        for r in sample or ():
            t.row(r)
            tallied += _row_bytes(r)
    return (_cpu() - began) / tallied if tallied else 0.0


def _relayed(sample):
    """CPU seconds a byte passing bytes on, 64 KB at a time, takes here:
    what a copy server to server costs this machine."""
    import io
    data = sample if isinstance(sample, (bytes, bytearray)) else b""
    if not data:
        return 0.0
    src, dst = io.BytesIO(bytes(data) * 4), io.BytesIO()
    began = _cpu()
    while True:
        chunk = src.read(1 << 16)
        if not chunk:
            break
        dst.write(chunk)
    return (_cpu() - began) / (4 * len(data))


def _nic_speed():
    """Bytes a second this machine's fastest interface that is up says it
    carries, or None - most virtual ones say nothing."""
    try:
        import psutil
        got = [s.speed for n, s in psutil.net_if_stats().items()
               if s.isup and s.speed and s.speed > 0
               and not n.startswith(("lo", "docker", "br-", "veth"))]
    except Exception:  # noqa: BLE001 - not readable here
        return None
    return max(got) * 125_000 if got else None

"""Cross-engine table copies where rows never pass through Python (backlog
R20.13 and R20.14), as ways of the table copier chosen per table.

The table copier reads rows into Python objects, renders and writes them:
the one way that works for every pair. Two more ways carry a range of rows
from one server to the other with nothing between them but bytes:

* the compiled mover (`COMPILED`): an in-process analytical engine
  attaches the source - MySQL, or a SQLite, DuckDB, Parquet, Delta or
  Iceberg file - and PostgreSQL, and runs one `INSERT INTO target SELECT
  ... FROM source` a range in compiled code, writing through binary COPY.
  The source is read by a query in its own SQL, so the hop's row filter is
  applied by the source as written, and each column as it is read to be
  moved (`canon.read_expr`, `READ`); the columns are named in the target's
  order, so the hop's column mapping is the statement's column lists; the
  target's own statement empties the range in the transaction the rows
  arrive in. Not into MySQL (`COMPILED_TARGETS`, measured);
* the source rendering the target's load format (`RENDERED`): MySQL
  renders each row as a line of PostgreSQL's COPY text, and PostgreSQL
  renders each row as MySQL's load reads it (`loadtext`); the lines go
  into the target's own load as they arrive - off MySQL's connection a
  packet at a time with no row object made, out of PostgreSQL's `COPY ...
  TO STDOUT` in its blocks - the range emptied first, in one transaction.

Neither is trusted: each range, once committed, is held to both real
servers' leaf of the digest tree over it (`proved`, `leaves`), and one
that differs, or a way that stopped, is copied again by the table copier,
which reads it back batch by batch - and the way is given up for the rest
of the table, with the reason said. A way that cannot carry one of a
table's columns exactly, or whose proof would leave a column out, is not
offered for that table at all; the table goes another way, and why is
said.

Which way each unit goes is the engine's own chooser (`Engine._trials`,
`measured_ways`), the rungs `ways()`: a way never timed is tried on one
range of the table, the ranges started meanwhile go to the next never
timed, and then each range takes the one measured cheapest on that table;
a table too small to split is one unit, held to the run's other whole
tables. The list's order is what was measured here (`bench/cross_rates.py`:
MySQL 8.4 and PostgreSQL 16 in containers sharing one 2-CPU VM, four
workers, a table shaped as `bench/seed.py`'s rows - a key, five texts, a
decimal, two integers, a flag, a datetime, a JSON document and 16 bytes -
built by migkit on the target; seconds for the whole move, every range
proved and every result checked):

    MySQL into PostgreSQL         copier  compiled  rendered  one-pass load
    1,000,000 rows with JSON        19.3      26.6      23.1  nothing (1)
      the same, another run         22.6      18.2      16.5
    1,000,000 rows without JSON     15.8      13.7      14.8  nothing (1)
    5,000,000 rows with JSON       402.0     101.8     101.6  nothing (1)

    PostgreSQL into MySQL         copier  rendered
    1,000,000 rows with JSON        56.1      23.1
    1,000,000 rows without JSON     30.6      20.2
    5,000,000 rows with JSON       337.5     186.1

    one stream, copy + proof      compiled         rendered
    1,000,000 rows with JSON      8.2 + 25.7       8.2 + 24.0
    1,000,000 rows without JSON   5.4 + 8.2        8.8 + 9.1
    5,000,000 rows with JSON      43.1 + 122.0     77.6 + 196.2

(1) the one-pass load program (3.6.10) could not sign in to MySQL 8.4,
said so in its log, loaded nothing and exited 0 - now a failure
(`movers.pgloader_move`). The compiled mover into MySQL took 1,042.8s for
the million rows the copier moved in 47.8s, and is not offered there.

What the numbers say: the copy itself is two to seven times as fast as
the table copier's whole move, and the proof of each range on both servers
is what a move waits on - most where a JSON column is hashed in this
process (`canon.FOLDED_HERE`), and on this rig, where both servers share
two processors. Where the proof costs what the copier's own read-back
does, the ways come out even (a million rows with JSON, within the
spread of two runs); at five million, and into MySQL, the copier's
round trips a batch cost two to four times as much. Each table takes what
it measures.

`MIGKIT_TABLE_COPY` names the one way every unit takes where it holds -
for measuring, like `MIGKIT_MOVER`, never needed to move.
"""
import os
import threading
import time

COMPILED, RENDERED, COPIER = "compiled", "rendered", "copier"


# --- which way a table goes -------------------------------------------------

def _pair(f):
    return f["pair"]


def _need_pair(pairs, why):
    from .decide import Need
    return Need("a pair it moves", lambda f: _pair(f) in pairs, why)


def _held_whole():
    from .decide import Need
    return Need("every column held to the digest",
                lambda f: not f["unproved"],
                lambda f: ("{} is not compared on both sides, so a range"
                           " it carried could not be proved").format(
                               f["unproved"][0]))


def _carries(refuse):
    from .decide import Need
    return Need("every column carried exactly",
                lambda f: refuse(f) is None, lambda f: refuse(f))


def _takes_the_load():
    from .decide import Need
    return Need("a target that takes the load",
                lambda f: f["pair"][1] != "mysql" or f["loads"],
                "the target does not take the load MySQL reads from its"
                " client (`local_infile` is off there), and migkit does not"
                " change a server's settings")


def _unheld():
    """Not where this process writes under a machine's number (`fencing`,
    backlog W1): the way writes on connections of its own, around the
    writer the target holds to the number, so a machine that lost the hop
    or the range would not be refused - the table copier, whose every
    write is held, goes instead."""
    from .decide import Need
    return Need("no machine's number to carry", lambda f: not f["held"],
                "{rung} writes around the number the machine holding this"
                " copy carries, so a machine that lost it could not be"
                " refused by the target")


def _unmasked():
    """Not a table the hop masks (`Hop.masks`): the way passes the values
    between the servers as the source holds them, where the table copier
    writes each masked one through the hop's transforms."""
    from .decide import Need
    return Need("no masked column", lambda f: not f["masked"],
                "{rung} passes the values as the source holds them, and the"
                " hop masks columns of this table")


def _installed():
    from .decide import Need
    return Need("its extensions installed",
                lambda f: not missing_extensions(f["pair"]),
                "the compiled mover's parts for this pair are not installed;"
                " migkit doctor --install puts them in place")


#: source engines the compiled mover attaches, and the target it writes.
#: Not MySQL: its writer there sends a range as INSERT statements, and
#: measured, a million rows from PostgreSQL took 1,042.8s where the table
#: copier took 47.8s, and one range's INSERT failed after minutes and was
#: copied again the copier's way
COMPILED_SOURCES = ("mysql", "sqlite", "duckdb", "parquet")
COMPILED_TARGETS = ("postgres",)
#: the pairs where the source renders the target's load format
RENDERED_PAIRS = {("mysql", "postgres"), ("postgres", "mysql")}


def _compiled_pairs():
    return {(s, d) for s in COMPILED_SOURCES for d in COMPILED_TARGETS
            if s != d}


def _joined():
    """The pairs one of the ways other than the table copier joins."""
    return _compiled_pairs() | RENDERED_PAIRS


def ways():
    """The ways a table's rows go, in the order measured fastest here
    (`__doc__`)."""
    from .decide import Rung
    return (
        Rung(RENDERED, "the source rendering the target's load format",
             needs=(_unheld(), _unmasked(), _need_pair(RENDERED_PAIRS,
                               "the source renders the target's load format"
                               " between MySQL and PostgreSQL only"),
                    _held_whole(), _carries(lambda f: f["rendered"]),
                    _takes_the_load()),
             because="measured fastest here: both servers do the work, and"
                     " the bytes between them are only passed on"),
        Rung(COMPILED, "the compiled mover",
             needs=(_unheld(), _unmasked(), _need_pair(_compiled_pairs(),
                               "the compiled mover does not join these two"
                               " engines"),
                    _held_whole(), _carries(lambda f: f["compiled"]),
                    _installed()),
             because="server to server in compiled code, with no row"
                     " through this process"),
        Rung(COPIER, "the table copier",
             because="it carries every pair and every type"),
    )


#: the work a range is timed as, beside the engine's other ways - the one
#: the same-engine pass-through times its ranges as (`pg_pass.WORK`), so
#: `measured_ways` reads every way a table's ranges took in one place
WORK = "range copy"
#: the work a table copied whole is timed as: one unit a table, so the
#: tables of a run are held to one another, a row a unit
WHOLE = "table copy, whole"


def columns(eng, db, src_t, dst_t):
    """[(source name, source type, target name, target type, class)] of
    what the copier carries, in the target's names sorted - the order the
    copier writes in - and [target names the digest leaves out]."""
    src_cols, dst_cols, _ = eng._move_columns(src_t, dst_t, db)
    stypes = dict(eng.src_engine.neutral_columns("src", db, src_t))
    dtypes = dict(eng.dst_engine.neutral_columns("dst", db, dst_t))
    out = [(s, stypes.get(s), d, dtypes.get(d), cls)
           for (s, cls), (d, _) in zip(src_cols, dst_cols)]
    sc, dc, _ = eng._comparable_columns(db, eng.src_engine, src_t,
                                        eng.dst_engine, dst_t)
    proved = {n for n, _ in dc}
    return out, [d for _, _, d, _, _ in out if d not in proved]


def _facts(eng, db, src_t, dst_t):
    from . import loadtext
    from .decide import Facts
    got = {}

    def cols():
        if "c" not in got:
            got["c"] = columns(eng, db, src_t, dst_t)
        return got["c"]
    from . import fencing
    return Facts(
        {"pair": (eng.src_engine.CANON_ENGINE, eng.dst_engine.CANON_ENGINE)},
        held=lambda: fencing.armed(eng.hop) is not None,
        masked=lambda: bool(getattr(eng, "_masks", None)
                            and eng._masks(db, src_t)),
        unproved=lambda: cols()[1],
        compiled=lambda: compiled_refuses(eng, db, src_t, cols()[0]),
        rendered=lambda: loadtext.refused(
            cols()[0], (eng.src_engine.CANON_ENGINE,
                        eng.dst_engine.CANON_ENGINE)),
        # asked of a MySQL target once, and only for the rendered way
        loads=lambda: _asked(eng.dst_engine._loads_here))


def _asked(read):
    """What a server answers, or False where it could not be asked - the
    way that needed it is passed over, and the one taken says its own."""
    try:
        return read()
    except Exception as e:  # noqa: BLE001 - a server that did not answer
        _only_operational(e)
        return False


def _others(rungs, facts):
    from .decide import prune
    kept, _ = prune([r for r in rungs if r.name != COPIER], facts)
    return bool(kept)


def besides_the_copier(eng, db, src_t, dst_t):
    """Whether a way besides the table copier holds for this table - asked
    before its work is set up, so a table that goes as it did sets up
    nothing."""
    return _others(_rungs(), _facts(eng, db, src_t, dst_t))


def rows_upto(eng, db, src_t, where, most):
    """The source's rows under `where`, counted as far as `most` + 1 - read
    through the key's index, and never past it. A catalogue's estimate is
    no count: a table loaded and never analysed has none, and a table of a
    million rows taken for a small one would be copied in one transaction
    that a stop starts again from nothing (measured: PostgreSQL's estimate
    of a table just filled by COPY was 0, and the whole of it went as one
    unit). Where the source takes no such statement, its own estimate."""
    src = eng.src_engine
    if src.SQL_DIALECT:
        try:
            got = src.run_rule("src", db, (
                "select count(*) from (select 1 from"
                f" {src._qualified('src', db, src_t)}"
                + (f" where {where}" if where else "")
                + f" limit {int(most) + 1}) migkit_upto"))
            return int(got[0][0])
        except Exception:  # noqa: BLE001 - the estimate, then
            pass
    try:
        return int((src.table_facts("src", db).get(src_t) or {})
                   .get("rows") or 0)
    except Exception:  # noqa: BLE001 - no estimate
        return 0


def _rungs():
    """`ways()`, or only the one `MIGKIT_TABLE_COPY` names: every unit on
    it where it holds, the table copier where it does not or has been
    given up on."""
    rungs = ways()
    forced = os.environ.get("MIGKIT_TABLE_COPY", "").strip()
    if not forced:
        return rungs
    if forced not in [r.name for r in rungs]:
        raise SystemExit(f"MIGKIT_TABLE_COPY={forced} is not one of"
                         f" {', '.join(r.name for r in rungs)}")
    return tuple(r for r in rungs if r.name == forced)


class TableWay:
    """The ways one table goes, on the engine's own chooser: a range at a
    time where the table is copied in ranges - each range climbing the
    ways on the table's own measurements (`Engine._trials`,
    `decide.Trials`), so a way never timed is tried on one range and the
    ranges after it take the cheapest - or the table whole, climbing on
    what the run's other whole tables measured (`WHOLE`). A way whose unit
    did not prove itself is given up on for the rest of the work, with the
    reason said as any other way's is; what each cost is what
    `measured_ways` reports."""

    def __init__(self, eng, db, src_t, dst_t, log, label, whole=False):
        from .decide import Costs, Trials
        self.eng, self.db, self.src_t, self.dst_t = eng, db, src_t, dst_t
        self.log, self.label = log, label
        self.facts = _facts(eng, db, src_t, dst_t)
        rungs = _rungs()
        if whole:
            # one unit a table: the costs are the run's, the trials the
            # table's - two tables at once each read and add to the same
            # numbers, and neither sees the other's facts
            key = eng._work_key(WHOLE)
            costs = eng.__dict__.setdefault("_way_cost", {}).setdefault(
                key, Costs())
            self.trials = Trials(key[0], rungs, self.facts, costs=costs)
            eng.__dict__.setdefault("_way_rungs", {})[key] = rungs
            eng.__dict__.setdefault("_way_facts", {})[key] = self.facts
        else:
            self.trials = eng._trials(eng._work_key(WORK, f"{db}.{src_t}"),
                                      rungs, self.facts)
        self.said = {}
        self._said = set()
        self._lock = threading.Lock()

    @property
    def joined(self):
        """Whether a way besides the table copier joins this pair: for a
        pair none joins, the table goes and reads as it did."""
        return self.facts["pair"] in _joined()

    def others(self):
        """Whether a way besides the table copier holds for this table."""
        return _others(self.trials.rungs, self.facts)

    def pick(self):
        """The way the next unit goes."""
        got = self.trials.pick()
        name = got.path or COPIER
        with self._lock:
            self.said[name] = got.said if got.rung else "the table copier"
            first = not self._said
            self._said.add(name)
            timed = {r.name: self.trials.costs.get(r.name)
                     for r in got.ranked}
            settled = (len(timed) > 1 and "settled" not in self._said
                       and None not in timed.values())
            if settled:
                self._said.add("settled")
        if self.joined and first:
            self.log(f"{self.label}: {self.said[name]} - {got.reason}")
        if self.joined and settled:
            costs = sorted(timed.items(), key=lambda kv: kv[1])
            self.log(f"{self.label}: each way timed on its own units - "
                     + ", ".join(f"{n} {c * 1e6:,.1f} us a row"
                                 for n, c in costs)
                     + f"; each takes the cheapest from here on"
                       f" ({costs[0][0]} now)")
        return name

    def took(self, way, rows, seconds):
        self.trials.took(way, rows, seconds)

    def fell(self, way, why, whole=False):
        """A unit on `way` did not prove itself, and was copied again by
        the table copier: `way` is given up on for the rest of the work."""
        if way == COPIER:
            return
        self.trials.give_up(way, why)
        said = self.said.get(way, way)
        self.log(f"{self.label}: {said} did not prove itself ({why}); "
                 + ("the table is copied again by the table copier" if whole
                    else "the range was copied again by the table copier,"
                         " and the rest climb again without it"))


# --- a range by a way -----------------------------------------------------

def scoped(eng, name, after, upto, extra):
    """`extra` and the range (after, upto] of `name`, in `eng`'s SQL."""
    q = eng._quote_ident(name)
    rng = f"{q} > {int(after)} and {q} <= {int(upto)}"
    return f"({extra}) and {rng}" if extra else rng


def proved(eng, db, src_t, dst_t, src_where, dst_where):
    """None where the source and the target hold the same rows under their
    `WHERE`s, else why not: each side's leaf of the digest tree over those
    rows (`leaves.scan`, one leaf: its count and the sum of its rows'
    hashes), read on both servers at once - the digest the check's own
    tree is made of, not a second hash beside it.

    Asked as rows alone, without the key's own hash: a row's hash covers
    its key already, and the key's is what the tree adds to find a row
    that differs, which a range whose sums differ is not asked for - it is
    copied again. Without it each server hashes a row once, not twice, and
    a side rendered in this process (`leaves.Side.sql`: a JSON, an XML or
    an address column) reads its rows in batches off a cursor the server
    keeps, not in key order a query at a time."""
    from . import leaves
    sc, dc, _ = eng._comparable_columns(db, eng.src_engine, src_t,
                                        eng.dst_engine, dst_t)
    a = leaves.Side(eng.src_engine, "src", db, src_t, list(sc), [],
                    src_where)
    b = leaves.Side(eng.dst_engine, "dst", db, dst_t, list(dc), [],
                    dst_where)
    spec = leaves.Spec("hash", bits=0, salt=eng.digest_salt())
    ta, tb = leaves.both(leaves.scan, a, b, spec)
    a, b = ta.root, tb.root
    if a == b:
        return None
    if a[0] != b[0]:
        return f"the source holds {a[0]:,} rows there and the target {b[0]:,}"
    return f"{a[0]:,} rows on each side, holding different values"


def copy(eng, db, src_t, dst_t, way, src_where, dst_where):
    """Rows of `src_t` under `src_where` put in place of the target's
    under `dst_where` by `way`, in one target transaction: how many."""
    cols, _ = columns(eng, db, src_t, dst_t)
    if way == COMPILED:
        return _compiled(eng, db, src_t, dst_t, cols, src_where, dst_where)
    if way == RENDERED:
        return _rendered(eng, db, src_t, dst_t, cols, src_where, dst_where)
    raise ValueError(f"no way {way!r} here")


def range_by(eng, db, src_t, dst_t, key, dst_key, after, upto, way):
    """One range by `way`, then proved: (rows, None), or (rows, why) where
    the digest says it did not land as the source holds it, or the way
    stopped - the caller then copies it again the table copier's way."""
    from . import failpoint
    src_where, dst_where = eng._row_scope(db, src_t)
    sw = scoped(eng.src_engine, key, after, upto, src_where)
    dw = scoped(eng.dst_engine, dst_key, after, upto, dst_where)
    try:
        rows = copy(eng, db, src_t, dst_t, way, sw, dw)
    except Exception as e:  # noqa: BLE001 - a way that could not run
        _only_operational(e)
        return 0, f"it stopped: {_short(e)}"
    # committed and not yet proved: a stop here leaves the range not
    # recorded, and the next run copies it again
    failpoint.hit("range.moved")
    return rows, proved(eng, db, src_t, dst_t, sw, dw)


def _only_operational(e):
    """A way that could not run - refused, unreachable, a server's own
    error - is copied again the table copier's way; anything else is a
    fault in migkit, raised to be seen (`decide.operational`)."""
    from . import decide
    if not decide.operational(e):
        raise e


def _short(e):
    text = str(e).strip().splitlines()
    return (text[0] if text else type(e).__name__)[:160]


def whole(eng, db, key, src_t, dst_t, st, ck, log):
    """A table not split into ranges, by the way chosen for it: every row
    under the hop's filter replaced in one transaction and proved. Returns
    True once done; else a function the table copier's way calls with the
    rows it copied once it has, so that way is timed as the others are -
    or None where this table goes as it did, untimed: a pair no way joins,
    and a large table the copier resumes by key."""
    from . import ranges
    src = eng.src_engine
    if (src.CANON_ENGINE, eng.dst_engine.CANON_ENGINE) not in _joined():
        return None
    src_where, dst_where = eng._row_scope(db, src_t)
    # a large table that resumes by key or by where its rows are stored
    # goes on as it did: one transaction of it would restart from nothing
    if src.RESUMES_BY_KEY and rows_upto(eng, db, src_t, src_where,
                                        2 * ranges.LEAST) > 2 * ranges.LEAST:
        return None
    way = TableWay(eng, db, src_t, dst_t, log, key, whole=True)
    name = way.pick()
    began = time.monotonic()
    if name == COPIER:
        return lambda moved: way.took(COPIER, moved,
                                      time.monotonic() - began)
    try:
        moved = copy(eng, db, src_t, dst_t, name, src_where, dst_where)
        why = proved(eng, db, src_t, dst_t, src_where, dst_where)
    except Exception as e:  # noqa: BLE001 - a way that could not run
        _only_operational(e)
        moved, why = 0, f"it stopped: {_short(e)}"
    if why:
        way.fell(name, why, whole=True)
        # the table copier starts it over, emptying what is there first: a
        # point it had reached before holds nothing now
        for k in ("last", "moved"):
            st.pop(k, None)
        return None
    way.took(name, moved, time.monotonic() - began)
    st["moved"] = moved
    st["done"] = True
    ck.save()
    log(f"{key}: {moved:,} rows by {way.said[name]}, the same on both"
        " servers")
    return True


# --- the compiled mover ---------------------------------------------------

#: the extension that attaches each engine
EXTENSIONS = {"mysql": "mysql", "postgres": "postgres", "sqlite": "sqlite"}
#: the extensions that read a Delta and an Iceberg table's log
LAKES = ("delta", "iceberg")

_HERE = {}


def installed_extensions():
    """The analytical engine's extensions installed on this machine, asked
    once a process - a move downloads nothing."""
    if "ext" not in _HERE:
        try:
            import duckdb
            c = duckdb.connect()
            _HERE["ext"] = {n for (n,) in c.execute(
                "select extension_name from duckdb_extensions() where"
                " installed").fetchall()}
            c.close()
        except Exception:  # noqa: BLE001 - none, then
            _HERE["ext"] = set()
    return _HERE["ext"]


def missing_extensions(pair):
    need = {EXTENSIONS[e] for e in pair if e in EXTENSIONS}
    have = installed_extensions()
    # an extension answers to its own name and the scanner it was
    return sorted(n for n in need
                  if n not in have and f"{n}_scanner" not in have)


def install_extensions(log=print):
    """The compiled mover's extensions, from the engine's own signed
    repository, for `doctor --install`. True when all are in place."""
    try:
        import duckdb
    except ImportError:
        return False
    c = duckdb.connect()
    ok = True
    for name in sorted(set(EXTENSIONS.values()) | set(LAKES)):
        try:
            c.execute(f"install {name}")
        except Exception:  # noqa: BLE001 - said
            log("  a part of the compiled mover did not install; run migkit"
                " doctor again to see what is still short")
            ok = False
    c.close()
    _HERE.pop("ext", None)
    return ok


def compiled_refuses(eng, db, src_t, cols):
    """Why the compiled mover cannot carry `cols` exactly, or None."""
    return _refused(eng.src_engine.CANON_ENGINE, eng.dst_engine.CANON_ENGINE,
                    cols, eng, db, src_t)


def _refused(src, dst, cols, eng=None, db=None, src_t=None):
    from . import canon, loadtext
    for name, stype, _, dtype, cls in cols:
        s, d = loadtext.base(stype), loadtext.base(dtype)
        took = CARRIED.get(src, {}).get(cls)
        if canon.type_class(dst, dtype) != cls:
            return (f"{name} is {stype} and lands in {dtype}, a value of"
                    " another kind, which the compiled mover does not turn"
                    " into it exactly")
        if took is None or s not in took:
            return (f"{name} is {stype}, which the compiled mover has not"
                    f" been proved to read exactly from {src}")
        if d not in LANDS.get(cls, ()):
            return (f"{name} lands in {dtype}, which the compiled mover has"
                    " not been proved to write exactly")
        why = _digits(stype, dtype) if cls == "decimal" else None
        if why:
            return f"{name}: {why}"
        if cls == "float" and s not in SINGLE and d in SINGLE:
            # widening a single-precision float is exact; narrowing a
            # double is not
            return (f"{name}: a {stype} into a {dtype} is rounded on the"
                    " way")
    if src == "parquet" and eng is not None:
        if _parquet_dir(eng, db, src_t) is None:
            return "its files are not on this machine's disk"
        lake = eng.src_engine.lake("src", db, src_t)
        if lake is not None and lake[0] not in installed_extensions():
            return ("the compiled mover's part that reads its log is not"
                    " installed; migkit doctor --install puts it in place")
    return None


#: the analytical engine's widest decimal: past it, it reads one as a
#: double
DIGITS = 38
#: single-precision floats, by the names each engine declares them
SINGLE = {"float", "real", "float4"}


def _digits(stype, dtype):
    """Why a decimal of `stype` does not go into `dtype` exactly, or None:
    each within the engine's own decimals, the target keeping at least the
    source's digits before the point and after it."""
    from . import canon
    s, d = canon.params(stype), canon.params(dtype)
    if len(d) < 1:
        return (f"a {dtype} without a precision is read by the compiled"
                " mover as a double")
    ps, ss = (s + (0, 0))[:2] if s else (None, None)
    pd, sd = (d + (0,))[:2]
    if pd > DIGITS or (ps or 0) > DIGITS:
        return (f"{stype} into {dtype}: past {DIGITS} digits the compiled"
                " mover reads a decimal as a double")
    if ps is not None and (sd < ss or pd - sd < ps - ss):
        return (f"{stype} into {dtype}: the target holds fewer digits,"
                " and the compiled mover rounds what the target would"
                " refuse")
    return None


#: {source engine: {class: the declared types (`loadtext.base`) the
#: compiled mover reads exactly}}, held to each server's own bytes in
#: `tests/test_a_cross_engine_copy_goes_server_to_server.py`; a class or
#: a type not listed is not carried, and the table goes another way with
#: its name said. Not a MySQL BIT (read as bytes, which no cast turns back
#: into its number) or YEAR, not a decimal past `DIGITS`, not a TIME past
#: a day (refused by the engine, and the range goes the copier's way)
CARRIED = {
    "mysql": {
        "integer": {"tinyint", "smallint", "mediumint", "int", "integer",
                    "bigint"},
        "decimal": {"decimal", "numeric"},
        "float": {"double", "double precision", "real", "float"},
        "text": {"char", "varchar", "tinytext", "text", "mediumtext",
                 "longtext", "enum", "set"},
        "bytes": {"binary", "varbinary", "tinyblob", "blob", "mediumblob",
                  "longblob"},
        "date": {"date"},
        "timestamp": {"datetime", "timestamp"},
        "time": {"time"},
        "json": {"json"},
    },
    "sqlite": {
        "integer": {"int", "integer", "tinyint", "smallint", "mediumint",
                    "bigint", "int2", "int8"},
        "float": {"real", "double", "double precision", "float"},
        "text": {"character", "varchar", "varying character", "nchar",
                 "native character", "nvarchar", "text", "clob"},
        "bytes": {"blob"},
    },
    "duckdb": {
        "integer": {"tinyint", "smallint", "integer", "bigint"},
        "decimal": {"decimal", "numeric"},
        "float": {"float", "real", "double"},
        "boolean": {"boolean"},
        "text": {"varchar", "text"},
        "bytes": {"blob"},
        "date": {"date"},
        "timestamp": {"timestamp", "timestamp with time zone"},
        "time": {"time"},
        "json": {"json"},
    },
    "parquet": {
        "integer": {"int64"},
        "decimal": {"decimal128"},
        "float": {"double"},
        "boolean": {"bool"},
        "text": {"string"},
        "bytes": {"binary"},
        "date": {"date32"},
        "timestamp": {"timestamp"},
        "time": {"time64"},
    },
}

#: {class: PostgreSQL's declared types the compiled mover writes it into
#: exactly}: not a CHAR, which pads what it holds
LANDS = {
    "integer": {"smallint", "integer", "bigint"},
    "decimal": {"numeric"},
    "float": {"double precision", "real"},
    "boolean": {"boolean"},
    "text": {"text", "character varying"},
    "bytes": {"bytea"},
    "date": {"date"},
    "timestamp": {"timestamp without time zone", "timestamp with time zone"},
    "time": {"time without time zone"},
    "json": {"json", "jsonb"},
}


def _parquet_dir(eng, db, table):
    """The table's local directory, or None where its files are in a
    bucket."""
    try:
        fs, path = eng.src_engine._dir("src", db, table)
    except Exception:  # noqa: BLE001
        return None
    import pyarrow.fs as pafs
    return path if isinstance(fs, pafs.LocalFileSystem) else None


def _literal(text):
    return "'" + str(text).replace("'", "''") + "'"


def _ident(name):
    return '"' + str(name).replace('"', '""') + '"'


def _secret(c, name, kind, ep, database):
    parts = [f"type {kind}", f"host {_literal(ep.host)}",
             f"port {int(ep.port)}", f"user {_literal(ep.user)}",
             f"password {_literal(ep.password or '')}",
             f"database {_literal(database)}"]
    c.execute(f"create or replace temporary secret {name} ("
              + ", ".join(parts) + ")")


def _tls(engine, side):
    """The side's TLS as the attach string takes it: libpq's own settings
    for PostgreSQL (`libpq_tls`), and for MySQL what its own client is
    given (`_client_tls`), flag for flag - one reading of the endpoint's
    options for every way that connects."""
    if engine.CANON_ENGINE == "postgres":
        ep = engine.hop.source if side == "src" else engine.hop.target
        return " ".join(f"{k}={v}" for k, v in ep.libpq_tls().items())
    out = []
    for flag in engine._client_tls(side):
        name, _, value = flag[2:].partition("=")
        name = name.replace("-", "_")
        out.append(f"{name}={value.lower() if name == 'ssl_mode' else value}")
    return " ".join(out)


def _attach(c, alias, engine, side, db):
    ep = engine.hop.source if side == "src" else engine.hop.target
    kind = engine.CANON_ENGINE
    ro = ", read_only" if side == "src" else ""
    if kind in ("mysql", "postgres"):
        _secret(c, f"{alias}_secret", kind, ep, engine._d(side, db))
        c.execute(f"attach {_literal(_tls(engine, side))} as {alias} (type"
                  f" {kind}, secret {alias}_secret{ro})")
    elif kind == "sqlite":
        c.execute(f"attach {_literal(engine._path(side))} as {alias} (type"
                  f" sqlite{ro})")
    elif kind == "duckdb":
        c.execute(f"attach {_literal(engine._path(side))} as {alias}"
                  f" ({ro.lstrip(', ') or 'read_only false'})")


def _duck(eng, db):
    """The analytical engine for one unit of work, in memory, with the
    source attached read-only and the target for writing; the caller
    closes it, and with it every connection it opened - none is left
    open on a server after its range."""
    import duckdb
    c = duckdb.connect(config={"TimeZone": "UTC", "threads": THREADS,
                               "memory_limit": MEMORY,
                               "preserve_insertion_order": False,
                               "autoinstall_known_extensions": False})
    try:
        for e in sorted({EXTENSIONS[n] for n in (
                eng.src_engine.CANON_ENGINE, eng.dst_engine.CANON_ENGINE)
                if n in EXTENSIONS}):
            c.execute(f"load {e}")
        if eng.src_engine.CANON_ENGINE != "parquet":
            _attach(c, "migkit_src", eng.src_engine, "src", db)
        _attach(c, "migkit_dst", eng.dst_engine, "dst", db)
        for setting in SETTINGS.get(eng.src_engine.CANON_ENGINE, ()):
            c.execute(setting)
    except BaseException:
        c.close()
        raise
    return c


#: the analytical engine's threads a unit of work: the ranges are side by
#: side already, one a worker, and more threads made one stream slower
#: (measured, 200,000 rows MySQL into PostgreSQL: 0.50s on one, 0.68s on
#: four)
THREADS = 1
#: what one unit's engine may hold: its default is most of the machine's
#: memory, and a range's rows are streamed, never needed whole - nor in
#: the source's order, which a range written in one transaction does not
#: keep anyway. Measured, 5,000,000 rows in four ranges side by side:
#: 908.5s with the defaults (four engines each taking most of the memory
#: there is), 101.8s with these
MEMORY = "1GB"


#: the analytical engine's own defaults that would change a value, by the
#: source they are for: a MySQL `tinyint(1)` and `bit(1)` read as
#: booleans, 2 landing as 1
SETTINGS = {"mysql": ("set mysql_tinyint1_as_boolean = false",
                      "set mysql_bit1_as_boolean = false")}


def _source_query(eng, db, src_t, cols, where):
    """The source's rows as the analytical engine reads them: a query in
    the source's own SQL where it has one, so the row filter is its own,
    each column as `_read` has the source give it."""
    src = eng.src_engine
    kind = src.CANON_ENGINE
    if kind in ("mysql", "sqlite"):
        names = ", ".join(_read(kind, src._quote_ident(n), t, c)
                          for n, t, _, _, c in cols)
        sql = (f"select {names} from {src._qualified('src', db, src_t)}"
               + (f" where {where}" if where else ""))
        return f"{kind}_query('migkit_src', {_literal(sql)})"
    pick = ", ".join(_ident(n) for n, _, _, _, _ in cols)
    if kind == "duckdb":
        sch = src._d("src", db) or "main"
        return (f"(select {pick} from"
                f" migkit_src.{_ident(sch)}.{_ident(src_t)})")
    if kind == "parquet":
        lake = src.lake("src", db, src_t)
        if lake is None:
            path = _parquet_dir(eng, db, src_t)
            return (f"(select {pick} from"
                    f" read_parquet({_literal(path + '/*.parquet')}))")
        # the table as its log says it is now, read by the log's own rules
        return f"(select {pick} from {lake[0]}_scan({_literal(lake[1])}))"
    raise ValueError(f"no source query for {kind}")


#: {(source engine, declared type): (what the source is asked for, what
#: the analytical engine makes of it)}, `{c}` the column: where reading a
#: column as it is would not be exact. A MySQL `timestamp` is read as the
#: session's zone shows it, and that is the server's own unless something
#: sets it - which a query cannot - so it is asked for as seconds since the
#: epoch, which no zone moves, and made a timestamp of UTC again (measured:
#: a server at +07:00 read 00:00:01 UTC as 07:00:01)
READ = {
    ("mysql", "timestamp"): (
        "unix_timestamp({c}) as {c}",
        "make_timestamp(cast({c} * 1000000 as bigint))"),
}


def _read(kind, quoted, stype, cls):
    """A column as the source is asked for it: as `READ` says, else as a
    column is read to be moved (`canon.read_expr` - a single-precision
    float as the double it is), under its own name."""
    from . import canon, loadtext
    got = READ.get((kind, loadtext.base(stype)))
    if got:
        return got[0].format(c=quoted)
    expr = canon.read_expr(kind, quoted, cls)
    return quoted if expr == quoted else f"{expr} as {quoted}"


def _target_name(eng, db, dst_t):
    sch, tbl = eng.dst_engine._split(dst_t)
    return f"migkit_dst.{_ident(sch)}.{_ident(tbl)}"


def _compiled(eng, db, src_t, dst_t, cols, src_where, dst_where):
    dst = eng.dst_engine
    kind = dst.CANON_ENGINE
    empty = (f"delete from {dst._qualified('dst', db, dst_t)}"
             + (f" where {dst_where}" if dst_where else ""))
    run = f"{kind}_execute"
    into = ", ".join(_ident(d) for _, _, d, _, _ in cols)
    picks = ", ".join(_cast(i, col, eng) for i, col in enumerate(cols))
    lake = (eng.src_engine.lake("src", db, src_t)
            if eng.src_engine.CANON_ENGINE == "parquet" else None)
    c = _duck(eng, db)
    try:
        if lake is not None:
            c.execute(f"load {lake[0]}")
        c.execute("begin")
        for pre in _session(eng, db):
            c.execute(f"call {run}('migkit_dst', {_literal(pre)})")
        c.execute(f"call {run}('migkit_dst', {_literal(empty)})")
        got = c.execute(
            f"insert into {_target_name(eng, db, dst_t)} ({into})"
            f" select {picks} from {_source_query(eng, db, src_t, cols, src_where)}"
            " as migkit_rows").fetchone()
        c.execute("commit")
    except BaseException:
        try:
            c.execute("rollback")
        except Exception:  # noqa: BLE001 - the error is the one to say
            pass
        raise
    finally:
        c.close()
    return int(got[0]) if got else 0


def _session(eng, db):
    """What the target's session is set to inside the transaction, as the
    table copier's own sessions are."""
    dst = eng.dst_engine
    if dst._d("dst", db) in dst.__dict__.get("_as_replica", ()):
        return ("set local session_replication_role = replica",)
    return ()


def _cast(i, col, eng):
    """The source's column `i` as the target column takes it exactly."""
    from . import loadtext
    name, stype, _, _, _ = col
    ref = f"migkit_rows.{_ident(name)}"
    got = READ.get((eng.src_engine.CANON_ENGINE, loadtext.base(stype)))
    return got[1].format(c=ref) if got else ref


# --- the source-rendered pipe ---------------------------------------------

def _rendered(eng, db, src_t, dst_t, cols, src_where, dst_where):
    """The source's lines in the target's load format into its load, the
    range emptied first, in one transaction on the target."""
    if eng.src_engine.CANON_ENGINE == "postgres":
        return _rendered_into_mysql(eng, db, src_t, dst_t, cols, src_where,
                                    dst_where)
    return _rendered_into_postgres(eng, db, src_t, dst_t, cols, src_where,
                                   dst_where)


def _rendered_into_postgres(eng, db, src_t, dst_t, cols, src_where,
                            dst_where):
    """MySQL's lines of COPY text into PostgreSQL."""
    from . import loadtext
    from .engines import pg_pass
    src, dst = eng.src_engine, eng.dst_engine
    sql = loadtext.select(src._qualified("src", db, src_t),
                          [(n, t, c) for n, t, _, _, c in cols], src_where)
    # the loading session as the same-engine pass-through's is set, and a
    # text read as UTF-8 and a time without a zone as UTC
    opts = (f"{pg_pass.LOAD_OPTIONS} -c TimeZone=UTC -c DateStyle=ISO"
            " -c client_encoding=UTF8")
    if dst._d("dst", db) in dst.__dict__.get("_as_replica", ()):
        opts += " -c session_replication_role=replica"
    table = dst._qualified("dst", db, dst_t)
    into = ", ".join(dst._quote_ident(d) for _, _, d, _, _ in cols)
    reader = src._conn("src")
    rows = 0
    try:
        with pg_pass.connect(dst, "dst", db, opts) as conn:
            with conn.transaction(), conn.cursor() as cur:
                cur.execute(f"delete from {table}"
                            + (f" where {dst_where}" if dst_where else ""))
                with cur.copy(f"copy {table} ({into}) from stdin") as cp:
                    for chunk, n in lines(reader, sql):
                        cp.write(chunk)
                        rows += n
    finally:
        reader.close()
    return rows


class _CopyOut:
    """PostgreSQL's `COPY ... TO STDOUT` as the stream MySQL's load reads
    (`read(n)`), the blocks passed on as they arrive, and the lines in
    them counted."""

    def __init__(self, copy):
        self.blocks = iter(copy)
        self.held = bytearray()
        self.lines = 0

    def read(self, n):
        while len(self.held) < n:
            block = next(self.blocks, None)
            if block is None:
                break
            block = bytes(block)
            self.lines += block.count(b"\n")
            self.held += block
        out = bytes(self.held[:n])
        del self.held[:n]
        return out


def _rendered_into_mysql(eng, db, src_t, dst_t, cols, src_where,
                         dst_where):
    """PostgreSQL's rows rendered as MySQL's load reads them
    (`loadtext.copy_out`) into the pinned `load data local`
    (`engines.mysql._load_feed`). The load's own answer holds it to what
    was sent: every line taken, no warning - a value MySQL took other than
    as it was sent is a warning, and the range goes back as it was."""
    from . import loadtext
    from .engines import pg_pass
    from .engines.mysql import _load_feed
    src, dst = eng.src_engine, eng.dst_engine
    sql, kinds = loadtext.copy_out(
        src._qualified("src", db, src_t),
        [(n, t, c) for n, t, _, _, c in cols], src_where)
    target = dst._d("dst", db)
    conn = dst._conn("dst")
    try:
        with pg_pass.connect(src, "src", db, loadtext.PG_SESSION) as reader:
            with reader.transaction(), reader.cursor() as rcur, \
                    rcur.copy(sql) as out, conn.cursor() as cur:
                cur.execute("set session time_zone = '+00:00'")
                # no gap locks: a range emptied under the default isolation
                # locked the gap past its end, which the next range's load
                # writes into - measured, four ranges side by side waited
                # on each other until one was chosen as a deadlock's victim
                cur.execute("set session transaction isolation level read"
                            " committed")
                cur.execute(f"delete from {dst._qualified('dst', db, dst_t)}"
                            + (f" where {dst_where}" if dst_where else ""))
                stream = _CopyOut(out)
                got = _load_feed(cur, target, dst_t,
                                 [d for _, _, d, _, _ in cols], kinds, stream)
                warned = int(cur.warning_count or 0)
        if warned or got != stream.lines:
            raise RuntimeError(
                f"the target's load took {got:,} of {stream.lines:,} rows"
                f" with {warned:,} warnings")
        conn.commit()
    except BaseException:
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001 - the error is the one to say
            pass
        raise
    finally:
        conn.close()
    return got


#: bytes of lines handed to COPY at once
CHUNK = 1 << 20


def lines(conn, sql):
    """(bytes of lines, how many) off a MySQL connection as its packets
    arrive: each row is one field, taken as the bytes it is and ended with
    a newline, no row object made for it."""
    from . import loadtext
    with conn.cursor() as cur:
        cur.execute(loadtext.SESSION)
    conn.query(sql, unbuffered=True)
    result = conn._result
    read = conn._read_packet
    buf, size, n = [], 0, 0
    try:
        while True:
            pkt = read()
            if pkt.is_eof_packet():
                result._check_packet_is_eof(pkt)
                result.unbuffered_active = False
                break
            line = pkt.read_length_coded_string()
            if line is None:
                raise RuntimeError("a row came back empty from the source's"
                                   " rendering")
            buf.append(line)
            size += len(line) + 1
            n += 1
            if size >= CHUNK:
                buf.append(b"")
                yield b"\n".join(buf), n
                buf, size, n = [], 0, 0
    finally:
        if result.unbuffered_active:
            # stopped early: what the server still sends is read and left
            result._finish_unbuffered_query()
    if buf:
        buf.append(b"")
        yield b"\n".join(buf), n

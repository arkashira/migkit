"""Numbers set by reasoning, tuned from what runs measure (backlog P0-DEPTH,
item 3, built with P0-SMART's knowledge base).

`canon.WIDE`, `ranges.BUCKET_ROWS`, `throttle.REPLICA_RISE`, the
PostgreSQL digest's memory, the probe floor (`model.PROBE_FROM`) and the
tie (`decide.TIE`) were each set once by reasoning about one machine. Each
is declared here (`TUNABLES`): today's value is its prior, read from the
module that holds it - one place for the number, as before - and a run
that measures what the value should have been says so (`observe`); the
value used from then on (`value`) is the geometric mean of the prior,
weighed as `PRIOR_WEIGHT` runs, and every run's measurement since - per
machine in the knowledge base (`costmodel.Knowledge`), per hop beside its
reports where the hop measured its own. Never further than `SPAN` times
from the prior either way: a run that measured something odd moves the
number, it does not throw it away.

Each declares what measures it (`how`); a module adopts its number by
reading `value(name)` where it read the constant, and says what it
measured with `observe(name, ...)`. Adopted so far: the probe floor and
the tie (the tie floors what two costs within their measured noise are
held to be, `decide._tied_estimates`); the others are declared with what
measures them, for the modules that hold them to adopt.
"""
import json
import math
import threading
from dataclasses import dataclass

#: the prior weighs as much as this many runs' measurements
PRIOR_WEIGHT = 3.0
#: a tuned value stays within this factor of its prior
SPAN = 8.0
#: where a hop keeps what its own runs measured
HOP_FILE = "tuned.json"


def _attr(module, name, owner=None):
    """A reader of today's value, from the module that holds it."""
    def read():
        import importlib
        mod = importlib.import_module("migkit." + module)
        obj = getattr(mod, owner) if owner else mod
        return float(getattr(obj, name))
    return read


#: who may move a number or a setting: migkit's own (`LOCAL`), a setting of
#: migkit's own server session (`SESSION`), the server's for everyone
#: (`GLOBAL`) or capacity that is paid for (`PAID`) - the last two only
#: where the operator authorized it (`may_set`)
LOCAL, SESSION, GLOBAL, PAID = "local", "session", "global", "paid"
OWNERS = (LOCAL, SESSION, GLOBAL, PAID)
#: the test file every inventoried scenario is a test of
SCENARIOS = "tests/test_autotune_every_dimension.py"


@dataclass(frozen=True)
class Tunable:
    name: str
    what: str
    prior: object          #: () -> today's value
    how: str               #: what a run measures it by
    adopted: bool = False  #: whether its module reads `value` yet
    #: the dimension of the work it sizes (`DIMENSIONS`)
    dimension: str = ""
    #: the facts that bound it, read before it moves (`SETTINGS`' names
    #: or an engine's facts)
    facts: tuple = ()
    #: (least, most) factors of its prior a tuning may take it to
    bounds: tuple = (1.0 / SPAN, SPAN)
    #: the guarantee it must never break, in words
    keeps: str = ""
    owner: str = LOCAL
    #: the test that proves it tuned (a test in `SCENARIOS`), or "" where
    #: its dimension's proof is owed (`UNPROVED`)
    scenario: str = ""


TUNABLES = {t.name: t for t in (
    Tunable("model.PROBE_FROM", "the least a database carries for the"
            " resources to be probed before its move",
            _attr("model", "PROBE_FROM"),
            "the bytes the move carried in the time its probes took: below"
            " that, the move takes about as long as the probes would",
            adopted=True, dimension="concurrency",
            keeps="a probe only reads"),
    Tunable("decide.TIE", "the share within which two measured costs are"
            " a tie", _attr("decide", "TIE"),
            "the relative noise of a unit's time about each way's fitted"
            " line, where a way ran enough units to say",
            adopted=True, dimension="conversion and validation",
            keeps="a tie only orders ways that hold the same guarantees"),
    Tunable("canon.WIDE", "the bytes past which a value is digested where"
            " it lies, not carried whole", _attr("canon", "WIDE"),
            "the largest value both servers digested in less time than"
            " carrying it took", adopted=True, dimension="large values",
            keeps="a digested value is compared by its length and its"
                  " whole md5, never a prefix"),
    Tunable("ranges.BUCKET_ROWS", "the rows of one bucket of a keyless"
            " table's hash", _attr("ranges", "BUCKET_ROWS"),
            "the rows one leaf scan hashed in the time a range is meant to"
            " take", adopted=True, dimension="partitions and ranges",
            keeps="the buckets cover every row once"),
    Tunable("throttle.REPLICA_RISE", "the seconds a replica may fall"
            " further behind while it is read", _attr("throttle",
                                                      "REPLICA_RISE"),
            "the rise in the replica's lag that went with its reads"
            " slowing", adopted=True, dimension="flow control",
            keeps="a read waits, it is never skipped"),
    Tunable("postgres.DIGEST_MEMORY", "the memory one PostgreSQL digest"
            " may hold", _attr("engines.postgres", "DIGEST_MEMORY",
                               "PostgresEngine"),
            "the most a digest was seen to hold, with room over it",
            adopted=True, dimension="queues and memory",
            facts=("work_mem",), owner=SESSION,
            keeps="a digest covers every row of its range"),
    Tunable("pieces.PIECE", "the bytes past which a large value is read"
            " and written a piece at a time", _attr("pieces", "PIECE"),
            "the piece the fastest measured value carried at, within the"
            " side's limits (`pieces.Sizing`)", dimension="large values",
            facts=("max_allowed_packet", "the largest result row"),
            keeps="every value arrives whole: pieces are joined, never cut"
                  " short", owner=SESSION,
            scenario="test_a_mis_sized_large_value_on_mysql_goes_in_pieces"),
    Tunable("pieces.BATCH_BYTES", "the bytes one batch of a copy holds",
            _attr("pieces", "BATCH_BYTES"),
            "the batch that carried its bytes fastest and was never refused"
            " for its size (`Retune`)", dimension="payload and statement",
            facts=("max_allowed_packet", "the expansion of a value as sent"),
            keeps="a statement carries whole rows",
            scenario="test_a_packet_refusal_halves_the_batch_and_completes"),
    Tunable("pieces.PAIR_ROWS", "the rows one read of the copier asks for",
            _attr("pieces", "PAIR_ROWS"),
            "the rows a batch held when it ran within its time and memory"
            " (`Retune`)", dimension="batch",
            facts=("statement_timeout", "innodb_lock_wait_timeout"),
            keeps="a batch commits in one transaction or not at all",
            scenario="test_a_timeout_on_postgres_cuts_the_batch_not_the"
                     "_rows"),
    Tunable("sizing.WORKER_MEMORY", "the memory one worker is counted to"
            " hold", _attr("sizing", "WORKER_MEMORY"),
            "the most a worker was seen to hold, with room over it",
            dimension="queues and memory", facts=("free memory here",),
            keeps="a worker's batch is held whole until it is written",
            scenario="test_memory_pressure_on_postgres_cuts_the_batch"
                     "_bytes"),
    Tunable("sizing.RUNNING_PER_CPU", "the queries running for each of a"
            " server's CPUs past which it is strained",
            _attr("sizing", "RUNNING_PER_CPU"),
            "the running queries per CPU at which a side's time a unit"
            " began to rise", dimension="concurrency",
            facts=("the server's CPUs", "connections free"),
            keeps="only schedules that keep each table's order run side by"
                  " side"),
    Tunable("throttle.MAX_WAIT_PER_UNIT", "the seconds a unit waits on a"
            " strained server before it goes on", _attr(
                "throttle", "MAX_WAIT_PER_UNIT"),
            "the waits after which the strain was gone",
            dimension="timeouts and retries", facts=("statement_timeout",),
            keeps="a unit waits, it is never dropped"),
    Tunable("decide.TRIAL_SECONDS", "the seconds a way never measured is"
            " boxed in on its one trial", _attr("decide", "TRIAL_SECONDS"),
            "the time measured ways took on a unit of the size tried",
            dimension="timeouts and retries",
            keeps="a trial past its box is undone, never half kept"),
)}

#: numbers that look like settings and are not: each is a bound a value's
#: correctness rests on, so no run's measurement moves it (`observe`
#: refuses them)
INVARIANTS = {
    "pieces.SENT_FACTOR": "a byte of a value is at most this many in a"
                          " statement's text: an upper bound, not a speed",
    "pieces.CHAR_BYTES": "a character is at most four bytes of UTF-8",
    "pieces.HEADROOM": "what a statement carries beside its value",
    "tuning.JSON_EXPANSION": "a control character in a JSON string is"
                             " written as six bytes",
    "the snapshot all units read at": "moving it would mix two points in"
                                      " time in one copy",
    "a batch's transaction": "a batch commits whole or not at all",
    "the order of changes on one key": "reordering changes the result",
}

_LOCK = threading.Lock()


def _prior(name):
    t = TUNABLES.get(name)
    if t is None:
        raise KeyError(f"no tunable {name!r}")
    return float(t.prior())


def _mean(prior, stats, bounds=(1.0 / SPAN, SPAN)):
    """The geometric mean of the prior (weighed `PRIOR_WEIGHT`) and the
    measurements (their weight and the sum of their logs), within
    `bounds`, factors of the prior (`SPAN` either way unless the tunable
    says tighter)."""
    n = float((stats or {}).get("n") or 0)
    logs = float((stats or {}).get("logs") or 0)
    if n <= 0 or prior <= 0:
        return prior
    got = math.exp((PRIOR_WEIGHT * math.log(prior) + logs)
                   / (PRIOR_WEIGHT + n))
    return min(max(got, prior * bounds[0]), prior * bounds[1])


def _hop_stats(hop, name):
    try:
        got = json.loads((hop.report_dir() / HOP_FILE).read_text())
    except (OSError, ValueError, AttributeError):
        return None
    return ((got or {}).get("params") or {}).get(name)


def value(name, hop=None):
    """`name`'s value here: the hop's own where its runs measured it, this
    machine's where other runs did, its prior otherwise."""
    prior = _prior(name)
    bounds = TUNABLES[name].bounds
    if hop is not None:
        mine = _hop_stats(hop, name)
        if mine and mine.get("n"):
            return _mean(prior, mine, bounds)
    from .costmodel import knowledge
    try:
        stats = (knowledge().data().get("params") or {}).get(name)
    except OSError:
        stats = None
    return _mean(prior, stats, bounds)


def observe(name, measured, hop=None, weight=1.0):
    """A run measured `measured` as what `name` should have been: kept for
    this machine, and for `hop` where given."""
    if name in INVARIANTS:
        raise ValueError(f"{name} is a bound correctness rests on, not a"
                         f" setting: {INVARIANTS[name]}")
    if name not in TUNABLES or not measured or measured <= 0:
        return
    add = {"n": float(weight), "logs": float(weight) * math.log(measured)}
    from .costmodel import knowledge

    def apply(got):
        params = got.setdefault("params", {})
        cur = params.setdefault(name, {"n": 0.0, "logs": 0.0})
        cur["n"] += add["n"]
        cur["logs"] += add["logs"]
    try:
        knowledge()._change(apply)
    except OSError:
        pass
    if hop is None:
        return
    from . import durable
    with _LOCK:
        try:
            durable.change_json(hop.report_dir() / HOP_FILE, apply)
        except (OSError, AttributeError):
            pass


def lines():
    """Every tunable with its value here and its prior, for `doctor`."""
    out = []
    for name, t in TUNABLES.items():
        try:
            prior, now = _prior(name), value(name)
        except Exception:  # noqa: BLE001 - a module not loadable here
            continue
        if abs(now - prior) > 1e-9 * max(prior, 1.0):
            out.append(f"{t.what}: {now:,.4g} here, tuned from {prior:,.4g}"
                       " by what runs here measured")
    return out


# --- every dimension a move is tuned in (backlog P0-AUTOTUNE-ALL) -----------

@dataclass(frozen=True)
class Dimension:
    """One dimension of the work that is tuned, not set: its numbers
    (`TUNABLES`), where the choice in it is made, the facts that bound
    it, the guarantee it never trades, who may change what it rests on
    and the test that proves it tuned."""
    name: str
    what: str
    decided_by: str
    facts: tuple = ()
    keeps: str = ""
    owner: str = LOCAL
    scenario: str = ""

    @property
    def tunables(self):
        return tuple(n for n, t in TUNABLES.items()
                     if t.dimension == self.name)


DIMENSIONS = {d.name: d for d in (
    Dimension("large values", "how a value wider than a batch is carried:"
              " whole, in pieces or streamed",
              "pieces.Sizing for the piece; tuning.payload_way for whole,"
              " pieces or a stream; tuning.Retune after a refusal",
              ("max_allowed_packet", "the largest result row",
               "the catalogue's longest value"),
              "every value arrives whole: never cut short, never truncated",
              SESSION,
              "test_a_mis_sized_large_value_on_mysql_goes_in_pieces"),
    Dimension("payload and statement", "the bytes one statement carries,"
              " counted as sent: escaped, in hex or serialised",
              "tuning.Retune", ("max_allowed_packet", "statement size"),
              "a statement carries whole rows", SESSION,
              "test_a_packet_refusal_halves_the_batch_and_completes"),
    Dimension("batch", "the rows one transaction writes", "tuning.Retune",
              ("statement_timeout", "innodb_lock_wait_timeout"),
              "a batch commits in one transaction or not at all", LOCAL,
              "test_a_timeout_on_postgres_cuts_the_batch_not_the_rows"),
    Dimension("partitions and ranges", "how a table's keys are cut into"
              " units", "ranges, and tuning.Retune.rebalance on measured"
              " skew", ("the rows of each range",),
              "the units cover the key space once: no gap, no overlap",
              LOCAL, "test_a_skewed_range_is_cut_where_it_is_slow"),
    Dimension("concurrency", "how many units run at once", "sizing.Pace",
              ("CPUs here and on each server", "connections free"),
              "only schedules that keep each table's order run side by"
              " side", LOCAL),
    Dimension("queues and memory", "what a worker and a queue hold",
              "sizing.Memory, tuning.Retune after memory ran out",
              ("free memory here", "work_mem"),
              "a batch is held whole until it is written", SESSION,
              "test_memory_pressure_on_postgres_cuts_the_batch_bytes"),
    Dimension("timeouts and retries", "how long a unit may take and how"
              " often it is tried", "attempts.Monitor, decide.attempt,"
              " tuning.Retune after a timeout",
              ("statement_timeout", "net_read_timeout",
               "wal_sender_timeout"),
              "a unit is reconciled before it runs again, never doubled",
              SESSION,
              "test_a_timeout_on_postgres_cuts_the_batch_not_the_rows"),
    Dimension("flow control", "how fast a side is read or written while it"
              " is strained", "sizing.Limit and throttle",
              ("the replica's lag", "the server's load"),
              "a unit waits, it is never skipped", LOCAL),
    Dimension("transport and compression", "how bytes cross the link",
              "the way's rung in decide.WAYS", ("the link's round trip",),
              "what arrives is what was read", LOCAL),
    Dimension("conversion and validation", "how values are converted and"
              " how the copy is proved", "decide.space and verifiers",
              ("the declared types on both sides",),
              "every row is proved, never a sample in place of all",
              LOCAL),
    Dimension("consistency boundary", "the point in time every unit reads"
              " at", "guarantees", ("the source's snapshot or position",),
              "one point in time for the whole copy: an invariant, only its"
              " cost is tuned", LOCAL),
    Dimension("session settings", "settings of migkit's own sessions, and"
              " the server's own where authorized", "tuning.may_set",
              ("each setting's scope as the server says it",),
              "a global or paid change only with authorization", GLOBAL,
              "test_a_global_setting_waits_for_authorization"),
)}

#: dimensions no scenario proves yet, and what is owed
UNPROVED = {d.name: "a scenario that moves it from a measured cause is"
            " owed; end-to-end on the sandbox's servers as well"
            for d in DIMENSIONS.values() if not d.scenario}
#: what every scenario here proves on the fake servers of its test, and
#: that the sandbox's own servers have not yet shown
SANDBOX_OWED = ("every dimension end-to-end on the sandbox's servers, with"
                " user intervention, time, resource use and retry waste"
                " measured against a fixed configuration and plain retries")


@dataclass(frozen=True)
class Setting:
    """A server setting a dimension rests on: whose it is (`SESSION`:
    migkit's own sessions, set by migkit; `GLOBAL`: the server's, changed
    only with authorization) and what it bounds."""
    engine: str
    name: str
    scope: str
    bounds: str
    dimension: str


SETTINGS = (
    Setting("mysql", "max_allowed_packet", GLOBAL, "one statement or result"
            " row, in bytes", "payload and statement"),
    Setting("mysql", "net_read_timeout", SESSION, "seconds a read of the"
            " client waits", "timeouts and retries"),
    Setting("mysql", "net_write_timeout", SESSION, "seconds a write to the"
            " client waits", "timeouts and retries"),
    Setting("mysql", "innodb_lock_wait_timeout", SESSION, "seconds a row"
            " lock is waited for", "batch"),
    Setting("postgres", "statement_timeout", SESSION, "seconds one"
            " statement runs", "timeouts and retries"),
    Setting("postgres", "lock_timeout", SESSION, "seconds a lock is waited"
            " for", "batch"),
    Setting("postgres", "work_mem", SESSION, "memory one sort or hash"
            " holds", "queues and memory"),
    Setting("postgres", "wal_sender_timeout", GLOBAL, "seconds a"
            " replication connection may stay silent", "timeouts and"
            " retries"),
    Setting("mssql", "lock_timeout", SESSION, "milliseconds a lock is"
            " waited for", "batch"),
)


def setting(engine, name):
    return next((s for s in SETTINGS
                 if s.engine == engine and s.name == name), None)


def may_set(engine, name, authorized=()):
    """None where migkit may change `name` on `engine` itself, otherwise
    why not: a global setting changes the server for everyone and waits
    for the operator's authorization (`authorized`, names)."""
    got = setting(engine, name)
    if got is None:
        return f"{name} on {engine} is not a setting migkit knows the scope of"
    if got.scope == SESSION or name in set(authorized or ()):
        return None
    return (f"{name} on {engine} is the server's own setting, for every"
            " session: it changes only where the operator authorizes it")


# --- a large value's bound, and the way it is carried ----------------------

#: bytes one byte of a value takes at most as a statement carries it:
#: escaped or in hex (`pieces.SENT_FACTOR`), and for JSON a control
#: character written as `\u00XX`, six bytes for one, then escaped
JSON_EXPANSION = 6
#: the longest value of a declared type each engine's catalogue promises,
#: in bytes (a length in characters is counted as `pieces.CHAR_BYTES`)
CATALOGUE = {
    "mysql": {"tinyblob": 255, "tinytext": 255, "blob": 65535,
              "text": 65535, "mediumblob": 16777215,
              "mediumtext": 16777215, "longblob": 4294967295,
              "longtext": 4294967295},
    "postgres": {"bytea": 1 << 30, "text": 1 << 30, "json": 1 << 30,
                 "jsonb": 1 << 30, "xml": 1 << 30},
}
#: declared types whose stored size does not bound their text as sent
_SERIALISED = ("json", "jsonb", "xml")


@dataclass(frozen=True)
class Bound:
    """The most bytes one value of a column takes: as stored (`most`) and
    as a statement sends it (`sent`), `certified` where the catalogue
    promises it or every row was measured - a sampled largest is said but
    certifies nothing."""
    most: int
    sent: int
    certified: bool
    said: str


def _family(engine):
    name = str(engine or "").lower()
    return "mysql" if name in ("mysql", "mariadb", "tidb", "oceanbase",
                               "polardbx", "dcdb") else name


def catalogue_bound(engine, declared):
    """The catalogue's promise for one declared type, in bytes, or None."""
    import re
    from . import pieces
    typ = "".join(str(declared or "").split()).lower()
    got = re.fullmatch(r"(var)?(char|binary)\((\d+)\)", typ)
    if got:
        n = int(got.group(3))
        return n * (pieces.CHAR_BYTES if got.group(2) == "char" else 1)
    return CATALOGUE.get(_family(engine), {}).get(typ.split("(")[0])


def payload_bound(engine, declared, sized=None):
    """The `Bound` of one column's values: the catalogue's promise, cut to
    the largest measured over every row (`sized`: `Engine.row_sizes`'
    entry, its "exact" true where every row was read). A JSON or XML
    column's stored size does not bound its text, so only the catalogue
    certifies it."""
    from . import pieces
    typ = str(declared or "").split("(")[0].strip().lower()
    expand = pieces.SENT_FACTOR * (JSON_EXPANSION if typ in _SERIALISED
                                   else 1)
    cat = catalogue_bound(engine, declared)
    sized = sized or {}
    largest = int(sized.get("largest") or 0)
    exact = bool(sized.get("exact")) and typ not in _SERIALISED
    if exact and largest:
        most = min(largest, cat) if cat else largest
        why = (f"the largest of every row, {most:,} bytes"
               if most == largest else
               f"the catalogue's {most:,} bytes for {declared}")
        return Bound(most, most * expand, True, why)
    if cat:
        why = f"the catalogue's {cat:,} bytes for {declared}"
        if largest:
            why += (f"; a sample's largest, {largest:,} bytes, certifies"
                    " nothing")
        return Bound(cat, cat * expand, True, why)
    return Bound(largest, largest * expand, False,
                 f"a sample's largest, {largest:,} bytes: no catalogue bound"
                 " and not every row read, so no value is known to fit")


def _fits(bound, limit):
    from . import pieces
    if limit.most <= 0 or limit.bounds == "row":
        return True
    need = bound.sent if limit.bounds == "sent" else bound.most
    return need + pieces.HEADROOM <= limit.most


def payload_way(bound, limits, appendable=False, streams=False,
                engine="", costs=None, authorized=()):
    """The `decide.Choice` of how a column's values are carried: whole in
    a statement (`whole`), only where a certified bound fits every limit
    as sent; written a piece at a time (`pieces`) where the target
    appends to a value; through a load that streams a value (`stream`)
    where the target has one. None of them holding is a `decide.Blocked`
    naming each limit and the setting that would carry them whole, with
    who may change it."""
    import math
    from . import decide, pieces
    limits = tuple(limits or ())
    tight = [x for x in limits if not _fits(bound, x)]
    rungs = (
        decide.Rung("whole", "each value whole in its row's statement",
                    needs=(decide.Need(
                        "a bound every value is known to keep",
                        lambda f: f["bound"].certified,
                        lambda f: f"no value is known to fit:"
                                  f" {f['bound'].said}"),
                        decide.Need(
                        "every limit to take the widest value as sent",
                        lambda f: not f["tight"],
                        lambda f: f"the widest value, {f['bound'].sent:,}"
                                  " bytes as sent, is past "
                                  + ", ".join(x.said() for x in f["tight"]))),
                    because="one statement a row: the fewest round trips"),
        decide.Rung("pieces", "each large value written a piece at a time",
                    needs=(decide.Need(
                        "a target that adds a piece to a value",
                        lambda f: f["appendable"],
                        "the target's type takes no piece added to it"),)),
        decide.Rung("stream", "each large value streamed through the"
                    " target's bulk load",
                    needs=(decide.Need(
                        "a bulk load that streams a value",
                        lambda f: f["streams"],
                        "the target has no load that streams a value"),)),
    )
    piece = max(1, min((x.most // max(pieces.SENT_FACTOR, 1)
                        for x in limits if x.most > 0
                        and x.bounds == "sent"), default=pieces.PIECE))
    priced = dict(costs or {})
    priced.setdefault("whole", 1.0)
    priced.setdefault("stream", 1.5)
    priced.setdefault("pieces", 1.0 + math.ceil(max(bound.most, 1) / piece))
    facts = decide.Facts({"bound": bound, "tight": tight,
                          "appendable": bool(appendable),
                          "streams": bool(streams)})
    got = decide.climb("a large value's way", rungs, facts, costs=priced)
    if got.rung is not None:
        return got
    raise decide.Blocked(blocker(bound, tight, engine, authorized, got))


def blocker(bound, tight, engine, authorized=(), choice=None):
    """What stops a value being carried, and what would let it."""
    from . import pieces
    why = "; ".join(w for _, w in (choice.passed if choice else ()))
    out = f"no way carries these values whole: {why or bound.said}"
    for x in tight:
        if x.bounds != "sent":
            continue
        want = bound.sent + pieces.HEADROOM
        held = may_set(engine, x.name, authorized)
        out += (f". {x.name} = {want:,} on the target would carry them"
                + (f" - {held}" if held else ", and migkit sets it for"
                   " its own sessions"))
    return out + ". Nothing is cut short to fit"


def engine_payload(eng, side, db, table, column, streams=False,
                   authorized=()):
    """`payload_way` for one column of a table, from what the engine says:
    its declared type, the table's sizes (`Engine.row_sizes`), the side's
    limits (`Engine.piece_limits`) and whether the type takes a piece
    added (`LARGE_VALUES.appendable`)."""
    from . import pieces
    declared = pieces.declared_of(eng, side, db, table).get(column)
    try:
        sized = (eng.row_sizes(side, db, [table]) or {}).get(table)
    except Exception:  # noqa: BLE001 - sized from the catalogue alone
        sized = None
    engine = type(eng).__module__.rsplit(".", 1)[-1]
    way = getattr(eng, "LARGE_VALUES", None)
    appendable = bool(way is not None and hasattr(way, "appendable")
                      and way.appendable(declared))
    bound = payload_bound(engine, declared, sized)
    return payload_way(bound, pieces._limits_of(eng, side, db), appendable,
                       streams, engine, authorized=authorized)


# --- a unit's failure, its cause, and what is changed for it ---------------

PACKET, TIMEOUT, MEMORY, LOCK, CONNECTION, UNKNOWN = (
    "packet", "timeout", "memory", "lock", "connection", "unknown")
#: causes a size answers: the same work again would fail the same way
SIZING = (PACKET, TIMEOUT, MEMORY, LOCK)
#: the failure catalogue's classes (`faults.CATALOGUE`) a size answers,
#: and the connection's, which a known limit may explain
BY_CLASS = {"packet": PACKET, "statement_timeout": TIMEOUT,
            "memory_pressure": MEMORY, "lock": LOCK,
            "connectivity": CONNECTION, "interruption": CONNECTION}


@dataclass(frozen=True)
class Cause:
    kind: str
    said: str

    @property
    def sizing(self):
        return self.kind in SIZING


def cause_of(error, sent=None, limits=()):
    """The `Cause` of a unit's failure from what the server said. A
    connection lost while sending more than a known limit takes is that
    limit: measured on MySQL 8.4, a row past `max_allowed_packet` ended
    in a lost connection that said nothing of the packet."""
    from . import faults
    text = f"{type(error).__name__}: {error}" if isinstance(
        error, BaseException) else str(error or "")
    told = faults.classify(error if isinstance(error, BaseException)
                           else RuntimeError(text))
    kind = BY_CLASS.get(told.fault.name, UNKNOWN)
    over = [x for x in limits or () if sent and x.bounds == "sent"
            and 0 < x.most < sent]
    if kind in (CONNECTION, UNKNOWN) and over:
        return Cause(PACKET, f"{sent:,} bytes sent past {over[0].said()},"
                             " and the connection was lost")
    return Cause(kind, text.strip()[-200:] or kind)


@dataclass(frozen=True)
class Adjustment:
    """What `Retune` changed after a unit failed: the cause, the way
    taken (`rung`), the settings before and after, and the rows the unit
    already holds durably, from which it goes on (`resume_from`)."""
    unit: str
    cause: Cause
    rung: str
    said: str
    before: dict
    after: dict
    resume_from: int = 0
    passed: tuple = ()


@dataclass(frozen=True)
class Verified:
    """A unit after a tuning: the fault gone (`ok`) only where it ended
    well and its rows were counted on the target as the source holds them
    - an error that stopped is not a copy that is whole."""
    unit: str
    ok: bool
    why: str


class Retune:
    """The sizes one work's units run at, moved by what failed them.

    A unit that fails for a cause a size answers (`SIZING`) is never run
    again as it was: the ways that would answer the cause are costed on
    what is left of the unit and one decision (`decide.climb`) picks - a
    smaller batch by rows or bytes, its values in pieces, streamed, the
    unit cut into ranges, or another way the caller has. What the unit
    already holds is reconciled first (`reconcile`) and it goes on from
    there. A size that failed is a ceiling: no later growth reaches it,
    growth waits `STEADY` units without a failure, and every growth that
    fails again doubles that wait - so the sizes settle instead of
    swinging. A fact read off the server (a `pieces.Limit`) caps them
    before anything is tried. Where no way answers, `decide.Blocked` says
    the cause and what would let the work go on, never a silent cut."""

    #: units without a failure before a size grows back
    STEADY = 4
    #: what one growth multiplies a size by
    GROW = 1.5
    #: a unit whose predicted time is this many times the median is cut
    SKEW = 2.0

    def __init__(self, work, rows=None, nbytes=None, limits=(),
                 appendable=False, streams=False, other=None, log=None,
                 hop=None, engine="", authorized=()):
        from . import pieces
        self.work, self.log, self.hop = work, log, hop
        self.engine, self.authorized = engine, tuple(authorized or ())
        self.limits = tuple(limits or ())
        self.appendable, self.streams, self.other = (
            bool(appendable), bool(streams), other)
        self.prior = {"rows": int(rows or value("pieces.PAIR_ROWS")),
                      "bytes": int(nbytes or value("pieces.BATCH_BYTES"))}
        cap = self._fact_cap()
        if cap:
            self.prior["bytes"] = min(self.prior["bytes"], cap)
        self.size = dict(self.prior)
        self.size.update({"way": "whole", "piece": pieces.PIECE,
                          "parts": 1})
        self.ceiling = {}
        self.calm, self.steady = 0, self.STEADY
        self.history, self.open, self.verified = [], {}, []
        self.rate = None
        self.units = {}

    def _fact_cap(self):
        """The bytes a batch may hold as sent under the limits read off the
        server, or None where none says."""
        from . import pieces
        got = [(x.most - pieces.HEADROOM) // pieces.SENT_FACTOR
               for x in self.limits if x.bounds == "sent" and x.most > 0]
        return max(1, min(got)) if got else None

    def settings(self):
        return dict(self.size)

    # --- a failure ------------------------------------------------------------

    def failed(self, unit, error, sent=None, rows=None, widest=None,
               reconcile=None):
        """The `Adjustment` for `unit` that failed with `error` on a batch
        of `sent` bytes in `rows` rows (`widest` its widest row), or
        None where the cause is not one a size answers (the caller's
        resilience retries it). `reconcile(unit)` says how many of the
        unit's rows the target holds durably."""
        import math
        from . import decide, pieces
        cause = cause_of(error, int(sent) * pieces.SENT_FACTOR
                         if sent else None, self.limits)
        self._unit(unit, state="failed")
        if not cause.sizing:
            return None
        done = int(reconcile(unit) or 0) if reconcile else 0
        before = self.settings()
        rows = max(1, int(rows or before["rows"]))
        sent = int(sent or before["bytes"])
        widest = int(widest or math.ceil(sent / rows))
        left = max(1, rows - done)
        cap = self._fact_cap()
        halve_bytes = max(1, min(sent // 2, cap or sent))
        if cause.kind == PACKET and cap:
            halve_bytes = min(halve_bytes, cap)
        plans = {
            "smaller batch": {"rows": max(1, rows // 2) if cause.kind in (
                TIMEOUT, LOCK) else max(1, int(rows * halve_bytes / sent)),
                "bytes": halve_bytes if cause.kind in (PACKET, MEMORY)
                else before["bytes"]},
            "pieces": {"way": "pieces", "rows": 1, "piece": max(
                1, min(cap or pieces.PIECE, pieces.PIECE,
                       before["piece"] // 2 if before["way"] == "pieces"
                       else pieces.PIECE))},
            "stream": {"way": "stream"},
            "smaller ranges": {"parts": before["parts"] * 2},
            "other way": {"way": "other"},
        }
        one_row_past = cause.kind == PACKET and widest * pieces.SENT_FACTOR \
            + pieces.HEADROOM > (cap or sent) * pieces.SENT_FACTOR
        facts = decide.Facts({
            "cause": cause.kind, "rows": rows, "one_row_past": one_row_past,
            "appendable": self.appendable, "streams": self.streams,
            "other": self.other is not None})

        def differs(name):
            return lambda f: any(before.get(k) != v
                                 for k, v in plans[name].items())

        rate = self.rate or 1.0
        trip = 1.0 / max(rate, 1e-9) * pieces.HEADROOM

        def cost(name):
            p = {**before, **plans[name]}
            per = max(1, p["rows"] // p["parts"])
            # each range a unit is cut into is planned over its keys anew
            trips = math.ceil(left / per) + p["parts"] - before["parts"]
            if p["way"] == "pieces":
                trips = left * math.ceil(widest / max(p["piece"], 1))
            return trips * trip + left * widest / max(rate, 1e-9)

        rungs = (
            decide.Rung("smaller batch", "a smaller batch", needs=(
                decide.Need("a batch of more than one row, or one that is"
                            " not one row past the limit",
                            lambda f: not f["one_row_past"] and (
                                f["rows"] > 1 or f["cause"] == MEMORY),
                            "one row alone is past what the server takes"),
                decide.Need("a size other than the one that failed",
                            differs("smaller batch"),
                            "it would run the same batch again"))),
            decide.Rung("pieces", "each large value written a piece at a"
                        " time", needs=(
                decide.Need("a refusal of a large value's size",
                            lambda f: f["cause"] in (PACKET, MEMORY),
                            "pieces answer a value's size, not a wait"),
                decide.Need("a target that adds a piece to a value",
                            lambda f: f["appendable"],
                            "the target's type takes no piece added to it"),
                decide.Need("pieces not already tried",
                            differs("pieces"), "pieces failed already"))),
            decide.Rung("stream", "each large value streamed through the"
                        " target's bulk load", needs=(
                decide.Need("a bulk load that streams a value",
                            lambda f: f["streams"],
                            "the target has no load that streams a value"),
                decide.Need("a stream not already tried", differs("stream"),
                            "streaming failed already"))),
            decide.Rung("smaller ranges", "the unit cut into smaller"
                        " ranges", needs=(
                decide.Need("a wait that a shorter unit answers",
                            lambda f: f["cause"] in (TIMEOUT, LOCK),
                            "a shorter unit does not answer a size"),
                decide.Need("rows to cut", lambda f: f["rows"] > 1,
                            "the unit is one row"))),
            decide.Rung("other way", "an equivalent other way of this"
                        " work", needs=(
                decide.Need("another way the work has",
                            lambda f: f["other"],
                            "the work has no other way"),
                decide.Need("another way not already tried",
                            differs("other way"), "it failed already"))),
        )
        got = decide.climb(f"{self.work}: after {cause.kind}", rungs, facts,
                           costs={r.name: cost(r.name) for r in rungs})
        if got.rung is None:
            raise decide.Blocked(
                f"{self.work}: {unit} failed - {cause.said}; nothing that"
                " answers it holds: "
                + "; ".join(w for _, w in got.passed)
                + self._raise_words(cause, widest))
        self.size.update(plans[got.rung.name])
        self._ceil(cause.kind, before)
        self.calm = 0
        adj = Adjustment(unit, cause, got.rung.name, got.rung.said, before,
                         self.settings(), done,
                         tuple((r.name, w) for r, w in got.passed))
        self.history.append(adj)
        self.open[unit] = adj
        if self.log is not None:
            self.log(f"{self.work}: {unit} failed - {cause.kind}: "
                     f"{got.rung.said} for what is left, from row"
                     f" {done:,}")
        return adj

    def _raise_words(self, cause, widest):
        from . import pieces
        if cause.kind != PACKET:
            return ""
        want = widest * pieces.SENT_FACTOR + pieces.HEADROOM
        held = may_set(self.engine, "max_allowed_packet", self.authorized)
        return (f". max_allowed_packet = {want:,} on the target would carry"
                " the widest row" + (f" - {held}" if held else ""))

    def _ceil(self, kind, before):
        """The size that failed for `kind`: what no growth reaches again."""
        key = "rows" if kind in (TIMEOUT, LOCK) else "bytes"
        was = self.ceiling.get(key)
        self.ceiling[key] = before[key] if was is None else min(
            was, before[key])
        if self._grew:
            self.steady *= 2
        self._grew = False

    _grew = False

    # --- a success -------------------------------------------------------------

    def succeeded(self, unit, rows, nbytes, seconds, expected=None,
                  counted=None):
        """`unit` ended well: `rows` rows of `nbytes` bytes in `seconds`.
        `expected` the rows the source holds for it, `counted` those the
        target holds after it. The `Verified` of a unit a tuning was for
        says whether the fault is gone with the unit whole; every unit
        moves the rate the costs are measured at, and, after `STEADY`
        quiet ones, the sizes back toward their priors."""
        if seconds and nbytes:
            got = float(nbytes) / max(float(seconds), 1e-9)
            self.rate = got if self.rate is None else (
                0.7 * self.rate + 0.3 * got)
        whole = expected is not None and counted is not None \
            and int(counted) == int(expected)
        self._unit(unit, state="done", rows=rows, nbytes=nbytes,
                   seconds=seconds)
        out = None
        adj = self.open.pop(unit, None)
        if adj is not None:
            why = ("the fault is gone and the unit holds every row" if whole
                   else "it ended well, but its rows were not counted whole"
                   " on the target" if counted is not None else
                   "it ended well; its rows are not yet counted, so the"
                   " tuning is not proved")
            out = Verified(unit, whole, why)
            self.verified.append(out)
            if whole and adj.cause.kind in (PACKET, MEMORY):
                observe("pieces.BATCH_BYTES", self.size["bytes"], self.hop)
            elif whole:
                observe("pieces.PAIR_ROWS", self.size["rows"], self.hop)
        if not whole and expected is not None:
            return out
        self.calm += 1
        if self.calm >= self.steady:
            self._grow()
        return out

    def _grow(self):
        changed = False
        for key in ("rows", "bytes"):
            ceil = self.ceiling.get(key)
            top = self.prior[key] if ceil is None else min(
                self.prior[key], int(ceil / self.GROW))
            got = min(top, int(self.size[key] * self.GROW))
            if got > self.size[key]:
                self.size[key], changed = got, True
        self.calm = 0
        self._grew = changed

    # --- skew, and the forecast ------------------------------------------------

    def rebalance(self, done, pending):
        """{unit: parts} for the `pending` units ({unit: rows}) predicted
        to take more than `SKEW` times the median unit, at the rows a
        second the `done` ones ({unit: (rows, seconds)}) measured - each
        cut into as many parts as bring it to the median. Nothing where
        too little was measured."""
        import math
        import statistics
        times = [s for r, s in done.values() if r and s]
        rates = [r / s for r, s in done.values() if r and s]
        if len(rates) < 2:
            return {}
        rate, median = statistics.median(rates), statistics.median(times)
        out = {}
        for name, rows in pending.items():
            took = float(rows or 0) / max(rate, 1e-9)
            if took > self.SKEW * median:
                out[name] = int(math.ceil(took / max(median, 1e-9)))
        return out

    def _unit(self, name, **kw):
        from .util import Unit
        was = self.units.get(name) or Unit(name, "move", attempts=0)
        rows, nbytes = kw.pop("rows", None), kw.pop("nbytes", None)
        upd = dict(kw, attempts=was.attempts + 1)
        if rows is not None:
            upd.update(rows=float(rows), done_rows=float(rows))
        if nbytes is not None:
            upd["bytes"] = float(nbytes)
        self.units[name] = was._replace(**upd)

    def forecast(self, pending=(), **kw):
        """`util.forecast` over the units run so far and `pending`
        (`util.Unit`s): a retuned unit counted with its attempts."""
        from .util import forecast
        return forecast(list(self.units.values()) + list(pending), **kw)


def sized(retune, unit, fn, apply, reconcile=None, sent=None, rows=None,
          most_rounds=8):
    """fn() run at `retune`'s sizes (`apply(settings)` before each try). A
    failure a size answers is retuned (`Retune.failed`) and the work goes
    again at the new size - never at the one that failed; any other
    failure is raised to the caller's resilience. `sent()`/`rows()` say
    what the failed try carried, where known."""
    from . import decide
    for rounds in range(most_rounds + 1):
        apply(retune.settings())
        try:
            return fn()
        except decide.Blocked:
            raise
        except Exception as e:  # noqa: BLE001 - its cause decides
            if rounds == most_rounds:
                raise
            adj = retune.failed(unit, e, sent=sent() if sent else None,
                                rows=rows() if rows else None,
                                reconcile=reconcile)
            if adj is None:
                raise
    raise decide.Blocked(f"{retune.work}: {unit} did not settle")


def run(retune, units, write, reconcile, count, size=None, most_rounds=64):
    """Each of `units` ({name: rows in key order}) written through
    `write(name, batch, settings)` in batches cut at `retune`'s sizes;
    a failure goes to `retune.failed`, which reconciles what the target
    holds (`reconcile(name)`: rows written durably, in key order) and the
    unit goes on from there at the sizes it chose. `count(name)` is the
    rows the target holds for a unit once it ends. {name: `Verified` or
    None}. A unit that fails more than `most_rounds` times is blocked."""
    import time
    from . import decide, pieces
    size = size or pieces.row_bytes
    out = {}
    for name, rows in units.items():
        at, rounds = 0, 0
        began, nbytes = time.monotonic(), 0
        while at < len(rows):
            s = retune.settings()
            most = max(1, s["rows"] // s["parts"]) \
                if s["way"] == "whole" else 1
            batch = next(iter(pieces.by_bytes(rows[at:], s["bytes"], most,
                                              size)))
            sent = sum(size(r) for r in batch)
            try:
                write(name, batch, s)
            except Exception as e:  # noqa: BLE001 - classified below
                rounds += 1
                if rounds > most_rounds:
                    raise decide.Blocked(f"{retune.work}: {name} failed"
                                         f" {rounds} times: {e}") from e
                adj = retune.failed(
                    name, e, sent=sent, rows=len(batch),
                    widest=max(size(r) for r in batch),
                    reconcile=reconcile)
                if adj is None:
                    raise
                at = adj.resume_from
                continue
            at += len(batch)
            nbytes += sent
        out[name] = retune.succeeded(name, len(rows), nbytes,
                                     time.monotonic() - began,
                                     expected=len(rows), counted=count(name))
    return out

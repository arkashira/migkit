"""One routine run on one side of the sandbox, and everything it did
written down (backlog R11).

A call is compared by what the caller and the database can see of it: the
value a function returns, a procedure's OUT and INOUT values and the
result sets it sends, the error it raises (by SQLSTATE class; by code and
message where the routine raised it itself), the rows it changed in every
table it writes - read by key, in the one rendering both engines are held
to (`canon`) - and how far it moved the tables' counters. Each call runs in
a transaction rolled back after it, or - for a routine that commits for
itself - on its own, with the tables it writes put back from the fixture.

A MySQL procedure that fails keeps the writes its earlier statements made
until its caller rolls back; a PostgreSQL CALL that fails keeps none. The
rows are read after a failure too, so that difference is seen, not missed.

A routine that reads the clock is compared exactly, not within a
tolerance, wherever the two sessions can be made to read one instant: a
MySQL session's clock is set (`SET TIMESTAMP`, which `NOW()`, `CURDATE()`,
`UTC_TIMESTAMP()` and `UNIX_TIMESTAMP()` answer from - `SYSDATE()` does
not) and a PostgreSQL call's transaction time (what `now()` and
`current_timestamp` answer) is read in the call's own transaction, so a
MySQL side is set to the PostgreSQL side's instant; two PostgreSQL sides
are compared by each value's distance from its own side's instant. Only a
clock no session can set or read ahead of the call - `SYSDATE()`,
`clock_timestamp()`, SQL Server's and Oracle's - is compared within five
seconds, and said.
"""
import datetime as _dt
import re
from dataclasses import dataclass, field
from decimal import Decimal

from .. import canon


#: seconds one call may take: a version that loops where the source's
#: returns is a version that differs, not one to wait on
CALL_SECONDS = 30
#: the comment every function call the proof makes carries, so the
#: source's record of its calls tells the proof's own (on the hop's own
#: servers, read-only) from the application's (`history`)
MARK = "/* migkit proof */"


class HarnessError(RuntimeError):
    """The sandbox could not run the call at all - not the routine's
    answer, and never compared as one."""


def render_any(value):
    """A value of a column no declaration names (a result set's), in the
    canonical text by what it is."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float):
        return canon.render_value("float", value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex().upper()
    if isinstance(value, _dt.datetime):
        return canon.render_value("timestamp", value)
    if isinstance(value, _dt.date):
        return canon.render_value("date", value)
    if isinstance(value, _dt.time):
        return canon.render_value("time", value)
    if isinstance(value, _dt.timedelta):
        # MySQL's TIME, as the driver hands it back
        total = value.total_seconds()
        sign = "-" if total < 0 else ""
        total = abs(total)
        h, rest = divmod(total, 3600)
        m, s = divmod(rest, 60)
        return f"{sign}{int(h):02d}:{int(m):02d}:{s:09.6f}"
    if isinstance(value, (dict, list)):
        return canon.json_text(value)
    return str(value)


def render(cls, value):
    """A value by the class its declaration gives it, falling back to
    what it is where the driver hands back something the class's renderer
    does not take (MySQL's TIME as an interval)."""
    if value is None:
        return None
    if cls:
        try:
            return canon.render_value(cls, value)
        except Exception:  # noqa: BLE001 - rendered by what it is instead
            pass
    return render_any(value)


@dataclass
class Outcome:
    """What one call did, as the caller and the database can see it."""
    value: object = None
    outs: tuple = ()
    sets: tuple = ()
    error: tuple = None           #: (SQLSTATE, message) or None
    changes: dict = field(default_factory=dict)
    counters: dict = field(default_factory=dict)
    #: statements a runner had to leave out of the comparison, and why
    notes: tuple = ()
    #: the instant the call's clock answered from - set by the runner
    #: (MySQL) or read in the call's own transaction (PostgreSQL); None
    #: where neither could be done
    now: object = None


#: SQLSTATE classes that name a failure the routine raised itself; their
#: exact code and message are compared, not only the class
RAISED = ("45", "P0")


def error_key(error):
    """What of an error is compared: the class, or for one the routine
    raised itself its code and message."""
    if error is None:
        return None
    state, message = error
    state = str(state or "")
    if state[:2] in RAISED and state not in SYSTEM_RAISED:
        return (state, str(message or ""))
    if state in SYSTEM_RAISED:
        return (state,)
    return (state[:2] or "?",)


#: PL/pgSQL's own conditions in the class a routine raises with: no row,
#: too many rows, an assertion - each engine says them in its own words,
#: so they are compared by their code alone
SYSTEM_RAISED = ("P0002", "P0003", "P0004")


#: a class that says only "an error": two sides erroring with it errored
#: alike, but not provably for the same reason
UNKNOWN_CLASSES = ("HY", "?")


def differences(a, b, clock=False):
    """[what differs] between two outcomes, source first; [] when equal.
    `clock`: False - every value exactly (no clock, or both sessions read
    one instant); RELATIVE - a time value equal to the other's at the
    same distance from its own side's instant (`a.now`, `b.now`), exactly;
    True - time values within five seconds of each other are equal (the
    routine reads a clock neither session can set or read)."""
    out = []

    def same(x, y):
        if x == y:
            return True
        if clock == RELATIVE and isinstance(x, str) and isinstance(y, str):
            return _same_distance(x, y, a.now, b.now)
        if clock is True and isinstance(x, str) and isinstance(y, str):
            return _close_times(x, y)
        if isinstance(x, (list, tuple)) and isinstance(y, (list, tuple)):
            return len(x) == len(y) and all(same(p, q) for p, q in zip(x, y))
        if isinstance(x, dict) and isinstance(y, dict):
            return x.keys() == y.keys() and all(same(x[k], y[k]) for k in x)
        return False
    ea, eb = error_key(a.error), error_key(b.error)
    if ea != eb and not (ea and eb and ea[0] in UNKNOWN_CLASSES
                         and eb[0] in UNKNOWN_CLASSES):
        out.append(f"error: {_said_error(a.error)} against"
                   f" {_said_error(b.error)}")
    if not same(a.value, b.value):
        out.append(f"returned {_said(a.value)} against {_said(b.value)}")
    if not same(list(a.outs), list(b.outs)):
        out.append(f"OUT values {list(a.outs)} against {list(b.outs)}")
    if not same(list(a.sets), list(b.sets)):
        out.append(f"result sets {_short(a.sets)} against {_short(b.sets)}")
    for t in sorted(set(a.changes) | set(b.changes)):
        x, y = a.changes.get(t) or {}, b.changes.get(t) or {}
        if not same(x, y):
            out.append(f"rows changed in {t}: {_short(x)} against"
                       f" {_short(y)}")
    # a failed insert takes a number on one engine and not on another; how
    # far a counter moved is compared where the call did its work
    both_failed = a.error is not None and b.error is not None
    for t in sorted(set(a.counters) | set(b.counters)):
        if not both_failed and a.counters.get(t, 0) != b.counters.get(t, 0):
            out.append(f"{t}'s counter moved {a.counters.get(t, 0)} against"
                       f" {b.counters.get(t, 0)}")
    return out


def _said(v):
    return "NULL" if v is None else repr(v)


def _said_error(e):
    if e is None:
        return "no error"
    return f"{e[0]} ({str(e[1] or '')[:60]})"


def _short(x, most=160):
    text = repr(x)
    return text if len(text) <= most else text[:most] + "..."


_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2}(\.\d+)?)?$")


def _close_times(x, y, seconds=5):
    if not (_TIME.match(x) and _TIME.match(y)):
        return False
    try:
        a = _dt.datetime.fromisoformat(x)
        b = _dt.datetime.fromisoformat(y)
    except ValueError:
        return False
    return abs((a - b).total_seconds()) <= seconds


def _instant(value):
    """A call's instant as a naive UTC datetime (the sessions run in UTC),
    or None."""
    if isinstance(value, _dt.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(_dt.timezone.utc).replace(tzinfo=None)
        return value
    return None


def _same_distance(x, y, at_x, at_y):
    """Whether two rendered time values stand at the same distance from
    their own side's instant, at the precision they are rendered in: a
    value cut to the second is measured from its instant cut to the
    second, so `now()` cut on both sides is one distance, not the
    fraction of a second the two servers' transactions began apart."""
    at_x, at_y = _instant(at_x), _instant(at_y)
    if at_x is None or at_y is None:
        return False
    if not (_TIME.match(x) and _TIME.match(y)):
        return False
    try:
        vx = _dt.datetime.fromisoformat(x)
        vy = _dt.datetime.fromisoformat(y)
    except ValueError:
        return False
    if len(x) == 10 and len(y) == 10:
        return (vx.date() - at_x.date()) == (vy.date() - at_y.date())
    digits = max(len(v.partition(".")[2]) for v in (x, y))

    def cut(t):
        step = 10 ** (6 - digits)
        return t.replace(microsecond=t.microsecond - t.microsecond % step)
    return (vx - cut(at_x)) == (vy - cut(at_y))


#: the clock mode that compares each time value by its distance from its
#: own side's instant (`differences`)
RELATIVE = "relative"

#: per engine, the clock functions its session answers from an instant the
#: harness sets or reads; anything else in `CLOCKS` reads the wall clock
#: as the statement runs
SET_OR_READ = {
    "mysql": r"now|curdate|curtime|current_date|current_time|"
             r"current_timestamp|utc_timestamp|utc_date|utc_time|localtime|"
             r"localtimestamp|unix_timestamp",
    "postgres": r"now|current_date|current_time|current_timestamp|"
                r"localtime|localtimestamp|transaction_timestamp",
}
CLOCKS = (r"now|sysdate|curdate|curtime|current_date|current_time|"
          r"current_timestamp|utc_timestamp|utc_date|utc_time|localtime|"
          r"localtimestamp|unix_timestamp|getdate|getutcdate|sysdatetime|"
          r"sysutcdatetime|sysdatetimeoffset|systimestamp|clock_timestamp|"
          r"statement_timestamp|transaction_timestamp|timeofday")


def reads_clock(text):
    return bool(re.search(rf"(?i)\b({CLOCKS})\b", text or ""))


def wall_clock(text, engine):
    """Whether a body reads a clock its engine's session cannot be set to
    or read ahead of the call: every clock it reads that is not one of
    `SET_OR_READ`'s."""
    mine = SET_OR_READ.get(engine)
    if mine is None:
        return reads_clock(text)
    for m in re.finditer(rf"(?i)\b({CLOCKS})\b", text or ""):
        if not re.fullmatch(rf"(?i)(?:{mine})", m.group(1)):
            return True
    return False


def clock_mode(a, b, source_body, target_body, commits=False):
    """How a routine's calls on runners `a` (source) and `b` (target) meet
    the clock: (mode, first, reads). `mode`: "pinned" (both sessions
    read one instant: exact), RELATIVE, True (within five seconds) or None
    (exact, nothing set); `first` the side called first, whose instant the
    other is set to ("a" or "b"), or None; `reads` whether either body
    reads a clock itself.

    The sessions are set to one instant wherever they can be, whether the
    body reads a clock or not: a trigger its write fires, or a function
    it calls, may - and `updated_at = NOW()` in a trigger answered a
    millisecond apart on the two servers is not a difference."""
    reads = reads_clock(source_body) or reads_clock(target_body)
    if reads and (wall_clock(source_body, a.engine) or
                  wall_clock(target_body, b.engine)):
        return True, None, True
    # a PostgreSQL routine that commits for itself starts a transaction
    # of its own at each commit, and its clock moves with it
    readable = {r: r.READS_CLOCK and not (commits and r.engine ==
                                          "postgres") for r in (a, b)}
    if a.SETS_CLOCK and b.SETS_CLOCK:
        return "pinned", None, reads
    if a.SETS_CLOCK and readable[b]:
        return "pinned", "b", reads
    if b.SETS_CLOCK and readable[a]:
        return "pinned", "a", reads
    if readable[a] and readable[b]:
        return RELATIVE, None, reads
    return (True if reads else None), None, reads


def epoch(instant):
    """An instant as MySQL's `SET TIMESTAMP` takes it: seconds since the
    epoch as an exact decimal literal - a float of today's seconds keeps
    15 or 16 digits, one short of the microseconds."""
    at = _instant(instant)
    whole = at.replace(microsecond=0) - _dt.datetime(1970, 1, 1)
    return f"{whole.days * 86400 + whole.seconds}.{at.microsecond:06d}"


def reads_chance(text):
    return bool(re.search(r"(?i)\b(rand|random|uuid|uuid_short|newid|"
                          r"gen_random_uuid|dbms_random)\s*\(", text or ""))


# ---- the tables a call is watched through ----------------------------------


@dataclass
class Watched:
    """One fixture table as one side names and reads it."""
    name: str                     #: this side's table
    columns: list                 #: [(column, class)], aligned across sides
    key: list                     #: this side's key columns (names)
    counter: str = None           #: the column a counter hands out, if any
    generated: list = None        #: columns the server writes itself


#: what a value a counter handed out is compared as
GENERATED = "<handed out by the table's counter>"


def row_changes(before, after, key_at, generated_at=None):
    """{"+": rows added, "-": rows gone, "~": rows changed} between two
    readings of a table, each a list of rendered rows. By key where the
    table has one; as multisets where it has none. A value the table's
    counter handed out (`generated_at`) is not compared in an added row:
    which row got which number follows the order the rows were written
    in, which neither engine promises; how far the counter moved is
    compared instead (`Outcome.counters`)."""
    if key_at:
        def keyed(rows):
            return {tuple(r[i] for i in key_at): r for r in rows}
        b, a = keyed(before), keyed(after)

        def masked(row):
            row = list(row)
            if generated_at is not None:
                row[generated_at] = GENERATED
            return row
        added = sorted((masked(a[k]) for k in a.keys() - b.keys()),
                       key=repr)
        gone = sorted((list(k) for k in b.keys() - a.keys()), key=repr)
        changed = sorted(([list(k), list(a[k])] for k in a.keys() & b.keys()
                          if a[k] != b[k]), key=repr)
    else:
        from collections import Counter
        b, a = Counter(map(tuple, before)), Counter(map(tuple, after))
        added = sorted((list(r) for r in (a - b).elements()), key=repr)
        gone = sorted((list(r) for r in (b - a).elements()), key=repr)
        changed = []
    out = {}
    if added:
        out["+"] = added
    if gone:
        out["-"] = gone
    if changed:
        out["~"] = changed
    return out


@dataclass
class _Named:
    """A routine known only by its kind and name - what a drop needs."""
    kind: str
    name: str
    settings: dict = field(default_factory=dict)


class Runner:
    """Calls on one sandbox server of one engine, over one connection."""

    engine = None
    #: whether the session's clock can be set to an instant for a call
    #: (`at`), and whether the instant a call's clock answers from can be
    #: read in the call's own transaction (`Outcome.now`)
    SETS_CLOCK = False
    READS_CLOCK = False
    #: the instant the next calls' clock is set to, where it can be
    at = None
    #: calls held in one transaction, one after another, each seeing what
    #: the ones before it did (`hold`, `sequences`); otherwise each call
    #: starts from the fixture and is rolled back
    held = False

    def __init__(self, eng, db, watched, side="dst", readonly=False):
        self.eng = eng
        self.db = db
        self.watched = watched            #: {source table: Watched}
        #: the side of `eng` to run on: a sandbox server is its "dst"; a
        #: hop's own server is only ever asked read-only
        self.side = side
        self.readonly = readonly
        self.base = {}
        self.raw = {}
        self.conn = None
        self.coverage_on = False

    # ---- what every engine gives ----------------------------------------

    def open(self):
        raise NotImplementedError

    def close(self):
        if self.conn is not None:
            try:
                self.conn.rollback()
            except Exception:  # noqa: BLE001 - closing anyway
                pass
            self.conn.close()
            self.conn = None

    def q(self, name):
        raise NotImplementedError

    def table(self, name):
        return ".".join(self.q(p) for p in str(name).split("."))

    def read(self, w):
        """(rendered rows, raw rows) of one watched table, in this
        connection's transaction."""
        cols = ", ".join(self.q(c) for c, _ in w.columns)
        with self.conn.cursor() as cur:
            cur.execute(f"select {cols} from {self.table(w.name)}")
            raw = [list(r) for r in cur.fetchall()]
        return [[render(c, v) for (_, c), v in zip(w.columns, r)]
                for r in raw], raw

    def baseline(self):
        for t, w in self.watched.items():
            self.base[t], self.raw[t] = self.read(w)
        self.end()

    def key_at(self, w):
        names = [c for c, _ in w.columns]
        return [names.index(k) for k in w.key if k in names] \
            if w.key and all(k in names for k in w.key) else []

    def changes(self, tables):
        out = {}
        for t in tables:
            w = self.watched[t]
            now, _ = self.read(w)
            names = [c for c, _ in w.columns]
            at = names.index(w.counter) if w.counter in names else None
            got = row_changes(self.base[t], now, self.key_at(w), at)
            if got:
                out[t] = got
        return out

    def end(self):
        self.conn.rollback()

    # ---- calls one after another --------------------------------------------

    def hold(self):
        """Every call from now on in one transaction, none rolled back
        until `release`: the counters set from the fixture once, here."""
        self.reset_counters(list(self.watched))
        self.held = True
        self._begin(force=True)

    def release(self):
        """The held calls rolled back, and the session begun afresh: what
        they left in it (variables, temporary tables, a package's state)
        must not reach the next call."""
        self.held = False
        self.end()
        self.fresh()

    def fresh(self):
        self.close()
        self.open()

    def _from_fixture(self, tables):
        if not self.held:
            self.reset_counters(tables)

    def _begin(self, force=False):
        # MySQL's session is in autocommit's own mode between calls; the
        # other drivers begin a transaction with the first statement
        if self.engine == "mysql" and (force or not self.held):
            self.conn.begin()

    def _done(self):
        if not self.held:
            self.end()

    # ---- per engine ------------------------------------------------------

    def create(self, statements, settings=None):
        raise NotImplementedError

    def drop(self, routine):
        raise NotImplementedError

    #: the objects a CREATE makes, by kind and name
    MADE = re.compile(r'(?is)\bcreate\s+(?:or\s+replace\s+)?(?:definer\s*=\s*'
                      r'\S+\s+)?(?:editionable\s+)?(function|procedure|'
                      r'trigger)\s+((?:"[^"]+"|`[^`]+`|\[[^\]]+\]|[\w$#]+)'
                      r'(?:\s*\.\s*(?:"[^"]+"|`[^`]+`|\[[^\]]+\]|[\w$#]+))*)')

    def clear(self, statements):
        """Whatever `statements` would make, dropped first: a candidate's
        version may name its helpers otherwise than the one before it did,
        and a helper left over refuses the next CREATE of that name."""
        for s in statements:
            for kind, name in self.MADE.findall(s):
                leaf = re.split(r"\s*\.\s*", name)[-1].strip('"`[]')
                try:
                    self.drop(_Named(kind.lower(), leaf))
                except Exception:  # noqa: BLE001 - nothing of that name
                    self.conn.rollback()

    def call(self, routine, args, tables, commits=False):
        raise NotImplementedError

    def run(self, statement, params, tables, rows=False):
        """A DML statement (a trigger's firing) and what it changed; with
        `rows`, the rows it answered too (an application's statement,
        `statements`), as one result set."""
        raise NotImplementedError

    def counters(self, tables):
        return {}

    def reset_counters(self, tables):
        pass

    def restore(self, tables):
        """The fixture's rows back in `tables`, after a call that
        committed."""
        raise NotImplementedError


#: MySQL's refusals sent with the class that says only "an error" (HY000)
#: or a warning's (01000), by error number, as the SQLSTATE PostgreSQL -
#: and through `MSSQL_STATES` and `ORACLE_STATES` the others - refuses the
#: same failure with: a CHECK refused (3819), a value no enum or column
#: of its type holds (1265, 1366), a column with no default left out
#: (1364). Compared by class, each would otherwise answer alike with no
#: other engine's
MYSQL_STATES = {3819: "23514", 1265: "22P02", 1366: "22P02", 1364: "23502"}


class MySQLRunner(Runner):
    engine = "mysql"
    SETS_CLOCK = True

    def open(self):
        import pymysql
        ep = self.eng.hop.target if self.side == "dst" else \
            self.eng.hop.source
        self.conn = pymysql.connect(
            host=ep.host, port=ep.port, user=ep.user, password=ep.password,
            database=self.eng._d(self.side, self.db), charset="utf8mb4",
            autocommit=False, connect_timeout=15,
            read_timeout=CALL_SECONDS * 2,
            client_flag=pymysql.constants.CLIENT.MULTI_RESULTS,
            **ep.mysql_tls())
        self.thread = self.conn.thread_id()
        with self.conn.cursor() as cur:
            cur.execute("set time_zone = '+00:00'")
            cur.execute("set session information_schema_stats_expiry = 0")
            # a function called by a SELECT stops at this; a CALL does not,
            # and is stopped by the read timeout and a KILL (`_lost`)
            cur.execute(f"set session max_execution_time ="
                        f" {CALL_SECONDS * 1000}")
        return self

    def _lost(self):
        """The call outlived the read timeout: its query is killed from a
        connection of its own and this one opened again, so the next call
        is not queued behind it."""
        import pymysql
        ep = self.eng.hop.target if self.side == "dst" else \
            self.eng.hop.source
        try:
            admin = pymysql.connect(host=ep.host, port=ep.port, user=ep.user,
                                    password=ep.password, connect_timeout=15,
                                    **ep.mysql_tls())
            with admin.cursor() as cur:
                cur.execute(f"kill {int(self.thread)}")
            admin.close()
        except Exception:  # noqa: BLE001 - opened again all the same
            pass
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001 - already gone
            pass
        self.open()

    def q(self, name):
        return "`" + str(name).replace("`", "``") + "`"

    def table(self, name):
        return self.q(str(name).split(".")[-1])

    def _error(self, e):
        """(SQLSTATE, message) of the error `e` just raised, as the server
        sent it with the error itself. Not the diagnostics area's first
        condition: measured, a call that rounded an argument (a warning,
        01000) and then divided by zero (22012) had the warning first. A
        refusal MySQL sends with a class that says only "an error" is
        given the one every other engine refuses the same with
        (`MYSQL_STATES`)."""
        args = getattr(e, "args", ())
        message = str(args[1] if len(args) > 1 else e)
        state = getattr(e, "sqlstate", None)
        number = args[0] if args and isinstance(args[0], int) else None
        if number in MYSQL_STATES and str(state or "?")[:2] in ("HY",
                                                                "01", "?"):
            state = MYSQL_STATES[number]
        return (str(state) if state else "?", message)

    def columns_made(self, table):
        """(the column a counter hands out, [columns written by the
        server]) of one of this side's tables."""
        with self.conn.cursor() as cur:
            cur.execute("select column_name, extra from"
                        " information_schema.columns where table_schema ="
                        " database() and table_name = %s",
                        (str(table).split(".")[-1],))
            rows = cur.fetchall()
        self.conn.commit()
        counter = next((c for c, x in rows
                        if "auto_increment" in str(x).lower()), None)
        return counter, [c for c, x in rows
                         if "generated" in str(x).lower()]

    def create(self, statements, settings=None):
        mode = (settings or {}).get("sql_mode")
        with self.conn.cursor() as cur:
            if mode is not None:
                cur.execute("set session sql_mode = %s", (str(mode),))
            for s in statements:
                cur.execute(s)
        self.conn.commit()

    def drop(self, routine):
        kind = "procedure" if routine.kind in ("procedure", "event") \
            else routine.kind
        with self.conn.cursor() as cur:
            cur.execute(f"drop {kind} if exists {self.q(routine.name)}")
        self.conn.commit()

    def counters(self, tables):
        out = {}
        with self.conn.cursor() as cur:
            for t in tables:
                w = self.watched[t]
                if not w.counter:
                    continue
                cur.execute("select auto_increment from"
                            " information_schema.tables where table_schema"
                            " = database() and table_name = %s",
                            (str(w.name).split(".")[-1],))
                got = cur.fetchone()
                out[t] = int(got[0] or 0) if got else 0
        return out

    def reset_counters(self, tables):
        # below the rows it is raised to past them, which is where the
        # fixture left it: every call starts from the same counter
        with self.conn.cursor() as cur:
            for t in tables:
                w = self.watched[t]
                if w.counter:
                    cur.execute(f"alter table {self.table(w.name)}"
                                " auto_increment = 1")
        self.conn.commit()

    def _sets(self, cur):
        sets = []
        while True:
            if cur.description is not None:
                sets.append([list(r) for r in cur.fetchall()])
            if not cur.nextset():
                break
        return sets

    def call(self, routine, args, tables, commits=False):
        out = Outcome()
        self._from_fixture(tables)
        before = self.counters(tables)
        self.conn.autocommit(commits)
        if self.readonly:
            with self.conn.cursor() as cur:
                cur.execute("start transaction read only")
        elif not commits:
            self._begin()
        name = self.q(routine.name)
        self._pin(out)
        try:
            with self.conn.cursor() as cur:
                if routine.kind == "function":
                    cur.execute(f"select {MARK} {name}"
                                f"({', '.join(['%s'] * len(args))})",
                                tuple(args))
                    out.value = cur.fetchone()[0]
                else:
                    places, params, outs = [], [], []
                    for k, p in enumerate(routine.params):
                        if p.mode == "in":
                            places.append("%s")
                            params.append(args[k])
                        else:
                            var = f"@migkit_o{k}"
                            cur.execute(f"set {var} = %s",
                                        (args[k] if p.mode == "inout"
                                         else None,))
                            places.append(var)
                            outs.append(var)
                    cur.execute(f"call {name}({', '.join(places)})",
                                tuple(params))
                    out.sets = self._sets(cur)
                    if outs:
                        cur.execute(f"select {', '.join(outs)}")
                        out.outs = list(cur.fetchone())
        except Exception as e:  # noqa: BLE001 - the routine's own answer
            if not _is_server_error(e):
                self._lost()
                if self.readonly:
                    raise HarnessError(str(e)) from e
                # it did not return in time: that is its answer
                out.error = ("57014", f"no answer within {CALL_SECONDS * 2}"
                                      " seconds")
                return out
            out.error = self._error(e)
        self._unpin()
        out.changes = self.changes(tables)
        after = self.counters(tables)
        out.counters = {t: after[t] - before.get(t, 0) for t in after
                        if after[t] != before.get(t, 0)}
        if commits:
            self.restore(tables)
        else:
            self._done()
        self.conn.autocommit(False)
        return out

    def _pin(self, out):
        """The session's clock set to `at` for the call - a session's own
        setting, which writes nothing and outlives no connection."""
        if self.at is None:
            return
        with self.conn.cursor() as cur:
            cur.execute(f"set timestamp = {epoch(self.at)}")
        out.now = self.at

    def _unpin(self):
        if self.at is None:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute("set timestamp = default")
        except Exception:  # noqa: BLE001 - a connection opened again
            pass           # after a lost call has its own clock

    def run(self, statement, params, tables, rows=False):
        out = Outcome()
        self._from_fixture(tables)
        before = self.counters(tables)
        self._begin()
        self._pin(out)
        try:
            with self.conn.cursor() as cur:
                cur.execute(statement, tuple(params))
                if rows and cur.description is not None:
                    out.sets = [[list(r) for r in cur.fetchall()]]
        except Exception as e:  # noqa: BLE001 - the routine's own answer
            if not _is_server_error(e):
                raise HarnessError(str(e)) from e
            out.error = self._error(e)
        self._unpin()
        out.changes = self.changes(tables)
        after = self.counters(tables)
        out.counters = {t: after[t] - before.get(t, 0) for t in after
                        if after[t] != before.get(t, 0)}
        self._done()
        return out

    def restore(self, tables):
        with self.conn.cursor() as cur:
            cur.execute("set foreign_key_checks = 0")
            for t in tables:
                w = self.watched[t]
                cur.execute(f"delete from {self.table(w.name)}")
                if self.raw[t]:
                    cols = ", ".join(self.q(c) for c, _ in w.columns)
                    marks = ", ".join(["%s"] * len(w.columns))
                    cur.executemany(f"insert into {self.table(w.name)}"
                                    f" ({cols}) values ({marks})",
                                    [tuple(r) for r in self.raw[t]])
            cur.execute("set foreign_key_checks = 1")
        self.conn.commit()

    # ---- coverage: the probes an instrumented copy writes ------------------

    def coverage_reset(self):
        from .mysql_pg import COVER_TABLE
        with self.conn.cursor() as cur:
            cur.execute(f"delete from {COVER_TABLE}")
        self.conn.commit()

    def probes_hit(self, routine):
        from .mysql_pg import COVER_TABLE
        with self.conn.cursor() as cur:
            cur.execute(f"select distinct probe from {COVER_TABLE} where"
                        " routine = %s", (routine.name,))
            return {int(r[0]) for r in cur.fetchall()}


class PostgresRunner(Runner):
    engine = "postgres"
    READS_CLOCK = True

    def open(self):
        self.conn = self.eng._conn(self.side,
                                   self.eng._d(self.side, self.db))
        if self.readonly:
            self.conn.set_session(readonly=True)
        with self.conn.cursor() as cur:
            cur.execute("set timezone = 'UTC'")
            cur.execute("set datestyle = 'ISO'")
            cur.execute("set extra_float_digits = 3")
            cur.execute(f"set statement_timeout = '{CALL_SECONDS}s'")
            self.coverage_on = False
            if not self.readonly:
                # the profiler, where the static check is installed in
                # this database: loaded first, since a setting of that
                # name is taken by any server whether it is there or not
                cur.execute("select 1 from pg_extension where extname ="
                            " 'plpgsql_check'")
                if cur.fetchone():
                    cur.execute("load 'plpgsql_check'")
                    cur.execute("set plpgsql_check.profiler = on")
                    self.coverage_on = True
        self.conn.commit()
        return self

    def q(self, name):
        return '"' + str(name).replace('"', '""') + '"'

    def columns_made(self, table):
        with self.conn.cursor() as cur:
            cur.execute("select a.attname, a.attidentity <> '' or"
                        " coalesce(pg_get_expr(d.adbin, d.adrelid), '') like"
                        " 'nextval(%%', a.attgenerated <> '' from"
                        " pg_attribute a left join pg_attrdef d on"
                        " d.adrelid = a.attrelid and d.adnum = a.attnum"
                        " where a.attrelid = %s::regclass and a.attnum > 0"
                        " and not a.attisdropped", (self.table(table),))
            rows = cur.fetchall()
        self.conn.commit()
        counter = next((c for c, ident, _ in rows if ident), None)
        return counter, [c for c, _, gen in rows if gen]

    def create(self, statements, settings=None):
        with self.conn.cursor() as cur:
            for s in statements:
                cur.execute(s)
        self.conn.commit()

    def drop(self, routine):
        with self.conn.cursor() as cur:
            if routine.kind == "trigger":
                cur.execute(
                    "select format('drop trigger if exists %%I on %%s',"
                    " t.tgname, t.tgrelid::regclass) from pg_trigger t"
                    " where t.tgname = %s and not t.tgisinternal",
                    (routine.name,))
                for (stmt,) in cur.fetchall():
                    cur.execute(stmt)
            schema, _, leaf = routine.name.rpartition(".") \
                if getattr(routine, "settings", {}).get("package") \
                else ("", "", routine.name)
            cur.execute(
                "select format('drop routine if exists %%s cascade',"
                " p.oid::regprocedure) from pg_proc p join pg_namespace n"
                " on n.oid = p.pronamespace where p.proname = %s and"
                " n.nspname not in ('pg_catalog', 'information_schema')"
                " and (%s = '' or n.nspname = %s)", (leaf, schema, schema))
            for (stmt,) in cur.fetchall():
                cur.execute(stmt)
        self.conn.commit()

    def _sequences(self, tables):
        out = {}
        for t in tables:
            w = self.watched[t]
            if w.counter:
                out[t] = w
        return out

    def counters(self, tables):
        out = {}
        with self.conn.cursor() as cur:
            for t, w in self._sequences(tables).items():
                cur.execute("select pg_get_serial_sequence(%s, %s)",
                            (self.table(w.name), w.counter))
                seq = cur.fetchone()[0]
                if not seq:
                    continue
                cur.execute(f"select case when is_called then last_value + 1"
                            f" else last_value end from {seq}")
                out[t] = int(cur.fetchone()[0])
        return out

    def reset_counters(self, tables):
        with self.conn.cursor() as cur:
            for t, w in self._sequences(tables).items():
                cur.execute(
                    "select setval(pg_get_serial_sequence(%s, %s),"
                    f" coalesce((select max({self.q(w.counter)}) from"
                    f" {self.table(w.name)}), 0) + 1, false)",
                    (self.table(w.name), w.counter))
        self.conn.commit()

    def _fetch_cursor(self, cur, name):
        cur.execute(f"fetch all from {self.q(name)}")
        return [list(r) for r in cur.fetchall()]

    def routine_name(self, routine):
        """A routine's name to call it by: a package's member in its
        package's schema (`packages`)."""
        if routine.settings.get("package") and "." in routine.name:
            return ".".join(self.q(p) for p in routine.name.split(".", 1))
        return self.q(routine.name)

    def call(self, routine, args, tables, commits=False):
        import psycopg2
        out = Outcome()
        self._from_fixture(tables)
        before = self.counters(tables)
        if commits:
            # a routine that commits for itself runs outside a transaction
            # of the proof's (a counter read is kept whatever ends it)
            self.conn.rollback()
            self.conn.autocommit = True
        name = self.routine_name(routine)
        cur = self.conn.cursor()
        try:
            if not commits:
                # the instant `now()` answers in this transaction, which
                # the call runs in
                cur.execute("select now()")
                out.now = cur.fetchone()[0]
                cur.execute("savepoint migkit_call")
            if routine.kind == "function":
                casts = [f"cast(%s as {p.type})" for p in routine.params]
                call = f"{name}({', '.join(casts)})"
                if routine.returns_set:
                    cur.execute(f"select {MARK} * from {call}", tuple(args))
                    out.sets = [[list(r) for r in cur.fetchall()]]
                else:
                    cur.execute(f"select {MARK} {call}", tuple(args))
                    out.value = cur.fetchone()[0]
            else:
                places, params = [], []
                for k, p in enumerate(routine.params):
                    if p.mode == "out":
                        places.append(f"cast(null as {p.type})")
                    else:
                        places.append(f"cast(%s as {p.type})")
                        params.append(args[k] if k < len(args) else None)
                cur.execute(f"call {name}({', '.join(places)})",
                            tuple(params))
                row = cur.fetchone() if cur.description else None
                outs = [p for p in routine.params if p.mode != "in"]
                values = list(row or [None] * len(outs))
                kept = []
                for p, v in zip(outs, values):
                    if p.type == "refcursor":
                        if v is not None:
                            out.sets = list(out.sets) + [
                                self._fetch_cursor(cur, v)]
                    else:
                        kept.append(v)
                out.outs = kept
        except psycopg2.Error as e:
            if e.pgcode is None:
                raise HarnessError(str(e)) from e
            out.error = (e.pgcode, (e.diag.message_primary or str(e)))
            if not commits:
                cur.execute("rollback to savepoint migkit_call")
        finally:
            cur.close()
        if commits:
            self.conn.autocommit = False
        out.changes = self.changes(tables)
        after = self.counters(tables)
        out.counters = {t: after[t] - before.get(t, 0) for t in after
                        if after[t] != before.get(t, 0)}
        if commits:
            self.restore(tables)
        else:
            self._done()
        return out

    def run(self, statement, params, tables, rows=False):
        import psycopg2
        out = Outcome()
        self._from_fixture(tables)
        before = self.counters(tables)
        cur = self.conn.cursor()
        try:
            cur.execute("select now()")
            out.now = cur.fetchone()[0]
            cur.execute("savepoint migkit_call")
            cur.execute(statement, tuple(params))
            if rows and cur.description is not None:
                out.sets = [[list(r) for r in cur.fetchall()]]
        except psycopg2.Error as e:
            if e.pgcode is None:
                raise HarnessError(str(e)) from e
            out.error = (e.pgcode, (e.diag.message_primary or str(e)))
            cur.execute("rollback to savepoint migkit_call")
        finally:
            cur.close()
        out.changes = self.changes(tables)
        after = self.counters(tables)
        out.counters = {t: after[t] - before.get(t, 0) for t in after
                        if after[t] != before.get(t, 0)}
        self._done()
        return out

    def restore(self, tables):
        with self.conn.cursor() as cur:
            cur.execute("set session_replication_role = replica")
            for t in tables:
                w = self.watched[t]
                cur.execute(f"delete from {self.table(w.name)}")
                if self.raw[t]:
                    cols = ", ".join(self.q(c) for c, _ in w.columns)
                    marks = ", ".join(["%s"] * len(w.columns))
                    cur.executemany(f"insert into {self.table(w.name)}"
                                    f" ({cols}) overriding system value"
                                    f" values ({marks})",
                                    [tuple(r) for r in self.raw[t]])
            cur.execute("set session_replication_role = origin")
        self.conn.commit()

    # ---- coverage: the static check's profiler -----------------------------

    def oids(self, routine):
        with self.conn.cursor() as cur:
            if routine.kind == "trigger":
                cur.execute("select t.tgfoid from pg_trigger t where"
                            " t.tgname = %s and not t.tgisinternal",
                            (routine.name,))
            else:
                schema, _, leaf = routine.name.rpartition(".") \
                    if routine.settings.get("package") \
                    else ("", "", routine.name)
                cur.execute("select p.oid from pg_proc p join pg_namespace"
                            " n on n.oid = p.pronamespace where p.proname"
                            " = %s and n.nspname not in ('pg_catalog',"
                            " 'information_schema') and (%s = '' or"
                            " n.nspname = %s)", (leaf, schema, schema))
            got = [r[0] for r in cur.fetchall()]
        self.conn.commit()
        return got

    def language(self, oid):
        with self.conn.cursor() as cur:
            cur.execute("select l.lanname from pg_proc p join pg_language l"
                        " on l.oid = p.prolang where p.oid = %s", (oid,))
            got = cur.fetchone()
        self.conn.commit()
        return got[0] if got else None

    def check(self, routine):
        """What the static check finds wrong in the routine made here -
        [(level, message, line)]; fail closed: an unreadable answer is a
        finding."""
        out = []
        for oid in self.oids(routine):
            if self.language(oid) != "plpgsql":
                continue
            with self.conn.cursor() as cur:
                try:
                    if routine.kind == "trigger":
                        # with the transition tables the trigger names,
                        # which the function reads as relations
                        cur.execute(
                            "select t.tgrelid, t.tgoldtable, t.tgnewtable"
                            " from pg_trigger t where t.tgname = %s and"
                            " t.tgfoid = %s", (routine.name, oid))
                        rel, old, new = cur.fetchone()
                        cur.execute("select level, message, lineno from"
                                    " plpgsql_check_function_tb("
                                    "%s::oid::regprocedure,"
                                    " %s::oid::regclass, oldtable => %s,"
                                    " newtable => %s)", (oid, rel, old, new))
                    else:
                        cur.execute("select level, message, lineno from"
                                    " plpgsql_check_function_tb("
                                    "%s::oid::regprocedure)",
                                    (oid,))
                    out += [(lv, m, ln) for lv, m, ln in cur.fetchall()]
                except Exception as e:  # noqa: BLE001 - fail closed
                    self.conn.rollback()
                    out.append(("error", f"the static check could not"
                                         f" read it: {str(e)[:100]}", None))
                    continue
            self.conn.commit()
        return out

    def coverage_reset(self):
        with self.conn.cursor() as cur:
            try:
                cur.execute("select plpgsql_profiler_reset_all()")
            except Exception:  # noqa: BLE001 - nothing to reset
                self.conn.rollback()
                return
        self.conn.commit()

    def coverage(self, routine):
        """(statements reached, statements, the share of branches taken or
        None where there are none, [(line, what)] never reached) over the
        routine's PL/pgSQL, as the static check's profiler counts them; or
        None where the profiler is not here. A `language sql` body is not
        counted: it is one statement, reached by every call that
        returned."""
        if not self.coverage_on:
            return None
        reached, stmts, shares, missed = 0, 0, [], []
        for oid in self.oids(routine):
            if self.language(oid) != "plpgsql":
                continue
            with self.conn.cursor() as cur:
                cur.execute("select lineno, stmtname, exec_stmts from"
                            " plpgsql_profiler_function_statements_tb("
                            "%s::oid::regprocedure)"
                            " order by stmtid", (oid,))
                rows = cur.fetchall()
                cur.execute("select plpgsql_coverage_branches("
                            "%s::oid::regprocedure)", (oid,))
                share = cur.fetchone()[0]
            self.conn.commit()
            for line, what, runs in rows:
                stmts += 1
                if runs:
                    reached += 1
                else:
                    missed.append((line, str(what or "")))
            if share is not None and share == share:
                shares.append(float(share))
        branch = min(shares) if shares else None
        return reached, stmts, branch, missed


#: SQL Server's error numbers as the SQLSTATE PostgreSQL raises the same
#: failure with; a number not here compares as "an error, class unknown"
MSSQL_STATES = {2627: "23505", 2601: "23505", 547: "23503", 515: "23502",
                8134: "22012", 245: "22P02", 8114: "22P02", 8152: "22001",
                2628: "22001", 220: "22003", 8115: "22003", 232: "22003",
                1205: "40P01", 3930: "25P02",
                # a MERGE meeting one target row with two source rows
                8672: "21000"}


class MSSQLRunner(Runner):
    """SQL Server (SQL Edge where no x86 runner is at hand). No coverage:
    the extended events its coverage tools read are not documented on
    SQL Edge, so a routine proved here is proved without it, and said
    so."""
    engine = "mssql"

    def open(self):
        import pymssql
        ep = self.eng.hop.target if self.side == "dst" else \
            self.eng.hop.source
        self.conn = pymssql.connect(
            server=ep.host, port=str(ep.port), user=ep.user,
            password=ep.password, database=self.eng._d(self.side, self.db),
            login_timeout=15, charset="UTF-8", tds_version="7.4",
            autocommit=False)
        return self

    def q(self, name):
        return "[" + str(name).replace("]", "]]") + "]"

    def table(self, name):
        parts = str(name).split(".", 1)
        return ".".join(self.q(p) for p in
                        (parts if len(parts) == 2 else ["dbo"] + parts))

    def _schema_of(self, routine):
        return self.q((routine.settings or {}).get("schema") or "dbo")

    def _error(self, e):
        args = getattr(e, "args", ())
        number = args[0] if args and isinstance(args[0], int) else None
        message = args[1] if len(args) > 1 else str(e)
        if isinstance(message, bytes):
            message = message.decode("utf-8", "replace")
        message = str(message)
        m = re.search(r"(?s)^(.*?)DB-Lib error message", message)
        message = (m.group(1) if m else message).strip()
        if number is not None and number >= 50000:
            return ("P0001", message)
        return (MSSQL_STATES.get(number, "?"), message)

    def columns_made(self, table):
        with self.conn.cursor() as cur:
            cur.execute("select c.name, c.is_identity, c.is_computed from"
                        " sys.columns c where c.object_id = object_id(%s)",
                        (self.table(table),))
            rows = cur.fetchall()
        self.conn.commit()
        counter = next((c for c, ident, _ in rows if ident), None)
        return counter, [c for c, _, comp in rows if comp]

    def create(self, statements, settings=None):
        with self.conn.cursor() as cur:
            for s in statements:
                cur.execute(s)
        self.conn.commit()

    def drop(self, routine):
        kind = {"event": "procedure"}.get(routine.kind, routine.kind)
        with self.conn.cursor() as cur:
            cur.execute(f"drop {kind} if exists {self._schema_of(routine)}."
                        f"{self.q(routine.name)}")
        self.conn.commit()

    def counters(self, tables):
        out = {}
        with self.conn.cursor() as cur:
            for t in tables:
                w = self.watched[t]
                if w.counter:
                    out[t] = self._last(cur, w)[0]
        return out

    def _last(self, cur, w):
        """(the value the identity handed out last - its seed less its
        step where it has handed out none - and whether it has). Not
        `IDENT_CURRENT`, which answers the seed both before the first
        value and after it."""
        cur.execute("select cast(last_value as bigint), cast(seed_value as"
                    " bigint), cast(increment_value as bigint) from"
                    " sys.identity_columns where object_id = object_id(%s)",
                    (self.table(w.name),))
        last, seed, step = cur.fetchone()
        return (int(last) if last is not None else int(seed) - int(step),
                last is not None)

    def reset_counters(self, tables):
        # an identity never used hands out its seed first; one used
        # hands out the value after the one it is reseeded to
        with self.conn.cursor() as cur:
            for t in tables:
                w = self.watched[t]
                if w.counter and self._last(cur, w)[1]:
                    cur.execute(f"select coalesce(max({self.q(w.counter)}),"
                                f" 0) from {self.table(w.name)}")
                    top = int(cur.fetchone()[0])
                    cur.execute(f"dbcc checkident ('{self.table(w.name)}',"
                                f" reseed, {top}) with no_infomsgs")
        self.conn.commit()

    def call(self, routine, args, tables, commits=False):
        out = Outcome()
        self._from_fixture(tables)
        before = self.counters(tables)
        self.conn.autocommit(commits)
        name = f"{self._schema_of(routine)}.{self.q(routine.name)}"
        try:
            with self.conn.cursor() as cur:
                if routine.kind == "function":
                    cur.execute(f"select {name}"
                                f"({', '.join(['%s'] * len(args))})",
                                tuple(args))
                    out.value = cur.fetchone()[0]
                else:
                    # the OUTPUT variables declared first, holding what an
                    # INOUT parameter is given; the placeholders are filled
                    # in the order they appear
                    declared, given, places, passed, outs = [], [], [], [], []
                    for k, p in enumerate(routine.params):
                        if p.mode == "in":
                            places.append("%s")
                            passed.append(args[k])
                        else:
                            var = f"@migkit_o{k}"
                            declared.append(f"declare {var} {p.type} = %s;")
                            given.append(args[k] if p.mode == "inout"
                                         else None)
                            places.append(f"{var} output")
                            outs.append(var)
                    sql = " ".join(declared) + f" exec {name}" \
                        f" {', '.join(places)};"
                    if outs:
                        sql += f" select {', '.join(outs)};"
                    cur.execute(sql, tuple(given + passed))
                    sets = []
                    while True:
                        if cur.description is not None:
                            sets.append([list(r) for r in cur.fetchall()])
                        if not cur.nextset():
                            break
                    if outs:
                        out.outs = list(sets.pop()[0]) if sets else []
                    out.sets = sets
        except Exception as e:  # noqa: BLE001 - the routine's own answer
            if type(e).__module__.split(".")[0] != "pymssql" or \
                    type(e).__name__ in ("InterfaceError",):
                raise HarnessError(str(e)) from e
            out.error = self._error(e)
        out.changes = self.changes(tables)
        after = self.counters(tables)
        out.counters = {t: after[t] - before.get(t, 0) for t in after
                        if after[t] != before.get(t, 0)}
        if commits:
            self.restore(tables)
        else:
            self._done()
        self.conn.autocommit(False)
        return out

    def run(self, statement, params, tables, rows=False):
        out = Outcome()
        self._from_fixture(tables)
        before = self.counters(tables)
        try:
            with self.conn.cursor() as cur:
                cur.execute(statement, tuple(params))
                if rows and cur.description is not None:
                    out.sets = [[list(r) for r in cur.fetchall()]]
        except Exception as e:  # noqa: BLE001 - the routine's own answer
            if type(e).__module__.split(".")[0] != "pymssql":
                raise HarnessError(str(e)) from e
            out.error = self._error(e)
        out.changes = self.changes(tables)
        after = self.counters(tables)
        out.counters = {t: after[t] - before.get(t, 0) for t in after
                        if after[t] != before.get(t, 0)}
        self._done()
        return out

    def restore(self, tables):
        with self.conn.cursor() as cur:
            for t in tables:
                w = self.watched[t]
                cur.execute(f"alter table {self.table(w.name)} nocheck"
                            " constraint all")
                cur.execute(f"delete from {self.table(w.name)}")
                if self.raw[t]:
                    cols = ", ".join(self.q(c) for c, _ in w.columns)
                    marks = ", ".join(["%s"] * len(w.columns))
                    if w.counter:
                        cur.execute(f"set identity_insert"
                                    f" {self.table(w.name)} on")
                    cur.executemany(f"insert into {self.table(w.name)}"
                                    f" ({cols}) values ({marks})",
                                    [tuple(r) for r in self.raw[t]])
                    if w.counter:
                        cur.execute(f"set identity_insert"
                                    f" {self.table(w.name)} off")
                cur.execute(f"alter table {self.table(w.name)} with check"
                            " check constraint all")
        self.conn.commit()


#: Oracle's error numbers as PostgreSQL's SQLSTATE for the same failure
ORACLE_STATES = {1: "23505", 1400: "23502", 1407: "23502", 2291: "23503",
                 2292: "23503", 1476: "22012", 1722: "22P02",
                 12899: "22001", 1438: "22003", 1403: "P0002",
                 1422: "P0003", 1830: "22007", 1843: "22008", 60: "40P01",
                 2290: "23514",
                 # an exception the routine declared and raised, unhandled:
                 # the class every rules' version raises one with
                 6510: "UE000"}


class OracleRunner(Runner):
    """Oracle (Oracle Free in the sandbox). The schema is the database;
    the session works in it, so a routine's own unqualified names are
    its schema's.

    Counters: an identity's sequence reports its cache's high-water mark
    (`LAST_NUMBER`), not the value it hands out next, so how far a call
    moved one is read by handing a value out: the sequence is set past
    its column's rows (`START WITH LIMIT VALUE`, as `sequences_behind`'s
    repair does), one value handed out and the sequence set back, so the
    call starts from that value; after the call the next value handed out
    is how far it went. Coverage: the server's own block coverage, kept in
    tables it makes itself in the sandbox (a run started before a
    routine's calls and read back after them)."""
    engine = "oracle"
    #: why the server's block coverage could not be had here, once asked
    coverage_off = None

    def open(self):
        self.conn = self.eng._connect(self.side, self.db)
        self.conn.autocommit = False
        cur = self.conn.cursor()
        cur.execute("alter session set time_zone = 'UTC'")
        cur.execute("alter session set current_schema ="
                    f" {self.eng._q(self.eng._owner(self.side, self.db))}")
        return self

    def q(self, name):
        return self.eng._q(name)

    def table(self, name):
        return self.eng._q(str(name).split(".")[-1])

    def read(self, w):
        cols = ", ".join(self.q(c) for c, _ in w.columns)
        cur = self.conn.cursor()
        cur.execute(f"select {cols} from {self.table(w.name)}")
        raw = [list(r) for r in cur.fetchall()]
        return [[render(c, v) for (_, c), v in zip(w.columns, r)]
                for r in raw], raw

    def _error(self, e):
        err = e.args[0] if e.args else None
        code = getattr(err, "code", None)
        message = str(getattr(err, "message", e)).split("\n")[0]
        if code is not None and 20000 <= code <= 20999:
            return ("P0001", re.sub(r"^ORA-\d+:\s*", "", message))
        return (ORACLE_STATES.get(code, "?"), message)

    def columns_made(self, table):
        cur = self.conn.cursor()
        cur.execute("select column_name, identity_column, virtual_column"
                    " from all_tab_cols where owner = :1 and table_name = :2",
                    (self._owner(), self.table(table)[1:-1]))
        rows = cur.fetchall()
        counter = next((self.eng._said(c) for c, ident, _ in rows
                        if ident == "YES"), None)
        return counter, [self.eng._said(c) for c, _, v in rows if v == "YES"]

    def _identity(self, w):
        """(sequence, generated always or by default, the column as the
        server names it) of a watched table's identity column."""
        cur = self.conn.cursor()
        cur.execute("select sequence_name, generation_type, column_name from"
                    " all_tab_identity_cols where owner = :1 and table_name"
                    " = :2", (self._owner(), self.table(w.name)[1:-1]))
        return cur.fetchone()

    def _set_past_rows(self, w, ident):
        seq, how, col = ident
        how = "always" if str(how).upper() == "ALWAYS" else "by default"
        self.conn.cursor().execute(
            f"alter table {self.table(w.name)} modify {self.eng._q(col)}"
            f" generated {how} as identity (start with limit value)")

    def _next(self, ident):
        cur = self.conn.cursor()
        cur.execute(f"select {self.eng._q(ident[0])}.nextval from dual")
        return int(cur.fetchone()[0])

    def reset_counters(self, tables):
        """Each identity set past its column's rows, the value it hands
        out first taken (`_start`) and the identity set back, so the call
        is handed that value first. A statement altering a table ends the
        transaction: this runs before the call's own begins."""
        self._start = {}
        for t in tables:
            w = self.watched[t]
            ident = self._identity(w) if w.counter else None
            if not ident:
                continue
            self._set_past_rows(w, ident)
            self._start[t] = self._next(ident)
            self._set_past_rows(w, ident)

    def counters(self, tables):
        """{table: the value its identity hands out next}, read by handing
        it out - which moves the sequence one on; the next call sets it
        back past the rows first."""
        out = {}
        for t in tables:
            w = self.watched[t]
            ident = self._identity(w) if w.counter else None
            if ident:
                out[t] = self._next(ident)
        return out

    def _moved(self, tables):
        """{table: how far the call moved its identity}."""
        before = getattr(self, "_start", {})
        after = self.counters(tables)
        return {t: after[t] - before.get(t, after[t]) for t in after
                if after[t] != before.get(t, after[t])}

    def fresh(self):
        """A session of the pool's, its packages' state put back as a new
        session has it: the pool may hand back the same session."""
        self.close()
        self.open()
        try:
            self.conn.cursor().execute("begin dbms_session.reset_package;"
                                       " end;")
        except Exception:  # noqa: BLE001 - a session with no package state
            pass

    # ---- coverage: the server's own block coverage --------------------------

    def _coverage_tables(self):
        """The schema holding the coverage tables, made once here; None
        (and why, in `coverage_off`) where the server cannot keep them."""
        if getattr(self, "_cov_owner", None) is not None:
            return self._cov_owner
        cur = self.conn.cursor()
        try:
            cur.execute("begin dbms_plsql_code_coverage"
                        ".create_coverage_tables(force_it => true); end;")
            cur.execute("select owner from all_tables where table_name ="
                        " 'DBMSPCC_BLOCKS' order by case owner when"
                        " sys_context('userenv', 'current_schema') then 0"
                        " when user then 1 else 2 end")
            got = cur.fetchone()
        except Exception as e:  # noqa: BLE001 - said, as not measured
            self.coverage_off = ("its server keeps no block coverage here:"
                                 f" {str(e).splitlines()[0][:100]}")
            return None
        if not got:
            self.coverage_off = "its coverage tables could not be found"
            return None
        self._cov_owner = got[0]
        return self._cov_owner

    def coverage_reset(self):
        """A coverage run started for the calls to come, any run before it
        stopped."""
        if self._coverage_tables() is None:
            return
        cur = self.conn.cursor()
        try:
            if getattr(self, "_run", None) is not None:
                cur.execute("begin dbms_plsql_code_coverage.stop_coverage;"
                            " end;")
            run = cur.var(int)
            cur.execute("begin :1 := dbms_plsql_code_coverage.start_coverage("
                        "run_comment => 'migkit'); end;", [run])
            self._run = run.getvalue()
        except Exception as e:  # noqa: BLE001 - said, as not measured
            self._run = None
            self.coverage_off = ("its block coverage could not be started:"
                                 f" {str(e).splitlines()[0][:100]}")

    def coverage(self, routine):
        """(blocks reached, blocks, [(line, what)] never reached) of the
        routine's PL/SQL over the calls since `coverage_reset`, as the
        server counts them - blocks it is told cannot be reached left out;
        None where coverage could not be had."""
        run = getattr(self, "_run", None)
        owner = self._coverage_tables()
        if run is None or owner is None:
            return None
        cur = self.conn.cursor()
        try:
            cur.execute("begin dbms_plsql_code_coverage.stop_coverage; end;")
            self._run = None
            name = routine.name.upper() if routine.name == \
                routine.name.lower() else routine.name
            lines = (0, 10 ** 9)
            if routine.settings.get("package"):
                # a member's blocks are its package body's, on its lines
                pkg = routine.settings["package"]
                name = pkg.upper() if pkg == pkg.lower() else pkg
                lines = routine.settings.get("lines") or lines
            cur.execute(
                f"select b.line, b.col, b.covered from {self.eng._q(owner)}"
                f".dbmspcc_blocks b join {self.eng._q(owner)}.dbmspcc_units"
                " u on u.run_id = b.run_id and u.object_id = b.object_id"
                " where b.run_id = :1 and u.owner = :2 and u.name = :3 and"
                " b.not_feasible = 0 and b.line between :4 and :5 order by"
                " b.line, b.col", (run, self._owner(), name, lines[0],
                                   lines[1]))
            rows = cur.fetchall()
        except Exception as e:  # noqa: BLE001 - said, as not measured
            self.coverage_off = ("its block coverage could not be read:"
                                 f" {str(e).splitlines()[0][:100]}")
            return None
        if not rows:
            self.coverage_off = ("the server counted no block of it: it was"
                                 " not run while coverage was on")
            return None
        missed = [(int(line), f"block at column {int(col)}")
                  for line, col, covered in rows if not int(covered or 0)]
        return len(rows) - len(missed), len(rows), missed

    def _owner(self):
        return self.eng._owner(self.side, self.db)

    def create(self, statements, settings=None):
        """Oracle makes a routine that does not compile and only warns;
        the errors it keeps for that name are the refusal."""
        cur = self.conn.cursor()
        for s in statements:
            cur.execute(s.rstrip().rstrip("/").rstrip())
            m = re.search(r'(?is)^\s*create\s+(or\s+replace\s+)?'
                          r'(editionable\s+)?(function|procedure|trigger|'
                          r'view|package(?:\s+body)?)\s+("[^"]+"|[\w$#.]+)',
                          s)
            if not m:
                continue
            made = m.group(4).split(".")[-1]
            made = made.strip('"') if made.startswith('"') else made.upper()
            # the schema the session makes it in, not the account's own
            got = cur.execute("select line, text from all_errors where owner"
                              " = :1 and name = :2 order by sequence",
                              (self._owner(), made)).fetchone()
            if got:
                raise RuntimeError(f"{made} line {got[0]}: {got[1]}")
        self.conn.commit()

    def drop(self, routine):
        kind = {"event": "procedure"}.get(routine.kind, routine.kind)
        cur = self.conn.cursor()
        try:
            if getattr(routine, "settings", {}).get("package"):
                # a member goes with its package: the package is made
                # whole again
                cur.execute(f"drop package"
                            f" {self.q(routine.settings['package'])}")
            else:
                cur.execute(f"drop {kind} {self.q(routine.name)}")
        except Exception:  # noqa: BLE001 - nothing of that name
            pass
        self.conn.commit()

    def call(self, routine, args, tables, commits=False):
        import oracledb
        out = Outcome()
        self._from_fixture(tables)
        member = routine.settings.get("package") and "." in routine.name
        name = ".".join(self.q(p) for p in routine.name.split(".", 1)) \
            if member else self.q(routine.name)
        cur = self.conn.cursor()
        if member and not self.held:
            # a package's state as a new session has it: each call starts
            # from it, as each call on the target starts from its own
            try:
                cur.execute("begin dbms_session.reset_package; end;")
            except Exception:  # noqa: BLE001 - called with it as it is
                pass
        try:
            if routine.kind == "function":
                marks = ", ".join(f":{k + 1}" for k in range(len(args)))
                cur.execute(f"select {name}({marks}) from dual", list(args))
                out.value = cur.fetchone()[0]
            else:
                values, outs = [], []
                for k, p in enumerate(routine.params):
                    if p.mode == "in":
                        values.append(args[k])
                        continue
                    kind = oracledb.CURSOR if "REF CURSOR" in \
                        str(p.type).upper() else oracledb.DB_TYPE_VARCHAR \
                        if "CHAR" in str(p.type).upper() else \
                        oracledb.DB_TYPE_DATE if "DATE" in \
                        str(p.type).upper() else oracledb.DB_TYPE_NUMBER
                    var = cur.var(kind)
                    if p.mode == "inout":
                        var.setvalue(0, args[k])
                    values.append(var)
                    outs.append((p, var))
                cur.callproc(".".join(
                    p.upper() if p == p.lower() else p
                    for p in routine.name.split(".", 1)) if member else
                    routine.name.upper() if routine.name ==
                    routine.name.lower() else routine.name, values)
                sets = [[list(r) for r in rs.fetchall()]
                        for rs in cur.getimplicitresults()]
                kept = []
                for p, var in outs:
                    v = var.getvalue()
                    if "REF CURSOR" in str(p.type).upper():
                        if v is not None:
                            sets.append([list(r) for r in v.fetchall()])
                    else:
                        kept.append(v)
                out.outs, out.sets = kept, sets
        except oracledb.DatabaseError as e:
            out.error = self._error(e)
        out.changes = self.changes(tables)
        # held calls are compared without it: reading one hands a value
        # out, and the next call would be handed another than the other
        # side's
        out.counters = self._moved(tables) if not self.held else {}
        if commits:
            self.restore(tables)
        else:
            self._done()
        return out

    def run(self, statement, params, tables, rows=False):
        import oracledb
        out = Outcome()
        self._from_fixture(tables)
        cur = self.conn.cursor()
        marks = iter(range(1, len(params) + 1))
        sql = re.sub(r"%s", lambda m: f":{next(marks)}", statement)
        try:
            cur.execute(sql, list(params))
            if rows and cur.description is not None:
                out.sets = [[list(r) for r in cur.fetchall()]]
        except oracledb.DatabaseError as e:
            out.error = self._error(e)
        out.changes = self.changes(tables)
        # held calls are compared without it: reading one hands a value
        # out, and the next call would be handed another than the other
        # side's
        out.counters = self._moved(tables) if not self.held else {}
        self._done()
        return out

    def restore(self, tables):
        cur = self.conn.cursor()
        for t in tables:
            w = self.watched[t]
            cur.execute(f"delete from {self.table(w.name)}")
            if self.raw[t]:
                cols = ", ".join(self.q(c) for c, _ in w.columns)
                marks = ", ".join(f":{k + 1}" for k in range(len(w.columns)))
                cur.executemany(f"insert into {self.table(w.name)} ({cols})"
                                f" values ({marks})",
                                [list(r) for r in self.raw[t]])
        self.conn.commit()


RUNNERS = {"mysql": MySQLRunner, "postgres": PostgresRunner,
           "mssql": MSSQLRunner, "oracle": OracleRunner}


def runner(eng, db, watched, **kw):
    cls = RUNNERS.get(eng.CANON_ENGINE)
    if cls is None:
        raise HarnessError(f"migkit cannot run {eng.CANON_ENGINE} routines"
                           " yet")
    return cls(eng, db, watched, **kw)


def _is_server_error(e):
    """An error the server answered with (the routine's), not the link
    failing."""
    mod = type(e).__module__ or ""
    if mod.startswith("pymysql"):
        import pymysql
        if isinstance(e, pymysql.err.OperationalError):
            code = e.args[0] if e.args else 0
            # 2003, 2006, 2013: no server, gone away, lost during a query
            return code not in (2003, 2006, 2013)
        return isinstance(e, pymysql.err.MySQLError)
    return False

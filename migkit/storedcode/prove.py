"""Stored code held to the source's answers, by running it (backlog R11).

Views are held to the source by their rows, on the hop's own servers, as
before: reading a view writes nothing. Everything else is run - and run
only in the sandbox (`sandbox`), never on the hop's own servers - with
the same inputs on both sides, and compared by everything the caller and
the database can see (`harness`). A function whose body is one expression
or reads nothing but tables is still asked on the hop's own servers where
no sandbox is named, read-only, as before.

The verdict per routine is never `ok` without execution evidence:

* `ok`: run on both sides, every call answered the same - value, OUT
  values, result sets, error class, rows changed, counters - and every
  statement and branch of it was reached on each side where that can be
  measured;
* `warn`: every call answered the same, but something is short - a branch
  never reached (named, with its line), a time value compared within a
  tolerance, an error class that says only "an error", a side whose
  coverage cannot be measured;
* `diff`: a call answered differently: the input (the smallest the search
  could shrink it to) and what differed, on each side;
* `not proved`: it could not be run here, with the reason;
* `missing`: it is not on the target.

The database's line is the worst of its routines, with the counts. It is
`warn` - not `skip` - when routines could not be proved: a `skip` beside
other checks' `ok` reads as "same" in the run's verdict, and a routine
nobody ran is not the same.
"""
import re
import time
from dataclasses import dataclass, field

from . import harness, inputs
from .harness import HarnessError, Watched, differences
from .routines import Param, Routine, by_key
from .sandbox import NoSandbox, Sandbox, spec
from .translate import Refused

#: seconds the search may spend on one routine, over and above the fixed
#: inputs; what it can do in them is sized from the routine's own call
#: time, measured on its first calls
SEARCH_SECONDS = 20
#: calls the search makes at most, however fast the routine is
SEARCH_MOST = 200

ORDER = {"ok": 0, "warn": 1, "not proved": 2, "missing": 3, "diff": 4}


@dataclass
class Verdict:
    """One routine's proof."""
    said: str                       #: "function add_tax"
    status: str                     #: a key of ORDER
    reason: str = ""
    calls: int = 0
    coverage: str = ""
    first: dict = field(default_factory=dict)   #: the differing call
    constructs: list = field(default_factory=list)
    #: the differing call's arguments themselves (not recorded: they may
    #: be the fixture's values)
    args: tuple = field(default=None, compare=False)

    def line(self):
        out = f"{self.said}: {self.status}"
        if self.reason:
            out += f" - {self.reason}"
        return out

    def record(self):
        return {"routine": self.said, "status": self.status,
                "reason": self.reason, "calls": self.calls,
                "coverage": self.coverage, "first": self.first,
                "constructs": self.constructs}


def routines_of(eng, side, db, notes=None):
    """The routines a side keeps: its engine's catalogue, or - where the
    engine reads only functions of one expression, or its catalogue could
    not be read (said in `notes`, which keeps the verdict from `ok`) -
    those, as routines."""
    try:
        got = eng.code_routines(side, db)
    except Exception as e:  # noqa: BLE001 - said, and read another way
        got = None
        if notes is not None:
            notes.append(f"the {'source' if side == 'src' else 'target'}'s"
                         " procedures, triggers and events could not be"
                         " read, so only its functions of one expression"
                         " are held here:"
                         f" {(str(e).strip().splitlines() or [''])[0][:100]}")
    if got:
        return got
    out = []
    for name, params, returns, one in eng.neutral_functions(side, db):
        out.append(Routine(
            kind="function" if returns else "procedure", name=name,
            params=[Param(p, t) for p, t in params], returns=returns,
            settings={"expression": one}))
    return out


def expressions(eng, side, db):
    """{name: its one expression} for the functions whose body is one."""
    try:
        return {n: one for n, _, _, one in eng.neutral_functions(side, db)
                if one is not None}
    except Exception:  # noqa: BLE001 - none known
        return {}


def read_only(routine, known, seen=None):
    """Whether a routine provably writes nothing: PostgreSQL says so of a
    STABLE or IMMUTABLE one and enforces it; a MySQL body is read here,
    statement by statement, and so is every routine it calls."""
    seen = set() if seen is None else seen
    if routine.name.lower() in seen:
        return True
    seen.add(routine.name.lower())
    data = str(routine.settings.get("data") or "")
    if routine.language in ("plpgsql", "sql") and routine.settings.get(
            "oid") is not None:
        return data in ("NO SQL", "READS SQL DATA")
    if routine.language == "tsql":
        # SQL Server refuses a function that writes a table at all
        return routine.kind == "function"
    if routine.language == "plsql":
        # an Oracle function may write through an autonomous transaction
        return False
    if routine.settings.get("expression") is not None and routine.body is None:
        return True
    if routine.kind != "function":
        return False
    from . import mysql_pg
    try:
        tree = mysql_pg.read(routine.body or "")
    except Refused:
        return False
    for node in tree.walk():
        if node.kind in ("sql", "call", "tx", "unsupported", "result"):
            return False
    for name in re.findall(r"(?i)([A-Za-z_]\w*)\s*\(", routine.body or ""):
        callee = known.get(name.lower())
        if callee is not None and callee is not routine and \
                not read_only(callee, known, seen):
            return False
    return True


def written(routine, known, tables, seen=None):
    """The source tables a MySQL routine may change: those its statements
    name, its callees', and those the triggers on them change; every
    table where that cannot be read off its body."""
    seen = set() if seen is None else seen
    if routine.name.lower() in seen:
        return set()
    seen.add(routine.name.lower())
    by_leaf = {str(t).split(".")[-1].lower(): t for t in tables}
    if routine.kind == "trigger":
        return set(tables)
    if routine.language == "tsql":
        return _tsql_written(routine, known, tables, seen, by_leaf)
    if routine.language != "sql" or routine.settings.get("oid") is not None:
        return set() if read_only(routine, known) else set(tables)
    from sqlglot import exp

    from . import mysql_pg
    from .translate import parse
    try:
        tree = mysql_pg.read(routine.body or "")
    except Refused:
        return set(tables)
    out = set()
    for node in tree.walk():
        if node.kind == "unsupported":
            return set(tables)
        if node.kind == "sql":
            try:
                stmt = parse(node.text if node.verb != "REPLACE" else
                             "INSERT" + node.text[len("REPLACE"):], "mysql")
            except Refused:
                return set(tables)
            target = stmt.this if isinstance(stmt, (exp.Insert, exp.Update,
                                                    exp.Delete)) else None
            if isinstance(target, exp.Schema):
                target = target.this
            names = [target.name] if isinstance(target, exp.Table) else [
                t.name for t in stmt.find_all(exp.Table)]
            for n in names:
                if n.lower() in by_leaf:
                    out.add(by_leaf[n.lower()])
        if node.kind == "call":
            callee = known.get(node.name.lower())
            out |= written(callee, known, tables, seen) if callee else \
                set(tables)
    for name in re.findall(r"(?i)([A-Za-z_]\w*)\s*\(", routine.body or ""):
        callee = known.get(name.lower())
        if callee is not None and callee.kind == "function":
            out |= written(callee, known, tables, seen)
    for trig in known.values():
        if trig.kind == "trigger" and trig.table and \
                trig.table.split(".")[-1].lower() in {
                    str(t).split(".")[-1].lower() for t in out}:
            out |= set(tables)
    return out


def _tsql_written(routine, known, tables, seen, by_leaf):
    """`written` for a SQL Server routine, read by its own statements."""
    if read_only(routine, known):
        return set()
    from sqlglot import exp

    from . import tsql_pg
    from .translate import parse
    try:
        stmts = tsql_pg._Parser(routine.body or "").statements(until=())
    except Refused:
        return set(tables)
    out = set()
    for stmt in stmts:
        for node in stmt.walk():
            if node.kind in ("unsupported",):
                return set(tables)
            if node.kind == "sql":
                try:
                    tree = parse(node.text, "tsql")
                except Refused:
                    return set(tables)
                target = tree.this if isinstance(
                    tree, (exp.Insert, exp.Update, exp.Delete)) else None
                if isinstance(target, exp.Schema):
                    target = target.this
                names = [target.name] if isinstance(target, exp.Table) else [
                    t.name for t in tree.find_all(exp.Table)]
                out |= {by_leaf[n.lower()] for n in names
                        if n.lower() in by_leaf}
            if node.kind == "exec":
                callee = known.get(node.name.lower())
                out |= written(callee, known, tables, seen) if callee else \
                    set(tables)
    if any(t.kind == "trigger" for t in known.values()) and out:
        out = set(tables)
    return out


# ---- the database's line ----------------------------------------------------


def said_counts(verdicts):
    counts = {}
    for v in verdicts:
        counts[v.status] = counts.get(v.status, 0) + 1
    return ", ".join(f"{counts[s]} {s}" for s in
                     ("ok", "warn", "diff", "not proved", "missing")
                     if counts.get(s))


def database_result(db, hop_name, verdicts, notes=(), others=0):
    from ..engines.base import Result
    if not verdicts:
        return None
    worst = max(verdicts, key=lambda v: ORDER[v.status]).status
    shown = sorted((v for v in verdicts if v.status != "ok"),
                   key=lambda v: (-ORDER[v.status], v.said))
    detail = f"{said_counts(verdicts)}"
    if shown:
        detail += "; " + "; ".join(v.line() for v in shown[:6])
        if len(shown) > 6:
            detail += f"; ... {len(shown) - 6} more"
    elif verdicts:
        calls = sum(v.calls for v in verdicts)
        detail += (f": every one run on both sides ({calls} calls) and"
                   " answering the same")
        if others:
            detail += (f" ({others} of them written by a model or a"
                       " person, not by the translator)")
    if notes:
        detail += "; " + "; ".join(notes)
    status = {"ok": "ok", "warn": "warn", "not proved": "warn",
              "missing": "warn", "diff": "diff"}[worst]
    if status == "ok" and any("could not be read" in n for n in notes):
        status = "warn"
    hint = ""
    if worst == "diff":
        hint = ("the conversion changed what it means: rewrite it by hand"
                " and check again")
    elif worst == "missing":
        hint = (f"migkit schema {hop_name} --convert --apply, and write by"
                " hand what it names as not converted")
    elif worst == "not proved":
        hint = ("name a sandbox for the proof (hop option proof_sandbox:"
                " docker, or two scratch servers)")
    return Result("schema", f"{db} converted code", status, detail, "", hint)


# ---- the proof --------------------------------------------------------------


class Proof:
    """One database's stored code, proved routine by routine."""

    def __init__(self, pair, db):
        self.pair = pair
        self.db = db
        #: what the verdict says beside the routines' own lines; a note
        #: keeps the database's line from `ok`
        self.notes = []
        self.src_list = routines_of(pair.src_engine, "src", db, self.notes)
        self.dst_list = routines_of(pair.dst_engine, "dst", db, self.notes)
        self.src_known = {r.name.lower(): r for r in self.src_list}
        self.expr = expressions(pair.src_engine, "src", db)
        for r in self.src_list:
            if r.name in self.expr:
                r.settings.setdefault("expression", self.expr[r.name])
        self.dst = by_key(self.dst_list)
        self.sandbox = None
        self.why_not = None
        self.bank = inputs.Bank(pair.hop.report_dir(db) / "code-bank.json")
        #: per routine, calls made and the source's answers to them
        self.kept = {}
        #: whether a proof goes past the fixed inputs to the search and the
        #: solver; the proof's own mutation score asks the fixed ones only
        self.searching = True

    def target_of(self, r):
        return self.dst.get(r.key)

    # ---- the sandbox --------------------------------------------------------

    def open(self, needed):
        if not needed:
            return
        sb = Sandbox(self.pair, self.db)
        nums, strs = set(), set()
        for r in self.src_list:
            n, s = inputs.constants(r.body or r.settings.get("expression"),
                                    self.pair.src_engine.SQL_DIALECT
                                    or "mysql")
            nums |= {int(x) for x in n if x == x.to_integral_value()}
            strs |= s
        sb.constants = tuple(sorted(nums)) + tuple(sorted(strs))
        try:
            self.sandbox = sb.open()
            self.notes += sb.notes
        except NoSandbox as e:
            sb.close()
            self.why_not = str(e)
        except (Exception, SystemExit) as e:  # noqa: BLE001 - said as the
            # reason; a move into it that stops says why by SystemExit
            sb.close()
            first = (str(e).strip().splitlines() or [""])[0][:160]
            self.why_not = f"the sandbox could not be made ready: {first}"

    def close(self):
        if self.sandbox is not None:
            self.sandbox.close()

    # ---- entry --------------------------------------------------------------

    def run(self):
        verdicts = []
        present = [(r, self.target_of(r)) for r in self.src_list]
        for r, t in present:
            if t is None:
                verdicts.append(Verdict(r.said, "missing",
                                        "not on the target yet"))
        runnable = [(r, t) for r, t in present if t is not None]
        # everything runs in the sandbox where one is named; where none
        # is, only a function that writes nothing is asked, read-only, on
        # the hop's own servers
        needs = any(not self._on_own(r) for r, _ in runnable)
        self.open(bool(runnable) and (needs or spec(self.pair.hop)
                                      is not None))
        try:
            if self.sandbox is not None:
                verdicts += self._in_sandbox(runnable)
            else:
                for r, t in runnable:
                    if self._on_own(r):
                        verdicts.append(self._on_own_servers(r, t))
                    else:
                        verdicts.append(Verdict(
                            r.said, "not proved", self.why_not or
                            "nothing was run"))
        finally:
            self.close()
            self.bank.save()
        return verdicts

    # ---- a function asked on the hop's own servers --------------------------

    def _on_own(self, r):
        """Whether a routine may be asked on the hop's own servers: a
        function, asked in a read-only transaction on each - a write it
        tries is refused by the server itself, and then it is said to need
        the sandbox. A procedure, a trigger or an event is there to write,
        and is only run in the sandbox."""
        return r.kind == "function"

    def _args(self, r, t, extra=()):
        pools = []
        nums, strs = inputs.constants(r.body or r.settings.get("expression"),
                                      self.pair.src_engine.SQL_DIALECT or
                                      "mysql")
        for p in r.params:
            if p.mode == "out":
                pools.append([None])
                continue
            cls = inputs.klass(self.pair.src_engine.CANON_ENGINE, p.type)
            pool = list(inputs.POOLS.get(cls, [None]))
            for v in inputs.from_constants(cls, nums, strs):
                if v not in pool:
                    pool.append(v)
            for v in (extra.get(p.name, []) if extra else []):
                if v not in pool:
                    pool.append(v)
            pools.append(pool)
        return inputs.combine(pools)

    def _classes(self, r):
        eng = self.pair.src_engine.CANON_ENGINE
        return (inputs.klass(eng, r.returns),
                [inputs.klass(eng, p.type) for p in r.params
                 if p.mode != "in"])

    def _render(self, r, out):
        """An outcome's values in the source's declared classes, result
        sets as multisets. A decimal whose declaration names no scale is
        compared by its value: Oracle's NUMBER keeps no trailing zeros, so
        its 2.5 is the 2.50 a PostgreSQL numeric answers, where a declared
        scale (and every MySQL decimal has one) is part of the answer."""
        ret, outs = self._classes(r)
        loose = self.pair.src_engine.CANON_ENGINE == "oracle"
        declared = [p.type for p in r.params if p.mode != "in"]
        out.value = harness.render(ret, _plain(out.value, loose or (
            r.returns and "(" not in str(r.returns))))
        out.outs = [harness.render(c, _plain(v, loose or "(" not in str(d)))
                    for c, v, d in zip(outs, out.outs, declared)]
        out.sets = [sorted(([harness.render_any(_plain(v, loose))
                             for v in row] for row in rows), key=repr)
                    for rows in out.sets]
        return out

    def _on_own_servers(self, r, t):
        pair = self.pair
        if len(t.params) != len(r.params):
            return Verdict(r.said, "diff", f"the target's version takes"
                           f" {len(t.params)} arguments where the source's"
                           f" takes {len(r.params)}")
        try:
            a = harness.runner(pair.src_engine, self.db, {}, side="src",
                               readonly=True).open()
            b = harness.runner(pair.dst_engine, self.db, {}, side="dst",
                               readonly=True).open()
        except HarnessError as e:
            return Verdict(r.said, "not proved", str(e))
        calls, seen_error = 0, set()
        try:
            for args in self._args(r, t):
                try:
                    x = self._render(r, a.call(r, list(args), []))
                    y = self._render(r, b.call(t, list(args), []))
                except HarnessError as e:
                    return Verdict(r.said, "not proved", f"a call could not"
                                   f" be made: {str(e)[:100]}")
                calls += 1
                if any(o.error and (o.error[0] == "25006" or "READ ONLY" in
                                    str(o.error[1]).upper())
                       for o in (x, y)):
                    return Verdict(r.said, "not proved",
                                   "it writes, and the hop's own servers are"
                                   " only read: it needs the sandbox", calls)
                found = differences(x, y, harness.reads_clock(r.body))
                if found:
                    self.bank.keep(f"{r.kind}:{r.name}", args)
                    return Verdict(r.said, "diff", self._first(r, args,
                                                               found),
                                   calls, first={"args": repr(args),
                                                 "found": found})
                if x.error:
                    seen_error.add(harness.error_key(x.error)[0])
        finally:
            a.close()
            b.close()
        if r.settings.get("expression") is not None:
            reason = "" if not (seen_error & set(harness.UNKNOWN_CLASSES)) \
                else "some calls errored on both sides with no class to" \
                     " compare"
            return Verdict(r.said, "warn" if reason else "ok", reason, calls,
                           coverage="one expression, reached by every call")
        return Verdict(r.said, "warn", f"{calls} calls answered the same on"
                       " the hop's own servers, read-only, but which of its"
                       " statements ran was not measured: " + (
                           self.why_not if spec(self.pair.hop) is not None
                           and self.why_not else "no sandbox is named for"
                           " this hop (proof_sandbox)"), calls)

    def _first(self, r, args, found):
        names = [p.name for p in r.params]
        shown = ", ".join(f"{n}={a!r}" for n, a in zip(names, args))
        return f"called with ({shown}): {found[0]}"

    # ---- everything in the sandbox ------------------------------------------

    def _watched(self):
        sb = self.sandbox
        twin = sb.twin
        src, dst = {}, {}
        a = harness.runner(sb.src, self.db, {}).open()
        b = harness.runner(sb.dst, sb.tdb, {}).open()
        try:
            for t, dt in sb.tables.items():
                sc, dc, _ = twin._comparable_columns(
                    self.db, twin.src_engine, t, twin.dst_engine, dt)
                ks = twin.src_engine.neutral_key("src", self.db, t)
                kd = twin.dst_engine.neutral_key("dst", self.db, dt)
                ca, ga = a.columns_made(t)
                cb, gb = b.columns_made(dt)
                src[t] = Watched(t, sc, ks, ca)
                dst[t] = Watched(dt, dc, kd, cb)
                src[t].generated, dst[t].generated = ga, gb
        finally:
            a.close()
            b.close()
        return src, dst

    def _create_all(self, runner, routines):
        """Every routine of a side in its sandbox; (made, {name: why
        not}). A MySQL routine goes in instrumented where probes change
        nothing it does. Several rounds, for a body the server checks
        against routines made after it."""
        from . import mysql_pg
        probes, failed = {}, {}
        if runner.engine == "mysql":
            with runner.conn.cursor() as cur:
                cur.execute(f"create table if not exists"
                            f" {mysql_pg.COVER_TABLE} (routine varchar(64),"
                            " probe int) engine = MyISAM")
            runner.conn.commit()
        todo = list(routines)
        for _ in range(3):
            left = []
            for r in todo:
                try:
                    stmts, got = self._statements(r, runner.engine)
                    runner.drop(r)
                    runner.create(stmts, r.settings)
                    if got:
                        probes[r.name if r.kind != "event" else
                               f"migkit_event_{r.name}"] = got
                    failed.pop(r.name, None)
                except Exception as e:  # noqa: BLE001 - said per routine
                    runner.conn.rollback()
                    failed[r.name] = (str(e).strip().splitlines()
                                      or [""])[0][:120]
                    left.append(r)
            if not left or len(left) == len(todo):
                break
            todo = left
        return probes, failed

    def _statements(self, r, engine):
        from . import mysql_pg
        if r.definition is None:
            raise HarnessError("its definition could not be read")
        if engine == "mysql":
            if r.kind == "event":
                # its body, as a procedure the proof can call
                name = f"migkit_event_{r.name}"
                r = Routine(kind="procedure", name=name, body=r.body,
                            definition=f"create procedure `{name}`()"
                                       f" {r.body}", settings=r.settings)
            got = mysql_pg.instrument(r)
            if got:
                return [got[0]], got[1]
            return [r.definition], None
        if engine in ("mssql", "oracle"):
            # a CREATE is a batch of its own; the server keeps it whole
            return [r.definition], None
        return [s for s in _pg_statements(r.definition)], None

    def begin(self, targets):
        """The sandbox's two runners, with the source's routines made in
        one and `targets` in the other, and the fixture read once."""
        sb = self.sandbox
        wsrc, wdst = self._watched()
        self.a = harness.runner(sb.src, self.db, wsrc).open()
        self.b = harness.runner(sb.dst, sb.tdb, wdst).open()
        self.probes, self.src_failed = self._create_all(self.a,
                                                        self.src_list)
        self.dst_probes, self.dst_failed = self._create_all(self.b, targets)
        self.a.baseline()
        self.b.baseline()

    def end(self):
        for runner in (getattr(self, "a", None), getattr(self, "b", None)):
            if runner is not None:
                runner.close()
        self.a = self.b = None

    def verdict_for(self, r, t):
        """One routine's verdict, its target's version `t` already made in
        the sandbox."""
        if r.name in self.src_failed:
            return Verdict(r.said, "not proved", "the source's own version"
                           " could not be made in the sandbox:"
                           f" {self.src_failed[r.name]}")
        if t.name in self.dst_failed:
            return Verdict(r.said, "diff", "the target's version is refused"
                           f" by its own engine: {self.dst_failed[t.name]}")
        try:
            return self.prove_one(self.a, self.b, r, t)
        except HarnessError as e:
            return Verdict(r.said, "not proved",
                           f"the sandbox could not run it: {e}")

    def made(self, r):
        """The target's version of `r` as the target's sandbox now keeps
        it, or None."""
        sb = self.sandbox
        return by_key(sb.dst.code_routines("dst", sb.tdb)).get(r.key)

    def install(self, r, statements):
        """A version already proved, made again in the target's sandbox in
        place of whatever stands there, without proving it again."""
        b = self.b
        b.drop(Routine(kind={"event": "procedure"}.get(r.kind, r.kind),
                       name=r.name))
        b.clear(statements)
        b.create(statements)

    def try_version(self, r, statements):
        """One candidate version of `r`, made in the target's sandbox in
        place of whatever stood there, gated (the target must take it; its
        static check, where it has one, must find nothing - fail closed)
        and proved. (verdict, the version as made, or None)."""
        b = self.b
        kind = {"event": "procedure"}.get(r.kind, r.kind)
        b.drop(Routine(kind=kind, name=r.name))
        try:
            b.clear(statements)
            b.create(statements)
        except Exception as e:  # noqa: BLE001 - the candidate's failure
            b.conn.rollback()
            return Verdict(r.said, "diff", "the target refuses it:"
                           f" {(str(e).strip().splitlines() or [''])[0][:160]}"
                           ), None
        t = self.made(r)
        if t is None:
            b.drop(Routine(kind=kind, name=r.name))
            return Verdict(r.said, "diff", f"it makes no {kind} named"
                           f" {r.name}"), None
        self.dst_failed.pop(t.name, None)
        findings = [f for f in (b.check(t) if hasattr(b, "check") else [])
                    if str(f[0]).startswith("error")]
        if findings:
            b.drop(t)
            level, message, line = findings[0]
            return Verdict(r.said, "diff", "the target's static check finds:"
                           f" {message}" + (f" (line {line})" if line
                                            else "")), None
        v = self.verdict_for(r, t)
        if v.status not in ("ok", "warn"):
            b.drop(t)
        return v, t

    def _in_sandbox(self, runnable):
        out = []
        try:
            self.begin(self.dst_list)
            for r, t in runnable:
                out.append(self.verdict_for(r, t))
        finally:
            self.end()
        return out

    # ---- one routine --------------------------------------------------------

    def prove_one(self, a, b, r, t):
        """One routine run on both sides; its verdict."""
        from . import mysql_pg
        tables = list(a.watched)
        if r.kind == "trigger":
            return self._trigger(a, b, r, t, tables)
        writes = sorted(written(r, self.src_known, tables)) \
            if r.kind != "event" else tables
        commits = False
        try:
            if a.engine == "mysql" and r.body:
                commits = any(n.kind == "tx" for n in
                              mysql_pg.read(r.body).walk())
            elif a.engine == "mssql" and r.body:
                from . import tsql_pg
                commits = any(n.kind == "tx" and n.what != "begin"
                              for s in tsql_pg._Parser(r.body).statements(
                                  until=()) for n in s.walk())
        except Refused:
            commits = False
        call_r = r if r.kind != "event" else Routine(
            kind="procedure", name=f"migkit_event_{r.name}")
        takes = [p for p in t.params if "refcursor" not in str(p.type)]
        if r.kind != "event" and len(takes) != len(r.params):
            return Verdict(r.said, "diff", f"the target's version takes"
                           f" {len(takes)} arguments where the source's takes"
                           f" {len(r.params)}")
        findings = b.check(t) if hasattr(b, "check") else []
        if hasattr(a, "coverage_reset"):
            a.coverage_reset()
        if hasattr(b, "coverage_reset"):
            b.coverage_reset()
        extra = self._fixture_values(a, r)
        argsets = list(self.bank.inputs(f"{r.kind}:{r.name}")) + \
            self._args(r, t, extra)
        clock = harness.reads_clock(r.body)
        if harness.reads_chance(r.body):
            return Verdict(r.said, "not proved", "it answers from a random"
                           " source, so two runs of the same call need not"
                           " agree")
        calls, masked, unknown = 0, False, False
        began = time.monotonic()
        from . import keep
        kept = self.kept.setdefault(r.name, []) if (
            keep.pure(r) and r.kind == "function" and
            self._classes(r)[0] in keep.EXACT) else None

        def once(args):
            nonlocal masked, unknown
            x = self._render(r, a.call(call_r, list(args), writes, commits))
            y = self._render(r, b.call(t, list(args), writes, commits))
            if kept is not None and len(kept) < keep.KEEP:
                kept.append((args, x))
            found = differences(x, y, clock)
            if clock and not found and differences(x, y, False):
                masked = True
            if x.error and y.error and harness.error_key(x.error)[0] in \
                    harness.UNKNOWN_CLASSES:
                unknown = True
            return found

        for args in argsets:
            found = once(args)
            calls += 1
            if found:
                small = self._shrink(once, r, args)
                self.bank.keep(f"{r.kind}:{r.name}", small)
                what = once(small)
                return Verdict(r.said, "diff", self._first(r, small, what),
                               calls, first={"args": repr(small),
                                             "found": what}, args=small)
        per_call = (time.monotonic() - began) / max(1, calls)
        found = self._search(once, r, per_call) if self.searching else None
        if found:
            args, what = found
            self.bank.keep(f"{r.kind}:{r.name}", args)
            return Verdict(r.said, "diff", self._first(r, args, what),
                           calls + self._searched, first={
                               "args": repr(args), "found": what},
                           args=args)
        calls += self._searched
        for args in (self._solved(a, r) if self.searching else ()):
            calls += 1
            found = once(args)
            if found:
                self.bank.keep(f"{r.kind}:{r.name}", args)
                return Verdict(r.said, "diff", self._first(r, args, found),
                               calls, first={"args": repr(args),
                                             "found": found}, args=args)
        return self._covered(a, b, r, t, calls, findings, masked, unknown)

    def _solved(self, a, r):
        """Inputs solved for the source's branches no call reached yet."""
        from . import concolic
        probes = self.probes.get(r.name) if a.engine == "mysql" else None
        if not probes or r.kind not in ("function", "procedure"):
            return []
        hit = a.probes_hit(r)
        missed = {(ln, what) for n, ln, what in probes if n not in hit}
        if not missed:
            return []
        return concolic.inputs(r, missed, lambda t: inputs.klass(
            self.pair.src_engine.CANON_ENGINE, t))

    _searched = 0

    def _search(self, once, r, per_call):
        """Values drawn by class for as long as the routine's own call time
        allows, kept by Hypothesis where they differ and shrunk to the
        smallest that still does."""
        self._searched = 0
        klasses = [inputs.klass(self.pair.src_engine.CANON_ENGINE, p.type)
                   for p in r.params if p.mode != "out"]
        if not klasses:
            return None
        most = int(max(5, min(SEARCH_MOST, SEARCH_SECONDS
                              / max(per_call, 1e-3))))
        from hypothesis import HealthCheck, Phase, given, settings
        from hypothesis import strategies as st
        found = {}
        ins = [k for k, p in enumerate(r.params) if p.mode != "out"]

        def full(drawn):
            args = [None] * len(r.params)
            for k, v in zip(ins, drawn):
                args[k] = v
            return tuple(args)

        @settings(max_examples=most, database=None, deadline=None,
                  derandomize=True, suppress_health_check=list(HealthCheck),
                  phases=[Phase.generate, Phase.shrink])
        @given(st.tuples(*[inputs.strategy(k) for k in klasses]))
        def probe(drawn):
            self._searched += 1
            args = full(drawn)
            what = once(args)
            if what:
                found["args"], found["what"] = args, what
                raise AssertionError(what[0])
        try:
            probe()
        except HarnessError:
            raise
        except Exception:  # noqa: BLE001 - a difference, or one that did
            # not come back when replayed: either way, the last call found
            # to differ is the answer
            if found:
                return found["args"], found["what"]
            raise
        return None

    def _shrink(self, once, r, args):
        """The smallest input that still differs: each argument tried as
        NULL, then as each value its class's pool puts before it (0 before
        1, '' before 'a'), kept where the difference stays."""
        small = list(args)
        for k, p in enumerate(r.params):
            if p.mode == "out" or small[k] is None:
                continue
            cls = inputs.klass(self.pair.src_engine.CANON_ENGINE, p.type)
            pool = inputs.POOLS.get(cls, [None])
            simpler = pool[:pool.index(small[k])] if small[k] in pool \
                else pool[:3]
            for value in [None] + [v for v in simpler if v is not None]:
                trial = list(small)
                trial[k] = value
                if once(tuple(trial)):
                    small = trial
                    break
        return tuple(small)

    def _fixture_values(self, a, r):
        """{parameter: values of the columns the body compares it with,
        and one those columns do not hold}."""
        names = [p.name for p in r.params if p.mode != "out"]
        out = {}
        for p, cols in inputs.compared_columns(r.body or r.settings.get(
                "expression"), names).items():
            param = next(x for x in r.params if x.name == p)
            cls = inputs.klass(self.pair.src_engine.CANON_ENGINE, param.type)
            seen = []
            for c in cols:
                col = c.split(".")[-1].lower()
                for t, w in a.watched.items():
                    names_here = {n.lower(): n for n, _ in w.columns}
                    if col in names_here:
                        at = [n for n, _ in w.columns].index(names_here[col])
                        seen += [row[at] for row in a.raw.get(t, [])][:5]
            if seen:
                out[p] = seen + [inputs.missing_value(cls, seen)]
        return out

    def _covered(self, a, b, r, t, calls, findings, masked, unknown):
        """The verdict of a routine every call of which answered the same:
        `ok` only with every statement and branch reached on each side
        that can say so."""
        short = []
        cov_said = []
        errors = [f for f in findings if str(f[0]).startswith("error")]
        if errors:
            short.append(f"the target's static check finds: {errors[0][1]}"
                         + (f" (line {errors[0][2]})" if errors[0][2] else ""))
        got = self._side_coverage(b, t, "target", self.dst_probes)
        cov_said.append(got[0])
        short += got[1]
        got = self._side_coverage(a, r, "source", self.probes)
        cov_said.append(got[0])
        short += got[1]
        if masked:
            short.append("a time value it reads from the clock was compared"
                         " within 5 seconds")
        if unknown:
            short.append("some calls errored on both sides with no class to"
                         " compare")
        if r.kind == "event":
            short.append(self._event_said(r))
        status = "warn" if short else "ok"
        reason = "; ".join(short) if short else \
            f"{calls} calls answered the same"
        return Verdict(r.said, status, reason, calls,
                       coverage="; ".join(c for c in cov_said if c))

    def _side_coverage(self, runner, routine, side, probes):
        """(what was reached, [what is short]) on one side."""
        name = routine.name if routine.kind != "event" else \
            f"migkit_event_{routine.name}"
        if runner.engine == "postgres":
            got = runner.coverage(routine)
            if got is None:
                return "", [f"the {side}'s coverage was not measured: its"
                            " sandbox has no profiler"]
            reached, stmts, branch, missed = got
            if stmts == 0:
                return f"{side}: one statement", []
            said = f"{side}: {reached}/{stmts} statements" + (
                f", {branch:.0%} of branches" if branch is not None else "")
            if missed:
                where = ", ".join(f"line {ln} ({w})" for ln, w in missed[:4])
                return said, [f"on the {side}, never reached: {where}"]
            if branch is not None and branch < 1:
                return said, [f"on the {side}, a branch was never taken"
                              f" ({branch:.0%} of them were)"]
            return said, []
        if runner.engine == "mysql":
            mine = probes.get(name)
            if not mine:
                return "", [f"the {side}'s coverage cannot be measured: it"
                            " reads ROW_COUNT(), FOUND_ROWS() or the"
                            " diagnostics area, which a probe would change"]
            hit = runner.probes_hit(Routine(kind=routine.kind, name=name))
            missed = [(ln, what) for n, ln, what in mine if n not in hit]
            said = f"{side}: {len(mine) - len(missed)}/{len(mine)} blocks"
            if missed:
                where = ", ".join(f"line {ln} ({w})" for ln, w in missed[:4])
                return said, [f"on the {side}, never reached: {where}"]
            return said, []
        return "", [f"the {side}'s coverage cannot be measured on"
                    f" {runner.engine}"]

    def _event_said(self, r):
        s = r.schedule or {}
        when = f"once at {s['at']}" if s.get("at") else \
            f"every {str(s.get('every', '?')).lower()}"
        return (f"its schedule ({when}) is not on the target: the target"
                " runs the body as a procedure, on a schedule set there by"
                " hand")

    # ---- triggers -----------------------------------------------------------

    def _trigger(self, a, b, r, t, tables):
        """A trigger fired by statements on its table, on both sides: a
        new row, each column changed, a change that changes nothing (it
        tells a trigger that fires on every UPDATE from MySQL's
        ON UPDATE, which fires only when something changed), a delete, and
        one statement over every row."""
        src_t = next((x for x in tables if str(x).split(".")[-1].lower()
                      == str(r.table).split(".")[-1].lower()), None)
        if src_t is None:
            return Verdict(r.said, "not proved", f"its table {r.table} is not"
                           " in the fixture")
        findings = b.check(t) if hasattr(b, "check") else []
        for runner in (a, b):
            if hasattr(runner, "coverage_reset"):
                runner.coverage_reset()
        statements = self._firings(a, b, src_t, r.events, r.body)
        if not statements:
            return Verdict(r.said, "not proved", f"{src_t} has no row in the"
                           " fixture to fire it with")
        calls = 0
        for what, (sa, pa), (sb_, pb) in statements:
            x = a.run(sa, pa, tables)
            y = b.run(sb_, pb, tables)
            calls += 1
            found = differences(x, y)
            if found:
                return Verdict(r.said, "diff", f"{what}: {found[0]}", calls,
                               first={"statement": what, "found": found})
        return self._covered(a, b, r, t, calls, findings, False, False)

    def _firings(self, a, b, t, events, body=None):
        """[(what, (source statement, params), (target statement,
        params))] firing every trigger on table `t`: each column changed
        by a step and set to each constant the trigger's body compares
        (and its neighbours), so its branches are reached."""
        wa, wb = a.watched[t], b.watched[t]
        if not a.raw.get(t) or not b.raw.get(t):
            return []
        ka, kb = a.key_at(wa), b.key_at(wb)
        if not ka or len(ka) != len(kb):
            return []
        rows_a = {tuple(a.base[t][i][j] for j in ka): a.raw[t][i]
                  for i in range(len(a.raw[t]))}
        rows_b = {tuple(b.base[t][i][j] for j in kb): b.raw[t][i]
                  for i in range(len(b.raw[t]))}
        key = sorted(rows_a.keys() & rows_b.keys(), key=repr)
        if not key:
            return []
        first = key[0]
        ra, rb = rows_a[first], rows_b[first]
        names_a = [c for c, _ in wa.columns]
        names_b = [c for c, _ in wb.columns]
        classes = [c for _, c in wa.columns]
        writable = [i for i in range(len(names_a))
                    if names_a[i] not in (wa.generated or [])
                    and names_b[i] not in (wb.generated or [])]
        out = []

        def where(runner, w, names, row):
            cond = " and ".join(f"{runner.q(names[j])} = %s"
                                for j in runner.key_at(w))
            return cond, [row[j] for j in runner.key_at(w)]

        if "INSERT" in events and len(ka) == 1 and classes[ka[0]] in (
                "integer", "text"):
            j = ka[0]
            fresh = (max(int(r[j]) for r in a.raw[t]) + 1000
                     if classes[j] == "integer" else "zz-migkit")
            # a key the table's counter hands out is left to it (SQL
            # Server refuses one given); any other key is a new value
            counted = names_a[j] == wa.counter or names_b[j] == wb.counter
            cols = [i for i in writable if not (counted and i == j)]

            def insert(runner, w, names, row):
                vals = [fresh if i == j else row[i] for i in cols]
                return (f"insert into {runner.table(w.name)}"
                        f" ({', '.join(runner.q(names[i]) for i in cols)})"
                        f" values ({', '.join(['%s'] * len(cols))})", vals)
            out.append(("a new row", insert(a, wa, names_a, ra),
                        insert(b, wb, names_b, rb)))
        if "UPDATE" in events:
            others = [i for i in writable if i not in ka]
            nums, strs = inputs.constants(body, self.pair.src_engine
                                          .SQL_DIALECT or "mysql")
            there = [c for _, c in wb.columns]
            for i in others[:5]:
                tries = [(f"{names_a[i]} changed",
                          _changed(classes[i], ra[i]),
                          _changed(classes[i], rb[i]))]
                for v in inputs.from_constants(classes[i], nums,
                                               strs)[:6]:
                    tries.append((f"{names_a[i]} set to {v!r}", v, v))
                for what, va, vb in tries:
                    if va is _SAME:
                        continue
                    if there[i] == "boolean" and vb is not None:
                        vb = bool(vb)
                    ca, pa = where(a, wa, names_a, ra)
                    cb, pb = where(b, wb, names_b, rb)
                    out.append((what,
                                (f"update {a.table(wa.name)} set"
                                 f" {a.q(names_a[i])} = %s where {ca}",
                                 [va] + pa),
                                (f"update {b.table(wb.name)} set"
                                 f" {b.q(names_b[i])} = %s where {cb}",
                                 [vb] + pb)))
            if others:
                i = others[0]
                ca, pa = where(a, wa, names_a, ra)
                cb, pb = where(b, wb, names_b, rb)
                out.append(("an update that changes nothing",
                            (f"update {a.table(wa.name)} set"
                             f" {a.q(names_a[i])} = {a.q(names_a[i])} where"
                             f" {ca}", pa),
                            (f"update {b.table(wb.name)} set"
                             f" {b.q(names_b[i])} = {b.q(names_b[i])} where"
                             f" {cb}", pb)))
                out.append(("one update over every row",
                            (f"update {a.table(wa.name)} set"
                             f" {a.q(names_a[i])} = {a.q(names_a[i])}", []),
                            (f"update {b.table(wb.name)} set"
                             f" {b.q(names_b[i])} = {b.q(names_b[i])}", [])))
        if "DELETE" in events:
            ca, pa = where(a, wa, names_a, ra)
            cb, pb = where(b, wb, names_b, rb)
            out.append(("a delete",
                        (f"delete from {a.table(wa.name)} where {ca}", pa),
                        (f"delete from {b.table(wb.name)} where {cb}", pb)))
        return out


_SAME = object()


def _plain(value, loose):
    """A decimal without its trailing zeros, where they say nothing."""
    from decimal import Decimal
    if loose and isinstance(value, Decimal) and value.is_finite():
        got = value.normalize()
        return got.quantize(Decimal(1)) if got == got.to_integral_value() \
            else got
    return value


def _changed(cls, value):
    """The same column's value, changed by class - so both sides get the
    same change of the same value."""
    import datetime as dt
    from decimal import Decimal
    if value is None:
        return {"integer": 7, "decimal": Decimal("7.5"), "float": 7.5,
                "text": "x", "boolean": 1, "date": dt.date(2024, 2, 29),
                "timestamp": dt.datetime(2024, 2, 29, 12, 0)}.get(cls, _SAME)
    if isinstance(value, bool):
        return not value
    if isinstance(value, int):
        return value + 1
    if isinstance(value, (Decimal, float)):
        return value + 1
    if isinstance(value, str):
        return value[:-1] if len(value) > 1 else value + "x"
    if isinstance(value, dt.datetime):
        return value + dt.timedelta(days=1)
    if isinstance(value, dt.date):
        return value + dt.timedelta(days=1)
    return _SAME


def _pg_statements(definition):
    """A PostgreSQL routine's definition as statements: a function's, and
    for a trigger the function's then the trigger's (`code_routines`
    joins them with ";\\n" after the dollar-quoted body)."""
    text = str(definition).strip()
    at = text.rfind(";\ncreate trigger") if "create trigger" in text.lower() \
        else -1
    if at < 0:
        m = re.search(r"(?is);\s*\n\s*(create\s+(constraint\s+)?trigger\b)",
                      text)
        at = m.start() if m else -1
    if at >= 0:
        return [text[:at], text[at + 1:].strip().rstrip(";")]
    return [text.rstrip(";")]


def prove_database(pair, db):
    """The `converted code` line of `check`: views by their rows on the
    hop's own servers, everything else by running it (see the module's
    docstring). None where the source keeps no code."""
    from .convert import translatable
    from .views import prove_views
    verdicts = prove_views(pair, db)
    proof = Proof(pair, db)
    verdicts += proof.run()
    ok = {v.said.split(" ", 1)[1] for v in verdicts if v.status == "ok"}
    if pair.dst_engine.CANON_ENGINE == "postgres" and proof.kept:
        from . import keep
        blocks = []
        for r in proof.src_list:
            t = proof.target_of(r)
            if r.name in ok and t is not None and proof.kept.get(r.name):
                blocks += keep.tests(r, t, proof.kept[r.name])
        if blocks:
            try:
                keep.write(pair.hop.report_dir(db) / "code-tests.sql",
                           blocks)
            except OSError:
                pass
    others = len(ok - translatable(pair, db)) if ok else 0
    result = database_result(db, pair.hop.name, verdicts, proof.notes,
                             others)
    try:
        import json
        (pair.hop.report_dir(db) / "code-proof.json").write_text(json.dumps(
            {"verdicts": [v.record() for v in verdicts],
             "sandbox": "used" if proof.sandbox is not None
             else (proof.why_not or "not needed")},
            indent=1, default=str))
    except OSError:
        pass
    return result

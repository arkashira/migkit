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

#: what is said of a routine whose time values were compared within a
#: tolerance: it reads a clock no session could be set to or read ahead
CLOCK_TOLERATED = ("a time value it reads from a clock no session can be"
                   " set to was compared within 5 seconds")


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
    #: what the source's record of its calls says of it: {"runs",
    #: "shapes", "tried"} (`history`)
    history: dict = None

    def line(self):
        out = f"{self.said}: {self.status}"
        if self.reason:
            out += f" - {self.reason}"
        return out

    def record(self):
        return {"routine": self.said, "status": self.status,
                "reason": self.reason, "calls": self.calls,
                "coverage": self.coverage, "first": self.first,
                "constructs": self.constructs,
                **({"history": self.history} if self.history else {})}


def taken(eng, side, db, kind, name, table=None):
    """Whether the hop carries this object of the source - a routine by
    its kind, a trigger by its table too, a view by its name
    (`Engine.takes`, the hop's `objects` and scope). What the target holds
    is read as it is."""
    takes = getattr(eng, "takes", None)
    if side != "src" or takes is None:
        return True
    return takes(db, kind, f"{table}.{name}" if table else name, table)


def routines_of(eng, side, db, notes=None):
    """The routines a side keeps: its engine's catalogue, or - where the
    engine reads only functions of one expression, or its catalogue could
    not be read (said in `notes`, which keeps the verdict from `ok`) -
    those, as routines; of the source, only those the hop carries
    (`taken`)."""
    return [r for r in _routines(eng, side, db, notes) if taken(
        eng, side, db, r.kind, r.name, getattr(r, "table", None))]


def _routines(eng, side, db, notes=None):
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
    if got and eng.CANON_ENGINE == "oracle":
        # each public subprogram of a package, as a routine of its own
        from . import packages
        try:
            got = list(got) + packages.members(eng, side, db, notes)
        except Exception as e:  # noqa: BLE001 - said; the rest proved
            if notes is not None:
                notes.append("the packages could not be read: " + (
                    str(e).strip().splitlines() or [""])[0][:100])
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
                if one is not None and taken(eng, side, db, "function", n)}
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


def weighted(verdicts):
    """(runs on record whose routine answered every call the same, runs on
    record) - the application's calls, weighed by how often it makes
    them (`history`)."""
    total = sum((v.history or {}).get("runs", 0) for v in verdicts)
    same = sum((v.history or {}).get("runs", 0) for v in verdicts
               if v.status in ("ok", "warn"))
    return same, total


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
    same, total = weighted(verdicts)
    if total:
        detail += (f"; of the {total:,} calls on the source's record,"
                   f" {same / total:.1%} are to routines that answered every"
                   " call the same")
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
        from . import packages
        #: the source's packages, which the target keeps as schemas
        self.packages = {r.settings["package"] for r in self.src_list
                         if r.settings.get("package")}
        packages.qualify(self.dst_list, self.packages)
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
        #: the source's record of its calls, read once when first asked
        self.history = None
        #: per pure function, the calls made and the source's answers,
        #: kept as the reference after the cutover (`reference`)
        from . import reference
        self.reference = {} if reference.turned_on(pair.hop) and \
            reference.after_cutover(pair.hop) is None else None

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

    def run(self, schema=None):
        """The routines' verdicts; with `schema` (a list), the schema's own
        proofs on the same sandbox's fixture appended to it - its
        constraints, collations and sized types (`schema_proofs`)."""
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
        named = spec(self.pair.hop) is not None
        wanted = self.application_statements() if schema is not None \
            else []
        self.open((bool(runnable) and (needs or named))
                  or (schema is not None and named))
        try:
            if self.sandbox is not None:
                if schema is not None:
                    schema += self.schema_proofs()
                from . import partitions
                if runnable or wanted or (schema is not None and
                                          partitions.turned_on(
                                              self.pair.hop)):
                    verdicts += self._in_sandbox(runnable, wanted, schema)
            else:
                for r, t in runnable:
                    if self._on_own(r):
                        verdicts.append(self._on_own_servers(r, t))
                    else:
                        verdicts.append(Verdict(
                            r.said, "not proved", self.why_not or
                            "nothing was run"))
                for where, scope, stmts, _ in wanted:
                    if stmts:
                        schema.append(self._unrun(scope, where, stmts))
        finally:
            self.close()
            self.bank.save()
        return verdicts

    # ---- the application's own statements ------------------------------

    def application_statements(self):
        """[(where they came from, the line's scope, [Statement], the file
        they are written to)] the hop turns on: the source's record of its
        statements (`workload_replay`, `corpus`) and the application's
        code (`application_sql`, `appscan`). What could not be read is
        said in the notes."""
        from . import appscan, corpus
        hop, out = self.pair.hop, []
        if corpus.turned_on(hop):
            stmts, why = corpus.statements(self.pair, self.db)
            if why:
                self.notes.append(f"the workload was not replayed: {why}")
            if stmts:
                corpus.keep(hop, self.db, stmts)
                out.append(("the source's record", "workload replayed",
                            stmts, "workload-replay"))
        if appscan.turned_on(hop):
            stmts, said = appscan.found(
                hop, self.pair.src_engine.SQL_DIALECT or "mysql")
            self.notes += said
            if stmts:
                out.append(("the application's code",
                            "application statements", stmts,
                            "application-sql"))
            elif not said:
                self.notes.append("the application's code named holds no"
                                  " statement migkit could find")
        return out

    def _unrun(self, scope, where, stmts):
        from ..engines.base import Result
        why = self.why_not or ("no sandbox is named for this hop"
                               " (proof_sandbox)")
        return Result("schema", f"{self.db} {scope}", "warn",
                      f"{len(stmts)} statements from {where}, none proved:"
                      f" {why}", "",
                      "name a sandbox for the proof (hop option"
                      " proof_sandbox: docker, or two scratch servers)")

    #: the mined cases of the functions the source's code calls, and those
    #: its target answers apart (`builtins`)
    mined, parting = {}, {}

    def builtin_cases(self, schema):
        """The cases the engines' own tests hold for the functions the
        source's code calls, run on both sandboxes before any routine is
        proved, so their constants are among every routine's inputs and a
        case answered apart is said (`builtins`)."""
        from . import builtins
        hop = self.pair.hop
        if not builtins.turned_on(hop):
            return
        texts = [r.body or r.settings.get("expression") or ""
                 for r in self.src_list]
        wanted = builtins.called(texts)
        if not wanted:
            return
        self.mined = builtins.mine(
            hop, self.pair.src_engine.SQL_DIALECT or "mysql", wanted)
        try:
            got = builtins.run(self, self.mined)
        except Exception as e:  # noqa: BLE001 - said, the proof goes on
            self.notes.append("the engines' own tests could not be run: "
                              + (str(e).strip().splitlines()
                                 or [""])[0][:100])
            return
        builtins.keep(hop, self.db, f"{self.pair.src_name}->"
                                    f"{self.pair.dst_name}", got)
        self.parting = {fn: v["apart"] for fn, v in got.items()
                        if v["apart"]}
        line = builtins.result(self.db, got)
        if line is not None and schema is not None:
            schema.append(line)

    def statement_proofs(self, wanted, schema):
        """The application's statements proved on the sandbox, its
        routines made in it (`statements`): a line each for the record's
        and the code's, their verdicts beside the reports."""
        from . import statements
        for where, scope, stmts, stem in wanted:
            got = statements.prove(self, stmts)
            statements.write(self.pair.hop.report_dir(self.db) / stem, got)
            line = statements.result(self.db, got, where, scope)
            if line is not None and schema is not None:
                schema.append(line)

    def schema_proofs(self):
        """[Result]: the schema's own proofs on the sandbox's fixture, made
        before any routine is made in either server - so a trigger the
        conversion got wrong is not taken for a constraint that differs."""
        from ..engines.base import Result
        from . import bench, collations, constraints, sizing
        out = []
        try:
            b = bench.Bench(self)
        except Exception as e:  # noqa: BLE001 - said, never ok
            return [Result("schema", f"{self.db} {what}", "error",
                           "the sandbox's fixture could not be read: " + (
                               str(e).strip().splitlines() or [""])[0][:120])
                    for what in ("constraints", "collations")]
        try:
            for what, proved in (("constraints", constraints.prove),
                                 ("collations", collations.prove),
                                 ("sized types", sizing.prove)):
                try:
                    out += proved(b)
                except HarnessError as e:
                    out.append(Result("schema", f"{self.db} {what}", "error",
                                      f"the sandbox could not run it: {e}"))
                except Exception as e:  # noqa: BLE001 - said, never ok
                    b.a.conn.rollback()
                    b.b.conn.rollback()
                    out.append(Result(
                        "schema", f"{self.db} {what}", "error",
                        "it could not be proved: " + (
                            str(e).strip().splitlines() or [""])[0][:120]))
        finally:
            b.close()
        return out

    # ---- a function asked on the hop's own servers --------------------------

    def _on_own(self, r):
        """Whether a routine may be asked on the hop's own servers: a
        function, asked in a read-only transaction on each - a write it
        tries is refused by the server itself, and then it is said to need
        the sandbox. A procedure, a trigger or an event is there to write,
        and is only run in the sandbox."""
        return r.kind == "function"

    def _recorded(self, r):
        """(argument tuples as the application calls `r`, the most run
        first; {parameter: the values it is called with}; what the record
        says of it, for the verdict) - from the source's record of its
        calls, where the hop's masking allows (`history`)."""
        from . import history
        if self.history is None:
            self.history = history.read(self.pair, self.db, self.src_list)
            if self.history.why:
                self.notes.append("the source's record of its calls was not"
                                  f" used: {self.history.why}")
        rec = self.history.of(r)
        if rec is None or not rec.shapes:
            return [], {}, None
        compared = inputs.compared_columns(
            r.body or r.settings.get("expression"),
            [p.name for p in r.params])
        ok = history.allowed(self.pair.hop, r, compared)
        tuples = rec.tuples(r.params, ok)
        values = {p.name: rec.values(k, ok) for k, p in enumerate(r.params)
                  if p.mode != "out"}
        return tuples, {k: v for k, v in values.items() if v}, {
            "runs": rec.runs, "shapes": len(rec.shapes),
            "tried": len(tuples)}

    def _args(self, r, t, extra=()):
        pools = []
        nums, strs = inputs.constants(r.body or r.settings.get("expression"),
                                      self.pair.src_engine.SQL_DIALECT or
                                      "mysql")
        if self.mined:
            from . import builtins
            more = builtins.constants(
                self.mined, r.body or r.settings.get("expression") or "",
                self.pair.src_engine.SQL_DIALECT or "mysql")
            nums, strs = nums | more[0], strs | more[1]
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
        calls, seen_error, masked = 0, set(), False
        mode = harness.clock_mode(a, b, _text(r), _text(t))
        recorded, values, said = self._recorded(r)
        ref = self._referenced(r, pair.src_engine.CANON_ENGINE)
        try:
            for args in _once(recorded + self._args(r, t, values)
                              + self._bounded(r, t)):
                try:
                    x, y, clock = self._both(
                        a, b, lambda: self._render(r, a.call(
                            r, list(args), [])),
                        lambda: self._render(r, b.call(
                            t, list(args), [])), mode)
                except HarnessError as e:
                    return Verdict(r.said, "not proved", f"a call could not"
                                   f" be made: {str(e)[:100]}")
                calls += 1
                if ref is not None and len(ref) < 20:
                    ref.append((args, x))
                if any(o.error and (o.error[0] == "25006" or "READ ONLY" in
                                    str(o.error[1]).upper())
                       for o in (x, y)):
                    return Verdict(r.said, "not proved",
                                   "it writes, and the hop's own servers are"
                                   " only read: it needs the sandbox", calls)
                found = differences(x, y, clock)
                if clock is True and not found and differences(x, y):
                    masked = True
                if found:
                    self.bank.keep(f"{r.kind}:{r.name}", args)
                    return Verdict(r.said, "diff", self._first(r, args,
                                                               found),
                                   calls, first={"args": repr(args),
                                                 "found": found},
                                   history=said)
                if x.error:
                    seen_error.add(harness.error_key(x.error)[0])
        finally:
            a.close()
            b.close()
        if r.settings.get("expression") is not None:
            short = []
            if seen_error & set(harness.UNKNOWN_CLASSES):
                short.append("some calls errored on both sides with no class"
                             " to compare")
            if masked:
                short.append(CLOCK_TOLERATED)
            bound = getattr(self, "bounded", {}).get(r.name)
            return Verdict(r.said, "warn" if short else "ok",
                           "; ".join(short), calls,
                           coverage="one expression, reached by every call"
                           + (f"; {bound}" if bound else ""),
                           history=said)
        return Verdict(r.said, "warn", f"{calls} calls answered the same on"
                       " the hop's own servers, read-only, but which of its"
                       " statements ran was not measured: " + (
                           self.why_not if spec(self.pair.hop) is not None
                           and self.why_not else "no sandbox is named for"
                           " this hop (proof_sandbox)"), calls,
                       history=said)

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
        nothing it does, and beside an instrumented copy of its own where
        they would (`runner.twins`, `mysql_pg.instrument`). Several rounds,
        for a body the server checks against routines made after it."""
        from . import mysql_pg
        probes, failed = {}, {}
        runner.twins, runner.opaque = {}, {}
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
                    stmts, got, twin = self._statements(r, runner.engine)
                    runner.drop(r)
                    if twin:
                        runner.drop(Routine(kind=r.kind, name=twin))
                    runner.create(stmts, r.settings)
                    name = r.name if r.kind != "event" else \
                        f"migkit_event_{r.name}"
                    if got:
                        probes[name] = got
                    if twin:
                        runner.twins[name] = twin
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
            if got and got[2]:
                # the routine as it is, and its instrumented copy beside
                return [r.definition, got[0]], got[1], got[2]
            if got:
                return [got[0]], got[1], None
            return [r.definition], None, None
        if engine == "oracle" and r.settings.get("made"):
            # a package's member: its spec and body, made whole
            return list(r.settings["made"]), None, None
        if engine in ("mssql", "oracle"):
            # a CREATE is a batch of its own; the server keeps it whole
            return [r.definition], None, None
        if engine == "postgres" and "." in r.name and \
                r.settings.get("package"):
            # a package's member on the target: its schema first
            return [f"create schema if not exists"
                    f" {runner_ident(r.name.split('.')[0])}"] + \
                [s for s in _pg_statements(r.definition)], None, None
        return [s for s in _pg_statements(r.definition)], None, None

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
            v = self.prove_one(self.a, self.b, r, t)
            if v.history is None and r.kind in ("function", "procedure"):
                v.history = self._recorded(r)[2]
            return v
        except HarnessError as e:
            return Verdict(r.said, "not proved",
                           f"the sandbox could not run it: {e}")

    def made(self, r):
        """The target's version of `r` as the target's sandbox now keeps
        it, or None."""
        from . import packages
        sb = self.sandbox
        return by_key(packages.qualify(sb.dst.code_routines("dst", sb.tdb),
                                       self.packages)).get(r.key)

    def install(self, r, statements):
        """A version already proved, made again in the target's sandbox in
        place of whatever stands there, without proving it again."""
        b = self.b
        b.drop(Routine(kind={"event": "procedure"}.get(r.kind, r.kind),
                       name=r.name))
        b.clear(statements)
        b.create(statements)
        t = self.made(r)
        if t is not None:
            self._probed(t)

    def _probed(self, t):
        """A version made in a MySQL target's sandbox made again with the
        probes its coverage is measured by - in its own place, or beside
        it where they would change what it reads - as the target's
        routines made at the start are (`_create_all`)."""
        b = self.b
        if b.engine != "mysql" or t.definition is None:
            return
        try:
            stmts, got, twin = self._statements(t, "mysql")
        except HarnessError:
            return
        if not got:
            return
        name = t.name if t.kind != "event" else f"migkit_event_{t.name}"
        try:
            b.drop(t)
            if twin:
                b.drop(Routine(kind=t.kind, name=twin))
            b.create(stmts, t.settings)
        except Exception:  # noqa: BLE001 - made again as it was
            b.conn.rollback()
            b.create([t.definition], t.settings)
            return
        self.dst_probes[name] = got
        if not hasattr(b, "twins"):
            b.twins, b.opaque = {}, {}
        if twin:
            b.twins[name] = twin
        else:
            b.twins.pop(name, None)
        b.opaque.pop(name, None)

    def try_version(self, r, statements):
        """One candidate version of `r`, made in the target's sandbox in
        place of whatever stood there, gated (the target must take it; its
        static check, where it has one, must find nothing - fail closed)
        and proved. (verdict, the version as made, or None)."""
        b = self.b
        kind = {"event": "procedure"}.get(r.kind, r.kind)
        b.drop(Routine(kind=kind, name=r.name))

        def gone():
            # whatever the version made, not only the routine of its name:
            # a trigger made once per event leaves the others to fire in
            # every later proof
            b.drop(Routine(kind=kind, name=r.name))
            b.clear(statements)
        try:
            b.clear(statements)
            b.create(statements)
        except Exception as e:  # noqa: BLE001 - the candidate's failure
            b.conn.rollback()
            gone()
            return Verdict(r.said, "diff", "the target refuses it:"
                           f" {(str(e).strip().splitlines() or [''])[0][:160]}"
                           ), None
        t = self.made(r)
        if t is None:
            gone()
            return Verdict(r.said, "diff", f"it makes no {kind} named"
                           f" {r.name}"), None
        self.dst_failed.pop(t.name, None)
        findings = [f for f in (b.check(t) if hasattr(b, "check") else [])
                    if str(f[0]).startswith("error")]
        if findings:
            gone()
            level, message, line = findings[0]
            return Verdict(r.said, "diff", "the target's static check finds:"
                           f" {message}" + (f" (line {line})" if line
                                            else "")), None
        self._probed(t)
        v = self.verdict_for(r, t)
        if v.status not in ("ok", "warn"):
            gone()
        return v, t

    def _in_sandbox(self, runnable, wanted=(), schema=None):
        out = []
        try:
            self.begin(self.dst_list)
            self.builtin_cases(schema)
            for r, t in runnable:
                out.append(self.verdict_for(r, t))
            self.in_sequences(runnable, out)
            if wanted:
                self.statement_proofs(wanted, schema)
            from . import partitions
            if schema is not None and partitions.turned_on(self.pair.hop):
                partitions.advise(self, schema)
        finally:
            self.end()
        return out

    def in_sequences(self, runnable, verdicts):
        """The routines every single call of which answered the same, held
        to the source over sequences of calls (`sequences`): a sequence
        that differs makes the verdict of the routine whose call differed
        `diff`, naming the calls before it. [Found]."""
        from . import sequences
        pairs = [(r, t) for (r, t), v in zip(runnable, verdicts)
                 if v.status in ("ok", "warn") and t is not None and
                 t.name not in self.dst_failed and
                 r.name not in self.src_failed]
        if not self.searching or not pairs:
            return []
        try:
            found, runs, left = sequences.prove(self, pairs)
        except HarnessError as e:
            self.notes.append("sequences of calls were not run: the"
                              f" sandbox could not run them ({e})")
            return []
        by = {v.said: v for v in verdicts}
        for f in found:
            r = f.calls[f.at][0]
            v = by.get(r.said)
            if v is None:
                continue
            v.status = "diff"
            v.reason = "in a sequence of calls, " + f.said()
            v.first = {"sequence": [[x.name, repr(a)] for x, a in
                                    f.calls[:f.at + 1]], "found": f.what}
        self.notes += sequences.said(found, runs, left)
        return found

    # ---- one routine --------------------------------------------------------

    def prove_one(self, a, b, r, t):
        """One routine run on both sides; its verdict."""
        tables = list(a.watched)
        if r.kind == "trigger":
            return self._trigger(a, b, r, t, tables)
        writes = sorted(written(r, self.src_known, tables)) \
            if r.kind != "event" else tables
        # a version that commits where the source does not is still run
        # as one that commits: held in the proof's transaction, it would
        # commit the fixture away under every call after it
        commits = commits_itself(a.engine, r) or \
            commits_itself(b.engine, t)
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
        argsets = self.argsets(a, r, t)
        mode = harness.clock_mode(a, b, _text(r), _text(t), commits)
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
        ref = self._referenced(r, a.engine)

        def once(args):
            nonlocal masked, unknown
            x, y, clock = self._both(
                a, b, lambda: self._call(a, r, call_r, args, writes,
                                         commits, mode),
                lambda: self._call(b, r, t, args, writes, commits, mode),
                mode)
            if kept is not None and len(kept) < keep.KEEP:
                kept.append((args, x))
            if ref is not None and len(ref) < keep.KEEP:
                ref.append((args, x))
            found = differences(x, y, clock)
            if clock is True and not found and differences(x, y):
                masked = True
            if x.error and y.error and harness.error_key(x.error)[0] in \
                    harness.UNKNOWN_CLASSES:
                unknown = True
            return found

        def apart(args, what, n):
            v = Verdict(r.said, "diff", self._first(r, args, what), n,
                        first={"args": repr(args), "found": what}, args=args)
            return self._where(v, r, t, args, writes, commits)

        for args in argsets:
            found = once(args)
            calls += 1
            if found:
                small = self._shrink(once, r, args)
                self.bank.keep(f"{r.kind}:{r.name}", small)
                what = once(small)
                return apart(small, what, calls)
        per_call = (time.monotonic() - began) / max(1, calls)
        found = self._search(once, r, per_call) if self.searching else None
        if found:
            args, what = found
            self.bank.keep(f"{r.kind}:{r.name}", args)
            return apart(args, what, calls + self._searched)
        calls += self._searched
        for args in (list(self._solved(a, r)) + self._bounded(r, t)
                     if self.searching else []):
            calls += 1
            found = once(args)
            if found:
                self.bank.keep(f"{r.kind}:{r.name}", args)
                return apart(args, found, calls)
        return self._covered(a, b, r, t, calls, findings, masked, unknown)

    def _referenced(self, r, engine):
        """The list a pure function's calls are kept in for the reference
        after the cutover, or None (`reference`)."""
        from . import reference
        if self.reference is None or not reference.pure(r, engine):
            return None
        return self.reference.setdefault(r.name, [])

    def reference_classes(self):
        """{name: (the class a kept function returns, whether a decimal is
        compared by its value)} - how its kept answers were rendered."""
        loose = self.pair.src_engine.CANON_ENGINE == "oracle"
        by = {r.name: r for r in self.src_list}
        return {n: (self._classes(by[n])[0], loose or bool(
            by[n].returns and "(" not in str(by[n].returns)))
            for n in (self.reference or {}) if n in by}

    def _where(self, v, r, t, args, writes, commits):
        """A differing verdict with where the two versions first part,
        from the states each reports statement by statement (`trace`),
        where both engines can report them."""
        if not self.searching:
            return v
        from . import trace
        try:
            where = trace.first_apart(self, r, t, args, writes, commits)
        except Exception:  # noqa: BLE001 - the verdict stands as it is
            where = None
        if where:
            v.reason += f"; {where}"
            v.first["state"] = where
        return v

    def _bounded(self, r, t):
        """Inputs the solver finds where a function of one expression and
        its target's version answer apart (`scalar`); each is run like any
        other, and only the run decides."""
        from . import scalar
        try:
            return scalar.counterexamples(self, r, t)
        except Exception:  # noqa: BLE001 - the calls made decide alone
            return []

    def argsets(self, a, r, t):
        """What `r` is called with, in the order tried: every input that
        ever told two versions of it apart, the calls the application
        makes (`history`), then the class pools, the body's constants and
        the fixture's values."""
        extra = self._fixture_values(a, r)
        recorded, values, _ = self._recorded(r) \
            if r.kind in ("function", "procedure") else ([], {}, None)
        for p, vs in values.items():
            extra[p] = vs + [v for v in extra.get(p, []) if v not in vs]
        return _once(list(self.bank.inputs(f"{r.kind}:{r.name}"))
                     + recorded + self._args(r, t, extra))

    def _both(self, a, b, on_a, on_b, mode):
        """(the source's outcome, the target's, how their time values are
        compared - `differences`' `clock`) of one call made on both
        sides by `on_a` and `on_b`, the two sessions' clocks set to one
        instant where `mode` (`harness.clock_mode`) says they can be: both
        to now where both can be set, else the side that cannot is called
        first and the other set to the instant it read."""
        how, first, reads = mode
        try:
            if how == "pinned" and first is None:
                a.at = b.at = _utcnow()
            if first == "b":
                y = on_b()
                a.at = harness._instant(y.now)
                x = on_a()
            else:
                x = on_a()
                if first == "a":
                    b.at = harness._instant(x.now)
                y = on_b()
        finally:
            a.at = b.at = None
        clock = {None: False, "pinned": False}.get(how, how)
        if how == "pinned" and reads and (x.now is None or y.now is None):
            # an instant that could not be read ahead (the call failed
            # before it): this call's times are compared within the
            # tolerance, and said
            clock = True
        return x, y, clock

    def _call(self, runner, r, routine, args, writes, commits, mode):
        """One call of `routine` on `runner`, rendered in the source's
        classes - and of the instrumented copy it is measured by, where it
        has one (`mysql_pg.instrument`): the copy is held to the routine
        call by call, and the first answer it gives apart is kept
        (`runner.opaque`), so its coverage is not counted."""
        got = self._render(r, runner.call(routine, list(args), writes,
                                          commits))
        twin = getattr(runner, "twins", {}).get(routine.name)
        if twin and routine.name not in runner.opaque:
            copy = Routine(kind=routine.kind, name=twin,
                           params=routine.params, returns=routine.returns,
                           returns_set=routine.returns_set)
            mine = self._render(r, runner.call(copy, list(args), writes,
                                               commits))
            apart = differences(got, mine, {None: False, "pinned": False}
                                .get(mode[0], mode[0]))
            if apart:
                runner.opaque[routine.name] = (tuple(args), apart[0])
        return got

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
            short.append(CLOCK_TOLERATED)
        if unknown:
            short.append("some calls errored on both sides with no class to"
                         " compare")
        if r.kind == "event":
            short.append(self._event_said(r))
        cov_said.append(getattr(self, "bounded", {}).get(r.name, ""))
        if self.parting:
            from . import builtins
            cov_said += builtins.caveats(self.parting, _text(r))
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
                return "", [f"the {side}'s coverage cannot be measured: "
                            + _unprobed(routine)]
            apart = getattr(runner, "opaque", {}).get(name)
            if apart:
                args, what = apart
                return "", [f"the {side}'s coverage is not counted: the"
                            " instrumented copy it is measured by answered"
                            f" {args!r} otherwise than the routine ({what})"]
            hit = runner.probes_hit(Routine(kind=routine.kind, name=name))
            missed = [(ln, what) for n, ln, what in mine if n not in hit]
            said = f"{side}: {len(mine) - len(missed)}/{len(mine)} blocks"
            if missed:
                where = ", ".join(f"line {ln} ({w})" for ln, w in missed[:4])
                return said, [f"on the {side}, never reached: {where}"]
            return said, []
        if runner.engine == "oracle":
            got = runner.coverage(routine)
            if got is None:
                return "", [f"the {side}'s coverage was not measured: "
                            + (runner.coverage_off or "its sandbox keeps"
                               " no block coverage")]
            reached, blocks, missed = got
            said = f"{side}: {reached}/{blocks} blocks"
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
        calls, masked = 0, False
        mode = harness.clock_mode(a, b, _text(r), _text(t))
        for what, (sa, pa), (sb_, pb) in statements:
            x, y, clock = self._both(
                a, b, lambda: a.run(sa, pa, tables),
                lambda: b.run(sb_, pb, tables), mode)
            calls += 1
            found = differences(x, y, clock)
            if clock is True and not found and differences(x, y):
                masked = True
            if found:
                return Verdict(r.said, "diff", f"{what}: {found[0]}", calls,
                               first={"statement": what, "found": found})
        return self._covered(a, b, r, t, calls, findings, masked, False)

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


def _unprobed(routine):
    """Why a MySQL routine carries no probes."""
    if routine.kind == "trigger" and re.search(
            r"(?i)\b(row_count|found_rows)\s*\(|\bget\s+(current\s+)?"
            r"diagnostics\b", routine.body or ""):
        return ("it is a trigger reading ROW_COUNT(), FOUND_ROWS() or the"
                " diagnostics area, which a probe would change, and a"
                " trigger fires by itself: no copy can be called beside it")
    if re.search(r"(?i)\bget\s+(current\s+)?diagnostics\s+condition\s+"
                 r"[^\d\s]", routine.body or ""):
        return ("it reads a condition by a number held in a variable,"
                " which no copy can keep for it")
    return "its body could not be read into blocks to probe"


def commits_itself(engine, r):
    """Whether a routine commits or rolls back for itself, as its own
    body says - so it cannot be run inside a transaction the proof rolls
    back, or held with others in one. An Oracle routine in an autonomous
    transaction, and a PostgreSQL one run over a loopback connection (the
    emulation of one), commit what they write whatever the caller does."""
    from . import mysql_pg, tsql_pg
    if r is None or not r.body:
        return False
    if engine == "oracle":
        return bool(re.search(r"(?i)\bpragma\s+autonomous_transaction\b|"
                              r"\b(commit|rollback)\b\s*(work\b\s*)?;",
                              r.body))
    if engine == "postgres" and re.search(r"(?i)\bdblink_exec\s*\(",
                                          r.body):
        return True
    try:
        if engine == "mysql":
            return any(n.kind == "tx" for n in mysql_pg.read(r.body).walk())
        if engine == "mssql":
            return any(n.kind == "tx" and n.what != "begin"
                       for s in tsql_pg._Parser(r.body).statements(
                           until=()) for n in s.walk())
        if engine == "postgres" and r.kind == "procedure" and \
                r.language == "plpgsql" and r.definition:
            return _plpgsql_commits(r.definition)
    except Refused:
        return False
    return False


def _plpgsql_commits(definition):
    """Whether a PL/pgSQL procedure's body holds a COMMIT or a ROLLBACK,
    as PostgreSQL's own parser reads it."""
    import pglast

    def walk(node):
        if isinstance(node, dict):
            if "PLpgSQL_stmt_commit" in node or \
                    "PLpgSQL_stmt_rollback" in node:
                return True
            return any(walk(v) for v in node.values())
        if isinstance(node, list):
            return any(walk(v) for v in node)
        return False
    try:
        return walk(pglast.parse_plpgsql(definition))
    except Exception:  # noqa: BLE001 - read by its words instead
        return bool(re.search(r"(?im)^\s*(commit|rollback)\s*;",
                              definition))


def runner_ident(name):
    """A schema's name as PostgreSQL reads it."""
    from .plpgsql import ident
    return ident(name)


def _once(argsets):
    """Argument tuples in their order, each once."""
    seen, out = set(), []
    for args in argsets:
        key = repr(args)
        if key not in seen:
            seen.add(key)
            out.append(args)
    return out


def _text(routine):
    """What of a routine is read for the clock it reads: its body, or
    its one expression."""
    if routine is None:
        return ""
    return routine.body or routine.settings.get("expression") or ""


def _utcnow():
    import datetime as dt
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


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


def prove_database(pair, db, schema=None):
    """The `converted code` line of `check`: views by their rows on the
    hop's own servers, everything else by running it (see the module's
    docstring). None where the source keeps no code. With `schema` (a
    list), the schema's own proofs on the sandbox are appended to it."""
    from . import reference
    from .convert import translatable
    from .views import prove_views
    after = reference.check(pair, db)
    if after is not None and schema is not None:
        schema.append(after)
    verdicts = prove_views(pair, db)
    proof = Proof(pair, db)
    verdicts += proof.run(schema)
    if proof.reference:
        reference.record(pair, db, {n: c for n, c in proof.reference.items()
                                    if c}, proof.reference_classes())
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

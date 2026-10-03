"""The source's own statements as a workload corpus, replayed on the
target's engine (leapfrog research P10, backlog CV.P10).

The source keeps a record of what the application ran (`history`
reads it, read-only: MySQL's statement digests with one sample each,
PostgreSQL's `pg_stat_statements`, SQL Server's Query Store, Oracle's
shared pool with the values its binds were captured with). Every
statement on it that reads or writes rows - not migkit's own, not a read
of the server's catalogue, not a call of a routine (those are the stored
code's proof) - is one entry of the corpus, weighed by how often it ran.
With the hop option `workload_replay` each entry is:

* translated into the target's SQL through the hop's restructuring and
  run on both sandboxes over the same fixture, answers and rows changed
  compared (`statements`) - a write is run there, never on either of the
  hop's own servers;
* planned and timed on the hop's own servers by the workload comparison
  (`workload.compare`), its reads held to the same answer - the corpus is
  its list of statements, so an Oracle source takes part through the
  shared pool (`source`): each statement's plan read from `V$SQL_PLAN`
  and its time from the pool's own average, since neither `EXPLAIN PLAN`
  (which writes its plan table) nor a timed run is made on the source.

Masked: a sample's literal values are run only where the hop's masking
allows - none under `mask: all`, and not a sample comparing or writing a
masked column; the statement's placeholders are then filled from the
fixture instead (`statements.fillings`). What is kept beside the reports
(`workload-corpus.json`) is each statement's normalised text - every
literal a `?` - its kind and how often it ran, never a value; a statement
seen on an earlier run and gone from the server's record since stays in
it, so the corpus grows as the record turns over.
"""
import datetime as _dt
import json
import re

from sqlglot import exp

from .statements import Holes, Statement, _partner, squash, tree_of

#: statements timed on the hop's own servers per kind, at most, where the
#: corpus is the workload comparison's list (within its time budget)
TIMED = 100
#: the kinds of statement the corpus holds: what reads or writes rows
READS = ("select", "with", "values")
WRITES = ("insert", "update", "delete", "merge", "replace", "upsert")
#: schemas and names that are a server's own catalogue, never the
#: application's rows
CATALOGUE = re.compile(r"(?i)^(pg_catalog|information_schema|"
                       r"performance_schema|mysql|sys|pg_toast)$")
CATALOGUE_NAMES = re.compile(r"(?i)^(pg_\w+|v\$\w+|gv\$\w+|all_\w+|dba_\w+|"
                             r"user_\w+|dual|sysobjects|syscolumns)$")


def turned_on(hop):
    return bool((hop.options or {}).get("workload_replay"))


def kind_of(text):
    first = re.match(r"\s*\(?\s*([A-Za-z]+)", text or "")
    word = first.group(1).lower() if first else ""
    if word in READS:
        return "read"
    if word in WRITES:
        return "write"
    return None


def _catalogue(tree):
    for t in tree.find_all(exp.Table):
        if CATALOGUE.match(str(t.args.get("db") or "")) or \
                CATALOGUE_NAMES.match(t.name or ""):
            return True
    return False


def normalized(text, dialect):
    """The statement with every literal and placeholder a `?`: what may be
    shown and kept of it."""
    tree = tree_of(text, dialect)
    if tree is None:
        return squash(re.sub(r"'(?:[^']|'')*'|\b\d+(\.\d+)?\b", "?",
                             text or ""), 160)
    for lit in list(tree.find_all(exp.Literal, exp.Placeholder)):
        lit.replace(exp.Var(this="?"))
    try:
        return squash(tree.sql(dialect=dialect), 160)
    except Exception:  # noqa: BLE001 - said by its words instead
        return squash(re.sub(r"'(?:[^']|'')*'|\b\d+(\.\d+)?\b", "?",
                             text or ""), 160)


def sample_allowed(hop, tree):
    """Whether a sample's literal values may be run: not under `mask:
    all`, and not where one is compared with or written to a masked
    column."""
    from .. import masking
    rules = masking._rules(hop)
    if rules is None:
        return True
    if rules is True:
        return False
    aliases = {str(t.alias_or_name).lower(): t.name
               for t in tree.find_all(exp.Table)}
    only = list(dict.fromkeys(aliases.values()))
    for lit in tree.find_all(exp.Literal):
        got = _partner(lit)
        if not isinstance(got, tuple):
            continue
        table, column = got
        name = aliases.get(table.lower()) if table else None
        for t in ([name] if name else only):
            if masking.column(hop, t, column):
                return False
    return True


def _records(eng, db):
    """[(text, runs, sample)] from the source's record, or a reason."""
    from . import history
    engine = eng.CANON_ENGINE
    if engine == "oracle":
        return [(t, r, s) for _, t, r, s, _ in
                history.oracle_statements(eng, db)]
    reader = history.READERS.get(engine)
    if reader is None:
        return f"{engine} keeps no record of its statements migkit reads"
    return reader(eng, db)


def statements(pair, db):
    """([Statement] of the corpus, why none could be read or None)."""
    from .harness import MARK
    eng = pair.src_engine
    dialect = eng.SQL_DIALECT
    try:
        got = _records(eng, db)
    except Exception as e:  # noqa: BLE001 - said, as the reason
        return [], ("the source's record of its statements could not be"
                    " read: " + squash(e, 100))
    if isinstance(got, str):
        return [], got
    own = tuple(getattr(eng, "MIGKIT_MARKS", ()))
    by = {}
    for text, runs, sample in got:
        text, sample = str(text or ""), str(sample or "")
        if eng.CANON_ENGINE == "mssql":
            from ..engines.mssql import _without_parameters
            text = _without_parameters(text)
            sample = _without_parameters(sample)
        kind = kind_of(text)
        if kind is None or MARK in text or MARK in sample:
            continue
        if any(m in f"{text} {sample}".lower() for m in own):
            continue
        whole = bool(sample) and not sample.rstrip().endswith("...") \
            and not Holes(sample, dialect).tokens
        tree = tree_of(sample if whole else text, dialect)
        if tree is not None and _catalogue(tree):
            continue
        label = normalized(text, dialect)
        one = by.get(label)
        if one is None:
            run = sample if whole and tree is not None and \
                sample_allowed(pair.hop, tree) else text
            cut = run.rstrip().endswith("...") or "(...)" in run
            one = by[label] = Statement(
                origin=f"record: {squash(label, 60)}", text=run,
                runs=0, label=label, kind=kind,
                dynamic="the source's record keeps it cut short" if cut
                else None)
        one.runs += int(runs or 0)
    return sorted(by.values(), key=lambda s: -s.runs), None


def keep(hop, db, stmts):
    """The corpus beside the reports, merged with what earlier runs kept:
    each statement's normalised text, kind and how often it ran - never a
    value."""
    path = hop.report_dir(db) / "workload-corpus.json"
    try:
        old = json.loads(path.read_text()) if path.exists() else {}
    except (OSError, ValueError):
        old = {}
    now = _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")
    kept = {e["statement"]: e for e in old.get("statements", [])
            if isinstance(e, dict) and e.get("statement")}
    for s in stmts:
        e = kept.setdefault(s.label, {"statement": s.label, "kind": s.kind,
                                      "first seen": now})
        e["runs"] = max(int(e.get("runs", 0)), int(s.runs or 0))
        e["last seen"] = now
    try:
        path.write_text(json.dumps(
            {"read at": now, "statements": sorted(
                kept.values(), key=lambda e: -int(e.get("runs", 0)))},
            indent=1))
    except OSError:
        pass
    return len(kept)


# ---- an Oracle source in the workload comparison -----------------------------


class OracleSource:
    """An Oracle source as the workload comparison asks one: its
    statements from the shared pool, the busiest first, each with the
    values its binds were captured with where they all were; its plan as
    the pool keeps it (`V$SQL_PLAN`) and its time as the pool's own
    average - nothing run on the source to plan or time it; its answer
    read in a read-only transaction of its own. Every other attribute is
    the engine's."""

    #: its times are the source's own statistics, not a run: said so
    RECORDED_TIMES = True

    def __init__(self, eng):
        self._eng = eng
        self._ids, self._means = {}, {}

    def __getattr__(self, name):
        return getattr(self._eng, name)

    def workload_reads(self, db, n):
        return self._workload(db, n, "read")

    def workload_writes(self, db, n):
        return self._workload(db, n, "write")

    def _workload(self, db, n, kind):
        from . import history
        out = []
        for sql_id, text, runs, sample, mean in history.oracle_statements(
                self._eng, db):
            if kind_of(text) != kind:
                continue
            tree = tree_of(text, "oracle")
            if tree is None or _catalogue(tree):
                continue
            runnable = bool(sample) and not Holes(sample, "oracle").tokens \
                and sample_allowed(self._eng.hop, tree)
            stmt = sample if runnable else text
            self._ids[stmt] = sql_id
            self._means[stmt] = mean
            out.append((squash(normalized(text, "oracle"), 70), stmt,
                        runnable, mean, int(runs or 1)))
            if len(out) >= n:
                break
        return out

    def _index_tables(self, side, db, names):
        from ..engines.dbapi import MARK
        if not names:
            return {}
        owner = self._eng._owner(side, db)
        got = {}
        for name in names:
            rows = self._eng._rows(side, db, "select table_name from"
                                   f" all_indexes where owner = {MARK} and"
                                   f" index_name = {MARK}", (owner, name))
            if rows:
                got[name] = rows[0][0]
        return got

    def workload_plan(self, side, db, sql):
        """{table: "index <name>" or None where the plan reads it whole},
        as the shared pool keeps the statement's plan."""
        from ..engines.dbapi import MARK
        sql_id = self._ids.get(sql)
        if sql_id is None:
            raise LookupError("the statement is not in the shared pool")
        rows = self._eng._rows(
            side, db, "select operation, options, object_name,"
                      " object_type from v$sql_plan where sql_id ="
                      f" {MARK} and child_number = (select"
                      " min(child_number) from v$sql_plan where sql_id ="
                      f" {MARK}) and object_name is not null order by id",
            (sql_id, sql_id))
        indexes = self._index_tables(side, db, sorted(
            {str(o) for op, _, o, t in rows if str(op).upper() == "INDEX"}))
        out = {}
        for op, options, obj, _ in rows:
            op, options = str(op).upper(), str(options or "").upper()
            if op == "TABLE ACCESS":
                if "FULL" in options:
                    out.setdefault(str(obj), None)
                else:
                    out.setdefault(str(obj), out.get(str(obj)))
            elif op == "INDEX" and str(obj) in indexes:
                out[str(indexes[str(obj)])] = f"index {obj}"
        return out

    def workload_time(self, side, db, sql):
        """The statement's average time in ms as the shared pool counts
        it: a read is not run on the source to time it."""
        return self._means.get(sql)

    def workload_rows(self, side, db, sql, most):
        """The statement's answer, at most `most` rows, in a read-only
        transaction on a connection of its own, in the schema."""
        conn = self._eng._dedicated(side)
        try:
            cur = conn.cursor()
            cur.execute("alter session set current_schema ="
                        f" {self._eng._q(self._eng._owner(side, db))}")
            cur.execute("set transaction read only")
            cur.execute(sql)
            return [tuple(r) for r in cur.fetchmany(int(most))]
        finally:
            try:
                conn.rollback()
            finally:
                conn.close()


def source(eng):
    """The source engine as the workload comparison asks it: an Oracle
    source through its shared pool (`OracleSource`), any other as it
    is."""
    if getattr(eng, "workload_reads", None) is None and \
            getattr(eng, "CANON_ENGINE", "") == "oracle":
        return OracleSource(eng)
    return eng

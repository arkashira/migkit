"""What the application really calls each routine with (leapfrog
research P6): the source's own record of the statements it ran, read-only.

Every engine keeps one, in its own shape:

* MySQL: the statement digests (`performance_schema`), each a normalised
  text, how often it ran, and one sample with its literal values;
* PostgreSQL: `pg_stat_statements` - how often each normalised statement
  ran, constants replaced by `$n` in a query (a `CALL` kept with its
  literal arguments by the servers that do not normalise it);
* SQL Server: the procedures' own execution counts, the arguments each
  cached plan was compiled for, and the statements Query Store kept that
  call a function;
* Oracle: the shared pool's statements (`V$SQL`), how often each ran, and
  the values its binds were captured with (`V$SQL_BIND_CAPTURE`).

From them, per routine: the shapes it is called in - which argument is a
value written in the statement, which a NULL, which a placeholder the
application fills - and how often each ran. The values written in
statements go to the proof as inputs of their own, tried before the class
pools, the most frequent first; a placeholder is filled from the column the
body compares it with (the fixture's values, as before). Values are taken
only where the hop's masking allows: none at all under `mask: all`, and not
for a parameter the body compares with a masked column - a sample can hold
a person's data, and a value the proof takes can be shown in its verdict.
The executions on record weigh the verdict: the share of the application's
calls whose routine answered the same is said beside the routines' counts.
"""
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from .harness import MARK

#: statements read from the source's record, at most, per kind
MOST = 2000
#: argument tuples taken from the record per routine, at most
TUPLES = 40


#: what an argument of a recorded call is: a value written in the
#: statement, NULL, a placeholder the application fills, or an expression
VALUE, NULL, PLACEHOLDER, EXPRESSION = "value", "null", "placeholder", \
    "expression"


@dataclass
class Shape:
    """One way a routine is called, and how often it ran."""
    args: tuple                     #: ((kind, value or text), ...)
    runs: int = 0

    def said(self):
        return "(" + ", ".join({VALUE: "a value", NULL: "NULL",
                                PLACEHOLDER: "?", EXPRESSION: "an"
                                " expression"}[k] for k, _ in self.args) \
            + ")"


@dataclass
class Record:
    """Everything the source's record says of one routine."""
    shapes: list = field(default_factory=list)

    @property
    def runs(self):
        return sum(s.runs for s in self.shapes)

    def add(self, args, runs):
        for s in self.shapes:
            if s.args == args:
                s.runs += runs
                return
        self.shapes.append(Shape(args, runs))

    def tuples(self, params, allowed):
        """Argument tuples to call the routine with, the most run first:
        each shape's values where `allowed(k)` says parameter k may take
        a recorded value, and None where the statement gave NULL or the
        parameter is one the routine writes. A placeholder or an
        expression leaves the tuple out - the fixture's values and the
        pools reach that parameter."""
        out, seen = [], set()
        for s in sorted(self.shapes, key=lambda x: -x.runs):
            if len(s.args) != len(params):
                continue
            one = []
            for k, (kind, value) in enumerate(s.args):
                if kind == NULL or params[k].mode == "out":
                    one.append(None)
                elif kind == VALUE and allowed(k):
                    one.append(value)
                else:
                    break
            else:
                key = repr(one)
                if key not in seen:
                    seen.add(key)
                    out.append(tuple(one))
            if len(out) >= TUPLES:
                break
        return out

    def values(self, k, allowed):
        """The values recorded for parameter k, the most run first."""
        if not allowed(k):
            return []
        out = []
        for s in sorted(self.shapes, key=lambda x: -x.runs):
            if k < len(s.args) and s.args[k][0] == VALUE and \
                    s.args[k][1] not in out:
                out.append(s.args[k][1])
        return out


class History:
    """The source's record of its calls, by routine name (lower case)."""

    def __init__(self, records=None, why=None, statements=0):
        self.records = records or {}
        #: why nothing could be read, where it could not
        self.why = why
        self.statements = statements

    def of(self, routine):
        return self.records.get(routine.name.lower())

    @property
    def runs(self):
        return sum(r.runs for r in self.records.values())


# ---- the arguments of one recorded statement --------------------------------


def _split(text, at):
    """The arguments of the parenthesised list starting at `text[at]`
    ("("), split at the commas outside quotes and brackets; None where it
    does not close (a sample cut at the server's text limit)."""
    depth, quote, buf, out, i = 0, None, [], [], at
    while i < len(text):
        ch = text[i]
        if quote:
            buf.append(ch)
            if ch == "\\" and quote != '"' and i + 1 < len(text):
                buf.append(text[i + 1])
                i += 2
                continue
            if ch == quote:
                if i + 1 < len(text) and text[i + 1] == quote:
                    buf.append(quote)
                    i += 2
                    continue
                quote = None
            i += 1
            continue
        if ch in ("'", '"'):
            quote = ch
            buf.append(ch)
        elif ch == "(":
            depth += 1
            if depth > 1:
                buf.append(ch)
        elif ch == ")":
            depth -= 1
            if depth == 0:
                got = "".join(buf).strip()
                if got or out:
                    out.append(got)
                return out
            buf.append(ch)
        elif ch == "," and depth == 1:
            out.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    return None


def _string(text, engine):
    """A quoted literal's value as the engine reads it."""
    body = text[1:-1]
    if engine == "mysql":
        body = re.sub(r"\\(.)", lambda m: {"n": "\n", "t": "\t", "r": "\r",
                                           "0": "\x00", "Z": "\x1a"}.get(
            m.group(1), m.group(1)), body)
    return body.replace(text[0] * 2, text[0])


def argument(text, engine):
    """(kind, value) of one argument as the statement wrote it."""
    t = text.strip()
    low = t.lower()
    if low == "null":
        return (NULL, None)
    if re.fullmatch(r"\?|\$\d+|:\w+|@\w+|%s", t):
        return (PLACEHOLDER, None)
    if re.fullmatch(r"[+-]?\d+", t):
        return (VALUE, int(t))
    if re.fullmatch(r"[+-]?(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?", t):
        try:
            return (VALUE, Decimal(t))
        except InvalidOperation:
            return (EXPRESSION, t)
    m = re.fullmatch(r"(?s)[nN]?('(?:[^'\\]|''|\\.)*')", t)
    if m:
        return (VALUE, _string(m.group(1), engine))
    if engine == "mysql":
        m = re.fullmatch(r'(?s)("(?:[^"\\]|""|\\.)*")', t)
        if m:
            return (VALUE, _string(m.group(1), engine))
    if engine == "oracle" and re.fullmatch(r"(?i)'.*'", t) is None and \
            low in ("true", "false"):
        return (VALUE, low == "true")
    return (EXPRESSION, t)


def calls_in(text, names, engine):
    """[(routine name, (argument kinds and values))] for every call of one
    of `names` (lower case) in one statement: `CALL p(...)`, `EXEC p ...`,
    a PL/SQL block's `p(...)`, a function anywhere in an expression."""
    out = []
    if not text:
        return out
    q = r"[`\"\[]?"
    qe = r"[`\"\]]?"
    for m in re.finditer(rf"(?i)(?<![\w$#.`\"\]])(?:{q}\w+{qe}\s*\.\s*)?"
                         rf"{q}([A-Za-z_][\w$#]*){qe}\s*\(", text):
        name = m.group(1).lower()
        if name not in names:
            continue
        got = _split(text, m.end() - 1)
        if got is None:
            continue
        out.append((name, tuple(argument(a, engine) for a in got)))
    if engine == "mssql":
        # EXEC p 1, 'a' / EXEC p @x = 1: arguments without parentheses
        for m in re.finditer(r"(?i)\bexec(?:ute)?\s+(?:@\w+\s*=\s*)?"
                             r"(?:\[?\w+\]?\.)?\[?([A-Za-z_]\w*)\]?"
                             r"(?![\w(])\s*([^;]*)", text):
            name = m.group(1).lower()
            if name not in names:
                continue
            rest = m.group(2).strip()
            args = _split("(" + rest + ")", 0) if rest else []
            if args is None:
                continue
            args = [re.sub(r"^@\w+\s*=\s*", "", a) for a in args]
            args = [re.sub(r"(?i)\s+out(put)?$", "", a) for a in args]
            out.append((name, tuple(argument(a, engine) for a in args)))
    return out


# ---- the record, per engine ------------------------------------------------


def _mysql(eng, db):
    """(normalised text, runs, sample) of each digest in the database - a
    server before 8.0.3 keeps no sample, and gives the shapes alone."""
    where = (" from performance_schema.events_statements_summary_by_digest"
             " where SCHEMA_NAME = %s order by COUNT_STAR desc limit"
             f" {int(MOST)}")
    try:
        rows = eng._q("src", "select DIGEST_TEXT, COUNT_STAR,"
                             " QUERY_SAMPLE_TEXT" + where,
                      (eng._d("src", db),))
    except Exception:  # noqa: BLE001 - no sample column: the digests
        rows = [(d, n, "") for d, n in eng._q(
            "src", "select DIGEST_TEXT, COUNT_STAR" + where,
            (eng._d("src", db),))]
    return [(d or "", int(n or 0), s or "") for d, n, s in rows]


def _postgres(eng, db):
    if str(eng._psql("src", db, "select count(*) from pg_extension where"
                                " extname = 'pg_stat_statements'")) != "1":
        return "the source keeps no statement statistics here"
    rows = eng._catalog("src", db, f"""
        select query, calls from pg_stat_statements
         where dbid = (select oid from pg_database
                        where datname = current_database())
           and userid <> (select oid from pg_roles
                           where rolname = current_user)
         order by calls desc limit {int(MOST)}""")
    return [(q or "", int(n or 0), q or "") for q, n in rows]


def _mssql(eng, db):
    out = []
    # the procedures' own counts, and the arguments each cached plan was
    # compiled for - the values the application sent when it was made
    for name, runs, plan in eng._rows(
            "src", db, f"select top ({int(MOST)}) object_name(s.object_id),"
                       " s.execution_count, cast(p.query_plan as"
                       " nvarchar(max)) from sys.dm_exec_procedure_stats s"
                       " cross apply sys.dm_exec_query_plan(s.plan_handle) p"
                       " where s.database_id = db_id() order by"
                       " s.execution_count desc"):
        values = re.findall(r'ParameterCompiledValue="([^"]*)"',
                            str(plan or ""))
        names = re.findall(r'<ColumnReference Column="(@\w+)"\s+'
                           r'ParameterCompiledValue', str(plan or ""))
        args = [v[1:-1] if v.startswith("(") and v.endswith(")") else v
                for v in values]
        text = f"exec {name} " + ", ".join(
            f"{n} = {a}" for n, a in zip(names, args)) if names else ""
        out.append((text or f"exec {name}", int(runs or 0), text))
    try:
        for text, runs in eng._rows(
                "src", db, f"select top ({int(MOST)}) t.query_sql_text,"
                           " sum(r.count_executions) from"
                           " sys.query_store_query_text t join"
                           " sys.query_store_query q on q.query_text_id ="
                           " t.query_text_id join sys.query_store_plan p on"
                           " p.query_id = q.query_id join"
                           " sys.query_store_runtime_stats r on r.plan_id ="
                           " p.plan_id group by t.query_sql_text order by 2"
                           " desc"):
            out.append((text or "", int(runs or 0), text or ""))
    except Exception:  # noqa: BLE001 - Query Store off: the counts alone
        pass
    return out


def _oracle(eng, db):
    return [(text, runs, sample) for _, text, runs, sample, _ in
            oracle_statements(eng, db, (3, 47, 170))]


def oracle_statements(eng, db, commands=(2, 3, 6, 7, 47, 170, 189)):
    """[(sql id, text, runs, sample, mean ms)] of the shared pool's
    statements parsed in the schema, of the kinds `commands` names (V$SQL's
    command types: 2 INSERT, 3 SELECT, 6 UPDATE, 7 DELETE, 47 a PL/SQL
    block, 170 CALL, 189 MERGE), the most run first; each sample with the
    values its binds were captured with in the place of their marks."""
    from ..engines.dbapi import MARK
    owner = eng._owner("src", db)
    kinds = ", ".join(str(int(c)) for c in commands)
    rows = eng._rows("src", db, "select sql_id, sql_fulltext, executions,"
                                " elapsed_time / greatest(executions, 1) /"
                                " 1000 from v$sql where parsing_schema_name"
                                f" = {MARK} and command_type in ({kinds})"
                                " order by executions desc fetch first"
                                f" {int(MOST)} rows only", (owner,))
    binds = {}
    try:
        for sql_id, pos, value in eng._rows(
                "src", db, "select sql_id, position, value_string from"
                           " v$sql_bind_capture where was_captured = 'YES'"
                           " order by sql_id, position"):
            binds.setdefault(sql_id, {})[int(pos)] = value
    except Exception:  # noqa: BLE001 - no captured binds to read
        pass
    out = []
    for sql_id, text, runs, mean in rows:
        text = str(text.read() if hasattr(text, "read") else text or "")
        sample = text
        got = binds.get(sql_id)
        if got:
            # a bind's captured value in the place of its mark, as a
            # literal the argument reader takes
            def lit(m, got=got, order=iter(sorted(got))):
                k = next(order, None)
                v = got.get(k) if k is not None else None
                if v is None:
                    return m.group(0)
                return v if re.fullmatch(r"[+-]?\d+(\.\d+)?", str(v)) \
                    else "'" + str(v).replace("'", "''") + "'"
            sample = re.sub(r":\w+", lit, text)
        out.append((sql_id, text, int(runs or 0), sample,
                    float(mean) if mean is not None else None))
    return out


READERS = {"mysql": _mysql, "postgres": _postgres, "mssql": _mssql,
           "oracle": _oracle}


def read(pair, db, routines):
    """The source's record of its calls to `routines`, read-only; a
    History with `why` set where the source keeps none here."""
    eng = pair.src_engine
    reader = READERS.get(eng.CANON_ENGINE)
    if reader is None:
        return History(why=f"{eng.CANON_ENGINE} keeps no record of its"
                           " calls migkit reads")
    names = {r.name.lower() for r in routines
             if r.kind in ("function", "procedure")}
    if not names:
        return History()
    try:
        got = reader(eng, db)
    except Exception as e:  # noqa: BLE001 - said, as the reason
        return History(why="the source's record of its calls could not be"
                           " had: " + (str(e).strip().splitlines()
                                       or [""])[0][:100])
    if isinstance(got, str):
        return History(why=got)
    records = {}
    engine = eng.CANON_ENGINE
    for text, runs, sample in got:
        if MARK in (sample or ""):
            # the proof's own call of a function on the hop's own server,
            # not the application's: its values are the proof's pools
            sample = ""
            if MARK in (text or ""):
                continue
        found = calls_in(sample, names, engine) or \
            calls_in(text, names, engine)
        for name, args in found:
            records.setdefault(name, Record()).add(args, runs)
    return History(records, statements=len(got))


def allowed(hop, routine, compared):
    """k -> whether parameter k of `routine` may take a recorded value:
    never under `mask: all`; not where the body compares it with a masked
    column (`compared`: {parameter: {column}}, `inputs.compared_columns`)."""
    from .. import masking
    rules = masking._rules(hop)
    if rules is None:
        return lambda k: True
    if rules is True:
        return lambda k: False
    names = [p.name for p in routine.params]

    def ok(k):
        if k >= len(names):
            return False
        for col in compared.get(names[k], ()):
            table, _, column = str(col).rpartition(".")
            if masking.column(hop, table, column):
                return False
        return True
    return ok

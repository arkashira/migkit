"""Where two versions of a routine part, not only that they do (leapfrog
research P5, backlog CV.P5).

A call that answers otherwise names its input and what differed at the
end; a routine of forty statements can part at the third and show it at
the last. Here each version reports its state - every variable and
parameter in its outermost scope - after each statement of its
outermost block, and the two reports are aligned: the source's states in
order, each looked for in the target's from where the last one was found
(a target that runs more statements for one of the source's - a TRY made
into blocks, a fetch into variables of its own - repeats a state, never
reorders one). The first of the source's states the target never reaches
is where they part: the statement after which it holds, its line, and
the variables the target holds otherwise at that point. Variables are
matched by name across the rules' renames (`@total`, `v_total`,
`p_total` are one), and each value compared by its declared class.

Each report is made by a copy of the routine made beside it under a name
of its own in the sandbox, reporting in a way that changes nothing the
routine does:

* PostgreSQL: a notice (`RAISE NOTICE`) - not an SQL statement, so
  `FOUND` and `GET DIAGNOSTICS ... ROW_COUNT` are untouched; notices
  reach the caller as they are sent, so a call that fails reports up to
  where it failed;
* MySQL: a row in a table no transaction holds (MyISAM), so a call rolled
  back still reports; not for a routine reading `ROW_COUNT()`,
  `FOUND_ROWS()` or the diagnostics area, which the insert would change;
* SQL Server: a `PRINT`, heard by the driver's message handler; not for a
  routine reading `@@ROWCOUNT` or `@@ERROR`, which a `PRINT` resets;
* Oracle: a procedure of its own in an autonomous transaction writing a
  table, so a call rolled back still reports; not for a routine reading
  `SQL%` attributes, which a call in between may change.

A copy is held to its routine before its report is read: called with the
same input, it must answer as the routine does, else its report is not
the routine's and nothing is said from it. Only functions and
procedures: a trigger fires by itself, and a copy cannot be fired
beside it.
"""
import collections
import json
import re

from . import harness
from .routines import Routine

#: states one call may report, at most
MOST = 5000
#: the trace table's name in a MySQL or Oracle sandbox
TABLE = "migkit_trace"
#: what a report line starts with
MARK = "migkit-trace"

#: what a report would change, per engine: a routine reading it is left
READS_WHAT_A_REPORT_CHANGES = {
    "mysql": re.compile(r"(?i)\b(row_count|found_rows)\s*\(|\bget\s+"
                        r"(current\s+|stacked\s+)?diagnostics\b"),
    "mssql": re.compile(r"(?i)@@(rowcount|error)\b"),
    "oracle": re.compile(r"(?i)\bsql\s*%\s*(rowcount|found|notfound|"
                         r"isopen|bulk_rowcount)\b"),
    "postgres": None,
}


# ---- what is in scope ------------------------------------------------------


def variables(routine, engine):
    """[(name, declared type)] of the parameters and the variables its
    outermost block declares; None where the body cannot be read."""
    from .translate import Refused
    out = [(p.name, p.type) for p in routine.params if p.name]
    body = routine.body or ""
    try:
        if engine == "mysql":
            from . import mysql_pg
            tree = mysql_pg.read(body)
            out += [(n, t) for n, t, _, _ in getattr(tree, "variables", [])]
        elif engine == "postgres":
            from . import pg_mysql
            tree = pg_mysql.read(body)
            out += [(d.name, d.type) for d in tree.decls
                    if d.what == "var" and d.type]
        elif engine == "mssql":
            from . import tsql_pg
            from .reduce import _tsql_spans
            for lo, hi in _tsql_spans(body):
                p = tsql_pg._Parser(body[lo:hi])
                if p.w() != "DECLARE":
                    continue
                node = p.statement()
                out += [(name, typ) for kind, name, typ, _ in node.items
                        if kind == "var"]
        elif engine == "oracle":
            out += _plsql_declarations(body)
        else:
            return None
    except Refused:
        return None
    return out


def _plsql_declarations(body):
    """The variables a PL/SQL body declares between its header's IS or AS
    and its outermost BEGIN; none where it declares a subprogram of its
    own (its scope is not one list)."""
    from sqlglot.dialects.dialect import Dialect
    from sqlglot.tokens import TokenType
    toks = Dialect.get_or_raise("oracle").tokenize(body)
    depth, start, end = 0, None, None
    for k, t in enumerate(toks):
        word = t.text.upper() if t.token_type not in (
            TokenType.STRING, TokenType.IDENTIFIER) else ""
        if t.token_type == TokenType.L_PAREN:
            depth += 1
        elif t.token_type == TokenType.R_PAREN:
            depth -= 1
        elif depth == 0 and start is None and word in ("IS", "AS"):
            start = t.end + 1
        elif depth == 0 and start is not None and word == "BEGIN":
            end = t.start
            break
    if start is None or end is None:
        return []
    out = []
    for part in body[start:end].split(";"):
        words = part.split()
        if not words:
            continue
        head = words[0].lower()
        if head in ("procedure", "function"):
            return []
        if head in ("cursor", "type", "subtype", "pragma") or \
                (len(words) > 1 and words[1].lower() == "exception"):
            continue
        rest = words[1:]
        if rest and rest[0].lower() == "constant":
            rest = rest[1:]
        if not rest:
            continue
        typ = re.split(r"(?i)\s+(not\s+null|:=|default)\b|:=",
                       " ".join(rest))[0].strip()
        out.append((words[0].strip('"'), typ))
    return out


def base(name):
    """A variable's name as both versions share it: without case, the
    T-SQL `@`, or the rules' `v_` and `p_`."""
    low = str(name).lower().lstrip("@").strip('"`[]')
    return re.sub(r"^(v|p)_", "", low)


def matched(src_vars, dst_vars):
    """{source name: target name} of the variables both versions hold, by
    `base`; a base two names of one side share is left out."""
    def by(names):
        got = collections.Counter(base(n) for n, _ in names)
        return {base(n): n for n, _ in names if got[base(n)] == 1}
    a, b = by(src_vars), by(dst_vars)
    return {a[k]: b[k] for k in a if k in b}


# ---- the copies that report ------------------------------------------------


def _report(engine, name, point, names):
    """The statement reporting `names` ([(name, declared type)]) at
    `point`, in `engine`'s code: each value as text, a moment in ISO
    form."""
    from .inputs import klass
    moments = {n for n, typ in names
               if klass(engine, typ) in ("date", "timestamp")}
    if engine == "postgres":
        from .plpgsql import ident
        pairs = ", ".join(f"'{n}', ({ident(n)})::text" for n, _ in names)
        return (f"raise notice '{MARK} {point} %',"
                f" json_build_object({pairs})::text;")
    if engine == "mysql":
        pairs = ", ".join(f"'{n}', cast(`{n}` as char)" for n, _ in names)
        return (f"insert into {TABLE} (routine, point, state) values"
                f" ('{name}', {point}, json_object({pairs}));")
    if engine == "mssql":
        cols = []
        for n, _ in names:
            var = n if n.startswith("@") else "@" + n
            cols.append(f"convert(nvarchar(max), {var}, 126)" if n in moments
                        else f"cast({var} as nvarchar(max))")
        listed = ", ".join(f"{c} as [{n.lstrip('@')}]"
                           for c, (n, _) in zip(cols, names))
        return (f";print N'{MARK} {point} ' + isnull((select {listed} for"
                " json path, without_array_wrapper, include_null_values),"
                " N'{}');")
    if engine == "oracle":
        pairs = ", ".join(
            f"'{n}' value to_char({n}, 'YYYY-MM-DD HH24:MI:SS')"
            if n in moments else f"'{n}' value to_char({n})"
            for n, _ in names)
        return (f"{TABLE}_put('{name}', {point},"
                f" json_object({pairs} null on null));")
    return None


def _after(body, hi):
    """Where a statement ending at `hi` is followed: past its semicolon."""
    j = hi
    while j < len(body) and body[j] in " \t\r\n":
        j += 1
    return j + 1 if j < len(body) and body[j] == ";" else hi


def reporting(routine, engine, name, names):
    """(the body of a copy reporting after each statement of its
    outermost block, [(point, line of the statement it follows)]); None
    where it cannot be cut into statements."""
    from . import reduce
    body = routine.body or ""
    spans = reduce.spans(routine, engine)
    if not spans or not names:
        return None
    out, at, points = [], 0, []
    for k, (lo, hi) in enumerate(spans):
        end = _after(body, hi)
        out.append(body[at:end])
        out.append("\n" + _report(engine, name, k + 1, names) + "\n")
        points.append((k + 1, body[:lo].count("\n") + 1))
        at = end
    out.append(body[at:])
    return "".join(out), points


def _setup(runner):
    """What a MySQL or Oracle sandbox reports into, made once."""
    if runner.engine == "mysql":
        with runner.conn.cursor() as cur:
            cur.execute(f"create table if not exists {TABLE} (seq int"
                        " auto_increment primary key, routine varchar(64),"
                        " point int, state longtext) engine = MyISAM")
        runner.conn.commit()
    elif runner.engine == "oracle":
        cur = runner.conn.cursor()
        cur.execute(f"begin execute immediate 'create table {TABLE} (seq"
                    " number generated always as identity, routine"
                    " varchar2(128), point number, state clob)'; exception"
                    " when others then if sqlcode <> -955 then raise; end"
                    " if; end;")
        cur.execute(f"create or replace procedure {TABLE}_put(r varchar2,"
                    " p number, s clob) is pragma autonomous_transaction;"
                    f" begin insert into {TABLE} (routine, point, state)"
                    " values (r, p, s); commit; end;")
        cur.execute(f"create or replace procedure {TABLE}_clear(r"
                    " varchar2) is pragma autonomous_transaction; begin"
                    f" delete from {TABLE} where routine = r; commit; end;")
        runner.conn.commit()


class _Heard(list):
    """What a SQL Server connection's message handler hears."""

    def __call__(self, state, severity, server, proc, line, text):
        if len(self) < MOST:
            self.append(text.decode("utf-8", "replace")
                        if isinstance(text, bytes) else str(text))


def _listen(runner, name):
    """Readies `runner` to hear the copy `name`; the thing to read after."""
    if runner.engine == "postgres":
        runner.conn.notices = collections.deque(maxlen=MOST)
        return runner.conn.notices
    if runner.engine == "mysql":
        with runner.conn.cursor() as cur:
            cur.execute(f"delete from {TABLE} where routine = %s", (name,))
        return None
    if runner.engine == "mssql":
        inner = getattr(runner.conn, "_conn", None)
        if inner is None or not hasattr(inner, "set_msghandler"):
            raise harness.HarnessError("the driver hears no message")
        heard = _Heard()
        inner.set_msghandler(heard)
        return heard
    if runner.engine == "oracle":
        runner.conn.cursor().execute(f"begin {TABLE}_clear(:1); end;",
                                     [name])
        return None
    raise harness.HarnessError(f"{runner.engine} reports no state")


def _heard(runner, name, listened):
    """[(point, {variable: text or None})] the copy reported, in order."""
    lines = []
    if runner.engine in ("postgres", "mssql"):
        for text in list(listened or ()):
            m = re.search(rf"{MARK} (\d+) (.*)$", str(text).strip(), re.S)
            if m:
                lines.append((int(m.group(1)), m.group(2)))
        if runner.engine == "mssql":
            try:
                runner.conn._conn.set_msghandler(None)
            except Exception:  # noqa: BLE001 - the next call sets its own
                pass
    elif runner.engine == "mysql":
        with runner.conn.cursor() as cur:
            cur.execute(f"select point, state from {TABLE} where routine ="
                        " %s order by seq", (name,))
            lines = [(int(p), s) for p, s in cur.fetchall()]
    elif runner.engine == "oracle":
        cur = runner.conn.cursor()
        cur.execute(f"select point, state from {TABLE} where routine = :1"
                    " order by seq", [name])
        lines = [(int(p), s.read() if hasattr(s, "read") else s)
                 for p, s in cur.fetchall()]
    out = []
    for point, state in lines[:MOST]:
        try:
            got = json.loads(state)
        except (TypeError, ValueError):
            continue
        if isinstance(got, dict):
            out.append((point, {str(k).lstrip("@"): v for k, v in
                                got.items()}))
    return out


# ---- values, compared by their class ----------------------------------------


def _canon(cls, text):
    """A reported value (text) as one both engines' reports agree on."""
    import datetime as dt
    from decimal import Decimal, InvalidOperation
    if text is None:
        return None
    s = str(text).strip()
    try:
        if cls == "integer":
            return int(Decimal(s))
        if cls in ("decimal", "float"):
            d = Decimal(s).normalize()
            return format(d, "f") if cls == "decimal" else \
                repr(round(float(d), 9))
        if cls == "boolean":
            return {"t": 1, "true": 1, "1": 1, "f": 0, "false": 0,
                    "0": 0}.get(s.lower(), s)
        if cls in ("timestamp", "date"):
            got = dt.datetime.fromisoformat(s.replace("T", " ")[:26])
            return got.isoformat(sep=" ") if cls == "timestamp" else \
                got.date().isoformat()
    except (InvalidOperation, ValueError):
        return s
    return s


def aligned(src, dst, names, classes):
    """Where two reports part: None where every state of the source's is
    met in the target's, in order; else {"point": the source's point,
    "target point": the target's nearest, "apart": [(variable, source's
    value, target's)]}. `src`, `dst` are [(point, {name: text})]; `names`
    is {source name: target name}; `classes` {source name: class}."""
    def state(values, side):
        out = {}
        for s, t in names.items():
            key = s if side == "src" else t
            k = next((x for x in values if base(x) == base(key)), None)
            if k is not None:
                out[s] = _canon(classes.get(s), values[k])
        return out
    a = [(p, state(v, "src")) for p, v in src]
    b = [(p, state(v, "dst")) for p, v in dst]
    j = 0
    for p, s in a:
        k = next((k for k in range(j, len(b)) if all(
            b[k][1].get(n) == s[n] for n in s if n in b[k][1])), None)
        if k is not None:
            j = k
            continue
        near = b[min(j + 1, len(b) - 1)] if b else (None, {})
        apart = [(n, s[n], near[1].get(n)) for n in sorted(s)
                 if n in near[1] and near[1][n] != s[n]]
        return {"point": p, "target point": near[0], "apart": apart}
    return None


# ---- the whole of it --------------------------------------------------------


def first_apart(proof, r, t, args, writes=None, commits=False):
    """Where the source's routine `r` and the target's version `t` part on
    `args`, in words; None where it cannot be said (said nowhere: the
    verdict already names the difference at the end)."""
    a, b = proof.a, proof.b
    if r.kind not in ("function", "procedure") or a is None or b is None:
        return None
    if "mssql" in (a.engine, b.engine) and r.kind == "function":
        # a T-SQL function may not PRINT
        return None
    for runner, routine in ((a, r), (b, t)):
        rule = READS_WHAT_A_REPORT_CHANGES.get(runner.engine, False)
        if rule is False or (rule is not None and rule.search(
                routine.body or "")):
            return None
    sv, dv = variables(r, a.engine), variables(t, b.engine)
    if not sv or not dv:
        return None
    names = matched(sv, dv)
    if not names:
        return None
    from . import inputs
    classes = {n: inputs.klass(proof.pair.src_engine.CANON_ENGINE, typ)
               for n, typ in sv}
    got = {}
    made = []
    try:
        types = {"src": dict(sv), "dst": dict(dv)}
        for side, runner, routine, mine in (
                ("src", a, r, list(names)),
                ("dst", b, t, [names[n] for n in names])):
            mine = [(n, types[side].get(n)) for n in mine]
            name = f"migkit_trace_{routine.name}"[:60]
            cut = reporting(routine, runner.engine, name, mine)
            if cut is None:
                return None
            body, points = cut
            from .reduce import _made_by, renamed
            made_def = renamed(routine, name, body, runner.q)
            if made_def is None:
                return None
            copy = Routine(kind=routine.kind, name=name,
                           params=routine.params, returns=routine.returns,
                           returns_set=routine.returns_set, body=body,
                           definition=made_def, settings=routine.settings,
                           language=routine.language)
            _setup(runner)
            runner.drop(copy)
            runner.create(_made_by(copy, runner.engine), routine.settings)
            made.append((runner, copy))
            got[side] = (runner, routine, copy, points)
        tables = list(writes if writes is not None else a.watched)
        mode = harness.clock_mode(a, b, r.body or "", t.body or "",
                                  commits)
        x, y, _ = proof._both(
            a, b, lambda: proof._render(r, a.call(r, list(args), tables,
                                                  commits)),
            lambda: proof._render(r, b.call(t, list(args), tables,
                                            commits)), mode)
        heard = {}

        def traced(side):
            runner, routine, copy, _ = got[side]
            listened = _listen(runner, copy.name)
            out = proof._render(r, runner.call(copy, list(args), tables,
                                               commits))
            heard[side] = _heard(runner, copy.name, listened)
            return out
        xt, yt, _ = proof._both(a, b, lambda: traced("src"),
                                lambda: traced("dst"), mode)
        # the copies answer as the routines do, or their reports are not
        # the routines'
        if harness.differences(x, xt, True) or \
                harness.differences(y, yt, True):
            return None
        where = aligned(heard["src"], heard["dst"], names, classes)
    except Exception:  # noqa: BLE001 - nothing said from a copy that
        # could not be made or called: the verdict stands as it is
        for runner in (a, b):
            try:
                runner.conn.rollback()
            except Exception:  # noqa: BLE001 - already reset
                pass
        return None
    finally:
        for runner, copy in made:
            try:
                runner.drop(copy)
            except Exception:  # noqa: BLE001 - nothing to drop
                runner.conn.rollback()
    src_points = dict(got["src"][3])
    dst_points = dict(got["dst"][3])
    if where is None:
        return ("every state the two versions share after each statement"
                " agrees: they part only in what they finally answer")
    line = src_points.get(where["point"])
    there = dst_points.get(where["target point"])
    shown = "; ".join(f"{n} holds {_said(v)} on the source and {_said(w)}"
                      " on the target" for n, v, w in where["apart"][:3])
    return (f"they first part after the source's statement at line {line}"
            + (f" (the target's at line {there})" if there else "")
            + (f": {shown}" if shown else ": the target never reaches that"
               " state"))


def _said(v):
    return "NULL" if v is None else repr(v)

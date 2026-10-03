"""Built-in functions' meaning at their edges, mined from the engines' own
test suites (leapfrog research P17, backlog CV.P17).

The translator maps a function by its name and arguments; where two
engines part is at the edges - NULL, empty text, zero or a negative
position, a half to round, the end of a month - and each engine's authors
wrote those edges down in their own tests. With the hop option
`builtin_suites` naming where such a suite is on this machine -
PostgreSQL's regression tests (`src/test/regress/sql`), sqllogictest's
`test/`, MySQL's `mysql-test/t` - read as data, never copied into migkit
(MySQL's suite is GPL-2.0 only) - every case in it that calls a built-in
function on constants alone is read (`mine`), for the functions the
source's routines and views call, and run on both sandboxes: the source's
engine answers the case as written, the target the translator's version
of it. Every case the two answer apart is kept beside the reports
(`builtin-semantics.json`) with the suite's file and line it came from,
and becomes the translator's rules three ways:

* a call written with exactly a case's constants is refused with that
  case and where it came from (`refusal`) - it is known to answer
  otherwise;
* the constants of every mined case of a function join the inputs of each
  routine calling it (`constants`), so the proof tries the edges the
  engine's own authors wrote down;
* a routine calling a function with a case that parts carries the case,
  cited, beside its verdict (`caveats`) - the proof may not reach it.

Only the source's own engine decides a case's answer: a case its engine
refuses (a suite of another engine, a test of an error) is left out.
"""
import json
import re
from pathlib import Path

from sqlglot import exp

#: cases run at most, and per function
MOST = 400
PER_FUNCTION = 40
FILE = "builtin-semantics.json"
#: words that are SQL's, not a function's name, before a parenthesis
_NOT_FUNCTIONS = frozenset("""
select from where and or not in exists values into as on join using when
then else case if while return returns declare set call exec execute begin
end over partition by group order having union all any some table create
insert update delete merge cast convert interval char varchar decimal
numeric int integer primary key references check unique default
""".split())


def paths(hop):
    got = (hop.options or {}).get("builtin_suites")
    if not got:
        return []
    got = [got] if isinstance(got, (str, Path)) else list(got)
    return [Path(str(p)).expanduser() for p in got]


def turned_on(hop):
    return bool(paths(hop))


def called(texts):
    """The functions (lower case) the texts call."""
    out = set()
    for text in texts:
        for m in re.finditer(r"(?i)\b([a-z_][a-z0-9_]*)\s*\(", text or ""):
            name = m.group(1).lower()
            if name not in _NOT_FUNCTIONS:
                out.add(name)
    return out


# ---- the suites, read as data ----------------------------------------------


def _regress(text):
    """[(line, statement)] of a PostgreSQL regression test: psql's
    backslash commands blanked, the rest split at its semicolons."""
    from .appscan import in_sql_file
    kept = "\n".join("" if ln.lstrip().startswith("\\") else ln
                     for ln in text.splitlines())
    return in_sql_file(kept)


def _logic(text, dialect):
    """[(line, statement)] of an sqllogictest file's queries, those meant
    for another engine (`onlyif`) left out."""
    out, lines = [], text.splitlines()
    i, only = 0, None
    while i < len(lines):
        ln = lines[i].strip()
        if ln.startswith("onlyif "):
            only = ln.split()[1].lower()
        elif ln.startswith("query ") or ln.startswith("statement "):
            start, body = i + 1, []
            i += 1
            while i < len(lines) and lines[i].strip() not in ("", "----"):
                body.append(lines[i])
                i += 1
            if ln.startswith("query ") and (only in (None, dialect)):
                out.append((start + 1, "\n".join(body)))
            only = None
            continue
        elif not ln:
            only = None
        i += 1
    return out


def _mysql_test(text):
    """[(line, statement)] of a MySQL test file: its own commands (`--`
    lines, `#` comments) left out, the statements split at the delimiter
    in force."""
    out, buf, first, delim = [], [], None, ";"
    for k, raw in enumerate(text.splitlines(), 1):
        ln = raw.strip()
        if not buf and (ln.startswith("--") or ln.startswith("#") or
                        not ln):
            continue
        m = re.match(r"(?i)^delimiter\s+(\S+)", ln)
        if m and not buf:
            # `delimiter //;` ends with the delimiter in force, and
            # `delimiter ;//` with the one it replaces
            got = m.group(1)
            delim = got[:-len(delim)] if got.endswith(delim) and \
                len(got) > len(delim) else got
            continue
        if first is None:
            first = k
        buf.append(raw)
        if ln.endswith(delim):
            stmt = "\n".join(buf).rstrip()[:-len(delim)].strip()
            if stmt:
                out.append((first, stmt))
            buf, first = [], None
    return out


def statements_of(path, dialect):
    """[(line, statement, the suite's dialect)] of one suite file."""
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return []
    if path.suffix == ".sql":
        return [(ln, s, "postgres") for ln, s in _regress(text)]
    if path.suffix == ".test":
        if re.search(r"(?m)^(query|statement) ", text):
            return [(ln, s, dialect) for ln, s in _logic(text, dialect)]
        return [(ln, s, "mysql") for ln, s in _mysql_test(text)]
    return []


def _written_name(f, dialect):
    """A call's function name as the translator writes it back."""
    if isinstance(f, exp.Anonymous):
        return str(f.name).lower()
    try:
        return f.sql(dialect=dialect).split("(")[0].strip().lower()
    except Exception:  # noqa: BLE001 - by its node's name instead
        return f.sql_name().lower()


def _function_names(node, dialect):
    return {_written_name(f, dialect) for f in node.find_all(exp.Func)}


def _constant(node):
    """Whether an expression holds constants and function calls only."""
    return not any(isinstance(n, (exp.Column, exp.Star, exp.Subquery,
                                  exp.Placeholder, exp.Parameter,
                                  exp.Select, exp.Window))
                   for n in node.walk())


def _canonical(name, dialect):
    """A function's name as the translator knows it (`substr` and
    `substring` are one), or as written where it does not know it."""
    from .translate import Refused, parse
    for args in ("'a'", "'a', 1", "'a', 1, 'b'"):
        try:
            node = parse(f"SELECT {name}({args})", dialect).expressions[0]
        except (Refused, IndexError, AttributeError):
            continue
        if isinstance(node, exp.Alias):
            node = node.this
        if isinstance(node, exp.Func):
            return _written_name(node, dialect)
    return name.lower()


def cases_in(statement, dialect, wanted):
    """[(the case's expression in `dialect`, {the functions of `wanted`
    it calls, as `wanted` writes them})] of one suite statement: each
    item of a `SELECT` without FROM that calls one on constants alone.
    `wanted` is {name as the source's code writes it}, or {canonical
    name: that name} already canonical."""
    from .translate import Refused, parse
    if not isinstance(wanted, dict):
        wanted = {_canonical(w, dialect): w for w in wanted}
    try:
        tree = parse(statement, dialect)
    except Refused:
        return []
    if not isinstance(tree, exp.Select) or tree.args.get("from_") or \
            tree.args.get("from"):
        return []
    written = _items(statement)
    out = []
    for k, item in enumerate(tree.expressions):
        node = item.this if isinstance(item, exp.Alias) else item
        if not _constant(node):
            continue
        names = {wanted[n] for n in _function_names(node, dialect)
                 if n in wanted}
        if not names:
            continue
        # the case as the suite writes it: the translator's own reading
        # may write another function for it (IFNULL as COALESCE)
        if len(written) == len(tree.expressions):
            text = re.sub(r'(?is)\s+as\s+("[^"]*"|`[^`]*`|\w+)\s*$', "",
                          written[k]).strip()
        else:
            try:
                text = node.sql(dialect=dialect)
            except Exception:  # noqa: BLE001 - not written back: left
                continue
        out.append((text, names))
    return out


def _items(statement):
    """The items of a `SELECT` list as written, split at the commas
    outside quotes and parentheses; [] where it does not begin with
    SELECT."""
    m = re.match(r"(?is)^\s*select\s+(.*)$", statement)
    if not m:
        return []
    text, out, depth, quote, buf = m.group(1), [], 0, None, []
    for ch in text:
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in "'\"`":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            out.append("".join(buf).strip())
            buf = []
            continue
        buf.append(ch)
    if "".join(buf).strip():
        out.append("".join(buf).strip())
    return out


def mine(hop, dialect, wanted):
    """{function: [(case, "file:line")]} from the hop's suites, for the
    functions in `wanted`, cases of the source's `dialect` only, at most
    MOST and PER_FUNCTION."""
    out, total = {}, 0
    wanted = {_canonical(w, dialect): w for w in wanted}
    for root in paths(hop):
        files = [root] if root.is_file() else sorted(
            p for p in root.rglob("*") if p.suffix in (".sql", ".test"))
        for f in files:
            for line, stmt, suite in statements_of(f, dialect):
                if suite != dialect:
                    continue
                for case, names in cases_in(stmt, suite, wanted):
                    where = f"{f.name}:{line}"
                    for n in names:
                        got = out.setdefault(n, [])
                        if len(got) < PER_FUNCTION and \
                                case not in [c for c, _ in got]:
                            got.append((case, where))
                            total += 1
                    if total >= MOST:
                        return out
    return out


# ---- run on both sides -----------------------------------------------------


def run(proof, mined):
    """{function: {"cases": n, "apart": [...]}} of the mined cases run on
    both sandboxes (`proof.a`, `proof.b`): each case as the source's
    engine answers it and as the target answers its translation."""
    src = proof.pair.src_engine.SQL_DIALECT
    dst = proof.pair.dst_engine.SQL_DIALECT
    a, b = proof.a, proof.b
    out, done = {}, {}
    for fn, cases in sorted(mined.items()):
        one = out.setdefault(fn, {"cases": 0, "apart": [],
                                  "not translated": 0})
        for case, where in cases:
            if case not in done:
                done[case] = _one(a, b, case, where, src, dst)
            got = done[case]
            if got == "refused":
                one["not translated"] += 1
            elif got is not None:
                one["cases"] += 1
                if got != "alike":
                    one["apart"].append(got)
    return out


def _runnable(sql, engine):
    """A case's SELECT as the runner takes it: Oracle's with its one-row
    table, the others with a `%` doubled for the driver."""
    if engine == "oracle":
        return sql if re.search(r"(?i)\bfrom\b", sql) else \
            f"{sql} from dual"
    return sql.replace("%", "%%")


def _one(a, b, case, where, src, dst):
    """"alike", what is kept of a case answered apart, "refused" where
    the translator refuses it, or None where the source's own engine
    refuses the case (not its case) or it could not be run."""
    from . import harness
    from .statements import apart
    from .translate import Refused, translate
    try:
        there = translate(f"select {case}", src, dst)
    except Refused:
        return "refused"
    try:
        x = a.run(_runnable(f"select {case}", a.engine), [], [], rows=True)
        if x.error is not None:
            return None
        y = b.run(_runnable(there, b.engine), [], [], rows=True)
    except harness.HarnessError:
        return None
    found = apart(x, y)
    if not found:
        return "alike"
    return {"case": case, "target": there, "from": where,
            "source answers": _first(x),
            "target answers": _first(y) if y.error is None
            else f"error {y.error[0]}", "found": found[0]}


def _first(o):
    from .harness import render_any
    rows = o.sets[0] if o.sets else []
    return render_any(rows[0][0]) if rows and rows[0] else None


def keep(hop, db, pair_name, got):
    path = hop.report_dir(db) / FILE
    try:
        path.write_text(json.dumps({"pair": pair_name, "functions": got},
                                   indent=1, default=str))
    except OSError:
        pass


def load(hop, db):
    """{function: [apart cases]} kept beside the reports, or {}."""
    try:
        got = json.loads((hop.report_dir(db) / FILE).read_text())
    except (OSError, ValueError):
        return {}
    return {fn: v.get("apart", []) for fn, v in
            (got.get("functions") or {}).items() if v.get("apart")}


def result(db, got):
    from ..engines.base import Result
    if not got:
        return None
    cases = sum(v["cases"] for v in got.values())
    parts = [(fn, c) for fn, v in sorted(got.items()) for c in v["apart"]]
    scope = f"{db} built-in functions"
    head = (f"{cases} cases of {len(got)} functions from the engines' own"
            " tests run on both sides")
    if not parts:
        return Result("schema", scope, "ok" if cases else "warn",
                      head + (": every one answered alike" if cases else
                              ": none could be run"))
    shown = "; ".join(f"{fn}: {c['case']} answers {c['source answers']!r}"
                      f" on the source and {c['target answers']!r} on the"
                      f" target ({c['from']})" for fn, c in parts[:5])
    return Result("schema", scope, "warn",
                  f"{head}; {len(parts)} answer apart: {shown}", "",
                  "a routine calling these functions at those edges answers"
                  " otherwise on the target; the proof tries each edge on"
                  " every routine that calls it")


# ---- the translator's rules, from the cases kept ---------------------------


def _normal(text, dialect):
    from .translate import Refused, parse
    try:
        return parse(f"SELECT {text}", dialect).expressions[0].sql(
            dialect=dialect).lower()
    except (Refused, IndexError, AttributeError):
        return " ".join(str(text).split()).lower()


def refusal(tree, parting, dialect):
    """Refused where the tree calls a function with exactly the constants
    of a case kept as answering apart (`parting`: {function: [case]})."""
    from .translate import Refused
    if not parting:
        return
    known = {}
    for fn, cases in parting.items():
        for c in cases:
            known[_normal(c["case"], dialect)] = (fn, c)
    for f in tree.find_all(exp.Func):
        if not _constant(f):
            continue
        try:
            text = f.sql(dialect=dialect).lower()
        except Exception:  # noqa: BLE001 - not a case it could be
            continue
        hit = known.get(text)
        if hit:
            fn, c = hit
            raise Refused(f"it calls {c['case']}, which the target answers"
                          f" otherwise ({c['source answers']!r} against"
                          f" {c['target answers']!r}; the engine's own test"
                          f" {c['from']})")


def constants(mined, text, dialect):
    """(numbers, strings) of the mined cases of every function `text`
    calls - more inputs for the proof."""
    from . import inputs
    nums, strs = set(), set()
    for fn in called([text]):
        for case, _ in mined.get(fn, []):
            n, s = inputs.constants(case, dialect)
            nums |= n
            strs |= s
    return nums, strs


def caveats(parting, text):
    """What is said of a routine calling a function with a case kept as
    answering apart."""
    out = []
    for fn in sorted(called([text]) & set(parting)):
        c = parting[fn][0]
        out.append(f"calls {fn}, which the target answers otherwise at"
                   f" {c['case']} (the engine's own test {c['from']})")
    return out

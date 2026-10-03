"""The application's own statements proved on the sandbox, as its stored
code is (leapfrog research P10, P11 and P21; backlog CV.P10, CV.P11,
CV.P21, W3).

An application reaches its database through its routines and through
the statements it sends itself. The routines are converted and proved
one by one (`prove`, `convert`); the statements come from two places -
the source's own record of what it ran, the most run first (`corpus`),
and the application's code, read for the SQL written in it (`appscan`) -
and each is carried into the target's SQL by the one translator migkit
has (`translate`), with the hop's restructuring applied first: its table
renames (`convert.localizer`) and its column renames, keeps and drops
(`restructured`). Each is then proved the same way as a routine: run on
both sandboxes, over the same fixture and with the target's converted
routines and triggers in place, and compared by what the application
sees - the rows it answers (a multiset of values, `workload.alike`), the
error it meets (by class), the rows it changes in every table and how far
it moves a counter (`harness.differences`).

A placeholder the application's driver fills (`?`, `%s`, `%(p)s`,
`:name`, `:1`, `$1`, `@p` from a T-SQL application, `#{p}`) is filled on
both sides with the same values: the ones the source's record kept where
the hop's masking allows, else the fixture's own values of the column it
is compared with or written to - rows that are there, a value that is
not - and NULL. A value from the fixture is each side's own, read by the
row's key, so a type each engine holds its own way (a MySQL flag, a
PostgreSQL boolean) is its own side's. A statement put together by the
application as it runs (an interpolated string, `${...}`, pieces joined
around a variable) cannot be run as written, and is said as such.

A statement is never `ok` without a run that did something: `ok` where
every run answered alike and at least one answered rows or changed some;
`warn` where every run answered alike but none did either (nothing in the
fixture meets it) or both refused every run alike; `diff` with the first
values that tell the two apart; `not proved` where it could not be run (a
table the fixture lacks, a placeholder with no column to take values
from); `not converted` where the translator refuses it, with why. The
database's line weighs them by how often the application runs each.

Restructuring (P21) is proved here and nowhere else: a statement reading
a renamed column reads it by its new name on the target, one reading a
column the hop leaves out is refused by name, and both sides' answers are
held to each other over the restructured fixture. What the hop cannot
say - a table split in two, two merged, a column computed from others -
is not restructuring migkit carries, and nothing here pretends to prove
it.
"""
import re
import time
from dataclasses import dataclass, field

from sqlglot import exp

from . import harness
from .translate import Refused, generate, parse

#: statements proved per database, at most, the most run first
MOST = 400
#: fixture rows whose values fill a statement's placeholders, per statement
ROWS = 3
#: seconds every statement of one database may take together; what is
#: left when they run out is said as not proved
BUDGET_SECONDS = 180

ORDER = {"ok": 0, "warn": 1, "not converted": 2, "not proved": 3,
         "diff": 4}
#: the words an application's statement starts with
VERBS = ("select", "with", "insert", "update", "delete", "merge",
         "replace", "upsert", "values", "call", "exec", "execute")

#: the column a placeholder turns into while its statement is translated:
#: a name no application writes, found again in the target's text
_MARK = "__mkp{}__"
_MARKED = re.compile(r"__mkp(\d+)__")


@dataclass
class Statement:
    """One statement the application sends."""
    origin: str                     #: where it was found, as said
    text: str                       #: as the application writes it
    runs: int = 1                   #: how often it ran, where on record
    #: value tuples it ran with, one value per placeholder (distinct by
    #: name), where the hop's masking allows them to be used
    values: list = field(default_factory=list)
    #: why it cannot be run as written (put together as it runs)
    dynamic: str = None
    #: what may be shown of it: no literal a record kept
    label: str = ""
    kind: str = ""                  #: read or write
    #: whether the translated text may be written beside the reports:
    #: an application's own code may, a record's sample may hold values
    shown: bool = False


@dataclass
class Verdict:
    origin: str
    label: str
    status: str
    reason: str = ""
    runs: int = 1
    calls: int = 0
    target: str = ""

    def line(self):
        out = f"{self.origin}: {self.status}"
        if self.reason:
            out += f" - {self.reason}"
        return out

    def record(self):
        return {"origin": self.origin, "statement": self.label,
                "status": self.status, "reason": self.reason,
                "runs": self.runs, "calls": self.calls,
                **({"target": self.target} if self.target else {})}


def starts_like_sql(text):
    """Whether `text` begins as one of the application's statements."""
    first = re.match(r"\s*\(?\s*([A-Za-z]+)", text or "")
    return bool(first) and first.group(1).lower() in VERBS


def squash(text, most=100):
    text = " ".join(str(text or "").split())
    return text if len(text) <= most else text[:most - 3] + "..."


# ---- placeholders ------------------------------------------------------------


class Holes:
    """A statement's placeholders, found by the characters around them:
    never inside a quoted string or name, a comment, a PostgreSQL
    dollar-quoted body, a cast (`::`) or a slice (`[1:2]`).

    `pieces` is the text between them (a `%%` a driver that formats with
    `%` reads as one made one), `tokens` each as written, `slots` the parameter
    each fills - a name used twice is one parameter - and `names` the
    parameters in the order they first appear."""

    def __init__(self, text, dialect):
        self.pieces, self.tokens, self.slots, self.names = [""], [], [], []
        self.substituted = False
        self._read(str(text or ""), dialect)

    def _hole(self, token, name):
        if name is not None and name in self.names:
            slot = self.names.index(name)
        else:
            self.names.append(name)
            slot = len(self.names) - 1
        self.tokens.append(token)
        self.slots.append(slot)
        self.pieces.append("")

    def _add(self, text):
        self.pieces[-1] += text

    def _read(self, text, dialect):
        i, n = 0, len(text)
        while i < n:
            ch = text[i]
            nxt = text[i + 1] if i + 1 < n else ""
            prev = text[i - 1] if i else ""
            quote = {"'": "'", '"': '"', "`": "`"}.get(ch)
            if ch == "[" and dialect == "tsql":
                quote = "]"
            if quote:
                j = i + 1
                while j < n:
                    if text[j] == "\\" and dialect == "mysql" and \
                            quote != "`":
                        j += 2
                        continue
                    if text[j] == quote:
                        if quote != "]" and j + 1 < n and \
                                text[j + 1] == quote:
                            j += 2
                            continue
                        break
                    j += 1
                self._add(text[i:j + 1])
                i = j + 1
                continue
            if ch == "-" and nxt == "-":
                j = text.find("\n", i)
                j = n if j < 0 else j
                self._add(text[i:j])
                i = j
                continue
            if ch == "/" and nxt == "*":
                j = text.find("*/", i + 2)
                j = n if j < 0 else j + 2
                self._add(text[i:j])
                i = j
                continue
            if ch == "$" and dialect == "postgres":
                m = re.match(r"\$([A-Za-z_]\w*)?\$", text[i:])
                if m:
                    j = text.find(m.group(0), i + len(m.group(0)))
                    j = n if j < 0 else j + len(m.group(0))
                    self._add(text[i:j])
                    i = j
                    continue
            got = self._placeholder(text, i, ch, nxt, prev, dialect)
            if got:
                token, name = got
                self._hole(token, name)
                i += len(token)
                continue
            if ch == "%" and nxt == "%":
                self._add("%%")
                i += 2
                continue
            self._add(ch)
            i += 1
        if any(t.startswith("%") for t in self.tokens):
            # a driver that formats the text with `%` reads `%%` as one,
            # in a quoted string too
            self.pieces = [p.replace("%%", "%") for p in self.pieces]

    def _placeholder(self, text, i, ch, nxt, prev, dialect):
        """(the placeholder as written, its name or None) at `i`."""
        rest = text[i:]
        if ch == "?" and nxt not in ("?", "|", "&") and prev != "?":
            m = re.match(r"\?(\d+)", rest)
            # JPA's numbered `?1` is one parameter wherever it is written
            return (m.group(0), m.group(1)) if m else ("?", None)
        if ch == "%" and nxt == "s":
            return "%s", None
        if ch == "%" and nxt == "(":
            m = re.match(r"%\((\w+)\)s", rest)
            return (m.group(0), m.group(1)) if m else None
        if ch == ":" and prev != ":" and nxt != ":" and nxt != "=" and \
                not (prev.isalnum() or prev in "_]"):
            m = re.match(r":([A-Za-z_]\w*|\d+)", rest)
            return (m.group(0), m.group(1)) if m else None
        if ch == "$" and nxt.isdigit() and not (prev.isalnum() or
                                               prev == "_"):
            m = re.match(r"\$(\d+)", rest)
            return m.group(0), m.group(1)
        if ch == "@" and dialect == "tsql" and nxt != "@" and prev != "@":
            m = re.match(r"@([A-Za-z_]\w*)", rest)
            return (m.group(0), m.group(1).lower()) if m else None
        if ch == "#" and nxt == "{":
            m = re.match(r"#\{\s*([\w.]+)[^}]*\}", rest)
            return (m.group(0), m.group(1)) if m else None
        if ch == "$" and nxt == "{":
            # a piece of text the application puts in as it runs: noted,
            # so the statement is said as built at run time
            self.substituted = True
        return None

    @property
    def normalized(self):
        """The statement with each placeholder a named one the translator
        reads in every dialect (`:mkpN`, N the placeholder's place)."""
        out = [self.pieces[0]]
        for k, piece in enumerate(self.pieces[1:]):
            out.append(f":mkp{k}")
            out.append(piece)
        return "".join(out)

    def executable(self, engine):
        """The statement as written, each placeholder the runner's own
        (`%s`), a `%` of the text doubled where the driver formats it."""
        pieces = self.pieces if engine == "oracle" else \
            [p.replace("%", "%%") for p in self.pieces]
        return "%s".join(pieces)

    def params(self, values):
        """A value per placeholder, in the text's order, from one per
        parameter."""
        return [values[k] for k in self.slots]


def tree_of(text, dialect):
    """The statement's tree, its placeholders read as the translator
    reads them in every dialect (`Holes`); None where it cannot be read
    as one statement that reads or writes rows."""
    try:
        tree = parse(Holes(text, dialect).normalized, dialect)
    except Refused:
        return None
    # the translator reads "Update failed" as an UPDATE of a table named
    # failed: a write says what it writes
    if isinstance(tree, exp.Update) and not tree.expressions:
        return None
    if isinstance(tree, exp.Insert) and tree.args.get("expression") is None:
        return None
    if isinstance(tree, exp.Select) and not tree.expressions:
        return None
    return tree if isinstance(tree, (exp.Query, exp.Insert, exp.Update,
                                     exp.Delete, exp.Merge)) else None


def marked(tree):
    """The tree with each placeholder a column named for its place, so the
    target's text says where each went (a T-SQL `TOP (?)` is a LIMIT at
    the end there)."""
    for p in list(tree.find_all(exp.Placeholder)):
        name = str(p.this or "")
        m = re.fullmatch(r"mkp(\d+)", name)
        if m:
            p.replace(exp.column(_MARK.format(m.group(1))))
    return tree


def unmarked(text, holes, engine=None):
    """(the target's text, [the hole each of its placeholders is]) - each
    marker made the runner's own placeholder where `engine` is named,
    else the placeholder as the application wrote it."""
    order = [int(m.group(1)) for m in _MARKED.finditer(text)]
    if engine is None:
        return _MARKED.sub(lambda m: holes.tokens[int(m.group(1))],
                           text), order
    if engine != "oracle":
        text = text.replace("%", "%%")
    return _MARKED.sub("%s", text), order


# ---- restructuring and translation ----------------------------------------


def _leaf(name):
    return str(name or "").split(".")[-1].strip('`"[]').lower()


def restructured(tree, hop, db, columns_of):
    """The source statement's tree with the hop's column rules applied
    (`mapping.columns`: rename, keep, drop), by the source's names - or
    Refused naming a column the hop leaves out of the target.
    `columns_of(table)` is the source table's columns, to place a column
    written without its table."""
    if not ((hop.mapping or {}).get("columns")):
        return tree
    tables = {}
    for t in tree.find_all(exp.Table):
        tables[str(t.alias_or_name).lower()] = t.name
        tables.setdefault(t.name.lower(), t.name)
    named = list(dict.fromkeys(tables.values()))

    def rules(table):
        return hop.column_rules(db, table) or {}

    def owner(col):
        if col.table:
            return tables.get(str(col.table).lower())
        found = [t for t in named if col.name.lower() in
                 {str(c).lower() for c in columns_of(t)}]
        return found[0] if len(found) == 1 else None

    def carried(table, name):
        got = rules(table)
        if not got:
            return name
        low = name.lower()
        keep = {c.lower() for c in got["keep"]}
        drop = {c.lower() for c in got["drop"]}
        if (keep and low not in keep) or low in drop:
            raise Refused(f"it reads {table}.{name}, which the hop leaves"
                          " out of the target")
        return next((b for a, b in got["rename"].items()
                     if a.lower() == low), name)

    for col in list(tree.find_all(exp.Column)):
        if isinstance(col.this, exp.Star):
            table = tables.get(str(col.table).lower()) if col.table \
                else (named[0] if len(named) == 1 else None)
            got = rules(table) if table else {}
            if got and (got["keep"] or got["drop"] or got["rename"]):
                raise Refused(f"it reads every column of {table}, whose"
                              " columns the hop renames or leaves out")
            continue
        table = owner(col)
        if table is None:
            continue
        new = carried(table, col.name)
        if new != col.name:
            col.set("this", exp.to_identifier(new))
    for star in tree.find_all(exp.Star):
        if isinstance(star.parent, exp.Column):
            continue
        for t in named:
            got = rules(t)
            if got and (got["keep"] or got["drop"] or got["rename"]):
                raise Refused(f"it reads every column of {t}, whose"
                              " columns the hop renames or leaves out")
    for schema in tree.find_all(exp.Schema):
        table = schema.this.name if isinstance(schema.this, exp.Table) \
            else None
        if not table:
            continue
        for ident in list(schema.expressions):
            if isinstance(ident, (exp.Identifier, exp.Column)):
                new = carried(table, ident.name)
                if new != ident.name:
                    ident.replace(exp.to_identifier(new) if isinstance(
                        ident, exp.Identifier) else exp.column(new))
    return tree


def localized(pair, db, columns_of=None):
    """tree -> tree in the target's names: the hop's column rules
    (`restructured`), then its table renames and qualifiers
    (`convert.localizer`) - the one rewrite every statement carried to
    the target takes (`workload`, this module)."""
    from .convert import localizer
    local = localizer(pair, db)
    hop = pair.hop

    def cols(table):
        if columns_of is not None:
            return columns_of(table)
        try:
            return [c for c, _ in pair.src_engine.neutral_columns(
                "src", db, table)]
        except Exception:  # noqa: BLE001 - placed by its table only
            return []
    def rewrite(tree):
        return local(restructured(tree, hop, db, cols))
    return rewrite


def translated(stmt, holes, pair, local):
    """(the target's text with markers, the source's tree) of one
    statement, or Refused."""
    src = pair.src_engine.SQL_DIALECT
    dst = pair.dst_engine.SQL_DIALECT
    tree = parse(holes.normalized, src)
    if not isinstance(tree, (exp.Query, exp.Insert, exp.Update,
                             exp.Delete, exp.Merge)):
        raise Refused("it is not a query or a write of rows")
    source = tree.copy()
    parting = getattr(local, "parting", None)
    if parting:
        from .builtins import refusal
        refusal(tree, parting, src)
    tree = local(tree)
    return generate(marked(tree), src, dst), source


# ---- the values placeholders are filled with ------------------------------


def _partner(node):
    """(table or alias, column) a placeholder is compared with or written
    to, "count" for a LIMIT or OFFSET, or None."""
    while isinstance(node.parent, (exp.Paren, exp.Cast, exp.TryCast)):
        node = node.parent
    parent = node.parent
    compare = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE,
               exp.Like, exp.ILike, exp.NullSafeEQ)
    if isinstance(parent, compare):
        other = parent.expression if parent.this is node else parent.this
        return _column_of(other)
    if isinstance(parent, (exp.In, exp.Between)) and node is not \
            parent.this:
        return _column_of(parent.this)
    if isinstance(parent, (exp.Limit, exp.Offset, exp.Fetch)):
        return "count"
    if isinstance(parent, exp.Tuple) and isinstance(parent.parent,
                                                    exp.Values):
        ins = parent.parent.parent
        schema = ins.this if isinstance(ins, exp.Insert) else None
        if isinstance(schema, exp.Schema) and \
                isinstance(schema.this, exp.Table):
            j = parent.expressions.index(node)
            cols = schema.expressions
            if j < len(cols):
                return schema.this.name, cols[j].name
    return None


def _column_of(node):
    while isinstance(node, (exp.Paren, exp.Cast, exp.TryCast)):
        node = node.this
    if isinstance(node, exp.Column) and not isinstance(node.this, exp.Star):
        return str(node.table or ""), node.name
    if isinstance(node, exp.Func):
        cols = [c for c in node.find_all(exp.Column)]
        if len(cols) == 1:
            return str(cols[0].table or ""), cols[0].name
    return None


def partners(tree):
    """{parameter place (N of `:mkpN`): (table, column) | "count"} over
    the source's tree, tables named as the tree names them (aliases
    resolved)."""
    aliases = {}
    for t in tree.find_all(exp.Table):
        aliases[str(t.alias_or_name).lower()] = t.name
    only = list(dict.fromkeys(aliases.values()))
    out = {}
    for p in tree.find_all(exp.Placeholder):
        m = re.fullmatch(r"mkp(\d+)", str(p.this or ""))
        if not m:
            continue
        got = _partner(p)
        if got is None or got == "count":
            out[int(m.group(1))] = got
            continue
        table, column = got
        name = aliases.get(table.lower()) if table else \
            (only[0] if len(only) == 1 else None)
        # a table the statement does not name, or none where it reads
        # several: the fixture's one table holding that column
        out[int(m.group(1))] = (name or "", column)
    return out


def _watched_table(runner, table, column):
    """The fixture table `table` (by any qualifier) holding `column`, or
    - with no table - the one table that holds it; None where none does."""
    hits = []
    for t, w in runner.watched.items():
        names = {c.lower() for c, _ in w.columns}
        if column.lower() not in names:
            continue
        if table and _leaf(t) != _leaf(table):
            continue
        hits.append(t)
    return hits[0] if len(hits) == 1 else None


def _aligned(a, b, t, most):
    """[(the source's raw row, the target's raw row)] of fixture table `t`,
    the same rows by their key, at most `most`, in the key's order."""
    wa, wb = a.watched[t], b.watched[t]
    ka, kb = a.key_at(wa), b.key_at(wb)
    if not ka or len(ka) != len(kb):
        return []
    there = {tuple(b.base[t][i][j] for j in kb): b.raw[t][i]
             for i in range(len(b.raw.get(t) or []))}
    out = []
    for i in sorted(range(len(a.raw.get(t) or [])),
                    key=lambda i: repr(tuple(a.base[t][i][j] for j in ka))):
        key = tuple(a.base[t][i][j] for j in ka)
        if key in there:
            out.append((a.raw[t][i], there[key]))
        if len(out) >= most:
            break
    return out


def _for(cls, value):
    """A value for a side whose column has class `cls`."""
    if value is None:
        return None
    if cls == "boolean" and isinstance(value, (int, str)) and \
            not isinstance(value, bool):
        return str(value).strip().lower() in ("1", "t", "true", "y", "yes")
    return value


def fillings(proof, stmt, holes, where):
    """[(source values, target values, what may be said of them)] a
    statement is run with - one value per parameter - or a reason (str)
    none can be made: the recorded tuples first, then fixture rows, a
    value no row holds, and NULL. `where` is `partners`'."""
    from . import inputs
    a, b = proof.a, proof.b
    n = len(holes.names)
    if n == 0:
        return [((), (), "")]
    # each parameter's first place, and what it is compared with there
    first = {}
    for place, slot in enumerate(holes.slots):
        first.setdefault(slot, place)
    spots = {}
    for slot, place in first.items():
        got = where.get(place)
        if got == "count":
            spots[slot] = "count"
            continue
        if not got:
            spots[slot] = None
            continue
        table, column = got
        t = _watched_table(a, table, column)
        spots[slot] = (t, column) if t is not None else None
    out = []
    for vals in stmt.values:
        if len(vals) != n:
            continue
        pa, pb = [], []
        for slot, v in enumerate(vals):
            pa.append(v)
            pb.append(_for(_cls(a, b, spots.get(slot)), v))
        out.append((tuple(pa), tuple(pb), "the values the source's record"
                                          " kept"))
    lacking = [holes.tokens[first[s]] for s in range(n)
               if spots.get(s) is None]
    if lacking and not out:
        return (f"its placeholder {lacking[0]} is compared with no column"
                " the fixture holds, so there is nothing to fill it with")
    tables = {s[0] for s in spots.values() if isinstance(s, tuple)}
    rows = {t: _aligned(a, b, t, ROWS) for t in tables}
    for i in range(ROWS):
        pa, pb, ok = [], [], True
        for slot in range(n):
            spot = spots.get(slot)
            if spot == "count":
                pa.append(3)
                pb.append(3)
                continue
            if spot is None:
                ok = False
                break
            t, column = spot
            got = rows.get(t) or []
            if i >= len(got):
                ok = False
                break
            ra, rb = got[i]
            j = [c.lower() for c, _ in a.watched[t].columns].index(
                column.lower())
            pa.append(ra[j])
            pb.append(rb[j])
        if ok:
            out.append((tuple(pa), tuple(pb), f"the fixture's row {i + 1}"))
    miss_a, miss_b = [], []
    for slot in range(n):
        spot = spots.get(slot)
        if spot == "count":
            miss_a.append(0)
            miss_b.append(0)
            continue
        if spot is None:
            miss_a.append(None)
            miss_b.append(None)
            continue
        t, column = spot
        j = [c.lower() for c, _ in a.watched[t].columns].index(
            column.lower())
        cls = a.watched[t].columns[j][1]
        seen = [r[j] for r in a.raw.get(t) or []]
        v = inputs.missing_value(cls, seen)
        miss_a.append(v)
        miss_b.append(_for(_cls(a, b, spot), v))
    out.append((tuple(miss_a), tuple(miss_b), "values no fixture row"
                                               " holds"))
    out.append(((None,) * n, (None,) * n, "NULL"))
    seen, kept = set(), []
    for one in out:
        key = repr(one[:2])
        if key not in seen:
            seen.add(key)
            kept.append(one)
    return kept


def _cls(a, b, spot):
    """The class the target's fixture gives the column at `spot` (the
    source's table and column): the columns of the two sides are aligned,
    and the target's may be renamed."""
    if not isinstance(spot, tuple):
        return None
    t, column = spot
    wa, wb = a.watched.get(t), b.watched.get(t)
    if wa is None or wb is None:
        return None
    names = [c.lower() for c, _ in wa.columns]
    if column.lower() not in names:
        return None
    j = names.index(column.lower())
    return wb.columns[j][1] if j < len(wb.columns) else None


# ---- the proof --------------------------------------------------------------


def apart(x, y, clock=False):
    """[what differs] between two runs of one statement, source first."""
    from .. import workload
    plain = [harness.Outcome(error=o.error, changes=o.changes,
                             counters=o.counters, now=o.now) for o in (x, y)]
    found = harness.differences(plain[0], plain[1], clock)
    if x.error is None and y.error is None:
        if len(x.sets) != len(y.sets):
            found.append("one side answered rows and the other none")
        for ra, rb in zip(x.sets, y.sets):
            same = workload.alike(ra, rb)
            if same is not True:
                found.append(f"its answer: {same}")
    return found


def _did(o):
    return bool(o.changes) or any(rows for rows in o.sets)


def _tables_missing(tree, runner):
    have = {_leaf(t) for t in runner.watched}
    ctes = {str(c.alias_or_name).lower() for c in tree.find_all(exp.CTE)}
    out = []
    for t in tree.find_all(exp.Table):
        name = t.name.lower()
        if not name or name in ctes or name in have:
            continue
        if isinstance(t.this, (exp.Anonymous, exp.Func)):
            continue
        out.append(t.name)
    return sorted(set(out))


def prove_one(proof, stmt, local):
    """One statement's verdict, the sandbox's runners open (`proof.a`,
    `proof.b`)."""
    shown = stmt.label or squash(stmt.text)
    if stmt.dynamic:
        return Verdict(stmt.origin, shown, "not proved", stmt.dynamic,
                       stmt.runs)
    src = proof.pair.src_engine.SQL_DIALECT
    holes = Holes(stmt.text, src)
    if holes.substituted:
        return Verdict(stmt.origin, shown, "not proved", "a piece of it is"
                       " put in by the application as text as it runs",
                       stmt.runs)
    try:
        there, source = translated(stmt, holes, proof.pair, local)
    except Refused as e:
        return Verdict(stmt.origin, shown, "not converted", str(e),
                       stmt.runs)
    target_said = unmarked(there, holes)[0] if stmt.shown else ""
    a, b = proof.a, proof.b
    missing = _tables_missing(source, a)
    if missing:
        return Verdict(stmt.origin, shown, "not proved", "it reads "
                       + ", ".join(missing[:3]) + ", which the sandbox's"
                       " fixture does not hold", stmt.runs,
                       target=target_said)
    got = fillings(proof, stmt, holes, partners(source))
    if isinstance(got, str):
        return Verdict(stmt.origin, shown, "not proved", got, stmt.runs,
                       target=target_said)
    src_sql = holes.executable(a.engine)
    dst_sql, order = unmarked(there, holes, b.engine)
    # a read is compared by its answer; a write - and a read calling a
    # routine of the application's, which may write - by every table too,
    # since a converted trigger may write where the statement does not
    writes = not isinstance(source, exp.Query) or \
        source.find(exp.Anonymous) is not None
    tables = list(a.watched) if writes else []
    mode = harness.clock_mode(a, b, stmt.text, there)
    calls, did, errors = 0, False, set()
    for pa, pb, said in got:
        params_a = holes.params(pa)
        params_b = [pb[holes.slots[k]] for k in order]
        try:
            x, y, clock = proof._both(
                a, b, lambda: a.run(src_sql, params_a, tables, rows=True),
                lambda: b.run(dst_sql, params_b, tables, rows=True), mode)
        except harness.HarnessError as e:
            return Verdict(stmt.origin, shown, "not proved", "the sandbox"
                           f" could not run it: {squash(e, 120)}",
                           stmt.runs, calls, target_said)
        calls += 1
        found = apart(x, y, clock)
        if found:
            return Verdict(stmt.origin, shown, "diff", f"run with {said}:"
                           f" {found[0]}", stmt.runs, calls, target_said)
        did = did or _did(x) or _did(y)
        if x.error is not None:
            errors.add(harness.error_key(x.error)[0])
    if not did:
        why = ("every run answered alike, and none answered a row or"
               " changed one: nothing in the fixture meets it")
        if errors and len(errors) == 1 and calls == len(got):
            why = (f"both sides refused every run alike (class"
                   f" {next(iter(errors))})")
        return Verdict(stmt.origin, shown, "warn", why, stmt.runs, calls,
                       target_said)
    return Verdict(stmt.origin, shown, "ok", f"{calls} runs answered"
                   " alike", stmt.runs, calls, target_said)


def prove(proof, stmts, budget=BUDGET_SECONDS):
    """[Verdict] for `stmts`, the most run first, within `budget` seconds;
    the sandbox's runners open (`proof.begin`)."""
    def columns_of(table):
        for t, w in proof.a.watched.items():
            if _leaf(t) == _leaf(table):
                return [c for c, _ in w.columns]
        return []
    local = localized(proof.pair, proof.db, columns_of)
    # the calls the engines' own tests show answering apart are refused
    local.parting = getattr(proof, "parting", None)
    began = time.monotonic()
    out = []
    ranked = sorted(stmts, key=lambda s: -int(s.runs or 0))[:MOST]
    for k, s in enumerate(ranked):
        if time.monotonic() - began > budget:
            for rest in ranked[k:]:
                out.append(Verdict(rest.origin, rest.label or
                                   squash(rest.text), "not proved",
                                   f"the {budget:g} seconds given to the"
                                   " application's statements ran out",
                                   rest.runs))
            break
        try:
            out.append(prove_one(proof, s, local))
        except Exception as e:  # noqa: BLE001 - said, never ok
            for runner in (proof.a, proof.b):
                try:
                    runner.conn.rollback()
                except Exception:  # noqa: BLE001 - opened again below
                    pass
            out.append(Verdict(s.origin, s.label or squash(s.text),
                               "not proved", "it could not be run: "
                               + squash(e, 120), s.runs))
    return out


def weighted(verdicts):
    """(runs of statements that answered alike, runs)."""
    total = sum(int(v.runs or 0) for v in verdicts)
    same = sum(int(v.runs or 0) for v in verdicts
               if v.status in ("ok", "warn"))
    return same, total


def result(db, verdicts, where, scope="application statements"):
    """The database's line for the statements `where` names (the
    source's record, the application's code)."""
    from ..engines.base import Result
    scope = f"{db} {scope}"
    if not verdicts:
        return None
    counts = {}
    for v in verdicts:
        counts[v.status] = counts.get(v.status, 0) + 1
    said = ", ".join(f"{counts[s]} {s}" for s in ORDER if counts.get(s))
    worst = max(verdicts, key=lambda v: ORDER[v.status]).status
    shown = sorted((v for v in verdicts if v.status != "ok"),
                   key=lambda v: (-ORDER[v.status], -int(v.runs or 0)))
    detail = f"{len(verdicts)} statements from {where}: {said}"
    if shown:
        detail += "; " + "; ".join(v.line() for v in shown[:6])
        if len(shown) > 6:
            detail += f"; ... {len(shown) - 6} more"
    same, total = weighted(verdicts)
    if total > len(verdicts):
        detail += (f"; of the {total:,} runs on record,"
                   f" {same / total:.1%} are of statements that answered"
                   " alike on both sides")
    status = {"ok": "ok", "warn": "warn", "not converted": "warn",
              "not proved": "warn", "diff": "diff"}[worst]
    hint = ""
    if worst == "diff":
        hint = ("the statement answers otherwise once translated: the"
                " application's SQL is to be changed by hand where it was"
                " found (application-sql.sql beside the reports holds each"
                " translated statement and where it came from)")
    elif worst == "not converted":
        hint = ("write the statements named as not converted in the"
                " target's SQL by hand")
    return Result("schema", scope, status, detail, "", hint)


def write(path, verdicts):
    """The verdicts beside the reports, and the application's translated
    statements with where each was found, for a person to put in the
    application - migkit changes none of it."""
    import json
    try:
        path.with_suffix(".json").write_text(json.dumps(
            [v.record() for v in verdicts], indent=1, default=str))
        lines = ["-- the application's statements in the target's SQL,"
                 " written by migkit's check;", "-- each with where it"
                 " was found and whether the proof held it to the"
                 " source's answers", ""]
        for v in verdicts:
            if not v.target:
                continue
            lines += [f"-- {v.origin}: {v.status}"
                      + (f" - {v.reason}" if v.reason and
                         v.status != "ok" else ""),
                      v.target.rstrip().rstrip(";") + ";", ""]
        if len(lines) > 3:
            path.with_suffix(".sql").write_text("\n".join(lines))
    except OSError:
        pass

"""A DDL statement as a change log carries it, read for the tables it
touches and what it does to each (`canon.ddl`).

A binlog in row format writes every DDL as the statement's own text, at
its place among the rows, with the database that was current when it ran
(MySQL's query event). What the tail needs from it is small: which tables,
and whether the statement empties one, drops one, renames one, makes one
or alters one - the rest of the statement is the server's business, and
the shape it leaves is read from the catalogue and from the rows after it
(`binlog_row_metadata = FULL` puts each column's name in every table map).
So this reads only the head of a statement, by rule, and a statement it
does not recognise touches nothing: the catalogue check before each batch
(`hetero._shape_gate`) sees what such a statement did to a table's shape.
An ALTER is read clause by clause (`clauses`): one that changes a table's
rows without a row event for them - a partition truncated, dropped or
exchanged - is a `reload`, never a harmless `alter`.
"""
import re

#: a name as MySQL writes one: back-quoted (a doubled back quote inside),
#: or bare
_NAME = r"(?:`(?:[^`]|``)+`|[A-Za-z0-9_$\u0080-\uffff]+)"
#: a table, with its database where the statement names one
_TABLE = rf"({_NAME})(?:\s*\.\s*({_NAME}))?"

_COMMENTS = re.compile(r"/\*.*?\*/|--[^\n]*\n|#[^\n]*\n", re.S)

_RULES = (
    ("truncate", re.compile(rf"^truncate\s+(?:table\s+)?{_TABLE}", re.I)),
    ("alter", re.compile(
        rf"^alter\s+(?:online\s+|offline\s+|ignore\s+)*table\s+"
        rf"(?:if\s+exists\s+)?{_TABLE}", re.I)),
    ("create", re.compile(
        rf"^create\s+(?:or\s+replace\s+)?table\s+(?:if\s+not\s+exists\s+)?"
        rf"{_TABLE}", re.I)),
    ("index", re.compile(
        rf"^(?:create\s+(?:unique\s+|fulltext\s+|spatial\s+)?index|drop\s+"
        rf"index)\s+{_NAME}\s+on\s+{_TABLE}", re.I)),
)
_DROP = re.compile(r"^drop\s+(?:temporary\s+)?tables?\s+(?:if\s+exists\s+)?"
                   r"(.*)$", re.I | re.S)
_RENAME = re.compile(r"^rename\s+tables?\s+(.*)$", re.I | re.S)
_TEMPORARY = re.compile(r"^(?:create|drop)\s+temporary\s", re.I)


def _unquote(name):
    if name is None:
        return None
    name = name.strip()
    if name.startswith("`") and name.endswith("`"):
        return name[1:-1].replace("``", "`")
    return name


def _pair(first, second, schema):
    """(database, table) from a matched name and its optional second part:
    `db.t` names both, `t` the statement's current database."""
    if second is None:
        return schema, _unquote(first)
    return _unquote(first), _unquote(second)


def _names(text, schema):
    """[(database, table)] of a list of names split by commas."""
    out = []
    for part in clauses(text):
        got = re.match(rf"^\s*{_TABLE}", part)
        if got:
            out.append(_pair(got.group(1), got.group(2), schema))
    return out


def clauses(text):
    """`text` split at its top-level commas: outside back quotes, quoted
    strings and parentheses - so `ENUM('a,b')` and a partition list stay
    whole. The one splitter of a statement's comma lists (`ddl`
    `needs_backfill` reads an ALTER's clauses with it too)."""
    out, cur, quote, depth = [], [], None, 0
    for ch in text:
        if quote:
            if ch == quote:
                quote = None
        elif ch in "`'\"":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            out.append("".join(cur))
            cur = []
            continue
        cur.append(ch)
    out.append("".join(cur))
    return [p for p in out if p.strip()]


#: what an ALTER does to a table's rows without writing one row event for
#: them: a partition's rows emptied, dropped or swapped with another
#: table's, a tablespace's rows discarded or imported whole, every row
#: thrown away by the BLACKHOLE engine. The rows the target holds are then
#: wrong with nothing in the log to say which, so the table is copied again
#: (`reload`) - or the tail stops where the hop says so. Reorganising,
#: coalescing or removing partitions moves rows between partitions of the
#: same table, which MySQL refuses to do when a row would not fit
#: (documented, 8.4 "ALTER TABLE Partition Operations"): the rows stay the
#: same, so those stay an `alter`.
_ROWS_GONE = re.compile(
    r"^(?:(?:truncate|drop)\s+partition\b|exchange\s+partition\b"
    r"|(?:discard|import)\s+(?:partition\b.*\b)?tablespace\b"
    r"|engine\s*=?\s*['\"`]?blackhole\b)", re.I | re.S)
_EXCHANGE = re.compile(rf"^exchange\s+partition\s+{_NAME}\s+with\s+table\s+"
                       rf"{_TABLE}", re.I)
_RENAME_CLAUSE = re.compile(
    rf"^rename\s+(?!(?:column|index|key)\b)(?:to\s+|as\s+)?{_TABLE}\s*$",
    re.I)


def _alter(d, t, rest, schema):
    """[(database, table, kind, to)] of one ALTER TABLE's clauses: each
    read on its own, so a rename after an added column is a rename, and a
    partition's rows gone are the table's (`_ROWS_GONE`) - and, for an
    exchange, the other table's too."""
    out, altered = [], False
    for clause in clauses(rest):
        c = clause.strip()
        moved = _RENAME_CLAUSE.match(c)
        if moved:
            out.append((d, t, "rename",
                        _pair(moved.group(1), moved.group(2), schema)))
            continue
        if _ROWS_GONE.match(c):
            out.insert(0, (d, t, "reload", None))
            other = _EXCHANGE.match(c)
            if other:
                od, ot = _pair(other.group(1), other.group(2), schema)
                out.insert(1, (od, ot, "reload", None))
            continue
        altered = True
    if altered or not out:
        out.insert(0, (d, t, "alter", None))
    # an alter beside a reload says nothing more than the reload
    if any(k == "reload" for _, _, k, _ in out):
        out = [x for x in out if x[2] != "alter"]
    return out


def mysql(query, schema):
    """[(database, table, kind, (database, table) it goes to or None)] for
    one query event's text, `schema` the database current when it ran;
    `kind` one of `canon.DDL_KINDS`.
    [] for a statement that is not a DDL of a table the log carries rows
    of - a temporary table's is never logged as rows."""
    if isinstance(query, bytes):
        query = query.decode("utf-8", "replace")
    if isinstance(schema, bytes):
        schema = schema.decode("utf-8", "replace")
    text = _COMMENTS.sub(" ", str(query) + "\n").strip().rstrip(";").strip()
    if not text or _TEMPORARY.match(text):
        return []
    got = _DROP.match(text)
    if got:
        return [(d, t, "drop", None) for d, t in _names(got.group(1),
                                                        schema)]
    got = _RENAME.match(text)
    if got:
        out = []
        for part in clauses(got.group(1)):
            pair = re.match(rf"^\s*{_TABLE}\s+to\s+{_TABLE}\s*$", part, re.I)
            if pair:
                a = _pair(pair.group(1), pair.group(2), schema)
                b = _pair(pair.group(3), pair.group(4), schema)
                out.append((a[0], a[1], "rename", b))
        return out
    for kind, rule in _RULES:
        got = rule.match(text)
        if not got:
            continue
        d, t = _pair(got.group(1), got.group(2), schema)
        if kind == "index":
            return [(d, t, "alter", None)]
        if kind == "alter":
            return _alter(d, t, text[got.end():], schema)
        return [(d, t, kind, None)]
    return []

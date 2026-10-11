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


# ---- Oracle -----------------------------------------------------------

#: a name as Oracle writes one: double-quoted (a doubled quote inside),
#: or bare - which the server folds to capitals
_ONAME = r'(?:"(?:[^"]|"")+"|[A-Za-z][\w$#]*)'
_OTABLE = rf"({_ONAME})(?:\s*\.\s*({_ONAME}))?"

_ORACLE = (
    ("truncate", re.compile(rf"^truncate\s+table\s+{_OTABLE}", re.I)),
    ("drop", re.compile(rf"^drop\s+table\s+{_OTABLE}", re.I)),
    # rows come back with no row event for them
    ("reload", re.compile(rf"^flashback\s+table\s+{_OTABLE}\s+to\s", re.I)),
    ("alter", re.compile(rf"^alter\s+table\s+{_OTABLE}", re.I)),
    ("create", re.compile(rf"^create\s+table\s+{_OTABLE}", re.I)),
    ("index", re.compile(
        rf"^create\s+(?:unique\s+|bitmap\s+)?index\s+{_OTABLE}\s+on\s+"
        rf"{_OTABLE}", re.I)),
)
_ORENAME = re.compile(rf"^rename\s+{_OTABLE}\s+to\s+({_ONAME})\s*$", re.I)
_ORENAME_CLAUSE = re.compile(rf"^rename\s+to\s+({_ONAME})\s*$", re.I)
#: what an ALTER does to a table's rows without writing a row event for
#: them: a partition emptied, dropped or swapped with another table's
_OROWS_GONE = re.compile(
    r"^(?:(?:truncate|drop)\s+(?:sub)?partition\b|exchange\s+(?:sub)?"
    r"partition\b)", re.I)
_OEXCHANGE = re.compile(
    rf"^exchange\s+(?:sub)?partition\s+{_ONAME}\s+with\s+table\s+"
    rf"{_OTABLE}", re.I)


def _ofold(name):
    """A name as the server keeps it: bare, in capitals; quoted, as it is
    written."""
    name = name.strip()
    if name.startswith('"') and name.endswith('"'):
        return name[1:-1].replace('""', '"')
    return name.upper()


def _opair(first, second, schema):
    """(schema, table) from a matched name and its optional second part."""
    if second is None:
        return schema, _ofold(first)
    return _ofold(first), _ofold(second)


def oracle(query, schema):
    """[(schema, table, kind, (schema, table) it goes to or None)] for one
    DDL statement of the redo, `schema` the owner it runs in where the
    statement names none; `kind` one of `canon.DDL_KINDS`. [] for a
    statement that is not a DDL of a table the redo carries rows of - a
    temporary table's is never logged as rows."""
    text = _COMMENTS.sub(" ", str(query) + "\n").strip().rstrip(";").strip()
    if not text or re.match(r"^create\s+global\s+temporary\b", text, re.I):
        return []
    got = _ORENAME.match(text)
    if got:
        a = _opair(got.group(1), got.group(2), schema)
        return [(a[0], a[1], "rename", (a[0], _ofold(got.group(3))))]
    for kind, rule in _ORACLE:
        got = rule.match(text)
        if not got:
            continue
        if kind == "index":
            # an index's name may be qualified, its table's follows `on`
            d, t = _opair(got.group(3), got.group(4), schema)
            return [(d, t, "alter", None)]
        d, t = _opair(got.group(1), got.group(2), schema)
        if kind == "alter":
            rest = text[got.end():].strip()
            moved = _ORENAME_CLAUSE.match(rest)
            if moved:
                return [(d, t, "rename", (d, _ofold(moved.group(1))))]
            if _OROWS_GONE.match(rest):
                out = [(d, t, "reload", None)]
                other = _OEXCHANGE.match(rest)
                if other:
                    od, ot = _opair(other.group(1), other.group(2), d)
                    out.append((od, ot, "reload", None))
                return out
            return [(d, t, "alter", None)]
        return [(d, t, kind, None)]
    return []


#: a statement that changes rows, in a binlog written in STATEMENT or
#: MIXED format - which carries the text of those, where ROW carries the
#: rows. Only the plain forms are read: one table, named after its
#: keyword. Anything else that writes (a join, a CALL, a DO, a trigger's
#: or a cascade's effect on another table) names tables the text does not.
_DML = (
    re.compile(rf"^(?:insert|replace)\s+(?:low_priority\s+|delayed\s+|"
               rf"high_priority\s+|ignore\s+)*(?:into\s+)?{_TABLE}", re.I),
    re.compile(rf"^update\s+(?:low_priority\s+|ignore\s+)*{_TABLE}"
               rf"(?:\s+(?:as\s+)?{_NAME})?\s+set\b", re.I),
    re.compile(rf"^delete\s+(?:low_priority\s+|quick\s+|ignore\s+)*from\s+"
               rf"{_TABLE}(?:\s+(?:where|order|limit)\b|\s*$)", re.I),
    re.compile(rf"^load\s+data\s.*?\binto\s+table\s+{_TABLE}", re.I | re.S),
)
#: a statement that writes rows by a way the text does not name the
#: tables of
_DML_UNREAD = re.compile(r"^(?:insert|replace|update|delete|load\s+data|call|"
                         r"do|execute|xa\s+commit)\b", re.I)
#: what a transaction's bracket and a session's setting look like
_NOT_ROWS = re.compile(r"^(?:begin|commit|rollback|savepoint|release|set|"
                       r"flush|grant|revoke|create\s+(?:user|role|database|"
                       r"view|function|procedure|trigger|event|definer)|"
                       r"drop\s+(?:user|role|database|view|function|"
                       r"procedure|trigger|event)|alter\s+(?:user|database|"
                       r"view|function|procedure|event)|analyze|optimize|"
                       r"use|xa\s+(?:start|end|prepare|rollback))\b", re.I)


def dml(query, schema):
    """([(database, table)] a statement changes rows of, whether it also
    changes rows of tables its text does not name). ([], False) for a
    statement that changes none - a transaction's bracket, a session's
    setting, the DDL `mysql` reads."""
    if isinstance(query, bytes):
        query = query.decode("utf-8", "replace")
    if isinstance(schema, bytes):
        schema = schema.decode("utf-8", "replace")
    text = _COMMENTS.sub(" ", str(query) + "\n").strip().rstrip(";").strip()
    if not text or _NOT_ROWS.match(text):
        return [], False
    for rule in _DML:
        got = rule.match(text)
        if got:
            # an INSERT ... SELECT, or an UPDATE of a view or a table with
            # a trigger, writes where the text does not say: the caller
            # knows which tables have triggers
            return [_pair(got.group(1), got.group(2), schema)], False
    return [], bool(_DML_UNREAD.match(text))

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
_ALTER_RENAME = re.compile(rf"\srename\s+(?:to\s+|as\s+)?{_TABLE}\s*$",
                           re.I)


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
    for part in _split_commas(text):
        got = re.match(rf"^\s*{_TABLE}", part)
        if got:
            out.append(_pair(got.group(1), got.group(2), schema))
    return out


def _split_commas(text):
    """`text` at the commas outside back quotes."""
    out, cur, quoted = [], [], False
    for ch in text:
        if ch == "`":
            quoted = not quoted
        if ch == "," and not quoted:
            out.append("".join(cur))
            cur = []
            continue
        cur.append(ch)
    out.append("".join(cur))
    return [p for p in out if p.strip()]


def mysql(query, schema):
    """[(database, table, kind, (database, table) it goes to or None)] for
    one query event's text, `schema` the database current when it ran.
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
        for part in _split_commas(got.group(1)):
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
            moved = _ALTER_RENAME.search(text)
            if moved and "," not in text[got.end():moved.start()]:
                b = _pair(moved.group(1), moved.group(2), schema)
                return [(d, t, "rename", b)]
        return [(d, t, kind, None)]
    return []

"""A hop's row filter, read one way and said in every engine's own words
(backlog P0-SCOPE.7).

`mapping.where` is written once, in the source's SQL, and asked of every
reader of the table: the copy on the source, the check on both sides, the
tail when a change crosses it, a repair. Every one of them must take the
same rows, or the move carries one set and the check compares another.
Three ways they used not to:

* **another language**: a document store has no SQL, so a filter could
  not be asked of it at all; and SQL's NULL is not a document store's -
  `a <> 'x'` leaves out a row whose `a` is NULL, where `{a: {$ne: 'x'}}`
  takes it. `to_mongo` says the predicate with SQL's three-valued logic
  kept, `NOT` pushed down so it cannot take the NULL rows back in.
* **the clock**: a filter that reads `now()` takes other rows at the
  check than at the copy, and rows cross it with no change at all.
  `problems` refuses it before anything is read.
* **case and accents**: `name = 'abc'` takes `ABC` under a collation
  without case and not under one with it; where the two sides compare
  text differently, the predicate's text comparisons are named
  (`text_compared`) for the pre-flight to hold against the two sides'
  collations.

`evaluate` is the reference: SQL's three-valued logic over a row in
Python, which the tests hold every translation to on the same rows, and
which a reader with no query language of its own (a file, a stream)
filters by.
"""
import datetime as _dt
import re

#: functions whose answer moves with the clock or the session - a filter
#: built on one takes other rows at every reading
MOVING = {"currenttimestamp", "currentdate", "currenttime", "now",
          "currentdatetime", "localtimestamp", "localtime", "sysdate",
          "systimestamp", "getdate", "getutcdate", "utctimestamp",
          "utcdate", "unixtimestamp", "rand", "random", "uuid",
          "currentuser", "sessionuser", "user"}


#: the SQL each engine's row filter is written in, where the engine's own
#: class does not say (`Engine.SQL_DIALECT`) - read for parsing it, not a
#: promise that the engine applies it
DIALECTS = {"postgres": "postgres", "mysql": "mysql", "tidb": "mysql",
            "mssql": "tsql", "ase": "tsql", "oracle": "oracle",
            "sqlite": "sqlite", "duckdb": "duckdb", "clickhouse": "clickhouse",
            "teradata": "teradata", "snowflake": "snowflake",
            "bigquery": "bigquery", "redshift": "redshift",
            "cockroachdb": "postgres", "dsql": "postgres",
            "mongodb": "postgres"}


def dialect_of(engine):
    """The SQL a hop on `engine` writes its row filters in."""
    from .util import _canonical
    return DIALECTS.get(_canonical(str(engine or "")), None)


def zone_offset(tz):
    """A time zone as a filter's times are read in: `+07:00`, `-03:30`,
    `UTC`, or a name in the zone database (`Asia/Bangkok`); SystemExit for
    one that is none of them."""
    text = str(tz).strip()
    if re.fullmatch(r"[+-](?:0\d|1[0-4]):[0-5]\d", text) or \
            text.upper() in ("UTC", "Z"):
        return text
    try:
        from zoneinfo import ZoneInfo
        ZoneInfo(text)
        return text
    except Exception:
        raise SystemExit(f"{tz!r} is not a time zone - an offset (+07:00)"
                         " or a zone name (Asia/Bangkok)")


def parse(pred, dialect):
    """The predicate's tree, read in the source's SQL; SystemExit naming
    the filter where it is not one."""
    import sqlglot
    try:
        return sqlglot.parse_one(str(pred), read=dialect or None)
    except Exception as e:
        raise SystemExit(f"the row filter {pred!r} is not SQL the source"
                         f" reads: {str(e).splitlines()[0][:120]}")


def problems(pred, dialect):
    """What makes a filter take other rows at another reading: a function
    of the clock or the session, a subquery (rows of another table, which
    move on their own). Empty where there is nothing to say."""
    from sqlglot import exp
    tree = parse(pred, dialect)
    out = []
    for node in tree.walk():
        name = type(node).__name__.lower()
        fn = node.name.lower() if isinstance(node, exp.Anonymous) else ""
        if name in MOVING or fn in MOVING:
            out.append(f"{node.sql(dialect=dialect or None)} reads the clock"
                       " or the session: the copy and the check would take"
                       " other rows - write the moment out as a literal")
        elif isinstance(node, (exp.Subquery, exp.Select)) and node is not \
                tree:
            out.append("a subquery reads another table, whose rows move on"
                       " their own - write the values out, or filter on"
                       " this table's own columns")
    return sorted(set(out))


def columns(pred, dialect):
    """The columns a filter reads - which the tail must be told, and the
    source's replica identity must hold, for a change to be judged by
    it."""
    from sqlglot import exp
    return sorted({c.name for c in parse(pred, dialect).find_all(exp.Column)})


def text_compared(pred, dialect):
    """The columns a filter compares as text - to a string literal, by
    LIKE, IN a list of strings - whose answer depends on the collation's
    case and accents."""
    from sqlglot import exp
    out = set()
    for node in parse(pred, dialect).walk():
        if isinstance(node, (exp.Like, exp.ILike)):
            out.update(c.name for c in node.this.find_all(exp.Column))
        elif isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT,
                               exp.LTE)):
            sides = (node.this, node.expression)
            if any(isinstance(s, exp.Literal) and s.is_string
                   for s in sides):
                for s in sides:
                    if isinstance(s, exp.Column):
                        out.add(s.name)
        elif isinstance(node, exp.In) and any(
                isinstance(e, exp.Literal) and e.is_string
                for e in node.expressions):
            out.update(c.name for c in node.this.find_all(exp.Column))
    return sorted(out)


def to_sql(pred, src_dialect, dst_dialect, renames=None):
    """The filter in another engine's SQL, each column under the name the
    hop's mapping gives it there (`renames`: source name -> target
    name). SystemExit where the target's SQL has no way to say it."""
    import sqlglot
    from sqlglot import exp
    tree = parse(pred, src_dialect)
    for col in tree.find_all(exp.Column):
        new = (renames or {}).get(col.name)
        if new and new != col.name:
            col.set("this", exp.to_identifier(new))
    try:
        return tree.sql(dialect=dst_dialect or None,
                        unsupported_level=sqlglot.ErrorLevel.RAISE)
    except Exception as e:
        raise SystemExit(f"the row filter {pred!r} has no translation into"
                         f" the target's SQL: {str(e).splitlines()[0][:120]}")


# ---- a document store --------------------------------------------------

def to_mongo(pred, dialect, renames=None):
    """The filter as a document store's query (`$match`), with SQL's NULL
    kept: a comparison with a NULL or missing field is not true, so it
    never takes the row, and `NOT` does not take it back in."""
    tree = parse(pred, dialect)
    return _m(tree, renames or {}, negate=False)


def _field(node, renames):
    from sqlglot import exp
    if not isinstance(node, exp.Column):
        raise SystemExit(f"{node.sql()} is not a column - a document store"
                         " is asked of its fields only")
    parts = [p.name for p in node.parts]
    name = ".".join(parts)
    return renames.get(name, name)


def _value(node):
    from sqlglot import exp
    if isinstance(node, exp.Paren):
        return _value(node.this)
    if isinstance(node, exp.Boolean):
        return bool(node.this)
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Neg):
        return -_value(node.this)
    if isinstance(node, exp.Literal):
        if node.is_string:
            return node.this
        text = str(node.this)
        return float(text) if any(c in text for c in ".eE") else int(text)
    raise SystemExit(f"{node.sql()} is not a value a document store's"
                     " query can hold - write it out as a literal")


_FLIP = {"EQ": "NEQ", "NEQ": "EQ", "GT": "LTE", "GTE": "LT", "LT": "GTE",
         "LTE": "GT"}
_OPS = {"EQ": "$eq", "NEQ": "$ne", "GT": "$gt", "GTE": "$gte", "LT": "$lt",
        "LTE": "$lte"}
_MIRROR = {"GT": "LT", "GTE": "LTE", "LT": "GT", "LTE": "GTE", "EQ": "EQ",
           "NEQ": "NEQ"}


def _present(field):
    """The field holds a value: SQL's comparisons are true only then."""
    return {field: {"$exists": True, "$ne": None}}


def _m(node, renames, negate):
    from sqlglot import exp
    if isinstance(node, exp.Paren):
        return _m(node.this, renames, negate)
    if isinstance(node, exp.Not):
        return _m(node.this, renames, not negate)
    if isinstance(node, (exp.And, exp.Or)):
        both = [_m(node.this, renames, negate),
                _m(node.expression, renames, negate)]
        # De Morgan: NOT (a AND b) is NOT a OR NOT b - each side keeping
        # its own NULLs out
        word = "$and" if isinstance(node, exp.And) != negate else "$or"
        return {word: both}
    kind = type(node).__name__
    if kind in _OPS:
        left, right = node.this, node.expression
        if not isinstance(left, exp.Column):
            left, right = right, left
            kind = _MIRROR[kind]
        field, value = _field(left, renames), _value(right)
        if value is None:
            # `= NULL` is never true in SQL, and neither is its NOT
            return {"$expr": False}
        if negate:
            kind = _FLIP[kind]
        if kind == "NEQ":
            return {"$and": [_present(field), {field: {"$ne": value}}]}
        return {field: {_OPS[kind]: value}}
    if isinstance(node, exp.Is):
        field = _field(node.this, renames)
        if not isinstance(node.expression, exp.Null):
            raise SystemExit(f"{node.sql()}: IS takes NULL only here")
        # IS NULL is true or false, never unknown: NOT turns it round
        return _present(field) if negate else \
            {"$or": [{field: {"$exists": False}}, {field: None}]}
    if isinstance(node, exp.In):
        field = _field(node.this, renames)
        values = [_value(e) for e in node.expressions]
        if node.args.get("query"):
            raise SystemExit("IN over a subquery has no document query")
        kept = [v for v in values if v is not None]
        if negate:
            if len(kept) != len(values):
                # NOT IN with a NULL among the values is never true
                return {"$expr": False}
            return {"$and": [_present(field), {field: {"$nin": kept}}]}
        return {field: {"$in": kept}}
    if isinstance(node, exp.Between):
        field = _field(node.this, renames)
        lo, hi = _value(node.args["low"]), _value(node.args["high"])
        if negate:
            return {"$and": [_present(field), {"$or": [
                {field: {"$lt": lo}}, {field: {"$gt": hi}}]}]}
        return {field: {"$gte": lo, "$lte": hi}}
    if isinstance(node, exp.Like):
        field = _field(node.this, renames)
        pattern = _value(node.expression)
        regex = "^" + "".join(".*" if c == "%" else "." if c == "_"
                              else re.escape(c) for c in str(pattern)) + "$"
        if negate:
            return {"$and": [_present(field),
                             {field: {"$not": {"$regex": regex,
                                               "$options": "s"}}}]}
        return {field: {"$regex": regex, "$options": "s"}}
    if isinstance(node, exp.Boolean):
        return {"$expr": bool(node.this) != negate}
    if isinstance(node, exp.Column):
        field = _field(node, renames)
        return {field: {"$eq": not negate}}
    raise SystemExit(f"{node.sql()}: no document query says it - filter on"
                     " comparisons, IN, BETWEEN, LIKE and IS NULL")


# ---- the reference -----------------------------------------------------

def evaluate(pred, dialect, row):
    """Whether a row (a mapping of column to value) is one the filter
    takes, by SQL's three-valued logic: a comparison with a NULL is
    unknown, unknown is not taken, and NOT unknown is unknown."""
    return _e(parse(pred, dialect), row) is True


def _cmp(a, b):
    if isinstance(a, str) and not isinstance(b, str):
        a = _coerce(a, b)
    elif isinstance(b, str) and not isinstance(a, str):
        b = _coerce(b, a)
    return a, b


def _coerce(text, like):
    if isinstance(like, bool):
        return text.lower() in ("t", "true", "1")
    if isinstance(like, (int, float)):
        return float(text)
    if isinstance(like, _dt.datetime):
        return _dt.datetime.fromisoformat(text)
    if isinstance(like, _dt.date):
        return _dt.date.fromisoformat(text[:10])
    return text


def _e(node, row):
    from sqlglot import exp
    if isinstance(node, exp.Paren):
        return _e(node.this, row)
    if isinstance(node, exp.Not):
        got = _e(node.this, row)
        return None if got is None else not got
    if isinstance(node, exp.And):
        a, b = _e(node.this, row), _e(node.expression, row)
        if a is False or b is False:
            return False
        return None if a is None or b is None else True
    if isinstance(node, exp.Or):
        a, b = _e(node.this, row), _e(node.expression, row)
        if a is True or b is True:
            return True
        return None if a is None or b is None else False
    if isinstance(node, exp.Column):
        return row.get(".".join(p.name for p in node.parts))
    if isinstance(node, (exp.Literal, exp.Boolean, exp.Null, exp.Neg)):
        return _value(node)
    kind = type(node).__name__
    if kind in _OPS:
        a, b = _e(node.this, row), _e(node.expression, row)
        if a is None or b is None:
            return None
        a, b = _cmp(a, b)
        return {"EQ": a == b, "NEQ": a != b, "GT": a > b, "GTE": a >= b,
                "LT": a < b, "LTE": a <= b}[kind]
    if isinstance(node, exp.Is):
        return _e(node.this, row) is None
    if isinstance(node, exp.In):
        a = _e(node.this, row)
        values = [_e(x, row) for x in node.expressions]
        if a is None:
            return None
        hits = [_cmp(a, v)[0] == _cmp(a, v)[1] for v in values
                if v is not None]
        if any(hits):
            return True
        return None if any(v is None for v in values) else False
    if isinstance(node, exp.Between):
        a = _e(node.this, row)
        lo, hi = _e(node.args["low"], row), _e(node.args["high"], row)
        if a is None or lo is None or hi is None:
            return None
        a1, lo = _cmp(a, lo)
        a2, hi = _cmp(a, hi)
        return lo <= a1 and a2 <= hi
    if isinstance(node, exp.Like):
        a, pattern = _e(node.this, row), _e(node.expression, row)
        if a is None or pattern is None:
            return None
        regex = "".join(".*" if c == "%" else "." if c == "_"
                        else re.escape(c) for c in str(pattern))
        return re.fullmatch(regex, str(a), re.S) is not None
    raise SystemExit(f"{node.sql()}: the reference does not read it")

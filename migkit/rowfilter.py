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
    except (KeyError, ValueError, OSError):
        raise SystemExit(f"{tz!r} is not a time zone - an offset (+07:00)"
                         " or a zone name (Asia/Bangkok)")


def parse(pred, dialect):
    """The predicate's tree, read in the source's SQL; SystemExit naming
    the filter where it is not one."""
    import sqlglot
    from sqlglot.errors import ParseError, TokenError
    try:
        return sqlglot.parse_one(str(pred), read=dialect or None)
    except (ParseError, TokenError) as e:
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
    except sqlglot.errors.UnsupportedError as e:
        raise SystemExit(f"the row filter {pred!r} has no translation into"
                         f" the target's SQL: {str(e).splitlines()[0][:120]}")


# ---- the zone a filter's times are read in -----------------------------

#: what a column's values mean for a time it is compared with: a point in
#: time (`canon.INSTANT`), the digits of a clock with no zone
#: (`canon.WALL`), or a time whose meaning this engine's is not measured
INSTANT, WALL, UNKNOWN = "instant", "wall clock", "unknown"


def _zone(tz):
    import datetime as dt
    text = zone_offset(tz)
    if text.upper() in ("UTC", "Z"):
        return dt.timezone.utc
    if text[0] in "+-":
        sign = -1 if text[0] == "-" else 1
        hours, minutes = int(text[1:3]), int(text[4:6])
        return dt.timezone(sign * dt.timedelta(hours=hours,
                                               minutes=minutes))
    from zoneinfo import ZoneInfo
    return ZoneInfo(text)


def instant(text, tz):
    """The UTC moment a filter's time literal names when read in `tz`; one
    that says its own offset is that moment. SystemExit for a text that is
    no time, and for a wall-clock time `tz` has twice (the hour a clock
    goes back) or never (the hour it goes forward): no one reading of it is
    exact."""
    import datetime as dt
    try:
        got = dt.datetime.fromisoformat(str(text).strip())
    except ValueError:
        raise SystemExit(f"{text!r} is compared with a column holding a"
                         " point in time, and is not a time")
    if got.tzinfo is not None:
        return got.astimezone(dt.timezone.utc)
    zone = _zone(tz)
    first, second = got.replace(tzinfo=zone, fold=0), \
        got.replace(tzinfo=zone, fold=1)
    # a time in the hour a clock skips reads back as another; one in the
    # hour it repeats reads back as itself under two offsets
    back = first.astimezone(dt.timezone.utc).astimezone(zone)
    if back.replace(tzinfo=None) != got:
        raise SystemExit(f"{text} never happens in {tz} (its clock goes"
                         " forward over it): write the time with its offset")
    if first.utcoffset() != second.utcoffset():
        raise SystemExit(f"{text} happens twice in {tz} (its clock goes back"
                         " over it): write the time with its offset")
    return first.astimezone(dt.timezone.utc)


def _cast(type_name, dialect):
    def build(at):
        from sqlglot import exp
        return exp.Cast(this=exp.Literal.string(
            at.strftime("%Y-%m-%d %H:%M:%S.%f") + "+00:00"),
            to=exp.DataType.build(type_name, dialect=dialect))
    return build


def _oracle(at):
    from sqlglot import exp
    return exp.Anonymous(this="TO_TIMESTAMP_TZ", expressions=[
        exp.Literal.string(at.strftime("%Y-%m-%d %H:%M:%S.%f") + " +00:00"),
        exp.Literal.string("YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM")])


def _epoch(at):
    from sqlglot import exp
    return exp.Literal.number(f"{at.timestamp():.6f}")


def _unix(col):
    from sqlglot import exp
    return exp.Anonymous(this="UNIX_TIMESTAMP", expressions=[col.copy()])


#: a point in time written so a dialect's server reads it as that point in
#: every session's zone: (the column it is compared with as it must be read
#: for that, or None for itself; the point); a dialect not here has no
#: exact way migkit writes one
_INSTANT_SQL = {
    "postgres": (None, _cast("TIMESTAMPTZ", "postgres")),
    "duckdb": (None, _cast("TIMESTAMPTZ", "duckdb")),
    "tsql": (None, _cast("DATETIMEOFFSET", "tsql")),
    "oracle": (None, _oracle),
    # a TIMESTAMP's own number of seconds, which the session's zone does
    # not touch (UNIX_TIMESTAMP of the column reads it as stored)
    "mysql": (_unix, _epoch),
}


def zone_free(pred, dialect, tz, kinds):
    """The filter with every time it compares a column holding a point in
    time written as that point (`instant`, read in `tz`), in the dialect's
    words for one no session's zone moves (`_INSTANT_SQL`) - so the copy,
    the check and the tail take the same rows whatever zone each server's
    session is in. `kinds` is {column: INSTANT | WALL | UNKNOWN} of the
    table's time columns; a wall clock's digits mean no zone and are left
    as written, and so is every other column.

    SystemExit where it cannot be made exact: a column holding a point in
    time read through a function or an expression (which reads it in the
    session's zone), a time column whose meaning this engine's is not
    measured, a dialect with no exact way to write the point, a time `tz`
    has twice or never."""
    from sqlglot import exp
    tree = parse(pred, dialect)
    kinds = {str(k): v for k, v in (kinds or {}).items()}

    def kind(node):
        return kinds.get(node.name) if isinstance(node, exp.Column) else None

    def literal(node):
        return isinstance(node, exp.Literal) and node.is_string

    done = set()

    def written(col, lits):
        if dialect not in _INSTANT_SQL:
            raise SystemExit(f"{col.name} holds a point in time, and this"
                             " engine's SQL has no way migkit writes one"
                             f" exactly: write the times with their"
                             " offset, or drop tz")
        col_form, lit_form = _INSTANT_SQL[dialect]
        for lit in lits:
            lit.replace(lit_form(instant(lit.this, tz)))
        if col_form is not None:
            wrapped = col_form(col)
            col.replace(wrapped)
            done.add(id(wrapped))
        done.add(id(col))

    for node in list(tree.walk()):
        if type(node).__name__ in _OPS:
            a, b = node.this, node.expression
            col, lit = (a, b) if kind(a) else (b, a)
            got = kind(col)
            if got == INSTANT and literal(lit):
                written(col, [lit])
            elif got == UNKNOWN and literal(lit):
                raise SystemExit(f"{col.name} holds a time whose meaning on"
                                 " this engine is not measured, so a time"
                                 f" read in {tz} has no exact match: write"
                                 " it with its offset, or drop tz")
        elif isinstance(node, exp.Between) and kind(node.this) and all(
                literal(node.args[k]) for k in ("low", "high")):
            if kind(node.this) == UNKNOWN:
                raise SystemExit(f"{node.this.name} holds a time whose"
                                 " meaning on this engine is not measured")
            if kind(node.this) == INSTANT:
                written(node.this, [node.args["low"], node.args["high"]])
        elif isinstance(node, exp.In) and kind(node.this) and all(
                literal(e) for e in node.expressions):
            if kind(node.this) == UNKNOWN:
                raise SystemExit(f"{node.this.name} holds a time whose"
                                 " meaning on this engine is not measured")
            if kind(node.this) == INSTANT:
                written(node.this, list(node.expressions))
    for col in tree.find_all(exp.Column):
        if kind(col) != INSTANT or id(col) in done or any(
                id(p) in done for p in _parents(col)):
            continue
        holder = col.parent
        if isinstance(holder, exp.Is) or (
                type(holder).__name__ in _OPS
                and kind(holder.this) == INSTANT
                and kind(holder.expression) == INSTANT):
            continue
        raise SystemExit(f"{col.name} holds a point in time, and the filter"
                         f" reads it through {holder.sql(dialect=dialect)},"
                         " which each server reads in its own session's"
                         " zone: compare the column itself with a time")
    return tree.sql(dialect=dialect)


def _parents(node):
    p = node.parent
    while p is not None:
        yield p
        p = p.parent


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
    if isinstance(node, exp.Is) and isinstance(node.expression, exp.Boolean):
        # `x IS TRUE` holds only where x does, `x IS FALSE` only where NOT x
        # does; their NOT where x is the other or unknown - the documents
        # the first query leaves (what a reader asks to find a row outside
        # the filter: `(filter) IS NOT TRUE`)
        inner = _m(node.this, renames, not node.expression.this)
        return {"$nor": [inner]} if negate else inner
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
    if isinstance(node, exp.Is) and isinstance(node.expression, exp.Boolean):
        return _e(node.this, row) is bool(node.expression.this)
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

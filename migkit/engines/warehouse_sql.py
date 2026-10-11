"""The canonical text of a row, and the digest over it, in a warehouse's own
SQL: BigQuery, Snowflake, Redshift, and DuckDB - a DuckDB file's own, and
the Iceberg and Delta tables it reads - and in the SQL of every engine that
speaks a dialect of its own from its module (`Dialect`, `speak`).

`canon.render_value` renders a value in this process; PostgreSQL and MySQL
render it in their own SQL (`canon.expr`). This does the same for the
engines that had none, so a warehouse answers a digest with two numbers
instead of sending every row across the network to be folded here. The
number is the same `canon.fold_rows` computes: each row rendered, encoded
as `rowtext` encodes it, hashed, the first `canon.DIGEST_HEX` hex digits
taken as an integer and summed exactly - in BIGNUMERIC, NUMBER(38),
DECIMAL(38) or HUGEINT, none of which wraps.

Which hash that is is read off `canon.digest_step` (`canon_hash`), not
written down twice: MD5 today, and SHA-256 in every dialect below the day
the fold moves to it. A fold this file does not recognise turns the
server's digest off rather than computing a different number.

A class is rendered here only where the dialect's text of it is known to
be migkit's, and a column's declared type is looked at as well as its
class: a CHAR is padded on Redshift, JSON's key order is the engine's own,
and a float's text past 1e18 is not an exact decimal anywhere but DuckDB
and BigQuery. A table with any such column is folded in this process, as
before, and says so. What is rendered here is proved again on each table
before it is trusted (`DigestsInSQL._proved`): the first rows, rendered by
the server and by this process, must agree.

Measured on the in-process engines and the emulators this runs against:
DuckDB 1.5.5 on this machine; PostgreSQL 16 standing in for Redshift where
the two share their SQL, with Redshift's own `STRTOL`, `SHA2` and `TO_HEX`
given to it as functions of the same meaning; BigQuery's emulator. Snowflake
has no emulator: what is written for it is its documented SQL, proved only
on a real account.
"""
from .. import canon, rowtext
from ..decide import Need, Rung

DIALECTS = ("bigquery", "snowflake", "redshift", "duckdb", "spanner")

#: the classes each dialect renders as migkit does, before the declared
#: type is looked at (`column_text`)
RENDERED = {
    "bigquery": {"integer", "decimal", canon.NUMBER, "float", "boolean",
                 "text", "bytes", "date", "timestamp", "time"},
    "snowflake": {"integer", "decimal", canon.NUMBER, "boolean", "text",
                  "bytes", "date", "timestamp", "time"},
    "redshift": {"integer", "decimal", canon.NUMBER, "boolean", "text",
                 "bytes", "date", "timestamp", "time"},
    "duckdb": {"integer", "decimal", canon.NUMBER, "float", "boolean",
               "text", "bytes", "date", "timestamp", "time", "uuid"},
    # Spanner's GoogleSQL: BigQuery's, without a BIGNUMERIC to write a
    # float's fixed places in, and without DATETIME or TIME
    "spanner": {"integer", canon.NUMBER, "boolean", "text", "bytes", "date",
                "timestamp"},
}

#: the scale a float's fixed rendering carries, and where it gives way to
#: the shortest text (canon's bands)
_ZEROS = "0" * canon.FLOAT_SCALE


class Dialect:
    """A server's SQL for what this module writes, spoken by an engine
    whose module says it (`speak`): the canonical text of a column
    (`column`), and the pieces a row, its hash and the sum are built of.
    Each piece is the standard SQL most servers take; an engine's own
    replaces the ones its SQL spells otherwise. What it renders is proved
    on each table's first rows before it is trusted, as every dialect's
    is (`DigestsInSQL._proved`): a rendering of its that is wrong folds
    the table here, never gives another number."""
    #: the classes rendered as migkit renders them, before the declared
    #: type is looked at (`column`)
    rendered = frozenset()

    def column(self, c, cls, declared):
        """SQL for the canonical text of column `c` of this class and
        declared type, NULL where it is NULL; None where it is not known
        to be migkit's."""
        return None

    def concat(self, parts):
        return " || ".join(parts)

    def text(self, e):
        """An integer's text."""
        return f"CAST({e} AS VARCHAR)"

    def length(self, e):
        """A text's length in characters, as `rowtext` counts them."""
        return f"LENGTH({e})"

    def octets(self, e, blob=False):
        """A value's length in bytes."""
        return f"OCTET_LENGTH({e})"

    def md5_hex(self, e):
        """The md5 of a text, as 32 hexadecimal digits in small letters."""
        return f"LOWER(MD5({e}))"

    def sha256_hex(self, e):
        return None

    def from_hex(self, hexed, n):
        """The first `n` hexadecimal digits of `hexed` as an integer."""
        raise ValueError(f"no hexadecimal reading in {type(self).__name__}")

    def summed(self, value):
        """The exact sum of `value` over the rows, as text; 0 for none."""
        return (f"CAST(COALESCE(SUM(CAST({value} AS DECIMAL(38,0))), 0) AS"
                " VARCHAR(40))")

    def exact_sum(self, value):
        return f"SUM(CAST({value} AS DECIMAL(38,0)))"

    def shifted(self, value, bits):
        """`value >> bits` of a non-negative integer, by division where a
        dialect has no shift."""
        return self.divided(value, 2 ** int(bits))

    def divided(self, value, by):
        return f"FLOOR(({value}) / ({by}))"

    def literal(self, text):
        return "'" + str(text).replace("'", "''") + "'"

    def unit_separator(self):
        """The character `canon` marks a value it does not write out with
        (`canon.WIDE_MARK`, `canon.uncomparable`): code point 31."""
        return "CHR(31)"

    def wide(self, c, text, blob=False):
        """`text`, the column's own rendering, except for a value wider
        than `canon.WIDE` bytes: its length and md5, hashed where it lies,
        as `canon.digest_value` writes them."""
        rest = canon.WIDE_MARK[1:]
        if canon.WIDE_MARK[0] != chr(31) or "'" in rest:
            raise ValueError(f"canon's wide mark {canon.WIDE_MARK!r} is not"
                             " one this SQL writes")
        n = self.octets(c, blob)
        said = self.concat([self.unit_separator(), self.literal(rest),
                            self.text(n), "':'", self.md5_hex(c)])
        return f"CASE WHEN {n} > {canon.WIDE} THEN {said} ELSE {text} END"

    def row_text(self, parts):
        """`rowtext.encode` in this SQL."""
        null, sep = rowtext.NULL_LEN, rowtext.SEP
        pieces = []
        for e in parts:
            if pieces:
                pieces.append(f"'{sep}'")
            pieces.append("(" + self.concat([
                f"COALESCE({self.text(self.length(e))}, '{null}')", "':'",
                f"COALESCE({e}, '')"]) + ")")
        return self.concat(pieces)

    def hash_hex(self, text, name):
        got = self.md5_hex(text) if name == "md5" else \
            self.sha256_hex(text)
        if got is None:
            raise ValueError(f"no {name} in {type(self).__name__}")
        return got


#: the dialects engines speak from their own modules (`speak`), by name
SPOKEN = {}


def speak(name, dialect):
    """Register `dialect` (a `Dialect`) as the SQL `name` is written in:
    an engine's module says it once, where it is loaded."""
    SPOKEN[name] = dialect
    return dialect


def canon_hash():
    """"md5" or "sha256": the hash `canon.digest_step` folds with, found by
    asking it, or None where it is neither - a salted or otherwise changed
    fold this file does not know, which the server must not imitate."""
    import hashlib
    probe = "migkit\x1fprobe|12:é"
    got = canon.digest_step(0, probe)
    for name in ("md5", "sha256"):
        h = getattr(hashlib, name)(probe.encode(), usedforsecurity=False)
        if int(h.hexdigest()[:canon.DIGEST_HEX], 16) == got:
            return name
    return None


def _base(declared):
    """('NUMERIC', (12, 2)) from 'NUMERIC(12, 2)'."""
    text = str(declared or "").strip()
    return text.split("(")[0].strip().upper(), canon.params(text)


def _scale(declared):
    nums = _base(declared)[1]
    return nums[1] if len(nums) == 2 else None


def _no_zeros(dialect, text):
    """A decimal's text without the zeros after its last digit, nor a point
    left bare: the value's own digits, as `canon.NUMBER` writes it."""
    if dialect == "duckdb":
        return (f"CASE WHEN strpos({text}, '.') > 0 THEN"
                f" rtrim(rtrim({text}, '0'), '.') ELSE {text} END")
    return (f"CASE WHEN POSITION('.' IN {text}) > 0 THEN"
            f" RTRIM(RTRIM({text}, '0'), '.') ELSE {text} END")


def _past_micro(dialect, text):
    """A text of nine digits after the second as `canon` writes it: the
    last three only where they are not all zeros (`canon.FRACTION`)."""
    if dialect == "duckdb":
        return (f"CASE WHEN right({text}, 3) = '000' THEN"
                f" left({text}, length({text}) - 3) ELSE {text} END")
    return (f"CASE WHEN RIGHT({text}, 3) = '000' THEN"
            f" LEFT({text}, LENGTH({text}) - 3) ELSE {text} END")


# ---- one column's text ----------------------------------------------------

def _bigquery(c, cls, declared):
    base, _ = _base(declared)
    if cls == "integer":
        return f"CAST({c} AS STRING)"
    if cls == canon.NUMBER:
        # BigQuery prints a NUMERIC with its own digits and no more
        return f"CAST({c} AS STRING)" if base in (
            "NUMERIC", "BIGNUMERIC", "DECIMAL", "BIGDECIMAL") else None
    if cls == "decimal":
        # BigQuery prints a NUMERIC without trailing zeros; the value read
        # through its driver is held at the declared scale (as the other
        # engines print it), so the text is padded to it here
        return _padded("bigquery", f"CAST({c} AS STRING)", _scale(declared))
    if cls == "float":
        return _float("bigquery", c)
    if cls == "boolean":
        return f"CASE WHEN {c} IS NULL THEN NULL WHEN {c} THEN '1' ELSE '0' END"
    if cls == "text":
        return _wide("bigquery", c, c) if base in ("STRING",) else None
    if cls == "bytes":
        return _wide("bigquery", c, f"UPPER(TO_HEX({c}))")
    if cls == "date":
        return f"FORMAT_DATE('%Y-%m-%d', {c})"
    if cls == "timestamp":
        if base == "TIMESTAMP":
            return f"FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', {c}, 'UTC')"
        return f"FORMAT_DATETIME('%Y-%m-%d %H:%M:%E6S', {c})"
    if cls == "time":
        return f"FORMAT_TIME('%H:%M:%E6S', {c})"
    return None


def _snowflake(c, cls, declared):
    base, nums = _base(declared)
    if cls == "integer":
        return f"TO_VARCHAR({c})"
    if cls in ("decimal", canon.NUMBER):
        if base not in ("NUMBER", "DECIMAL", "NUMERIC"):
            return None
        # a NUMBER(p, s) prints its s places, as the driver's value is held
        text = f"TO_VARCHAR({c})"
        return text if cls == "decimal" else _no_zeros("snowflake", text)
    if cls == "boolean":
        return f"CASE WHEN {c} IS NULL THEN NULL WHEN {c} THEN '1' ELSE '0' END"
    if cls == "text":
        return (_wide("snowflake", c, c)
                if base in ("TEXT", "VARCHAR", "STRING") else None)
    if cls == "bytes":
        # Snowflake hashes a string, not a binary's own bytes: a column
        # that can hold a value wider than `canon.WIDE` (a BINARY is 8 MB
        # unless declared less) is folded here instead
        return (f"TO_VARCHAR({c}, 'HEX')"
                if _narrow(declared, 8 * 2 ** 20) else None)
    if cls == "date":
        return f"TO_VARCHAR({c}, 'YYYY-MM-DD')"
    # a time kept to nine digits (Snowflake's own, unless declared fewer)
    # is read with all nine, and written with them where they are not
    # zeros; one declared to six at most is read at six
    nine = (nums[0] if nums else 9) > 6
    if cls == "timestamp":
        if base in ("TIMESTAMP_LTZ", "TIMESTAMP_TZ"):
            c = f"CONVERT_TIMEZONE('UTC', {c})::TIMESTAMP_NTZ"
        if nine:
            return _past_micro("snowflake", f"TO_VARCHAR({c},"
                                            " 'YYYY-MM-DD HH24:MI:SS.FF9')")
        return f"TO_VARCHAR({c}, 'YYYY-MM-DD HH24:MI:SS.FF6')"
    if cls == "time":
        if nine:
            return _past_micro("snowflake",
                               f"TO_VARCHAR({c}, 'HH24:MI:SS.FF9')")
        return f"TO_VARCHAR({c}, 'HH24:MI:SS.FF6')"
    return None


def _redshift(c, cls, declared):
    base, _ = _base(declared)
    if cls in ("integer", "decimal"):
        return f"CAST({c} AS VARCHAR)"
    if cls == canon.NUMBER:
        return _no_zeros("redshift", f"CAST({c} AS VARCHAR)")
    if cls == "boolean":
        return f"CASE WHEN {c} IS NULL THEN NULL WHEN {c} THEN '1' ELSE '0' END"
    if cls == "text":
        # a CHAR is padded, and what its text is depends on the reader
        return c if base in ("CHARACTER VARYING", "VARCHAR", "TEXT",
                             "NVARCHAR") else None
    if cls == "bytes":
        # as Snowflake's: a VARBYTE declared past `canon.WIDE` (it holds up
        # to 16 MB; 64,000 bytes where no size is declared) is folded here.
        # A VARCHAR holds 65,535 bytes at most, never a wide value
        return (f"UPPER(TO_HEX({c}))" if _narrow(declared, 64000)
                else None)
    if cls == "date":
        return f"TO_CHAR({c}, 'YYYY-MM-DD')"
    if cls == "timestamp":
        # an instant as UTC shows it: the session is set to UTC
        return f"TO_CHAR({c}, 'YYYY-MM-DD HH24:MI:SS.US')"
    if cls == "time":
        if "ZONE" in base and "WITHOUT" not in base:
            return None
        return f"TO_CHAR(DATE '2000-01-01' + {c}, 'HH24:MI:SS.US')"
    return None


def _duckdb(c, cls, declared):
    base, _ = _base(declared)
    if cls in ("integer", "decimal"):
        return f"CAST({c} AS VARCHAR)"
    if cls == canon.NUMBER:
        return _no_zeros("duckdb", f"CAST({c} AS VARCHAR)")
    if cls == "float":
        return _float("duckdb", c)
    if cls == "boolean":
        return f"CASE WHEN {c} IS NULL THEN NULL WHEN {c} THEN '1' ELSE '0' END"
    if cls == "text":
        return (_wide("duckdb", c, c)
                if base in ("VARCHAR", "STRING", "TEXT") else None)
    if cls == "uuid":
        # its text in small letters with its four hyphens, as `canon`
        # writes one
        return f"CAST({c} AS VARCHAR)" if base == "UUID" else None
    if cls == "bytes":
        return _wide("duckdb", c, f"upper(hex({c}))", blob=True)
    if cls == "date":
        return f"strftime({c}, '%Y-%m-%d')"
    if cls == "timestamp":
        # an instant as UTC shows it: the connection's zone is UTC. A
        # nanosecond one (DuckDB's TIMESTAMP_NS, Iceberg's timestamp_ns)
        # with the three digits past the sixth where it has any
        if base.endswith("_NS"):
            return _past_micro("duckdb",
                               f"strftime({c}, '%Y-%m-%d %H:%M:%S.%n')")
        return f"strftime({c}, '%Y-%m-%d %H:%M:%S.%f')"
    if cls == "time":
        if base.endswith("_NS"):
            return None
        return f"strftime(DATE '2000-01-01' + {c}, '%H:%M:%S.%f')"
    return None


def _spanner(c, cls, declared):
    base, _ = _base(declared)
    if cls == "integer":
        # an ENUM's text is its name, not its number
        return f"CAST({c} AS STRING)" if base == "INT64" else None
    if cls == canon.NUMBER:
        # a NUMERIC printed with its own digits and no more
        return f"CAST({c} AS STRING)" if base == "NUMERIC" else None
    if cls == "boolean":
        return f"CASE WHEN {c} IS NULL THEN NULL WHEN {c} THEN '1' ELSE '0' END"
    if cls == "text":
        return c if base == "STRING" else None
    if cls == "bytes":
        return f"UPPER(TO_HEX({c}))" if base == "BYTES" else None
    if cls == "date":
        return f"FORMAT_DATE('%Y-%m-%d', {c})"
    if cls == "timestamp":
        # nine digits, the last three only where they are not all zeros
        # (`canon.FRACTION`): at six, a copy that lost them compared equal
        text = f"FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E9S', {c}, 'UTC')"
        return (f"CASE WHEN ENDS_WITH({text}, '000') THEN SUBSTR({text}, 1,"
                f" LENGTH({text}) - 3) ELSE {text} END")
    return None


_COLUMN = {"bigquery": _bigquery, "snowflake": _snowflake,
           "redshift": _redshift, "duckdb": _duckdb, "spanner": _spanner}


def _wide(dialect, c, text, blob=False):
    """`text`, the column's own rendering, except for a value wider than
    `canon.WIDE` bytes: its length and md5 as `canon.digest_value` writes
    them, hashed where it lies - the text every other engine and this
    process fold for it, never the value written out in full."""
    rest = canon.WIDE_MARK[1:]
    if canon.WIDE_MARK[0] != chr(31) or "'" in rest:
        raise ValueError(f"canon's wide mark {canon.WIDE_MARK!r} is not one"
                         " this SQL writes")
    if dialect == "duckdb":
        n = f"octet_length({c})" if blob else f"strlen({c})"
        said = (f"chr(31) || '{rest}' || CAST({n} AS VARCHAR) || ':' ||"
                f" md5({c})")
    elif dialect == "bigquery":
        n = f"BYTE_LENGTH({c})"
        said = (f"CONCAT(CODE_POINTS_TO_STRING([31]), '{rest}',"
                f" CAST({n} AS STRING), ':', TO_HEX(MD5({c})))")
    elif dialect == "snowflake":
        n = f"OCTET_LENGTH({c})"
        said = (f"CHR(31) || '{rest}' || TO_VARCHAR({n}) || ':' ||"
                f" MD5({c})")
    else:
        raise ValueError(f"no wide rendering for {dialect}")
    return f"CASE WHEN {n} > {canon.WIDE} THEN {said} ELSE {text} END"


def _narrow(declared, default):
    """Whether every value of a byte column of this declared type is
    within `canon.WIDE`: its declared length, or the dialect's `default`
    where it declares none."""
    nums = _base(declared)[1]
    most = int(nums[0]) if nums else int(default)
    return 0 < most <= canon.WIDE


def _padded(dialect, text, scale):
    """A decimal's text with `scale` places, as `canon.render_value` writes
    a value held at that scale. Nothing to add where the scale is not
    known, or is 0."""
    if not scale:
        return text
    if dialect == "bigquery":
        return (f"CASE WHEN STRPOS({text}, '.') = 0 THEN CONCAT({text}, '.',"
                f" REPEAT('0', {int(scale)})) ELSE RPAD({text},"
                f" STRPOS({text}, '.') + {int(scale)}, '0') END")
    raise ValueError(f"no padding for {dialect}")


def _unrendered_float(dialect, c):
    """(the test, the text) for a NaN or an infinity: `canon.uncomparable`'s
    marker with the word for which it is, as `canon` writes it."""
    words = (("NaN", f"isnan({c})" if dialect == "duckdb"
              else f"IS_NAN({c})"),
             ("Infinity", f"{c} = CAST('inf' AS {_DOUBLE[dialect]})"),
             ("-Infinity", f"{c} = CAST('-inf' AS {_DOUBLE[dialect]})"))
    whens = []
    for word, test in words:
        text = canon.uncomparable(word)
        if not text.startswith("\x1f") or "'" in text:
            raise ValueError(f"canon's marker {text!r} is not one this"
                             " SQL writes")
        lit =(f"chr(31) || '{text[1:]}'" if dialect == "duckdb" else
               f"CONCAT(CODE_POINTS_TO_STRING([31]), '{text[1:]}')")
        whens.append(f"WHEN {test} THEN {lit}")
    either = (f"isnan({c}) OR isinf({c})" if dialect == "duckdb"
              else f"IS_NAN({c}) OR IS_INF({c})")
    return either, "CASE " + " ".join(whens) + " END"


_DOUBLE = {"duckdb": "DOUBLE", "bigquery": "FLOAT64"}


def _float(dialect, c):
    """`canon._float_text` in SQL: NaN and the infinities marked
    uncomparable; zero, then a fixed twenty places between 1e-20 and 1e45
    taken from the shortest text that reads back as the same double (so
    0.1 is 0.1, not the binary value's tail), rounded half to even as
    Python's decimal rounds; the shortest text itself outside that, with
    its exponent's `+` dropped.

    Below 1e-4 the shortest text has more than twenty places, and the
    rounding is done on the text: a value exactly halfway rounds to the
    even digit, where the engines' casts round it up. At 1e18 and above it
    is a whole number longer than their decimals hold, and is written out
    from the exponent form."""
    zeros = _ZEROS
    if dialect == "duckdb":
        txt = f"CAST({c} AS VARCHAR)"
        bad, mark = _unrendered_float(dialect, c)
        wide = f"CAST({txt} AS DECIMAL(38,37))"
        wide_txt = f"CAST({wide} AS VARCHAR)"
        cut = f"CAST(CAST({wide} AS DECIMAL(38,20)) AS VARCHAR)"
        mid = f"CAST(CAST({txt} AS DECIMAL(38,20)) AS VARCHAR)"
        frac = f"split_part({wide_txt}, '.', 2)"
        whole = f"split_part({wide_txt}, '.', 1)"
        tie = (f"regexp_matches(substr({frac}, 21), '^50*$') AND"
               f" CAST(substr({frac}, 20, 1) AS INTEGER) % 2 = 0")
        parts = f"regexp_extract({txt}, '^(-?)([0-9])(?:[.]([0-9]+))?e[+]([0-9]+)$', ['s', 'd', 'f', 'e'])"
        big = (f"{parts}['s'] || {parts}['d'] || rpad(coalesce({parts}['f'],"
               f" ''), CAST({parts}['e'] AS INTEGER), '0') || '.{zeros}'")
        outer = f"replace({txt}, 'e+', 'e')"
        return (f"CASE WHEN {c} IS NULL THEN NULL WHEN {bad} THEN {mark}"
                f" WHEN {c} = 0 THEN '0.{zeros}'"
                f" WHEN abs({c}) >= {canon.FLOAT_MAX} OR abs({c}) <"
                f" {canon.FLOAT_MIN} THEN {outer}"
                f" WHEN abs({c}) < 1e-4 THEN CASE WHEN {tie} THEN {whole}"
                f" || '.' || substr({frac}, 1, 20) ELSE {cut} END"
                f" WHEN abs({c}) < 1e18 THEN {mid}"
                f" ELSE {big} END")
    if dialect == "bigquery":
        txt = f"CAST({c} AS STRING)"
        bad, mark = _unrendered_float(dialect, c)
        num = f"CAST({txt} AS BIGNUMERIC)"
        mid = _padded("bigquery", f"CAST(ROUND({num}, 20, 'ROUND_HALF_EVEN')"
                                  " AS STRING)", canon.FLOAT_SCALE)
        rx = r"^(-?)([0-9])(?:[.]([0-9]+))?e[+]([0-9]+)$"
        big = (f"CONCAT(REGEXP_REPLACE({txt}, r'{rx}', r'\\1\\2'),"
               f" RPAD(REGEXP_REPLACE({txt}, r'{rx}', r'\\3'),"
               f" CAST(REGEXP_REPLACE({txt}, r'{rx}', r'\\4') AS INT64),"
               f" '0'), '.{zeros}')")
        outer = f"REPLACE({txt}, 'e+', 'e')"
        return (f"CASE WHEN {c} IS NULL THEN NULL WHEN {bad} THEN {mark}"
                f" WHEN {c} = 0 THEN '0.{zeros}'"
                f" WHEN ABS({c}) >= {canon.FLOAT_MAX} OR ABS({c}) <"
                f" {canon.FLOAT_MIN} THEN {outer}"
                f" WHEN ABS({c}) < 1e18 THEN {mid}"
                f" ELSE {big} END")
    return None


def column_text(dialect, c, cls, declared):
    """SQL giving the canonical text of column `c` (already quoted) of this
    class and declared type, NULL where it is NULL - or None where this
    dialect's text of it is not known to be migkit's, and the table is
    folded in this process instead."""
    spoken = SPOKEN.get(dialect)
    if spoken is not None:
        return spoken.column(c, cls, declared) if cls in spoken.rendered \
            else None
    if cls not in RENDERED.get(dialect, ()):
        return None
    return _COLUMN[dialect](c, cls, declared)


# ---- a row, its hash, the sum ---------------------------------------------

def row_text(dialect, parts):
    """`rowtext.encode` in SQL: each value's length (or `N` for NULL), a
    colon, the value; joined by the separator."""
    if dialect in SPOKEN:
        return SPOKEN[dialect].row_text(parts)
    null, sep = rowtext.NULL_LEN, rowtext.SEP
    if dialect in ("bigquery", "spanner"):
        frags = [f"COALESCE(CAST(LENGTH({e}) AS STRING), '{null}'), ':',"
                 f" COALESCE({e}, '')" for e in parts]
        return "CONCAT(" + f", '{sep}', ".join(frags) + ")"
    if dialect == "snowflake":
        frags = [f"COALESCE(TO_VARCHAR(LENGTH({e})), '{null}') || ':' ||"
                 f" COALESCE({e}, '')" for e in parts]
    elif dialect == "redshift":
        frags = [f"COALESCE(CAST(LENGTH({e}) AS VARCHAR), '{null}') || ':'"
                 f" || COALESCE({e}, '')" for e in parts]
    elif dialect == "duckdb":
        frags = [f"coalesce(CAST(length({e}) AS VARCHAR), '{null}') || ':'"
                 f" || coalesce({e}, '')" for e in parts]
    else:
        raise ValueError(f"no row text for {dialect}")
    return f" || '{sep}' || ".join(f"({f})" for f in frags)


def _hash_hex(dialect, text, name):
    if dialect in SPOKEN:
        return SPOKEN[dialect].hash_hex(text, name)
    if dialect in ("bigquery", "spanner"):
        return f"TO_HEX({'MD5' if name == 'md5' else 'SHA256'}({text}))"
    if dialect == "snowflake":
        return f"MD5({text})" if name == "md5" else f"SHA2({text}, 256)"
    if dialect == "redshift":
        return f"MD5({text})" if name == "md5" else f"SHA2({text}, 256)"
    if dialect == "duckdb":
        return f"md5({text})" if name == "md5" else f"sha256({text})"
    raise ValueError(dialect)


def _prefix_int(dialect, hexed):
    n = canon.DIGEST_HEX
    if dialect in SPOKEN:
        return SPOKEN[dialect].from_hex(hexed, n)
    if dialect in ("bigquery", "spanner"):
        return f"CAST(CONCAT('0x', SUBSTR({hexed}, 1, {n})) AS INT64)"
    if dialect == "snowflake":
        return f"TO_NUMBER(UPPER(SUBSTR({hexed}, 1, {n})), '{'X' * n}')"
    if dialect == "redshift":
        return f"STRTOL(SUBSTRING({hexed}, 1, {n}), 16)"
    if dialect == "duckdb":
        return f"CAST('0x' || substr({hexed}, 1, {n}) AS BIGINT)"
    raise ValueError(dialect)


def _summed(dialect, value):
    if dialect in SPOKEN:
        return SPOKEN[dialect].summed(value)
    if dialect == "bigquery":
        return f"CAST(COALESCE(SUM(CAST({value} AS BIGNUMERIC)), 0) AS STRING)"
    if dialect == "spanner":
        # a NUMERIC holds 29 digits before its point: 2**60 a row over
        # 10**10 rows stays below that
        return f"CAST(COALESCE(SUM(CAST({value} AS NUMERIC)), 0) AS STRING)"
    if dialect == "snowflake":
        return f"TO_VARCHAR(COALESCE(SUM({value}), 0))"
    if dialect == "redshift":
        return (f"CAST(COALESCE(SUM(CAST({value} AS DECIMAL(38,0))), 0)"
                " AS VARCHAR)")
    if dialect == "duckdb":
        return f"CAST(coalesce(sum(CAST({value} AS HUGEINT)), 0) AS VARCHAR)"
    raise ValueError(dialect)


def row_expr(dialect, columns, quote):
    """The row's canonical text for `[(name, class, declared)]`, or None
    where a column's text is not rendered here."""
    parts = []
    for name, cls, declared in columns:
        got = column_text(dialect, quote(name), cls, declared)
        if got is None:
            return None
        parts.append(got)
    return row_text(dialect, parts) if parts else None


def digest_select(dialect, columns, quote):
    """`count, digest` as a select list over `[(name, class, declared)]`,
    or None where the server cannot give canon's number for these."""
    name = canon_hash()
    row = row_expr(dialect, columns, quote)
    if name is None or row is None:
        return None
    one = _prefix_int(dialect, _hash_hex(dialect, row, name))
    return f"COUNT(*), {_summed(dialect, one)}"


def unrendered(dialect, columns):
    """The columns of `[(name, class, declared)]` this dialect does not
    render, as `name (declared)`, for saying why a table was folded here."""
    return [f"{n} ({d})" for n, c, d in columns
            if column_text(dialect, "x", c, d) is None]


def agree(cols, values, texts):
    """Whether each row of values, rendered in this process as a digest
    renders it (`canon.digest_value`: a wide value as its length and md5),
    is the text the server rendered for it."""
    return all(rowtext.encode([canon.digest_value(c, v) for (_, c, _), v
                               in zip(cols, row)]) == text
               for row, text in zip(values, texts))


# ---- a table's leaves (`leaves`), in the same SQL ------------------------

def salted_hash(dialect, text, salt):
    """SQL for `leaves.text_hash`: the first `canon.DIGEST_HEX` hex digits
    of md5(salt || text), as an integer."""
    lit = "'" + str(salt).replace("'", "''") + "'"
    if dialect in SPOKEN:
        joined = SPOKEN[dialect].concat([lit, text])
    elif dialect in ("bigquery", "spanner"):
        joined = f"CONCAT({lit}, {text})"
    else:
        joined = f"{lit} || {text}"
    return _prefix_int(dialect, _hash_hex(dialect, joined, "md5"))


def exact_sum(dialect, value):
    """A sum of 60-bit hashes that does not wrap: BigQuery's INT64 and
    Redshift's BIGINT sum would."""
    if dialect in SPOKEN:
        return SPOKEN[dialect].exact_sum(value)
    if dialect == "bigquery":
        return f"SUM(CAST({value} AS BIGNUMERIC))"
    if dialect == "spanner":
        return f"SUM(CAST({value} AS NUMERIC))"
    if dialect == "redshift":
        return f"SUM(CAST({value} AS DECIMAL(38,0)))"
    return f"SUM({value})"


def shifted(dialect, value, bits):
    """`value >> bits`, a non-negative integer's top bits."""
    if dialect in SPOKEN:
        return SPOKEN[dialect].shifted(value, bits)
    if dialect == "snowflake":
        return f"BITSHIFTRIGHT({value}, {int(bits)})"
    return f"({value} >> {int(bits)})"


def divided(dialect, value, by):
    """A non-negative integer divided by another, the fraction dropped."""
    if dialect in SPOKEN:
        return SPOKEN[dialect].divided(value, by)
    if dialect in ("bigquery", "spanner"):
        return f"DIV({value}, {by})"
    if dialect == "snowflake":
        return f"TRUNC(({value}) / ({by}))"
    if dialect == "duckdb":
        return f"(({value}) // ({by}))"
    return f"(({value}) / ({by}))"


class DigestsInSQL:
    """A DB-API engine whose digest is `canon.fold_rows`'s number computed
    in its own SQL (`DIGEST_DIALECT`): the warehouses, and DuckDB. Before
    that SQL is trusted on a table, the table's first rows are read as the
    engine reads them, with the server's text of each beside them, and the
    two renderings held to each other; a table whose columns the dialect
    does not render, whose rows it renders differently, or whose SQL the
    server refuses, is folded here instead - the same number the slow way,
    never a different one. The leaves of a table's digest (`leaves`) are
    computed in the same SQL, on the same proof."""
    #: the dialect the digest is written in
    DIGEST_DIALECT = None
    #: a digest of a key range is the server's to answer (`hetero`)
    DIGESTS_IN_SERVER = True
    #: rows rendered both ways before the server's digest is trusted
    PROOF_ROWS = 64
    #: whether the last digest asked was folded here, for the report's line
    #: on which side's rows crossed the network
    folded_last = True
    #: where a table's digest is worked out (`neutral_digest`)
    DIGEST_WAYS = (
        Rung("server", "the digest worked out by the server in its own SQL",
             needs=(Need("every column rendered in its SQL",
                         lambda f: f["pick"] is not None,
                         "a column has no rendering in the server's SQL"),),
             prove=lambda f: None if f["proved"] else
             "the server does not render the table's first rows as migkit"
             " does"),
        Rung("here", "the rows read and folded here"))

    def _fix_read(self, side, db, table, columns, rows):
        """Values as this engine's reads hand them on (a decimal at its
        declared scale): what the in-process rendering is held to."""
        return rows

    def _declared_cols(self, side, db, table, columns):
        declared = dict(self.neutral_columns(side, db, table))
        return [(n, c, declared.get(n)) for n, c in columns]

    def _proved(self, side, db, table, cols):
        """Whether the server renders this table's first rows as this
        process does, asked once a table and set of columns."""
        known = self.__dict__.setdefault("_proofs", {})
        at = (side, db, str(table), tuple(cols))
        if at in known:
            return known[at]
        text = row_expr(self.DIGEST_DIALECT, cols, self._q)
        names = [n for n, _, _ in cols]
        # read as the engine's reads read them: a time of nine digits by
        # its text, then held as the value it is
        wide = self._wide_times(side, db, table)
        try:
            rows = self._rows(side, db, self._select(
                ", ".join(wide.get(n, self._q(n)) for n in names)
                + f", {text}", self._qualified(side, db, table), "", "",
                self.PROOF_ROWS))
        except Exception:  # noqa: BLE001 - its SQL is refused: folded here
            known[at] = False
            return False
        values = self._fix_read(side, db, table, [(n, c) for n, c, _ in cols],
                                self._whole_times(side, db, table, names,
                                                  [list(r[:-1])
                                                   for r in rows]))
        ok = agree(cols, values, [r[-1] for r in rows])
        if rows:
            # an empty table proves nothing, and is asked again once it
            # holds rows - a copy digests its target from its first batch
            known[at] = ok
        return ok

    def neutral_digest(self, side, db, table, columns, where=None):
        """(rows, digest) computed by the server where every column is
        rendered in its SQL and the rendering is proved on the table's
        first rows; folded here otherwise, over rows read in batches."""
        from .. import decide
        cols = self._declared_cols(side, db, table, columns)
        pick = (digest_select(self.DIGEST_DIALECT, cols, self._q)
                if cols else None)
        folded = super().neutral_digest

        def run(choice):
            if choice.path == "here":
                self.folded_last = True
                return folded(side, db, table, columns, where)
            # refused on a later row than the proof read: folded here, the
            # same number, switched to by itself and said
            got = self._rows(side, db, f"select {pick} from"
                                       f" {self._qualified(side, db, table)}"
                             + (f" where ({where})" if where else ""))
            if not got:
                raise decide.Blocked("the server answered no digest")
            self.folded_last = False
            return int(got[0][0]), str(got[0][1])
        return self._attempt(
            str(table), self.DIGEST_WAYS, run, decide.Facts(
                {"pick": pick},
                proved=lambda: self._proved(side, db, table, cols)))[1]

    # ---- what `leaves` asks of a side it reads in SQL ----------------------

    def leaf_ready(self, side, db, table, cols):
        """Whether the leaves of this table are computed in the server: every
        column rendered in its SQL, and the rendering proved on the table's
        first rows."""
        got = self._declared_cols(side, db, table, cols)
        return (row_expr(self.DIGEST_DIALECT, got, self._q) is not None
                and self._proved(side, db, table, got))

    def leaf_row(self, side, db, table, cols):
        """The canonical text of `[(name, class)]` of a row, in SQL."""
        return row_expr(self.DIGEST_DIALECT,
                        self._declared_cols(side, db, table, cols), self._q)

    def leaf_hash(self, expr, salt):
        return salted_hash(self.DIGEST_DIALECT, expr, salt)

    def leaf_sum(self, value):
        return exact_sum(self.DIGEST_DIALECT, value)

    def leaf_shift(self, value, bits):
        return shifted(self.DIGEST_DIALECT, value, bits)

    def leaf_div(self, value, by):
        return divided(self.DIGEST_DIALECT, value, by)

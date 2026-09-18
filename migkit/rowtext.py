"""How a row becomes one string before it is hashed.

A checksum is only as good as the text it is computed over, and the obvious
way to build that text is wrong. Joining column values with a separator is
ambiguous the moment a value contains the separator:

    ('x#y', 'z')  ->  "x#y#z"
    ('x', 'y#z')  ->  "x#y#z"

Measured on MySQL 8, those two rows produced the identical CRC32 3898531935,
and a source holding the first against a target holding the second was
reported by migkit as `rows 2==2, checksum 810ced44==810ced44` - identical.
A verifier that certifies a difference as equality has failed at the only job
it has, and `#` is not an exotic character: hex colours, flat numbers, issue
references and hashtags all carry it.

The same applies to standing in for NULL with a literal. `~null~` was the
token, so a genuine NULL and the four-character string `~null~` also collided.

The rule this module exists to hold: **the encoding must be injective**.
Distinct tuples must produce distinct strings, with no assumption about what
the data does or does not contain. That is achieved by writing each value's
length in front of it, which makes the string parseable in exactly one way -
so a separator inside a value cannot be mistaken for a separator, and no
literal can impersonate NULL because NULL is marked by a length that is not a
number.

PostgreSQL reaches the same guarantee by a different route and was measured to
already have it: `ROW(a, b)::text` quotes and escapes, rendering the pair above
as `(x#y,z)` and `(x,y#z)`, and a NULL as an empty field distinct from a
quoted empty string. It does not need length prefixes for the row hash - but
its key hash concatenates with `chr(2)` and stands in for NULL with `chr(1)`,
which has the same defect in principle, so that path uses this encoding too.

The constants live here rather than in either engine because the two used to
disagree: the MySQL engine alone had three different separators (`#`, `\\x02`
and a tab) for three different hashes.
"""

# Any separator is safe once lengths are present; a printable one keeps the
# generated SQL readable when someone prints it during an incident.
SEP = "|"
# Marks a NULL. Not a number, so no length can collide with it.
NULL_LEN = "N"


def fragment(text_expr, length_expr):
    """SQL for one column: its length (or the NULL marker), then its value.

    `text_expr` renders the column as text; `length_expr` gives that text's
    length. Both are engine-specific, which is why they are passed in - the
    encoding is not.
    """
    return (f"concat(ifnull(cast({length_expr} as char), '{NULL_LEN}'),"
            f" ':', ifnull({text_expr}, ''))")


def join(fragments):
    """Combine per-column fragments into the string that gets hashed."""
    return f"concat_ws('{SEP}', " + ", ".join(fragments) + ")"


def mysql_row(columns, quote='`'):
    """The injective row string for MySQL, given column names."""
    return join([
        fragment(f"cast({quote}{c}{quote} as char)",
                 f"char_length(cast({quote}{c}{quote} as char))")
        for c in columns
    ])


def postgres_row(columns, alias="t"):
    """The injective key string for PostgreSQL, given column names.

    Used for the key hash and the per-column fingerprints, not for the
    whole-row hash: that goes through `ROW(...)::text`, which PostgreSQL was
    measured to render unambiguously already.

    `alias` is the table alias the columns hang off, or "" where the query
    does not use one.
    """
    q = f'{alias}."{{}}"' if alias else '"{}"'
    parts = [
        f"""coalesce(length({q.format(c)}::text)::text, '{NULL_LEN}')"""
        f""" || ':' || coalesce({q.format(c)}::text, '')"""
        for c in columns
    ]
    return (" || '" + SEP + "' || ").join(parts)


def parse(encoded):
    """Values back out of an encoded string. The inverse of `join`.

    Lives beside the encoder on purpose: the drilldown builds a key from
    several primary-key columns and then has to take it apart again to write
    a WHERE clause. When those two lived apart, the taking-apart was a split
    on a tab - which a tab inside a key value quietly broke.
    """
    out, i, n = [], 0, len(encoded)
    while i <= n:
        mark = encoded.find(":", i)
        if mark < 0:
            raise ValueError(f"not an encoded row: {encoded[:60]!r}")
        head = encoded[i:mark]
        if head == NULL_LEN:
            out.append(None)
            i = mark + 1
        else:
            try:
                ln = int(head)
            except ValueError:
                raise ValueError(f"bad length {head!r} in {encoded[:60]!r}")
            end = mark + 1 + ln
            # Python slicing shortens silently, which would turn a truncated
            # string into a plausible-looking value instead of an error
            if ln < 0 or end > n:
                raise ValueError(f"length {ln} runs past the end of"
                                 f" {encoded[:60]!r}")
            out.append(encoded[mark + 1:end])
            i = end
        if i >= n:
            break
        if encoded[i] != SEP:
            raise ValueError(f"expected {SEP!r} at {i} in {encoded[:60]!r}")
        i += 1
    return out

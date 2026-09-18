"""Reading what a PostgreSQL logical slot says, when it says it in text.

PostgreSQL ships two output plugins and neither is comfortable. `pgoutput` is
the one replication actually uses and it speaks a binary protocol that needs
its own decoder. `test_decoding` is readable over an ordinary connection
through `pg_logical_slot_get_changes`, and is named after what its authors
intended it for. `wal2json`, which emits JSON and needs no parsing at all, is
not in a stock server - measured, a `postgres:16` image carries exactly
`pgoutput.so` and `test_decoding.so`.

So this file parses `test_decoding`, and does it properly rather than with a
split on spaces, because the format has three traps that a quick parse walks
straight into. All three were read off a running server:

    table public.t: INSERT: id[bigint]:9
      name[character varying]:'has '' quote and : colon and [bracket]'
      amount[numeric]:1.0000 b[bytea]:null flag[boolean]:false

1. A quoted value can contain the separator, the colon, and the brackets that
   delimit the type. Splitting on any of them tears the value apart.
2. `''` inside quotes is one apostrophe, and the value ends at the quote that
   is *not* doubled.
3. `null` unquoted is NULL; `'null'` quoted is the four-letter word. A parse
   that compares the text would confuse a column holding "null" with one
   holding nothing.

And two shapes that are not traps but are easy to miss:

    UPDATE: id[bigint]:3 name[...]:'plain' ...
    UPDATE: old-key: id[bigint]:1 new-tuple: id[bigint]:3 name[...]:'moved'

The first is an UPDATE that did not move the key: there is one tuple and it is
the new one. The second appears when the key changed, or when the table is set
to REPLICA IDENTITY FULL - and then `old-key` is the whole previous row rather
than just its key.

One more, measured and worth knowing: with REPLICA IDENTITY FULL a DELETE's
old tuple **omits the columns that were NULL**. A parser that treated a
missing column as "this column does not exist" would build a key out of
whatever happened to be non-null in that row.
"""

# What the plugin prints for a value that is not there. Unquoted, which is
# what tells it apart from the string.
NULL_TOKEN = "null"

# The markers that split an UPDATE into its two tuples.
OLD_KEY = "old-key:"
NEW_TUPLE = "new-tuple:"


def _unquote(text):
    """A quoted value with its doubled apostrophes collapsed."""
    return text[1:-1].replace("''", "'")


def _scan_fields(body):
    """[(name, declared type, raw value, quoted)] from one tuple's text.

    Hand-written rather than a regular expression: the value is quoted with
    an escape that is itself the quote character, and expressing "ends at the
    first apostrophe that is not doubled" as a regex is where a parser like
    this usually goes wrong quietly.
    """
    out, i, n = [], 0, len(body)
    while i < n:
        while i < n and body[i] == " ":
            i += 1
        if i >= n:
            break
        open_bracket = body.find("[", i)
        if open_bracket < 0:
            raise ValueError(f"no column type in {body[i:i + 60]!r}")
        name = body[i:open_bracket]
        close_bracket = body.find("]", open_bracket)
        if close_bracket < 0:
            raise ValueError(f"unclosed type in {body[i:i + 60]!r}")
        declared = body[open_bracket + 1:close_bracket]
        if close_bracket + 1 >= n or body[close_bracket + 1] != ":":
            raise ValueError(f"no value after {name}[{declared}]")
        i = close_bracket + 2
        if i < n and body[i] == "'":
            j = i + 1
            while j < n:
                if body[j] == "'":
                    if j + 1 < n and body[j + 1] == "'":
                        j += 2
                        continue
                    break
                j += 1
            if j >= n:
                raise ValueError(f"unterminated value for {name}")
            out.append((name, declared, _unquote(body[i:j + 1]), True))
            i = j + 1
        else:
            j = body.find(" ", i)
            if j < 0:
                j = n
            out.append((name, declared, body[i:j], False))
            i = j
    return out


def parse_line(line):
    """One decoded line as a dict, or None for a line that is not a change.

    `BEGIN`, `COMMIT` and the plugin's other bookkeeping come back as None:
    they are real output, not errors, and a transaction boundary carries
    nothing migkit applies.

    The dict is deliberately not a change record yet - it still holds the
    PostgreSQL type names, which is what turns the text back into values.
    """
    line = (line or "").strip()
    if not line or not line.startswith("table "):
        return None
    head, sep, rest = line[len("table "):].partition(": ")
    if not sep:
        raise ValueError(f"no table name in {line[:80]!r}")
    op, sep, body = rest.partition(": ")
    if not sep:
        raise ValueError(f"no operation in {line[:80]!r}")
    op = op.strip().lower()
    if op not in ("insert", "update", "delete"):
        raise ValueError(f"unknown operation {op!r} in {line[:80]!r}")

    old, new = [], []
    if body.startswith(OLD_KEY):
        body = body[len(OLD_KEY):]
        marker = body.find(" " + NEW_TUPLE + " ")
        if marker < 0:
            raise ValueError(f"{OLD_KEY} with no {NEW_TUPLE} in"
                             f" {line[:80]!r}")
        old = _scan_fields(body[:marker])
        new = _scan_fields(body[marker + len(NEW_TUPLE) + 2:])
    elif op == "delete":
        old = _scan_fields(body)
    else:
        new = _scan_fields(body)
    return {"table": head.strip(), "op": op, "old": old, "new": new}


def value(declared, raw, quoted):
    """One field turned back into something a driver can be handed.

    `quoted` is not decoration: it is the only thing separating a NULL from
    the string "null", and the only thing separating the boolean `true` from
    a text column holding the word.
    """
    from . import canon
    if not quoted and raw == NULL_TOKEN:
        return None
    cls = canon.type_class("postgres", declared)
    return canon.from_text(cls, raw) if cls else raw


def change(parsed, keys):
    """A parsed line as a `canon.change` record, given the table's key.

    `keys` comes from the catalogue rather than from the line, because the
    line does not always carry one: an UPDATE that did not move the key has
    no `old-key` section at all, and a DELETE under REPLICA IDENTITY FULL
    drops whatever columns were NULL.
    """
    from . import canon
    table = parsed["table"]
    old = {n: value(t, v, q) for n, t, v, q in parsed["old"]}
    new = {n: value(t, v, q) for n, t, v, q in parsed["new"]}
    if parsed["op"] == "delete":
        source = old or new
    else:
        source = old or new
    missing = [k for k in keys if k not in source]
    if missing:
        raise ValueError(
            f"{table}: the change log did not carry {', '.join(missing)},"
            " so migkit cannot say which row it meant. Set REPLICA IDENTITY"
            f" to a unique index on {table}, or exclude it")
    key = {k: source[k] for k in keys}
    if parsed["op"] == "delete":
        return canon.change("delete", table, key)
    return canon.change(parsed["op"], table, key, new)

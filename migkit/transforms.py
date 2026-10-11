"""A column written other than as it was read, and proved as written
(backlog P0-SCOPE.7: column transforms the check understands).

A move that masks a column - a test copy of production, an archive kept
for analytics - writes a value the source never held, so a check that
compares the two sides value for value calls every such row different,
and one that leaves the column out proves nothing of it. Each transform
here is a function both readers can take: the copy writes `apply(rule,
value)`, and the check asks the same of the source's value and compares
it with the target's, so a masked column is compared, not skipped.

    null              nothing written
    constant <text>   the same text in every row
    hash              a keyed digest of the value (the hop's `mask_key`,
                      a secret reference): the same value gives the same
                      digest on every run and every machine, so a join on
                      it still lines up and a second run writes the same;
                      without the key it would be a dictionary away from
                      the value, and is refused
    truncate <n>      the first n characters

Never on a key (`movers.mask_plan` holds that), never random (nothing
could be proved of it). `sql` says the same function in an engine's own
SQL where the server can compute it - a digest the server takes over the
masked column is then the target's own, read from the source - and every
form of it is held to `apply` on the same values by the tests.
"""
import hashlib

#: the transforms, and whether a check can prove the value written
KINDS = {"null": True, "constant": True, "hash": True, "truncate": True,
         "random": False}


def parse(rule):
    """(kind, argument) of one rule as a hop writes it - `null`,
    `constant <text>`, `hash`, `truncate <n>`; SystemExit for one that
    is none of them or cannot be proved."""
    if isinstance(rule, tuple) and len(rule) == 2:
        rule = f"{rule[0]} {rule[1]}".strip()
    text = "" if rule is None else str(rule).strip()
    kind, _, arg = text.partition(" ")
    if ":" in kind:
        # `constant:text`, as hops written before transforms said it
        kind, _, rest = kind.partition(":")
        arg = (rest + " " + arg).strip()
    kind = kind.lower()
    arg = arg.strip()
    if kind not in KINDS:
        raise SystemExit(f"`{text or '(nothing)'}` is not a transform:"
                         " null, constant <text>, hash or truncate <n>")
    if not KINDS[kind]:
        raise SystemExit("a random value cannot be held to anything after"
                         " the load, and a second run writes another")
    if kind == "constant" and (not arg or "'" in arg or "\n" in arg):
        raise SystemExit("a constant must be text without quotes or line"
                         " breaks")
    if kind == "truncate":
        if not arg.strip().isdigit() or int(arg) < 1:
            raise SystemExit("truncate takes how many characters to keep,"
                             " a whole number above 0")
        return kind, int(arg)
    if kind in ("null", "hash") and arg:
        raise SystemExit(f"{kind} takes nothing after it")
    return kind, arg


def apply(rule, value, key=None):
    """The value the target holds for a source value under `rule` (a
    `parse`d pair or its text). NULL stays NULL under every transform but
    `constant`, which writes its text whatever was there."""
    kind, arg = parse(rule) if isinstance(rule, str) else rule
    if kind == "constant":
        return arg
    if value is None or kind == "null":
        return None
    if isinstance(value, (bytes, bytearray, memoryview)):
        # a text column a driver hands over as its bytes: its text
        value = bytes(value).decode("utf-8")
    if kind == "truncate":
        return str(value)[:arg]
    if not key:
        raise SystemExit("hash needs the hop's mask_key (a secret"
                         " reference): a digest without a key is a"
                         " dictionary away from the value")
    data = (str(key) + str(value)).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _lit(text):
    return "'" + str(text).replace("'", "''") + "'"


#: the engines whose SQL computes `hash` byte for byte as `apply` does:
#: the key and the value's text in UTF-8, SHA-256, lower-case hex
_HASH_SQL = {
    "postgres": "encode(sha256(convert_to({k} || {c}, 'UTF8')), 'hex')",
    # the column's text in UTF-8 whatever its character set: SHA2 hashes
    # the bytes of the string as it holds them
    "mysql": "SHA2(CONCAT({k}, CONVERT({c} USING utf8mb4)), 256)",
    "oracle": ("CASE WHEN {c} IS NULL THEN NULL ELSE"
               " LOWER(RAWTOHEX(STANDARD_HASH({k} || {c}, 'SHA256'))) END"),
}
_LEFT_SQL = {"postgres": "left({c}, {n})", "mysql": "LEFT({c}, {n})",
             "mssql": "LEFT({c}, {n})", "oracle": "SUBSTR({c}, 1, {n})",
             "sqlite": "substr({c}, 1, {n})"}


def sql(rule, column, dialect, key=None):
    """The transform of `column` (already quoted) in the engine's SQL, or
    None where its SQL cannot compute it alike - the value is then taken
    in Python (`apply`) by whichever reader holds it."""
    kind, arg = parse(rule) if isinstance(rule, str) else rule
    if kind == "null":
        # a NULL of the column's own type, which every rendering of the
        # column's class takes as it takes the column
        return f"CASE WHEN 1 = 0 THEN {column} END"
    if kind == "constant":
        return _lit(arg)
    if kind == "truncate":
        form = _LEFT_SQL.get(dialect)
        return form.format(c=column, n=int(arg)) if form else None
    form = _HASH_SQL.get(dialect)
    if not form or not key:
        return None
    return form.format(k=_lit(key), c=column)


def text_only(rule):
    """Whether the transform reads the value as text, so it is taken only
    on a column whose text is the same on every engine: `hash` and
    `truncate` of a timestamp or a decimal would read another text on
    another server."""
    kind, _ = parse(rule) if isinstance(rule, str) else rule
    return kind in ("hash", "truncate")

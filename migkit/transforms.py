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
    fpe [alphabet]    the value encrypted in its own shape (format-
                      preserving encryption, FF1 over AES): digits for
                      digits, letters for letters, the same length, and
                      the same value gives the same one - a key stays
                      unique and a join on it still lines up. The key is
                      the user's own in the state directory
                      (`transform.key`), or derived from the hop's
                      `mask_key` where one is given, which is then the
                      same on every machine. The alphabet is `digits`
                      (the default), `lower`, `upper`, `alpha`, `alnum`,
                      or the characters themselves; a character outside
                      it is refused. Reversed with the key (`reverse`)
    token [domain]    a token that stands for the value, kept in a local
                      vault in the state directory (`token.vault`): the
                      same value gives the same token, the vault holds the
                      value sealed under the key, and only the key's
                      holder gets it back (`reverse`). A domain keeps one
                      kind of value apart from another
    sql <expression>  the column's value through an expression of the
                      analytic SQL the batches pass through (`lower(email)`,
                      `regexp_replace(phone, '[0-9]', '#', 'g')`), naming
                      the column it is written on and nothing else: one
                      value out for one value in, never an aggregate, a
                      window, a query or a clock or random value, so the
                      check runs the same statement over the source and
                      the copy, the tail and a repair write what it says.
                      A batch is taken through one statement over all its
                      columns so written (`apply_rows`), a lone value (a
                      change the tail carries) through the same one; the
                      statement reads no file and no network. Written in
                      the hop as `transform:` (`hopkeys`); where it runs -
                      in flight, on the target after the copy, or refused -
                      is `decide.transform_place`'s (`place`)

Which of mask, fpe and token a column takes is `decide.hide_column`'s, on
the column's facts (`choose`). Never on a key (`movers.mask_plan` holds that), never random (nothing
could be proved of it). `sql` says the same function in an engine's own
SQL where the server can compute it - a digest the server takes over the
masked column is then the target's own, read from the source - and every
form of it is held to `apply` on the same values by the tests.
"""
import hashlib
import hmac
import threading
import time

#: the transforms, and whether a check can prove the value written
KINDS = {"null": True, "constant": True, "hash": True, "truncate": True,
         "fpe": True, "token": True, "sql": True, "random": False}

#: how often a statement ran, by how (`apply_rows` once a batch, `apply`
#: once a lone value): what the tests hold the batch way to
RUNS = {"batch": 0, "row": 0}

#: the alphabets `fpe` names; any other argument is the characters
ALPHABETS = {"digits": "0123456789", "lower": "abcdefghijklmnopqrstuvwxyz",
             "upper": "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
             "alpha": "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
             "alnum": "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                      "abcdefghijklmnopqrstuvwxyz"}

#: FF1 is held to a domain of a million values at least (NIST SP 800-38G);
#: a shorter value is a dictionary away from its plain text
LEAST_DOMAIN = 1_000_000


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
                         " null, constant <text>, hash, truncate <n>,"
                         " fpe [alphabet] or token [domain]")
    if not KINDS[kind]:
        raise SystemExit("a random value cannot be held to anything after"
                         " the load, and a second run writes another")
    if kind == "constant" and (not arg or "'" in arg or "\n" in arg):
        raise SystemExit("a constant must be text without quotes or line"
                         " breaks")
    if kind == "fpe":
        alphabet(arg)
        return kind, arg
    if kind == "token":
        return kind, arg
    if kind == "sql":
        statement(arg)
        return kind, arg
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
    if kind == "sql":
        return _run_sql(arg, [[value]])[0]
    if value is None or kind == "null":
        return None
    if isinstance(value, (bytes, bytearray, memoryview)):
        # a text column a driver hands over as its bytes: its text
        value = bytes(value).decode("utf-8")
    if kind == "truncate":
        return str(value)[:arg]
    if kind == "fpe":
        return fpe_encrypt(arg, value, key)
    if kind == "token":
        return tokenise(arg, value, key)
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
    if kind in ("fpe", "token", "sql"):
        # a cipher and a vault the server does not hold; a statement of
        # the analytic SQL, run over the source as it is over the batches
        return None
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
    return kind in ("hash", "truncate", "fpe", "token")


# --- format-preserving encryption and tokens (backlog W4.b) -----------------

def alphabet(name):
    """The characters an `fpe` argument names: `digits` where none is
    given, a named set, or the characters themselves (two or more, none
    twice)."""
    text = str(name or "").strip() or "digits"
    chars = ALPHABETS.get(text, text)
    if len(chars) < 2 or len(set(chars)) != len(chars):
        raise SystemExit(f"fpe alphabet `{text}` is neither digits, lower,"
                         " upper, alpha, alnum nor two or more different"
                         " characters")
    return chars


def _key(key, purpose):
    """32 bytes for `purpose` from the hop's key where given (the same on
    every machine), else from the user's own in the state directory."""
    from . import masking
    base = str(key).encode() if key else masking.state_key("transform.key")
    return hmac.new(base, b"migkit-transform-v1|" + purpose,
                    hashlib.sha256).digest()


def _aes(key):
    try:
        from cryptography.hazmat.primitives.ciphers import (
            Cipher, algorithms, modes)
    except ImportError:
        raise SystemExit("fpe and token need the cryptography library, which"
                         " is not installed") from None
    return Cipher(algorithms.AES(key), modes.ECB()).encryptor()  # noqa: S305


def _num(digits, radix):
    n = 0
    for d in digits:
        n = n * radix + d
    return n


def _str(n, radix, length):
    out = []
    for _ in range(length):
        n, d = divmod(n, radix)
        out.append(d)
    return out[::-1]


def _ff1(key, tweak, digits, radix, decrypt):
    """NIST SP 800-38G FF1 over a list of digits in `radix`; AES-256 with
    the CBC-MAC of the spec as its pseudo-random function."""
    n = len(digits)
    u, v = n // 2, n - n // 2
    aes = _aes(key)

    def prf(block):
        out = bytes(16)
        for i in range(0, len(block), 16):
            out = aes.update(bytes(a ^ b for a, b in
                                   zip(out, block[i:i + 16])))
        return out
    # b = ceil(ceil(v * log2(radix)) / 8), exact in integers
    bits = (radix ** v - 1).bit_length()
    b = -(-bits // 8)
    d = 4 * -(-b // 4) + 4
    t = len(tweak)
    p = (bytes([1, 2, 1]) + radix.to_bytes(3, "big")
         + bytes([10, u % 256]) + n.to_bytes(4, "big") + t.to_bytes(4, "big"))
    pad = bytes((-t - b - 1) % 16)
    a, bb = digits[:u], digits[u:]
    rounds = range(9, -1, -1) if decrypt else range(10)
    for i in rounds:
        side = a if decrypt else bb
        q = tweak + pad + bytes([i]) + _num(side, radix).to_bytes(b, "big")
        r = prf(p + q)
        s = r
        for j in range(1, -(-d // 16)):
            s += aes.update((int.from_bytes(r, "big") ^ j).to_bytes(16, "big"))
        y = int.from_bytes(s[:d], "big")
        m = u if i % 2 == 0 else v
        mod = radix ** m
        if decrypt:
            c = (_num(bb, radix) - y) % mod
            a, bb = _str(c, radix, m), a
        else:
            c = (_num(a, radix) + y) % mod
            a, bb = bb, _str(c, radix, m)
    return a + bb


def _digits(rule_alphabet, value, refusing):
    chars = alphabet(rule_alphabet)
    text = str(value)
    for i, ch in enumerate(text):
        if ch not in chars:
            raise SystemExit(
                f"fpe over {str(rule_alphabet or 'digits')!r} refuses a"
                f" value with a character outside the alphabet (position"
                f" {i + 1} of {len(text)}): {refusing}")
    if len(chars) ** len(text) < LEAST_DOMAIN:
        raise SystemExit(
            f"fpe over {str(rule_alphabet or 'digits')!r} refuses a value"
            f" of {len(text)} characters: only {len(chars) ** len(text):,}"
            f" values exist, fewer than the {LEAST_DOMAIN:,} below which"
            " the encryption is a dictionary away; the column takes a"
            " token or a hash")
    return chars, [chars.index(ch) for ch in text]


#: the tweak every column shares, so a key in one table and the column that
#: refers to it encrypt alike and a join on it still lines up
TWEAK = b"migkit-fpe-v1"


def fpe_encrypt(rule_alphabet, value, key=None):
    """`value` encrypted in its own shape and length (see the module)."""
    chars, digits = _digits(rule_alphabet, value, "encrypt")
    out = _ff1(_key(key, b"fpe"), TWEAK, digits, len(chars), False)
    return "".join(chars[d] for d in out)


def fpe_decrypt(rule_alphabet, value, key=None):
    """What `fpe_encrypt` made `value` of, for the holder of the key."""
    chars, digits = _digits(rule_alphabet, value, "decrypt")
    out = _ff1(_key(key, b"fpe"), TWEAK, digits, len(chars), True)
    return "".join(chars[d] for d in out)


#: {vault path: connection}, opened once per process
_VAULTS = {}


def _vault():
    import sqlite3
    import threading

    from . import config, durable
    path = config.state_home() / "token.vault"
    got = _VAULTS.get(str(path))
    if got is None:
        durable.private_dir(path.parent)
        db = sqlite3.connect(str(path), check_same_thread=False,
                             isolation_level=None)
        db.execute("create table if not exists vault (token text primary key,"
                   " sealed blob not null)")
        try:
            path.chmod(0o600)
        except OSError:
            pass
        got = _VAULTS[str(path)] = (db, threading.Lock())
    return got


def tokenise(domain, value, key=None):
    """The token of `value` in `domain`: the same value gives the same
    token, and the vault holds the value sealed under the key. Two values
    that came to one token would be told apart by a longer one."""
    import os
    text = str(value)
    tokkey = _key(key, b"token|" + str(domain or "").encode())
    seal = _key(key, b"vault")
    mac = hmac.new(tokkey, text.encode("utf-8", "surrogatepass"),
                   hashlib.sha256).hexdigest()
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    db, lock = _vault()
    for size in (24, 32, 48, 64):
        tok = "tok-" + mac[:size]
        with lock:
            row = db.execute("select sealed from vault where token = ?",
                             (tok,)).fetchone()
            if row is None:
                nonce = os.urandom(12)
                sealed = nonce + AESGCM(seal).encrypt(
                    nonce, text.encode("utf-8", "surrogatepass"),
                    tok.encode())
                db.execute("insert into vault values (?, ?)", (tok, sealed))
                return tok
        got = AESGCM(seal).decrypt(row[0][:12], row[0][12:], tok.encode())
        if got == text.encode("utf-8", "surrogatepass"):
            return tok
    raise SystemExit("a token collided on every length; nothing is written")


def reverse(rule, shown, key=None):
    """The value a transformed one stands for, for the holder of the key:
    an `fpe` value decrypted, a token looked up in the vault and opened. A
    token the vault does not hold, or the wrong key, is refused; a value
    of any other transform cannot be reversed."""
    kind, arg = parse(rule) if isinstance(rule, str) else rule
    if shown is None:
        return None
    if kind == "fpe":
        return fpe_decrypt(arg, shown, key)
    if kind != "token":
        raise SystemExit(f"a value made by {kind} cannot be reversed: only"
                         " fpe and token keep the way back")
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    db, lock = _vault()
    with lock:
        row = db.execute("select sealed from vault where token = ?",
                         (str(shown),)).fetchone()
    if row is None:
        raise SystemExit("this token is not in the vault of this user: it"
                         " was made elsewhere or the vault was removed")
    try:
        return AESGCM(_key(key, b"vault")).decrypt(
            row[0][:12], row[0][12:], str(shown).encode()).decode(
                "utf-8", "surrogatepass")
    except InvalidTag:
        raise SystemExit("the key does not open this token: it is not the"
                         " key the token was made under") from None


def choose(facts):
    """(the rule for a column, the `decide.hide_column` `Choice` that
    names it): `fpe` (with the alphabet the column's values are in),
    `token`, or `hash` - from the column's type, length, whether it must
    stay unique, how many keys refer to it and whether the value must come
    back. Read `Choice.said` / `.reason` for why."""
    from . import decide
    got = decide.hide_column(facts)
    if got.path == "fpe":
        return ("fpe " + str(facts.get("alphabet") or "digits")).strip(), got
    return got.path, got


# --- a statement over the batches as they pass (backlog W4.a, W4.b) --------

#: what a statement over a value may not call: what gives another value on
#: another run, or reads or writes outside the batch
_UNSTABLE = {"random", "rand", "uuid", "gen_random_uuid", "uuidv4", "now",
             "current_timestamp", "current_date", "current_time", "today",
             "get_current_time", "get_current_timestamp", "localtimestamp",
             "nextval", "currval", "setseed", "read_csv", "read_csv_auto",
             "read_parquet", "read_json", "read_json_auto", "read_text",
             "read_blob", "getenv", "current_setting"}


def _tree(expression):
    import sqlglot
    from sqlglot import exp
    text = str(expression or "").strip()
    try:
        tree = sqlglot.parse_one(text, read="duckdb")
    except Exception as e:  # noqa: BLE001 - its own words, said below
        raise SystemExit(f"sql `{text}` is not an expression: {e}") from None
    if not text or isinstance(tree, (exp.Query, exp.Command)) \
            or tree.find(exp.Query, exp.Subquery) is not None:
        raise SystemExit(f"sql `{text}` is not an expression of one value")
    return tree


def statement(expression):
    """The one column `expression` reads (its name as written); SystemExit
    for one that is not an expression of that column's value alone - one
    naming no column or two, an aggregate or a window (a batch would give
    another value than the whole table), a query, a star, or a function of
    the clock, of chance or of a file or setting."""
    from sqlglot import exp
    tree = _tree(expression)
    text = str(expression).strip()
    if tree.find(exp.AggFunc, exp.Window) is not None:
        raise SystemExit(f"sql `{text}` aggregates or windows over rows: a"
                         " batch would give another value than the table")
    if tree.find(exp.Star) is not None:
        raise SystemExit(f"sql `{text}` reads every column: name the one"
                         " it is written on")
    for f in tree.find_all(exp.Func):
        name = (f.name if isinstance(f, exp.Anonymous)
                else f.sql_name()).lower()
        if name in _UNSTABLE or isinstance(
                f, (exp.Rand, exp.CurrentTimestamp, exp.CurrentDate,
                    exp.CurrentTime)):
            raise SystemExit(f"sql `{text}` calls {name}: "
                             + ("it reads outside the batch" if name.startswith(
                                 ("read_", "getenv", "current_setting"))
                                else "its value is not the same on another"
                                " run, so nothing could be proved of it"))
    names = {c.name for c in tree.find_all(exp.Column)}
    if len(names) != 1:
        raise SystemExit(f"sql `{text}` names {len(names) or 'no'}"
                         " column(s): it names the column it is written on,"
                         " and only that one")
    return names.pop()


def _bound(expression, name):
    """`expression` in the analytic SQL with its column read as `name`."""
    from sqlglot import exp
    tree = _tree(expression)
    for c in list(tree.find_all(exp.Column)):
        c.replace(exp.column(name, quoted=True))
    return tree.sql(dialect="duckdb")


_local = threading.local()


def _engine():
    """This thread's analytic engine, in memory and shut off from files,
    the network and its own settings once made."""
    con = getattr(_local, "con", None)
    if con is None:
        import duckdb
        con = duckdb.connect(":memory:")
        con.execute("set enable_external_access = false")
        con.execute("set lock_configuration = true")
        _local.con = con
    return con


def _plain(v):
    if isinstance(v, (bytes, bytearray, memoryview)):
        # a text column a driver hands over as its bytes: its text, as
        # every other transform reads it
        return bytes(v).decode("utf-8")
    return v


def _run_sql(expressions, columns):
    """The values of each of `expressions` (one, or a list beside
    `columns`) over `columns` ([[value of each row]] per expression): one
    statement over the batch, its rows in the order given."""
    import pyarrow as pa
    one = isinstance(expressions, str)
    exprs = [expressions] if one else list(expressions)
    n = len(columns[0]) if columns else 0
    arrays = {"__n": pa.array(range(n), type=pa.int64())}
    for i, values in enumerate(columns):
        values = [_plain(v) for v in values]
        try:
            arrays[f"c{i}"] = pa.array(values)
        except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
            # values of more than one type in a column: their text, as a
            # driver that hands them so would
            arrays[f"c{i}"] = pa.array(
                [None if v is None else str(v) for v in values])
    batch = pa.table(arrays)
    sel = ", ".join(f"{_bound(e, f'c{i}')} as \"o{i}\""
                    for i, e in enumerate(exprs))
    con = _engine()
    RUNS["row" if one and n == 1 else "batch"] += 1
    try:
        con.register("batch", batch)
        try:
            got = con.execute(f"select {sel} from batch order by __n"
                              ).fetchall()
        finally:
            con.unregister("batch")
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001 - the engine's words, said
        raise SystemExit(f"the transform {', '.join(exprs)} cannot run over"
                         f" these values: {e}. Nothing was written of"
                         " them") from None
    if one:
        return [r[0] for r in got]
    return got


def apply_rows(at, rows, key=None):
    """`rows` as the target is to hold them under `at` ([(position,
    rule)]): every `sql` rule through one statement over the whole batch
    (`RUNS["batch"]`), each other rule value by value (`apply`). Rows come
    back in their order, lists or tuples as they came."""
    if not at or not rows:
        return rows
    at = [(i, parse(r) if isinstance(r, str) else r) for i, r in at]
    by_sql = [(i, r[1]) for i, r in at if r[0] == "sql"]
    rest = [(i, r) for i, r in at if r[0] != "sql"]
    rows = list(rows)
    out = [list(r) for r in rows]
    if by_sql:
        got = _run_sql([e for _, e in by_sql],
                       [[r[i] for r in rows] for i, _ in by_sql])
        for row, values in zip(out, got):
            for (i, _), v in zip(by_sql, values):
                row[i] = v
    for row, orig in zip(out, rows):
        for i, rule in rest:
            row[i] = apply(rule, orig[i], key)
    return [o if isinstance(r, list) else tuple(o)
            for o, r in zip(out, rows)]


def measure(at, rows):
    """Seconds a row the `sql` rules of `at` take over `rows` (the first
    batches read, at the copy's own batch size, so what a batch costs
    besides its rows is spread as the copy spreads it), through the batch
    way the copy takes - 0.0 where
    `rows` holds none to measure. SystemExit where the statement cannot
    run over them, which refuses the hop before anything is written."""
    sql_at = [(i, r) for i, r in at
              if (parse(r) if isinstance(r, str) else r)[0] == "sql"]
    if not sql_at or not rows:
        return 0.0
    _engine()  # made once a thread, which the copy's batches do not pay
    began = time.perf_counter()
    apply_rows(sql_at, rows)
    return (time.perf_counter() - began) / len(rows)


def on_target(expression, column, dialect):
    """`expression` in the target's own SQL over `column` (already
    quoted), or None where it does not translate - the target cannot then
    run it itself."""
    import sqlglot
    from sqlglot import exp
    try:
        tree = _tree(expression)
        for c in list(tree.find_all(exp.Column)):
            c.replace(sqlglot.parse_one(column, read=dialect))
        return tree.sql(dialect=dialect, unsupported_level="raise")
    except Exception:  # noqa: BLE001 - a form the target has not
        return None


def authorized(hop):
    """Whether the hop lets the target be rewritten after the copy
    (`transform: on_target: true`), holding the values as read there
    until the rewrite: what `place` asks before the target may take it."""
    from . import hopkeys
    said = hopkeys.opt(hop, "transform") or {}
    return isinstance(said, dict) and said.get("on_target") is True


def place(rules, sample, rows, copy_s, dialect, authorized, budget_s=None):
    """The `decide.transform_place` `Choice` for one table's `sql` rules
    `rules` ([(position, rule, quoted column)]): their cost measured over
    `sample` (the first batches' rows), for the table's `rows`, beside the
    copy's own measured seconds `copy_s` and the hop's time budget where
    it says one; whether the target's SQL (`dialect`) can run each itself
    and whether the hop lets the target be changed after the copy
    (`authorized`)."""
    from . import decide
    at = [(i, r) for i, r, _ in rules]
    per_row = measure(at, sample)
    said = [(parse(r) if isinstance(r, str) else r)[1] for _, r, _ in rules]
    there = [on_target(e, q, dialect) for e, (_, _, q) in zip(said, rules)]
    in_flight = per_row * (rows or 0)
    return decide.transform_place(
        {"statement": ", ".join(said), "measured": bool(sample),
         "sampled": len(sample or ()), "rows": rows or 0,
         "in_flight_s": in_flight, "copy_s": copy_s, "budget_s": budget_s,
         "target_runs": all(t is not None for t in there),
         "authorized": bool(authorized)},
        costs={"in_flight": in_flight, "on_target": copy_s})

"""COBOL copybooks, and the records they describe, read and written exactly.

A mainframe extract is bytes: EBCDIC text in the code page of the system
that wrote it, numbers as zoned or packed decimals and big-endian binaries,
floats in IBM's hexadecimal format, and a record's shape given only by the
copybook the programs were compiled with. The failures this module exists
to prevent are the quiet ones: a packed field holding x'00' read as zero, a
sign nibble nobody checked, a space in a zoned field turned into a 0, a
signed zoned field taken to be one byte longer than it is and every field
after it read from the wrong place.

Why migkit's own reader, chosen by a corpus
-------------------------------------------

The reader was chosen by running each candidate over one corpus of
copybooks and records whose values are known from IBM's documented
encodings (`tests/copybook_corpus.py`), measured 2026-09-28:

* Stingray Reader 5.1.1 (MIT, the maintained pure-Python one), given the
  elementary fields one at a time so one it cannot parse hides nothing
  else: of 48 values, 6 read wrong (an unsigned `PIC 9(4) COMP` x'FFFE'
  as -2; `SIGN LEADING SEPARATE` -12 as 12 and +0 as 14000; packed and
  zoned -0 as -0.00) and 21 refused (`COMP-5`, `COMP-1`, `COMP-2`, `P`
  scaling, `S9(18) BINARY`, `SIGN LEADING`, `SIGN TRAILING SEPARATE
  CHARACTER`); 6 of the 7 values that must fail read as numbers (packed
  x'12345A789C' as 12345107.89, a blank inside a zoned field as a 0, a
  letter as a digit); Thai text read as cp037; the `OCCURS DEPENDING ON`
  and `SYNC` records refused whole. 21 of its 139 corpus values right.
* Cobrix (Apache-2.0, JVM) is read by Spark or from Scala code - there is
  no program of its own to drive - and by its own documentation a field it
  cannot decode becomes NULL, its bytes kept only in an optional
  `_corrupt_fields` column, and 88 levels are not data to it: the silent
  loss the must-fail values exist to catch, behind a program migkit would
  have had to write.
* This module: all 139 values as IBM documents them, all 10 must-fail
  values refused, each naming its field, its bytes and why.

What is read, and how
---------------------

* Text (`PIC X`, `A`, numeric-edited pictures) in the table's code page,
  through Python's codecs and the `ebcdic` package's (Thai cp838, cp1047,
  cp1140-cp1149, cp273, cp500, cp870, cp875 ...), with the padding blanks
  after it taken off; `PIC N` as UTF-16. DBCS pictures (`PIC G`) are
  refused: no code page here reads them.
* Zoned decimals with the sign overpunched in the last byte (the default),
  the first (`SIGN LEADING`), or as its own `+`/`-` byte (`SEPARATE`);
  packed decimals with sign nibbles C/A/E/F positive and D/B negative;
  binary (`COMP`, `COMP-4`, `BINARY`, `COMP-5`) big-endian, signed where
  the picture is; `P` scaling; `BLANK WHEN ZERO`; `SYNC` slack.
* `COMP-1` as a float (its 24-bit fraction is exact in a double);
  `COMP-2` as an exact decimal: its 56-bit fraction is not always a
  double's 53, and rounding it would change the value it moves.
* Negative zero is zero: COBOL compares -0 equal to +0, and the other
  engines have no negative zero to keep it in.
* A field of LOW-VALUES (every byte x'00') is NULL, and NULL is written
  back as LOW-VALUES: the one convention by which an unset field in a
  record has a value that round-trips.
* `REDEFINES`: the redefined layout, or the one a rule on a discriminator
  field picks per record; `OCCURS` flattened into numbered columns; `OCCURS
  DEPENDING ON` placing what follows the table by the count (IBM's complex
  ODO), entries past the count NULL; 88-levels as boolean columns, compared
  as COBOL compares: alphanumerics byte by byte in the record's own code
  page, numbers by value.
* Must-fail values are never a value: an invalid nibble, a zone that is not
  a digit's, a sign an unsigned field cannot hold, more digits than the
  picture, a truncated record - each is an `Unreadable` naming the record,
  the field, its bytes and why.
"""
import codecs
import re
from decimal import Decimal, localcontext

#: the code page a table is read in when it names none
DEFAULT_CODEPAGE = "cp037"

#: record formats: fixed, fixed blocked (the same bytes once transferred),
#: variable with a record descriptor word, variable blocked with a block
#: descriptor word too, and z/OSMF's record mode (a 4-byte length before
#: each record, nothing else)
RECFMS = ("F", "FB", "V", "VB", "ZOSMF")


class CopybookError(ValueError):
    """A copybook migkit cannot read, said with where."""


class Unreadable(ValueError):
    """A value that is not one: its field, its bytes and why."""

    def __init__(self, field, raw, why):
        super().__init__(f"{field}: {why} (bytes {bytes(raw).hex().upper()})")
        self.field, self.raw, self.why = field, bytes(raw), why


def _codepages():
    """Registers the `ebcdic` package's code pages with Python's codecs."""
    import ebcdic  # noqa: F401 - registering is the import


# ---- the copybook's text ---------------------------------------------------

def _fixed_format(lines):
    """Whether the copybook is in the fixed reference format (sequence area
    in columns 1-6, indicator in 7, code from 8): every line that is not
    blank has an indicator migkit knows in column 7."""
    seen = False
    for line in lines:
        if not line.strip():
            continue
        seen = True
        if len(line) < 7 or line[6] not in " *-/Dd":
            return False
    return seen


def _open_literal(text):
    """The quote a literal still open at the end of `text` began with, or
    None."""
    q = None
    for ch in text:
        if q is None and ch in "'\"":
            q = ch
        elif ch == q:
            q = None
    return q


def source_text(text, source_format=None):
    """The copybook's code, with the reference format taken off: comments
    and debug lines dropped, continuation lines joined - a continued
    literal carries on from its first line's column 72, blanks included,
    as COBOL reads it."""
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    fixed = (source_format or "").lower() == "fixed" or (
        not source_format and _fixed_format(lines))
    out, last = [], ""
    for raw in lines:
        if fixed:
            ind = raw[6] if len(raw) > 6 else " "
            code = raw[7:72]
            if ind in "*/Dd":
                continue
            if ind == "-" and out:
                q = _open_literal(out[-1])
                rest = code.lstrip()
                if q:
                    out[-1] += " " * (65 - len(last))
                    rest = rest[1:] if rest.startswith(q) else rest
                    out[-1] += rest
                else:
                    out[-1] = out[-1].rstrip() + rest
                last = code
                continue
            out.append(code)
            last = code
            continue
        if raw.lstrip().startswith("*>") or raw.startswith("*"):
            continue
        q, cut = None, None
        for i, ch in enumerate(raw):
            if q is None and ch in "'\"":
                q = ch
            elif ch == q:
                q = None
            elif q is None and raw.startswith("*>", i):
                cut = i
                break
        out.append(raw[:cut] if cut is not None else raw)
    return "\n".join(out)


_COPY = re.compile(r"\bCOPY\s+(['\"]?)([\w$#@-]+)\1(?:\s+(?:OF|IN)\s+[\w$#@-]+)?"
                   r"((?:\s+REPLACING\s+.*?)?)\s*\.(?=\s|$)", re.I | re.S)
_PSEUDO = re.compile(r"==(.*?)==\s+BY\s+==(.*?)==", re.S | re.I)


def expand_copies(code, member, depth=0):
    """`COPY name [REPLACING ==a== BY ==b== ...].` replaced by the member's
    code, read through `member(name)`: nested members included, ten
    deep at most."""
    if depth > 10:
        raise CopybookError("COPY members nest more than ten deep")

    def one(m):
        name, replacing = m.group(2), m.group(3) or ""
        if member is None:
            raise CopybookError(f"COPY {name}: the copybook includes a member"
                                " and no directory to find it in was given")
        text = member(name)
        if text is None:
            raise CopybookError(f"COPY {name}: no member of that name beside"
                                " the copybook")
        body = source_text(text)
        for a, b in _PSEUDO.findall(replacing):
            body = body.replace(a.strip(), b.strip())
        return expand_copies(body, member, depth + 1)
    return _COPY.sub(one, code)


_TOKEN = re.compile(r"""
    (?P<lit>[XxNnGg]?'(?:[^']|'')*'|[XxNnGg]?"(?:[^"]|"")*")
  | (?P<word>[^\s,;'"]+)
  | (?P<sep>[,;])
""", re.X)


def sentences(code):
    """[[token, ...], ...]: one list per data description entry. A period
    ends an entry where a blank or the end of the text follows it, so the
    periods inside `PIC ZZ9.99` do not."""
    out, cur = [], []
    for m in _TOKEN.finditer(code):
        tok = m.group("lit") or m.group("word")
        if not tok:
            continue
        end = m.end()
        after = code[end:end + 1]
        if m.group("word") and tok.endswith(".") and (not after
                                                      or after.isspace()):
            if tok != ".":
                cur.append(tok[:-1])
            if cur:
                out.append(cur)
            cur = []
            continue
        cur.append(tok)
    if cur:
        out.append(cur)
    return out


# ---- data description entries ---------------------------------------------

USAGES = {
    "DISPLAY": "display", "COMP": "binary", "COMPUTATIONAL": "binary",
    "COMP-4": "binary", "COMPUTATIONAL-4": "binary", "BINARY": "binary",
    "COMP-5": "native", "COMPUTATIONAL-5": "native",
    "COMP-3": "packed", "COMPUTATIONAL-3": "packed",
    "PACKED-DECIMAL": "packed", "COMP-1": "hfp4", "COMPUTATIONAL-1": "hfp4",
    "COMP-2": "hfp8", "COMPUTATIONAL-2": "hfp8", "NATIONAL": "national",
    "DISPLAY-1": "dbcs", "INDEX": "index", "POINTER": "pointer",
    "POINTER-32": "pointer", "PROCEDURE-POINTER": "pointer8",
    "FUNCTION-POINTER": "pointer",
}

KEYWORDS = set(USAGES) | {
    "REDEFINES", "PIC", "PICTURE", "USAGE", "IS", "ARE", "OCCURS", "TIMES",
    "TO", "DEPENDING", "ON", "ASCENDING", "DESCENDING", "KEY", "INDEXED",
    "BY", "SIGN", "LEADING", "TRAILING", "SEPARATE", "CHARACTER", "SYNC",
    "SYNCHRONIZED", "LEFT", "RIGHT", "JUST", "JUSTIFIED", "BLANK", "WHEN",
    "ZERO", "ZEROS", "ZEROES", "VALUE", "VALUES", "THRU", "THROUGH",
    "EXTERNAL", "GLOBAL", "RENAMES", "GROUP-USAGE", "FILLER", "DYNAMIC",
    "LENGTH", "VOLATILE", "BASED", "ANY",
}

class Figurative:
    """SPACES, ZEROS, LOW-VALUES and the rest: one character or byte that
    fills whatever it is compared with."""

    def __init__(self, fill):
        self.fill = fill

    def __repr__(self):
        return f"Figurative({self.fill!r})"


FIGURATIVE = {"SPACE": " ", "SPACES": " ", "ZERO": "0", "ZEROS": "0",
              "ZEROES": "0", "QUOTE": '"', "QUOTES": '"',
              "LOW-VALUE": b"\x00", "LOW-VALUES": b"\x00",
              "HIGH-VALUE": b"\xff", "HIGH-VALUES": b"\xff",
              "NULL": b"\x00", "NULLS": b"\x00"}


class Item:
    """One data description entry, and the tree under it."""

    def __init__(self, level, name):
        self.level, self.name = level, name
        self.pic = None
        self.usage = None
        self.redefines = None
        self.occurs = None
        self.occurs_min = None
        self.depending = None
        self.sign = None
        self.separate = False
        self.sync = False
        self.blank_zero = False
        self.values = []
        self.children = []
        self.conditions = []
        self.parent = None

    @property
    def filler(self):
        return self.name is None or self.name.upper() == "FILLER"

    def __repr__(self):
        return f"Item({self.level}, {self.name})"


def _literal(tok):
    """A VALUE literal as Python: text, bytes (hex), a Decimal, or a
    `Figurative`."""
    up = tok.upper()
    if up in FIGURATIVE:
        return Figurative(FIGURATIVE[up])
    if tok[:1] in "'\"":
        return tok[1:-1].replace(tok[0] * 2, tok[0])
    if up[:2] in ("X'", 'X"'):
        return bytes.fromhex(tok[2:-1])
    if up[:2] in ("N'", 'N"', "G'", 'G"'):
        return tok[2:-1]
    try:
        return Decimal(tok)
    except Exception:  # noqa: BLE001 - said as the copybook's
        raise CopybookError(f"{tok!r} is not a literal migkit reads")


def entry(tokens):
    """An `Item` from one entry's tokens."""
    try:
        level = int(tokens[0])
    except ValueError:
        raise CopybookError(f"{' '.join(tokens[:4])}: an entry starts with"
                            " its level number")
    rest = tokens[1:]
    name = None
    if rest and rest[0].upper() not in KEYWORDS - {"FILLER"}:
        name = rest[0]
        rest = rest[1:]
    it = Item(level, name)
    i, n = 0, len(rest)

    def word(k):
        return rest[k].upper() if k < n else ""
    while i < n:
        w = word(i)
        if w in ("IS", "ARE"):
            i += 1
        elif w == "REDEFINES":
            it.redefines = rest[i + 1]
            i += 2
        elif w in ("PIC", "PICTURE"):
            i += 1
            if word(i) == "IS":
                i += 1
            it.pic = rest[i]
            i += 1
        elif w == "USAGE":
            i += 1
        elif w in USAGES:
            it.usage = USAGES[w]
            i += 1
        elif w == "OCCURS":
            i += 1
            first = int(rest[i])
            i += 1
            if word(i) == "TO":
                it.occurs_min = first
                it.occurs = int(rest[i + 1])
                i += 2
            else:
                it.occurs = first
            if word(i) == "TIMES":
                i += 1
            if word(i) == "DEPENDING":
                i += 1
                if word(i) == "ON":
                    i += 1
                it.depending = rest[i]
                i += 1
            while word(i) in ("ASCENDING", "DESCENDING", "INDEXED"):
                i += 1
                while i < n and word(i) not in KEYWORDS - {"KEY", "IS", "BY",
                                                            "ON"} | {
                        "ASCENDING", "DESCENDING", "INDEXED"}:
                    i += 1
        elif w == "SIGN" or w in ("LEADING", "TRAILING"):
            if w == "SIGN":
                i += 1
                if word(i) == "IS":
                    i += 1
            it.sign = word(i).lower()
            i += 1
            if word(i) == "SEPARATE":
                it.separate = True
                i += 1
                if word(i) == "CHARACTER":
                    i += 1
        elif w in ("SYNC", "SYNCHRONIZED"):
            it.sync = True
            i += 1
            if word(i) in ("LEFT", "RIGHT"):
                i += 1
        elif w in ("JUST", "JUSTIFIED"):
            i += 1
            if word(i) == "RIGHT":
                i += 1
        elif w == "BLANK":
            i += 1
            if word(i) == "WHEN":
                i += 1
            it.blank_zero = True
            i += 1
        elif w in ("VALUE", "VALUES"):
            i += 1
            if word(i) in ("IS", "ARE"):
                i += 1
            while i < n and word(i) not in KEYWORDS - {
                    "ZERO", "ZEROS", "ZEROES", "ALL"} | {"THRU", "THROUGH"}:
                if word(i) == "ALL":
                    i += 1
                lo = _literal(rest[i])
                i += 1
                hi = None
                if word(i) in ("THRU", "THROUGH"):
                    hi = _literal(rest[i + 1])
                    i += 2
                it.values.append((lo, hi))
        elif w == "RENAMES" or level == 66:
            raise CopybookError(f"{name}: level 66 RENAMES is not read; name"
                                " the fields it renames instead")
        elif w in ("EXTERNAL", "GLOBAL", "VOLATILE", "DYNAMIC", "LENGTH",
                   "ANY", "BASED", "GROUP-USAGE"):
            if w == "GROUP-USAGE":
                raise CopybookError(f"{name}: GROUP-USAGE NATIONAL is not"
                                    " read")
            i += 1
        else:
            raise CopybookError(f"{name or 'FILLER'}: {rest[i]!r} is not a"
                                " clause migkit reads")
    return it


def items(code):
    """The top-level entries (01, 77, or the lowest level present) of a
    copybook's code, each with its tree."""
    top, stack = [], []
    for toks in sentences(code):
        it = entry(toks)
        if it.level == 88:
            if not stack:
                raise CopybookError(f"{it.name}: an 88 level with no item"
                                    " before it")
            it.parent = stack[-1]
            stack[-1].conditions.append(it)
            continue
        if it.level == 77:
            it.level = 1
        while stack and stack[-1].level >= it.level:
            stack.pop()
        if stack:
            it.parent = stack[-1]
            stack[-1].children.append(it)
        else:
            top.append(it)
        stack.append(it)
    if not top:
        raise CopybookError("the copybook describes no data")
    return top


def parse(text, member=None, source_format=None):
    """The records a copybook describes: [Item] for each 01 level. A copybook
    that starts below 01 is one record."""
    return records_of(expand_copies(source_text(text, source_format),
                                    member))


def records_of(code):
    """`parse`, for code the reference format is already off and whose
    members are already in."""
    top = items(code)
    if all(t.level == 1 for t in top):
        return top
    rec = Item(1, "RECORD")
    for t in top:
        t.parent = rec
    rec.children = top
    return [rec]


# ---- pictures --------------------------------------------------------------

class Picture:
    """What a PICTURE says: `kind` numeric, text, national or dbcs; for a
    number its digits, scale (negative for trailing `P`s: `9(3)PP` holds
    hundreds) and sign; for text its length in characters."""

    def __init__(self, source):
        s = source.upper()
        chars, at = [], 0
        for m in re.finditer(r"(CR|DB|[A-Z0-9$+\-*/,.])(?:\((\d+)\))?", s):
            if m.start() != at:
                break
            chars += [m.group(1)] * int(m.group(2) or 1)
            at = m.end()
        if not chars or at != len(s):
            raise CopybookError(f"PIC {source} is not a picture migkit reads")
        self.source = source
        self.signed = "S" in chars
        body = [c for c in chars if c != "S"]
        self.digits, self.scale = 0, 0
        if body and set(body) <= {"9", "V", "P"}:
            self.kind = "numeric"
            nines, ps = body.count("9"), body.count("P")
            if ps:
                lead = "9" not in body or body.index("P") < body.index("9")
                self.scale = ps + nines if lead else -ps
            elif "V" in body:
                self.scale = body[body.index("V") + 1:].count("9")
            self.digits = nines
            self.length = nines
        elif set(body) <= {"N"}:
            self.kind, self.length = "national", len(body)
        elif "G" in body and set(body) <= {"G", "B"}:
            self.kind, self.length = "dbcs", len(body)
        else:
            self.kind = "text"
            self.length = sum(2 if c in ("CR", "DB") else
                              0 if c in ("V", "P") else 1 for c in body)


# ---- the layout of one record ------------------------------------------------

def _column(name):
    return name.lower().replace("-", "_")


class Leaf:
    """One elementary field of the record, at one subscript."""
    __slots__ = ("path", "cobol", "column", "offset", "size", "kind", "pic",
                 "item", "subs", "odo", "before", "alt", "index", "decode",
                 "encode", "declared", "cls")

    def __init__(self, **kw):
        for k in self.__slots__:
            setattr(self, k, kw.get(k))


class Table:
    """An `OCCURS DEPENDING ON` table: where it starts with every table
    before it at its most, its most and fewest entries, the leaf holding
    its count, and one entry's size."""

    def __init__(self, ident, offset, most, fewest, counter):
        self.ident, self.offset, self.most = ident, offset, most
        self.fewest, self.counter, self.size = fewest, counter, 0

    @property
    def end(self):
        return self.offset + self.most * self.size


class Condition:
    """An 88 level at one subscript: its column, where the item it
    describes lies, and that item's leaf where it is elementary."""
    __slots__ = ("column", "item", "values", "offset", "size", "odo",
                 "before", "alt", "leaf")

    def __init__(self, **kw):
        for k in self.__slots__:
            setattr(self, k, kw.get(k))


def _binary_size(digits):
    if digits <= 4:
        return 2
    if digits <= 9:
        return 4
    if digits <= 18:
        return 8
    raise CopybookError(f"a binary field of {digits} digits is not one IBM"
                        " stores")


#: digits each binary width holds, signed and unsigned
_CAPACITY = {(2, True): 5, (2, False): 5, (4, True): 10, (4, False): 10,
             (8, True): 19, (8, False): 20}

NUMERIC = ("zoned", "packed", "binary", "native", "hfp4", "hfp8")


def _inherited(it, attr):
    """An attribute a group gives the items under it (USAGE, SIGN, SYNC)."""
    while it is not None:
        if getattr(it, attr):
            return getattr(it, attr)
        it = it.parent
    return None


def _storage(it):
    """(kind, bytes, Picture or None) for an elementary item."""
    usage = _inherited(it, "usage") or "display"
    pic = Picture(it.pic) if it.pic else None
    name = it.name or "FILLER"
    if usage == "hfp4":
        return "hfp4", 4, pic
    if usage == "hfp8":
        return "hfp8", 8, pic
    if usage in ("index", "pointer"):
        return "bytes", 4, pic
    if usage == "pointer8":
        return "bytes", 8, pic
    if pic is None:
        raise CopybookError(f"{name}: an elementary item needs a PICTURE")
    if pic.kind == "dbcs" or usage == "dbcs":
        raise CopybookError(f"{name}: PIC G / DISPLAY-1 is DBCS, which no"
                            " code page migkit reads decodes")
    if usage == "national" or pic.kind == "national":
        if pic.kind == "numeric":
            raise CopybookError(f"{name}: a national numeric field is not"
                                " read")
        return "national", 2 * pic.length, pic
    if pic.kind == "text":
        if usage != "display":
            raise CopybookError(f"{name}: PIC {pic.source} with a numeric"
                                " USAGE")
        return "text", pic.length, pic
    if usage == "display":
        separate = pic.signed and bool(_inherited(it, "separate"))
        return "zoned", pic.digits + (1 if separate else 0), pic
    if usage == "packed":
        return "packed", pic.digits // 2 + 1, pic
    if usage in ("binary", "native"):
        return usage, _binary_size(pic.digits), pic
    raise CopybookError(f"{name}: USAGE {usage} is not read")


def _boundary(it):
    """The boundary a SYNCHRONIZED item starts on - its own size for a
    binary or a float - or 1."""
    if it.children or not _inherited(it, "sync"):
        return 1
    kind, size, _ = _storage(it)
    return size if kind in ("binary", "native", "hfp4", "hfp8", "bytes") \
        else 1


def _widest(it):
    if not it.children:
        return _boundary(it)
    return max([_widest(c) for c in it.children] + [1])


class Layout:
    """A record's fields placed, named and given a class, compiled for one
    code page and one table's rules."""

    def __init__(self, record, codepage=DEFAULT_CODEPAGE, redefines=None,
                 odo="actual", as_bytes=(), keep_padding=False):
        _codepages()
        try:
            codecs.lookup(codepage)
        except LookupError:
            raise CopybookError(f"code page {codepage!r} is not one migkit"
                                " reads")
        if "0".encode(codepage, "replace") != b"\xf0":
            raise CopybookError(f"code page {codepage!r} is not EBCDIC; the"
                                " zoned and packed rules here are EBCDIC's")
        self.record, self.codepage = record, codepage
        self.odo_max = str(odo or "actual").lower() == "max"
        self.as_bytes = {_column(str(b)) for b in as_bytes}
        self.keep_padding = bool(keep_padding)
        self.space = " ".encode(codepage)
        self.plus, self.minus = "+".encode(codepage), "-".encode(codepage)
        self.rules = self._read_rules(redefines or {})
        self.leaves, self.tables, self.alts, self.conditions = [], {}, {}, []
        self.length = self._place(record, 0, (), (), None, None)
        self._name_columns()
        self._decision()
        for lf in self.leaves:
            lf.declared, lf.cls = _declared(self, lf)
            lf.decode, lf.encode = _codec(self, lf)
        self._compile()

    # ---- placing -----------------------------------------------------------

    @staticmethod
    def _read_rules(rules):
        """{base: (discriminator, {value: alternative}, otherwise)} from the
        table's `redefines`; a name alone picks that alternative for every
        record."""
        out = {}
        for base, rule in rules.items():
            if isinstance(rule, str):
                out[str(base).upper()] = (None, {}, rule.upper())
                continue
            out[str(base).upper()] = (
                str(rule.get("by") or rule.get("field") or "").upper()
                or None,
                {str(k): str(v).upper() for k, v in
                 (rule.get("when") or {}).items()},
                str(rule["else"]).upper() if rule.get("else") else None)
        return out

    def _before(self, at, inside):
        """The tables that end before `at`, other than the one `at` is in:
        where they hold fewer entries than their most, a field at `at`
        lies that much earlier."""
        return tuple(t.ident for t in self.tables.values()
                     if t.ident != inside and t.size and t.end <= at)

    def _place(self, it, offset, subs, path, alt, odo):
        """Places `it` at `offset` with every table at its most, and returns
        its size. `alt` is the REDEFINES alternative it lies in, `odo` the
        (table, entry) it lies in."""
        name = (it.name or "FILLER").upper()
        path = path + (name,)
        times = it.occurs or 1
        table = None
        if it.depending:
            if odo or subs:
                raise CopybookError(f"{name}: OCCURS DEPENDING ON inside"
                                    " another OCCURS is not read")
            counter = it.depending.upper()
            found = [lf for lf in self.leaves if lf.cobol == counter]
            if not found:
                raise CopybookError(f"{name}: its count {counter} is not a"
                                    " field before it")
            table = f"T{len(self.tables)}"
            self.tables[table] = Table(table, offset, it.occurs,
                                       it.occurs_min or 0, found[-1].index)
        one = None
        for k in range(times):
            at = offset + (k * one if k else 0)
            sub = subs + ((k + 1,) if it.occurs else ())
            here = (table, k) if table else odo
            inside = here[0] if here else None
            if it.children:
                size = self._place_group(it, at, sub, path, alt, here)
                if one is None and it.occurs:
                    # every occurrence starts on the boundary its widest
                    # SYNC item needs, so each one is aligned alike
                    size += -size % _widest(it)
            else:
                kind, size, pic = _storage(it)
                self.leaves.append(Leaf(
                    path=path, cobol=name, offset=at, size=size, kind=kind,
                    pic=pic, item=it, subs=sub, odo=here, alt=alt,
                    index=len(self.leaves), before=self._before(at, inside)))
            one = size if one is None else one
            if table and k == 0:
                self.tables[table].size = one
            for cond in it.conditions:
                self.conditions.append(Condition(
                    column=_column(cond.name) + "".join(f"_{s}" for s in sub),
                    item=it, values=cond.values, offset=at, size=one,
                    odo=here, alt=alt, before=self._before(at, inside),
                    leaf=None if it.children else self.leaves[-1]))
        return (one or 0) * times

    def _place_group(self, it, at, sub, path, alt, odo):
        pos, end, placed = at, at, {}
        for ch in it.children:
            cname = (ch.name or "FILLER").upper()
            if ch.redefines:
                base = ch.redefines.upper()
                if base not in placed:
                    raise CopybookError(f"{cname}: REDEFINES {base}, which is"
                                        " not an item before it at its"
                                        " level")
                start = placed[base]
                self.alts.setdefault((base, sub, alt), [base]).append(cname)
                size = self._place(ch, start, sub, path,
                                   (base, cname, sub, alt), odo)
                placed[cname] = start
                end = max(end, start + size)
                continue
            b = _boundary(ch)
            if b > 1:
                pos += -pos % b
            own = (cname, cname, sub, alt) if any(
                (c.redefines or "").upper() == cname
                for c in it.children) else alt
            size = self._place(ch, pos, sub, path, own, odo)
            placed[cname] = pos
            pos += size
            end = max(end, pos)
        return end - at

    # ---- naming, and which alternatives are read ---------------------------

    def _name_columns(self):
        """Each field's column: its own name, qualified by the groups above
        it only as far as makes it unique, its subscripts after it."""
        named = [lf for lf in self.leaves if lf.cobol != "FILLER"]
        depth = {lf.index: 1 for lf in named}
        while True:
            names = {}
            for lf in named:
                parts = [p for p in lf.path[-depth[lf.index]:]
                         if p != "FILLER"]
                names.setdefault("_".join(_column(p) for p in parts)
                                 + "".join(f"_{s}" for s in lf.subs),
                                 []).append(lf)
            clash = [g for g in names.values() if len(g) > 1]
            if not clash:
                break
            grew = False
            for g in clash:
                for lf in g:
                    if depth[lf.index] < len(lf.path) - 1:
                        depth[lf.index] += 1
                        grew = True
            if not grew:
                raise CopybookError(f"{clash[0][0].cobol}: two fields no"
                                    " group name tells apart")
        for col, (lf,) in names.items():
            lf.column = col

    def _decision(self):
        """For each REDEFINES set: the base alone where the table has no
        rule for it, else the rule, whose alternatives are all columns."""
        self.picks = {}
        for (base, sub, outer), names in self.alts.items():
            rule = self.rules.get(base)
            if rule is None:
                self.picks[(base, sub, outer)] = (None, {}, base)
                continue
            field, when, other = rule
            for v in list(when.values()) + ([other] if other else []):
                if v not in names:
                    raise CopybookError(f"the REDEFINES rule for {base}"
                                        f" names {v}, which is not one of"
                                        f" {', '.join(names)}")
            if field and field not in {lf.cobol for lf in self.leaves}:
                raise CopybookError(f"the REDEFINES rule for {base} reads"
                                    f" {field}, which is not a field of the"
                                    " record")
            self.picks[(base, sub, outer)] = rule
        unknown = set(self.rules) - {b for b, _, _ in self.alts}
        if unknown:
            raise CopybookError("REDEFINES rules for"
                                f" {', '.join(sorted(unknown))}, which"
                                " nothing redefines")

    def _wanted(self, alt):
        """Whether a field in alternative `alt` is read for some record."""
        while alt:
            base, name, sub, outer = alt
            field, when, other = self.picks.get((base, sub, outer),
                                                (None, {}, base))
            if field is None and name != other:
                return False
            if field is not None and name not in set(when.values()) | {other}:
                return False
            alt = outer
        return True

    def _compile(self):
        self.read = [lf for lf in self.leaves
                     if lf.cobol != "FILLER" and self._wanted(lf.alt)]
        self.at = {lf.index: i for i, lf in enumerate(self.read)}
        self.conds = [c for c in self.conditions if self._wanted(c.alt)]
        taken = {lf.column for lf in self.read}
        for c in self.conds:
            if c.column in taken:
                raise CopybookError(f"{c.column}: an 88 level with the name"
                                    " of a field")
            taken.add(c.column)
        self.columns = ([(lf.column, lf.declared) for lf in self.read]
                        + [(c.column, "condition") for c in self.conds])
        self.classes = ([lf.cls for lf in self.read]
                        + ["boolean"] * len(self.conds))
        self.varies = bool(self.tables) or any(
            rule[0] for rule in self.picks.values())
        self._fixed = [(i, lf.offset, lf.offset + lf.size, lf.decode)
                       for i, lf in enumerate(self.read)]

    def column_of(self, name):
        """The column a COBOL name or a column name stands for, or None."""
        want = str(name)
        for lf in self.read:
            if lf.column == _column(want) or (not lf.subs and
                                              lf.cobol == want.upper()):
                return lf.column
        for c in self.conds:
            if c.column == _column(want):
                return c.column
        return None

    # ---- reading ------------------------------------------------------------

    def _shift(self, before, counts):
        """How much earlier a field lies than it would with every table in
        `before` at its most."""
        if self.odo_max or not before:
            return 0
        return sum((self.tables[t].most - counts[t]) * self.tables[t].size
                   for t in before if t in counts)

    def decode(self, rec):
        """(values in column order, [Unreadable]) for one record's bytes."""
        values, bad = [None] * len(self.columns), []
        if not self.varies:
            if len(rec) < self.length:
                bad.append(Unreadable("record", rec[-8:],
                                      f"{len(rec)} bytes where the record is"
                                      f" {self.length}"))
                return values, bad
            for i, a, b, dec in self._fixed:
                try:
                    values[i] = dec(rec[a:b])
                except Unreadable as e:
                    bad.append(e)
            self._conditions(rec, values, {}, None, None)
            return values, bad
        counts, got, chosen = {}, {}, {}
        counters = {t.counter: t for t in self.tables.values()}
        # fields outside every alternative first: a discriminator is read
        # before the alternatives it picks between
        order = sorted(range(len(self.read)),
                       key=lambda i: (self.read[i].alt is not None, i))
        for i in order:
            lf = self.read[i]
            if lf.alt is not None and not self._picked(lf.alt, got, chosen,
                                                       bad):
                continue
            if lf.odo and lf.odo[1] >= counts.get(lf.odo[0], 0):
                continue
            start = lf.offset - self._shift(lf.before, counts)
            raw = rec[start:start + lf.size]
            if len(raw) < lf.size:
                bad.append(Unreadable(lf.column, raw, "the record ends"
                                      " before this field"))
                continue
            try:
                v = lf.decode(raw)
            except Unreadable as e:
                bad.append(e)
                continue
            values[i] = v
            got.setdefault(lf.cobol, v)
            t = counters.get(lf.index)
            if t is not None:
                n = -1 if v is None else int(v)
                if not t.fewest <= n <= t.most:
                    bad.append(Unreadable(lf.column, raw,
                                          f"counts {n} entries of a table of"
                                          f" {t.fewest} to {t.most}"))
                    n = 0
                counts[t.ident] = n
        need = self.length - self._shift(tuple(self.tables), counts)
        if len(rec) < need:
            bad.append(Unreadable("record", rec[-8:], f"{len(rec)} bytes"
                                  f" where its fields need {need}"))
        self._conditions(rec, values, counts, got, chosen)
        return values, bad

    def _picked(self, alt, got, chosen, bad):
        """Whether this record reads alternative `alt`, by the rules of
        every set it lies in."""
        while alt:
            base, name, sub, outer = alt
            key = (base, sub, outer)
            if key not in chosen:
                field, when, other = self.picks.get(key, (None, {}, base))
                pick = other
                if field is not None:
                    v = got.get(field)
                    for k, alt_name in when.items():
                        if _matches(v, k):
                            pick = alt_name
                            break
                    if pick is None:
                        bad.append(Unreadable(base, b"",
                                              f"{field} is {v!r}, which the"
                                              " REDEFINES rule for it does"
                                              " not name"))
                chosen[key] = pick
            if chosen[key] != name:
                return False
            alt = outer
        return True

    def _conditions(self, rec, values, counts, got, chosen):
        base = len(self.read)
        for j, c in enumerate(self.conds):
            if c.odo and c.odo[1] >= counts.get(c.odo[0], 0):
                continue
            if c.alt and chosen is not None and not self._picked(
                    c.alt, got, chosen, []):
                continue
            start = c.offset - self._shift(c.before, counts)
            values[base + j] = _condition(self, c, rec[start:start + c.size])

    def encode(self, values):
        """The record's bytes for `values` in column order: FILLER as blanks,
        a field whose value is None as LOW-VALUES."""
        counts = {}
        for t in self.tables.values():
            i = self.at.get(t.counter)
            v = values[i] if i is not None else None
            counts[t.ident] = t.most if v is None else int(v)
        size = self.length - self._shift(tuple(self.tables), counts)
        out = bytearray(self.space * size)
        got, chosen = {}, {}
        for i, lf in enumerate(self.read):
            got.setdefault(lf.cobol, values[i])
        for i, lf in enumerate(self.read):
            if lf.alt is not None and not self._picked(lf.alt, got, chosen,
                                                       []):
                continue
            if lf.odo and lf.odo[1] >= counts.get(lf.odo[0], 0):
                continue
            start = lf.offset - self._shift(lf.before, counts)
            out[start:start + lf.size] = lf.encode(values[i])
        return bytes(out)


def _matches(value, key):
    if value is None:
        return False
    if isinstance(value, str):
        return value.rstrip() == str(key).rstrip()
    try:
        return Decimal(str(value)) == Decimal(str(key))
    except Exception:  # noqa: BLE001 - not a number: not this one
        return False


def _condition(layout, c, raw):
    """An 88 level's truth for one record: a number compared by value, as
    COBOL compares numbers; anything else byte by byte in the record's own
    code page, padded with blanks, as COBOL compares alphanumerics - so a
    `THRU` range runs in the code page's order, not Unicode's."""
    lf = c.leaf
    if lf is not None and lf.kind in NUMERIC:
        try:
            v = lf.decode(raw)
        except Unreadable:
            return None
        if v is None:
            return None
        v = Decimal(str(v))
        for lo, hi in c.values:
            lo, hi = _as_number(lo), _as_number(hi)
            if lo is None:
                continue
            if (hi is None and v == lo) or (hi is not None and lo <= v <= hi):
                return True
        return False
    size = len(raw)
    for lo, hi in c.values:
        a = _as_bytes(layout, lo, size)
        if hi is None:
            if raw == a:
                return True
        elif a <= raw <= _as_bytes(layout, hi, size):
            return True
    return False


def _as_number(lit):
    if isinstance(lit, Figurative):
        return Decimal(0) if lit.fill == "0" else None
    if isinstance(lit, Decimal):
        return lit
    return None


def _as_bytes(layout, lit, size):
    if isinstance(lit, Figurative):
        one = lit.fill if isinstance(lit.fill, bytes) else \
            lit.fill.encode(layout.codepage)
        return (one * size)[:size]
    if isinstance(lit, bytes):
        return lit.ljust(size, layout.space)[:size]
    if isinstance(lit, Decimal):
        lit = format(lit, "f")
    return str(lit).encode(layout.codepage).ljust(size, layout.space)[:size]


# ---- the classes a field's values have ------------------------------------

def _declared(layout, lf):
    """(declared type, canonical class) for a leaf. A number whose digits
    fit a 64-bit integer at scale 0 is an integer; any other is a decimal
    at the precision its storage can hold, so a target built from it never
    has to round or refuse a value the field can carry."""
    k, pic = lf.kind, lf.pic
    if k == "bytes" or (lf.column or "") in layout.as_bytes:
        return f"bytes({lf.size})", "bytes"
    if k == "text":
        return f"text({pic.length})", "text"
    if k == "national":
        return f"national({pic.length})", "text"
    if k == "hfp4":
        return "hex float", "float"
    if k == "hfp8":
        return "long hex float", "decimal"
    if k in ("binary", "native"):
        digits = _CAPACITY[(lf.size, pic.signed)] + max(-pic.scale, 0)
        if pic.scale <= 0 and digits <= 19 and (pic.signed or lf.size < 8):
            return f"{k} integer({digits})", "integer"
        return f"{k} decimal({digits},{max(pic.scale, 0)})", "decimal"
    digits = pic.digits + max(-pic.scale, 0)
    if pic.scale <= 0 and digits <= 18:
        return f"{k} integer({digits})", "integer"
    return (f"{k} decimal({max(digits, pic.scale)},{max(pic.scale, 0)})",
            "decimal")


# ---- one field's bytes to a value, and back --------------------------------

def _codec(layout, lf):
    """(decode, encode) for one leaf."""
    name, size, k = lf.column or lf.cobol, lf.size, lf.kind
    if lf.cls == "bytes":
        return _bytes_codec(name, size)
    if k == "text":
        return _text_codec(name, size, layout.codepage, layout)
    if k == "national":
        return _text_codec(name, size, "utf-16-be", layout)
    if k == "zoned":
        return _zoned_codec(name, lf, layout)
    if k == "packed":
        return _packed_codec(name, lf)
    if k in ("binary", "native"):
        return _binary_codec(name, lf)
    if k in ("hfp4", "hfp8"):
        return _hfp_codec(name, size)
    raise CopybookError(f"{name}: {k} has no codec")


def _low(raw):
    """LOW-VALUES: every byte x'00'."""
    return not raw.strip(b"\x00")


def _bytes_codec(name, size):
    def decode(raw):
        return None if _low(raw) else bytes(raw)

    def encode(v):
        if v is None:
            return b"\x00" * size
        b = bytes(v)
        if len(b) > size:
            raise Unreadable(name, b[:8], f"{len(b)} bytes into {size}")
        return b.ljust(size, b"\x00")
    return decode, encode


def _text_codec(name, size, cp, layout):
    pad = " ".encode(cp)
    keep = layout.keep_padding

    def decode(raw):
        if _low(raw):
            return None
        try:
            text = raw.decode(cp)
        except UnicodeDecodeError as e:
            raise Unreadable(name, raw, f"not text in {cp}: {e.reason}")
        return text if keep else text.rstrip(" ")

    def encode(v):
        if v is None:
            return b"\x00" * size
        try:
            b = str(v).encode(cp)
        except UnicodeEncodeError as e:
            raise Unreadable(name, str(v).encode("utf-8")[:8],
                             f"holds a character {cp} has not ({e.reason})")
        if len(b) > size:
            raise Unreadable(name, b[:8], f"{len(b)} bytes of text into a"
                             f" field of {size}")
        return b + pad * ((size - len(b)) // len(pad))
    return decode, encode


def _number(digits, scale, negative, integer):
    """The value of `digits` (text of 0-9) at `scale`: an int for an
    integer class, else a Decimal keeping the scale. Negative zero is
    zero."""
    if integer:
        n = int(digits) * (10 ** -scale if scale < 0 else 1)
        return -n if negative else n
    d = Decimal(digits).scaleb(-scale) if scale > 0 else \
        Decimal(int(digits) * 10 ** -scale)
    return -d if negative and d else d


def _digits_of(name, v, digits, scale):
    """(digit text of `digits` places, negative) for a value going into a
    field of `digits` digits at `scale`, refusing what does not fit rather
    than rounding it."""
    d = v if isinstance(v, Decimal) else Decimal(str(v))
    if not d.is_finite():
        raise Unreadable(name, b"", f"{v} is not a number a field holds")
    unit = d.scaleb(scale)
    if unit != unit.to_integral_value():
        raise Unreadable(name, b"", f"{v} has more decimal places than the"
                         f" field's {max(scale, 0)}")
    n = int(unit)
    text = str(abs(n))
    if len(text) > digits:
        raise Unreadable(name, b"", f"{v} has more digits than the field's"
                         f" {digits}")
    return text.zfill(digits), n < 0


#: zoned digits x'F0'-x'F9' to '0'-'9', every other byte to '?'
_ZONED = bytes((0x30 + (b & 0x0F)) if 0xF0 <= b <= 0xF9 else 0x3F
               for b in range(256))


def _zoned_codec(name, lf, layout):
    pic, size, item = lf.pic, lf.size, lf.item
    signed = pic.signed
    leading = signed and (_inherited(item, "sign") == "leading")
    separate = signed and bool(_inherited(item, "separate"))
    blanks = layout.space * size if item.blank_zero else None
    integer = lf.cls == "integer"
    digits, scale = pic.digits, pic.scale
    plus, minus = layout.plus, layout.minus

    def decode(raw):
        if _low(raw):
            return None
        if raw == blanks:
            return _number("0", scale, False, integer)
        body, negative = raw, False
        if separate:
            s = raw[:1] if leading else raw[-1:]
            body = raw[1:] if leading else raw[:-1]
            if s == minus:
                negative = True
            elif s != plus:
                raise Unreadable(name, raw, "its separate sign is neither +"
                                 " nor -")
        else:
            at = 0 if leading else len(body) - 1
            zone, digit = body[at] >> 4, body[at] & 0x0F
            if digit > 9:
                raise Unreadable(name, raw, f"byte {at + 1} is not a digit")
            if zone in (0xD, 0xB):
                if not signed:
                    raise Unreadable(name, raw, "a negative sign in a field"
                                     " with no S")
                negative = True
            elif zone not in (0xC, 0xF, 0xA, 0xE) or (not signed
                                                      and zone != 0xF
                                                      and zone != 0xC):
                raise Unreadable(name, raw, f"byte {at + 1} carries no sign"
                                 " a zoned decimal has")
            if zone != 0xF:
                body = body[:at] + bytes((0xF0 | digit,)) + body[at + 1:]
        text = body.translate(_ZONED)
        bad = text.find(b"?")
        if bad >= 0:
            raise Unreadable(name, raw, f"byte {bad + 1 + (1 if separate and leading else 0)}"
                             " is not a digit")
        return _number(text.decode("ascii"), scale, negative, integer)

    def encode(v):
        if v is None:
            return b"\x00" * size
        text, negative = _digits_of(name, v, digits, scale)
        if negative and not signed:
            raise Unreadable(name, b"", f"{v} is negative in a field with no"
                             " sign")
        if blanks is not None and not text.strip("0"):
            return blanks
        out = bytearray(0xF0 | int(c) for c in text)
        if separate:
            s = minus if negative else plus
            return bytes(s + out if leading else out + s)
        if signed:
            at = 0 if leading else len(out) - 1
            out[at] = (0xD0 if negative else 0xC0) | (out[at] & 0x0F)
        return bytes(out)
    return decode, encode


def _packed_codec(name, lf):
    pic, size = lf.pic, lf.size
    signed = pic.signed
    digits, scale = pic.digits, pic.scale
    integer = lf.cls == "integer"
    # an even number of digits leaves the first nibble spare, and it holds 0
    spare = size * 2 - 1 - digits

    def decode(raw):
        if _low(raw):
            return None
        h = raw.hex()
        body, s = h[:-1], h[-1]
        if not body.isdigit():
            raise Unreadable(name, raw, "a digit nibble above 9")
        if s in "db":
            if not signed:
                raise Unreadable(name, raw, "a negative sign in a field with"
                                 " no S")
            negative = True
        elif s in "cfae":
            negative = False
        else:
            raise Unreadable(name, raw, f"sign nibble {s.upper()} is no sign")
        if spare and body[:spare].strip("0"):
            raise Unreadable(name, raw, f"more digits than PIC {pic.source}")
        return _number(body[spare:], scale, negative, integer)

    def encode(v):
        if v is None:
            return b"\x00" * size
        text, negative = _digits_of(name, v, digits, scale)
        if negative and not signed:
            raise Unreadable(name, b"", f"{v} is negative in a field with no"
                             " sign")
        nib = "d" if negative else ("c" if signed else "f")
        return bytes.fromhex("0" * spare + text + nib)
    return decode, encode


def _binary_codec(name, lf):
    pic, size = lf.pic, lf.size
    signed, scale = pic.signed, pic.scale
    integer = lf.cls == "integer"

    def decode(raw):
        n = int.from_bytes(raw, "big", signed=signed)
        if integer:
            return n * (10 ** -scale if scale < 0 else 1)
        if scale > 0:
            return Decimal(n).scaleb(-scale)
        return Decimal(n * 10 ** -scale)

    def encode(v):
        if v is None:
            return b"\x00" * size
        d = v if isinstance(v, Decimal) else Decimal(str(v))
        unit = d.scaleb(scale)
        if unit != unit.to_integral_value():
            raise Unreadable(name, b"", f"{v} has more decimal places than"
                             f" the field's {max(scale, 0)}")
        try:
            return int(unit).to_bytes(size, "big", signed=signed)
        except OverflowError:
            raise Unreadable(name, b"", f"{v} does not fit {size} bytes")
    return decode, encode


def hfp_value(raw):
    """An IBM hexadecimal float's exact value, as (negative, fraction,
    binary exponent) with magnitude = fraction * 2 ** exponent: a sign bit,
    seven bits of exponent in excess 64 of powers of 16, and the rest a
    fraction."""
    n = int.from_bytes(raw, "big")
    bits = len(raw) * 8 - 8
    negative = bool(n >> (len(raw) * 8 - 1))
    exp = (n >> bits) & 0x7F
    return negative, n & ((1 << bits) - 1), 4 * (exp - 64) - bits


def _hfp_codec(name, size):
    import math
    from fractions import Fraction
    bits = size * 8 - 8

    def decode(raw):
        negative, frac, e2 = hfp_value(raw)
        if size == 4:
            v = math.ldexp(frac, e2)
            return -v if negative and frac else v
        if not frac:
            return Decimal(0)
        with localcontext() as ctx:
            # 2 ** -312 is the smallest step; every value is exact in 400
            ctx.prec = 400
            d = Decimal(frac) * (Decimal(2) ** e2) if e2 >= 0 else \
                Decimal(frac) / (Decimal(2) ** -e2)
        return -d if negative else d

    def encode(v):
        """Rounded to the nearest the format holds, as IBM converts: a value
        that was read from one writes back exactly, and one that was not
        is held to what the field keeps by the check that follows."""
        if v is None:
            return b"\x00" * size
        f = Fraction(v if isinstance(v, (Decimal, float, int))
                     else Decimal(str(v)))
        negative, f = f < 0, abs(f)
        if f == 0:
            return b"\x00" * size
        exp = 64
        while f >= 1:
            f /= 16
            exp += 1
        while f < Fraction(1, 16):
            f *= 16
            exp -= 1
        frac = round(f * (1 << bits))
        if frac >> bits:
            frac >>= 4
            exp += 1
        if not 0 <= exp <= 127:
            raise Unreadable(name, b"", f"{v} is outside a hexadecimal"
                             " float's range")
        n = (int(negative) << (size * 8 - 1)) | (exp << bits) | frac
        return n.to_bytes(size, "big")
    return decode, encode


def field_codec(kind, precision, scale=0, signed=True, name="value"):
    """(decode, encode) for one packed (`packed`) or zoned (`zoned`)
    decimal outside a copybook - in a record image another program laid
    out, as Db2 for i's journal lays out a row (`engines.db2`): the same
    rules, and the same refusals, as a copybook's field. A value at scale
    0 is an int, as a driver hands a DECIMAL(p,0) over once migkit has
    read it (`FoldsToCapitals._unpad`)."""
    from types import SimpleNamespace
    whole = precision - scale
    pic = Picture(("S" if signed else "")
                  + (f"9({whole})" if whole else "")
                  + (f"V9({scale})" if scale else ""))
    item = Item(5, name)
    item.pic = pic.source
    size = precision // 2 + 1 if kind == "packed" else precision
    lf = Leaf(kind=kind, pic=pic, item=item, size=size, column=name,
              cobol=name, cls="integer" if scale == 0 else "decimal")
    if kind == "packed":
        return _packed_codec(name, lf)
    if kind == "zoned":
        return _zoned_codec(name, lf, SimpleNamespace(
            space=b"\x40", plus=b"\x4e", minus=b"\x60"))
    raise CopybookError(f"{name}: {kind} is not a decimal kind")


# ---- records in a file -------------------------------------------------------

def records(f, recfm, lrecl, start=0, end=None, chunk=1 << 22):
    """(offset, bytes) of each record whose first byte (its descriptor's,
    where it has one) lies in [start, end), read from the open binary file
    `f`. A leftover that is not a whole record ends it as `Unreadable`."""
    recfm = str(recfm or "F").upper()
    if recfm in ("F", "FB"):
        if lrecl <= 0:
            raise CopybookError("a fixed record has no length")
        first = start + (-start % lrecl)
        f.seek(first)
        at = first
        per = max(1, chunk // lrecl)
        while end is None or at < end:
            want = per * lrecl
            if end is not None:
                want = min(want, -(-(end - at) // lrecl) * lrecl)
            buf = f.read(want)
            if not buf:
                return
            whole = len(buf) - len(buf) % lrecl
            for i in range(0, whole, lrecl):
                yield at + i, buf[i:i + lrecl]
            if whole < len(buf):
                raise Unreadable("file", buf[whole:][:8],
                                 f"{len(buf) - whole} bytes after the last"
                                 f" whole record at offset {at + whole}")
            at += whole
        return
    if recfm == "VB":
        yield from _blocks(f, start, end)
        return
    if recfm not in ("V", "ZOSMF"):
        raise CopybookError(f"record format {recfm} is not one of"
                            f" {', '.join(RECFMS)}")
    f.seek(start)
    at, buf, pos = start, b"", 0
    while end is None or at < end:
        if len(buf) - pos < 4:
            buf = buf[pos:] + f.read(chunk)
            pos = 0
            if not buf:
                return
            if len(buf) < 4:
                raise Unreadable("file", buf, f"{len(buf)} bytes after the"
                                 f" last whole record at offset {at}")
        head = buf[pos:pos + 4]
        if recfm == "V":
            n = int.from_bytes(head[:2], "big") - 4
            if head[2:] != b"\x00\x00":
                raise Unreadable("file", head, f"the record at offset {at}"
                                 " spans blocks (its descriptor's segment"
                                 " bytes are not zero); spanned records are"
                                 " not read")
        else:
            n = int.from_bytes(head, "big")
        if n < 0:
            raise Unreadable("file", head, f"a record descriptor at offset"
                             f" {at} gives a length shorter than itself")
        while len(buf) - pos < 4 + n:
            more = f.read(max(chunk, 4 + n))
            if not more:
                raise Unreadable("file", buf[pos:pos + 8],
                                 f"the record at offset {at} says {n} bytes"
                                 f" and the file ends after"
                                 f" {len(buf) - pos - 4}")
            buf = buf[pos:] + more
            pos = 0
        yield at, buf[pos + 4:pos + 4 + n]
        pos += 4 + n
        at += 4 + n


def _blocks(f, start, end):
    f.seek(start)
    at = start
    while end is None or at < end:
        head = f.read(4)
        if not head:
            return
        if len(head) < 4:
            raise Unreadable("file", head, f"{len(head)} bytes after the last"
                             f" whole block at offset {at}")
        if head[0] & 0x80:
            n = int.from_bytes(head, "big") & 0x7FFFFFFF
        else:
            n = int.from_bytes(head[:2], "big")
            if head[2:] != b"\x00\x00":
                raise Unreadable("file", head, f"the block descriptor at"
                                 f" offset {at} is not one")
        block = f.read(n - 4)
        if len(block) < n - 4:
            raise Unreadable("file", head, f"the block at offset {at} says"
                             f" {n} bytes and the file ends first")
        pos = 0
        while pos < len(block):
            rdw = block[pos:pos + 4]
            m = int.from_bytes(rdw[:2], "big")
            if len(rdw) < 4 or m < 4 or pos + m > len(block):
                raise Unreadable("file", rdw, f"a record descriptor inside"
                                 f" the block at offset {at} does not fit it")
            if rdw[2:] != b"\x00\x00":
                raise Unreadable("file", rdw, f"a record in the block at"
                                 f" offset {at} spans blocks; spanned"
                                 " records are not read")
            yield at, block[pos + 4:pos + m]
            pos += m
        at += n


def starts(f, recfm, lrecl, size, every):
    """The offsets at which every `every`-th record (every `every`-th
    record's block, for VB) begins: where a range of the file can start
    reading. A fixed file's are computed; a variable one's are found by
    walking the descriptors alone."""
    recfm = str(recfm or "F").upper()
    if recfm in ("F", "FB"):
        step = max(1, every) * lrecl
        return list(range(0, size, step))
    out, n = [], 0
    if recfm == "VB":
        f.seek(0)
        at = 0
        while at < size:
            head = f.read(4)
            if len(head) < 4:
                break
            if n == 0 or n >= every:
                out.append(at)
                n = 0
            blen = (int.from_bytes(head, "big") & 0x7FFFFFFF
                    if head[0] & 0x80 else int.from_bytes(head[:2], "big"))
            block = f.read(blen - 4)
            pos = 0
            while pos + 4 <= len(block):
                m = int.from_bytes(block[pos:pos + 2], "big")
                if m < 4:
                    break
                pos += m
                n += 1
            at += blen
        return out
    f.seek(0)
    at = 0
    while at < size:
        head = f.read(4)
        if len(head) < 4:
            break
        if n % max(1, every) == 0:
            out.append(at)
        m = (int.from_bytes(head[:2], "big") if recfm == "V"
             else int.from_bytes(head, "big") + 4)
        if m < 4:
            break
        f.seek(m - 4, 1)
        at += m
        n += 1
    return out


def frame(recfm, data, lrecl):
    """One record's bytes as the record format stores them."""
    recfm = str(recfm or "F").upper()
    if recfm in ("F", "FB"):
        if len(data) > lrecl:
            raise CopybookError(f"a record of {len(data)} bytes in a fixed"
                                f" file of {lrecl}")
        return data.ljust(lrecl, b"\x40")
    if len(data) > 32752 and recfm in ("V", "VB"):
        raise CopybookError(f"a record of {len(data)} bytes is longer than a"
                            " variable record holds")
    if recfm == "V":
        return (len(data) + 4).to_bytes(2, "big") + b"\x00\x00" + data
    if recfm == "VB":
        # a block of one record: what a reader of VB takes, whatever the
        # block size the dataset is later given
        return ((len(data) + 8).to_bytes(2, "big") + b"\x00\x00"
                + (len(data) + 4).to_bytes(2, "big") + b"\x00\x00" + data)
    if recfm == "ZOSMF":
        return len(data).to_bytes(4, "big") + data
    raise CopybookError(f"record format {recfm} is not one of"
                        f" {', '.join(RECFMS)}")

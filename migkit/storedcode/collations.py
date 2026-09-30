"""A column's collation held to the source's by the column's own values,
asked of both servers (leapfrog research P13, backlog CV.P13).

A collation decides that `Bob` and `bob` are one name, that `é` sorts
after `e`, that `a` and `a ` are one key. Two engines never share their
collations' names, and a table from one name to another is a model: it
misses a case the first time a language it did not think of turns up,
and a case missed is a unique key that held on the source taking two
rows on the target, or a GROUP BY that counts twice. So nothing here
models a collation - the way `key_merges` asks a MySQL server its own
rather than modelling it: every question is put to the servers, in the
sandbox, under the column's own collation on each side, and their
answers are compared.

Two sets of questions are asked of each text column of the fixture's
tables (keys and unique columns first, `MOST_COLUMNS` at most):

* the kinds of difference (`CLASSES`), a few pairs of strings each -
  case, accents, trailing and leading spaces, full-width forms,
  expansions, the Turkish i, composed and decomposed accents,
  punctuation, digits against letters, upper against lower case in
  order, the empty string - and of each pair: are they equal, which
  sorts first, does one match the other as a LIKE pattern with no
  wildcards, does a GROUP BY take them for one;
* the column's own values: up to `MOST_VALUES` distinct values read from
  the hop's own source (read-only, through the hop's row filter), and on
  each side which of them the server takes for one (GROUP BY, then `=`)
  and the order it puts them in (ORDER BY).

Both go into one table on each sandbox server, whose column has the
column's type and collation. A string the column cannot hold - a
character outside its character set, the empty string Oracle keeps as
NULL - is seen when the table is read back, where a CAST would have
converted it without a word. The table is an ordinary one, not a
temporary one: the pairs are asked by joining it to itself, which MySQL
refuses for a temporary table; the sandbox is scratch, and the table is
dropped at once.

Which kind of difference two values have is read off the two strings
(`kinds`); whether they compare or sort alike is only ever the servers'
answer. A kind is held by the column's values where two values the
servers take for one, or put next to each other, differ by it.

The verdict per column:

* `diff`: its own values compare or sort differently on the target -
  the kinds, and up to `EXAMPLES` values, masked where the hop asks;
* `warn`: every value read compares and sorts alike, but the collations
  part on kinds of difference (named, with whether the values hold
  them: values written later would part), or the target refuses a LIKE
  the source answers;
* `ok`: every value read alike, and no kind of difference asked parts;
* not asked, with why: a collation the sandbox cannot be asked under, a
  type the source compares no value of, a column past `MOST_COLUMNS`. A
  column not asked keeps the database's line from `ok`.

Where the target's own collation does not agree, the target's other
collations are tried, each a way (`decide.Rung`) proved by the same
questions under it: the first that agrees with the source on the values
and on every kind of difference they hold is named in the advice, the
one that changes least first (its footprint: the column's own, then
another deterministic one, then - on PostgreSQL - a nondeterministic
ICU collation made in the target's sandbox for the proof, which turns
B-tree deduplication off, refuses LIKE before PostgreSQL 18, and must be
shared by a key that references the column). No candidate is timed: each
is asked the same questions, so the list's order - by footprint - stands.

A libc collation answers as the sandbox's C library sorts, which can be
another release than the one the hop's own server runs: the verdict is
the sandbox's answer for the collation named.
"""
import json
import re
import unicodedata
from contextlib import contextmanager
from dataclasses import dataclass, field

from .. import decide, masking

#: text columns asked per database, keys and unique columns first: each
#: is a table made and asked on both sandbox servers, and on the target's
#: again for every other collation tried
MOST_COLUMNS = 24
#: distinct values of a column read from the source and asked
MOST_VALUES = 5000
#: rows of a table read for its columns' values at most
MOST_ROWS = 200_000
#: a longer value is left out, and counted: a key that long is compared
#: by a prefix on some servers (MySQL's sort length, Oracle's collation
#: key), which would be their limit answering, not the collation
LONGEST = 400
#: values shown per column
EXAMPLES = 3

#: (key, what an operator reads, pairs of strings that differ by it and
#: by nothing else). No pattern character (`%`, `_`, `[`) and no
#: backslash: each pair is asked as a LIKE pattern too, and must mean
#: itself there
CLASSES = (
    ("case", "case", (("a", "A"), ("ab", "AB"), ("é", "É"))),
    ("accent", "accents", (("e", "é"), ("a", "å"),
                           ("n", "ñ"))),
    ("trailing", "trailing spaces", (("a", "a "), ("ab", "ab  "))),
    ("leading", "leading spaces", (("a", " a"),)),
    ("width", "full-width forms", (("A", "\uff21"), ("1", "\uff11"))),
    ("expand", "expansions (ß and ss, æ and ae)",
     (("ß", "ss"), ("æ", "ae"), ("œ", "oe"))),
    ("turkish", "the Turkish i", (("I", "ı"), ("i", "İ"))),
    ("compose", "composed and decomposed accents",
     (("\u00e9", "e\u0301"), ("\u00c5", "A\u030a"))),
    ("punct", "punctuation", (("co-op", "coop"), ("o'neil", "oneil"))),
    ("digits", "digits against letters in order",
     (("2", "10"), ("a2", "a10"), ("1", "a"))),
    ("upper", "upper against lower case in order",
     (("a", "B"), ("Z", "a"))),
    ("empty", "the empty string", (("", "a"), ("", " "))),
)
SAID = {k: s for k, s, _ in CLASSES}
SAID["held"] = "values the target cannot hold as they are"
SAID["other"] = "differences of no kind asked"
_AT = {k: i for i, (k, _, _) in enumerate(CLASSES)}

ORDER = {"ok": 0, "warn": 1, "not asked": 2, "diff": 3}


# ---- what kind of difference two strings have -------------------------------

#: letters that sort as two, in some collations
EXPANSIONS = (("ß", "ss"), ("ẞ", "SS"), ("æ", "ae"),
              ("Æ", "AE"), ("œ", "oe"), ("Œ", "OE"))


def _unaccented(s):
    return unicodedata.normalize("NFC", "".join(
        c for c in unicodedata.normalize("NFD", s)
        if not unicodedata.combining(c)))


def _narrow(c):
    d = unicodedata.decomposition(c)
    return chr(int(d.split()[1], 16)) if d.startswith(("<wide>",
                                                       "<narrow>")) else c


def _expanded(s):
    for a, b in EXPANSIONS:
        s = s.replace(a, b)
    return s


def _unpunctuated(s):
    return "".join(c for c in s if unicodedata.category(c)[0] != "P"
                   and unicodedata.category(c) != "Cf")


#: each kind of difference as what erases it, finest first: two strings
#: one of these makes equal differ by that kind
FOLDS = (
    ("compose", lambda s: unicodedata.normalize("NFC", s)),
    ("width", lambda s: "".join(map(_narrow, s))),
    ("turkish", lambda s: s.replace("ı", "i").replace("İ", "I")),
    ("expand", _expanded),
    ("accent", _unaccented),
    ("case", str.lower),
    ("punct", _unpunctuated),
    ("trailing", lambda s: s.rstrip(" ")),
    ("leading", lambda s: s.lstrip(" ")),
)


def _folded(s, keys):
    for k, f in FOLDS:
        if k in keys:
            s = f(s)
    return s


def kinds(x, y):
    """The kinds of difference between two strings (`CLASSES`' keys):
    the finest one that erased makes them one, or else the fewest that
    together do; and where none does, whether they part at a digit or at
    two letters of unlike case - what decides which sorts first. Read
    off the strings only to name the difference: whether a server takes
    them for one is its own answer, never this."""
    if x == y:
        return set()
    out = {"empty"} if "" in (x, y) else set()
    one = next((k for k, f in FOLDS if f(x) == f(y)), None)
    if one:
        out.add(one)
    else:
        keys = [k for k, _ in FOLDS]
        if _folded(x, keys) == _folded(y, keys):
            for k in reversed([k for k, _ in FOLDS]):
                rest = [o for o in keys if o != k]
                if _folded(x, rest) == _folded(y, rest):
                    keys = rest
            out.update(keys)
    if out - {"empty"}:
        # what makes them one also decides their order
        return out
    i = 0
    while i < min(len(x), len(y)) and x[i] == y[i]:
        i += 1
    a, b = x[i:i + 1], y[i:i + 1]
    if a.isdigit() or b.isdigit() or (i and x[i - 1].isdigit()
                                      and not (a and b)):
        out.add("digits")
    if a.isalpha() and b.isalpha() and a.isupper() != b.isupper() \
            and a.lower() != b.lower():
        out.add("upper")
    return out


def _said(keys):
    keys = sorted(set(keys), key=lambda k: (_AT.get(k, len(_AT)), k))
    return ", ".join(SAID.get(k, k) for k in keys)


def _lit(v):
    return "NULL" if v is None else repr(v)


def _first(e):
    return (str(e).strip().splitlines() or [type(e).__name__])[0][:160]


def _num(n):
    return ("no", "one", "two", "three", "four", "five", "six", "seven",
            "eight", "nine", "ten")[n] if 0 <= n <= 10 else f"{n:,}"


def _listed(shown):
    head = shown[:EXAMPLES]
    said = head[0] if len(head) == 1 else \
        ", ".join(head[:-1]) + " and " + head[-1]
    return said + (f" (and {len(shown) - len(head)} more)"
                   if len(shown) > len(head) else "")


# ---- a collation, and the answers under it ---------------------------------


class _NotAsked(Exception):
    """Why a column could not be asked: said as the reason, never `ok`."""


@dataclass(frozen=True)
class Collation:
    """A collation as one engine is asked under it."""
    name: str                   #: what the advice names
    said: str                   #: what the verdict says of it
    clause: str = ""            #: what a column takes it by
    eq: str = ""                #: Oracle: the sort `=`, GROUP BY, LIKE use
    order: str = ""             #: Oracle: the sort ORDER BY uses
    body: str = ""              #: PostgreSQL: how it is made, where made
    nondeterministic: bool = False

    @property
    def make(self):
        return f"CREATE COLLATION {self.name} ({self.body})" \
            if self.body else ""

    @property
    def sig(self):
        return f"{self.name}|{self.clause}|{self.eq}|{self.order}"


@dataclass
class Reading:
    """One side's answers, under one collation, for one column's rows."""
    said: str
    held: set                   #: ids the column keeps as they were given
    back: dict                  #: id -> what was read back
    pairs: dict                 #: (a, b) -> (a = b, a < b, a > b)
    like: dict                  #: (a, b) -> b LIKE a
    refused: str                #: why the server refused LIKE, or ""
    group: dict                 #: id -> the least id GROUP BY puts with it
    order: list                 #: the values' ids, as ORDER BY gives them


@dataclass
class _Layout:
    """The strings asked of one column, numbered alike on both sides."""
    texts: dict                 #: id -> string
    pairs: dict                 #: class key -> [(x id, y id)]
    data: list                  #: the column's values' ids, in order
    rows: list                  #: (id, class of the row, string)


def _layout(values):
    texts, pairs, rows, n = {}, {}, [], 0
    for key, _, got in CLASSES:
        for i, (x, y) in enumerate(got):
            ids = []
            for s in (x, y):
                n += 1
                texts[n] = s
                rows.append((n, f"{key}{i}", s))
                ids.append(n)
            pairs.setdefault(key, []).append(tuple(ids))
    data = []
    for v in values:
        n += 1
        texts[n] = v
        rows.append((n, "data", v))
        data.append(n)
    return _Layout(texts, pairs, data, rows)


# ---- one sandbox server -----------------------------------------------------

_ID = {"oracle": "number(10)"}
_CLS = {"mysql": "varchar(16) character set ascii collate ascii_bin",
        "postgres": 'varchar(16) collate "C"',
        "mssql": "varchar(16) collate Latin1_General_BIN2",
        "oracle": "varchar2(16)"}
_NAME = re.compile(r"^[A-Za-z0-9_$#]+$")
#: what makes a SQL Server collation's name one of a family
_MSSQL_SUFFIX = r"_(BIN2?|C[IS]_A[IS](_KS)?(_WS)?(_VSS)?(_SC)?(_UTF8)?)$"
#: PostgreSQL's nondeterministic ICU collations, made in the target's
#: sandbox for the proof: (name, ICU locale)
NONDETERMINISTIC = (
    ("migkit_cs", "und-u-ks-level3"),
    ("migkit_ci", "und-u-ks-level2"),
    ("migkit_ci_ai", "und-u-ks-level1"),
    ("migkit_cs_ai", "und-u-kc-ks-level1"),
    ("migkit_ci_punct", "und-u-ka-shifted-ks-level2"),
)


def _pgq(name):
    return '"' + str(name).replace('"', '""') + '"'


def _sql_text(v):
    return "'" + str(v).replace("'", "''") + "'"


class _Side:
    """One sandbox server, asked through the bench's runner on it: the
    tables made to ask it and the collations made for the proof, and how
    its engine names a column's type and a collation. `own` is the hop's
    own engine of this side, read only where the sandbox cannot carry
    something - a database's own collation, the sort a session defaults
    to."""

    def __init__(self, runner, own, own_side, db, word):
        self.run = runner
        self.engine = runner.engine
        self.own, self.own_side, self.db, self.word = own, own_side, db, word
        self.tables = []
        self.colls = {}           #: name -> None where made, else why not
        self.made = []            #: collations this proof made
        self.session = []         #: what puts the session back
        self.facts = {}
        self._n = 0
        self._default = None
        self._nls_own = None
        self._nls_here = None
        self._prepare()

    @property
    def conn(self):
        return self.run.conn

    def rows(self, sql, args=None):
        cur = self.conn.cursor()
        try:
            if args is None:
                cur.execute(sql)
            else:
                cur.execute(sql, args)
            return [tuple(r) for r in cur.fetchall()]
        finally:
            cur.close()

    def do(self, sql, args=None):
        cur = self.conn.cursor()
        try:
            if args is None:
                cur.execute(sql)
            else:
                cur.execute(sql, args)
        finally:
            cur.close()

    def ok(self):
        self.conn.commit()

    def undo(self):
        try:
            self.conn.rollback()
        except Exception:  # noqa: BLE001 - nothing to roll back
            pass

    def _own(self, sql):
        """A catalogue read on the hop's own server of this side, in a
        read-only session."""
        eng, side = self.own, self.own_side
        if self.engine == "postgres":
            conn = eng._conn(side, eng._d(side, self.db))
            try:
                conn.rollback()
                conn.set_session(readonly=True)
                with conn.cursor() as cur:
                    cur.execute(sql)
                    return [tuple(r) for r in cur.fetchall()]
            finally:
                conn.close()
        conn = eng._connect(side, self.db)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            return [tuple(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def _prepare(self):
        """The session as the questions need it, and what puts it back: a
        MySQL sort compares no more than `max_sort_length` bytes of a
        value; an Oracle session's own sort is what a LIKE is asked
        under, and is set back after."""
        try:
            if self.engine == "mysql":
                (n,), = self.rows("select @@session.max_sort_length")
                self.do("set session max_sort_length = 8388608")
                self.session.append(f"set session max_sort_length = {int(n)}")
            elif self.engine == "oracle":
                got = dict(self.rows(
                    "select parameter, value from nls_session_parameters"
                    " where parameter in ('NLS_SORT', 'NLS_COMP')"))
                self._nls_here = (str(got.get("NLS_COMP") or "BINARY"),
                                  str(got.get("NLS_SORT") or "BINARY"))
        except Exception:  # noqa: BLE001 - asked with the defaults
            self.undo()

    @contextmanager
    def linguistic(self, coll):
        """Oracle compares a LIKE by the session's sort: set to the
        collation's for the question, and back after."""
        if self.engine != "oracle":
            yield
            return
        eq = coll.eq or "BINARY"
        self.do(f"alter session set nls_comp ="
                f" {'BINARY' if eq == 'BINARY' else 'LINGUISTIC'}"
                f" nls_sort = {eq}")
        try:
            yield
        finally:
            comp, sort = self._nls_here or ("BINARY", "BINARY")
            if _NAME.match(comp) and _NAME.match(sort):
                self.do(f"alter session set nls_comp = {comp}"
                        f" nls_sort = {sort}")

    def key(self, expr, coll, which):
        """What `=`, GROUP BY (`eq`) or ORDER BY (`order`) compares: the
        column itself, which carries its collation - or on Oracle the
        collation's key of it, since a column there takes its sort from
        the session."""
        if self.engine != "oracle":
            return expr
        name = coll.eq if which == "eq" else coll.order
        return f"nlssort({expr}, 'NLS_SORT={name or 'BINARY'}')"

    # ---- the column's catalogue ------------------------------------------

    def _stored(self, name):
        return self.run.q(name)[1:-1] if self.engine == "oracle" else name

    def column(self, table, name):
        if table not in self.facts:
            self.facts[table] = self._read_facts(table)
        got = self.facts[table]
        for k in (name, self._stored(name), str(name).lower()):
            if k in got:
                return got[k]
        return None

    def _read_facts(self, table):
        """{column: {"name", "kind" (fixed, varying, as-is or none),
        "base", "declared", "charset", "collation", ...}} of one sandbox
        table: `kind` says how the table the questions are asked in
        declares the column - a fixed width compares as its engine pads
        it, `as-is` keeps the declared type (an enum, a domain), `none`
        is a type the engine compares no value of, and `why` says so."""
        out = {}
        e = self.engine
        leaf = str(table).split(".")[-1]
        if e == "mysql":
            for n, dt, ct, cs, co in self.rows(
                    "select column_name, data_type, column_type,"
                    " character_set_name, collation_name from"
                    " information_schema.columns where table_schema ="
                    " database() and table_name = %s", (leaf,)):
                dt = str(dt).lower()
                kind = "fixed" if dt == "char" else "as-is" \
                    if dt in ("enum", "set") else "varying"
                out[str(n)] = {"name": str(n), "kind": kind, "base": dt,
                               "declared": str(ct), "charset": cs,
                               "collation": co}
        elif e == "postgres":
            for n, base, declared, oid, coll, nsp, got in self.rows(
                    "select a.attname, coalesce(bt.typname, t.typname),"
                    " format_type(a.atttypid, a.atttypmod),"
                    " a.attcollation::bigint, coalesce(c.collname, ''),"
                    " coalesce(n.nspname, ''),"
                    " coalesce(to_jsonb(c)::text, '')"
                    " from pg_attribute a join pg_type t on t.oid ="
                    " a.atttypid left join pg_type bt on bt.oid ="
                    " t.typbasetype and t.typtype = 'd' left join"
                    " pg_collation c on c.oid = a.attcollation left join"
                    " pg_namespace n on n.oid = c.collnamespace where"
                    " a.attrelid = %s::regclass and a.attnum > 0 and not"
                    " a.attisdropped", (self.run.table(table),)):
                kind = "fixed" if base == "bpchar" else "varying" \
                    if base in ("text", "varchar", "name") else "as-is"
                out[str(n)] = {"name": str(n), "kind": kind, "base": base,
                               "declared": declared, "oid": int(oid or 0),
                               "collation": coll, "nsp": nsp,
                               "json": got}
        elif e == "mssql":
            for n, base, co in self.rows(
                    "select c.name, type_name(c.system_type_id),"
                    " c.collation_name from sys.columns c where"
                    " c.object_id = object_id(%s)",
                    (self.run.table(table),)):
                base = str(base).lower()
                kind = "fixed" if base in ("char", "nchar") else "varying" \
                    if base in ("varchar", "nvarchar") else "none" \
                    if base in ("text", "ntext") else "as-is"
                out[str(n).lower()] = {
                    "name": str(n), "kind": kind, "base": base,
                    "declared": base, "collation": co,
                    "why": f"SQL Server compares no {base} value"}
        elif e == "oracle":
            owner = self.run.eng._owner(self.run.side, self.run.db)
            name = self._stored(leaf)
            try:
                got = self.rows(
                    "select column_name, data_type, collation from"
                    " all_tab_cols where owner = :1 and table_name = :2",
                    (owner, name))
            except Exception:  # noqa: BLE001 - before 12.2: no column
                # collation, every column takes the session's
                self.undo()
                got = [(n, t, None) for n, t in self.rows(
                    "select column_name, data_type from all_tab_cols where"
                    " owner = :1 and table_name = :2", (owner, name))]
            for n, base, co in got:
                base = str(base).upper()
                kind = "fixed" if base in ("CHAR", "NCHAR") else "varying" \
                    if base in ("VARCHAR2", "NVARCHAR2", "VARCHAR") else \
                    "none" if base in ("CLOB", "NCLOB", "LONG") else "as-is"
                out[str(n)] = {"name": str(n), "kind": kind, "base": base,
                               "declared": base, "collation": co,
                               "why": f"Oracle compares no {base} value"
                                      " with ="}
        for k in list(out):
            out.setdefault(k.lower(), out[k])
        return out

    # ---- the column's collation ------------------------------------------

    def collation(self, info):
        """The collation the column is compared by on this side."""
        e, co = self.engine, info.get("collation")
        if e == "mysql":
            cs = info.get("charset")
            if not co or not cs:
                raise _NotAsked(f"the {self.word}'s column has no collation")
            if not (_NAME.match(co) and _NAME.match(cs)):
                raise _NotAsked(f"the {self.word}'s collation {co!r} cannot"
                                " be named in a statement")
            return Collation(co, co, f" character set {cs} collate {co}")
        if e == "postgres":
            if not info.get("oid"):
                raise _NotAsked(f"the {self.word}'s column has no collation")
            if info["oid"] == 100:
                return self._pg_default()
            got = json.loads(info.get("json") or "{}")
            provider = {"c": "libc", "i": "icu", "b": "builtin",
                        "d": "the database's"}.get(
                got.get("collprovider"), str(got.get("collprovider") or ""))
            locale = got.get("colllocale") or got.get("colliculocale") or \
                got.get("collcollate") or ""
            det = got.get("collisdeterministic", True)
            said = f'"{co}" ({provider}' + (f" {locale}" if locale else "") \
                + (", nondeterministic" if det is False else "") + ")"
            return Collation(co, said, f" collate {_pgq(info['nsp'])}."
                                       f"{_pgq(co)}",
                             nondeterministic=det is False)
        if e == "mssql":
            if not co:
                raise _NotAsked(f"the {self.word}'s column has no collation")
            if not _NAME.match(co):
                raise _NotAsked(f"the {self.word}'s collation {co!r} cannot"
                                " be named in a statement")
            return Collation(co, co, f" collate {co}")
        if e == "oracle":
            declared = str(co or "USING_NLS_COMP").upper()
            if declared.startswith("USING_NLS"):
                comp, sort = self._oracle_nls()
                if declared == "USING_NLS_COMP":
                    eq = sort if comp in ("LINGUISTIC", "ANSI") else "BINARY"
                    order = sort
                else:
                    eq = order = sort + declared[len("USING_NLS_SORT"):]
                said = (f"{declared} (comparing by {eq}" + (
                    f" and sorting by {order}" if order != eq else "")
                    + f", as the {self.word}'s sessions default to)")
            else:
                eq = order = declared
                said = declared
            if not (_NAME.match(eq) and _NAME.match(order)):
                raise _NotAsked(f"the {self.word}'s sort {eq!r} cannot be"
                                " named in a statement")
            return Collation(declared, said, "", eq=eq, order=order)
        raise _NotAsked(f"migkit asks no {e} server its collations yet")

    def _oracle_nls(self):
        """(NLS_COMP, NLS_SORT) the hop's own server gives a session:
        what a column that takes its collation from the session is
        compared by. The sandbox's are its image's, not the server's."""
        if self._nls_own is None:
            try:
                got = dict((str(k), str(v)) for k, v in self._own(
                    "select parameter, value from nls_session_parameters"
                    " where parameter in ('NLS_SORT', 'NLS_COMP')"))
            except Exception as e:  # noqa: BLE001 - said as the reason
                raise _NotAsked(f"the {self.word}'s own session sort could"
                                f" not be read: {_first(e)}")
            self._nls_own = (got.get("NLS_COMP", "BINARY").upper(),
                             got.get("NLS_SORT", "BINARY").upper())
        return self._nls_own

    def _pg_default(self):
        if self._default is None:
            try:
                self._default = self._pg_default_asked()
            except _NotAsked as e:
                self._default = e
        if isinstance(self._default, _NotAsked):
            raise self._default
        return self._default

    def _pg_default_asked(self):
        """The database's own collation, as the hop's own database has it:
        the sandbox's database was made with its server's defaults, which
        need not be the hop's. Asked as "default" where the two are one,
        and otherwise made in the sandbox from the hop's own locale."""
        sql = ("select to_jsonb(d)::text from pg_database d where datname"
               " = current_database()")
        here = json.loads(self.rows(sql)[0][0])
        try:
            there = json.loads(self._own(sql)[0][0])
        except Exception as e:  # noqa: BLE001 - said as the reason
            raise _NotAsked(f"the {self.word} database's own collation"
                            f" could not be read: {_first(e)}")

        def locale(d):
            return (d.get("datlocprovider") or "c",
                    d.get("datlocale") or d.get("daticulocale") or "",
                    d.get("datcollate") or "", d.get("datctype") or "",
                    d.get("daticurules") or "")
        provider, loc, collate, ctype, rules = locale(there)
        words = {"c": "libc", "i": "icu", "b": "builtin"}.get(provider,
                                                              provider)
        shown = loc if provider in ("i", "b") else collate
        said = f'"default" ({words} {shown}, the {self.word} database\'s own)'
        if locale(here) == locale(there):
            return Collation("default", said, ' collate "default"')
        if provider == "i":
            body = f"provider = icu, locale = {_sql_text(loc)}" + (
                f", rules = {_sql_text(rules)}" if rules else "")
        elif provider == "b":
            body = f"provider = builtin, locale = {_sql_text(loc)}"
        else:
            body = (f"lc_collate = {_sql_text(collate)}, lc_ctype ="
                    f" {_sql_text(ctype)}")
        made = Collation("migkit_db_default", said,
                         ' collate public."migkit_db_default"', body=body)
        why = self.make(made)
        if why:
            raise _NotAsked(f"the {self.word} database's own collation"
                            f" ({words} {shown}) cannot be made on its"
                            f" sandbox's server: {why}")
        return made

    def make(self, coll):
        """None where the collation is on this PostgreSQL sandbox (made
        here now if it was not), else why it cannot be."""
        if not coll.body:
            return None
        if coll.name in self.colls:
            return self.colls[coll.name]
        try:
            if not self.rows("select 1 from pg_collation where collname ="
                             " %s and collnamespace = 'public'::regnamespace",
                             (coll.name,)):
                self.do(f"create collation public.{_pgq(coll.name)}"
                        f" ({coll.body})")
                self.ok()
                self.made.append(coll.name)
            why = None
        except Exception as e:  # noqa: BLE001 - said as the reason
            self.undo()
            why = _first(e)
        self.colls[coll.name] = why
        return why

    def typed(self, info, width, coll):
        """The column's type in the table the questions are asked in: its
        own family at the width the strings need."""
        e, kind, base = self.engine, info["kind"], info.get("base") or ""
        if kind == "as-is":
            typ = info["declared"]
        elif e == "mysql":
            typ = f"char({min(width, 255)})" if kind == "fixed" \
                else f"varchar({min(width, 4000)})"
        elif e == "postgres":
            typ = f"character({width})" if kind == "fixed" else "text"
        elif e == "mssql":
            most = 4000 if base.startswith("n") else 8000
            typ = f"{base}({min(width, most)})"
        else:
            typ = {"CHAR": f"char({min(width, 2000)} char)",
                   "NCHAR": f"nchar({min(width, 1000)})",
                   "NVARCHAR2": f"nvarchar2({min(width, 2000)})"}.get(
                base, f"varchar2({min(width, 4000)} char)")
        return typ + coll.clause

    def candidates(self, info, own):
        """[(Collation, footprint, because)] this side offers the column,
        its own first: another deterministic collation of the same
        character set or family changes the column; a nondeterministic
        one changes more."""
        e = self.engine
        out = [(own, 0, "the target column's own collation")]
        more, later = [], []
        if e == "mysql":
            cs = info.get("charset") or "utf8mb4"
            names = [f"{cs}_{s}" for s in (
                "0900_as_cs", "0900_ai_ci", "0900_as_ci", "general_ci",
                "bin", "unicode_ci", "0900_bin", "uca1400_as_cs",
                "uca1400_ai_ci", "uca1400_as_ci", "unicode_520_ci",
                "general_cs")]
            there = {str(n) for (n,) in self.rows(
                "select collation_name from information_schema.collations"
                " where character_set_name = %s", (cs,))}
            more = [Collation(n, n, f" character set {cs} collate {n}")
                    for n in names if n in there and _NAME.match(n)]
        elif e == "postgres":
            there = {str(n) for (n,) in self.rows(
                "select collname from pg_collation where collname in ('C',"
                " 'und-x-icu') and collencoding in (-1, (select encoding"
                " from pg_database where datname = current_database()))")}
            more = [Collation(n, f'"{n}"', f" collate {_pgq(n)}")
                    for n in ("C", "und-x-icu") if n in there]
            later = [Collation(n, f"{n} (icu {loc}, nondeterministic)",
                               f' collate public."{n}"',
                               body=f"provider = icu, locale = '{loc}',"
                                    " deterministic = false",
                               nondeterministic=True)
                     for n, loc in NONDETERMINISTIC]
        elif e == "mssql":
            got = self.rows("select convert(varchar(128),"
                            " databasepropertyex(db_name(), 'Collation'))")
            family = []
            for n in (own.name, got[0][0] if got else None):
                if n:
                    p = re.sub(_MSSQL_SUFFIX, "", str(n), flags=re.I)
                    if p not in family:
                        family.append(p)
            names = [p + s for p in family for s in
                     ("_CI_AS", "_CS_AS", "_CI_AI", "_CS_AI", "_BIN2")]
            if names:
                there = {str(n) for (n,) in self.rows(
                    "select name from sys.fn_helpcollations() where name in"
                    f" ({', '.join(['%s'] * len(names))})", tuple(names))}
                more = [Collation(n, n, f" collate {n}") for n in names
                        if n in there and _NAME.match(n)]
        elif e == "oracle":
            more = [Collation(n, n, "", eq=n, order=n)
                    for n in ("BINARY", "BINARY_CI", "BINARY_AI")]
        seen = {own.sig, (own.eq, own.order) if e == "oracle" else None}
        for c in more:
            if c.sig not in seen and not (e == "oracle" and (c.eq, c.order)
                                          in seen):
                seen.add(c.sig)
                out.append((c, 1, ""))
        for c in later:
            if c.sig not in seen and c.name != own.name:
                seen.add(c.sig)
                out.append((c, 2, ""))
        return out

    # ---- the table the questions are asked in -------------------------------

    def create(self, info, coll, width):
        self._n += 1
        t = self.run.table(("migkit$coll_" if self.engine == "oracle"
                            else "migkit_coll_") + str(self._n))
        self.drop(t)
        try:
            self.do(f"create table {t} (id {_ID.get(self.engine, 'int')}"
                    f" primary key, cls {_CLS[self.engine]}, v"
                    f" {self.typed(info, width, coll)})")
            self.ok()
        except Exception as e:  # noqa: BLE001 - said as the reason
            self.undo()
            raise _NotAsked(f"the {self.word}'s server makes no column of"
                            f" it under {coll.said}: {_first(e)}")
        self.tables.append(t)
        return t

    def insert(self, table, rows):
        cur = self.conn.cursor()
        try:
            if self.engine == "postgres":
                from psycopg2.extras import execute_values
                execute_values(cur, f"insert into {table} (id, cls, v)"
                                    " values %s", rows, page_size=1000)
            elif self.engine == "oracle":
                cur.executemany(f"insert into {table} (id, cls, v) values"
                                " (:1, :2, :3)", rows)
            elif self.engine == "mssql":
                # a thousand rows is the most one VALUES takes
                for k in range(0, len(rows), 300):
                    part = rows[k:k + 300]
                    cur.execute(f"insert into {table} (id, cls, v) values "
                                + ", ".join(["(%s, %s, %s)"] * len(part)),
                                tuple(x for r in part for x in r))
            else:
                cur.executemany(f"insert into {table} (id, cls, v) values"
                                " (%s, %s, %s)", rows)
        finally:
            cur.close()

    def drop(self, table):
        sql = {"mssql": f"if object_id(N'{table}', N'U') is not null drop"
                        f" table {table}",
               "oracle": f"drop table {table} purge"}.get(
            self.engine, f"drop table if exists {table}")
        try:
            self.do(sql)
            self.ok()
        except Exception:  # noqa: BLE001 - not there
            self.undo()
        if table in self.tables:
            self.tables.remove(table)

    def close(self):
        for t in list(self.tables):
            self.drop(t)
        for name in self.made:
            try:
                self.do(f"drop collation if exists public.{_pgq(name)}")
                self.ok()
            except Exception:  # noqa: BLE001 - a scratch server's
                self.undo()
        self.made = []
        for s in self.session:
            try:
                self.do(s)
            except Exception:  # noqa: BLE001 - the session ends anyway
                self.undo()
        self.session = []
        try:
            self.ok()
        except Exception:  # noqa: BLE001 - nothing open
            pass


def _put(side, table, rows):
    """The rows into the table; [ids] the server refused (a character
    outside the column's character set, where the server refuses rather
    than converts), found by halving, so a refused one costs a few
    statements and not one statement per row."""
    if not rows:
        return []
    try:
        side.insert(table, rows)
        side.ok()
        return []
    except Exception:  # noqa: BLE001 - halved until the refused are found
        side.undo()
        if len(rows) == 1:
            return [rows[0][0]]
        mid = len(rows) // 2
        return _put(side, table, rows[:mid]) + _put(side, table, rows[mid:])


def _same(back, given, fixed):
    """Whether the column kept a string as it was given; a fixed-width
    column pads, so its trailing spaces are not what it keeps."""
    if back is None:
        return False
    if fixed:
        return str(back).rstrip(" ") == given.rstrip(" ")
    return back == given


def _ask(side, info, coll, lay):
    """Every question, under `coll`, on one side: the rows in a table of
    their own, read back, then asked; the table dropped after."""
    width = max([1] + [len(s.encode("utf-8")) for _, _, s in lay.rows]) + 1
    t = side.create(info, coll, width)
    try:
        _put(side, t, lay.rows)
        back = {int(i): v for i, v in side.rows(f"select id, v from {t}")}
        fixed = info["kind"] == "fixed"
        held = {i for i, _, s in lay.rows
                if i in back and _same(back[i], s, fixed)}

        def eq(x):
            return side.key(x, coll, "eq")

        def order(x):
            return side.key(x, coll, "order")
        pairs_of = (f" from {t} a join {t} b on b.cls = a.cls and b.id <>"
                    " a.id where a.cls <> 'data' and b.cls <> 'data'")
        pairs = {(int(a), int(b)): (int(e), int(lt), int(gt))
                 for a, b, e, lt, gt in side.rows(
                     f"select a.id, b.id, case when {eq('a.v')} ="
                     f" {eq('b.v')} then 1 else 0 end, case when"
                     f" {order('a.v')} < {order('b.v')} then 1 else 0 end,"
                     f" case when {order('a.v')} > {order('b.v')} then 1"
                     " else 0 end" + pairs_of)}
        like, refused = {}, ""
        try:
            with side.linguistic(coll):
                like = {(int(a), int(b)): bool(int(m))
                        for a, b, m in side.rows(
                            "select a.id, b.id, case when b.v like a.v then"
                            " 1 else 0 end" + pairs_of)}
        except Exception as e:  # noqa: BLE001 - a refusal is an answer
            side.undo()
            refused = _first(e)
        group = {int(i): int(m) for i, m in side.rows(
            f"select r.id, g.m from {t} r join (select cls, {eq('v')} as k,"
            f" min(id) as m from {t} group by cls, {eq('v')}) g on g.cls ="
            f" r.cls and g.k = {eq('r.v')}")}
        seq = [int(i) for (i,) in side.rows(
            f"select id from {t} where cls = 'data' order by {order('v')},"
            " id")]
    except _NotAsked:
        raise
    except Exception as e:  # noqa: BLE001 - said as the reason
        side.undo()
        raise _NotAsked(f"the {side.word}'s server could not be asked under"
                        f" {coll.said}: {_first(e)}")
    finally:
        side.drop(t)
    return Reading(coll.said, held, back, pairs, like, refused, group, seq)


# ---- the two sides' answers compared ----------------------------------------


@dataclass
class Parting:
    """Where the target's answers part from the source's."""
    classes: dict = field(default_factory=dict)   #: kind -> the first seen
    like: str = ""              #: the target refuses a LIKE the source answers
    count: int = 0              #: values that compare or sort differently
    kinds: set = field(default_factory=set)
    examples: list = field(default_factory=list)

    def value(self, found, words):
        self.count += 1
        self.kinds |= found or {"other"}
        if len(self.examples) < EXAMPLES:
            self.examples.append(words)


def _pair_parts(src, dst, x, y, texts):
    a, b = _lit(texts[x]), _lit(texts[y])
    s, d = src.pairs.get((x, y)), dst.pairs.get((x, y))
    if s and d:
        if s[0] != d[0]:
            return (f"{a} and {b} are {'equal' if s[0] else 'unequal'} on"
                    f" the source and {'equal' if d[0] else 'unequal'} on"
                    " the target")
        if s[1:] != d[1:]:
            def rel(t):
                return "before" if t[1] else "after" if t[2] else "level with"
            return (f"{a} sorts {rel(s)} {b} on the source and {rel(d)} it"
                    " on the target")
    sg = src.group.get(x, x) == src.group.get(y, y)
    dg = dst.group.get(x, x) == dst.group.get(y, y)
    if sg != dg:
        return (f"GROUP BY takes {a} and {b} for {'one' if sg else 'two'} on"
                f" the source and {'one' if dg else 'two'} on the target")
    if not (src.refused or dst.refused):
        for p, q in ((x, y), (y, x)):
            sl, dl = src.like.get((p, q)), dst.like.get((p, q))
            if sl is not None and dl is not None and sl != dl:
                return (f"{_lit(texts[q])} LIKE {_lit(texts[p])} is"
                        f" {'true' if sl else 'false'} on the source and"
                        f" {'true' if dl else 'false'} on the target")
    return ""


def _groups(rd, ids):
    out = {}
    for i in ids:
        out.setdefault(rd.group.get(i, i), []).append(i)
    return list(out.values())


def _runs(rd, keep):
    """The values in the order ORDER BY gives, those it takes for one
    together."""
    runs, last = [], None
    for i in rd.order:
        if i not in keep:
            continue
        g = rd.group.get(i, i)
        if runs and g == last:
            runs[-1].append(i)
        else:
            runs.append([i])
            last = g
    return runs


def compare(src, dst, lay, show=_lit):
    """Where `dst` parts from `src`: per kind of difference, the first
    pair answered otherwise (a string the target cannot hold is a pair
    answered otherwise); and the column's own values - taken for one on
    one side and kept apart on the other, or sorted the other way round.
    A pair the source cannot hold is no answer to keep, and is passed
    over. `show` is how a value of the column is written in the words."""
    out = Parting()
    for key, pairs in lay.pairs.items():
        for x, y in pairs:
            if x not in src.held or y not in src.held:
                continue
            if x not in dst.held or y not in dst.held:
                lost = x if x not in dst.held else y
                said = (f"the target keeps {_lit(lay.texts[lost])} as"
                        f" {_lit(dst.back.get(lost))}")
            else:
                said = _pair_parts(src, dst, x, y, lay.texts)
            if said:
                out.classes[key] = said
                break
    if not src.refused and dst.refused:
        out.like = dst.refused
    ids = [i for i in lay.data if i in src.held]
    for i in ids:
        if i not in dst.held:
            out.value({"held"}, f"the target keeps {show(lay.texts[i])} as"
                                f" {show(dst.back.get(i))}")
    ids = [i for i in ids if i in dst.held]
    keep = set(ids)
    for mine, theirs, split in ((src, dst, True), (dst, src, False)):
        for members in _groups(mine, ids):
            if len(members) < 2:
                continue
            there = {}
            for i in members:
                there.setdefault(theirs.group.get(i, i), []).append(i)
            if len(there) < 2:
                continue
            firsts = [v[0] for v in there.values()]
            shown = _listed([show(lay.texts[i]) for i in members])
            words = (f"{shown} are one value on the source and"
                     f" {_num(len(there))} on the target") if split else (
                f"{shown} are {_num(len(there))} values on the source and"
                " one on the target")
            out.value(kinds(lay.texts[firsts[0]], lay.texts[firsts[1]]),
                      words)
    ranks = {i: n for n, run in enumerate(_runs(dst, keep)) for i in run}
    runs = _runs(src, keep)
    for a, b in zip(runs, runs[1:]):
        a = [i for i in a if i in ranks]
        b = [i for i in b if i in ranks]
        if not a or not b:
            continue
        x, y = max(a, key=ranks.get), min(b, key=ranks.get)
        if ranks[x] > ranks[y]:
            out.value(kinds(lay.texts[x], lay.texts[y]),
                      f"{show(lay.texts[x])} sorts before"
                      f" {show(lay.texts[y])} on the source and after it on"
                      " the target")
    return out


def present(readings, lay):
    """The kinds of difference the column's values hold, by the servers'
    own answers: two values a side takes for one, or puts next to each
    other, and the kinds they differ by."""
    out = set()
    data = set(lay.data)
    for rd in readings:
        for members in _groups(rd, [i for i in lay.data if i in rd.held]):
            for j in members[1:]:
                out |= kinds(lay.texts[members[0]], lay.texts[j])
        seq = [i for i in rd.order if i in rd.held and i in data]
        for x, y in zip(seq, seq[1:]):
            out |= kinds(lay.texts[x], lay.texts[y])
    if any(lay.texts[i] == "" for i in lay.data):
        out.add("empty")
    return out


# ---- the verdict ------------------------------------------------------------


@dataclass
class _Verdict:
    said: str
    status: str
    text: str
    hint: str = ""

    def line(self):
        return f"{self.said}: {self.status} - {self.text}"


def judge(said, parting, held, counted, where):
    """A column's verdict from where the target parts (`compare`) and the
    kinds its values hold (`present`)."""
    if parting.count:
        return _Verdict(said, "diff", f"its own values compare or sort"
                        f" differently on the target"
                        f" ({_said(parting.kinds)}) under {where}: "
                        + " / ".join(parting.examples))
    bits = []
    on_held = [k for k in parting.classes if k in held]
    on_rest = [k for k in parting.classes if k not in held]
    if on_held:
        first = sorted(on_held, key=_AT.get)[0]
        bits.append(f"but the collations part on {_said(on_held)}, which"
                    f" its values hold ({parting.classes[first]})")
    if on_rest:
        first = sorted(on_rest, key=_AT.get)[0]
        bits.append(f"{'and' if on_held else 'but'} the collations part on"
                    f" {_said(on_rest)}, which none of its values holds:"
                    f" values written later would part"
                    f" ({parting.classes[first]})")
    if parting.like:
        bits.append(f"{'and' if bits else 'but'} the target refuses LIKE on"
                    f" it ({parting.like}) where the source answers")
    head = f"every value read ({counted}) compares and sorts alike under" \
           f" {where}"
    if bits:
        return _Verdict(said, "warn", head + ", " + ", ".join(bits))
    return _Verdict(said, "ok", head + f", and the collations part on none"
                    f" of the {len(CLASSES)} kinds of difference asked")


def _columns(bench):
    """[(rank, source table, column, target table, target column)] of
    every text column of the fixture's tables: keys first, then unique
    columns - a collation that takes two keys for one loses a row - then
    the rest."""
    out = []
    for t, _ in bench.tables.items():
        wa, wb = bench.a.watched.get(t), bench.b.watched.get(t)
        if wa is None or wb is None:
            continue
        key = set(wa.key or [])
        try:
            unique = {c for _, u, cols, _ in
                      bench.pair.src_engine.neutral_indexes("src", bench.db,
                                                            t)
                      if u for c in cols}
        except Exception:  # noqa: BLE001 - the key alone first
            unique = set()
        for (c, cls), (dc, _) in zip(wa.columns, wb.columns):
            if cls != "text":
                continue
            rank = 0 if c in key else 1 if c in unique else 2
            out.append((rank, t, c, wb.name, dc))
    out.sort(key=lambda x: x[0])
    return out


def _values(bench, table, columns):
    """({column: its distinct values, sorted}, {column: values left out as
    longer than `LONGEST`}, whether the read stopped at `MOST_ROWS`) - of
    the columns' values on the hop's own source, through the hop's row
    filter, read once for all of a table's columns."""
    pair, db = bench.pair, bench.db
    where = pair._row_scope(db, table)[0]
    seen = {c: set() for c in columns}
    long = {c: set() for c in columns}
    read, capped = 0, False
    got = pair.src_engine._every_row("src", db, table,
                                     [(c, "text") for c in columns], where)
    try:
        for rows in got:
            for r in rows:
                read += 1
                for c, v in zip(columns, r):
                    if v is None:
                        continue
                    if isinstance(v, (bytes, bytearray, memoryview)):
                        v = bytes(v).decode("utf-8", "replace")
                    v = str(v)
                    if len(v) > LONGEST:
                        if len(long[c]) < MOST_VALUES:
                            long[c].add(v)
                    elif len(seen[c]) < MOST_VALUES:
                        seen[c].add(v)
            if all(len(s) >= MOST_VALUES for s in seen.values()):
                break
            if read >= MOST_ROWS:
                capped = True
                break
    finally:
        close = getattr(got, "close", None)
        if close is not None:
            close()
    return ({c: sorted(s) for c, s in seen.items()},
            {c: len(s) for c, s in long.items()}, capped)


def _climb(b, di, own, lay, src, held, readings, work, strict):
    """The `decide.Choice` of the target's collations for the column:
    each a rung proved by asking its questions - agreeing with the source
    on the column's values and on every kind of difference they hold, or
    with `strict` on every kind asked."""
    cands = b.candidates(di, own)

    def reading(c):
        if c.sig not in readings:
            try:
                readings[c.sig] = _ask(b, di, c, lay)
            except _NotAsked as e:
                readings[c.sig] = str(e)
        return readings[c.sig]

    def proof(c):
        def asked(facts):
            rd = reading(c)
            if isinstance(rd, str):
                return rd
            p = compare(src, rd, lay)
            if p.count:
                return ("the column's own values compare or sort"
                        f" differently under it ({_said(p.kinds)})")
            holds = held | present([rd], lay)
            parts = [k for k in p.classes if strict or k in holds]
            if parts:
                return (f"it parts from the source on {_said(parts)}"
                        + ("" if strict else
                           ", which the column's values hold"))
            return None
        return asked

    rungs = []
    for c, footprint, because in cands:
        needs = ()
        if c.body:
            needs = (decide.Need(
                "to be made on the target's server",
                lambda f, c=c: b.make(c) is None,
                lambda f, c=c: f"the target's server cannot make {c.name}:"
                               f" {b.make(c)}"),)
        rungs.append(decide.Rung(name=c.sig, said=f"the collation {c.said}",
                                 needs=needs, prove=proof(c),
                                 because=because, footprint=footprint))
    # no candidate is timed - each is asked the same questions - so the
    # list's order stands, and it goes by what each changes
    rungs.sort(key=lambda r: r.footprint)
    got = decide.climb(work, rungs, decide.Facts())
    return got, {c.sig: c for c, _, _ in cands}


def _hint(target, got, cands, held, strict):
    if got.rung is None:
        if strict:
            return ""
        return (f"no collation the target offers agrees with the source on"
                f" {target}'s values ({got.reason[:300]})")
    c = cands[got.rung.name]
    made = f" ({c.make})" if c.make else ""
    if strict:
        out = (f"so values written later compare alike too, give {target}"
               f" the collation {c.name} on the target{made}: it agrees"
               " with the source on every kind of difference asked")
    else:
        out = (f"give {target} the collation {c.name} on the target{made}:"
               " it agrees with the source on every kind of difference its"
               " values hold" + (f" ({_said(held)})" if held else ""))
    if c.nondeterministic:
        out += (" - a nondeterministic collation turns B-tree deduplication"
                " off, refuses LIKE before PostgreSQL 18, and a key that"
                " references the column must share it")
    return out


def _column(bench, a, b, t, c, dt, dc, values):
    said = f"{t}.{c}"
    si, di = a.column(t, c), b.column(dt, dc)
    if si is None or di is None:
        raise _NotAsked("its catalogue entry could not be read on the"
                        f" {'source' if si is None else 'target'}'s sandbox")
    if si["kind"] == "none":
        raise _NotAsked(f"{si['why']}: nothing to hold the target to")
    if di["kind"] == "none":
        raise _NotAsked(f"{di['why']}: a query that compares or sorts it"
                        " fails on the target")
    scoll, dcoll = a.collation(si), b.collation(di)
    lay = _layout(values)

    def show(v):
        return "NULL" if v is None else repr(masking.shown(bench.hop, t, c,
                                                           v))
    src = _ask(a, si, scoll, lay)
    own = _ask(b, di, dcoll, lay)
    readings = {dcoll.sig: own}
    p = compare(src, own, lay, show)
    held = present([src, own], lay)
    counted = f"{len(lay.data):,} distinct value" + (
        "s" if len(lay.data) != 1 else "")
    v = judge(said, p, held, counted,
              f"{scoll.said} on the source and {dcoll.said} on the target")
    lost = [i for i in lay.data if i not in src.held]
    if lost:
        # a value the source's own column type did not keep as it was read
        # was asked of neither side: what was compared is not every value
        v.text += (f" ({len(lost):,} value{'s' if len(lost) != 1 else ''}"
                   " the source's sandbox column did not keep as read, not"
                   " compared)")
        if v.status == "ok":
            v.status = "warn"
    target = f"{str(dt).split('.')[-1]}.{dc}"
    if v.status in ("diff", "warn"):
        strict = v.status == "warn"
        if not strict or p.classes:
            got, cands = _climb(b, di, dcoll, lay, src, held, readings,
                                said, strict)
            if got.rung is None or got.rung.name != dcoll.sig:
                v.hint = _hint(target, got, cands, held, strict)
    return v


def _prove(bench, a, b):
    verdicts, notes = [], []
    chosen = _columns(bench)
    for _, t, c, _, _ in chosen[MOST_COLUMNS:]:
        verdicts.append(_Verdict(f"{t}.{c}", "not asked", f"past the"
                                 f" {MOST_COLUMNS} text columns asked per"
                                 " database, keys and unique columns"
                                 " first"))
    by_table = {}
    for _, t, c, dt, dc in chosen[:MOST_COLUMNS]:
        by_table.setdefault(t, []).append((c, dt, dc))
    for t, cols in by_table.items():
        try:
            values, long, capped = _values(bench, t, [c for c, _, _ in cols])
        except (Exception, SystemExit) as e:  # noqa: BLE001 - said; a
            # filter with no translation stops by SystemExit
            for c, _, _ in cols:
                verdicts.append(_Verdict(f"{t}.{c}", "not asked", "its"
                                         " values could not be read from"
                                         f" the source: {_first(e)}"))
            continue
        if capped:
            notes.append(f"{t}: the values of its first {MOST_ROWS:,} rows"
                         " read")
        for c, dt, dc in cols:
            try:
                v = _column(bench, a, b, t, c, dt, dc, values[c])
            except _NotAsked as e:
                v = _Verdict(f"{t}.{c}", "not asked", str(e))
            except Exception as e:  # noqa: BLE001 - said, never ok
                a.undo()
                b.undo()
                v = _Verdict(f"{t}.{c}", "not asked",
                             f"it could not be asked: {_first(e)}")
            if v.status != "not asked" and len(values[c]) >= MOST_VALUES:
                v.text += (f" (the first {MOST_VALUES:,} distinct values"
                           " read)")
            if long[c]:
                v.text += (f" ({long[c]:,} value{'s' if long[c] != 1 else ''}"
                           f" longer than {LONGEST} characters left out)")
            verdicts.append(v)
    if a.engine == "oracle" and verdicts:
        notes.append("the empty string is not asked of the source: Oracle"
                     " keeps '' as NULL, so none of its values is one")
    return verdicts, notes


def _result(scope, verdicts, notes):
    from ..engines.base import Result
    if not verdicts:
        return Result("schema", scope, "skip", "no text column in the"
                      " fixture's tables: no collation to compare", "", "")
    counts = {}
    for v in verdicts:
        counts[v.status] = counts.get(v.status, 0) + 1
    shown = sorted(verdicts, key=lambda v: -ORDER[v.status])
    detail = (f"{len(verdicts)} text column"
              f"{'s' if len(verdicts) != 1 else ''}: "
              + ", ".join(f"{counts[s]} {s}" for s in
                          ("diff", "not asked", "warn", "ok")
                          if counts.get(s)))
    detail += "; " + "; ".join(v.line() for v in shown[:10])
    if len(shown) > 10:
        detail += f"; ... {len(shown) - 10} more"
    if notes:
        detail += "; " + "; ".join(notes)
    status = {"ok": "ok", "warn": "warn", "not asked": "warn",
              "diff": "diff"}[shown[0].status]
    hints = [v.hint for v in shown if v.hint]
    return Result("schema", scope, status, detail, "", "; ".join(hints[:6]))


def prove(bench):
    """[Result] - one for the database, check "schema", scope
    "<db> collations": every text column of the fixture's tables held to
    the source's collation by the questions above, asked in the bench's
    two sandbox servers; the hop's own servers are only read."""
    from ..engines.base import Result
    scope = f"{bench.db} collations"
    sides = []
    try:
        sides.append(_Side(bench.a, bench.pair.src_engine, "src", bench.db,
                           "source"))
        sides.append(_Side(bench.b, bench.pair.dst_engine, "dst", bench.db,
                           "target"))
        verdicts, notes = _prove(bench, *sides)
    except Exception as e:  # noqa: BLE001 - said, never ok
        return [Result("schema", scope, "warn", "the collations could not be"
                       f" compared: {_first(e)}", "", "")]
    finally:
        for s in sides:
            s.close()
    return [_result(scope, verdicts, notes)]

"""PostgreSQL's PL/pgSQL (and `language sql`) routines as MySQL stored
programs, by migkit's own rules - the mirror of `mysql_pg`.

PL/pgSQL's statement grammar is read here, token by token, so every
statement keeps its place in the body (the reducer cuts the body by it,
`spans`); PostgreSQL's own parser reads the same body first and the two
readings are held to each other statement by statement - kind, line,
whether it selects INTO and STRICT - so a body read here otherwise than
PostgreSQL reads it is refused, not converted. (Its own tree cannot be
used alone: measured on the installed build, it keeps no type modifier -
`numeric(10,2)` comes back as `numeric` -, no variable a bare `RETURN x`
returns, no block a declaration belongs to and no offset of anything.)
Every expression and SQL statement goes to the query translator, with the
rules below applied on its tree; what has no rule is refused by name - the
construct and its line - never passed through.

The rules that keep meaning, not only syntax:

* an integer divided by an integer is an integer cut toward zero
  (`DIV`), where MySQL's `/` answers a decimal; a division whose operands'
  types are not known is refused. A division by zero raises in
  PostgreSQL and answers NULL in MySQL, so where the divisor is the
  routine's own values and the division is not under a condition, the
  statement is preceded by a check raising `division_by_zero`;
* `SELECT ... INTO` with no row sets its targets to NULL and with several
  takes the first; MySQL leaves them as they were and refuses several. So
  the targets are set to NULL first and the query takes one row. `INTO
  STRICT` raises P0002 and P0003 with PostgreSQL's own messages;
* an exception block rolls back everything its protected part did before
  its handler runs; a MySQL handler rolls back only the statement that
  failed. So only a protected part that writes nothing is carried (an
  EXIT handler in a block of its own), and one that writes is refused;
* a CALL of a PostgreSQL procedure is all or nothing; a MySQL procedure
  that fails keeps what its earlier statements wrote. So a procedure that
  writes before something that can still fail sets a savepoint in its
  caller's transaction and rolls back to it before passing the error on;
* PostgreSQL compares text exactly; MySQL's default collation is blind to
  case and accents. Every string a routine holds is declared with the
  binary collation, which also wins over a column's case-blind one where
  the two meet (MySQL's rule for mixing a `_bin` collation with a `_ci`
  one) - so an order comparison of text follows code points;
* `greatest`/`least` ignore NULLs, `concat` skips them, `log(x)` is base
  10, `length` counts characters, `substring` from before the first
  character counts from it, `date + integer` adds days, `||` and `::text`
  write a boolean as `true`/`false` and RAISE as `t`/`f`, a timestamp
  without its zero fraction and a numeric of no declared scale without
  the zeros MySQL's DECIMAL(65,30) pads it with;
* `ON CONFLICT (k)` is `ON DUPLICATE KEY UPDATE` only where the table has
  that one unique key (which key MySQL hits first is its own choice), and
  `UPDATE`'s row count is the rows it found, as PostgreSQL counts, not the
  rows MySQL changed;
* a trigger answers several events in PostgreSQL and one in MySQL, so it
  is made once per event, `TG_OP` the event's own word and the branches
  for the other events left out (MySQL refuses a trigger naming a row its
  event has not).

A FOR loop over a query, a `%TYPE` of a column and a record's fields need
the target's column types: `Context.column_types`, where the move gives
them ({table: {column: MySQL type}}), else they are refused.
"""
import json
import re

from sqlglot import exp
from sqlglot.dialects.mysql import MySQL

from .plpgsql import MYSQL_ERRNO, Converted, Emitter
from .translate import Refused, generate, parse


class _RulesMySQL(MySQL):
    """MySQL as the rules write it: an integer division as `DIV`, which
    the translator writes as a rounding cast (7 DIV 2 is 3, CAST(3.5 AS
    SIGNED) is 4)."""

    class Generator(MySQL.Generator):
        TRANSFORMS = {**MySQL.Generator.TRANSFORMS,
                      exp.IntDiv: lambda self, e: self.binary(e, "DIV")}


#: the collation every string of a converted routine is held in
BINARY = " CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin"

#: MySQL's reserved words, and the words its SET reads as a statement of
#: its own (`SET names = ...` sets the connection's character set): a
#: variable or label named one is quoted
_RESERVED = frozenset(w.lower() for w in MySQL.Generator.RESERVED_KEYWORDS) \
    | frozenset(("names", "password", "character", "charset", "transaction",
                 "global", "session", "local", "persist", "persist_only",
                 "resource", "role", "default"))

# ---- reading: tokens -------------------------------------------------------

_LEX = re.compile(r"""
 (?P<ws>\s+)
|(?P<line>--[^\n]*)
|(?P<block>/\*)
|(?P<dollar>\$(?:[A-Za-z_\x80-￿][\w\x80-￿]*)?\$)
|(?P<estring>[Ee]'(?:[^'\\]|\\.|'')*')
|(?P<string>(?:[BbXxNn]|[Uu]&)?'(?:[^']|'')*')
|(?P<qident>(?:[Uu]&)?"(?:[^"]|"")*")
|(?P<param>\$\d+)
|(?P<number>(?:\d+\.(?!\.)\d*|\.\d+|\d+)(?:[Ee][+-]?\d+)?)
|(?P<word>[A-Za-z_\x80-￿][\w$\x80-￿]*)
|(?P<op>:=|::|\.\.|[+\-*/<>=~!@\#%^&|`?]+)
|(?P<punct>[;,()\[\].:])
""", re.X | re.S)


class Tok:
    __slots__ = ("kind", "text", "start", "end", "line")

    def __init__(self, kind, text, start, end, line):
        self.kind, self.text = kind, text
        self.start, self.end, self.line = start, end, line

    @property
    def word(self):
        return self.text.lower() if self.kind == "word" else None


def _lex(text):
    out, i, line = [], 0, 1
    while i < len(text):
        m = _LEX.match(text, i)
        if not m:
            raise Refused(f"its body could not be read at line {line}:"
                          f" {text[i:i + 20]!r}")
        kind, end = m.lastgroup, m.end()
        if kind == "block":
            depth, j = 1, end
            while depth and j < len(text):
                if text.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif text.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            if depth:
                raise Refused(f"its body could not be read: a comment at"
                              f" line {line} does not end")
            end = j
        elif kind == "dollar":
            close = text.find(m.group(), end)
            if close < 0:
                raise Refused(f"its body could not be read: a quoted string"
                              f" at line {line} does not end")
            end, kind = close + len(m.group()), "string"
        elif kind == "estring":
            kind = "string"
        if kind not in ("ws", "line", "block"):
            out.append(Tok(kind, text[i:end], i, end, line))
        line += text.count("\n", i, end)
        i = end
    return out


def _literal(tok, line):
    """The value of a string literal token."""
    text = tok.text
    if text.startswith("$"):
        tag = text[:text.index("$", 1) + 1]
        return text[len(tag):-len(tag)]
    if text[0] in "Ee":
        out, body, k = [], text[2:-1], 0
        simple = {"b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t"}
        while k < len(body):
            c = body[k]
            if c == "\\" and k + 1 < len(body):
                nxt = body[k + 1]
                if nxt in simple:
                    out.append(simple[nxt])
                elif nxt.isalnum():
                    raise Refused(f"line {line}: a string escape (\\{nxt})"
                                  " migkit's rules do not read")
                else:
                    out.append(nxt)
                k += 2
            elif c == "'" and body[k + 1:k + 2] == "'":
                out.append("'")
                k += 2
            else:
                out.append(c)
                k += 1
        return "".join(out)
    if text[0] in "BbXxUu":
        raise Refused(f"line {line}: a bit, hex or Unicode-escaped string"
                      " migkit's rules do not read")
    if text[0] in "Nn":
        text = text[1:]
    return text[1:-1].replace("''", "'")


# ---- reading: statements ---------------------------------------------------

#: what a statement list ends at
_ENDS = {"end", "else", "elsif", "elseif", "when", "exception"}

#: statements that are SQL, by their first word
_SQL = {"select", "with", "insert", "update", "delete", "merge", "truncate",
        "values", "table", "lock", "create", "drop", "alter", "grant",
        "revoke", "set", "reset", "show", "notify", "listen", "unlisten",
        "analyze", "vacuum", "cluster", "refresh", "comment", "explain",
        "copy", "import", "prepare", "deallocate", "discard", "savepoint",
        "release", "start", "begin", "security", "load", "checkpoint",
        "reindex", "declare"}


class Node:
    """One statement: its kind (PostgreSQL's own name for it), its parts,
    and where its text is."""

    def __init__(self, kind, start, line, **a):
        self.kind, self.start, self.line, self.a = kind, start, line, a
        self.end = start

    def __getattr__(self, name):
        try:
            return self.__dict__["a"][name]
        except KeyError:
            raise AttributeError(name) from None

    def children(self):
        """The statements directly inside this one."""
        a = self.a
        out = list(a.get("body") or [])
        for branch in a.get("branches") or a.get("whens") or []:
            out += branch[1]
        out += a.get("otherwise") or []
        for handler in a.get("handlers") or []:
            out += handler[1]
        return out

    def walk(self):
        yield self
        for child in self.children():
            yield from child.walk()


class Decl:
    """One declaration of a block."""

    def __init__(self, what, name, line, **a):
        self.what, self.name, self.line = what, name, line
        self.type = a.get("type")
        self.constant = a.get("constant", False)
        self.notnull = a.get("notnull", False)
        self.default = a.get("default")
        self.target = a.get("target")
        self.query = a.get("query")
        self.args = a.get("args")


class _Reader:
    def __init__(self, text):
        self.text = text
        self.t = _lex(text)
        self.i = 0
        self.directives = {}

    # ---- tokens --------------------------------------------------------

    def tok(self, k=0):
        j = self.i + k
        return self.t[j] if 0 <= j < len(self.t) else None

    def w(self, k=0):
        t = self.tok(k)
        return t.word if t is not None else None

    def is_(self, kind, text, k=0):
        t = self.tok(k)
        return t is not None and t.kind == kind and t.text == text

    def at_end(self):
        return self.i >= len(self.t)

    def pos(self):
        t = self.tok()
        return t.start if t is not None else len(self.text)

    def line(self):
        t = self.tok()
        return t.line if t is not None else self.text.count("\n") + 1

    def eat(self, *words):
        for k, word in enumerate(words):
            if self.w(k) != word:
                return False
        self.i += len(words)
        return True

    def need(self, *words):
        if not self.eat(*words):
            got = self.tok().text if self.tok() else "the end"
            raise Refused(f"its body could not be read: expected"
                          f" {' '.join(words).upper()} at line"
                          f" {self.line()}, found {got}")

    def done_at(self, start):
        return self.t[self.i - 1].end if self.i > 0 else start

    def name(self):
        """A name as PostgreSQL folds it: bare in lower case, quoted as
        written."""
        t = self.tok()
        if t is None or t.kind not in ("word", "qident"):
            raise Refused(f"its body could not be read: a name was expected"
                          f" at line {self.line()}")
        self.i += 1
        return _name(t)

    def scan(self, words=(), ops=(), comma=False):
        """Advance to the first token at depth 0 that is one of `words`,
        one of `ops`, a semicolon or (with `comma`) a comma; the text
        passed over."""
        start, depth, case = self.pos(), 0, 0
        first = self.i
        while not self.at_end():
            t = self.tok()
            if t.kind == "punct" and t.text in "([":
                depth += 1
            elif t.kind == "punct" and t.text in ")]":
                if depth == 0:
                    break
                depth -= 1
            elif depth == 0 and t.word == "case":
                case += 1
            elif depth == 0 and t.word == "end" and case:
                case -= 1
                self.i += 1
                continue
            if depth == 0 and case == 0 and (
                    t.word in words or (t.kind == "op" and t.text in ops)
                    or (t.kind == "punct" and t.text == ";")
                    or (comma and t.kind == "punct" and t.text == ",")):
                break
            self.i += 1
        if self.i == first:
            return ""
        return self.text[start:self.t[self.i - 1].end].strip()

    def semicolon(self):
        if not self.is_("punct", ";"):
            raise Refused(f"its body could not be read: a statement does not"
                          f" end at line {self.line()}")

    # ---- the body and its blocks -----------------------------------------

    def body(self):
        while self.is_("op", "#"):
            self.i += 1
            key = self.name()
            value = self.name() if self.tok() is not None and \
                self.tok().kind in ("word", "qident") else ""
            self.directives[key] = value
        start = self.pos()
        label = self.label()
        node = self.block(label, start)
        while self.is_("punct", ";"):
            self.i += 1
        if not self.at_end():
            raise Refused("its body could not be read: text after its end"
                          f" at line {self.line()}")
        return node

    def label(self):
        if self.is_("op", "<<") and self.tok(2) is not None and \
                self.tok(2).text == ">>":
            self.i += 1
            name = self.name()
            self.i += 1
            return name
        return None

    def end_label(self, label):
        t = self.tok()
        if t is not None and t.kind in ("word", "qident") and \
                t.word not in ("if", "loop", "case"):
            if label is None or _name(t) != label:
                raise Refused(f"its body could not be read: END names"
                              f" {t.text} at line {t.line}, not its own"
                              " label")
            self.i += 1

    def block(self, label, start):
        decls = []
        if self.eat("declare"):
            while self.w() != "begin":
                if self.at_end():
                    raise Refused("its body could not be read: DECLARE has"
                                  " no BEGIN")
                if self.eat("declare"):
                    continue
                decls.append(self.decl())
        line = self.line()
        self.need("begin")
        body = self.statements()
        handlers = []
        if self.eat("exception"):
            while self.eat("when"):
                hline = self.line()
                conds = [self.condition()]
                while self.eat("or"):
                    conds.append(self.condition())
                self.need("then")
                handlers.append((conds, self.statements(), hline))
        self.need("end")
        self.end_label(label)
        return Node("block", start, line, label=label, decls=decls,
                    body=body, handlers=handlers)

    def condition(self):
        if self.eat("sqlstate"):
            t = self.tok()
            if t is None or t.kind != "string":
                raise Refused(f"its body could not be read: SQLSTATE needs a"
                              f" code at line {self.line()}")
            self.i += 1
            return ("sqlstate", _literal(t, t.line))
        return ("name", self.name())

    def decl(self):
        line = self.line()
        name = self.name()
        if self.eat("alias"):
            self.need("for")
            t = self.tok()
            self.i += 1
            target = t.text if t is not None and t.kind == "param" else \
                _name(t) if t is not None else ""
            self.semicolon()
            self.i += 1
            return Decl("alias", name, line, target=target)
        back = self.i
        self.eat("no")
        self.eat("scroll")
        if self.eat("cursor"):
            args = None
            if self.is_("punct", "("):
                self.i += 1
                args = self.scan()
                self.i += 1
            if not (self.eat("for") or self.eat("is")):
                raise Refused(f"its body could not be read: a cursor at line"
                              f" {line} names no query")
            query = self.scan()
            self.semicolon()
            self.i += 1
            return Decl("cursor", name, line, query=query, args=args)
        self.i = back
        constant = self.eat("constant")
        start, depth = self.pos(), 0
        while not self.at_end():
            t = self.tok()
            if t.kind == "punct" and t.text == "(":
                depth += 1
            elif t.kind == "punct" and t.text == ")":
                depth -= 1
            elif depth == 0 and (
                    (t.kind == "punct" and t.text == ";") or
                    (t.kind == "op" and t.text in (":=", "=")) or
                    t.word in ("default", "collate") or
                    (t.word == "not" and self.w(1) == "null")):
                break
            self.i += 1
        typ = self.text[start:self.done_at(start)].strip()
        if self.eat("collate"):
            raise Refused(f"line {line}: {name} is declared with a collation"
                          " of its own, which migkit's rules do not carry")
        notnull = self.eat("not", "null")
        default = None
        if self.eat("default") or self.is_("op", ":=") or \
                self.is_("op", "="):
            if self.tok().kind == "op":
                self.i += 1
            default = self.scan()
        self.semicolon()
        self.i += 1
        if not typ:
            raise Refused(f"its body could not be read: {name} at line"
                          f" {line} has no type")
        return Decl("var", name, line, type=typ, constant=constant,
                    notnull=notnull, default=default)

    def statements(self):
        out = []
        while True:
            if self.at_end() or self.w() in _ENDS:
                return out
            out.append(self.statement())
            self.semicolon()
            self.i += 1

    def statement(self):
        start = self.pos()
        label = self.label()
        line, w = self.line(), self.w()
        if w in ("declare", "begin") and (w == "declare" or label or
                                          self.w(1) != "transaction"):
            node = self.block(label, start)
        elif w in ("loop", "while", "for", "foreach"):
            node = getattr(self, "_" + w)(start, line, label)
        elif label:
            raise Refused(f"its body could not be read: a label at line"
                          f" {line} is not on a block or a loop")
        elif w in ("if", "case", "exit", "continue", "return", "raise",
                   "assert", "perform", "execute", "get", "open", "fetch",
                   "move", "close", "call", "commit", "rollback", "null",
                   "do"):
            node = getattr(self, "_" + w)(start, line)
        else:
            node = self._assign_or_sql(start, line)
        node.start = start
        node.end = self.done_at(start)
        return node

    # ---- control -------------------------------------------------------

    def _if(self, start, line):
        self.i += 1
        branches = []
        while True:
            cline = self.line()
            cond = self.scan(("then",))
            self.need("then")
            branches.append((cond, self.statements(), cline))
            if not (self.eat("elsif") or self.eat("elseif")):
                break
        otherwise = None
        if self.eat("else"):
            otherwise = self.statements()
        self.need("end", "if")
        return Node("if", start, line, branches=branches, otherwise=otherwise)

    def _case(self, start, line):
        self.i += 1
        subject = None
        if self.w() != "when":
            subject = self.scan(("when",))
        whens = []
        while self.eat("when"):
            wline = self.line()
            value = self.scan(("then",))
            self.need("then")
            whens.append((value, self.statements(), wline))
        otherwise = None
        if self.eat("else"):
            otherwise = self.statements()
        self.need("end", "case")
        return Node("case", start, line, subject=subject, whens=whens,
                    otherwise=otherwise)

    def _loop(self, start, line, label):
        self.need("loop")
        body = self.statements()
        self.need("end", "loop")
        self.end_label(label)
        return Node("loop", start, line, label=label, body=body)

    def _while(self, start, line, label):
        self.need("while")
        cond = self.scan(("loop",))
        self.need("loop")
        body = self.statements()
        self.need("end", "loop")
        self.end_label(label)
        return Node("while", start, line, label=label, cond=cond, body=body)

    def _for(self, start, line, label):
        self.need("for")
        targets = [self.target()]
        while self.is_("punct", ","):
            self.i += 1
            targets.append(self.target())
        self.need("in")
        a = {"label": label, "targets": targets}
        if self.eat("execute"):
            a["query"] = self.scan(("loop",))
            kind = "dynfors"
        else:
            reverse = self.eat("reverse")
            first = self.scan(("loop",), ops=("..",))
            if self.is_("op", ".."):
                self.i += 1
                a.update(reverse=reverse, lower=first,
                         upper=self.scan(("loop", "by")), step=None)
                if self.eat("by"):
                    a["step"] = self.scan(("loop",))
                kind = "fori"
            elif re.fullmatch(r'(?is)("[^"]+"|[a-z_]\w*)\s*(\(.*\))?', first) \
                    and not re.match(r"(?i)(select|with|values|table)\b",
                                     first):
                a["cursor"] = first
                kind = "forc"
            else:
                a["query"] = first
                kind = "fors"
        self.need("loop")
        a["body"] = self.statements()
        self.need("end", "loop")
        self.end_label(label)
        return Node(kind, start, line, **a)

    def _foreach(self, start, line, label):
        self.need("foreach")
        self.scan(("loop",))
        self.need("loop")
        body = self.statements()
        self.need("end", "loop")
        self.end_label(label)
        return Node("foreach_a", start, line, label=label, body=body)

    def _exit(self, start, line):
        is_exit = self.w() == "exit"
        self.i += 1
        label = None
        t = self.tok()
        if t is not None and t.kind in ("word", "qident") and \
                t.word != "when":
            label = self.name()
        cond = self.scan() if self.eat("when") else None
        return Node("exit", start, line, is_exit=is_exit, label=label,
                    cond=cond)

    _continue = _exit

    def _return(self, start, line):
        self.i += 1
        if self.eat("next"):
            return Node("return_next", start, line, value=self.scan())
        if self.eat("query"):
            return Node("return_query", start, line, value=self.scan())
        return Node("return", start, line, value=self.scan() or None)

    def _raise(self, start, line):
        self.i += 1
        level = "exception"
        if self.w() in ("debug", "log", "info", "notice", "warning",
                        "exception"):
            level = self.w()
            self.i += 1
        fmt, args, cond, options = None, [], None, []
        t = self.tok()
        bare = t is None or self.is_("punct", ";")
        if bare:
            pass
        elif t.kind == "string":
            fmt = _literal(t, t.line)
            self.i += 1
            while self.is_("punct", ","):
                self.i += 1
                args.append(self.scan(("using",), comma=True))
        elif t.word == "sqlstate":
            self.i += 1
            code = self.tok()
            self.i += 1
            cond = ("sqlstate", _literal(code, line))
        elif t.word != "using":
            cond = ("name", self.name())
        if self.eat("using"):
            while True:
                opt = self.name()
                if not (self.is_("op", "=") or self.is_("op", ":=")):
                    raise Refused(f"its body could not be read: RAISE's"
                                  f" {opt} at line {line} has no value")
                self.i += 1
                options.append((opt, self.scan(comma=True)))
                if not self.is_("punct", ","):
                    break
                self.i += 1
        return Node("raise", start, line, level=level, fmt=fmt, args=args,
                    cond=cond, options=options,
                    reraise=bare and level == "exception")

    def _assert(self, start, line):
        self.i += 1
        cond = self.scan(comma=True)
        message = None
        if self.is_("punct", ","):
            self.i += 1
            message = self.scan()
        return Node("assert", start, line, cond=cond, message=message)

    def _perform(self, start, line):
        self.i += 1
        return Node("perform", start, line, query="SELECT " + self.scan())

    def _execute(self, start, line):
        self.i += 1
        return Node("dynexecute", start, line, text=self.scan())

    def _get(self, start, line):
        self.i += 1
        stacked = self.eat("stacked")
        self.eat("current")
        self.need("diagnostics")
        items = []
        while True:
            target = self.target()
            if not (self.is_("op", "=") or self.is_("op", ":=")):
                raise Refused(f"its body could not be read: GET DIAGNOSTICS"
                              f" at line {line}")
            self.i += 1
            items.append((target, self.name()))
            if not self.is_("punct", ","):
                break
            self.i += 1
        return Node("getdiag", start, line, stacked=stacked, items=items)

    def _open(self, start, line):
        self.i += 1
        cursor = self.name()
        rest = self.scan()
        return Node("open", start, line, cursor=cursor, rest=rest)

    def _fetch(self, start, line):
        is_move = self.w() == "move"
        self.i += 1
        direction = []
        while self.tok() is not None and (
                self.w() in ("next", "prior", "first", "last", "absolute",
                             "relative", "forward", "backward", "all")
                or self.tok().kind == "number" or self.is_("op", "-")) and \
                self.w(1) not in ("into",) and not self.is_("punct", ";", 1):
            direction.append(self.tok().text)
            self.i += 1
        if not self.eat("from"):
            self.eat("in")
        cursor = self.name()
        targets = []
        if self.eat("into"):
            targets = [self.target()]
            while self.is_("punct", ","):
                self.i += 1
                targets.append(self.target())
        return Node("fetch", start, line, is_move=is_move, cursor=cursor,
                    direction=" ".join(direction).lower(), targets=targets)

    _move = _fetch

    def _close(self, start, line):
        self.i += 1
        return Node("close", start, line, cursor=self.name())

    def _call(self, start, line):
        self.i += 1
        name = self.name()
        while self.is_("punct", "."):
            self.i += 1
            name = self.name()
        args = []
        if self.is_("punct", "("):
            self.i += 1
            while not self.is_("punct", ")"):
                if self.at_end():
                    raise Refused(f"its body could not be read: CALL at line"
                                  f" {line}")
                args.append(self.scan(comma=True))
                if self.is_("punct", ","):
                    self.i += 1
            self.i += 1
        return Node("call", start, line, name=name, args=args)

    def _do(self, start, line):
        self.i += 1
        return Node("call", start, line, name=None, args=[],
                    text=self.scan())

    def _commit(self, start, line):
        what = self.w()
        self.i += 1
        chain = self.eat("and", "chain")
        if not chain and self.eat("and", "no", "chain"):
            chain = False
        return Node(what, start, line, chain=chain)

    _rollback = _commit

    def _null(self, start, line):
        self.i += 1
        return Node("null", start, line)

    # ---- assignments and SQL ---------------------------------------------

    def target(self):
        """A name an assignment, INTO or FOR writes: `x`, `r.f`,
        `new.col`."""
        t = self.tok()
        if t is None or t.kind not in ("word", "qident"):
            raise Refused(f"its body could not be read: a variable was"
                          f" expected at line {self.line()}")
        start = t.start
        self.i += 1
        while self.is_("punct", ".") and self.tok(1) is not None and \
                self.tok(1).kind in ("word", "qident"):
            self.i += 2
        if self.is_("punct", "["):
            self.scan(ops=())
            depth = 0
            while not self.at_end():
                t = self.tok()
                self.i += 1
                if t.text == "[":
                    depth += 1
                elif t.text == "]":
                    depth -= 1
                    if depth <= 0 and not self.is_("punct", "["):
                        break
        return self.text[start:self.done_at(start)]

    def _assign_or_sql(self, start, line):
        back = self.i
        t = self.tok()
        if t is not None and t.kind in ("word", "qident") and (
                t.word not in _SQL or self.is_("op", ":=", 1) or
                (self.is_("op", "=", 1) and t.word != "set")):
            target = self.target()
            if self.is_("op", ":=") or self.is_("op", "="):
                self.i += 1
                return Node("assign", start, line, target=target,
                            value=self.scan())
            self.i = back
        return self._sql(start, line)

    def _sql(self, start, line):
        s = self.i
        depth, into_at, k = 0, None, s
        while k < len(self.t):
            t = self.t[k]
            if t.kind == "punct" and t.text == ";" and depth == 0:
                break
            if t.kind == "punct" and t.text in "([":
                depth += 1
            elif t.kind == "punct" and t.text in ")]":
                depth -= 1
            elif depth == 0 and t.word == "into" and into_at is None and \
                    not (k > 0 and self.t[k - 1].word in ("insert",
                                                         "merge")):
                into_at = k
            k += 1
        if k == s:
            raise Refused(f"its body could not be read at line {line}")
        verb = self.t[s].word or self.t[s].text
        targets, strict = None, False
        if into_at is None:
            text = self.text[self.t[s].start:self.t[k - 1].end]
        else:
            self.i = into_at + 1
            strict = self.eat("strict")
            targets = [self.target()]
            while self.is_("punct", ","):
                self.i += 1
                targets.append(self.target())
            after = self.i
            text = self.text[self.t[s].start:self.t[into_at].start] + " " + \
                (self.text[self.t[after].start:self.t[k - 1].end]
                 if after < k else "")
        self.i = k
        return Node("execsql", start, line, verb=verb, text=text.strip(),
                    targets=targets, strict=strict)


def _name(tok):
    if tok.kind == "qident":
        if tok.text[0] in "Uu":
            raise Refused(f"line {tok.line}: a Unicode-escaped name"
                          " migkit's rules do not read")
        return tok.text[1:-1].replace('""', '"')
    return tok.text.lower()


def read(body):
    """The parsed body (its outermost block), or Refused."""
    return _Reader(body or "").body()


def spans(body):
    """(start, end) character offsets, in `body`, of the statements of its
    outermost block - each without the semicolon ending it - or [] where
    it is not a block of statements (a `language sql` body)."""
    try:
        tree = read(body)
    except Refused:
        return []
    return [(s.start, s.end) for s in tree.body]


# ---- the other way: a PostgreSQL routine made again ------------------------


def _pg_ident(name):
    text = str(name)
    if re.fullmatch(r"[a-z_][a-z0-9_$]*", text) and text not in _PG_WORDS:
        return text
    return '"' + text.replace('"', '""') + '"'


_PG_WORDS = frozenset("""all analyse analyze and any array as asc asymmetric
both case cast check collate column constraint create current_catalog
current_date current_role current_time current_timestamp current_user
default deferrable desc distinct do else end except false fetch for foreign
from grant group having in initially intersect into lateral leading limit
localtime localtimestamp not null offset on only or order placing primary
references returning select session_user some symmetric system_user table
then to trailing true union unique user using variadic when where window
with""".split())

_VOLATILITY = {"NO SQL": "immutable", "READS SQL DATA": "stable"}


def definition(routine, name, body):
    """A PostgreSQL statement making `routine` again under `name` with
    `body`: its parameters, what it returns, its language, volatility and
    strictness - for a cut copy of it in the source's sandbox."""
    r = routine
    tag, n = "$migkit$", 0
    while tag in (body or ""):
        n += 1
        tag = f"$migkit{n}$"
    params = ", ".join(
        f"{'' if p.mode == 'in' else p.mode + ' '}"
        f"{_pg_ident(p.name) + ' ' if p.name else ''}{p.type}"
        for p in r.params) if r.kind != "trigger" else ""
    lang = r.language or "plpgsql"
    quoted = f"{tag}{body}{tag}"
    if r.kind == "procedure":
        return (f"create procedure {_pg_ident(name)}({params}) language"
                f" {lang} as {quoted}")
    returns = "trigger" if r.kind == "trigger" else (
        ("setof " if r.returns_set else "") + str(r.returns or "void"))
    options = ""
    if r.kind == "function":
        options = " " + _VOLATILITY.get(
            str(r.settings.get("data") or ""), "volatile")
        if _options(r)["strict"]:
            options += " strict"
    return (f"create function {_pg_ident(name)}({params}) returns {returns}"
            f" language {lang}{options} as {quoted}")


def _options(routine):
    """What the source's own definition says of it besides its body."""
    text = str(routine.definition or "")
    m = re.search(r"(?is)\bas\s+(\$[^$]*\$|')", text)
    head = text[:m.start()] if m else ""
    opened, depth, closed = head.find("("), 0, -1
    for k in range(max(opened, 0), len(head) if opened >= 0 else 0):
        depth += {"(": 1, ")": -1}.get(head[k], 0)
        if depth == 0:
            closed = k
            break
    inside = head[opened:closed + 1] if closed > 0 else ""
    after_params = head[closed + 1:] if closed > 0 else ""
    return {"strict": bool(re.search(r"(?i)\bstrict\b|\breturns\s+null\s+on"
                                     r"\s+null\s+input\b", after_params)),
            "definer": bool(re.search(r"(?i)\bsecurity\s+definer\b",
                                      after_params)),
            "settings": re.findall(r"(?im)^\s*set\s+(\w+)", after_params),
            "defaults": bool(re.search(r"(?i)\bdefault\b|=", inside)),
            "variadic": bool(re.search(r"(?i)\bvariadic\b", inside))}


# ---- the gate: PostgreSQL's own reading of the body ------------------------

_NO_LINE = ("null",)


def _shape(node, out):
    if node.kind not in _NO_LINE:
        out.append((node.kind, node.line) + (
            (node.targets is not None, bool(node.strict))
            if node.kind == "execsql" else ()))
    for child in node.children():
        _shape(child, out)
    return out


def _pg_shape(tree):
    out = []

    def walk(v):
        if isinstance(v, dict):
            for k, x in v.items():
                if k.startswith("PLpgSQL_stmt_") and isinstance(x, dict):
                    if "lineno" in x:
                        kind = k[len("PLpgSQL_stmt_"):]
                        out.append((kind, x["lineno"]) + (
                            (bool(x.get("into")), bool(x.get("strict")))
                            if kind == "execsql" else ()))
                walk(x)
        elif isinstance(v, list):
            for x in v:
                walk(x)
    walk(tree)
    return out


def _gate(routine, tree):
    """PostgreSQL's own parser reads the body, and reads the statements
    this module read - or Refused."""
    from pglast.parser import ParseError, parse_plpgsql_json
    text = definition(routine, routine.name, routine.body or "")
    try:
        raw = parse_plpgsql_json(text)
    except ParseError as e:
        raise Refused("PostgreSQL's own parser refuses its body: "
                      + (str(e).splitlines() or [""])[0][:100]) from None
    except Exception:  # noqa: BLE001 - the wrapper's own trouble
        return
    try:
        got = json.loads(raw)
    except ValueError:
        # measured: a trigger function's tree comes back with a `}` too
        # many after each datum it does not write out
        try:
            got = json.loads(re.sub(r"\{\}\}(?=\s*[,\]])", "{}", raw))
        except ValueError:
            return
    theirs = _pg_shape(got)
    mine = _shape(tree, [])
    if theirs != mine:
        at = next((i for i, (x, y) in enumerate(zip(mine, theirs))
                   if x != y), min(len(mine), len(theirs)))
        where = (mine[at] if at < len(mine) else theirs[at])[1] \
            if max(len(mine), len(theirs)) > at else 0
        raise Refused(f"line {where}: its body was not read here as"
                      " PostgreSQL reads it")


# ---- types -----------------------------------------------------------------

#: PostgreSQL's types by name, as (MySQL's type, the kind of value)
_TYPES = {
    "integer": ("INT", "int"), "int": ("INT", "int"), "int4": ("INT", "int"),
    "bigint": ("BIGINT", "int"), "int8": ("BIGINT", "int"),
    "smallint": ("SMALLINT", "int"), "int2": ("SMALLINT", "int"),
    "real": ("FLOAT", "float"), "float4": ("FLOAT", "float"),
    "double precision": ("DOUBLE", "float"), "float8": ("DOUBLE", "float"),
    "text": ("LONGTEXT", "text"),
    "boolean": ("TINYINT(1)", "bool"), "bool": ("TINYINT(1)", "bool"),
    "date": ("DATE", "date"),
    "bytea": ("LONGBLOB", "bytes"),
    "json": ("JSON", "json"), "jsonb": ("JSON", "json"),
    "uuid": ("CHAR(36)", "text"),
}


def my_type(declared, param=False):
    """(MySQL's type, the kind of value, a note or None) for a PostgreSQL
    type as a declaration or the catalogue (`param`, no modifier) writes
    it - or Refused. Strings are declared in the binary collation."""
    text = " ".join(str(declared).lower().split())
    text = re.sub(r"^pg_catalog\.", "", text)
    if text.endswith("[]") or text.startswith("_"):
        raise Refused(f"its type {declared} is an array, which MySQL does"
                      " not have")
    m = re.search(r"\(([^)]*)\)", text)
    args = [a.strip() for a in m.group(1).split(",")] if m else []
    base = " ".join(re.sub(r"\([^)]*\)", " ", text).split())
    if base in ("timestamp with time zone", "timestamptz",
                "time with time zone", "timetz"):
        raise Refused(f"its type {declared} keeps a time zone, which a MySQL"
                      " time value does not")
    if base in _TYPES and not args:
        typ, kind = _TYPES[base]
        return (typ + BINARY if kind == "text" else typ), kind, None
    if base in ("numeric", "decimal"):
        if not args:
            return "DECIMAL(65,30)", "num", (
                "numeric without a declared precision is held as"
                " DECIMAL(65,30): 35 digits before the point and 30 after;"
                " past either MySQL rounds or refuses where PostgreSQL"
                " keeps every digit")
        p, s = int(args[0]), int(args[1]) if len(args) > 1 else 0
        if not (0 < p <= 65 and 0 <= s <= 30 and s <= p):
            raise Refused(f"its type {declared} is wider than MySQL's"
                          " DECIMAL holds")
        return f"DECIMAL({p},{s})", "dec", None
    if base in ("character varying", "varchar"):
        if not args:
            return "LONGTEXT" + BINARY, "text", None
        if int(args[0]) > 16383:
            raise Refused(f"its type {declared} is longer than a MySQL"
                          " VARCHAR of four-byte characters holds")
        return f"VARCHAR({int(args[0])})" + BINARY, "text", None
    if base in ("character", "char", "bpchar"):
        if not args and param:
            raise Refused(f"its type {declared} has no length where it is"
                          " declared, and MySQL's CHAR needs one")
        n = int(args[0]) if args else 1
        if n > 255:
            raise Refused(f"its type {declared} is longer than MySQL's CHAR"
                          " holds")
        return f"CHAR({n})" + BINARY, "text", None
    if base in ("timestamp", "timestamp without time zone"):
        n = int(args[0]) if args else 6
        return f"DATETIME({n})", "ts", None
    if base in ("time", "time without time zone"):
        n = int(args[0]) if args else 6
        return f"TIME({n})", "time", None
    if base in ("float", "double"):
        n = int(args[0]) if args else 53
        return ("FLOAT", "float", None) if n <= 24 else \
            ("DOUBLE", "float", None)
    raise Refused(f"its type {declared} has no counterpart in migkit's"
                  " rules")


def _my_kind(mysql_type):
    """The kind of value a MySQL column type holds."""
    t = str(mysql_type).lower()
    if t.startswith("tinyint(1)"):
        return "bool"
    if re.match(r"(tiny|small|medium|big)?int|integer", t):
        return "int"
    if t.startswith(("decimal", "numeric")):
        return "dec"
    if t.startswith(("float", "double", "real")):
        return "float"
    if t.startswith(("char", "varchar", "text", "tinytext", "mediumtext",
                     "longtext", "enum", "set")):
        return "text"
    if t.startswith("datetime") or t.startswith("timestamp"):
        return "ts"
    if t.startswith("date"):
        return "date"
    if t.startswith("time"):
        return "time"
    if t.startswith("json"):
        return "json"
    if "blob" in t or "binary" in t:
        return "bytes"
    return None


def _declared(mysql_type):
    """A target column's type as a variable is declared with it."""
    t = str(mysql_type)
    kind = _my_kind(t)
    t = re.sub(r"(?i)\s+(character set|charset|collate)\s+\S+", "", t)
    return (t.upper() + BINARY if kind == "text" else t.upper()), kind


def _quote(name):
    """A MySQL variable or label as written: bare where it can be."""
    text = str(name)
    if re.fullmatch(r"[a-z_][a-z0-9_]*", text) and text not in _RESERVED:
        return text
    return "`" + text.replace("`", "``") + "`"


def _sql_string(text):
    return exp.Literal.string(text).sql(dialect=_RulesMySQL)


# ---- conditions ------------------------------------------------------------

_CODES = None


def _codes():
    """{condition name: SQLSTATE}, PostgreSQL's own list."""
    global _CODES
    if _CODES is None:
        from psycopg2 import errorcodes
        _CODES = {k.lower(): v for k, v in vars(errorcodes).items()
                  if isinstance(v, str) and len(v) == 5 and k.isupper()
                  and not k.startswith("CLASS_")}
    return _CODES


def _errnos():
    """{condition name: [MySQL error numbers raising the same failure]}"""
    out = {}
    for n, name in MYSQL_ERRNO.items():
        out.setdefault(name, []).append(n)
    return out


def _code_of(cond, line):
    """The SQLSTATE of ("name", n) or ("sqlstate", code)."""
    kind, value = cond
    if kind == "sqlstate":
        if not re.fullmatch(r"[0-9A-Z]{5}", value):
            raise Refused(f"line {line}: SQLSTATE {value!r} is not a code")
        return value
    code = _codes().get(value)
    if code is None:
        raise Refused(f"line {line}: {value} is not a condition PostgreSQL"
                      " names")
    return code


# ---- values ----------------------------------------------------------------


class _Var:
    """A name the body reads: a parameter, a variable, a record's field,
    a trigger's word."""

    def __init__(self, name, my, mytype=None, kind=None, notnull=False,
                 record=False, literal=None, cursor=None):
        self.name, self.my, self.mytype, self.kind = name, my, mytype, kind
        self.notnull, self.record, self.literal = notnull, record, literal
        self.cursor = cursor
        self.fields = {}
        #: read by the body (a handler's SQLSTATE and SQLERRM)
        self.used = False

    def node(self):
        if self.literal is not None:
            return exp.Literal.string(self.literal)
        return exp.column(exp.to_identifier(
            self.my.strip("`").replace("``", "`"),
            quoted=self.my.startswith("`")))


def _lit(text):
    return exp.Literal.string(text)


def _fn(name, *args):
    return exp.Anonymous(this=name, expressions=list(args))


def _cast(node, typ):
    return exp.Cast(this=node, to=exp.DataType.build(typ, dialect="mysql"))


def _trim(node, chars, where="TRAILING"):
    return exp.Trim(this=node, expression=_lit(chars), position=where)


#: functions the rules carry as they are: same name, same meaning, or a
#: meaning the query translator already keeps
_KEPT = {getattr(exp, n) for n in (
    "Upper", "Lower", "Coalesce", "Nullif", "Abs", "Ceil", "Floor", "Round",
    "Sign", "Trunc", "Count", "Sum", "Avg", "Min", "Max", "Length",
    "Substring", "Trim", "Concat", "ConcatWs", "Greatest", "Least",
    "CurrentDate", "CurrentTimestamp", "Localtimestamp", "Localtime",
    "Cast", "Extract", "Case", "If", "Pow", "Sqrt", "Ln", "Log", "Exp",
    "Left", "Right", "Pad", "Repeat", "Reverse", "Replace", "StrPosition",
    "MD5", "Rand", "Uuid", "RowNumber", "Rank", "DenseRank", "Exists",
    "Anonymous", "DateAdd", "DateSub", "IntDiv", "Paren", "And", "Or")
    if hasattr(exp, n)}

#: what `Anonymous` calls of the rules' own making answer
_MADE = {"NOW": "ts", "SYSDATE": "ts", "CURDATE": "date", "CURTIME": "time",
         "DATEDIFF": "int", "LOG10": "float", "DAYOFWEEK": "int",
         "DAYOFYEAR": "int", "OCTET_LENGTH": "int", "LENGTH": "int",
         "DATE_FORMAT": "text", "HEX": "text", "MICROSECOND": "int"}

#: the words PostgreSQL gives a trigger, and what each is in MySQL's
_TG = ("tg_op", "tg_when", "tg_level", "tg_name", "tg_table_name",
       "tg_relname", "tg_table_schema", "tg_nargs", "tg_argv", "tg_relid")

_NUMERIC = ("int", "dec", "num", "float")


class _Gen:
    def __init__(self, routine, ctx):
        from .mysql_pg import Context
        self.r = routine
        self.ctx = ctx or Context()
        self.types = getattr(self.ctx, "column_types", None) or {}
        self.e = Emitter()
        self.codes = []
        self.notes = []
        self.residue = []
        self.serial = 0
        self.helpers = []            # (name, type, default)
        self.scopes = [{}]
        self.labels = []             # (pg label, my label, kind, handler)
        self.in_handler = 0
        self.pending = []            # division checks for the next statement
        self.commits = False
        self.event = None
        self.found = None
        self.rows = None
        self.body_label = "_migkit_body"
        self.tail = None
        self.words = set()
        self.handler_scope = None
        self.handler_at_top = False
        self.handler_last = None
        self.ended = False

    # ---- bookkeeping ---------------------------------------------------

    def code(self, what, line):
        if (what, line) not in self.codes:
            self.codes.append((what, line))

    def note(self, text):
        if text and text not in self.notes:
            self.notes.append(text)

    def fresh(self, stem):
        self.serial += 1
        return f"_migkit_{stem}{self.serial}"

    def helper(self, stem, typ, default=None):
        name = self.fresh(stem)
        self.helpers.append((name, typ, default))
        return name

    def lookup(self, name):
        for scope in reversed(self.scopes):
            if name in scope:
                return scope[name]
        return None

    # ---- expressions -----------------------------------------------------

    def kind(self, node):
        """The kind of value an expression answers, as PostgreSQL types it,
        or None where it is not known."""
        k = self._kind
        if isinstance(node, exp.Paren):
            return k(node.this)
        if isinstance(node, exp.Literal):
            if node.is_string:
                return "text"
            return "int" if re.fullmatch(r"\d+", node.this) else "dec"
        if isinstance(node, exp.Boolean):
            return "bool"
        if isinstance(node, exp.Column):
            return self._column_kind(node)
        if isinstance(node, exp.Cast):
            return self._type_kind(node.to)
        if isinstance(node, exp.Neg):
            return k(node.this)
        if isinstance(node, (exp.Add, exp.Sub, exp.Mul, exp.Mod, exp.Div)):
            a, b = k(node.this), k(node.expression)
            if a in ("date", "ts") or b in ("date", "ts"):
                return self._date_sum_kind(node, a, b)
            if isinstance(node, exp.Div) and a == b == "int":
                return "int"
            return _wider(a, b)
        if isinstance(node, exp.IntDiv):
            return "int"
        if isinstance(node, (exp.DPipe, exp.Concat, exp.ConcatWs, exp.Upper,
                             exp.Lower, exp.Substring, exp.Trim,
                             exp.Replace, exp.Left, exp.Right, exp.Pad,
                             exp.Repeat, exp.Reverse, exp.MD5)):
            return "text"
        if isinstance(node, (exp.Length, exp.StrPosition, exp.Count)):
            return "int"
        if isinstance(node, exp.Extract):
            return "dec"
        if isinstance(node, (exp.Round, exp.Trunc)):
            a = k(node.this)
            return "dec" if a in ("num", "dec") else a
        if isinstance(node, (exp.Abs, exp.Ceil, exp.Floor, exp.Sign,
                             exp.Min, exp.Max)):
            return k(node.this)
        if isinstance(node, exp.Avg):
            return "dec" if k(node.this) in ("int", "dec", "num") else \
                k(node.this)
        if isinstance(node, (exp.Pow, exp.Sqrt, exp.Ln, exp.Log, exp.Exp)):
            return "float" if k(node.this) in ("int", "float") else "num"
        if isinstance(node, (exp.Coalesce, exp.Greatest, exp.Least,
                             exp.Nullif)):
            kinds = {k(x) for x in [node.this] + list(
                node.expressions if not isinstance(node, exp.Nullif)
                else []) if not isinstance(x, exp.Null)}
            return _common(kinds)
        if isinstance(node, exp.Case):
            kinds = {k(i.args.get("true")) for i in node.args.get("ifs") or []}
            if node.args.get("default") is not None and not isinstance(
                    node.args["default"], exp.Null):
                kinds.add(k(node.args["default"]))
            return _common(kinds)
        if isinstance(node, exp.If):
            return _common({k(node.args.get("true")),
                            k(node.args.get("false"))})
        if isinstance(node, (exp.Predicate, exp.Connector, exp.Not,
                             exp.Exists, exp.Is, exp.In, exp.Between)):
            return "bool"
        if isinstance(node, (exp.CurrentTimestamp, exp.Localtimestamp)):
            return "ts"
        if isinstance(node, exp.CurrentDate):
            return "date"
        if isinstance(node, (exp.DateAdd, exp.DateSub)):
            return k(node.this)
        if isinstance(node, exp.Subquery):
            return k(node.this)
        if isinstance(node, exp.Select) and len(node.expressions) == 1:
            return k(node.expressions[0].unalias())
        if isinstance(node, exp.Anonymous):
            name = str(node.name).upper()
            if name in _MADE:
                return _MADE[name]
            callee = self.ctx.routines.get(str(node.name).lower())
            if callee is not None and callee.returns:
                try:
                    return my_type(callee.returns, param=True)[1]
                except Refused:
                    return None
        if isinstance(node, exp.Null):
            return None
        return None

    def _kind(self, node):
        return self.kind(node) if node is not None else None

    def _date_sum_kind(self, node, a, b):
        if isinstance(node, exp.Sub) and a == b == "date":
            return "int"
        if a in ("date", "ts") and (b == "int" or isinstance(
                node.expression, exp.Interval)):
            return a
        if b in ("date", "ts") and a == "int" and isinstance(node, exp.Add):
            return b
        return None

    def _type_kind(self, to):
        T = exp.DataType.Type
        t = to.this
        if t in (T.INT, T.BIGINT, T.SMALLINT, T.TINYINT):
            return "int"
        if t == T.DECIMAL:
            return "dec" if to.expressions else "num"
        if t in (T.FLOAT, T.DOUBLE):
            return "float"
        if t in (T.TEXT, T.VARCHAR, T.CHAR, T.NVARCHAR, T.NCHAR):
            return "text"
        if t == T.BOOLEAN:
            return "bool"
        if t == T.DATE:
            return "date"
        if t in (T.TIMESTAMP, T.DATETIME):
            return "ts"
        if t == T.TIME:
            return "time"
        if t in (T.JSON, T.JSONB):
            return "json"
        return None

    def _column_kind(self, col):
        var = self._var_of(col)
        if var is not None:
            return var.kind
        if not col.table and col.name.lower() == "found":
            return "bool"
        if not col.table and col.name.lower() in _TG:
            return "text"
        table = col.table.lower() if col.table else None
        if table in ("new", "old") and self.r.kind == "trigger":
            return _my_kind(self._trigger_types().get(col.name.lower(), ""))
        if not self.types:
            return None
        # a table's column: its type on the target, by the tables of the
        # statement around it
        around = col.find_ancestor(exp.Select, exp.Update, exp.Delete,
                                   exp.Insert)
        tables = {}
        for t in around.find_all(exp.Table) if around is not None else ():
            leaf = self.ctx.tables.get(t.name.lower(), t.name).lower()
            tables[t.alias_or_name.lower()] = leaf
        names = [tables.get(table)] if table else set(tables.values())
        kinds = {_my_kind(self.types.get(n, {}).get(col.name.lower()))
                 for n in names if n and col.name.lower() in
                 self.types.get(n, {})}
        return kinds.pop() if len(kinds) == 1 else None

    def _trigger_types(self):
        leaf = str(self.r.table or "").split(".")[-1]
        return self.types.get(self._target_table(leaf).lower()) or {}

    def _var_of(self, col):
        """The variable a column reference names, or None."""
        name = col.name if col.this.args.get("quoted") else col.name.lower()
        if not col.table:
            got = self.lookup(name)
            return got if got is not None and not got.record else None
        qual = col.table if col.args["table"].args.get("quoted") else \
            col.table.lower()
        owner = self.lookup(qual)
        if owner is not None and owner.record:
            got = owner.fields.get(name)
            if got is None:
                raise Refused(f"its record {qual} has no field {name}"
                              " known here")
            return got
        if any(lab[0] == qual for lab in self.labels) or \
                qual == self.r.name.lower():
            return self.lookup(name)
        return None

    def rewrite(self, tree, line, query, wrap=None):
        """The rules, then the names, on one statement's or expression's
        tree; `wrap` rebuilds an expression's value between the two."""
        for node in list(tree.find_all(exp.Parameter, exp.Placeholder)):
            text = node.sql("postgres").lstrip("$@")
            params = list(self.r.params)
            if not text.isdigit() or not 0 < int(text) <= len(params) or \
                    not params[int(text) - 1].name:
                raise Refused(f"line {line} reads ${text}, which is not one"
                              " of its named parameters")
            p = params[int(text) - 1]
            node.replace(exp.column(exp.to_identifier(
                p.name, quoted=p.name != p.name.lower())))
        if self.ctx.local is not None:
            tree = self.ctx.local(tree)
        for table in list(tree.find_all(exp.Table)):
            made = self.ctx.tables.get(table.name.lower())
            if made:
                table.set("this", exp.to_identifier(made))
        self.fix(tree, line, query)
        if wrap is not None:
            value = tree.expressions[0]
            value.replace(wrap(value.copy()))
        self.names(tree, line)
        return tree

    def finish(self, tree, line, query=False):
        tree = self.rewrite(tree, line, query)
        return generate(tree, "postgres", _RulesMySQL)

    def expr_tree(self, text, line):
        tree = parse(f"SELECT {text}", "postgres")
        if not isinstance(tree, exp.Select) or len(tree.expressions) != 1 \
                or any(tree.args.get(k) for k in (
                    "from", "from_", "where", "group", "having", "order",
                    "limit", "joins", "with", "distinct")) or \
                isinstance(tree.expressions[0], exp.Alias):
            raise Refused(f"line {line}: the expression {text[:50]} could"
                          " not be read as one value")
        return tree

    def expr(self, text, line, render=None, to=None):
        """One expression in MySQL's SQL: `render` as PostgreSQL writes it
        as text ("text" or "raise"), `to` the kind it is assigned to."""
        wrap = (lambda v: self.text_of(v, render)) if render else \
            (lambda v: self.coerce(v, to, line)) if to is not None else None
        tree = self.rewrite(self.expr_tree(text, line), line, query=False,
                            wrap=wrap)
        return generate(tree, "postgres", _RulesMySQL)[len("SELECT "):]

    def cond(self, text, line):
        """(a condition in MySQL's SQL, True/False where it is decided
        without running it - a trigger's TG_OP - else None)."""
        tree = self.rewrite(self.expr_tree(text, line), line, query=False)
        value = _fold(tree.expressions[0])
        tree.expressions[0].replace(value)
        return generate(tree, "postgres", _RulesMySQL)[len("SELECT "):], \
            _constant(value)

    def coerce(self, node, to, line):
        """A value as PostgreSQL assigns it to a variable of kind `to`."""
        k = self.kind(node)
        if to == "text" and k not in ("text", None):
            return self.text_of(node, "text")
        if to == "bool" and k == "text":
            got = _constant_text(node)
            if got is None or got.lower() not in _BOOL_WORDS:
                raise Refused(f"line {line} assigns text to a boolean, which"
                              " MySQL does not read as PostgreSQL does")
            return exp.Boolean(this=_BOOL_WORDS[got.lower()])
        return node

    def text_of(self, node, style):
        """`node` written as text as PostgreSQL writes it: a boolean as
        true/false (`||`, `::text`) or t/f (RAISE, concat), a timestamp
        without a zero fraction, a numeric of no declared scale without
        the zeros MySQL pads it with, bytes as \\x and hex."""
        k = self.kind(node)
        if k in ("text", None):
            return node
        if k == "bool":
            t, f = ("t", "f") if style == "raise" else ("true", "false")
            return exp.Case(ifs=[
                exp.If(this=node.copy(), true=_lit(t)),
                exp.If(this=exp.Not(this=exp.Paren(this=node.copy())),
                       true=_lit(f))])
        if k == "num":
            return _trim(_trim(_cast(node, "CHAR"), "0"), ".")
        if k in ("ts", "time"):
            fmt = "%Y-%m-%d %H:%i:%s" if k == "ts" else "%H:%i:%s"
            return exp.If(
                this=exp.EQ(this=_fn("MICROSECOND", node.copy()),
                            expression=exp.Literal.number(0)),
                true=_fn("DATE_FORMAT", node.copy(), _lit(fmt)),
                false=_trim(_fn("DATE_FORMAT", node.copy(),
                                _lit(fmt + ".%f")), "0"))
        if k == "bytes":
            return exp.DPipe(this=_lit("\\x"), expression=_fn(
                "LOWER", _fn("HEX", node)))
        return _cast(node, "CHAR")

    def fix(self, tree, line, query):
        """PostgreSQL's meaning kept where MySQL's operator or function of
        the same name means something else. Children first: a rule that
        rebuilds a node copies children already carried."""
        for node in reversed(list(tree.walk())):
            if node.parent is None and node is not tree:
                continue
            new = self._fix_one(node, line, query)
            if new is not None and new is not node:
                node.replace(new)
        return tree

    def _fix_one(self, node, line, query):
        k = self.kind
        if isinstance(node, exp.Func) and type(node) not in _KEPT:
            raise Refused(f"line {line} uses"
                          f" {node.sql('postgres').split('(')[0][:40]},"
                          " which migkit's rules have no counterpart for")
        if isinstance(node, (exp.RegexpLike, exp.SimilarTo, exp.Array,
                             exp.Bracket, exp.GroupConcat, exp.Any,
                             exp.All, exp.AtTimeZone, exp.Collate)) or \
                type(node).__name__.startswith("JSON"):
            raise Refused(f"line {line} uses {node.sql('postgres')[:40]},"
                          " which migkit's rules have no counterpart for")
        if isinstance(node, exp.Interval):
            return self._interval(node, line)
        if isinstance(node, exp.DPipe):
            if "json" in (k(node.this), k(node.expression)):
                raise Refused(f"line {line} merges JSON values with ||,"
                              " which migkit's rules have no counterpart for")
            return exp.DPipe(this=self.text_of(node.this.copy(), "text"),
                             expression=self.text_of(node.expression.copy(),
                                                     "text"))
        if isinstance(node, (exp.Concat, exp.ConcatWs)):
            for arg in list(node.expressions):
                arg.replace(self.text_of(arg.copy(), "raise"))
            return None
        if isinstance(node, (exp.Div, exp.Mod, exp.IntDiv)):
            return self._division(node, line, query)
        if isinstance(node, (exp.Add, exp.Sub)):
            return self._date_arith(node, line)
        if isinstance(node, exp.Cast) and not isinstance(node, exp.TryCast):
            return self._cast(node, line)
        if isinstance(node, (exp.CurrentTimestamp, exp.Localtimestamp)):
            self.code("postgres.clock", line)
            return _fn("NOW", exp.Literal.number(6))
        if isinstance(node, exp.CurrentDate):
            return _fn("CURDATE")
        if isinstance(node, exp.Localtime):
            return _fn("CURTIME", exp.Literal.number(6))
        if isinstance(node, exp.CurrentTime):
            raise Refused(f"line {line} reads current_time, a time with a"
                          " time zone, which MySQL does not have")
        if isinstance(node, exp.Substring):
            return self._substring(node, line)
        if isinstance(node, (exp.Greatest, exp.Least)):
            args = [node.this] + list(node.expressions)
            if len(args) < 2:
                return None
            # PostgreSQL leaves NULLs out; MySQL answers NULL for any
            made = [exp.Coalesce(this=a.copy(), expressions=[
                b.copy() for j, b in enumerate(args) if j != i])
                for i, a in enumerate(args)]
            return type(node)(this=made[0], expressions=made[1:])
        if isinstance(node, exp.Length):
            if k(node.this) == "bytes":
                return _fn("LENGTH", node.this.copy())
            return None
        if isinstance(node, exp.Trim):
            chars = node.args.get("expression")
            if chars is not None and not (
                    isinstance(chars, exp.Literal) and chars.is_string and
                    len(chars.this) == 1):
                raise Refused(f"line {line} trims a set of characters;"
                              " MySQL trims one string, not any of a set")
            return None
        if isinstance(node, exp.Log):
            if node.args.get("expression") is None:
                return _fn("LOG10", node.this.copy())
            return None
        if isinstance(node, exp.Trunc):
            if node.args.get("decimals") is None:
                node.set("decimals", exp.Literal.number(0))
            return None
        if isinstance(node, (exp.Left, exp.Right)):
            return self._left_right(node)
        if isinstance(node, exp.Pad):
            n = node.expression
            if not (isinstance(n, exp.Literal) and not n.is_string):
                node.set("expression", exp.Greatest(
                    this=n.copy(), expressions=[exp.Literal.number(0)]))
            return None
        if isinstance(node, exp.Extract):
            return self._extract(node, line)
        if isinstance(node, exp.Anonymous):
            return self._call(node, line)
        if isinstance(node, exp.Distinct) and node.args.get("on"):
            raise Refused(f"line {line} selects DISTINCT ON, which MySQL"
                          " does not have")
        if isinstance(node, exp.Update):
            self._update_order(node, line)
        return None

    def _interval(self, node, line):
        amount = node.this
        if not (isinstance(amount, exp.Literal) and re.fullmatch(
                r"-?\d+", str(amount.this).strip())) or \
                not isinstance(node.parent, (exp.Add, exp.Sub)):
            raise Refused(f"line {line} uses the interval"
                          f" {node.sql('postgres')[:40]}, which migkit's"
                          " rules carry only as a whole number of one unit"
                          " added to or taken from a date or time")
        unit = str(node.args.get("unit").name if node.args.get("unit")
                   else "").upper().rstrip("S")
        if unit not in ("MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY",
                        "WEEK", "MONTH", "YEAR"):
            raise Refused(f"line {line} uses an interval of {unit or '?'},"
                          " which migkit's rules have no counterpart for")
        return exp.Interval(this=exp.Literal.number(int(amount.this)),
                            unit=exp.Var(this=unit))

    def _division(self, node, line, query):
        a, b = self.kind(node.this), self.kind(node.expression)
        what = "divides" if not isinstance(node, exp.Mod) else \
            "takes the remainder of"
        if a is None or b is None:
            raise Refused(f"line {line} {what} values whose types are not"
                          " known here; PostgreSQL divides integers without"
                          " a remainder, MySQL with one")
        if a not in _NUMERIC or b not in _NUMERIC:
            raise Refused(f"line {line} {what} values that are not numbers")
        self.code("postgres.division", line)
        divisor = node.expression
        if not (isinstance(divisor, exp.Literal) and not divisor.is_string
                and float(divisor.this) != 0):
            if not query and self._local(divisor) and not self._guarded(node):
                # PostgreSQL raises for a zero divisor; MySQL answers NULL
                self.pending.append(divisor.copy())
            else:
                self.note("a division by zero inside a query, or under a"
                          " condition, answers NULL in MySQL where"
                          " PostgreSQL raises division_by_zero")
        if isinstance(node, exp.Div) and a == b == "int":
            return exp.IntDiv(this=node.this.copy(),
                              expression=node.expression.copy())
        return None

    def _local(self, node):
        """Whether an expression reads only the routine's own values - so
        it can be asked again, before the statement, with no effect."""
        for n in node.walk():
            if isinstance(n, (exp.Select, exp.Subquery, exp.Anonymous,
                              exp.Rand, exp.Uuid)):
                return False
            if isinstance(n, exp.Column):
                table = (n.table or "").lower()
                if table in ("new", "old") and self.r.kind == "trigger":
                    continue
                if self._var_of(n) is None and not (
                        not n.table and n.name.lower() in _TG + ("found",)):
                    return False
        return True

    def _guarded(self, node):
        """Whether an expression is evaluated only under a condition."""
        p = node.parent
        while p is not None:
            if isinstance(p, (exp.Case, exp.If, exp.Coalesce, exp.Nullif,
                              exp.And, exp.Or)):
                return True
            p = p.parent
        return False

    def _date_arith(self, node, line):
        a, b = self.kind(node.this), self.kind(node.expression)
        if a not in ("date", "ts") and b not in ("date", "ts"):
            return None
        self.code("postgres.date-arithmetic", line)
        if isinstance(node.expression, exp.Interval) or isinstance(
                node.this, exp.Interval):
            return None
        if isinstance(node, exp.Sub) and a == b == "date":
            return _fn("DATEDIFF", node.this.copy(), node.expression.copy())
        if a == "date" and b == "int":
            cls = exp.DateAdd if isinstance(node, exp.Add) else exp.DateSub
            return cls(this=node.this.copy(), expression=node.expression.copy(),
                       unit=exp.Var(this="DAY"))
        if b == "date" and a == "int" and isinstance(node, exp.Add):
            return exp.DateAdd(this=node.expression.copy(),
                               expression=node.this.copy(),
                               unit=exp.Var(this="DAY"))
        raise Refused(f"line {line} does arithmetic on times"
                      f" ({node.sql('postgres')[:40]}) migkit's rules have"
                      " no counterpart for")

    def _cast(self, node, line):
        T = exp.DataType.Type
        to = node.to
        t = to.this
        src = self.kind(node.this)
        x = node.this.copy()
        if t in (T.INT, T.BIGINT, T.SMALLINT):
            self.note("a cast to an integer is to MySQL's 64-bit SIGNED: the"
                      " range of the type it names is checked where the"
                      " value is stored, not by the cast")
            return None
        if t == T.DECIMAL:
            if to.expressions:
                return None
            self.note(my_type("numeric")[2])
            return _cast(x, "DECIMAL(65,30)")
        if t in (T.FLOAT, T.DOUBLE, T.DATE):
            return None
        if t in (T.TEXT, T.VARCHAR, T.NVARCHAR) or (t == T.CHAR and not
                                                    to.expressions):
            text = self.text_of(x, "text")
            if to.expressions:
                n = to.expressions[0].this
                return _cast(text, f"CHAR({n.this if hasattr(n, 'this') else n})")
            return text if text is not x else _cast(x, "CHAR")
        if t == T.BOOLEAN:
            if src == "bool":
                return x
            if src == "int":
                return exp.NEQ(this=x, expression=exp.Literal.number(0))
            got = _constant_text(x)
            if got is not None and got.strip().lower() in _BOOL_WORDS:
                return exp.Boolean(this=_BOOL_WORDS[got.strip().lower()])
            raise Refused(f"line {line} casts to boolean from a value MySQL"
                          " does not read as PostgreSQL does")
        if t in (T.TIMESTAMP, T.DATETIME):
            n = to.expressions[0].this.this if to.expressions else 6
            return _cast(x, f"DATETIME({n})")
        if t == T.TIME:
            n = to.expressions[0].this.this if to.expressions else 6
            return _cast(x, f"TIME({n})")
        if t in (T.JSON, T.JSONB):
            return _cast(x, "JSON")
        raise Refused(f"line {line} casts to {to.sql('postgres')}, which"
                      " migkit's rules have no counterpart for")

    def _substring(self, node, line):
        start = node.args.get("start")
        length = node.args.get("length")
        if start is not None and self.kind(start) == "text":
            raise Refused(f"line {line} takes a substring by a pattern,"
                          " which migkit's rules have no counterpart for")
        if start is None or (isinstance(start, exp.Literal) and
                             not start.is_string and int(start.this) >= 1):
            return None
        # from before the first character PostgreSQL counts the length
        # from there; MySQL answers '' (0) or counts from the end (< 0)
        first = exp.Greatest(this=start.copy(),
                             expressions=[exp.Literal.number(1)])
        made = exp.Substring(this=node.this.copy(), start=first)
        if length is not None:
            made.set("length", exp.Sub(
                this=exp.Greatest(this=exp.Add(this=start.copy(),
                                               expression=length.copy()),
                                  expressions=[exp.Literal.number(1)]),
                expression=first.copy()))
        return made

    def _left_right(self, node):
        n = node.expression
        if isinstance(n, exp.Literal) and not n.is_string and \
                int(n.this) >= 0:
            return None
        # a negative count is all but that many characters in PostgreSQL,
        # nothing in MySQL
        cut = type(node)(this=node.this.copy(), expression=exp.Greatest(
            this=exp.Add(this=_fn("CHAR_LENGTH", node.this.copy()),
                         expression=n.copy()),
            expressions=[exp.Literal.number(0)]))
        if isinstance(n, exp.Neg) or (isinstance(n, exp.Literal) and
                                      int(n.this) < 0):
            return cut
        return exp.Case(ifs=[exp.If(
            this=exp.GTE(this=n.copy(), expression=exp.Literal.number(0)),
            true=type(node)(this=node.this.copy(), expression=n.copy()))],
            default=cut)

    def _extract(self, node, line):
        unit = str(node.this.name).upper()
        if unit in ("YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "QUARTER"):
            return None
        if unit == "DOW":
            return exp.Sub(this=_fn("DAYOFWEEK", node.expression.copy()),
                           expression=exp.Literal.number(1))
        if unit == "DOY":
            return _fn("DAYOFYEAR", node.expression.copy())
        raise Refused(f"line {line} extracts {unit.lower()}, which MySQL"
                      " counts otherwise")

    def _call(self, node, line):
        name = str(node.name)
        low = name.lower()
        callee = self.ctx.routines.get(low)
        if callee is not None and callee.kind == "function":
            self.code("postgres.call-function", line)
            node.set("this", name)
            return None
        if low in ("now", "transaction_timestamp", "statement_timestamp"):
            self.code("postgres.clock", line)
            return _fn("NOW", exp.Literal.number(6))
        if low == "clock_timestamp":
            self.code("postgres.clock", line)
            return _fn("SYSDATE", exp.Literal.number(6))
        if name.upper() in _MADE:
            return None
        raise Refused(f"line {line} calls {name}(), which migkit's rules have"
                      " no counterpart for")

    def _update_order(self, node, line):
        """MySQL's UPDATE reads a column set earlier in the same SET list
        as its new value; PostgreSQL reads every one as it was."""
        seen = set()
        for eq in node.expressions:
            if not isinstance(eq, exp.EQ):
                raise Refused(f"line {line} sets several columns from one row"
                              " value, which MySQL does not have")
            for col in eq.expression.find_all(exp.Column):
                if col.name.lower() in seen:
                    raise Refused(f"line {line} reads {col.name} after setting"
                                  " it in the same UPDATE: MySQL reads the"
                                  " new value, PostgreSQL the old")
            seen.add(eq.this.name.lower())

    # ---- names -----------------------------------------------------------

    def names(self, tree, line):
        """Variables, record fields, a trigger's rows and words, as MySQL
        names them; tables' columns as the target keeps them."""
        for col in list(tree.find_all(exp.Column)):
            if col.parent is None and col is not tree:
                continue
            if isinstance(col.parent, exp.EQ) and col.parent.this is col and \
                    isinstance(col.parent.parent, (exp.Update,
                                                   exp.OnConflict)):
                col.set("this", self._column_name(col.name))
                continue
            qual = (col.table or "").lower()
            if qual in ("new", "old") and self.r.kind == "trigger":
                self._row(col, qual, line)
                continue
            if not col.table:
                special = self._special(col, line)
                if special is not None:
                    col.replace(special)
                    continue
            var = self._var_of(col)
            if var is not None:
                var.used = True
                col.replace(var.node())
                continue
            if not col.table and self.lookup(col.name.lower()) is not None \
                    and self.lookup(col.name.lower()).record:
                raise Refused(f"line {line} reads the record {col.name} as a"
                              " whole, which MySQL has no value for")
            col.set("this", self._column_name(col.name))

    def _column_name(self, name):
        made = self.ctx.columns.get(str(name).lower(), str(name))
        return exp.to_identifier(made, quoted=not (
            made == made.lower() and made.isidentifier()))

    def _row(self, col, which, line):
        event = self.event
        if (which == "old" and event == "INSERT") or \
                (which == "new" and event == "DELETE"):
            # the row the event has not: NULL in PostgreSQL, refused by
            # MySQL's CREATE
            col.replace(exp.Null())
            return
        col.set("table", exp.to_identifier(which.upper()))
        col.set("this", self._column_name(col.name))

    def _special(self, col, line):
        name = col.name.lower()
        if col.this.args.get("quoted"):
            return None
        if name == "found" and self.lookup("found") is None:
            return exp.column(self.found)
        if name in _TG:
            if self.r.kind != "trigger":
                return None
            words = {"tg_op": self.event, "tg_when": self.r.timing,
                     "tg_level": "ROW", "tg_name": self.r.name,
                     "tg_table_name": str(self.r.table).split(".")[-1],
                     "tg_relname": str(self.r.table).split(".")[-1]}
            if name not in words:
                raise Refused(f"line {line} reads {name}, which a MySQL"
                              " trigger has no counterpart for")
            self.code("postgres.trigger-word", line)
            return _lit(words[name])
        return None

    # ---- the routine -----------------------------------------------------

    def convert(self):
        r = self.r
        if r.language not in (None, "plpgsql", "sql"):
            raise Refused(f"it is written in {r.language}, which migkit's"
                          " rules do not read")
        opts = _options(r)
        if opts["variadic"]:
            raise Refused("it takes a VARIADIC parameter, which MySQL does"
                          " not have")
        if opts["defaults"] and r.kind != "trigger":
            self.residue.append(f"{r.said} gives some parameters defaults;"
                                " MySQL takes every argument, so a caller"
                                " leaving one out needs it written in")
        if opts["settings"]:
            self.residue.append(f"{r.said} sets {', '.join(opts['settings'])}"
                                " for its own run; MySQL keeps no setting"
                                " with a routine")
        if r.kind == "function":
            if r.returns_set:
                raise Refused("it returns a set of rows (SETOF or TABLE); a"
                              " MySQL function answers one value")
            if any(p.mode != "in" for p in r.params):
                raise Refused("it has OUT parameters, so it answers a row; a"
                              " MySQL function answers one value")
            if str(r.returns).lower() in ("void", "record", "trigger"):
                raise Refused(f"it returns {r.returns}; a MySQL function"
                              " answers one value of a type")
        scope = self.scopes[0]
        self.params = []
        for p in r.params if r.kind != "trigger" else ():
            if not p.name:
                raise Refused("a parameter has no name")
            typ, kind, note = my_type(p.type, param=True)
            self.note(note)
            key = _key(p)
            if any(v.my.strip("`").lower() == p.name.lower()
                   for v in scope.values()):
                raise Refused(f"its parameters {p.name} and another differ"
                              " only by case, which MySQL does not tell"
                              " apart")
            scope[key] = _Var(key, _quote(p.name), typ, kind)
            self.params.append((p, typ))
        if r.language == "sql":
            return self._sql_body(opts)
        if not r.body:
            raise Refused("its body is kept parsed by the server (a SQL"
                          " standard body), not as text")
        reader = _Reader(r.body)
        tree = reader.body()
        if reader.directives.get("variable_conflict") == "use_column":
            raise Refused("it reads a name that is both a column and a"
                          " variable as the column (#variable_conflict"
                          " use_column); MySQL reads it as the variable")
        _gate(r, tree)
        self.words = {t.word for t in reader.t if t.kind == "word"}
        if "found" in self.words:
            self.found = self.helper("found", "BOOLEAN", "FALSE")
            self.code("postgres.found", tree.line)
        if any(n.kind == "getdiag" and any(i[1] == "row_count"
                                           for i in n.items)
               for n in tree.walk()):
            self.rows = self.helper("rows", "BIGINT", "0")
        return tree, opts

    def body(self, tree, opts):
        """The routine's statements under its own BEGIN: the rules'
        helpers, then the source's outermost block."""
        r = self.r
        inner = Emitter()
        inner.depth = 1
        self.e = inner
        self.tail = tree.body[-1] if tree.body else None
        if r.kind == "function" and opts["strict"] and r.params:
            # a STRICT function is not run at all for a NULL argument
            test = " OR ".join(f"{self.scopes[0][_key(p)].my} IS NULL"
                               for p, _ in self.params)
            inner(f"IF {test} THEN")
            inner.push()
            inner("RETURN NULL;")
            inner.pop()
            inner("END IF;")
        atomic = r.kind == "procedure" and self._atomic(tree)
        committing = r.kind == "procedure" and any(
            n.kind in ("commit", "rollback") for n in tree.walk())
        if committing:
            self.commits = True
            atomic = False
        sp = None
        if atomic:
            # a CALL is all or nothing in PostgreSQL: what the procedure
            # wrote is rolled back when it fails, inside its caller's
            # transaction
            sp = _quote(f"_migkit_sp_{r.name.lower()}"[:64])
            own = self.helper("sp", "BOOLEAN", "FALSE")
            self.code("postgres.call-atomic", tree.line)
            inner("IF @@in_transaction = 1 THEN")
            inner.push()
            inner(f"SAVEPOINT {sp};")
            inner(f"SET {own} = TRUE;")
            inner.pop()
            inner("END IF;")
            self.note("a procedure that writes and then fails leaves nothing"
                      " written inside its caller's transaction, as in"
                      " PostgreSQL; called in autocommit mode, MySQL keeps"
                      " what each of its statements wrote")
        if committing:
            inner("START TRANSACTION;")
        label = self.label_for(tree.label) if tree.label else self.body_label
        self.body_label = label
        self.block(tree, label, outermost=True)
        if committing:
            inner("COMMIT;")
        handler = []
        if atomic:
            handler = ["DECLARE EXIT HANDLER FOR SQLEXCEPTION", "BEGIN",
                       f"  IF {own} THEN", f"    ROLLBACK TO SAVEPOINT {sp};",
                       "  END IF;", "  RESIGNAL;", "END;"]
        elif committing:
            handler = ["DECLARE EXIT HANDLER FOR SQLEXCEPTION", "BEGIN",
                       "  ROLLBACK;", "  RESIGNAL;", "END;"]
        head = [f"DECLARE {n} {t}" + (f" DEFAULT {d}" if d is not None
                                       else "") + ";"
                for n, t, d in self.helpers] + handler
        return ["BEGIN"] + ["  " + h for h in head] + inner.lines + ["END"]

    def _atomic(self, tree):
        """Whether a procedure writes before something that can still
        fail: every write but a last statement of its own."""
        writes = [n for n in tree.walk() if self._writes_here(n)]
        if not writes:
            return False
        body = list(tree.body)
        while body and body[-1].kind in ("null", "return"):
            body.pop()
        return not (len(writes) == 1 and body and writes[0] is body[-1]
                    and not tree.handlers)

    def _writes_here(self, node):
        """Whether a statement may write: DML, a CALL, a transaction's
        end, or a call of a source routine that writes."""
        if node.kind == "execsql":
            if node.verb not in ("select", "with", "values", "table"):
                return True
            try:
                tree = parse(node.text, "postgres")
                if any(True for _ in tree.find_all(exp.Insert, exp.Update,
                                                   exp.Delete)):
                    return True
            except Refused:
                return True
        if node.kind in ("call", "commit", "rollback"):
            return True
        text = " ".join(str(v) for v in node.a.values()
                        if isinstance(v, str))
        return any(self._callee_writes(n)
                   for n in re.findall(r"([A-Za-z_]\w*)\s*\(", text))

    def _callee_writes(self, name, seen=None):
        """Whether a source routine called by name may write."""
        seen = set() if seen is None else seen
        callee = self.ctx.routines.get(name.lower())
        if callee is None or name.lower() in seen:
            return False
        seen.add(name.lower())
        if str(callee.settings.get("data") or "") in ("NO SQL",
                                                       "READS SQL DATA"):
            return False
        body = callee.body or ""
        if callee.language == "sql":
            return bool(re.search(r"(?i)\b(insert|update|delete|merge|"
                                  r"truncate)\b", body))
        try:
            tree = read(body)
        except Refused:
            return True
        for n in tree.walk():
            if n.kind in ("execsql", "call") and self._writes_here(n):
                return True
        return any(self._callee_writes(m, seen) for m in re.findall(
            r"([A-Za-z_]\w*)\s*\(", body))

    def label_for(self, pg_label):
        mine = pg_label if pg_label and len(pg_label) <= 16 and \
            re.fullmatch(r"[a-z_][a-z0-9_]*", pg_label) and \
            pg_label not in _RESERVED else None
        if mine is None or any(lab[1] == mine for lab in self.labels):
            self.serial += 1
            mine = f"_migkit_l{self.serial}"
        return mine

    def _sql_body(self, opts):
        """A `language sql` routine: one statement."""
        r = self.r
        stmts = [s for s in _split(r.body or "") if s.strip()]
        if len(stmts) != 1:
            raise Refused(f"its `language sql` body is {len(stmts)}"
                          " statements; migkit's rules carry one")
        text = stmts[0]
        tree = parse(text, "postgres")
        e = Emitter()
        e.depth = 1
        self.e = e
        if r.kind == "function":
            if not isinstance(tree, (exp.Select, exp.Union)):
                raise Refused("its `language sql` body is not a query")
            if isinstance(tree, exp.Select) and not tree.args.get("from_") \
                    and not tree.args.get("from") and \
                    len(tree.expressions) == 1:
                value = self.expr(tree.expressions[0].unalias().sql(
                    "postgres"), 1, to=my_type(r.returns, True)[1])
                self.flush(1)
                e(f"RETURN {value};")
            else:
                if isinstance(tree, exp.Select) and len(tree.expressions) != 1:
                    raise Refused("its query answers several columns; the"
                                  " function answers one value")
                self.code("postgres.sql-function", 1)
                if tree.args.get("limit") is None:
                    # the first row, as PostgreSQL takes it; MySQL refuses a
                    # subquery of several rows
                    tree = tree.limit(1)
                e(f"RETURN ({self.finish(tree, 1, query=True)});")
            if opts["strict"] and r.params:
                test = " OR ".join(f"{self.scopes[0][_key(p)].my} IS NULL"
                                   for p, _ in self.params)
                e.lines.insert(0, f"  IF {test} THEN RETURN NULL; END IF;")
        else:
            if not isinstance(tree, (exp.Insert, exp.Update, exp.Delete)):
                raise Refused("its `language sql` body is not one INSERT,"
                              " UPDATE or DELETE")
            self.s_execsql(Node("execsql", 0, 1, verb=tree.key, text=text,
                                targets=None, strict=False))
        return ["BEGIN"] + [f"  DECLARE {n} {t}" + (
            f" DEFAULT {d}" if d is not None else "") + ";"
            for n, t, d in self.helpers] + e.lines + ["END"]

    # ---- blocks ----------------------------------------------------------

    def block(self, node, label, outermost=False):
        e = self.e
        scope = {}
        self.scopes.append(scope)
        decls = []
        cursors = []
        records = []
        for d in node.decls:
            if d.name in scope:
                raise Refused(f"line {d.line} declares {d.name} twice")
            if d.what == "alias":
                target = d.target
                if target.startswith("$"):
                    k = int(target[1:])
                    if not 0 < k <= len(self.r.params):
                        raise Refused(f"line {d.line} names ${k}, not a"
                                      " parameter")
                    target = _key(self.r.params[k - 1])
                got = self.lookup(target)
                if got is None:
                    raise Refused(f"line {d.line} names {d.target}, not a"
                                  " variable")
                scope[d.name] = got
                continue
            if d.what == "cursor":
                if d.args:
                    raise Refused(f"line {d.line} declares a cursor with"
                                  " arguments, which MySQL does not have")
                scope[d.name] = _Var(d.name, _quote(self.fresh("c")),
                                     cursor=d)
                cursors.append((scope[d.name], d))
                continue
            typ = d.type.strip()
            if typ.lower() in ("record",) or typ.lower().endswith(
                    "%rowtype"):
                if typ.lower().endswith("%rowtype"):
                    raise Refused(f"line {d.line} declares {d.name} as a"
                                  " table's row (%ROWTYPE), whose columns"
                                  " MySQL needs one variable each for")
                scope[d.name] = _Var(d.name, _quote(d.name), record=True)
                records.append(scope[d.name])
                continue
            mytype, kind = self._decl_type(typ, d)
            my = _quote(d.name)
            self._clash(d.name, d.line)
            value = None
            if d.default is not None:
                value = self.expr(d.default, d.line, to=kind)
                if self.pending:
                    raise Refused(f"line {d.line} divides by a value in a"
                                  " declaration's default, which migkit's"
                                  " rules cannot check first")
            elif d.notnull:
                raise Refused(f"line {d.line} declares {d.name} NOT NULL"
                              " with no default")
            scope[d.name] = _Var(d.name, my, mytype, kind, notnull=d.notnull)
            decls.append(f"DECLARE {my} {mytype}" +
                         (f" DEFAULT {value}" if value is not None else "")
                         + ";")
        for rec in records:
            for field, (typ, kind) in self._record_fields(rec, node).items():
                my = self._field_name(rec.name, field)
                rec.fields[field] = _Var(field, my, typ, kind)
                decls.append(f"DECLARE {my} {typ};")
        cursor_lines = []
        for var, d in cursors:
            self.code("postgres.cursor", d.line)
            cursor_lines.append(f"DECLARE {var.my} CURSOR FOR"
                                f" {self.query(d.query, d.line)};")
        handlers = self.handlers(node, label, outermost) \
            if node.handlers else []
        self.labels.append((node.label, label, "block", self.in_handler))
        body = Emitter()
        body.depth = e.depth + 1
        self.e = body
        self.run(node.body)
        if node.handlers:
            # a handler that runs goes on after the block
            self.ended = False
        if outermost:
            while body.lines and body.lines[-1].strip() == f"LEAVE {label};":
                body.lines.pop()
        self.e = e
        self.labels.pop()
        self.scopes.pop()
        e(f"{label}: BEGIN" if label else "BEGIN")
        e.push()
        for line in decls + cursor_lines:
            e(line)
        e.pop()
        e.lines.extend(handlers)
        e.lines.extend(body.lines)
        e(f"END {label};" if label else "END;")

    def _decl_type(self, typ, d):
        m = re.fullmatch(r'(?is)\s*("[^"]+"|[\w$]+)(?:\s*\.\s*("[^"]+"|[\w$]+'
                         r'))?\s*%\s*type\s*', typ)
        if m:
            if m.group(2) is None:
                got = self.lookup(_unquote(m.group(1)))
                if got is None or got.mytype is None:
                    raise Refused(f"line {d.line} declares {d.name} as the"
                                  f" type of {m.group(1)}, not a variable")
                return got.mytype, got.kind
            table = self._target_table(_unquote(m.group(1)))
            col = _unquote(m.group(2)).lower()
            found = (self.types.get(table.lower()) or {}).get(col)
            if found is None:
                raise Refused(f"line {d.line} declares {d.name} as the type"
                              f" of {m.group(1)}.{m.group(2)}, a column whose"
                              " type on the target is not known here")
            return _declared(found)
        mytype, kind, note = my_type(typ)
        self.note(note)
        return mytype, kind

    def _target_table(self, name):
        tree = exp.select("*").from_(exp.to_table(name))
        if self.ctx.local is not None:
            tree = self.ctx.local(tree)
        table = tree.find(exp.Table)
        return self.ctx.tables.get(table.name.lower(), table.name)

    def _clash(self, name, line):
        low = name.lower()
        for scope in self.scopes:
            for other in scope.values():
                if other.my.strip("`").lower() == low and other.name != name:
                    raise Refused(f"line {line}: {name} and {other.name}"
                                  " differ only by case, which MySQL does not"
                                  " tell apart")

    def _field_name(self, rec, field):
        plain = f"{rec}_{field}".lower()
        if plain in self.words or plain in self.ctx.columns or \
                not re.fullmatch(r"[a-z_][a-z0-9_]*", plain):
            plain = self.fresh(re.sub(r"\W", "_", plain))
        return _quote(plain)

    def _record_fields(self, rec, block):
        """{field: (MySQL type, kind)} of a record, from every query that
        fills it in this block."""
        out = {}

        def visit(nodes):
            for n in nodes:
                if n.kind == "block" and any(
                        d.name == rec.name for d in n.decls):
                    continue
                query = None
                if n.kind == "fors" and len(n.targets) == 1 and \
                        _unquote(n.targets[0]) == rec.name:
                    query = n.query
                elif n.kind == "execsql" and n.targets and \
                        len(n.targets) == 1 and \
                        _unquote(n.targets[0]) == rec.name:
                    query = n.text
                if query is not None:
                    for f, t in self.fields_of(query, n.line):
                        if f in out and out[f] != t:
                            raise Refused(f"line {n.line} fills {rec.name}"
                                          f"'s field {f} with another type"
                                          " than before")
                        out[f] = t
                visit(n.children())
        visit(block.body)
        for _, stmts, _ in block.handlers:
            visit(stmts)
        return out

    def fields_of(self, query, line):
        """[(name, (MySQL type, kind))] of a query's columns."""
        tree = parse(query, "postgres")
        selects = [tree] if isinstance(tree, exp.Select) else \
            list(tree.find_all(exp.Select))[:1]
        if not selects:
            raise Refused(f"line {line}: its query could not be read")
        sel = selects[0]
        if self.ctx.local is not None:
            sel = self.ctx.local(sel.copy())
        tables = {}
        for t in sel.find_all(exp.Table):
            leaf = self.ctx.tables.get(t.name.lower(), t.name)
            tables[(t.alias_or_name or t.name).lower()] = leaf.lower()
            tables[t.name.lower()] = leaf.lower()
        out = []
        for item in sel.expressions:
            if isinstance(item, exp.Star) or (isinstance(item, exp.Column)
                                              and item.is_star):
                raise Refused(f"line {line} selects * into a record; MySQL"
                              " needs each column named")
            name = item.alias_or_name
            if not name:
                raise Refused(f"line {line}: a column of its query has no"
                              " name")
            value = item.unalias()
            typ = None
            if isinstance(value, exp.Cast):
                try:
                    typ = my_type(value.to.sql("postgres"))[:2]
                except Refused:
                    typ = None
            elif isinstance(value, exp.Column):
                var = self._var_of(value) if not value.table else None
                if var is not None and var.mytype:
                    typ = (var.mytype, var.kind)
                else:
                    owner = tables.get((value.table or "").lower()) if \
                        value.table else None
                    names = [owner] if owner else list(set(tables.values()))
                    got = [self.types.get(t, {}).get(value.name.lower())
                           for t in names]
                    got = [g for g in got if g]
                    if len(got) == 1:
                        typ = _declared(got[0])
            elif isinstance(value, exp.Count):
                typ = ("BIGINT", "int")
            if typ is None:
                raise Refused(f"line {line}: the type of its query's column"
                              f" {name} is not known here (the target's"
                              " column types are not given)")
            out.append((name.lower(), typ))
        return out

    def handlers(self, node, label, outermost):
        """The EXIT handlers of an exception block, as DECLAREs."""
        writes = [n for s in node.body for n in s.walk()
                  if self._writes_here(n) or n.kind in ("commit",
                                                        "rollback")]
        if writes:
            self.code("postgres.exception-block-writes", node.line)
            raise Refused(f"line {writes[0].line} writes inside a block"
                          " with an EXCEPTION handler: PostgreSQL rolls back"
                          " everything the block did before its handler"
                          " runs, MySQL only the statement that failed")
        self.code("postgres.exception-block", node.line)
        out, seen, after_others = [], set(), False
        for conds, stmts, line in node.handlers:
            if after_others:
                self.code("postgres.handler-unreachable", line)
                continue
            values = []
            for c in conds:
                for v in self._handler_values(c, line):
                    if v not in seen:
                        seen.add(v)
                        values.append(v)
                if c == ("name", "others"):
                    after_others = True
            if not values:
                continue
            e = Emitter()
            e.depth = self.e.depth + 1
            outer, self.e = self.e, e
            self.in_handler += 1
            hscope = {"sqlstate": _Var("sqlstate", self.fresh("state"),
                                       "CHAR(5)", "text"),
                      "sqlerrm": _Var("sqlerrm", self.fresh("errm"),
                                      "TEXT" + BINARY, "text")}
            errno = self.fresh("errno")
            self.scopes.append(hscope)
            saved = (self.handler_scope, self.handler_at_top,
                     self.handler_last)
            self.handler_scope, self.handler_at_top = hscope, outermost
            self.handler_last = stmts[-1] if stmts else None
            try:
                body = Emitter()
                body.depth = e.depth + 1
                self.e = body
                self.run(stmts)
                self.ended = False
                reads = hscope["sqlstate"].used or hscope["sqlerrm"].used
            finally:
                self.scopes.pop()
                self.in_handler -= 1
                self.handler_scope, self.handler_at_top, \
                    self.handler_last = saved
                self.e = outer
            e(f"DECLARE EXIT HANDLER FOR {', '.join(values)}")
            e("BEGIN")
            e.push()
            if reads:
                st, em = hscope["sqlstate"].my, hscope["sqlerrm"].my
                self.helpers += [(st, "CHAR(5)", None),
                                 (em, "TEXT" + BINARY, None),
                                 (errno, "INT", None)]
                e(f"GET STACKED DIAGNOSTICS CONDITION 1 {st} ="
                  f" RETURNED_SQLSTATE, {em} = MESSAGE_TEXT, {errno} ="
                  " MYSQL_ERRNO;")
                # MySQL's own failures carry its codes; PostgreSQL's code
                # for the same failure where one is known
                cases = " ".join(f"WHEN {n} THEN '{_codes()[name]}'"
                                 for n, name in sorted(MYSQL_ERRNO.items())
                                 if name in _codes())
                e(f"SET {st} = CASE {errno} {cases} ELSE {st} END;")
                self.note("SQLSTATE and SQLERRM in a handler hold MySQL's own"
                          " message for a failure MySQL raises itself, and"
                          " PostgreSQL's code where migkit knows it")
            e.pop()
            e.lines.extend(body.lines)
            e("END;")
            out += e.lines
        return out

    def _handler_values(self, cond, line):
        if cond == ("name", "others"):
            return ["SQLEXCEPTION"]
        code = _code_of(cond, line)
        if code[2:] == "000":
            raise Refused(f"line {line} handles a whole class of failures"
                          f" ({cond[1]}), whose MySQL codes migkit cannot"
                          " list")
        if code[:2] in ("00", "01", "02"):
            raise Refused(f"line {line} handles {cond[1]}, which is not an"
                          " error")
        name = next((n for n, c in _codes().items() if c == code), cond[1])
        return [str(n) for n in sorted(_errnos().get(name, []))] + \
            [f"SQLSTATE '{code}'"]

    # ---- statements --------------------------------------------------------

    def statement(self, node):
        getattr(self, "s_" + node.kind, self.s_unsupported)(node)

    def s_unsupported(self, node):
        said = {"return_next": "RETURN NEXT", "return_query": "RETURN QUERY",
                "foreach_a": "FOREACH", "dynexecute": "EXECUTE",
                "dynfors": "FOR ... IN EXECUTE", "forc": "FOR over a cursor",
                "open": "OPEN"}.get(node.kind, node.kind.upper())
        self.code(f"postgres.{node.kind.replace('_', '-')}", node.line)
        raise Refused(f"line {node.line}: {said} - migkit's rules have no"
                      " counterpart for it")

    def s_block(self, node):
        label = self.label_for(node.label) if node.label else None
        self.block(node, label)

    def s_null(self, node):
        pass

    def flush(self, line):
        """The division checks the statement about to be written needs,
        before it."""
        checks, self.pending = self.pending, []
        for divisor in checks:
            tree = exp.select(exp.EQ(this=divisor,
                                     expression=exp.Literal.number(0)))
            self.names(tree, line)
            test = generate(tree, "postgres", _RulesMySQL)[len("SELECT "):]
            self.e(f"IF {test} THEN")
            self.e.push()
            self.e("SIGNAL SQLSTATE '22012' SET MESSAGE_TEXT = 'division by"
                   " zero';")
            self.e.pop()
            self.e("END IF;")

    def run(self, stmts):
        """Statements in order, up to one after which none runs (RETURN,
        EXIT, RAISE): past it PostgreSQL never goes, and a trigger's
        statements there may name a row its event has not."""
        for k, s in enumerate(stmts or []):
            self.statement(s)
            if self.ended:
                if k + 1 < len(stmts):
                    self.code("postgres.unreachable", stmts[k + 1].line)
                return

    def list(self, stmts):
        """A statement list inside IF/CASE/LOOP; MySQL refuses an empty
        one."""
        e = self.e
        e.push()
        before = len(e.lines)
        self.run(stmts)
        self.ended = False
        if len(e.lines) == before:
            e("BEGIN END;")
        e.pop()

    def target(self, text, line):
        """(MySQL's name for an assignment's target, its variable or
        None)."""
        parts = [p.strip() for p in re.split(r"\s*\.\s*", text.strip())]
        if "[" in text:
            raise Refused(f"line {line} assigns to an element of an array,"
                          " which MySQL does not have")
        names = [_unquote(p) for p in parts]
        if len(names) == 2 and names[0].lower() in ("new", "old") and \
                self.r.kind == "trigger":
            which = names[0].lower()
            if which == "old" or self.r.timing != "BEFORE" or \
                    self.event == "DELETE":
                raise Refused(f"line {line} assigns to {text}, which MySQL"
                              " lets a trigger write only as NEW before"
                              " the row is written")
            self.code("postgres.trigger-new", line)
            col = self._column_name(names[1]).sql(dialect=_RulesMySQL)
            return f"NEW.{col}", None
        if len(names) == 2:
            owner = self.lookup(names[0])
            if owner is not None and owner.record:
                got = owner.fields.get(names[1].lower())
                if got is None:
                    raise Refused(f"line {line}: the record {names[0]} has no"
                                  f" field {names[1]} known here")
                return got.my, got
            if any(lab[0] == names[0] for lab in self.labels):
                names = names[1:]
        if len(names) != 1:
            raise Refused(f"line {line} assigns to {text}, which migkit's"
                          " rules do not read")
        got = self.lookup(names[0])
        if got is None or got.literal is not None or got.cursor or \
                got.record:
            raise Refused(f"line {line} assigns to {text}, which is not a"
                          " variable")
        return got.my, got

    def after_set(self, vars_):
        for var in vars_:
            if var is not None and var.notnull:
                self.e(f"IF {var.my} IS NULL THEN")
                self.e.push()
                self.e("SIGNAL SQLSTATE '22004' SET MESSAGE_TEXT = 'null"
                       f" value cannot be assigned to variable \"{var.name}\""
                       " declared NOT NULL';")
                self.e.pop()
                self.e("END IF;")

    def s_assign(self, node):
        name, var = self.target(node.target, node.line)
        to = var.kind if var is not None else None
        value = self.expr(node.value, node.line, to=to)
        self.flush(node.line)
        self.e(f"SET {name} = {value};")
        self.after_set([var])

    def s_if(self, node):
        e = self.e
        live = []
        for cond, stmts, line in node.branches:
            text, const = self.cond(cond, line)
            checks, self.pending = self.pending, []
            if const is False:
                self.code("postgres.branch-not-taken", line)
                continue
            live.append((text, const, checks, stmts, line))
            if const is True:
                break
        closes, opened, done = 0, False, False
        for text, const, checks, stmts, line in live:
            if const is True:
                if not opened:
                    self.run(stmts)
                else:
                    e("ELSE")
                    self.list(stmts)
                done = True
                break
            if checks and opened:
                e("ELSE")
                e.push()
                closes += 1
                opened = False
            self.pending = checks
            if not opened:
                self.flush(line)
                e(f"IF {text} THEN")
                opened = True
            else:
                e(f"ELSEIF {text} THEN")
            self.list(stmts)
        if not done and node.otherwise is not None:
            if opened:
                e("ELSE")
                self.list(node.otherwise)
            else:
                self.run(node.otherwise)
        if opened:
            e("END IF;")
        for _ in range(closes):
            e.pop()
            e("END IF;")

    def s_case(self, node):
        e = self.e
        subject = None
        if node.subject is not None:
            subject = self.expr(node.subject, node.line)
            if self.pending:
                raise Refused(f"line {node.line}: CASE divides by a value"
                              " migkit's rules cannot check first")
        whens = []
        for value, stmts, line in node.whens:
            if subject is not None:
                tree = parse(f"SELECT {value}", "postgres")
                items = [self.finish(exp.select(x.copy()), line)[
                    len("SELECT "):] for x in tree.expressions]
                test = f"{subject} IN ({', '.join(items)})" \
                    if len(items) > 1 else None
                # a trigger's TG_OP against words: decided here
                const = (subject in items) if all(
                    re.fullmatch(r"'[^']*'", x) for x in items + [subject]) \
                    else None
                whens.append((test, items[0] if test is None else None,
                              const, stmts, line))
            else:
                text, const = self.cond(value, line)
                whens.append((text, None, const, stmts, line))
            if self.pending:
                raise Refused(f"line {line}: CASE divides by a value"
                              " migkit's rules cannot check first")
        live = []
        for w in whens:
            if w[2] is False:
                self.code("postgres.branch-not-taken", w[4])
                continue
            live.append(w)
            if w[2] is True:
                break
        otherwise = node.otherwise
        if live and live[-1][2] is True:
            otherwise = live.pop()[3]
        if not live:
            if otherwise is not None:
                self.run(otherwise)
                return
            # no branch and no ELSE: case_not_found, as in both
            self.e("SIGNAL SQLSTATE '20000' SET MESSAGE_TEXT = 'case not"
                   " found';")
            self.ended = True
            return
        simple = subject is not None and all(w[1] is not None for w in live)
        if subject is not None and not simple and not _simple_sql(subject):
            raise Refused(f"line {node.line}: CASE on an expression with"
                          " several values in a WHEN, which MySQL would"
                          " evaluate more than once")
        # a CASE with no branch taken and no ELSE raises case_not_found in
        # both (MySQL's 1339 is SQLSTATE 20000)
        e(f"CASE {subject}" if simple else "CASE")
        for test, value, _, stmts, line in live:
            if simple:
                e(f"  WHEN {value} THEN")
            else:
                e(f"  WHEN {test if test else f'{subject} = {value}'} THEN")
            e.push()
            self.list(stmts)
            e.pop()
        if otherwise is not None:
            e("  ELSE")
            e.push()
            self.list(otherwise)
            e.pop()
        e("END CASE;")

    # ---- loops -----------------------------------------------------------

    def _loop_label(self, node):
        return self.label_for(node.label)

    def s_loop(self, node):
        label = self._loop_label(node)
        self.e(f"{label}: LOOP")
        self.labels.append((node.label, label, "loop", self.in_handler))
        self.list(node.body)
        self.labels.pop()
        self.e(f"END LOOP {label};")

    def s_while(self, node):
        label = self._loop_label(node)
        cond, const = self.cond(node.cond, node.line)
        if self.pending:
            raise Refused(f"line {node.line}: WHILE divides by a value"
                          " migkit's rules cannot check before each test")
        self.e(f"{label}: WHILE {cond} DO")
        self.labels.append((node.label, label, "loop", self.in_handler))
        self.list(node.body)
        self.labels.pop()
        self.e(f"END WHILE {label};")

    def s_fori(self, node):
        """An integer FOR: its bounds evaluated once, its variable a copy of
        a counter of its own (the body writing it does not move the
        loop), stepped at the top so CONTINUE steps too."""
        e = self.e
        self.code("postgres.for-integer", node.line)
        if len(node.targets) != 1:
            raise Refused(f"line {node.line}: an integer FOR over several"
                          " variables")
        values = []
        for what, text in (("lower", node.lower), ("upper", node.upper),
                           ("BY", node.step)):
            if text is None:
                values.append(None)
                continue
            values.append((what, self.expr(text, node.line), text))
        self.flush(node.line)
        lo = self.helper("lo", "INT")
        hi = self.helper("hi", "INT")
        at = self.helper("i", "BIGINT")
        e(f"SET {lo} = {values[0][1]}, {hi} = {values[1][1]};")
        step = "1"
        if values[2] is not None:
            by = self.helper("by", "INT")
            e(f"SET {by} = {values[2][1]};")
            step = by
        for (what, _, text), name in ((values[0], lo), (values[1], hi)) + (
                ((values[2], step),) if values[2] is not None else ()):
            if _plain_number(text):
                continue
            said = f"{what} bound of FOR loop" if what != "BY" else \
                "BY value of FOR loop"
            e(f"IF {name} IS NULL THEN")
            e.push()
            e(f"SIGNAL SQLSTATE '22004' SET MESSAGE_TEXT = '{said} cannot be"
              " null';")
            e.pop()
            e("END IF;")
        if values[2] is not None and not _plain_number(values[2][2]):
            e(f"IF {step} <= 0 THEN")
            e.push()
            e("SIGNAL SQLSTATE '22023' SET MESSAGE_TEXT = 'BY value of FOR"
              " loop must be greater than zero';")
            e.pop()
            e("END IF;")
        elif values[2] is not None and int(values[2][2]) <= 0:
            raise Refused(f"line {node.line}: a FOR loop's BY is not above"
                          " zero")
        name = _unquote(node.targets[0])
        var = _Var(name, _quote(name), "INT", "int")
        self._clash(name, node.line)
        sign, past = ("-", "<") if node.reverse else ("+", ">")
        e("BEGIN")
        e.push()
        e(f"DECLARE {var.my} INT;")
        e(f"SET {at} = {lo} {'+' if node.reverse else '-'} {step};")
        label = self._loop_label(node)
        e(f"{label}: LOOP")
        e.push()
        e(f"SET {at} = {at} {sign} {step};")
        e(f"IF {at} {past} {hi} THEN")
        e.push()
        e(f"LEAVE {label};")
        e.pop()
        e("END IF;")
        e(f"SET {var.my} = {at};")
        went = None
        if self.found:
            # FOUND is set when the loop ends, to whether it went round;
            # inside it only the body's own statements move it
            went = self.helper("went", "BOOLEAN", "FALSE")
            e(f"SET {went} = TRUE;")
        e.pop()
        self.scopes.append({name: var})
        self.labels.append((node.label, label, "loop", self.in_handler))
        self.list(node.body)
        self.labels.pop()
        self.scopes.pop()
        e(f"END LOOP {label};")
        e.pop()
        e("END;")
        if went:
            e(f"SET {self.found} = {went};")

    def s_fors(self, node):
        """FOR over a query: a cursor, its row fetched into the record's
        fields (or the variables named), the NOT FOUND of each FETCH held
        to the FETCH alone - so a SELECT INTO in the body finding no row
        does not end the loop."""
        e = self.e
        self.code("postgres.for-query", node.line)
        targets = self._fetch_targets(node.targets, node.query, node.line)
        cursor = _quote(self.fresh("cur"))
        done = self.fresh("done")
        went = self.fresh("went") if self.found or self.rows else None
        e("BEGIN")
        e.push()
        e(f"DECLARE {done} BOOLEAN DEFAULT FALSE;")
        e(f"DECLARE {cursor} CURSOR FOR {self.query(node.query, node.line)};")
        if self.pending:
            raise Refused(f"line {node.line}: its query divides by a value"
                          " migkit's rules cannot check first")
        if went:
            self.helpers.append((went, "BIGINT", "0"))
            e(f"SET {went} = 0;")
        e(f"OPEN {cursor};")
        label = self._loop_label(node)
        e(f"{label}: LOOP")
        e.push()
        e("BEGIN")
        e.push()
        e(f"DECLARE CONTINUE HANDLER FOR NOT FOUND SET {done} = TRUE;")
        e(f"FETCH {cursor} INTO {', '.join(targets)};")
        e.pop()
        e("END;")
        e(f"IF {done} THEN")
        e.push()
        e(f"LEAVE {label};")
        e.pop()
        e("END IF;")
        if went:
            e(f"SET {went} = {went} + 1;")
        e.pop()
        self.labels.append((node.label, label, "loop", self.in_handler))
        self.list(node.body)
        self.labels.pop()
        e(f"END LOOP {label};")
        e(f"CLOSE {cursor};")
        e.pop()
        e("END;")
        if self.found:
            e(f"SET {self.found} = {went} > 0;")
        if self.rows:
            e(f"SET {self.rows} = {went};")

    def _fetch_targets(self, targets, query, line):
        if len(targets) == 1:
            var = self.lookup(_unquote(targets[0]))
            if var is not None and var.record:
                if not var.fields:
                    raise Refused(f"line {line}: the record {targets[0]}'s"
                                  " fields are not known here")
                names = [f for f, _ in self.fields_of(query, line)]
                return [var.fields[f].my for f in names]
        out = []
        for t in targets:
            name, var = self.target(t, line)
            if var is None or var.notnull:
                raise Refused(f"line {line} fetches into {t}, which is not a"
                              " variable, or one declared NOT NULL")
            out.append(name)
        return out

    def s_exit(self, node):
        e = self.e
        if node.label is not None:
            got = next((lab for lab in reversed(self.labels)
                        if lab[0] == node.label), None)
            if got is None:
                raise Refused(f"line {node.line} names the label"
                              f" {node.label}, not around it")
        else:
            got = next((lab for lab in reversed(self.labels)
                        if lab[2] == "loop"), None)
            if got is None:
                raise Refused(f"line {node.line}: EXIT or CONTINUE outside a"
                              " loop")
        if got[3] < self.in_handler:
            raise Refused(f"line {node.line} leaves an exception handler for"
                          " a label outside it, which MySQL refuses")
        if not node.is_exit and got[2] != "loop":
            raise Refused(f"line {node.line}: CONTINUE names a block")
        word = "LEAVE" if node.is_exit else "ITERATE"
        if node.cond is None:
            e(f"{word} {got[1]};")
            self.ended = True
            return
        cond, const = self.cond(node.cond, node.line)
        self.flush(node.line)
        e(f"IF {cond} THEN")
        e.push()
        e(f"{word} {got[1]};")
        e.pop()
        e("END IF;")

    # ---- RETURN, RAISE, ASSERT ---------------------------------------------

    def s_return(self, node):
        r = self.r
        e = self.e
        if r.kind == "function":
            if node.value is None:
                raise Refused(f"line {node.line}: RETURN with no value in a"
                              " function that answers one")
            kind = my_type(r.returns, param=True)[1]
            value = self.expr(node.value, node.line, to=kind)
            self.flush(node.line)
            e(f"RETURN {value};")
            self.ended = True
            return
        if r.kind == "procedure":
            if node.value is not None:
                raise Refused(f"line {node.line}: RETURN with a value in a"
                              " procedure")
            self._leave_body(node)
            self.ended = True
            return
        # a trigger: what it returns decides what happens to the row
        value = (node.value or "").strip().lower()
        if value not in ("new", "old", "null"):
            raise Refused(f"line {node.line} returns {node.value} from a"
                          " trigger; migkit's rules carry NEW, OLD and NULL")
        if r.timing == "BEFORE":
            skips = value == "null" or (value == "new" and
                                        self.event == "DELETE") or (
                value == "old" and self.event == "INSERT")
            if skips:
                raise Refused(f"line {node.line}: a BEFORE trigger returning"
                              f" {value.upper()} skips the row in PostgreSQL;"
                              " a MySQL trigger cannot skip a row")
            if value == "old" and self.event == "UPDATE":
                raise Refused(f"line {node.line}: a BEFORE UPDATE trigger"
                              " returning OLD keeps the old row, which a"
                              " MySQL trigger cannot")
        self.code("postgres.trigger-return", node.line)
        self._leave_body(node)
        self.ended = True

    def _leave_body(self, node):
        """RETURN from a procedure or a trigger: leaving its outermost
        block - or nothing, where it is the block's last statement or the
        last of a handler of that block (whose EXIT leaves it anyway)."""
        if self.in_handler:
            if self.in_handler == 1 and self.handler_at_top and \
                    node is self.handler_last:
                return
            raise Refused(f"line {node.line} returns from inside an"
                          " exception handler, which MySQL cannot leave for"
                          " the routine's end")
        if node is not self.tail:
            self.e(f"LEAVE {self.body_label};")

    def s_raise(self, node):
        e = self.e
        if node.reraise:
            if not self.in_handler:
                raise Refused(f"line {node.line}: RAISE with nothing outside"
                              " a handler")
            e("RESIGNAL;")
            self.ended = True
            return
        if node.level != "exception":
            # a notice writes nothing a caller or a table sees
            self.code(f"postgres.raise-{node.level}", node.line)
            return
        self.code("postgres.raise", node.line)
        code, message = None, None
        if node.cond is not None:
            code = _code_of(node.cond, node.line)
            message = _sql_string(node.cond[1])
        for opt, value in node.options:
            opt = opt.lower()
            if opt == "errcode":
                got = _constant_sql_text(value)
                if got is None:
                    raise Refused(f"line {node.line}: RAISE's ERRCODE is not"
                                  " written as a literal")
                code = _code_of(("sqlstate", got) if re.fullmatch(
                    r"[0-9A-Z]{5}", got) else ("name", got.lower()),
                    node.line)
            elif opt == "message":
                message = self.expr(value, node.line, render="text")
            else:
                self.code(f"postgres.raise-{opt}", node.line)
                self.note("RAISE's DETAIL, HINT and the names of the column,"
                          " constraint, type, table and schema are not kept:"
                          " MySQL's SIGNAL has no place for them")
        if node.fmt is not None:
            message = self._format(node.fmt, node.args, node.line)
        code = code or "P0001"
        if code[:2] in ("00", "01", "02"):
            raise Refused(f"line {node.line} raises SQLSTATE {code}, which is"
                          " not an error")
        if message is None:
            message = _sql_string(code)
        self.note("MySQL keeps 128 characters of an error's message; a longer"
                  " one is not the same")
        self.flush(node.line)
        self.signal(code, message)
        self.ended = True

    def signal(self, code, message):
        e = self.e
        if re.fullmatch(r"'(?:[^'\\]|''|\\.)*'", message):
            e(f"SIGNAL SQLSTATE '{code}' SET MESSAGE_TEXT = {message};")
            return
        # SIGNAL takes a literal or a variable, not an expression
        held = self.helper("msg", "TEXT" + BINARY)
        e(f"SET {held} = {message};")
        e(f"SIGNAL SQLSTATE '{code}' SET MESSAGE_TEXT = {held};")

    def _format(self, fmt, args, line):
        parts, k, i, lit = [], 0, 0, []
        while i < len(fmt):
            c = fmt[i]
            if c == "%" and fmt[i + 1:i + 2] == "%":
                lit.append("%")
                i += 2
                continue
            if c == "%":
                if k >= len(args):
                    raise Refused(f"line {line}: RAISE has fewer arguments"
                                  " than its message has %")
                if lit:
                    parts.append(_sql_string("".join(lit)))
                    lit = []
                value = self.expr(args[k], line, render="raise")
                # PostgreSQL writes a NULL argument as <NULL>
                parts.append(f"COALESCE({value}, '<NULL>')")
                k += 1
                i += 1
                continue
            lit.append(c)
            i += 1
        if k != len(args):
            raise Refused(f"line {line}: RAISE has more arguments than its"
                          " message has %")
        if lit:
            parts.append(_sql_string("".join(lit)))
        if not parts:
            return "''"
        return parts[0] if len(parts) == 1 else f"CONCAT({', '.join(parts)})"

    def s_assert(self, node):
        self.code("postgres.assert", node.line)
        cond, const = self.cond(node.cond, node.line)
        message = self.expr(node.message, node.line, render="text") \
            if node.message else "'assertion failed'"
        self.flush(node.line)
        self.e(f"IF NOT COALESCE({cond}, FALSE) THEN")
        self.e.push()
        self.signal("P0004", message)
        self.e.pop()
        self.e("END IF;")

    # ---- SQL -------------------------------------------------------------

    def query(self, text, line):
        tree = parse(text, "postgres")
        if not isinstance(tree, (exp.Select, exp.Union)):
            raise Refused(f"line {line}: a cursor's or a loop's query is not a"
                          " SELECT")
        self._no_writes(tree, line)
        return self.finish(tree, line, query=True)

    def _no_writes(self, tree, line):
        if any(True for _ in tree.find_all(exp.Insert, exp.Update,
                                           exp.Delete)):
            raise Refused(f"line {line} writes inside a query's WITH, which"
                          " MySQL does not have")

    def s_perform(self, node):
        self.code("postgres.perform", node.line)
        tree = parse(node.query, "postgres")
        if not isinstance(tree, exp.Select):
            raise Refused(f"line {node.line}: PERFORM of something that is"
                          " not a query")
        plain = not (tree.args.get("from_") or tree.args.get("from"))
        if plain:
            text = self.finish(tree, node.line)[len("SELECT "):]
            self.flush(node.line)
            self.e(f"DO {text};")
            if self.found:
                self.e(f"SET {self.found} = TRUE;")
            if self.rows:
                self.e(f"SET {self.rows} = 1;")
            return
        if any(isinstance(n, exp.Anonymous) for x in tree.expressions
               for n in x.walk()):
            raise Refused(f"line {node.line}: PERFORM calls a function for"
                          " each row of a query; MySQL cannot run a query"
                          " for its rows' sake")
        if tree.args.get("locks"):
            raise Refused(f"line {node.line}: PERFORM locks rows, which"
                          " migkit's rules have no counterpart for")
        query = self.finish(tree, node.line, query=True)
        if self.rows:
            self.e(f"SELECT COUNT(*) INTO {self.rows} FROM ({query}) AS"
                   " _migkit_q;")
            if self.found:
                self.e(f"SET {self.found} = {self.rows} > 0;")
        elif self.found:
            self.e(f"SET {self.found} = EXISTS ({query});")
        else:
            self.e(f"DO EXISTS ({query});")

    def s_execsql(self, node):
        verb = (node.verb or "").lower()
        if verb in ("select", "with", "values", "table") and \
                node.targets is not None:
            return self._select_into(node)
        if verb in ("select", "with", "values", "table"):
            raise Refused(f"line {node.line}: a SELECT with no INTO has"
                          " nowhere to put its rows")
        if verb not in ("insert", "update", "delete"):
            self.code(f"postgres.{verb}", node.line)
            why = {"truncate": " (MySQL's TRUNCATE commits by itself)",
                   "merge": ""}.get(verb, "")
            raise Refused(f"line {node.line}: {verb.upper()} - migkit's rules"
                          f" have no counterpart for it{why}")
        tree = parse(node.text, "postgres")
        if tree.args.get("with") is not None or tree.args.get("with_"):
            raise Refused(f"line {node.line}: a WITH before {verb.upper()},"
                          " which MySQL does not have")
        self._no_writes_nested(tree, node.line)
        returning = tree.args.get("returning")
        into = None
        if returning is not None:
            into = self._returning(tree, node, returning)
        elif node.targets is not None:
            raise Refused(f"line {node.line}: INTO with no RETURNING")
        counted = self.found or self.rows
        conflict = tree.args.get("conflict")
        if conflict is not None and counted and \
                str(conflict.args.get("action") or "").upper() != \
                "DO NOTHING":
            raise Refused(f"line {node.line} counts the rows of an ON CONFLICT"
                          " DO UPDATE: MySQL counts an updated row twice")
        if isinstance(tree, exp.Delete) and tree.args.get("using"):
            raise Refused(f"line {node.line}: DELETE ... USING, which"
                          " migkit's rules have no counterpart for")
        if isinstance(tree, exp.Insert) and tree.args.get("default"):
            raise Refused(f"line {node.line}: INSERT ... DEFAULT VALUES,"
                          " which MySQL writes otherwise")
        if self.r.kind == "trigger":
            written = tree.this.this if isinstance(tree.this, exp.Schema) \
                else tree.this
            if isinstance(written, exp.Table) and written.name.lower() == \
                    str(self.r.table).split(".")[-1].lower():
                raise Refused(f"line {node.line} writes the table its trigger"
                              " fires on, which MySQL refuses")
        precount = None
        if isinstance(tree, exp.Update) and counted:
            precount = self._precount(tree, node.line)
        self.code(f"postgres.{verb}", node.line)
        text = self._dml(tree, node.line, conflict)
        self.flush(node.line)
        if precount:
            self.e(precount)
        self.e(f"{text};")
        if counted:
            held = self.rows or self.helper("n", "BIGINT", "0")
            if not precount:
                self.e(f"SET {held} = ROW_COUNT();")
            if self.found:
                self.e(f"SET {self.found} = {held} > 0;")
        if into:
            self.e(into)

    def _no_writes_nested(self, tree, line):
        for n in tree.find_all(exp.Insert, exp.Update, exp.Delete):
            if n is not tree:
                raise Refused(f"line {line} writes inside another statement,"
                              " which MySQL does not have")

    def _returning(self, tree, node, returning):
        """`INSERT ... RETURNING key INTO v` of one row, the key the table's
        counter hands out: LAST_INSERT_ID(). Anything else returned is
        refused."""
        if not isinstance(tree, exp.Insert) or node.targets is None:
            raise Refused(f"line {node.line}: RETURNING from"
                          f" {tree.key.upper()}, which MySQL does not have")
        values = tree.expression
        cols = returning.expressions
        if not isinstance(values, exp.Values) or \
                len(values.expressions) != 1 or len(cols) != 1 or \
                len(node.targets) != 1:
            raise Refused(f"line {node.line}: RETURNING from an INSERT of"
                          " other than one row and one key")
        table = tree.this.this if isinstance(tree.this, exp.Schema) else \
            tree.this
        given = [c.name.lower() for c in tree.this.expressions] \
            if isinstance(tree.this, exp.Schema) else None
        leaf = self._target_table(table.name)
        keys = self.ctx.keys.get(leaf) or self.ctx.keys.get(leaf.lower()) \
            or []
        col = cols[0].unalias()
        if not isinstance(col, exp.Column) or given is None or \
                col.name.lower() in given or not keys or \
                [k.lower() for k in keys[0]] != [col.name.lower()]:
            raise Refused(f"line {node.line}: RETURNING {col.sql()} is not"
                          " the table's key left to its counter")
        tree.set("returning", None)
        self.code("postgres.returning-key", node.line)
        name, var = self.target(node.targets[0], node.line)
        return f"SET {name} = LAST_INSERT_ID();"

    def _precount(self, tree, line):
        """PostgreSQL counts the rows an UPDATE found; MySQL's ROW_COUNT()
        the rows it changed (unless the client asked otherwise). So the
        rows are counted, locked, before it."""
        if tree.args.get("from") or tree.args.get("from_") or \
                tree.args.get("joins"):
            raise Refused(f"line {line} counts the rows of an UPDATE ..."
                          " FROM, which migkit's rules have no counterpart"
                          " for")
        held = self.rows or self.helper("n", "BIGINT", "0")
        count = exp.select(exp.Count(this=exp.Star())).from_(tree.this.copy())
        if tree.args.get("where") is not None:
            count.set("where", tree.args["where"].copy())
        count.set("locks", [exp.Lock(update=True)])
        text = self.finish(count, line, query=True)
        self.code("postgres.update-count", line)
        return f"{text} INTO {held};"

    def _dml(self, tree, line, conflict):
        if conflict is None:
            return self.finish(tree, line, query=True)
        return self._on_conflict(tree, conflict, line)

    def _on_conflict(self, tree, conflict, line):
        """ON CONFLICT as ON DUPLICATE KEY UPDATE: DO NOTHING without a
        target on any key, as MySQL's; with a target, or DO UPDATE, only
        where the table has that one unique key."""
        a = conflict.args
        action = str(a.get("action") or "").upper()
        if a.get("constraint") is not None:
            raise Refused(f"line {line}: ON CONFLICT ON CONSTRAINT, which"
                          " migkit's rules have no counterpart for")
        if a.get("where") is not None or a.get("index_predicate") is not \
                None or conflict.find(exp.Where):
            raise Refused(f"line {line}: ON CONFLICT with a WHERE, which"
                          " MySQL does not have")
        schema = tree.this
        if not isinstance(schema, exp.Schema) or not schema.expressions:
            raise Refused(f"line {line}: ON CONFLICT on an INSERT naming no"
                          " columns")
        table = schema.this
        if table.alias:
            raise Refused(f"line {line}: an INSERT target with an alias,"
                          " which MySQL does not have")
        leaf = self._target_table(table.name)
        keys = self.ctx.keys.get(leaf) or self.ctx.keys.get(leaf.lower()) \
            or []
        target = [k.this.name.lower() if isinstance(k, exp.Ordered)
                  else k.name.lower() for k in a.get("conflict_keys") or []]
        if target or action != "DO NOTHING":
            if len(keys) != 1:
                raise Refused(f"line {line}: ON CONFLICT on {leaf}, which has"
                              f" {len(keys)} unique keys here, not one: which"
                              " key a row hits first is MySQL's own choice")
            if target and sorted(target) != sorted(k.lower()
                                                    for k in keys[0]):
                raise Refused(f"line {line}: ON CONFLICT names columns that"
                              f" are not {leaf}'s unique key")
        self.code("postgres.on-conflict", line)
        tree.set("conflict", None)
        values = tree.expression
        sets = []
        if action == "DO NOTHING":
            first = schema.expressions[0].name
            col = self._column_name(first).sql(dialect=_RulesMySQL)
            sets.append(f"{col} = {col}")
        else:
            if not isinstance(values, exp.Values):
                raise Refused(f"line {line}: ON CONFLICT DO UPDATE after an"
                              " INSERT ... SELECT, which migkit's rules have"
                              " no counterpart for")
            upd = exp.Update(this=table.copy(),
                             expressions=[x.copy() for x in
                                          conflict.expressions])
            for col in upd.find_all(exp.Column):
                if (col.table or "").lower() == "excluded":
                    col.set("table", exp.to_identifier("_migkit_new"))
            self.fix(upd, line, True)
            self.names(upd, line)
            text = generate(upd, "postgres", _RulesMySQL)
            sets.append(text[text.index(" SET ") + len(" SET "):])
        body = self.finish(tree, line, query=True)
        alias = " AS _migkit_new" if isinstance(values, exp.Values) else ""
        return f"{body}{alias} ON DUPLICATE KEY UPDATE {', '.join(sets)}"

    def _select_into(self, node):
        """SELECT INTO: the targets NULL first and one row taken, as
        PostgreSQL sets them; STRICT's no row and several rows raised with
        PostgreSQL's codes and messages."""
        tree = parse(node.text, "postgres")
        if not isinstance(tree, (exp.Select, exp.Union)):
            raise Refused(f"line {node.line}: SELECT INTO from a statement"
                          " migkit's rules cannot read")
        self._no_writes(tree, node.line)
        targets = self._fetch_targets(node.targets, node.text, node.line)
        width = len(tree.expressions) if isinstance(tree, exp.Select) else \
            len(tree.find(exp.Select).expressions)
        if any(isinstance(x, exp.Star) or (isinstance(x, exp.Column) and
                                           x.is_star)
               for x in (tree.expressions if isinstance(tree, exp.Select)
                         else [])):
            raise Refused(f"line {node.line} selects * INTO; MySQL needs each"
                          " column named")
        if width != len(targets):
            raise Refused(f"line {node.line} selects {width} columns into"
                          f" {len(targets)} variables")
        flag = None
        if node.strict or self.found or self.rows:
            if not isinstance(tree, exp.Select):
                raise Refused(f"line {node.line}: SELECT INTO STRICT, or"
                              " FOUND after it, over a UNION")
            flag = self.helper("got", "BOOLEAN", "FALSE")
            tree.set("expressions", [exp.true()] + list(tree.expressions))
        local = not (tree.args.get("from_") or tree.args.get("from"))
        if not node.strict:
            limit = tree.args.get("limit")
            if limit is None:
                tree = tree.limit(1)
            elif isinstance(limit.expression, exp.Literal) and \
                    int(limit.expression.this) > 1:
                limit.set("expression", exp.Literal.number(1))
            elif not isinstance(limit.expression, exp.Literal):
                raise Refused(f"line {node.line}: SELECT INTO with a LIMIT"
                              " that is not a number")
        text = self.finish(tree, node.line, query=not local)
        into = ", ".join(([flag] if flag else []) + targets)
        e = self.e
        self.flush(node.line)
        self.code("postgres.select-into-strict" if node.strict
                  else "postgres.select-into", node.line)
        if flag:
            e(f"SET {flag} = FALSE;")
        if node.strict:
            e("BEGIN")
            e.push()
            e("DECLARE EXIT HANDLER FOR 1172 SIGNAL SQLSTATE 'P0003' SET"
              " MESSAGE_TEXT = 'query returned more than one row';")
            e(f"{text} INTO {into};")
            e.pop()
            e("END;")
            e(f"IF NOT {flag} THEN")
            e.push()
            e("SIGNAL SQLSTATE 'P0002' SET MESSAGE_TEXT = 'query returned"
              " no rows';")
            e.pop()
            e("END IF;")
        else:
            e(f"SET {', '.join(f'{t} = NULL' for t in targets)};")
            e(f"{text} INTO {into};")
        if self.found:
            e(f"SET {self.found} = {flag};")
        if self.rows:
            e(f"SET {self.rows} = {flag};")

    def s_call(self, node):
        if node.name is None:
            raise Refused(f"line {node.line}: DO - migkit's rules have no"
                          " counterpart for it")
        callee = self.ctx.routines.get(node.name.lower())
        if callee is None or callee.kind != "procedure":
            raise Refused(f"line {node.line} calls {node.name}, which is not"
                          " one of the source's procedures")
        if self.rows:
            raise Refused(f"line {node.line} calls a procedure in a routine"
                          " that reads ROW_COUNT, whose count after a CALL"
                          " migkit's rules do not keep")
        if self.r.kind in ("function", "trigger") and self._callee_writes(
                node.name):
            self.note("a procedure called by a function or a trigger sets its"
                      " savepoint inside the calling statement")
        args = []
        for k, text in enumerate(node.args):
            if "=>" in text or ":=" in text:
                raise Refused(f"line {node.line} passes an argument by name,"
                              " which MySQL does not have")
            mode = callee.params[k].mode if k < len(callee.params) else "in"
            args.append(self.target(text, node.line)[0] if mode != "in"
                        else self.expr(text, node.line))
        self.flush(node.line)
        self.code("postgres.call", node.line)
        self.e(f"CALL `{callee.name.replace('`', '``')}`({', '.join(args)});")

    def s_getdiag(self, node):
        e = self.e
        sets = []
        for target, item in node.items:
            name, var = self.target(target, node.line)
            item = item.lower()
            if node.stacked:
                if not self.in_handler:
                    raise Refused(f"line {node.line}: GET STACKED DIAGNOSTICS"
                                  " outside a handler")
                scope = self.handler_scope
                got = {"returned_sqlstate": scope["sqlstate"],
                       "message_text": scope["sqlerrm"]}.get(item)
                if got is None:
                    raise Refused(f"line {node.line} reads {item}, which"
                                  " MySQL does not keep")
                got.used = True
                sets.append(f"{name} = {got.my}")
            else:
                if item != "row_count":
                    raise Refused(f"line {node.line} reads {item}, which"
                                  " MySQL does not keep")
                self.code("postgres.row-count", node.line)
                sets.append(f"{name} = {self.rows}")
        e(f"SET {', '.join(sets)};")

    def s_fetch(self, node):
        if node.is_move or node.direction not in ("", "next"):
            what = "MOVE" if node.is_move else \
                f"FETCH {node.direction.upper()}"
            raise Refused(f"line {node.line}: {what} - migkit's rules carry"
                          " only FETCH NEXT")
        var = self.lookup(node.cursor)
        if var is None or not var.cursor:
            raise Refused(f"line {node.line} fetches from {node.cursor}, not"
                          " a cursor declared in the routine")
        e = self.e
        self.code("postgres.fetch", node.line)
        names = [self.target(t, node.line)[0] for t in node.targets]
        got = self.helper("row", "BOOLEAN", "FALSE")
        # a FETCH past the last row sets its targets to NULL in
        # PostgreSQL and leaves them in MySQL; its NOT FOUND held to it
        e(f"SET {got} = TRUE;")
        e("BEGIN")
        e.push()
        e(f"DECLARE CONTINUE HANDLER FOR NOT FOUND SET {got} = FALSE;")
        e(f"FETCH {var.my} INTO {', '.join(names)};")
        e.pop()
        e("END;")
        e(f"IF NOT {got} THEN")
        e.push()
        e(f"SET {', '.join(f'{n} = NULL' for n in names)};")
        e.pop()
        e("END IF;")
        if self.found:
            e(f"SET {self.found} = {got};")
        if self.rows:
            e(f"SET {self.rows} = {got};")

    def s_open(self, node):
        var = self.lookup(node.cursor)
        if node.rest or var is None or not var.cursor:
            return self.s_unsupported(node)
        self.e(f"OPEN {var.my};")

    def s_close(self, node):
        var = self.lookup(node.cursor)
        if var is None or not var.cursor:
            raise Refused(f"line {node.line} closes {node.cursor}, not a"
                          " cursor declared in the routine")
        self.e(f"CLOSE {var.my};")

    def s_commit(self, node):
        self._tx(node, "COMMIT")

    def s_rollback(self, node):
        self._tx(node, "ROLLBACK")

    def _tx(self, node, word):
        if self.r.kind != "procedure":
            raise Refused(f"line {node.line} ends a transaction, which only a"
                          " procedure can")
        if self.in_handler:
            raise Refused(f"line {node.line} ends a transaction inside an"
                          " exception handler")
        self.commits = True
        self.code("postgres.transaction", node.line)
        # PostgreSQL starts the next transaction at once, so what follows is
        # one transaction again, not a statement at a time
        self.e(f"{word}{' AND CHAIN' if node.chain else ''};")
        if not node.chain:
            self.e("START TRANSACTION;")


# ---- helpers ---------------------------------------------------------------

_BOOL_WORDS = {"true": True, "t": True, "yes": True, "y": True, "on": True,
               "1": True, "false": False, "f": False, "no": False,
               "n": False, "off": False, "0": False}


def _key(param):
    return param.name if param.name != param.name.lower() else \
        param.name.lower()


def _unquote(text):
    text = str(text).strip()
    if text.startswith('"') and text.endswith('"'):
        return text[1:-1].replace('""', '"')
    return text.lower()


def _wider(a, b):
    if a is None or b is None:
        return None
    if a not in _NUMERIC or b not in _NUMERIC:
        return None
    for k in ("float", "num", "dec", "int"):
        if k in (a, b):
            return k
    return None


def _common(kinds):
    kinds = {k for k in kinds}
    if None in kinds or not kinds:
        return None
    if len(kinds) == 1:
        return kinds.pop()
    if kinds <= set(_NUMERIC):
        for k in ("float", "num", "dec", "int"):
            if k in kinds:
                return k
    return None


def _constant_text(node):
    if isinstance(node, exp.Literal) and node.is_string:
        return node.this
    return None


def _constant_sql_text(text):
    try:
        tree = parse(f"SELECT {text}", "postgres")
    except Refused:
        return None
    if isinstance(tree, exp.Select) and len(tree.expressions) == 1:
        return _constant_text(tree.expressions[0])
    return None


def _constant(node):
    """True or False where a condition is decided by literals alone (a
    trigger's TG_OP compared with a word), else None."""
    if isinstance(node, exp.Paren):
        return _constant(node.this)
    if isinstance(node, exp.Boolean):
        return bool(node.this)
    if isinstance(node, exp.Not):
        got = _constant(node.this)
        return None if got is None else not got
    if isinstance(node, exp.And):
        a, b = _constant(node.this), _constant(node.expression)
        if a is False or b is False:
            return False
        return True if a and b else None
    if isinstance(node, exp.Or):
        a, b = _constant(node.this), _constant(node.expression)
        if a or b:
            return True
        return False if a is False and b is False else None
    if isinstance(node, (exp.EQ, exp.NEQ)):
        x, y = node.this, node.expression
        if isinstance(x, exp.Literal) and isinstance(y, exp.Literal) and \
                x.is_string and y.is_string:
            return (x.this == y.this) == isinstance(node, exp.EQ)
    if isinstance(node, exp.In) and isinstance(node.this, exp.Literal) and \
            node.this.is_string and node.expressions and all(
                isinstance(v, exp.Literal) and v.is_string
                for v in node.expressions):
        return node.this.this in {v.this for v in node.expressions}
    return None


def _fold(node):
    """A condition with its parts decided by literals alone written as
    TRUE or FALSE, and the AND/OR around them simplified."""
    if isinstance(node, (exp.And, exp.Or)):
        a, b = _fold(node.this.copy()), _fold(node.expression.copy())
        ca, cb = _constant(a), _constant(b)
        is_and = isinstance(node, exp.And)
        for x, cx, other in ((a, ca, b), (b, cb, a)):
            if cx is (not is_and):
                return exp.Boolean(this=not is_and)
            if cx is is_and:
                return other
        return type(node)(this=a, expression=b)
    if isinstance(node, exp.Paren):
        inner = _fold(node.this.copy())
        return inner if isinstance(inner, exp.Boolean) else \
            exp.Paren(this=inner)
    got = _constant(node)
    if got is not None and not isinstance(node, exp.Boolean):
        return exp.Boolean(this=got)
    return node


def _plain_number(text):
    return bool(re.fullmatch(r"\s*-?\d+\s*", text or ""))


def _simple_sql(text):
    return bool(re.fullmatch(r"`?[\w$]+`?|'[^']*'|-?\d+(\.\d+)?", text.strip()))


def _split(text):
    """A SQL text's statements, split at semicolons outside strings."""
    out, start = [], 0
    for t in _lex(text):
        if t.kind == "punct" and t.text == ";":
            out.append(text[start:t.start])
            start = t.end
    out.append(text[start:])
    return out


# ---- the conversion --------------------------------------------------------


def convert(routine, ctx=None):
    """`routine` (a PostgreSQL one) as MySQL statements, each runnable
    alone, or Refused."""
    r = routine
    g = _Gen(r, ctx)
    if r.kind == "view":
        # a view is one query: the rules on it, nothing around it
        tree = parse(r.body or "", "postgres")
        if not isinstance(tree, (exp.Select, exp.Union)):
            raise Refused("its body is not a query")
        g._no_writes(tree, 1)
        text = g.finish(tree, 1, query=True)
        return _made(g, [f"CREATE VIEW `{r.name.replace('`', '``')}` AS"
                         f" {text}"])
    got = g.convert()
    if isinstance(got, list):
        return _made(g, [_header(g, r) + "\n" + "\n".join(got)])
    tree, opts = got
    if r.kind in ("function", "procedure"):
        return _made(g, [_header(g, r) + "\n" + "\n".join(g.body(tree,
                                                                  opts))])
    if r.kind != "trigger":
        raise Refused(f"a {r.kind} is not something migkit's rules convert")
    return _made(g, _trigger(g, r, tree, opts))


def _made(g, statements):
    return Converted(statements, constructs=g.codes, notes=g.notes,
                     commits=g.commits, residue=g.residue)


def _header(g, r):
    name = "`" + r.name.replace("`", "``") + "`"
    security = "SQL SECURITY DEFINER" if _options(r)["definer"] else \
        "SQL SECURITY INVOKER"
    if r.kind == "function":
        params = ", ".join(f"{g.scopes[0][_key(p)].my} {typ}"
                           for p, typ in g.params)
        returns = my_type(r.returns, param=True)
        g.note(returns[2])
        data = str(r.settings.get("data") or "MODIFIES SQL DATA")
        det = "DETERMINISTIC" if data == "NO SQL" else "NOT DETERMINISTIC"
        return (f"CREATE FUNCTION {name}({params}) RETURNS {returns[0]}\n"
                f"  {det} {data} {security}")
    params = ", ".join(f"{p.mode.upper()} {g.scopes[0][_key(p)].my} {typ}"
                       for p, typ in g.params)
    return f"CREATE PROCEDURE {name}({params})\n  MODIFIES SQL DATA {security}"


def _trigger_parts(r):
    """(the WHEN condition or None) of a trigger, from its CREATE TRIGGER,
    or Refused for what a MySQL trigger cannot be."""
    text = str(r.definition or "")
    m = re.search(r"(?is)\bcreate\s+(constraint\s+)?trigger\b.*$", text)
    made = m.group(0) if m else ""
    if m and m.group(1):
        raise Refused("it is a constraint trigger, which MySQL does not"
                      " have")
    if re.search(r"(?is)\bupdate\s+of\b", made):
        raise Refused("it fires on an UPDATE of named columns; a MySQL"
                      " trigger fires on every UPDATE")
    if re.search(r"(?is)\breferencing\b", made):
        raise Refused("it reads transition tables (REFERENCING), which"
                      " MySQL does not have")
    when = re.search(r"(?is)\bwhen\s*\((.*)\)\s*execute\s+(function|"
                     r"procedure)\b", made)
    if when and re.search(r"(?i)\b(new|old)\s*\.\s*\*", when.group(1)):
        raise Refused("its WHEN compares whole rows, which a MySQL trigger"
                      " has no value for")
    return when.group(1).strip() if when else None


def _trigger(g, r, tree, opts):
    """One MySQL trigger per event: MySQL's trigger answers one."""
    if r.level != "row":
        raise Refused("it is a statement-level trigger; MySQL's fire for each"
                      " row")
    if r.timing not in ("BEFORE", "AFTER"):
        raise Refused(f"it is an {r.timing} trigger, which MySQL does not"
                      " have")
    if "TRUNCATE" in r.events:
        raise Refused("it fires on TRUNCATE, which MySQL's triggers do not")
    when = _trigger_parts(r)
    table = exp.to_table(r.table)
    if g.ctx.local is not None:
        table = g.ctx.local(exp.select("*").from_(table)).find(exp.Table)
    made = g.ctx.tables.get(table.name.lower())
    if made:
        table.set("this", exp.to_identifier(made))
    table.set("db", None)
    table.set("catalog", None)
    on = table.sql(dialect=_RulesMySQL)
    out, made = [], []
    g.code("postgres.trigger", 1)
    for event in r.events:
        g2 = _Gen(r, g.ctx)
        g2.event = event
        g2.convert()
        body = g2.body(tree, opts)
        if event != r.events[0] and not [
                x for x in body[1:-1] if not x.strip().startswith(
                    ("DECLARE ", f"{g2.body_label}: BEGIN",
                     f"END {g2.body_label};"))]:
            # nothing for this event: PostgreSQL's function only returns
            g.code(f"postgres.trigger-{event.lower()}-empty", 1)
            continue
        if when:
            text, const = g2.cond(when, 1)
            if const is False:
                continue
            if const is None:
                inner = body[1:-1]
                heads = [x for x in inner if x.startswith("  DECLARE ")]
                rest = [x for x in inner if not x.startswith("  DECLARE ")]
                body = ["BEGIN"] + heads + [f"  IF {text} THEN"] + [
                    "  " + x for x in rest] + ["  END IF;", "END"]
        for code in g2.codes:
            if code not in g.codes:
                g.codes.append(code)
        for note in g2.notes:
            g.note(note)
        g.residue += [x for x in g2.residue if x not in g.residue]
        name = "`" + _trigger_name(r, event).replace("`", "``") + "`"
        out.append(f"CREATE TRIGGER {name} {r.timing} {event} ON {on} FOR"
                   f" EACH ROW\n" + "\n".join(body))
        made.append(f"{_trigger_name(r, event)} for {event}")
    if not out:
        raise Refused("its WHEN is never true on any of its events")
    if len(r.events) > 1:
        g.note(f"trigger {r.name} is made once per event, as MySQL's trigger"
               f" answers one: {', '.join(made)}" + (
                   "" if len(made) == len(r.events) else
                   " (on the other events it only returns)"))
    return out


def _trigger_name(r, event):
    if event == r.events[0]:
        return r.name
    return f"{r.name}_{event.lower()}"[:64]

"""Oracle's PL/SQL in PostgreSQL's PL/pgSQL, by migkit's own rules -
standalone functions, procedures and triggers, and packages (backlog W3).

PL/pgSQL was modelled on PL/SQL, so most of a body carries as it is
written: blocks, IF, CASE, the loops, EXIT and CONTINUE, labels, cursors
declared and opened, `%TYPE` and `%ROWTYPE`. Each SQL statement and
expression goes to the query translator with the rules below on top, and
what has no rule is refused by name. The rules that keep meaning, not only
syntax:

* an exception handler: Oracle rolls back the one statement that failed
  and keeps what the block did before it; a PL/pgSQL exception block rolls
  back the whole block. So a block with handlers runs each of its
  statements as a block of its own which, on a failure, records it and
  leaves; the error is raised again where the handlers - unchanged - are
  the only ones to catch it. Named exceptions keep their names
  (`NO_DATA_FOUND`, `TOO_MANY_ROWS`, `DUP_VAL_ON_INDEX` as
  `unique_violation`, `ZERO_DIVIDE`, `VALUE_ERROR` as the data errors it
  stands for); a declared exception gets a code of its own, and
  `PRAGMA EXCEPTION_INIT` the condition of the Oracle error it names;
  `RAISE_APPLICATION_ERROR` raises with the message it gives;
* `SELECT ... INTO` raises when no row or more than one comes, as `INTO
  STRICT` does; `SQL%ROWCOUNT`, `SQL%FOUND` and a cursor's `%FOUND`,
  `%NOTFOUND`, `%ROWCOUNT` and `%ISOPEN` are kept in variables of their own
  after each statement that sets them (PL/pgSQL's `FOUND` is changed by
  every loop and fetch);
* Oracle holds an empty string as NULL: `''` is NULL, `a || b` is NULL only
  where both are, and a SUBSTR that is empty is NULL; `SUBSTR` with a
  start of 0 starts at 1, and a negative start counts from the end; `/`
  divides exactly (numeric), never as a float; `FOR i IN REVERSE a..b`
  counts down from b; `seq.NEXTVAL` is `nextval('seq')`; `SYSDATE` has no
  fractions of a second;
* where the target runs an emulation of Oracle's functions (orafce), a
  function it holds is called there, by its schema;
* a package is a schema of the same name on the target, each of its
  subprograms a routine in it; its variables - Oracle keeps them for the
  session - are kept in the session's own settings (`set_config`), one per
  variable, read and written through functions of the package's own, and
  set to their first values (and the package's initialisation run) by the
  first that is called in a session. Said where it is used: a setting is
  undone by a ROLLBACK and by an error caught in a block, where Oracle's
  package state is not; a package's cursors and its variables of record
  or collection types are refused;
* `PRAGMA AUTONOMOUS_TRANSACTION`: PostgreSQL has none, so the
  procedure's body is a procedure of its own run over a loopback
  connection of the target to itself (the dblink extension, which the
  conversion makes where it is not), whose commit is its own; the target
  must let its own user in over its local socket. An autonomous function
  is refused (its answer would come back over that connection).

Not carried, each refused by name: dynamic SQL (`EXECUTE IMMEDIATE`),
`GOTO`, collections and records declared by `TYPE`, `BULK COLLECT` and
`FORALL`, local subprograms, `SQLCODE`, savepoints, `PIPE ROW`, compound
and INSTEAD OF triggers, a trigger asking `UPDATING('col')`.
"""
import hashlib
import re

from sqlglot import exp

from .plpgsql import QUOTE, Converted, Emitter, ident
from .translate import Refused, generate, parse

# ---- the text, as tokens -------------------------------------------------

_LEX = re.compile(r"""
    (?P<ws>\s+)
  | (?P<comment>--[^\n]*|/\*.*?\*/)
  | (?P<qstring>[nN]?[qQ]')
  | (?P<string>[nN]?'(?:[^']|'')*')
  | (?P<qident>"(?:[^"]|"")*")
  | (?P<number>(?:\d+\.\d*|\.\d+|\d+)(?:[eE][+-]?\d+)?(?![.]))
  | (?P<int>\d+)
  | (?P<bind>:[A-Za-z_]\w*)
  | (?P<word>[A-Za-z_][A-Za-z0-9_$#]*)
  | (?P<op>:=|=>|\.\.|\|\||<=|>=|<>|!=|~=|\^=|\*\*|<<|>>|[-+*/<>=%(),;.@:])
""", re.S | re.X)


class Tok:
    __slots__ = ("kind", "text", "start", "end", "line")

    def __init__(self, kind, text, start, end, line):
        self.kind, self.text, self.start, self.end, self.line = \
            kind, text, start, end, line

    @property
    def word(self):
        return self.text.lower() if self.kind == "word" else None


def _lex(text):
    out, at, line = [], 0, 1
    while at < len(text):
        m = _LEX.match(text, at)
        if not m:
            raise Refused(f"line {line}: {text[at:at + 20]!r} could not be"
                          " read")
        kind = m.lastgroup
        if kind == "qstring":
            raise Refused(f"line {line} writes a string in Oracle's q'...'"
                          " quoting, which migkit's rules do not read")
        if kind not in ("ws", "comment"):
            out.append(Tok("number" if kind == "int" else kind, m.group(0),
                           m.start(), m.end(), line))
        line += m.group(0).count("\n")
        at = m.end()
    return out


# ---- the body, as statements ----------------------------------------------


class Node:
    def __init__(self, kind, line, **a):
        self.kind, self.line, self.a = kind, line, a

    def __getattr__(self, name):
        try:
            return self.__dict__["a"][name]
        except KeyError:
            raise AttributeError(name) from None

    def children(self):
        out = []
        for k in ("body", "otherwise"):
            v = self.a.get(k)
            if isinstance(v, list):
                out += v
        for _, stmts in self.a.get("branches") or []:
            out += stmts
        for _, stmts in self.a.get("handlers") or []:
            out += stmts
        return out

    def walk(self):
        yield self
        for c in self.children():
            yield from c.walk()


_ENDS = {"end", "else", "elsif", "when", "exception"}
#: Oracle's named exceptions, as the condition PL/pgSQL catches the same
#: failure by
NAMED = {"no_data_found": ["no_data_found"],
         "too_many_rows": ["too_many_rows"],
         "dup_val_on_index": ["unique_violation"],
         "zero_divide": ["division_by_zero"],
         "invalid_number": ["invalid_text_representation"],
         "value_error": ["invalid_text_representation",
                         "numeric_value_out_of_range",
                         "string_data_right_truncation", "null_value_not_allowed"],
         "case_not_found": ["case_not_found"],
         "cursor_already_open": ["duplicate_cursor"],
         "invalid_cursor": ["invalid_cursor_state"],
         "others": ["others"]}
#: an Oracle error number (`PRAGMA EXCEPTION_INIT`) as the condition the
#: target raises for the same failure
ORA_ERRORS = {-1: "unique_violation", -1400: "not_null_violation",
              -1407: "not_null_violation", -2291: "foreign_key_violation",
              -2292: "foreign_key_violation", -2290: "check_violation",
              -1476: "division_by_zero", -1722: "invalid_text_representation",
              -1403: "no_data_found", 100: "no_data_found",
              -1422: "too_many_rows", -12899: "string_data_right_truncation",
              -1438: "numeric_value_out_of_range", -60: "deadlock_detected",
              -54: "lock_not_available"}


class _Reader:
    """A PL/SQL subprogram's text read into its header, declarations and
    statements."""

    def __init__(self, text):
        self.text = text
        self.t = _lex(text)
        self.i = 0

    def tok(self, k=0):
        j = self.i + k
        return self.t[j] if 0 <= j < len(self.t) else None

    def w(self, k=0):
        t = self.tok(k)
        return t.word if t is not None else None

    def is_(self, text, k=0):
        t = self.tok(k)
        return t is not None and t.kind in ("op", "word") and \
            t.text.lower() == text

    def at_end(self):
        return self.i >= len(self.t)

    def line(self):
        t = self.tok()
        return t.line if t is not None else 0

    def eat(self, *words):
        for k, word in enumerate(words):
            if not self.is_(word, k):
                return False
        self.i += len(words)
        return True

    def need(self, *words):
        if not self.eat(*words):
            got = self.tok().text if self.tok() else "the end"
            raise Refused(f"its body could not be read: expected"
                          f" {' '.join(words).upper()} at line {self.line()},"
                          f" found {got}")

    def name(self):
        t = self.tok()
        if t is None or t.kind not in ("word", "qident"):
            raise Refused(f"its body could not be read: a name was expected"
                          f" at line {self.line()}")
        self.i += 1
        return t.text.strip('"') if t.kind == "qident" else t.text

    def dotted(self):
        parts = [self.name()]
        while self.is_(".") and self.tok(1) is not None and \
                self.tok(1).kind in ("word", "qident"):
            self.i += 1
            parts.append(self.name())
        return ".".join(parts)

    def src(self, a, b):
        if a >= b:
            return ""
        return self.text[self.t[a].start:self.t[b - 1].end]

    def until(self, stop, semicolon=True):
        """Tokens up to the first at depth 0 where `stop(tok)` holds or (with
        `semicolon`) a semicolon; (start index, end index)."""
        start, depth = self.i, 0
        while not self.at_end():
            t = self.tok()
            if t.kind == "op" and t.text == "(":
                depth += 1
            elif t.kind == "op" and t.text == ")":
                if depth == 0:
                    break
                depth -= 1
            elif depth == 0 and ((semicolon and t.text == ";") or stop(t)):
                break
            self.i += 1
        return start, self.i

    def semicolon(self):
        if not self.eat(";"):
            raise Refused(f"its body could not be read: a statement does"
                          f" not end at line {self.line()}")

    # ---- a subprogram -------------------------------------------------

    def subprogram(self):
        """{kind, name, params, returns, decls, block, autonomous} of
        `FUNCTION|PROCEDURE name [(params)] [RETURN type] [options]
        IS|AS decls BEGIN ... END [name];`."""
        line = self.line()
        kind = self.w()
        if kind not in ("function", "procedure"):
            raise Refused("its body could not be read: it does not begin"
                          " FUNCTION or PROCEDURE")
        self.i += 1
        name = self.dotted()
        params = []
        if self.eat("("):
            while True:
                params.append(self.param())
                if self.eat(")"):
                    break
                self.need(",")
        returns = None
        if kind == "function":
            self.need("return")
            a, b = self.until(lambda t: t.word in (
                "is", "as", "deterministic", "pipelined", "parallel_enable",
                "result_cache", "authid", "aggregate"))
            returns = self.src(a, b)
        while self.w() in ("deterministic", "pipelined", "parallel_enable",
                           "result_cache", "authid", "accessible"):
            if self.w() == "pipelined":
                raise Refused(f"line {self.line()}: a pipelined function"
                              " has no counterpart in migkit's rules")
            self.i += 1
            if self.w() in ("current_user", "definer"):
                self.i += 1
            if self.is_("("):
                self.until(lambda t: False, semicolon=False)
                self.eat(")")
        if self.w() not in ("is", "as"):
            raise Refused(f"line {self.line()}: IS or AS was expected after"
                          " the header")
        self.i += 1
        decls = self.declarations()
        block = self.block_rest(None, line)
        return {"kind": kind, "name": name, "params": params,
                "returns": returns, "decls": decls, "block": block,
                "line": line}

    def param(self):
        name = self.name()
        mode = "in"
        if self.eat("in", "out"):
            mode = "inout"
        elif self.eat("out"):
            mode = "out"
        else:
            self.eat("in")
        self.eat("nocopy")
        a, b = self.until(lambda t: t.text in (",", ":=") or
                          t.word == "default", semicolon=False)
        typ = self.src(a, b)
        default = None
        if self.eat(":=") or self.eat("default"):
            a, b = self.until(lambda t: t.text == ",", semicolon=False)
            default = self.src(a, b)
        return {"name": name, "mode": mode, "type": typ, "default": default}

    # ---- declarations ---------------------------------------------------

    def declarations(self, until=("begin",)):
        """[declaration] up to `until` (BEGIN, or the END of a package
        spec)."""
        out = []
        while not self.at_end() and self.w() not in until:
            line = self.line()
            w = self.w()
            if w == "pragma":
                self.i += 1
                what = self.w()
                self.i += 1
                if what == "autonomous_transaction":
                    out.append({"what": "autonomous", "line": line})
                elif what == "exception_init":
                    self.need("(")
                    name = self.name()
                    self.need(",")
                    neg = self.eat("-")
                    num = int(self.tok().text)
                    self.i += 1
                    self.need(")")
                    out.append({"what": "exception_init", "name": name,
                                "code": -num if neg else num,
                                "line": line})
                else:
                    a, b = self.until(lambda t: False)
                self.semicolon()
                continue
            if w == "cursor":
                self.i += 1
                name = self.name()
                args = None
                if self.eat("("):
                    a, b = self.until(lambda t: False, semicolon=False)
                    args = self.src(a, b)
                    self.need(")")
                if self.eat("return"):
                    self.until(lambda t: t.word == "is")
                self.need("is")
                a, b = self.until(lambda t: False)
                out.append({"what": "cursor", "name": name, "args": args,
                            "query": self.src(a, b), "line": line})
                self.semicolon()
                continue
            if w in ("type", "subtype"):
                raise Refused(f"line {line} declares a {w} of its own (a"
                              " record or a collection), which migkit's"
                              " rules do not carry")
            if w in ("procedure", "function"):
                if until == ("end",):
                    # a package spec's subprogram: its header alone
                    a, b = self.until(lambda t: False)
                    out.append({"what": "spec", "text": self.src(a, b),
                                "line": line})
                    self.semicolon()
                    continue
                raise Refused(f"line {line} declares a {w} inside the"
                              " routine, which migkit's rules do not carry")
            name = self.name()
            if self.eat("exception"):
                out.append({"what": "exception", "name": name, "line": line})
                self.semicolon()
                continue
            constant = self.eat("constant")
            a, b = self.until(lambda t: t.text == ":=" or t.word in (
                "default", "not"))
            typ = self.src(a, b)
            notnull = self.eat("not", "null")
            default = None
            if self.eat(":=") or self.eat("default"):
                a, b = self.until(lambda t: False)
                default = self.src(a, b)
            self.semicolon()
            out.append({"what": "var", "name": name, "type": typ,
                        "constant": constant, "notnull": notnull,
                        "default": default, "line": line})
        return out

    # ---- blocks and statements --------------------------------------------

    def block_rest(self, label, line):
        """BEGIN stmts [EXCEPTION handlers] END [name];"""
        self.need("begin")
        body = self.statements()
        handlers = []
        if self.eat("exception"):
            while self.eat("when"):
                names = [self.dotted().lower()]
                while self.eat("or"):
                    names.append(self.dotted().lower())
                self.need("then")
                handlers.append((names, self.statements()))
        self.need("end")
        if self.tok() is not None and self.tok().kind in ("word", "qident") \
                and self.w() not in ("if", "loop", "case"):
            self.i += 1
        self.eat(";")
        return Node("block", line, label=label, decls=[], body=body,
                    handlers=handlers)

    def statements(self):
        out = []
        while not self.at_end() and self.w() not in _ENDS:
            out.append(self.statement())
        return out

    def statement(self):
        line = self.line()
        label = None
        if self.is_("<<"):
            self.i += 1
            label = self.name()
            self.need(">>")
        w = self.w()
        if w == "declare":
            self.i += 1
            decls = self.declarations()
            node = self.block_rest(label, line)
            node.a["decls"] = decls
            return node
        if w == "begin":
            return self.block_rest(label, line)
        if w == "if":
            self.i += 1
            branches = []
            a, b = self.until(lambda t: t.word == "then", semicolon=False)
            self.need("then")
            branches.append((self.src(a, b), self.statements()))
            otherwise = None
            while True:
                if self.eat("elsif"):
                    a, b = self.until(lambda t: t.word == "then",
                                      semicolon=False)
                    self.need("then")
                    branches.append((self.src(a, b), self.statements()))
                elif self.eat("else"):
                    otherwise = self.statements()
                else:
                    break
            self.need("end", "if")
            self.semicolon()
            return Node("if", line, branches=branches, otherwise=otherwise)
        if w == "case":
            self.i += 1
            selector = None
            if self.w() != "when":
                a, b = self.until(lambda t: t.word == "when",
                                  semicolon=False)
                selector = self.src(a, b)
            branches, otherwise = [], None
            while self.eat("when"):
                a, b = self.until(lambda t: t.word == "then",
                                  semicolon=False)
                self.need("then")
                branches.append((self.src(a, b), self.statements()))
            if self.eat("else"):
                otherwise = self.statements()
            self.need("end", "case")
            if self.tok() is not None and self.tok().kind == "word":
                self.i += 1
            self.semicolon()
            return Node("case", line, selector=selector, branches=branches,
                        otherwise=otherwise)
        if w in ("loop", "while", "for"):
            return self.loop(label, line)
        if w in ("exit", "continue"):
            self.i += 1
            target = None
            if self.tok() is not None and self.tok().kind == "word" and \
                    self.w() != "when":
                target = self.name()
            cond = None
            if self.eat("when"):
                a, b = self.until(lambda t: False)
                cond = self.src(a, b)
            self.semicolon()
            return Node(w, line, target=target, cond=cond)
        if w == "return":
            self.i += 1
            a, b = self.until(lambda t: False)
            self.semicolon()
            return Node("return", line, value=self.src(a, b) or None)
        if w == "null":
            self.i += 1
            self.semicolon()
            return Node("null", line)
        if w == "raise":
            self.i += 1
            name = None
            if not self.is_(";"):
                name = self.dotted().lower()
            self.semicolon()
            return Node("raise", line, name=name)
        if w in ("select", "with"):
            a, b = self.until(lambda t: False)
            self.semicolon()
            return Node("select_into", line, text=self.src(a, b))
        if w in ("insert", "update", "delete", "merge"):
            a, b = self.until(lambda t: False)
            self.semicolon()
            return Node("dml", line, text=self.src(a, b), verb=w)
        if w in ("open", "close"):
            self.i += 1
            name = self.name()
            args = None
            if w == "open" and self.eat("("):
                a, b = self.until(lambda t: False, semicolon=False)
                args = self.src(a, b)
                self.need(")")
            if w == "open" and self.w() == "for":
                raise Refused(f"line {line} opens a cursor variable for a"
                              " query of its own (OPEN ... FOR)")
            self.semicolon()
            return Node(w, line, cursor=name, args=args)
        if w == "fetch":
            self.i += 1
            name = self.name()
            if self.w() == "bulk":
                raise Refused(f"line {line} fetches in bulk (BULK COLLECT),"
                              " which migkit's rules do not carry")
            self.need("into")
            targets = [self.dotted()]
            while self.eat(","):
                targets.append(self.dotted())
            self.semicolon()
            return Node("fetch", line, cursor=name, targets=targets)
        if w in ("commit", "rollback"):
            self.i += 1
            if self.w() == "to":
                raise Refused(f"line {line} rolls back to a savepoint")
            self.eat("work")
            self.semicolon()
            return Node("tx", line, what=w)
        if w in ("savepoint", "goto", "forall", "pipe"):
            raise Refused(f"line {line}: {w.upper()} has no counterpart in"
                          " migkit's rules")
        if w == "execute" and self.w(1) == "immediate":
            raise Refused(f"line {line} runs SQL built at run time (EXECUTE"
                          " IMMEDIATE)")
        # an assignment or a call
        a, b = self.until(lambda t: t.text == ":=")
        if self.eat(":="):
            target = self.src(a, b)
            c, d = self.until(lambda t: False)
            self.semicolon()
            return Node("assign", line, target=target, value=self.src(c, d))
        self.semicolon()
        return Node("call", line, text=self.src(a, b))

    def loop(self, label, line):
        w = self.w()
        if w != "loop":
            self.i += 1
        kind, head = "loop", {}
        if w == "while":
            a, b = self.until(lambda t: t.word == "loop", semicolon=False)
            kind, head = "while", {"cond": self.src(a, b)}
        elif w == "for":
            var = self.name()
            self.need("in")
            reverse = self.eat("reverse")
            if self.is_("("):
                self.i += 1
                a, b = self.until(lambda t: False, semicolon=False)
                query = self.src(a, b)
                self.need(")")
                kind, head = "for_query", {"var": var, "query": query}
            else:
                a, b = self.until(lambda t: t.text == ".." or
                                  t.word == "loop", semicolon=False)
                first = self.src(a, b)
                if self.eat(".."):
                    c, d = self.until(lambda t: t.word == "loop",
                                      semicolon=False)
                    kind, head = "for_range", {
                        "var": var, "low": first, "high": self.src(c, d),
                        "reverse": reverse}
                else:
                    m = re.fullmatch(r"(?s)\s*(\w+)\s*(\((.*)\))?\s*", first)
                    if not m:
                        raise Refused(f"line {line}: a FOR loop over"
                                      f" {first[:40]} could not be read")
                    kind, head = "for_cursor", {
                        "var": var, "cursor": m.group(1),
                        "args": m.group(3)}
            if w == "for" and kind != "for_range" and reverse:
                raise Refused(f"line {line}: REVERSE over a query")
        self.need("loop")
        body = self.statements()
        self.need("end", "loop")
        if self.tok() is not None and self.tok().kind == "word":
            self.i += 1
        self.semicolon()
        return Node(kind, line, label=label, body=body, **head)


def read(text):
    """The subprogram written in `text` (`FUNCTION ...` or `PROCEDURE
    ...`), as `_Reader.subprogram` gives it."""
    r = _Reader(text)
    got = r.subprogram()
    return got


# ---- types ------------------------------------------------------------------


def pg_type(declared, ctx=None):
    """An Oracle type as PostgreSQL's; `%TYPE` and `%ROWTYPE` kept, their
    table named as the target names it."""
    text = " ".join(str(declared).split())
    low = text.lower()
    m = re.fullmatch(r"([\w$#.\"]+)\s*%\s*(rowtype|type)", low)
    if m:
        name = m.group(1)
        parts = name.split(".")
        if ctx is not None and getattr(ctx, "tables", None):
            parts[0] = ctx.tables.get(parts[0].strip('"'), parts[0])
        return ".".join(parts) + "%" + m.group(2)
    m = re.match(r"(\w+(?:\s+\w+)*?)\s*(\(([^)]*)\))?\s*(byte|char)?\s*$",
                 low)
    if not m:
        raise Refused(f"its type {declared} could not be read")
    base, args = m.group(1), (m.group(3) or "").replace(" ", "")
    args = re.sub(r"(?i)(byte|char)$", "", args)
    if base in ("number", "numeric", "decimal", "dec"):
        if not args or args == "*":
            return "numeric"
        return f"numeric({args})"
    if base in ("integer", "int", "smallint"):
        return "numeric(38)"
    if base in ("pls_integer", "binary_integer", "simple_integer",
                "natural", "positive", "naturaln", "positiven", "signtype"):
        return "integer"
    if base in ("float", "binary_double", "double precision", "real"):
        return "double precision"
    if base == "binary_float":
        return "real"
    if base in ("varchar2", "varchar", "nvarchar2", "string",
                "character varying"):
        return f"varchar({args})" if args else "varchar"
    if base in ("char", "nchar", "character"):
        return f"char({args or 1})"
    if base in ("clob", "nclob", "long"):
        return "text"
    if base in ("blob", "raw", "long raw", "bfile"):
        return "bytea"
    if base == "date":
        return "timestamp(0)"
    if base == "timestamp":
        return f"timestamp({args})" if args else "timestamp(6)"
    if base in ("timestamp with time zone", "timestamp with local time"
                " zone"):
        return "timestamptz"
    if base == "boolean":
        return "boolean"
    if base == "sys_refcursor":
        return "refcursor"
    raise Refused(f"its type {declared} has no counterpart in migkit's"
                  " rules")


def user_code(scope, name):
    """The SQLSTATE a declared exception is raised with on the target:
    class UE, then three characters from its name - steady across runs,
    so a handler in another routine catches it."""
    digest = hashlib.sha256(f"{scope}.{name}".lower().encode()).digest()
    chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return "UE" + "".join(chars[b % 36] for b in digest[:3])


# ---- package state ----------------------------------------------------------


class Package:
    """What one package's routines share on the target: its schema, its
    variables kept in the session's settings, its exceptions, its
    initialisation."""

    def __init__(self, name):
        self.name = name.lower()
        self.schema = ident(self.name)
        self.vars = {}               # lower name -> (pg type, default, const)
        self.exceptions = {}         # lower name -> SQLSTATE or condition
        self.init = None             # the initialisation block, read
        self.members = {}            # lower name -> kind

    def setting(self, var):
        return f"migkit_{self.name}.{var.lower()}"

    def getter(self, var):
        return f"{self.schema}.{ident('_migkit_get_' + var.lower())}()"

    def setter(self, var, value):
        return (f"perform {self.schema}."
                f"{ident('_migkit_set_' + var.lower())}({value});")

    def text_of(self, typ, value):
        if typ.startswith("timestamp"):
            return f"to_char({value}, 'YYYY-MM-DD HH24:MI:SS.US')"
        return f"({value})::text"

    def value_of(self, typ, text):
        if typ.startswith("timestamptz"):
            return f"({text})::timestamptz"
        if typ.startswith("timestamp"):
            return (f"cast(to_timestamp({text}, 'YYYY-MM-DD HH24:MI:SS.US')"
                    f" as {typ})")
        return f"cast({text} as {typ})"


# ---- the generator ----------------------------------------------------------


class _Gen:
    def __init__(self, routine, ctx, unit, package=None, trigger=None):
        self.r = routine
        self.ctx = ctx
        self.unit = unit
        self.package = package
        self.trigger = trigger
        self.e = Emitter()
        self.codes = []
        self.serial = 0
        self.decls = []
        #: lower name -> (pg name, pg type) of variables and parameters
        self.vars = {}
        self.cursors = {}            # lower name -> pg name
        self.exceptions = {}         # lower name -> SQLSTATE or condition
        #: the blocks with handlers around, innermost last: (label, err,
        #: state, msg)
        self.frames = []
        self.records = set()         # loop records PL/pgSQL must declare
        self.uses_sql_attrs = False
        self.commits = False

    def code(self, what, line):
        if (what, line) not in self.codes:
            self.codes.append((what, line))

    def fresh(self, stem):
        self.serial += 1
        return f"_migkit_{stem}{self.serial}"

    # ---- expressions -------------------------------------------------------

    def _attrs(self, text, line):
        """Cursor and SQL attributes as the variables that keep them."""
        def attr(m):
            owner, what = m.group(1).lower(), m.group(2).lower()
            if owner == "sql":
                self.uses_sql_attrs = True
                return {"rowcount": "_migkit_sql_rowcount",
                        "found": "(_migkit_sql_rowcount > 0)",
                        "notfound": "(_migkit_sql_rowcount = 0)",
                        "isopen": "false"}.get(what) or self._no(
                    f"SQL%{what.upper()}", line)
            c = self.cursors.get(owner)
            if c is None:
                raise Refused(f"line {line} reads {m.group(0)}, of a cursor"
                              " not declared before it")
            return {"found": f"{c}_found", "notfound": f"(not {c}_found)",
                    "rowcount": f"{c}_rows", "isopen": f"{c}_open"}.get(
                what) or self._no(m.group(0), line)
        return re.sub(r"(?i)\b(\w+)\s*%\s*(rowcount|found|notfound|isopen|"
                      r"bulk_rowcount|bulk_exceptions)\b", attr, text)

    def _no(self, what, line):
        raise Refused(f"line {line} reads {what}, which migkit's rules have"
                      " no counterpart for")

    def _pre(self, text, line):
        """What the query translator cannot read, rewritten first: the
        attributes, a trigger's `:new` and `:old`, and Oracle's text
        conversions without a format."""
        text = self._attrs(text, line)
        if re.search(r"(?i)\bsqlcode\b", text):
            raise Refused(f"line {line} reads SQLCODE, Oracle's error"
                          " number, which the target does not keep")
        if re.search(r"(?i)\brownum\b", text):
            raise Refused(f"line {line} reads ROWNUM")
        text = re.sub(r"(?i):(new|old)\b", r"\1", text)
        text = re.sub(r"(?i)\bto_number\s*\(\s*([^,()]+?)\s*\)",
                      r"cast(\1 as numeric)", text)
        return text

    def rewrite(self, tree, line, statement=False):
        """The rules on a tree read as Oracle's SQL; in a `statement` a bare
        name is a column first, as Oracle resolves it there."""
        if self.ctx is not None and getattr(self.ctx, "local", None):
            tree = self.ctx.local(tree)
        emulated = set(getattr(self.ctx, "emulated", None) or ())
        for node in reversed(list(tree.walk())):
            if isinstance(node, exp.Literal) and node.is_string and \
                    node.this == "":
                # Oracle's empty string is NULL
                node.replace(exp.Null())
            elif isinstance(node, exp.Column) and node.table and \
                    node.name.lower() in ("nextval", "currval") and \
                    not node.args.get("db"):
                node.replace(exp.Anonymous(this=node.name.lower(),
                                           expressions=[exp.Literal.string(
                                               node.table)]))
            elif isinstance(node, exp.Column) and not node.table and \
                    self.package is not None and \
                    node.name.lower() in self.package.vars and \
                    node.name.lower() not in self.vars:
                node.replace(exp.maybe_parse(self.package.getter(node.name)))
            elif isinstance(node, exp.Column) and not node.table and \
                    not statement and node.name.lower() not in self.vars \
                    and self._function(node.name) is not None:
                # PL/SQL calls a function of no arguments without
                # parentheses; PL/pgSQL reads that as a column
                node.replace(exp.maybe_parse(
                    self._function(node.name) + "()"))
            elif isinstance(node, exp.Anonymous) and \
                    self._function(node.name) is not None and \
                    not isinstance(node.parent, exp.Dot):
                node.replace(exp.maybe_parse(
                    self._function(node.name) + "(" + ", ".join(
                        a.sql("postgres") for a in node.expressions) + ")"))
            elif isinstance(node, exp.Column) and node.table and \
                    self.package is not None and \
                    str(node.table).lower() == self.package.name and \
                    node.name.lower() in self.package.vars:
                node.replace(exp.maybe_parse(self.package.getter(node.name)))
        for node in [n for n in tree.walk() if isinstance(n, exp.DPipe)
                     and not isinstance(n.parent, exp.DPipe)]:
            parts, stack = [], [node]
            while stack:
                n = stack.pop()
                if isinstance(n, exp.DPipe):
                    stack += [n.expression, n.this]
                else:
                    parts.append(n.copy())
            # NULL only where every piece is: Oracle's || skips a NULL
            node.replace(exp.Nullif(this=exp.Anonymous(
                this="concat", expressions=parts),
                expression=exp.Literal.string("")))
        for node in reversed(list(tree.walk())):
            if isinstance(node, exp.Div):
                node.set("this", exp.cast(node.this.copy(),
                                          exp.DataType.build("numeric")))
            elif isinstance(node, exp.Substring) and "substr" not in \
                    emulated:
                node.replace(self._substr(node))
            elif emulated and isinstance(node, exp.Func):
                name = (node.name if isinstance(node, exp.Anonymous)
                        else node.sql("oracle").split("(")[0]).lower()
                if name in emulated:
                    args = node.expressions if isinstance(
                        node, exp.Anonymous) else [
                        v for k in node.arg_types
                        for v in (node.args.get(k) if isinstance(
                            node.args.get(k), list) else [node.args.get(k)])
                        if isinstance(v, exp.Expression)]
                    node.replace(exp.Dot(
                        this=exp.to_identifier("oracle"),
                        expression=exp.Anonymous(this=name, expressions=[
                            a.copy() for a in args])))
        return tree

    def _function(self, name):
        """The target's name of a function of the package's or the
        source's own, called by `name` alone; None for any other."""
        low = str(name).lower()
        if self.package is not None and \
                self.package.members.get(low) == "function":
            return f"{self.package.schema}.{ident(low)}"
        routines = getattr(self.ctx, "routines", None) or {}
        got = routines.get(low)
        if got is not None and got.kind == "function" and \
                not got.settings.get("package"):
            return ident(got.name)
        return None

    def _substr(self, node):
        """Oracle's SUBSTR: a start of 0 is 1, a negative start counts from
        the end, and an empty answer is NULL."""
        s = node.this
        start = node.args.get("start") or exp.Literal.number(1)
        length = node.args.get("length")
        fixed = _whole(start)
        if fixed is not None and fixed >= 0:
            call = exp.Anonymous(this="substr", expressions=[
                s.copy(), exp.Literal.number(max(fixed, 1))] + (
                [length.copy()] if length is not None else []))
            return exp.Nullif(this=call, expression=exp.Literal.string(""))
        st = exp.maybe_parse(
            f"case when ({start.sql('postgres')}) = 0 then 1 when"
            f" ({start.sql('postgres')}) < 0 then length({s.sql('postgres')})"
            f" + ({start.sql('postgres')}) + 1 else ({start.sql('postgres')})"
            " end", dialect="postgres")
        call = exp.Anonymous(this="substr", expressions=[s.copy(), st] + (
            [length.copy()] if length is not None else []))
        guard = exp.Nullif(this=call, expression=exp.Literal.string(""))
        if isinstance(start, exp.Neg) or (
                isinstance(start, exp.Literal) and str(start.this).startswith(
                    "-")):
            # beyond the text's start counting back: NULL, as Oracle
            return exp.maybe_parse(
                f"case when length({s.sql('postgres')}) +"
                f" ({start.sql('postgres')}) < 0 then null else"
                f" {guard.sql('postgres')} end", dialect="postgres")
        return guard

    def expr(self, text, line):
        # SYSDATE has no fractions of a second; the translator writes it
        # as a timestamp that has them
        text = re.sub(r"(?i)\bsysdate\b", "migkit_sysdate",
                      self._pre(text, line))
        tree = parse(f"SELECT {text}", "oracle")
        if not isinstance(tree, exp.Select) or len(tree.expressions) != 1 \
                or tree.args.get("from_") or tree.args.get("from"):
            raise Refused(f"the expression at line {line} could not be read")
        tree = self.rewrite(tree, line)
        out = generate(tree, "oracle", "postgres", normalize=False)
        out = out[len("SELECT "):]
        return re.sub(r"(?i)\bmigkit_sysdate\b", "localtimestamp(0)", out)

    def sql(self, text, line):
        text = self._pre(text, line)
        text = re.sub(r"(?i)\bsysdate\b", "migkit_sysdate", text)
        tree = parse(text, "oracle")
        tree = self.rewrite(tree, line, statement=True)
        out = generate(tree, "oracle", "postgres", normalize=False)
        return re.sub(r"(?i)\bmigkit_sysdate\b", "localtimestamp(0)", out)

    # ---- declarations ---------------------------------------------------

    def declare(self, decls, scope):
        """The declarations of one block: [lines of PL/pgSQL's DECLARE]."""
        out = []
        for d in decls:
            what = d["what"]
            if what == "var":
                typ = pg_type(d["type"], self.ctx)
                pg = ident(d["name"])
                self.vars[d["name"].lower()] = (pg, typ)
                line = f"{pg} {'constant ' if d['constant'] else ''}{typ}"
                if d["notnull"]:
                    line += " not null"
                if d["default"] is not None:
                    value = self.expr(d["default"], d["line"])
                    line += f" := {self._assigned(typ, value)}"
                out.append(line + ";")
            elif what == "cursor":
                self.code("plsql.cursor", d["line"])
                pg = ident(d["name"])
                self.cursors[d["name"].lower()] = pg
                args = ""
                if d["args"]:
                    got = _Reader(d["args"])
                    listed = []
                    while not got.at_end():
                        one = got.param()
                        listed.append(f"{ident(one['name'])}"
                                      f" {pg_type(one['type'], self.ctx)}")
                        self.vars[one["name"].lower()] = (
                            ident(one["name"]), pg_type(one["type"],
                                                        self.ctx))
                        got.eat(",")
                    args = "(" + ", ".join(listed) + ")"
                out.append(f"{pg} cursor{args} for"
                           f" {self.sql(d['query'], d['line'])};")
                out += [f"{pg}_found boolean;", f"{pg}_rows integer := 0;",
                        f"{pg}_open boolean := false;"]
            elif what == "exception":
                self.exceptions[d["name"].lower()] = user_code(
                    scope, d["name"])
            elif what == "exception_init":
                self.exceptions[d["name"].lower()] = _initialised(
                    d["code"], d["line"])
            elif what == "autonomous":
                pass
        return out

    def _assigned(self, typ, value):
        """A text value as Oracle stores it: empty is NULL."""
        if typ.startswith(("varchar", "char", "text")):
            return f"nullif({value}, '')"
        return value

    # ---- statements -----------------------------------------------------------

    def statements(self, stmts):
        for s in stmts:
            getattr(self, "s_" + s.kind)(s)

    def guarded(self, lines):
        """`lines` under the innermost block with handlers: a block of their
        own which, on a failure, records it and leaves that block - so the
        statements before keep what they did, as Oracle keeps it."""
        e = self.e
        if not self.frames:
            for ln in lines:
                e(ln)
            return
        label, err, state, msg = self.frames[-1]
        e("begin")
        e.push()
        for ln in lines:
            e(ln)
        e.pop()
        e("exception when others then")
        e.push()
        e(f"get stacked diagnostics {state} = returned_sqlstate,"
          f" {msg} = message_text;")
        e(f"{err} := true;")
        e(f"exit {label};")
        e.pop()
        e("end;")

    def s_block(self, node):
        e = self.e
        decls = self.declare(node.decls, self.unit)
        if not node.handlers:
            if decls:
                e("declare")
                e.push()
                for d in decls:
                    e(d)
                e.pop()
                e("begin")
                e.push()
                self.statements(node.body)
                e.pop()
                e("end;")
            else:
                self.statements(node.body)
            return
        self.code("plsql.exception-block", node.line)
        label = self.fresh("blk")
        err, state, msg = self.fresh("err"), self.fresh("state"), \
            self.fresh("msg")
        self.decls += [f"{err} boolean := false;", f"{state} text;",
                       f"{msg} text;"]
        e(f"{err} := false;")
        e(f"<<{label}>>")
        if decls:
            e("declare")
            e.push()
            for d in decls:
                e(d)
            e.pop()
        e("begin")
        e.push()
        self.frames.append((label, err, state, msg))
        self.statements(node.body)
        self.frames.pop()
        if not node.body:
            e("null;")
        e.pop()
        e(f"end {label};")
        e(f"if {err} then")
        e.push()
        e("begin")
        e.push()
        e(f"raise exception using errcode = {state}, message = {msg};")
        e.pop()
        e("exception")
        e.push()
        apps = [c for names, _ in node.handlers for n in names
                for c in self._condition(n, node.line) if isinstance(c, tuple)]
        if apps and (len(apps) > 1 or any(
                c in ("others", "sqlstate 'P0001'") for names, _ in
                node.handlers for n in names
                for c in self._condition(n, node.line))):
            raise Refused(f"line {node.line} handles one of Oracle's"
                          " application errors by its number beside a"
                          " handler that also catches it, which migkit's"
                          " rules do not tell apart")
        for names, stmts in node.handlers:
            conds = []
            for n in names:
                conds += self._condition(n, node.line)
            app = [c for c in conds if isinstance(c, tuple)]
            conds = ["sqlstate 'P0001'" if isinstance(c, tuple) else c
                     for c in conds]
            e(f"when {' or '.join(dict.fromkeys(conds))} then")
            e.push()
            if app:
                # Oracle's error number rides in the hint: another
                # application error is not this handler's
                hint = self.fresh("hint")
                self.decls.append(f"{hint} text;")
                e(f"get stacked diagnostics {hint} = pg_exception_hint;")
                e(f"if {hint} is distinct from 'ORA{app[0][1]}' then")
                e("  raise;")
                e("end if;")
            outer, self._in_handler = self._in_handler, True
            self.statements(stmts)
            self._in_handler = outer
            if not stmts:
                e("null;")
            e.pop()
        e.pop()
        e("end;")
        e.pop()
        e("end if;")

    def _condition(self, name, line):
        name = name.lower()
        if name in NAMED:
            return NAMED[name]
        for got in (self.exceptions.get(name),
                    self.package.exceptions.get(name.split(".")[-1])
                    if self.package is not None and (
                        "." not in name or name.split(".")[0] ==
                        self.package.name) else None):
            if isinstance(got, tuple):
                return [got]
            if got:
                return [f"sqlstate '{got}'" if re.fullmatch(r"[0-9A-Z]{5}",
                                                             got) else got]
        raise Refused(f"line {line} handles {name}, which migkit's rules"
                      " have no condition for")

    def s_null(self, node):
        self.e("null;")

    def _target(self, target, line):
        """An assignment's target in PL/pgSQL: a variable, a record's
        field, a package's variable (whose setter is called), or a
        trigger's row."""
        t = target.strip()
        low = t.lower()
        if low.startswith((":new.", ":old.")):
            return t[1:].lower(), None
        if self.package is not None:
            if low in self.package.vars and low not in self.vars:
                return None, low
            if "." in low and low.split(".")[0] == self.package.name and \
                    low.split(".")[1] in self.package.vars:
                return None, low.split(".")[1]
        head = low.split(".")[0]
        if head not in self.vars and head not in self.records and \
                not (self.trigger and head in ("new", "old")):
            raise Refused(f"line {line} assigns {t}, not declared before it")
        return t, None

    def s_assign(self, node):
        target, pkg_var = self._target(node.target, node.line)
        value = self.expr(node.value, node.line)
        if pkg_var is not None:
            typ = self.package.vars[pkg_var][0]
            self.code("plsql.package-state", node.line)
            self.guarded([self.package.setter(pkg_var,
                                              self._assigned(typ, value))])
            return
        typ = self.vars.get(target.lower(), (None, ""))[1]
        self.guarded([f"{target} := {self._assigned(typ or '', value)};"])

    def s_if(self, node):
        e = self.e
        for k, (cond, stmts) in enumerate(node.branches):
            e(f"{'if' if k == 0 else 'elsif'} {self.expr(cond, node.line)}"
              " then")
            e.push()
            self.statements(stmts)
            if not stmts:
                e("null;")
            e.pop()
        if node.otherwise is not None:
            e("else")
            e.push()
            self.statements(node.otherwise)
            if not node.otherwise:
                e("null;")
            e.pop()
        e("end if;")

    def s_case(self, node):
        e = self.e
        e("case" + (f" {self.expr(node.selector, node.line)}"
                    if node.selector else ""))
        e.push()
        for cond, stmts in node.branches:
            e(f"when {self.expr(cond, node.line)} then")
            e.push()
            self.statements(stmts)
            if not stmts:
                e("null;")
            e.pop()
        if node.otherwise is not None:
            e("else")
            e.push()
            self.statements(node.otherwise)
            if not node.otherwise:
                e("null;")
            e.pop()
        e.pop()
        e("end case;")

    def _loop(self, node, head):
        e = self.e
        if node.label:
            e(f"<<{ident(node.label)}>>")
        e(head)
        e.push()
        self.statements(node.body)
        if not node.body:
            e("null;")
        e.pop()
        e("end loop;")

    def s_loop(self, node):
        self._loop(node, "loop")

    def s_while(self, node):
        self._loop(node, f"while {self.expr(node.cond, node.line)} loop")

    def s_for_range(self, node):
        low = self.expr(node.low, node.line)
        high = self.expr(node.high, node.line)
        var = ident(node.var)
        self.vars[node.var.lower()] = (var, "integer")
        if node.reverse:
            # Oracle counts down from the high bound; PL/pgSQL takes the
            # bounds the way it counts
            self._loop(node, f"for {var} in reverse {high}..{low} loop")
        else:
            self._loop(node, f"for {var} in {low}..{high} loop")

    def s_for_cursor(self, node):
        c = self.cursors.get(node.cursor.lower())
        if c is None:
            raise Refused(f"line {node.line} loops over {node.cursor}, not a"
                          " cursor declared before it")
        self.code("plsql.cursor-loop", node.line)
        rec = ident(node.var)
        self.records.add(node.var.lower())
        args = f"({self.expr(node.args, node.line)})" if node.args else ""
        self._loop(node, f"for {rec} in {c}{args} loop")

    def s_for_query(self, node):
        rec = ident(node.var)
        self.records.add(node.var.lower())
        self.decls.append(f"{rec} record;")
        self._loop(node, f"for {rec} in {self.sql(node.query, node.line)}"
                         " loop")

    def s_exit(self, node):
        self.e(f"exit{' ' + ident(node.target) if node.target else ''}"
               + (f" when {self.expr(node.cond, node.line)}"
                  if node.cond else "") + ";")

    def s_continue(self, node):
        self.e(f"continue{' ' + ident(node.target) if node.target else ''}"
               + (f" when {self.expr(node.cond, node.line)}"
                  if node.cond else "") + ";")

    def s_return(self, node):
        if self.trigger is not None:
            self.e(self.trigger["return"])
            return
        if self.r.kind == "function":
            if node.value is None:
                raise Refused(f"line {node.line} returns nothing from a"
                              " function")
            typ = pg_type(self.unit_returns, self.ctx)
            value = self._assigned(typ, self.expr(node.value, node.line))
            self.guarded([f"return {value};"])
            return
        self.e("return;")

    def s_raise(self, node):
        if node.name is None:
            if not self._in_handler:
                raise Refused(f"line {node.line} raises again outside a"
                              " handler")
            self.e("raise;")
            return
        conds = self._condition(node.name, node.line)
        cond = conds[0]
        if isinstance(cond, tuple):
            self.code("plsql.application-error", node.line)
            self.guarded([f"raise exception using errcode = 'P0001',"
                          f" message = '', hint = 'ORA{cond[1]}';"])
        elif cond.startswith("sqlstate '"):
            code = cond[len("sqlstate '"):-1]
            self.code("plsql.user-exception", node.line)
            self.guarded([f"raise exception using errcode = '{code}',"
                          " message = 'user-defined exception';"])
        elif cond == "others":
            raise Refused(f"line {node.line} raises OTHERS")
        else:
            code = _STATES.get(cond, cond)
            self.guarded([f"raise exception using errcode = '{code}';"])

    _in_handler = False

    def s_select_into(self, node):
        m = re.search(r"(?is)\binto\b(.*?)\bfrom\b", node.text)
        if not re.search(r"(?i)\binto\b", node.text):
            raise Refused(f"line {node.line} sends a result set, which a"
                          " PL/SQL routine does not")
        if re.search(r"(?i)\bbulk\s+collect\b", node.text):
            raise Refused(f"line {node.line} selects in bulk (BULK"
                          " COLLECT)")
        if m is None:
            raise Refused(f"line {node.line}: its INTO could not be read")
        named, after = self._targets(m.group(1), node.line)
        query = node.text[:m.start()] + " " + node.text[m.end() - 4:]
        text = self.sql(query, node.line)
        lines = [_into_strict(text, named)] + after
        if self.uses_sql_attrs or self._reads_sql_attrs():
            lines.append("get diagnostics _migkit_sql_rowcount = row_count;")
        self.guarded(lines)

    def _targets(self, text, line):
        """(the names a SELECT or a RETURNING puts its values in, the
        statements after it): a package's variable is put in a local one
        first, then set through its package."""
        named, after = [], []
        for t in (x.strip() for x in text.split(",")):
            target, pkg_var = self._target(t, line)
            if pkg_var is None:
                named.append(target)
                continue
            typ = self.package.vars[pkg_var][0]
            held = self.fresh("p")
            self.decls.append(f"{held} {typ};")
            named.append(held)
            after.append(self.package.setter(pkg_var, held))
            self.code("plsql.package-state", line)
        return named, after

    def _reads_sql_attrs(self):
        return bool(re.search(r"(?i)\bsql\s*%", self.unit_text))

    def s_dml(self, node):
        text = node.text
        returning = re.search(r"(?is)\breturning\b(.*?)\binto\b(.*)$", text)
        into, after = "", []
        if returning:
            text = text[:returning.start()]
            cols = ", ".join(self.expr(c, node.line)
                             for c in _split(returning.group(1)))
            named, after = self._targets(returning.group(2), node.line)
            into = f" returning {cols} into strict {', '.join(named)}"
        if node.verb == "merge":
            self.code("plsql.merge", node.line)
        lines = [f"{self.sql(text, node.line)}{into};"] + after
        if self._reads_sql_attrs():
            lines.append("get diagnostics _migkit_sql_rowcount = row_count;")
        self.guarded(lines)

    def s_open(self, node):
        c = self.cursors.get(node.cursor.lower())
        if c is None:
            raise Refused(f"line {node.line} opens {node.cursor}, not a"
                          " cursor declared before it")
        args = f"({self.expr(node.args, node.line)})" if node.args else ""
        self.guarded([f"open {c}{args};", f"{c}_open := true;",
                      f"{c}_found := null;", f"{c}_rows := 0;"])

    def s_close(self, node):
        c = self.cursors.get(node.cursor.lower())
        if c is None:
            raise Refused(f"line {node.line} closes {node.cursor}, not a"
                          " cursor declared before it")
        self.guarded([f"close {c};", f"{c}_open := false;"])

    def s_fetch(self, node):
        c = self.cursors.get(node.cursor.lower())
        if c is None:
            raise Refused(f"line {node.line} fetches {node.cursor}, not a"
                          " cursor declared before it")
        for t in node.targets:
            self._target(t, node.line)
        # PL/pgSQL sets the targets to NULL past the last row; Oracle
        # leaves them as they were
        shadows, copies = [], []
        for t in node.targets:
            typ = self.vars.get(t.lower(), (None, None))[1]
            if typ is None:
                shadows = None
                break
            s = self.fresh("f")
            self.decls.append(f"{s} {typ};")
            shadows.append(s)
            copies.append(f"{t} := {s};")
        if shadows is None:
            lines = [f"fetch {c} into {', '.join(node.targets)};"]
        else:
            lines = [f"fetch {c} into {', '.join(shadows)};",
                     "if found then"] + ["  " + x for x in copies] + \
                ["end if;"]
        lines += [f"{c}_found := found;",
                  f"if found then {c}_rows := {c}_rows + 1; end if;"]
        self.guarded(lines)

    def s_tx(self, node):
        if self.r.kind != "procedure" and not self.autonomous:
            raise Refused(f"line {node.line} ends a transaction, which only a"
                          " procedure can")
        if self.frames:
            raise Refused(f"line {node.line} ends a transaction inside a"
                          " block that handles errors; PostgreSQL cannot"
                          " commit there")
        self.code("plsql.transaction", node.line)
        self.commits = True
        self.e(f"{node.what};")

    autonomous = False

    def s_call(self, node):
        text = node.text.strip()
        m = re.fullmatch(r"(?is)dbms_output\s*\.\s*put_line\s*\((.*)\)", text)
        if m:
            self.e(f"raise notice '%', {self.expr(m.group(1), node.line)};")
            return
        m = re.fullmatch(r"(?is)raise_application_error\s*\((.*)\)", text)
        if m:
            parts = _split(m.group(1))
            if len(parts) < 2:
                raise Refused(f"line {node.line}: RAISE_APPLICATION_ERROR"
                              " could not be read")
            self.code("plsql.application-error", node.line)
            number = _whole(parse(f"SELECT {parts[0]}", "oracle")
                            .expressions[0])
            hint = f"'ORA{number}'" if number is not None else \
                f"'ORA' || ({self.expr(parts[0], node.line)})::text"
            self.guarded([f"raise exception using errcode = 'P0001',"
                          f" message = {self.expr(parts[1], node.line)},"
                          f" hint = {hint};"])
            return
        m = re.fullmatch(r"(?is)([\w$#.\"]+)\s*(\((.*)\))?", text)
        if not m:
            raise Refused(f"line {node.line}: {text[:40]} could not be read")
        name = m.group(1).strip('"').lower()
        args = _split(m.group(3)) if m.group(3) else []
        callee = self._callee(name, node.line)
        passed = []
        for a in args:
            named = re.match(r"(?s)^\s*(\w+)\s*=>\s*(.*)$", a)
            if named:
                passed.append(f"{ident(named.group(1))} =>"
                              f" {self.expr(named.group(2), node.line)}")
            else:
                passed.append(self.expr(a, node.line))
        self.code("plsql.call", node.line)
        self.guarded([f"call {callee}({', '.join(passed)});"])

    def _callee(self, name, line):
        parts = name.split(".")
        routines = getattr(self.ctx, "routines", None) or {}
        if len(parts) == 1:
            if self.package is not None and parts[0] in self.package.members:
                return f"{self.package.schema}.{ident(parts[0])}"
            if parts[0] in routines:
                return ident(routines[parts[0]].name)
        elif len(parts) == 2:
            pkg = parts[0]
            if self.package is not None and pkg == self.package.name:
                return f"{self.package.schema}.{ident(parts[1])}"
            known = getattr(self.ctx, "packages", None) or {}
            if pkg in known:
                return f"{ident(pkg)}.{ident(parts[1])}"
        raise Refused(f"line {line} calls {name}, which is not one of the"
                      " source's own routines")


#: the conditions a named exception is raised by, as their SQLSTATE
_STATES = {"no_data_found": "P0002", "too_many_rows": "P0003",
           "unique_violation": "23505", "division_by_zero": "22012",
           "invalid_text_representation": "22P02",
           "numeric_value_out_of_range": "22003",
           "string_data_right_truncation": "22001",
           "null_value_not_allowed": "22004", "case_not_found": "20000",
           "duplicate_cursor": "42P03", "invalid_cursor_state": "24000",
           "not_null_violation": "23502", "foreign_key_violation": "23503",
           "check_violation": "23514", "deadlock_detected": "40P01",
           "lock_not_available": "55P03"}


def _initialised(code, line):
    """What `PRAGMA EXCEPTION_INIT(e, code)` makes `e`: the condition of
    the Oracle error, or ("app", code) for an application error number."""
    if -20999 <= code <= -20000:
        return ("app", code)
    cond = ORA_ERRORS.get(code)
    if cond is None:
        raise Refused(f"line {line} names Oracle's error {code}, which"
                      " migkit's rules have no condition for")
    return cond


def _whole(node):
    """A whole number written as a constant, or None."""
    sign = 1
    if isinstance(node, exp.Neg):
        sign, node = -1, node.this
    if isinstance(node, exp.Literal) and not node.is_string and \
            re.fullmatch(r"\d+", str(node.this)):
        return sign * int(node.this)
    return None


def _split(text):
    out, depth, buf, quote = [], 0, [], False
    for ch in text or "":
        if ch == "'":
            quote = not quote
        if not quote:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                out.append("".join(buf).strip())
                buf = []
                continue
        buf.append(ch)
    if "".join(buf).strip():
        out.append("".join(buf).strip())
    return out


def _into_strict(select_sql, targets):
    """A generated SELECT with `INTO STRICT a, b` after its select list:
    no row and more than one raise, as Oracle's SELECT INTO does."""
    tree = parse(select_sql, "postgres")
    tree.set("into", exp.Into(this=exp.to_table("_migkit_into_")))
    text = generate(tree, "postgres", "postgres", normalize=False)
    return text.replace("INTO _migkit_into_",
                        f"INTO STRICT {', '.join(targets)}", 1) + ";"


# ---- a routine ----------------------------------------------------------------


def _header_params(r, read_params, ctx):
    """[(pg name, pg type, mode, default)] of a routine's parameters, the
    catalogue's names, types and modes and the text's defaults."""
    defaults = {p["name"].lower(): p["default"] for p in read_params}
    texts = {p["name"].lower(): p for p in read_params}
    out = []
    params = r.params or [type("P", (), {"name": p["name"], "type": p["type"],
                                         "mode": p["mode"]})()
                          for p in read_params]
    for p in params:
        typ = pg_type(texts.get(p.name.lower(), {}).get("type") or p.type,
                      ctx)
        # a parameter's length and scale are not kept by PostgreSQL
        if not typ.endswith("type"):
            typ = re.sub(r"\(.*\)$", "", typ)
        out.append((ident(p.name), typ, p.mode,
                    defaults.get(p.name.lower())))
    return out


def _body(g, sub, decl_lines, extra=()):
    """The PL/pgSQL text of one subprogram's body."""
    head = ["#variable_conflict use_column"]
    g.e.push()
    for line in extra:
        g.e(line)
    g.statements([sub["block"]] if sub["block"].handlers else
                 sub["block"].body)
    g.e.pop()
    decls = list(decl_lines) + g.decls
    if g.uses_sql_attrs or g._reads_sql_attrs():
        decls.append("_migkit_sql_rowcount bigint := 0;")
    if decls:
        head += ["declare"] + ["  " + d for d in decls]
    text = "\n".join(head + ["begin"] + g.e.lines + ["end"])
    if QUOTE in text:
        raise Refused("its body holds migkit's own quoting mark")
    return text


def _subprogram(r, ctx, sub, package=None, name=None, trigger=None,
                scope=None):
    """[statements] making one subprogram on the target, and its
    generator (constructs, commits)."""
    g = _Gen(r, ctx, scope or r.name, package=package, trigger=trigger)
    g.unit_text = r.body or ""
    g.unit_returns = sub["returns"]
    autonomous = any(d["what"] == "autonomous" for d in sub["decls"])
    params = _header_params(r, sub["params"], ctx) if trigger is None \
        else []
    for pg, typ, mode, _ in params:
        g.vars[pg.strip('"').lower()] = (pg, typ)
    g.autonomous = autonomous
    decl_lines = g.declare(sub["decls"], scope or r.name)
    extra = [f"perform {package.schema}.{ident('_migkit_init')}();"] \
        if package is not None and package.vars else []
    body = _body(g, sub, decl_lines, extra)
    target = name or ident(r.name)
    if trigger is not None:
        return [f"create or replace function {target}() returns trigger"
                f" language plpgsql as {QUOTE}\n{body}\n{QUOTE}"], g
    listed = []
    for pg, typ, mode, default in params:
        if mode == "out" and r.kind == "function":
            raise Refused("it returns values through OUT parameters as well"
                          " as its own, which a PostgreSQL function cannot")
        d = f" default {g.expr(default, 1)}" if default is not None else ""
        listed.append(f"{'' if mode == 'in' else mode + ' '}{pg} {typ}{d}")
    if autonomous:
        return _autonomous(r, target, listed, params, body, g), g
    if r.kind == "function":
        ret = pg_type(sub["returns"], ctx)
        ret = re.sub(r"\(.*\)$", "", ret) if not ret.endswith("type") \
            else ret
        return [f"create or replace function {target}({', '.join(listed)})"
                f" returns {ret} language plpgsql as"
                f" {QUOTE}\n{body}\n{QUOTE}"], g
    return [f"create or replace procedure {target}({', '.join(listed)})"
            f" language plpgsql as {QUOTE}\n{body}\n{QUOTE}"], g


def _autonomous(r, target, listed, params, body, g):
    """An autonomous procedure: its body a procedure of its own, called
    over the target's loopback connection to itself, whose commit is its
    own."""
    if r.kind != "procedure":
        raise Refused("it is an autonomous function: its answer would come"
                      " back over a loopback connection, which migkit's"
                      " rules do not make")
    if any(mode != "in" for _, _, mode, _ in params):
        raise Refused("it is an autonomous procedure with OUT parameters,"
                      " whose values a loopback connection does not bring"
                      " back")
    g.code("plsql.autonomous", 1)
    inner = target[:-1] + '__autonomous"' if target.endswith('"') else \
        f"{target}__autonomous"
    passed = " || ', ' || ".join(
        f"quote_nullable({pg}) || '::{typ}'" for pg, typ, _, _ in params) \
        or "''"
    wrapper = "\n".join([
        "begin",
        "  perform dblink_exec('dbname=' || quote_literal(current_database())"
        " || ' user=' || quote_literal(current_user),",
        f"    'call {inner.replace(chr(39), chr(39) * 2)}(' || {passed} ||"
        " ')');",
        "end"])
    return ["create extension if not exists dblink",
            f"create or replace procedure {inner}({', '.join(listed)})"
            f" language plpgsql as {QUOTE}\n{body}\n{QUOTE}",
            f"create or replace procedure {target}({', '.join(listed)})"
            f" language plpgsql as {QUOTE}\n{wrapper}\n{QUOTE}"]


def _trigger(r, ctx):
    """A row or statement trigger: its function and the trigger."""
    text = r.body or ""
    m = re.match(r"(?is)\s*trigger\s+(\"[^\"]+\"|[\w$#.]+)\s+(.*?)\b(declare|"
                 r"begin)\b(.*)$", text)
    if not m:
        raise Refused("its header could not be read")
    head, rest = m.group(2), m.group(3) + m.group(4)
    if re.search(r"(?i)\bcompound\b|\binstead\s+of\b", head):
        raise Refused("it is a compound or INSTEAD OF trigger, which"
                      " migkit's rules do not carry")
    if re.search(r"(?i)\b(updating|inserting|deleting)\s*\(", text):
        raise Refused("it asks which columns the statement set"
                      " (UPDATING('col')), which the target does not say")
    when = re.search(r"(?is)\bwhen\s*\((.*)\)\s*$", head)
    row = bool(re.search(r"(?i)\bfor\s+each\s+row\b", head))
    timing = (r.timing or "AFTER").upper()
    # which statement fired it, as PostgreSQL says it
    rest = re.sub(r"(?i)\binserting\b", "(tg_op = 'INSERT')", rest)
    rest = re.sub(r"(?i)\bupdating\b", "(tg_op = 'UPDATE')", rest)
    rest = re.sub(r"(?i)\bdeleting\b", "(tg_op = 'DELETE')", rest)
    reader = _Reader(rest)
    decls = []
    if reader.eat("declare"):
        decls = reader.declarations()
    block = reader.block_rest(None, 1)
    sub = {"kind": "trigger", "params": [], "returns": None,
           "decls": decls, "block": block, "line": 1}
    ret = ("return old;" if r.events == ("DELETE",) else "return new;") \
        if row and timing == "BEFORE" else "return null;"
    fn = ident(f"{r.name}_fn")
    made, g = _subprogram(r, ctx, sub, name=fn,
                          trigger={"return": ret})
    # the function's body ends returning, as PostgreSQL wants
    made[0] = made[0].replace(f"\nend\n{QUOTE}", f"\n  {ret}\nend\n{QUOTE}")
    table = r.table
    if ctx is not None and getattr(ctx, "local", None) is not None:
        table = ctx.local(exp.select("*").from_(exp.to_table(table))) \
            .find(exp.Table).name
    cond = ""
    if when:
        cond = f" when ({g.expr(when.group(1), 1)})"
    made.append(f"create trigger {ident(r.name)} {timing.lower()}"
                f" {' or '.join(e.lower() for e in r.events)} on {table}"
                f" for each {'row' if row else 'statement'}{cond} execute"
                f" function {fn}()")
    g.code("plsql.trigger", 1)
    return Converted(made, constructs=g.codes)


def convert(routine, ctx=None):
    """`routine` (an Oracle one: standalone, or a package's subprogram) as
    PostgreSQL statements, or Refused."""
    r = routine
    if r.settings.get("package"):
        return convert_package(r, ctx)
    if r.kind == "trigger":
        return _trigger(r, ctx)
    if r.kind not in ("function", "procedure"):
        raise Refused(f"a {r.kind} is not something migkit's rules convert")
    sub = read(r.body or "")
    made, g = _subprogram(r, ctx, sub)
    return Converted(made, constructs=g.codes, commits=g.commits)


# ---- packages -----------------------------------------------------------------


def _package_parts(spec, body):
    """(the spec's declarations, the body's declarations, the body's
    subprograms {lower name: (kind, text)}, the initialisation block)."""
    rs = _Reader(spec)
    rs.need("package")
    rs.dotted()
    while rs.w() in ("authid", "accessible"):
        rs.until(lambda t: t.word in ("is", "as"), semicolon=False)
    if rs.w() not in ("is", "as"):
        raise Refused("its package's spec could not be read")
    rs.i += 1
    public = rs.declarations(until=("end",))
    rb = _Reader(body)
    rb.need("package")
    rb.need("body")
    rb.dotted()
    if rb.w() not in ("is", "as"):
        raise Refused("its package's body could not be read")
    rb.i += 1
    private, subs, init = [], {}, None
    while not rb.at_end() and rb.w() not in ("begin", "end"):
        if rb.w() in ("function", "procedure"):
            start = rb.i
            sub = rb.subprogram()
            subs[sub["name"].lower()] = (sub["kind"], rb.src(start, rb.i),
                                         sub)
            continue
        got = rb.declarations(until=("begin", "end", "function",
                                     "procedure"))
        private += got
    if rb.w() == "begin":
        init = rb.block_rest(None, rb.line())
    return public, private, subs, init


def convert_package(r, ctx):
    """Every subprogram of `r`'s package, its state and initialisation, as
    PostgreSQL statements - the same for each member the proof asks: the
    package is made whole."""
    name = r.settings["package"]
    cache = getattr(ctx, "package_made", None)
    if cache is None and ctx is not None:
        try:
            ctx.package_made = cache = {}
        except AttributeError:
            cache = None
    if cache is not None and name.lower() in cache:
        got = cache[name.lower()]
        if isinstance(got, Refused):
            raise got
        return got
    try:
        made = _package(r, ctx, name)
    except Refused as e:
        if cache is not None:
            cache[name.lower()] = e
        raise
    if cache is not None:
        cache[name.lower()] = made
    return made


def _package(r, ctx, name):
    spec = r.settings.get("spec") or ""
    body = r.settings.get("package body") or ""
    public, private, subs, init = _package_parts(spec, body)
    pkg = Package(name)
    codes = []
    for d in public + private:
        if d["what"] == "cursor":
            raise Refused(f"its package declares the cursor {d['name']},"
                          " whose state across calls migkit's rules do not"
                          " carry")
        if d["what"] == "var":
            typ = pg_type(d["type"], ctx)
            if typ.endswith("%rowtype"):
                raise Refused(f"its package keeps a record ({d['name']}),"
                              " whose state migkit's rules do not carry")
            pkg.vars[d["name"].lower()] = (typ, d["default"], d["constant"])
        elif d["what"] == "exception":
            pkg.exceptions[d["name"].lower()] = user_code(name, d["name"])
        elif d["what"] == "exception_init":
            pkg.exceptions[d["name"].lower()] = _initialised(d["code"],
                                                             d["line"])
    for sub_name, (kind, _, _) in subs.items():
        pkg.members[sub_name] = kind
    made = [f"create schema if not exists {pkg.schema}"]
    if pkg.vars:
        made += _state(pkg, ctx, init, r)
        codes.append(("plsql.package-state", 1))
    from .routines import Param, Routine
    for sub_name, (kind, text, sub) in subs.items():
        member = Routine(kind=kind, name=f"{name}.{sub_name}",
                         params=[Param(p["name"], p["type"], p["mode"])
                                 for p in sub["params"]],
                         returns=sub["returns"], body=text)
        stmts, g = _subprogram(member, ctx, sub, package=pkg,
                               name=f"{pkg.schema}.{ident(sub_name)}",
                               scope=name)
        made += stmts
        codes += g.codes
    codes.append(("plsql.package", 1))
    return Converted(made, constructs=list(dict.fromkeys(codes)),
                     residue=[f"the package {name}'s variables are kept in"
                              " session settings: a ROLLBACK or an error"
                              " caught in a block undoes their changes,"
                              " where Oracle keeps them"]
                     if pkg.vars else [])


def _state(pkg, ctx, init, r):
    """The functions reading and writing each package variable, and the
    one setting them to their first values and running the package's
    initialisation, once a session."""
    out = []
    for var, (typ, default, constant) in pkg.vars.items():
        get = ident("_migkit_get_" + var)
        put = ident("_migkit_set_" + var)
        setting = pkg.setting(var)
        if constant:
            g = _Gen(r, ctx, pkg.name, package=pkg)
            g.unit_text = ""
            value = g.expr(default, 1) if default is not None else "null"
            out.append(f"create or replace function {pkg.schema}.{get}()"
                       f" returns {typ} language sql as {QUOTE} select"
                       f" cast({value} as {typ}) {QUOTE}")
            continue
        out.append(
            f"create or replace function {pkg.schema}.{get}() returns {typ}"
            f" language plpgsql as {QUOTE}\ndeclare\n  s text :="
            f" current_setting('{setting}', true);\nbegin\n  if s is null or"
            f" s = '' then\n    perform {pkg.schema}."
            f"{ident('_migkit_init')}();\n    s := current_setting("
            f"'{setting}', true);\n  end if;\n  if s is null or s = '' or s"
            f" = 'n' then\n    return null;\n  end if;\n  return"
            f" {pkg.value_of(typ, 'substr(s, 2)')};\nend\n{QUOTE}")
        out.append(
            f"create or replace function {pkg.schema}.{put}(x {typ}) returns"
            f" void language plpgsql as {QUOTE}\nbegin\n  perform"
            f" {pkg.schema}.{ident('_migkit_init')}();\n  perform"
            f" set_config('{setting}', case when x is null then 'n' else 'v'"
            f" || {pkg.text_of(typ, 'x')} end, false);\nend\n{QUOTE}")
    g = _Gen(r, ctx, pkg.name, package=pkg)
    g.unit_text = ""
    g.e.push()
    g.e(f"if current_setting('{pkg.setting('_init')}', true) = 'y' then")
    g.e("  return;")
    g.e("end if;")
    g.e(f"perform set_config('{pkg.setting('_init')}', 'y', false);")
    for var, (typ, default, constant) in pkg.vars.items():
        if constant:
            continue
        value = g.expr(default, 1) if default is not None else "null"
        g.e(f"perform set_config('{pkg.setting(var)}', case when"
            f" ({value}) is null then 'n' else 'v' ||"
            f" {pkg.text_of(typ, value)} end, false);")
    if init is not None:
        g.code("plsql.package-init", 1)
        g.statements([init] if init.handlers else init.body)
    g.e.pop()
    body = "\n".join(["begin"] + g.e.lines + ["end"])
    decls = g.decls
    if decls:
        body = "declare\n" + "\n".join("  " + d for d in decls) + "\n" + body
    out.insert(0, f"create or replace function {pkg.schema}."
                  f"{ident('_migkit_init')}() returns void language plpgsql"
                  f" as {QUOTE}\n{body}\n{QUOTE}")
    return out

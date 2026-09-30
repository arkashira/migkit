"""MySQL stored programs in PostgreSQL's PL/pgSQL, by migkit's own rules.

MySQL's compound-statement grammar is small (`BEGIN ... END`, `DECLARE`,
`IF`, `CASE`, `WHILE`, `REPEAT`, `LOOP`, `LEAVE`, `ITERATE`, handlers,
cursors, `SIGNAL`), so it is read here statement by statement, and every
SQL statement inside it goes to the query translator with the rules below
applied on the way. What has no rule is refused by name - the construct
and its line - never passed through.

The rules that keep meaning, not only syntax:

* a `CONTINUE` handler resumes after the statement that failed, and the
  statements before it keep their writes; a PL/pgSQL exception block rolls
  back everything inside it. So each statement under a handler is its own
  block, which rolls back only that statement - as MySQL rolls back a
  failed statement - and an `EXIT` handler leaves the block that declared
  it;
* `SELECT ... INTO` with no row leaves the variables as they were and
  raises NOT FOUND; PL/pgSQL would set them to NULL and carry on. So it is
  `INTO STRICT`, no row goes to the NOT FOUND handler (or nowhere), and
  more than one row is raised in MySQL's error class;
* `FETCH` past the last row leaves the variables as they were; PL/pgSQL's
  sets them to NULL. So it fetches into variables of its own and copies
  only a row that came;
* a parameter declared `DECIMAL(10,2)` is rounded to two places on the way
  in, a `VARCHAR(5)` refused past five characters under a strict
  `sql_mode`; PostgreSQL ignores both on a parameter and a result, so they
  are said in the body;
* `LENGTH` is bytes, `CONCAT` is NULL where any part is and turns numbers
  into text, `/` divides as decimals and answers NULL for a zero divisor,
  `CAST(... AS CHAR)` is text of any length, `CAST(... AS UNSIGNED)` a
  64-bit number without a sign, `DELETE ... LIMIT` deletes that many rows,
  `ON DUPLICATE KEY UPDATE` updates the row the one unique key names;
* a procedure's plain `SELECT` sends a result set to its caller; a
  PostgreSQL procedure hands its caller a cursor instead (an `INOUT
  refcursor` parameter per result set), which is how drivers read them.

The same parse puts coverage probes in the sandbox's copy of a MySQL
routine (`instrument`): MySQL measures no coverage of its own.
"""
import re

from sqlglot import exp
from sqlglot.dialects.mysql import MySQL
from sqlglot.tokens import TokenType

from .plpgsql import (MYSQL_ERRNO, MYSQL_SQLSTATE, QUOTE, Converted, Emitter,
                      ident)
from .translate import Refused, generate, parse

#: tokens that are values or quoted names, never keywords
_QUOTED = {getattr(TokenType, n) for n in (
    "STRING", "IDENTIFIER", "NUMBER", "NATIONAL_STRING", "HEX_STRING",
    "BIT_STRING", "BYTE_STRING", "RAW_STRING", "HEREDOC_STRING",
    "UNICODE_STRING") if hasattr(TokenType, n)}

#: what a statement list inside a compound statement ends at
_ENDS = {"END", "ELSE", "ELSEIF", "WHEN", "UNTIL"}

#: statements the parser reads, by their first word
_STARTS = ("BEGIN", "IF", "CASE", "WHILE", "REPEAT", "LOOP", "LEAVE",
           "ITERATE", "RETURN", "SET", "OPEN", "CLOSE", "FETCH", "SIGNAL",
           "RESIGNAL", "CALL", "GET", "START", "COMMIT", "ROLLBACK", "SELECT",
           "WITH", "INSERT", "UPDATE", "DELETE", "REPLACE", "TRUNCATE",
           "DECLARE")


class Node:
    """One statement: its kind, its parts, and where its text is."""

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
        out += [h[2] for h in a.get("handlers") or []]
        return out

    def walk(self):
        yield self
        for child in self.children():
            yield from child.walk()


class _Parser:
    def __init__(self, text):
        self.text = text
        try:
            self.t = MySQL().tokenize(text)
        except Exception as e:  # noqa: BLE001 - said as a refusal
            raise Refused(f"its body could not be read: {str(e)[:80]}")
        self.i = 0

    # ---- tokens --------------------------------------------------------

    def tok(self, k=0):
        j = self.i + k
        return self.t[j] if 0 <= j < len(self.t) else None

    def is_(self, kind, k=0):
        t = self.tok(k)
        return t is not None and t.token_type == kind

    def w(self, k=0):
        t = self.tok(k)
        if t is None or t.token_type in _QUOTED:
            return None
        return t.text.upper()

    def at_end(self):
        return self.i >= len(self.t)

    def pos(self):
        t = self.tok()
        return t.start if t is not None else len(self.text)

    def line(self):
        t = self.tok()
        return t.line if t is not None else 0

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
                          f" {' '.join(words)} at line {self.line()},"
                          f" found {got}")

    def name(self):
        t = self.tok()
        if t is None or t.token_type in (TokenType.STRING, TokenType.NUMBER,
                                         TokenType.SEMICOLON):
            raise Refused(f"its body could not be read: a name was expected"
                          f" at line {self.line()}")
        self.i += 1
        return t.text

    def _done_at(self, start):
        return self.t[self.i - 1].end + 1 if self.i > 0 else start

    def scan(self, stop, cases=True, comma=False):
        """Advance to the first token at depth 0 where `stop(word)` holds,
        a semicolon, or (with `comma`) a comma; (start, end) of what was
        passed over, as character offsets."""
        start, depth, case = self.pos(), 0, 0
        while not self.at_end():
            t = self.tok()
            word = self.w()
            if t.token_type == TokenType.L_PAREN:
                depth += 1
            elif t.token_type == TokenType.R_PAREN:
                if depth == 0:
                    break
                depth -= 1
            elif depth == 0 and cases and word == "CASE":
                case += 1
            elif depth == 0 and cases and word == "END" and case:
                case -= 1
                self.i += 1
                continue
            if depth == 0 and case == 0 and (
                    stop(word) or t.token_type == TokenType.SEMICOLON
                    or (comma and t.token_type == TokenType.COMMA)):
                break
            self.i += 1
        return start, max(start, self._done_at(start))

    def value(self):
        """An expression that ends at a comma or a semicolon."""
        return self.src(self.scan(lambda x: False, comma=True))

    def until_semicolon(self):
        return self.scan(lambda x: False, cases=False)

    def src(self, span):
        return self.text[span[0]:span[1]].strip()

    def semicolon(self):
        if self.is_(TokenType.SEMICOLON):
            self.i += 1
            return True
        return self.at_end()

    # ---- statements ----------------------------------------------------

    def body(self):
        node = self.statement()
        while self.is_(TokenType.SEMICOLON):
            self.i += 1
        if not self.at_end():
            raise Refused("its body could not be read: text after its end"
                          f" at line {self.line()}")
        return node

    def statements(self):
        out = []
        while True:
            while self.is_(TokenType.SEMICOLON):
                self.i += 1
            if self.at_end() or self.w() in _ENDS:
                return out
            out.append(self.statement())
            if not self.semicolon():
                raise Refused("its body could not be read: a statement"
                              f" does not end at line {self.line()}")

    def statement(self):
        label = None
        if self.is_(TokenType.COLON, 1) and \
                self.w(2) in ("BEGIN", "LOOP", "WHILE", "REPEAT"):
            label = self.tok().text
            self.i += 2
        start, line, w = self.pos(), self.line(), self.w()
        if self.is_(TokenType.L_PAREN):
            node = self._select(start, line)
        elif w in _STARTS:
            read = getattr(self, "_" + w.lower())
            node = read(start, line, label) if w in (
                "BEGIN", "WHILE", "REPEAT", "LOOP") else read(start, line)
        else:
            node = Node("unsupported", start, line, what=w or "?",
                        text=self.src(self.until_semicolon()))
        node.end = self._done_at(start)
        return node

    def _declare(self, start, line):
        raise Refused(f"a DECLARE at line {line} is not at the start of its"
                      " block")

    def _begin(self, start, line, label):
        self.need("BEGIN")
        self.eat("NOT", "ATOMIC")
        variables, cursors, handlers, conditions = [], [], [], {}
        while self.w() == "DECLARE":
            dline = self.line()
            self.i += 1
            if self.w(1) == "CONDITION":
                cname = self.name()
                self.need("CONDITION", "FOR")
                conditions[cname.lower()] = self._condition_value()
            elif self.w(1) == "CURSOR":
                cname = self.name()
                self.need("CURSOR", "FOR")
                cursors.append((cname, self.src(self.until_semicolon()),
                                dline))
            elif self.w() in ("CONTINUE", "EXIT", "UNDO") and \
                    self.w(1) == "HANDLER":
                kind = self.w()
                self.i += 2
                self.need("FOR")
                conds = [self._handler_condition()]
                while self.eat(","):
                    conds.append(self._handler_condition())
                handlers.append((kind, conds, self.statement(), dline))
            else:
                names = [self.name()]
                while self.eat(","):
                    names.append(self.name())
                typ = self.src(self.scan(lambda x: x == "DEFAULT",
                                         cases=False))
                default = None
                if self.eat("DEFAULT"):
                    default = self.src(self.until_semicolon())
                for n in names:
                    variables.append((n, typ, default, dline))
            if not self.semicolon():
                raise Refused("its body could not be read: a DECLARE does"
                              f" not end at line {dline}")
        body_at = self.pos()
        body = self.statements()
        end_at = self.pos()
        self.need("END")
        self._label_after(label)
        return Node("block", start, line, label=label, variables=variables,
                    cursors=cursors, handlers=handlers, conditions=conditions,
                    body=body, body_at=body_at, end_at=end_at)

    def _label_after(self, label):
        if label and self.w() == label.upper():
            self.i += 1

    def _condition_value(self):
        if self.eat("SQLSTATE"):
            self.eat("VALUE")
            t = self.tok()
            self.i += 1
            return ("sqlstate", t.text)
        t = self.tok()
        self.i += 1
        if t is not None and t.token_type == TokenType.NUMBER:
            return ("errno", int(t.text))
        raise Refused(f"a condition at line {self.line()} names neither a"
                      " SQLSTATE nor an error number")

    def _handler_condition(self):
        if self.eat("SQLEXCEPTION"):
            return ("class", "exception")
        if self.eat("SQLWARNING"):
            return ("class", "warning")
        if self.eat("NOT", "FOUND"):
            return ("class", "not found")
        if self.w() == "SQLSTATE" or self.is_(TokenType.NUMBER):
            return self._condition_value()
        return ("name", self.name().lower())

    def _if(self, start, line):
        self.need("IF")
        branches = []
        while True:
            cline = self.line()
            cond = self.src(self.scan(lambda x: x == "THEN"))
            self.need("THEN")
            at = self.pos()
            branches.append((cond, self.statements(), at, cline))
            if not self.eat("ELSEIF"):
                break
        otherwise, else_at = None, None
        if self.eat("ELSE"):
            else_at = self.pos()
            otherwise = self.statements()
        end_at = self.pos()
        self.need("END", "IF")
        return Node("if", start, line, branches=branches, otherwise=otherwise,
                    else_at=else_at, end_at=end_at)

    def _case(self, start, line):
        self.need("CASE")
        subject = None
        if self.w() != "WHEN":
            subject = self.src(self.scan(lambda x: x == "WHEN"))
        whens = []
        while self.eat("WHEN"):
            wline = self.line()
            value = self.src(self.scan(lambda x: x == "THEN"))
            self.need("THEN")
            at = self.pos()
            whens.append((value, self.statements(), at, wline))
        otherwise, else_at = None, None
        if self.eat("ELSE"):
            else_at = self.pos()
            otherwise = self.statements()
        end_at = self.pos()
        self.need("END", "CASE")
        return Node("case", start, line, subject=subject, whens=whens,
                    otherwise=otherwise, else_at=else_at, end_at=end_at)

    def _while(self, start, line, label):
        self.need("WHILE")
        cond = self.src(self.scan(lambda x: x == "DO"))
        self.need("DO")
        at = self.pos()
        body = self.statements()
        self.need("END", "WHILE")
        self._label_after(label)
        return Node("while", start, line, label=label, cond=cond, body=body,
                    body_at=at)

    def _repeat(self, start, line, label):
        self.need("REPEAT")
        at = self.pos()
        body = self.statements()
        self.need("UNTIL")
        cond = self.src(self.scan(lambda x: x == "END"))
        self.need("END", "REPEAT")
        self._label_after(label)
        return Node("repeat", start, line, label=label, cond=cond, body=body,
                    body_at=at)

    def _loop(self, start, line, label):
        self.need("LOOP")
        at = self.pos()
        body = self.statements()
        self.need("END", "LOOP")
        self._label_after(label)
        return Node("loop", start, line, label=label, body=body, body_at=at)

    def _leave(self, start, line):
        self.i += 1
        return Node("leave", start, line, label=self.name())

    def _iterate(self, start, line):
        self.i += 1
        return Node("iterate", start, line, label=self.name())

    def _return(self, start, line):
        self.i += 1
        return Node("return", start, line,
                    value=self.src(self.until_semicolon()))

    def _set(self, start, line):
        self.i += 1
        if self.w() in ("GLOBAL", "SESSION", "PERSIST", "PERSIST_ONLY",
                        "LOCAL", "NAMES", "CHARACTER", "TRANSACTION") or \
                self.is_(TokenType.SESSION_PARAMETER):
            return Node("unsupported", start, line, what="SET",
                        text="SET " + self.src(self.until_semicolon()))
        pairs = []
        while True:
            first = self.tok()
            last = None
            while not self.at_end() and not (
                    self.is_(TokenType.EQ) or self.is_(TokenType.COLON_EQ)):
                last = self.tok()
                self.i += 1
            if self.at_end() or first is None or last is None:
                raise Refused(f"a SET at line {line} could not be read")
            target = self.text[first.start:last.end + 1].strip()
            self.i += 1
            pairs.append((target, self.value()))
            if not self.eat(","):
                break
        return Node("set", start, line, pairs=pairs)

    def _open(self, start, line):
        self.i += 1
        return Node("open", start, line, cursor=self.name())

    def _close(self, start, line):
        self.i += 1
        return Node("close", start, line, cursor=self.name())

    def _fetch(self, start, line):
        self.i += 1
        if self.is_(TokenType.STRING):
            # the tokenizer keeps what follows FETCH at a statement's start
            # as one piece of text; it is read again, as its own tokens
            rest = _Parser(self.tok().text)
            self.i += 1
            node = rest._fetch_rest(start, line)
            if not rest.at_end():
                raise Refused(f"a FETCH at line {line} could not be read")
            return node
        return self._fetch_rest(start, line)

    def _fetch_rest(self, start, line):
        if not self.eat("NEXT", "FROM"):
            self.eat("FROM")
        cursor = self.name()
        self.need("INTO")
        targets = [self._target()]
        while self.eat(","):
            targets.append(self._target())
        return Node("fetch", start, line, cursor=cursor, targets=targets)

    def _target(self):
        if self.is_(TokenType.PARAMETER):
            self.i += 1
            return "@" + self.name()
        return self.name()

    def _signal(self, start, line):
        which = self.w().lower()
        self.i += 1
        cond = None
        if self.w() == "SQLSTATE":
            cond = self._condition_value()
        elif not self.at_end() and self.w() != "SET" and \
                not self.is_(TokenType.SEMICOLON):
            cond = ("name", self.name().lower())
        items = {}
        if self.eat("SET"):
            while True:
                item = self.name().upper()
                self.need("=")
                items[item] = self.value()
                if not self.eat(","):
                    break
        return Node(which, start, line, cond=cond, items=items)

    _resignal = _signal

    def _call(self, start, line):
        self.i += 1
        if self.is_(TokenType.STRING):
            # the tokenizer keeps what follows CALL as one piece of text,
            # as it does FETCH's; it is read again, as its own tokens
            rest = _Parser(self.tok().text)
            self.i += 1
            node = rest._call_rest(start, line)
            if not rest.at_end():
                raise Refused(f"a CALL at line {line} could not be read")
            return node
        return self._call_rest(start, line)

    def _call_rest(self, start, line):
        name = self.name()
        while self.eat("."):
            name = self.name()
        args = []
        if self.eat("("):
            while not self.eat(")"):
                args.append(self.src(self.scan(lambda x: False, comma=True)))
                self.eat(",")
        return Node("call", start, line, name=name, args=args)

    def _get(self, start, line):
        self.i += 1
        stacked = self.eat("STACKED")
        self.eat("CURRENT")
        self.need("DIAGNOSTICS")
        condition = None
        if self.eat("CONDITION"):
            condition = self.tok().text
            self.i += 1
        items = []
        while True:
            target = self._target()
            self.need("=")
            items.append((target, self.name().upper()))
            if not self.eat(","):
                break
        return Node("diagnostics", start, line, condition=condition,
                    items=items, stacked=stacked)

    def _start(self, start, line):
        text = self.src(self.until_semicolon())
        if not text.upper().startswith("START TRANSACTION"):
            return Node("unsupported", start, line, what="START", text=text)
        return Node("tx", start, line, what="start")

    def _commit(self, start, line):
        self.until_semicolon()
        return Node("tx", start, line, what="commit")

    def _rollback(self, start, line):
        text = self.src(self.until_semicolon())
        if " TO " in f" {text.upper()} ":
            return Node("unsupported", start, line, what="ROLLBACK TO",
                        text=text)
        return Node("tx", start, line, what="rollback")

    def _select(self, start, line):
        span = self.until_semicolon()
        text = self.src(span)
        into = self._into(span)
        if into is None:
            return Node("result", start, line, text=text)
        query, targets = into
        if targets is None:
            return Node("unsupported", start, line, what="SELECT INTO a file",
                        text=text)
        return Node("select_into", start, line, text=query, targets=targets)

    _with = _select

    def _into(self, span):
        """(the query without its INTO, [targets]), or None where it has no
        INTO; targets None for INTO OUTFILE / DUMPFILE."""
        depth = 0
        toks = [t for t in self.t if span[0] <= t.start < span[1]]
        for k, t in enumerate(toks):
            if t.token_type == TokenType.L_PAREN:
                depth += 1
            elif t.token_type == TokenType.R_PAREN:
                depth -= 1
            elif depth == 0 and t.token_type not in _QUOTED and \
                    t.text.upper() == "INTO":
                if k + 1 < len(toks) and toks[k + 1].text.upper() in (
                        "OUTFILE", "DUMPFILE"):
                    return ("", None)
                targets, j = [], k + 1
                while j < len(toks):
                    if toks[j].token_type == TokenType.PARAMETER and \
                            j + 1 < len(toks):
                        targets.append("@" + toks[j + 1].text)
                        j += 2
                    else:
                        targets.append(toks[j].text)
                        j += 1
                    if j < len(toks) and toks[j].token_type == \
                            TokenType.COMMA:
                        j += 1
                        continue
                    break
                cut_to = toks[j].start if j < len(toks) else span[1]
                query = (self.text[span[0]:t.start] + " "
                         + self.text[cut_to:span[1]]).strip()
                return (query, targets)
        return None

    def _dml(self, start, line):
        text = self.src(self.until_semicolon())
        return Node("sql", start, line, text=text,
                    verb=text.split(None, 1)[0].upper())

    _insert = _update = _delete = _replace = _truncate = _dml


def read(body):
    """The parsed body, or Refused."""
    return _Parser(body).body()


# ---- types -------------------------------------------------------------

_INT = {"tinyint": "smallint", "smallint": "smallint", "mediumint": "integer",
        "int": "integer", "integer": "integer", "bigint": "bigint"}
_UNSIGNED = {"tinyint": "smallint", "smallint": "integer",
             "mediumint": "integer", "int": "bigint", "integer": "bigint",
             "bigint": "numeric(20)"}


def pg_type(declared):
    """A MySQL declared type as the PostgreSQL type holding the same
    values as exactly as it can: an INT stays 32 bits, so an overflow
    fails where MySQL's does."""
    text = " ".join(str(declared).lower().split())
    text = re.sub(r"\s+(character set|charset|collate)\s+\S+", "", text)
    m = re.match(r"([a-z]+)\s*(\(([^)]*)\))?\s*(unsigned)?", text)
    if not m:
        raise Refused(f"its type {declared} could not be read")
    base, args, unsigned = m.group(1), m.group(3), bool(m.group(4))
    if base in ("bool", "boolean"):
        return "smallint"
    if base in _INT:
        return (_UNSIGNED if unsigned else _INT)[base]
    if base in ("decimal", "numeric", "dec", "fixed"):
        return f"numeric({args.replace(' ', '')})" if args \
            else "numeric(10,0)"
    if base == "float":
        return "real"
    if base in ("double", "real"):
        return "double precision"
    if base in ("char", "varchar"):
        return f"varchar({args})" if args else "varchar(1)"
    if base in ("text", "tinytext", "mediumtext", "longtext", "enum", "set"):
        return "text"
    if base in ("binary", "varbinary", "blob", "tinyblob", "mediumblob",
                "longblob"):
        return "bytea"
    if base == "date":
        return "date"
    if base in ("datetime", "timestamp"):
        return f"timestamp({args})" if args else "timestamp(0)"
    if base == "time":
        return f"time({args})" if args else "time(0)"
    if base == "year":
        return "smallint"
    if base == "json":
        return "jsonb"
    raise Refused(f"its type {declared} has no counterpart in migkit's"
                  " rules")


#: the integer types whose range PostgreSQL has no type for, and the
#: range MySQL holds them to: (signed, unsigned)
_RANGES = {"tinyint": ((-128, 127), (0, 255)),
           "smallint": (None, (0, 65535)),
           "mediumint": ((-8388608, 8388607), (0, 16777215)),
           "int": (None, (0, 4294967295)),
           "integer": (None, (0, 4294967295)),
           "bigint": (None, (0, 18446744073709551615))}


def int_range(declared):
    """(lowest, highest) a MySQL integer type holds where the PostgreSQL
    type it is carried in holds more, else None."""
    m = re.match(r"\s*([a-z]+)\s*(\([^)]*\))?\s*(unsigned)?",
                 str(declared).lower())
    base = {"bool": "tinyint", "boolean": "tinyint"}.get(
        m.group(1), m.group(1)) if m else None
    if base not in _RANGES:
        return None
    return _RANGES[base][1 if m.group(3) else 0]


def _strict(settings):
    mode = str((settings or {}).get("sql_mode") or "").upper()
    return "STRICT_TRANS_TABLES" in mode or "STRICT_ALL_TABLES" in mode


# ---- the conversion ----------------------------------------------------


class Context:
    """What a conversion needs to know of the move around it.

    `local` renames the tables a statement names to the target's names;
    `routines` are the source's, so a call names the target's routine as
    it was made there; `columns` and `tables` are the target's names by
    their lower case, so one made with capitals is quoted; `keys` are the
    target's unique keys per table, for `ON DUPLICATE KEY`."""

    def __init__(self, local=None, routines=(), columns=None, keys=None,
                 tables=None, table_columns=None):
        self.local = local
        self.routines = {r.name.lower(): r for r in routines}
        self.columns = columns or {}
        self.keys = keys or {}
        self.tables = tables or {}
        self.table_columns = table_columns or {}
        self._sends = {}

    def sends_rows(self, routine):
        """Whether a MySQL procedure sends result sets to its caller."""
        if routine.name not in self._sends:
            try:
                self._sends[routine.name] = any(
                    n.kind == "result" for n in read(routine.body or "")
                    .walk())
            except Refused:
                self._sends[routine.name] = False
        return self._sends[routine.name]


def _quoted(name):
    made = str(name)
    return exp.to_identifier(made, quoted=not (
        made == made.lower() and made.isidentifier()))


class _Frame:
    def __init__(self, label):
        self.label = label
        self.handlers = []       # [(kind, [conditions], body)]
        self.notfound = None     # (kind, body)


class _Gen:
    def __init__(self, routine, ctx):
        self.r = routine
        self.ctx = ctx or Context()
        self.e = Emitter()
        self.codes = []
        self.scopes = [{}]
        self.frames = []
        self.decls = [[]]
        self.stack = []
        self.user_vars = {}
        self.result_sets = []
        self.commits = False
        self.in_handler = 0
        self.serial = 0
        self.checks = []
        self.uses_row_count = bool(re.search(r"(?i)\brow_count\s*\(",
                                             routine.body or ""))
        self.strict = _strict(routine.settings)

    def code(self, what, line):
        if (what, line) not in self.codes:
            self.codes.append((what, line))

    def fresh(self, stem):
        self.serial += 1
        return f"_migkit_{stem}{self.serial}"

    # ---- names ---------------------------------------------------------

    def var(self, name):
        low = str(name).strip("`").lower()
        for scope in reversed(self.scopes):
            if low in scope:
                return scope[low]
        return None

    def condition(self, name):
        for scope in reversed(self.scopes):
            if ("condition", name) in scope:
                return scope[("condition", name)]
        return None

    def user_var(self, name, line):
        self.code("mysql.user-variable", line)
        return self.user_vars.setdefault(name.lower(),
                                         ident("_at_" + name.lower()))

    def target(self, text, line):
        """A SET/INTO/FETCH target as (its PL/pgSQL name, its type)."""
        text = text.strip()
        if text.startswith("@@"):
            raise Refused(f"line {line} sets a server variable, which the"
                          " target has no counterpart for")
        if text.startswith("@"):
            return self.user_var(text[1:].strip().strip("`"), line), "text"
        m = re.match(r"(?is)^(new|old)\s*\.\s*`?([^`]+?)`?$", text)
        if m:
            if self.r.kind != "trigger":
                raise Refused(f"line {line} assigns to {text} outside a"
                              " trigger")
            column = _quoted(self._column(m.group(2))).sql("postgres")
            return f"{m.group(1).lower()}.{column}", None
        got = self.var(text)
        if got is None or got[1] == "cursor":
            raise Refused(f"line {line} assigns to {text}, which is not a"
                          " variable or parameter")
        return got

    def _column(self, name):
        return self.ctx.columns.get(str(name).lower(), str(name))

    # ---- expressions and statements --------------------------------------

    def rewrite(self, tree, line):
        """The rules, on one statement's or expression's tree."""
        if self.ctx.local is not None:
            tree = self.ctx.local(tree)
        for table in list(tree.find_all(exp.Table)):
            made = self.ctx.tables.get(table.name.lower())
            if made:
                table.set("this", _quoted(made))
        for node in list(tree.walk()):
            if isinstance(node, exp.Parameter):
                pg = self.user_var(node.name, line)
                node.replace(exp.column(exp.to_identifier(
                    pg.strip('"'), quoted=pg.startswith('"'))))
            elif isinstance(node, exp.SessionParameter):
                raise Refused(f"line {line} reads a server variable"
                              f" (@@{node.name}), which the target has no"
                              " counterpart for")
        fix(tree, self.ctx, line, self.code)
        for col in list(tree.find_all(exp.Column)):
            if col.table:
                if col.table.lower() in ("new", "old"):
                    col.set("table", exp.to_identifier(col.table.lower()))
                    col.set("this", _quoted(self._column(col.name)))
                elif col.name.lower() in self.ctx.columns:
                    col.set("this", _quoted(self._column(col.name)))
                continue
            got = self.var(col.name)
            if got is not None and got[1] != "cursor":
                col.set("this", exp.to_identifier(
                    got[0].strip('"'), quoted=got[0].startswith('"')))
            elif col.name.lower() in self.ctx.columns:
                col.set("this", _quoted(self._column(col.name)))
        for node in list(tree.find_all(exp.Anonymous)):
            callee = self.ctx.routines.get(str(node.name).lower())
            if callee is not None:
                node.set("this", exp.to_identifier(callee.name, quoted=True))
            elif str(node.name).upper() == "ROW_COUNT":
                self.uses_row_count = True
                self.code("mysql.row-count", line)
                node.replace(exp.column("_migkit_row_count"))
            elif str(node.name).upper() == "FOUND_ROWS":
                raise Refused(f"line {line} reads FOUND_ROWS(), which the"
                              " target has no counterpart for")
        return tree

    def expr(self, text, line):
        tree = parse(f"SELECT {text}", "mysql")
        if not isinstance(tree, exp.Select) or len(tree.expressions) != 1 \
                or tree.args.get("from_"):
            raise Refused(f"the expression at line {line} could not be read")
        tree = self.rewrite(tree, line)
        return generate(tree, "mysql", "postgres",
                        normalize=False)[len("SELECT "):]

    def sql(self, text, line, into=None):
        tree = self.rewrite(parse(text, "mysql"), line)
        if into is not None:
            if not isinstance(tree, exp.Select):
                raise Refused(f"line {line} selects INTO from a statement"
                              " migkit's rules cannot read")
            tree.set("into", exp.Into(this=exp.to_table("_migkit_into_")))
        out = generate(tree, "mysql", "postgres", normalize=False)
        if into is not None:
            out = out.replace("INTO _migkit_into_",
                              f"INTO STRICT {', '.join(into)}", 1)
        return out

    # ---- the routine -----------------------------------------------------

    def convert(self):
        r = self.r
        tree = read(r.body or "")
        scope = self.scopes[0]
        entry = []
        for p in r.params if r.kind in ("function", "procedure") else ():
            typ = pg_type(p.type)
            name = ident(p.name)
            scope[p.name.lower()] = (name, typ)
            if p.mode in ("in", "inout") and "(" in typ and \
                    typ.startswith(("numeric", "varchar")):
                # the declared length and scale, which PostgreSQL ignores
                # on a parameter, applied where MySQL applies them: on the
                # way in
                if typ.startswith("varchar") and self.strict:
                    check = self.fresh("len")
                    self.checks.append((check, typ))
                    entry.append(f"{check} := {name};")
                else:
                    entry.append(f"{name} := cast({name} as {typ});")
            span = int_range(p.type)
            if p.mode in ("in", "inout") and span:
                # a TINYINT is held in a SMALLINT here: its own range, as
                # MySQL checks it on the way in - refused under a strict
                # sql_mode, cut to the nearest end outside one
                lo, hi = span
                if self.strict:
                    entry.append(
                        f"if {name} < {lo} or {name} > {hi} then raise"
                        " exception using errcode = '22003', message ="
                        f" 'Out of range value for column ''{p.name}'' at"
                        " row 1'; end if;")
                else:
                    entry.append(f"{name} := greatest({lo}, least({hi},"
                                 f" {name}));")
        body = Emitter()
        body.push()
        for line in entry:
            body(line)
        self.e = body
        if tree.kind == "block":
            self.block(tree)
        else:
            self.block(Node("block", 0, 1, label=None, variables=[],
                            cursors=[], handlers=[], conditions={},
                            body=[tree]))
        for line in self.tail():
            body(line)
        head = [f"{n} text;" for n in sorted(set(self.user_vars.values()))]
        if self.uses_row_count:
            head.append("_migkit_row_count bigint := 0;")
        head += [f"{n} {t};" for n, t in self.checks]
        text = "\n".join(["#variable_conflict use_variable"]
                         + (["declare"] + ["  " + h for h in head]
                            if head else [])
                         + ["begin"] + body.lines + ["end"])
        if QUOTE in text:
            raise Refused("its body holds migkit's own quoting mark")
        return text

    def tail(self):
        r = self.r
        if r.kind != "trigger":
            return []
        if r.timing == "AFTER":
            return ["return null;"]
        return ["return old;" if r.events == ("DELETE",) else "return new;"]

    # ---- blocks and handlers ----------------------------------------------

    def block(self, node):
        e = self.e
        label = ident(node.label) if node.label else self.fresh("b")
        scope = {}
        self.scopes.append(scope)
        decls = []
        self.decls.append(decls)
        for cname, value in node.conditions.items():
            scope[("condition", cname)] = value
        for name, typ, default, line in node.variables:
            pg = pg_type(typ)
            scope[name.lower()] = (ident(name), pg)
            value = f" := {self._value(default, pg, line)}" \
                if default is not None else ""
            decls.append(f"{ident(name)} {pg}{value};")
        for cname, query, line in node.cursors:
            self.code("mysql.cursor", line)
            decls.append(f"{ident(cname)} cursor for"
                         f" {self.sql(query, line)};")
            scope[cname.lower()] = (ident(cname), "cursor")
        frame = _Frame(label)
        for kind, conds, hbody, line in node.handlers:
            if kind == "UNDO":
                raise Refused(f"line {line} declares an UNDO handler, which"
                              " MySQL itself does not run")
            caught = []
            for c in conds:
                if c[0] == "name":
                    got = self.condition(c[1])
                    if got is None:
                        raise Refused(f"line {line} handles {c[1]}, not"
                                      " declared before it")
                    c = got
                if c in (("class", "not found"), ("sqlstate", "02000"),
                         ("errno", 1329)):
                    frame.notfound = (kind, hbody)
                    self.code(f"mysql.handler.{kind.lower()}.not-found", line)
                elif c == ("class", "warning"):
                    # a warning is not an error on the target: this
                    # handler never fires there, which the proof tells
                    self.code("mysql.handler.sqlwarning", line)
                else:
                    caught.append(self._condition(c, line))
                    self.code(f"mysql.handler.{kind.lower()}", line)
            if caught:
                # the most specific first, as MySQL picks among them
                caught.sort(key=lambda x: x == "others")
                frame.handlers.append((kind, caught, hbody))
        self.frames.append(frame)
        body = Emitter()
        body.depth = e.depth + 1
        self.e = body
        for stmt in node.body:
            self.statement(stmt)
        if not node.body:
            body("null;")
        self.e = e
        self.frames.pop()
        self.decls.pop()
        self.scopes.pop()
        e(f"<<{label}>>")
        if decls:
            e("declare")
            e.push()
            for d in decls:
                e(d)
            e.pop()
        e("begin")
        e.lines.extend(body.lines)
        e(f"end {label};")

    def _condition(self, c, line):
        kind, value = c
        if kind == "class":
            return "others"
        if kind == "errno":
            got = MYSQL_ERRNO.get(value)
            if not got:
                raise Refused(f"line {line} handles MySQL error {value},"
                              " which migkit's rules have no counterpart"
                              " for")
            return got
        if value in MYSQL_SQLSTATE:
            got = MYSQL_SQLSTATE[value]
            if not got:
                raise Refused(f"line {line} handles SQLSTATE {value},"
                              " which names no one failure")
            return got
        if not re.fullmatch(r"[0-9A-Z]{5}", value):
            raise Refused(f"line {line} handles SQLSTATE {value!r}")
        return f"sqlstate '{value}'"

    def _active(self):
        """[(frame index, handler)] catching a failure here, innermost
        block first."""
        return [(i, h) for i in range(len(self.frames) - 1, -1, -1)
                for h in self.frames[i].handlers]

    def _notfound(self):
        for i in range(len(self.frames) - 1, -1, -1):
            if self.frames[i].notfound:
                return i, self.frames[i].notfound
        return None

    def run_handler(self, index, kind, hbody):
        """A handler's body, under the handlers of the blocks outside the
        one that declared it (MySQL's rule), then - for an EXIT handler -
        leaving that block."""
        label = self.frames[index].label
        frames, self.frames = self.frames, self.frames[:index]
        self.in_handler += 1
        try:
            self.statement(hbody)
        finally:
            self.in_handler -= 1
            self.frames = frames
        if kind == "EXIT":
            self.e(f"exit {label};")

    def guarded(self, lines):
        """`lines` under the handlers in force: a block of their own,
        which rolls back only this statement when it fails."""
        e = self.e
        active = self._active()
        if not active:
            for line in lines:
                e(line)
            return
        e("begin")
        e.push()
        for line in lines:
            e(line)
        e.pop()
        e("exception")
        e.push()
        for index, (kind, conds, hbody) in active:
            e(f"when {' or '.join(conds)} then")
            e.push()
            self.run_handler(index, kind, hbody)
            e.pop()
        e.pop()
        e("end;")

    # ---- statements --------------------------------------------------------

    def statement(self, node):
        self.stack.append(node.kind)
        try:
            getattr(self, "s_" + node.kind)(node)
        finally:
            self.stack.pop()

    def s_block(self, node):
        self.block(node)

    def s_unsupported(self, node):
        what = node.what.upper()
        code = "mysql.dynamic-sql" if what in ("PREPARE", "EXECUTE",
                                              "DEALLOCATE") \
            else f"mysql.{what.lower().replace(' ', '-')}"
        self.code(code, node.line)
        raise Refused(f"line {node.line}: {node.text[:60]} - migkit's rules"
                      " have no counterpart for it")

    def _value(self, text, typ, line):
        value = self.expr(text, line)
        if typ and typ.startswith("varchar") and not self.strict:
            # outside a strict sql_mode MySQL cuts what does not fit
            return f"cast({value} as {typ})"
        return value

    def s_set(self, node):
        lines = []
        for target, value in node.pairs:
            name, typ = self.target(target, node.line)
            lines.append(f"{name} := {self._value(value, typ, node.line)};")
        self.guarded(lines)

    def s_return(self, node):
        value = self.expr(node.value, node.line)
        ret = pg_type(self.r.returns) if self.r.returns else None
        if ret and ret.startswith(("numeric(", "varchar(")):
            # the declared scale and length, which PostgreSQL ignores on a
            # function's result, applied where MySQL applies them
            value = f"cast({value} as {ret})"
        span = int_range(self.r.returns) if self.r.returns else None
        if span:
            # the declared integer range, which the wider type holding it
            # here does not keep
            held = self.fresh("r")
            self.decls[-1].append(f"{held} {ret};")
            lo, hi = span
            check = (f"if {held} < {lo} or {held} > {hi} then raise"
                     " exception using errcode = '22003', message = 'Out of"
                     " range value'; end if;") if self.strict else \
                f"{held} := greatest({lo}, least({hi}, {held}));"
            self.guarded([f"{held} := {value};", check,
                          f"return {held};"])
            return
        self.guarded([f"return {value};"])

    def _guarded_test(self, text):
        """Whether a condition must be evaluated under the handlers first:
        it reads a table and a handler is in force, so a failure in it is
        caught where MySQL catches it - and the statement testing it is
        then skipped, as a CONTINUE handler skips it."""
        return bool(self._active()) and bool(
            re.search(r"(?i)\bselect\b", text))

    def _guard(self, text):
        value, ok = self.fresh("c"), self.fresh("ok")
        self.decls[-1] += [f"{value} boolean;", f"{ok} boolean;"]
        self.e(f"{ok} := false;")
        self.guarded([f"{value} := ({text});", f"{ok} := true;"])
        return value, ok

    def s_if(self, node):
        e = self.e
        closes = 0
        for k, (cond, stmts, _, line) in enumerate(node.branches):
            text = self.expr(cond, line)
            if self._guarded_test(text):
                if k:
                    e("else")
                    e.push()
                    closes += 1
                value, ok = self._guard(text)
                e(f"if {ok} then")
                e.push()
                closes += 1
                e(f"if {value} then")
            else:
                e(f"{'elsif' if k else 'if'} {text} then")
            self._list(stmts)
        if node.otherwise is not None:
            e("else")
            self._list(node.otherwise)
        e("end if;")
        for _ in range(closes):
            e.pop()
            e("end if;")

    def _list(self, stmts):
        e = self.e
        e.push()
        if not stmts:
            e("null;")
        for s in stmts:
            self.statement(s)
        e.pop()

    def _plain(self, cond, line, what):
        text = self.expr(cond, line)
        if self._guarded_test(text):
            self.code("mysql.handler.condition-reads", line)
            raise Refused(f"line {line}: {what} tests a query under a"
                          " handler, which migkit's rules have no"
                          " counterpart for")
        return text

    def s_case(self, node):
        e = self.e
        subject = self._plain(node.subject, node.line, "CASE") \
            if node.subject else None
        e(f"case {subject}" if subject else "case")
        e.push()
        for value, stmts, _, line in node.whens:
            e(f"when {self._plain(value, line, 'CASE')} then")
            self._list(stmts)
        if node.otherwise is not None:
            e("else")
            self._list(node.otherwise)
        e.pop()
        e("end case;")

    def _loop_head(self, node, what):
        return f"<<{ident(node.label)}>> {what}" if node.label else what

    def _loop_end(self, node):
        return f"end loop{' ' + ident(node.label) if node.label else ''};"

    def s_while(self, node):
        e = self.e
        e(self._loop_head(node, f"while"
                          f" {self._plain(node.cond, node.line, 'WHILE')}"
                          " loop"))
        self._list(node.body)
        e(self._loop_end(node))

    def s_repeat(self, node):
        e = self.e
        e(self._loop_head(node, "loop"))
        self._list(node.body)
        e.push()
        e(f"exit when {self._plain(node.cond, node.line, 'UNTIL')};")
        e.pop()
        e(self._loop_end(node))

    def s_loop(self, node):
        e = self.e
        e(self._loop_head(node, "loop"))
        self._list(node.body)
        e(self._loop_end(node))

    def s_leave(self, node):
        self.e(f"exit {ident(node.label)};")

    def s_iterate(self, node):
        self.e(f"continue {ident(node.label)};")

    def _cursor(self, node):
        got = self.var(node.cursor)
        if got is None or got[1] != "cursor":
            raise Refused(f"line {node.line} names a cursor not declared"
                          " before it")
        return got[0]

    def s_open(self, node):
        self.guarded([f"open {self._cursor(node)};"])

    def s_close(self, node):
        self.guarded([f"close {self._cursor(node)};"])

    def s_fetch(self, node):
        # into variables of its own: PL/pgSQL sets its targets to NULL when
        # no row comes, where MySQL leaves them as they were
        cursor = self._cursor(node)
        shadows, copies = [], []
        for t in node.targets:
            name, typ = self.target(t, node.line)
            shadow = self.fresh("f")
            self.decls[-1].append(f"{shadow} {typ or 'text'};")
            shadows.append(shadow)
            copies.append(f"{name} := {shadow};")
        self.guarded([f"fetch {cursor} into {', '.join(shadows)};"])
        e = self.e
        e("if found then")
        e.push()
        for c in copies:
            e(c)
        e.pop()
        nf = self._notfound()
        if nf:
            index, (kind, hbody) = nf
            e("else")
            e.push()
            self.run_handler(index, kind, hbody)
            e.pop()
        e("end if;")

    def s_select_into(self, node):
        targets = [self.target(t, node.line)[0] for t in node.targets]
        query = self.sql(node.text, node.line, into=targets)
        self.code("mysql.select-into", node.line)
        e = self.e
        outer = e.depth
        inner = Emitter()
        inner.depth = outer
        self.e = inner
        inner("begin")
        inner.push()
        inner(f"{query};")
        inner.pop()
        inner("exception")
        inner.push()
        inner("when no_data_found then")
        inner.push()
        nf = self._notfound()
        if nf:
            index, (kind, hbody) = nf
            self.run_handler(index, kind, hbody)
        else:
            inner("null;")
        inner.pop()
        inner("when too_many_rows then")
        inner.push()
        inner("raise exception using errcode = '42000', message ="
              " 'Result consisted of more than one row';")
        inner.pop()
        inner.pop()
        inner("end;")
        self.e = e
        self.guarded([line[2 * outer:] for line in inner.lines])

    def s_result(self, node):
        if self.r.kind != "procedure":
            raise Refused(f"line {node.line} sends a result set, which only"
                          " a procedure can")
        if any(k in ("while", "repeat", "loop") for k in self.stack[:-1]):
            raise Refused(f"line {node.line} sends a result set from inside"
                          " a loop: how many there are is not fixed")
        self.code("mysql.result-set", node.line)
        name = f"_migkit_rs{len(self.result_sets) + 1}"
        self.result_sets.append(name)
        self.guarded([f"open {name} for {self.sql(node.text, node.line)};"])

    def s_sql(self, node):
        if node.verb == "REPLACE":
            text = self._replace(node)
        else:
            text = self.sql(node.text, node.line)
        lines = [f"{text};"]
        if self.uses_row_count:
            lines.append("get diagnostics _migkit_row_count = row_count;")
        self.guarded(lines)

    def _replace(self, node):
        """REPLACE as an insert that updates the row its one unique key
        names. The columns it does not give go back to their defaults, as
        a REPLACE's deleted and re-inserted row has them."""
        tree = parse("INSERT" + node.text[len("REPLACE"):], "mysql")
        if not isinstance(tree, exp.Insert):
            raise Refused(f"line {node.line}: REPLACE could not be read")
        tree.set("conflict", exp.OnConflict(duplicate=True, expressions=[]))
        self.code("mysql.replace", node.line)
        tree = self.rewrite(tree, node.line)
        return generate(tree, "mysql", "postgres", normalize=False)

    def s_call(self, node):
        callee = self.ctx.routines.get(node.name.lower())
        if callee is None or callee.kind != "procedure":
            raise Refused(f"line {node.line} calls {node.name}, which is not"
                          " one of the source's procedures")
        if self.ctx.sends_rows(callee):
            raise Refused(f"line {node.line} calls {node.name}, whose result"
                          " sets MySQL passes on to this caller's caller")
        args = []
        for k, text in enumerate(node.args):
            mode = callee.params[k].mode if k < len(callee.params) else "in"
            args.append(self.target(text, node.line)[0] if mode != "in"
                        else self.expr(text, node.line))
        self.code("mysql.call", node.line)
        name = '"' + callee.name.replace('"', '""') + '"'
        self.guarded([f"call {name}({', '.join(args)});"])

    def s_signal(self, node):
        self.code("mysql.signal", node.line)
        state = None
        if node.cond is not None:
            c = node.cond
            if c[0] == "name":
                got = self.condition(c[1])
                if got is None:
                    raise Refused(f"line {node.line} signals {c[1]}, not"
                                  " declared before it")
                c = got
            if c[0] != "sqlstate":
                raise Refused(f"line {node.line} signals a condition named"
                              " for an error number, which MySQL refuses")
            state = c[1]
        message = node.items.get("MESSAGE_TEXT")
        text = self.expr(message, node.line) if message else None
        if state and state.startswith("01"):
            default = "'Unhandled user-defined warning condition'"
            self.e(f"raise warning '%', {text or default};")
            return
        if state and state.startswith("02"):
            raise Refused(f"line {node.line} signals NOT FOUND, which"
                          " migkit's rules have no counterpart for")
        default = "'Unhandled user-defined exception condition'"
        using = ([f"errcode = '{state}'"] if state else []) + \
            [f"message = {text or default}"]
        self.guarded([f"raise exception using {', '.join(using)};"])

    def s_resignal(self, node):
        if not self.in_handler or node.cond or node.items:
            raise Refused(f"line {node.line}: RESIGNAL outside a handler, or"
                          " with a condition of its own, has no counterpart"
                          " in migkit's rules")
        self.e("raise;")

    def s_diagnostics(self, node):
        if node.condition is not None:
            if not self.in_handler:
                raise Refused(f"line {node.line} reads a condition outside a"
                              " handler")
            items = {"RETURNED_SQLSTATE": "returned_sqlstate",
                     "MESSAGE_TEXT": "message_text"}
            got = []
            for target, item in node.items:
                if item not in items:
                    raise Refused(f"line {node.line} reads {item}, which the"
                                  " target does not keep")
                got.append(f"{self.target(target, node.line)[0]} ="
                           f" {items[item]}")
            self.e(f"get stacked diagnostics {', '.join(got)};")
            return
        got = []
        for target, item in node.items:
            if item != "ROW_COUNT":
                raise Refused(f"line {node.line} reads {item}, which the"
                              " target does not keep")
            got.append(f"{self.target(target, node.line)[0]} = row_count")
        self.e(f"get diagnostics {', '.join(got)};")

    def s_tx(self, node):
        if self.r.kind not in ("procedure", "event"):
            raise Refused(f"line {node.line} ends a transaction, which only a"
                          " procedure can")
        if self._active() or self.in_handler:
            raise Refused(f"line {node.line} ends a transaction under a"
                          " handler; PostgreSQL cannot commit inside a block"
                          " that catches errors")
        self.commits = True
        self.code("mysql.transaction", node.line)
        # START TRANSACTION commits what came before it, as COMMIT does
        self.e("rollback;" if node.what == "rollback" else "commit;")


def fix(tree, ctx, line, code):
    """MySQL's meaning kept where PostgreSQL's function of the same name
    means something else - in a query, an expression or a statement.
    Children first: a rule that rebuilds a node copies children already
    carried."""
    for node in reversed(list(tree.walk())):
        clock = _clock(node, line)
        if clock is not None:
            code("mysql.clock", line)
            node.replace(clock)
        elif isinstance(node, exp.Length) and node.args.get("binary"):
            code("mysql.length-bytes", line)
            node.replace(exp.Anonymous(this="octet_length",
                                       expressions=[node.this.copy()]))
        elif isinstance(node, (exp.Concat, exp.DPipe)):
            parts = node.expressions if isinstance(node, exp.Concat) \
                else [node.this, node.expression]
            for p in list(parts):
                if not (isinstance(p, exp.Literal) and p.is_string) and \
                        not isinstance(p, (exp.Cast, exp.DPipe, exp.Concat)):
                    p.replace(exp.cast(p.copy(), exp.DataType.build("text")))
        elif isinstance(node, exp.Cast) and not isinstance(node, exp.TryCast):
            to = node.args.get("to")
            if to is not None and to.this == exp.DataType.Type.CHAR and \
                    not to.expressions:
                node.set("to", exp.DataType.build("text"))
            elif to is not None and to.this == exp.DataType.Type.UBIGINT:
                node.set("to", exp.DataType.build("numeric(20)"))
        elif isinstance(node, exp.Div):
            # MySQL divides as decimals and answers NULL for a zero
            # divisor, whatever the translator would make of the division
            node.replace(exp.Div(
                this=exp.cast(node.this.copy(), exp.DataType.build("numeric")),
                expression=exp.Nullif(this=node.expression.copy(),
                                      expression=exp.Literal.number(0)),
                typed=False, safe=False))
        elif isinstance(node, exp.GroupConcat):
            code("mysql.group-concat", line)
            node.set("this", exp.cast(node.this.copy(),
                                      exp.DataType.build("text")))
        elif isinstance(node, exp.Anonymous) and \
                str(node.name).upper() == "LAST_INSERT_ID" and \
                not node.expressions:
            code("mysql.last-insert-id", line)
            node.replace(exp.Anonymous(this="lastval", expressions=[]))
        elif isinstance(node, exp.Insert) and isinstance(
                node.args.get("conflict"), exp.OnConflict) and \
                node.args["conflict"].args.get("duplicate"):
            _on_duplicate(node, ctx, line, code)
        elif isinstance(node, (exp.Delete, exp.Update)) and \
                node.args.get("limit") is not None:
            _limited(node, line, code)
    return tree


#: MySQL's clock functions, by the instant each answers from in
#: PL/pgSQL: the transaction's (as MySQL's statement time is, and as the
#: proof sets both sessions to one), the wall clock's, or UTC's
_CLOCK_AT = {"now": "localtimestamp", "localtime": "localtimestamp",
             "localtimestamp": "localtimestamp",
             "current_timestamp": "localtimestamp",
             "sysdate": "cast(clock_timestamp() as timestamp)",
             "utc_timestamp": "(now() at time zone 'UTC')"}
_CLOCK_NODES = {exp.CurrentTimestamp: "current_timestamp",
                exp.Localtime: "localtime",
                exp.Localtimestamp: "localtimestamp",
                exp.UtcTimestamp: "utc_timestamp",
                exp.CurrentTime: "curtime", exp.UtcTime: "utc_time",
                exp.UtcDate: "utc_date"}
#: the fractional digits a clock is cut to, as PostgreSQL's `date_trunc`
#: names them
_CUT = {0: "second", 3: "milliseconds", 6: None}


def _clock(node, line):
    """A MySQL clock function as PL/pgSQL answering the same value from
    the same instant, or None where `node` is none. MySQL's `NOW()` is
    the statement's time cut (not rounded) to its digits, none by
    default; PostgreSQL's `now()` keeps every microsecond and
    `localtimestamp(0)` rounds - so each is cut as MySQL cuts it.
    `UNIX_TIMESTAMP()` is whole seconds. A clock read to a number of
    digits `date_trunc` has no unit for is refused."""
    from .translate import parse
    name = _CLOCK_NODES.get(type(node))
    args = []
    if name is None and isinstance(node, exp.Anonymous):
        name = str(node.name).lower()
        args = list(node.expressions)
    elif name is not None and node.args.get("this") is not None:
        args = [node.args["this"]]
    if name not in _CLOCK_AT and name not in ("curtime", "utc_time",
                                              "utc_date", "unix_timestamp"):
        return None
    if name == "unix_timestamp":
        if args:
            return None
        return parse("cast(floor(extract(epoch from now())) as bigint)",
                     "postgres")
    if name == "utc_date":
        return parse("cast((now() at time zone 'UTC') as date)", "postgres")
    digits = 0
    if args:
        if not (isinstance(args[0], exp.Literal) and
                str(args[0].this).isdigit()):
            raise Refused(f"line {line} reads the clock to a number of"
                          " digits that is not written in it")
        digits = int(args[0].this)
    if digits not in _CUT:
        raise Refused(f"line {line} reads the clock to {digits} digits,"
                      " which migkit's rules cut only to 0, 3 or 6")
    at = _CLOCK_AT.get(name) or (
        "(now() at time zone 'UTC')" if name == "utc_time"
        else "localtimestamp")
    cut = f"date_trunc('{_CUT[digits]}', {at})" if _CUT[digits] else at
    if name in ("curtime", "utc_time"):
        cut = f"cast({cut} as time)"
    return parse(cut, "postgres")


def _on_duplicate(node, ctx, line, code):
    """`ON DUPLICATE KEY UPDATE` as `ON CONFLICT` on the one unique key
    MySQL would have hit - refused where the table has several, since
    which one a row hits first is MySQL's to choose."""
    table = node.find(exp.Table)
    name = table.name if table is not None else ""
    keys = ctx.keys.get(name) or ctx.keys.get(name.lower()) or []
    if len(keys) != 1:
        raise Refused(f"line {line} updates on a duplicate key, and the"
                      f" target's {name} has {len(keys)} unique keys, not"
                      " one")
    code("mysql.on-duplicate-key", line)
    conflict = node.args["conflict"]
    sets = []

    def excluded(node):
        # VALUES(col): the value the row would have had, which
        # PostgreSQL calls excluded.col
        if isinstance(node, exp.Anonymous) and \
                str(node.name).upper() == "VALUES" and node.expressions:
            return exp.column(node.expressions[0].name, table="excluded")
        return node
    for eq in conflict.expressions or []:
        sets.append(exp.EQ(this=exp.column(eq.this.name),
                           expression=eq.expression.transform(excluded)))
    if not conflict.expressions:
        cols = node.this.expressions if isinstance(node.this, exp.Schema) \
            else []
        given = {c.name for c in cols}
        sets = [exp.EQ(this=exp.column(c.name),
                       expression=exp.column(c.name, table="excluded"))
                for c in cols if c.name not in keys[0]]
        # what a REPLACE does not give goes back to its default, as the
        # row it deletes and inserts again has it
        sets += [exp.EQ(this=exp.column(c), expression=exp.var("DEFAULT"))
                 for c in ctx.table_columns.get(name.lower(), [])
                 if c not in given and c not in keys[0]]
    keys_ = [exp.Ordered(this=exp.column(k)) for k in keys[0]]
    node.set("conflict", exp.OnConflict(
        expressions=sets, action=exp.var("DO UPDATE"),
        conflict_keys=keys_) if sets else exp.OnConflict(
        action=exp.var("DO NOTHING"), conflict_keys=keys_))


def _limited(node, line, code):
    """`DELETE ... LIMIT n` and `UPDATE ... LIMIT n` touch n rows, chosen
    by the statement's own ORDER BY where it has one (and, like MySQL's,
    by nothing in particular where it has none)."""
    code("mysql.limited-write", line)
    table = node.this
    inner = exp.select("ctid").from_(table.copy())
    for part in ("where", "order"):
        if node.args.get(part) is not None:
            inner.set(part, node.args[part].copy())
    inner.set("limit", node.args["limit"].copy())
    node.set("where", exp.Where(this=exp.In(
        this=exp.column("ctid"), query=exp.Subquery(this=inner))))
    node.set("limit", None)
    node.set("order", None)


def convert(routine, ctx=None):
    """`routine` (a MySQL one) as PostgreSQL statements, or Refused."""
    g = _Gen(routine, ctx)
    body = g.convert()
    r = routine
    name = '"' + r.name.replace('"', '""') + '"'
    params = [f"{'' if p.mode == 'in' else p.mode + ' '}{ident(p.name)}"
              f" {pg_type(p.type)}" for p in r.params]
    quoted = f"{QUOTE}\n{body}\n{QUOTE}"
    residue = []
    if r.kind == "function":
        stmts = [f"create function {name}({', '.join(params)}) returns"
                 f" {pg_type(r.returns)} language plpgsql as {quoted}"]
    elif r.kind in ("procedure", "event"):
        params = (params if r.kind == "procedure" else []) + [
            f"inout {n} refcursor default null" for n in g.result_sets]
        stmts = [f"create procedure {name}({', '.join(params)}) language"
                 f" plpgsql as {quoted}"]
        if r.kind == "event":
            g.code("mysql.event", 1)
            residue.append(_schedule_said(r))
    elif r.kind == "trigger":
        table = exp.to_table(r.table)
        if g.ctx.local is not None:
            table = g.ctx.local(exp.select("*").from_(table)).find(exp.Table)
        made = g.ctx.tables.get(table.name.lower())
        if made:
            table.set("this", _quoted(made))
        g.code("mysql.trigger", 1)
        stmts = [f"create function {name}() returns trigger language plpgsql"
                 f" as {quoted}",
                 f"create trigger {name} {r.timing.lower()}"
                 f" {' or '.join(e.lower() for e in r.events)} on"
                 f" {table.sql('postgres')} for each row execute function"
                 f" {name}()"]
    else:
        raise Refused(f"a {r.kind} is not something migkit's rules convert")
    return Converted(stmts, constructs=g.codes, result_sets=g.result_sets,
                     commits=g.commits, residue=residue)


def _schedule_said(r):
    s = r.schedule or {}
    when = f"once at {s['at']}" if s.get("at") else \
        f"every {s.get('every', '?').lower()}" + (
            f" from {s['starts']}" if s.get("starts") else "") + (
            f" until {s['ends']}" if s.get("ends") else "")
    return (f"event {r.name} runs {when}"
            f" ({str(s.get('status', '')).lower()}); the target has no"
            f" scheduler of its own - its body is the procedure {r.name},"
            " to be run on that schedule by the target's scheduler or by"
            " hand")


# ---- coverage of the MySQL side ------------------------------------------

#: the table the probes write to, in the sandbox's copy of the database
COVER_TABLE = "migkit_cover"


#: what a routine can read of the statement before it, which a probe's
#: insert would change: {what it reads: (the variable it is kept in,
#: its type)}
_KEPT = {"row_count": ("_migkit_rc", "bigint"),
         "found_rows": ("_migkit_fr", "bigint"),
         "number": ("_migkit_dn", "int")}
#: a condition's items, typed as the variable each is kept in
_ITEM_TYPES = {"MYSQL_ERRNO": "int"}
#: the statements after which what the routine reads is kept: every one
#: that runs and goes on to the next
_LEAVES = ("set", "open", "close", "fetch", "call", "diagnostics", "tx",
           "result", "select_into", "sql", "unsupported")


def _reads(body, tree):
    """(what `body` reads of the statement before it - "row_count",
    "found_rows", "number", ("condition", k, item) -, the spans of its
    `ROW_COUNT()` and `FOUND_ROWS()` calls, the diagnostics statements
    reading the current area), or None where it reads something no
    variable can keep: a condition by a number held in a variable, or an
    item of the area itself other than its count and its number."""
    toks = _Parser(body).t
    what, calls = set(), []
    for k, t in enumerate(toks):
        word = t.text.lower() if t.token_type not in _QUOTED else ""
        if word in ("row_count", "found_rows") and k + 2 < len(toks) and \
                toks[k + 1].token_type == TokenType.L_PAREN and \
                toks[k + 2].token_type == TokenType.R_PAREN:
            what.add(word)
            calls.append((t.start, toks[k + 2].end + 1, word))
    current = []
    for node in tree.walk():
        if node.kind != "diagnostics" or node.stacked:
            # the stacked area is the copy made as its handler was
            # entered, which no statement inside the handler changes
            continue
        if node.condition is not None:
            if not str(node.condition).isdigit():
                return None
            for _, item in node.items:
                what.add(("condition", int(node.condition), item))
        else:
            for _, item in node.items:
                if item not in ("ROW_COUNT", "NUMBER"):
                    return None
                what.add(item.lower())
        current.append(node)
    return what, calls, current


def _kept_name(item):
    if isinstance(item, tuple):
        return f"_migkit_c{item[1]}_{item[2].lower()}"
    return _KEPT[item][0]


def _from_area(what):
    """Whether the count is kept from the diagnostics area rather than
    from `ROW_COUNT()`: where the routine reads the area at all, one
    reading keeps both, so keeping one cannot move the other."""
    return any(isinstance(x, tuple) or x == "number" for x in what)


def _keep(what):
    """The statements keeping what the routine reads of the statement just
    run: the diagnostics area read first (reading it clears nothing),
    then the conditions it holds, then `ROW_COUNT()` and `FOUND_ROWS()`
    in one SET, so no keeping moves another."""
    out = []
    conds = sorted(x for x in what if isinstance(x, tuple))
    if _from_area(what):
        items = ["_migkit_dn = NUMBER"]
        if "row_count" in what:
            items.append("_migkit_rc = ROW_COUNT")
        out.append("get diagnostics " + ", ".join(items))
        for k in sorted({c[1] for c in conds}):
            items = ", ".join(f"{_kept_name(c)} = {c[2]}" for c in conds
                              if c[1] == k)
            out.append(f"if _migkit_dn >= {k} then get diagnostics"
                       f" condition {k} {items}; end if")
    plain = [x for x in ("row_count", "found_rows") if x in what
             and not (x == "row_count" and _from_area(what))]
    if plain:
        out.append("set " + ", ".join(f"{_KEPT[x][0]} = {x}()"
                                      for x in plain))
    return "; ".join(out)


def instrument(routine):
    """(the statement making an instrumented copy of `routine`, [(probe,
    line, what)], the name the copy is made under - None where it takes
    the routine's own place), or None where no copy is made.

    A probe is an insert, and an insert changes what `ROW_COUNT()`,
    `FOUND_ROWS()` and the diagnostics area say of the statement before
    it. So a routine reading them gets a copy under another name, beside
    itself: every statement the copy runs is followed by one keeping what
    it reads in variables of its own, and it reads those - a probe between
    the statement and the read changes nothing read. The routine itself
    stays as it is and its calls are the answers compared; each call is
    made of the copy too, and the copy's coverage counts only while it
    answers every call as the routine does (`prove`). A trigger fires by
    itself and cannot be called beside a copy: one that reads them is not
    instrumented, and its coverage is said not measured."""
    body = routine.body or ""
    definition = routine.definition or ""
    at = definition.rfind(body) if body else -1
    if at < 0:
        return None
    try:
        tree = read(body)
    except Refused:
        return None
    got = _reads(body, tree)
    if got is None:
        return None
    what, calls, current = got
    twin = None
    if what:
        if routine.kind == "trigger":
            return None
        twin = f"migkit_cover_{routine.name}"[:64]
    probes, edits = [], []
    who = routine.name.replace("'", "''")
    keep = _keep(what) if what else ""

    def insert(n):
        return f" insert into {COVER_TABLE} values ('{who}', {n}); "

    def edit(lo, hi, text, order):
        # at one place, the lower `order` ends up first
        edits.append((lo, hi, text, order))

    def probe(pos, line, what_):
        probes.append((len(probes) + 1, line, what_))
        edit(pos, pos, insert(len(probes)), 1)

    def at_list(stmts, fallback, line, what_):
        probe(stmts[0].start if stmts else fallback, line, what_)

    for node in tree.walk():
        if node.kind == "block":
            at_list(node.body, node.end_at, node.line, "block")
            for _, _, hbody, hline in node.handlers:
                probes.append((len(probes) + 1, hline, "handler"))
                # a condition read in the handler is kept as the handler
                # is entered, before its probe clears the area
                entry = f" {keep};" if keep else ""
                edit(hbody.start, hbody.start,
                     f" begin{entry}{insert(len(probes))}", 1)
                edit(hbody.end, hbody.end, "; end ", 2)
        elif node.kind == "if":
            for _, stmts, pos, line in node.branches:
                at_list(stmts, pos, line, "then")
            if node.otherwise is not None:
                at_list(node.otherwise, node.else_at, node.line, "else")
            else:
                probes.append((len(probes) + 1, node.line, "else (none)"))
                edit(node.end_at, node.end_at,
                     f" else{insert(len(probes))}", 1)
        elif node.kind == "case":
            for _, stmts, pos, line in node.whens:
                at_list(stmts, pos, line, "when")
            if node.otherwise is not None:
                at_list(node.otherwise, node.else_at, node.line, "else")
        elif node.kind in ("while", "repeat", "loop"):
            at_list(node.body, node.body_at, node.line, node.kind)
        if keep and node.kind in _LEAVES:
            edit(node.end, node.end, f"; {keep}", 0)
    declared = ""
    if keep:
        for lo, hi, word in calls:
            edit(lo, hi, _KEPT[word][0], 9)
        for node in current:
            sets = ", ".join(
                f"{_target_text(t)} = " + _kept_name(
                    ("condition", int(node.condition), item)
                    if node.condition is not None else item.lower())
                for t, item in node.items)
            edit(node.start, node.end, f"set {sets}", 9)
        names = {_kept_name(x): (_KEPT[x][1] if not isinstance(x, tuple)
                                 else _ITEM_TYPES.get(x[2],
                                                      "varchar(512)"))
                 for x in what}
        if _from_area(what):
            names.setdefault("_migkit_dn", "int")
        declared = " ".join(f"declare {n} {t};"
                            for n, t in sorted(names.items()))
        if tree.kind == "block":
            if body[tree.start:tree.start + 5].lower() != "begin":
                return None
            # the variables first in the outermost block, as MySQL wants
            # them before its cursors and handlers; what the routine would
            # read before its first statement kept before its first probe
            edit(tree.start + 5, tree.start + 5, f" {declared}", 5)
            first = tree.body[0].start if tree.body else tree.end_at
            edit(first, first, f" {keep};", 0)
    text = body
    for lo, hi, what_, _ in sorted(edits, key=lambda x: (-x[0], -x[3])):
        text = text[:lo] + what_ + text[hi:]
    if tree.kind != "block":
        # one statement for a body: a block around it, entered each call
        probes.insert(0, (0, tree.line, "body"))
        first = f" {declared} {keep};" if keep else ""
        text = f"begin{first}{insert(0)}{text}; end"
    made = definition[:at] + text + definition[at + len(body):]
    if twin is not None:
        made = re.sub(r"(?is)^(\s*create\s+(?:definer\s*=\s*\S+\s+)?"
                      r"(?:function|procedure)\s+)(`(?:[^`]|``)+`|[\w$]+)",
                      lambda m: m.group(1) + "`" + twin.replace("`", "``")
                      + "`", made, count=1)
    return made, probes, twin


def _target_text(target):
    """A GET DIAGNOSTICS target as a SET writes it."""
    return target if str(target).startswith("@") else f"`{target}`"

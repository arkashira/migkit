"""SQL Server's T-SQL procedures, functions and triggers in PostgreSQL's
PL/pgSQL, by migkit's own rules.

T-SQL ends a statement where the next one begins as often as at a
semicolon, so its bodies are read here statement by statement - an `IF`
has no THEN, a `SELECT` after an `INSERT` belongs to it, one after a
`UNION` too - and each SQL statement inside goes to the query translator
with the rules below on top. What has no rule is refused by name.

The rules that keep meaning, not only syntax:

* `BEGIN TRY ... BEGIN CATCH`: a failed statement in the TRY block is
  rolled back and the ones before it keep their writes; a PL/pgSQL
  exception block would roll them all back. So each statement in a TRY is
  its own block, which on a failure records the error and leaves the TRY;
  the CATCH runs after, `ERROR_MESSAGE()` reads the recorded message and
  `THROW;` raises the recorded error again;
* a variable is `@name`, apart from every column; PL/pgSQL has one
  namespace. So variables become `v_name` and parameters `p_name`, and the
  body resolves a bare name to the column, as T-SQL does;
* `SELECT @v = col FROM ...` leaves `@v` as it was when no row comes;
  PL/pgSQL's `SELECT ... INTO` would set it to NULL. So it selects into
  variables of its own and copies only a row that came;
* `+` between strings joins them (NULL where either is NULL, as
  `CONCAT_NULL_YIELDS_NULL` has it); `LEN` does not count trailing
  spaces; `DELETE TOP (n)` / `UPDATE TOP (n)` touch n rows;
* a temporary table `#t` is the procedure's own and is gone when it
  returns; a PostgreSQL temporary table lives to the end of the session,
  so it is dropped where the procedure returns, and before it is made;
* a trigger reads the statement's rows as the pseudo-tables `inserted`
  and `deleted`; a PostgreSQL statement trigger reads them as transition
  tables of those names - one trigger per event, since a transition
  table exists only for the events that have it;
* `OUTPUT` hands back the rows a write wrote: into a table variable, a
  temporary table or a table, it is the write's `RETURNING` inserted
  there in one statement (`WITH ... INSERT`); to the caller, it is a
  result set read from a temporary table the write's rows are put in
  (a cursor cannot be opened over a write). `deleted.` in an UPDATE's or
  a MERGE's, and `inserted.` in a MERGE's, are the row before and after,
  which PostgreSQL returns from version 18 (`old.`, `new.`), and
  `$action` is `merge_action()` from 17 - refused on an older target,
  by its version;
* `MERGE` runs on PostgreSQL from version 15, and `WHEN NOT MATCHED BY
  SOURCE` from 17 - refused on an older target; a row of the target met
  by two of the source's fails on both (SQL Server's 8672, PostgreSQL's
  cardinality violation);
* the procedure-wide transaction - `BEGIN TRY BEGIN TRAN ... COMMIT END
  TRY BEGIN CATCH IF @@TRANCOUNT > 0 ROLLBACK ... END CATCH` - is one
  block with an exception handler: the handler's rollback of the block's
  writes is the ROLLBACK, the rest of the CATCH runs in the handler, and
  no COMMIT is written, so a caller's transaction keeps what SQL Server's
  nested COMMIT leaves to it. Refused where the CATCH writes and then
  raises (SQL Server keeps those writes, the target's raise takes them
  back), or the procedure writes before the transaction and the CATCH
  raises;
* a table variable keeps its rows through a ROLLBACK; the temporary table
  it becomes does not - a routine with both is refused.
"""
import re

from sqlglot import exp
from sqlglot.dialects.tsql import TSQL
from sqlglot.tokens import TokenType

from .plpgsql import QUOTE, Converted, Emitter, ident
from .translate import Refused, generate, parse

class _Tokens(TSQL.Tokenizer):
    """T-SQL's tokens with an END read as END wherever it stands: the
    translator's own tokenizer reads an END after a semicolon as a command
    and keeps what follows it as one piece of text, so `...; END TRY
    BEGIN CATCH ...` was one string and its body could not be read."""
    COMMANDS = TSQL.Tokenizer.COMMANDS - {TokenType.END}


def _tokenize(text):
    return _Tokens(dialect=TSQL()).tokenize(text)


_QUOTED = {getattr(TokenType, n) for n in (
    "STRING", "IDENTIFIER", "NUMBER", "NATIONAL_STRING", "HEX_STRING",
    "BIT_STRING", "BYTE_STRING", "RAW_STRING", "UNICODE_STRING")
    if hasattr(TokenType, n)}

#: words a statement starts with; one of them at depth 0 ends the
#: statement before it, but for the continuations `_continues` names
STARTERS = {"SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "SET",
            "DECLARE", "IF", "ELSE", "WHILE", "BEGIN", "END", "RETURN",
            "EXEC", "EXECUTE", "PRINT", "RAISERROR", "THROW", "OPEN",
            "FETCH", "CLOSE", "DEALLOCATE", "BREAK", "CONTINUE", "GOTO",
            "WITH", "TRUNCATE", "COMMIT", "ROLLBACK", "SAVE", "WAITFOR",
            "CREATE", "DROP", "ALTER"}


def _continues(head, prev, word, seen=()):
    """Whether `word` at depth 0 belongs to the statement begun by
    `head` rather than starting the next one. `seen` holds the words met
    at depth 0 so far: an UPDATE takes one SET, and a second is the next
    statement."""
    if word == "SELECT" and (head in ("INSERT", "WITH", "DECLARE")
                             or prev in ("UNION", "ALL", "EXCEPT",
                                         "INTERSECT", "FOR")):
        return True
    if word == "SET" and (head == "UPDATE" and "SET" not in seen
                          or prev == "UPDATE"):
        return True
    if word in ("INSERT", "UPDATE", "DELETE") and head == "MERGE":
        return True
    if word in ("INSERT", "UPDATE", "DELETE", "MERGE", "SELECT") and \
            head == "WITH" and prev == ")":
        return True
    if word in ("EXEC", "EXECUTE") and head == "INSERT":
        return True
    if word == "DELETE" and prev == "ON":
        return True
    return False


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
        for k in ("body", "then", "otherwise", "try_", "catch"):
            v = self.a.get(k)
            if isinstance(v, list):
                out += v
            elif isinstance(v, Node):
                out.append(v)
        return out

    def walk(self):
        yield self
        for c in self.children():
            yield from c.walk()


class _Parser:
    def __init__(self, text):
        self.text = text
        try:
            self.t = _tokenize(text)
        except Exception as e:  # noqa: BLE001 - said as a refusal
            raise Refused(f"its body could not be read: {str(e)[:80]}")
        self.i = 0

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
        if t is None:
            raise Refused("its body could not be read: a name was expected")
        self.i += 1
        return t.text

    def var(self):
        """`@name` -> name."""
        if not self.is_(TokenType.PARAMETER):
            raise Refused(f"a variable was expected at line {self.line()}")
        self.i += 1
        return self.name()

    def _end(self, start):
        return self.t[self.i - 1].end + 1 if self.i > 0 else start

    def span(self, head, stop=None):
        """The tokens of one simple statement or expression: up to a
        statement starter at depth 0 (or `stop(word)`), a semicolon, or
        the end. (start, end) as character offsets."""
        start = self.tok().start if self.tok() else len(self.text)
        depth, case, prev = 0, 0, head
        first = True
        seen = set()
        while not self.at_end():
            t = self.tok()
            word = self.w()
            if t.token_type == TokenType.L_PAREN:
                depth += 1
            elif t.token_type == TokenType.R_PAREN:
                if depth == 0:
                    break
                depth -= 1
            elif depth == 0 and word == "CASE":
                case += 1
            elif depth == 0 and word == "END" and case:
                case -= 1
                prev = word
                self.i += 1
                first = False
                continue
            if depth == 0 and case == 0 and not first:
                if t.token_type == TokenType.SEMICOLON:
                    break
                if stop is not None and stop(word):
                    break
                if word in STARTERS and not _continues(head, prev, word,
                                                       seen):
                    break
            if depth == 0 and t.token_type == TokenType.SEMICOLON:
                break
            if depth == 0 and word:
                seen.add(word)
            prev = word if word else (")" if t.token_type ==
                                      TokenType.R_PAREN else prev)
            first = False
            self.i += 1
        return start, max(start, self._end(start))

    def src(self, span):
        return self.text[span[0]:span[1]].strip()

    # ---- statements -------------------------------------------------------

    def statements(self, until=("END",)):
        out = []
        while True:
            while self.is_(TokenType.SEMICOLON):
                self.i += 1
            if self.at_end() or self.w() in until:
                return out
            out.append(self.statement())

    def statement(self):
        while self.is_(TokenType.SEMICOLON):
            self.i += 1
        line, w = self.line(), self.w()
        if self.is_(TokenType.COLON, 1) and w and w not in STARTERS:
            raise Refused(f"line {line} is a label for GOTO, which"
                          " migkit's rules have no counterpart for")
        if w == "BEGIN":
            if self.w(1) == "TRY":
                return self._try(line)
            if self.w(1) in ("TRAN", "TRANSACTION", "DISTRIBUTED"):
                self.i += 2
                if self.tok() is not None and self.w() not in STARTERS and \
                        not self.is_(TokenType.SEMICOLON):
                    self.i += 1
                return Node("tx", line, what="begin")
            self.i += 1
            body = self.statements()
            self.need("END")
            return Node("block", line, body=body)
        if w == "IF":
            self.i += 1
            cond = self.src(self.span("IF"))
            then = self.statement()
            otherwise = None
            while self.is_(TokenType.SEMICOLON):
                self.i += 1
            if self.eat("ELSE"):
                otherwise = self.statement()
            return Node("if", line, cond=cond, then=then, otherwise=otherwise)
        if w == "WHILE":
            self.i += 1
            cond = self.src(self.span("WHILE"))
            return Node("while", line, cond=cond, body=[self.statement()])
        if w == "DECLARE":
            return self._declare(line)
        if w == "SET":
            return self._set(line)
        if w in ("SELECT", "WITH") or self.is_(TokenType.L_PAREN):
            text = self.src(self.span(w or "SELECT"))
            return Node("select", line, text=text)
        if w in ("INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE"):
            text = self.src(self.span(w))
            return Node("sql", line, text=text, verb=w)
        if w == "RETURN":
            self.i += 1
            if self.at_end() or self.w() in STARTERS or \
                    self.is_(TokenType.SEMICOLON):
                return Node("return", line, value=None)
            return Node("return", line, value=self.src(self.span("RETURN")))
        if w in ("EXEC", "EXECUTE"):
            self.i += 1
            return self._exec(line)
        if w == "PRINT":
            self.i += 1
            return Node("print", line, value=self.src(self.span("PRINT")))
        if w == "RAISERROR":
            self.i += 1
            text = self.src(self.span("RAISERROR"))
            return Node("raiserror", line, text=text)
        if w == "THROW":
            self.i += 1
            if self.at_end() or self.w() in STARTERS or \
                    self.is_(TokenType.SEMICOLON):
                return Node("throw", line, args=None)
            return Node("throw", line,
                        args=self.src(self.span("THROW")))
        if w in ("OPEN", "CLOSE", "DEALLOCATE"):
            self.i += 1
            self.eat("GLOBAL")
            return Node(w.lower(), line, cursor=self.name())
        if w == "FETCH":
            return self._fetch(line)
        if w in ("BREAK", "CONTINUE"):
            self.i += 1
            return Node(w.lower(), line)
        if w in ("COMMIT", "ROLLBACK"):
            self.i += 1
            if self.w() in ("TRAN", "TRANSACTION", "WORK"):
                self.i += 1
                if self.tok() is not None and self.w() not in STARTERS \
                        and not self.is_(TokenType.SEMICOLON):
                    raise Refused(f"line {line} commits or rolls back to a"
                                  " named transaction or savepoint")
            return Node("tx", line, what=w.lower())
        if w == "CREATE" and self.w(1) == "TABLE" and self.is_(
                TokenType.HASH, 2):
            text = self.src(self.span("CREATE"))
            return Node("temp_create", line, text=text)
        if w == "DROP" and self.w(1) == "TABLE":
            text = self.src(self.span("DROP"))
            return Node("temp_drop", line, text=text)
        text = self.src(self.span(w or "?"))
        return Node("unsupported", line, what=w or "?", text=text)

    def _try(self, line):
        self.need("BEGIN", "TRY")
        body = self.statements(until=("END",))
        self.need("END", "TRY")
        while self.is_(TokenType.SEMICOLON):
            self.i += 1
        self.need("BEGIN", "CATCH")
        catch = self.statements(until=("END",))
        self.need("END", "CATCH")
        return Node("try", line, try_=body, catch=catch)

    def _declare(self, line):
        self.i += 1
        out = []
        while True:
            if self.is_(TokenType.PARAMETER):
                name = self.var()
                self.eat("AS")
                if self.w() == "TABLE":
                    self.i += 1
                    cols = self.src(self._parens())
                    out.append(("table", name, cols, None))
                elif self.w() == "CURSOR":
                    raise Refused(f"line {line} declares a cursor variable")
                else:
                    typ = self.src(self._type())
                    default = None
                    if self.is_(TokenType.EQ):
                        self.i += 1
                        default = self.src(self._value())
                    out.append(("var", name, typ, default))
            else:
                cname = self.name()
                self.eat("INSENSITIVE")
                self.eat("SCROLL")
                self.need("CURSOR")
                while self.w() in ("LOCAL", "GLOBAL", "FORWARD_ONLY",
                                   "SCROLL", "STATIC", "KEYSET", "DYNAMIC",
                                   "FAST_FORWARD", "READ_ONLY",
                                   "SCROLL_LOCKS", "OPTIMISTIC",
                                   "TYPE_WARNING"):
                    self.i += 1
                self.need("FOR")
                query = self.src(self.span("DECLARE"))
                if query.upper().endswith(" FOR READ ONLY"):
                    query = query[:-len(" FOR READ ONLY")]
                out.append(("cursor", cname, query, None))
            if not self.eat(","):
                break
        return Node("declare", line, items=out)

    def _parens(self):
        if not self.is_(TokenType.L_PAREN):
            raise Refused(f"a list in parentheses was expected at line"
                          f" {self.line()}")
        start = self.tok().start
        depth = 0
        while not self.at_end():
            if self.is_(TokenType.L_PAREN):
                depth += 1
            elif self.is_(TokenType.R_PAREN):
                depth -= 1
                if depth == 0:
                    self.i += 1
                    break
            self.i += 1
        return start, self._end(start)

    def _type(self):
        start = self.tok().start
        self.i += 1
        while self.is_(TokenType.DOT):
            self.i += 2
        if self.is_(TokenType.L_PAREN):
            self._parens()
        return start, self._end(start)

    def _value(self):
        """An expression that ends at a comma, a statement starter or a
        semicolon, at depth 0."""
        start = self.tok().start if self.tok() else len(self.text)
        depth, case = 0, 0
        first = True
        while not self.at_end():
            t, word = self.tok(), self.w()
            if t.token_type == TokenType.L_PAREN:
                depth += 1
            elif t.token_type == TokenType.R_PAREN:
                if depth == 0:
                    break
                depth -= 1
            elif depth == 0 and word == "CASE":
                case += 1
            elif depth == 0 and word == "END" and case:
                case -= 1
                self.i += 1
                first = False
                continue
            if depth == 0 and case == 0 and (
                    t.token_type in (TokenType.COMMA, TokenType.SEMICOLON)
                    or (not first and word in STARTERS)):
                break
            first = False
            self.i += 1
        return start, max(start, self._end(start))

    def _set(self, line):
        self.i += 1
        if not self.is_(TokenType.PARAMETER):
            option = self.src(self.span("SET"))
            return Node("option", line, text=option)
        name = self.var()
        t = self.tok()
        op = "="
        if t is not None and t.token_type != TokenType.EQ:
            op = t.text
            self.i += 1
            if not self.is_(TokenType.EQ):
                raise Refused(f"line {line}: SET @{name} {op} could not be"
                              " read")
            op = op + "="
        self.i += 1
        value = self.src(self.span("SET"))
        return Node("set", line, name=name, op=op, value=value)

    def _exec(self, line):
        if self.is_(TokenType.STRING):
            # what follows EXEC at a statement's start the tokenizer keeps
            # as one piece of text; it is read again, as its own tokens
            rest = _Parser(self.tok().text)
            self.i += 1
            return rest._exec(line)
        if self.is_(TokenType.L_PAREN):
            raise Refused(f"line {line} runs SQL built at run time")
        status = None
        if self.is_(TokenType.PARAMETER) and self.is_(TokenType.EQ, 2):
            status = self.var()
            self.i += 1
        name = self.name()
        while self.is_(TokenType.DOT):
            self.i += 1
            name = self.name()
        if name.lower() in ("sp_executesql",):
            raise Refused(f"line {line} runs SQL built at run time")
        args = []
        while not self.at_end() and not self.is_(TokenType.SEMICOLON) and \
                not (self.w() in STARTERS):
            named = None
            if self.is_(TokenType.PARAMETER) and self.is_(TokenType.EQ, 2):
                named = self.var()
                self.i += 1
            value = self.src(self._value())
            out = False
            if value.upper().endswith(" OUTPUT") or value.upper().endswith(
                    " OUT"):
                value = value.rsplit(None, 1)[0]
                out = True
            args.append((named, value, out))
            if not self.eat(","):
                break
        return Node("exec", line, name=name, args=args, status=status)

    def _fetch(self, line):
        self.i += 1
        if self.is_(TokenType.STRING):
            rest = _Parser(self.tok().text)
            self.i += 1
            return rest._fetch_rest(line)
        return self._fetch_rest(line)

    def _fetch_rest(self, line):
        if self.w() in ("NEXT", "PRIOR", "FIRST", "LAST"):
            if self.w() != "NEXT":
                raise Refused(f"line {line} fetches other than the next row")
            self.i += 1
        self.eat("FROM")
        self.eat("GLOBAL")
        cursor = self.name()
        targets = []
        if self.eat("INTO"):
            targets.append(self.var())
            while self.eat(","):
                targets.append(self.var())
        return Node("fetch", line, cursor=cursor, targets=targets)


def body_of(definition, kind):
    """(the text after the header's AS, the trigger's table and events)."""
    p = _Parser(definition)
    toks = p.t
    depth, k = 0, 0
    after_var = False
    table, events, timing = None, [], None
    for k, t in enumerate(toks):
        word = None if t.token_type in _QUOTED else t.text.upper()
        if t.token_type == TokenType.L_PAREN:
            depth += 1
        elif t.token_type == TokenType.R_PAREN:
            depth -= 1
        elif depth == 0 and kind == "trigger":
            if word == "ON" and table is None:
                j, parts = k + 1, []
                while j < len(toks):
                    parts.append(toks[j].text)
                    if j + 1 < len(toks) and toks[j + 1].token_type == \
                            TokenType.DOT:
                        j += 2
                        continue
                    break
                table = parts[-1]
            elif word in ("FOR", "AFTER"):
                timing = "AFTER"
            elif word == "INSTEAD":
                timing = "INSTEAD OF"
            elif word in ("INSERT", "UPDATE", "DELETE") and timing:
                events.append(word)
        if depth == 0 and word == "AS" and not after_var:
            if kind != "trigger" or (timing and events):
                return definition[t.end + 1:].strip(), (table, timing,
                                                        tuple(events))
        after_var = t.token_type == TokenType.VAR and k > 0 and \
            toks[k - 1].token_type == TokenType.PARAMETER
    raise Refused("its header could not be read")


# ---- types ------------------------------------------------------------------


def pg_type(declared):
    text = " ".join(str(declared).lower().split())
    m = re.match(r"(\w+)\s*(\(([^)]*)\))?", text)
    if not m:
        raise Refused(f"its type {declared} could not be read")
    base, args = m.group(1), (m.group(3) or "").replace(" ", "")
    if base in ("int", "integer"):
        return "integer"
    if base in ("bigint", "smallint"):
        return base
    if base == "tinyint":
        return "smallint"
    if base == "bit":
        return "boolean"
    if base in ("decimal", "numeric"):
        return f"numeric({args})" if args else "numeric(18,0)"
    if base == "money":
        return "numeric(19,4)"
    if base == "smallmoney":
        return "numeric(10,4)"
    if base == "float":
        return "real" if args and args.isdigit() and int(args) <= 24 \
            else "double precision"
    if base == "real":
        return "real"
    if base in ("char", "nchar"):
        return f"char({args or 1})"
    if base in ("varchar", "nvarchar"):
        return "text" if args in ("max", "") else f"varchar({args})"
    if base in ("text", "ntext"):
        return "text"
    if base in ("binary", "varbinary", "image"):
        return "bytea"
    if base == "date":
        return "date"
    if base == "datetime":
        return "timestamp(3)"
    if base == "smalldatetime":
        return "timestamp(0)"
    if base == "datetime2":
        return f"timestamp({args or 6})" if not args or int(args) <= 6 \
            else "timestamp(6)"
    if base == "time":
        return f"time({min(int(args or 6), 6)})"
    if base == "datetimeoffset":
        return "timestamptz"
    if base == "uniqueidentifier":
        return "uuid"
    if base == "xml":
        return "xml"
    raise Refused(f"its type {declared} has no counterpart in migkit's"
                  " rules")


_STRING_TYPES = ("varchar", "char", "text")


class _Gen:
    def __init__(self, routine, ctx):
        self.r = routine
        self.ctx = ctx
        self.e = Emitter()
        self.codes = []
        self.vars = {}             # lower name -> (pg name, pg type)
        self.decls = []
        self.cursors = []
        self.temps = set()
        #: the TRYs around, innermost last: (label, err, state, msg)
        self.frames = []
        self.catching = []         # CATCH blocks we are inside
        self.result_sets = []
        self.serial = 0
        self.commits = False
        self.uses_rowcount = False
        self.uses_fetch = False
        self.event = None          # a trigger's event, per version
        #: whether a table variable is declared (it outlives a ROLLBACK)
        self.tablevars = False

    def code(self, what, line):
        if (what, line) not in self.codes:
            self.codes.append((what, line))

    def fresh(self, stem):
        self.serial += 1
        return f"_migkit_{stem}{self.serial}"

    def declare_var(self, name, typ):
        pg = ident("v_" + name)
        self.vars[name.lower()] = (pg, typ)
        self.decls.append(f"{pg} {typ};")
        return pg

    def lookup(self, name, line):
        got = self.vars.get(name.lower())
        if got is None:
            raise Refused(f"line {line} reads @{name}, not declared before"
                          " it")
        return got

    # ---- expressions -------------------------------------------------------

    def rewrite(self, tree, line):
        if self.ctx is not None and self.ctx.local is not None:
            tree = self.ctx.local(tree)
        for node in reversed(list(tree.walk())):
            if isinstance(node, exp.Parameter) and isinstance(
                    node.parent, exp.Parameter):
                # the name inside `@@ROWCOUNT`, read with its outer `@`
                continue
            if isinstance(node, exp.Parameter):
                inner = node.this
                if isinstance(inner, exp.Parameter):
                    self._session(inner.name, node, line)
                    continue
                pg, _ = self.lookup(node.name, line)
                node.replace(exp.column(exp.to_identifier(
                    pg.strip('"'), quoted=pg.startswith('"'))))
            elif isinstance(node, exp.Table) and (
                    node.name.startswith("#") or node.args.get(
                        "temporary") or str(node.this).startswith("#")):
                name = self._temp(node.name.lstrip("#"))
                node.set("this", exp.to_identifier(name))
            elif isinstance(node, exp.Table) and self.ctx is not None and \
                    node.name.lower() in (self.ctx.tables or {}):
                made = self.ctx.tables[node.name.lower()]
                node.set("this", exp.to_identifier(
                    made, quoted=not (made == made.lower()
                                      and made.isidentifier())))
        for node in reversed(list(tree.walk())):
            if isinstance(node, exp.Length):
                # LEN does not count trailing spaces
                node.set("this", exp.Anonymous(this="rtrim", expressions=[
                    node.this.copy()]))
            elif isinstance(node, exp.Add) and (
                    self._stringy(node.this) or
                    self._stringy(node.expression)):
                node.replace(exp.DPipe(this=node.this.copy(),
                                       expression=node.expression.copy()))
            elif isinstance(node, exp.Anonymous) and str(node.name).upper() \
                    in ("SCOPE_IDENTITY", "IDENT_CURRENT"):
                self.code("tsql.scope-identity", line)
                node.replace(exp.Anonymous(this="lastval", expressions=[]))
            elif isinstance(node, exp.Anonymous) and str(node.name).upper() \
                    == "ERROR_MESSAGE":
                if not self.catching:
                    raise Refused(f"line {line} reads ERROR_MESSAGE()"
                                  " outside a CATCH")
                node.replace(exp.column(self.catching[-1][3]))
            elif isinstance(node, exp.Anonymous) and str(node.name).upper() \
                    in ("ERROR_NUMBER", "ERROR_SEVERITY", "ERROR_STATE",
                        "ERROR_LINE", "ERROR_PROCEDURE", "XACT_STATE",
                        "@@ERROR"):
                raise Refused(f"line {line} reads {node.name}(), which the"
                              " target does not keep")
            elif isinstance(node, exp.Anonymous) and self.ctx is not None \
                    and str(node.name).lower() in self.ctx.routines:
                callee = self.ctx.routines[str(node.name).lower()]
                node.set("this", exp.to_identifier(callee.name, quoted=True))
        return tree

    def _session(self, name, node, line):
        up = name.upper()
        if up == "ROWCOUNT":
            self.uses_rowcount = True
            self.code("tsql.rowcount", line)
            node.replace(exp.column("_migkit_rowcount"))
        elif up == "FETCH_STATUS":
            self.uses_fetch = True
            node.replace(exp.column("_migkit_fetch_status"))
        elif up in ("IDENTITY",):
            self.code("tsql.identity", line)
            node.replace(exp.Anonymous(this="lastval", expressions=[]))
        elif up == "TRANCOUNT":
            raise Refused(f"line {line} reads @@TRANCOUNT")
        else:
            raise Refused(f"line {line} reads @@{name}, which the target"
                          " does not keep")

    def _stringy(self, node):
        if isinstance(node, exp.Literal):
            return node.is_string
        if isinstance(node, exp.Parameter) and not isinstance(
                node.this, exp.Parameter):
            got = self.vars.get(node.name.lower())
            return bool(got and got[1].startswith(_STRING_TYPES))
        if isinstance(node, exp.Column) and not node.table:
            for pg, typ in self.vars.values():
                if pg.strip('"') == node.name:
                    return typ.startswith(_STRING_TYPES)
        if isinstance(node, (exp.Upper, exp.Lower, exp.Substring, exp.Trim,
                             exp.DPipe, exp.Concat)):
            return True
        if isinstance(node, exp.Anonymous) and str(node.name).upper() in (
                "LEFT", "RIGHT", "LTRIM", "RTRIM", "REPLACE", "STUFF",
                "REPLICATE", "NCHAR", "CHAR", "SPACE", "FORMAT"):
            return True
        if isinstance(node, exp.Cast):
            return node.to.this in (exp.DataType.Type.VARCHAR,
                                    exp.DataType.Type.NVARCHAR,
                                    exp.DataType.Type.CHAR,
                                    exp.DataType.Type.NCHAR,
                                    exp.DataType.Type.TEXT)
        if isinstance(node, exp.Add):
            return self._stringy(node.this) or self._stringy(node.expression)
        return False

    def _temp(self, name):
        pg = f"_migkit_tmp_{name.lower()}"
        self.temps.add(pg)
        return pg

    def expr(self, text, line):
        tree = parse(f"SELECT {text}", "tsql")
        if not isinstance(tree, exp.Select) or len(tree.expressions) != 1 \
                or tree.args.get("from_"):
            raise Refused(f"the expression at line {line} could not be read")
        tree = self.rewrite(tree, line)
        return generate(tree, "tsql", "postgres",
                        normalize=False)[len("SELECT "):]

    def sql(self, text, line):
        top = re.match(r"(?is)^\s*(delete|update)\s+top\s*\(\s*(\d+)\s*\)"
                       r"\s*(.*)$", text)
        limit = None
        if top:
            self.code("tsql.top-write", line)
            text = f"{top.group(1)} {top.group(3)}"
            limit = int(top.group(2))
        tree = self.rewrite(parse(text, "tsql"), line)
        if limit is not None:
            from .mysql_pg import _limited
            tree.set("limit", exp.Limit(expression=exp.Literal.number(limit)))
            _limited(tree, line, lambda c, ln: None)
        return generate(tree, "tsql", "postgres", normalize=False)

    # ---- the routine --------------------------------------------------------

    def convert(self):
        r = self.r
        body, (table, timing, events) = body_of(r.definition or "", r.kind)
        self.table, self.timing, self.events = table, timing, events
        for p in r.params:
            typ = pg_type(p.type)
            self.vars[p.name.lstrip("@").lower()] = (
                ident("p_" + p.name.lstrip("@")), typ)
        parser = _Parser(body)
        stmts = parser.statements(until=())
        if not parser.at_end():
            raise Refused("its body could not be read to its end")
        self.tree = stmts
        return stmts

    def emit(self, stmts):
        out = Emitter()
        out.push()
        self.e = out
        for p in self.r.params:
            typ = self.vars[p.name.lstrip("@").lower()][1]
            name = self.vars[p.name.lstrip("@").lower()][0]
            if p.mode != "out" and typ.startswith(("numeric(", "varchar(",
                                                    "char(")):
                # the declared scale and length, applied on the way in as
                # SQL Server applies them
                out(f"{name} := cast({name} as {typ});")
        for s in stmts:
            self.statement(s)
        for t in sorted(self.temps):
            out(f"drop table if exists {t};")
        if self.r.kind == "trigger":
            out("return null;")
        head = ["#variable_conflict use_column"]
        decls = list(self.decls) + list(self.cursors)
        if self.uses_rowcount:
            decls.append("_migkit_rowcount bigint := 0;")
        if self.uses_fetch:
            decls.append("_migkit_fetch_status integer := -1;")
        if decls:
            head += ["declare"] + ["  " + d for d in decls]
        text = "\n".join(head + ["begin"] + out.lines + ["end"])
        if QUOTE in text:
            raise Refused("its body holds migkit's own quoting mark")
        return text

    # ---- statements ---------------------------------------------------------

    def statement(self, node):
        getattr(self, "s_" + node.kind)(node)

    def guarded(self, lines, line=0):
        """`lines` under the innermost TRY: a block of their own which, on
        a failure, records it and leaves the TRY."""
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

    def after_dml(self, lines):
        if self.uses_rowcount or re.search(r"@@rowcount", self.r.definition
                                           or "", re.I):
            self.uses_rowcount = True
            lines.append("get diagnostics _migkit_rowcount = row_count;")
        return lines

    def s_block(self, node):
        for s in node.body:
            self.statement(s)

    def s_unsupported(self, node):
        code = {"GOTO": "tsql.goto", "WAITFOR": "tsql.waitfor",
                "SAVE": "tsql.savepoint"}.get(node.what,
                                              f"tsql.{node.what.lower()}")
        self.code(code, node.line)
        raise Refused(f"line {node.line}: {node.text[:60]} - migkit's rules"
                      " have no counterpart for it")

    def s_option(self, node):
        text = node.text.upper()
        if text.startswith(("NOCOUNT", "ANSI_", "QUOTED_IDENTIFIER",
                            "ARITHABORT", "CONCAT_NULL_YIELDS_NULL")):
            return
        if text.startswith("XACT_ABORT"):
            self.code("tsql.xact-abort", node.line)
            return
        raise Refused(f"line {node.line} sets {node.text[:40]}, which"
                      " migkit's rules have no counterpart for")

    def s_declare(self, node):
        for kind, name, typ, default in node.items:
            if kind == "var":
                pg = self.declare_var(name, pg_type(typ))
                if default is not None:
                    self.guarded([f"{pg} := {self.expr(default, node.line)};"])
            elif kind == "table":
                self.code("tsql.table-variable", node.line)
                self.tablevars = True
                if re.search(r"(?i)\brollback\b", self.r.definition or ""):
                    raise Refused(f"line {node.line} declares a table"
                                  " variable, which keeps its rows through a"
                                  " ROLLBACK the routine makes; the target's"
                                  " temporary table does not")
                t = self._temp("tv_" + name)
                self.vars[name.lower()] = (t, "table")
                cols = parse(f"CREATE TABLE x {typ}", "tsql")
                made = generate(cols, "tsql", "postgres").replace(
                    "CREATE TABLE x", f"CREATE TEMPORARY TABLE {t}", 1)
                self.e(f"drop table if exists {t};")
                self.e(f"{made};")
            else:
                self.code("tsql.cursor", node.line)
                self.cursors.append(f"{ident(name)} cursor for"
                                    f" {self.sql(typ, node.line)};")
                self.vars["cursor:" + name.lower()] = (ident(name), "cursor")

    def s_set(self, node):
        pg, typ = self.lookup(node.name, node.line)
        value = self.expr(node.value, node.line)
        if node.op != "=":
            op = node.op[:-1]
            if op == "+" and typ.startswith(_STRING_TYPES):
                value = f"{pg} || ({value})"
            else:
                value = f"{pg} {op} ({value})"
        self.guarded([f"{pg} := {value};"])

    def s_select(self, node):
        tree = parse(node.text, "tsql")
        if isinstance(tree, exp.Select) and tree.expressions and all(
                isinstance(e, exp.EQ) and isinstance(e.this, exp.Parameter)
                for e in tree.expressions):
            return self._assign(tree, node)
        if isinstance(tree, exp.Select) and tree.args.get("into"):
            into = tree.args["into"]
            name = into.find(exp.Table).name.lstrip("#")
            tree.set("into", None)
            t = self._temp(name)
            text = generate(self.rewrite(tree, node.line), "tsql",
                            "postgres", normalize=False)
            self.guarded([f"drop table if exists {t};",
                          f"create temporary table {t} as {text};"])
            return
        if self.r.kind != "procedure":
            raise Refused(f"line {node.line} sends a result set, which only"
                          " a procedure can")
        self.code("tsql.result-set", node.line)
        name = f"_migkit_rs{len(self.result_sets) + 1}"
        self.result_sets.append(name)
        self.guarded([f"open {name} for {self.sql(node.text, node.line)};"])

    def _assign(self, tree, node):
        """`SELECT @a = x, @b = y [FROM ...]`: from nothing, assignments;
        from a table, into variables of its own, copied where a row came
        (T-SQL leaves the variables as they were where none does)."""
        pairs = [(e.this.name, e.expression) for e in tree.expressions]
        if not tree.args.get("from_"):
            for name, value in pairs:
                pg, _ = self.lookup(name, node.line)
                v = generate(self.rewrite(exp.select(value.copy()),
                                          node.line), "tsql", "postgres",
                             normalize=False)[len("SELECT "):]
                self.guarded([f"{pg} := {v};"])
            return
        self.code("tsql.select-assign", node.line)
        shadows, copies = [], []
        query = tree.copy()
        query.set("expressions", [p[1].copy() for p in pairs])
        for name, _ in pairs:
            pg, typ = self.lookup(name, node.line)
            shadow = self.fresh("s")
            self.decls.append(f"{shadow} {typ};")
            shadows.append(shadow)
            copies.append(f"{pg} := {shadow};")
        text = generate(self.rewrite(query, node.line), "tsql", "postgres",
                        normalize=False)
        lines = [_into(text, shadows) + ";",
                 "if found then"] + ["  " + c for c in copies] + ["end if;"]
        self.guarded(self.after_dml(lines))

    def _pg_version(self):
        return getattr(self.ctx, "pg_version", None) if self.ctx is not None \
            else None

    def _needs(self, version, what, line):
        """Refused where the target is older than `version` (or its version
        is not known) for `what`."""
        have = self._pg_version()
        if have is None or have < version:
            raise Refused(f"line {line} {what}, which PostgreSQL runs from"
                          f" version {version}"
                          + (f" and the target runs {have}" if have else
                             "; the target's version is not known here"))

    def s_sql(self, node):
        if node.verb == "MERGE":
            self.code("tsql.merge", node.line)
            self._needs(15, "merges", node.line)
            if re.search(r"(?i)\bnot\s+matched\s+by\s+source\b",
                         node.text):
                self._needs(17, "merges rows the source no longer has (WHEN"
                                " NOT MATCHED BY SOURCE)", node.line)
        out = _output_clause(node.text)
        if out is not None:
            return self._with_output(node, *out)
        text = self.sql(node.text, node.line)
        self.guarded(self.after_dml([f"{text};"]))

    # ---- OUTPUT ----------------------------------------------------------

    def _output_item(self, verb, item, line, table=None):
        """One OUTPUT item in the target's SQL: for the write's RETURNING,
        or - with `table` - for the shape of the table it is put in."""
        m = re.match(r'(?is)^(.*?)\s+as\s+(\[[^\]]+\]|"[^"]+"|\w+)$',
                     item.strip())
        expr, alias = (m.group(1), m.group(2).strip('[]"')) if m else \
            (item.strip(), None)

        def side(m):
            which, col = m.group(1).lower(), m.group(2)
            if table is not None:
                return f"{table}.{col}"
            if verb == "INSERT" and which == "inserted" or \
                    verb == "DELETE" and which == "deleted":
                return col
            if verb == "UPDATE" and which == "inserted":
                return f"new.{col}" if (self._pg_version() or 0) >= 18 \
                    else col
            self._needs(18, f"returns the row {'before' if which == 'deleted' else 'after'}"
                            f" the {verb.lower()} (OUTPUT {which}.)", line)
            return f"{'old' if which == 'deleted' else 'new'}.{col}"
        if re.search(r"(?i)\$action\b", expr):
            if table is not None:
                expr = re.sub(r"(?i)\$action\b", "cast('' as text)", expr)
            else:
                self._needs(17, "returns what a merge did with each row"
                                " ($action)", line)
                expr = re.sub(r"(?i)\$action\b", "merge_action()", expr)
        expr = re.sub(r"(?i)\b(inserted|deleted)\s*\.\s*(\[[^\]]+\]|\*|\w+)",
                      side, expr)
        got = self.expr(expr, line) if expr.strip() != "*" else "*"
        return got + (f" as {ident(alias)}" if alias else "")

    def _output_target(self, into, line):
        name = into.strip()
        if name.startswith("@"):
            pg, typ = self.lookup(name[1:], line)
            if typ != "table":
                raise Refused(f"line {line} puts the rows it wrote into"
                              f" {name}, which is not a table variable")
            return pg
        if name.startswith("#"):
            return self._temp(name[1:].strip("[]"))
        tree = self.rewrite(parse(f"SELECT 1 FROM {name}", "tsql"), line)
        return tree.find(exp.Table).sql(dialect="postgres")

    def _written_table(self, text, line):
        tree = self.rewrite(parse(text, "tsql"), line)
        t = tree.this if isinstance(tree.this, exp.Table) else \
            tree.find(exp.Table)
        if t is None:
            raise Refused(f"line {line}: the table it writes could not be"
                          " read")
        t = t.copy()
        t.set("alias", None)
        return t.sql(dialect="postgres")

    def _with_output(self, node, rest, items, into, cols):
        """A write with OUTPUT: its RETURNING put where OUTPUT puts it."""
        line = node.line
        self.code("tsql.output", line)
        dml = self.sql(rest, line)
        returning = ", ".join(self._output_item(node.verb, i, line)
                              for i in items)
        cte = self.fresh("o")
        written = f"with {cte} as ({dml} returning {returning})"
        if into is not None:
            target = self._output_target(into, line)
            listed = f" ({', '.join(ident(c) for c in cols)})" if cols \
                else ""
            self.guarded(self.after_dml([
                f"{written} insert into {target}{listed} select * from"
                f" {cte};"]))
            return
        if self.r.kind != "procedure":
            raise Refused(f"line {line} sends the rows it wrote (OUTPUT),"
                          " which only a procedure can")
        table = self._written_table(rest, line)
        shape = ", ".join(self._output_item(node.verb, i, line, table)
                          for i in items)
        name = f"_migkit_rs{len(self.result_sets) + 1}"
        self.result_sets.append(name)
        held = f"_migkit_out{len(self.result_sets)}"
        self.guarded(self.after_dml([
            f"drop table if exists {held};",
            f"create temporary table {held} as select {shape} from {table}"
            " where false;",
            f"{written} insert into {held} select * from {cte};"]))
        self.guarded([f"open {name} for select * from {held};"])

    def s_temp_create(self, node):
        tree = parse(node.text, "tsql")
        table = tree.find(exp.Table)
        t = self._temp(table.name.lstrip("#"))
        text = generate(tree, "tsql", "postgres").replace(
            f"CREATE TEMPORARY TABLE {table.name.lstrip('#')}",
            f"CREATE TEMPORARY TABLE {t}", 1)
        self.guarded([f"drop table if exists {t};", f"{text};"])

    def s_temp_drop(self, node):
        tree = parse(node.text, "tsql")
        names = [t.name.lstrip("#") for t in tree.find_all(exp.Table)]
        if not all(n for n in names) or not node.text.replace(
                "IF EXISTS", "").split("TABLE", 1)[-1].strip().startswith("#"):
            raise Refused(f"line {node.line} drops a table")
        self.guarded([f"drop table if exists {self._temp(n)};"
                      for n in names])

    def s_if(self, node):
        e = self.e
        e(f"if {self.expr(node.cond, node.line)} then")
        e.push()
        self.statement(node.then)
        e.pop()
        if node.otherwise is not None:
            e("else")
            e.push()
            self.statement(node.otherwise)
            e.pop()
        e("end if;")

    def s_while(self, node):
        e = self.e
        e(f"while {self.expr(node.cond, node.line)} loop")
        e.push()
        for s in node.body:
            self.statement(s)
        e.pop()
        e("end loop;")

    def s_break(self, node):
        self.e("exit;")

    def s_continue(self, node):
        self.e("continue;")

    def s_return(self, node):
        if self.r.kind == "function":
            value = self.expr(node.value, node.line)
            ret = pg_type(self.r.returns) if self.r.returns else None
            if ret and ret.startswith(("numeric(", "varchar(", "char(")):
                value = f"cast({value} as {ret})"
            self.guarded([f"return {value};"])
            return
        if node.value is not None and node.value.strip() not in ("0",):
            raise Refused(f"line {node.line} returns a status code, which a"
                          " PostgreSQL procedure has no counterpart for")
        for t in sorted(self.temps):
            self.e(f"drop table if exists {t};")
        self.e("return;" if self.r.kind != "trigger" else "return null;")

    def s_exec(self, node):
        if node.status is not None:
            raise Refused(f"line {node.line} keeps a procedure's status code")
        callee = self.ctx.routines.get(node.name.lower()) if self.ctx \
            else None
        if callee is None or callee.kind != "procedure":
            raise Refused(f"line {node.line} runs {node.name}, which is not"
                          " one of the source's procedures")
        args = []
        for named, value, out in node.args:
            if out:
                target = value.strip()
                if not target.startswith("@"):
                    raise Refused(f"line {node.line} passes OUTPUT something"
                                  " other than a variable")
                text = self.lookup(target[1:], node.line)[0]
            else:
                text = self.expr(value, node.line)
            args.append(f"{ident('p_' + named)} => {text}" if named else
                        text)
        self.code("tsql.exec", node.line)
        name = '"' + callee.name.replace('"', '""') + '"'
        self.guarded([f"call {name}({', '.join(args)});"])

    def s_print(self, node):
        self.e(f"raise notice '%', {self.expr(node.value, node.line)};")

    def s_raiserror(self, node):
        inner = node.text.strip()
        if inner.startswith("(") and inner.endswith(")"):
            inner = inner[1:-1]
        parts = _split_args(inner)
        if len(parts) < 3:
            raise Refused(f"line {node.line}: RAISERROR could not be read")
        msg = self.expr(parts[0], node.line)
        try:
            severity = int(parts[1].strip())
        except ValueError:
            raise Refused(f"line {node.line}: RAISERROR's severity is not a"
                          " number")
        args = [self.expr(a, node.line) for a in parts[3:]]
        fmt = re.sub(r"%[-+ 0#]*\d*(\.\d+)?[dioxXusS]", "%s", msg)
        message = f"format({fmt}, {', '.join(args)})" if args else fmt
        self.code("tsql.raiserror", node.line)
        if severity <= 10:
            self.e(f"raise notice '%', {message};")
        else:
            self.guarded([f"raise exception using errcode = 'P0001',"
                          f" message = {message};"])

    def s_throw(self, node):
        self.code("tsql.throw", node.line)
        if node.args is None:
            if not self.catching:
                raise Refused(f"line {node.line}: THROW without arguments"
                              " outside a CATCH")
            _, _, state, msg = self.catching[-1]
            self.guarded([f"raise exception using errcode = {state},"
                          f" message = {msg};"])
            return
        parts = _split_args(node.args)
        if len(parts) != 3:
            raise Refused(f"line {node.line}: THROW could not be read")
        msg = self.expr(parts[1], node.line)
        self.guarded([f"raise exception using errcode = 'P0001',"
                      f" message = {msg};"])

    def _cursor(self, name, line):
        got = self.vars.get("cursor:" + name.lower())
        if got is None:
            raise Refused(f"line {line} names a cursor not declared before"
                          " it")
        return got[0]

    def s_open(self, node):
        self.guarded([f"open {self._cursor(node.cursor, node.line)};"])

    def s_close(self, node):
        self.guarded([f"close {self._cursor(node.cursor, node.line)};"])

    def s_deallocate(self, node):
        self._cursor(node.cursor, node.line)

    def s_fetch(self, node):
        cursor = self._cursor(node.cursor, node.line)
        self.uses_fetch = True
        shadows, copies = [], []
        for t in node.targets:
            pg, typ = self.lookup(t, node.line)
            shadow = self.fresh("f")
            self.decls.append(f"{shadow} {typ};")
            shadows.append(shadow)
            copies.append(f"{pg} := {shadow};")
        lines = [f"fetch {cursor} into {', '.join(shadows)};"
                 if shadows else f"move {cursor};",
                 "_migkit_fetch_status := case when found then 0 else -1"
                 " end;", "if found then"] + ["  " + c for c in copies] + [
            "end if;"]
        self.guarded(lines)

    def _transaction_try(self, node):
        """(the CATCH after its ROLLBACK, whether it raises) where `node` is
        the procedure-wide transaction (`BEGIN TRY BEGIN TRAN ... COMMIT
        END TRY BEGIN CATCH [IF @@TRANCOUNT > 0] ROLLBACK ... END
        CATCH`), else None; Refused where its meaning cannot be kept."""
        body, catch = node.try_, node.catch
        if len(body) < 2 or body[0].kind != "tx" or \
                body[0].what != "begin" or body[-1].kind != "tx" or \
                body[-1].what != "commit" or not catch:
            return None
        if any(n.kind == "tx" for st in body[1:-1] for n in st.walk()):
            return None
        first = catch[0]
        then = getattr(first, "then", None) if first.kind == "if" else None
        if then is not None and then.kind == "block" and \
                len(then.body) == 1:
            then = then.body[0]
        if first.kind == "tx" and first.what == "rollback":
            pass
        elif first.kind == "if" and first.otherwise is None and \
                re.search(r"(?i)@@trancount|xact_state\s*\(", first.cond) \
                and then is not None and then.kind == "tx" and \
                then.what == "rollback":
            pass
        else:
            return None
        rest = catch[1:]
        if any(n.kind == "tx" for st in rest for n in st.walk()):
            return None
        raises = bool(rest) and rest[-1].kind in ("throw", "raiserror") \
            and (rest[-1].kind == "throw" or
                 _severity(rest[-1].text) > 10)
        if raises and any(n.kind in ("sql", "exec") for st in rest
                          for n in st.walk()):
            raise Refused(f"line {node.line}: its CATCH writes after the"
                          " ROLLBACK and then raises; SQL Server keeps those"
                          " writes, and the target's raise takes them back")
        top = getattr(self, "tree", [])
        while len(top) == 1 and top[0].kind == "block":
            # the body's own BEGIN ... END
            top = top[0].body
        if node not in top:
            return None
        if raises and any(n.kind in ("sql", "exec") for st in
                          top[:top.index(node)] for n in st.walk()):
            raise Refused(f"line {node.line}: the procedure writes before"
                          " its transaction begins and raises after rolling"
                          " it back; SQL Server keeps the writes before it,"
                          " and the target's raise takes them back")
        if self.tablevars:
            raise Refused(f"line {node.line} rolls back a transaction that"
                          " a table variable outlives; the target's"
                          " temporary table does not")
        return rest, raises

    def _try_transaction(self, node, rest):
        e = self.e
        self.code("tsql.try-transaction", node.line)
        state, msg = self.fresh("state"), self.fresh("msg")
        self.decls += [f"{state} text;", f"{msg} text;"]
        e("begin")
        e.push()
        for st in node.try_[1:-1]:
            self.statement(st)
        if len(node.try_) == 2:
            e("null;")
        e.pop()
        e("exception when others then")
        e.push()
        e(f"get stacked diagnostics {state} = returned_sqlstate,"
          f" {msg} = message_text;")
        self.catching.append((None, None, state, msg))
        for st in rest:
            self.statement(st)
        if not rest:
            e("null;")
        self.catching.pop()
        e.pop()
        e("end;")

    def s_try(self, node):
        pattern = self._transaction_try(node) if not self.frames else None
        if pattern is not None:
            return self._try_transaction(node, pattern[0])
        e = self.e
        label = self.fresh("try")
        err, state, msg = self.fresh("err"), self.fresh("state"), \
            self.fresh("msg")
        self.decls += [f"{err} boolean := false;", f"{state} text;",
                       f"{msg} text;"]
        self.code("tsql.try-catch", node.line)
        e(f"{err} := false;")
        e(f"<<{label}>>")
        e("begin")
        e.push()
        self.frames.append((label, err, state, msg))
        for s in node.try_:
            self.statement(s)
        if not node.try_:
            e("null;")
        self.frames.pop()
        e.pop()
        e(f"end {label};")
        e(f"if {err} then")
        e.push()
        self.catching.append((label, err, state, msg))
        for s in node.catch:
            self.statement(s)
        if not node.catch:
            e("null;")
        self.catching.pop()
        e.pop()
        e("end if;")

    def s_tx(self, node):
        if node.what == "rollback" and self.tablevars:
            raise Refused(f"line {node.line} rolls back, and a table"
                          " variable keeps its rows through a rollback"
                          " where the target's temporary table does not")
        if self.r.kind != "procedure":
            raise Refused(f"line {node.line} ends a transaction, which only a"
                          " procedure can")
        if self.frames:
            raise Refused(f"line {node.line} ends a transaction inside a"
                          " TRY; PostgreSQL cannot commit inside a block"
                          " that catches errors")
        self.code("tsql.transaction", node.line)
        self.commits = True
        if node.what == "begin":
            return
        self.e("rollback;" if node.what == "rollback" else "commit;")


#: the words that end an OUTPUT clause's list where no INTO does
_AFTER_OUTPUT = {"VALUES", "SELECT", "DEFAULT", "FROM", "WHERE", "OPTION",
                 "WHEN", "EXEC", "EXECUTE"}


def _output_clause(text):
    """(the statement without its OUTPUT clause, [the items as written],
    the INTO target or None, [its columns]) of a write, or None where it
    has no OUTPUT."""
    try:
        toks = _tokenize(text)
    except Exception:  # noqa: BLE001 - read as it is by the translator
        return None
    depth, at = 0, None
    for k, t in enumerate(toks):
        if t.token_type == TokenType.L_PAREN:
            depth += 1
        elif t.token_type == TokenType.R_PAREN:
            depth -= 1
        elif depth == 0 and t.token_type not in _QUOTED and \
                t.text.upper() == "OUTPUT":
            at = k
            break
    if at is None or at + 1 >= len(toks):
        return None
    k, depth = at + 1, 0
    while k < len(toks):
        t = toks[k]
        word = t.text.upper() if t.token_type not in _QUOTED else ""
        if t.token_type == TokenType.L_PAREN:
            depth += 1
        elif t.token_type == TokenType.R_PAREN:
            depth -= 1
        elif depth == 0 and (word == "INTO" or word in _AFTER_OUTPUT or
                             t.token_type == TokenType.SEMICOLON):
            break
        k += 1
    if k == at + 1:
        return None
    items = _split_args(text[toks[at + 1].start:toks[k - 1].end + 1])
    into, cols, after = None, [], k
    if k < len(toks) and toks[k].text.upper() == "INTO":
        j = k + 1
        if j >= len(toks):
            return None
        begin = toks[j].start
        if toks[j].token_type in (TokenType.PARAMETER, TokenType.HASH):
            j += 2
        else:
            j += 1
            while j + 1 < len(toks) and toks[j].token_type == TokenType.DOT:
                j += 2
        into = text[begin:toks[j - 1].end + 1]
        if j < len(toks) and toks[j].token_type == TokenType.L_PAREN:
            depth, m = 0, j
            while m < len(toks):
                if toks[m].token_type == TokenType.L_PAREN:
                    depth += 1
                elif toks[m].token_type == TokenType.R_PAREN:
                    depth -= 1
                    if depth == 0:
                        break
                m += 1
            cols = [c.strip().strip("[]") for c in
                    text[toks[j].end + 1:toks[m].start].split(",")
                    if c.strip()]
            j = m + 1
        after = j
    rest = text[:toks[at].start].rstrip() + (
        " " + text[toks[after].start:] if after < len(toks) else "")
    return rest.strip().rstrip(";"), items, into, cols


def _severity(text):
    """A RAISERROR's severity, or 16 where it cannot be read."""
    inner = str(text or "").strip()
    if inner.startswith("(") and inner.endswith(")"):
        inner = inner[1:-1]
    parts = _split_args(inner)
    try:
        return int(parts[1].strip())
    except (IndexError, ValueError):
        return 16


def _into(select_sql, targets):
    """A generated SELECT with `INTO a, b` after its select list, where
    PL/pgSQL reads it."""
    tree = parse(select_sql, "postgres")
    tree.set("into", exp.Into(this=exp.to_table("_migkit_into_")))
    return generate(tree, "postgres", "postgres", normalize=False).replace(
        "INTO _migkit_into_", f"INTO {', '.join(targets)}", 1)


def _split_args(text):
    out, depth, buf, quote = [], 0, [], False
    for ch in text:
        if ch == "'" and not quote:
            quote = True
        elif ch == "'" and quote:
            quote = False
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


def convert(routine, ctx=None):
    """`routine` (a SQL Server one) as PostgreSQL statements, or
    Refused."""
    r = routine
    g = _Gen(r, ctx)
    stmts = g.convert()
    name = '"' + r.name.replace('"', '""') + '"'
    params = []
    for p in r.params:
        pg, typ = g.vars[p.name.lstrip("@").lower()]
        mode = "" if p.mode == "in" else "inout "
        params.append(f"{mode}{pg} {typ}")
    if r.kind == "function":
        if not r.returns or r.returns.lower().startswith("table"):
            raise Refused("it returns a table, which migkit's rules do not"
                          " convert yet")
        body = g.emit(stmts)
        made = [f"create function {name}({', '.join(params)}) returns"
                f" {pg_type(r.returns)} language plpgsql as"
                f" {QUOTE}\n{body}\n{QUOTE}"]
    elif r.kind == "procedure":
        body = g.emit(stmts)
        params += [f"inout {n} refcursor default null" for n in g.result_sets]
        made = [f"create procedure {name}({', '.join(params)}) language"
                f" plpgsql as {QUOTE}\n{body}\n{QUOTE}"]
    elif r.kind == "trigger":
        if g.timing != "AFTER":
            raise Refused("it runs instead of the statement, which a"
                          " PostgreSQL table trigger cannot")
        if re.search(r"(?i)\b(update|columns_updated)\s*\(", r.body or
                     r.definition or ""):
            raise Refused("it asks which columns the statement set"
                          " (UPDATE() / COLUMNS_UPDATED()), which the target"
                          " does not say")
        table = g.table
        if ctx is not None and ctx.local is not None:
            table = ctx.local(exp.select("*").from_(exp.to_table(table))) \
                .find(exp.Table).name
        made = []
        for event in g.events:
            # one version per event: a transition table exists only for
            # the events that have one, and the other reads as empty
            g2 = _Gen(r, ctx)
            g2.convert()
            g2.event = event
            body = g2.emit(stmts)
            refs = {"INSERT": "new table as inserted",
                    "DELETE": "old table as deleted",
                    "UPDATE": "old table as deleted new table as inserted"
                    }[event]
            for missing in ({"INSERT": ["deleted"], "DELETE": ["inserted"],
                             "UPDATE": []}[event]):
                body = re.sub(rf"(?i)\b{missing}\b",
                              f"(select * from {table} where false) as"
                              f" {missing}", body)
            fn = '"' + f"{r.name}_{event.lower()}".replace('"', '""') + '"'
            # the first keeps the source's name, which is how the proof
            # finds it
            trig = name if event == g.events[0] else fn
            made.append(f"create function {fn}() returns trigger language"
                        f" plpgsql as {QUOTE}\n{body}\n{QUOTE}")
            made.append(f"create trigger {trig} after {event.lower()} on"
                        f" {table} referencing {refs} for each statement"
                        f" execute function {fn}()")
        g.code("tsql.trigger", 1)
    else:
        raise Refused(f"a {r.kind} is not something migkit's rules convert")
    return Converted(made, constructs=g.codes, result_sets=g.result_sets,
                     commits=g.commits)

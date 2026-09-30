"""One query or expression in another engine's SQL, or a refusal that says
why - never a statement the target runs with another meaning.

The translator under this is good at queries and silent about three
things, each measured on the installed build (30.18):

* a hierarchical query (`CONNECT BY`, `PRIOR`, `START WITH`) is generated
  back as written for every dialect, with no warning, so asking it to
  refuse what it cannot carry does not stop it: PostgreSQL then rejects the
  view at `CREATE`, or - worse - a MySQL target reads `connect` as
  something else;
* Oracle's `(+)` outer joins are refused, although a rewrite into ANSI
  joins ships beside it and is simply never applied;
* `TRY_CAST` becomes a plain `CAST` on PostgreSQL, MySQL and SQLite, which
  raises (or answers 0) where the source answered NULL, and passes through
  as `TRY_CAST` to Oracle, which has no such function.

A procedural body the parser cannot read comes back as a block of kept
text (`Command`), generated back verbatim; and an `IF` it half-reads comes
back as `... END AS IF`. Both are refused here: a body of statements goes
to the converters that read statements (`mysql_pg`, `tsql_pg`), not
through a query translator.
"""
import sqlglot
from sqlglot import exp

#: dialects whose own cast answers NULL where the conversion fails, and
#: how the translator already writes it there
NULL_ON_FAILURE = {"tsql": "try_cast", "duckdb": "try_cast",
                   "bigquery": "safe_cast", "spark": "try_cast",
                   "databricks": "try_cast", "trino": "try_cast"}

#: the nodes of a procedural body: the query translator has no generator
#: for them on PostgreSQL, MySQL or Oracle and writes an empty string or
#: garbage in their place
PROCEDURAL = (exp.Command, exp.Block, exp.IfBlock, exp.WhileBlock,
              exp.LoopBlock, exp.RepeatBlock, exp.StoredProcedure,
              exp.Declare, exp.DeclareItem, exp.Execute, exp.ExecuteSql,
              exp.Try)


class Refused(ValueError):
    """What is not translated, in the words an operator reads."""


def parse(sql, src):
    """The tree of `sql` read as `src`, or Refused."""
    import logging
    said = logging.getLogger("sqlglot")
    level = said.level
    # a statement it cannot read is said here as a refusal; its own
    # warning on the way to the same conclusion is noise on the console
    said.setLevel(logging.ERROR)
    try:
        tree = sqlglot.parse_one(sql, read=src)
    except Exception as e:  # noqa: BLE001 - said in migkit's words
        raise Refused("it could not be read as "
                      f"{src} SQL: {(str(e).splitlines() or [''])[0][:80]}")
    finally:
        said.setLevel(level)
    if tree is None:
        raise Refused("there is nothing in it to translate")
    return tree


def _procedural(tree):
    for node in tree.walk():
        if isinstance(node, PROCEDURAL):
            return node
        if isinstance(node, exp.Set) and not isinstance(
                node.parent, (exp.Update, exp.Insert)):
            return node
    return None


def _try_cast(node, dst):
    """A cast that answers NULL where the conversion fails, in `dst`'s own
    form - or Refused where `dst` has none."""
    if dst in NULL_ON_FAILURE:
        return node
    to = node.args["to"]
    operand = node.this
    if dst == "postgres":
        # `pg_input_is_valid` (PostgreSQL 16) asks the type's own input
        # function whether the text would convert; on an older server the
        # function is missing and the target refuses the statement at
        # `CREATE`, which is a refusal said by the server, not a change of
        # meaning. The operand goes through text, as the source's
        # `TRY_CAST` of a string does.
        text = exp.cast(operand.copy(), exp.DataType.build("text"))
        typename = exp.Literal.string(to.sql("postgres"))
        return exp.Case(ifs=[exp.If(
            this=exp.Anonymous(this="pg_input_is_valid",
                               expressions=[text, typename]),
            true=exp.cast(text.copy(), to.copy()))])
    if dst == "oracle":
        return exp.Cast(this=operand.copy(), to=to.copy(), default=exp.Null())
    if dst == "clickhouse":
        plain = to.copy()
        plain.set("nullable", False)
        return exp.Anonymous(this="accurateCastOrNull", expressions=[
            operand.copy(), exp.Literal.string(plain.sql("clickhouse"))])
    raise Refused(f"it casts with a result of NULL where the value does not"
                  f" convert, and {dst} has no cast that does that - its"
                  " own answers 0 or raises")


def generate(tree, src, dst, normalize=True):
    """`tree` (read as `src`) in `dst`'s SQL, or Refused. `normalize=False`
    keeps a function's name as written - a routine made with a quoted,
    mixed-case name is called by that name."""
    found = _procedural(tree)
    if found is not None:
        raise Refused("its body is statements, and a query translator"
                      " cannot carry statements")
    if dst != "oracle" and tree.find(exp.Connect) is not None:
        raise Refused(f"it is a hierarchical query (CONNECT BY), which"
                      f" {dst} does not run; it needs rewriting as a"
                      " recursive query")
    if tree.find(exp.Prior, exp.ConnectByRoot) is not None and \
            dst != "oracle":
        raise Refused(f"it reads a hierarchy's pseudo-columns, which {dst}"
                      " does not have")
    if src == "oracle" and dst != "oracle" and any(
            c.args.get("join_mark") for c in tree.find_all(exp.Column)):
        from sqlglot.transforms import eliminate_join_marks
        tree = eliminate_join_marks(tree)
        if any(c.args.get("join_mark") for c in tree.find_all(exp.Column)):
            raise Refused("an outer join written with (+) could not be"
                          " rewritten as an ANSI join")
    for node in list(tree.find_all(exp.TryCast)):
        new = _try_cast(node, dst)
        if new is not node:
            node.replace(new)
    try:
        return tree.sql(dialect=dst,
                        unsupported_level=sqlglot.ErrorLevel.RAISE,
                        **({} if normalize else
                           {"normalize_functions": False}))
    except Exception as e:  # noqa: BLE001 - said in migkit's words
        raise Refused((str(e).splitlines() or [""])[0][:100])


def translate(sql, src, dst, local=None):
    """`sql` read as `src`, handed to `local` (renames, qualifiers), and
    written as `dst` - or Refused."""
    tree = parse(sql, src)
    if local is not None:
        tree = local(tree)
    return generate(tree, src, dst)

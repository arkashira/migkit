"""A name or a text reaches the MySQL engine's SQL through one quoting
helper each (audit 2026-10-01, SEC4).

Names were put between back quotes by hand 175 times - `` `{db}`.`{t}` ``
- and three helpers did it besides; a name holding a back quote ended
itself where the quote stood (measured: `_quote_ident("a`b")` gave
`` `a``b` `` while the inline form gave `` `a`b` ``). They go through
`_qi` and `_ql` now (`util.quote_name`), and this reads the engine's
modules and fails on an f-string that puts a value between quotes itself,
or quotes glued around a value outside the helpers.
"""
import ast
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[1]
FILES = ("migkit/engines/mysql.py", "migkit/engines/my_pass.py")

#: a value put between two back quotes, or two single quotes, with
#: nothing but name characters around it: `` `{t}` ``, `'{x}'`
BETWEEN = (re.compile(r"`[^`\s]*\x00[^`\s]*`"),
           re.compile(r"'[^'\s]*\x00[^'\s]*'"))

#: quotes glued around a value, or a quote doubled by hand, outside the
#: helpers that are the one place it is done
GLUED = re.compile(r"""\"`\" \+|\+ \"`\"|'`' \+|\+ '`'"""
                   r"""|\.replace\(["']`["'], ["']``["']\)"""
                   r"""|\"'\" \+ |\+ \"'\"(?!\w)""")
#: the helpers, and the one place a default is written in standard SQL
#: for another engine (`default_shown`)
HELPERS = ("_qi", "_ql", "default_shown")


def quoted_by_hand(src):
    """[(line, what)] of every f-string in `src` that puts a value between
    quotes itself."""
    out = []
    for node in ast.walk(ast.parse(src)):
        if not isinstance(node, ast.JoinedStr):
            continue
        tpl = "".join(v.value if isinstance(v, ast.Constant) else "\x00"
                      for v in node.values)
        for p in BETWEEN:
            out += [(node.lineno, m.group(0).replace("\x00", "{}"))
                    for m in p.finditer(tpl)]
    return out


def glued_by_hand(src):
    """[(line, text)] of quotes glued around a value outside the helpers."""
    tree = ast.parse(src)
    inside = set()
    for fn in ast.walk(tree):
        if isinstance(fn, ast.FunctionDef) and fn.name in HELPERS:
            inside |= set(range(fn.lineno, fn.end_lineno + 1))
    lines = src.splitlines()
    return [(n, line.strip()) for n, line in enumerate(lines, 1)
            if n not in inside and GLUED.search(line)
            and not line.lstrip().startswith("#")]


def test_the_scanner_finds_what_it_is_for():
    # held to the shapes that were in the engine, so a scanner that finds
    # nothing is not taken for a clean file
    bad = ("db = 'a'\nt = 'b'\nq = f'select 1 from `{db}`.`{t}`'\n"
           "r = f\"show variables like '{t}'\"\n"
           "s = f\"`{t.replace('`', '``')}`\"\n")
    got = quoted_by_hand(bad)
    assert [line for line, _ in got] == [3, 3, 4, 5], got
    assert glued_by_hand("x = '`' + name + '`'\n") == [
        (1, "x = '`' + name + '`'")]
    assert glued_by_hand("y = name.replace('`', '``')\n") == [
        (1, "y = name.replace('`', '``')")]
    # the helpers' own work, and a quote that is SQL's own text, pass
    fine = ("def _qi(name):\n    return '`' + name.replace('`', '``')"
            " + '`'\n"
            "q = f\"select {x} from t where kind = 'r'\"\n")
    assert quoted_by_hand(fine) == [] and glued_by_hand(fine) == []


def test_no_name_is_quoted_by_hand_in_the_mysql_engine():
    bad = []
    for rel in FILES:
        src = (ROOT / rel).read_text()
        bad += [f"{rel}:{n}: {what}" for n, what in quoted_by_hand(src)]
        bad += [f"{rel}:{n}: {what}" for n, what in glued_by_hand(src)]
    assert not bad, "\n".join(bad)


def test_the_helpers_quote_what_broke_the_statements():
    from migkit.engines.mysql import MySQLEngine, _qi, _ql
    assert _qi("a`b") == "`a``b`"
    assert MySQLEngine._my_ident("a`b") == _qi("a`b")
    assert _ql("it's") == "'it''s'"
    # MySQL reads a backslash in a string as an escape: doubled
    assert _ql("C:\\new") == "'C:\\\\new'"


def test_a_hostile_name_stays_one_name_in_the_statements():
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(Hop(name="h", engine="mysql", source=Endpoint(),
                          target=Endpoint()))
    name = "x`; drop table t; --"
    sql = eng.neutral_create_index_sql("dst", "app", name, "ix", False,
                                       [name])
    assert "`x``; drop table t; --`" in sql, sql
    assert sql.count("`x``; drop table t; --`") == 2, sql
    create = eng.neutral_create_sql("dst", "app", name,
                                    [(name, "integer", ())], key=(name,))
    assert create.startswith("create table `app`.`x``; drop table t; --`")

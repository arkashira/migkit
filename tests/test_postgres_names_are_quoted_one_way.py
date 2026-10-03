"""A name or a text reaches the PostgreSQL engine's SQL through one
quoting helper each (audit 2026-10-01, SEC4).

Names were put between quotes by hand some 270 times - `"{sch}"."{tbl}"`,
`'{sch}'` - so a name holding a quote broke the statement, and a newline in
one written into a script of the client program's started a line of its
own. They go through `_ident` and `_literal` now, and this reads the
engine's modules and fails on an f-string that puts a value between quotes
itself, or a string built by gluing quotes around a value.
"""
import ast
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[1]
FILES = ("migkit/engines/postgres.py", "migkit/engines/pg_pass.py",
         "migkit/legs.py")

#: a value put between two quotes of the same kind with nothing but name
#: characters around it: `"{x}"`, `'{x}.{y}'`, `'({n},0)'`
BETWEEN = (re.compile(r'"[^"\s]*\x00[^"\s]*"'),
           re.compile(r"'[^'\s]*\x00[^'\s]*'"))

#: quotes glued around a value, or a quote doubled by hand, outside the
#: helpers that are the one place it is done
GLUED = re.compile(r"""'"' \+|\+ '"'|'"\."'|"'" \+ |\+ "'"(?!\w)"""
                   r"""|\.replace\('"', '""'\)|\.replace\("'", "''"\)""")
HELPERS = ("_ident", "_literal", "_conninfo_value")


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
    # held to the three shapes that were in the engine, so a scanner that
    # finds nothing is not taken for a clean file
    bad = ("sch = 'a'\ntbl = 'b'\nq = f'select 1 from \"{sch}\".\"{tbl}\"'\n"
           "r = f\"where n.nspname = '{sch}'\"\n"
           "s = f\"ctid >= '({tbl},0)'::tid\"\n")
    got = quoted_by_hand(bad)
    assert [line for line, _ in got] == [3, 3, 4, 5], got
    assert glued_by_hand("x = '\"' + name + '\"'\n") == [
        (1, "x = '\"' + name + '\"'")]
    # the helpers' own work, and a quote that is SQL's own text, pass
    fine = ("def _ident(name):\n    return '\"' + name + '\"'\n"
            "q = f\"select {x} from t where kind = 'r'\"\n")
    assert quoted_by_hand(fine) == [] and glued_by_hand(fine) == []


def test_no_name_is_quoted_by_hand_in_the_postgresql_engine():
    bad = []
    for rel in FILES:
        src = (ROOT / rel).read_text()
        bad += [f"{rel}:{n}: {what}" for n, what in quoted_by_hand(src)]
        bad += [f"{rel}:{n}: {what}" for n, what in glued_by_hand(src)]
    assert not bad, "\n".join(bad)


def test_the_helpers_quote_what_broke_the_statements():
    from migkit.engines.postgres import _ident, _literal
    assert _ident('a"b') == '"a""b"'
    assert _ident("line\nbreak") == '"line\nbreak"'
    assert _literal("it's") == "'it''s'"
    # a backslash reads the same whatever standard_conforming_strings is
    assert _literal("C:\\new") == "E'C:\\\\new'"
    assert _literal("plain") == "'plain'"


def test_a_file_name_made_from_a_table_name_stays_in_its_directory():
    from migkit.engines.postgres import _file_name
    assert _file_name("public.orders") == "public.orders"
    hostile = _file_name("../../etc/passwd")
    assert "/" not in hostile and not hostile.startswith(".")
    # two names that sanitise alike are two files
    assert _file_name("a/b") != _file_name("a_b")
    assert "\n" not in _file_name("new\nline")

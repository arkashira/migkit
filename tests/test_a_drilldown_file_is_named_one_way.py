"""A drilldown's file is named from its table in one place
(`engines.base.drill_file`, audit AUD-Z): a table name written raw into
`data-<table>.<kind>` reached outside the hop's report directory with a
`/` and shared a file with another table, and every reader that took the
name back by slicing read a quoted one wrong. This fails on a `data-`
file name built anywhere else."""
import ast
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
OWNER = ("engines/base.py", "drill_file")


def raw_names(src):
    """Lines of an f-string that builds a `data-` name with a value in it,
    or a slice that reads one back, outside `drill_file`/`drill_table`."""
    bad = []
    tree = ast.parse(src)
    owned = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in (
                "drill_file", "drill_table"):
            owned |= {id(n) for n in ast.walk(node)}
    for node in ast.walk(tree):
        if id(node) in owned:
            continue
        if isinstance(node, ast.JoinedStr) and node.values and isinstance(
                node.values[0], ast.Constant) and \
                node.values[0].value == "data-" and len(node.values) > 1 \
                and isinstance(node.values[1], ast.FormattedValue):
            bad.append(node.lineno)
        if isinstance(node, ast.Subscript) and "len(\"data-\")" in \
                ast.unparse(node).replace("'", '"'):
            bad.append(node.lineno)
    return bad


def test_no_drilldown_file_is_named_outside_the_one_helper():
    bad = [f"{p.relative_to(ROOT)}:{ln}"
           for p in sorted((ROOT / "migkit").rglob("*.py"))
           for ln in raw_names(p.read_text())]
    assert not bad, "name it through engines.base.drill_file:\n" + \
        "\n".join(bad)


def test_the_scan_would_notice_one():
    assert raw_names('p = d / f"data-{t}.{kind}"\n') == [1]
    assert raw_names('t = f.name[len("data-"):-len(".x")]\n') == [1]
    # a fixed name and a glob carry no table's name
    assert raw_names('p = d / "data-evidence.txt"\n'
                     'g = d.glob(f"data-*.{k}")\n') == []

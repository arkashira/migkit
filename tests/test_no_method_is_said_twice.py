"""No class or module says the same function twice.

The second of two `def`s of one name in a class silently replaces the
first: measured on 2026-10-03, a pre-flight added under a name a class
already had took the place of three checks - the tables in scope, names
differing only by case, another writer on the target - and nothing said
so. Two parts written apart and merged are where it happens; this reads
every module's tree and names any such pair.
"""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent / "migkit"


def _twice():
    out = []
    for p in sorted(ROOT.rglob("*.py")):
        tree = ast.parse(p.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, (ast.ClassDef, ast.Module)):
                continue
            seen = {}
            for b in node.body:
                if not isinstance(b, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                said = [ast.unparse(d) for d in b.decorator_list]
                if b.name in seen and not any(
                        w in d for d in said
                        for w in (".setter", ".deleter", "overload",
                                  ".register")):
                    out.append(f"{p.relative_to(ROOT.parent)}:"
                               f" {getattr(node, 'name', '<module>')}."
                               f"{b.name} at {seen[b.name]} and {b.lineno}")
                seen[b.name] = b.lineno
    return out


def test_no_function_replaces_another_of_its_name():
    assert _twice() == []

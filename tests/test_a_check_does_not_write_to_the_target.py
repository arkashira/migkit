"""A check reads. The programs it drives have to read too.

`migkit check` is the command an operator runs against a target that is
often still serving an application, and sometimes against a source they do
not own. Nothing it does should leave a mark. That holds for migkit's own
SQL, and it has to hold for the programs migkit drives underneath -
because those have their own ideas about what they may create.

Measured on Liquibase 4.33.0, pointed at a database it had never touched,
with a command that **failed**:

    liquibase ... rollback --tag=nope
    ERROR: Could not find tag 'nope' in the database

    select tablename from pg_tables where schemaname='public'
    databasechangelog
    databasechangeloglock
    t

Two tracking tables, from a command that did nothing else. The same probe
on the subcommand migkit actually uses:

    liquibase ... diff
    target before: t
    target after:  t
    source:        t

`diff` is clean, which is why the schema check can keep using it. This
file pins that migkit never reaches for one of the others.

**And it is why `rollback` is not wrapped.** It can only undo changesets
recorded in its own `DATABASECHANGELOG` table, and migkit never puts any
there - it generates DDL and the operator applies it, so there is never
anything for it to roll back. Getting to the point where there was would
mean writing those two tables into somebody's target as a side effect of
using migkit. migkit's own undo needs neither: `revert.py` takes the same
diff in the opposite direction at the same instant, names every forward
statement that no DDL can undo, and says so when no undo could be
generated at all.
"""
import ast
import pathlib

import pytest

#: Subcommands that only read. Anything else either changes the database or
#: records that it was there.
READ_ONLY = {"liquibase": {"diff", "diff-changelog", "snapshot", "status",
                           "--version"},
             "atlas": {"schema", "version"}}


#: how a subprocess is actually started in this package
RUNNERS = {"run", "Popen", "check_output", "call"}


def _argv_lists():
    """Every list literal handed to a subprocess runner as its argv.

    Scoped to the call rather than to any list of strings, because
    `tools.py` holds `["atlas", "liquibase"]` as a list of *names* - a
    scan that read that as a command line would report migkit running
    `atlas liquibase`, which is not a thing.
    """
    root = pathlib.Path(__file__).resolve().parents[1] / "migkit"
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            fn = (node.func.id if isinstance(node.func, ast.Name)
                  else node.func.attr if isinstance(node.func, ast.Attribute)
                  else "")
            if fn not in RUNNERS:
                continue
            argv = node.args[0]
            if not isinstance(argv, ast.List) or not argv.elts:
                continue
            words = [e.value for e in argv.elts
                     if isinstance(e, ast.Constant)
                     and isinstance(e.value, str)]
            if words:
                yield path.name, argv.lineno, words


@pytest.mark.parametrize("program", sorted(READ_ONLY))
def test_only_read_only_subcommands_are_invoked(program):
    bad = []
    for name, lineno, words in _argv_lists():
        if not words or words[0] != program:
            continue
        rest = [w for w in words[1:] if not w.startswith("-")]
        sub = rest[0] if rest else (words[1] if len(words) > 1 else "")
        if sub not in READ_ONLY[program]:
            bad.append(f"{name}:{lineno} {program} {sub}")
    assert not bad, bad


def test_the_scan_reaches_the_invocation_it_is_about():
    """A scan that matched nothing would pass for the wrong reason."""
    found = [(n, ln, w) for n, ln, w in _argv_lists()
             if w and w[0] == "liquibase"]
    assert found, "the liquibase invocation was not seen at all"
    assert all(w[1] == "diff" for _, _, w in found), found


def test_a_write_subcommand_would_be_caught():
    """The guard has teeth: the same check over an invented argv."""
    words = ["liquibase", "rollback", "--tag=x"]
    rest = [w for w in words[1:] if not w.startswith("-")]
    assert rest[0] not in READ_ONLY["liquibase"]


def test_migkit_has_its_own_undo_and_says_when_it_has_none():
    """Why nothing here needs a tracking table on the target."""
    from migkit import revert
    forward = 'alter table "t" drop column "v";'
    assert revert.script(forward, "", "structural-fix.sql") == "", \
        "an empty reverse must not produce a file that claims to be one"
    assert revert.summary(forward, "") == "", \
        "and the check must not claim an undo it does not have"
    body = revert.script(forward, 'alter table "t" add column "v" text;',
                         "structural-fix.sql")
    assert "cannot be" in body and "undone by any DDL" in body, body
    assert "Take one before" in body, body


def test_the_caller_says_so_when_there_is_no_undo():
    """`summary` returning '' has to become a sentence, not a gap."""
    src = (pathlib.Path(__file__).resolve().parents[1] / "migkit" /
           "engines" / "postgres.py").read_text()
    assert "no undo could be generated - take a backup first" in src

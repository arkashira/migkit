"""What migkit runs underneath is not the operator's problem.

migkit drives pgcopydb, reladiff, atlas, liquibase, mydumper, Debezium and
the rest. Someone using migkit is moving and verifying a database so their
application keeps working; which library read which catalogue is migkit's
business, and a verdict that says *"pgcopydb found a difference migkit
missed"* hands them a puzzle instead of a finding. They cannot act on it:
they did not install pgcopydb, they cannot run it, and the sentence tells
them nothing about their data.

The information those lines carry is real and worth keeping - how much
scrutiny a pair got, and whether two independent readings agreed. That
survives without a brand name: *"read two ways, with two answers"* says the
same thing and says it to someone who can use it.

This file pins that for the cross-check verdicts, across every branch, and
for the advice attached to them - a `fix_hint` telling the operator to run
`pgcopydb compare data` by hand is the same leak wearing a different hat.

The line that *is* allowed: the database's own vocabulary. `CREATE
SUBSCRIPTION` is PostgreSQL, not something migkit bolted on, and an
operator's DBA knows it. Only the programs migkit drives are hidden.

The other line worth stating: a missing prerequisite used to name the
program so the operator could install it, which is the one case where the
name buys something. It buys less than `migkit doctor --install`, which is
migkit's own command and installs whatever that machine is short of - so
the messages point there instead, and the name goes too.

The repo-wide scan at the bottom is what keeps this from drifting: every
string that reaches `Result(...)`, `SystemExit(...)` or `print(...)` is
checked, not just the two builders this file started with.
"""
import ast
import pathlib

import pytest

from migkit.config import Endpoint, Hop

#: names of things migkit drives, which an operator has no way to act on
TOOLS = ("pgcopydb", "reladiff", "sqeleton", "datacompy", "mydumper",
         "myloader", "pgloader", "mongodump", "debezium", "redpanda",
         "atlas", "liquibase", "pg_dump", "pg_restore", "mongorestore",
         "mongosync", "mysqldump", "sqlcmd", "riot", "redis-shake",
         "mirrormaker", "pgcompare")


def _engine(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="x", engine="postgres",
              source=Endpoint(host="h", port=1, user="u", password="p"),
              target=Endpoint(host="h", port=2, user="u", password="p"),
              databases=["appdb"])
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return PostgresEngine(hop)


def _leaks(result):
    text = f"{result.detail} {result.fix_hint}".lower()
    return [t for t in TOOLS if t in text]


DATA_BRANCHES = [
    ("could not be read twice", ({}, {}, False)),
    ("they disagree", ({"public.a": True}, {"public.a": False}, True)),
    ("no shared table", ({"public.a": True}, {"public.b": True}, True)),
    ("they agree", ({"public.a": True}, {"public.a": True}, True)),
]


@pytest.mark.parametrize("label,args", DATA_BRANCHES,
                         ids=[b[0] for b in DATA_BRANCHES])
def test_the_data_cross_check_never_names_a_tool(tmp_path, label, args):
    mine, theirs, ran = args
    got = _engine(tmp_path)._crosscheck_result("appdb", mine, theirs, ran)
    assert not _leaks(got), (label, got.detail, got.fix_hint)


SCHEMA_BRANCHES = [
    ("could not be read twice", (None, [], True)),
    ("no verdict to hold it against", (True, ["x"], None)),
    ("found what the check passed over", (True, ["Failed to find table"
                                                 " public.b"], True)),
    ("the check found what it does not look for", (False, [], False)),
    ("both say they differ", (True, ["x"], False)),
    ("both say they match", (False, [], True)),
]


@pytest.mark.parametrize("label,args", SCHEMA_BRANCHES,
                         ids=[b[0] for b in SCHEMA_BRANCHES])
def test_the_schema_cross_check_never_names_a_tool(tmp_path, label, args):
    theirs, found, mine = args
    got = _engine(tmp_path)._crosscheck_schema_result("appdb", theirs, found,
                                                      mine)
    assert not _leaks(got), (label, got.detail, got.fix_hint)


def test_every_branch_of_both_is_covered():
    """A guard on the guard: a new branch added to either builder would
    otherwise be unchecked, and this file would still be green."""
    src = (pathlib.Path(__file__).resolve().parents[1] / "migkit" /
           "engines" / "postgres.py").read_text()
    tree = ast.parse(src)
    for name, expected in (("_crosscheck_result", len(DATA_BRANCHES)),
                           ("_crosscheck_schema_result",
                            len(SCHEMA_BRANCHES))):
        fn = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef) and n.name == name)
        returns = [n for n in ast.walk(fn) if isinstance(n, ast.Return)]
        assert len(returns) == expected, (name, len(returns), expected)


def test_the_advice_points_at_a_migkit_command(tmp_path):
    """A `fix_hint` is what the operator does next, so it has to be
    something they can actually run."""
    eng = _engine(tmp_path)
    clash = eng._crosscheck_result("appdb", {"public.a": True},
                                   {"public.a": False}, True)
    assert "migkit " in clash.fix_hint, clash.fix_hint
    schema = eng._crosscheck_schema_result("appdb", True, ["x"], True)
    hint = schema.fix_hint
    assert hint and not _leaks(schema), hint
    # and it names files migkit itself wrote, which are on their disk
    assert "schema-src.sql" in hint and "schema-dst.sql" in hint, hint


def test_what_the_line_still_tells_them(tmp_path):
    """Removing the brand name must not remove the finding. Each verdict
    still has to say how much scrutiny the pair got and what it means."""
    eng = _engine(tmp_path)
    once = eng._crosscheck_result("appdb", {}, {}, False)
    assert "compared once" in once.detail, once.detail

    twice = eng._crosscheck_result("appdb", {"public.a": True},
                                   {"public.a": True}, True)
    assert "second time" in twice.detail and "same verdict" in twice.detail

    clash = eng._crosscheck_result("appdb", {"public.a": True},
                                   {"public.a": False}, True)
    assert "public.a" in clash.detail, clash.detail
    assert "cannot be relied on" in clash.detail, clash.detail

    missed = eng._crosscheck_schema_result(
        "appdb", True, ["Failed to find table public.b"], True)
    assert "public.b" in missed.detail, missed.detail
    assert "unverified" in missed.detail, missed.detail


#: call sites whose strings a person driving migkit reads
FACING = {"Result", "SystemExit", "print", "log", "chat", "say"}


def _facing_strings():
    """Every string constant that reaches an operator, with where it is.

    Scoped to the call rather than the file because `movers.py` is mostly
    argv - `["pgcopydb", "follow", ...]` is how the program is started, not
    something anybody reads, and banning the word there would ban running
    it at all.
    """
    root = pathlib.Path(__file__).resolve().parents[1] / "migkit"
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = (node.func.id if isinstance(node.func, ast.Name)
                  else node.func.attr if isinstance(node.func, ast.Attribute)
                  else "")
            if fn not in FACING:
                continue
            for c in ast.walk(node):
                if isinstance(c, ast.Constant) and isinstance(c.value, str):
                    yield path.name, c.lineno, c.value


def test_nothing_an_operator_reads_names_a_program_migkit_drives():
    leaks = [(f, ln, s.strip()[:70], t)
             for f, ln, s in _facing_strings()
             for t in TOOLS if t in s.lower()]
    assert not leaks, "\n".join(f"{f}:{ln} names {t}: {s}"
                                for f, ln, s, t in leaks)


def test_the_scan_would_notice():
    """A scan that matches nothing would pass for the wrong reason, so the
    reach is asserted rather than assumed."""
    found = list(_facing_strings())
    assert len(found) > 200, len(found)
    assert any("migkit doctor --install" in s for _, _, s in found), \
        "the scan is not reaching the strings it claims to read"
    # a planted leak has to be caught: the same check, over a string the
    # scan really would see
    planted = [t for t in TOOLS if t in "this needs pgcopydb installed"]
    assert planted == ["pgcopydb"], planted


def test_the_databases_own_vocabulary_is_not_on_the_list():
    """`CREATE SUBSCRIPTION`, `wal_level`, a replication slot - those are
    PostgreSQL, not something migkit bolted on, and hiding them would make
    the message useless to the DBA who has to act on it."""
    for word in ("create subscription", "replication slot", "wal_level",
                 "start replica", "binlog"):
        assert not [t for t in TOOLS if t in word], word


def _facing_messages():
    """One entry per call, with its string parts joined.

    An f-string split over several lines is several `Constant` nodes, and
    the sentence the operator reads is all of them - so a message whose
    advice sits in a later fragment would look like it had none.
    """
    root = pathlib.Path(__file__).resolve().parents[1] / "migkit"
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = (node.func.id if isinstance(node.func, ast.Name)
                  else node.func.attr if isinstance(node.func, ast.Attribute)
                  else "")
            if fn not in FACING:
                continue
            parts = [c.value for c in ast.walk(node)
                     if isinstance(c, ast.Constant)
                     and isinstance(c.value, str)]
            if parts:
                yield path.name, node.lineno, " ".join(parts)


PREREQ = ("needs a component that is not installed",
          "needs a component that is not available",
          "is not installed on this machine",
          "needs a comparison component that is not")


def test_a_missing_prerequisite_points_at_migkits_own_installer():
    """The one case where naming the program used to buy something. It buys
    less than a command that installs whatever this machine is short of."""
    said = [(f, ln, s) for f, ln, s in _facing_messages()
            if any(p in s for p in PREREQ)]
    assert len(said) >= 4, said
    for f, ln, s in said:
        assert "migkit doctor" in s, f"{f}:{ln} {s}"

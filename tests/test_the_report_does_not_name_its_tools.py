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

#: names of things migkit drives, which an operator has no way to act on:
#: every name the registry of programs knows (`tools.names`), and the
#: libraries and programs no install recipe carries
_LIBRARIES = ("pgcopydb", "reladiff", "sqeleton", "datacompy", "mydumper",
              "myloader", "pgloader", "mongodump", "debezium", "redpanda",
              "atlas", "liquibase", "pg_dump", "pg_restore", "mongorestore",
              "mongosync", "mysqldump", "sqlcmd", "riot", "redis-shake",
              "mirrormaker", "pgcompare", "ora2pg", "sqlines",
              "plpgsql_check", "sqlglot", "pglast", "mysqlsh", "mysql shell",
              "pg_basebackup", "pg_verifybackup", "openlogreplicator",
              "cobrix", "stingray", "jt400", "apprise", "redisshake",
              "dsbulk", "connect-mirror-maker", "reindex-from-snapshot",
              "documentsfromsnapshot", "pg_upgrade", "initdb",
              "mssql-scripter", "data_validation", "data-validation",
              "teradatasql", "pyodata", "singer", "airbyte",
              "ast-grep", "ast_grep", "tree-sitter", "hypopg")


def _registry_names():
    from migkit import tools
    return {n.lower() for n in tools.names()}


TOOLS = tuple(sorted(set(_LIBRARIES) | _registry_names()))


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


#: call sites whose strings a person driving migkit reads: the result rows
#: and the lines said, and the failures the runner raises a program's end
#: as (`runner.classify`), which a move says as they are
FACING = {"Result", "SystemExit", "print", "log", "chat", "say", "Refused",
          "Verdict", "NoSandbox", "HarnessError", "Failed", "Transient",
          "Blocked", "Stalled", "Exhausted"}


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


def test_no_help_text_names_a_program():
    """`migkit --help` is the first thing an operator reads, and the static
    scan never looked at it: it said the generic engine reaches
    "anything reladiff speaks". Every command's help, hidden ones too."""
    from click.testing import CliRunner

    from migkit import cli
    runner = CliRunner()
    leaks = []
    for name in [None] + sorted(cli.main.commands):
        argv = ([name] if name else []) + ["--help"]
        said = runner.invoke(cli.main, argv).output
        assert said.strip(), argv
        leaks += [(name or "migkit", t) for t in TOOLS if t in said.lower()]
    assert not leaks, leaks


def test_doctor_names_no_program(tmp_path, monkeypatch):
    """It said `brew install pgloader`: `doctor --install` is the answer,
    and a name only appears for what nothing here can install."""
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli, tools
    conf = tmp_path / "hops.yaml"
    conf.write_text("hops: {}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    # a machine short of everything, with a package manager to fix it
    monkeypatch.setattr(tools, "which", lambda n: None)
    monkeypatch.setattr(tools.shutil, "which",
                        lambda n, **kw: "/bin/brew" if n == "brew" else None)
    monkeypatch.setattr(tools.platform, "system", lambda: "Darwin")
    said = CliRunner().invoke(cli.main, ["doctor"]).output
    assert "migkit doctor --install" in said, said
    assert not [t for t in TOOLS if t in said.lower()], said


def test_the_evidence_export_names_no_program(tmp_path):
    """The evidence is handed to auditors and change boards: every file
    of it is read here, with a program's name planted in everything it is
    built from - the record's entries, the rates, the ways chosen, the
    digests' statements, the stored code, the assessment, the findings,
    a cutover's reason, a position - and none may come out."""
    import json

    from migkit import audit, export, session
    hop = Hop(name="x", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=5432, user="app"),
              target=Endpoint(host="10.0.0.2", port=5432, user="app"),
              databases=["appdb"])
    hop.report_dir = lambda db=None, _p=tmp_path: _p / db if db else _p
    (tmp_path / "appdb").mkdir()
    run = session.current()
    log = tmp_path / "changelog.jsonl"
    audit.append(log, {"op": "move-pgcopydb", "db": "appdb",
                       "table": "public.t"})
    audit.append(log, {"op": "move-mydumper", "db": "appdb"})
    audit.append(log, {"op": "repair", "db": "appdb", "kind": "rows",
                       "detail": "rows written back by mydumper"})
    (tmp_path / "throughput.json").write_text(json.dumps({
        "pgcopydb": [{"rows": 1000, "seconds": 2.0, "at": "2026-09-30T10:00"
                      ":00"}],
        "cdc/follow": [{"rows": 10, "seconds": 1.0,
                        "at": "2026-09-30T10:00:01"}],
        "reladiff": [{"rows": 10, "seconds": 1.0,
                      "at": "2026-09-30T10:00:02"}]}))
    (tmp_path / session.DECISIONS).write_text(json.dumps({
        "session": run, "at": "2026-09-30 10:00:00", "work": "appdb",
        "way": "the streaming copy",
        "reason": "the version-matched pgcopydb build is here",
        "passed": [{"way": "the dump and restore in parallel",
                    "why": "pg_dump is not installed"}]}) + "\n")
    (tmp_path / "digests.jsonl").write_text("".join(json.dumps({
        "session": run, "at": "2026-09-30 10:00:00", "db": "appdb",
        "table": "public.t", "side": side, "rows": 3, "digest": "42",
        "method": "own", "engine": "postgres", "salt": "ab",
        "sql": "select count(*) from public.t -- read like reladiff"})
        + "\n" for side in ("src", "dst")))
    (tmp_path / "appdb" / "code-conversion.json").write_text(json.dumps(
        {"routines": [{"routine": "function f", "class": "plpgsql",
                       "rung": "ora2pg"}]}))
    (tmp_path / "appdb" / "code-proof.json").write_text(json.dumps(
        {"verdicts": [{"routine": "function f", "status": "proved",
                       "reason": "converted by sqlines, run on both",
                       "first": None}]}))
    (tmp_path / "assess.json").write_text(json.dumps([
        {"level": "warn", "scope": "appdb", "item": "programs",
         "detail": "pgloader is not installed"}]))
    (tmp_path / "summary.json").write_text(json.dumps([
        {"check": "data", "scope": "appdb", "status": "diff",
         "category": "data", "detail": "debezium left 2 rows behind",
         "fix_hint": "run liquibase"}]))
    (tmp_path / "verdict.json").write_text(json.dumps(
        {"status": "different", "session": run}))
    (tmp_path / "cutover-runs.jsonl").write_text(json.dumps(
        {"id": "c1", "state": "undone", "why": "pgcopydb exited 1",
         "took": {"freeze": 0.5}, "settings": {"budget": 30}}) + "\n")
    (tmp_path / "appdb" / "tail-token.json").write_text(json.dumps(
        {"lsn": "0/16B3748", "reader": "pgcopydb follow"}))
    out = export.write(hop)
    said = {name: (out / name).read_text() for name in export.FILES}
    for name, text in said.items():
        leaks = [t for t in TOOLS if t in text.lower()]
        assert not leaks, (name, leaks)
    doc = json.loads(said["evidence.json"])
    # what was planted was read, in migkit's words - the scan reaches
    assert "move by the streaming copy" in [s["op"] for s in doc["steps"]]
    assert next(r for r in doc["moved"]["runs"] if r["rows"] == 1000)[
        "way"] == "the streaming copy", doc["moved"]["runs"]
    assert doc["code"][-1]["converted by"] == "the copy", doc["code"]
    assert doc["decisions"][0]["passed"][0]["why"] == \
        "the copy is not installed", doc["decisions"]
    assert doc["assessment"][0]["detail"] == "the copy is not installed"
    assert doc["positions"]["appdb"]["tail-token.json"]["lsn"] == \
        "0/16B3748", doc["positions"]
    assert doc["tables"][0]["src"]["statement"].startswith(
        "select count(*) from public.t"), doc["tables"]


# ---- the runner's own words, read as they are said ---------------------------
#
# The scan above reads string constants only: `f"{tool} ..."` and a command
# line joined into a message are invisible to it. What the runner says of a
# program is built from what the program printed and from its command line,
# so it is read here as it comes out, each program named as one migkit drives
# and printing the names of others.


def _leaks_in(text):
    low = str(text).lower()
    return [t for t in TOOLS if t in low]


def _program(tmp_path, name, body):
    """A stand-in program named as one migkit drives: `body` is its shell."""
    path = tmp_path / name
    path.write_text("#!/bin/sh\n" + body + "\n")
    path.chmod(0o755)
    return str(path)


def test_the_runner_says_a_failure_without_a_name_or_a_command_line(
        tmp_path):
    from migkit import runner
    prog = _program(tmp_path, "pg_dump",
                    "echo 'pg_dump: error: mydumper could not read table"
                    " orders' >&2; exit 3")
    with pytest.raises(runner.Failed) as e:
        runner.run([prog, "--password=CHANGE_ME_x", "--host", "10.0.0.9"])
    said = str(e.value)
    assert "could not read table orders" in said, said
    assert not _leaks_in(said), said
    # its command line is not in it: neither the flag nor the value
    assert "CHANGE_ME_x" not in said and "--password" not in said, said
    assert "10.0.0.9" not in said, said


@pytest.mark.parametrize("printed,kind", [
    ("pg_restore: FATAL:  password authentication failed for user app",
     "Refused"),
    ("mydumper: ERROR:  permission denied for table orders", "Refused"),
    ("pgcopydb: [TARGET 1] [53100] ERROR:  could not extend file", "Refused"),
    ("myloader: server has gone away, lost connection", "Transient"),
    ("dsbulk: some fault of its own", "Failed"),
])
def test_each_kind_of_failure_is_said_without_a_name(printed, kind):
    from migkit import runner
    got = runner.classify(1, printed, "the copy")
    assert type(got).__name__ == kind, (type(got), got)
    assert not _leaks_in(got), got


def test_a_program_ended_by_a_signal_or_a_limit_is_said_without_a_name(
        tmp_path):
    from migkit import runner
    got = runner.classify(-9, "mongorestore: restoring")
    assert "SIGKILL" in str(got) and not _leaks_in(got), got
    prog = _program(tmp_path, "dsbulk", "echo 'dsbulk: starting';"
                    " sleep 30")
    with pytest.raises(runner.Transient) as e:
        runner.run([prog], timeout=1, what="the online sync")
    assert "did not finish in 1s" in str(e.value), e.value
    assert not _leaks_in(e.value), e.value


def test_a_missing_program_points_at_migkits_installer_by_no_name():
    from migkit import runner
    with pytest.raises(runner.Blocked) as e:
        runner.run(["/nonexistent/riotx", "replicate"])
    assert "migkit doctor --install" in str(e.value), e.value
    assert not _leaks_in(e.value), e.value


def test_a_build_that_does_not_take_an_option_is_said_by_its_role(
        monkeypatch):
    from migkit import runner, toolversion
    monkeypatch.setattr(toolversion, "options",
                        lambda program, *sub, **kw: frozenset({"--threads"}))
    monkeypatch.setattr(toolversion, "version", lambda program: "0.9.1")
    said = runner.flags_refused(["mydumper", "--threads", "4",
                                 "--trx-tables"])
    assert "--trx-tables" in said and "dump program" in said, said
    assert "0.9.1" in said and "migkit doctor --install" in said, said
    assert not _leaks_in(said), said

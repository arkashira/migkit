"""Every flag migkit hands a wrapped program exists in the build installed.

Twice already a flag migkit used was gone from the program underneath:
`--trx-consistency-only` became `--trx-tables`, and `--purge-mode` went
away altogether. Each time the program stopped at option parsing, and only
a live run found out. This reads the command lines each bulk path builds
for its dry run and holds every long flag against the program's own
`--help` option column. It is the check backlog 46 asks CI to run against
each version in its matrix.

It found a gap in its own reader first: the MongoDB tools spell their flags
in camelCase with `=<value>`, and the option reader, written for lower-case
flags followed by a column gap, saw 6 of mongodump's 37.
"""
import pytest

from migkit import movers
from migkit.config import Endpoint, Hop


def _hop(engine, tmp_path):
    hop = Hop(name="w", engine=engine,
              source=Endpoint(host="10.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=2, user="u",
                              password="CHANGE_ME"),
              databases=["appdb"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _flags(argv):
    return {a.split("=", 1)[0] for a in argv
            if isinstance(a, str) and a.startswith("--")}


@pytest.mark.parametrize("mover,engine,programs", [
    ("mydumper_move", "mysql", ("mydumper", "myloader")),
    ("mongodump_move", "mongodb", ("mongodump", "mongorestore")),
    ("pgdump_move", "postgres", ("pg_dump", "pg_restore")),
    ("pgcopydb_move", "postgres", ("pgcopydb",)),
])
def test_every_flag_on_the_command_line_is_one_the_program_has(
        tmp_path, monkeypatch, mover, engine, programs):
    if not all(movers.which(p) for p in programs):
        pytest.skip(f"{programs} not installed here")
    # the streaming copy's plan with a table that outlasts the rest, so
    # the split's flags are on the line too
    monkeypatch.setattr(movers, "pgcopydb_runner", lambda: "local")
    monkeypatch.setattr(movers, "_source_sizes",
                        lambda hop, db: {"public.big": 900 << 20,
                                         "public.small": 1 << 20})
    steps = getattr(movers, mover)(_hop(engine, tmp_path), "appdb", 2, False,
                                   None)
    seen = 0
    for step in steps:
        argv = getattr(step, "argv", None)
        if not argv:
            continue
        # a piped step carries two programs, split at the pipe
        parts, cur = [], []
        for a in argv:
            if a == "|":
                parts.append(cur)
                cur = []
            else:
                cur.append(a)
        parts.append(cur)
        for part in parts:
            program = str(part[0]).rsplit("/", 1)[-1]
            if program not in programs:
                continue
            # a program with subcommands keeps its options there
            missing = _flags(part) - movers._long_options(
                program, *movers._subcommand(part))
            assert not missing, (program, sorted(missing))
            seen += 1
    assert seen, "no command line of these programs was looked at"


def test_the_reader_sees_the_mongodb_tools_flags():
    if not movers.which("mongorestore"):
        pytest.skip("mongorestore not installed here")
    got = movers._long_options("mongorestore")
    assert {"--archive", "--drop", "--bypassDocumentValidation",
            "--nsInclude", "--nsExclude"} <= got, sorted(got)[:20]


def test_prose_inside_a_description_is_still_not_a_flag():
    """The trap the reader was built around: mydumper names
    `--overwrite-tables` inside another option's description, and the
    binary does not have it."""
    if not movers.which("mydumper"):
        pytest.skip("mydumper not installed here")
    assert "--overwrite-tables" not in movers._long_options("mydumper")


def test_a_program_with_subcommands_is_asked_about_the_one_it_runs():
    """pgcopydb's own `--help` lists no option at all: asked without the
    subcommand, every flag the copy was passed looked absent."""
    if not movers.which("pgcopydb"):
        pytest.skip("pgcopydb not installed here")
    assert "--table-jobs" not in movers._long_options("pgcopydb")
    assert "--table-jobs" in movers._long_options("pgcopydb", "copy",
                                                  "table-data")
    assert movers._subcommand(["pgcopydb", "copy", "table-data",
                               "--dir", "/x"]) == ("copy", "table-data")
    assert movers._subcommand(["mydumper", "-h", "x"]) == ()
    assert movers._subcommand(["pgloader", "mysql://u@h/db", "pg"]) == ()


def test_a_column_one_space_short_is_still_read():
    """`--large-objects-jobs Number of ...`: the build's column is one
    space short for its longest flag, and the reader missed it."""
    if not movers.which("pgcopydb"):
        pytest.skip("pgcopydb not installed here")
    assert "--large-objects-jobs" in movers._long_options(
        "pgcopydb", "copy", "blobs")
    assert "--summary" in movers._long_options("pgcopydb", "list",
                                               "progress")
    # and prose after a flag inside a description is still not a flag
    text = ("  --real-one   Does a thing\n"
            "      use --inside-prose instead of it\n")
    assert movers._option_column(text) == {"--real-one"}

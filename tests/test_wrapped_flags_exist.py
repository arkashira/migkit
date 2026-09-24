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
])
def test_every_flag_on_the_command_line_is_one_the_program_has(
        tmp_path, mover, engine, programs):
    if not all(movers.which(p) for p in programs):
        pytest.skip(f"{programs} not installed here")
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
            missing = _flags(part) - movers._long_options(program)
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

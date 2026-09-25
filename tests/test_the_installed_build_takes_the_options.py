"""The build installed where the move runs takes every option the move
passes it (backlog 46).

`test_wrapped_flags_exist.py` holds the command lines against the builds
installed where the tests run. The move runs somewhere else, against
whatever build that machine has. Every wrapped program has changed
underneath migkit once already: a renamed flag, a removed one. Each time
the program stopped at option parsing, after the move had started.

Now `assess` names the installed build of each program the move would run,
says whether it is the build migkit was measured with, and says whether it
takes every option. A move with a build that does not stops before anything
is written. Neither names the program.
"""
import os
import stat
import subprocess

import pytest

from migkit import movers
from migkit.config import Endpoint, Hop


def _hop(engine, tmp_path):
    hop = Hop(name="w", engine=engine,
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="127.0.0.1", port=2, user="u",
                              password="CHANGE_ME"),
              databases=["appdb"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


@pytest.mark.parametrize("via,engine", [("mydumper", "mysql"),
                                        ("mongodump", "mongodb"),
                                        ("pgdump", "postgres")])
def test_the_builds_here_take_every_option(tmp_path, via, engine):
    if not all(movers.which(p) for p in movers.PROGRAMS[via]):
        pytest.skip("not installed here")
    hop = _hop(engine, tmp_path)
    assert movers.options_missing(hop, "appdb", via) == {}
    rows = movers.bulk_path_report(hop, "appdb", via)
    assert [r[0] for r in rows] == ["pass", "pass"], rows
    assert rows[0][1].startswith("the dump program, "), rows
    assert rows[1][1].startswith("the load program, "), rows
    assert all("takes every option" in r[2] for r in rows), rows


@pytest.fixture
def older_dump_program(tmp_path, monkeypatch):
    """A dump program on the path ahead of the real one: the same help,
    less one option the move passes, and another version."""
    import migkit.util as util
    real = movers.which("mydumper")
    if not real or not movers.which("myloader"):
        pytest.skip("not installed here")
    helptext = subprocess.run([real, "--help"], capture_output=True,
                              text=True, env=util.tool_env(None))
    kept = "".join(line for line in
                   (helptext.stdout + helptext.stderr).splitlines(True)
                   if "--no-schemas " not in line)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "help.txt").write_text(kept)
    fake = bin_dir / "mydumper"
    fake.write_text("#!/bin/sh\n"
                    'case "$1" in\n'
                    '  --version) echo "mydumper v0.12.7-2" ;;\n'
                    f'  --help) cat "{bin_dir / "help.txt"}" ;;\n'
                    "  *) echo ran >> \"$(dirname \"$0\")/ran\" ;;\n"
                    "esac\n")
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setattr(util, "TOOL_PATHS", [str(bin_dir)]
                        + list(util.TOOL_PATHS))
    return bin_dir


def test_a_build_without_an_option_is_named_in_assess(tmp_path,
                                                      older_dump_program):
    hop = _hop("mysql", tmp_path)
    assert movers.options_missing(hop, "appdb", "mydumper") == \
        {"mydumper": ["--no-schemas"]}
    rows = movers.bulk_path_report(hop, "appdb", "mydumper")
    dump = rows[0]
    assert dump[0] == "fail", rows
    assert dump[1] == "the dump program, 0.12.7", dump
    assert "not the build migkit was measured with (1.0.5)" in dump[2]
    assert "--no-schemas" in dump[2] and "option parsing" in dump[2], dump
    assert rows[1][0] == "pass", rows
    said = " ".join(" ".join(r) for r in rows)
    assert "mydumper" not in said and "myloader" not in said, said


def test_the_move_stops_before_it_writes_anything(tmp_path,
                                                  older_dump_program):
    hop = _hop("mysql", tmp_path)
    with pytest.raises(SystemExit) as e:
        movers.run_via("mydumper", hop, "appdb", 2, True, print)
    said = str(e.value)
    assert "the installed dump program (0.12.7)" in said, said
    assert "--no-schemas" in said and "Nothing has been written" in said
    assert "mydumper" not in said, said
    # the program was asked for its options and version, and never run
    assert not (older_dump_program / "ran").exists()


def test_assess_carries_the_rows(tmp_path, older_dump_program, monkeypatch):
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.delenv("MIGKIT_MOVER", raising=False)
    got = MySQLEngine(_hop("mysql", tmp_path))._bulk_path_rows()
    assert [(r["level"], r["scope"]) for r in got] == [
        ("fail", "bulk path"), ("pass", "bulk path")], got


def test_a_build_with_none_of_a_flags_spellings_is_a_failed_row(
        tmp_path, older_dump_program):
    """`tool_flag` stops the build of the command line itself; assess says
    so as a row rather than stopping."""
    helptext = older_dump_program / "help.txt"
    helptext.write_text("".join(l for l in helptext.read_text()
                                .splitlines(True)
                                if "--trx-tables" not in l
                                and "--trx-consistency-only" not in l))
    rows = movers.bulk_path_report(_hop("mysql", tmp_path), "appdb",
                                   "mydumper")
    assert rows[0][0] == "fail" and "--trx-tables" in rows[0][2], rows
    assert os.path.exists(helptext)

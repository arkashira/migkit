"""Every command ends one way (audit V6, V7, V8, V16, W1).

Only `move` turned a program's failure into words; every other command
showed a traceback with the program's name and whatever it printed, and
every stop, traceback and Ctrl-C ended 1 - the code of a difference.
SIGTERM ended Python where it stood. Text from servers and files was read
as Rich markup: `[dbo].[orders] src=10 dst=7` printed `. src=10 dst=7`,
and a `[/x]` in one raised before the check's summary was written.
"""
import os
import signal
import sqlite3

import pytest

from migkit import cli, verdict


@pytest.fixture
def lite(tmp_path, monkeypatch):
    import migkit.config as cfg
    for name in ("a.db", "b.db"):
        con = sqlite3.connect(tmp_path / name)
        con.execute("create table t (id integer primary key)")
        con.commit()
        con.close()
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 'a.db'}, user: x,"
        " password: s3cret-pw}\n"
        f"    target: {{host: {tmp_path / 'b.db'}, user: x,"
        " password: s3cret-pw}\n"
        "    databases: [main]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return tmp_path


def _run(*argv):
    from click.testing import CliRunner
    got = CliRunner().invoke(cli.main, list(argv))
    return got, got.output


def test_a_program_s_failure_is_said_without_it_or_a_password(
        lite, monkeypatch):
    def fails(hop):
        raise RuntimeError(
            "pg_dump: error: connection to server at \"10.0.0.9\", port"
            " 5432 failed: password authentication failed for user"
            " \"app\"\npostgresql://app:s3cret-pw@10.0.0.9:5432/app")
    monkeypatch.setattr(cli, "get_engine", fails)
    got, said = _run("check", "lite")
    assert got.exit_code == verdict.EXIT["failed"], said
    assert "Traceback" not in said and "pg_dump" not in said, said
    assert "s3cret-pw" not in said, said
    assert "password authentication failed" in said, said
    # the traceback is kept, for whoever debugs migkit - scrubbed too
    kept = (lite / "reports" / "lite" / "commands.log").read_text()
    assert "Traceback" in kept and "s3cret-pw" not in kept, kept


def test_ctrl_c_ends_130_not_as_a_difference(lite, monkeypatch):
    def stop(*a, **k):
        raise KeyboardInterrupt
    monkeypatch.setattr(cli, "_check", stop)
    got, said = _run("check", "lite")
    assert got.exit_code == verdict.EXIT["interrupted"], said


def test_a_stop_in_words_is_not_a_difference(lite):
    got, said = _run("rollback", "lite", "--db", "main")
    # no saved state: a stop, said, ending 3 - it ended 1
    assert got.exit_code == verdict.EXIT["failed"], said
    assert "no saved states" in said, said


def test_sigterm_is_taken_as_ctrl_c_for_the_command():
    import time
    before = signal.getsignal(signal.SIGTERM)
    with pytest.raises(KeyboardInterrupt):
        with cli._term_as_interrupt():
            os.kill(os.getpid(), signal.SIGTERM)
            # the handler raises at the next instruction; this never ends
            time.sleep(30)
    assert signal.getsignal(signal.SIGTERM) == before


def test_what_a_server_says_is_shown_as_it_is(capsys):
    cli._said_row({"check": "data", "scope": "[dbo].[orders]",
                   "status": "diff", "detail": "src=10 dst=7 [/x]"})
    out = capsys.readouterr().out
    assert "[dbo].[orders]" in out and "[/x]" in out, out
    cli.say_raw("select setval('[a]', 1)")
    assert "[a]" in capsys.readouterr().out


def test_the_console_does_not_cut_a_verdict_from_its_path():
    assert cli.console.soft_wrap is True


def test_the_playbooks_name_no_program():
    from migkit import advisors
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    for service in advisors.PLAYBOOKS:
        said = advisors.playbook(service).lower()
        assert not [t for t in TOOLS if t in said], (service, said)
    helps = " ".join(str(p.help or "") for c in cli.main.commands.values()
                     for p in c.params)
    assert "Flyway" not in helps and "DMS-style" not in (cli.sync.help
                                                         or "")

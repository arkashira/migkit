"""The vocabulary every run speaks to the operator.

Owner's rule (2026-09-24): progress and logs are normalised into migkit's
own events and words; nothing a wrapped program prints reaches the operator
as that program's output. These tests pin the vocabulary itself. Wiring it
into each path, and the live scan of what an operator actually sees, are
separate.
"""
import pytest

from migkit import wording as progress
from tests.test_the_report_does_not_name_its_tools import TOOLS

#: labels used inside migkit that are not program names but still mean
#: nothing to an operator
LABELS = ("pgdump", "pg_dump", "pg_restore", "builtin", "via")


def _clean(text):
    low = text.lower()
    return not [t for t in TOOLS + LABELS if t in low]


def test_no_phase_is_worded_with_a_program_name():
    for key, words in progress.PHASES.items():
        assert _clean(words), (key, words)


def test_a_phase_reads_as_a_sentence_with_its_facts():
    got = progress.phase("load", tables=12, workers=4)
    assert got == ("loading the local copy into the target:"
                   " 12 tables, 4 at a time"), got
    assert _clean(got)


def test_facts_that_are_empty_are_not_said():
    assert progress.phase("empty", left_out=0) == \
        "emptying the target's tables"


def test_an_unknown_phase_is_refused_not_printed_raw():
    with pytest.raises(ValueError):
        progress.phase("pg_restore")


def test_an_unworded_fact_is_refused_too():
    """A fact shown to the operator has to have wording decided for it,
    or a program's flag name ends up on the screen."""
    with pytest.raises(ValueError):
        progress.phase("load", jobs=4)


def test_progress_says_how_far_how_fast_and_how_long():
    got = progress.progress("orders", 1_200_000, 3_000_000, started=0.0,
                            now=10.0)
    assert got == ("orders: 1,200,000 of 3,000,000 rows (40%),"
                   " 120,000 rows/s, about 15s left"), got


def test_no_rate_or_time_left_is_invented():
    """Before anything has moved there is no rate, and a number made up
    for the moment is worse than none."""
    assert progress.progress("orders", 0, 3_000_000, started=0.0,
                             now=5.0) == "orders: 0 of 3,000,000 rows (0%)"
    assert progress.progress("orders", 50) == "orders: 50 rows"


def test_sizes_read_as_sizes():
    assert progress.human_bytes(512) == "512 bytes"
    assert progress.human_bytes(13_314_398_618) == "12.4 GB"


@pytest.mark.parametrize("argv,secrets,hidden", [
    (["x", "--source", "postgres://app:s3cr3t@10.0.0.1:5432/db"], (),
     "s3cr3t"),
    (["x", "--password=CHANGE_ME_42", "-B", "db"], ("CHANGE_ME_42",),
     "CHANGE_ME_42"),
    (["x", "-e", "PGPASSWORD", "--token", "tok-CHANGE_ME"],
     ("tok-CHANGE_ME",), "tok-CHANGE_ME"),
])
def test_the_debug_log_never_holds_a_secret(argv, secrets, hidden):
    line = progress.redact(argv, secrets)
    assert hidden not in line, line
    assert "***" in line, line


def test_the_debug_log_is_a_file_not_the_screen(tmp_path, capsys):
    log = progress.DebugLog(tmp_path / "run" / "commands.log")
    log.command(["x", "--password=CHANGE_ME_42"], ("CHANGE_ME_42",))
    assert capsys.readouterr().out == ""
    body = (tmp_path / "run" / "commands.log").read_text()
    assert "CHANGE_ME_42" not in body and "***" in body, body


def _pghop(tmp_path, exclude=()):
    from migkit.config import Endpoint, Hop
    hop = Hop(name="pg", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=5432, user="app",
                              password="CHANGE_ME_src"),
              target=Endpoint(host="10.0.0.2", port=5432, user="app",
                              password="CHANGE_ME_dst"),
              databases=["appdb"], exclude=list(exclude))
    hop.report_dir = lambda db=None: tmp_path
    return hop


def test_the_dump_plan_is_said_in_phases_and_runs_its_own_command(tmp_path):
    """Built once: the line shown and the command run are one step."""
    from migkit import movers
    steps = movers.pgdump_move(_pghop(tmp_path), "appdb", 4, False, None)
    for s in steps:
        assert _clean(s), s
    run = [s for s in steps if getattr(s, "argv", None)]
    assert [s.argv[0] for s in run] == ["pg_dump", "pg_restore"], run
    assert run[0] == progress.phase("dump", workers=4)
    assert run[1] == progress.phase("load", workers=4)
    assert "CHANGE_ME" not in " ".join(s.command for s in run)


def test_a_command_goes_to_the_debug_log_not_the_screen(tmp_path,
                                                         monkeypatch):
    from migkit import movers
    said = []
    monkeypatch.setattr(movers, "_DEBUG",
                        (progress.DebugLog(tmp_path / "commands.log"),
                         ("CHANGE_ME_42",)))
    movers._sh(["sh", "-c", "true", "--password=CHANGE_ME_42"],
               {"PGPASSWORD": "CHANGE_ME_env"}, said.append)
    assert said == [], said
    body = (tmp_path / "commands.log").read_text()
    assert "sh -c true" in body, body
    assert "CHANGE_ME_42" not in body, body


def test_a_failing_program_keeps_the_databases_words_and_loses_its_name():
    from migkit import movers
    with pytest.raises(RuntimeError) as e:
        movers._sh(["sh", "-c",
                    "echo 'pg_restore: error: connection to server at"
                    " 10.0.0.2 failed: password authentication failed"
                    " for user app' >&2; exit 1"])
    said = str(e.value)
    assert "password authentication failed for user app" in said, said
    assert _clean(said), said


def test_every_bulk_path_writes_its_commands_to_the_runs_debug_log(
        tmp_path, monkeypatch):
    """Set once, where every bulk path is dispatched, and cleared after, so
    a later run never writes into an earlier run's log."""
    from migkit import movers
    monkeypatch.setattr(movers, "which", lambda name: "/usr/bin/" + name)

    def fake(hop, db, workers, go, log):
        movers._sh(["sh", "-c", "true", "CHANGE_ME_dst"])
        return []

    monkeypatch.setattr(movers, "pgdump_move", fake)
    movers.run_via("pgdump", _pghop(tmp_path), "appdb", 1, True, None)
    body = (tmp_path / "commands.log").read_text()
    assert "sh -c true ***" in body, body
    assert movers._DEBUG is None


def test_a_missing_program_is_not_named(tmp_path, monkeypatch):
    from migkit import movers
    monkeypatch.setattr(movers, "which", lambda name: None)
    with pytest.raises(SystemExit) as e:
        movers.run_via("pgdump", _pghop(tmp_path), "appdb", 1, False, None)
    said = str(e.value)
    assert "migkit doctor --install" in said, said
    assert _clean(said), said


def _hop_for(engine, tmp_path):
    from migkit.config import Endpoint, Hop
    hop = Hop(name="h", engine=engine,
              source=Endpoint(host="10.0.0.1", port=1, user="app",
                              password="CHANGE_ME_src"),
              target=Endpoint(host="10.0.0.2", port=2, user="app",
                              password="CHANGE_ME_dst"),
              databases=["appdb"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


@pytest.mark.parametrize("mover,engine", [
    ("pgdump_move", "postgres"), ("mydumper_move", "mysql"),
    ("pgloader_move", "hetero"), ("mongodump_move", "mongodb")])
def test_every_bulk_plan_is_said_in_migkit_words(tmp_path, mover, engine):
    """The dry run is what an operator reads before `--go`, and pastes into
    a ticket. It used to be the command lines themselves."""
    from migkit import movers
    if mover == "mydumper_move" and not movers.which("mydumper"):
        pytest.skip("the MySQL dump program is not installed here")
    steps = getattr(movers, mover)(_hop_for(engine, tmp_path), "appdb", 4,
                                   False, None)
    for s in steps:
        assert _clean(s), s
        assert "CHANGE_ME" not in s, s
    assert [s for s in steps if getattr(s, "argv", None)], steps


def test_a_dry_run_leaves_no_file_holding_passwords(tmp_path):
    """The one-pass MySQL-to-PostgreSQL path wrote its load file, with both
    passwords in it, on a dry run too, and never removed it."""
    from migkit import movers
    movers.pgloader_move(_hop_for("hetero", tmp_path), "appdb", 2, False,
                         None)
    assert not list(tmp_path.iterdir()), list(tmp_path.iterdir())


def test_a_failed_copy_is_a_sentence_not_a_traceback(tmp_path, monkeypatch):
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli, movers
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  pg:\n    engine: postgres\n"
        "    source: {host: 10.0.0.1, port: 5432, user: u, password: p}\n"
        "    target: {host: 10.0.0.2, port: 5432, user: u, password: p}\n"
        "    databases: [appdb]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setattr(movers, "chosen", lambda engine, table="": "pgdump")

    def boom(*a, **k):
        raise RuntimeError("pg_restore: error: connection to server at"
                           " 10.0.0.2 failed: Connection refused")

    monkeypatch.setattr(movers, "run_via", boom)
    got = CliRunner().invoke(cli.main, ["move", "pg", "--mode", "full",
                                        "--go"])
    assert isinstance(got.exception, SystemExit), got.exception
    said = str(got.exception)
    assert "Connection refused" in said, said
    assert _clean(said), said

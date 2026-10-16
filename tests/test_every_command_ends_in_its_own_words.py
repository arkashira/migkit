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


# --- R21.9: the thirty ranked messages of the next-step audit, one row each

def _hop(tmp_path, monkeypatch):
    from migkit import config
    from migkit.config import Endpoint, Hop
    monkeypatch.setattr(config, "REPORTS", tmp_path)
    return Hop(name="h", engine="mysql", source=Endpoint(host="example.com"),
               target=Endpoint(host="example.com"), databases=["db"])


def _stopped_tail(hop, monkeypatch, which):
    from types import SimpleNamespace
    from migkit import tailctl
    monkeypatch.setattr("migkit.cutover.settings", lambda hop: None)
    monkeypatch.setattr(cli, "_topology", lambda hop: (False, False))
    monkeypatch.setattr(tailctl, "alive", lambda *a: True)
    monkeypatch.setattr(tailctl, "pause", lambda *a: False)
    monkeypatch.setattr(tailctl, "stop", lambda *a: None)
    monkeypatch.setattr(tailctl, "resume", lambda *a: None)
    ticks = iter(range(0, 10 ** 6, 100))
    monkeypatch.setattr(cli, "time", SimpleNamespace(
        monotonic=lambda: next(ticks), sleep=lambda s: None))
    engine = SimpleNamespace(tail_apply=lambda *a: None)
    leg = cli._stream_leg(hop, engine, "db", ["db"], "mysql")
    with pytest.raises(SystemExit) as stopped:
        getattr(leg, which)("db")
    return str(stopped.value)


def _row1(tmp_path, monkeypatch):
    return _stopped_tail(_hop(tmp_path, monkeypatch), monkeypatch, "end")


def _row2(tmp_path, monkeypatch):
    return _stopped_tail(_hop(tmp_path, monkeypatch), monkeypatch, "down")


def _row3(tmp_path, monkeypatch):
    from migkit import movers
    monkeypatch.setattr(movers, "_still_running",
                        lambda path: [(4242, "copy")])
    with pytest.raises(SystemExit) as stopped:
        cli._programs_ready(_hop(tmp_path, monkeypatch), "db", None)
    return str(stopped.value)


def _row4(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from migkit.state import Taken
    remote = SimpleNamespace(
        read_record=lambda path, sealed: ({}, 1),
        write_record=lambda *a, **k: (_ for _ in ()).throw(Taken()))
    ours = SimpleNamespace(remote=remote, path="p", _mine=set(),
                           items=lambda: [])
    monkeypatch.setattr("time.sleep", lambda s: None)
    with pytest.raises(SystemExit) as stopped:
        cli._Checkpoint._merge_into_remote(ours)
    return str(stopped.value)


def _row5(tmp_path, monkeypatch):
    from types import SimpleNamespace
    eng = SimpleNamespace(stream_writers=lambda db: [("the tail", True)],
                          pause_writer=lambda db, what, window: False,
                          resume_writer=lambda db, what: None)
    with pytest.raises(SystemExit) as stopped:
        cli._make_room_for_rows(_hop(tmp_path, monkeypatch), eng, "db",
                                [SimpleNamespace(kind="rows")])
    return str(stopped.value)


def _verdict_row(records, coverage=None, **kw):
    got = verdict.decide(records, hop=None, coverage=coverage)
    return got.why + " | " + got.next


def _row16(tmp_path, monkeypatch):
    return _verdict_row([])


def _row17(tmp_path, monkeypatch):
    return _verdict_row([{"status": "skip", "scope": "db"}])


def _row18(tmp_path, monkeypatch):
    return _verdict_row([{"status": "ok", "scope": "db",
                          "reused_from": "earlier"}])


def _row19(tmp_path, monkeypatch):
    return _verdict_row([{"status": "ok", "scope": "db"}],
                        {"db": ["one"]})


def _row20(tmp_path, monkeypatch):
    return _verdict_row([{"status": "error", "scope": "db"},
                         {"status": "odd", "scope": "db2"}])


def _row29(tmp_path, monkeypatch):
    src = {"p1": {"rows": 10, "digest": None, "bound": "a",
                  "catchall": False}}
    return " ".join(verdict.partition_differences("t", src, {}, hop="h"))


def _row30(tmp_path, monkeypatch):
    from migkit import report
    return report.no_join_key("public.t", "h", "db")


def _by_site(name, args):
    """A message the report module holds, the call site proved to use it."""
    def row(tmp_path, monkeypatch):
        from migkit import report
        owner = Path(cli.__file__)
        if name in ("address_unreadable", "source_unreachable"):
            owner = Path(guide.__file__)
        used = owner.read_text()
        assert f"_words.{name}(" in used or f"report.{name}(" in used, name
        return getattr(report, name)(*args)
    return row


from pathlib import Path  # noqa: E402

from migkit import guide  # noqa: E402

#: priority, trigger, words of the next step the operator is given
ROWS = [
    (1, _row1, ("cutover stopped", "write freeze", "rerun the cutover plan")),
    (2, _row2, ("cutover stopped", "keep the cutover blocked",
                "rerun the cutover plan")),
    (3, _row3, ("do not start another move", "4242",
                "rerun the original move command")),
    (4, _row4, ("reconcile their state", "rerun the original move command")),
    (5, _row5, ("no repair was applied", "asked to resume",
                "rerun the repair plan")),
    (6, _by_site("restore_unreachable", ("20260101-000000", "s3", "h")),
     ("migkit rollback h",)),
    (7, _by_site("record_changed", ("line 4", "h")), ("migkit history h",)),
    (8, _by_site("no_saved_states", ("h",)), ("migkit history h",)),
    (9, _by_site("schema_failed", (2, False, ["create t"], "h")),
     ("migkit schema h --convert",)),
    (10, _by_site("mask_refused", ("h", "d", [("c", "no such type")])),
     ("migkit move h",)),
    (11, _by_site("two_way_refused", ("no marker column", "h")),
     ("migkit doctor",)),
    (12, _by_site("replication_refused", ("the scope changed", "h")),
     ("migkit move h",)),
    (13, _by_site("copy_not_started", ("d", "too wide for memory", "h")),
     ("migkit doctor",)),
    (14, _by_site("repair_stopped", ("d", "the scratch copy failed", "h")),
     ("migkit sync h",)),
    (15, _by_site("scope_empty", ("h",)), ("migkit check h",)),
    (16, _row16, ("nothing was compared", "check scope", "migkit doctor",
                  "migkit check", "not treat this run as verified")),
    (17, _row17, ("every answer was skipped", "read each skip reason",
                  "migkit check")),
    (18, _row18, ("reused", "without `--resume`", "migkit check")),
    (19, _row19, ("narrowed to", "full configured hop", "without --db")),
    (20, _row20, ("could not be taken", "inspect the named failing findings",
                  "saved evidence", "migkit check")),
    (21, _by_site("config_unreadable", ("hops.yaml", "line 3")),
     ("migkit doctor",)),
    (22, _by_site("address_unreadable", ("source",)), ("migkit init",)),
    (23, _by_site("source_unreachable", ("refused",)), ("migkit init",)),
    # 24 (a side's connection failure) is doctor's own text, W16C's region
    (25, _by_site("probe_unasked", ("h", "src", "replication", "timed out")),
     ("migkit doctor",)),
    (26, _by_site("transport_failed", ("source", "unverified", "no check")),
     ("migkit doctor",)),
    (27, _by_site("port_silent", ("target", "timed out")),
     ("migkit doctor",)),
    (28, _by_site("hop_not_reread", ("bad yaml", "h")), ("next cycle",)),
    (29, _row29, ("missing on target, the source's holds 10 rows",
                  "`migkit schema h`", "`migkit check h`")),
    (30, _row30, ("no primary key for row joining",
                  "`migkit check h --db db --only data`",
                  "schema/key plan")),
]


@pytest.mark.parametrize("priority,trigger,words", ROWS,
                         ids=[f"row{p}" for p, _, _ in ROWS])
def test_each_ranked_message_says_the_next_step(priority, trigger, words,
                                                tmp_path, monkeypatch):
    said = trigger(tmp_path, monkeypatch)
    low = said.lower()
    for word in words:
        assert word.lower() in low, (priority, word, said)
    assert not any(t in low for t in ("pgcopydb", "reladiff", "mydumper",
                                      "debezium", "pt-table")), said


@pytest.mark.parametrize("records,coverage,status,action", [
    ([], None, "incomplete", "check scope"),
    ([{"status": "error"}], None, "error",
     "inspect the named failing findings"),
    ([{"status": "unknown"}], None, "error", "saved evidence"),
    ([{"status": "diff"}], None, "different", "review a repair plan"),
    ([{"status": "warn"}], None, "incomplete", "read each warning"),
    ([{"status": "skip"}], None, "incomplete", "read each skip reason"),
    ([{"status": "ok", "reused_from": "earlier"}], None, "incomplete",
     "without `--resume`"),
    ([{"status": "ok"}], {"db": ["one"]}, "incomplete",
     "full configured hop"),
    ([{"status": "ok"}], None, "same", "confirm the intended scope"),
])
def test_a_verdict_carries_its_next_step_without_changing_its_decision(
        records, coverage, status, action):
    from copy import deepcopy
    before = deepcopy(records)
    decided = verdict.decide(records, coverage=coverage)
    assert decided.status == status
    assert action in decided.next.lower(), decided
    # the step is its own field: `why` says what was found, once
    assert decided.next not in decided.why
    assert records == before


def test_the_next_step_is_in_the_record_the_report_and_the_view_read(
        tmp_path, monkeypatch):
    from migkit.report import render
    hop = _hop(tmp_path, monkeypatch)
    records = [{"check": "data", "scope": "db", "status": "error",
                "detail": "<script>bad</script>", "fix_hint": ""}]
    env = verdict.summarize(hop, records)
    assert env["next"] == verdict.decide(records, hop).next
    html = render(hop, records)
    assert "<script>bad</script>" not in html
    assert "&lt;script&gt;bad&lt;/script&gt;" in html
    import html as h
    assert h.escape(env["next"]) in html


def test_a_stop_that_names_no_step_gets_the_one_next_step():
    from migkit import report
    bare = report.stop_next("the target holds rows the copy would empty",
                            "move", "h")
    assert "`migkit doctor`" in bare and "rerun `migkit move h`" in bare
    assert report.stop_next("x failed", "doctor").endswith("`migkit doctor`")
    # a stop that already names its step is not given a second
    for said in ("then rerun the original move command",
                 "run `migkit history h` to see",
                 "fill it in, then: migkit doctor"):
        assert report.stop_next(said, "move", "h") == "", said


def test_a_stop_ends_on_its_step_at_the_command_line(lite, monkeypatch):
    def stop(*a, **k):
        raise SystemExit("the target holds rows the copy would empty")
    monkeypatch.setattr(cli, "_check", stop)
    got, said = _run("check", "lite")
    assert got.exit_code == verdict.EXIT["failed"], said
    assert "would empty" in said and "migkit doctor" in said, said
    assert "rerun `migkit check lite`" in said, said

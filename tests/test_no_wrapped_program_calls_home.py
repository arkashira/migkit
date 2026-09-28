"""A program migkit runs never calls its vendor on the operator's behalf.

The row-sync program checks for updates by default and sends the versions
of the operating system, Perl, MySQL and its driver to its vendor, then
prints what it hears on standard output - where the repair reads its
statements. The schema differs check a version server and send anonymous
usage. None of that is something the operator agreed to by running
`migkit check`, so every run is told not to, in the way each program
reads it: a switch on the command line, a variable in its environment, a
line of its configuration file.
"""
import subprocess

from migkit import util
from migkit.config import Endpoint, Hop


def _mysql_engine():
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="x", engine="mysql",
              source=Endpoint(host="10.0.0.1", port=3306, user="u",
                              password="p"),
              target=Endpoint(host="10.0.0.2", port=3306, user="u",
                              password="p"),
              databases=["appdb"])
    return MySQLEngine(hop)


def _capture(monkeypatch, stdout=""):
    seen = []

    def fake(cmd, **kw):
        seen.append((list(cmd) if not isinstance(cmd, str) else cmd,
                     kw.get("env") or {}))
        return subprocess.CompletedProcess(cmd, 0, stdout, "")
    monkeypatch.setattr(util.subprocess, "run", fake)
    return seen


def test_the_row_sync_is_told_not_to_check_for_updates(monkeypatch):
    import migkit.engines.mysql as my
    monkeypatch.setattr(my, "which", lambda n: "/usr/bin/" + n)
    seen = _capture(monkeypatch, "DELETE FROM `t` WHERE `id`=1;\n")
    got = _mysql_engine()._pt_sync_sql("appdb", "t", ["id"], [(1,)])
    assert got == ["DELETE FROM `t` WHERE `id`=1;"]
    (argv, env), = seen
    assert argv[0] == "pt-table-sync", argv
    assert argv[1] == "--no-version-check", argv
    assert argv.count("--no-version-check") == 1


def test_every_percona_program_gets_the_switch_once():
    assert util.quiet_argv(["pt-table-checksum", "h=x"]) == [
        "pt-table-checksum", "--no-version-check", "h=x"]
    assert util.quiet_argv(["/opt/bin/pt-table-sync", "--no-version-check",
                            "--print"]) == [
        "/opt/bin/pt-table-sync", "--no-version-check", "--print"]
    # a program with nothing to turn off is left as it is
    assert util.quiet_argv(["pg_dump", "-d", "x"]) == ["pg_dump", "-d", "x"]
    assert util.quiet_argv("echo pt-x") == "echo pt-x"


def test_the_schema_differs_are_told_through_their_environment(monkeypatch):
    seen = _capture(monkeypatch)
    util.run(["atlas", "schema", "diff"], check=False)
    (_, env), = seen
    for k, v in (("ATLAS_NO_UPDATE_NOTIFIER", "true"),
                 ("ATLAS_NO_ANON_TELEMETRY", "true"),
                 ("LIQUIBASE_ANALYTICS_ENABLED", "false")):
        assert env.get(k) == v, (k, env.get(k))


def test_a_caller_cannot_turn_it_back_on_by_accident(monkeypatch):
    """The operator's own environment is passed through, but migkit's
    switches are applied over it."""
    monkeypatch.setenv("LIQUIBASE_ANALYTICS_ENABLED", "true")
    assert util.tool_env()["LIQUIBASE_ANALYTICS_ENABLED"] == "false"


def test_a_program_started_by_the_movers_gets_the_same(monkeypatch):
    from migkit import movers
    seen = []

    class P:
        returncode = 0

        def __init__(self, cmd, **kw):
            seen.append((cmd, kw.get("env") or {}))
            self.pid = 0

        def communicate(self):
            return "", ""

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0
    monkeypatch.setattr(movers.subprocess, "Popen", P)
    movers._sh(["pt-archiver", "--source", "h=x"])
    (argv, env), = seen
    assert argv[:2] == ["pt-archiver", "--no-version-check"], argv
    assert env.get("DO_NOT_TRACK") == "1"


def test_the_online_sync_is_configured_with_telemetry_off():
    """Kept, and held: the sync reads it from its configuration file."""
    import inspect

    from migkit import movers
    src = inspect.getsource(movers.mongosync_move)
    assert '"disableTelemetry": True' in src
